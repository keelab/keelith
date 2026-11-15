# 数据持久化、缓存与可靠事件

在微服务业务落地的过程中，最容易造成数据脏写和资损的痛点莫过于：**数据库双写不一致（Dual-Write Hazard）、缓存穿透与雪崩、以及消息发送在网络抖动下的丢失或重复**。

Keelith 提倡并内置了生产级的黄金持久化架构：**SQL 作为唯一事实源（Source of Truth） + Redis 旁路缓存（Cache-Aside） + 事务型发件箱（Transactional Outbox） + 幂接收件箱（Idempotent Inbox）**。

---

## 1. 生产数据读写黄金架构

```text
                     [写操作: 事务型发件箱]
               ┌────────────────────────────────┐
               │ 业务操作开始 (在一个本地事务内) │
               │   1. INSERT 业务表 (orders)    │
               │   2. INSERT 出站事件 (outbox)  │
               │   3. COMMIT 本地事务成功       │
               └───────────────┬────────────────┘
                               │
                               ▼ 异步轮询 / CDC 监听
               ┌────────────────────────────────┐
               │ Outbox Dispatcher (发件箱分发) │
               │   4. 发送消息至 Kafka / RabbitMQ│
               │   5. 收到 Broker Ack 后标记完成 │
               └───────────────┬────────────────┘
                               │
                               ▼
────────────────────────── 消息中间件 ──────────────────────────
                               │
                               ▼
                     [读操作与消息消费: 幂接收件箱]
               ┌────────────────────────────────┐
               │ Consumer 消费 (消费端本地事务) │
               │   1. 检查 Inbox 表 (是否存在)  │
               │   2. 若已消费，直接 Ack 跳过    │
               │   3. 执行下游业务并更新 Inbox  │
               │   4. 清理或刷新 Redis 旁路缓存 │
               └────────────────────────────────┘
```

---

## 2. 事务型发件箱 (Transactional Outbox)

在传统的微服务代码中，开发者经常写出如下危险模式：
```go
// ❌ 错误示范：存在严重双写不一致隐患！
func CreateOrder(order *Order) error {
    db.Save(order)            // 1. 写数据库成功
    kafkaProducer.Send(order) // 2. 若此时网络断开或服务被杀，Kafka 消息丢失，下游永久无法收到通知！
    return nil
}
```

Keelith 提倡通过 SQL Outbox 表将业务变更与消息广播绑定在同一个 ACID 本地事务中：

```go
package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/keelab/keelith/outbox"
)

type OrderStore struct {
	db     *sql.DB
	outbox *outbox.Dispatcher
}

func (s *OrderStore) CreateOrderWithEvent(ctx context.Context, order *Order) error {
	// 开启本地数据库事务
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. 写入业务主表
	_, err = tx.ExecContext(ctx, "INSERT INTO orders (id, amount) VALUES ($1, $2)", order.ID, order.Amount)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	// 2. 将事件写入同一数据库的 outbox 表
	eventPayload := fmt.Sprintf(`{"order_id":"%s","status":"CREATED"}`, order.ID)
	_, err = tx.ExecContext(ctx,
		"INSERT INTO keelith_outbox (topic, payload, created_at) VALUES ($1, $2, NOW())",
		"order.events", eventPayload,
	)
	if err != nil {
		return fmt.Errorf("enqueue outbox event: %w", err)
	}

	// 3. 提交本地事务，主表与事件表原子生效
	return tx.Commit()
}
```

后台由独立的 `outbox.Dispatcher` 轮询或捕获 Binlog，将事件批量可靠推送到 Kafka 集群。即便机器在任意时刻崩溃重启，发件箱中的未确认消息也绝对不会遗失，实现 **至少一次（At-Least-Once）投递保证**。

---

## 3. 消费端收件箱与幂等控制 (Inbox Pattern)

由于消息网络可能发生重试，“至少一次投递”意味着下游必须具备防重消费能力：
1. 提取消息头中的 `keelith-outbox-id` 或业务唯一键（`x-idempotency-key`）。
2. 在处理业务逻辑的同一个事务中，将此 ID 写入 `inbox` 消费去重表。
3. 如果发生主键唯一索引冲突，说明该消息属于网络重试的重复包，直接提交并向 Broker 返回 Ack，杜绝重复扣款等资损事故。

---

## 4. 旁路缓存 (Cache-Aside) 与失效策略

对于高频读取数据，推荐使用标准旁路缓存策略：
- **读路径**：先读 Redis；命中则直接返回。未命中则回表查询 PostgreSQL，并写入 Redis（设置合理 TTL 与随机打散时间，防止缓存雪崩）。
- **写路径**：先更新 PostgreSQL，事务提交后**淘汰（Invalidate）**对应的 Redis 缓存键，而不是直接在内存中原地修改缓存值。

---

## 5. 数据库版本迁移规范

> [!IMPORTANT]
> **生产严禁使用 AutoMigrate**：
> 在生产环境中，切勿使用 ORM 的 `AutoMigrate` 自动建表或改表。字段的增加、索引调整、数据清洗必须使用版本化的 SQL 迁移工具（如 `go-migrate` 或 `goose`）进行版本追踪和灰度执行。
