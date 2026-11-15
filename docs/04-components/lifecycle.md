# 应用生命周期与组件拓扑

在生产环境中，一个看似正常的微服务在发布、重启或扩缩容时，很容易出现偶发 502 错误或数据库连接打满的情况。这通常是因为**启动与停机生命周期缺乏确定性编排**导致的：
- 数据库连接池还未初始化完毕，HTTP 端口就已经对外开放接收请求。
- 收到停止信号时，进程直接暴力退出，导致正在执行到一半的支付扣款事务被硬性中断。
- 在停止服务时，数据库连接池先被关闭了，导致仍在排空的在途请求遭遇连接断开报错。

Keelith 通过显式的 **DAG 组件拓扑** 与 **确定性生命周期状态机**，彻底消除了这些隐患。

---

## 1. 核心抽象：Server 与 Component

在 Keelith 中，所有具有启动与停止状态的单元被严格划分为两类：

### 1.1 `server.Server` 接口
专指**接收外部网络请求并提供服务的监听端点**（如 HTTP Server、gRPC Server、WebSocket Server、Ops Server）：
```go
type Server interface {
    Start(context.Context) error
    Stop(context.Context) error
}
```
- `Start` 返回时，该端口必须已经准备就绪，可以立刻接客。
- `Stop` 必须是**幂等且平滑**的：停止接收新请求，并等待存量在途请求处理完毕。

### 1.2 `app.Component` 接口
专指**服务所依赖的底层资源、后台任务或驱动**（如 数据库连接池、Redis 客户端、Kafka 消费者、定时任务 Worker）：
```go
type Component interface {
    Name() string
    Dependencies() []string
    Start(context.Context) error
    Stop(context.Context) error
}
```

---

## 2. 拓扑编排：拓扑正序启动，相反逆序停止

Keelith 自动分析各个组件之间的 `DependsOn` 依赖声明，构建有向无环图（DAG）：

```text
[启动流程: 拓扑正序]
PostgreSQL Pool ──► Redis Client ──► Kafka Consumer ──► HTTP / gRPC Server
  (基础存储)          (旁路缓存)         (后台消费者)         (对外服务就绪)

─────────────────────────────────────────────────────────────────────────

[停止流程: 严格逆序平滑排空]
HTTP / gRPC Server ──► Kafka Consumer ──► Redis Client ──► PostgreSQL Pool
  (1.停止接收新请求)    (2.停止消费消息)    (3.关闭缓存连接)    (4.最后释放主库)
  (2.等待在途请求排空)
```

### 2.1 自定义组件接入示例
你可以通过 `app.ComponentFunc` 轻松将数据库连接池纳入生命周期：

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
	"github.com/keelab/keelith"
	kapp "github.com/keelab/keelith/app"
)

func main() {
	var db *sql.DB

	// 定义数据库组件
	dbComponent := kapp.ComponentFunc{
		ComponentName: "postgres-pool",
		DependsOn:     nil, // 顶级依赖
		StartFunc: func(ctx context.Context) error {
			var err error
			db, err = sql.Open("postgres", "postgres://...")
			if err != nil {
				return err
			}
			return db.PingContext(ctx) // 确保在启动阶段验证联通性
		},
		StopFunc: func(ctx context.Context) error {
			if db != nil {
				return db.Close()
			}
			return nil
		},
	}

	// 定义依赖于数据库的后台 Worker 组件
	workerComponent := kapp.ComponentFunc{
		ComponentName: "order-reconciliation-worker",
		DependsOn:     []string{"postgres-pool"}, // 显式声明依赖关系
		StartFunc: func(ctx context.Context) error {
			// 启动后台对账任务
			return nil
		},
		StopFunc: func(ctx context.Context) error {
			// 停止后台任务
			return nil
		},
	}

	app, err := keelith.New(
		keelith.WithName("order-app"),
		keelith.WithHTTP("127.0.0.1:8080"),
		// 注册组件
		keelith.WithComponent(dbComponent, workerComponent),
		// 设置平滑停机的总安全超时时间
		keelith.WithStopTimeout(30*time.Second),
	)
	if err != nil {
		panic(err)
	}

	_ = app.Run(context.Background())
}
```

---

## 3. 生命周期状态流转图

应用的完整状态机流转如下图所示：

```text
       ┌──────────┐
       │   NEW    │ 内存构造对象图与依赖检查 (keelith.New)
       └────┬─────┘
            │ app.Run(ctx)
            ▼
       ┌──────────┐
       │ STARTING │ 按 DAG 依赖顺序启动 Components，拉起 Servers
       └────┬─────┘
            │ 全部启动成功
            ▼
       ┌──────────┐
       │  READY   │ 对外开放流量，Kubernetes Readiness 探针转为 200 OK
       └────┬─────┘
            │ 捕获 SIGINT / SIGTERM / ctx.Done()
            ▼
       ┌──────────┐
       │ DRAINING │ 立即将健康状态切为 Unready (通知 K8s 切断流量)
       └────┬─────┘ 允许在途存量请求继续完成排空
            │
            ▼
       ┌──────────┐
       │ STOPPING │ 停止 Servers，按逆序关闭所有 Components
       └────┬─────┘
            │
            ▼
       ┌──────────┐
       │ STOPPED  │ 进程安全退出 (Zero In-Flight Drop)
       └──────────┘
```

> [!TIP]
> **Draining 阶段的生产价值**：
> 在 Kubernetes 滚动更新时，Pod 从收到 Terminating 信号到 Endpoint 从集群负载均衡上摘除需要数十毫秒至数秒的网络同步时间。Keelith 进入 `DRAINING` 阶段会立刻使 `/readyz` 返回 503，使外部流量切断生效，同时内部 Server 仍保持连接为老请求服务，真正做到零报错滚动更新。
