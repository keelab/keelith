# 项目工程结构最佳实践

高质量的微服务项目不仅需要优秀的底层运行时，更需要一套清晰、可扩展且符合团队协作规范的工程目录结构。Keelith 推荐参考业界广泛认可的 **Clean Architecture（整洁架构）** 与 **DDD（领域驱动设计）** 思想，建立具备清晰单向依赖的目录规范。

---

## 1. 推荐的应用工程目录树

使用 `keelith new <name> --template service` 初始化创建的服务具有如下标准目录结构：

```text
my-service/
├── api/                          # [公开] API 契约定义与生成代码
│   └── order/
│       └── v1/
│           ├── order.proto       # Protobuf 接口与消息契约
│           ├── order.pb.go       # 消息序列化代码（protoc-gen-go 生成）
│           ├── order_grpc.pb.go  # gRPC 服务桩（protoc-gen-go-grpc 生成）
│           └── order.keelith.pb.go # Keelith 契约绑定（protoc-gen-go-keelith 生成）
│
├── cmd/                          # [入口] 服务组合根 (Composition Root)
│   └── order-service/
│       ├── main.go               # 应用程序唯一入口（组装对象、监听信号）
│       └── wiring.go             # (可选) 静态 DI 编译期生成的注入代码
│
├── configs/                      # [配置] 静态配置文件模板
│   ├── config.yaml               # 本地/默认环境配置（严禁存敏感明文）
│   └── config.prod.yaml          # 生产环境配置参考
│
├── internal/                     # [私有] 内部业务代码（外部包无法直接 import）
│   ├── biz/                      # 领域业务逻辑层 (Domain Layer)
│   │   ├── order.go              # 领域模型、聚合根、业务用例 (UseCase)
│   │   └── repo.go               # 领域持久化接口契约 (Repository Interface)
│   │
│   ├── data/                     # 数据访问基础设施层 (Infrastructure Layer)
│   │   ├── data.go               # 数据库/Redis 连接池初始化与生命周期包装
│   │   ├── order_repo.go         # 数据库访问实现（实现 biz.OrderRepo 接口）
│   │   └── outbox.go             # 事务消息 Outbox 投递组件
│   │
│   ├── service/                  # 契约适配与应用编排层 (Application/Adapter Layer)
│   │   └── order_service.go      # 实现 api/order/v1 定义的 RPC/HTTP 接口
│   │
│   └── server/                   # 传输服务器组装
│       ├── http.go               # HTTP 监听器、路由策略与特定中间件配置
│       ├── grpc.go               # gRPC 监听器与拦截器配置
│       └── ops.go                # 专属 Ops/健康检查运维端口配置
│
├── buf.yaml                      # Buf 契约工具链依赖配置
├── buf.gen.yaml                  # Protobuf 插件生成规则配置
├── go.mod                        # Go 模块依赖定义
└── keelith.project.json          # Keelith 项目与静态 DI 元数据声明
```

---

## 2. 核心分层与单向依赖原则

为了保证代码具备出色的**可测试性（Testability）**与**长期可维护性**，各层之间的依赖关系必须保持严格的单向流动：

```text
       ┌────────────────────────┐
       │   cmd/ (组合根 main)   │
       └───────────┬────────────┘
                   │ 组装并初始化各层
                   ▼
       ┌────────────────────────┐
       │  internal/service/     │ (应用服务层: 实现 proto 契约接口)
       └───────────┬────────────┘
                   │ 调用业务用例
                   ▼
       ┌────────────────────────┐
       │   internal/biz/        │ (领域核心层: 领域实体、纯粹业务逻辑)
       └───────────▲────────────┘
                   │ 实现 Repo 接口 (依赖倒置 DIP)
                   │
       ┌───────────┴────────────┐
       │   internal/data/       │ (数据持久层: PostgreSQL, Redis, Kafka)
       └────────────────────────┘
```

### 2.1 各分层职责详解

#### 1. `api/`：外部协议与唯一契约
- 存放服务对外暴露的 `*.proto` 文件及由插件自动生成的文件。
- 外部调用者（如客户端或其他服务）可以通过 `go get` 直接引用此目录下的类型定义，而无需触碰任何业务实现代码。

#### 2. `internal/biz/`：纯粹的领域核心
- 包含系统的领域实体（Entity）、值对象（Value Object）和业务用例（UseCase）。
- **完全解耦传输与存储**：`biz` 层绝不引入 `net/http`、`google.golang.org/grpc`，也不直接导入特定的数据库驱动或 ORM。
- 定义持久化数据接口（如 `type OrderRepo interface`），交由 `internal/data` 实现，遵循依赖倒置原则。

#### 3. `internal/data/`：基础设施与外部资源
- 负责与 PostgreSQL、MySQL、Redis、Kafka、第三方外部微服务进行真实通信。
- 封装复杂的 SQL、缓存击穿保护、事务控制以及 Keelith 事务型 Outbox 投递。
- **生命周期所有权**：数据库连接池（如 `*sql.DB`）应包装为 `app.Component`，由 Keelith 统一纳管连接建立与优雅关闭。

#### 4. `internal/service/`：接口适配与请求转发
- 实现 `api/order/v1` 中生成的服务端接口（如 `OrderServiceKeelithServer`）。
- 职责极其专一：校验入参 DTO -> 转换为领域模型 -> 调用 `biz` 层用例 -> 将业务结果或错误转换为 proto 响应并返回。

#### 5. `cmd/`：纯粹的组合根 (Composition Root)
- `main.go` 负责：
  1. 读取配置文件或环境变量。
  2. 构造日志、追踪等全局遥测组件。
  3. 依次实例化 `data` -> `biz` -> `service` -> `server`。
  4. 调用 `keelith.New(...)` 创建应用。
  5. 监听操作系统中断信号（SIGINT, SIGTERM），触发 `app.Run(ctx)`。

---

## 3. 工程防踩坑黄金法则 (Golden Rules)

> [!IMPORTANT]
> **法则一：禁止在 `init()` 函数中执行副作用操作**
> 绝对不要在库或包的 `func init()` 中连接数据库、启动后台 Goroutine 或读取文件系统。所有的状态初始化都必须显式通过构造函数（如 `NewOrderRepo(...)`）在组合根阶段完成。

> [!TIP]
> **法则二：保持 `internal/` 的绝对私有性**
> Go 编译器天然支持 `internal` 目录的访问控制限制。只有当前项目内的代码才能导入 `internal` 包，有效防止业务逻辑被外部项目非法反向依赖。

> [!WARNING]
> **法则三：错误与业务语义在 `service` 层统一转化**
> 数据库底层的 `sql.ErrNoRows` 不应直接透传给客户端。应当在 `data` 层转化为领域错误，再由 `service` 层包装为具备标准 `Code` 与 `Reason` 的 Keelith 统一错误（例如 `errors.New(404, "ORDER_NOT_FOUND", "订单不存在")`）。
