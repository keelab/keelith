# 设计理念与核心概念

在微服务架构的大规模实践中，真正让系统变得难以维护的，往往不是写出一个处理 HTTP 请求的 Handler，而是**如何让跨协议的 HTTP、gRPC、后台 Worker、配置监听器、健康探针与运维端点，在生命周期、错误模型、元数据传递、弹性治理与可观测性上保持高度一致**。

Keelith（契利思）正是在这一背景下诞生。它是一个面向生产级集群、**契约优先（Contract-First）**的 Go 运行时组合层。

---

## 1. Keelith 解决什么问题

传统的微服务框架往往走向两个极端：
1. **全家桶框架**：内置了 ORM、网关、自研 RPC、配置中心和全套脚手架。初期上手快，但随着团队演进，基础设施必须适配已有公司自研组件时，框架的强耦合会带来巨大的迁移阵痛和版本包袱。
2. **零散库拼装**：自由组合 Gin/Echo + gRPC + Zap + Viper + GORM。然而各个独立库之间缺乏统一的生命周期编排与上下文契约：HTTP Header 里的 trace ID 无法自动流转到 gRPC metadata；配置热更新时数据库连接池没有排空导致请求中断；重试策略在客户端和网关重复配置引发“重试风暴”。

Keelith 选择了一条新的路径：**不做全家桶，也不做松散粘合剂，而是充当服务运行时的“契约中枢与组合根”**。

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        Keelith 运行时中枢                                │
│                                                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │ HTTP Surface │  │ gRPC Surface │  │ Async Worker │  │ Ops/Health │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘  │
│         │                 │                 │                │         │
│  ───────┴─────────────────┴─────────────────┴────────────────┴───────  │
│                     统一运行时契约 (Runtime Contract)                    │
│   • Operation • Metadata • Errors • Validation • Identity • Placement │
│  ────────────────────────────────────────────────────────────────────  │
│                                                                        │
│  ┌───────────────────────┐   ┌──────────────────────────────────────┐  │
│  │ 弹性治理 (Governance) │   │ 统一生命周期 (Lifecycle DAG)         │  │
│  │ Timeout, Retry Budget,│   │ 拓扑依赖启动, 就绪探针, 逆序优雅停机 │  │
│  │ Breaker, Bulkhead,    │   └──────────────────────────────────────┘  │
│  │ RateLimit, Shedding   │   ┌──────────────────────────────────────┐  │
│  └───────────────────────┘   │ 零敏感描述 (Secret-Free Diagnostics) │  │
│                              │ Application.Describe() 静态快照审计  │  │
│                              └──────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 核心架构设计哲学

### 2.1 契约优先 (Contract-First)
业务逻辑的唯一输入和输出必须建立在严谨的契约之上。Keelith 提倡以 **Protobuf / API Schema** 作为系统唯一的事实来源（Single Source of Truth）：
- 协议定义生成统一的 Go 接口与数据模型。
- 通过 `service.Binding` 自动将业务实现映射为 HTTP 路由与 gRPC 服务注册。
- **传输无关性**：领域逻辑不直接依赖 `*http.Request` 或 gRPC 的 `metadata.MD`，所有跨系统上下文均通过 Keelith 传输中立的 `metadata.Metadata` 传递。

### 2.2 显式组合根 (Composition Root)
在经典的架构设计中，依赖注入与对象拓扑的构建应当发生在程序的入口点（`main.go`）。
- **`keelith.New(...)` 纯装配**：构造阶段仅做依赖图构建、参数校验与环依赖检测，**严禁产生任何网络 I/O、连接建立或 Goroutine 泄漏**。
- **`Application.Run(ctx)` 纯执行**：显式启动应用，按 DAG 拓扑顺序拉起各底层组件与服务。
- 杜绝在任何子包的 `init()` 函数中启动后台线程或连接外部系统。

### 2.3 零凭证泄漏的运行时快照 (Secret-Free Diagnostics)
在生产运维与故障排查时，获取系统的运行时拓扑（如：监听了哪些端口、注册了哪些服务、使用了哪些治理策略）是强需求。Keelith 的 `Application.Describe()` 方法提供结构化的静态快照：
- 导出服务名、HTTP/gRPC 监听地址、加载的 Component、路由列表。
- **内置安全脱敏（Redaction）**：严禁向描述快照、日志和错误文本中输出密码、Token、私钥或数据库 DSN。

---

## 3. 请求在运行时中的生命周期流转

无论请求来自 HTTP 还是 gRPC，在 Keelith 内部都经历一致的严谨流水线：

```text
外部客户端请求 (Client Request)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│ 1. 传输接入层 (Transport Layer: HTTP / gRPC Listener)    │
│    • 解析协议报文，提取 Header / Metadata               │
│    • 建立链路追踪 Span (OpenTelemetry)                  │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│ 2. 传输中立元数据与追踪 (Metadata & Correlation)        │
│    • 提取并清洗 x-request-id, x-idempotency-key 等      │
│    • 绑定到 context.Context                             │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│ 3. 全局入站中间件链 (Inbound Middleware Bundle)         │
│    • Panic 异常恢复 (Recovery)                          │
│    • 链路日志记录 (Structured Logging)                  │
│    • 指标打点统计 (Prometheus Metrics)                  │
│    • 全局限流与过载保护 (Load Shedding)                 │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│ 4. 服务群组与端口策略 (Group & Surface Policies)        │
│    • 校验 Placement (匹配特定服务分组与端口)             │
│    • 身份鉴权与权限校验 (Authentication Policy)         │
│    • 针对特定服务的熔断与超时控制                       │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│ 5. 生成契约适配器 (Generated Service Binding)           │
│    • 参数格式反序列化与校验 (Validation)                │
│    • 将标准 Context 传递给业务实现                      │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│ 6. 核心业务实现 (Domain Implementation)                 │
│    • 纯净的业务逻辑处理                                 │
│    • 调用下游依赖 (Outbox, Client, Database)            │
└─────────────────────────────────────────────────────────┘
```

---

## 4. Keelith 在 Go 微服务生态中的定位

为了更好地理解何时应该选择 Keelith，我们将其与 Go 社区中主流的微服务框架进行客观对比：

| 框架 | 核心擅长领域 | Keelith 与其对比与互补 |
| :--- | :--- | :--- |
| **Kratos** | 云原生微服务、Protobuf 驱动 API、优雅架构设计 | Kratos 是优秀的对照标准。Keelith 进一步将契约延伸至**显式生命周期 DAG、多 Surface 端口隔离、编译期静态 DI、可编程 Continuation/Projection** 等高阶工程场景。 |
| **go-zero** | 全内聚代码生成、内置限流与缓存组件、开箱即用 | go-zero 提供极具效率的端到端生成体验；Keelith 则更加强调**非侵入性与扩展边界**，不将特定的 ORM 或缓存方案强绑定进核心。 |
| **CloudWeGo (Kitex / Hertz)** | 极致网络性能、自研 Netpoll、面向超大吞吐场景 | Kitex/Hertz 是出色的传输引擎。Keelith 在独立模块 `github.com/keelab/x` 中提供了 CloudWeGo Profile，为其注入统一的治理与契约规范。 |
| **Goa** | Design-First 领域 DSL 设计 API | Goa 侧重 API 设计与文档生成；Keelith 则关注服务**启动、治理、停机排空、可靠事件一致性**的完整工程生命周期。 |
| **Encore** | 全托管 Infra-as-Code、极佳的云端研发体验 | Encore 深度绑定其云平台和编译器插件；Keelith 将所有部署与基础设施控制权**完全交给用户显式配置**。 |

---

## 5. 什么时候使用“最小路由”，什么时候升级为“生成契约”

Keelith 提供了两条平滑演进的使用路径：

### 路径 A：最小路由（WithRoute）
适用于极小工具、健康探测适配、学习体验和测试场景：
```go
app, err := keelith.New(
    keelith.WithName("probe-svc"),
    keelith.WithHTTP("127.0.0.1:8080"),
    keelith.WithRoute("GET", "/ping", func(ctx context.Context, r *http.Request) (any, error) {
        return map[string]string{"status": "ok"}, nil
    }),
)
```
> [!WARNING]
> 不要将复杂的业务接口长期留在 `WithRoute` 中。`WithRoute` 不具备 Protobuf 的类型安全、字段校验、gRPC 双协议支持以及自动文档生成能力。

### 路径 B：契约优先（WithServices / WithProfile）
适用于工业级生产服务，契约与实现分离：
```go
// 1. 契约生成物（由 protoc-gen-go-keelith 生成）
binding := taskv1.NewTaskServiceBinding(taskServiceImpl)

// 2. 组合进应用
app, err := keelith.New(
    keelith.WithName("task-service"),
    keelith.WithHTTP("127.0.0.1:8080"),
    keelith.WithGRPC("127.0.0.1:9090"),
    keelith.WithServices(binding),
)
```

通过这一层抽象，业务实现天然支持 HTTP 与 gRPC 双向访问，且共享统一的中间件鉴权、链路追踪与错误返回格式。
