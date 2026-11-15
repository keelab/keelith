# 常见问题答疑 (FAQ)

本章节汇总了开发者在初学和使用 Keelith 框架过程中最常遇到的技术问题与架构解惑。

---

### Q1: 为什么 `keelith.New` 不会直接连接数据库或启动监听器？
**A**: 这是 Keelith 极其关键的架构原则：**分离对象图构造（Construction）与运行时执行（Execution）**。
- `keelith.New(...)` 是纯内存操作：它仅负责解析配置、校验参数合法性、构建依赖拓扑图、检测循环依赖，并执行静态防呆检查。这保证了在单元测试中实例化应用极度迅速且可预测。
- 所有的网络 I/O、端口监听、外部系统握手（如 PostgreSQL 连接池握手、Redis Ping、Kafka 消费者注册），全部严格推迟到 `app.Run(ctx)` 阶段。

---

### Q2: `WithServices` 与 `WithProfile` 有什么区别？为什么二者互斥？
**A**:
- `WithServices(bindings ...service.Binding)`：是最常用的便捷入口，适合直接传入由代码生成器生成的多个服务绑定。Keelith 内部会自动为其生成默认 Profile。
- `WithProfile(profile *service.Profile)`：适用于大型复杂应用。当您需要精细控制多服务的合并顺序、统一策略配置（Policy）或定制服务分组（Group）时，可以先通过 `service.NewProfileFromBindings(...)` 创建并验证 Profile，再显式传入应用。
- **互斥原因**：为了保证服务拓扑的唯一性和确定性，Keelith 拒绝在同一个应用中同时混用隐式 bindings 和显式 profile。

---

### Q3: 为什么 Keelith 核心库没有内置某个特定 ORM（如 GORM、Ent）？
**A**:
- 在大型企业和长周期微服务演进中，不同业务线、不同部门可能分别偏好 GORM、Ent、sqlc 甚至原生 `database/sql`。
- 如果框架核心与某一特定 ORM 强绑定，升级框架时极易遭遇依赖冲突。
- Keelith 提供了极简的 `app.Component` 接口（只需要实现 `Start` 和 `Stop`）。任何数据库连接池或三方客户端只需实现这两个方法，即可完美无缝融入 Keelith 的生命周期管理中。

---

### Q4: 如何防止微服务调用链中的“重试风暴（Retry Storm）”？
**A**:
- 当链路中多个节点（如 网关 -> 服务A -> 服务B）同时配置了失败重试时，一次底层的偶发超时可能被层层放大为几何级数的请求洪峰，彻底击垮下游。
- Keelith 通过**重试预算（Retry Budget）**与**上下文超时取消（Deadline Propagation）**双重防护：
  1. 重试预算严格限制重试请求占正常请求的百分比（如不超过 10%）。
  2. 必须结合 context 的 Deadline 进行检查，如果父调用超时已耗尽，本地即使发生错误也严禁触发下一次重试。

---

### Q5: `WithRoute` 和 `WithServices` 的适用场景分别是什么？
**A**:
- **`WithRoute`**：专为快速原型验证、教程演练、极小的健康探针设计。它是极简的 `(ctx, *http.Request) -> (any, error)` 签名，开箱即用。
- **`WithServices`**：生产环境推荐标准。通过 Protobuf 定义接口，生成严格的强类型 DTO、入参校验、自动文档以及 HTTP/gRPC 双协议绑定。

---

### Q6: `Application.Describe()` 导出的运行时快照安全吗？会不会泄漏数据库密码？
**A**:
- **绝对安全**。Keelith 的 `Describe()` 专为生产诊断设计，内置了严格的字段脱敏机制（Redaction）。
- 它仅输出服务的元数据（服务名、监听地址、注册的契约方法列表、已加载的组件名等），绝不输出具体的数据连接密码、私钥、API Secret 或完整 DSN 字符串。

---

### Q7: 可以在同一个应用里同时监听 HTTP 和 gRPC 吗？
**A**:
- **完全可以，这是 Keelith 的核心强项**。
- 你只需在 `keelith.New` 中同时声明：
  ```go
  keelith.WithHTTP("127.0.0.1:8080"),
  keelith.WithGRPC("127.0.0.1:9090"),
  ```
- 只要注册的 `service.Binding` 同时包含 HTTP 与 gRPC 的注册函数，生成的双协议端口将自动共享同一套业务逻辑实现、中间件链与可观测性追踪。
