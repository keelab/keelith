# Keelith 官方框架教程

> **一次构建服务，让运行时契约贯穿始终**

Keelith 是一个面向生产环境、**契约优先（Contract-First）** 的 Go 微服务运行时。它与传统“大包大揽”的全家桶式框架不同：Keelith 并不试图将所有的外部库绑定在自身之中，而是将**服务契约、传输适配、依赖注入、运行时配置、弹性治理、后台工作负载、可观测性与应用生命周期**组装为一条清晰、确定、可审计的运行链路。

本教程深度参考 [Go-Kratos 文档体系与工程结构](https://go-kratos.dev/docs/) 进行编排，帮助开发者从零上手最小验证原型，逐步进阶到大规模服务集群的架构设计、微服务治理和生产落地。

---

## 教程知识全景图 (Sitemap)

```text
Keelith Docs
├── 01. 简介与设计理念 (Intro)
│   ├── 设计理念与核心概念   (01-intro/design.md)
│   ├── 项目工程结构最佳实践 (01-intro/layout.md)
│   └── 常见问题答疑 (FAQ)  (01-intro/faq.md)
│
├── 02. 快速开始 (Getting Started)
│   ├── 快速入门 (Quick Start)   (02-getting-started/quick-start.md)
│   ├── 手写最小 HTTP 路由       (02-getting-started/route.md)
│   └── Keelith CLI 工具全景手册 (02-getting-started/cli-usage.md)
│
├── 03. 核心开发指南 (Guide)
│   ├── 服务契约与 Protobuf 规范 (03-guide/contracts.md)
│   ├── 编译期静态 DI 与 Wiring  (03-guide/di-wiring.md)
│   ├── Surface、Group 与 Policy 边界 (03-guide/surfaces.md)
│   └── 传输无关的统一错误模型   (03-guide/errors.md)
│
├── 04. 核心组件深度剖析 (Components)
│   ├── 配置管理与动态环境 (04-components/config.md)
│   ├── 应用生命周期与组件拓扑 (04-components/lifecycle.md)
│   ├── 传输层：HTTP 与 gRPC 详解 (04-components/transport.md)
│   ├── 弹性治理与流量防护 (04-components/governance.md)
│   ├── 数据持久化、缓存与可靠事件 (04-components/persistence.md)
│   └── 可编程工作负载运行时 (04-components/programmable.md)
│
└── 05. 运维与生产就绪 (Operations)
    ├── 统一可观测性与 Ops 诊断 (05-operations/observability.md)
    └── 生产就绪检查清单与发布防线 (05-operations/production.md)
```

---

## 推荐学习路径

无论你是微服务入门开发者，还是平台架构师，都可以根据以下路径循序渐进：

### 阶段一：初识 Keelith 与极速原型（15 分钟）
1. 阅读 [设计理念与核心概念](01-intro/design.md)：理解为什么需要契约优先，Keelith 与 Kratos、go-zero、Kitex 的定位差异。
2. 阅读 [项目工程结构](01-intro/layout.md)：掌握符合 DDD 与 Clean Architecture 思想的微服务目录结构。
3. 跟随 [快速入门](02-getting-started/quick-start.md)：安装 `keelith-cli`，用一条命令生成并跑通第一个双协议微服务。
4. 阅读 [手写最小 HTTP 路由](02-getting-started/route.md)：掌握仅依赖 Go 标准库与 Keelith 门面（Facade）构建轻量服务的方式。

### 阶段二：工业级服务构建（1 小时）
5. 学习 [服务契约与 Protobuf 规范](03-guide/contracts.md)：掌握以 Protobuf 为唯一事实源生成 HTTP/gRPC 双传输绑定，使用 `service.Binding` 和 `service.Profile` 组装服务。
6. 学习 [编译期静态 DI 与 Wiring](03-guide/di-wiring.md)：告别运行时反射隐患，使用 `di.Module`、`di.Provide`、`di.Export` 构建安全、高效的依赖注入图。
7. 学习 [Surface、Group 与 Policy 边界](03-guide/surfaces.md)：精细隔离公网与内网端口，对服务群组施加声明式安全与鉴权策略。
8. 学习 [传输无关的统一错误模型](03-guide/errors.md)：跨越 HTTP Status Code 与 gRPC Code 鸿沟，建立具备 Code/Reason/Message/Metadata 的标准错误系统。

### 阶段三：生产级组件与弹性治理（2 小时）
9. 掌握 [配置管理与动态环境](04-components/config.md)：多源配置融合（YAML + 环境变量）、动态监听与热重载、严格字段防呆校验。
10. 掌握 [应用生命周期与组件拓扑](04-components/lifecycle.md)：理解确定性的 DAG 拓扑启动与逆序优雅停机（Graceful Shutdown），保护在途请求。
11. 掌握 [传输层：HTTP 与 gRPC 详解](04-components/transport.md)：配置 Listener、统一拦截器、流式通信与编解码器扩展。
12. 掌握 [弹性治理与流量防护](04-components/governance.md)：组合配置超时、重试预算（防止重试风暴）、熔断降级、并发隔离（Bulkhead）与自适应限流。
13. 掌握 [数据持久化、缓存与可靠事件](04-components/persistence.md)：落地“PostgreSQL 真实源 + Redis 旁路缓存 + 事务型 Outbox + Kafka 投递”黄金范式。
14. 探索 [可编程工作负载运行时](04-components/programmable.md)：学习 Continuation 持久化状态机、增量 Projection 物化视图以及不可变 Topology 计划。

### 阶段四：生产交付与运维审计（30 分钟）
15. 落地 [统一可观测性与 Ops 诊断](05-operations/observability.md)：打通 OpenTelemetry Tracing、Prometheus Metrics、结构化日志，暴露专属 Ops 运维与健康检查端点。
16. 核对 [生产就绪检查清单](05-operations/production.md)：执行代码门禁、安全无密审计（`Describe` 静态审计），建立生产发布安全防线。

---

## 前置环境要求

- **Go 语言环境**：Go 1.26 或更高版本（请以主仓库 `go.mod` 为准）。
- **本地构建环境**：标准 `make`、`git` 工具，能顺畅执行 `go test ./...`。
- **协议生成工具（可选，推荐）**：
  - [Buf CLI](https://buf.build/docs/installation)：现代化的 Protobuf 管理与生成工具。
  - `protoc-gen-go`、`protoc-gen-go-grpc`：Protobuf 官方编译插件。
  - `protoc-gen-go-keelith`：Keelith 官方契约绑定生成插件。

---

## 安装与快速验证

```bash
# 1. 安装 Keelith 官方开发脚手架 CLI
go install github.com/keelab/keelith-cli/cmd/keelith@latest

# 2. 检查 CLI 版本与系统环境
keelith --version

# 3. 安装契约代码生成器
go install github.com/keelab/keelith-cli/cmd/protoc-gen-go-keelith@latest

# 4. 在项目中引入 Keelith 核心运行时
go get github.com/keelab/keelith@latest
```

---

## 示例约定与架构原则

1. **显式优于隐式**：Keelith 的核心构造函数（如 `keelith.New`）只负责装配对象图与静态校验，绝对不会在构造阶段偷偷建立网络连接或触发 I/O。一切网络连接与后台 Worker 均在 `Application.Run` 阶段显式启动。
2. **零敏感凭证泄漏**：无论在日志输出、链路追踪标签，还是 `Application.Describe()` 导出的运行时拓扑快照中，Keelith 均具备内置的 Redaction 机制，严禁泄露密码、Token、DSN 等敏感信息。
3. **真实环境独立验收**：单元测试和代码生成校验证明的是代码逻辑与契约的一致性，不可与真实数据库、Kafka 消息队列、Consul 注册中心的联通性验收混为一谈。
