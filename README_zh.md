# Keelith

### 一次构建服务，让运行时契约贯穿始终

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelith.svg)](https://pkg.go.dev/github.com/keelab/keelith)
[![License](https://img.shields.io/github/license/keelab/keelith)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](go.mod)

**[English](README.md) | 中文**

Keelith 是一个面向生产环境、契约优先的 Go 微服务运行时。它为生成式服务提供统一方式，组合 HTTP 与 gRPC 传输、中间件、配置、服务发现、治理、后台任务、可观测性和优雅生命周期管理。

Keelith 保持模块化：从核心契约开始，按服务需要引入集成，并把基础设施依赖留在边界层。

如果你正在将 Keelith 与 Kratos、go-zero、CloudWeGo、Encore 或 Goa 对比，最关键的区别在于关注点：Keelith 是面向服务集群的运行时契约与组合层，而不是另一个全家桶式应用框架，也不是只解决单一协议的 RPC 库。

## 为什么选择 Keelith

- **统一应用生命周期**：为服务器、Worker、组件和配置监听器提供确定性的启动、就绪、优雅停止与有界清理。
- **与传输无关的契约**：HTTP、gRPC、Hertz 和 Kitex 共享 operation、metadata、error、validation、identity 与 placement。
- **可组合治理能力**：超时、重试、对冲、熔断、Bulkhead、限流、过载保护、准入控制、降级和异常节点检测。
- **可编程工作负载**：持久化 continuation、projection、拓扑计划、Saga、Job、Worker、缓存失效与幂等原语。
- **默认可观测**：结构化日志、指标、追踪、健康检查、诊断和不包含敏感信息的运行时描述。
- **清晰的扩展边界**：可选适配器位于 `contrib`；CloudWeGo profile 位于独立的 `github.com/keelab/x` 模块。

## 不同的关注点

许多 Go 微服务项目都在某一层做得非常出色：HTTP/RPC 性能、代码生成、API 设计或基础设施自动化。Keelith 更关注这些能力之间的接缝——生产系统最容易在这里积累重复的策略代码和生命周期代码。

| 如果你需要…… | Keelith 的做法 |
| --- | --- |
| 在同一服务集群中同时使用 HTTP 与 gRPC | 通过与传输无关的契约统一 operation、metadata、error、validation、identity 和 placement |
| 保持一致的生产行为 | 在运行时边界组合治理策略与重试预算 |
| 不止是请求/响应处理器 | 提供持久化 continuation、projection、Saga、Job、Worker 与幂等原语 |
| 自由选择基础设施 | 小而清晰的核心，加上可选适配器以及独立的 Hertz/Kitex Profile |
| 让运维行为可推理、可排查 | 提供显式生命周期、就绪状态、诊断能力和不含敏感信息的运行时描述 |

因此，Keelith 特别适合平台团队和长期演进的服务集群：在不强迫所有服务采用同一套基础设施的前提下，保持跨服务的行为一致。

## Keelith 在 Go 生态中的位置

Keelith 并不试图替代已经在某个层面做得很好的项目，而是与它们形成互补：

- **Kratos**：云原生 HTTP/gRPC 服务与可插拔组件的自然对照。
- **go-zero**：当代码生成、API/RPC 规范和开箱即用的流量保护是首要目标时，是很强的选择。
- **CloudWeGo Kitex + Hertz**：高性能 RPC/HTTP 组合；Keelith 可以通过可选 Profile 提供共享契约与策略层。
- **Goa**：以 Design-first API 生成见长；Keelith 将契约继续延伸到生命周期、治理和持久化工作负载。
- **Encore**：强调托管式、基础设施感知的开发体验；Keelith 则把部署和基础设施选择保留在显式边界中。

当真正困难的事情不是启动一个服务，而是让多个服务随着协议、策略和工作负载演进仍保持一致时，可以选择 Keelith。

## 快速开始

要求：Go 1.26 或更高版本。CLI 可以创建一个最小可运行的 HTTP 服务：

```bash
go install github.com/keelab/keelith-cli/cmd/keelith@latest
keelith new hello
cd hello
keelith dev
# 在另一个终端执行
curl http://127.0.0.1:8080/ping
```

这个快速开始使用有意保持轻量的 `minimal` 模板。需要契约优先、带有生成的
HTTP/gRPC binding 的服务路径时，可改用 `keelith new hello --template service`；
该模板的 starter endpoint 是 `/v1/ping`。

`NAME` 默认也是目标目录。显式传入 `--path` 时，`--path` 表示完整目标目录，
`NAME` 仍是用于默认 module 和应用名的项目身份，例如：

```bash
keelith new orders --path ./services/orders-api
```

`keelith dev` 会自动发现项目根目录，刷新生成契约和 wiring，然后运行应用。
需要只读检查项目、工具链和生成文件时使用 `keelith check`。框架维护所需的
底层 wiring 命令保留在 `keelith x wiring` 下。
开发时可使用 `keelith dev --watch`，它会在源码变化后重新生成并重新加载；生成或
编译失败时会保留当前正在运行的进程。

顶层 `keelith generate` 只作用于包含 Keelith 声明或已生成契约/组件 manifest
的项目，不会修改普通 Go module。普通 Go module 直接使用 `keelith dev` 运行；
如果只需要独立生成器，请明确使用 `keelith generate data` 或
`keelith generate kitex-facade`。

`keelith add component` 会写入组件声明、配置和生成的资源组装代码；默认模板的
`main.go` 仍调用统一的 `NewApplication`，该入口会自动组装已声明的组件，并在应用
启动阶段检查 Redis、SQL、Kafka 等外部依赖。需要远程配置或自定义依赖时，才在应用
入口显式使用生成的 `NewApplicationWithComponents` 并传入对应依赖。

service 和 production 模板带有预生成的 starter 契约，但不预设 Buf 工具链。
修改 Proto 或使用 `keelith add api` 前，请先在项目根配置 `buf.yaml`、
`buf.gen.yaml` 和所需的 Buf 依赖，安装 Buf 及对应插件，然后执行
`buf dep update`，再运行 `keelith generate`。

完整的生成式服务请前往 [Keelab examples](https://github.com/keelab/examples) 仓库。公开 API 参见 [pkg.go.dev](https://pkg.go.dev/github.com/keelab/keelith)；项目脚手架和开发命令已经独立到 [keelith-cli](https://github.com/keelab/keelith-cli) 模块。

安装核心模块：

```bash
go get github.com/keelab/keelith
go install github.com/keelab/keelith-cli/cmd/protoc-gen-go-keelith@latest
```

## 核心模块

| 领域 | 包 | 用途 |
| --- | --- | --- |
| 应用 | `keelith`、`app`、`server`、`health` | 运行时组装、生命周期、就绪与停止 |
| 服务模型 | `service`、`operation`、`placement`、`metadata`、`errors`、`validation` | 所有传输共享的稳定契约 |
| 传输 | `transport/http`、`transport/grpc`、`transport/sse`、`transport/websocket` | HTTP/1.1、HTTP/2、gRPC、SSE 与 WebSocket |
| 治理 | `governance/*`、`policy`、`middleware` | 韧性、流量控制、鉴权与请求策略 |
| 运行时状态 | `config`、`secret`、`cache`、`registry`、`coordination` | 动态配置、密钥、缓存与服务发现 |
| 工作执行 | `worker`、`job`、`outbox`、`saga`、`programmable/*` | 后台任务、消息、Saga、投影与持久化工作流 |
| 运维 | `observability`、`ops`、`diagnostics` | 日志、指标、追踪、健康与运维端点 |
| 集成 | [`contrib`](contrib)、[`operator`](operator)、[`x`](x) | 第三方适配器、Kubernetes 控制面、Hertz 与 Kitex |

## 传输 Profile

核心模块提供标准 HTTP 与 gRPC 实现。可选的 [`github.com/keelab/x`](x) 模块提供 CloudWeGo 集成：

```bash
go get github.com/keelab/x/transport/hertz
go get github.com/keelab/x/transport/kitex
```

`x/transport/hertz` 用于 Hertz HTTP 服务，`x/transport/kitex` 用于生成的 Kitex server 与 client。重试预算应由 Keelith 统一治理，不要同时启用底层客户端的另一套独立重试策略。

## 仓库结构

```text
keelith/
├── app/             生命周期与依赖感知运行时
├── transport/       HTTP、gRPC、流式、鉴权与协议辅助
├── governance/      韧性与流量策略
├── programmable/    continuation、projection 与 topology
├── observability/   日志、指标、追踪与诊断
├── contrib/         可选基础设施适配（独立模块）
├── operator/        Kubernetes TopologyRevision 控制器（独立模块）
└── x/               Hertz 与 Kitex profile（独立模块）
```

## 文档与示例

- [API 参考](https://pkg.go.dev/github.com/keelab/keelith)
- [Keelab examples](https://github.com/keelab/examples)
- [Contrib 集成](contrib/README.md)
- [Kubernetes Operator](operator/README.md)
- [CloudWeGo Profile](x/README.md)

## 开发

```bash
go test ./...
go vet ./...
```

`contrib`、`operator` 和 `x` 为独立模块，分别执行：

```bash
for dir in contrib operator x; do (cd "$dir" && GOWORK=off go test ./... && GOWORK=off go vet ./...); done
```

部分集成检查需要外部服务或 Kubernetes 测试集群，请先阅读对应模块的 README。

## 兼容性与安全

Keelith 在各模块内遵循面向契约的语义化 API。漏洞报告方式见 `SECURITY.md`。不要把凭证、原始 payload 或敏感 endpoint 写入日志或运行时描述；请使用 `secret`、metadata policy 与脱敏包。

## 许可证

Keelith 使用 [MIT License](LICENSE) 发布。
