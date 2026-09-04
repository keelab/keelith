# Keelith

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelith.svg)](https://pkg.go.dev/github.com/keelab/keelith)
[![License](https://img.shields.io/github/license/keelab/keelith)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](go.mod)

**[English](README.md) | 中文**

Keelith 是一个面向生产环境、契约优先的 Go 微服务运行时。它为生成式服务提供统一方式，组合 HTTP 与 gRPC 传输、中间件、配置、服务发现、治理、后台任务、可观测性和优雅生命周期管理。

Keelith 保持模块化：从核心契约开始，按服务需要引入集成，并把基础设施依赖留在边界层。

## 为什么选择 Keelith

- **统一应用生命周期**：为服务器、Worker、组件和配置监听器提供确定性的启动、就绪、优雅停止与有界清理。
- **与传输无关的契约**：HTTP、gRPC、Hertz 和 Kitex 共享 operation、metadata、error、validation、identity 与 placement。
- **可组合治理能力**：超时、重试、对冲、熔断、Bulkhead、限流、过载保护、准入控制、降级和异常节点检测。
- **可编程工作负载**：持久化 continuation、projection、拓扑计划、Saga、Job、Worker、缓存失效与幂等原语。
- **默认可观测**：结构化日志、指标、追踪、健康检查、诊断和不包含敏感信息的运行时描述。
- **清晰的扩展边界**：可选适配器位于独立的 `github.com/keelab/contrib` 模块；CloudWeGo profile 位于独立的 `github.com/keelab/x` 模块。

## 快速开始

要求：Go 1.26 或更高版本。推荐从生成的 service binding 开始。CLI 可以创建一个最小可运行的 HTTP 服务：

```bash
go install github.com/keelab/keelith/cmd/keelith@latest
keelith new hello
cd hello
go run .
# 在另一个终端执行
curl http://127.0.0.1:8080/ping
```

完整的生成式服务请前往 [Keelab examples](https://github.com/keelab/examples) 仓库，或使用 `internal/scaffold` 中的脚手架。公开 API 参见 [pkg.go.dev](https://pkg.go.dev/github.com/keelab/keelith)。

安装核心模块：

```bash
go get github.com/keelab/keelith
go install github.com/keelab/keelith/cmd/protoc-gen-go-keelith@latest
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
| 集成 | [`contrib`](https://github.com/keelab/contrib)、[`operator`](https://github.com/keelab/operator)、[`x`](https://github.com/keelab/x) | 第三方适配器、Kubernetes 控制面、Hertz 与 Kitex |

## 传输 Profile

核心模块提供标准 HTTP 与 gRPC 实现。可选的 [`github.com/keelab/x`](https://github.com/keelab/x) 模块提供 CloudWeGo 集成：

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
└── ...              可选适配器与 profile 维护在独立仓库
```

## 文档与示例

- [API 参考](https://pkg.go.dev/github.com/keelab/keelith)
- [Keelab examples](https://github.com/keelab/examples)
- [Contrib 集成](https://github.com/keelab/contrib)
- [Kubernetes Operator](https://github.com/keelab/operator)
- [CloudWeGo Profile](https://github.com/keelab/x)

## 开发

```bash
go test ./...
go vet ./...
```

`contrib`、`operator` 和 `x` 仓库独立进行验证。部分集成检查需要外部服务或 Kubernetes 测试集群，请先阅读对应仓库的 README。

## 兼容性与安全

Keelith 在各模块内遵循面向契约的语义化 API。漏洞报告方式见 `SECURITY.md`。不要把凭证、原始 payload 或敏感 endpoint 写入日志或运行时描述；请使用 `secret`、metadata policy 与脱敏包。

## 许可证

Keelith 使用 [MIT License](LICENSE) 发布。
