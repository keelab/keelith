# Keelith

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelith.svg)](https://pkg.go.dev/github.com/keelab/keelith)
[![License](https://img.shields.io/github/license/keelab/keelith)](LICENSE)

Keelith 是面向 Go 服务的运行时与开发工具集，覆盖应用生命周期、依赖装配、服务治理、传输适配、配置发布、可观测性和可编程拓扑控制。

它适合希望把框架能力保留在进程边界、治理中间件和生成工具里的团队：业务代码仍然是普通 Go 函数、普通构造函数和普通协议定义，Keelith 负责把这些部件连接成可运行、可诊断、可渐进交付的服务。

## 核心能力

- 应用生命周期：`app` 负责有序启动、健康状态、优雅停止、hook 回滚和 server 退出处理。
- 依赖装配：`di` 提供实例级依赖图、模块组合、命名绑定、分组绑定、插件排序、运行时构建和静态 wiring 生成。
- 配置系统：`config` 支持多来源合并、未知字段策略、类型绑定、订阅发布、版本化配置和重启边界。
- 服务治理：`governance` 提供 timeout、retry、rate limit、bulkhead、circuit breaker、hedging、loadshed、fallback、idempotency 等按 `operation.Operation` 解析的中间件。
- 传输层：根模块内置 HTTP、gRPC、SSE、WebSocket、metadata、auth、TLS reload 和 framework error 映射。
- 可观测性与运维面：`observability` 和 `ops` 提供 App 级日志、trace、metrics、审计、健康检查、pprof、运行时状态和可编程 runtime 管理入口。
- 可编程运行时：`programmable/continuation`、`programmable/projection`、`programmable/topology` 提供 durable call、投影调度、拓扑计划和流量 epoch 控制。
- 开发工具：`keelith` CLI 支持项目增量生成、配置管理、doctor、依赖图检查、wiring 同步和离线模型生成；`protoc-gen-go-keelith` 从 Protobuf 注解生成传输适配代码。

## 模块

| 模块 | 导入路径 | 说明 |
| --- | --- | --- |
| 核心 | `github.com/keelab/keelith` | 稳定运行时、治理、传输、CLI 和代码生成器 |
| 扩展适配 | `github.com/keelab/contrib` | etcd、Nacos、Redis、Kafka、SQL、Kubernetes、Vault、JWT、OTLP、Prometheus、Protovalidate 等外部系统适配 |
| 实验扩展 | `github.com/keelab/x` | CloudWeGo Hertz、Kitex 和 Kitex generic profile |
| Operator | `github.com/keelab/operator` | Kubernetes `TopologyRevision` API、渲染器和 namespaced controller |

## 安装

Keelith 当前按 `go.mod` 中的 Go 版本开发和验证。

```bash
go install github.com/keelab/keelith/cmd/keelith@latest
go install github.com/keelab/keelith/cmd/protoc-gen-go-keelith@latest
```

查看 CLI 能力：

```bash
keelith --help
```

当前命令分组包括：

- `add`：添加 API、依赖或应用组件。
- `config`：管理版本化配置的 stage、active、history、activate、rollback。
- `doctor`：检查工具链和项目完整性。
- `generate`：生成 adapter、facade 或离线数据模型。
- `graph`：检查服务和依赖合约。
- `wiring`：同步、校验和检查依赖 wiring 产物。

## 快速开始

创建一个普通 Go 模块后，先用 CLI 初始化或增量补齐项目结构：

```bash
mkdir orders && cd orders
go mod init example.com/orders
keelith doctor --path .
keelith add --help
keelith wiring --help
```

在应用代码里，核心运行方式是组合 server、component 和 hook，然后交给 `app.App` 管理生命周期。下面是最小骨架；实际服务应通过 options 加入 HTTP/gRPC server 或 component：

```go
package main

import (
	"log"

	"github.com/keelab/keelith/app"
)

func main() {
	application, err := app.New()
	if err != nil {
		log.Fatal(err)
	}
	_ = application
}
```

带有 server 的应用通常在进程入口阻塞运行：

```go
if err := application.Run(context.Background()); err != nil {
	log.Fatal(err)
}
```

实际服务通常还会加入 HTTP/gRPC server、治理中间件、配置 manager、health registry、observability bundle 和业务组件。Keelith 不要求业务构造函数依赖容器；`di` 只在构建图时解析依赖。

## 项目结构

| 路径 | 说明 |
| --- | --- |
| `api/` | Keelith Protobuf API、错误协议和 generated manifest |
| `app/` | 应用生命周期、component 注册、drain 和 termination |
| `cache/` | 进程内缓存、codec、失效事件和策略 |
| `cmd/` | `keelith` CLI 和 `protoc-gen-go-keelith` |
| `config/` | 配置源、合并、typed binding、versioned runtime |
| `di/` | 模块、provider、graph、静态 wiring 和 topology bridge |
| `governance/` | retry、timeout、ratelimit、bulkhead、breaker、hedging 等治理中间件 |
| `middleware/` | 传输无关 unary/stream middleware 组合 |
| `observability/` | 日志、审计、trace、metrics、resource 和 programmable adapter |
| `ops/` | 独立运维 HTTP server 和诊断接口 |
| `programmable/` | continuation、projection、topology 控制运行时 |
| `registry/`、`selector/` | 服务注册发现抽象和节点选择策略 |
| `transport/` | HTTP、gRPC、SSE、WebSocket、TLS、auth 适配 |
| `worker/`、`inbox/`、`outbox/`、`saga/` | 后台任务、消息一致性和流程状态基础设施 |
| `contrib/` | 外部系统适配模块，见 [contrib/README.md](contrib/README.md) |
| `x/` | 实验传输扩展，见 [x/README.md](x/README.md) |
| `operator/` | Kubernetes 拓扑 operator，见 [operator/README.md](operator/README.md) |

## 开发

安装依赖：

```bash
go mod download
```

常用检查：

```bash
go test ./...
make test
make vet
make verify
```

`Makefile` 会按模块运行核心、`contrib`、`x`、`operator` 和示例模块的检查。涉及集成能力时使用专门目标，例如：

```bash
make integration
make projection-storage-integration
make topology-kubernetes-integration
make topology-operator-integration
```

生成协议和兼容性检查：

```bash
make generated-check
make compatibility-check
```

## 安全与贡献

安全问题不要通过公开 Issue 报告，请遵循 [SECURITY.md](SECURITY.md)。贡献流程、提交规范和本地检查见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## License

Keelith 使用 [Apache License 2.0](LICENSE)。
