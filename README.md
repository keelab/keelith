# Keelith

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelith.svg)](https://pkg.go.dev/github.com/keelab/keelith)
[![License](https://img.shields.io/github/license/keelab/keelith)](LICENSE)

Keelith 是面向 Go 服务的运行时与开发工具集，覆盖应用生命周期、依赖装配、服务治理、传输适配、配置发布、可观测性和可编程拓扑控制。

它适合希望把框架能力保留在进程边界、治理中间件和生成工具里的团队：业务代码仍然是普通 Go 函数、普通构造函数和普通协议定义，Keelith 负责把这些部件连接成可运行、可诊断、可渐进交付的服务。

## 核心能力

- 应用生命周期：`app` 负责有序启动、健康状态、优雅停止、hook 回滚和 server 退出处理。
- 依赖装配：`di` 提供实例级依赖图、模块组合、命名绑定、分组绑定、插件排序和运行时构建；项目静态 wiring 由 `keelith wiring` CLI 统一编译生成。
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

`keelith new` 的模板会固定依赖模板声明的 Keelith 版本；发布包含根 Facade 的新版本时，
需要同步更新脚手架中的版本与 `go.sum` 自模块校验值。当前工作树新增的根 Facade 尚未进入
公开的 `v0.0.3`，所以从这份未发布源码 checkout 运行 CLI 时，应先用本地 `replace` 做 smoke
验证；完成发布后再把默认版本切到包含 Facade 的 tag。

查看 CLI 能力：

```bash
keelith --help
```

当前命令分组包括：

- `add`：添加服务、API、依赖或应用组件。
- `config`：管理版本化配置的 stage、active、history、activate、rollback。
- `doctor`：检查工具链和项目完整性。
- `generate`：生成 adapter、facade 或离线数据模型。
- `graph`：检查服务和依赖合约。
- `new`：从零创建可运行的 minimal、service 或 production 项目。
- `version`：输出 CLI 的构建版本（支持 `--format text|json`）。
- `wiring`：同步、校验和检查依赖 wiring 产物。

## 快速开始

最快的方式是直接创建一个最小 HTTP 项目：

```bash
keelith new hello
cd hello
go run .
# 另一个终端
curl http://127.0.0.1:8080/ping
```

最小路径只需要理解 `New`、`WithName`、`WithHTTP`、`WithRoute` 和 `Run`。生成的入口已经包含信号处理和优雅停止，不需要先执行 `doctor`、`graph` 或 `wiring`。

仓库当前不把示例应用作为核心模块发布物；如果只想验证 API，可直接运行上面的
Facade 代码。需要完整生产参考时，请结合 `contrib/` 和 `operator/` 的说明准备外部依赖，
再按部署环境补充配置。

如果要从代码开始，根包提供了一个薄的高层 Facade；它仍然复用 `app.App`、现有 transport 和服务 Binding：

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	keelith "github.com/keelab/keelith"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := keelith.New(
		keelith.WithName("hello"),
		keelith.WithHTTP(":8080"),
		keelith.WithRoute(http.MethodGet, "/ping", func(context.Context, *http.Request) (any, error) {
			return map[string]string{"message": "pong"}, nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := application.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

需要类型化协议时，升级到标准服务模板：

```bash
keelith new orders --template service --module example.com/orders
cd orders
go run .
curl http://127.0.0.1:8080/v1/ping
```

它包含 Proto、生成风格的 Binding、HTTP + gRPC 和由 Keelith CLI 管理的 `internal/keelithgen` 产物，但默认不连接 PostgreSQL、Redis、Kafka、注册中心或远程配置。`production` 模板用于展示显式 Profile/Group/Config 接线；完整 Outbox、Topology 和外部依赖参考仍保留在完整 demo 与 contrib/operator 文档中。

建议按下面的顺序逐步增加概念：

```text
最小 HTTP → 业务路由 → Binding/Proto + HTTP/gRPC → 配置/Component/Ops
→ 安全/发现/Job/Cache/Streaming/Client → DI/Continuation/Topology
→ 数据库/Outbox 与外部适配器
```

高级应用仍可以直接使用 `app.App`、`di.Build`、`service.NewProfile` 和各 transport 包。标准模板不要求业务项目手写 wiring frontend；Keelith CLI 负责生成 `internal/keelithgen`。

复杂项目只需用 CLI 声明业务构造函数，类型检查、依赖排序、代码生成和图校验都在 CLI 内完成：

```bash
keelith wiring add-provider ./internal/repository.NewOrderRepository \
  --as example.com/orders/internal/domain.OrderRepository
keelith wiring sync
```

需要多个进程入口时，由 CLI 管理 Root 声明：

```bash
keelith wiring add-root http --kind http
keelith wiring add-root worker --kind worker --provider ./internal/worker.NewServer
```

项目声明保存在 `keelith.project.json`，不包含调用表达式、构造顺序或运行时配置；生成文件带有受管标记，CLI 不会覆盖未标记的手写文件。带 `di.Out` 的构造函数会按字段生成独立绑定，带 Cleanup 的资源会在 Application 关闭时按逆序释放。

需要独立的运维监听器时，可在 Facade 上显式启用 `keelith.WithOps(ops.WithAddress("127.0.0.1:9090"))`；它只加入 loopback 健康端点，debug、pprof 和 admin 端点仍需通过对应的 `ops.Option` 显式开启。

## 项目结构

| 路径 | 说明 |
| --- | --- |
| `api/` | Keelith Protobuf API、错误协议和 generated manifest |
| `app/` | 应用生命周期、component 注册、drain 和 termination |
| `cache/` | 进程内缓存、codec、失效事件和策略 |
| `cmd/` | `keelith` CLI 和 `protoc-gen-go-keelith` |
| `config/` | 配置源、合并、typed binding、versioned runtime |
| `di/` | 模块、provider、graph 和 topology bridge |
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

`Makefile` 通过 `MODULE_DIRS` 按模块运行检查；默认覆盖核心、`contrib`、`x`、`operator`，以及可选的
`examples/programmable-commerce` 示例模块（未检出该目录时请调整 `MODULE_DIRS`）。涉及集成能力时使用专门目标，例如：

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
