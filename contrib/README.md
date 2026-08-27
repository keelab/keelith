# Keelith Contrib

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/contrib.svg)](https://pkg.go.dev/github.com/keelab/contrib)
[![License](https://img.shields.io/github/license/keelab/keelith)](../LICENSE)

`github.com/keelab/contrib` 收纳 Keelith 对外部基础设施和第三方生态的适配。核心模块只保留稳定抽象；需要连接 etcd、Nacos、Redis、Kafka、SQL、Kubernetes、Vault、JWT、OTLP、Prometheus、Protovalidate 等系统时，从 contrib 引入对应包。

## 设计边界

- contrib 包依赖核心模块的公共 contract，不把第三方 SDK 类型泄漏回核心模块。
- 配置和密钥入口保持显式，DSN、token、证书等敏感值通常通过 `secret.Reference` 或 resolver 解析。
- 运行时描述避免暴露 endpoint、payload、credential 和原始错误文本等高敏信息。
- 适配包按具体系统和能力命名，避免一个通用包吞下多个生命周期。

## 包分组

| 路径 | 能力 |
| --- | --- |
| `cache/redis` | Redis-backed cache |
| `config/etcd`、`config/nacos`、`config/kubernetes`、`config/cache` | 远程配置源、本地 LKG/cache 和版本化配置后端 |
| `coordination/kubernetes`、`coordination/redis` | 租约和协调原语 |
| `data/sql`、`data/gorm`、`data/bbolt`、`data/sqlite` | SQL/GORM 连接、continuation、projection、inbox、outbox、saga 存储 |
| `docs/yapi` | YApi 文档上报 |
| `idempotency/redis`、`ratelimit/redis` | Redis-backed 幂等和分布式限流 |
| `job/cron`、`job/xxl`、`job/ownership/*` | 定时任务、XXL executor 和任务所有权 |
| `mq/kafka` | Kafka producer、consumer、outbox 和 trace propagation |
| `observability/otlpgrpc`、`observability/prometheus` | OTLP gRPC pipeline、Prometheus exporter |
| `profiling/cpu`、`profiling/pyroscope` | CPU profile 与 Pyroscope runtime |
| `registry/etcd`、`registry/nacos`、`registry/consul`、`registry/kubernetes`、`registry/xds` | 服务注册发现和 xDS projection |
| `secret/kubernetes`、`secret/vault` | Kubernetes Secret 和 Vault provider |
| `security/jwt` | JWT bearer authenticator、JWKS 和 key source |
| `topology/etcd`、`topology/kubernetes` | Topology plan source |
| `transport/grpcdiagnostics`、`transport/mcp` | gRPC diagnostics 和 MCP transport |
| `validation/protovalidate` | Buf Protovalidate 到 Keelith validation contract 的适配 |

## 安装

在应用模块中按需引入具体包：

```bash
go get github.com/keelab/contrib/registry/etcd
go get github.com/keelab/contrib/observability/otlpgrpc
go get github.com/keelab/contrib/validation/protovalidate
```

本仓库开发时，`contrib/go.mod` 通过 `replace github.com/keelab/keelith => ..` 指向相邻核心模块。

## 示例

etcd registry client：

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/keelab/contrib/registry/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	sdk, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sdk.Close()

	client, err := etcd.New(sdk, etcd.Options{
		Prefix: "/keelith/registry",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(context.Background())
}
```

Protovalidate middleware：

```go
validator, err := protovalidate.Messages()
if err != nil {
	return err
}
_ = validator
```

## 开发与验证

```bash
cd contrib
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

需要外部服务的测试使用 integration 或 chaos build tag，并由根仓库 `Makefile` 中的集成目标准备依赖。

## 版本关系

contrib 跟随 Keelith 核心模块演进。新增适配应优先复用核心模块已有 contract；只有外部系统语义确实无法表达时，才在核心模块补充公共抽象。
