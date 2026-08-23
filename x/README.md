# Keelith X

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/x.svg)](https://pkg.go.dev/github.com/keelab/x)
[![License](https://img.shields.io/github/license/keelab/keelith)](../LICENSE)

`github.com/keelab/x` 提供 Keelith 的实验性传输 profile。当前模块面向 CloudWeGo Hertz 和 Kitex，把 Keelith 的 middleware、metadata、operation、discovery、错误投影和 stream 生命周期接入对应框架。

这些包适合已经使用 CloudWeGo 生态、但希望继续使用 Keelith 治理和诊断 contract 的服务。稳定 HTTP/gRPC 能力位于核心模块 `github.com/keelab/keelith/transport`。

## 包

| 路径 | 能力 |
| --- | --- |
| `transport/hertz` | Hertz server/client、JSON codec、SSE、WebSocket、metadata policy、per-attempt picker feedback 和响应大小保护 |
| `transport/kitex` | Generated Kitex server adapter、client suite、stream middleware、metadata propagation、framework error codec 和 picker |
| `transport/kitex/generic` | Kitex generic unary client runtime、proto-json 调用、TLS/显式 insecure 配置和 Ops runtime status |

## 安装

```bash
go get github.com/keelab/x/transport/hertz
go get github.com/keelab/x/transport/kitex
```

`x` 当前依赖 `github.com/keelab/keelith`，并引入 CloudWeGo Hertz、Kitex、dynamicgo 等可选依赖。只在需要对应 profile 的服务中引入它。

## Hertz 示例

```go
package main

import (
	"context"
	"log"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/x/transport/hertz"
)

type request struct {
	Name string `json:"name"`
}

func main() {
	server, err := hertz.NewServer()
	if err != nil {
		log.Fatal(err)
	}

	target, err := operation.New("http", "greeter", "hello", operation.KindUnary)
	if err != nil {
		log.Fatal(err)
	}

	err = server.Handle(
		"POST",
		"/hello",
		target,
		hertz.DecodeJSON[request](),
		func(ctx context.Context, value any) (any, error) {
			input := value.(request)
			return map[string]string{"message": "hello " + input.Name}, nil
		},
		hertz.EncodeJSON,
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

实际应用通常会通过 `WithMiddleware`、`WithMetadataPolicy`、`WithPropagator` 和 client picker 接入治理、metadata 和服务发现。

## Kitex 集成

Kitex generated server 通过 `kitex.Factory` 交给 Keelith adapter 托管生命周期；generated client 通过 `kitex.NewClientSuite` 注入 middleware、metadata、error codec 和 discovery picker。

```go
suite, err := kitex.NewClientSuite(
	kitex.WithClientMiddleware(bundle),
	kitex.WithClientPicker(picker),
)
if err != nil {
	return err
}
_ = suite
```

不要同时启用 native Kitex retry 和 Keelith retry policy；重试预算和错误分类应由一个治理层负责。

## 开发与验证

```bash
cd x
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

由于本模块绑定外部框架，公开行为应优先通过 profile 边界测试验证：metadata 是否按 policy 传播、错误是否可恢复为 Keelith framework error、stream lifecycle 是否正确关闭、picker feedback 是否只记录低敏状态。
