# 传输层：HTTP 与 gRPC 详解

在微服务系统中，传输层（Transport Layer）负责将底层的网络连接与字节流转换为应用所理解的统一请求模型。Keelith 内置了对 **HTTP/1.1、HTTP/2、gRPC、SSE（Server-Sent Events）与 WebSocket** 的完整支持，同时在独立包中提供了统一的客户端治理通道。

---

## 1. HTTP 传输服务 (`transport/http`)

Keelith HTTP 传输引擎构建在高性能路由与标准库扩展之上，既支持 Protobuf 生成的服务契约绑定，也支持原生 HTTP 控制。

### 1.1 服务端核心配置与选项
在应用组合根或独立装配中，HTTP 服务端支持丰富的调优参数：

```go
package main

import (
	"context"
	"time"

	"github.com/keelab/keelith"
	khttp "github.com/keelab/keelith/transport/http"
)

func main() {
	app, err := keelith.New(
		keelith.WithName("web-service"),
		// 配置 HTTP 服务端监听与网络参数
		keelith.WithHTTP("0.0.0.0:8080"),
		keelith.WithStopTimeout(15*time.Second),
	)
	if err != nil {
		panic(err)
	}

	_ = app.Run(context.Background())
}
```

### 1.2 HTTP 请求解码与响应编码机制
当接收到一个 HTTP 请求时，Keelith 内部的解码管道会根据 `Content-Type` 与 `Accept` Header 进行智能适配：
1. **Request Decoder**：从 `Content-Type`（如 `application/json` 或 `application/x-protobuf`）选择对应编解码器（Codec），将 Body 反序列化为契约消息对象。
2. **Response Encoder**：业务逻辑执行成功返回后，将 Response 结构体序列化并以对应格式写入 `http.ResponseWriter`，返回 `200 OK`。
3. **Error Encoder**：若业务返回错误，通过统一错误编码器将 `*errors.Error` 转换为对应 HTTP 状态码与结构化 JSON 报文。

---

## 2. gRPC 传输服务 (`transport/grpc`)

Keelith 对 gRPC 提供了工业级支持，无缝打通了 Unary（单次调用）与 Stream（流式调用）的拦截器机制。

### 2.1 服务端配置与启动
```go
app, err := keelith.New(
    keelith.WithName("grpc-service"),
    // 监听 gRPC 专用端口
    keelith.WithGRPC("0.0.0.0:9090"),
    // 注册业务契约
    keelith.WithServices(orderBinding),
)
```

### 2.2 拦截器执行顺序
gRPC 的调用流水线严格遵循：
```text
Client Call
  ──► gRPC Recovery (Panic 捕获)
  ──► OTel Tracing (Span 注入)
  ──► Inbound Middleware (认证、鉴权、全局限流)
  ──► Stream / Unary Policy (针对具体 RPC 方法的治理)
  ──► Service Method Implementation
```

---

## 3. 客户端出站治理 (`client.Outbound`)

微服务架构中，调用外部下游服务（无论是通过 HTTP 还是 gRPC）都需要具备防御能力。Keelith 提供了 `client.NewOutbound` 统一出站组合入口：

```go
package client

import (
	"time"

	"github.com/keelab/keelith/client"
	"github.com/keelab/keelith/governance/dependency"
)

func CreateCatalogClient() (*client.Outbound, error) {
	// 配置针对 downstream "catalog-service" 的出站治理策略
	outbound, err := client.NewOutbound(client.OutboundConfig{
		Dependency: dependency.Config{
			Name: "catalog-service",
			// 单次 RPC 超时控制
			Timeout: 2 * time.Second,
			// 重试预算：最多重试 2 次，重试流量不得超过总请求的 10%
			Retry: dependency.RetryConfig{
				MaxAttempts: 2,
				BudgetRate:  0.1,
			},
			// 熔断策略：当 10 秒内失败率超过 30% 时触发熔断
			Breaker: dependency.BreakerConfig{
				FailureRate: 0.3,
				Window:      10 * time.Second,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return outbound, nil
}
```

通过统一的 `Outbound` 实例创建的所有下游客户端连接，将自动共享这一套防御策略与链路追踪，杜绝无节制重试拖垮下游。

---

## 4. 传输协议选型建议

| 协议 | 适用场景 | 核心优势 |
| :--- | :--- | :--- |
| **HTTP/JSON** | 前端 Web/App 交互、公网开放网关 API | 兼容性极佳、调试方便、跨平台通用 |
| **gRPC** | 集群内部服务间 RPC 通信、高吞吐大促调用 | Protobuf 二进制高性能压缩、流式传输（Streaming）、强类型桩 |
| **SSE** | 单向服务器事件推流（如 AI 大模型打字机流式输出、通知推送） | 基于标准 HTTP，零协议升级开销，代理与防火墙友好 |
| **WebSocket** | 网页即时双向互动通信（如 在线协作、聊天室、实时游戏） | 全双工低延迟双向数据传输 |
