# 手写最小 HTTP 路由

在需要编写极小脚本、内部轻量管理端点、健康探测适配器或初学体验时，你可以完全跳过代码生成流程，直接使用 Keelith 提供的极简 HTTP 门面 API。

---

## 1. 最小可运行示例

在任何已初始化的 Go 模块中，只需引入 Keelith 核心包：

```bash
go get github.com/keelab/keelith@latest
```

编写 `main.go`：

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/keelab/keelith"
)

func main() {
	// 1. 创建响应操作系统退出信号的 Context
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	// 2. 纯内存装配应用对象图（不执行任何网络 I/O）
	app, err := keelith.New(
		keelith.WithName("simple-api"),
		keelith.WithHTTP("127.0.0.1:8080"),
		keelith.WithStopTimeout(10*time.Second),

		// 注册轻量 HTTP 路由
		keelith.WithRoute("GET", "/ping", func(ctx context.Context, r *http.Request) (any, error) {
			return map[string]string{
				"status":  "ok",
				"message": "pong",
			}, nil
		}),

		// 注册带动态参数处理的路由
		keelith.WithRoute("POST", "/echo", func(ctx context.Context, r *http.Request) (any, error) {
			name := r.URL.Query().Get("name")
			if name == "" {
				name = "guest"
			}
			return map[string]string{
				"greeting": fmt.Sprintf("Hello, %s!", name),
				"time":     time.Now().Format(time.RFC3339),
			}, nil
		}),
	)
	if err != nil {
		panic(fmt.Errorf("build keelith application: %w", err))
	}

	// 3. 显式启动应用（进入监听状态，阻塞直至信号触发或发生严重错误）
	if err := app.Run(ctx); err != nil {
		panic(fmt.Errorf("run keelith application: %w", err))
	}
}
```

运行程序：
```bash
go run main.go
```

测试接口：
```bash
curl http://127.0.0.1:8080/ping
# 输出: {"message":"pong","status":"ok"}

curl -X POST "http://127.0.0.1:8080/echo?name=Keelab"
# 输出: {"greeting":"Hello, Keelab!","time":"2026-09-05T17:30:00+08:00"}
```

---

## 2. 核心机制解析

### 2.1 `RouteHandler` 函数签名
```go
type RouteHandler func(context.Context, *http.Request) (any, error)
```
- **入参**：
  - `context.Context`：已经注入了链路追踪 Span、请求 ID（`X-Request-Id`）及超时控制的上下文。
  - `*http.Request`：原始的标准库 HTTP 请求，可读取 Query 参数、Header 或 Body。
- **返回值**：
  - `any`：成功返回的对象，Keelith 内部会自动将其以 `application/json` 序列化并写入响应体，默认返回 `200 OK`。
  - `error`：若返回非空错误，Keelith 会将其匹配为 HTTP 错误响应（若为 Keelith 统一错误，将自动输出对应的 HTTP 状态码与 JSON 错误报文）。

### 2.2 信号监听与优雅停机
在上述代码中：
```go
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer cancel()
...
app.Run(ctx)
```
当你在终端按下 `Ctrl + C`（或 Kubernetes 容器向进程发送 `SIGTERM`）时，`ctx` 会被取消。`app.Run` 会自动捕获取消事件，进入平滑停机流程：
1. 立即停止接收新的外部 HTTP 连接。
2. 等待在途请求处理完毕（最长不超过 `WithStopTimeout` 指定的时间，示例中为 10 秒）。
3. 依次释放内部组件资源，最后安全退出。

---

## 3. 为什么及何时升级到“契约模式”？

虽然 `WithRoute` 极度便捷，但它有严格的架构边界：

| 特性 | `WithRoute`（轻量路由） | `service.Binding`（契约优先） |
| :--- | :--- | :--- |
| **支持协议** | 仅支持 HTTP/JSON | 同时支持 HTTP 与 gRPC 双协议 |
| **类型安全** | 依靠手动解析和运行时断言 | 编译期强类型安全、无运行时猜测 |
| **文档生成** | 无自动文档 | 自动生成 OpenAPI / Swagger 规范文档 |
| **入参校验** | 需手写繁琐的 `if` 逻辑 | 基于 Protobuf 规则自动校验（Validate） |
| **团队协同** | 前后端依靠口头约定字段 | 以 Proto 文件为团队跨语言唯一契约 |

> [!CAUTION]
> **切勿把生产核心业务堆砌在 `WithRoute` 中**。当你的接口需要服务间 RPC 调用、跨语言 SDK 生成、复杂参数校验时，请务必使用下一章介绍的契约生成机制。
