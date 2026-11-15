# Surface、Group 与 Policy 边界

在复杂的分布式生产环境中，一个微服务很少只有一个对外暴露的监听端口。往往需要：
- **公共端口（Public API）**：面向前端或网关，需要严格的跨域 CORS、身份鉴权、防重放保护与按 IP 限流。
- **内部端口（Internal RPC）**：面向集群内同机房服务，需要 mTLS、服务网格元数据传递与宽松的速率限制。
- **运维端口（Ops / Admin）**：面向 Prometheus 和 Kubernetes 探针，仅开放健康检查、指标抓取与诊断端点。

Keelith 通过 **Surface（网络物理平面）**、**Group（服务逻辑分组）** 与 **Policy（声明式策略）** 三层抽象，实现了优雅的多端口隔离与策略精细编排。

---

## 1. 概念模型：从物理网络到服务策略

```text
                  ┌─────────────────────────────────────┐
                  │ 物理平面 (Surface): 8080 (Public)   │
                  │  • HTTP Server 全局中间件            │
                  └──────────────────┬──────────────────┘
                                     │
                 ┌───────────────────┴───────────────────┐
                 │                                       │
                 ▼                                       ▼
    ┌─────────────────────────┐             ┌─────────────────────────┐
    │ 逻辑群组 (Group: public)│             │ 逻辑群组 (Group: auth)  │
    │  • 策略: X-Request-Id   │             │  • 策略: JWT 鉴权        │
    │  • 策略: 公网按 IP 限流 │             │  • 策略: 严格超时控制   │
    └────────────┬────────────┘             └────────────┬────────────┘
                 │                                       │
                 ▼                                       ▼
     ┌───────────────────────┐               ┌───────────────────────┐
     │ Binding: 商品浏览服务 │               │ Binding: 订单支付服务 │
     └───────────────────────┘               └───────────────────────┘
```

---

## 2. 策略执行流水线 (Policy Chain Pipeline)

当一个网络请求进入 Keelith 后，中间件与策略的执行顺序是严格确定的：

```text
1. Placement 匹配
   └─ 2. Server 全局中间件链 (全局日志、全局 Panic 恢复)
       └─ 3. Group 继承策略链 (群组鉴权、群组限流)
           └─ 4. Binding 特殊策略链 (特定接口白名单)
               └─ 5. Generated Adapter (反序列化与参数校验)
                   └─ 6. Domain Handler (业务代码执行)
```

> [!IMPORTANT]
> **Placement 防伪造安全原则**：
> Placement（路由与服务归属匹配）必须在传输层协议报文成功解析、请求头通过合法性清洗后才会注入 Context。在早期阶段就被端口拒绝的非法探测请求，绝对无法伪造内部 Group 或 Service 的 Placement 上下文。

---

## 3. 代码实战：配置服务群组与策略

以下代码展示了如何将公开接口与私有敏感接口进行逻辑分组，并声明不同的安全能力（Capability）：

```go
package main

import (
	"context"
	"net/http"

	"github.com/keelab/keelith"
	"github.com/keelab/keelith/service"
	catalogv1 "example.com/catalog/api/catalog/v1"
	orderv1 "example.com/order/api/order/v1"
)

func main() {
	// 1. 定义公开服务群组 (Public Group)
	publicGroup := service.NewGroup("public").
		// 声明必须具备的能力
		RequireHTTP(service.CapabilityRequestID).
		// 挂载公开策略
		UseHTTPPolicies(service.NewPolicy(requestIDPolicy, service.CapabilityRequestID)).
		// 绑定公开服务实现
		Bind(catalogv1.NewCatalogServiceBinding(catalogImpl))

	// 2. 定义受保护群组 (Authenticated Group)
	authGroup := service.NewGroup("authenticated").
		RequireHTTP(service.CapabilityRequestID).
		// 挂载 JWT 鉴权策略
		UseHTTPPolicies(service.NewPolicy(jwtAuthPolicy, "auth.jwt")).
		// 绑定敏感服务实现
		Bind(orderv1.NewOrderServiceBinding(orderImpl))

	// 3. 将群组合并为一个整体 Profile
	profile, err := service.NewProfileFromGroups("my-platform", publicGroup, authGroup)
	if err != nil {
		panic(err)
	}

	// 4. 组装进应用
	app, err := keelith.New(
		keelith.WithName("gateway-service"),
		keelith.WithHTTP("0.0.0.0:8080"),
		keelith.WithProfile(profile),
	)
	if err != nil {
		panic(err)
	}

	_ = app.Run(context.Background())
}

// 示例：简单的请求 ID 注入策略
func requestIDPolicy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" {
			reqID = "gen-" + time.Now().Format("150405.000")
		}
		w.Header().Set("X-Request-Id", reqID)
		next.ServeHTTP(w, r)
	})
}
```

---

## 4. 低基数元数据在可观测性中的价值

在生产监控中，高基数标签（如带有用户 ID 或订单号的 URL）会击垮 Prometheus 导致 OOM。
Keelith 在设计 Surface、Group、Service 名字时，严格保证其为**静态、确定的低基数标签**：
- 日志中的字段：`surface="http-public"`，`group="authenticated"`，`service="order.v1.OrderService"`。
- Metrics 标签：所有的请求耗时 Histogram 统一按低基数分组，方便在 Grafana 中进行统一聚合看板展示。
- 未分配任何 Group 的散装服务绑定，Keelith 自动赋予固定的默认值 `_none`。
