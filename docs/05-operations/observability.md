# 统一可观测性与 Ops 诊断

在微服务治理中，**“无法观测就无法度量，无法度量就无法治理”**。
Keelith 在核心设计中就将可观测性（Observability）作为一等公民，无缝打通了**分布式链路追踪（Tracing）、监控指标（Metrics）、结构化日志（Logging）、应用健康状态机（Health Registry）与独立运维端点（Ops Server）**。

---

## 1. 可观测三支柱打通

```text
外部请求传入 (带有或生成 Trace ID)
       │
       ├─► 1. 链路追踪 (OpenTelemetry Tracing)
       │      • 遵循 W3C TraceContext 标准
       │      • 跨进程在 HTTP Header / gRPC Metadata 间透明透传
       │
       ├─► 2. 结构化日志 (Structured Logging)
       │      • 自动在每一行日志附加 trace_id, span_id, request_id
       │      • 支持根据运行时配置动态调节日志级别
       │
       └─► 3. 监控指标 (Prometheus Metrics)
              • 自动打点请求总数、QPS、耗时分布 (P50/P90/P99)
              • 采用低基数标签，杜绝 Prometheus 内存膨胀
```

---

## 2. 统一健康检查注册中心 (`health.Registry`)

在 Kubernetes 环境中，容器的存活探针（Liveness）与就绪探针（Readiness）对稳定性至关重要。
Keelith 提供了结构化的 `health.Registry`：

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/keelab/keelith"
	"github.com/keelab/keelith/health"
)

func main() {
	// 1. 创建健康检查注册表
	healthRegistry := health.NewRegistry()

	// 2. 注册数据库依赖探针
	err := healthRegistry.Register(
		health.KindDependency,
		"postgres-primary",
		func(ctx context.Context) error {
			// 执行轻量级探测 (注意必须具备超时限制，严禁长时间挂起)
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			return db.PingContext(ctx)
		},
	)
	if err != nil {
		panic(err)
	}

	// 3. 注册到应用
	app, err := keelith.New(
		keelith.WithName("user-service"),
		keelith.WithHTTP("127.0.0.1:8080"),
		keelith.WithHealth(healthRegistry),
		// 开启独立 Ops 运维端口 (默认 9091)
		keelith.WithOps(),
	)
	if err != nil {
		panic(err)
	}

	_ = app.Run(context.Background())
}
```

### 2.1 生产探针语义对照表
| 探针端点 | 对应 Kubernetes 探针 | 失败时的平台行为 | Keelith 判断标准 |
| :--- | :--- | :--- | :--- |
| `/livez` | `livenessProbe` | 容器被杀死并重启 | 进程未死锁、核心事件循环正常运转 |
| `/readyz` | `readinessProbe` | 摘除 Pod 流量（不重启） | 处于 `READY` 状态且外部核心依赖均健康 |
| `/healthz`| 通用综合健康状态 | 监控大盘告警 | 综合展示所有已注册检查项的诊断详情 |

---

## 3. 专属 Ops 运维服务 (`ops.Server`)

为了避免将敏感的监控指标与性能分析端点暴露给公网用户，Keelith 采用**物理端口隔离策略**：
- 业务流量走公开端口（如 `8080`）。
- 监控采集与健康检查走独立的内部管理端口（如 `9091`）。

在 Ops 服务上默认提供：
- `/healthz`, `/livez`, `/readyz`：健康检查。
- `/metrics`：Prometheus 采集端点。
- `/debug/pprof/*`（可配置开启）：Go 原生性能分析接口。

---

## 4. 零敏感凭证泄露快照：`Application.Describe()`

在生产排障或配置巡检时，平台往往需要自动化收集实例的运行拓扑。
调用 `app.Describe()` 会导出一份纯内存、无锁、无敏感信息的 JSON 快照：

```json
{
  "name": "user-service",
  "state": "READY",
  "http": {
    "enabled": true,
    "address": "0.0.0.0:8080"
  },
  "grpc": {
    "enabled": true,
    "address": "0.0.0.0:9090"
  },
  "ops": {
    "enabled": true,
    "address": "127.0.0.1:9091"
  },
  "profile": {
    "services": [
      "user.v1.UserService"
    ]
  },
  "components": [
    "postgres-pool",
    "redis-client"
  ]
}
```

> [!TIP]
> **无敏感信息（Secret-Free）保证**：
> `Describe()` 永远不会输出任何密码、连接串、私钥或业务数据。即使运维脚本或自动化 Agent 定期抓取该端点，也无需担心敏感信息泄露至日志系统。
