# 配置管理与动态环境

在云原生微服务中，配置管理不仅需要支持静态的 YAML/JSON 文件解析，更需要满足**环境变量覆盖（12-Factor App）、运行期动态热重载、强类型结构体绑定与拼写防呆严格校验**等生产要求。

Keelith 通过统一的 `config` 体系，将多源配置与应用生命周期紧密结合。

---

## 1. 快速上手：文件与环境变量融合

在应用构造中，通过 `keelith.WithConfigFile` 开启标准配置运行时：

```go
package main

import (
	"context"
	"time"

	"github.com/keelab/keelith"
)

func main() {
	app, err := keelith.New(
		keelith.WithName("order-service"),
		keelith.WithHTTP("127.0.0.1:8080"),

		// 启用标准配置文件与环境变量注入
		keelith.WithConfigFile(
			"configs/app.yaml",
			// 1. 环境变量前缀映射：例如 ORDER_HTTP__ADDRESS 会覆盖 http.address
			keelith.WithConfigEnvPrefix("ORDER"),
			// 2. 配置轮询热更新检查间隔
			keelith.WithConfigPollInterval(5*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}

	_ = app.Run(context.Background())
}
```

> [!IMPORTANT]
> **延迟加载原则**：
> 配置文件的读取与解析是在 `Application.Run` 开始时触发，而不是在 `keelith.New` 构造函数执行期间。这确保了在应用构造阶段不会因磁盘 I/O 阻塞或配置暂未下发而引发崩溃。

---

## 2. 强类型结构体绑定 (Typed Binding)

为了避免在业务代码中散落硬编码的键名字符串，Keelith 提供了强类型的配置绑定。定义与 YAML 对应的 Go 结构体：

```go
package config

import (
	"time"
)

type DatabaseConfig struct {
	Driver          string        `json:"driver" yaml:"driver"`
	DSN             string        `json:"dsn" yaml:"dsn"`
	MaxOpenConns    int           `json:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime" yaml:"conn_max_lifetime"`
}

type ServerConfig struct {
	HTTPAddress string        `json:"http_address" yaml:"http_address"`
	ReadTimeout time.Duration `json:"read_timeout" yaml:"read_timeout"`
}
```

绑定并监听热重载：

```go
// 创建强类型配置容器
dbCfg := &config.DatabaseConfig{}

// 构建绑定定义
binding := config.NewTypedBinding("database", dbCfg, func(newVal any) {
    updated := newVal.(*config.DatabaseConfig)
    log.Printf("检测到数据库配置动态变更: max_conns=%d", updated.MaxOpenConns)
})

// 传入 WithConfigFile
keelith.WithConfigFile(
    "configs/app.yaml",
    keelith.WithConfigBindings(binding),
)
```

---

## 3. 严格模式与拼写防呆校验 (Strict Mode)

运维人员在修改配置 YAML 时，最容易发生拼写错误（例如将 `timeout` 误写为 `time_out`），导致默认值生效引发不可预知的线上事故。

Keelith 提供了 `WithConfigStrict` 机制，强制校验未识别字段：

```go
keelith.WithConfigFile(
    "configs/app.yaml",
    // 开启严格模式：任何未在 knownFields 中声明的多余字段都会导致加载报错
    keelith.WithConfigStrict(),
    keelith.WithConfigKnownFields(
        "http.address",
        "http.timeout",
        "database.driver",
        "database.dsn",
        "redis.addr",
    ),
)
```

如果 YAML 文件中出现了类似 `database.dsnn: "..."` 的笔误，应用在 `Run` 启动初期的校验阶段就会立即阻止服务启动，并打印出具体不认识的字段名，提前拦截事故。

---

## 4. 环境变量覆盖规则

Keelith 使用双下划线 `__` 表示层级嵌套：

| 环境变量名 | 对应配置项路径 | 转换说明 |
| :--- | :--- | :--- |
| `ORDER_HTTP__ADDRESS` | `http.address` | 顶级前缀 `ORDER_`，双下划线转为点 `.` |
| `ORDER_DATABASE__MAX_OPEN_CONNS` | `database.max_open_conns` | 单下划线保留为字段内部下划线 |
| `ORDER_REDIS__TIMEOUT` | `redis.timeout` | 自动将字符串如 `"3s"` 解析为 `3 * time.Second` |

---

## 5. 敏感凭证管理与安全脱敏

> [!CAUTION]
> **生产安全禁令**：
> 1. 绝对不要将明文的数据库密码、云厂商 API Key、私钥直接提交到 Git 仓库中的 `config.yaml` 中。
> 2. 生产环境敏感值应通过容器环境变量（Kubernetes Secret 挂载）动态注入。
> 3. Keelith 的 `Application.Describe()` 与日志组件已针对 DSN、Password、Secret 执行了自动掩码处理（Redaction）。在编写业务日志时，也请遵循同样规范，杜绝打印敏感凭证。
