# 依赖注入与静态 Wiring

随着微服务代码规模的增长，构造函数和依赖项会变得越来越庞杂。若采用运行时反射（如 Uber Dig / FX），不仅会拖慢启动速度，更会导致依赖缺失或类型不匹配等问题只能在生产运行时爆发。

Keelith 引入了**编译期静态依赖注入（Compile-time Static DI）**体系：
- 采用纯原生的 Go 构造函数。
- 遵循 **Module（模块化）** 与 **Information Hiding（信息隐藏）** 原则：私有实现仅在模块内部可见，只向外 `Export` 最小接口。
- 通过 CLI 工具静态扫描并生成确定的 `wiring.go` 代码，不依赖任何黑盒反射。

---

## 1. 核心概念：Module、Provide 与 Export

在 Keelith `di` 体系中，一个业务域被封装为一个 `di.Module`：

```go
package task

import (
	"github.com/keelab/keelith/di"
	taskv1 "example.com/task/api/task/v1"
	"example.com/task/internal/biz"
	"example.com/task/internal/data"
	"example.com/task/internal/service"
)

// TaskModule 声明了任务域的完整依赖组装规则
var TaskModule = di.MustModule("task",
	// 1. 注册数据持久化实现（作为私有组件，不对外暴露具体结构体）
	di.Provide(data.NewTaskRepo, di.As((*biz.TaskRepo)(nil))),

	// 2. 注册核心领域用例
	di.Provide(biz.NewTaskUseCase),

	// 3. 注册服务接口实现，并声明其实现了 Protobuf 契约接口
	di.Provide(service.NewTaskService, di.As((*taskv1.TaskServiceKeelithServer)(nil))),

	// 4. 显式导出对外接口（只有被 Export 的接口才能被外部模块或 main 组合根导入）
	di.Export((*taskv1.TaskServiceKeelithServer)(nil)),
)
```

### 1.1 `di.As`：接口绑定与依赖倒置
通过 `di.As((*InterfaceType)(nil))`，Keelith 将某个具体的结构体构造函数（如 `data.NewTaskRepo`）与领域层接口（`biz.TaskRepo`）绑定在一起，保证业务层永远只依赖抽象接口。

### 1.2 `di.Export`：严格的信息隐藏边界
这是 Keelith DI 的一大特色：**未经 `Export` 声明的类型属于模块内部私有状态**。
例如，存储层的 PostgreSQL 实现或 Redis 客户端只在当前 Module 内流转；外部应用只能获取到最终生成的契约 Server。这样，即使底层将存储从 MySQL 替换为 MongoDB，整个应用的组装图依然保持干净稳定。

---

## 2. 声明式配置：`keelith.project.json`

Keelith CLI 通过项目根目录的声明文件感知工程结构：

```json
{
  "$schema": "https://keelith.dev/schema/v1/project.json",
  "name": "task-service",
  "modules": [
    "example.com/task/internal/data",
    "example.com/task/internal/biz",
    "example.com/task/internal/service"
  ],
  "roots": [
    "example.com/task/cmd/task-service"
  ]
}
```

---

## 3. CLI 静态生成与依赖校验

使用 `keelith-cli` 可以自动化生成并校验依赖注入代码：

```bash
# 1. 静态分析依赖拓扑，并在 cmd/task-service/ 下生成 wiring.go
keelith wiring sync

# 2. 校验依赖图是否存在环形依赖 (Cycle) 或未导出的私有接口泄露
keelith wiring check

# 3. 严格验证全链路完整性
keelith wiring verify

# 4. 导出可视化依赖图 (DOT 格式)
keelith wiring graph --format dot > di_graph.dot
```

生成的 `wiring.go` 是标准的、易于排查的普通 Go 代码，直接将各层对象按拓扑顺序初始化并连接。

---

## 4. 接入 Keelith 组合根

在 `cmd/task-service/main.go` 中，将静态生成的依赖图传入 `keelith.New`：

```go
package main

import (
	"context"
	"github.com/keelab/keelith"
	taskv1 "example.com/task/api/task/v1"
)

func main() {
	// 1. 初始化由 keelith wiring 生成的依赖图
	graph, cleanup, err := InitializeGraph()
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// 2. 从 graph 中获取导出的服务契约
	taskServer := graph.TaskServer()
	binding := taskv1.NewTaskServiceBinding(taskServer)

	// 3. 启动应用
	app, err := keelith.New(
		keelith.WithName("task-service"),
		keelith.WithHTTP("127.0.0.1:8080"),
		keelith.WithGRPC("127.0.0.1:9090"),
		keelith.WithServices(binding),
		// 将 DI Graph 的组件生命周期纳管进 Keelith
		keelith.WithGraph(graph),
	)
	if err != nil {
		panic(err)
	}

	_ = app.Run(context.Background())
}
```

---

## 5. 依赖拓扑防环与失败回滚 (Rollback Safety)

在系统启动阶段，如果某个依赖对象的构造函数返回了 `error`（例如数据库配置错误）：
1. **拓扑防环**：Keelith 在编译和初始化阶段构建有向无环图（DAG）。一旦检测到 `A -> B -> A` 的环形引用，直接拒绝编译并高亮循环路径。
2. **自动 Cleanup 回滚**：如果第 4 个组件构造失败，Keelith 会按照**严格相反的顺序**，自动触发前 3 个已创建对象的 `Close / Cleanup` 回滚逻辑，杜绝打开的文件句柄或连接池泄露。
