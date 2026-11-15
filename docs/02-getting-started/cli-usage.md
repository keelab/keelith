# Keelith CLI 工具全景手册

`keelith-cli` 是与 Keelith 运行时配套的官方脚手架与日常开发助手。它负责创建微服务模板、驱动热重载开发、维护组件声明、执行只读代码巡检以及生成静态依赖图。

---

## 1. 安装与升级

```bash
# 安装最新版 CLI
go install github.com/keelab/keelith-cli/cmd/keelith@latest

# 安装 Proto 契约生成插件
go install github.com/keelab/keelith-cli/cmd/protoc-gen-go-keelith@latest

# 检查当前版本
keelith --version
```

---

## 2. 命令速查表

| 命令 | 类别 | 用途说明 |
| :--- | :--- | :--- |
| `keelith new <name>` | 脚手架 | 从官方模板初始化创建微服务新工程 |
| `keelith dev` | 开发 | 自动刷新契约与静态 wiring，编译并运行应用 |
| `keelith dev --watch` | 开发 | 开启热重载模式，文件变动时自动重新编译重启 |
| `keelith check` | 质量门禁 | 只读检查项目结构、工具链版本、生成物与依赖图一致性 |
| `keelith add api <name>` | 契约扩展 | 快速向项目追加新的 Proto API 契约声明与目录 |
| `keelith add component <name>`| 基础设施 | 向项目引入 Redis / PostgreSQL / Kafka 等标准基础设施组件 |
| `keelith generate` | 代码生成 | 根据契约和 manifest 执行受控的代码生成 |
| `keelith wiring sync` | 静态 DI | 分析依赖图并同步生成静态依赖注入代码 |
| `keelith wiring graph`| 架构可视化 | 导出 DOT 格式的组件依赖拓扑图 |

---

## 3. 核心子命令深度解析

### 3.1 `keelith new`：创建新工程
```bash
keelith new <project_name> [flags]
```
**可用参数：**
- `--template`（简写 `-t`）：模板类型。
  - `service`（默认）：包含完整 Proto 契约、双协议绑定、DDD 分层的工业级微服务模板。
  - `minimal`：仅保留核心运行时的极简轻量模板。
- `--path`：指定生成的绝对或相对目标目录。若不传，默认在当前目录下创建以项目名命名的文件夹。

**示例：**
```bash
# 在指定目录创建订单服务
keelith new orders --path ./services/orders-api --template service
```

---

### 3.2 `keelith dev`：开发环境驱动器
`keelith dev` 是本地开发时的第一入口。每次执行时，它会自动完成以下流水线：
1. 向上查找并确认当前 Go 项目根目录。
2. 扫描 `keelith.project.json` 与 Proto 文件，刷新契约物。
3. 执行编译并前台运行应用，将格式化的统一日志打印到标准输出。

**Watch 模式：**
```bash
keelith dev --watch
```
> [!TIP]
> **开发防崩溃保护**：在 `--watch` 模式下，如果你正在编辑的代码产生语法错误或编译失败，CLI **不会杀死当前正在运行的旧服务实例**。它会在终端高亮显示错误，直到你修复编译通过后，再执行平滑置换。

---

### 3.3 `keelith check`：CI/CD 只读巡检
与 `generate` 或 `dev` 不同，`keelith check` **是严格只读的**。它绝不会修改磁盘上的任何源码，适合放在 Git Pre-commit 钩子或 CI 流水线门禁中：
```bash
keelith check
```
**检查项包括：**
- 依赖的 `go.mod` 与工具链版本是否合规。
- 已生成的 `.pb.go` 和 `.keelith.pb.go` 是否与当前 `*.proto` 文件保持 100% 同步（防止遗漏提交）。
- 依赖注入拓扑（DI Graph）是否存在环依赖、未导出的私有依赖或未解析的依赖项。

---

### 3.4 `keelith add`：快速追加组件与契约

#### 快速添加 API 契约
```bash
keelith add api payment.v1
```
CLI 会在 `api/payment/v1/` 下自动生成标准模板文件 `payment.proto`，并配置好推荐的路径与包名规则。

#### 快速接入官方组件
```bash
keelith add component redis
keelith add component postgres
```
CLI 会在 `keelith.project.json` 中追加组件声明，生成标准的基础设施配置模板，并在 `internal/data/` 下补充对应的连接池包装代码。

---

### 3.5 `keelith wiring`：静态依赖注入维护
底层依赖注入相关的管理命令集成在 `wiring` 子命令中：

```bash
# 校验当前项目的 DI 拓扑是否健康
keelith wiring check

# 重新生成依赖注入组装代码
keelith wiring sync

# 导出依赖关系拓扑图（可重定向到 Graphviz 生成图片）
keelith wiring graph --format dot > graph.dot
dot -Tpng graph.dot -o graph.png
```
