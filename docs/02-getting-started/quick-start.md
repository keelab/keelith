# 快速入门 (Quick Start)

本指南将带你从零开始搭建开发环境，使用官方脚手架工具 `keelith-cli` 创建并运行你的第一个 Keelith 微服务。

---

## 1. 环境准备

在开始之前，请确保本地已安装并配置好以下基础开发工具：

1. **Go 编程语言**：
   - 建议版本：**Go 1.26 或更高**（符合主仓库 `go.mod` 规范）。
   - 验证安装：`go version`
2. **包管理工具**：
   - 开启 Go 模块支持：`go env -w GO111MODULE=on`
   - 国内用户建议配置镜像源：`go env -w GOPROXY=https://goproxy.cn,direct`
3. **接口调试工具**：
   - `curl`：用于测试 HTTP 接口。
   - `grpcurl`（可选）：用于测试 gRPC 接口。

---

## 2. 安装 Keelith 核心工具链

执行以下命令安装官方开发脚手架：

```bash
# 安装脚手架 CLI
go install github.com/keelab/keelith-cli/cmd/keelith@latest

# 安装契约生成插件 (供后续生成 HTTP/gRPC 绑定使用)
go install github.com/keelab/keelith-cli/cmd/protoc-gen-go-keelith@latest
```

安装完成后，在终端验证：
```bash
keelith --version
```
如果能够正常输出版本信息，说明工具链安装成功。

---

## 3. 创建微服务项目

Keelith 提供了两种标准项目脚手架模板：

### 方式 A：契约优先模板（工业级推荐：`service`）
包含预设的 Protobuf 定义、双协议代码生成和符合 Clean Architecture 的工程结构：

```bash
# 1. 使用 service 模板创建项目
keelith new hello --template service

# 2. 进入项目目录
cd hello

# 3. 下载依赖包
go mod download
```

### 方式 B：极简模板（轻量原型：`minimal`）
仅包含最基础的 HTTP 启动依赖，适合快速验证开发工具链：

```bash
keelith new hello --template minimal
cd hello
```

> [!TIP]
> **指定目标路径**：
> `NAME` 默认既是项目标识也是文件夹名。如需指定自定义存放路径，可使用 `--path` 参数：
> ```bash
> keelith new orders --path ./services/orders-api
> ```

---

## 4. 运行与开发模式

进入项目目录后，通过 `keelith dev` 一键启动开发运行时：

```bash
keelith dev
```

输出日志类似于：
```text
INFO msg=config loaded: configs/config.yaml format=yaml
INFO msg=[HTTP] server listening on: 127.0.0.1:8080
INFO msg=[gRPC] server listening on: 127.0.0.1:9090
INFO msg=[Ops]  server listening on: 127.0.0.1:9091
INFO msg=application ready name=hello state=READY
```

### 开发热重载（Watch 模式）
在日常开发业务代码时，推荐开启 `--watch` 参数：
```bash
keelith dev --watch
```
- 当检测到源码或契约文件变更时，CLI 会自动刷新生成物、重新编译并重启服务。
- 若修改的代码存在编译错误，**Watch 模式会自动保留上一个正常运行的实例**，避免因手误导致调试中断。

---

## 5. 测试接口

打开另一个终端窗口，测试正在运行的服务：

### 5.1 测试 HTTP 接口
```bash
curl -i http://127.0.0.1:8080/v1/ping
```
响应示例：
```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
X-Request-Id: 018e47f2-a3c4-7910-b193-c91823901bc4

{
  "message": "pong"
}
```

### 5.2 测试 gRPC 接口
若安装了 `grpcurl`，可直接反射或调用 gRPC 接口：
```bash
grpcurl -plaintext 127.0.0.1:9090 hello.v1.HelloService/Ping
```
响应示例：
```json
{
  "message": "pong"
}
```

### 5.3 测试 Ops 健康探针
Keelith 默认将运维端点隔离开放（默认端口 `9091`）：
```bash
curl http://127.0.0.1:9091/healthz
```
响应 `200 OK` 即表示服务内部依赖健康。

---

## 6. 只读代码检查（keelith check）

在提交 Git 变更或 CI 流水线前，可以使用 `check` 子命令执行只读工程巡检：

```bash
keelith check
```
它会检查项目工程目录、Proto 生成物的新鲜度、依赖图完整性以及是否存在未同步的静态 wiring 代码。
