# 生产就绪检查清单与发布防线

在将基于 Keelith 构建的微服务发布到预发或生产环境之前，必须建立清晰的**质量门禁（Quality Gates）**与**生产检查清单（Production Checklist）**。

我们强烈建议将本清单集成至持续集成流水线（CI/CD）和团队发布评审流程中。

---

## 1. 自动化流水线代码门禁 (CI Code Gates)

在每一次代码提交和 PR 合并前，自动化流水线必须全绿通过以下检查：

```bash
# 1. 静态语法与代码规范检查
go vet ./...

# 2. 单元测试与竞态检测 (Race Detector)
go test -race -v ./...

# 3. Keelith 官方只读巡检 (检查 Proto 生成物新鲜度与静态 Wiring 拓扑)
keelith check
```

> [!IMPORTANT]
> **多模块项目检查**：
> 如果你的工程使用了独立扩展模块（如 `contrib`、`operator`、`x`），请在各子模块目录下分别执行：
> ```bash
> GOWORK=off go test -race ./...
> ```

---

## 2. 生产发布前核对清单 (Pre-Release Checklist)

### 2.1 契约与传输一致性
- [ ] **契约版本同步**：生成的 `*.pb.go`、HTTP/gRPC 绑定与当前仓库中的 `*.proto` 处于同一 Git Commit，不存在滞后生成的代码。
- [ ] **接口向前兼容**：未随意修改已上线字段的 tag 编号；已废弃字段使用了 `reserved` 保护，未造成破坏性变更（Breaking Change）。
- [ ] **端口物理隔离**：外部业务端口（如 `8080`）与内部 Ops 运维端口（如 `9091`）已明确分离，Ops 端口未向公网直接暴露。

### 2.2 弹性治理与容灾防范
- [ ] **全链路超时预算**：每个外部 RPC、HTTP、数据库调用均显式设置了有界的 `Timeout`，严禁使用 `context.Background()` 直接发起下游网络请求。
- [ ] **重试预算控制**：已开启 `Retry Budget`，将重试流量锁定在总请求的 10% 以内；严禁在网关和下游服务同时开启多层乘法重试。
- [ ] **核心写操作幂等**：重要写接口（如 订单创建、转账扣款、第三方退款）均配置了 `x-idempotency-key` 幂等去重保护。

### 2.3 资源生命周期与平滑停机
- [ ] **连接池纳管**：数据库（PostgreSQL/MySQL）、缓存（Redis）、消息队列（Kafka Producer）均通过 `app.Component` 接入生命周期管理，未在全局 `var` 中使用不可控连接。
- [ ] **排空超时匹配**：`keelith.WithStopTimeout(30*time.Second)` 已设置；Kubernetes 配置中的 `terminationGracePeriodSeconds`（建议设为 45~60 秒）大于应用内部排空超时，留出充足的流量摘除与数据刷盘时间。
- [ ] **探针配置正确**：Kubernetes 的 `readinessProbe` 指向 `/readyz`，`livenessProbe` 指向 `/livez`。

### 2.4 安全与零凭证泄露
- [ ] **脱敏审计通过**：调用 `app.Describe()` 验证，返回的 JSON 快照中绝对不含密码、Token、API Key 或明文 DSN。
- [ ] **日志合规**：业务日志已对用户手机号、身份证号、银行卡号及鉴权 Authorization Header 进行了脱敏处理。

---

## 3. 证据边界与发布验收分类

在真实工程落地中，**“编译通过”不等于“环境验收通过”**。
Keelith 提倡在发布记录或发布工单中明确区分以下三类证据：

| 证据类别 | 包含的验证项 | 判定标准 |
| :--- | :--- | :--- |
| **已验证 (Verified)** | 单元测试、代码生成、DI 防环校验、静态语法检查 | 本地与 CI 自动化全绿通过 |
| **环境阻断 (Blocked)** | 真实测试环境专线故障、三方沙箱环境暂不可用 | 明确记录阻断原因并由对应负责人签署放行 |
| **部署待验 (Pending)** | 线上配置下发、流量灰度切入、金丝雀观测指标 | 部署到生产后持续观察 15 分钟指标与报警看板 |
