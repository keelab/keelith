# Keelith Operator

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/operator.svg)](https://pkg.go.dev/github.com/keelab/operator)
[![License](https://img.shields.io/github/license/keelab/keelith)](../LICENSE)

`github.com/keelab/operator` 是 Keelith programmable topology 的 Kubernetes 控制面。它定义 namespaced `TopologyRevision` API，并把一个通过校验的拓扑 revision 渲染为普通 Kubernetes Deployment、Service 等 namespace-scoped workload。

Operator 只协调控制面对象，不代理业务流量。

## 能力

- `api/v1alpha1`：`topology.keelith.dev/v1alpha1`、`TopologyRevision`、CRD YAML 和 plan JSON schema。
- `operator.Reconciler`：读取、校验、签名验证、单调 revision 检查、资源 apply、stale resource 清理和 status 更新。
- `operator.RunController`：对单个 namespace 执行 List/Watch reconcile loop，watch 断开后重新 List，使用有界退避。
- `kubernetes.Render`：把 topology plan 和 workload spec 渲染为 Kubernetes workload 资源。
- `cmd/keelith-topology-controller`：可运行的 controller 入口，支持 in-cluster config 或可选 kubeconfig。

## 安装 CRD

```bash
kubectl apply -f api/v1alpha1/topologyrevision.crd.yaml
```

控制器需要能在目标 namespace 中读取 `TopologyRevision`，并创建、更新、删除由 revision 管理的 workload 资源。具体 RBAC 应按部署环境生成和收紧。

## 运行 controller

本地调试：

```bash
cd operator
GOWORK=off go run ./cmd/keelith-topology-controller \
  --namespace default \
  --kubeconfig "$HOME/.kube/config"
```

集群内运行时，namespace 默认从 `POD_NAMESPACE` 读取：

```bash
keelith-topology-controller --namespace default
```

信任策略必须二选一：

- `KEELITH_TOPOLOGY_PUBLIC_KEY`：base64 编码 Ed25519 public key，用于验证 signed topology revision。
- `KEELITH_TOPOLOGY_ALLOW_UNSIGNED=true`：允许 unsigned revision，适合受控开发环境。

健康检查：

- `GET /healthz`：进程存活。
- `GET /readyz`：controller 完成首次 list/reconcile 后就绪。

默认健康监听地址是 `:8081`，可通过 `--health-address` 修改。

## TopologyRevision

最小结构包含 revision、plan 和 workload：

```yaml
apiVersion: topology.keelith.dev/v1alpha1
kind: TopologyRevision
metadata:
  name: orders-r1
spec:
  revision: 1
  plan:
    apiVersion: keelith.dev/topology/v1
    epoch: 1
    placements:
      - primary
    components:
      - id: orders-api
        placement: primary
    dependencies: []
  workloads:
    - placement: primary
      name: orders-api
      image: example.com/orders-api:1.0.0
      replicas: 2
      containerPort: 8080
      expose: true
```

字段约束以 `api/v1alpha1/topologyrevision.crd.yaml` 和 `api/v1alpha1/plan.schema.json` 为准。被拒绝的 revision 会进入 `Rejected` phase，已应用的 last-good 资源保持不变。

## Reconcile 语义

- revision 必须通过 schema、身份、拓扑计划和 reachability 校验。
- signed 模式下，plan 必须通过 Ed25519 verifier。
- 已应用 revision 只能单调前进；相同 revision 不能切换到不同 hash。
- controller 会给受管资源加 owner/finalizer/label，避免接管其他 owner 的资源。
- 删除 `TopologyRevision` 时，finalizer 会先清理受管资源。

## 开发与验证

```bash
cd operator
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

根仓库提供面向 Kubernetes 的检查目标：

```bash
make topology-kubernetes-integration
make topology-operator-integration
```

这些目标依赖本地 Kubernetes 测试环境和脚本配置。
