# 服务契约与 HTTP/gRPC 传输

在分布式微服务架构中，接口契约（API Contract）是不同团队、服务之间最重要的信任桥梁。Keelith 采用**契约优先（Contract-First）**的设计思想，统一以 **Protobuf** 作为业务接口与数据交换的唯一事实来源，并通过官方插件直接生成强类型的 HTTP 与 gRPC 传输绑定。

---

## 1. Protobuf 契约规范与定义

在 `api/catalog/v1/catalog.proto` 中定义服务接口：

```protobuf
syntax = "proto3";

package catalog.v1;

option go_package = "example.com/catalog/api/catalog/v1;catalogv1";

// 目录服务契约
service CatalogService {
  // 获取单个商品详情 (同时映射 HTTP GET 与 gRPC Unary)
  rpc GetItem (GetItemRequest) returns (GetItemResponse);

  // 创建新商品 (同时映射 HTTP POST 与 gRPC Unary)
  rpc CreateItem (CreateItemRequest) returns (CreateItemResponse);
}

message GetItemRequest {
  string id = 1;
}

message GetItemResponse {
  string id = 1;
  string title = 2;
  int64 price_cents = 3;
}

message CreateItemRequest {
  string title = 1;
  int64 price_cents = 2;
}

message CreateItemResponse {
  string id = 1;
}
```

---

## 2. 编译与代码生成

我们推荐使用现代化的 [Buf CLI](https://buf.build) 进行插件编排。在项目根目录配置 `buf.gen.yaml`：

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go:v1.34.2
    out: .
    opt: paths=source_relative
  - remote: buf.build/grpc/go:v1.5.1
    out: .
    opt: paths=source_relative
  # Keelith 官方契约绑定生成插件
  - local: protoc-gen-go-keelith
    out: .
    opt: paths=source_relative
```

在终端执行生成命令：
```bash
keelith generate
```
生成器会产出 3 个文件：
1. `catalog.pb.go`：Protobuf 结构体与二进制序列化代码。
2. `catalog_grpc.pb.go`：原生 gRPC 客户端桩与服务端接口。
3. `catalog.keelith.pb.go`：包含 Keelith 专用的 `service.Binding` 构造器与双协议 HTTP/gRPC 路由注册适配器。

---

## 3. 深入理解 `service.Binding`

生成的 `catalog.keelith.pb.go` 中核心产物是 `service.Binding`。它负责封装服务元数据与注册函数：

```go
// 由 protoc-gen-go-keelith 自动生成的核心绑定代码示意
func NewCatalogServiceBinding(server CatalogServiceKeelithServer) service.Binding {
    return service.NewBinding(service.BindingSpec{
        Name:           "catalog.v1.CatalogService",
        Implementation: server,
        RegisterHTTP: func(router *http.Router) error {
            return RegisterCatalogServiceHTTP(router, server)
        },
        RegisterGRPC: func(grpcServer *grpc.Server) error {
            return RegisterCatalogServiceGRPC(grpcServer, server)
        },
    })
}
```

### 3.1 严格的静态验证 (`Binding.Validate`)
Keelith 在应用装配阶段会对每一个 `service.Binding` 执行防御性检查：
- **服务名非空**：服务名必须具有全局唯一性（如 `catalog.v1.CatalogService`），严禁为空。
- **实现对象有效**：`Implementation` 不能为 `nil`。
- **传输注册函数至少存在一个**：一个契约绑定至少要支持 HTTP 或 gRPC 中的一种；如果两者皆为空，则拒绝通过。
- **中间件类型安全**：挂载在绑定上的中间件必须与目标协议匹配。

---

## 4. 组装服务：`WithServices` 与 `WithProfile`

### 4.1 常用方式：`WithServices`
在绝大多数业务场景下，直接在 `cmd/main.go` 中传入一个或多个契约绑定即可：

```go
package main

import (
	"context"
	"github.com/keelab/keelith"
	catalogv1 "example.com/catalog/api/catalog/v1"
	"example.com/catalog/internal/service"
)

func main() {
	// 1. 实例化业务逻辑服务
	catalogImpl := service.NewCatalogService()

	// 2. 构造契约绑定
	catalogBinding := catalogv1.NewCatalogServiceBinding(catalogImpl)

	// 3. 启动应用
	app, err := keelith.New(
		keelith.WithName("catalog-service"),
		keelith.WithHTTP("127.0.0.1:8080"),
		keelith.WithGRPC("127.0.0.1:9090"),
		keelith.WithServices(catalogBinding),
	)
	if err != nil {
		panic(err)
	}
	_ = app.Run(context.Background())
}
```

### 4.2 平台工程进阶：`WithProfile`
当单体应用拆分、微服务聚合或平台网关需要承载大量子服务，并需要对服务合并顺序、多端口隔离（Surface）做确定性拓扑管理时，可以使用 `service.Profile`：

```go
// 显式构建并校验服务拓扑 Profile
profile, err := service.NewProfileFromBindings(
    "ecommerce-catalog",
    catalogBinding,
    inventoryBinding,
    pricingBinding,
)
if err != nil {
    return fmt.Errorf("build service profile: %w", err)
}

// 注册到应用中
app, err := keelith.New(
    keelith.WithName("ecommerce-core"),
    keelith.WithHTTP("127.0.0.1:8080"),
    keelith.WithGRPC("127.0.0.1:9090"),
    keelith.WithProfile(profile),
)
```

> [!IMPORTANT]
> **互斥约束**：`WithServices` 和 `WithProfile` 是互相排斥的配置。如果你已经组装好了显式的 `Profile`，请直接使用 `WithProfile`；不要在同一个应用里同时调用两者。

---

## 5. 契约优先的最佳实践

1. **先写 Proto，再写实现**：前后端、客户端及不同服务团队之间，以 Git 仓库中的 Proto 文件完成评审与联调对齐。
2. **入参严格校验**：结合 `protoc-gen-validate` (PGV) 或 `buf.validate` 定义字段规则（如长度、正整数、邮箱格式），Keelith 会在中间件层自动执行参数拦截校验，杜绝非法请求进入业务层。
3. **版本演进兼容性**：遵循语义化版本规范（如 `v1`、`v2`），字段废弃时使用 `reserved` 关键字，保持二进制向前向后兼容。
