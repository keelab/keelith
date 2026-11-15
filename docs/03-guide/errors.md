# 传输无关的统一错误模型

在微服务系统中，跨进程错误处理常常面临诸多痛点：
- HTTP 使用数字状态码（如 400、404、500），而 gRPC 使用专属枚举（如 `codes.InvalidArgument`、`codes.NotFound`）。
- 服务 A 将底层数据库的 `sql.ErrNoRows` 直接原样返回，导致上游无法确定到底是哪个业务实体不存在，甚至泄露了内网表结构信息。
- 如果简单使用标准库 `errors.New("user not found")`，上游判断错误只能做脆弱的字符串匹配；一旦文案稍作修改，原有的 `if` 判断逻辑就会全部失效。

Keelith 在 `github.com/keelab/keelith/errors` 中定义了**传输中立（Transport-Neutral）**的统一错误规范。

---

## 1. 核心模型与结构设计

Keelith 的 `*errors.Error` 结构体具备清晰的双层设计：

```go
type Error struct {
    code     int32             // 机器码（对应 HTTP 状态码或 gRPC Code）
    reason   string            // 机器可读的唯一原因（如 USER_NOT_FOUND）
    message  string            // 面向人类的描述文案（可国际化、可根据场景微调）
    metadata map[string]string // 结构化上下文字段（如 item_id, limit）
    cause    error             // 底层原因（私有，不直接暴露给外部客户端）
}
```

- **机器标识（Machine Identity）**：由 `Code` 与 `Reason` 共同组成。
- **人读文案（Human Message）**：仅用于前端展示或排查日志，它的改动不会影响程序判定。
- **信息保护（Cause Hiding）**：底层的原始实现错误保存在私有的 `cause` 中，只有在代码内部通过 `errors.Unwrap(err)` 或服务端日志才会看到，对外输出时彻底隐藏，保护内部系统细节。

---

## 2. 构造与使用示例

### 2.1 定义标准业务错误
推荐在各个服务包或契约包中预先声明哨兵错误（Sentinel Errors）：

```go
package orders

import (
	"github.com/keelab/keelith/errors"
)

var (
	// 定义标准的订单不存在错误
	ErrOrderNotFound = errors.New(404, "ORDER_NOT_FOUND", "order does not exist")

	// 定义库存不足错误
	ErrInsufficientStock = errors.New(400, "INSUFFICIENT_STOCK", "inventory is out of stock")
)
```

### 2.2 在业务中包装与附加上下文
在实际业务执行中，可以使用 `Wrap` 将底层数据库错误包裹起来，并附带动态元数据：

```go
func (s *OrderService) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	order, err := s.repo.FindByID(ctx, orderID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 包装底层 sql 错误，并附加元数据
			return nil, errors.Wrap(
				err,
				404,
				"ORDER_NOT_FOUND",
				"target order was not found in catalog",
				errors.WithMetadata(map[string]string{
					"order_id": orderID,
				}),
			)
		}
		// 未预期的内部错误
		return nil, errors.Wrap(err, 500, "INTERNAL_DATABASE_ERROR", "database read failure")
	}
	return order, nil
}
```

---

## 3. 精确匹配：`errors.Is` 语义

Keelith 的 `Error` 实现了标准库的 `Is(target error) bool` 方法：

```go
err := s.GetOrder(ctx, "ord-123")

// 即使返回的 err 中文案被定制、或者附加了不同的 metadata，
// 只要 Code 和 Reason 一致，errors.Is 即可稳定判定为 true！
if errors.Is(err, orders.ErrOrderNotFound) {
    // 命中预期业务分支：重定向到创建页面或提示用户
}
```

> [!TIP]
> 这种匹配机制解耦了业务判定逻辑与文案展示。产品团队即便频繁更改报错提示语，后端的错误处理分支逻辑也永远不会被破坏。

---

## 4. 跨协议传输映射与响应报文

当一个 `*errors.Error` 被传递给传输层时，Keelith 自动执行双向协议适配：

### 4.1 HTTP 响应映射
- **HTTP 状态码**：直接采用 `Error.Code`（如 400、404、500）。
- **HTTP Response Body**（自动序列化为标准 JSON）：
```json
{
  "code": 404,
  "reason": "ORDER_NOT_FOUND",
  "message": "target order was not found in catalog",
  "metadata": {
    "order_id": "ord-123"
  }
}
```

### 4.2 gRPC 响应映射
- 将 `code` 自动映射为等效的标准 gRPC 状态码（如 `404` 映射为 `codes.NotFound`，`400` 映射为 `codes.InvalidArgument`）。
- 将 `reason` 与 `metadata` 封装到 gRPC 的 `ErrorInfo` 详情对象中。
- 客户端的 gRPC Client 拦截器收到后，会将其无损反序列化回本地的 `*errors.Error`，使得下游代码能够继续无缝调用 `errors.Is(err, ErrOrderNotFound)`。
