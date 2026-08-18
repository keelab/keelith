# 贡献指南

感谢你参与 Keelith。提交贡献即表示你同意遵守本项目的行为准则，并同意你的贡献按 Apache License 2.0 授权。

## 开始之前

- 对缺陷修复，可以直接提交 Pull Request，并关联对应 Issue。
- 对新增公共 API、破坏性变更或较大重构，请先创建 Issue 讨论方案。
- 安全漏洞不要通过公开 Issue 报告，请遵循 [安全策略](SECURITY.md)。

## 本地开发

需要安装 `go.mod` 指定版本的 Go，以及与 `.golangci.yml` v2 配套的 `golangci-lint`。

```bash
go mod download
go test -race -shuffle=on ./...
golangci-lint run ./...
govulncheck ./...
```

提交前请运行 `gofmt` 或 `golangci-lint fmt`，并确保新增行为具有相应测试。

## 提交规范

提交信息建议遵循 Conventional Commits：

```text
feat: 增加新的 CLI 子命令
fix: 修复配置覆盖顺序
docs: 补充公共 API 示例
```

常用类型包括 `feat`、`fix`、`docs`、`test`、`refactor`、`build`、`ci` 和 `chore`。一个提交应聚焦一个逻辑变更，避免混入无关格式化或重构。

## Pull Request

Pull Request 应：

- 清楚说明问题、解决方式和兼容性影响；
- 关联对应 Issue；
- 为新增或修改行为补充测试和文档；
- 保持公共 API 向后兼容，破坏性变更必须明确标注并说明迁移方式；
- 通过所有必需的 CI 检查。

维护者可能要求拆分过大的变更。审查通过不代表立即合并，最终合并还会考虑版本计划和项目方向。
