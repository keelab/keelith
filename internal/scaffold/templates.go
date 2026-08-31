package scaffold

import (
	"fmt"
	"strings"
)

func supportedProjectTemplate(template string) bool {
	switch template {
	case "minimal", "service", "production":
		return true
	default:
		return false
	}
}

func projectFiles(
	template string,
	module string,
	name string,
	frameworkVersion string,
) (map[string][]byte, error) {
	switch template {
	case "minimal":
		return minimalProjectFiles(module, name, frameworkVersion), nil
	case "service":
		return serviceProjectFiles(module, name, frameworkVersion, false)
	case "production":
		return serviceProjectFiles(module, name, frameworkVersion, true)
	default:
		return nil, fmt.Errorf("%w: unsupported template %q", ErrInvalidInput, template)
	}
}

func serviceProjectFiles(
	module string,
	name string,
	frameworkVersion string,
	production bool,
) (map[string][]byte, error) {
	mode := "标准"
	mainBuilder := standardMainSource()
	config := ""
	opsDescription := ""
	configDescription := "需要配置时，通过项目声明启用配置；配置会在 App 启动阶段加载并由 Config Runtime watch。\n\n"
	if production {
		mode = "生产参考"
		mainBuilder = productionMainSource()
		config = "runtime:\n  name: " + name + "\n"
		opsDescription = "生产参考模板另有 loopback Ops：`http://127.0.0.1:9090/health/live` 和 `/debug/build`。\n\n"
		configDescription = "生产参考模板已通过 Keelith CLI wiring 声明启用严格文件配置；配置会在 App 启动阶段加载并由 Config Runtime watch。\n\n"
	}
	wiringSpec, err := NewWiringSpec(module, name, map[bool]string{false: "service", true: "production"}[production])
	if err != nil {
		return nil, fmt.Errorf("render wiring project: %w", err)
	}
	wiringArtifacts, err := BuiltinWiringArtifacts(wiringSpec)
	if err != nil {
		return nil, fmt.Errorf("render wiring artifacts: %w", err)
	}
	wiringProject, err := MarshalWiringSpec(wiringSpec)
	if err != nil {
		return nil, fmt.Errorf("marshal wiring project: %w", err)
	}
	return map[string][]byte{
		".gitignore": []byte(".secrets/\n"),
		"README.md": fmt.Appendf(nil,
			"# %s\n\n"+
				"这是 Keelith 的%s服务模板。\n\n"+
				"## 运行\n\n"+
				"```bash\n"+
				"go run .\n"+
				"```\n\n"+
				"业务 HTTP：`http://127.0.0.1:8080/v1/ping`。\n"+
				"gRPC：`127.0.0.1:8081`。\n\n"+
				"%s"+
				"%s"+
				"## 目录\n\n"+
				"- `api/echo/v1/echo.proto`：协议源文件。\n"+
				"- `gen/echo/v1/echo.keelith.gen.go`：类型化 Binding 和 HTTP/gRPC 适配（完整协议变更可用 protoc 插件重新生成）。\n"+
				"- `internal/echo`：业务 Handler。\n"+
				"- `internal/keelithgen`：由 Keelith CLI 生成的静态 Application 组合根。\n"+
				"- `dependency-graph.json`：由 Keelith CLI 生成的受管依赖图。\n\n"+
				"本模板默认不连接 PostgreSQL、Redis、Kafka、注册中心或远程配置。"+
				"需要外部依赖时，通过现有 `keelith add component` 和 `keelith wiring` 显式接入。\n",
			name,
			mode,
			opsDescription,
			configDescription,
		),
		"go.mod":                   renderProjectGoMod(module, frameworkVersion, true),
		"go.sum":                   renderProjectGoSum(frameworkVersion),
		"main.go":                  fmt.Appendf(nil, mainBuilder, module),
		"internal/echo/service.go": fmt.Appendf(nil, echoServiceSource, module),
		"keelith.project.json":     wiringProject,
		"dependency-graph.json":    wiringArtifacts.Manifest,
		"internal/keelithgen/application.keelith.gen.go": wiringArtifacts.Application,
		"api/echo/v1/echo.keelith.manifest.json":         fmt.Appendf(nil, echoContractManifestSource, module, module),
		"gen/echo/v1/echo.keelith.gen.go":                []byte(generatedEchoSource),
		"api/echo/v1/echo.proto":                         fmt.Appendf(nil, echoProtoSource, module),
		"configs/config.yaml":                            []byte(config),
	}, nil
}

func renderProjectGoMod(module, frameworkVersion string, transport bool) []byte {
	var builder strings.Builder
	fmt.Fprintf(&builder, "module %s\n\ngo %s\n\nrequire (\n", module, defaultGoVersion)
	fmt.Fprintf(&builder, "\tgithub.com/keelab/keelith %s\n", frameworkVersion)
	if transport {
		builder.WriteString("\tgoogle.golang.org/grpc v1.83.1\n")
		builder.WriteString("\tgoogle.golang.org/protobuf v1.36.12\n")
	}
	builder.WriteString(")\n")
	if frameworkVersion == defaultFrameworkVersion {
		builder.WriteString("\nrequire (\n")
		for _, dependency := range defaultProjectDependencies {
			if transport && (dependency.module == "google.golang.org/grpc" ||
				dependency.module == "google.golang.org/protobuf") {
				continue
			}
			fmt.Fprintf(&builder, "\t%s %s // indirect\n", dependency.module, dependency.version)
		}
		builder.WriteString(")\n")
	}
	return []byte(builder.String())
}

type projectDependency struct {
	module  string
	version string
}

// defaultProjectDependencies keep the generated default module tidy for the
// facade's transitive imports. This makes `go run .` work immediately after
// `keelith new`; custom framework versions still intentionally require a
// normal `go mod tidy` because their dependency graph may differ.
var defaultProjectDependencies = []projectDependency{
	{module: "github.com/cespare/xxhash/v2", version: "v2.3.0"},
	{module: "github.com/go-logr/logr", version: "v1.4.4"},
	{module: "github.com/go-logr/stdr", version: "v1.2.2"},
	{module: "github.com/google/uuid", version: "v1.6.0"},
	{module: "go.opentelemetry.io/auto/sdk", version: "v1.2.1"},
	{module: "go.opentelemetry.io/otel", version: "v1.45.0"},
	{module: "go.opentelemetry.io/otel/metric", version: "v1.45.0"},
	{module: "go.opentelemetry.io/otel/sdk", version: "v1.45.0"},
	{module: "go.opentelemetry.io/otel/sdk/metric", version: "v1.45.0"},
	{module: "go.opentelemetry.io/otel/trace", version: "v1.45.0"},
	{module: "golang.org/x/net", version: "v0.58.0"},
	{module: "golang.org/x/sys", version: "v0.47.0"},
	{module: "golang.org/x/text", version: "v0.41.0"},
	{
		module:  "google.golang.org/genproto/googleapis/rpc",
		version: "v0.0.0-20260819154853-08b0e4226688",
	},
	{module: "google.golang.org/grpc", version: "v1.83.1"},
	{module: "google.golang.org/protobuf", version: "v1.36.12"},
	{module: "gopkg.in/yaml.v3", version: "v3.0.1"},
}

func renderProjectGoSum(frameworkVersion string) []byte {
	if frameworkVersion == defaultFrameworkVersion {
		return []byte(defaultGoSum)
	}
	lines := strings.Split(strings.TrimSuffix(defaultGoSum, "\n"), "\n")
	filtered := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(line, "github.com/keelab/keelith ") {
			continue
		}
		filtered = append(filtered, line)
	}
	return []byte(strings.Join(filtered, "\n") + "\n")
}

// defaultGoSum keeps a freshly generated module runnable with Go's default
// read-only module mode. Update it together with the framework release and
// the self-module checksums when publishing a new version.
const defaultGoSum = `github.com/cespare/xxhash/v2 v2.3.0 h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=
github.com/cespare/xxhash/v2 v2.3.0/go.mod h1:VGX0DQ3Q6kWi7AoAeZDth3/j3BFtOZR5XLFGgcrjCOs=
buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260709200747-435963d16310.1 h1:6nlcxMOui23ZRVAfJM451duu79P1npA5JRdZqMilrrQ=
buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260709200747-435963d16310.1/go.mod h1:TCt1lluMFnctISJXvkIQ4x3ABrPuUKCWKyjKdkJNBpw=
github.com/jhump/protoreflect v1.18.0 h1:TOz0MSR/0JOZ5kECB/0ufGnC2jdsgZ123Rd/k4Z5/2w=
github.com/jhump/protoreflect v1.18.0/go.mod h1:ezWcltJIVF4zYdIFM+D/sHV4Oh5LNU08ORzCGfwvTz8=
github.com/robfig/cron/v3 v3.0.1 h1:WdRxkvbJztn8LMz/QEvLN5sBU+xKpSqwwUO1Pjr4qDs=
github.com/robfig/cron/v3 v3.0.1/go.mod h1:eQICP3HwyT7UooqI/z+Ov+PtYAWygg1TEWWzGIFLtro=
github.com/spf13/cobra v1.10.2 h1:DMTTonx5m65Ic0GOoRY2c16WCbHxOOw6xxezuLaBpcU=
github.com/spf13/cobra v1.10.2/go.mod h1:7C1pvHqHw5A4vrJfjNwvOdzYu0Gml16OCs2GRiTUUS4=
github.com/pelletier/go-toml/v2 v2.2.4 h1:mye9XuhQ6gvn5h28+VilKrrPoQVanw5PMw/TB0t5Ec4=
github.com/pelletier/go-toml/v2 v2.2.4/go.mod h1:2gIqNv+qfxSVS7cM2xJQKtLSTLUE9V8t9Stt+h56mCY=
github.com/davecgh/go-spew v1.1.1 h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=
github.com/davecgh/go-spew v1.1.1/go.mod h1:J7Y8YcW2NihsgmVo/mv3lAwl/skON4iLHjSsI+c5H38=
github.com/go-logr/logr v1.2.2/go.mod h1:jdQByPbusPIv2/zmleS9BjJVeZ6kBagPoEUsqbVz/1A=
github.com/go-logr/logr v1.4.4 h1:tG4xh9yMsRCAiodLVTxyrkzSZ9+o0L1Kg/+cPVcbP/8=
github.com/go-logr/logr v1.4.4/go.mod h1:9T104GzyrTigFIr8wt5mBrctHMim0Nb2HLGrmQ40KvY=
github.com/go-logr/stdr v1.2.2 h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=
github.com/go-logr/stdr v1.2.2/go.mod h1:mMo/vtBO5dYbehREoey6XUKy/eSumjCCveDpRre4VKE=
github.com/golang/protobuf v1.5.4 h1:i7eJL8qZTpSEXOPTxNKhASYpMn+8e5Q6AdndVa1dWek=
github.com/golang/protobuf v1.5.4/go.mod h1:lnTiLA8Wa4RWRcIUkrtSVa5nRhsEGBg48fD6rSs7xps=
github.com/google/go-cmp v0.7.0 h1:wk8382ETsv4JYUZwIsn6YpYiWiBsYLSJiTsyBybVuN8=
github.com/google/go-cmp v0.7.0/go.mod h1:pXiqmnSA92OHEEa9HXL2W4E7lf9JzCmGVUdgjX3N/iU=
github.com/google/uuid v1.6.0 h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=
github.com/google/uuid v1.6.0/go.mod h1:TIyPZe4MgqvfeYDBFedMoGGpEw/LqOeaOT+nhxU+yHo=
github.com/kr/pretty v0.3.1 h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=
github.com/kr/pretty v0.3.1/go.mod h1:hoEshYVHaxMs3cyo3Yncou5ZscifuDolrwPKZanG3xk=
github.com/kr/text v0.2.0 h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=
github.com/kr/text v0.2.0/go.mod h1:eLer722TekiGuMkidMxC/pM04lWEeraHUUmBw8l2grE=
github.com/pmezard/go-difflib v1.0.0 h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=
github.com/pmezard/go-difflib v1.0.0/go.mod h1:iKH77koFhYxTK1pcRnkKkqfTogsbg7gZNVY4sRDYZ/4=
github.com/rogpeppe/go-internal v1.14.1 h1:UQB4HGPB6osV0SQTLymcB4TgvyWu6ZyliaW0tI/otEQ=
github.com/rogpeppe/go-internal v1.14.1/go.mod h1:MaRKkUm5W0goXpeCfT7UZI6fk/L7L7so1lCWt35ZSgc=
github.com/stretchr/testify v1.11.1 h1:7s2iGBzp5EwR7/aIZr8ao5+dra3wiQyKjjFuvgVKu7U=
github.com/stretchr/testify v1.11.1/go.mod h1:wZwfW3scLgRK+23gO65QZefKpKQRnfz6sD981Nm4B6U=
gopkg.in/check.v1 v0.0.0-20161208181325-20d25e280405/go.mod h1:Co6ibVJAznAaIkqp8huTwlJQCZ016jof/cbN4VW5Yz0=
go.opentelemetry.io/auto/sdk v1.2.1 h1:jXsnJ4Lmnqd11kwkBV2LgLoFMZKizbCi5fNZ/ipaZ64=
go.opentelemetry.io/auto/sdk v1.2.1/go.mod h1:KRTj+aOaElaLi+wW1kO/DZRXwkF4C5xPbEe3ZiIhN7Y=
go.opentelemetry.io/otel v1.45.0 h1:pdrWmLHofpubmArBv1LgFSv1Z0Ie/ppdZzu+kUN5EeU=
go.opentelemetry.io/otel v1.45.0/go.mod h1:XZxIqPapzEYnhNSScF5DIqXhm/rYi0FzCe2XddAwZfQ=
go.opentelemetry.io/otel/metric v1.45.0 h1:7Eg1uH7CJ5cXv9is6tnBe1FI6rj1nwUdbFypRm3br/M=
go.opentelemetry.io/otel/metric v1.45.0/go.mod h1:HAPbm1nd3p1PmFH7v2dR+6BjXxw+Lq4a2+pndMAm08s=
go.opentelemetry.io/otel/metric/x v0.67.0 h1:PcicCNZFkZ4bXfSooXdo3WN7RBOVOtjVdo1wD358Uns=
go.opentelemetry.io/otel/metric/x v0.67.0/go.mod h1:FBjCWZe6wgcqxcMtjdGiClDKXb2YxxXii0CXftE4QtI=
go.opentelemetry.io/otel/sdk v1.45.0 h1:4VVSMgQ83dUgW2aoX5f6JgLvHwIvzcuLnF9lUdCSpCw=
go.opentelemetry.io/otel/sdk v1.45.0/go.mod h1:Sr40LgXV7DsKMMJMKOhUWOgMWTfAaqvm2kF0g7ilwuA=
go.opentelemetry.io/otel/sdk/metric v1.45.0 h1:oVFszMfyj1Am6s24Vtc7wBb8BKLcwepJjNEYILuiE3o=
go.opentelemetry.io/otel/sdk/metric v1.45.0/go.mod h1:vUWUxDZvu1WVRj8JA8S0AdhsPrZoDpA2DdZauIh4mDA=
go.opentelemetry.io/otel/trace v1.45.0 h1:l/mP6Uv7oNO7/TblbhpbgMidxhq1uO/rPsikOyVhxag=
go.opentelemetry.io/otel/trace v1.45.0/go.mod h1:qoJJA2xNMnxRrdISU/kLtfUH2wNeQbiv+jhs/CxI8bc=
golang.org/x/sync v0.22.0 h1:SZjpbeLmrCk4xhRSZFNZW5gFUeCeFgjekvI/+gfScek=
golang.org/x/sync v0.22.0/go.mod h1:9xrNwdLfx4jkKbNva9FpL6vEN7evnE43NNNJQ2LF3+0=
golang.org/x/net v0.58.0 h1:ynWG7rqYi4ccpTEuPZ2QGWHktVEM9DMCj9yzDE0Q7To=
golang.org/x/net v0.58.0/go.mod h1:YwCddHnFlT7eLQqVprV19OnhLGtc5xOKgE0RyqgfWAU=
golang.org/x/sys v0.47.0 h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs=
golang.org/x/sys v0.47.0/go.mod h1:4GL1E5IUh+htKOUEOaiffhrAeqysfVGipDYzABqnCmw=
golang.org/x/text v0.41.0 h1:vz/seA0lnX87Othu2f/0L24RcgrXD9/YFTSuGjj3rH8=
golang.org/x/text v0.41.0/go.mod h1:jvf1O8ajNzZqhSrQBPbutR/EB83Cc0CFrezNQIwbb5M=
google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 h1:cYNAzI2sUwhmCcoj9TxvihSrqsxt6uIkj3rDRhSDmW4=
google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688/go.mod h1:DjtHYE8FKJLivXcBEjGwndXfIC23G0VpXiXKqG179uA=
google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688 h1:ax2KzoSRIZU/M0cIxri3pKxy99vniH1PVxWC6si/eZI=
google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688/go.mod h1:1RJ9BQGyNdZwkGc1eTqkErfRZ6RJyYPHZo73BZ1vQqI=
google.golang.org/grpc v1.83.1 h1:HIO0+BEtBP6soyqvqC8sNUjZ7bTs+0hFQuFF+RAy++Y=
google.golang.org/grpc v1.83.1/go.mod h1:kDyl6SKsiHKt0uylY5gtn5cEjkrIOhQOGDgIc4JGwzQ=
google.golang.org/protobuf v1.36.12 h1:pJOKDDOyeXErUroCihFAd5LQuwXBSpVnKGrj5o/fwxc=
google.golang.org/protobuf v1.36.12/go.mod h1:HTf+CrKn2C3g5S8VImy6tdcUvCska2kB7j23XfzDpco=
gopkg.in/yaml.v3 v3.0.1 h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=
gopkg.in/yaml.v3 v3.0.1/go.mod h1:K4uyk7z7BCEPqu6E+C64Yfv1cQ7kz7rIZviUmN+EgEM=
github.com/keelab/keelith v0.0.3 h1:PQRdZPU0o4L6wzWZnVdPbzXD5kG6Q0EU8Lbp1QSa1gA=
github.com/keelab/keelith v0.0.3/go.mod h1:XWRAbQKEyw0BifgXvg4YRDJfWUyei9O8/mzwJPstRN8=
`

func standardMainSource() string {
	return `package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"%s/internal/keelithgen"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := keelithgen.NewApplication(ctx)
	if err != nil {
		return err
	}
	if err := application.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
`
}

func productionMainSource() string {
	return standardMainSource()
}

const echoContractManifestSource = `{
  "schemaVersion": "v3",
  "generatorProtocol": "v1",
  "source": "echo/v1/echo.proto",
  "package": "echo.v1",
  "goImportPath": "%s/gen/echo/v1",
  "goPackage": "echov1",
  "services": [
    {
      "name": "echo.v1.EchoService",
      "goName": "EchoService",
      "methods": [
        {
          "name": "Ping",
          "operation": "/echo.v1.EchoService/Ping",
          "kind": "unary",
          "input": "google.protobuf.Empty",
          "output": "google.protobuf.Empty",
          "http": {
            "method": "GET",
            "path": "/v1/ping"
          }
        }
      ]
    }
  ],
  "listeners": [
    {
      "transport": "grpc",
      "service": "echo.v1.EchoService"
    },
    {
      "transport": "http",
      "service": "echo.v1.EchoService",
      "routes": [
        {
          "method": "GET",
          "path": "/v1/ping",
          "operation": "/echo.v1.EchoService/Ping"
        }
      ]
    }
  ],
  "dependencies": [
    {
      "kind": "generated-adapter",
      "transport": "grpc",
      "service": "echo.v1.EchoService",
      "goImportPath": "%s/gen/echo/v1",
      "goPackage": "echov1",
      "goName": "EchoService",
      "reason": "generated-http-gateway",
      "operations": [
        "/echo.v1.EchoService/Ping"
      ]
    }
  ]
}
`

const echoServiceSource = `package echo

import (
	"context"

	echov1 "%s/gen/echo/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service is the small business implementation shared by HTTP and gRPC.
type Service struct{}

// New constructs the example service without external dependencies.
func New() *Service { return &Service{} }

// Ping returns an empty successful response.
func (*Service) Ping(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

var _ echov1.EchoServiceKeelithServer = (*Service)(nil)
`

const echoProtoSource = `syntax = "proto3";

package echo.v1;

import "google/protobuf/empty.proto";

option go_package = "%s/gen/echo/v1;echov1";

service EchoService {
  rpc Ping(google.protobuf.Empty) returns (google.protobuf.Empty);
}
`

const generatedEchoSource = `// Code generated by keelith new. DO NOT EDIT.

package echov1

import (
	"context"
	"fmt"
	"net/http"

	keelitherrors "github.com/keelab/keelith/errors"
	keelithidempotency "github.com/keelab/keelith/governance/idempotency"
	keelithservice "github.com/keelab/keelith/service"
	keelithhttp "github.com/keelab/keelith/transport/http"
	"github.com/keelab/keelith/operation"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const EchoServiceKeelithGeneratorProtocol = "v1"

// EchoServiceKeelithServer is the transport-neutral implementation contract.
type EchoServiceKeelithServer interface {
	Ping(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

// UnimplementedEchoServiceKeelithServer provides forward-compatible defaults
// for implementations generated by the Keelith generator.
type UnimplementedEchoServiceKeelithServer struct{}

func (*UnimplementedEchoServiceKeelithServer) Ping(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, keelitherrors.New(501, "UNIMPLEMENTED", "echo.v1.EchoService.Ping is not implemented")
}

var _ EchoServiceKeelithServer = (*UnimplementedEchoServiceKeelithServer)(nil)

// EchoServiceKeelithIdempotencyRegistrations returns generated unary rules.
// The starter contract declares no idempotency annotations yet.
func EchoServiceKeelithIdempotencyRegistrations() ([]keelithidempotency.Registration, error) {
	return nil, nil
}

// RegisterEchoServiceHTTP binds the example method to Keelith HTTP.
func RegisterEchoServiceHTTP(router *keelithhttp.Router, implementation EchoServiceKeelithServer) error {
	if router == nil || implementation == nil {
		return fmt.Errorf("keelith generated HTTP: router or service is nil")
	}
	target, err := operation.New("http", "echo.v1.EchoService", "Ping", operation.KindUnary)
	if err != nil {
		return fmt.Errorf("build Ping operation: %w", err)
	}
	return router.Handle(
		http.MethodGet,
		"/v1/ping",
		target,
		keelithhttp.NoBody,
		func(ctx context.Context, _ any) (any, error) {
			return implementation.Ping(ctx, &emptypb.Empty{})
		},
		keelithhttp.EncodeProto,
	)
}

func _EchoService_Ping_Handler(
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EchoServiceKeelithServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/echo.v1.EchoService/Ping",
	}
	handler := func(ctx context.Context, request interface{}) (interface{}, error) {
		return srv.(EchoServiceKeelithServer).Ping(ctx, request.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// EchoServiceKeelithGRPCServiceDesc is the generated gRPC registration.
var EchoServiceKeelithGRPCServiceDesc = grpc.ServiceDesc{
	ServiceName: "echo.v1.EchoService",
	HandlerType: (*EchoServiceKeelithServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler:    _EchoService_Ping_Handler,
		},
	},
	Metadata: "api/echo/v1/echo.proto",
}

// RegisterEchoServiceGRPC registers the generated adapter with grpc-go.
func RegisterEchoServiceGRPC(registrar grpc.ServiceRegistrar, implementation EchoServiceKeelithServer) {
	registrar.RegisterService(&EchoServiceKeelithGRPCServiceDesc, implementation)
}

// BindEchoService creates one immutable HTTP/gRPC service Binding.
func BindEchoService(implementation EchoServiceKeelithServer, options ...keelithservice.BindingOption) keelithservice.Binding {
	return keelithservice.NewBinding(keelithservice.BindingSpec{
		Name:           "echo.v1.EchoService",
		Implementation: implementation,
		RegisterHTTP: func(router *keelithhttp.Router) error {
			return RegisterEchoServiceHTTP(router, implementation)
		},
		RegisterGRPC: func(registrar grpc.ServiceRegistrar) error {
			RegisterEchoServiceGRPC(registrar, implementation)
			return nil
		},
	}, options...)
}
`
