# Keelith

### Build the service once. Keep the runtime contract everywhere.

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelith.svg)](https://pkg.go.dev/github.com/keelab/keelith)
[![License](https://img.shields.io/github/license/keelab/keelith)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](go.mod)

**English | [中文](README_zh.md)**

Keelith is a contract-first Go runtime for production microservices. It gives generated services one consistent way to compose HTTP and gRPC transports, middleware, configuration, discovery, governance, background work, observability, and graceful lifecycle management.

Keelith is intentionally modular: start with the core contracts, add only the integrations your service needs, and keep infrastructure-specific dependencies at the edge.

If you are comparing Keelith with Kratos, go-zero, CloudWeGo, Encore, or Goa, the key distinction is its center of gravity: Keelith is a runtime contract and composition layer for a fleet of services—not another all-in-one application framework or a single-protocol RPC library.

## Why Keelith

- **One application lifecycle** — deterministic startup, readiness, graceful shutdown, and bounded cleanup for servers, workers, components, and configuration watchers.
- **Transport-neutral contracts** — shared operations, metadata, errors, validation, identity, and placement across HTTP, gRPC, Hertz, and Kitex.
- **Composable governance** — timeout, retry, hedging, circuit breaking, bulkheads, rate limiting, load shedding, admission control, fallback, and outlier detection.
- **Programmable workloads** — durable continuations, projections, topology plans, sagas, jobs, workers, cache invalidation, and idempotency primitives.
- **Observable by default** — structured logging, metrics, tracing, health, diagnostics, and secret-free runtime descriptions.
- **Explicit extension boundaries** — optional adapters live in `contrib`; CloudWeGo profiles live in the separate `github.com/keelab/x` module.

## A different center of gravity

Most Go microservice projects are excellent at one part of the problem: HTTP/RPC performance, code generation, API design, or infrastructure automation. Keelith focuses on the seams between those parts, where production systems usually accumulate duplicated policy and lifecycle code.

| If you need… | Keelith’s approach |
| --- | --- |
| HTTP and gRPC in the same service fleet | One transport-neutral contract for operations, metadata, errors, validation, identity, and placement |
| Consistent production behavior | Governance policies and retry budgets composed at the runtime boundary |
| More than request/response handlers | Durable continuations, projections, sagas, jobs, workers, and idempotency primitives |
| Freedom to choose infrastructure | A small core with optional adapters and separate Hertz/Kitex profiles |
| Operations you can reason about | Explicit lifecycle, readiness, diagnostics, and secret-free runtime descriptions |

This makes Keelith a strong fit for platform teams and long-lived service fleets that need shared behavior without forcing every service into the same infrastructure stack.

## Where it fits in the Go ecosystem

Keelith complements rather than replaces the projects that already excel at a particular layer:

- **Kratos** is a natural comparison for cloud-native HTTP/gRPC services and pluggable components.
- **go-zero** is a strong choice when generated API/RPC structure and built-in traffic protection are the priority.
- **CloudWeGo Kitex + Hertz** is a compelling high-performance RPC/HTTP combination; Keelith can provide the shared contract and policy layer through its optional profiles.
- **Goa** leads with design-first API generation; Keelith carries the contract further into lifecycle, governance, and durable work execution.
- **Encore** optimizes for a managed, infrastructure-aware development experience; Keelith keeps deployment and infrastructure choices at explicit boundaries.

Choose Keelith when the hard problem is not starting one service, but keeping many services consistent as protocols, policies, and workloads evolve.

## Quick start

Requirements: Go 1.26 or newer. The CLI creates a minimal runnable HTTP service:

```bash
go install github.com/keelab/keelith-cli/cmd/keelith@latest
keelith new hello
cd hello
keelith dev
# in another terminal
curl http://127.0.0.1:8080/ping
```

This quick start uses the deliberately small `minimal` template. For the
contract-first service path with generated HTTP/gRPC bindings, run
`keelith new hello --template service` instead; its starter endpoint is
`/v1/ping`.

`NAME` is also the default target directory. When `--path` is supplied, it is
the complete target directory and `NAME` remains the project identity used for
the default module and application name, for example:

```bash
keelith new orders --path ./services/orders-api
```

`keelith dev` discovers the project root, refreshes generated contracts and
wiring, and then runs the application. Use `keelith check` for the equivalent
read-only project, toolchain, and generated-file checks. Low-level wiring
commands remain available under `keelith x wiring` for framework maintenance.
Use `keelith dev --watch` to regenerate and reload after source changes; a
failed generation or build keeps the previous process running.

The top-level `keelith generate` command is intentionally limited to projects
with a Keelith declaration or generated contract/component manifests. A plain
Go module is never mutated by this command; use `keelith dev` to run it, or use
the explicit `generate data` and `generate kitex-facade` subcommands when those
standalone generators are what you need.

`keelith add component` writes the component declaration, configuration, and
generated resource assembly. The default template `main.go` still calls the
single `NewApplication` entry point; it automatically assembles declared
components and checks Redis, SQL, Kafka, and other external dependencies during
application startup. Use the generated `NewApplicationWithComponents` only
when supplying a remote configuration source or custom dependencies.

The service and production templates include a pre-generated starter contract,
but do not assume a Buf toolchain. Before changing Proto files or using
`keelith add api`, configure `buf.yaml` and `buf.gen.yaml`, install Buf and the
required plugins, then run `buf dep update` followed by `keelith generate`.

For a complete generated service, follow the projects in [Keelab examples](https://github.com/keelab/examples). The public API is documented on [pkg.go.dev](https://pkg.go.dev/github.com/keelab/keelith); project scaffolding and developer commands live in the standalone [keelith-cli](https://github.com/keelab/keelith-cli) module.

Install the core module:

```bash
go get github.com/keelab/keelith
go install github.com/keelab/keelith-cli/cmd/protoc-gen-go-keelith@latest
```

## Core building blocks

| Area | Packages | Purpose |
| --- | --- | --- |
| Application | `keelith`, `app`, `server`, `health` | Runtime assembly, lifecycle, readiness, and shutdown |
| Service model | `service`, `operation`, `placement`, `metadata`, `errors`, `validation` | Stable contracts shared by every transport |
| Transports | `transport/http`, `transport/grpc`, `transport/sse`, `transport/websocket` | HTTP/1.1, HTTP/2, gRPC, SSE, and WebSocket surfaces |
| Governance | `governance/*`, `policy`, `middleware` | Resilience, traffic control, authorization, and request policy |
| Runtime state | `config`, `secret`, `cache`, `registry`, `coordination` | Reloadable configuration, secret providers, caching, and discovery |
| Work execution | `worker`, `job`, `outbox`, `saga`, `programmable/*` | Background jobs, messaging, sagas, projections, and durable workflows |
| Operations | `observability`, `ops`, `diagnostics` | Logs, metrics, traces, health, and operational endpoints |
| Integrations | [`contrib`](contrib), [`operator`](operator), [`x`](x) | Third-party adapters, Kubernetes control plane, Hertz, and Kitex |

## Transport profiles

The core module provides standard HTTP and gRPC implementations. The optional [`github.com/keelab/x`](x) module adds CloudWeGo integrations:

```bash
go get github.com/keelab/x/transport/hertz
go get github.com/keelab/x/transport/kitex
```

Use `x/transport/hertz` for Hertz HTTP services and `x/transport/kitex` for generated Kitex servers and clients. Keelith owns middleware, metadata, error, retry, and picker policy; avoid enabling a second independent retry budget in the underlying client.

## Repository layout

```text
keelith/
├── app/             lifecycle and dependency-aware runtime
├── transport/       HTTP, gRPC, streaming, auth, and protocol helpers
├── governance/      resilience and traffic policies
├── programmable/    continuations, projections, and topology
├── observability/   logging, metrics, tracing, and diagnostics
├── contrib/         optional infrastructure adapters (separate module)
├── operator/        Kubernetes TopologyRevision controller (separate module)
└── x/               Hertz and Kitex profiles (separate module)
```

## Documentation and examples

- [API reference](https://pkg.go.dev/github.com/keelab/keelith)
- [Keelab examples](https://github.com/keelab/examples)
- [Contrib integrations](contrib/README.md)
- [Kubernetes operator](operator/README.md)
- [CloudWeGo profiles](x/README.md)

## Development

```bash
go test ./...
go vet ./...
```

Modules under `contrib`, `operator`, and `x` are validated independently:

```bash
for dir in contrib operator x; do (cd "$dir" && GOWORK=off go test ./... && GOWORK=off go vet ./...); done
```

Some integration checks require external services or a Kubernetes test cluster; see the module-specific README files before running them.

## Compatibility and security

Keelith follows semantic, contract-oriented APIs within each module. Review `SECURITY.md` for vulnerability reporting. Do not place credentials, raw payloads, or sensitive endpoint data in logs or runtime descriptions; use the `secret`, metadata policy, and redaction packages.

## License

Keelith is released under the [MIT License](LICENSE).
