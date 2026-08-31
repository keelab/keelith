# Keelith

[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelith.svg)](https://pkg.go.dev/github.com/keelab/keelith)
[![License](https://img.shields.io/github/license/keelab/keelith)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](go.mod)

**English | [中文](README_zh.md)**

Keelith is a contract-first Go runtime for production microservices. It gives generated services one consistent way to compose HTTP and gRPC transports, middleware, configuration, discovery, governance, background work, observability, and graceful lifecycle management.

Keelith is intentionally modular: start with the core contracts, add only the integrations your service needs, and keep infrastructure-specific dependencies at the edge.

## Why Keelith

- **One application lifecycle** — deterministic startup, readiness, graceful shutdown, and bounded cleanup for servers, workers, components, and configuration watchers.
- **Transport-neutral contracts** — shared operations, metadata, errors, validation, identity, and placement across HTTP, gRPC, Hertz, and Kitex.
- **Composable governance** — timeout, retry, hedging, circuit breaking, bulkheads, rate limiting, load shedding, admission control, fallback, and outlier detection.
- **Programmable workloads** — durable continuations, projections, topology plans, sagas, jobs, workers, cache invalidation, and idempotency primitives.
- **Observable by default** — structured logging, metrics, tracing, health, diagnostics, and secret-free runtime descriptions.
- **Explicit extension boundaries** — optional adapters live in `contrib`; CloudWeGo profiles live in the separate `github.com/keelab/x` module.

## Quick start

Requirements: Go 1.26 or newer. Generated service bindings are the recommended entry point. The CLI creates a minimal runnable HTTP service:

```bash
go install github.com/keelab/keelith/cmd/keelith@latest
keelith new hello
cd hello
go run .
# in another terminal
curl http://127.0.0.1:8080/ping
```

For a complete generated service, follow the projects in [Keelab examples](https://github.com/keelab/examples) or use the scaffold tools in `internal/scaffold`. The public API is documented on [pkg.go.dev](https://pkg.go.dev/github.com/keelab/keelith).

Install the core module:

```bash
go get github.com/keelab/keelith
go install github.com/keelab/keelith/cmd/protoc-gen-go-keelith@latest
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
