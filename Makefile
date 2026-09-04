GO ?= go
NPM ?= npm
PYTHON ?= python3
GOLANGCI_LINT_VERSION ?= v2.13.1
GOVULNCHECK_VERSION ?= v1.6.0
BUF_VERSION ?= v1.69.0
PROMETHEUS_VERSION ?= v3.5.0
FUZZ_TIME ?= 1s
SOAK_DURATION ?= 24h
CHAOS_ROUNDS ?= 20
PROGRAMMABLE_REPORT_DIR ?= $(CURDIR)/artifacts/programmable-runtime
MODULE_DIRS ?= .
CONTRIB_DIR ?= ../contrib
EXAMPLES_DIR ?= ../examples/programmable-commerce

define run_in_modules
	@set -eu; \
	for module in $(MODULE_DIRS); do \
		echo "==> $$module: $(1)"; \
		(cd "$$module" && GOWORK=off $(GO) $(1)); \
	done
endef

.PHONY: verify fmt-check vet test race lint vuln conformance fuzz-smoke \
	integration generated-check benchmark-check compatibility-check \
	compatibility-update quality programmable-runtime-race \
	programmable-runtime-release-check programmable-runtime-v2-release-check \
	programmable-runtime-soak-smoke programmable-runtime-soak-full \
	programmable-runtime-chaos-smoke programmable-runtime-chaos \
	programmable-runtime-upgrade-check sdk-check \
	projection-storage-integration \
	programmable-observability-check \
	topology-kubernetes-integration topology-operator-integration

verify: fmt-check vet test race lint vuln

quality: conformance fuzz-smoke generated-check benchmark-check compatibility-check

fmt-check:
	@test -z "$$(gofmt -l .)" || { \
		echo "gofmt is required for:"; \
		gofmt -l .; \
		exit 1; \
	}

vet:
	$(call run_in_modules,vet ./...)

test:
	$(call run_in_modules,test ./...)

race:
	$(call run_in_modules,test -race ./...)

programmable-runtime-race:
	$(GO) test -race \
		./programmable/continuation/... \
		./programmable/component/... \
		./programmable/topology/... \
		./programmable/projection/...

lint:
	$(call run_in_modules,run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...)

vuln:
	$(call run_in_modules,run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...)

conformance:
	GOWORK=off $(GO) test \
		./app \
		./config \
		./errors \
		./registry/memory \
		./selector \
		./transport/http \
		./transport/grpc \
		./worker \
		-count=1

fuzz-smoke:
	@for target in ErrorFrame Metadata ConfigMerge HeaderCodec IDLGeneration; do \
		$(GO) test ./test/fuzz -run '^$$' -fuzz "^Fuzz$${target}$$" -fuzztime $(FUZZ_TIME) || exit 1; \
	done

integration:
	docker compose -f test/integration/compose.yaml up -d --wait
	@trap 'docker compose -f test/integration/compose.yaml down -v' EXIT; \
		(cd "$(CONTRIB_DIR)" && \
			KEELITH_ETCD_ENDPOINTS=http://127.0.0.1:12379 \
			GOWORK=off $(GO) test -tags=integration ./... -count=1); \
		(cd "$(EXAMPLES_DIR)" && \
			GOWORK=off $(GO) test -tags=integration ./... -count=1)

projection-storage-integration:
	docker compose -p keelith-projection-storage \
		-f test/integration/compose.yaml up -d --wait mysql
	@trap 'docker compose -p keelith-projection-storage -f test/integration/compose.yaml down -v' EXIT; \
		cd "$(CONTRIB_DIR)" && \
		KEELITH_MYSQL_DSN='root:keelith@tcp(127.0.0.1:13306)/keelith?parseTime=true&loc=UTC&multiStatements=true' \
		GOWORK=off $(GO) test -race -tags=integration \
			./data/sql/projection/mysql -count=1

programmable-observability-check:
	GOWORK=off $(GO) test -race -tags=integration \
		./observability/programmable ./ops -count=1
	cd "$(CONTRIB_DIR)" && GOWORK=off $(GO) test -race -tags=integration \
		./observability/programmable -count=1
	docker run --rm \
		-v "$(CURDIR)/deploy/observability/prometheus/programmable-runtime-rules.yaml:/rules.yaml:ro" \
		--entrypoint=/bin/promtool \
		prom/prometheus:$(PROMETHEUS_VERSION) \
		check rules /rules.yaml

programmable-runtime-soak-smoke:
	@mkdir -p "$(PROGRAMMABLE_REPORT_DIR)"
	KEELITH_PROGRAMMABLE_SOAK_MODE=smoke \
	KEELITH_PROGRAMMABLE_SOAK_REPORT="$(PROGRAMMABLE_REPORT_DIR)/soak-smoke.json" \
	$(GO) test -race ./test/soak/programmable -count=1

programmable-runtime-soak-full:
	@mkdir -p "$(PROGRAMMABLE_REPORT_DIR)"
	KEELITH_PROGRAMMABLE_SOAK_MODE=full \
	KEELITH_PROGRAMMABLE_SOAK_DURATION="$(SOAK_DURATION)" \
	KEELITH_PROGRAMMABLE_SOAK_REPORT="$(PROGRAMMABLE_REPORT_DIR)/soak-full.json" \
	$(GO) test ./test/soak/programmable -run TestProgrammableRuntimeSoak -count=1 -timeout 0

programmable-runtime-chaos-smoke:
	@mkdir -p "$(PROGRAMMABLE_REPORT_DIR)"
	docker compose -p keelith-programmable-chaos \
		-f test/integration/compose.yaml up -d --wait mysql
	@container="$$(docker compose -p keelith-programmable-chaos \
		-f test/integration/compose.yaml ps -q mysql)"; \
	trap 'docker compose -p keelith-programmable-chaos -f test/integration/compose.yaml down -v' EXIT; \
	(cd "$(CONTRIB_DIR)" && \
		KEELITH_CHAOS_MYSQL_DSN='root:keelith@tcp(127.0.0.1:13306)/keelith?parseTime=true&loc=UTC&multiStatements=true' \
		KEELITH_CHAOS_MYSQL_CONTAINER="$$container" \
		KEELITH_CHAOS_REPORT="$(PROGRAMMABLE_REPORT_DIR)/chaos-smoke.json" \
		GOWORK=off $(GO) test -race -tags=chaos \
			./test/chaos/programmable -count=1)

programmable-runtime-chaos:
	@mkdir -p "$(PROGRAMMABLE_REPORT_DIR)"
	docker compose -p keelith-programmable-chaos \
		-f test/integration/compose.yaml up -d --wait mysql
	@container="$$(docker compose -p keelith-programmable-chaos \
		-f test/integration/compose.yaml ps -q mysql)"; \
	trap 'docker compose -p keelith-programmable-chaos -f test/integration/compose.yaml down -v' EXIT; \
	for round in $$(seq 1 "$(CHAOS_ROUNDS)"); do \
		(cd "$(CONTRIB_DIR)" && \
			KEELITH_CHAOS_MYSQL_DSN='root:keelith@tcp(127.0.0.1:13306)/keelith?parseTime=true&loc=UTC&multiStatements=true' \
			KEELITH_CHAOS_MYSQL_CONTAINER="$$container" \
			KEELITH_CHAOS_REPORT="$(PROGRAMMABLE_REPORT_DIR)/chaos-$${round}.json" \
			GOWORK=off $(GO) test -race -tags=chaos \
				./test/chaos/programmable -count=1) || exit 1; \
	done

programmable-runtime-upgrade-check:
	$(GO) test ./test/compatibility \
		-run TestProgrammableRuntimeV1ToV2StateCompatibility -count=1

topology-kubernetes-integration:
	./scripts/test-topology-configmap-integration.sh

topology-operator-integration:
	./scripts/test-topology-operator-integration.sh

generated-check:
	$(GO) run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) lint
	$(GO) run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate
	$(GO) test ./internal/generator -count=1
	git diff --exit-code -- api

benchmark-check:
	KEELITH_BENCHMARK_CHECK=1 $(GO) test ./internal/quality \
		-run TestBenchmarkBudgets -count=1

compatibility-check:
	./scripts/compatibility-check.sh

compatibility-update:
	./scripts/update-compatibility-baseline.sh

sdk-check:
	cd sdk/typescript/continuation && \
		$(NPM) ci --ignore-scripts && \
		$(NPM) run build && \
		$(NPM) test && \
		$(NPM) run fixtures:check && \
		$(NPM) run pack:check
	cd sdk/python && \
		$(PYTHON) -m unittest discover -s tests -v && \
		$(PYTHON) -m py_compile \
			keelith_continuation/__init__.py fixtures/checkout.py tests/e2e.py
	@wheel_dir="$$(mktemp -d)"; \
		trap 'rm -rf "$$wheel_dir"' EXIT; \
		$(PYTHON) sdk/python/tools/build_wheel.py "$$wheel_dir"; \
		$(PYTHON) -m zipfile --test \
			"$$wheel_dir/keelith_continuation-0.1.0-py3-none-any.whl"
	$(GO) test -tags=sdk ./test/sdk -count=1

programmable-runtime-release-check: programmable-runtime-race test vet \
	generated-check compatibility-check benchmark-check conformance \
	fuzz-smoke integration

programmable-runtime-v2-release-check: programmable-runtime-release-check \
	sdk-check projection-storage-integration programmable-observability-check \
	topology-kubernetes-integration topology-operator-integration \
	programmable-runtime-soak-smoke programmable-runtime-chaos-smoke \
	programmable-runtime-upgrade-check
