# keel: the everyday commands. `make` lists them; `make check` runs what CI
# runs, so a green check here is a green check there.

GO         ?= go
BENCH_PKGS := ./breaker/ ./ratelimit/ ./retry/

.DEFAULT_GOAL := help

help: ## List the targets
	@grep -E '^[a-z][a-z-]*:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

fmt: ## Format every Go file in place
	gofmt -w .

fmt-check: ## Fail if any Go file is not gofmt-formatted
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

vet: ## Run go vet
	$(GO) vet ./...

tidy: ## Run go mod tidy
	$(GO) mod tidy

tidy-check: ## Fail if go mod tidy would change go.mod or go.sum
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

test: ## Quick run: -short trims the model and chaos iterations
	$(GO) test -short ./...

test-race: ## The full suite with the race detector on 1, 2 and 4 CPUs, twice
	$(GO) test -race -cpu 1,2,4 -count=2 -timeout 15m ./...

test-redis: ## The Redis store against a server: REDIS_ADDR, or a Valkey container via docker
	$(GO) test -v ./ratelimit/goredis/

bench: ## Benchmarks with allocations; the source of the tables in docs/
	$(GO) test -run '^$$' -bench . -benchmem $(BENCH_PKGS)

bench-smoke: ## Benchmarks compile and run once
	$(GO) test -run '^$$' -bench . -benchtime=1x $(BENCH_PKGS)

contrib-check: ## Validate the alert rules (promtool, when installed) and the dashboard JSON
	@if command -v promtool >/dev/null 2>&1; then promtool check rules contrib/prometheus/alerts.yaml; \
	else echo "promtool not installed; alert rules not checked"; fi
	@if command -v jq >/dev/null 2>&1; then jq -e . contrib/grafana/keel.json >/dev/null && echo "contrib/grafana/keel.json: valid JSON"; \
	else echo "jq not installed; dashboard JSON not checked"; fi

check: fmt-check tidy-check vet test-race bench-smoke contrib-check ## Everything CI runs

.PHONY: help fmt fmt-check vet tidy tidy-check test test-race test-redis bench bench-smoke contrib-check check
