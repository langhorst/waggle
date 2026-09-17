# Waggle 🐝 — build, run, and test.
# `make` (or `make help`) lists targets; `make check` is the pre-push gate.

BIN     := bin/waggle
SIMBIN  := bin/waggle-sim
GO      := go
PKGS    := ./...
FUZZPKGS := ./internal/format/hl7v2 ./internal/format/astm ./internal/format/csvfmt ./internal/format/jsonfmt ./internal/format/xmlfmt
BENCHPKGS := ./internal/format/hl7v2 ./internal/channel
FUZZTIME ?= 30s

.DEFAULT_GOAL := help

.PHONY: help build release run tui test test-race cover bench fuzz \
        fmt fmt-check vet tidy-check golangci lint check clean

help: ## List available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the waggle and waggle-sim binaries into bin/
	$(GO) build -o $(BIN) ./cmd/waggle
	$(GO) build -o $(SIMBIN) ./cmd/waggle-sim

release: ## Build a stripped, trimmed release binary
	$(GO) build -trimpath -ldflags '-s -w' -o $(BIN) ./cmd/waggle

run: build ## Run the daemon against the examples/ configuration
	cd examples && ../$(BIN) daemon

tui: build ## Attach the observer TUI to the daemon started by make run
	cd examples && ../$(BIN) tui

test: ## Run all tests
	$(GO) test $(PKGS)

test-race: ## Run all tests with the race detector
	$(GO) test -race $(PKGS)

cover: ## Run race tests with coverage and print the summary
	@mkdir -p bin
	$(GO) test -race -coverprofile=bin/cover.out \
		$$($(GO) list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' $(PKGS))
	$(GO) tool cover -func=bin/cover.out | tail -1

bench: ## Run parser and pipeline benchmarks
	$(GO) test -bench=. -run=NONE $(BENCHPKGS)

fuzz: ## Fuzz each format parser for FUZZTIME (default 30s)
	@for pkg in $(FUZZPKGS); do \
		echo "fuzzing $$pkg"; \
		$(GO) test $$pkg -fuzz=FuzzParse -fuzztime=$(FUZZTIME) || exit 1; \
	done

fmt: ## Format all Go sources
	gofmt -w .

fmt-check: ## Fail if any file needs formatting
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; \
	fi

vet: ## Run go vet
	$(GO) vet $(PKGS)

tidy-check: ## Fail if go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

golangci: ## Run golangci-lint (install: https://golangci-lint.run/docs/welcome/install/)
	golangci-lint run $(PKGS)

lint: fmt-check vet tidy-check golangci ## Formatting check + vet + tidy + golangci-lint

check: lint test-race ## The pre-push gate: lint + race tests

clean: ## Remove build artifacts and example runtime state
	rm -rf bin examples/data examples/out
