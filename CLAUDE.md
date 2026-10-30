# Waggle

A self-contained integration engine in Go, built on the vocabulary of
*Enterprise Integration Patterns*. `README.md` is the reference for
concepts, configuration and the HTTP API; read the relevant section before
changing behavior it describes. `docs/CODE_REVIEW.md` is the September 2026
review, kept as the rationale for much of the current structure.

## Workflow

- `main` is protected. Every change is a branch and a pull request, merged
  once the `lint and race tests` check is green. `govulncheck` runs on
  every PR too; a newly published advisory can turn it red on a PR that
  did not cause it.
- PR descriptions use **Why**, **What changes** and **Testing** sections.
- Run `make check` before every push. It is exactly what CI runs: gofmt,
  vet, `go mod tidy -diff`, golangci-lint and the race-enabled test suite.
- golangci-lint must be the version pinned in `.github/workflows/ci.yml`,
  built with a Go at least as new as the `toolchain` line in `go.mod`. If
  `make lint` says the Go used to build golangci-lint is lower than the
  targeted version, build a matching one into the ignored `bin/`:

  ```sh
  GOTOOLCHAIN=$(go env GOVERSION) GOBIN=$PWD/bin \
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@<ci.yml version>
  PATH=$PWD/bin:$PATH make check
  ```

- `make vulncheck` needs network access to vuln.go.dev.
- Releases are annotated `vX.Y.Z` tags on `main`. The Go toolchain stamps
  the version from git (see the README's Versions and releases), so never
  set it with `-ldflags -X`.
- Keep a PR to one change. A bug found along the way gets its own PR or
  issue unless the change cannot pass without fixing it.

## Commit messages

- Subject: `area: what changed`, where the area is the package or
  subsystem (`store:`, `channel:`, `sim:`, `web:`, `deps:`, `ci:`). Use a
  plain imperative sentence for changes that cut across areas. No period.
- Body: prose wrapped at about 72 columns that explains why: what was wrong
  or missing, the design decision and the alternative rejected, and how it
  was verified, with numbers where there are any.

## Architecture rules

- Name things with the EIP terms in the README glossary: Channel Adapter,
  Message Filter, Message Translator, Recipient List, Guaranteed Delivery,
  Dead Letter Channel.
- One binary, fully offline at runtime. Pure Go only, no cgo (SQLite is
  `modernc.org/sqlite`). Web UI assets live under `internal/api/web/` and
  are embedded with `go:embed`; never load anything from a CDN.
- Adapters and formats are compile-time plugins: implement the interface,
  register from `init()` (`adapter.RegisterInbound`,
  `adapter.RegisterOutbound`, `format.Register`), then add a blank import
  to `cmd/waggle/main.go`, and to `internal/testutil` if the examples or
  end-to-end tests use it.
- Metadata keys that cross package boundaries are constants in
  `internal/meta`, under its namespaces. Never use a bare string key, and
  never give an inbound and an outbound key the same name.
- A store failure fails the message. Never discard an error from the
  `Recorder` or the store.
- Schema changes are a new numbered file in `internal/store/migrations/`
  (`NNNN_description.sql`). Never edit a migration that has been merged:
  existing databases have already applied it.
- Config: defaults go in `Normalize`, checks in `Validate`; unknown YAML
  keys are load errors. New keys are documented in the README's
  Configuration section and shown in `examples/` where they apply.
- Errors are wrapped with `%w` and prefixed with the component:
  `fmt.Errorf("store: record: %w", err)`. Sentinel errors are
  `var ErrX = errors.New("pkg: ...")`, matched with `errors.Is`.
- Logging is `log/slog` through injected `Log *slog.Logger` fields. `info`
  is the per-message lifecycle, `debug` adds transform detail, `warn` and
  `error` are failures.

## Security

- The API and web UI are a full-control surface, and scripts are code.
  Every route requires credentials, the default listen address is
  loopback, state-changing browser requests must be same-origin, and
  script paths go through `confineScriptPath`. A change that weakens any
  of these says so in its PR title.
- A `//nolint` names the linter and the reason:
  `//nolint:gosec // confined by confineScriptPath`. Do not disable a rule
  in `.golangci.yml` to silence one site.
- Test and example data is synthetic (`internal/sim` generates it). Never
  commit real patient data, de-identified or not.

## Tests

- Engine-level tests use `internal/testutil`: `NewFixture`,
  `StartChannelYAML`, `StartExample`, `AckingReceiver`, `Eventually`,
  `WaitEvent`, `WaitMessageState`. Extend it rather than writing another
  harness.
- End-to-end tests in `internal/engine` run the shipped channels in
  `examples/channels/` through `StartExample` with address rewrites. A
  changed example keeps its test passing; a new example gets a test.
- Wait on conditions (`Eventually`, `WaitEvent`), not fixed sleeps. To
  assert that something did not happen, wait for the event that proves the
  pipeline finished.
- Before calling a timing fix done, run the test repeatedly under the race
  detector: `go test -race -count=100 -cpu 1,2,4 -run TestName ./internal/<pkg>/`.
- Every format has a `FuzzParse` target; run `make fuzz` after parser
  changes. Golden files live in the package's own `testdata/`.

## Dependencies and toolchain

- In `go.mod`, the `go` line is the language version and sets the GODEBUG
  defaults; the `toolchain` line pins the Go that builds and tests. Raising
  the `go` line changes runtime behavior, so it is never a side effect of
  your own `go get`: check `git diff go.mod` after adding or updating a
  module. When a Dependabot PR raises it because a dependency requires the
  newer Go, read the GODEBUG changes in the release notes of each Go
  version it moves past before merging.
- Dependabot opens grouped weekly PRs and `govulncheck` runs weekly. When
  it flags the standard library, bump the toolchain with
  `go get toolchain@go1.N.P`.
- New dependencies must be pure Go, and the PR says why the standard
  library is not enough.
