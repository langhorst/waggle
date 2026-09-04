# Waggle codebase review

Date: 2026-09-04. Scope: the whole repository at commit `f2d084d`
(~9.6k lines of Go outside tests, ~6.8k in tests). Every finding below
cites a file and line; measured numbers were reproduced against this
checkout. Line numbers refer to that commit.

## Executive summary

Waggle is in unusually good shape for its age. The architecture is
coherent and well explained (EIP vocabulary, compile-time registries for
formats and adapters, a single `Recorder` seam between pipeline and
store, a non-blocking event bus with resync semantics), the code is
consistently documented, `gofmt`/`go vet` are clean, and the full suite
passes under `-race` in about 30 seconds. The Guaranteed Delivery design
(per-destination SQLite queue, one worker per destination, at-least-once
with transactional mark-sent) is sound.

The biggest risks are not in the pipeline. They are:

1. **The management surface has no authentication, and the script editor
   endpoint is an arbitrary-file-write inside the channels directory.**
   Combined with the reload endpoint that is remote code execution on
   the host. This must be fixed before the daemon is exposed to anything
   but localhost.
2. **Recoverable parser input can kill the process.** Deeply nested JSON
   or XML under the default 10 MiB body limit triggers a Go stack
   overflow, which is a fatal runtime error that `recover()` cannot
   catch.
3. **Persistence errors are ignored on the hot path.** Every `Recorder`
   call in the pipeline discards its error, so a store failure produces
   a queued delivery with an empty payload rather than a rejected
   message.
4. **Two adapters silently break their own contracts**: inbound HTTP
   metadata reuses the keys the outbound HTTP sender reads as routing
   overrides, and the file reader moves files to the error directory on
   transient failures.
5. **Structural duplication is the main maintainability drag**: hl7v2
   and astm are ~85% copy-paste of each other, the two TCP listeners and
   the two TCP senders share their lifecycle code verbatim, and the
   end-to-end test harness is re-implemented six times.

Everything else is ordinary hygiene: `go.mod` was never tidied, there is
no CI, and around 50 unchecked error returns.

The rest of this document is ranked. P0 items should block any
deployment. P1 items are correctness bugs a user will hit. P2 items are
design and structure. P3 items are tests and tooling.

---

## P0: Security

### P0.1 No authentication or authorization on the API or web UI

`internal/api/server.go:40-68` and `internal/api/webui.go:177-198`
register every route on a bare `http.ServeMux`; `cmd/waggle/main.go`
wraps it in an `http.Server` with no middleware; the default listen
address is `":8420"` on all interfaces (`internal/config/config.go:35`).
Anyone who can reach the port can stop channels, read and replay
messages (which will carry PHI in the intended deployments), and write
files to disk (P0.2).

Suggested fix: a `daemon.yaml` `auth:` block (bearer token or basic
auth, plus optional TLS), enforced by one middleware around the whole
handler. Default to binding `127.0.0.1:8420` and require an explicit
opt-in to bind wider. A "no auth configured" warning in the log at
startup is not sufficient for a healthcare integration engine.

### P0.2 `PUT /api/scripts` is an arbitrary file write under the channels directory

`main.go:174` sets `ScriptsRoot` to `cfg.ChannelsDir`, the directory
holding the channel YAML. `confineScriptPath` (`server.go:351-370`) only
checks the path prefix, and `handleScriptWrite` (`server.go:411`) calls
`os.WriteFile` with no extension allow-list and creates files that do
not exist. So `PUT /api/scripts?path=<channels>/feed.yaml` rewrites a
channel definition, and `POST /api/channels/feed/reload` applies it. A
rewritten channel can use `file-writer` with any `dir` or `http-sender`
with any URL.

Suggested fix, all three:

- Restrict the endpoint to paths the loaded channel configs actually
  reference (`config.Channel.ScriptPaths()` already exists) or at
  minimum to `*.js`.
- Resolve with `filepath.EvalSymlinks` on both root and target before
  the prefix check (a symlink inside the root escapes today), and reject
  `abs == root`.
- Write atomically (temp file + rename) so the hot-reload watcher never
  sees a half-written file, and read the body with
  `io.ReadAll(http.MaxBytesReader(...))`. The hand-rolled read loop at
  `server.go:398-410` breaks on any error including
  `io.ErrUnexpectedEOF` and writes the truncated buffer to disk.

### P0.3 CSRF and DNS rebinding on state-changing routes

All HTMX `POST` routes (`webui.go:190-197`) and the JSON `POST` routes
take no body, no token, and check neither `Origin`/`Sec-Fetch-Site` nor
`Host`. A form on any web page can start/stop channels or trigger replays
on a daemon the operator's browser can reach. Fix together with P0.1:
require the auth header (which browsers will not add cross-site) and
check `Sec-Fetch-Site` for the UI routes.

### P0.4 Missing HTTP server hardening

- `main.go`: the API server sets only `Addr` and `Handler`. Add
  `ReadHeaderTimeout`, `IdleTimeout`, `MaxHeaderBytes`. A blanket
  `WriteTimeout` would kill SSE, so use
  `http.ResponseController.SetWriteDeadline` per request instead.
- `internal/adapter/httpin/listener.go:126-130` sets only
  `ReadTimeout`; add `WriteTimeout` and `IdleTimeout`.
- `internal/api/sse.go:25`: no cap on concurrent SSE subscribers.
- `server.go:166,281`: `limit` is parsed but never clamped upward.
- `server.go:78` `writeError` returns raw `err.Error()` to clients,
  including absolute filesystem paths.

### P0.5 `http.path` accepts absolute URLs

`internal/adapter/httpout/sender.go:117-122` resolves the script-set
path with `ResolveReference`, so `meta['http.path'] = 'https://elsewhere/x'`
redirects the entire request. Reject any reference with a scheme or
host. Also bound the drained 2xx body at `sender.go:147` with
`io.LimitReader`.

---

## P1: Correctness

### P1.1 Deeply nested JSON/XML kills the process

`jsonfmt.decodeValue` (`internal/format/jsonfmt/jsonfmt.go:62-111`) and
`xmlfmt.parser.element` (`internal/format/xmlfmt/xmlfmt.go:117-189`)
recurse once per nesting level with no depth cap. A 9 MB payload of
`[[[[…` or a 10 MB `<a><a>…` (both under the httpin default
`maxBodySize` of 10 MiB) ends in `fatal error: stack overflow`. This was
reproduced. `encodeValue`, `flatten`, `encodeElement`, and
`message.Node.Clone` (`internal/message/message.go:32`) recurse the same
way. Add a depth limit (a few hundred is generous) that returns a parse
error.

### P1.2 Pipeline ignores every persistence error

`internal/channel/channel.go` discards the result of every `Recorder`
call after `Record` (`SetState`, `SetTransformed`, `SetDestinationState`
at lines 297, 311, 314, 371, 376, 383, 391, 396, 404, 416, 420, 439).
Concrete consequence: if `SetDestinationState(QUEUED, payload)` at
`channel.go:396` fails and `Enqueue` succeeds, the worker's `Head`
(`internal/store/queue.go:41-46`) LEFT JOINs the missing destination row,
COALESCEs the payload to `x''`, and sends an empty message. For a
"Guaranteed Delivery" engine a store error must fail the message (state
ERROR, AE to the sender), not be logged and forgotten. Likewise the
serialize error at `channel.go:310` is swallowed: the message is marked
TRANSFORMED with no transformed payload stored.

Related: `Head` should treat a missing destination row or NULL payload as
an error and dead-letter, not send empty bytes.

### P1.3 Deadlock window in `Channel.Pause`

`channel.go:158-170` holds `c.mu` while calling `Source.Stop()`. The
MLLP and ASTM listeners' `Stop` waits on their `WaitGroup`
(`internal/adapter/mllp/listener.go:150`) for every connection
goroutine, and those goroutines call `c.deliver`, whose first action is
`c.mu.Lock()` (`channel.go:225`). A frame that arrives while `Pause` is
running blocks on the mutex, `Stop` waits for it forever, and the
channel is wedged. `Channel.Stop` (`channel.go:187-209`) already
releases the mutex before calling `Source.Stop()`; `Pause` and `Resume`
should do the same. A test that fires MLLP traffic in a loop while
pausing would catch this.

### P1.4 Unbounded goroutines and no backpressure in intake

`channel.deliver` (`channel.go:247-254`) spawns one goroutine per
received message, all of which then serialize on `pipeMu`. Under a burst
from a fast MLLP sender in `ackMode: immediate` the ACK returns after
`Record`, so the peer keeps sending and goroutines pile up without
limit. Replace with one pipeline goroutine per channel consuming from a
bounded channel of `*Message`; a full buffer becomes natural
backpressure on the transport (delay the ACK). The same goroutine body is
duplicated in `Inject` (`channel.go:267-273`). `Inject` also calls
`wg.Add` outside any lock after a status check, which races with
`Stop`'s `wg.Wait`.

### P1.5 HTTP inbound and outbound share metadata keys

`internal/adapter/httpin/listener.go:201-202` stamps `http.method` and
`http.path` on every inbound message. `internal/adapter/httpout/sender.go:38-39`
reads exactly those keys as per-message overrides. Metadata flows
through the pipeline untouched, so an `http-listener` → `http-sender`
channel silently replays the inbound path and method against the
outbound base URL. Namespace inbound metadata as `source.http.*` (the
file and MLLP adapters already use `source.*`), and put every metadata
key in one `meta` package of constants. Today `"message.id"`,
`"channel.id"`, `"destination.id"` are string literals duplicated in
`channel.go:422-424` and `internal/queue/queue.go:93-95`.

### P1.6 File reader violates the delivery contract

`adapter.DeliverFunc` documents (`internal/adapter/adapter.go:40-41`)
that on error the file source should leave the input in place.
`internal/adapter/file/reader.go:153-155` moves it to `errorDir`, so a
transient store failure (SQLite busy, disk full) permanently shunts a
file. `reader.go:171` discards the `os.Rename` error; on a cross-device
`processedDir` the file stays put and is redelivered every poll. The
reader also ignores `Receipt.Done`, so it is the one source with no
`ackMode`.

### P1.7 Setting what you read is lossy in every format

`Value` renders interior nodes with separators (`delimited.go:256`,
`jsonfmt.go:351`, `xmlfmt.go:525`), but `Set` always stores a leaf and
`Serialize` escapes it. Measured: `msg.set('PID-5', msg.get('PID-5'))`
yields `DOE\S\JOHN` on the wire; the Mirth idiom
`msg.set('PID-5', 'SMITH^JOHN')` produces the same. Either re-split
delimited values on `Set` (Mirth semantics; `delimited.FieldNode` already
does this) or add an explicit `setRaw` and document the trap in the
README's script section.

### P1.8 Other format bugs (each reproduced)

- `jsonfmt.Set` coerces any mismatched container, not just scalars
  (`jsonfmt.go:271-282`): `set('name.family','X')` on `name:[…]` replaces
  the array with an object. Reads fan out over arrays; writes should
  target element 0 or error.
- `xml:lang` / `xml:space` parse but can never serialize
  (`xmlfmt.go:210-221` never maps the XML namespace URI back to its
  prefix; `validName` at `xmlfmt.go:318` then rejects it). FHIR
  narrative and CDA use `xml:lang`.
- Non-UTF-8 XML declarations are rejected (`xmlfmt.go:71`, no
  `CharsetReader`). Legacy CDA feeds are commonly ISO-8859-1.
- `Set("MSH-2.2", …)` corrupts the header (`delimited.go:216-224`).
- Whitespace-only XML text is dropped (`xmlfmt.go:156`): `<a> </a>` →
  `<a/>`.
- `jsonfmt` Flatten and Resolve use different escape grammars for
  bracketed keys (`jsonfmt.go:391-400` vs `path.go:481-485`), so a
  flattened path may not resolve.

### P1.9 Adapter and script details

- `adapter.Duration` (`internal/adapter/duration.go:24-28`) treats a
  bare YAML integer as nanoseconds: `holdTimeout: 30` is 30 ns and passes
  every `<= 0` default check. Error on bare numbers or treat them as
  seconds.
- `internal/adapter/httpin/listener.go:158` cancels the run context
  before `Shutdown`, so a request held in `ackMode: destination` returns
  from the `select` without writing a status and net/http emits an empty
  200. Reorder, and write an explicit 503.
- MLLP sender writes have no deadline (`mllp/sender.go:100`); a stalled
  peer holds `s.mu` forever. Neither TCP sender honours `ctx.Done()`.
- `response.setAck` from a destination-level script is silently
  discarded: `sendTo` copies the message (`channel.go:357-366`) and only
  the channel-level `m.AckCode` is read (`channel.go:321`). Either
  support it or reject it at compile time.
- `script/env.go:135` builds a logger from `slog.Default()` instead of
  the injected logger; `Script.log` is unused there.
- `getAll` is quadratic: `Value` re-walks the tree per node
  (`delimited.go:257,272`). Measured `getAll('OBX-5')`: 2000 OBX 0.24 s,
  8000 OBX 5.9 s. Return depth from `Resolve` or record it on the node.
- `store.Record` runs the retention DELETE inline on the pipeline path
  every 100 inserts (`store.go:154`); signal the prune goroutine instead.

---

## P2: Design and structure

### P2.1 hl7v2 and astm are one format with two configs

`internal/format/hl7v2` and `internal/format/astm` duplicate
`delimsFromHeader`, `treeDelims`, `leafValue`, the escape
decoder/encoder, `Parse`, `Serialize`, `parsePath`, and all six interface
methods. The real differences are the header segment names, the segment
name regex, delimiter order in the header, whether subcomponents exist,
and the path regex. Make `delimited` provide a `Format` struct that
implements `format.DataType` from those parameters and reduce each
package to registration plus its escape table. `atoiZero` is defined
three times (also `csvfmt.go:397`).

### P2.2 Shared TCP adapter plumbing

- The listener lifecycle in `mllp/listener.go:51-152` and
  `astm1381/listener.go:52-152` is byte-identical (struct, `Addr`, accept
  loop with `context.AfterFunc(conn.Close)`, `Stop`). Extract an
  `adapter/tcpserver` helper: `Serve(ctx, addr, func(ctx, net.Conn))`.
- Sender connection management (`mllp/sender.go:64-91` and
  `astm1381/sender.go:74-101`: `Close`, `dropConnLocked`,
  `ensureConnLocked`, deadline merging) is the same code twice.
- `AckMode` constants, their validation, and the hold-for-decision loop
  appear three times (`mllp/listener.go:25-33,179-187`,
  `astm1381/listener.go:24-33,258-270`, `httpin/listener.go:33-36,222-234`).
  Move `AckMode` into `adapter` with one `AwaitDecision(ctx, receipt,
  hold)` helper.
- Settings inconsistencies to normalize while doing this: `addr` vs
  `listen`; ASTM accepts only `AA` as success while HTTP and MLLP also
  accept `CA`; hold-timeout fallback is configurable for MLLP only;
  write deadlines are 5 s, 10 s, or absent depending on adapter.

### P2.3 The `format.DataType` interface is under-specified

Two optional interfaces (`SegmentJoiner`, `TypedSetter`) across five
formats, with an HL7-shaped default join in the consumer
(`script/env.go:112`), is the sign. The default is wrong for CSV
(`row.get('3')` errors with `invalid path "R-3"`) and silently returns
empty for JSON (`segments('name')[0].get('family')` becomes
`name-family`). Replace both with `ResolveFrom(n, rel)` and
`SetFrom(n, rel, value any)` on the interface itself, and fold typed
setting into `Set(root, path string, value any)`. `Value(root, n)` only
takes `root` because delimited rediscovers depth by DFS; fixing P1.9's
quadratic issue removes that parameter.

### P2.4 API and web UI duplicate view logic three ways

Channel enrichment (`server.go:113-129` vs `webui.go:200-216`),
script-reference building (`server.go:320-348` vs `webui.go:425-459`),
and the lifecycle switch (`server.go:131-156` vs `webui.go:231-251`) are
near-verbatim copies, and the TUI has a third copy of the stage list
(`tui/model.go:376-382`) and count arithmetic (`tui/view.go:492` vs
`_channels.html:24`). Move these into `engine` as view-model functions
and have all three surfaces call them. Also: status codes are chosen by
`strings.Contains(err.Error(), ...)` (`server.go:147,418`); export
sentinel errors from `engine` and `script`. `render` (`webui.go:165-170`)
writes templates straight to the `ResponseWriter`; buffer first so a
template error does not produce a truncated 200.

### P2.5 The TUI boots a second engine

`waggle tui` (`main.go:65-120`) starts a full engine on the same
`messages.db`, channel directory, and adapters as the daemon. Run beside
the daemon it double-processes file-reader inputs and fights for MLLP
and HTTP listen ports. The `Backend` seam (`tui/backend.go`) already
anticipates an HTTP-backed implementation; implement it, or move the TUI
to a separate binary and drop 17 modules from the daemon's dependency
graph. The TUI also does synchronous SQLite work inside `Update`
(`tui/model.go:181-188, 293-345`), which belongs in `tea.Cmd`s.

Note that the vendored `web/static` (656 KB, including the Tailwind
play-CDN runtime that compiles CSS in the browser) is a larger and less
justified single-binary cost than bubbletea. Build the CSS at
development time and ship the output.

### P2.6 Composition root duplication

`runTUI` and `runDaemon` in `main.go` repeat ~40 lines (load config,
create data dir, open store, script engine, engine, load channels).
Extract a `bootstrap(cfg) (*engine.Engine, func())`. Both also ignore
the error slice from `StartEnabled`.

### P2.7 Store and queue scaling notes

- `SetMaxOpenConns(1)` (`store.go:55`) means every UI read queues
  behind pipeline writes and vice versa. WAL mode allows a separate
  read pool; split reads from writes before this becomes the wall.
- Each queue worker polls every 250 ms (`queue.go:51`). With N
  destinations that is 4N queries per second when idle. Add a per-queue
  wake channel that `Enqueue` signals, with the poll as a fallback for
  `not_before` expiry.
- `Requeue` (`store/queue.go:144-192`) checks "already queued" in one
  statement and inserts in another; it is correct only because of the
  single connection. A unique index on
  `destination_queue(destination_id, message_id)` makes the invariant
  explicit.
- `pruneLoop` and `pruneAll` swallow every error (`store.go:225-256`).

### P2.8 Configuration

- `Channel.Validate` (`config.go:195`) mutates the config (`Name`,
  `DataType` defaults). Split into `normalize` and `validate`.
- `dataType` is validated at config load but `adapter.type` only at
  engine build; validate both in the same place.
- `config.go:28` says `hotReload` watches channel configs; only scripts
  are watched.
- `LoadDaemon` applies defaults twice (`DefaultDaemon()` then the
  `== ""` checks).

### P2.9 Small things

- Stale "phase N" comments from the original plan in `channel.go:59-60,
  354-355`, `engine.go:24`, `main.go:3-4`, `api/server.go:2`.
- `mllp/listener.go:160-162` is an empty `if` (a lost log line).
- `astm1381/proto.go:375-381` `readFrame` is test-only; `proto.go:312`
  `soh` and `duration.go:32` `MarshalYAML` are unused.
- `astm1381/sender.go:153` `MaxRetries` counts attempts, not retries.
- `tui/model.go:408` defines `max`, shadowing the builtin.
- `tui/view.go:717` `truncate` slices bytes and can split a rune.
- Two flavours of "0 means default" coexist: config uses `*int` so that
  `-1` can mean unlimited, adapters use `<= 0` guards. Pick one.

---

## P3: Tests and tooling

### P3.1 One end-to-end harness instead of six

The recipe "temp store → script engine → engine → YAML string →
`LoadChannel` → `Start` → cast `ch.Source.(*X)` for its address → send →
poll the store" is re-implemented in `engine_test.go:115-157`,
`e2e_test.go:467-492`, `bridge_e2e_test.go:65-86`,
`orders_e2e_test.go:316-346`, `api_e2e_test.go:40-75`, and
`api/api_test.go:43-104`, with three differently named poll helpers in
one package (`waitFor`, `waitForCondition`, `waitUntil`) using three
different deadlines, and the "ACKing fake receiver" closure copied four
times. An `internal/testutil` package should expose `NewEngine(t)`,
`StartChannelYAML(t, yaml)`, `StartExample(t, name, rewrites)`,
`ListenAddr(t, ch)` (behind an `adapter.Addresser` interface, removing
the type casts), `Eventually(t, cond)`, `AckingReceiver(t, kind)`, and
`WaitForState(t, ...)`.

### P3.2 Tests do not load the example files

Tests import the example scripts by path but retype the YAML inline
(`orders_e2e_test.go:304`, `api_e2e_test.go:42`, `bridge_e2e_test.go:35`).
Drift is therefore invisible, and there is drift today:
`examples/channels/astm-to-mllp.yaml:18` sends ORU^R01 results to
`127.0.0.1:6661`, which is `adt-to-csv.yaml`'s listener guarded by
`only-adt.js`, so under `make run` every bridged lab result is
FILTERED. The README says the pair is chained end-to-end; only the test
chains them. Point it at `:6663` and have the harness load the real
files with address rewriting.

### P3.3 Flakiness risks

- Negative assertions by fixed sleep: `engine_test.go:392` (200 ms) and
  `orders_e2e_test.go:485` (150 ms), plus `astm1381_test.go:239` and
  `httpin/listener_test.go:208`. Assert on a bus event that the pipeline
  finished instead.
- `TestHotReload` (`script_test.go:298-316`) relies on `os.Chtimes` plus
  a 100 ms sleep against a 20 ms poll; poll for the compile error.
- Unsynchronized writes from handler goroutines in
  `httpin/listener_test.go:78-82` and `httpout/sender_test.go:293-301`.
- `mllp_test.go:158` and `astm1381_test.go:306` assume port 1 is closed.
- No `t.Parallel()` anywhere; every port is `127.0.0.1:0` so the e2e
  package could run in parallel.

### P3.4 Coverage gaps against README claims

Verified absent: pause keeps draining queues; config reload that actually
changes a channel (and the "now defines channel X" error path); the 30 s
retention ticker and 100-insert trigger; `maxAttempts: -1` retry path;
engine-level crash recovery (stop mid-queue, reopen, drain);
`enabled: false`; SSE resync and per-channel filtering; `ackMode:
destination` with a queued (non-waitForAck) destination; any
Stop→Start restart of an inbound adapter; MLLP framing violations and
`maxMessageSize`; symlink and `path=<root>` cases for the scripts API;
script handles for CSV and JSON segments (which is why P2.3 went
unnoticed). Format fuzz targets for hl7v2/astm only assert re-parse
succeeds; csv/json/xml also assert canonical stability, and hl7v2/astm
should too.

### P3.5 Repository hygiene

- `go.mod` marks every dependency `// indirect`, including the five
  direct ones (bubbletea, lipgloss, goja, yaml.v3, sqlite). `go mod tidy
  -diff` is non-empty and `go.sum` is missing sums tidy wants. Run
  `go mod tidy` and add `go mod tidy -diff` to `make lint`.
- No `.github/workflows`, no `.golangci.yml`, no Dockerfile, no
  CONTRIBUTING or ARCHITECTURE doc, no tags, no version symbol in the
  binary. `make check` passes today but nothing enforces it. A minimal
  CI running `make check`, `go mod tidy -diff`, and `golangci-lint` with
  `errcheck`, `staticcheck`, `bodyclose`, `gosec` would have caught
  several items above.
- `golangci-lint` with `errcheck` reports 48 unchecked returns, most in
  tests, but also `astm1381/listener.go:123,134,148`,
  `file/writer.go:72-81`, and both senders' `conn.Close`.
- `.gitignore` anchors `/out/` and `/data/` at the root, but `make run`
  writes to `examples/out` and `examples/data`; a demo run dirties the
  tree (only `*.db*` is caught).
- `TestLoadChannelErrors` (`config_test.go:523-550`) iterates a map
  without subtests, so failures neither name the case nor order
  deterministically. `queue_test.go:232` contains a no-op assertion.
- Golden trees under `testdata/` are reached via `../../../testdata`;
  move them into each package's own `testdata`.

---

## Suggested order of work

1. **Week one, blocking:** P0.1 auth + localhost default, P0.2 script
   endpoint confinement, P0.3 CSRF, P1.1 parser depth limits, P1.3 Pause
   deadlock. Add CI with `make check` and `go mod tidy -diff`.
2. **Next:** P1.2 stop ignoring Recorder errors and make `Head` refuse
   empty payloads; P1.5 metadata namespacing with a constants package;
   P1.6 file reader contract; P1.9 `Duration` bare-integer handling.
3. **Then the structural work**, each of which pays for itself in the
   next feature: P3.1 test harness (do this before the refactors so they
   are covered), P2.1 delimited base format, P2.2 TCP adapter helpers,
   P2.3 interface cleanup, P2.4 view-model consolidation.
4. **Format correctness backlog** (P1.7, P1.8) can be scheduled as
   individual issues; each has a one-line reproduction above.
5. **Decide the TUI's future** (P2.5) before adding more to it.
