# Waggle 🐝

A self-contained integration engine in Go — named for the honeybee waggle
dance, nature's own message routing — inspired by Mirth Connect and
built strictly on the vocabulary of *Enterprise Integration Patterns*
(Hohpe & Woolf). One binary, no external dependencies at runtime, fully
offline: a headless daemon hosts any number of **channels**, each moving
messages from an inbound **Channel Adapter** through a **Message Filter**
and a chain of **Message Translators** (JavaScript) to a **Recipient List**
of outbound Channel Adapters with **Guaranteed Delivery**.

```
                        ┌────────────────────── channel ──────────────────────┐
  MLLP / HTTP / ──────► │ inbound adapter → filter → translator chain ─┬─► queue → MLLP sender
  files                 │        │                                     ├─► queue → HTTP sender
                        │     SQLite (messages, queues, replay)        └─► queue → file writer
                        └─────────────────────────────────────────────────────┘
                                  ▲                        ▲
                          TUI (embedded)          web UI + REST/SSE API
```

## Quick start

```sh
make run    # build + run the daemon on the examples/ config — web UI on http://localhost:8420
make tui    # or attach the read-only observer TUI (engine runs in-process)
```

(Or without make: `go build -o bin/waggle ./cmd/waggle`, then run
`../bin/waggle daemon` from `examples/`.) Module path:
`github.com/langhorst/waggle`.

Drop an HL7 file into a file-reader channel's directory, or fire messages
at an MLLP listener, and watch them flow in the UI: live message list,
structural diff of received vs. sent, tree explorer, dead-letter queue,
replay.

## EIP glossary → implementation

| EIP term (Hohpe & Woolf)   | Here                                                              |
|----------------------------|-------------------------------------------------------------------|
| Message                    | `internal/message.Message` — raw bytes + canonical tree + states  |
| Message Channel            | Go channels + the per-destination SQLite queue                    |
| Channel Adapter (inbound)  | `mllp-listener`, `astm-listener`, `http-listener`, `file-reader` |
| Polling Consumer           | the `file-reader` source                                          |
| Pipes and Filters          | the channel pipeline (`internal/channel`)                         |
| Message Filter             | `filter:` script — distinct step, drops retain the message        |
| Message Translator         | `transformers:` script chain (goja JavaScript)                    |
| Recipient List             | `destinations:` — each with its own filter/translator chain       |
| Channel Adapter (outbound) | `mllp-sender`, `astm-sender`, `http-sender`, `file-writer`       |
| Guaranteed Delivery        | SQLite-backed per-destination queues with retry/backoff           |
| Dead Letter Channel        | exhausted retries & application NAKs (`dead_letter`, requeueable) |
| Invalid Message Channel    | parse/script failures (state `ERROR`, replayable)                 |
| Message Store              | `internal/store` (SQLite via modernc.org/sqlite, pure Go)         |

## Concepts

**Formats.** Every message parses into a generic tree shared by all
formats; format modules (`hl7v2`, `astm`, `csv`, `json`) provide parse,
serialize, and a path dialect. Each dialect counts the way its format's
own ecosystem counts — HL7/ASTM/CSV are 1-based like their specs and
Mirth, JSON is 0-based like JavaScript/JSONPath/jq (XML, when it lands,
will be 1-based like XPath):

- HL7 v2: `PID-5.1`, `PID-3[2].1`, `OBX[2]-5`, `MSH-9.1.2` (segment →
  field → repetition → component → subcomponent, escapes decoded/encoded
  at the parse boundary, delimiters honored from MSH-1/MSH-2)
- ASTM E1394: `H-5`, `R[2]-3.1` (delimiters from the H record)
- CSV: `R.1`, `R[2].3` (rows and columns; 1-based like a spreadsheet)
- JSON: `patient.name[0].given`, `entry[1].resource.id`, `["odd.key"]`
  (0-based arrays; a key on an array fans out across its elements like an
  HL7 repetition). Leaves are type-tagged: numbers, booleans, and null
  round-trip exactly as typed, and `msg.set` from a script keeps the JS
  value's type — `{"count":5}` never mutates into `{"count":"5"}`.

Adding a format means implementing `format.DataType` and registering it —
compile-time, like adapters. Transformers are data; adapters and formats
are code.

**Scripts.** Filters and transformers are plain JavaScript files (goja —
pure Go, sandboxed: no filesystem, no network, interrupt timeout):

```js
// filter: return true to keep the message
function filter(msg) { return msg.get('MSH-9.1') === 'ADT'; }

// transformer: mutate in place…
function transform(msg) {
  msg.set('PID-5.1', msg.get('PID-5.1').toUpperCase());
  msg.segments('OBX').forEach((obx, i) => obx.set('1', String(i + 1)));
}

// …or convert formats by returning a new message
function transform(msg) {
  var out = newMessage('csv');
  out.set('R.1', msg.get('PID-3.1'));
  return out;
}
```

Script API: `msg.get/set/getAll/segments`, `msg.raw`, `msg.dataType`,
`newMessage(dataType)`, `meta`, `logger.info/warn/error`, and
`response.reject(code, text)` / `response.setAck(code, text)` for
validation-driven ACKs. `meta` is writable: entries a transformer sets
travel with the delivery (persisted alongside the queued payload), which
is how scripts route the `http-sender` per message. Hot reload is on by default: edit a script and the
next message uses it; a broken save keeps the previous version running.

**ACK modes (MLLP).** `ackMode: immediate` acknowledges as soon as the
message is durably recorded (the Guaranteed Delivery handoff).
`ackMode: destination` holds the ACK until the pipeline finishes: scripts
can reject with a meaningful `AR`, and destinations marked
`waitForAck: true` deliver synchronously (single attempt — the upstream
sender owns retry) with their outcome deciding the ACK. Non-waiting
destinations always go through the queue.

**ASTM E1381 transport.** `astm-listener` / `astm-sender` speak the CLSI
LIS01-A2 low-level protocol over TCP: ENQ/ACK establishment, checksummed
STX…ETB/ETX frames with mod-8 frame numbers and NAK retransmission, EOT
termination — one session per E1394 message. The listener delivers the
assembled message *before* acknowledging the final frame (a recording
failure NAKs it), and in `ackMode: destination` a pipeline rejection
answers EOT — the E1381 receiver interrupt — since the protocol has no
application-status channel. Sender-side failures (busy NAK, retry
exhaustion, interrupts, timeouts) are all transient: the delivery queue
retries with backoff and dead-letters after `maxAttempts`.

**HTTP adapters.** `http-listener` turns Waggle into an API: each listener
owns its address, path, and optional TLS (`certFile`/`keyFile`), guarded by
basic auth and/or shared-secret headers. The HTTP response is the transport
ACK with the same two modes as MLLP — `ackMode: immediate` answers 202 once
the message is durably recorded; `ackMode: destination` holds the response
for the pipeline outcome (AA→200, AR→400, AE→500, hold timeout→504), so a
`response.reject` in a script or a failed `waitForAck` delivery surfaces to
the caller. `http-sender` makes Waggle an API client: YAML sets the url,
method, content type, static headers (Authorization etc.), basic auth, and
an optional private CA; scripts override per message with
`meta['http.path']` and `meta['http.method']`. Failure classification
drives Guaranteed Delivery — network errors, 408, 429, and 5xx retry with
backoff; any other non-2xx is an application rejection that dead-letters
immediately with the API's response body in the error text.

**Lab bridge examples** (`examples/channels/`): the full bidirectional lab
workflow, with results flowing up and orders/queries flowing down —

```
            results (ORU^R01)                      results (E1394)
   HIS/LIS ◄──────────────────── integration ◄──────────────────── instrument
            MLLP                   channel          E1381
   HIS/LIS ────────────────────►            ────────────────────► instrument
            orders (ORM^O01)                       orders (E1394)
                                            ◄──── query (Q record)
                                            ────► order response
```

- `astm-to-mllp.yaml` / `mllp-to-astm.yaml`: E1394 results over E1381 →
  HL7 ORU^R01 over MLLP, and the reverse (`astm-to-hl7.js`,
  `hl7-to-astm.js`). Chained end-to-end by `TestASTMBridgeRoundTrip`.
- `orders-to-instrument.yaml`: HL7 ORM^O01 over MLLP → ASTM order message
  over E1381 (`hl7-orm-to-astm.js`; ORC-1 order control maps to the O-11
  action code: NW→N, CA→C). Covered by `TestOrderDownloadEndToEnd`.
- `instrument-query.yaml`: an instrument's ASTM query (Q record) is
  answered with an order message for the queried specimen — request-reply
  as two opposite half-duplex E1381 sessions on one channel
  (`only-queries.js` filter + `astm-query-to-order.js`; swap the script
  body for a real order lookup). Covered by `TestQueryResponseRoundTrip`,
  including the assertion that non-query traffic is FILTERED and produces
  no response.

**API examples** (`examples/channels/`): the same engine speaking REST —

- `adt-to-fhir.yaml`: HL7 ADT over MLLP → FHIR R4-shaped Patient resource
  POSTed to an API (`adt-to-fhir-patient.js`); A28/A31 updates PUT to
  `/fhir/Patient/{id}` via the script's meta routing. Covered by
  `TestADTToFHIREndToEnd`, including a 404 dead-lettering with the API's
  response body.
- `fhir-webhook-to-hl7.yaml`: a webhook POST becomes HL7 ADT^A31 over MLLP
  (`fhir-patient-to-adt.js`), with `ackMode: destination` + `waitForAck` so
  the caller's HTTP status reflects the HIS's actual ACK — and a script
  rejection of a non-Patient payload answers 400. Covered by
  `TestFHIRWebhookToHL7`.

**Delivery.** Each queueing destination has exactly one worker draining
its FIFO queue: transient failures back off exponentially (jittered,
capped at 5m) without reordering; application NAKs (AE/AR) dead-letter
immediately; exhausted retries dead-letter after `maxAttempts`. Queues
live in SQLite, so a crash or restart loses nothing. Paused channels stop
intake but keep draining.

**Replay.** Any stored message can re-enter the pipeline (new message, same
correlation ID, `replay_of` lineage) or have its already-transformed
payload requeued to a single destination.

## HTTP API

```
GET    /api/status                                  GET  /api/events            (SSE)
GET    /api/channels                                GET  /api/channels/{id}/events
POST   /api/channels/{id}/start|stop|pause|reload
GET    /api/channels/{id}/messages?limit&before_id&state=
GET    /api/channels/{id}/dlq                       POST /api/dlq/{msg}/{dest}/requeue
GET    /api/channels/{id}/scripts                   GET|PUT /api/scripts?path=
GET    /api/messages/{id}
GET    /api/messages/{id}/tree?stage=received|transformed|dest:{destID}
GET    /api/messages/{id}/diff?dest={destID}
POST   /api/messages/{id}/replay[?destination={destID}]
```

The web UI (HTMX + Flowbite, embedded and offline) is served at `/` and is
the full-control surface; the TUI is a read-only observer that embeds the
engine directly.

## Configuration

See `examples/`. One `daemon.yaml` plus one YAML file per channel;
scripts are separate `.js` files referenced by path (relative to the
channel file). Unknown keys are load-time errors. `retention: -1` keeps
messages forever; queue-referenced messages are never pruned.

## Development

```sh
make check    # the pre-push gate: gofmt check + go vet + race-enabled tests
make test     # plain test run (unit, functional, E2E — no network needed)
make cover    # race tests with a coverage summary
make fuzz     # 30s of parser fuzzing per format (FUZZTIME=2m make fuzz for longer)
make bench    # parser + pipeline benchmarks
```

`make help` lists everything; the plain `go test ./...` / `go vet ./...`
commands behind these targets work as ever.

Package map: `internal/message` (tree, states, diff) · `internal/format/*`
(data types) · `internal/adapter/*` (Channel Adapters + registry) ·
`internal/channel` (pipeline) · `internal/script` (goja) ·
`internal/store` (SQLite) · `internal/queue` (delivery workers) ·
`internal/engine` (composition root) · `internal/api` (REST/SSE + web UI)
· `internal/tui` (observer).

Delivery is at-least-once: a crash between a successful send and its
acknowledgment in the store can re-send one message on restart. Design
receivers idempotent (HL7 receivers generally are, keyed on MSH-10).
