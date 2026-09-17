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
make tui    # attach the read-only observer TUI to that daemon (in another terminal)
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
| Polling Consumer           | the `file-reader` source (`ackMode` like the network sources)     |
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
formats; format modules (`hl7v2`, `astm`, `csv`, `json`, `xml`) provide
parse, serialize, and a path dialect. Each dialect counts the way its
format's own ecosystem counts — HL7/ASTM/CSV are 1-based like their specs
and Mirth, JSON is 0-based like JavaScript/JSONPath/jq, XML is 1-based
like XPath:

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
- XML: `Patient/name[1]/family`, `Patient/@id`, `entry/resource/id/@value`
  (an XPath-flavored subset — slash steps, 1-based predicates, `@attr`).
  Tags keep their namespace prefixes exactly as written; xmlns
  declarations round-trip as ordinary attributes. The canonical form is
  compact: formatting whitespace, comments, and DOCTYPE drop at parse,
  attribute order is preserved, and segment handles join with slashes
  (`msg.segments('entry')[0].get('resource/id/@value')`).

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

For the delimited formats, `msg.set` splits its value on the separators
below the addressed level, the way the parser splits wire text:
`msg.set('PID-5', 'DOE^JOHN')` produces two components and
`msg.set('PID-5', msg.get('PID-5'))` round-trips. Separators at or above
that level stay literal and are escaped on the wire. For JSON, a key step
on a non-empty array writes element 0 (what `get` reads), and an index
step on a non-empty object is an error; populated containers are never
replaced silently.

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
`meta['http.path']` and `meta['http.method']`; what the listener knew
about the inbound request lives under `meta['source.http.*']` (path,
method, query parameters, content type), a separate namespace so a
listener-to-sender channel never replays the inbound path. Failure classification
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
- `fhir-xml-webhook.yaml`: the same webhook contract accepting the Patient
  as FHIR **XML** (`fhir-xml-patient-to-adt.js` reads the value-attribute
  style: `Patient/name[1]/family/@value`) — two front doors, one HIS.
  Covered by `TestFHIRXMLWebhookToHL7`, including 400 for a non-Patient
  document and 500 for malformed XML.

**Intake.** Each channel processes messages on one goroutine, in arrival
order, from a bounded buffer (`maxPending`, default 256). When the buffer
is full the source adapter blocks and its transport ACK waits, so a burst
becomes backpressure on the sender rather than unbounded memory here.

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

The web UI (HTMX + Flowbite, embedded and offline, light and dark) is served at `/` and is
the full-control surface; the TUI is a read-only observer that attaches to
a running daemon over the same API and event stream:

```sh
waggle tui                                   # reads ./daemon.yaml for the address and credentials
waggle tui -config examples/daemon.yaml      # or point it at one elsewhere
waggle tui -addr 127.0.0.1:8420 -token ...   # or supply them directly
waggle tui -addr 127.0.0.1:8420 -user ops -password ...
```

It takes whichever credential the daemon is configured with, from the
config file, the flags, or `$WAGGLE_TOKEN` / `$WAGGLE_USER` /
`$WAGGLE_PASSWORD`. Flags win over the environment, which wins over the
config. A rejected credential says which kind was tried rather than just
`401`.

## A worked channel: ADT to CSV and FHIR

`examples/channels/adt-to-csv-and-fhir.yaml` is the end-to-end workflow:
an MLLP listener takes an ADT feed and fans each message out to a single
CSV (`id,last_name,first_name,dob`) and to a FHIR server as a Patient
resource. Point the simulator at it and the two should agree:

```sh
waggle daemon -config daemon.yaml
waggle-sim -mllp 127.0.0.1:2575 -day-in 10m

tail -f out/patients.csv
cut -d, -f1 out/patients.csv | tail -n +2 | sort -u | wc -l   # distinct MRNs
curl -s "$FHIR_BASE/Patient?_count=0" | jq .total             # Patients stored
```

The CSV logs a row per message, so an A08 that corrects a name appears as a
second row; the FHIR side writes `PUT [base]/Patient/<mrn>`, so the same
MRN is one resource however many messages arrive for it. The two counts
therefore reconcile as distinct-MRNs against Patients, not rows against
Patients.

The listener uses `ackMode: destination`, so the MLLP ACK is held until both
destinations have accepted. A NAK back to the simulator means the data
genuinely did not land.

## Simulating feeds

`waggle-sim` generates the traffic a hospital network's interfaces would,
so a channel can be exercised without a real EMR:

```sh
waggle-sim -mllp 127.0.0.1:2575 -day-in 10m   # live, paced
waggle-sim -dir ./in -days 3 -fast            # files, as fast as possible
waggle-sim -corpus ./corpus -days 30 -fast    # a replayable corpus
```

`-day-in` sets how long one simulated day takes, so the same run works as a
demo at ten minutes a day or a soak test at real time; `-fast` removes
pacing entirely. The rate is pacing only -- a given `-seed` produces the
same messages with the same timestamps at any speed, so a failure found
overnight reproduces in milliseconds.

It models one world (patients, encounters, finite beds) and renders feeds
from it, rather than generating messages independently. A visit is admitted
before it is transferred or discharged, a bed holds one patient, and the
census the feed implies matches the census the model holds. ADT (HL7 2.5.1,
A01/A02/A03/A08/A11) is the feed that exists today; orders and results are
renderers over the same events. Identity is per-facility MRN plus an
enterprise id, both sent in PID-3.

All of it is invented: the names, addresses and provider ids are synthetic,
and identifiers carry a facility-letter prefix no numeric MRN range can
collide with.

## Configuration

See `examples/`. One `daemon.yaml` plus one YAML file per channel;
scripts are separate `.js` files referenced by path (relative to the
channel file). Unknown keys are load-time errors. `retention: -1` keeps
messages forever; queue-referenced messages are never pruned.

**Auth.** The API and web UI are the full-control surface, so every route
requires credentials. There are no default credentials — nothing is
generated for you — and a daemon with none configured would reject every
request, so one of these is required or startup fails with a message
naming all three:

| setting | how you authenticate |
| --- | --- |
| `auth.token` | `Authorization: Bearer <token>`, and also the basic-auth **password with any user name**, so a browser prompt accepts it |
| `auth.basicUser` + `auth.basicPassword` | a named login for the browser prompt (both halves required) |
| `auth.disabled: true` | serve unauthenticated — only behind an authenticating reverse proxy, or on an isolated host |

The daemon listens on `127.0.0.1:8420` by default. `auth.disabled` on a
non-loopback address is allowed but warns loudly at startup, since anyone
who can reach the port controls every channel. State-changing requests
that carry a browser `Origin` or `Sec-Fetch-Site` header must be
same-origin.

## Development

```sh
make check    # the pre-push gate: lint + race-enabled tests (what CI runs)
make lint     # gofmt check + go vet + go mod tidy check + golangci-lint
make test     # plain test run (unit, functional, E2E — no network needed)
make cover    # race tests with a coverage summary
make fuzz     # 30s of parser fuzzing per format (FUZZTIME=2m make fuzz for longer)
make bench    # parser + pipeline benchmarks
```

`make help` lists everything; the plain `go test ./...` / `go vet ./...`
commands behind these targets work as ever. `make lint` needs
[golangci-lint](https://golangci-lint.run/docs/welcome/install/) on the
path; its configuration lives in `.golangci.yml`.

Package map: `internal/message` (tree, states, diff) · `internal/format/*`
(data types) · `internal/adapter/*` (Channel Adapters + registry) ·
`internal/channel` (pipeline) · `internal/script` (goja) ·
`internal/store` (SQLite) · `internal/queue` (delivery workers) ·
`internal/engine` (composition root) · `internal/api` (REST/SSE + web UI)
· `internal/tui` (observer).

Delivery is at-least-once: a crash between a successful send and its
acknowledgment in the store can re-send one message on restart. Design
receivers idempotent (HL7 receivers generally are, keyed on MSH-10).
