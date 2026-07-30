package engine

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
	"github.com/langhorst/integration-channel/internal/adapter/astm1381"
	"github.com/langhorst/integration-channel/internal/adapter/mllp"
	"github.com/langhorst/integration-channel/internal/config"
	"github.com/langhorst/integration-channel/internal/format"
	"github.com/langhorst/integration-channel/internal/message"
	"github.com/langhorst/integration-channel/internal/script"
	"github.com/langhorst/integration-channel/internal/store"
)

// instrumentSide is a test E1381 receiver playing the instrument.
type instrumentSide struct {
	listener *astm1381.Listener
	mu       sync.Mutex
	received [][]byte
}

func newInstrumentSide(t *testing.T) *instrumentSide {
	t.Helper()
	inst := &instrumentSide{}
	l, err := astm1381.NewListener(map[string]any{"addr": "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		inst.mu.Lock()
		inst.received = append(inst.received, append([]byte(nil), raw...))
		inst.mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	if err := l.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Stop() })
	inst.listener = l
	return inst
}

func (i *instrumentSide) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.received)
}

func (i *instrumentSide) waitForMessage(t *testing.T, n int) []byte {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		i.mu.Lock()
		if len(i.received) >= n {
			msg := i.received[n-1]
			i.mu.Unlock()
			return msg
		}
		i.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("instrument never received message %d", n)
	return nil
}

// astmGetter parses raw ASTM and returns a path getter for assertions.
func astmGetter(t *testing.T, raw []byte) func(string) string {
	t.Helper()
	dt, _ := format.Get("astm")
	root, err := dt.Parse(raw)
	if err != nil {
		t.Fatalf("output does not parse as ASTM: %v\n%q", err, raw)
	}
	return func(path string) string {
		nodes, err := dt.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}
}

func exampleScript(t *testing.T, name string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "examples", "channels", "scripts", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("example script missing: %v", err)
	}
	return path
}

func newOrderTestEngine(t *testing.T, work string) *Engine {
	t.Helper()
	st, err := store.Open(filepath.Join(work, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	scripts := script.New(script.Options{Log: slog.New(slog.DiscardHandler)})
	t.Cleanup(scripts.Close)
	eng := New(Options{Store: st, Scripts: scripts, Log: slog.New(slog.DiscardHandler)})
	t.Cleanup(eng.Shutdown)
	return eng
}

func loadChannelYAML(t *testing.T, eng *Engine, work, name, yaml string) {
	t.Helper()
	path := filepath.Join(work, name)
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background(), cfg.ID); err != nil {
		t.Fatal(err)
	}
}

// TestOrderDownloadEndToEnd: an HL7 ORM^O01 with two ORC/OBR pairs arrives
// over MLLP and reaches the instrument as an ASTM order message over E1381,
// with order control codes mapped onto O-11 action codes.
func TestOrderDownloadEndToEnd(t *testing.T) {
	work := t.TempDir()
	ctx := context.Background()
	instrument := newInstrumentSide(t)
	eng := newOrderTestEngine(t, work)

	loadChannelYAML(t, eng, work, "orders.yaml", `
id: orders-to-instrument
source:
  type: mllp-listener
  dataType: hl7v2
  settings: {addr: "127.0.0.1:0"}
transformers: ["`+exampleScript(t, "hl7-orm-to-astm.js")+`"]
destinations:
  - id: to-instrument
    dataType: astm
    adapter:
      type: astm-sender
      settings: {addr: "`+instrument.listener.Addr()+`"}
    queue: {retryInterval: 20ms}
`)
	ch, _ := eng.Channel("orders-to-instrument")
	mllpAddr := ch.Source.(*mllp.Listener).Addr()

	orm := "MSH|^~\\&|HIS|HOSP|LIS|LAB|20260730120000||ORM^O01|ORD001|P|2.5.1\r" +
		"PID|1||MRN77||ROE^RICHARD||19751224|M\r" +
		"ORC|NW|PLACER001\r" +
		"OBR|1|PLACER001||GLU^Glucose|S||20260730120500\r" +
		"ORC|CA|PLACER002\r" +
		"OBR|2|PLACER002||CBC^Blood Count|R||20260730120600\r"
	sender, err := mllp.NewSender(map[string]any{"addr": mllpAddr, "ackTimeout": "10s"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.Send(ctx, []byte(orm), nil); err != nil {
		t.Fatalf("ORM send: %v", err)
	}

	get := astmGetter(t, instrument.waitForMessage(t, 1))
	for path, want := range map[string]string{
		"H-5":      "HIS",
		"P-3":      "MRN77",
		"P-5.1":    "ROE",
		"O-1":      "1",
		"O-2":      "PLACER001",
		"O-4.4":    "GLU",
		"O-5":      "S",
		"O-6":      "20260730120500",
		"O-11":     "N", // ORC-1 NW → new order
		"O[2]-2":   "PLACER002",
		"O[2]-4.4": "CBC",
		"O[2]-5":   "R",
		"O[2]-11":  "C", // ORC-1 CA → cancel
		"L-1":      "1",
	} {
		if got := get(path); got != want {
			t.Errorf("order %s = %q, want %q", path, got, want)
		}
	}

	counts, err := eng.Store().MessageCounts(ctx, "orders-to-instrument")
	if err != nil || counts[message.StateSent] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
}

// TestQueryResponseRoundTrip: the instrument queries for a specimen over
// E1381 and receives an order response in a second, opposite session.
// Non-query ASTM traffic is FILTERED and never reaches the responder.
func TestQueryResponseRoundTrip(t *testing.T) {
	work := t.TempDir()
	ctx := context.Background()
	instrument := newInstrumentSide(t)
	eng := newOrderTestEngine(t, work)

	loadChannelYAML(t, eng, work, "query.yaml", `
id: instrument-query
source:
  type: astm-listener
  dataType: astm
  settings: {addr: "127.0.0.1:0"}
filter: "`+exampleScript(t, "only-queries.js")+`"
transformers: ["`+exampleScript(t, "astm-query-to-order.js")+`"]
destinations:
  - id: order-response
    dataType: astm
    adapter:
      type: astm-sender
      settings: {addr: "`+instrument.listener.Addr()+`"}
    queue: {retryInterval: 20ms}
`)
	ch, _ := eng.Channel("instrument-query")
	queryAddr := ch.Source.(*astm1381.Listener).Addr()

	sender, err := astm1381.NewSender(map[string]any{"addr": queryAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	query := "H|\\^&|||INSTR|||||||P|LIS2-A2|20260730121500\r" +
		"Q|1|^SID999|^^^GLU|O\r" +
		"L|1|N\r"
	if err := sender.Send(ctx, []byte(query), nil); err != nil {
		t.Fatalf("query send: %v", err)
	}

	get := astmGetter(t, instrument.waitForMessage(t, 1))
	for path, want := range map[string]string{
		"O-2":   "SID999", // the queried specimen
		"O-4.4": "GLU",    // the requested test
		"O-11":  "Q",      // response to query
		"L-2":   "F",      // final: no more data
		"H-14":  "20260730121500",
	} {
		if got := get(path); got != want {
			t.Errorf("response %s = %q, want %q", path, got, want)
		}
	}

	// A result message (no Q record) on the same channel is FILTERED: the
	// transport accepts it, but the responder never fires.
	result := "H|\\^&|||INSTR\r" +
		"P|1||PATID123\r" +
		"R|1|^^^GLU|105|mg/dL\r" +
		"L|1|N\r"
	if err := sender.Send(ctx, []byte(result), nil); err != nil {
		t.Fatalf("result send: %v", err)
	}
	waitForCondition(t, "result message FILTERED", func() bool {
		counts, err := eng.Store().MessageCounts(ctx, "instrument-query")
		return err == nil && counts[message.StateFiltered] == 1
	})
	time.Sleep(150 * time.Millisecond) // give a wrong response time to appear
	if n := instrument.count(); n != 1 {
		t.Errorf("instrument received %d messages; filtered traffic must not produce responses", n)
	}
}

func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
