// Package testutil is the shared harness for engine-level tests: a
// store-backed engine with a script engine, channels started from YAML
// text or from the shipped examples, fake acknowledging receivers for the
// far end of a channel, and the small helpers (wait for a condition, read a
// path out of raw bytes) that every end-to-end test otherwise reinvents.
//
// It is importable from any external test package; internal tests of the
// engine package cannot use it (import cycle), which is why the engine's
// end-to-end tests live in package engine_test.
package testutil

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/adapter/astm1381"
	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"

	// Every adapter and format the examples use.
	_ "github.com/langhorst/waggle/internal/adapter/file"
	_ "github.com/langhorst/waggle/internal/adapter/httpin"
	_ "github.com/langhorst/waggle/internal/adapter/httpout"
	_ "github.com/langhorst/waggle/internal/format/astm"
	_ "github.com/langhorst/waggle/internal/format/csvfmt"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
	_ "github.com/langhorst/waggle/internal/format/jsonfmt"
	_ "github.com/langhorst/waggle/internal/format/xmlfmt"
)

// Timeout is the single deadline every wait in the harness uses. It is
// generous because the race detector and CI runners are slow; a passing
// test never waits this long.
const Timeout = 15 * time.Second

// Fixture is one engine under test with everything it needs.
type Fixture struct {
	Work    string // scratch directory (t.TempDir)
	Store   *store.Store
	Scripts *script.Engine
	Eng     *engine.Engine
	Bus     *events.Bus

	events <-chan events.Event
	seen   []events.Event
	mu     sync.Mutex
}

// Option customizes NewFixture.
type Option func(*fixtureOptions)

type fixtureOptions struct {
	memory bool
}

// InMemory builds the engine without a store (memory recorder, synchronous
// delivery) for tests that only care about pipeline behaviour.
func InMemory() Option { return func(o *fixtureOptions) { o.memory = true } }

// NewFixture boots a store, a script engine, an event bus, and an engine,
// all torn down through t.Cleanup in the right order.
func NewFixture(t *testing.T, opts ...Option) *Fixture {
	t.Helper()
	var o fixtureOptions
	for _, opt := range opts {
		opt(&o)
	}
	f := &Fixture{Work: t.TempDir(), Bus: events.NewBus()}
	log := slog.New(slog.DiscardHandler)

	f.Scripts = script.New(script.Options{Log: log})
	t.Cleanup(f.Scripts.Close)
	if !o.memory {
		st, err := store.Open(filepath.Join(f.Work, "messages.db"))
		if err != nil {
			t.Fatal(err)
		}
		f.Store = st
	}
	f.Eng = engine.New(engine.Options{Store: f.Store, Scripts: f.Scripts, Bus: f.Bus, Log: log})
	// Shutdown before the store closes: workers must not race a closed DB.
	if f.Store != nil {
		t.Cleanup(func() { f.Store.Close() })
	}
	t.Cleanup(f.Eng.Shutdown)

	evs, cancel := f.Bus.Subscribe(1024)
	f.events = evs
	t.Cleanup(cancel)
	return f
}

// StartChannelYAML writes yaml as <Work>/<name>.yaml, loads it, starts the
// channel, and returns it. Relative script paths in the YAML resolve
// against Work.
func (f *Fixture) StartChannelYAML(t *testing.T, name, yaml string) *channel.Channel {
	t.Helper()
	path := filepath.Join(f.Work, name+".yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return f.startChannelFile(t, path)
}

// StartExample loads examples/channels/<name>.yaml, the file operators
// actually ship, with textual rewrites applied (listen addresses to
// 127.0.0.1:0, peer addresses to a test receiver, output directories to
// Work). Script references stay pointed at the examples' own scripts.
func (f *Fixture) StartExample(t *testing.T, name string, rewrites map[string]string) *channel.Channel {
	t.Helper()
	dir := ExamplesDir(t)
	raw, err := os.ReadFile(filepath.Join(dir, name+".yaml"))
	if err != nil {
		t.Fatalf("example channel: %v", err)
	}
	text := string(raw)
	for from, to := range rewrites {
		if !strings.Contains(text, from) {
			t.Fatalf("example %s.yaml does not contain %q (rewrite target drifted)", name, from)
		}
		text = strings.ReplaceAll(text, from, to)
	}
	// Scripts are referenced relative to the channel file; the copy lives
	// in Work, so point them at the examples directory explicitly.
	text = strings.ReplaceAll(text, "scripts/", filepath.Join(dir, "scripts")+string(filepath.Separator))
	return f.StartChannelYAML(t, name, text)
}

func (f *Fixture) startChannelFile(t *testing.T, path string) *channel.Channel {
	t.Helper()
	cfg, err := config.LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}
	if err := f.Eng.Start(context.Background(), cfg.ID); err != nil {
		t.Fatal(err)
	}
	ch, ok := f.Eng.Channel(cfg.ID)
	if !ok {
		t.Fatalf("channel %s not registered after load", cfg.ID)
	}
	return ch
}

// ExamplesDir is the absolute path of examples/channels.
func ExamplesDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate testutil source")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "examples", "channels")
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("examples dir: %v", err)
	}
	return abs
}

// ExampleScript is the absolute path of one shipped script.
func ExampleScript(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(ExamplesDir(t), "scripts", name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("example script: %v", err)
	}
	return path
}

// ListenAddr is the bound address of a channel's inbound adapter.
func ListenAddr(t *testing.T, ch *channel.Channel) string {
	t.Helper()
	a, ok := ch.Source.(adapter.Addresser)
	if !ok {
		t.Fatalf("channel %s: source %T has no listen address", ch.ID, ch.Source)
	}
	addr := a.Addr()
	if addr == "" {
		t.Fatalf("channel %s: source not started", ch.ID)
	}
	return addr
}

// Eventually polls cond until it holds or Timeout elapses.
func Eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(Timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", Timeout, what)
}

// WaitEvent blocks until an event matching pred has been published since
// the fixture was created. Events are retained, so a match that happened
// before the call is found immediately.
func (f *Fixture) WaitEvent(t *testing.T, what string, pred func(events.Event) bool) events.Event {
	t.Helper()
	f.mu.Lock()
	for _, ev := range f.seen {
		if pred(ev) {
			f.mu.Unlock()
			return ev
		}
	}
	f.mu.Unlock()
	deadline := time.After(Timeout)
	for {
		select {
		case ev, ok := <-f.events:
			if !ok {
				t.Fatalf("event bus closed while waiting for %s", what)
			}
			f.mu.Lock()
			f.seen = append(f.seen, ev)
			f.mu.Unlock()
			if pred(ev) {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out after %v waiting for event: %s", Timeout, what)
		}
	}
}

// WaitMessageState waits for a message event with the given pipeline state
// on channelID (destID "" for pipeline-level states) and returns its id.
func (f *Fixture) WaitMessageState(t *testing.T, channelID, destID string, state message.State) int64 {
	t.Helper()
	ev := f.WaitEvent(t, string(state)+" on "+channelID+"/"+destID, func(ev events.Event) bool {
		return ev.Type == events.TypeMessage && ev.ChannelID == channelID && ev.DestinationID == destID && ev.State == state
	})
	return ev.MessageID
}

// WaitForDestinationState polls the store until the newest message on
// channelID has destination dest in state and returns that message's id.
func (f *Fixture) WaitForDestinationState(t *testing.T, channelID, dest string, state message.State) int64 {
	t.Helper()
	if f.Store == nil {
		t.Fatal("WaitForDestinationState needs a store")
	}
	ctx := context.Background()
	var id int64
	Eventually(t, "destination "+dest+" "+string(state), func() bool {
		list, err := f.Store.ListMessages(ctx, channelID, store.ListQuery{Limit: 1})
		if err != nil || len(list) == 0 {
			return false
		}
		d, err := f.Store.GetMessage(ctx, list[0].ID)
		if err != nil {
			return false
		}
		for _, ds := range d.Destinations {
			if ds.DestinationID == dest && ds.State == state {
				id = d.ID
				return true
			}
		}
		return false
	})
	return id
}

// Getter parses raw as dataType and returns a function reading one path
// (first match, "" when absent) for assertions on delivered payloads.
func Getter(t *testing.T, dataType string, raw []byte) func(path string) string {
	t.Helper()
	dt, ok := format.Get(dataType)
	if !ok {
		t.Fatalf("unknown data type %q", dataType)
	}
	root, err := dt.Parse(raw)
	if err != nil {
		t.Fatalf("payload does not parse as %s: %v\n%q", dataType, err, raw)
	}
	return func(path string) string {
		nodes, err := dt.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}
}

// Receiver is a test-side inbound adapter playing the far system (a HIS,
// an instrument): it records what it receives and acknowledges everything.
type Receiver struct {
	listener adapter.Inbound
	addr     adapter.Addresser
	mu       sync.Mutex
	received [][]byte
}

// AckingReceiver starts a receiver of the given transport ("mllp" or
// "astm") on a free port.
func AckingReceiver(t *testing.T, transport string) *Receiver {
	t.Helper()
	r := &Receiver{}
	settings := map[string]any{"addr": "127.0.0.1:0"}
	var err error
	switch transport {
	case "mllp":
		var l *mllp.Listener
		l, err = mllp.NewListener(settings)
		r.listener, r.addr = l, l
	case "astm":
		var l *astm1381.Listener
		l, err = astm1381.NewListener(settings)
		r.listener, r.addr = l, l
	default:
		t.Fatalf("unknown transport %q", transport)
	}
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		r.mu.Lock()
		r.received = append(r.received, append([]byte(nil), raw...))
		n := len(r.received)
		r.mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{MessageID: int64(n), Done: done}, nil
	}
	if err := r.listener.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.listener.Stop() })
	return r
}

// Addr is the receiver's bound address.
func (r *Receiver) Addr() string { return r.addr.Addr() }

// Count is how many messages have arrived.
func (r *Receiver) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.received)
}

// Received is a copy of everything received so far.
func (r *Receiver) Received() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.received...)
}

// WaitFor blocks until at least n messages have arrived and returns the
// n-th (1-based).
func (r *Receiver) WaitFor(t *testing.T, n int) []byte {
	t.Helper()
	Eventually(t, "receiver message "+strconv.Itoa(n), func() bool { return r.Count() >= n })
	return r.Received()[n-1]
}

// WriteFile writes content to path, creating parent directories.
func WriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
