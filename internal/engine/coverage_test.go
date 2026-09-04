package engine_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"
	"github.com/langhorst/waggle/internal/testutil"
)

// TestPausedChannelKeepsDraining: pause stops intake but the delivery
// queue keeps working. Messages are queued while the receiver is down,
// the channel is paused, the receiver comes up, and the queue drains.
func TestPausedChannelKeepsDraining(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	inDir := filepath.Join(f.Work, "in")
	hisAddr := testutil.FreeAddr(t)

	f.StartChannelYAML(t, "drain", `
id: drain
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 30ms, minAge: 1ms}
destinations:
  - id: his
    adapter:
      type: mllp-sender
      settings: {addr: "`+hisAddr+`", connectTimeout: 200ms}
    queue: {retryInterval: 20ms, maxAttempts: -1}
`)
	for i := 0; i < 3; i++ {
		testutil.WriteFile(t, filepath.Join(inDir, "m"+string(rune('0'+i))+".hl7"),
			strings.Replace(sampleHL7, "CTRL001", "CTRL00"+string(rune('0'+i)), 1))
	}
	// All three are queued and failing to connect.
	testutil.Eventually(t, "three queued deliveries", func() bool {
		depth, err := f.Store.QueueDepth(ctx, "drain")
		return err == nil && depth["his"] == 3
	})

	if err := f.Eng.Pause("drain"); err != nil {
		t.Fatal(err)
	}
	ch, _ := f.Eng.Channel("drain")
	if ch.Status() != channel.StatusPaused {
		t.Fatalf("status = %s", ch.Status())
	}

	his := testutil.AckingReceiverAt(t, "mllp", hisAddr)
	testutil.Eventually(t, "queue drained while paused", func() bool { return his.Count() == 3 })
	if ch.Status() != channel.StatusPaused {
		t.Errorf("draining changed the status to %s", ch.Status())
	}
	summary, err := f.Eng.ChannelSummary(ctx, "drain")
	if err != nil || summary.Sent() != 3 || summary.Queued() != 0 {
		t.Errorf("summary = %+v, %v", summary, err)
	}
}

// TestReloadAppliesChangedConfig: a reloaded channel picks up its rewritten
// YAML (here a different transformer), and a file that now defines a
// different channel id is refused.
func TestReloadAppliesChangedConfig(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	inDir := filepath.Join(f.Work, "in")
	outDir := filepath.Join(f.Work, "out")
	testutil.WriteFile(t, filepath.Join(f.Work, "scripts", "v1.js"),
		`function transform(msg) { msg.set('PID-5.1', 'V1'); }`)
	testutil.WriteFile(t, filepath.Join(f.Work, "scripts", "v2.js"),
		`function transform(msg) { msg.set('PID-5.1', 'V2'); }`)
	yaml := func(script string) string {
		return `
id: reloadable
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: ` + inDir + `, interval: 30ms, minAge: 1ms}
transformers: [scripts/` + script + `]
destinations:
  - id: out
    adapter:
      type: file-writer
      settings: {dir: ` + outDir + `, pattern: "{id}.hl7"}
`
	}
	f.StartChannelYAML(t, "reloadable", yaml("v1.js"))
	testutil.WriteFile(t, filepath.Join(inDir, "a.hl7"), sampleHL7)
	first := f.WaitForDestinationState(t, "reloadable", "out", message.StateSent)
	if raw, _ := os.ReadFile(filepath.Join(outDir, "1.hl7")); !strings.Contains(string(raw), "V1") {
		t.Fatalf("first output = %q", raw)
	}

	// Rewrite the YAML in place and reload: the running state is kept.
	testutil.WriteFile(t, filepath.Join(f.Work, "reloadable.yaml"), yaml("v2.js"))
	if err := f.Eng.ReloadChannel(ctx, "reloadable"); err != nil {
		t.Fatal(err)
	}
	ch, _ := f.Eng.Channel("reloadable")
	if ch.Status() != channel.StatusStarted {
		t.Fatalf("status after reload = %s", ch.Status())
	}
	testutil.WriteFile(t, filepath.Join(inDir, "b.hl7"), strings.Replace(sampleHL7, "CTRL001", "CTRL002", 1))
	testutil.Eventually(t, "second message delivered", func() bool {
		id := f.WaitForDestinationState(t, "reloadable", "out", message.StateSent)
		return id != first
	})
	entries, _ := os.ReadDir(outDir)
	sawV2 := false
	for _, e := range entries {
		raw, _ := os.ReadFile(filepath.Join(outDir, e.Name()))
		if strings.Contains(string(raw), "V2") {
			sawV2 = true
		}
	}
	if !sawV2 {
		t.Error("reload did not apply the new transformer")
	}

	// A file that now defines another channel cannot be reloaded in place.
	testutil.WriteFile(t, filepath.Join(f.Work, "reloadable.yaml"), strings.Replace(yaml("v2.js"), "id: reloadable", "id: renamed", 1))
	if err := f.Eng.ReloadChannel(ctx, "reloadable"); err == nil || !strings.Contains(err.Error(), "now defines channel") {
		t.Errorf("reload with a changed id: %v", err)
	}
}

// TestStartEnabledSkipsDisabledChannels: enabled: false keeps a channel
// loaded but stopped.
func TestStartEnabledSkipsDisabledChannels(t *testing.T) {
	f := testutil.NewFixture(t, testutil.InMemory())
	off := false
	for _, cfg := range []*config.Channel{
		{ID: "on", Source: config.Source{Type: "file-reader", DataType: "hl7v2", Settings: map[string]any{"dir": filepath.Join(f.Work, "on")}},
			Destinations: []config.Destination{{ID: "out", Adapter: config.AdapterRef{Type: "file-writer", Settings: map[string]any{"dir": filepath.Join(f.Work, "out")}}}}},
		{ID: "off", Enabled: &off, Source: config.Source{Type: "file-reader", DataType: "hl7v2", Settings: map[string]any{"dir": filepath.Join(f.Work, "off")}},
			Destinations: []config.Destination{{ID: "out", Adapter: config.AdapterRef{Type: "file-writer", Settings: map[string]any{"dir": filepath.Join(f.Work, "out")}}}}},
	} {
		cfg.Normalize()
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := f.Eng.LoadChannel(cfg); err != nil {
			t.Fatal(err)
		}
	}
	if errs := f.Eng.StartEnabled(context.Background()); len(errs) != 0 {
		t.Fatal(errs)
	}
	want := map[string]channel.Status{"on": channel.StatusStarted, "off": channel.StatusStopped}
	for _, info := range f.Eng.Channels() {
		if info.Status != want[info.ID] {
			t.Errorf("channel %s status = %s, want %s", info.ID, info.Status, want[info.ID])
		}
	}
}

// TestQueueSurvivesRestart: a delivery queued when the process stops is
// delivered by the next process. This is the Guaranteed Delivery promise
// at engine level: stop the engine mid-retry, reopen the same database,
// bring the receiver up, and the message arrives.
func TestQueueSurvivesRestart(t *testing.T) {
	work := t.TempDir()
	inDir := filepath.Join(work, "in")
	hisAddr := testutil.FreeAddr(t)
	dbPath := filepath.Join(work, "messages.db")
	yamlPath := filepath.Join(work, "durable.yaml")
	testutil.WriteFile(t, yamlPath, `
id: durable
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 30ms, minAge: 1ms}
destinations:
  - id: his
    adapter:
      type: mllp-sender
      settings: {addr: "`+hisAddr+`", connectTimeout: 200ms}
    queue: {retryInterval: 20ms, maxAttempts: -1}
`)
	log := slog.New(slog.DiscardHandler)
	boot := func() (*engine.Engine, *store.Store) {
		st, err := store.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		scripts := script.New(script.Options{Log: log})
		t.Cleanup(scripts.Close)
		eng := engine.New(engine.Options{Store: st, Scripts: scripts, Log: log})
		cfg, err := config.LoadChannel(yamlPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.LoadChannel(cfg); err != nil {
			t.Fatal(err)
		}
		if err := eng.Start(context.Background(), "durable"); err != nil {
			t.Fatal(err)
		}
		return eng, st
	}

	// Process one: the receiver is down, the delivery is queued and retrying.
	eng1, st1 := boot()
	testutil.WriteFile(t, filepath.Join(inDir, "a.hl7"), sampleHL7)
	testutil.Eventually(t, "delivery queued and attempted", func() bool {
		list, err := st1.ListMessages(context.Background(), "durable", store.ListQuery{Limit: 1})
		if err != nil || len(list) == 0 {
			return false
		}
		d, err := st1.GetMessage(context.Background(), list[0].ID)
		return err == nil && len(d.Destinations) == 1 && d.Destinations[0].Attempts >= 1
	})
	eng1.Shutdown()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}

	// Process two: same database, receiver now up.
	his := testutil.AckingReceiverAt(t, "mllp", hisAddr)
	eng2, st2 := boot()
	t.Cleanup(eng2.Shutdown)
	t.Cleanup(func() { st2.Close() })
	testutil.Eventually(t, "queued delivery sent after restart", func() bool { return his.Count() == 1 })
	if got := string(his.Received()[0]); got != sampleHL7 {
		t.Errorf("delivered %q", got)
	}
	// MarkSent follows the send; wait for the queue row to go.
	testutil.Eventually(t, "queue row removed", func() bool {
		depth, err := st2.QueueDepth(context.Background(), "durable")
		return err == nil && depth["his"] == 0
	})
}

// TestDestinationAckWithQueuedDestination: in destination-ACK mode a
// destination that is not waitForAck goes through the queue, so the source
// gets AA as soon as the pipeline finishes, before delivery happens.
func TestDestinationAckWithQueuedDestination(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	hisAddr := testutil.FreeAddr(t) // nothing listens yet: delivery must wait

	ch := f.StartChannelYAML(t, "queued-ack", `
id: queued-ack
source:
  type: mllp-listener
  dataType: hl7v2
  settings: {addr: "127.0.0.1:0", ackMode: destination, holdTimeout: 10s}
destinations:
  - id: his
    adapter:
      type: mllp-sender
      settings: {addr: "`+hisAddr+`", connectTimeout: 200ms}
    queue: {retryInterval: 20ms, maxAttempts: -1}
`)
	sender, err := mllp.NewSender(map[string]any{"addr": testutil.ListenAddr(t, ch), "ackTimeout": "10s"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.Send(ctx, []byte(sampleHL7), nil); err != nil {
		t.Fatalf("expected AA before delivery (queued destination), got %v", err)
	}
	depth, _ := f.Store.QueueDepth(ctx, "queued-ack")
	if depth["his"] != 1 {
		t.Fatalf("delivery not queued: depth %v", depth)
	}
	his := testutil.AckingReceiverAt(t, "mllp", hisAddr)
	testutil.Eventually(t, "queued delivery arrives", func() bool { return his.Count() == 1 })
}
