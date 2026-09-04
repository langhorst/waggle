package engine_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
	"github.com/langhorst/waggle/internal/testutil"
)

const sampleHL7 = "MSH|^~\\&|SEND|SFAC|RECV|RFAC|20260730||ADT^A01|CTRL001|P|2.5\rPID|1||MRN1||DOE^JOHN\r"

// TestFileToFileEndToEnd: a config-defined channel moving a message from a
// file source to a file destination through the full engine, no scripts,
// no store.
func TestFileToFileEndToEnd(t *testing.T) {
	f := testutil.NewFixture(t, testutil.InMemory())
	inDir := filepath.Join(f.Work, "in")
	outDir := filepath.Join(f.Work, "out")

	f.StartChannelYAML(t, "file-passthrough", `
id: file-passthrough
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 50ms, minAge: 1ms}
destinations:
  - id: to-file
    adapter:
      type: file-writer
      settings: {dir: `+outDir+`, pattern: "{id}.hl7"}
`)
	testutil.WriteFile(t, filepath.Join(inDir, "msg1.hl7"), sampleHL7)

	f.WaitMessageState(t, "file-passthrough", "to-file", message.StateSent)
	entries, err := os.ReadDir(outDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("out dir: %v, %d entries", err, len(entries))
	}
	raw, err := os.ReadFile(filepath.Join(outDir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != sampleHL7 {
		t.Errorf("output = %q, want canonical round trip of input", raw)
	}
	testutil.Eventually(t, "input moved to processed", func() bool {
		processed, _ := os.ReadDir(filepath.Join(inDir, "processed"))
		return len(processed) == 1
	})
}

// TestStoreBackedQueueAndReplay: messages flow through the persistent queue
// to their destination, everything is recorded in SQLite, and a stored
// message can be replayed through the pipeline or requeued to one
// destination.
func TestStoreBackedQueueAndReplay(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	inDir := filepath.Join(f.Work, "in")
	outDir := filepath.Join(f.Work, "out")

	f.StartChannelYAML(t, "persisted", `
id: persisted
retention: 100
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 50ms, minAge: 1ms}
destinations:
  - id: to-file
    adapter:
      type: file-writer
      settings: {dir: `+outDir+`, pattern: "{id}.hl7"}
    queue: {retryInterval: 10ms}
`)
	testutil.WriteFile(t, filepath.Join(inDir, "m1.hl7"), sampleHL7)

	msgID := f.WaitForDestinationState(t, "persisted", "to-file", message.StateSent)
	d, err := f.Store.GetMessage(ctx, msgID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != message.StateTransformed || len(d.Raw) == 0 || len(d.Transformed) == 0 {
		t.Errorf("stored message = state %s, raw %d bytes, transformed %d bytes", d.State, len(d.Raw), len(d.Transformed))
	}

	// Replay: same raw re-enters the pipeline as a new message.
	replayID, err := f.Eng.Replay(ctx, msgID, "")
	if err != nil {
		t.Fatal(err)
	}
	if replayID == msgID || replayID == 0 {
		t.Fatalf("replay id = %d", replayID)
	}
	testutil.Eventually(t, "replay delivered", func() bool {
		rd, err := f.Store.GetMessage(ctx, replayID)
		return err == nil && len(rd.Destinations) == 1 && rd.Destinations[0].State == message.StateSent
	})
	rd, _ := f.Store.GetMessage(ctx, replayID)
	if rd.ReplayOf != msgID || rd.CorrelationID != d.CorrelationID {
		t.Errorf("replay lineage: %+v", rd.MessageSummary)
	}
	entries, _ := os.ReadDir(outDir)
	if len(entries) != 2 {
		t.Errorf("expected original + replay output files, got %d", len(entries))
	}

	// Destination requeue: re-send the stored payload without re-transform.
	if _, err := f.Eng.Replay(ctx, msgID, "to-file"); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, "requeued payload delivered", func() bool {
		entries, _ := os.ReadDir(outDir)
		return len(entries) == 3
	})
	if _, err := f.Store.ListMessages(ctx, "persisted", store.ListQuery{}); err != nil {
		t.Fatal(err)
	}
}

func TestChannelLifecycleViaEngine(t *testing.T) {
	work := t.TempDir()
	cfg := &config.Channel{
		ID:   "lc",
		Name: "lc",
		Source: config.Source{
			Type:     "file-reader",
			DataType: "hl7v2",
			Settings: map[string]any{"dir": filepath.Join(work, "in")},
		},
		Destinations: []config.Destination{{
			ID:      "out",
			Adapter: config.AdapterRef{Type: "file-writer", Settings: map[string]any{"dir": filepath.Join(work, "out")}},
		}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	eng := engine.New(engine.Options{Log: slog.New(slog.DiscardHandler)})
	if err := eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := eng.Start(ctx, "lc"); err != nil {
		t.Fatal(err)
	}
	if infos := eng.Channels(); infos[0].Status != channel.StatusStarted {
		t.Errorf("status = %s", infos[0].Status)
	}
	if err := eng.Pause("lc"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx, "lc"); err != nil { // resume via Start
		t.Fatal(err)
	}
	if err := eng.Stop("lc"); err != nil {
		t.Fatal(err)
	}
	if infos := eng.Channels(); infos[0].Status != channel.StatusStopped {
		t.Errorf("status = %s", infos[0].Status)
	}
	if err := eng.Start(ctx, "nope"); err == nil || !strings.Contains(err.Error(), "unknown channel") {
		t.Errorf("expected unknown channel error, got %v", err)
	}
}

// TestScriptedFormatConversion: a channel driven purely by YAML config plus
// .js scripts (channel filter, channel transformer chain, per-destination
// transformers converting HL7 to CSV and to ASTM).
func TestScriptedFormatConversion(t *testing.T) {
	f := testutil.NewFixture(t, testutil.InMemory())
	inDir := filepath.Join(f.Work, "in")
	csvDir := filepath.Join(f.Work, "csv")
	astmDir := filepath.Join(f.Work, "astm")
	scriptsDir := filepath.Join(f.Work, "scripts")

	testutil.WriteFile(t, filepath.Join(scriptsDir, "only-adt.js"),
		`function filter(msg) { return msg.get('MSH-9.1') === 'ADT'; }`)
	testutil.WriteFile(t, filepath.Join(scriptsDir, "uppercase-name.js"),
		`function transform(msg) { msg.set('PID-5.1', msg.get('PID-5.1').toUpperCase()); }`)
	testutil.WriteFile(t, filepath.Join(scriptsDir, "to-csv.js"), `
function transform(msg) {
	var out = newMessage('csv');
	out.set('R.1', msg.get('PID-3'));
	out.set('R.2', msg.get('PID-5.1'));
	out.set('R.3', msg.get('PID-5.2'));
	return out;
}`)
	testutil.WriteFile(t, filepath.Join(scriptsDir, "to-astm.js"), `
function transform(msg) {
	var out = newMessage('astm');
	out.set('H-1', '|');
	out.set('H-2', '\\^&');
	out.set('P-1', '1');
	out.set('P-3', msg.get('PID-3'));
	out.set('P-5.1', msg.get('PID-5.1'));
	out.set('P-5.2', msg.get('PID-5.2'));
	out.set('L-1', '1');
	return out;
}`)

	f.StartChannelYAML(t, "hl7-fanout", `
id: hl7-fanout
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 50ms, minAge: 1ms}
filter: scripts/only-adt.js
transformers: [scripts/uppercase-name.js]
destinations:
  - id: csv-out
    dataType: csv
    transformers: [scripts/to-csv.js]
    adapter:
      type: file-writer
      settings: {dir: `+csvDir+`, pattern: "{id}.csv"}
  - id: astm-out
    dataType: astm
    transformers: [scripts/to-astm.js]
    adapter:
      type: file-writer
      settings: {dir: `+astmDir+`, pattern: "{id}.astm"}
`)

	// One ADT (passes filter) and one ORU (dropped).
	oru := strings.Replace(sampleHL7, "ADT^A01", "ORU^R01", 1)
	testutil.WriteFile(t, filepath.Join(inDir, "a-adt.hl7"), sampleHL7)
	testutil.WriteFile(t, filepath.Join(inDir, "b-oru.hl7"), oru)

	f.WaitMessageState(t, "hl7-fanout", "csv-out", message.StateSent)
	f.WaitMessageState(t, "hl7-fanout", "astm-out", message.StateSent)
	// The ORU's pipeline has finished once its FILTERED event is out; no
	// output can appear for it after that.
	f.WaitMessageState(t, "hl7-fanout", "", message.StateFiltered)

	csvs, _ := os.ReadDir(csvDir)
	if len(csvs) != 1 {
		t.Fatalf("csv outputs = %d, want 1 (filtered ORU must not produce output)", len(csvs))
	}
	raw, _ := os.ReadFile(filepath.Join(csvDir, csvs[0].Name()))
	if string(raw) != "MRN1,DOE,JOHN\n" {
		t.Errorf("csv output = %q", raw)
	}
	astms, _ := os.ReadDir(astmDir)
	if len(astms) != 1 {
		t.Fatalf("astm outputs = %d, want 1", len(astms))
	}
	raw, _ = os.ReadFile(filepath.Join(astmDir, astms[0].Name()))
	want := "H|\\^&\rP|1||MRN1||DOE^JOHN\rL|1\r"
	if string(raw) != want {
		t.Errorf("astm output = %q, want %q", raw, want)
	}
}

func TestScriptsRejectedWithoutEngine(t *testing.T) {
	cfg := &config.Channel{
		ID:     "scripted",
		Name:   "scripted",
		Filter: "f.js",
		Source: config.Source{Type: "file-reader", DataType: "hl7v2", Settings: map[string]any{"dir": "x"}},
		Destinations: []config.Destination{{
			ID:      "out",
			Adapter: config.AdapterRef{Type: "file-writer", Settings: map[string]any{"dir": "y"}},
		}},
	}
	eng := engine.New(engine.Options{Log: slog.New(slog.DiscardHandler)})
	if err := eng.LoadChannel(cfg); err == nil || !strings.Contains(err.Error(), "no script engine") {
		t.Errorf("expected script engine error, got %v", err)
	}
}
