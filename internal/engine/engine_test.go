package engine

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"

	_ "github.com/langhorst/waggle/internal/adapter/file"
	_ "github.com/langhorst/waggle/internal/adapter/mllp"
	_ "github.com/langhorst/waggle/internal/format/astm"
	_ "github.com/langhorst/waggle/internal/format/csvfmt"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
)

const sampleHL7 = "MSH|^~\\&|SEND|SFAC|RECV|RFAC|20260730||ADT^A01|CTRL001|P|2.5\rPID|1||MRN1||DOE^JOHN\r"

// TestFileToFileEndToEnd is the phase 2 deliverable: a config-defined
// channel moving a message from a file source to a file destination through
// the full engine, no scripts involved.
func TestFileToFileEndToEnd(t *testing.T) {
	work := t.TempDir()
	inDir := filepath.Join(work, "in")
	outDir := filepath.Join(work, "out")

	chYAML := `
id: file-passthrough
source:
  type: file-reader
  dataType: hl7v2
  settings:
    dir: ` + inDir + `
    interval: 50ms
    minAge: 1ms
destinations:
  - id: to-file
    adapter:
      type: file-writer
      settings:
        dir: ` + outDir + `
        pattern: "{id}.hl7"
`
	chPath := filepath.Join(work, "channel.yaml")
	if err := os.WriteFile(chPath, []byte(chYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadChannel(chPath)
	if err != nil {
		t.Fatal(err)
	}

	bus := events.NewBus()
	evCh, cancel := bus.Subscribe(64)
	defer cancel()

	eng := New(Options{Bus: bus, Log: slog.New(slog.DiscardHandler)})
	if err := eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}
	if errs := eng.StartEnabled(context.Background()); len(errs) > 0 {
		t.Fatal(errs)
	}
	defer eng.Shutdown()

	if err := os.MkdirAll(inDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inDir, "msg1.hl7"), []byte(sampleHL7), 0o644); err != nil {
		t.Fatal(err)
	}

	// Wait for the SENT event.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-evCh:
			if ev.Type == events.TypeMessage && ev.State == "SENT" && ev.DestinationID == "to-file" {
				goto delivered
			}
		case <-deadline:
			t.Fatal("timed out waiting for SENT event")
		}
	}
delivered:
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
	// Source file consumed.
	processed, _ := os.ReadDir(filepath.Join(inDir, "processed"))
	if len(processed) != 1 {
		t.Errorf("input should be moved to processed, found %d", len(processed))
	}
}

// TestStoreBackedQueueAndReplay is the phase 3 deliverable: messages flow
// through the persistent queue to their destination, everything is recorded
// in SQLite, and a stored message can be replayed through the pipeline.
func TestStoreBackedQueueAndReplay(t *testing.T) {
	work := t.TempDir()
	inDir := filepath.Join(work, "in")
	outDir := filepath.Join(work, "out")

	st, err := store.Open(filepath.Join(work, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	chYAML := `
id: persisted
retention: 100
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: ` + inDir + `, interval: 50ms, minAge: 1ms}
destinations:
  - id: to-file
    adapter:
      type: file-writer
      settings: {dir: ` + outDir + `, pattern: "{id}.hl7"}
    queue: {retryInterval: 10ms}
`
	chPath := filepath.Join(work, "channel.yaml")
	if err := os.WriteFile(chPath, []byte(chYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadChannel(chPath)
	if err != nil {
		t.Fatal(err)
	}

	eng := New(Options{Store: st, Log: slog.New(slog.DiscardHandler)})
	if err := eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx, "persisted"); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown()

	if err := os.MkdirAll(inDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inDir, "m1.hl7"), []byte(sampleHL7), 0o644); err != nil {
		t.Fatal(err)
	}

	// The queue worker delivers and the store records the full lifecycle.
	var msgID int64
	waitFor(t, "message SENT in store", func() bool {
		list, err := st.ListMessages(ctx, "persisted", store.ListQuery{})
		if err != nil || len(list) == 0 {
			return false
		}
		msgID = list[0].ID
		d, err := st.GetMessage(ctx, msgID)
		if err != nil || len(d.Destinations) == 0 {
			return false
		}
		return d.Destinations[0].State == "SENT"
	})

	d, err := st.GetMessage(ctx, msgID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != "TRANSFORMED" || len(d.Raw) == 0 || len(d.Transformed) == 0 {
		t.Errorf("stored message = state %s, raw %d bytes, transformed %d bytes", d.State, len(d.Raw), len(d.Transformed))
	}

	// Replay: same raw re-enters the pipeline as a new message.
	replayID, err := eng.Replay(ctx, msgID, "")
	if err != nil {
		t.Fatal(err)
	}
	if replayID == msgID || replayID == 0 {
		t.Fatalf("replay id = %d", replayID)
	}
	waitFor(t, "replay delivered", func() bool {
		rd, err := st.GetMessage(ctx, replayID)
		return err == nil && len(rd.Destinations) == 1 && rd.Destinations[0].State == "SENT"
	})
	rd, _ := st.GetMessage(ctx, replayID)
	if rd.ReplayOf != msgID || rd.CorrelationID != d.CorrelationID {
		t.Errorf("replay lineage: %+v", rd.MessageSummary)
	}
	entries, _ := os.ReadDir(outDir)
	if len(entries) != 2 {
		t.Errorf("expected original + replay output files, got %d", len(entries))
	}

	// Destination requeue: re-send the stored payload without re-transform.
	if _, err := eng.Replay(ctx, msgID, "to-file"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "requeued payload delivered", func() bool {
		entries, _ := os.ReadDir(outDir)
		return len(entries) == 3
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
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
	eng := New(Options{Log: slog.New(slog.DiscardHandler)})
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

// TestScriptedFormatConversion is the phase 4 deliverable: a channel driven
// purely by YAML config plus .js scripts — channel filter, channel
// transformer chain, and per-destination transformers converting HL7 to CSV
// and to ASTM.
func TestScriptedFormatConversion(t *testing.T) {
	work := t.TempDir()
	inDir := filepath.Join(work, "in")
	csvDir := filepath.Join(work, "csv")
	astmDir := filepath.Join(work, "astm")
	scriptsDir := filepath.Join(work, "scripts")
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	writeFile := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(filepath.Join(scriptsDir, "only-adt.js"),
		`function filter(msg) { return msg.get('MSH-9.1') === 'ADT'; }`)
	writeFile(filepath.Join(scriptsDir, "uppercase-name.js"),
		`function transform(msg) { msg.set('PID-5.1', msg.get('PID-5.1').toUpperCase()); }`)
	writeFile(filepath.Join(scriptsDir, "to-csv.js"), `
function transform(msg) {
	var out = newMessage('csv');
	out.set('R.1', msg.get('PID-3'));
	out.set('R.2', msg.get('PID-5.1'));
	out.set('R.3', msg.get('PID-5.2'));
	return out;
}`)
	writeFile(filepath.Join(scriptsDir, "to-astm.js"), `
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

	chYAML := `
id: hl7-fanout
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: ` + inDir + `, interval: 50ms, minAge: 1ms}
filter: scripts/only-adt.js
transformers: [scripts/uppercase-name.js]
destinations:
  - id: csv-out
    dataType: csv
    transformers: [scripts/to-csv.js]
    adapter:
      type: file-writer
      settings: {dir: ` + csvDir + `, pattern: "{id}.csv"}
  - id: astm-out
    dataType: astm
    transformers: [scripts/to-astm.js]
    adapter:
      type: file-writer
      settings: {dir: ` + astmDir + `, pattern: "{id}.astm"}
`
	chPath := filepath.Join(work, "channel.yaml")
	writeFile(chPath, chYAML)
	cfg, err := config.LoadChannel(chPath)
	if err != nil {
		t.Fatal(err)
	}

	scripts := script.New(script.Options{Log: slog.New(slog.DiscardHandler)})
	defer scripts.Close()
	eng := New(Options{Log: slog.New(slog.DiscardHandler), Scripts: scripts})
	if err := eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background(), "hl7-fanout"); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown()

	if err := os.MkdirAll(inDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// One ADT (passes filter) and one ORU (dropped).
	oru := strings.Replace(sampleHL7, "ADT^A01", "ORU^R01", 1)
	writeFile(filepath.Join(inDir, "a-adt.hl7"), sampleHL7)
	writeFile(filepath.Join(inDir, "b-oru.hl7"), oru)

	waitFor(t, "csv and astm outputs", func() bool {
		csvs, _ := os.ReadDir(csvDir)
		astms, _ := os.ReadDir(astmDir)
		return len(csvs) == 1 && len(astms) == 1
	})

	csvs, _ := os.ReadDir(csvDir)
	raw, _ := os.ReadFile(filepath.Join(csvDir, csvs[0].Name()))
	if string(raw) != "MRN1,DOE,JOHN\n" {
		t.Errorf("csv output = %q", raw)
	}
	astms, _ := os.ReadDir(astmDir)
	raw, _ = os.ReadFile(filepath.Join(astmDir, astms[0].Name()))
	want := "H|\\^&\rP|1||MRN1||DOE^JOHN\rL|1\r"
	if string(raw) != want {
		t.Errorf("astm output = %q, want %q", raw, want)
	}

	// The ORU was filtered, not delivered: give the pipeline a beat, then
	// confirm no second file appeared.
	time.Sleep(200 * time.Millisecond)
	csvs, _ = os.ReadDir(csvDir)
	if len(csvs) != 1 {
		t.Errorf("filtered ORU produced output: %d files", len(csvs))
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
	eng := New(Options{Log: slog.New(slog.DiscardHandler)})
	if err := eng.LoadChannel(cfg); err == nil || !strings.Contains(err.Error(), "no script engine") {
		t.Errorf("expected script engine error, got %v", err)
	}
}
