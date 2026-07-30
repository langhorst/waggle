package engine

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/integration-channel/internal/channel"
	"github.com/langhorst/integration-channel/internal/config"
	"github.com/langhorst/integration-channel/internal/events"

	_ "github.com/langhorst/integration-channel/internal/adapter/file"
	_ "github.com/langhorst/integration-channel/internal/adapter/mllp"
	_ "github.com/langhorst/integration-channel/internal/format/csvfmt"
	_ "github.com/langhorst/integration-channel/internal/format/hl7v2"
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
