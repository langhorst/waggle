package engine

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/adapter/astm1381"
	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"

	_ "github.com/langhorst/waggle/internal/format/astm"
)

// TestASTMBridgeRoundTrip demonstrates the lab bridge in both directions
// using the shipped example scripts: the test plays an instrument sending
// E1394 over E1381 into channel A (converted to HL7 ORU^R01, sent over
// MLLP), whose output feeds channel B (HL7 back to E1394, sent over E1381)
// into the test's own E1381 receiver. The fields that come out the far end
// must match what went in.
func TestASTMBridgeRoundTrip(t *testing.T) {
	work := t.TempDir()
	ctx := context.Background()

	// The example conversion scripts are the ones under test.
	scriptsDir, err := filepath.Abs(filepath.Join("..", "..", "examples", "channels", "scripts"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"astm-to-hl7.js", "hl7-to-astm.js"} {
		if _, err := os.Stat(filepath.Join(scriptsDir, s)); err != nil {
			t.Fatalf("example script missing: %v", err)
		}
	}

	// Test-side E1381 receiver: the "instrument" the bridge delivers to.
	var mu sync.Mutex
	var final [][]byte
	instrument, err := astm1381.NewListener(map[string]any{"addr": "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		mu.Lock()
		final = append(final, append([]byte(nil), raw...))
		mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	if err := instrument.Start(ctx, deliver); err != nil {
		t.Fatal(err)
	}
	defer instrument.Stop()

	st, err := store.Open(filepath.Join(work, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	scripts := script.New(script.Options{Log: slog.New(slog.DiscardHandler)})
	defer scripts.Close()
	eng := New(Options{Store: st, Scripts: scripts, Log: slog.New(slog.DiscardHandler)})
	defer eng.Shutdown()

	writeChannel := func(name, yaml string) *config.Channel {
		t.Helper()
		path := filepath.Join(work, name)
		if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.LoadChannel(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}

	// Channel B first (its MLLP listen address feeds channel A's config):
	// HL7 over MLLP in → E1394 out over E1381 to the test instrument.
	cfgB := writeChannel("mllp-to-astm.yaml", `
id: mllp-to-astm
source:
  type: mllp-listener
  dataType: hl7v2
  settings: {addr: "127.0.0.1:0"}
transformers: ["`+filepath.Join(scriptsDir, "hl7-to-astm.js")+`"]
destinations:
  - id: to-instrument
    dataType: astm
    adapter:
      type: astm-sender
      settings: {addr: "`+instrument.Addr()+`"}
    queue: {retryInterval: 20ms}
`)
	if err := eng.LoadChannel(cfgB); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx, "mllp-to-astm"); err != nil {
		t.Fatal(err)
	}
	chB, _ := eng.Channel("mllp-to-astm")
	mllpAddr := chB.Source.(*mllp.Listener).Addr()

	// Channel A: E1394 over E1381 in → HL7 ORU^R01 out over MLLP to B.
	cfgA := writeChannel("astm-to-mllp.yaml", `
id: astm-to-mllp
source:
  type: astm-listener
  dataType: astm
  settings: {addr: "127.0.0.1:0"}
transformers: ["`+filepath.Join(scriptsDir, "astm-to-hl7.js")+`"]
destinations:
  - id: to-his
    dataType: hl7v2
    adapter:
      type: mllp-sender
      settings: {addr: "`+mllpAddr+`"}
    queue: {retryInterval: 20ms}
`)
	if err := eng.LoadChannel(cfgA); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx, "astm-to-mllp"); err != nil {
		t.Fatal(err)
	}
	chA, _ := eng.Channel("astm-to-mllp")
	astmAddr := chA.Source.(*astm1381.Listener).Addr()

	// The test plays the sending instrument.
	original := "H|\\^&|||LIS|||||||P|LIS2-A2|20260730120000\r" +
		"P|1||PATID123||DOE^JOHN||19800101|M\r" +
		"O|1|SPEC001||^^^GLU|R|20260730113000\r" +
		"R|1|^^^GLU|105|mg/dL||N||F\r" +
		"R|2|^^^HBA1C|5.4|%||N||F\r" +
		"L|1|N\r"
	sender, err := astm1381.NewSender(map[string]any{"addr": astmAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.Send(ctx, []byte(original), nil); err != nil {
		t.Fatalf("instrument send: %v", err)
	}

	// Wait for the message to traverse both channels back to the test.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(final)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(final) != 1 {
		t.Fatalf("instrument received %d messages", len(final))
	}

	// The round-tripped E1394 carries the original clinical content.
	dt, _ := format.Get("astm")
	root, err := dt.Parse(final[0])
	if err != nil {
		t.Fatalf("final message does not parse as ASTM: %v\n%q", err, final[0])
	}
	get := func(path string) string {
		nodes, err := dt.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}
	for path, want := range map[string]string{
		"H-5":      "LIS",
		"P-3":      "PATID123",
		"P-5.1":    "DOE",
		"P-5.2":    "JOHN",
		"P-7":      "19800101",
		"O-2":      "SPEC001",
		"O-4.4":    "GLU",
		"R-2.4":    "GLU",
		"R-3":      "105",
		"R-4":      "mg/dL",
		"R[2]-2.4": "HBA1C",
		"R[2]-3":   "5.4",
		"L-1":      "1",
	} {
		if got := get(path); got != want {
			t.Errorf("round-tripped %s = %q, want %q", path, got, want)
		}
	}

	// Both channels recorded a successful delivery.
	for _, channelID := range []string{"astm-to-mllp", "mllp-to-astm"} {
		counts, err := st.MessageCounts(ctx, channelID)
		if err != nil || counts[message.StateSent] != 1 {
			t.Errorf("channel %s counts = %v, %v", channelID, counts, err)
		}
	}
}
