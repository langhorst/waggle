package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/store"
	"github.com/langhorst/waggle/internal/testutil"
)

// TestMLLPToFileEndToEnd: an MLLP listener source in destination-ACK mode,
// a JS transformer, a waitForAck file destination, fired at with a real
// MLLP sender and checked for the ACK round trip, stored lifecycle, and
// transformed output.
func TestMLLPToFileEndToEnd(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	outDir := filepath.Join(f.Work, "out")
	testutil.WriteFile(t, filepath.Join(f.Work, "scripts", "validate.js"), `
function transform(msg) {
	if (!msg.get('PID-3')) { response.reject('AR', 'missing patient identifier'); }
	msg.set('PID-5.1', msg.get('PID-5.1').toUpperCase());
}`)

	ch := f.StartChannelYAML(t, "mllp-feed", `
id: mllp-feed
source:
  type: mllp-listener
  dataType: hl7v2
  settings:
    addr: "127.0.0.1:0"
    ackMode: destination
    holdTimeout: 10s
transformers: [scripts/validate.js]
destinations:
  - id: archive
    waitForAck: true
    adapter:
      type: file-writer
      settings: {dir: `+outDir+`, pattern: "{id}.hl7"}
`)
	sender, err := mllp.NewSender(map[string]any{"addr": testutil.ListenAddr(t, ch), "ackTimeout": "10s"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	// A good message: ACK arrives only after the waitForAck destination
	// wrote the file, so the output must exist the moment Send returns.
	good := "MSH|^~\\&|EPIC|HOSP|LAB|LABFAC|20260730||ADT^A01|OK1|P|2.5\rPID|1||MRN1||doe^JOHN\r"
	if err := sender.Send(ctx, []byte(good), nil); err != nil {
		t.Fatalf("good message NAKed: %v", err)
	}
	entries, err := os.ReadDir(outDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("destination-ACK returned before delivery: %d files (%v)", len(entries), err)
	}
	raw, _ := os.ReadFile(filepath.Join(outDir, entries[0].Name()))
	if !strings.Contains(string(raw), "DOE^JOHN") {
		t.Errorf("output not transformed: %q", raw)
	}

	// A message failing script validation: the sender sees the script's AR
	// with its text, and nothing new is written.
	bad := "MSH|^~\\&|EPIC|HOSP|LAB|LABFAC|20260730||ADT^A01|BAD1|P|2.5\rPID|1\r"
	err = sender.Send(ctx, []byte(bad), nil)
	if err == nil || !strings.Contains(err.Error(), "missing patient identifier") {
		t.Fatalf("validation rejection not propagated: %v", err)
	}
	if entries, _ := os.ReadDir(outDir); len(entries) != 1 {
		t.Error("rejected message must not reach the destination")
	}

	// Both messages recorded: one TRANSFORMED with a SENT destination, one
	// ERROR in the Invalid Message Channel.
	list, err := f.Store.ListMessages(ctx, "mllp-feed", store.ListQuery{})
	if err != nil || len(list) != 2 {
		t.Fatalf("stored messages = %d, %v", len(list), err)
	}
	counts, _ := f.Store.MessageCounts(ctx, "mllp-feed")
	if counts["ERROR"] != 1 || counts["SENT"] != 1 {
		t.Errorf("counts = %v", counts)
	}
}
