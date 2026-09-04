package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

func writeInput(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// Backdate so minAge doesn't skip it.
	old := time.Now().Add(-time.Minute)
	_ = os.Chtimes(filepath.Join(dir, name), old, old)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestReaderDeliversAndMoves(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "b.hl7", "second")
	writeInput(t, dir, "a.hl7", "first")
	writeInput(t, dir, "ignore.txt", "not matched")

	r, err := NewReader(map[string]any{
		"dir":      dir,
		"pattern":  "*.hl7",
		"interval": "50ms",
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var got []string
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		mu.Lock()
		got = append(got, string(raw))
		mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	if err := r.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	waitFor(t, "both files delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 2
	})
	mu.Lock()
	// Lexicographic order within one poll.
	if got[0] != "first" || got[1] != "second" {
		t.Errorf("delivery order = %v", got)
	}
	mu.Unlock()
	waitFor(t, "files moved to processed", func() bool {
		entries, _ := os.ReadDir(filepath.Join(dir, "processed"))
		return len(entries) == 2
	})
	if _, err := os.Stat(filepath.Join(dir, "ignore.txt")); err != nil {
		t.Error("non-matching file must be left alone")
	}
}

// TestReaderRefusalLeavesFileInPlace: a deliver error means "not accepted
// right now" (channel paused, store busy), so the file stays where it is
// and is offered again on the next poll. It used to be moved to the error
// directory, turning a transient refusal into a permanent loss.
func TestReaderRefusalLeavesFileInPlace(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "held.hl7", "content")

	r, err := NewReader(map[string]any{"dir": dir, "interval": "20ms"})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	attempts := 0
	accept := false
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if !accept {
			return adapter.Receipt{}, errors.New("channel not accepting")
		}
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	if err := r.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	waitFor(t, "file to be offered more than once", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 3
	})
	if _, err := os.Stat(filepath.Join(dir, "held.hl7")); err != nil {
		t.Fatal("refused file was moved out of the input directory")
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "error")); len(entries) != 0 {
		t.Fatal("refused file landed in the error directory")
	}

	mu.Lock()
	accept = true
	mu.Unlock()
	waitFor(t, "file accepted and moved", func() bool {
		entries, _ := os.ReadDir(filepath.Join(dir, "processed"))
		return len(entries) == 1
	})
}

// TestReaderDestinationAck: in destination mode the pipeline outcome
// decides between processed and error.
func TestReaderDestinationAck(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "good.hl7", "ok")
	writeInput(t, dir, "rejected.hl7", "reject me")

	r, err := NewReader(map[string]any{"dir": dir, "interval": "20ms", "ackMode": "destination"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		done := make(chan adapter.AckDecision, 1)
		if string(raw) == "reject me" {
			done <- adapter.AckDecision{Code: "AR", Text: "script rejected"}
		} else {
			done <- adapter.AckDecision{Code: "AA"}
		}
		return adapter.Receipt{MessageID: 1, Done: done}, nil
	}
	if err := r.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	waitFor(t, "files settled", func() bool {
		p, _ := os.ReadDir(filepath.Join(dir, "processed"))
		e, _ := os.ReadDir(filepath.Join(dir, "error"))
		return len(p) == 1 && len(e) == 1
	})
	if _, err := os.Stat(filepath.Join(dir, "processed", "good.hl7")); err != nil {
		t.Error("accepted file not in processed/")
	}
	if _, err := os.Stat(filepath.Join(dir, "error", "rejected.hl7")); err != nil {
		t.Error("rejected file not in error/")
	}
}

// TestReaderImmovableFileIsNotRedelivered: when a delivered file cannot be
// moved out of the input directory it must not be read again every poll.
func TestReaderImmovableFileIsNotRedelivered(t *testing.T) {
	dir := t.TempDir()
	processed := filepath.Join(dir, "processed")
	writeInput(t, dir, "one.hl7", "content")

	r, err := NewReader(map[string]any{"dir": dir, "interval": "20ms"})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	deliveries := 0
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		mu.Lock()
		deliveries++
		mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	// Start creates the directories; then turn processed/ into a plain
	// file so every move into it fails.
	if err := r.Start(context.Background(), func(context.Context, []byte, map[string]string) (adapter.Receipt, error) {
		return adapter.Receipt{}, errors.New("not yet")
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(processed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(processed, []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Start again would recreate the directory via MkdirAll, which fails
	// on a file; drive poll directly instead.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 5; i++ {
		r.poll(ctx, deliver)
	}
	mu.Lock()
	defer mu.Unlock()
	if deliveries != 1 {
		t.Fatalf("immovable file delivered %d times, want exactly once", deliveries)
	}
	if _, err := os.Stat(filepath.Join(dir, "one.hl7")); err != nil {
		t.Error("file should still be in the input directory")
	}
}

func TestReaderConfigValidation(t *testing.T) {
	if _, err := NewReader(map[string]any{"dir": t.TempDir(), "ackMode": "sometimes"}); err == nil {
		t.Error("bad ackMode accepted")
	}
}

func TestReaderStopHaltsPolling(t *testing.T) {
	dir := t.TempDir()
	r, err := NewReader(map[string]any{"dir": dir, "interval": "20ms"})
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{}, 16)
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		delivered <- struct{}{}
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	if err := r.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	writeInput(t, dir, "late.hl7", "after stop")
	select {
	case <-delivered:
		t.Error("delivery after Stop")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestWriter(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(map[string]any{"dir": dir, "pattern": "{channel}-{id}.hl7"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{"message.id": "42", "channel.id": "adt"}
	if err := w.Send(context.Background(), []byte("payload"), meta); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "adt-42.hl7"))
	if err != nil || string(raw) != "payload" {
		t.Fatalf("output file: %v %q", err, raw)
	}
	// Same name again: must not overwrite.
	if err := w.Send(context.Background(), []byte("second"), meta); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "adt-42.hl7.1")); err != nil {
		t.Error("collision should produce a suffixed file")
	}
}

func TestWriterConfigValidation(t *testing.T) {
	if _, err := NewWriter(map[string]any{}); err == nil {
		t.Error("missing dir should fail")
	}
	if _, err := NewWriter(map[string]any{"dir": "x", "pattern": "a/b"}); err == nil {
		t.Error("path separator in pattern should fail")
	}
	if _, err := NewReader(map[string]any{"dir": "x", "pattern": "["}); err == nil {
		t.Error("invalid glob should fail")
	}
}
