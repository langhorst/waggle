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

func TestReaderRejectionMovesToError(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "bad.hl7", "nope")

	r, err := NewReader(map[string]any{"dir": dir, "interval": "50ms"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		return adapter.Receipt{}, errors.New("channel not accepting")
	}
	if err := r.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	waitFor(t, "file moved to error dir", func() bool {
		entries, _ := os.ReadDir(filepath.Join(dir, "error"))
		return len(entries) == 1
	})
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
