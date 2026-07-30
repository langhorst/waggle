package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
	"github.com/langhorst/integration-channel/internal/message"
	"github.com/langhorst/integration-channel/internal/store"
)

// scriptedAdapter fails the first failN sends per payload, then succeeds.
type scriptedAdapter struct {
	mu        sync.Mutex
	failN     int
	permanent bool
	attempts  map[string]int
	delivered []string
}

func (a *scriptedAdapter) Open(ctx context.Context) error { return nil }
func (a *scriptedAdapter) Close() error                   { return nil }
func (a *scriptedAdapter) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.attempts == nil {
		a.attempts = map[string]int{}
	}
	key := string(payload)
	a.attempts[key]++
	if a.attempts[key] <= a.failN {
		if a.permanent {
			return adapter.Permanent(errors.New("AR: rejected"))
		}
		return errors.New("receiver down")
	}
	a.delivered = append(a.delivered, key)
	return nil
}
func (a *scriptedAdapter) deliveredList() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.delivered...)
}

func setup(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func enqueue(t *testing.T, s *store.Store, channelID, destID, payload string) int64 {
	t.Helper()
	ctx := context.Background()
	m := &message.Message{
		ChannelID: channelID, CorrelationID: payload, Raw: []byte(payload),
		DataType: "hl7v2", State: message.StateReceived, ReceivedAt: time.Now(),
	}
	if err := s.Record(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDestinationState(ctx, m.ID, destID, message.StateQueued, []byte(payload), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue(ctx, channelID, destID, m.ID); err != nil {
		t.Fatal(err)
	}
	return m.ID
}

func runWorker(t *testing.T, w *Worker) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel
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

func destState(t *testing.T, s *store.Store, id int64) store.DestinationStatus {
	t.Helper()
	d, err := s.GetMessage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Destinations) != 1 {
		t.Fatalf("destinations = %+v", d.Destinations)
	}
	return d.Destinations[0]
}

func TestWorkerRetriesThenDelivers(t *testing.T) {
	s := setup(t)
	out := &scriptedAdapter{failN: 2}
	id := enqueue(t, s, "c1", "d1", "msg-1")

	runWorker(t, &Worker{
		Store: s, Adapter: out, ChannelID: "c1", DestID: "d1",
		MaxAttempts: 10, BaseInterval: 5 * time.Millisecond,
		PollInterval: 5 * time.Millisecond,
		Log:          slog.New(slog.DiscardHandler),
	})

	waitFor(t, "delivery after retries", func() bool {
		return destState(t, s, id).State == message.StateSent
	})
	ds := destState(t, s, id)
	if ds.Attempts != 2 { // two failed attempts recorded; the success clears the error
		t.Errorf("attempts = %d, want 2 recorded failures", ds.Attempts)
	}
	if ds.LastError != "" {
		t.Errorf("lastError should clear on success, got %q", ds.LastError)
	}
}

func TestWorkerPreservesOrderDuringBackoff(t *testing.T) {
	s := setup(t)
	out := &scriptedAdapter{failN: 3}
	enqueue(t, s, "c1", "d1", "msg-A") // will fail 3 times first
	enqueue(t, s, "c1", "d1", "msg-B")

	runWorker(t, &Worker{
		Store: s, Adapter: out, ChannelID: "c1", DestID: "d1",
		MaxAttempts: 10, BaseInterval: 5 * time.Millisecond,
		PollInterval: 5 * time.Millisecond,
		Log:          slog.New(slog.DiscardHandler),
	})

	waitFor(t, "both delivered", func() bool { return len(out.deliveredList()) == 2 })
	got := out.deliveredList()
	if got[0] != "msg-A" || got[1] != "msg-B" {
		t.Errorf("delivery order = %v: FIFO must hold across backoff", got)
	}
}

func TestWorkerPermanentErrorDeadLetters(t *testing.T) {
	s := setup(t)
	out := &scriptedAdapter{failN: 1000, permanent: true}
	id := enqueue(t, s, "c1", "d1", "rejected")

	runWorker(t, &Worker{
		Store: s, Adapter: out, ChannelID: "c1", DestID: "d1",
		MaxAttempts: 10, BaseInterval: time.Millisecond,
		PollInterval: 5 * time.Millisecond,
		Log:          slog.New(slog.DiscardHandler),
	})

	waitFor(t, "dead letter", func() bool { return destState(t, s, id).DeadLetter })
	ds := destState(t, s, id)
	if ds.State != message.StateError || ds.Attempts != 1 {
		t.Errorf("permanent rejection: %+v (must not retry)", ds)
	}
	dlq, err := s.DeadLetters(context.Background(), "c1", 10)
	if err != nil || len(dlq) != 1 {
		t.Errorf("dlq = %+v, %v", dlq, err)
	}
}

func TestWorkerExhaustsRetriesThenDeadLetters(t *testing.T) {
	s := setup(t)
	out := &scriptedAdapter{failN: 1000}
	id := enqueue(t, s, "c1", "d1", "doomed")

	runWorker(t, &Worker{
		Store: s, Adapter: out, ChannelID: "c1", DestID: "d1",
		MaxAttempts: 3, BaseInterval: time.Millisecond,
		PollInterval: time.Millisecond,
		Log:          slog.New(slog.DiscardHandler),
	})

	waitFor(t, "dead letter after exhaustion", func() bool { return destState(t, s, id).DeadLetter })
	ds := destState(t, s, id)
	if ds.Attempts != 3 {
		t.Errorf("attempts = %d, want exactly maxAttempts", ds.Attempts)
	}

	// Requeue from the DLQ delivers on a now-healthy adapter.
	out.mu.Lock()
	out.failN = 0
	out.mu.Unlock()
	if err := s.Requeue(context.Background(), id, "d1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "requeued delivery", func() bool {
		return destState(t, s, id).State == message.StateSent
	})
}

func TestBackoffSchedule(t *testing.T) {
	w := &Worker{BaseInterval: time.Second, CapInterval: 5 * time.Minute, MaxAttempts: -1, PollInterval: time.Second}
	for attempt, want := range map[int]time.Duration{
		1:  time.Second,
		2:  2 * time.Second,
		3:  4 * time.Second,
		10: 5 * time.Minute, // 512s uncapped, clamped to the 5m cap
	} {
		got := w.backoffDelay(attempt)
		min := float64(want) * 0.75
		max := float64(want) * 1.25
		if float64(got) < min || float64(got) > max {
			t.Errorf("attempt %d: delay %v outside [%v, %v]", attempt, got, time.Duration(min), time.Duration(max))
		}
	}
	if fmt.Sprint(w.backoffDelay(50)) == "" { // must not overflow
		t.Fatal("unreachable")
	}
}
