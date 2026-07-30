package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/langhorst/integration-channel/internal/message"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func record(t *testing.T, s *Store, channelID, raw string) *message.Message {
	t.Helper()
	m := &message.Message{
		ChannelID:     channelID,
		CorrelationID: "corr-" + raw,
		Raw:           []byte(raw),
		DataType:      "hl7v2",
		State:         message.StateReceived,
		ReceivedAt:    time.Now(),
		Meta:          map[string]string{"origin": "test"},
	}
	if err := s.Record(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRecordAndGet(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	m := record(t, s, "c1", "raw-bytes")
	if m.ID == 0 {
		t.Fatal("Record must assign an ID")
	}
	if err := s.SetTransformed(ctx, m.ID, []byte("transformed")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetState(ctx, m.ID, message.StateTransformed, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDestinationState(ctx, m.ID, "d1", message.StateQueued, []byte("payload"), ""); err != nil {
		t.Fatal(err)
	}
	// Later state change without payload must preserve the stored payload.
	if err := s.SetDestinationState(ctx, m.ID, "d1", message.StateSent, nil, ""); err != nil {
		t.Fatal(err)
	}

	d, err := s.GetMessage(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Raw) != "raw-bytes" || string(d.Transformed) != "transformed" {
		t.Errorf("payloads: raw=%q transformed=%q", d.Raw, d.Transformed)
	}
	if d.State != message.StateTransformed || d.Meta["origin"] != "test" {
		t.Errorf("detail = %+v", d)
	}
	if len(d.Destinations) != 1 || d.Destinations[0].State != message.StateSent {
		t.Fatalf("destinations = %+v", d.Destinations)
	}
	if d.Destinations[0].SentAt == nil || d.Destinations[0].QueuedAt == nil {
		t.Error("timestamps should be recorded for queued and sent")
	}
	payload, err := s.DestinationPayload(ctx, m.ID, "d1")
	if err != nil || string(payload) != "payload" {
		t.Errorf("payload = %q, %v", payload, err)
	}

	if _, err := s.GetMessage(ctx, 99999); err != ErrNotFound {
		t.Errorf("missing message: %v", err)
	}
}

func TestListMessages(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		record(t, s, "c1", "m")
	}
	record(t, s, "other", "x")

	list, err := s.ListMessages(ctx, "c1", ListQuery{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 || list[0].ID < list[1].ID {
		t.Fatalf("expected 3 newest-first, got %+v", list)
	}
	// Page 2.
	page2, err := s.ListMessages(ctx, "c1", ListQuery{Limit: 3, BeforeID: list[2].ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 = %+v", page2)
	}
	// State filter.
	if err := s.SetState(ctx, list[0].ID, message.StateError, "boom"); err != nil {
		t.Fatal(err)
	}
	errs, err := s.ListMessages(ctx, "c1", ListQuery{State: message.StateError})
	if err != nil || len(errs) != 1 || errs[0].ErrorText != "boom" {
		t.Errorf("state filter = %+v, %v", errs, err)
	}
}

func TestRetention(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.SetRetention("c1", 3)

	var ids []int64
	for i := 0; i < 6; i++ {
		ids = append(ids, record(t, s, "c1", "m").ID)
	}
	// Reference one old message from the queue: it must survive pruning.
	if err := s.SetDestinationState(ctx, ids[0], "d1", message.StateQueued, []byte("p"), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue(ctx, "c1", "d1", ids[0]); err != nil {
		t.Fatal(err)
	}

	s.Prune(ctx)
	list, err := s.ListMessages(ctx, "c1", ListQuery{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 4 { // newest 3 + the queue-referenced one
		t.Fatalf("after prune: %d messages", len(list))
	}
	found := false
	for _, m := range list {
		if m.ID == ids[0] {
			found = true
		}
	}
	if !found {
		t.Error("queue-referenced message was pruned")
	}

	// Unlimited retention prunes nothing.
	s.SetRetention("c2", -1)
	for i := 0; i < 5; i++ {
		record(t, s, "c2", "m")
	}
	s.Prune(ctx)
	if list, _ := s.ListMessages(ctx, "c2", ListQuery{Limit: 100}); len(list) != 5 {
		t.Errorf("unlimited retention pruned: %d left", len(list))
	}
}

func TestQueueLifecycle(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	m1 := record(t, s, "c1", "first")
	m2 := record(t, s, "c1", "second")
	for _, m := range []*message.Message{m1, m2} {
		if err := s.SetDestinationState(ctx, m.ID, "d1", message.StateQueued, m.Raw, ""); err != nil {
			t.Fatal(err)
		}
		if err := s.Enqueue(ctx, "c1", "d1", m.ID); err != nil {
			t.Fatal(err)
		}
	}

	// FIFO head.
	head, err := s.Head(ctx, "c1", "d1")
	if err != nil || head == nil || head.MessageID != m1.ID || string(head.Payload) != "first" {
		t.Fatalf("head = %+v, %v", head, err)
	}

	// Backoff keeps it at the head with attempts recorded.
	notBefore := time.Now().Add(time.Hour)
	if err := s.Backoff(ctx, head, "d1", notBefore, "receiver down"); err != nil {
		t.Fatal(err)
	}
	head, _ = s.Head(ctx, "c1", "d1")
	if head.MessageID != m1.ID || head.Attempts != 1 || head.NotBefore.Before(time.Now().Add(30*time.Minute)) {
		t.Fatalf("after backoff: %+v", head)
	}

	// MarkSent removes it; next head is m2.
	if err := s.MarkSent(ctx, head, "d1"); err != nil {
		t.Fatal(err)
	}
	head, _ = s.Head(ctx, "c1", "d1")
	if head == nil || head.MessageID != m2.ID {
		t.Fatalf("next head = %+v", head)
	}

	// DeadLetter removes and flags.
	if err := s.DeadLetter(ctx, head, "d1", "gave up"); err != nil {
		t.Fatal(err)
	}
	if head, _ = s.Head(ctx, "c1", "d1"); head != nil {
		t.Fatalf("queue should be empty, head = %+v", head)
	}
	dlq, err := s.DeadLetters(ctx, "c1", 10)
	if err != nil || len(dlq) != 1 || dlq[0].ID != m2.ID || dlq[0].Destination.LastError != "gave up" {
		t.Fatalf("dlq = %+v, %v", dlq, err)
	}

	// Requeue puts it back with a clean slate.
	if err := s.Requeue(ctx, m2.ID, "d1"); err != nil {
		t.Fatal(err)
	}
	head, _ = s.Head(ctx, "c1", "d1")
	if head == nil || head.MessageID != m2.ID || head.Attempts != 0 {
		t.Fatalf("after requeue: %+v", head)
	}
	// Double requeue while queued is rejected.
	if err := s.Requeue(ctx, m2.ID, "d1"); err == nil {
		t.Error("requeue of an already-queued delivery should fail")
	}

	depth, err := s.QueueDepth(ctx, "c1")
	if err != nil || depth["d1"] != 1 {
		t.Errorf("depth = %v, %v", depth, err)
	}
}

func TestQueueSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "durable.db")
	ctx := context.Background()

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m := record(t, s, "c1", "durable")
	if err := s.SetDestinationState(ctx, m.ID, "d1", message.StateQueued, []byte("payload"), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue(ctx, "c1", "d1", m.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// "Restart": the queue row and payload must still be there.
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	head, err := s2.Head(ctx, "c1", "d1")
	if err != nil || head == nil || string(head.Payload) != "payload" {
		t.Fatalf("after reopen: head = %+v, %v", head, err)
	}
}

func TestNewReplay(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	orig := record(t, s, "c1", "original")

	replay, err := s.NewReplay(ctx, orig.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.ID != 0 || replay.ReplayOf != orig.ID ||
		replay.CorrelationID != orig.CorrelationID || string(replay.Raw) != "original" {
		t.Errorf("replay = %+v", replay)
	}
	// Recording it creates a distinct row carrying the lineage.
	if err := s.Record(ctx, replay); err != nil {
		t.Fatal(err)
	}
	d, err := s.GetMessage(ctx, replay.ID)
	if err != nil || d.ReplayOf != orig.ID {
		t.Errorf("stored replay = %+v, %v", d, err)
	}
}

func TestMessageCounts(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	a := record(t, s, "c1", "a")
	record(t, s, "c1", "b")
	if err := s.SetState(ctx, a.ID, message.StateError, "x"); err != nil {
		t.Fatal(err)
	}
	counts, err := s.MessageCounts(ctx, "c1")
	if err != nil || counts[message.StateReceived] != 1 || counts[message.StateError] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
}
