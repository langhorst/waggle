package events

import "testing"

func TestSubscribeReceivesPublished(t *testing.T) {
	b := NewBus()
	ch, cancel := b.Subscribe(4)
	defer cancel()

	b.Publish(Event{Type: TypeMessage, ChannelID: "c1", MessageID: 1})
	ev := <-ch
	if ev.Type != TypeMessage || ev.MessageID != 1 {
		t.Errorf("got %+v", ev)
	}
}

func TestOverflowYieldsResync(t *testing.T) {
	b := NewBus()
	ch, cancel := b.Subscribe(1)
	defer cancel()

	b.Publish(Event{Type: TypeMessage, MessageID: 1}) // fills buffer
	b.Publish(Event{Type: TypeMessage, MessageID: 2}) // dropped
	b.Publish(Event{Type: TypeMessage, MessageID: 3}) // still dropped

	if ev := <-ch; ev.MessageID != 1 {
		t.Fatalf("first event = %+v", ev)
	}
	// Buffer has space again: next publish delivers the owed resync first.
	b.Publish(Event{Type: TypeMessage, MessageID: 4})
	if ev := <-ch; ev.Type != TypeResync {
		t.Fatalf("expected resync, got %+v", ev)
	}
	b.Publish(Event{Type: TypeMessage, MessageID: 5})
	if ev := <-ch; ev.MessageID != 5 {
		t.Fatalf("expected message 5, got %+v", ev)
	}
}

func TestCancelClosesChannel(t *testing.T) {
	b := NewBus()
	ch, cancel := b.Subscribe(1)
	cancel()
	if _, open := <-ch; open {
		t.Error("channel should be closed after cancel")
	}
	// Publishing after cancel must not panic.
	b.Publish(Event{Type: TypeMessage})
	cancel() // double-cancel is safe
}

func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	b := NewBus()
	slow, cancelSlow := b.Subscribe(1)
	defer cancelSlow()
	_ = slow // never read

	fast, cancelFast := b.Subscribe(16)
	defer cancelFast()

	for i := 1; i <= 10; i++ {
		b.Publish(Event{Type: TypeMessage, MessageID: int64(i)})
	}
	if ev := <-fast; ev.MessageID != 1 {
		t.Errorf("fast subscriber got %+v", ev)
	}
}
