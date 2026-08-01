// Package events is the in-process event bus: every message state change and
// channel status transition is published here, feeding the SSE API and the
// embedded TUI observer. Publishing never blocks the pipeline — a slow
// subscriber overflows its bounded buffer and receives a Resync event
// instead, telling it to refetch current state.
package events

import (
	"sync"

	"github.com/langhorst/waggle/internal/message"
)

// Type classifies an event.
type Type string

const (
	// TypeMessage: a message (or message/destination pair) changed state.
	TypeMessage Type = "message"
	// TypeChannelStatus: a channel started, stopped, or paused.
	TypeChannelStatus Type = "channel_status"
	// TypeResync: this subscriber overflowed and missed events; refetch
	// state instead of relying on the stream.
	TypeResync Type = "resync"
)

// Event carries identifiers only — consumers fetch details they need. This
// keeps fan-out cheap and slow-subscriber overflow harmless.
type Event struct {
	Type          Type          `json:"type"`
	ChannelID     string        `json:"channelId,omitempty"`
	ChannelStatus string        `json:"channelStatus,omitempty"`
	MessageID     int64         `json:"messageId,omitempty"`
	State         message.State `json:"state,omitempty"`
	DestinationID string        `json:"destinationId,omitempty"`
}

type subscriber struct {
	ch      chan Event
	dropped bool // overflowed since last successful send; owes a resync
}

// Bus fans events out to subscribers. The zero value is not usable; call
// NewBus.
type Bus struct {
	mu     sync.Mutex
	nextID int
	subs   map[int]*subscriber
}

func NewBus() *Bus {
	return &Bus{subs: map[int]*subscriber{}}
}

// Subscribe registers a subscriber with a buffer of size buf (minimum 1).
// The returned cancel func releases the subscription; the channel is closed
// afterwards.
func (b *Bus) Subscribe(buf int) (<-chan Event, func()) {
	if buf < 1 {
		buf = 1
	}
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	sub := &subscriber{ch: make(chan Event, buf)}
	b.subs[id] = sub
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		if s, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(s.ch)
		}
		b.mu.Unlock()
	}
	return sub.ch, cancel
}

// Publish delivers ev to every subscriber without blocking. A subscriber
// whose buffer is full misses the event and is handed a TypeResync marker as
// soon as space frees up.
func (b *Bus) Publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sub := range b.subs {
		if sub.dropped {
			// Owes a resync. The resync supersedes this event too: it tells
			// the subscriber to refetch state, which covers everything
			// missed including this one.
			select {
			case sub.ch <- Event{Type: TypeResync}:
				sub.dropped = false
			default:
				// Still full; keep owing.
			}
			continue
		}
		select {
		case sub.ch <- ev:
		default:
			sub.dropped = true
		}
	}
}
