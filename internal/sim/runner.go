package sim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Wiring binds one feed to where its messages go and how they misbehave.
type Wiring struct {
	Feed Feed
	Sink Sink
	// Faults are applied in order, after rendering and before the sink.
	// Empty means the feed is well behaved.
	Faults []Fault
}

// Runner fans the world's events out to the wired feeds.
//
// Events are delivered in simulated-time order, and to feeds in the order
// they were wired, so a run is reproducible end to end: the same seed and
// the same wiring produce the same bytes at every sink. Rendering and
// sending happen on the caller's goroutine, which makes back-pressure from a
// slow sink slow the hospital down rather than accumulate in memory.
type Runner struct {
	Log *slog.Logger

	mu      sync.Mutex
	wiring  []Wiring
	seq     uint64
	sent    map[string]int // messages per feed, for the run summary
	stopped bool
}

// NewRunner returns a runner with no feeds wired.
func NewRunner(log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Runner{Log: log, sent: map[string]int{}}
}

// Wire adds a feed. Wiring the same feed name twice is allowed -- the same
// interface can legitimately go to two places, such as a live MLLP link and
// a corpus on disk.
func (r *Runner) Wire(w Wiring) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wiring = append(r.wiring, w)
}

// Emit renders one event through every interested feed and sends the result.
//
// The event's Seq is assigned here, so ordering is the runner's single
// source of truth rather than something each caller has to maintain.
func (r *Runner) Emit(ctx context.Context, ev Event) error {
	r.mu.Lock()
	r.seq++
	ev.Seq = r.seq
	wiring := make([]Wiring, len(r.wiring))
	copy(wiring, r.wiring)
	r.mu.Unlock()

	for _, w := range wiring {
		if !w.Feed.Wants(ev.Kind) {
			continue
		}
		msgs, err := w.Feed.Render(ev)
		if err != nil {
			// One feed failing to render must not stop the hospital: the
			// run is a test fixture, and a renderer bug should be visible
			// without discarding the rest of the traffic.
			r.Log.Error("rendering event", "feed", w.Feed.Name(), "kind", ev.Kind, "error", err)
			continue
		}
		for _, m := range msgs {
			out := []Message{m}
			for _, f := range w.Faults {
				var next []Message
				for _, mm := range out {
					next = append(next, f.Apply(mm)...)
				}
				out = next
			}
			for _, mm := range out {
				if err := w.Sink.Send(ctx, mm); err != nil {
					return fmt.Errorf("%s sink: %w", w.Feed.Name(), err)
				}
				r.mu.Lock()
				r.sent[w.Feed.Name()]++
				r.mu.Unlock()
			}
		}
	}
	return nil
}

// Sent reports how many messages each feed has delivered.
func (r *Runner) Sent() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.sent))
	for k, v := range r.sent {
		out[k] = v
	}
	return out
}

// Close closes every wired sink, returning the joined errors. It is safe to
// call twice; the second call is a no-op.
func (r *Runner) Close() error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopped = true
	wiring := make([]Wiring, len(r.wiring))
	copy(wiring, r.wiring)
	r.mu.Unlock()

	var errs []error
	for _, w := range wiring {
		if err := w.Sink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s sink: %w", w.Feed.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// ControlIDs hands out MSH-10 values.
//
// They are drawn from the run's own counter rather than from wall-clock time
// or randomness, so a rerun produces the same control IDs and two runs can
// be diffed. Prefix keeps a simulated feed's IDs from ever being mistaken
// for a real system's.
type ControlIDs struct {
	Prefix string

	mu sync.Mutex
	n  uint64
}

// Next returns the next control ID.
func (c *ControlIDs) Next() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	prefix := c.Prefix
	if prefix == "" {
		prefix = "SIM"
	}
	return fmt.Sprintf("%s%09d", prefix, c.n)
}

// HL7Time formats an instant as an HL7 DTM to the second, the precision
// almost every interface actually uses.
func HL7Time(t time.Time) string { return t.Format("20060102150405") }

// HL7Date formats an instant as an HL7 date.
func HL7Date(t time.Time) string { return t.Format("20060102") }
