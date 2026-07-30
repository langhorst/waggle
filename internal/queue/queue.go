// Package queue implements Guaranteed Delivery: one worker per destination
// drains that destination's persistent queue in FIFO order, retrying
// transient failures with exponential backoff and dead-lettering permanent
// rejections or exhausted retries. Because the queue lives in SQLite, a
// crash loses nothing — workers simply resume from the stored head.
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
	"github.com/langhorst/integration-channel/internal/events"
	"github.com/langhorst/integration-channel/internal/message"
	"github.com/langhorst/integration-channel/internal/store"
)

// Worker drains one destination's queue. Exactly one Worker may run per
// (channel, destination) pair — that single consumer is what guarantees
// per-destination ordering.
type Worker struct {
	Store     *store.Store
	Adapter   adapter.Outbound
	ChannelID string
	DestID    string

	// MaxAttempts before dead-lettering; -1 retries forever.
	MaxAttempts int
	// BaseInterval is the first retry delay; it doubles per attempt (with
	// ±20% jitter) up to CapInterval.
	BaseInterval time.Duration
	CapInterval  time.Duration
	// PollInterval is the idle sleep between queue checks.
	PollInterval time.Duration

	Bus *events.Bus
	Log *slog.Logger
}

func (w *Worker) defaults() {
	if w.BaseInterval <= 0 {
		w.BaseInterval = time.Second
	}
	if w.CapInterval <= 0 {
		w.CapInterval = 5 * time.Minute
	}
	if w.PollInterval <= 0 {
		w.PollInterval = 250 * time.Millisecond
	}
	if w.MaxAttempts == 0 {
		w.MaxAttempts = 10
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
}

// Run drains the queue until ctx is canceled. Blocking; run in a goroutine.
func (w *Worker) Run(ctx context.Context) {
	w.defaults()
	log := w.Log.With("channel", w.ChannelID, "destination", w.DestID)
	for ctx.Err() == nil {
		item, err := w.Store.Head(ctx, w.ChannelID, w.DestID)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("reading queue head", "error", err)
			}
			sleepCtx(ctx, w.PollInterval)
			continue
		}
		if item == nil {
			sleepCtx(ctx, w.PollInterval)
			continue
		}
		// FIFO: if the head is backing off, wait for it — never deliver a
		// newer message ahead of an older one.
		if wait := time.Until(item.NotBefore); wait > 0 {
			sleepCtx(ctx, minDuration(wait, w.PollInterval))
			continue
		}
		w.attempt(ctx, log, item)
	}
}

func (w *Worker) attempt(ctx context.Context, log *slog.Logger, item *store.QueueItem) {
	meta := map[string]string{
		"message.id":     fmt.Sprintf("%d", item.MessageID),
		"channel.id":     w.ChannelID,
		"destination.id": w.DestID,
	}
	err := w.Adapter.Send(ctx, item.Payload, meta)
	switch {
	case err == nil:
		if err := w.Store.MarkSent(ctx, item, w.DestID); err != nil {
			log.Error("marking sent", "message", item.MessageID, "error", err)
			return
		}
		w.publish(item.MessageID, message.StateSent)

	case adapter.IsPermanent(err):
		log.Warn("permanent rejection, dead-lettering", "message", item.MessageID, "error", err)
		if err := w.Store.DeadLetter(ctx, item, w.DestID, err.Error()); err != nil {
			log.Error("dead-lettering", "message", item.MessageID, "error", err)
			return
		}
		w.publish(item.MessageID, message.StateError)

	default:
		if ctx.Err() != nil {
			return // shutdown interrupted the send; leave the item queued
		}
		attempts := item.Attempts + 1
		if w.MaxAttempts != -1 && attempts >= w.MaxAttempts {
			log.Warn("retries exhausted, dead-lettering",
				"message", item.MessageID, "attempts", attempts, "error", err)
			if err := w.Store.DeadLetter(ctx, item, w.DestID, err.Error()); err != nil {
				log.Error("dead-lettering", "message", item.MessageID, "error", err)
				return
			}
			w.publish(item.MessageID, message.StateError)
			return
		}
		delay := w.backoffDelay(attempts)
		log.Info("delivery failed, backing off",
			"message", item.MessageID, "attempt", attempts, "retryIn", delay, "error", err)
		if err := w.Store.Backoff(ctx, item, w.DestID, time.Now().Add(delay), err.Error()); err != nil {
			log.Error("recording backoff", "message", item.MessageID, "error", err)
		}
	}
}

// backoffDelay is BaseInterval doubled per completed attempt, capped, with
// ±20% jitter so a fleet of retries doesn't thunder in lockstep.
func (w *Worker) backoffDelay(attempts int) time.Duration {
	d := w.BaseInterval
	for i := 1; i < attempts && d < w.CapInterval; i++ {
		d *= 2
	}
	if d > w.CapInterval {
		d = w.CapInterval
	}
	jitter := 1 + (rand.Float64()*0.4 - 0.2)
	return time.Duration(float64(d) * jitter)
}

func (w *Worker) publish(messageID int64, state message.State) {
	if w.Bus == nil {
		return
	}
	w.Bus.Publish(events.Event{
		Type:          events.TypeMessage,
		ChannelID:     w.ChannelID,
		MessageID:     messageID,
		State:         state,
		DestinationID: w.DestID,
	})
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
