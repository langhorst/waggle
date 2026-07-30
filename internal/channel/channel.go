// Package channel implements the channel runtime: one inbound Channel
// Adapter feeding a Pipes-and-Filters pipeline — Message Filter, Message
// Translator chain, then a Recipient List of destinations, each with its own
// optional filter/translator chain and outbound Channel Adapter.
//
// Message processing is strictly sequential per channel. Every state
// transition goes through the Recorder (the persistence seam) and is
// published on the event bus.
package channel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
	"github.com/langhorst/integration-channel/internal/events"
	"github.com/langhorst/integration-channel/internal/format"
	"github.com/langhorst/integration-channel/internal/message"
)

// Status is a channel's lifecycle state.
type Status string

const (
	StatusStopped Status = "STOPPED"
	StatusStarted Status = "STARTED"
	// StatusPaused: intake stopped, everything else (queues, later) keeps
	// draining.
	StatusPaused Status = "PAUSED"
)

// FilterFunc is a Message Filter step: false drops the message (state
// FILTERED, retained). An error sends the message to the Invalid Message
// Channel.
type FilterFunc func(m *message.Message) (bool, error)

// TranslateFunc is a Message Translator step. It may mutate m.Tree in place
// or replace it entirely (and change m.DataType) for format conversion.
type TranslateFunc func(m *message.Message) error

// Recorder is the persistence seam: the pipeline reports every lifecycle
// transition through it. Phase 2 runs with NewMemoryRecorder; the SQLite
// store implements this in phase 3.
type Recorder interface {
	// Record persists a freshly received message and assigns m.ID. Once it
	// returns nil the engine owns the message (Guaranteed Delivery handoff).
	Record(ctx context.Context, m *message.Message) error
	// SetState records a pipeline-level state change.
	SetState(ctx context.Context, id int64, state message.State, errText string) error
	// SetTransformed stores the serialized output of the channel-level
	// translator chain (feeds the received-vs-sent diff).
	SetTransformed(ctx context.Context, id int64, payload []byte) error
	// SetDestinationState records a per-destination state change; payload is
	// the destination-serialized bytes when known.
	SetDestinationState(ctx context.Context, id int64, destID string, state message.State, payload []byte, errText string) error
}

// Destination is one entry of the channel's Recipient List.
type Destination struct {
	ID         string
	OutType    format.DataType
	Adapter    adapter.Outbound
	WaitForAck bool
	Filter     FilterFunc      // optional
	Translate  []TranslateFunc // optional chain
}

// Channel is one configured integration channel.
type Channel struct {
	ID           string
	Name         string
	InType       format.DataType
	Source       adapter.Inbound
	Filter       FilterFunc      // optional
	Translate    []TranslateFunc // optional chain
	Destinations []*Destination

	Recorder Recorder
	Bus      *events.Bus
	Log      *slog.Logger

	mu        sync.Mutex
	status    Status
	runCancel context.CancelFunc
	// pipeMu serializes message processing: one message at a time per
	// channel, in arrival order.
	pipeMu sync.Mutex
	wg     sync.WaitGroup
}

// Status returns the channel's lifecycle state.
func (c *Channel) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == "" {
		return StatusStopped
	}
	return c.status
}

// Start begins intake. Starting a paused channel resumes it.
func (c *Channel) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == StatusStarted {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	for _, d := range c.Destinations {
		if err := d.Adapter.Open(runCtx); err != nil {
			cancel()
			return fmt.Errorf("channel %s: destination %s: %w", c.ID, d.ID, err)
		}
	}
	if err := c.Source.Start(runCtx, c.deliver); err != nil {
		cancel()
		return fmt.Errorf("channel %s: source: %w", c.ID, err)
	}
	c.runCancel = cancel
	c.setStatusLocked(StatusStarted)
	return nil
}

// Pause stops intake only.
func (c *Channel) Pause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status != StatusStarted {
		return fmt.Errorf("channel %s: not started", c.ID)
	}
	if err := c.Source.Stop(); err != nil {
		return err
	}
	c.setStatusLocked(StatusPaused)
	return nil
}

// Resume restarts intake on a paused channel.
func (c *Channel) Resume(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status != StatusPaused {
		return fmt.Errorf("channel %s: not paused", c.ID)
	}
	if err := c.Source.Start(ctx, c.deliver); err != nil {
		return err
	}
	c.setStatusLocked(StatusStarted)
	return nil
}

// Stop halts intake, waits for in-flight messages, and closes destination
// adapters.
func (c *Channel) Stop() error {
	c.mu.Lock()
	status := c.status
	cancel := c.runCancel
	c.runCancel = nil
	c.mu.Unlock()

	if status == StatusStarted || status == StatusPaused {
		if status == StatusStarted {
			_ = c.Source.Stop()
		}
		if cancel != nil {
			cancel()
		}
		c.wg.Wait()
		for _, d := range c.Destinations {
			_ = d.Adapter.Close()
		}
	}
	c.mu.Lock()
	c.setStatusLocked(StatusStopped)
	c.mu.Unlock()
	return nil
}

func (c *Channel) setStatusLocked(s Status) {
	c.status = s
	if c.Bus != nil {
		c.Bus.Publish(events.Event{
			Type:          events.TypeChannelStatus,
			ChannelID:     c.ID,
			ChannelStatus: string(s),
		})
	}
}

// deliver is the adapter.DeliverFunc handed to the source adapter.
func (c *Channel) deliver(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
	c.mu.Lock()
	started := c.status == StatusStarted
	c.mu.Unlock()
	if !started {
		return adapter.Receipt{}, fmt.Errorf("channel %s: not accepting messages", c.ID)
	}

	m := &message.Message{
		ChannelID:     c.ID,
		CorrelationID: newCorrelationID(),
		Raw:           append([]byte(nil), raw...),
		DataType:      c.InType.Name(),
		State:         message.StateReceived,
		ReceivedAt:    time.Now(),
		Meta:          copyMeta(meta),
	}
	if err := c.Recorder.Record(ctx, m); err != nil {
		return adapter.Receipt{}, fmt.Errorf("channel %s: recording message: %w", c.ID, err)
	}
	c.publishMessage(m.ID, message.StateReceived, "")

	done := make(chan adapter.AckDecision, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.pipeMu.Lock()
		defer c.pipeMu.Unlock()
		done <- c.process(context.WithoutCancel(ctx), m)
	}()
	return adapter.Receipt{MessageID: m.ID, Done: done}, nil
}

// process runs one message through the pipeline and returns the ACK
// decision for destination-ACK sources.
func (c *Channel) process(ctx context.Context, m *message.Message) adapter.AckDecision {
	log := c.Log.With("channel", c.ID, "message", m.ID)

	tree, err := c.InType.Parse(m.Raw)
	if err != nil {
		c.fail(ctx, m, fmt.Sprintf("parse (%s): %v", c.InType.Name(), err))
		return adapter.AckDecision{Code: "AE", Text: err.Error()}
	}
	m.Tree = tree

	if c.Filter != nil {
		keep, err := c.Filter(m)
		if err != nil {
			c.fail(ctx, m, fmt.Sprintf("filter: %v", err))
			return adapter.AckDecision{Code: "AE", Text: err.Error()}
		}
		if !keep {
			m.State = message.StateFiltered
			_ = c.Recorder.SetState(ctx, m.ID, message.StateFiltered, "")
			c.publishMessage(m.ID, message.StateFiltered, "")
			return adapter.AckDecision{Code: "AA"}
		}
	}

	for i, translate := range c.Translate {
		if err := translate(m); err != nil {
			c.fail(ctx, m, fmt.Sprintf("translator %d: %v", i+1, err))
			return adapter.AckDecision{Code: "AE", Text: err.Error()}
		}
	}
	outType := c.transformedType(m)
	if transformed, err := outType.Serialize(m.Tree); err == nil {
		_ = c.Recorder.SetTransformed(ctx, m.ID, transformed)
	}
	m.State = message.StateTransformed
	_ = c.Recorder.SetState(ctx, m.ID, message.StateTransformed, "")
	c.publishMessage(m.ID, message.StateTransformed, "")

	// Recipient List fan-out.
	decision := adapter.AckDecision{Code: "AA"}
	for _, d := range c.Destinations {
		if err := c.sendTo(ctx, d, m); err != nil {
			log.Warn("destination delivery failed", "destination", d.ID, "error", err)
			if d.WaitForAck && decision.Code == "AA" {
				code := "AE"
				if adapter.IsPermanent(err) {
					code = "AR"
				}
				decision = adapter.AckDecision{Code: code, Text: fmt.Sprintf("destination %s: %v", d.ID, err)}
			}
		}
	}
	return decision
}

// sendTo runs one destination's chain: filter, translators, serialize,
// deliver. Phase 2 delivers synchronously; the Guaranteed Delivery queue
// replaces the direct send for non-waitForAck destinations in phase 3.
func (c *Channel) sendTo(ctx context.Context, d *Destination, m *message.Message) error {
	dm := &message.Message{
		ID:            m.ID,
		ChannelID:     m.ChannelID,
		CorrelationID: m.CorrelationID,
		Raw:           m.Raw,
		Tree:          m.Tree.Clone(),
		DataType:      m.DataType,
		ReceivedAt:    m.ReceivedAt,
		Meta:          copyMeta(m.Meta),
	}

	if d.Filter != nil {
		keep, err := d.Filter(dm)
		if err != nil {
			_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateError, nil, fmt.Sprintf("filter: %v", err))
			c.publishDestination(m.ID, d.ID, message.StateError)
			return fmt.Errorf("filter: %w", err)
		}
		if !keep {
			_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateFiltered, nil, "")
			c.publishDestination(m.ID, d.ID, message.StateFiltered)
			return nil
		}
	}
	for i, translate := range d.Translate {
		if err := translate(dm); err != nil {
			_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateError, nil, fmt.Sprintf("translator %d: %v", i+1, err))
			c.publishDestination(m.ID, d.ID, message.StateError)
			return fmt.Errorf("translator %d: %w", i+1, err)
		}
	}

	payload, err := d.OutType.Serialize(dm.Tree)
	if err != nil {
		_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateError, nil, fmt.Sprintf("serialize (%s): %v", d.OutType.Name(), err))
		c.publishDestination(m.ID, d.ID, message.StateError)
		return fmt.Errorf("serialize: %w", err)
	}

	_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateQueued, payload, "")
	c.publishDestination(m.ID, d.ID, message.StateQueued)

	meta := copyMeta(dm.Meta)
	meta["message.id"] = fmt.Sprintf("%d", m.ID)
	meta["channel.id"] = c.ID
	meta["destination.id"] = d.ID
	if err := d.Adapter.Send(ctx, payload, meta); err != nil {
		_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateError, payload, err.Error())
		c.publishDestination(m.ID, d.ID, message.StateError)
		return err
	}
	_ = c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateSent, payload, "")
	c.publishDestination(m.ID, d.ID, message.StateSent)
	return nil
}

// transformedType is the data type of the tree after the channel translator
// chain: a translator may have replaced the tree for format conversion.
func (c *Channel) transformedType(m *message.Message) format.DataType {
	if m.DataType != c.InType.Name() {
		if dt, ok := format.Get(m.DataType); ok {
			return dt
		}
	}
	return c.InType
}

func (c *Channel) fail(ctx context.Context, m *message.Message, errText string) {
	m.State = message.StateError
	m.Error = errText
	_ = c.Recorder.SetState(ctx, m.ID, message.StateError, errText)
	c.publishMessage(m.ID, message.StateError, "")
	c.Log.Error("message routed to invalid message channel",
		"channel", c.ID, "message", m.ID, "error", errText)
}

func (c *Channel) publishMessage(id int64, state message.State, destID string) {
	if c.Bus == nil {
		return
	}
	c.Bus.Publish(events.Event{
		Type:          events.TypeMessage,
		ChannelID:     c.ID,
		MessageID:     id,
		State:         state,
		DestinationID: destID,
	})
}

func (c *Channel) publishDestination(id int64, destID string, state message.State) {
	c.publishMessage(id, state, destID)
}

func copyMeta(meta map[string]string) map[string]string {
	out := make(map[string]string, len(meta)+3)
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
