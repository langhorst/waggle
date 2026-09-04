// Package channel implements the channel runtime: one inbound Channel
// Adapter feeding a Pipes-and-Filters pipeline — Message Filter, Message
// Translator chain, then a Recipient List of destinations, each with its own
// optional filter/translator chain and outbound Channel Adapter.
//
// Message processing is strictly sequential per channel: one pipeline
// goroutine drains a bounded intake buffer in arrival order. When the
// buffer is full the source adapter blocks in deliver, which delays its
// transport ACK, so backpressure reaches the sender instead of piling up
// goroutines. Every state transition goes through the Recorder (the
// persistence seam) and is published on the event bus.
package channel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
	metakey "github.com/langhorst/waggle/internal/meta"
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

// Rejection is a deliberate, application-level rejection raised by a filter
// or transformer script (response.reject in the script API): processing
// stops, the message lands in the Invalid Message Channel, and in
// destination-ACK mode the source receives exactly this code and text.
type Rejection struct {
	Code string // MSA-1 style: usually "AR" or "AE"
	Text string
}

func (r *Rejection) Error() string { return fmt.Sprintf("rejected (%s): %s", r.Code, r.Text) }

// FilterFunc is a Message Filter step: false drops the message (state
// FILTERED, retained). An error sends the message to the Invalid Message
// Channel.
type FilterFunc func(m *message.Message) (bool, error)

// TranslateFunc is a Message Translator step. It may mutate m.Tree in place
// or replace it entirely (and change m.DataType) for format conversion.
type TranslateFunc func(m *message.Message) error

// Recorder is the persistence seam: the pipeline reports every lifecycle
// transition through it. The SQLite store is the production implementation;
// NewMemoryRecorder serves tests and storeless runs.
type Recorder interface {
	// Record persists a freshly received message and assigns m.ID. Once it
	// returns nil the engine owns the message (Guaranteed Delivery handoff).
	Record(ctx context.Context, m *message.Message) error
	// SetState records a pipeline-level state change.
	SetState(ctx context.Context, id int64, state message.State, errText string) error
	// SetTransformed stores the serialized output of the channel-level
	// translator chain and its data type — which may differ from the
	// inbound type after format conversion (feeds the received-vs-sent
	// diff and the tree explorer).
	SetTransformed(ctx context.Context, id int64, payload []byte, dataType string) error
	// SetDestinationState records a per-destination state change; payload is
	// the destination-serialized bytes when known.
	// meta, when non-nil, replaces the delivery's stored metadata (script
	// meta writes that outbound adapters read, e.g. http.path); nil keeps
	// whatever is stored.
	SetDestinationState(ctx context.Context, id int64, destID string, state message.State, payload []byte, meta map[string]string, errText string) error
}

// Queuer hands a recorded delivery to the Guaranteed Delivery queue (the
// serialized payload is already stored via SetDestinationState). Implemented
// by the store; nil means deliveries happen synchronously in the pipeline.
type Queuer interface {
	Enqueue(ctx context.Context, channelID, destID string, messageID int64) error
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
	// Queue enables Guaranteed Delivery: non-waitForAck destinations are
	// enqueued for their per-destination worker instead of sent inline. Nil
	// keeps deliveries synchronous (tests, storeless runs).
	Queue Queuer
	Bus   *events.Bus
	Log   *slog.Logger
	// MaxPending bounds messages recorded but not yet processed. Zero
	// means DefaultMaxPending.
	MaxPending int

	// lifecycleMu serializes Start/Pause/Resume/Stop. It is held across
	// adapter calls; mu never is. Inbound adapters call deliver from their
	// own goroutines and Stop implementations wait for those goroutines,
	// so holding mu (which deliver needs) while calling Source.Stop would
	// deadlock the moment a message arrived mid-transition.
	lifecycleMu sync.Mutex
	// mu guards the fields below.
	mu        sync.Mutex
	status    Status
	stopping  bool // Stop in progress: refuse intake, keep draining
	runCancel context.CancelFunc
	// pending feeds the pipeline goroutine; nil while stopped. inflight
	// counts deliver/Inject calls between their status check and their
	// handoff into pending, so Stop knows when it may close the buffer.
	pending  chan *pendingMessage
	pipeDone chan struct{}
	inflight sync.WaitGroup
}

// DefaultMaxPending is the intake buffer size when MaxPending is unset.
const DefaultMaxPending = 256

// pendingMessage is one recorded message waiting for the pipeline.
type pendingMessage struct {
	ctx  context.Context
	m    *message.Message
	done chan<- adapter.AckDecision // nil when nobody waits (replay)
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
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	switch c.Status() {
	case StatusStarted:
		return nil
	case StatusPaused:
		return c.resumeLocked(ctx)
	}
	runCtx, cancel := context.WithCancel(ctx)
	for _, d := range c.Destinations {
		if err := d.Adapter.Open(runCtx); err != nil {
			cancel()
			return fmt.Errorf("channel %s: destination %s: %w", c.ID, d.ID, err)
		}
	}
	size := c.MaxPending
	if size <= 0 {
		size = DefaultMaxPending
	}
	pending := make(chan *pendingMessage, size)
	pipeDone := make(chan struct{})
	c.mu.Lock()
	c.pending, c.pipeDone = pending, pipeDone
	c.mu.Unlock()
	go c.run(pending, pipeDone)

	if err := c.Source.Start(runCtx, c.deliver); err != nil {
		cancel()
		c.mu.Lock()
		c.pending, c.pipeDone = nil, nil
		c.mu.Unlock()
		close(pending)
		<-pipeDone
		for _, d := range c.Destinations {
			_ = d.Adapter.Close()
		}
		return fmt.Errorf("channel %s: source: %w", c.ID, err)
	}
	c.mu.Lock()
	c.runCancel = cancel
	c.setStatusLocked(StatusStarted)
	c.mu.Unlock()
	return nil
}

// run is the pipeline goroutine: it processes pending messages one at a
// time, in arrival order, until the buffer is closed and drained.
func (c *Channel) run(pending <-chan *pendingMessage, done chan<- struct{}) {
	defer close(done)
	for pm := range pending {
		decision := c.process(pm.ctx, pm.m)
		if pm.done != nil {
			pm.done <- decision
		}
	}
}

// Pause stops intake only. Messages the source hands over while the stop
// is in progress are still accepted and processed: a transport that has
// already read a frame needs to answer it.
func (c *Channel) Pause() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.Status() != StatusStarted {
		return fmt.Errorf("channel %s: not started", c.ID)
	}
	if err := c.Source.Stop(); err != nil {
		return err
	}
	c.setStatus(StatusPaused)
	return nil
}

// Resume restarts intake on a paused channel.
func (c *Channel) Resume(ctx context.Context) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.Status() != StatusPaused {
		return fmt.Errorf("channel %s: not paused", c.ID)
	}
	return c.resumeLocked(ctx)
}

func (c *Channel) resumeLocked(ctx context.Context) error {
	if err := c.Source.Start(ctx, c.deliver); err != nil {
		return err
	}
	c.setStatus(StatusStarted)
	return nil
}

// Stop halts intake, drains messages already accepted, and closes
// destination adapters.
func (c *Channel) Stop() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	status := c.status
	c.mu.Unlock()
	if status != StatusStarted && status != StatusPaused {
		c.setStatus(StatusStopped)
		return nil
	}

	// 1. The source stops producing. Its goroutines may be blocked in
	//    deliver on a full buffer; the pipeline goroutine is still
	//    draining, so they return.
	if status == StatusStarted {
		_ = c.Source.Stop()
	}
	// 2. Refuse new intake, wait for handoffs already past the status
	//    check, then close the buffer and let the pipeline drain it.
	c.mu.Lock()
	c.stopping = true
	pending, pipeDone := c.pending, c.pipeDone
	cancel := c.runCancel
	c.pending, c.pipeDone, c.runCancel = nil, nil, nil
	c.mu.Unlock()
	c.inflight.Wait()
	if pending != nil {
		close(pending)
		<-pipeDone
	}
	// 3. Nothing is processing any more: tear down adapters.
	if cancel != nil {
		cancel()
	}
	for _, d := range c.Destinations {
		_ = d.Adapter.Close()
	}
	c.mu.Lock()
	c.stopping = false
	c.setStatusLocked(StatusStopped)
	c.mu.Unlock()
	return nil
}

func (c *Channel) setStatus(s Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setStatusLocked(s)
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
	pending, err := c.admit(StatusStarted)
	if err != nil {
		return adapter.Receipt{}, err
	}
	defer c.inflight.Done()

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
	c.handoff(ctx, pending, m, done)
	return adapter.Receipt{MessageID: m.ID, Done: done}, nil
}

// Inject records and processes a message that did not arrive through the
// source adapter — message replay. The channel must not be stopped
// (paused is fine: replay bypasses the source).
func (c *Channel) Inject(ctx context.Context, m *message.Message) (int64, error) {
	pending, err := c.admit(StatusStarted, StatusPaused)
	if err != nil {
		return 0, fmt.Errorf("%w; start it to replay messages", err)
	}
	defer c.inflight.Done()
	if err := c.Recorder.Record(ctx, m); err != nil {
		return 0, fmt.Errorf("channel %s: recording replay: %w", c.ID, err)
	}
	c.publishMessage(m.ID, message.StateReceived, "")
	c.handoff(ctx, pending, m, nil)
	return m.ID, nil
}

// admit checks that the channel accepts intake in one of the given states
// and registers the caller as in flight (the caller must inflight.Done()).
// The returned buffer is the one to hand the message to; Stop closes it
// only after every in-flight caller has finished.
func (c *Channel) admit(states ...Status) (chan *pendingMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ok := !c.stopping && c.pending != nil
	if ok {
		ok = false
		for _, s := range states {
			if c.status == s {
				ok = true
				break
			}
		}
	}
	if !ok {
		return nil, fmt.Errorf("channel %s: not accepting messages", c.ID)
	}
	c.inflight.Add(1)
	return c.pending, nil
}

// handoff queues a recorded message for the pipeline. It blocks while the
// buffer is full: the message is already recorded (the engine owns it),
// so it must be processed, and blocking here is what turns a burst into
// backpressure on the transport. The wait is bounded by the pipeline
// draining, which Stop keeps running until the buffer is empty.
func (c *Channel) handoff(ctx context.Context, pending chan *pendingMessage, m *message.Message, done chan<- adapter.AckDecision) {
	pending <- &pendingMessage{ctx: context.WithoutCancel(ctx), m: m, done: done}
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
			return decisionForError(err)
		}
		if !keep {
			m.State = message.StateFiltered
			if err := c.Recorder.SetState(ctx, m.ID, message.StateFiltered, ""); err != nil {
				c.fail(ctx, m, fmt.Sprintf("recording filtered state: %v", err))
				return storeFailure(err)
			}
			c.publishMessage(m.ID, message.StateFiltered, "")
			return adapter.AckDecision{Code: "AA"}
		}
	}

	for i, translate := range c.Translate {
		if err := translate(m); err != nil {
			c.fail(ctx, m, fmt.Sprintf("translator %d: %v", i+1, err))
			return decisionForError(err)
		}
	}
	outType := c.transformedType(m)
	transformed, err := outType.Serialize(m.Tree)
	if err != nil {
		c.fail(ctx, m, fmt.Sprintf("serialize (%s): %v", outType.Name(), err))
		return adapter.AckDecision{Code: "AE", Text: err.Error()}
	}
	if err := c.Recorder.SetTransformed(ctx, m.ID, transformed, outType.Name()); err != nil {
		c.fail(ctx, m, fmt.Sprintf("recording transformed payload: %v", err))
		return storeFailure(err)
	}
	m.State = message.StateTransformed
	if err := c.Recorder.SetState(ctx, m.ID, message.StateTransformed, ""); err != nil {
		c.fail(ctx, m, fmt.Sprintf("recording transformed state: %v", err))
		return storeFailure(err)
	}
	c.publishMessage(m.ID, message.StateTransformed, "")

	// Recipient List fan-out.
	decision := adapter.AckDecision{Code: "AA"}
	// A channel-level script may have set an explicit ACK without stopping
	// processing (response.setAck).
	if m.AckCode != "" {
		decision = adapter.AckDecision{Code: m.AckCode, Text: m.AckText}
	}
	for _, d := range c.Destinations {
		if err := c.sendTo(ctx, d, m); err != nil {
			log.Warn("destination delivery failed", "destination", d.ID, "error", err)
			if d.WaitForAck && decision.Code == "AA" {
				var rej *Rejection
				switch {
				case errors.As(err, &rej):
					decision = adapter.AckDecision{Code: rej.Code, Text: rej.Text}
				case adapter.IsPermanent(err):
					decision = adapter.AckDecision{Code: "AR", Text: fmt.Sprintf("destination %s: %v", d.ID, err)}
				default:
					decision = adapter.AckDecision{Code: "AE", Text: fmt.Sprintf("destination %s: %v", d.ID, err)}
				}
			}
		}
	}
	return decision
}

// storeFailure is the ACK for a message whose outcome could not be
// recorded. The engine no longer owns it durably, so the sender gets AE
// (retry later), never AR.
func storeFailure(err error) adapter.AckDecision {
	return adapter.AckDecision{Code: "AE", Text: "persistence failure: " + err.Error()}
}

// decisionForError maps a pipeline error to the source ACK: script
// rejections carry their own code and text, anything else is AE.
func decisionForError(err error) adapter.AckDecision {
	var rej *Rejection
	if errors.As(err, &rej) {
		return adapter.AckDecision{Code: rej.Code, Text: rej.Text}
	}
	return adapter.AckDecision{Code: "AE", Text: err.Error()}
}

// sendTo runs one destination's chain: filter, translators, serialize,
// deliver. Non-waitForAck destinations hand off to the Guaranteed Delivery
// queue when one is configured; everything else is sent inline.
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
			return c.failDestination(ctx, m, d, nil, fmt.Errorf("filter: %w", err))
		}
		if !keep {
			c.recordDestination(ctx, m, d, message.StateFiltered, nil, nil, "")
			return nil
		}
	}
	for i, translate := range d.Translate {
		if err := translate(dm); err != nil {
			return c.failDestination(ctx, m, d, nil, fmt.Errorf("translator %d: %w", i+1, err))
		}
	}

	payload, err := d.OutType.Serialize(dm.Tree)
	if err != nil {
		return c.failDestination(ctx, m, d, nil, fmt.Errorf("serialize (%s): %w", d.OutType.Name(), err))
	}

	// The QUEUED record carries the payload the worker will send, so it
	// must be durable before the queue row exists: a queue entry with no
	// payload behind it is a delivery of nothing.
	if err := c.Recorder.SetDestinationState(ctx, m.ID, d.ID, message.StateQueued, payload, dm.Meta, ""); err != nil {
		return c.failDestination(ctx, m, d, payload, fmt.Errorf("recording delivery: %w", err))
	}
	c.publishDestination(m.ID, d.ID, message.StateQueued)

	// Guaranteed Delivery: hand non-waitForAck deliveries to the queue
	// worker. waitForAck destinations stay synchronous — single attempt, the
	// upstream sender owns retry (their outcome drives the source ACK).
	if c.Queue != nil && !d.WaitForAck {
		if err := c.Queue.Enqueue(ctx, c.ID, d.ID, m.ID); err != nil {
			return c.failDestination(ctx, m, d, payload, fmt.Errorf("enqueue: %w", err))
		}
		return nil
	}

	meta := copyMeta(dm.Meta)
	meta[metakey.MessageID] = fmt.Sprintf("%d", m.ID)
	meta[metakey.ChannelID] = c.ID
	meta[metakey.DestinationID] = d.ID
	if err := d.Adapter.Send(ctx, payload, meta); err != nil {
		return c.failDestination(ctx, m, d, payload, err)
	}
	c.recordDestination(ctx, m, d, message.StateSent, payload, nil, "")
	return nil
}

// recordDestination writes one destination's state and publishes it. A
// store failure here is logged rather than returned: the delivery outcome
// is already decided (sent, filtered, or failed) and the caller has no
// better recourse. The one state that must not be lost, QUEUED with its
// payload, is written directly in sendTo and checked there.
func (c *Channel) recordDestination(ctx context.Context, m *message.Message, d *Destination, state message.State, payload []byte, meta map[string]string, errText string) {
	if err := c.Recorder.SetDestinationState(ctx, m.ID, d.ID, state, payload, meta, errText); err != nil {
		c.Log.Error("recording destination state failed",
			"channel", c.ID, "message", m.ID, "destination", d.ID, "state", state, "error", err)
	}
	c.publishDestination(m.ID, d.ID, state)
}

// failDestination records a destination-level failure and returns cause
// unchanged, so callers can still classify it (Rejection, Permanent).
func (c *Channel) failDestination(ctx context.Context, m *message.Message, d *Destination, payload []byte, cause error) error {
	c.recordDestination(ctx, m, d, message.StateError, payload, nil, cause.Error())
	return cause
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
	if err := c.Recorder.SetState(ctx, m.ID, message.StateError, errText); err != nil {
		c.Log.Error("recording error state failed",
			"channel", c.ID, "message", m.ID, "error", err)
	}
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
