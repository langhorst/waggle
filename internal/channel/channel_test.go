package channel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
	"github.com/langhorst/integration-channel/internal/events"
	"github.com/langhorst/integration-channel/internal/format"
	"github.com/langhorst/integration-channel/internal/message"

	_ "github.com/langhorst/integration-channel/internal/format/csvfmt"
	_ "github.com/langhorst/integration-channel/internal/format/hl7v2"
)

const sampleHL7 = "MSH|^~\\&|SEND|SFAC|RECV|RFAC|20260730||ADT^A01|CTRL001|P|2.5\rPID|1||MRN1||DOE^JOHN\r"

// fakeSource delivers messages on demand.
type fakeSource struct {
	deliver adapter.DeliverFunc
	started bool
}

func (s *fakeSource) Start(ctx context.Context, deliver adapter.DeliverFunc) error {
	s.deliver = deliver
	s.started = true
	return nil
}
func (s *fakeSource) Stop() error { s.started = false; return nil }

// fakeOut records sent payloads and can be programmed to fail.
type fakeOut struct {
	mu       sync.Mutex
	payloads [][]byte
	fail     error
}

func (o *fakeOut) Open(ctx context.Context) error { return nil }
func (o *fakeOut) Close() error                   { return nil }
func (o *fakeOut) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fail != nil {
		return o.fail
	}
	o.payloads = append(o.payloads, append([]byte(nil), payload...))
	return nil
}
func (o *fakeOut) sent() [][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([][]byte(nil), o.payloads...)
}

// recordingRecorder captures every state transition for assertions.
type recordingRecorder struct {
	MemoryRecorder
	mu     sync.Mutex
	states []string
}

func (r *recordingRecorder) log(f string, args ...any) {
	r.mu.Lock()
	r.states = append(r.states, fmt.Sprintf(f, args...))
	r.mu.Unlock()
}

func (r *recordingRecorder) SetState(ctx context.Context, id int64, state message.State, errText string) error {
	r.log("msg:%s", state)
	return nil
}
func (r *recordingRecorder) SetTransformed(ctx context.Context, id int64, payload []byte) error {
	r.log("transformed")
	return nil
}
func (r *recordingRecorder) SetDestinationState(ctx context.Context, id int64, destID string, state message.State, payload []byte, errText string) error {
	r.log("dest:%s:%s", destID, state)
	return nil
}
func (r *recordingRecorder) history() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.states...)
}

func hl7Type() format.DataType {
	dt, _ := format.Get("hl7v2")
	return dt
}
func csvType() format.DataType {
	dt, _ := format.Get("csv")
	return dt
}

func newTestChannel(rec Recorder, dests ...*Destination) (*Channel, *fakeSource) {
	src := &fakeSource{}
	ch := &Channel{
		ID:           "test",
		Name:         "Test",
		InType:       hl7Type(),
		Source:       src,
		Destinations: dests,
		Recorder:     rec,
		Bus:          events.NewBus(),
		Log:          slog.New(slog.DiscardHandler),
	}
	return ch, src
}

func deliverAndWait(t *testing.T, src *fakeSource, raw string) adapter.AckDecision {
	t.Helper()
	rec, err := src.deliver(context.Background(), []byte(raw), map[string]string{"origin": "test"})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	select {
	case d := <-rec.Done:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not complete")
		return adapter.AckDecision{}
	}
}

func TestPipelineHappyPath(t *testing.T) {
	rec := &recordingRecorder{}
	out := &fakeOut{}
	ch, src := newTestChannel(rec, &Destination{ID: "d1", OutType: hl7Type(), Adapter: out})
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	d := deliverAndWait(t, src, sampleHL7)
	if d.Code != "AA" {
		t.Errorf("decision = %+v", d)
	}
	sent := out.sent()
	if len(sent) != 1 || !strings.HasPrefix(string(sent[0]), "MSH|") {
		t.Fatalf("sent = %v", sent)
	}
	want := []string{"transformed", "msg:TRANSFORMED", "dest:d1:QUEUED", "dest:d1:SENT"}
	if got := rec.history(); !equalStrings(got, want) {
		t.Errorf("history = %v, want %v", got, want)
	}
}

func TestPipelineParseError(t *testing.T) {
	rec := &recordingRecorder{}
	out := &fakeOut{}
	ch, src := newTestChannel(rec, &Destination{ID: "d1", OutType: hl7Type(), Adapter: out})
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	d := deliverAndWait(t, src, "not an hl7 message")
	if d.Code != "AE" {
		t.Errorf("decision = %+v", d)
	}
	if len(out.sent()) != 0 {
		t.Error("nothing should be sent on parse error")
	}
	if got := rec.history(); !equalStrings(got, []string{"msg:ERROR"}) {
		t.Errorf("history = %v", got)
	}
}

func TestPipelineChannelFilterDrops(t *testing.T) {
	rec := &recordingRecorder{}
	out := &fakeOut{}
	ch, src := newTestChannel(rec, &Destination{ID: "d1", OutType: hl7Type(), Adapter: out})
	ch.Filter = func(m *message.Message) (bool, error) { return false, nil }
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	d := deliverAndWait(t, src, sampleHL7)
	if d.Code != "AA" {
		t.Errorf("filtered message should still ACK AA, got %+v", d)
	}
	if len(out.sent()) != 0 {
		t.Error("filtered message must not reach destinations")
	}
	if got := rec.history(); !equalStrings(got, []string{"msg:FILTERED"}) {
		t.Errorf("history = %v", got)
	}
}

func TestPipelineTranslatorChainAndFormatConversion(t *testing.T) {
	rec := &recordingRecorder{}
	out := &fakeOut{}
	hl7 := hl7Type()
	ch, src := newTestChannel(rec, &Destination{
		ID:      "csv-out",
		OutType: csvType(),
		Adapter: out,
		Translate: []TranslateFunc{func(m *message.Message) error {
			// Format conversion: build a CSV tree from the HL7 tree.
			name, mrn := "", ""
			if nodes, _ := hl7.Resolve(m.Tree, "PID-5.1"); len(nodes) > 0 {
				name = hl7.Value(m.Tree, nodes[0])
			}
			if nodes, _ := hl7.Resolve(m.Tree, "PID-3"); len(nodes) > 0 {
				mrn = hl7.Value(m.Tree, nodes[0])
			}
			csv := csvType()
			root := &message.Node{Name: "csv"}
			_ = csv.Set(root, "R.1", mrn)
			_ = csv.Set(root, "R.2", name)
			m.Tree = root
			m.DataType = "csv"
			return nil
		}},
	})
	// Channel-level chain: two steps, order matters.
	ch.Translate = []TranslateFunc{
		func(m *message.Message) error { return hl7.Set(m.Tree, "PID-5.1", "SMITH") },
		func(m *message.Message) error {
			nodes, _ := hl7.Resolve(m.Tree, "PID-5.1")
			return hl7.Set(m.Tree, "PID-5.1", strings.ToLower(hl7.Value(m.Tree, nodes[0])))
		},
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	d := deliverAndWait(t, src, sampleHL7)
	if d.Code != "AA" {
		t.Fatalf("decision = %+v", d)
	}
	sent := out.sent()
	if len(sent) != 1 || string(sent[0]) != "MRN1,smith\n" {
		t.Errorf("csv output = %q", sent)
	}
}

func TestPipelineDestinationFilterAndError(t *testing.T) {
	rec := &recordingRecorder{}
	okOut, failOut := &fakeOut{}, &fakeOut{fail: errors.New("receiver down")}
	ch, src := newTestChannel(rec,
		&Destination{
			ID: "skipped", OutType: hl7Type(), Adapter: okOut,
			Filter: func(m *message.Message) (bool, error) { return false, nil },
		},
		&Destination{ID: "failing", OutType: hl7Type(), Adapter: failOut},
		&Destination{ID: "working", OutType: hl7Type(), Adapter: okOut},
	)
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	// No waitForAck destinations: delivery failures don't affect the ACK.
	d := deliverAndWait(t, src, sampleHL7)
	if d.Code != "AA" {
		t.Errorf("decision = %+v", d)
	}
	if len(okOut.sent()) != 1 {
		t.Error("working destination should have received the message")
	}
	history := strings.Join(rec.history(), " ")
	for _, want := range []string{"dest:skipped:FILTERED", "dest:failing:ERROR", "dest:working:SENT"} {
		if !strings.Contains(history, want) {
			t.Errorf("history missing %s: %v", want, rec.history())
		}
	}
}

func TestPipelineWaitForAckFailure(t *testing.T) {
	rec := &recordingRecorder{}
	failOut := &fakeOut{fail: errors.New("receiver down")}
	ch, src := newTestChannel(rec,
		&Destination{ID: "critical", OutType: hl7Type(), Adapter: failOut, WaitForAck: true},
	)
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	d := deliverAndWait(t, src, sampleHL7)
	if d.Code != "AE" || !strings.Contains(d.Text, "critical") {
		t.Errorf("decision = %+v, want AE naming the destination", d)
	}
}

func TestPipelineWaitForAckPermanentRejection(t *testing.T) {
	rec := &recordingRecorder{}
	failOut := &fakeOut{fail: adapter.Permanent(errors.New("AR: bad message"))}
	ch, src := newTestChannel(rec,
		&Destination{ID: "critical", OutType: hl7Type(), Adapter: failOut, WaitForAck: true},
	)
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.Stop()

	d := deliverAndWait(t, src, sampleHL7)
	if d.Code != "AR" {
		t.Errorf("permanent rejection should surface as AR, got %+v", d)
	}
}

func TestLifecycle(t *testing.T) {
	rec := &recordingRecorder{}
	out := &fakeOut{}
	ch, src := newTestChannel(rec, &Destination{ID: "d1", OutType: hl7Type(), Adapter: out})

	if ch.Status() != StatusStopped {
		t.Errorf("initial status = %s", ch.Status())
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ch.Status() != StatusStarted || !src.started {
		t.Error("channel should be started")
	}
	if err := ch.Pause(); err != nil {
		t.Fatal(err)
	}
	if ch.Status() != StatusPaused || src.started {
		t.Error("paused channel should stop its source")
	}
	// Delivery while paused is refused (file stays put, MLLP NAKs).
	if _, err := src.deliver(context.Background(), []byte(sampleHL7), nil); err == nil {
		t.Error("paused channel must refuse delivery")
	}
	if err := ch.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ch.Status() != StatusStarted {
		t.Error("resume should restart intake")
	}
	if err := ch.Stop(); err != nil {
		t.Fatal(err)
	}
	if ch.Status() != StatusStopped {
		t.Error("channel should be stopped")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
