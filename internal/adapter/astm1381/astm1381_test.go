package astm1381

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

const sampleMsg = "H|\\^&|||LIS|||||||P|LIS2-A2|20260730\rP|1||PATID123||DOE^JOHN\rR|1|^^^GLU|105|mg/dL\rL|1|N\r"

// collector records deliveries and resolves Done with the given decision.
type collector struct {
	mu       sync.Mutex
	received [][]byte
	fail     error
	decision adapter.AckDecision
	delay    time.Duration
}

func (c *collector) deliver(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return adapter.Receipt{}, c.fail
	}
	c.received = append(c.received, append([]byte(nil), raw...))
	done := make(chan adapter.AckDecision, 1)
	decision := c.decision
	if decision.Code == "" {
		decision = adapter.AckDecision{Code: "AA"}
	}
	if c.delay > 0 {
		delay := c.delay
		go func() {
			time.Sleep(delay)
			done <- decision
		}()
	} else {
		done <- decision
	}
	return adapter.Receipt{MessageID: int64(len(c.received)), Done: done}, nil
}

func (c *collector) messages() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.received...)
}

func startListener(t *testing.T, settings map[string]any, deliver adapter.DeliverFunc) *Listener {
	t.Helper()
	settings["addr"] = "127.0.0.1:0"
	l, err := NewListener(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Stop() })
	return l
}

func newSender(t *testing.T, addr string, extra map[string]any) *Sender {
	t.Helper()
	settings := map[string]any{"addr": addr, "ackTimeout": "5s"}
	for k, v := range extra {
		settings[k] = v
	}
	s, err := NewSender(settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestLoopbackSingleMessage(t *testing.T) {
	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	s := newSender(t, l.Addr(), nil)

	if err := s.Send(context.Background(), []byte(sampleMsg), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	got := c.messages()
	if len(got) != 1 || string(got[0]) != sampleMsg {
		t.Fatalf("received = %d messages, first %q", len(got), got)
	}

	// A second session over the same connection.
	second := strings.Replace(sampleMsg, "PATID123", "PATID456", 1)
	if err := s.Send(context.Background(), []byte(second), nil); err != nil {
		t.Fatalf("second send: %v", err)
	}
	if got := c.messages(); len(got) != 2 || string(got[1]) != second {
		t.Fatalf("second message: %d received", len(got))
	}
}

func TestLoopbackFrameWraparound(t *testing.T) {
	// 20 records → frame numbers wrap 1..7,0,1... twice.
	var sb strings.Builder
	sb.WriteString("H|\\^&\r")
	for i := 0; i < 18; i++ {
		sb.WriteString("C|comment record\r")
	}
	sb.WriteString("L|1\r")
	msg := sb.String()

	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	s := newSender(t, l.Addr(), nil)
	if err := s.Send(context.Background(), []byte(msg), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := c.messages(); len(got) != 1 || string(got[0]) != msg {
		t.Fatal("wraparound message mangled")
	}
}

func TestLoopbackLongRecordSplit(t *testing.T) {
	long := "R|1|^^^NOTE|" + strings.Repeat("x", 600) + "\r"
	msg := "H|\\^&\r" + long + "L|1\r"

	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	s := newSender(t, l.Addr(), nil)
	if err := s.Send(context.Background(), []byte(msg), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := c.messages(); len(got) != 1 || string(got[0]) != msg {
		t.Fatal("ETB-split message mangled")
	}
}

// rawClient speaks the wire protocol directly for fault-injection tests.
type rawClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &rawClient{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func (r *rawClient) send(p []byte) {
	r.t.Helper()
	if _, err := r.conn.Write(p); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rawClient) expect(want byte) {
	r.t.Helper()
	_ = r.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, err := r.br.ReadByte()
	if err != nil {
		r.t.Fatalf("expected 0x%02X, read failed: %v", want, err)
	}
	if b != want {
		r.t.Fatalf("expected 0x%02X, got 0x%02X", want, b)
	}
}

func TestListenerNAKsCorruptFrameThenAcceptsRetransmit(t *testing.T) {
	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	raw := dialRaw(t, l.Addr())

	raw.send([]byte{enq})
	raw.expect(ack)

	good := encodeFrame(frame{Number: '1', Text: []byte("H|\\^&\rL|1\r"), Last: true})
	corrupt := append([]byte(nil), good...)
	corrupt[3] ^= 0xFF // flip a text byte: checksum mismatch

	raw.send(corrupt)
	raw.expect(nak)
	raw.send(good) // retransmit
	raw.expect(ack)
	raw.send([]byte{eot})

	if got := c.messages(); len(got) != 1 || string(got[0]) != "H|\\^&\rL|1\r" {
		t.Fatalf("received after retransmit = %q", got)
	}
}

func TestListenerWrongFrameNumberNAKed(t *testing.T) {
	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	raw := dialRaw(t, l.Addr())

	raw.send([]byte{enq})
	raw.expect(ack)
	// First frame must be number '1'; send '3'.
	raw.send(encodeFrame(frame{Number: '3', Text: []byte("H|\\^&\r"), Last: false}))
	raw.expect(nak)
	// Correct frame is then accepted.
	raw.send(encodeFrame(frame{Number: '1', Text: []byte("H|\\^&\r"), Last: true}))
	raw.expect(ack)
	raw.send([]byte{eot})
	if len(c.messages()) != 1 {
		t.Fatal("message not delivered after sequence recovery")
	}
}

func TestListenerDuplicateFinalFrameNotRedelivered(t *testing.T) {
	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	raw := dialRaw(t, l.Addr())

	raw.send([]byte{enq})
	raw.expect(ack)
	final := encodeFrame(frame{Number: '1', Text: []byte("H|\\^&\rL|1\r"), Last: true})
	raw.send(final)
	raw.expect(ack)
	// The ACK "was lost": sender retransmits the same frame. The listener
	// re-ACKs without delivering a duplicate.
	raw.send(final)
	raw.expect(ack)
	raw.send([]byte{eot})

	time.Sleep(50 * time.Millisecond)
	if got := c.messages(); len(got) != 1 {
		t.Fatalf("duplicate final frame delivered %d messages", len(got))
	}
}

func TestListenerDeliverFailureNAKsFinalFrame(t *testing.T) {
	c := &collector{fail: errors.New("channel not accepting")}
	l := startListener(t, map[string]any{}, c.deliver)
	s := newSender(t, l.Addr(), map[string]any{"maxRetries": 2})

	err := s.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !strings.Contains(err.Error(), "rejected 2 times") {
		t.Fatalf("expected retry exhaustion, got %v", err)
	}
	if adapter.IsPermanent(err) {
		t.Error("E1381 failures must be transient (queue retries)")
	}

	// The channel recovers: the same sender delivers on the next attempt.
	c.mu.Lock()
	c.fail = nil
	c.mu.Unlock()
	if err := s.Send(context.Background(), []byte(sampleMsg), nil); err != nil {
		t.Fatalf("send after recovery: %v", err)
	}
	if len(c.messages()) != 1 {
		t.Fatal("recovered send not delivered")
	}
}

func TestListenerDestinationModeRejectionInterrupts(t *testing.T) {
	c := &collector{decision: adapter.AckDecision{Code: "AR", Text: "validation failed"}, delay: 20 * time.Millisecond}
	l := startListener(t, map[string]any{"ackMode": "destination"}, c.deliver)
	s := newSender(t, l.Addr(), nil)

	err := s.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("expected EOT interrupt, got %v", err)
	}
	// The message WAS recorded (persist-before-ACK); only the transport
	// handshake reports failure.
	if len(c.messages()) != 1 {
		t.Fatal("message should have been recorded before rejection")
	}
}

func TestListenerDestinationModeSuccessAcks(t *testing.T) {
	c := &collector{decision: adapter.AckDecision{Code: "AA"}, delay: 20 * time.Millisecond}
	l := startListener(t, map[string]any{"ackMode": "destination"}, c.deliver)
	s := newSender(t, l.Addr(), nil)
	if err := s.Send(context.Background(), []byte(sampleMsg), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func TestListenerHoldTimeoutInterrupts(t *testing.T) {
	c := &collector{delay: 5 * time.Second} // pipeline never resolves in time
	l := startListener(t, map[string]any{"ackMode": "destination", "holdTimeout": "100ms"}, c.deliver)
	s := newSender(t, l.Addr(), nil)
	err := s.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("expected interrupt on hold timeout, got %v", err)
	}
}

func TestSenderReceiverDown(t *testing.T) {
	s := newSender(t, "127.0.0.1:1", map[string]any{"connectTimeout": "200ms"})
	err := s.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || adapter.IsPermanent(err) {
		t.Fatalf("expected transient connect error, got %v", err)
	}
}

func TestSenderBusyReceiver(t *testing.T) {
	// A raw server that NAKs the ENQ (busy), then accepts the next session.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		// Session 1: busy.
		if b, _ := br.ReadByte(); b == enq {
			conn.Write([]byte{nak})
		}
		// Session 2: accept everything.
		if b, _ := br.ReadByte(); b == enq {
			conn.Write([]byte{ack})
		}
		for {
			b, err := br.ReadByte()
			if err != nil {
				return
			}
			switch b {
			case stx:
				f, err := readFrameAfter(stx, br, defaultFrameSize)
				if err != nil {
					conn.Write([]byte{nak})
					continue
				}
				conn.Write([]byte{ack})
				// After the last frame keep reading until EOT.
				_ = f.Last
			case eot:
				return
			}
		}
	}()

	s := newSender(t, ln.Addr().String(), nil)
	err = s.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("expected busy error, got %v", err)
	}
	// Retry (as the queue would) succeeds over the same connection.
	if err := s.Send(context.Background(), []byte(sampleMsg), nil); err != nil {
		t.Fatalf("retry after busy: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := NewListener(map[string]any{}); err == nil {
		t.Error("missing addr should fail")
	}
	if _, err := NewListener(map[string]any{"addr": ":0", "ackMode": "bogus"}); err == nil {
		t.Error("bad ackMode should fail")
	}
	if _, err := NewListener(map[string]any{"addr": ":0", "unknown": 1}); err == nil {
		t.Error("unknown setting should fail")
	}
	if _, err := NewSender(map[string]any{}); err == nil {
		t.Error("missing sender addr should fail")
	}
	// FrameSize above the E1381 maximum clamps to 240.
	s, err := NewSender(map[string]any{"addr": "x:1", "frameSize": 9999})
	if err != nil || s.cfg.FrameSize != defaultFrameSize {
		t.Errorf("frameSize clamp: %d, %v", s.cfg.FrameSize, err)
	}
}

func TestMessageBytesArePreservedVerbatim(t *testing.T) {
	// Content with characters that must pass through untouched (the E1394
	// layer handles escaping; E1381 is 8-bit clean apart from control bytes).
	msg := []byte("H|\\^&\rC|1|text with &E& escapes ~ and pipes||\rL|1\r")
	c := &collector{}
	l := startListener(t, map[string]any{}, c.deliver)
	s := newSender(t, l.Addr(), nil)
	if err := s.Send(context.Background(), msg, nil); err != nil {
		t.Fatal(err)
	}
	if got := c.messages(); !bytes.Equal(got[0], msg) {
		t.Fatalf("payload altered in transit:\n got %q\nwant %q", got[0], msg)
	}
}
