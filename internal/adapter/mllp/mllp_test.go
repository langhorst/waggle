package mllp

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
)

const sampleMsg = "MSH|^~\\&|SEND|SFAC|RECV|RFAC|20260730||ADT^A01|CTRL001|P|2.5\rPID|1||X\r"

func TestBuildAck(t *testing.T) {
	ack := BuildAck([]byte(sampleMsg), "AA", "")
	root, err := hl7.Parse(ack)
	if err != nil {
		t.Fatalf("ACK does not parse: %v\n%q", err, ack)
	}
	get := func(path string) string {
		nodes, _ := hl7.Resolve(root, path)
		if len(nodes) == 0 {
			return ""
		}
		return hl7.Value(root, nodes[0])
	}
	if got := get("MSA-1"); got != "AA" {
		t.Errorf("MSA-1 = %q", got)
	}
	if got := get("MSA-2"); got != "CTRL001" {
		t.Errorf("MSA-2 = %q (should echo inbound control ID)", got)
	}
	if got := get("MSH-9.1"); got != "ACK" {
		t.Errorf("MSH-9.1 = %q", got)
	}
	if got := get("MSH-9.2"); got != "A01" {
		t.Errorf("MSH-9.2 = %q (should echo trigger)", got)
	}
	// Sender/receiver swapped.
	if got := get("MSH-3"); got != "RECV" {
		t.Errorf("MSH-3 = %q", got)
	}
	if got := get("MSH-5"); got != "SEND" {
		t.Errorf("MSH-5 = %q", got)
	}
}

func TestBuildAckUnparseable(t *testing.T) {
	ack := BuildAck([]byte("garbage"), "AE", "bad message")
	root, err := hl7.Parse(ack)
	if err != nil {
		t.Fatalf("static NAK does not parse: %v\n%q", err, ack)
	}
	nodes, _ := hl7.Resolve(root, "MSA-1")
	if len(nodes) == 0 || hl7.Value(root, nodes[0]) != "AE" {
		t.Error("static NAK should carry MSA-1=AE")
	}
}

// startListener boots a listener on a random port with the given deliver
// func and returns it plus a connected sender.
func startListener(t *testing.T, settings map[string]any, deliver adapter.DeliverFunc) (*Listener, *Sender) {
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

	s, err := NewSender(map[string]any{"addr": l.Addr(), "ackTimeout": "5s"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return l, s
}

func TestLoopbackImmediateAck(t *testing.T) {
	var mu sync.Mutex
	var received [][]byte
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		mu.Lock()
		received = append(received, append([]byte(nil), raw...))
		mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{MessageID: int64(len(received)), Done: done}, nil
	}
	_, sender := startListener(t, map[string]any{"ackMode": "immediate"}, deliver)

	// Two messages over one connection, strictly ordered.
	for i := 0; i < 2; i++ {
		if err := sender.Send(context.Background(), []byte(sampleMsg), nil); err != nil {
			t.Fatalf("send %d: %v", i+1, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || string(received[0]) != sampleMsg {
		t.Errorf("received %d messages", len(received))
	}
}

func TestLoopbackDeliverErrorNAKs(t *testing.T) {
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		return adapter.Receipt{}, context.DeadlineExceeded
	}
	_, sender := startListener(t, map[string]any{"ackMode": "immediate"}, deliver)

	err := sender.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !adapter.IsPermanent(err) {
		t.Fatalf("expected permanent rejection, got %v", err)
	}
}

func TestLoopbackDestinationAck(t *testing.T) {
	decision := adapter.AckDecision{Code: "AR", Text: "missing PID-3"}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		done := make(chan adapter.AckDecision, 1)
		go func() {
			time.Sleep(50 * time.Millisecond) // pipeline runs...
			done <- decision
		}()
		return adapter.Receipt{MessageID: 1, Done: done}, nil
	}
	_, sender := startListener(t, map[string]any{"ackMode": "destination"}, deliver)

	err := sender.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !adapter.IsPermanent(err) {
		t.Fatalf("expected permanent AR rejection, got %v", err)
	}
	if !strings.Contains(err.Error(), "missing PID-3") {
		t.Errorf("rejection should carry MSA-3 text, got %v", err)
	}
}

func TestLoopbackDestinationAckHoldTimeout(t *testing.T) {
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		return adapter.Receipt{MessageID: 1, Done: make(chan adapter.AckDecision, 1)}, nil // never resolves
	}
	_, sender := startListener(t, map[string]any{
		"ackMode":     "destination",
		"holdTimeout": "100ms",
	}, deliver)

	err := sender.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil || !adapter.IsPermanent(err) {
		t.Fatalf("expected fallback AE after hold timeout, got %v", err)
	}
}

func TestSenderReceiverDown(t *testing.T) {
	s, err := NewSender(map[string]any{"addr": "127.0.0.1:1", "connectTimeout": "200ms"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Send(context.Background(), []byte(sampleMsg), nil)
	if err == nil {
		t.Fatal("expected connect error")
	}
	if adapter.IsPermanent(err) {
		t.Error("transport failure must be transient (retryable), not permanent")
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var sb strings.Builder
	if err := writeFrame(&sb, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	wire := sb.String()
	if wire != "\x0Bpayload\x1C\x0D" {
		t.Errorf("frame = %q", wire)
	}
}

func TestListenerConfigValidation(t *testing.T) {
	if _, err := NewListener(map[string]any{}); err == nil {
		t.Error("missing addr should fail")
	}
	if _, err := NewListener(map[string]any{"addr": ":0", "ackMode": "bogus"}); err == nil {
		t.Error("bad ackMode should fail")
	}
	if _, err := NewListener(map[string]any{"addr": ":0", "bogusKey": 1}); err == nil {
		t.Error("unknown setting should fail")
	}
}
