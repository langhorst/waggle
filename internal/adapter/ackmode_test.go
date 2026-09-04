package adapter

import (
	"context"
	"testing"
	"time"
)

func TestParseAckMode(t *testing.T) {
	for in, want := range map[string]AckMode{"": AckImmediate, "immediate": AckImmediate, "destination": AckDestination} {
		if got, err := ParseAckMode(in); err != nil || got != want {
			t.Errorf("ParseAckMode(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := ParseAckMode("later"); err == nil {
		t.Error("bad mode accepted")
	}
}

func TestAccepted(t *testing.T) {
	for code, want := range map[string]bool{"": true, "AA": true, "CA": true, "AE": false, "AR": false, "CR": false} {
		if got := (AckDecision{Code: code}).Accepted(); got != want {
			t.Errorf("Accepted(%q) = %v", code, got)
		}
	}
}

func TestAwaitDecision(t *testing.T) {
	done := make(chan AckDecision, 1)
	done <- AckDecision{Code: "AR", Text: "no"}
	if d, o := AwaitDecision(context.Background(), Receipt{Done: done}, time.Second); o != Decided || d.Code != "AR" {
		t.Errorf("decided = %+v, %v", d, o)
	}
	if d, o := AwaitDecision(context.Background(), Receipt{Done: make(chan AckDecision)}, 10*time.Millisecond); o != TimedOut || d.Code != "AE" {
		t.Errorf("timeout = %+v, %v", d, o)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, o := AwaitDecision(ctx, Receipt{Done: make(chan AckDecision)}, time.Second); o != Canceled {
		t.Errorf("canceled = %v", o)
	}
}
