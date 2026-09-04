package adapter

import (
	"context"
	"fmt"
	"time"
)

// AckMode selects when an inbound adapter acknowledges its sender.
type AckMode string

const (
	// AckImmediate acknowledges as soon as the engine has recorded the
	// message (the Guaranteed Delivery handoff): fire-and-forget downstream.
	AckImmediate AckMode = "immediate"
	// AckDestination holds the transport acknowledgment until the pipeline
	// finishes: filter, transformers (which may reject via the response
	// API), and delivery to every waitForAck destination.
	AckDestination AckMode = "destination"
)

// ParseAckMode validates a configured ack mode; "" means AckImmediate.
func ParseAckMode(s string) (AckMode, error) {
	switch AckMode(s) {
	case "":
		return AckImmediate, nil
	case AckImmediate, AckDestination:
		return AckMode(s), nil
	}
	return "", fmt.Errorf("ackMode must be %q or %q", AckImmediate, AckDestination)
}

// Accepted reports whether the decision means the pipeline took the
// message: HL7 AA, the enhanced-mode CA, or no code at all.
func (d AckDecision) Accepted() bool {
	return d.Code == "" || d.Code == "AA" || d.Code == "CA"
}

// Outcome says how AwaitDecision returned.
type Outcome int

const (
	// Decided: the pipeline produced its decision.
	Decided Outcome = iota
	// TimedOut: hold elapsed first; the message is recorded and still
	// processing, only its outcome is unknown.
	TimedOut
	// Canceled: ctx ended (the adapter is stopping); as with TimedOut the
	// message is recorded.
	Canceled
)

// AwaitDecision waits up to hold for a receipt's pipeline decision. It is
// the destination-mode hold every inbound adapter performs; the adapter
// then maps the decision and outcome onto its transport.
func AwaitDecision(ctx context.Context, rec Receipt, hold time.Duration) (AckDecision, Outcome) {
	timer := time.NewTimer(hold)
	defer timer.Stop()
	select {
	case d := <-rec.Done:
		return d, Decided
	case <-timer.C:
		return AckDecision{Code: "AE", Text: "processing timed out"}, TimedOut
	case <-ctx.Done():
		return AckDecision{}, Canceled
	}
}
