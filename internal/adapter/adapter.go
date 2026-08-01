// Package adapter defines the Channel Adapter contracts (Enterprise
// Integration Patterns) between the engine and the outside world, plus the
// compile-time registry configured adapter types resolve from. Adapters are
// code: adding one means implementing an interface and registering a factory
// from an init function, then rebuilding. Transformers stay dynamic.
package adapter

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// AckDecision is the application-level acknowledgment outcome for one
// inbound message: what an MLLP listener turns into an HL7 ACK. Codes follow
// HL7 MSA-1 semantics (AA accept, AE error, AR reject); non-HL7 sources map
// them as they see fit.
type AckDecision struct {
	Code string
	Text string
}

// Receipt is what the engine hands back when an inbound adapter delivers a
// message.
type Receipt struct {
	// MessageID identifies the recorded message.
	MessageID int64
	// Done resolves with the pipeline's final AckDecision once processing
	// (including any waitForAck destinations) completes. Sources in
	// destination-ACK mode hold their transport response on it; sources in
	// immediate mode ignore it. Always non-nil; receives exactly one value.
	Done <-chan AckDecision
}

// DeliverFunc is the engine's intake, called by inbound Channel Adapters
// once per received message. A nil error means the message is recorded
// (Guaranteed Delivery handoff: from here on the engine owns it) — in
// immediate ACK mode the adapter acknowledges its sender now. A non-nil
// error means the message was NOT accepted and the adapter should reject
// (MLLP AE) or leave the input in place (file source).
type DeliverFunc func(ctx context.Context, raw []byte, meta map[string]string) (Receipt, error)

// Inbound is an inbound Channel Adapter (message source). Start begins
// receiving (binding sockets, starting poll loops) and returns once the
// source is active; delivery happens on internal goroutines until Stop.
type Inbound interface {
	Start(ctx context.Context, deliver DeliverFunc) error
	// Stop halts intake and waits for in-flight deliveries to hand off.
	Stop() error
}

// Outbound is an outbound Channel Adapter (message destination). Send
// delivers one serialized payload; a returned error is transient (retry
// with backoff) unless wrapped as Permanent.
type Outbound interface {
	Open(ctx context.Context) error
	Send(ctx context.Context, payload []byte, meta map[string]string) error
	Close() error
}

// PermanentError marks a delivery failure that retrying cannot fix — an
// application-level rejection such as an HL7 AE/AR ACK. The queue routes
// these straight to the Dead Letter Channel instead of retrying.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err (or anything it wraps) is a
// PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// Factories build adapters from the settings block of a channel's YAML
// config, already decoded into a generic map.
type (
	InboundFactory  func(settings map[string]any) (Inbound, error)
	OutboundFactory func(settings map[string]any) (Outbound, error)
)

var (
	inbound  = map[string]InboundFactory{}
	outbound = map[string]OutboundFactory{}
)

// RegisterInbound adds an inbound adapter type to the registry, panicking on
// duplicates (registration runs from init functions).
func RegisterInbound(typ string, f InboundFactory) {
	if _, dup := inbound[typ]; dup {
		panic(fmt.Sprintf("adapter: duplicate inbound type %q", typ))
	}
	inbound[typ] = f
}

// RegisterOutbound adds an outbound adapter type to the registry.
func RegisterOutbound(typ string, f OutboundFactory) {
	if _, dup := outbound[typ]; dup {
		panic(fmt.Sprintf("adapter: duplicate outbound type %q", typ))
	}
	outbound[typ] = f
}

// NewInbound builds an inbound adapter of the given registered type.
func NewInbound(typ string, settings map[string]any) (Inbound, error) {
	f, ok := inbound[typ]
	if !ok {
		return nil, fmt.Errorf("adapter: unknown inbound type %q (registered: %v)", typ, InboundTypes())
	}
	return f(settings)
}

// NewOutbound builds an outbound adapter of the given registered type.
func NewOutbound(typ string, settings map[string]any) (Outbound, error) {
	f, ok := outbound[typ]
	if !ok {
		return nil, fmt.Errorf("adapter: unknown outbound type %q (registered: %v)", typ, OutboundTypes())
	}
	return f(settings)
}

// InboundTypes returns all registered inbound type names, sorted.
func InboundTypes() []string { return sortedKeys(inbound) }

// OutboundTypes returns all registered outbound type names, sorted.
func OutboundTypes() []string { return sortedKeys(outbound) }

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
