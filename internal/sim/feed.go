// Package sim simulates the systems a hospital network runs, as a source of
// realistic traffic for waggle channels.
//
// The shape to understand first is that feeds are not independent. A real
// EMR's outbound interfaces all describe one hospital: an order is placed
// for a patient on a visit, and the result that comes back carries that
// order's numbers and the same patient and visit as the ADT feed announced.
// Generating each feed separately would produce traffic that is individually
// plausible and collectively incoherent -- results for orders nobody placed,
// PV1s that disagree with the census -- which tests nothing an integration
// engine will meet in production.
//
// So the world is the single source of truth. It owns patients, encounters,
// beds and orders, and advancing it emits Events that say what happened in
// hospital terms, not HL7 terms. A Feed subscribes to the event kinds it
// cares about and renders its own message type:
//
//	ADT   Admitted, Transferred, Discharged, ...  -> ADT^A01, A02, A03
//	ORM   OrderPlaced, OrderCancelled             -> ORM^O01
//	ORU   ResultAvailable                         -> ORU^R01
//
// Adding a feed is therefore additive: a new renderer over an event stream
// that already carries the identifiers it needs, with cross-feed referential
// integrity a property of the model rather than something each feed has to
// reproduce.
package sim

import (
	"context"
	"fmt"
	"time"
)

// EventKind names something that happened in the hospital. Kinds are
// deliberately clinical rather than HL7: one kind may render as different
// trigger events in different dialects, or as no message at all in a feed
// that does not care about it.
type EventKind string

const (
	// Patient and encounter lifecycle. The ADT feed's territory.
	PatientRegistered    EventKind = "patient.registered"
	PatientUpdated       EventKind = "patient.updated"
	PatientMerged        EventKind = "patient.merged"
	EncounterAdmitted    EventKind = "encounter.admitted"
	EncounterTransferred EventKind = "encounter.transferred"
	EncounterDischarged  EventKind = "encounter.discharged"
	EncounterCancelled   EventKind = "encounter.cancelled"
	EncounterPreAdmitted EventKind = "encounter.preadmitted"

	// Orders. The ORM feed's territory, and the cause of results.
	OrderPlaced    EventKind = "order.placed"
	OrderCancelled EventKind = "order.cancelled"

	// Results. The ORU feed's territory.
	SpecimenCollected EventKind = "specimen.collected"
	ResultAvailable   EventKind = "result.available"
	ResultCorrected   EventKind = "result.corrected"
)

// Event is one thing that happened, at one simulated instant.
//
// At is the simulated time the event occurred and is what a feed must stamp
// its message with; it does not vary with the rate the run was paced at. Seq
// is a run-wide ordering number, so a corpus can be replayed in exactly the
// order it was produced even when several events share an instant.
//
// The pointers are into the world and must be treated as read-only by feeds:
// a renderer that mutated an encounter would change what later feeds see.
type Event struct {
	Kind EventKind
	At   time.Time
	Seq  uint64

	Patient   *Patient
	Encounter *Encounter
	Order     *Order
	Result    *Result

	// From and To carry a transfer's endpoints; both nil otherwise.
	From, To *Location

	// Prior is the record a merge collapsed into Patient, for PatientMerged.
	Prior *Patient

	// Note is free text for the simulation's own log, never for the wire.
	Note string
}

// String renders an event the way the run log shows it.
func (e Event) String() string {
	who := "-"
	if e.Patient != nil {
		who = e.Patient.MRN
	}
	return fmt.Sprintf("%s %s %s", e.At.Format("2006-01-02 15:04:05"), e.Kind, who)
}

// Message is one rendered HL7 message on its way to a sink, with enough
// provenance attached to explain it after the fact.
type Message struct {
	// Feed is the feed that produced it, e.g. "adt".
	Feed string
	// ControlID is MSH-10, unique per message unless a fault duplicates it.
	ControlID string
	// Trigger is the HL7 event the renderer chose, e.g. "ADT^A01".
	Trigger string
	// At is the simulated instant, copied from the Event.
	At time.Time
	// Raw is the encoded message, segment-separated, ready for a sink.
	Raw []byte
	// Cause is the event this message renders, for diagnostics. A fault may
	// alter Raw; Cause still describes what was meant.
	Cause Event
}

// Feed renders one outbound interface: it decides which hospital events it
// speaks for and what messages they become.
//
// Render may return no messages (the event does not concern this feed, or
// this dialect has no trigger for it) or several (one hospital event can be
// several messages: a merge is often an A40 per affected identifier).
type Feed interface {
	// Name identifies the feed in configuration, logs and metrics.
	Name() string
	// Wants reports whether Render should be called for this kind. It lets
	// the runner skip rendering entirely for uninteresting events, and
	// documents each feed's territory in one place.
	Wants(EventKind) bool
	// Render turns one event into zero or more messages.
	Render(Event) ([]Message, error)
}

// Sink is where a feed's messages go: an MLLP connection, a directory of
// files, or a corpus on disk.
//
// Send is called in simulated-time order for a given feed. A sink that
// blocks holds the simulation up, which is deliberate -- back-pressure from
// a slow receiver should slow the hospital down rather than pile up
// unboundedly in memory.
type Sink interface {
	Send(ctx context.Context, m Message) error
	Close() error
}

// Fault optionally misbehaves on the way to the sink: corrupting a message,
// duplicating it, holding it back to arrive out of order, or dropping it.
//
// It sits last, after rendering, so the world still believes it sent a clean
// message and a run can always be diffed against what was meant.
type Fault interface {
	// Apply returns the messages to actually send in place of m. Returning
	// an empty slice drops it.
	Apply(m Message) []Message
}

// Summary describes a message in the terms an operator watching a terminal
// cares about: which patient, which visit, where they are.
//
// It reads the domain objects the message was rendered from rather than
// re-parsing the wire bytes, so the log says what the simulation meant even
// when a fault has since corrupted what it sent.
func (m Message) Summary() []any {
	attrs := []any{"feed", m.Feed, "trigger", m.Trigger, "ctrl", m.ControlID}

	ev := m.Cause
	if p := ev.Patient; p != nil {
		attrs = append(attrs,
			"mrn", p.MRN,
			"name", p.Family+","+p.Given,
			"dob", HL7Date(p.BirthDate),
			"sex", p.Sex,
		)
	}
	if enc := ev.Encounter; enc != nil {
		attrs = append(attrs, "visit", enc.VisitNumber, "class", string(enc.Class))
		if enc.Location != nil {
			attrs = append(attrs, "loc", enc.Location.String())
		}
		if enc.DischargeDisposition != "" && ev.Kind == EncounterDischarged {
			attrs = append(attrs, "disposition", enc.DischargeDisposition)
		}
	}
	// A transfer is only legible with both ends of it.
	if ev.From != nil {
		attrs = append(attrs, "from", ev.From.String())
	}
	if o := ev.Order; o != nil {
		attrs = append(attrs, "placer", o.PlacerNumber, "service", o.Code)
	}
	if r := ev.Result; r != nil {
		attrs = append(attrs, "obs", r.Code, "value", r.Value, "status", string(r.Status))
	}
	return attrs
}
