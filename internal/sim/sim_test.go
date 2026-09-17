package sim_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/sim"
	"github.com/langhorst/waggle/internal/sim/simtime"
)

var epoch = time.Date(2026, 3, 1, 7, 0, 0, 0, time.UTC)

// memSink records what it was sent, in order.
type memSink struct {
	mu   sync.Mutex
	msgs []sim.Message
}

func (s *memSink) Send(_ context.Context, m sim.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, m)
	return nil
}
func (s *memSink) Close() error { return nil }
func (s *memSink) all() []sim.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sim.Message(nil), s.msgs...)
}

// adtFeed and ormFeed stand in for the real renderers: enough to prove the
// seam, not the dialect. They assert the property that matters -- both read
// the same patient and visit identifiers out of one event stream.
type adtFeed struct{ ids *sim.ControlIDs }

func (adtFeed) Name() string { return "adt" }
func (adtFeed) Wants(k sim.EventKind) bool {
	switch k {
	case sim.EncounterAdmitted, sim.EncounterTransferred, sim.EncounterDischarged:
		return true
	}
	return false
}
func (f adtFeed) Render(ev sim.Event) ([]sim.Message, error) {
	trigger := map[sim.EventKind]string{
		sim.EncounterAdmitted:    "ADT^A01",
		sim.EncounterTransferred: "ADT^A02",
		sim.EncounterDischarged:  "ADT^A03",
	}[ev.Kind]
	raw := strings.Join([]string{
		"MSH|^~\\&|SIM|MERCY|WAGGLE|TEST|" + sim.HL7Time(ev.At) + "||" + trigger + "|X|P|2.5.1",
		"PID|1||" + ev.Patient.MRN + "||" + ev.Patient.Name(),
		"PV1|1|I|" + ev.Encounter.Location.String() + "|||||||||||||||" + ev.Encounter.VisitNumber,
	}, "\r") + "\r"
	return []sim.Message{{
		Feed: "adt", ControlID: f.ids.Next(), Trigger: trigger,
		At: ev.At, Raw: []byte(raw), Cause: ev,
	}}, nil
}

type ormFeed struct{ ids *sim.ControlIDs }

func (ormFeed) Name() string               { return "orm" }
func (ormFeed) Wants(k sim.EventKind) bool { return k == sim.OrderPlaced }
func (f ormFeed) Render(ev sim.Event) ([]sim.Message, error) {
	raw := strings.Join([]string{
		"MSH|^~\\&|SIM|MERCY|LIS|TEST|" + sim.HL7Time(ev.At) + "||ORM^O01|X|P|2.5.1",
		"PID|1||" + ev.Patient.MRN + "||" + ev.Patient.Name(),
		"PV1|1|I|" + ev.Encounter.Location.String() + "|||||||||||||||" + ev.Encounter.VisitNumber,
		"ORC|NW|" + ev.Order.PlacerNumber + "|" + ev.Order.FillerNumber,
		"OBR|1|" + ev.Order.PlacerNumber + "|" + ev.Order.FillerNumber + "|" + ev.Order.ServiceID(),
	}, "\r") + "\r"
	return []sim.Message{{
		Feed: "orm", ControlID: f.ids.Next(), Trigger: "ORM^O01",
		At: ev.At, Raw: []byte(raw), Cause: ev,
	}}, nil
}

// A hospital event stream both feeds observe. This is the shape the design
// exists for: the ORM feed never invents a patient or a visit, it reads the
// ones the ADT feed already announced.
func TestFeedsShareOneCoherentWorld(t *testing.T) {
	ids := &sim.ControlIDs{Prefix: "SIMT"}
	adtSink, ormSink := &memSink{}, &memSink{}
	r := sim.NewRunner(nil)
	r.Wire(sim.Wiring{Feed: adtFeed{ids}, Sink: adtSink})
	r.Wire(sim.Wiring{Feed: ormFeed{ids}, Sink: ormSink})

	bed := &sim.Location{Facility: "MERCY", Unit: "3WEST", Room: "310", Bed: "A"}
	patient := &sim.Patient{
		MRN: "MRN0001", HomeFacility: "MERCY", EnterpriseID: "EID0001",
		Family: "DOE", Given: "JOHN", Sex: "M",
		BirthDate: time.Date(1975, 4, 2, 0, 0, 0, 0, time.UTC),
	}
	enc := &sim.Encounter{
		VisitNumber: "V00001", AccountNumber: "A00001",
		Patient: patient, Facility: "MERCY", Class: sim.ClassInpatient,
		State: sim.StateActive, Location: bed, AdmittedAt: epoch,
	}
	bed.Occupant = enc
	order := &sim.Order{
		PlacerNumber: "P0001", FillerNumber: "F0001", Encounter: enc,
		Code: "CBC", Text: "COMPLETE BLOOD COUNT", CodeSystem: "L",
		Status: sim.OrderStatusNew, PlacedAt: epoch.Add(2 * time.Hour),
	}
	enc.Orders = append(enc.Orders, order)

	ctx := context.Background()
	for _, ev := range []sim.Event{
		{Kind: sim.EncounterAdmitted, At: epoch, Patient: patient, Encounter: enc},
		{Kind: sim.OrderPlaced, At: epoch.Add(2 * time.Hour), Patient: patient, Encounter: enc, Order: order},
		{Kind: sim.EncounterDischarged, At: epoch.Add(50 * time.Hour), Patient: patient, Encounter: enc},
	} {
		if err := r.Emit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	adt, orm := adtSink.all(), ormSink.all()
	if len(adt) != 2 || len(orm) != 1 {
		t.Fatalf("adt got %d messages, orm got %d; want 2 and 1", len(adt), len(orm))
	}
	if adt[0].Trigger != "ADT^A01" || adt[1].Trigger != "ADT^A03" {
		t.Errorf("adt triggers = %s, %s", adt[0].Trigger, adt[1].Trigger)
	}

	// The coherence claim: the order message carries the same MRN and visit
	// number the ADT feed announced, because both read one world.
	for _, want := range []string{"MRN0001", "V00001", "3WEST^310^A^MERCY"} {
		if !strings.Contains(string(adt[0].Raw), want) {
			t.Errorf("A01 missing %q", want)
		}
		if !strings.Contains(string(orm[0].Raw), want) {
			t.Errorf("ORM missing %q, so the feeds disagree about the patient", want)
		}
	}

	// Control IDs are unique across feeds and drawn in emission order.
	seen := map[string]bool{}
	for _, m := range append(append([]sim.Message{}, adt...), orm...) {
		if seen[m.ControlID] {
			t.Errorf("duplicate control id %s", m.ControlID)
		}
		seen[m.ControlID] = true
	}
	if n := r.Sent()["adt"]; n != 2 {
		t.Errorf("runner counted %d adt messages", n)
	}
}

// A feed must only be asked for the events it claims, or adding a feed would
// mean auditing every other feed's renderer.
func TestFeedsOnlySeeWhatTheyWant(t *testing.T) {
	ids := &sim.ControlIDs{}
	ormSink := &memSink{}
	r := sim.NewRunner(nil)
	r.Wire(sim.Wiring{Feed: ormFeed{ids}, Sink: ormSink})

	// An ADT-only event with no Order attached: if the ORM feed were asked
	// to render it, it would panic dereferencing ev.Order.
	err := r.Emit(context.Background(), sim.Event{
		Kind: sim.EncounterAdmitted, At: epoch,
		Patient:   &sim.Patient{MRN: "MRN1"},
		Encounter: &sim.Encounter{VisitNumber: "V1", Location: &sim.Location{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ormSink.all()); n != 0 {
		t.Errorf("orm feed rendered %d messages for an admission", n)
	}
}

// dropEveryOther is a fault: it drops alternate messages.
type dropEveryOther struct{ n int }

func (d *dropEveryOther) Apply(m sim.Message) []sim.Message {
	d.n++
	if d.n%2 == 0 {
		return nil
	}
	return []sim.Message{m}
}

// duplicator resends each message once, the classic idempotency test.
type duplicator struct{}

func (duplicator) Apply(m sim.Message) []sim.Message { return []sim.Message{m, m} }

func TestFaultsApplyAfterRendering(t *testing.T) {
	ids := &sim.ControlIDs{}
	sink := &memSink{}
	r := sim.NewRunner(nil)
	r.Wire(sim.Wiring{Feed: adtFeed{ids}, Sink: sink, Faults: []sim.Fault{duplicator{}}})

	bed := &sim.Location{Facility: "MERCY", Unit: "ICU", Room: "1", Bed: "A"}
	enc := &sim.Encounter{VisitNumber: "V9", Location: bed, State: sim.StateActive}
	p := &sim.Patient{MRN: "MRN9", Family: "ROE", Given: "JANE"}
	if err := r.Emit(context.Background(), sim.Event{
		Kind: sim.EncounterAdmitted, At: epoch, Patient: p, Encounter: enc,
	}); err != nil {
		t.Fatal(err)
	}
	got := sink.all()
	if len(got) != 2 {
		t.Fatalf("duplicating fault produced %d messages, want 2", len(got))
	}
	// A duplicate carries the same control id: that is the point of it.
	if got[0].ControlID != got[1].ControlID {
		t.Errorf("duplicate should reuse the control id, got %s and %s", got[0].ControlID, got[1].ControlID)
	}
	// The world still believes it sent one clean message.
	if got[0].Cause.Kind != sim.EncounterAdmitted {
		t.Errorf("provenance lost: cause = %s", got[0].Cause.Kind)
	}
}

func TestDroppingFaultRemovesMessages(t *testing.T) {
	ids := &sim.ControlIDs{}
	sink := &memSink{}
	r := sim.NewRunner(nil)
	r.Wire(sim.Wiring{Feed: adtFeed{ids}, Sink: sink, Faults: []sim.Fault{&dropEveryOther{}}})

	bed := &sim.Location{Facility: "MERCY", Unit: "ICU", Room: "1", Bed: "A"}
	enc := &sim.Encounter{VisitNumber: "V9", Location: bed, State: sim.StateActive}
	p := &sim.Patient{MRN: "MRN9"}
	for i := 0; i < 4; i++ {
		if err := r.Emit(context.Background(), sim.Event{
			Kind: sim.EncounterAdmitted, At: epoch, Patient: p, Encounter: enc,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(sink.all()); n != 2 {
		t.Errorf("half the messages should have been dropped, got %d of 4", n)
	}
}

// Events emitted from scheduled tasks must reach sinks in simulated-time
// order whatever the rate, which is what makes a corpus replayable.
func TestEmissionFollowsSimulatedOrderAtAnyRate(t *testing.T) {
	order := func(scale float64) []string {
		ids := &sim.ControlIDs{}
		sink := &memSink{}
		r := sim.NewRunner(nil)
		r.Wire(sim.Wiring{Feed: adtFeed{ids}, Sink: sink})

		clock := simtime.New(epoch, scale, nil)
		sched := simtime.NewScheduler(clock)
		bed := &sim.Location{Facility: "MERCY", Unit: "3W", Room: "1", Bed: "A"}
		p := &sim.Patient{MRN: "MRN1", Family: "DOE", Given: "JOHN"}
		enc := &sim.Encounter{VisitNumber: "V1", Location: bed, State: sim.StateActive}

		// Scheduled deliberately out of order.
		sched.At(epoch.Add(3*time.Hour), func(at time.Time) {
			_ = r.Emit(context.Background(), sim.Event{Kind: sim.EncounterDischarged, At: at, Patient: p, Encounter: enc})
		})
		sched.At(epoch.Add(time.Hour), func(at time.Time) {
			_ = r.Emit(context.Background(), sim.Event{Kind: sim.EncounterAdmitted, At: at, Patient: p, Encounter: enc})
		})
		sched.At(epoch.Add(2*time.Hour), func(at time.Time) {
			_ = r.Emit(context.Background(), sim.Event{Kind: sim.EncounterTransferred, At: at, Patient: p, Encounter: enc})
		})
		if err := sched.Run(context.Background(), time.Time{}); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, m := range sink.all() {
			out = append(out, m.Trigger+"@"+sim.HL7Time(m.At))
		}
		return out
	}

	want := []string{
		"ADT^A01@20260301080000",
		"ADT^A02@20260301090000",
		"ADT^A03@20260301100000",
	}
	for _, scale := range []float64{simtime.Unbounded, 1e7, 1e6} {
		got := order(scale)
		if len(got) != len(want) {
			t.Fatalf("rate %v produced %v", scale, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("rate %v produced %v, want %v", scale, got, want)
				break
			}
		}
	}
}

// The summary is what an operator reads in a terminal, so it must name the
// patient and the visit, not just the trigger.
func TestMessageSummaryNamesThePatient(t *testing.T) {
	bed := &sim.Location{Facility: "MERCY", Unit: "3WEST", Room: "312", Bed: "B"}
	prior := &sim.Location{Facility: "MERCY", Unit: "ED", Room: "104", Bed: "A"}
	p := &sim.Patient{
		MRN: "MERC0000042", Family: "THORNQUIST", Given: "IMOGEN", Sex: "F",
		BirthDate: time.Date(1958, 7, 14, 0, 0, 0, 0, time.UTC),
	}
	enc := &sim.Encounter{
		VisitNumber: "V000000123", Patient: p, Class: sim.ClassInpatient,
		Location: bed, PriorLocation: prior,
	}
	m := sim.Message{
		Feed: "adt", Trigger: "ADT^A02", ControlID: "SIM000000009",
		Cause: sim.Event{
			Kind: sim.EncounterTransferred, Patient: p, Encounter: enc,
			From: prior, To: bed,
		},
	}
	got := map[string]any{}
	attrs := m.Summary()
	for i := 0; i+1 < len(attrs); i += 2 {
		got[attrs[i].(string)] = attrs[i+1]
	}
	for k, want := range map[string]any{
		"feed": "adt", "trigger": "ADT^A02", "ctrl": "SIM000000009",
		"mrn": "MERC0000042", "name": "THORNQUIST,IMOGEN", "dob": "19580714",
		"sex": "F", "visit": "V000000123", "class": "I",
		"loc": "3WEST^312^B^MERCY", "from": "ED^104^A^MERCY",
	} {
		if got[k] != want {
			t.Errorf("summary[%s] = %v, want %v", k, got[k], want)
		}
	}
}

// A message with no domain objects behind it must still summarise rather
// than panic: faults and future feeds can produce one.
func TestMessageSummaryToleratesEmptyCause(t *testing.T) {
	m := sim.Message{Feed: "adt", Trigger: "ADT^A01", ControlID: "X1"}
	attrs := m.Summary()
	if len(attrs) < 6 || len(attrs)%2 != 0 {
		t.Fatalf("summary = %v", attrs)
	}
}
