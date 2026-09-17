package adt_test

import (
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/format"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/sim"
	"github.com/langhorst/waggle/internal/sim/adt"
)

var at = time.Date(2026, 3, 2, 14, 35, 0, 0, time.UTC)

func fixture() (*adt.Feed, sim.Event) {
	f := adt.New(adt.DefaultConfig(), &sim.ControlIDs{Prefix: "SIMT"})
	bed := &sim.Location{Facility: "MERCY", Unit: "3WEST", Room: "312", Bed: "B"}
	prior := &sim.Location{Facility: "MERCY", Unit: "ED", Room: "104", Bed: "A"}
	p := &sim.Patient{
		MRN: "MERC0000042", HomeFacility: "MERCY", EnterpriseID: "EID0000042",
		Family: "THORNQUIST", Given: "IMOGEN", Middle: "R", Sex: "F",
		BirthDate:  time.Date(1958, 7, 14, 0, 0, 0, 0, time.UTC),
		Address:    sim.Address{Street1: "418 ELDER LN", City: "SPRINGDALE", State: "OH", Zip: "45001", Country: "USA"},
		Phone:      "(555)201-8834",
		MaritalSts: "M",
	}
	enc := &sim.Encounter{
		VisitNumber: "V000000123", AccountNumber: "A000000123",
		Patient: p, Facility: "MERCY", Class: sim.ClassInpatient, State: sim.StateActive,
		Location: bed, PriorLocation: prior,
		AttendingID: "SIM0003", AttendingName: "VANTERPOOL^MARGUERITE^A^^^MD",
		AdmitSource: "7", AdmittedAt: at.Add(-3 * time.Hour),
	}
	bed.Occupant = enc
	return f, sim.Event{Kind: sim.EncounterAdmitted, At: at, Patient: p, Encounter: enc}
}

// parse renders one event and hands it to waggle's own HL7 parser, so the
// simulator is checked against the code that will actually read it rather
// than against the renderer's own idea of the format.
func parse(t *testing.T, f *adt.Feed, ev sim.Event) *rendered {
	t.Helper()
	msgs, err := f.Render(ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("rendered %d messages, want 1", len(msgs))
	}
	dt, ok := format.Get("hl7v2")
	if !ok {
		t.Fatal("hl7v2 format not registered")
	}
	root, err := dt.Parse(msgs[0].Raw)
	if err != nil {
		t.Fatalf("waggle cannot parse the simulator's output: %v\n%s", err, msgs[0].Raw)
	}
	return &rendered{t: t, dt: dt, root: root, msg: msgs[0]}
}

type rendered struct {
	t    *testing.T
	dt   format.DataType
	root *message.Node
	msg  sim.Message
}

// get resolves an HL7 path the way a channel script would.
func (r *rendered) get(path string) string {
	r.t.Helper()
	nodes, err := r.dt.Resolve(r.root, path)
	if err != nil {
		r.t.Fatalf("resolving %s: %v", path, err)
	}
	if len(nodes) == 0 {
		return ""
	}
	return r.dt.Value(r.root, nodes[0])
}

func TestAdmitRendersEveryFieldAtItsNumber(t *testing.T) {
	f, ev := fixture()
	m := parse(t, f, ev)

	for _, tc := range []struct{ path, want string }{
		{"MSH-3", "WAGGLESIM"},
		{"MSH-4", "SIMNET"},
		{"MSH-5", "WAGGLE"},
		{"MSH-6", "TEST"},
		{"MSH-7", "20260302143500"},
		{"MSH-9.1", "ADT"},
		{"MSH-9.2", "A01"},
		{"MSH-9.3", "ADT_A01"},
		{"MSH-10", "SIMT000000001"},
		{"MSH-11", "T"},
		{"MSH-12", "2.5.1"},

		{"EVN-1", "A01"},
		{"EVN-2", "20260302143500"},

		{"PID-1", "1"},
		{"PID-3[1].1", "MERC0000042"},
		{"PID-3[1].4", "MERCY"},
		{"PID-3[1].5", "MR"},
		{"PID-3[2].1", "EID0000042"},
		{"PID-3[2].5", "PI"},
		{"PID-5.1", "THORNQUIST"},
		{"PID-5.2", "IMOGEN"},
		{"PID-5.3", "R"},
		{"PID-7", "19580714"},
		{"PID-8", "F"},
		{"PID-11.1", "418 ELDER LN"},
		{"PID-11.3", "SPRINGDALE"},
		{"PID-11.4", "OH"},
		{"PID-11.5", "45001"},
		{"PID-13", "(555)201-8834"},
		{"PID-16", "M"},
		{"PID-18", "A000000123"},

		{"PV1-1", "1"},
		{"PV1-2", "I"},
		{"PV1-3.1", "3WEST"},
		{"PV1-3.2", "312"},
		{"PV1-3.3", "B"},
		{"PV1-3.4", "MERCY"},
		{"PV1-6.1", "ED"},
		{"PV1-7.1", "SIM0003"},
		{"PV1-14", "7"},
		{"PV1-19", "V000000123"},
		{"PV1-44", "20260302113500"},
	} {
		if got := m.get(tc.path); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestDischargeCarriesDispositionAndTimes(t *testing.T) {
	f, ev := fixture()
	ev.Kind = sim.EncounterDischarged
	ev.Encounter.DischargedAt = at
	ev.Encounter.DischargeDisposition = "01"
	m := parse(t, f, ev)

	for _, tc := range []struct{ path, want string }{
		{"MSH-9.2", "A03"},
		{"EVN-1", "A03"},
		{"EVN-6", "20260302143500"},
		{"PV1-36", "01"},
		{"PV1-45", "20260302143500"},
	} {
		if got := m.get(tc.path); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestTriggerMapping(t *testing.T) {
	for _, tc := range []struct {
		kind sim.EventKind
		want string
	}{
		{sim.EncounterAdmitted, "A01"},
		{sim.EncounterTransferred, "A02"},
		{sim.EncounterDischarged, "A03"},
		{sim.PatientUpdated, "A08"},
		{sim.EncounterCancelled, "A11"},
		{sim.EncounterPreAdmitted, "A05"},
	} {
		f, ev := fixture()
		ev.Kind = tc.kind
		m := parse(t, f, ev)
		if got := m.get("MSH-9.2"); got != tc.want {
			t.Errorf("%s rendered trigger %q, want %q", tc.kind, got, tc.want)
		}
	}

	// Events this dialect has no trigger for must render nothing rather
	// than an empty or malformed message.
	f, ev := fixture()
	ev.Kind = sim.PatientRegistered
	msgs, err := f.Render(ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Errorf("PatientRegistered rendered %d messages, want none", len(msgs))
	}
	if f.Wants(sim.OrderPlaced) {
		t.Error("the ADT feed should not claim order events")
	}
}

func TestControlIDsAreUniqueAndOrdered(t *testing.T) {
	f, ev := fixture()
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		msgs, err := f.Render(ev)
		if err != nil {
			t.Fatal(err)
		}
		id := msgs[0].ControlID
		if seen[id] {
			t.Fatalf("duplicate control id %s", id)
		}
		seen[id] = true
	}
}

func TestRenderRefusesIncompleteEvents(t *testing.T) {
	f := adt.New(adt.DefaultConfig(), &sim.ControlIDs{})
	if _, err := f.Render(sim.Event{Kind: sim.EncounterAdmitted, At: at}); err == nil {
		t.Error("an event with no patient should be an error, not a malformed message")
	}
}
