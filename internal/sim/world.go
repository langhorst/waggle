package sim

import (
	"fmt"
	"time"
)

// Location is a place a patient can be: a bed on a unit in a facility. It
// renders into PV1-3 as point-of-care^room^bed^facility.
type Location struct {
	Facility string // e.g. "MERCY"
	Unit     string // point of care, e.g. "3WEST"
	Room     string
	Bed      string

	// Occupant is the encounter currently holding the bed, nil when free.
	// The world keeps this consistent so a transfer cannot put two patients
	// in one bed without an explicit swap.
	Occupant *Encounter
}

func (l *Location) String() string {
	if l == nil {
		return ""
	}
	return fmt.Sprintf("%s^%s^%s^%s", l.Unit, l.Room, l.Bed, l.Facility)
}

// Free reports whether the bed can take an admission.
func (l *Location) Free() bool { return l != nil && l.Occupant == nil }

// Unit is a ward: a named set of beds with a clinical character that drives
// how long patients stay and what gets ordered for them.
type Unit struct {
	Facility string
	Name     string
	Kind     UnitKind
	Beds     []*Location
}

// UnitKind shapes length of stay and ordering behaviour. It is deliberately
// coarse: the simulation needs plausible rhythm, not a clinical model.
type UnitKind string

const (
	UnitED      UnitKind = "ED"
	UnitMedSurg UnitKind = "MEDSURG"
	UnitICU     UnitKind = "ICU"
	UnitLD      UnitKind = "LD"
	UnitNursery UnitKind = "NURSERY"
	UnitPeriop  UnitKind = "PERIOP"
)

// Facility is one hospital in the network.
type Facility struct {
	Code  string // MSH-4 / PV1-3 facility, e.g. "MERCY"
	Name  string
	Units []*Unit
}

// Beds returns every bed in the facility.
func (f *Facility) Beds() []*Location {
	var out []*Location
	for _, u := range f.Units {
		out = append(out, u.Beds...)
	}
	return out
}

// Patient is a person known to the network. Identity is the part of an ADT
// simulation most worth getting right, so it is modelled explicitly rather
// than as a bag of strings: MRNs are per-facility, an enterprise ID spans
// them, and a merge records where a retired record went.
type Patient struct {
	// MRN is the medical record number at HomeFacility.
	MRN string
	// HomeFacility is the facility that assigned MRN.
	HomeFacility string
	// EnterpriseID identifies the person across facilities.
	EnterpriseID string

	Family, Given, Middle string
	Sex                   string // M, F, O, U per HL7 table 0001
	BirthDate             time.Time

	Address    Address
	Phone      string
	SSNLast4   string
	MaritalSts string

	// MergedInto is set when this record has been retired into another by a
	// merge; the ADT feed renders that as A18/A40. It is nil for live
	// records, and following it repeatedly reaches the surviving record.
	MergedInto *Patient
}

// Surviving follows a merge chain to the record still in use.
func (p *Patient) Surviving() *Patient {
	seen := map[*Patient]bool{}
	for p != nil && p.MergedInto != nil && !seen[p] {
		seen[p] = true
		p = p.MergedInto
	}
	return p
}

// Name renders the HL7 XPN family^given^middle.
func (p *Patient) Name() string {
	return fmt.Sprintf("%s^%s^%s", p.Family, p.Given, p.Middle)
}

// Address is a postal address, rendered into PID-11.
type Address struct {
	Street1, Street2, City, State, Zip, Country string
}

func (a Address) String() string {
	return fmt.Sprintf("%s^%s^%s^%s^%s^%s", a.Street1, a.Street2, a.City, a.State, a.Zip, a.Country)
}

// EncounterClass is PV1-2: inpatient, outpatient, emergency, and so on.
type EncounterClass string

const (
	ClassInpatient   EncounterClass = "I"
	ClassOutpatient  EncounterClass = "O"
	ClassEmergency   EncounterClass = "E"
	ClassPreAdmit    EncounterClass = "P"
	ClassRecurring   EncounterClass = "R"
	ClassObservation EncounterClass = "B"
)

// EncounterState is where a visit has got to. The ADT feed reads transitions
// between these states, so the set is small and the transitions explicit --
// an A03 can only follow an admission, which is the coherence an engine
// under test is entitled to assume.
type EncounterState string

const (
	StatePending    EncounterState = "PENDING"
	StateActive     EncounterState = "ACTIVE"
	StateDischarged EncounterState = "DISCHARGED"
	StateCancelled  EncounterState = "CANCELLED"
)

// Encounter is one visit: a patient in a bed for a stretch of time, with the
// orders placed during it.
type Encounter struct {
	// VisitNumber is PV1-19, unique per encounter.
	VisitNumber string
	// AccountNumber is PID-18, the billing account for the visit.
	AccountNumber string

	Patient  *Patient
	Facility string
	Class    EncounterClass
	State    EncounterState

	Location      *Location
	PriorLocation *Location

	AttendingID   string
	AttendingName string
	AdmitSource   string
	AdmitReason   string

	ScheduledAt          time.Time
	AdmittedAt           time.Time
	DischargedAt         time.Time
	DischargeDisposition string

	// Orders placed during this visit, in the order they were placed. The
	// ORM and ORU feeds read these; ADT never does.
	Orders []*Order
}

// Active reports whether the encounter is open.
func (e *Encounter) Active() bool { return e != nil && e.State == StateActive }

// OrderStatus is ORC-1/ORC-5 territory, kept as the few states the
// simulation actually moves an order through.
type OrderStatus string

const (
	OrderStatusNew       OrderStatus = "NW"
	OrderStatusInProcess OrderStatus = "IP"
	OrderStatusComplete  OrderStatus = "CM"
	OrderStatusCancelled OrderStatus = "CA"
)

// Order is a request placed during an encounter. It carries both the placer
// and filler numbers because the pairing is what lets an ORU be matched back
// to its ORM -- the single most valuable thing to get right once the results
// feed exists.
type Order struct {
	// PlacerNumber is ORC-2, assigned by the ordering system.
	PlacerNumber string
	// FillerNumber is ORC-3, assigned by the ancillary. Empty until the
	// filler has accepted the order.
	FillerNumber string

	Encounter *Encounter

	// Universal service identifier: code^text^coding system.
	Code       string
	Text       string
	CodeSystem string

	Status      OrderStatus
	Priority    string // STAT, ROUTINE
	PlacedAt    time.Time
	CollectedAt time.Time

	OrderingProviderID   string
	OrderingProviderName string

	// Results produced for this order.
	Results []*Result
}

// ServiceID renders the HL7 CE code^text^system.
func (o *Order) ServiceID() string {
	return fmt.Sprintf("%s^%s^%s", o.Code, o.Text, o.CodeSystem)
}

// ResultStatus is OBX-11.
type ResultStatus string

const (
	ResultStatusPreliminary ResultStatus = "P"
	ResultStatusFinal       ResultStatus = "F"
	ResultStatusCorrected   ResultStatus = "C"
)

// Result is one observation reported against an order, rendered as an OBX.
type Result struct {
	Order *Order

	// Set identifies the OBX within the report, 1-based.
	Set int

	Code       string
	Text       string
	CodeSystem string

	Value      string
	Units      string
	Range      string
	Abnormal   string // OBX-8: N, L, H, LL, HH, A
	Status     ResultStatus
	ObservedAt time.Time
	ReportedAt time.Time
}

// ObservationID renders the HL7 CE for OBX-3.
func (r *Result) ObservationID() string {
	return fmt.Sprintf("%s^%s^%s", r.Code, r.Text, r.CodeSystem)
}
