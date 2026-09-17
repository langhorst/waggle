package sim

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"github.com/langhorst/waggle/internal/sim/simtime"
)

// arrivalShape weights admissions by hour of day, midnight first. Hospitals
// do not admit uniformly: the curve rises through the morning, peaks in the
// afternoon as clinics and surgery refer in, and falls away overnight to the
// ED's baseline. The numbers are a plausible shape, not a measured one.
var arrivalShape = [24]float64{
	0.35, 0.28, 0.24, 0.22, 0.24, 0.32, // 00-05
	0.55, 0.85, 1.20, 1.45, 1.55, 1.60, // 06-11
	1.55, 1.60, 1.65, 1.60, 1.45, 1.30, // 12-17
	1.20, 1.10, 0.95, 0.80, 0.60, 0.45, // 18-23
}

// dischargeShape weights discharges by hour. The late-morning wave is one of
// the most recognisable rhythms in an ADT feed, and a receiving system that
// cannot cope with it will not cope in production either.
var dischargeShape = [24]float64{
	0.10, 0.08, 0.06, 0.06, 0.06, 0.10,
	0.25, 0.50, 0.95, 1.60, 2.10, 2.30,
	2.00, 1.70, 1.45, 1.25, 1.00, 0.80,
	0.60, 0.45, 0.35, 0.25, 0.18, 0.12,
}

// weekendFactor thins arrivals at the weekend, when elective work stops.
func weekendFactor(t time.Time) float64 {
	switch t.Weekday() {
	case time.Saturday:
		return 0.62
	case time.Sunday:
		return 0.58
	default:
		return 1.0
	}
}

// edAdmitRate is the share of ED patients admitted upstairs rather than
// discharged home. Real EDs run in the mid teens; it is the single biggest
// driver of inpatient demand, so it is named rather than buried in a
// literal.
const edAdmitRate = 0.15

// Simulation advances the hospital and emits events. It is driven entirely
// by the scheduler: every activity schedules whatever follows it, so the run
// is a chain of simulated-time consequences rather than a polling loop.
//
// All of it runs on the scheduler's single goroutine, so the model needs no
// locks and every draw from rnd happens in a fixed order -- which is what
// makes a seed reproduce a run exactly.
type Simulation struct {
	cfg    Config
	net    *Network
	people *People
	rnd    *rand.Rand
	clock  *simtime.Clock
	sched  *simtime.Scheduler
	runner *Runner

	// emitErr keeps the first sink failure so Run can report it: tasks
	// cannot return errors to the scheduler.
	emitErr error
	ctx     context.Context

	// diverted counts arrivals turned away because every suitable bed was
	// taken. Diversion is real behaviour, but a simulation that diverts
	// most of its arrivals is miscalibrated rather than busy, so the count
	// is reported instead of being swallowed.
	diverted int
	admitted int
}

// NewSimulation builds the world from cfg and prepares it to run.
func NewSimulation(cfg Config, clock *simtime.Clock, sched *simtime.Scheduler, runner *Runner) (*Simulation, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	rnd := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	return &Simulation{
		cfg:    cfg,
		net:    BuildNetwork(cfg),
		people: NewPeople(rnd),
		rnd:    rnd,
		clock:  clock,
		sched:  sched,
		runner: runner,
	}, nil
}

// Network exposes the built world, for status reporting.
func (s *Simulation) Network() *Network { return s.net }

// Stats reports what the run did with its arrivals. A diverted share above
// a few percent means demand has outgrown the bed inventory: the feed is
// then a record of a hospital permanently full, which is not the traffic
// most tests want.
func (s *Simulation) Stats() (admitted, diverted int) { return s.admitted, s.diverted }

// Run advances the hospital until until (zero for no limit) or ctx ends.
func (s *Simulation) Run(ctx context.Context, until time.Time) error {
	s.ctx = ctx
	s.scheduleArrival(s.clock.Now())
	if err := s.sched.Run(ctx, until); err != nil {
		return err
	}
	return s.emitErr
}

// emit sends an event, remembering the first failure. A sink that has gone
// away stops the run at the next scheduler turn rather than silently
// dropping the rest of the day.
func (s *Simulation) emit(ev Event) {
	if s.emitErr != nil {
		return
	}
	if err := s.runner.Emit(s.ctx, ev); err != nil {
		s.emitErr = err
	}
}

// scheduleArrival queues the next arrival after now, drawing an exponential
// gap from the rate in force at that hour.
func (s *Simulation) scheduleArrival(now time.Time) {
	gap := s.nextArrivalGap(now)
	at := now.Add(gap)
	s.sched.At(at, func(at time.Time) {
		s.arrive(at)
		s.scheduleArrival(at)
	})
}

// nextArrivalGap draws the interval to the next arrival. The mean rate is
// the configured daily total reshaped by hour and weekday; the draw itself
// is exponential, which is what makes arrivals clump the way real ones do
// rather than arriving like a metronome.
func (s *Simulation) nextArrivalGap(now time.Time) time.Duration {
	perDay := s.cfg.ArrivalsPerDay * arrivalShape[now.Hour()] * weekendFactor(now)
	if perDay <= 0 {
		perDay = 0.01
	}
	meanGap := float64(24*time.Hour) / perDay
	// rand.ExpFloat64 has mean 1; scale it to the mean gap.
	gap := time.Duration(s.rnd.ExpFloat64() * meanGap)
	if gap < time.Second {
		gap = time.Second
	}
	return gap
}

// arrive registers a patient and admits them, if a bed can be found.
func (s *Simulation) arrive(at time.Time) {
	fac := s.net.Facilities[s.rnd.IntN(len(s.net.Facilities))]

	// Where the patient presents decides the encounter class and, later,
	// how long they stay.
	kind, class := s.presentation()
	unit := s.pickUnit(fac, kind)
	if unit == nil {
		s.diverted++
		return // this facility has no such unit
	}
	bed := unit.FreeBed(s.rnd)
	if bed == nil {
		// Full. Try any other unit of the same kind in the network before
		// giving up: a diverted patient is real behaviour, an invented bed
		// is not.
		for _, u := range s.net.Units(kind) {
			if b := u.FreeBed(s.rnd); b != nil {
				unit, bed = u, b
				break
			}
		}
		if bed == nil {
			s.diverted++
			return
		}
	}

	patient := s.people.NewPatient(unit.Facility, at)
	visit, account := s.people.NextVisit()
	provID, provName := s.people.Provider()
	enc := &Encounter{
		VisitNumber:   visit,
		AccountNumber: account,
		Patient:       patient,
		Facility:      unit.Facility,
		Class:         class,
		State:         StateActive,
		Location:      bed,
		AttendingID:   provID,
		AttendingName: provName,
		AdmitSource:   s.admitSource(kind),
		AdmittedAt:    at,
	}
	bed.Occupant = enc
	s.admitted++

	s.emit(Event{Kind: PatientRegistered, At: at, Patient: patient, Encounter: enc})
	s.emit(Event{Kind: EncounterAdmitted, At: at, Patient: patient, Encounter: enc, To: bed})

	// A small share of admissions turn out to be errors and are cancelled
	// shortly after, which is the A11 every receiver claims to handle.
	if s.rnd.Float64() < s.cfg.CancelRate {
		s.sched.At(at.Add(time.Duration(5+s.rnd.IntN(40))*time.Minute), func(at time.Time) {
			s.cancel(enc, at)
		})
		return
	}

	s.planStay(enc, unit, at)
}

// planStay schedules the rest of an encounter: an optional transfer, an
// optional demographic update, and the discharge that closes it.
func (s *Simulation) planStay(enc *Encounter, unit *Unit, at time.Time) {
	stay := s.lengthOfStay(unit.Kind)
	discharge := s.dischargeTime(at, at.Add(stay))

	if unit.Kind == UnitED && s.rnd.Float64() < edAdmitRate {
		// The ED patients who are admitted rather than sent home: an A02
		// into an inpatient bed, with a longer stay to follow.
		when := at.Add(time.Duration(40+s.rnd.IntN(200)) * time.Minute)
		if when.Before(discharge) {
			s.sched.At(when, func(at time.Time) { s.transferUpstairs(enc, at) })
			return
		}
	}
	if s.rnd.Float64() < s.cfg.TransferRate {
		when := at.Add(time.Duration(float64(stay) * (0.2 + 0.5*s.rnd.Float64())))
		if when.Before(discharge) {
			s.sched.At(when, func(at time.Time) { s.transferWithin(enc, at) })
		}
	}
	if s.rnd.Float64() < s.cfg.UpdateRate*float64(stay/time.Hour) {
		when := at.Add(time.Duration(float64(stay) * s.rnd.Float64()))
		if when.Before(discharge) {
			s.sched.At(when, func(at time.Time) { s.update(enc, at) })
		}
	}
	s.sched.At(discharge, func(at time.Time) { s.discharge(enc, at) })
}

// transferUpstairs moves an ED encounter into an inpatient bed and converts
// the class, the commonest transfer in a real feed.
func (s *Simulation) transferUpstairs(enc *Encounter, at time.Time) {
	if !enc.Active() {
		return
	}
	fac := s.net.Facility(enc.Facility)
	kind := UnitMedSurg
	if s.rnd.Float64() < 0.18 {
		kind = UnitICU
	}
	unit := s.pickUnit(fac, kind)
	if unit == nil {
		return
	}
	bed := unit.FreeBed(s.rnd)
	if bed == nil {
		// No bed upstairs: the patient boards in the ED, which is also what
		// really happens. Leave the stay to run its course.
		s.sched.At(s.dischargeTime(at, at.Add(s.lengthOfStay(UnitED))), func(at time.Time) {
			s.discharge(enc, at)
		})
		return
	}
	s.move(enc, bed, at)
	enc.Class = ClassInpatient
	s.planStay(enc, unit, at)
}

// transferWithin moves a patient to another bed of the same kind.
func (s *Simulation) transferWithin(enc *Encounter, at time.Time) {
	if !enc.Active() {
		return
	}
	fac := s.net.Facility(enc.Facility)
	var candidates []*Unit
	for _, u := range fac.Units {
		if u.Name != enc.Location.Unit {
			candidates = append(candidates, u)
		}
	}
	if len(candidates) == 0 {
		return
	}
	unit := candidates[s.rnd.IntN(len(candidates))]
	if bed := unit.FreeBed(s.rnd); bed != nil {
		s.move(enc, bed, at)
	}
}

// move relocates an encounter and emits the transfer.
func (s *Simulation) move(enc *Encounter, to *Location, at time.Time) {
	from := enc.Location
	if from != nil {
		from.Occupant = nil
	}
	enc.PriorLocation = from
	enc.Location = to
	to.Occupant = enc
	s.emit(Event{
		Kind: EncounterTransferred, At: at,
		Patient: enc.Patient, Encounter: enc, From: from, To: to,
	})
}

// update emits a demographic correction (A08), the message that most often
// exposes a receiver treating updates as inserts.
func (s *Simulation) update(enc *Encounter, at time.Time) {
	if !enc.Active() {
		return
	}
	switch s.rnd.IntN(3) {
	case 0:
		enc.Patient.Phone = "(555)" + padded(s.rnd.IntN(1000), 3) + "-" + padded(s.rnd.IntN(10000), 4)
	case 1:
		place := cities[s.rnd.IntN(len(cities))]
		enc.Patient.Address.City, enc.Patient.Address.State, enc.Patient.Address.Zip = place.City, place.State, place.Zip
	default:
		enc.Patient.MaritalSts = []string{"S", "M", "D", "W"}[s.rnd.IntN(4)]
	}
	s.emit(Event{Kind: PatientUpdated, At: at, Patient: enc.Patient, Encounter: enc})
}

// cancel retracts an admission (A11) and frees the bed.
func (s *Simulation) cancel(enc *Encounter, at time.Time) {
	if !enc.Active() {
		return
	}
	enc.State = StateCancelled
	if enc.Location != nil {
		enc.Location.Occupant = nil
	}
	s.emit(Event{Kind: EncounterCancelled, At: at, Patient: enc.Patient, Encounter: enc})
}

// discharge closes an encounter and frees the bed.
func (s *Simulation) discharge(enc *Encounter, at time.Time) {
	if !enc.Active() {
		return
	}
	enc.State = StateDischarged
	enc.DischargedAt = at
	enc.DischargeDisposition = s.disposition()
	if enc.Location != nil {
		enc.Location.Occupant = nil
	}
	s.emit(Event{Kind: EncounterDischarged, At: at, Patient: enc.Patient, Encounter: enc})
}

// presentation picks where a patient turns up and the class that implies.
func (s *Simulation) presentation() (UnitKind, EncounterClass) {
	// Most arrivals present to the ED; direct inpatient admissions are the
	// minority, because in a real hospital most inpatients are admitted
	// through the ED rather than booked straight to a ward. The split is
	// what keeps inpatient demand inside the bed inventory.
	switch r := s.rnd.Float64(); {
	case r < 0.82:
		return UnitED, ClassEmergency
	case r < 0.90:
		return UnitMedSurg, ClassInpatient
	case r < 0.97:
		return UnitLD, ClassInpatient
	default:
		return UnitICU, ClassInpatient
	}
}

func (s *Simulation) admitSource(kind UnitKind) string {
	if kind == UnitED {
		return "7" // emergency room
	}
	return "1" // physician referral
}

func (s *Simulation) disposition() string {
	switch r := s.rnd.Float64(); {
	case r < 0.78:
		return "01" // home
	case r < 0.90:
		return "03" // skilled nursing
	case r < 0.97:
		return "06" // home health
	default:
		return "20" // expired
	}
}

// pickUnit returns a unit of the given kind at fac, or nil.
func (s *Simulation) pickUnit(fac *Facility, kind UnitKind) *Unit {
	var matching []*Unit
	for _, u := range fac.Units {
		if u.Kind == kind {
			matching = append(matching, u)
		}
	}
	if len(matching) == 0 {
		return nil
	}
	return matching[s.rnd.IntN(len(matching))]
}

// lengthOfStay draws a stay for the unit kind. Stays are lognormal-ish: most
// are short, a few are very long, which is what makes census build rather
// than oscillate.
func (s *Simulation) lengthOfStay(kind UnitKind) time.Duration {
	var medianHours, sigma float64
	switch kind {
	case UnitED:
		medianHours, sigma = 3.5, 0.55
	case UnitICU:
		medianHours, sigma = 78, 0.75
	case UnitLD:
		medianHours, sigma = 48, 0.45
	case UnitNursery:
		medianHours, sigma = 44, 0.40
	case UnitPeriop:
		medianHours, sigma = 6, 0.50
	default:
		medianHours, sigma = 62, 0.70
	}
	hours := medianHours * math.Exp(sigma*s.rnd.NormFloat64())
	if hours < 0.5 {
		hours = 0.5
	}
	return time.Duration(hours * float64(time.Hour))
}

// dischargeTime nudges a computed discharge instant towards the hours when
// discharges actually happen, so the feed shows the late-morning wave.
//
// earliest floors the result. The nudge moves in both directions, and for a
// short stay -- an ED visit of a few hours -- an earlier candidate can land
// before the admission it follows, which would put a discharge ahead of its
// own admit in the stream. A receiving system is entitled to assume that
// never happens.
func (s *Simulation) dischargeTime(earliest, t time.Time) time.Time {
	if t.Before(earliest) {
		t = earliest
	}
	// Try a few nearby hours and take the one the shape favours. Sampling
	// beats solving here: it keeps the draw cheap and reproducible.
	best := t
	bestWeight := dischargeShape[t.Hour()] * s.rnd.Float64()
	for i := 0; i < 6; i++ {
		cand := t.Add(time.Duration(s.rnd.IntN(16)-4) * time.Hour)
		if cand.Before(earliest) {
			continue
		}
		if w := dischargeShape[cand.Hour()] * s.rnd.Float64(); w > bestWeight {
			best, bestWeight = cand, w
		}
	}
	// Land on a plausible minute rather than exactly on the hour.
	out := best.Truncate(time.Hour).Add(time.Duration(s.rnd.IntN(60)) * time.Minute)
	if out.Before(earliest) {
		// Truncating to the hour can itself cross the floor.
		out = earliest.Add(time.Duration(s.rnd.IntN(60)) * time.Minute)
	}
	return out
}

func padded(v, width int) string {
	s := ""
	for i := 0; i < width; i++ {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	return s
}
