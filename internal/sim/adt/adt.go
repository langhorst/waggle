// Package adt renders hospital events as HL7 v2.5.1 ADT messages.
//
// The dialect is generic 2.5.1: the fields an interface of any flavour
// expects, and nothing vendor-specific. Vendor profiles (Epic's and Cerner's
// Z-segments, their identifier conventions) are the reason Config exists as
// a struct rather than constants -- a profile is a different Config and, in
// time, a different segment set, not a different renderer.
package adt

import (
	"fmt"
	"strings"
	"time"

	"github.com/langhorst/waggle/internal/sim"
)

// Config describes one ADT interface: who is sending, to whom, and in what
// dialect.
type Config struct {
	// SendingApp and SendingFacility are MSH-3 and MSH-4.
	SendingApp      string
	SendingFacility string
	// ReceivingApp and ReceivingFacility are MSH-5 and MSH-6.
	ReceivingApp      string
	ReceivingFacility string
	// Version is MSH-12.
	Version string
	// ProcessingID is MSH-11: D debugging, T training, P production. A
	// simulator should say T unless it is deliberately impersonating
	// production traffic.
	ProcessingID string
	// AssigningAuthority qualifies MRNs in PID-3, and is what makes the
	// per-facility MRN plus enterprise id model legible on the wire.
	AssigningAuthority string
}

// DefaultConfig is a workable generic 2.5.1 interface.
func DefaultConfig() Config {
	return Config{
		SendingApp:         "WAGGLESIM",
		SendingFacility:    "SIMNET",
		ReceivingApp:       "WAGGLE",
		ReceivingFacility:  "TEST",
		Version:            "2.5.1",
		ProcessingID:       "T",
		AssigningAuthority: "SIMNET",
	}
}

// Feed renders ADT messages. It implements sim.Feed.
type Feed struct {
	Cfg Config
	IDs *sim.ControlIDs
}

// New returns a feed with cfg, minting control IDs from ids.
func New(cfg Config, ids *sim.ControlIDs) *Feed {
	if cfg.Version == "" {
		cfg.Version = "2.5.1"
	}
	if cfg.ProcessingID == "" {
		cfg.ProcessingID = "T"
	}
	return &Feed{Cfg: cfg, IDs: ids}
}

func (f *Feed) Name() string { return "adt" }

// triggers maps hospital events onto the trigger events this dialect sends.
// PatientRegistered renders nothing: in a 2.5.1 inpatient feed the admit
// itself carries the registration, and sending both would double every
// patient. Keeping the mapping in one table is what lets a later profile
// disagree without touching the renderer.
var triggers = map[sim.EventKind]string{
	sim.EncounterAdmitted:    "A01",
	sim.EncounterTransferred: "A02",
	sim.EncounterDischarged:  "A03",
	sim.PatientUpdated:       "A08",
	sim.EncounterCancelled:   "A11",
	sim.EncounterPreAdmitted: "A05",
}

func (f *Feed) Wants(k sim.EventKind) bool {
	_, ok := triggers[k]
	return ok
}

// Render builds the message for ev.
func (f *Feed) Render(ev sim.Event) ([]sim.Message, error) {
	code, ok := triggers[ev.Kind]
	if !ok {
		return nil, nil
	}
	if ev.Patient == nil || ev.Encounter == nil {
		return nil, fmt.Errorf("adt: %s needs a patient and an encounter", ev.Kind)
	}
	controlID := f.IDs.Next()

	segments := []string{
		f.msh(code, controlID, ev.At),
		f.evn(code, ev),
		f.pid(ev.Patient, ev.Encounter),
		f.pv1(ev.Encounter),
	}
	// A02 states where the patient came from. PV1-6 carries the prior
	// location, and a receiver that tracks census needs it to vacate the
	// bed it had them in.
	raw := strings.Join(segments, "\r") + "\r"

	return []sim.Message{{
		Feed:      f.Name(),
		ControlID: controlID,
		Trigger:   "ADT^" + code,
		At:        ev.At,
		Raw:       []byte(raw),
		Cause:     ev,
	}}, nil
}

// segment builds a segment from explicit field numbers. Counting runs of
// empty strings is how PV1-36 quietly becomes PV1-38, so every field is
// written as the number its interface spec names.
func segment(name string, fields map[int]string) string {
	highest := 0
	for n := range fields {
		if n > highest {
			highest = n
		}
	}
	parts := make([]string, highest+1)
	parts[0] = name
	for n, v := range fields {
		parts[n] = v
	}
	for len(parts) > 1 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return strings.Join(parts, "|")
}

// mshSegment is the exception: MSH-1 is the field separator itself, so the
// separator that follows the name already occupies field 1 and the numbered
// fields start at 2.
func mshSegment(fields map[int]string) string {
	highest := 0
	for n := range fields {
		if n > highest {
			highest = n
		}
	}
	parts := make([]string, 0, highest)
	for n := 2; n <= highest; n++ {
		parts = append(parts, fields[n])
	}
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return "MSH|" + strings.Join(parts, "|")
}

func (f *Feed) msh(code, controlID string, at time.Time) string {
	structure := "ADT_" + code
	switch code {
	case "A08", "A05":
		structure = "ADT_A01" // both carry the A01 structure
	case "A11":
		structure = "ADT_A09"
	}
	return mshSegment(map[int]string{
		2:  "^~\\&",                         // encoding characters
		3:  f.Cfg.SendingApp,                // MSH-3
		4:  f.Cfg.SendingFacility,           // MSH-4
		5:  f.Cfg.ReceivingApp,              // MSH-5
		6:  f.Cfg.ReceivingFacility,         // MSH-6
		7:  sim.HL7Time(at),                 // MSH-7 message built
		9:  "ADT^" + code + "^" + structure, // MSH-9
		10: controlID,                       // MSH-10
		11: f.Cfg.ProcessingID,              // MSH-11
		12: f.Cfg.Version,                   // MSH-12
	})
}

// evn carries the event type and its times. EVN-2 is when the message was
// recorded and EVN-6 when the event actually occurred; a receiver should
// trust EVN-6 over MSH-7, and the two differ in a real system.
func (f *Feed) evn(code string, ev sim.Event) string {
	fields := map[int]string{
		1: code,               // EVN-1 event type
		2: sim.HL7Time(ev.At), // EVN-2 recorded
	}
	if ev.Kind == sim.EncounterDischarged && !ev.Encounter.DischargedAt.IsZero() {
		fields[6] = sim.HL7Time(ev.Encounter.DischargedAt) // EVN-6 occurred
	}
	return segment("EVN", fields)
}

func (f *Feed) pid(p *sim.Patient, enc *sim.Encounter) string {
	// PID-3 is the identifier list: the facility MRN qualified by its
	// assigning authority, then the enterprise id. Sending both is what
	// makes cross-facility identity resolvable downstream, and is the hook
	// the deferred merge events will hang on.
	mrn := p.MRN + "^^^" + p.HomeFacility + "^MR"
	eid := p.EnterpriseID + "^^^" + f.Cfg.AssigningAuthority + "^PI"

	return segment("PID", map[int]string{
		1:  "1",                      // PID-1 set id
		3:  mrn + "~" + eid,          // PID-3 identifier list
		5:  p.Name(),                 // PID-5 name
		7:  sim.HL7Date(p.BirthDate), // PID-7 date of birth
		8:  p.Sex,                    // PID-8 sex
		11: p.Address.String(),       // PID-11 address
		13: p.Phone,                  // PID-13 home phone
		16: p.MaritalSts,             // PID-16 marital status
		18: enc.AccountNumber,        // PID-18 account number
	})
}

func (f *Feed) pv1(enc *sim.Encounter) string {
	fields := map[int]string{
		1:  "1",                         // PV1-1 set id
		2:  string(enc.Class),           // PV1-2 patient class
		3:  enc.Location.String(),       // PV1-3 assigned location
		14: enc.AdmitSource,             // PV1-14 admit source
		19: enc.VisitNumber,             // PV1-19 visit number
		44: sim.HL7Time(enc.AdmittedAt), // PV1-44 admit date/time
	}
	if enc.PriorLocation != nil {
		fields[6] = enc.PriorLocation.String() // PV1-6 prior location
	}
	if enc.AttendingID != "" {
		fields[7] = enc.AttendingID + "^" + enc.AttendingName // PV1-7 attending
	}
	if enc.DischargeDisposition != "" {
		fields[36] = enc.DischargeDisposition // PV1-36 discharge disposition
	}
	if !enc.DischargedAt.IsZero() {
		fields[45] = sim.HL7Time(enc.DischargedAt) // PV1-45 discharge date/time
	}
	return segment("PV1", fields)
}
