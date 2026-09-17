package sim

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Everything below is invented. The name lists are deliberately small and
// obviously synthetic, and identifiers carry a facility-letter prefix that
// no numeric MRN range can collide with, so simulated data is recognisable
// at a glance in a log, a database or a bug report.

var familyNames = []string{
	"AALBERG", "BRANNOCK", "CALLOWFIELD", "DRAYMOOR", "ELDERWICK",
	"FENWRIGHT", "GALLOWAY", "HOLLINGER", "IVESDALE", "JANNAWAY",
	"KESTREL", "LARKHAM", "MERRIWEATHER", "NORBRIDGE", "OAKHURST",
	"PENHALLOW", "QUILLINGTON", "RAVENSCAR", "STANBURY", "THORNQUIST",
	"UNDERHILL", "VANCROFT", "WETHERBY", "YARBOROUGH", "ZELLWOOD",
}

var givenNamesM = []string{
	"ALDEN", "BRAM", "CEDRIC", "DESMOND", "EDWIN", "FLETCHER", "GARRETT",
	"HOLLIS", "IVOR", "JASPER", "KENDRICK", "LOWELL", "MERRICK", "NIGEL",
}

var givenNamesF = []string{
	"ADELINE", "BRYONY", "CLEMENTINE", "DELPHINE", "ELSPETH", "FREYA",
	"GWENDOLYN", "HARRIET", "IMOGEN", "JUNIPER", "KEZIAH", "LINNEA",
	"MARIGOLD", "NERISSA",
}

var middleInitials = []string{"A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "P", "R", "S", "T", ""}

var streets = []string{
	"ELDER LN", "MARIGOLD WAY", "QUARRY RD", "THISTLE CT", "WESTGATE AVE",
	"BRIAR HOLLOW", "CANTERBURY DR", "FOXGLOVE ST", "HAWTHORN PL",
}

var cities = []struct{ City, State, Zip string }{
	{"SPRINGDALE", "OH", "45001"},
	{"FAIRHAVEN", "OH", "45002"},
	{"NORTHFIELD", "OH", "45003"},
	{"WESTBROOK", "KY", "41001"},
	{"MILLERTON", "KY", "41002"},
}

// providers are the attending physicians the simulation assigns. Also
// invented; the NPI-shaped ids are outside any issued range.
var providers = []struct{ ID, Name string }{
	{"SIM0001", "HALLOWELL^ROSE^E^^^MD"},
	{"SIM0002", "OKONKWO^DANIEL^^^^MD"},
	{"SIM0003", "VANTERPOOL^MARGUERITE^A^^^MD"},
	{"SIM0004", "STRAND^BENEDIKT^^^^MD"},
	{"SIM0005", "ASHDOWN^PRIYA^N^^^MD"},
	{"SIM0006", "CASTELLANOS^EMILIO^^^^DO"},
}

// People mints patients. It holds the counters that make identifiers unique
// and reproducible: the same seed and the same call order give the same
// people, which is what lets a failing run be replayed.
//
// Identity is modelled the way a real network works and the way the later
// merge events will need: an MRN belongs to the facility that assigned it,
// and an enterprise id spans the network. A person seen at a second facility
// gets a second MRN under the same enterprise id -- already true here, so
// A18/A40 merge support later is a new renderer rather than a new model.
type People struct {
	rnd *rand.Rand

	mrnSeq   map[string]int // per-facility MRN counter
	eidSeq   int
	visitSeq int
	acctSeq  int
}

// NewPeople returns a mint drawing from rnd.
func NewPeople(rnd *rand.Rand) *People {
	return &People{rnd: rnd, mrnSeq: map[string]int{}}
}

// NewPatient invents a person registered at facility.
func (p *People) NewPatient(facility string, now time.Time) *Patient {
	p.eidSeq++
	sex := "F"
	given := givenNamesF[p.rnd.IntN(len(givenNamesF))]
	if p.rnd.IntN(2) == 0 {
		sex = "M"
		given = givenNamesM[p.rnd.IntN(len(givenNamesM))]
	}
	place := cities[p.rnd.IntN(len(cities))]
	return &Patient{
		MRN:          p.NextMRN(facility),
		HomeFacility: facility,
		EnterpriseID: fmt.Sprintf("EID%07d", p.eidSeq),
		Family:       familyNames[p.rnd.IntN(len(familyNames))],
		Given:        given,
		Middle:       middleInitials[p.rnd.IntN(len(middleInitials))],
		Sex:          sex,
		BirthDate:    p.birthDate(now),
		Address: Address{
			Street1: fmt.Sprintf("%d %s", 100+p.rnd.IntN(9800), streets[p.rnd.IntN(len(streets))]),
			City:    place.City, State: place.State, Zip: place.Zip, Country: "USA",
		},
		Phone:      fmt.Sprintf("(555)%03d-%04d", p.rnd.IntN(1000), p.rnd.IntN(10000)),
		SSNLast4:   fmt.Sprintf("%04d", p.rnd.IntN(10000)),
		MaritalSts: []string{"S", "M", "D", "W", ""}[p.rnd.IntN(5)],
	}
}

// NextMRN assigns the next medical record number at facility. It is exported
// because a patient seen at a second facility needs one there too -- the
// case the enterprise id exists for.
func (p *People) NextMRN(facility string) string {
	p.mrnSeq[facility]++
	prefix := facility
	if len(prefix) > 4 {
		prefix = prefix[:4]
	}
	return fmt.Sprintf("%s%07d", prefix, p.mrnSeq[facility])
}

// NextVisit returns the next visit number (PV1-19) and account number
// (PID-18) for an encounter.
func (p *People) NextVisit() (visit, account string) {
	p.visitSeq++
	p.acctSeq++
	return fmt.Sprintf("V%09d", p.visitSeq), fmt.Sprintf("A%09d", p.acctSeq)
}

// Provider picks an attending.
func (p *People) Provider() (id, name string) {
	pr := providers[p.rnd.IntN(len(providers))]
	return pr.ID, pr.Name
}

// birthDate draws an age with a rough hospital skew: adults throughout, and
// a heavier tail past sixty, where the admissions actually are.
func (p *People) birthDate(now time.Time) time.Time {
	var age int
	switch r := p.rnd.Float64(); {
	case r < 0.08:
		age = p.rnd.IntN(18) // paediatric
	case r < 0.40:
		age = 18 + p.rnd.IntN(42)
	default:
		age = 60 + p.rnd.IntN(35)
	}
	days := p.rnd.IntN(365)
	return now.AddDate(-age, 0, -days).Truncate(24 * time.Hour)
}
