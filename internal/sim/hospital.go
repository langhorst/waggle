package sim

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// UnitConfig describes one ward to build.
type UnitConfig struct {
	Name  string   `yaml:"name"`
	Kind  UnitKind `yaml:"kind"`
	Beds  int      `yaml:"beds"`
	Rooms int      `yaml:"rooms"` // beds are spread over this many rooms; 0 means two per room
}

// FacilityConfig describes one hospital.
type FacilityConfig struct {
	Code  string       `yaml:"code"`
	Name  string       `yaml:"name"`
	Units []UnitConfig `yaml:"units"`
}

// Config is the whole simulated network.
type Config struct {
	// Seed makes a run reproducible. Two runs with the same seed and config
	// produce the same patients, the same beds and the same messages.
	Seed uint64 `yaml:"seed"`
	// StartAt is the simulated instant the run begins. Fixing it keeps
	// timestamps reproducible too.
	StartAt time.Time `yaml:"startAt"`

	Facilities []FacilityConfig `yaml:"facilities"`

	// ArrivalsPerDay is the network-wide mean, before the diurnal shaping
	// below redistributes it across the hours.
	ArrivalsPerDay float64 `yaml:"arrivalsPerDay"`

	// UpdateRate is the chance an active encounter emits an A08 on any
	// given simulated hour.
	UpdateRate float64 `yaml:"updateRate"`
	// TransferRate is the chance an inpatient stay includes a transfer.
	TransferRate float64 `yaml:"transferRate"`
	// CancelRate is the chance an admission is cancelled shortly after
	// (A11) rather than running its course.
	CancelRate float64 `yaml:"cancelRate"`
}

// DefaultConfig is a small two-hospital network that runs out of the box.
func DefaultConfig() Config {
	return Config{
		Seed:    1,
		StartAt: time.Date(2026, 3, 2, 6, 0, 0, 0, time.UTC),
		Facilities: []FacilityConfig{
			{Code: "MERCY", Name: "Mercy General", Units: []UnitConfig{
				{Name: "ED", Kind: UnitED, Beds: 28},
				{Name: "3WEST", Kind: UnitMedSurg, Beds: 48},
				{Name: "4EAST", Kind: UnitMedSurg, Beds: 48},
				{Name: "5NORTH", Kind: UnitMedSurg, Beds: 40},
				{Name: "ICU", Kind: UnitICU, Beds: 20, Rooms: 20},
				{Name: "LD", Kind: UnitLD, Beds: 16},
			}},
			{Code: "STLUKE", Name: "St Luke Community", Units: []UnitConfig{
				{Name: "ED", Kind: UnitED, Beds: 14},
				{Name: "2NORTH", Kind: UnitMedSurg, Beds: 36},
				{Name: "ICU", Kind: UnitICU, Beds: 10, Rooms: 10},
			}},
		},
		// Calibrated against the bed inventory above: this rate, the
		// presentation mix and the lengths of stay settle at roughly 80%
		// inpatient occupancy. A hospital pinned at 100% diverts most of
		// its arrivals, which makes for a degenerate feed.
		ArrivalsPerDay: 180,
		UpdateRate:     0.02,
		TransferRate:   0.25,
		CancelRate:     0.03,
	}
}

// Validate reports configuration that cannot produce a running hospital.
func (c *Config) Validate() error {
	if len(c.Facilities) == 0 {
		return fmt.Errorf("sim: at least one facility is required")
	}
	seen := map[string]bool{}
	for _, f := range c.Facilities {
		if f.Code == "" {
			return fmt.Errorf("sim: every facility needs a code")
		}
		if seen[f.Code] {
			return fmt.Errorf("sim: duplicate facility code %q", f.Code)
		}
		seen[f.Code] = true
		if len(f.Units) == 0 {
			return fmt.Errorf("sim: facility %s has no units", f.Code)
		}
		units := map[string]bool{}
		for _, u := range f.Units {
			if u.Name == "" {
				return fmt.Errorf("sim: facility %s has an unnamed unit", f.Code)
			}
			if units[u.Name] {
				return fmt.Errorf("sim: facility %s has two units named %q", f.Code, u.Name)
			}
			units[u.Name] = true
			if u.Beds <= 0 {
				return fmt.Errorf("sim: unit %s/%s needs at least one bed", f.Code, u.Name)
			}
		}
	}
	if c.ArrivalsPerDay <= 0 {
		return fmt.Errorf("sim: arrivalsPerDay must be positive")
	}
	return nil
}

// Network is the built world: facilities with real, finite bed inventory.
//
// Beds being finite is what keeps the traffic honest. An admission that
// cannot find a bed is diverted rather than conjuring one, so the census a
// receiving system computes from the feed matches the census here.
type Network struct {
	Facilities []*Facility
	byCode     map[string]*Facility
}

// BuildNetwork constructs the world described by cfg.
func BuildNetwork(cfg Config) *Network {
	n := &Network{byCode: map[string]*Facility{}}
	for _, fc := range cfg.Facilities {
		f := &Facility{Code: fc.Code, Name: fc.Name}
		for _, uc := range fc.Units {
			rooms := uc.Rooms
			if rooms <= 0 {
				rooms = (uc.Beds + 1) / 2 // two beds to a room
			}
			u := &Unit{Facility: fc.Code, Name: uc.Name, Kind: uc.Kind}
			for i := 0; i < uc.Beds; i++ {
				room := fmt.Sprintf("%d", 100+(i%rooms)+1)
				bed := string(rune('A' + i/rooms))
				u.Beds = append(u.Beds, &Location{
					Facility: fc.Code, Unit: uc.Name, Room: room, Bed: bed,
				})
			}
			f.Units = append(f.Units, u)
		}
		n.Facilities = append(n.Facilities, f)
		n.byCode[fc.Code] = f
	}
	return n
}

// Facility looks up a facility by code.
func (n *Network) Facility(code string) *Facility { return n.byCode[code] }

// Units returns every unit of the given kinds across the network.
func (n *Network) Units(kinds ...UnitKind) []*Unit {
	want := map[UnitKind]bool{}
	for _, k := range kinds {
		want[k] = true
	}
	var out []*Unit
	for _, f := range n.Facilities {
		for _, u := range f.Units {
			if len(want) == 0 || want[u.Kind] {
				out = append(out, u)
			}
		}
	}
	return out
}

// FreeBed finds an empty bed in u, or nil when the unit is full. The choice
// is drawn from rnd so runs stay reproducible; scanning in order would make
// bed assignment an artefact of array layout.
func (u *Unit) FreeBed(rnd *rand.Rand) *Location {
	free := make([]*Location, 0, len(u.Beds))
	for _, b := range u.Beds {
		if b.Free() {
			free = append(free, b)
		}
	}
	if len(free) == 0 {
		return nil
	}
	return free[rnd.IntN(len(free))]
}

// Occupancy reports how many of the unit's beds are taken.
func (u *Unit) Occupancy() (occupied, total int) {
	for _, b := range u.Beds {
		if !b.Free() {
			occupied++
		}
	}
	return occupied, len(u.Beds)
}

// Census counts active encounters across the network.
func (n *Network) Census() int {
	total := 0
	for _, f := range n.Facilities {
		for _, u := range f.Units {
			occupied, _ := u.Occupancy()
			total += occupied
		}
	}
	return total
}
