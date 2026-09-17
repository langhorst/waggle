package sim_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/format"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
	"github.com/langhorst/waggle/internal/sim"
	"github.com/langhorst/waggle/internal/sim/adt"
	"github.com/langhorst/waggle/internal/sim/simtime"
)

// runDays simulates days of hospital and returns every message the ADT feed
// produced, plus the simulation for its statistics.
func runDays(t *testing.T, days float64, tweak func(*sim.Config)) ([]sim.Message, *sim.Simulation) {
	t.Helper()
	cfg := sim.DefaultConfig()
	if tweak != nil {
		tweak(&cfg)
	}
	collector := &memSink{}
	runner := sim.NewRunner(nil)
	runner.Wire(sim.Wiring{
		Feed: adt.New(adt.DefaultConfig(), &sim.ControlIDs{Prefix: "SIM"}),
		Sink: collector,
	})
	clock := simtime.New(cfg.StartAt, simtime.Unbounded, nil)
	sched := simtime.NewScheduler(clock)
	s, err := sim.NewSimulation(cfg, clock, sched, runner)
	if err != nil {
		t.Fatal(err)
	}
	until := cfg.StartAt.Add(time.Duration(days * float64(24*time.Hour)))
	if err := s.Run(context.Background(), until); err != nil {
		t.Fatal(err)
	}
	return collector.all(), s
}

// The seed is the contract: a run that fails is only worth reporting if it
// can be reproduced exactly, so identical seeds must produce identical
// bytes, control ids and timestamps.
func TestSameSeedReproducesTheRunExactly(t *testing.T) {
	a, _ := runDays(t, 3, nil)
	b, _ := runDays(t, 3, nil)
	if len(a) != len(b) {
		t.Fatalf("same seed produced %d and %d messages", len(a), len(b))
	}
	if len(a) == 0 {
		t.Fatal("no messages produced")
	}
	for i := range a {
		if string(a[i].Raw) != string(b[i].Raw) {
			t.Fatalf("message %d differs between runs:\n%s\n%s", i, a[i].Raw, b[i].Raw)
		}
		if !a[i].At.Equal(b[i].At) || a[i].ControlID != b[i].ControlID {
			t.Fatalf("message %d differs in stamp or id", i)
		}
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	a, _ := runDays(t, 2, nil)
	b, _ := runDays(t, 2, func(c *sim.Config) { c.Seed = 99 })
	same := len(a) == len(b)
	if same {
		for i := range a {
			if string(a[i].Raw) != string(b[i].Raw) {
				same = false
				break
			}
		}
	}
	if same {
		t.Error("a different seed produced an identical run")
	}
}

// Every message must parse with waggle's own HL7 parser: a simulator that
// emits what its engine cannot read is worse than no simulator.
func TestEveryMessageParses(t *testing.T) {
	msgs, _ := runDays(t, 2, nil)
	dt, ok := format.Get("hl7v2")
	if !ok {
		t.Fatal("hl7v2 not registered")
	}
	for _, m := range msgs {
		if _, err := dt.Parse(m.Raw); err != nil {
			t.Fatalf("unparseable %s %s: %v\n%s", m.Trigger, m.ControlID, err, m.Raw)
		}
	}
}

// The coherence claim the whole design rests on: a visit is admitted before
// it is transferred or discharged, discharged at most once, and never
// referenced after it closes. This is what a stateless generator cannot
// give, and what makes the feed worth testing an engine against.
func TestEncounterLifecycleIsCoherent(t *testing.T) {
	msgs, _ := runDays(t, 5, nil)
	dt, _ := format.Get("hl7v2")

	type visitState struct {
		admitted, closed bool
		lastAt           time.Time
	}
	visits := map[string]*visitState{}
	value := func(m sim.Message, path string) string {
		root, err := dt.Parse(m.Raw)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		nodes, err := dt.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}

	for _, m := range msgs {
		visit := value(m, "PV1-19")
		if visit == "" {
			t.Fatalf("%s carries no visit number", m.Trigger)
		}
		st := visits[visit]
		if st == nil {
			st = &visitState{}
			visits[visit] = st
		}
		trigger := value(m, "MSH-9.2")
		switch trigger {
		case "A01":
			if st.admitted {
				t.Errorf("visit %s admitted twice", visit)
			}
			st.admitted = true
		case "A02", "A08":
			if !st.admitted {
				t.Errorf("visit %s got %s before any admission", visit, trigger)
			}
			if st.closed {
				t.Errorf("visit %s got %s after it closed", visit, trigger)
			}
		case "A03", "A11":
			if !st.admitted {
				t.Errorf("visit %s got %s before any admission", visit, trigger)
			}
			if st.closed {
				t.Errorf("visit %s closed twice", visit)
			}
			st.closed = true
		}
		// Events for one visit must arrive in time order.
		if m.At.Before(st.lastAt) {
			t.Errorf("visit %s went backwards in time at %s", visit, m.At)
		}
		st.lastAt = m.At
	}
	if len(visits) < 10 {
		t.Fatalf("only %d visits in five days; the run is too small to prove anything", len(visits))
	}
}

// A bed holds one patient. A simulation that double-books beds would report
// a census no receiving system could reproduce.
func TestBedsAreNeverDoubleOccupied(t *testing.T) {
	msgs, s := runDays(t, 5, nil)
	dt, _ := format.Get("hl7v2")

	occupant := map[string]string{} // location -> visit
	for _, m := range msgs {
		root, err := dt.Parse(m.Raw)
		if err != nil {
			t.Fatal(err)
		}
		get := func(path string) string {
			nodes, err := dt.Resolve(root, path)
			if err != nil || len(nodes) == 0 {
				return ""
			}
			return dt.Value(root, nodes[0])
		}
		trigger, visit, loc := get("MSH-9.2"), get("PV1-19"), get("PV1-3")
		switch trigger {
		case "A01":
			if held, ok := occupant[loc]; ok && held != visit {
				t.Errorf("%s admitted into %s while %s still held it", visit, loc, held)
			}
			occupant[loc] = visit
		case "A02":
			if prior := get("PV1-6"); prior != "" && occupant[prior] == visit {
				delete(occupant, prior)
			}
			if held, ok := occupant[loc]; ok && held != visit {
				t.Errorf("%s transferred into %s while %s still held it", visit, loc, held)
			}
			occupant[loc] = visit
		case "A03", "A11":
			if occupant[loc] == visit {
				delete(occupant, loc)
			}
		}
	}
	// The beds still held at the end should match the model's own census.
	if census := s.Network().Census(); census != len(occupant) {
		t.Errorf("feed implies %d occupied beds, model says %d", len(occupant), census)
	}
}

// The defaults must describe a hospital that works: one pinned at capacity
// diverts most of its arrivals and produces a degenerate feed.
func TestDefaultsAreCalibrated(t *testing.T) {
	_, s := runDays(t, 30, nil)
	admitted, diverted := s.Stats()
	if admitted == 0 {
		t.Fatal("no admissions")
	}
	if rate := float64(diverted) / float64(admitted+diverted); rate > 0.10 {
		t.Errorf("%.1f%% of arrivals diverted; the default network is short of beds", 100*rate)
	}

	beds := 0
	for _, f := range s.Network().Facilities {
		beds += len(f.Beds())
	}
	occupancy := float64(s.Network().Census()) / float64(beds)
	if occupancy < 0.30 || occupancy > 0.95 {
		t.Errorf("census settled at %.0f%% of beds, outside a plausible band", 100*occupancy)
	}
}

// The mix should look like a hospital: mostly admits and discharges, which
// roughly balance once the census has settled.
func TestTriggerMixLooksLikeAHospital(t *testing.T) {
	msgs, _ := runDays(t, 30, nil)
	count := map[string]int{}
	for _, m := range msgs {
		count[m.Trigger]++
	}
	for _, want := range []string{"ADT^A01", "ADT^A02", "ADT^A03", "ADT^A08"} {
		if count[want] == 0 {
			t.Errorf("no %s in thirty days", want)
		}
	}
	admits, discharges := count["ADT^A01"], count["ADT^A03"]+count["ADT^A11"]
	if ratio := float64(discharges) / float64(admits); ratio < 0.75 || ratio > 1.25 {
		t.Errorf("discharges/admits = %.2f; a settled census should roughly balance", ratio)
	}
}

// The diurnal shaping has to be visible, or the feed is a flat stream no
// receiver has to cope with.
func TestTrafficFollowsTimeOfDay(t *testing.T) {
	msgs, _ := runDays(t, 21, nil)
	byHour := [24]int{}
	for _, m := range msgs {
		if m.Trigger == "ADT^A01" {
			byHour[m.At.Hour()]++
		}
	}
	night, afternoon := 0, 0
	for h := 1; h <= 4; h++ {
		night += byHour[h]
	}
	for h := 12; h <= 15; h++ {
		afternoon += byHour[h]
	}
	if afternoon <= night*2 {
		t.Errorf("admissions are flat: %d in the small hours vs %d in the afternoon", night, afternoon)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		tweak   func(*sim.Config)
		wantErr string
	}{
		{"no facilities", func(c *sim.Config) { c.Facilities = nil }, "at least one facility"},
		{"duplicate facility", func(c *sim.Config) {
			c.Facilities = append(c.Facilities, c.Facilities[0])
		}, "duplicate facility code"},
		{"no beds", func(c *sim.Config) { c.Facilities[0].Units[0].Beds = 0 }, "at least one bed"},
		{"no arrivals", func(c *sim.Config) { c.ArrivalsPerDay = 0 }, "arrivalsPerDay must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sim.DefaultConfig()
			tc.tweak(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
	def := sim.DefaultConfig()
	if err := def.Validate(); err != nil {
		t.Errorf("the defaults must validate: %v", err)
	}
}
