package simtime

import (
	"context"
	"math"
	"testing"
	"time"
)

var epoch = time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)

func TestScaleForDayIn(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want float64
	}{
		{time.Minute, 1440},
		{10 * time.Minute, 144},
		{time.Hour, 24},
		{24 * time.Hour, 1},
	} {
		if got := ScaleForDayIn(tc.in); got != tc.want {
			t.Errorf("ScaleForDayIn(%s) = %v, want %v", tc.in, got, tc.want)
		}
		// Round trip: the rate must describe the same simulated day.
		if got := DayIn(tc.want); got != tc.in {
			t.Errorf("DayIn(%v) = %s, want %s", tc.want, got, tc.in)
		}
	}
	if got := ScaleForDayIn(0); got != Unbounded {
		t.Errorf("a zero day means no waiting, got %v", got)
	}
}

func TestPausedClockDoesNotAdvance(t *testing.T) {
	c := New(epoch, Paused, nil)
	if !c.Paused() {
		t.Fatal("clock should report paused")
	}
	start := c.Now()
	time.Sleep(2 * time.Millisecond)
	if got := c.Now(); !got.Equal(start) {
		t.Errorf("paused clock advanced from %s to %s", start, got)
	}
}

func TestRateChangeIsContinuous(t *testing.T) {
	c := New(epoch, ScaleForDayIn(time.Minute), nil)
	before := c.Now()
	c.SetScale(ScaleForDayIn(time.Hour)) // 60x slower
	after := c.Now()
	if after.Before(before) {
		t.Errorf("simulated time went backwards across a rate change: %s then %s", before, after)
	}
	// The jump must be a pacing artifact only, not a re-scaling of the
	// elapsed wall time since the clock started.
	if d := after.Sub(before); d > time.Minute {
		t.Errorf("rate change jumped simulated time by %s", d)
	}
	if got, want := c.Scale(), ScaleForDayIn(time.Hour); got != want {
		t.Errorf("scale = %v, want %v", got, want)
	}
}

func TestInvalidRatesPause(t *testing.T) {
	for _, scale := range []float64{math.NaN(), -1, math.Inf(-1)} {
		if c := New(epoch, scale, nil); !c.Paused() {
			t.Errorf("rate %v should pause, got %v", scale, c.Scale())
		}
	}
	if c := New(epoch, math.Inf(1), nil); finite(c.Scale()) {
		t.Error("+Inf should be an unbounded rate")
	}
}

func TestUnboundedSleepJumpsWithoutWaiting(t *testing.T) {
	c := New(epoch, Unbounded, nil)
	target := epoch.Add(72 * time.Hour)
	wallStart := time.Now()
	if err := c.SleepUntil(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if got := c.Now(); got.Before(target) {
		t.Errorf("simulated time = %s, want at least %s", got, target)
	}
	// Three simulated days must not have cost meaningful wall time. The
	// bound is loose on purpose: the property is "does not wait", not a
	// latency figure.
	if waited := time.Since(wallStart); waited > time.Second {
		t.Errorf("unbounded sleep waited %s", waited)
	}
}

func TestSleepUntilPastReturnsImmediately(t *testing.T) {
	c := New(epoch, RealTime, nil)
	if err := c.SleepUntil(context.Background(), epoch.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func TestSleepUntilHonoursContext(t *testing.T) {
	c := New(epoch, Paused, nil) // never arrives on its own
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.SleepUntil(ctx, epoch.Add(time.Hour)); err == nil {
		t.Error("a cancelled context should end the sleep")
	}
}

// A sleeper on a paused clock must wake when the rate changes, not stay
// blocked until its original deadline: this is what makes speed adjustable
// mid-run rather than only between runs.
func TestRateChangeWakesSleeper(t *testing.T) {
	c := New(epoch, Paused, nil)
	done := make(chan error, 1)
	go func() { done <- c.SleepUntil(context.Background(), epoch.Add(24*time.Hour)) }()
	c.Resume(Unbounded)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sleep ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sleeper did not wake on the rate change")
	}
}

func TestSchedulerRunsInSimulatedOrder(t *testing.T) {
	c := New(epoch, Unbounded, nil)
	s := NewScheduler(c)
	var order []string
	// Scheduled out of order, including two at the same instant.
	s.At(epoch.Add(3*time.Hour), func(time.Time) { order = append(order, "third") })
	s.At(epoch.Add(time.Hour), func(time.Time) { order = append(order, "first") })
	s.At(epoch.Add(2*time.Hour), func(time.Time) { order = append(order, "tie-a") })
	s.At(epoch.Add(2*time.Hour), func(time.Time) { order = append(order, "tie-b") })

	if err := s.Run(context.Background(), time.Time{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"first", "tie-a", "tie-b", "third"}
	if len(order) != len(want) {
		t.Fatalf("ran %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("ran %v, want %v (ties keep insertion order)", order, want)
		}
	}
}

func TestSchedulerTasksCanScheduleMore(t *testing.T) {
	c := New(epoch, Unbounded, nil)
	s := NewScheduler(c)
	var times []time.Time
	var step func(at time.Time)
	step = func(at time.Time) {
		times = append(times, at)
		if len(times) < 5 {
			s.At(at.Add(30*time.Minute), step)
		}
	}
	s.At(epoch, step)
	if err := s.Run(context.Background(), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if len(times) != 5 {
		t.Fatalf("ran %d steps, want 5", len(times))
	}
	for i, got := range times {
		want := epoch.Add(time.Duration(i) * 30 * time.Minute)
		if !got.Equal(want) {
			t.Errorf("step %d at %s, want %s", i, got, want)
		}
	}
}

func TestSchedulerStopsAtUntil(t *testing.T) {
	c := New(epoch, Unbounded, nil)
	s := NewScheduler(c)
	var ran int
	for i := 1; i <= 10; i++ {
		s.At(epoch.Add(time.Duration(i)*time.Hour), func(time.Time) { ran++ })
	}
	if err := s.Run(context.Background(), epoch.Add(4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ran != 4 {
		t.Errorf("ran %d tasks, want the 4 within the window", ran)
	}
	if n := s.Len(); n != 6 {
		t.Errorf("%d tasks left pending, want 6", n)
	}
}

// The property the whole design rests on: the rate is pacing only. The same
// schedule run at wildly different speeds must produce identical simulated
// timestamps, because tasks are handed the instant they were scheduled for
// rather than a reading of the clock.
func TestRateDoesNotChangeGeneratedTimestamps(t *testing.T) {
	run := func(scale float64) []time.Time {
		c := New(epoch, scale, nil)
		s := NewScheduler(c)
		var stamps []time.Time
		var step func(at time.Time)
		step = func(at time.Time) {
			stamps = append(stamps, at)
			if len(stamps) < 8 {
				s.At(at.Add(7*time.Minute), step)
			}
		}
		s.At(epoch, step)
		if err := s.Run(context.Background(), time.Time{}); err != nil {
			t.Fatal(err)
		}
		return stamps
	}

	// Three rates: no pacing at all, and two finite ones that exercise the
	// waiting path. The finite rates are fast enough to keep the test cheap
	// -- what is under test is that content is rate-independent, and the
	// wall-clock cost of proving it is pure waste. TestUntilReflectsRate
	// covers the arithmetic for operator-scale rates without sleeping.
	fast := run(Unbounded)
	quick := run(1e7)
	quicker := run(1e6)

	if len(fast) != 8 {
		t.Fatalf("got %d stamps", len(fast))
	}
	for i := range fast {
		if !quick[i].Equal(fast[i]) || !quicker[i].Equal(fast[i]) {
			t.Errorf("stamp %d differs by rate: unbounded %s, 1e7 %s, 1e6 %s",
				i, fast[i], quick[i], quicker[i])
		}
	}
}

// The rates an operator actually sets are slow by nature, so their pacing is
// checked arithmetically rather than by waiting for it.
func TestUntilReflectsRate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dayIn   time.Duration
		simSpan time.Duration
		want    time.Duration
	}{
		{"a day in a minute, one simulated hour", time.Minute, time.Hour, 2500 * time.Millisecond},
		{"a day in ten minutes, one simulated hour", 10 * time.Minute, time.Hour, 25 * time.Second},
		{"a day in an hour, one simulated hour", time.Hour, time.Hour, 150 * time.Second},
		{"real time, one simulated hour", 24 * time.Hour, time.Hour, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(epoch, ScaleForDayIn(tc.dayIn), nil)
			got := c.Until(epoch.Add(tc.simSpan))
			// Now() moves as the test runs, so allow a small shortfall.
			if diff := tc.want - got; diff < 0 || diff > 50*time.Millisecond {
				t.Errorf("Until = %s, want about %s", got, tc.want)
			}
		})
	}

	// Neither of these reaches the deadline by waiting.
	if got := New(epoch, Paused, nil).Until(epoch.Add(time.Hour)); got != 0 {
		t.Errorf("paused Until = %s, want 0", got)
	}
	if got := New(epoch, Unbounded, nil).Until(epoch.Add(time.Hour)); got != 0 {
		t.Errorf("unbounded Until = %s, want 0", got)
	}
}

func TestClockString(t *testing.T) {
	for _, tc := range []struct {
		scale float64
		want  string
	}{
		{Paused, "paused"},
		{Unbounded, "unbounded"},
		{RealTime, "real time"},
		{ScaleForDayIn(time.Minute), "1440x (a day in 1m0s)"},
	} {
		if got := New(epoch, tc.scale, nil).String(); got != tc.want {
			t.Errorf("String() = %q, want %q", got, tc.want)
		}
	}
}
