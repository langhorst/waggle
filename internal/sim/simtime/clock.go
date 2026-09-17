// Package simtime is the simulation's notion of time: a clock whose rate
// relative to the wall clock is adjustable at runtime, and a scheduler that
// runs work in simulated-time order.
//
// The rate is a pacing control and nothing more. What the simulation
// decides -- which patient is admitted, in what order, carrying which
// simulated timestamp -- is computed in simulated time from a seeded
// source, so one seed produces the same messages whether the run takes a
// minute or an hour. Changing the rate changes only how long the wall clock
// waits between them. Scheduled work is handed the time it was scheduled
// for rather than a reading of the clock, so a message's timestamps do not
// drift with the speed it was generated at.
package simtime

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Rates are expressed as simulated seconds per wall-clock second, which
// composes better than named presets: a hospital-day in a minute and one in
// ten minutes are the same knob at two settings.
const (
	// RealTime advances simulated time at wall-clock speed.
	RealTime = 1.0
	// Unbounded runs with no waiting at all: simulated time jumps straight
	// to each scheduled event. This is the mode for corpus generation,
	// benchmarks and tests, where pacing is only a delay.
	Unbounded = math.MaxFloat64
	// Paused freezes simulated time. Sleepers wait for a rate change rather
	// than a deadline.
	Paused = 0.0
)

// ScaleForDayIn gives the rate that compresses 24 simulated hours into d of
// wall-clock time: ScaleForDayIn(time.Minute) is a hospital-day a minute,
// ScaleForDayIn(time.Hour) a day an hour.
func ScaleForDayIn(d time.Duration) float64 {
	if d <= 0 {
		return Unbounded
	}
	return float64(24*time.Hour) / float64(d)
}

// DayIn is the inverse: how long a simulated day takes at rate scale. It
// reports 0 for rates that do not wait.
func DayIn(scale float64) time.Duration {
	if !finite(scale) || scale <= 0 {
		return 0
	}
	return time.Duration(float64(24*time.Hour) / scale)
}

// Wall is the real-time source a Clock paces against. Production uses
// SystemWall; tests substitute their own.
//
// After's channel needs no cleanup: since Go 1.23 an unreferenced timer is
// collected without having to fire.
type Wall interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemWall is the real clock.
type SystemWall struct{}

func (SystemWall) Now() time.Time                         { return time.Now() }
func (SystemWall) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Clock maps wall-clock time onto simulated time at an adjustable rate.
//
// It is safe for concurrent use. Rate changes are continuous: simulated time
// never jumps backwards or skips forward across a change, because the clock
// re-anchors on the current simulated instant before adopting the new rate.
type Clock struct {
	wall Wall

	mu     sync.Mutex
	scale  float64
	simAt  time.Time // simulated time at wallAt
	wallAt time.Time
	// changed is closed and replaced on every rate change, waking sleepers
	// so they can recompute their deadline against the new rate.
	changed chan struct{}
}

// New starts a clock at simulated time start running at scale simulated
// seconds per wall second. A nil wall uses the real clock.
//
// scale may be RealTime, Unbounded, Paused or any positive value; NaN and
// negative rates are treated as Paused.
func New(start time.Time, scale float64, wall Wall) *Clock {
	if wall == nil {
		wall = SystemWall{}
	}
	return &Clock{
		wall:    wall,
		scale:   sanitize(scale),
		simAt:   start,
		wallAt:  wall.Now(),
		changed: make(chan struct{}),
	}
}

// sanitize maps unusable rates onto Paused, so a bad configuration stalls
// visibly rather than running time backwards.
func sanitize(scale float64) float64 {
	if math.IsNaN(scale) || scale < 0 {
		return Paused
	}
	if math.IsInf(scale, 1) {
		return Unbounded
	}
	return scale
}

func finite(scale float64) bool {
	return !math.IsInf(scale, 0) && !math.IsNaN(scale) && scale < Unbounded
}

// Now reports the current simulated time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nowLocked()
}

func (c *Clock) nowLocked() time.Time {
	// Paused and Unbounded clocks do not derive simulated time from elapsed
	// wall time: a paused one does not advance, and an unbounded one
	// advances only by jumping to the next scheduled event.
	if c.scale == Paused || !finite(c.scale) {
		return c.simAt
	}
	elapsed := c.wall.Now().Sub(c.wallAt)
	return c.simAt.Add(time.Duration(float64(elapsed) * c.scale))
}

// Scale reports the current rate.
func (c *Clock) Scale() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scale
}

// SetScale changes the rate, taking effect immediately for anyone already
// sleeping. Simulated time is continuous across the change.
func (c *Clock) SetScale(scale float64) {
	c.mu.Lock()
	// Re-anchor on the present instant before adopting the new rate, or the
	// elapsed wall time since the last anchor would be re-scaled and
	// simulated time would jump.
	c.simAt = c.nowLocked()
	c.wallAt = c.wall.Now()
	c.scale = sanitize(scale)
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
}

// Pause freezes simulated time; Resume restarts it at scale.
func (c *Clock) Pause()               { c.SetScale(Paused) }
func (c *Clock) Resume(scale float64) { c.SetScale(scale) }
func (c *Clock) Paused() bool         { return c.Scale() == Paused }

// Until reports how long the wall clock must wait for simulated time to
// reach t. It is zero when t has passed, and also zero for paused and
// unbounded clocks, neither of which reaches t by waiting: use SleepUntil,
// which blocks for a rate change or jumps outright.
func (c *Clock) Until(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.untilLocked(t)
}

func (c *Clock) untilLocked(t time.Time) time.Duration {
	remaining := t.Sub(c.nowLocked())
	if remaining <= 0 {
		return 0
	}
	if !finite(c.scale) {
		return 0 // unbounded: reached by jumping, no waiting
	}
	if c.scale == Paused {
		return 0 // never reached by waiting; SleepUntil blocks instead
	}
	return time.Duration(float64(remaining) / c.scale)
}

// SleepUntil blocks until simulated time reaches t, or ctx ends. A rate
// change while it waits is picked up immediately, so speeding up a run
// shortens sleeps already in progress rather than only future ones.
//
// On an unbounded clock it does not wait: simulated time jumps to t.
func (c *Clock) SleepUntil(ctx context.Context, t time.Time) error {
	for {
		c.mu.Lock()
		now := c.nowLocked()
		if !now.Before(t) {
			c.mu.Unlock()
			return nil
		}
		scale, changed := c.scale, c.changed
		if !finite(scale) {
			// Unbounded: advance simulated time to the deadline outright.
			c.simAt = t
			c.wallAt = c.wall.Now()
			c.mu.Unlock()
			return ctx.Err()
		}
		wait := c.untilLocked(t)
		c.mu.Unlock()

		if scale == Paused {
			// Nothing to wait for but a rate change.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
			continue
		}
		if wait <= 0 {
			// Rounded below the clock's resolution; treat as arrived rather
			// than spinning on a sub-nanosecond remainder.
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			// Recompute against the new rate.
		case <-c.wall.After(wait):
			return nil
		}
	}
}

// Sleep blocks for d of simulated time.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	return c.SleepUntil(ctx, c.Now().Add(d))
}

// String renders the rate the way an operator configured it.
func (c *Clock) String() string {
	scale := c.Scale()
	switch {
	case scale == Paused:
		return "paused"
	case !finite(scale):
		return "unbounded"
	case scale == RealTime:
		return "real time"
	default:
		return fmt.Sprintf("%gx (a day in %s)", scale, DayIn(scale).Round(time.Second))
	}
}
