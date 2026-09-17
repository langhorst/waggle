package simtime

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Task is work to run at a simulated instant. It receives the time it was
// scheduled for, not a reading of the clock: a task that stamps a message
// must use this value, so the same seed yields the same timestamps whatever
// rate the run used. Tasks may schedule further tasks, which is how the
// model advances -- an admission schedules its own discharge.
type Task func(at time.Time)

// Scheduler runs tasks in simulated-time order.
//
// Ordering is total and reproducible: tasks at the same simulated instant
// run in the order they were scheduled, so a run never depends on map
// iteration or goroutine timing. Tasks run one at a time on Run's
// goroutine, which lets the model be written without locks.
type Scheduler struct {
	clock *Clock

	mu   sync.Mutex
	q    taskQueue
	seq  uint64
	wake chan struct{}
}

// NewScheduler returns a scheduler pacing against clock.
func NewScheduler(clock *Clock) *Scheduler {
	return &Scheduler{clock: clock, wake: make(chan struct{}, 1)}
}

// At schedules fn for simulated time t. A t in the past runs at the next
// opportunity rather than being dropped, keeping a late task in the stream
// instead of silently losing it.
func (s *Scheduler) At(t time.Time, fn Task) {
	s.mu.Lock()
	s.seq++
	heap.Push(&s.q, scheduled{at: t, seq: s.seq, fn: fn})
	s.mu.Unlock()
	// Wake Run in case this task lands before whatever it is waiting on.
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// After schedules fn for d of simulated time from now.
func (s *Scheduler) After(d time.Duration, fn Task) {
	s.At(s.clock.Now().Add(d), fn)
}

// Len reports how many tasks are pending.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q.Len()
}

// Next reports the simulated time of the earliest pending task, and whether
// there is one.
func (s *Scheduler) Next() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.q.Len() == 0 {
		return time.Time{}, false
	}
	return s.q[0].at, true
}

// Run executes tasks until the queue empties, until simulated time passes
// until (a zero until means no limit), or until ctx ends.
//
// An empty queue ends the run: the model is expected to keep itself fed by
// scheduling ahead, so running dry means the simulation is genuinely over.
func (s *Scheduler) Run(ctx context.Context, until time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.q.Len() == 0 {
			s.mu.Unlock()
			return nil
		}
		next := s.q[0]
		if !until.IsZero() && next.at.After(until) {
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()

		// Wait for the task's instant. A rate change or a newly scheduled
		// earlier task both interrupt the wait, so re-check the queue head
		// rather than assuming next is still the earliest.
		if err := s.sleepUntil(ctx, next.at); err != nil {
			return err
		}
		s.mu.Lock()
		if s.q.Len() == 0 || s.q[0].seq != next.seq {
			s.mu.Unlock()
			continue // something earlier arrived; start over
		}
		if s.clock.Now().Before(next.at) {
			s.mu.Unlock()
			continue // woken early by a rate change
		}
		task := heap.Pop(&s.q).(scheduled)
		s.mu.Unlock()

		// The task is handed its scheduled instant, never clock.Now(): that
		// is what keeps generated content independent of the rate.
		task.fn(task.at)
	}
}

// sleepUntil waits for t, returning early if a task is scheduled meanwhile.
func (s *Scheduler) sleepUntil(ctx context.Context, t time.Time) error {
	done := make(chan error, 1)
	sleepCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { done <- s.clock.SleepUntil(sleepCtx, t) }()
	select {
	case err := <-done:
		if err != nil && ctx.Err() == nil {
			return nil // our own cancel; the caller re-checks the queue
		}
		return err
	case <-s.wake:
		cancel()
		<-done
		return ctx.Err()
	}
}

// scheduled is one queued task. seq breaks ties so that equal instants keep
// insertion order.
type scheduled struct {
	at  time.Time
	seq uint64
	fn  Task
}

type taskQueue []scheduled

func (q taskQueue) Len() int { return len(q) }
func (q taskQueue) Less(i, j int) bool {
	if q[i].at.Equal(q[j].at) {
		return q[i].seq < q[j].seq
	}
	return q[i].at.Before(q[j].at)
}
func (q taskQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *taskQueue) Push(x any)   { *q = append(*q, x.(scheduled)) }
func (q *taskQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = scheduled{}
	*q = old[:n-1]
	return item
}
