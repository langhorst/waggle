// Command waggle-sim simulates the systems a hospital network runs and feeds
// their traffic to a waggle daemon.
//
// It models one world -- patients, encounters, beds -- and renders feeds from
// it, so the messages are coherent with each other rather than merely
// well-formed. ADT is the feed that exists today; orders and results are
// renderers over the same world.
//
// Usage:
//
//	waggle-sim -mllp 127.0.0.1:2575 -day-in 10m
//	waggle-sim -dir ./in -days 3 -fast
//	waggle-sim -corpus ./corpus -days 30 -fast
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/langhorst/waggle/internal/sim"
	"github.com/langhorst/waggle/internal/sim/adt"
	"github.com/langhorst/waggle/internal/sim/simtime"
	"github.com/langhorst/waggle/internal/sim/sink"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("waggle-sim", flag.ExitOnError)
	var (
		mllpAddr = fs.String("mllp", "", "send ADT over MLLP to this host:port")
		dirPath  = fs.String("dir", "", "write each message as a file in this directory")
		corpus   = fs.String("corpus", "", "write a replayable corpus to this directory")
		dayIn    = fs.Duration("day-in", 10*time.Minute, "wall-clock time one simulated day should take")
		fast     = fs.Bool("fast", false, "run with no pacing at all (overrides -day-in)")
		realTime = fs.Bool("real-time", false, "run at wall-clock speed (overrides -day-in)")
		days     = fs.Float64("days", 0, "stop after this many simulated days (0 runs until interrupted)")
		seed     = fs.Uint64("seed", 1, "seed; the same seed reproduces the same run exactly")
		arrivals = fs.Float64("arrivals", 0, "mean arrivals per simulated day (0 uses the default)")
		startAt  = fs.String("start", "", "simulated start instant, RFC3339 (default: a fixed date, for reproducibility)")
		ackTmo   = fs.Duration("ack-timeout", 30*time.Second, "how long to wait for an MLLP ACK")
		tolerate = fs.Bool("tolerate-rejects", false, "keep running when the receiver rejects a message")
		quiet    = fs.Bool("quiet", false, "only report the run summary")
	)
	_ = fs.Parse(args)

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if *quiet {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	cfg := sim.DefaultConfig()
	cfg.Seed = *seed
	if *arrivals > 0 {
		cfg.ArrivalsPerDay = *arrivals
	}
	if *startAt != "" {
		t, err := time.Parse(time.RFC3339, *startAt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parsing -start: %v\n", err)
			return 2
		}
		cfg.StartAt = t
	}

	// Assemble the sinks. Several may be given at once: a live MLLP link and
	// a corpus of the same run is a useful combination.
	var sinks sink.Multi
	if *mllpAddr != "" {
		s, err := sink.NewMLLP(*mllpAddr, *ackTmo)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mllp sink: %v\n", err)
			return 2
		}
		s.Tolerate = *tolerate
		s.OnReject = func(m sim.Message, err error) {
			log.Warn("receiver rejected message", "control_id", m.ControlID, "trigger", m.Trigger, "error", err)
		}
		sinks = append(sinks, s)
	}
	if *dirPath != "" {
		sinks = append(sinks, &sink.Dir{Path: *dirPath})
	}
	if *corpus != "" {
		sinks = append(sinks, &sink.Corpus{Path: *corpus})
	}
	if len(sinks) == 0 {
		fmt.Fprintln(os.Stderr, "nothing to do: give -mllp, -dir or -corpus (-h for help)")
		return 2
	}

	scale := simtime.ScaleForDayIn(*dayIn)
	switch {
	case *fast:
		scale = simtime.Unbounded
	case *realTime:
		scale = simtime.RealTime
	}

	clock := simtime.New(cfg.StartAt, scale, nil)
	sched := simtime.NewScheduler(clock)
	runner := sim.NewRunner(log)

	adtCfg := adt.DefaultConfig()
	runner.Wire(sim.Wiring{
		Feed: adt.New(adtCfg, &sim.ControlIDs{Prefix: "SIM"}),
		Sink: sinks,
	})

	s, err := sim.NewSimulation(cfg, clock, sched, runner)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}

	var until time.Time
	if *days > 0 {
		until = cfg.StartAt.Add(time.Duration(*days * float64(24*time.Hour)))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	beds := 0
	for _, f := range s.Network().Facilities {
		beds += len(f.Beds())
	}
	log.Info("simulation starting",
		"facilities", len(s.Network().Facilities), "beds", beds,
		"clock", clock.String(), "seed", cfg.Seed, "from", cfg.StartAt.Format(time.RFC3339))

	started := time.Now()
	runErr := s.Run(ctx, until)
	if closeErr := runner.Close(); closeErr != nil && runErr == nil {
		runErr = closeErr
	}

	sent := runner.Sent()
	total := 0
	for _, n := range sent {
		total += n
	}
	simulated := clock.Now().Sub(cfg.StartAt)
	admitted, diverted := s.Stats()
	census := s.Network().Census()
	fmt.Fprintf(os.Stderr, "sent %d messages over %s of simulated time in %s of wall time\n",
		total, simulated.Round(time.Minute), time.Since(started).Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "census %d/%d beds (%.0f%%), admitted %d, diverted %d (%.1f%%)\n",
		census, beds, 100*float64(census)/float64(beds), admitted, diverted,
		100*float64(diverted)/math.Max(1, float64(admitted+diverted)))
	if admitted+diverted > 0 && float64(diverted)/float64(admitted+diverted) > 0.10 {
		fmt.Fprintln(os.Stderr, "warning: more than a tenth of arrivals were diverted; "+
			"the network is short of beds for this arrival rate")
	}

	if runErr != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "run failed: %v\n", runErr)
		return 1
	}
	return 0
}
