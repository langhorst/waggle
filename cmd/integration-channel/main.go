// Command integration-channel is the self-contained integration engine
// daemon. It hosts multiple channels defined by YAML files, exposes the
// HTTP API and web UI (phase 6/7), and can alternatively run with the
// embedded TUI observer attached (phase 5).
//
// Usage:
//
//	integration-channel [daemon] [-config daemon.yaml]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/langhorst/integration-channel/internal/config"
	"github.com/langhorst/integration-channel/internal/engine"
	"github.com/langhorst/integration-channel/internal/script"
	"github.com/langhorst/integration-channel/internal/store"

	// Register the built-in adapters and format modules.
	_ "github.com/langhorst/integration-channel/internal/adapter/file"
	_ "github.com/langhorst/integration-channel/internal/adapter/mllp"
	_ "github.com/langhorst/integration-channel/internal/format/astm"
	_ "github.com/langhorst/integration-channel/internal/format/csvfmt"
	_ "github.com/langhorst/integration-channel/internal/format/hl7v2"
)

func main() {
	args := os.Args[1:]
	cmd := "daemon"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "daemon":
		os.Exit(runDaemon(args))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (expected: daemon)\n", cmd)
		os.Exit(2)
	}
}

func runDaemon(args []string) int {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	configPath := fs.String("config", "daemon.yaml", "path to daemon config")
	_ = fs.Parse(args)

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	cfg, err := config.LoadDaemon(*configPath)
	if err != nil {
		log.Error("loading daemon config", "error", err)
		return 1
	}
	channels, err := config.LoadChannels(cfg.ChannelsDir)
	if err != nil {
		log.Error("loading channels", "error", err)
		return 1
	}
	if len(channels) == 0 {
		log.Warn("no channels configured", "dir", cfg.ChannelsDir)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		log.Error("creating data dir", "error", err)
		return 1
	}
	st, err := store.Open(filepath.Join(cfg.DataDir, "messages.db"))
	if err != nil {
		log.Error("opening message store", "error", err)
		return 1
	}
	defer st.Close()

	scripts := script.New(script.Options{HotReload: cfg.HotReload, Log: log})
	defer scripts.Close()

	eng := engine.New(engine.Options{Log: log, Store: st, Scripts: scripts})
	for _, ch := range channels {
		if err := eng.LoadChannel(ch); err != nil {
			log.Error("loading channel", "channel", ch.ID, "error", err)
			return 1
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng.StartEnabled(ctx)
	log.Info("daemon running", "channels", len(channels))

	<-ctx.Done()
	log.Info("shutting down")
	eng.Shutdown()
	return 0
}
