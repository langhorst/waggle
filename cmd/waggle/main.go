// Command waggle is the self-contained integration engine daemon. It hosts
// multiple channels defined by YAML files and exposes the HTTP API and web
// UI; `waggle tui` attaches the terminal observer instead.
//
// Usage:
//
//	waggle [daemon] [-config daemon.yaml]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/langhorst/waggle/internal/api"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"
	"github.com/langhorst/waggle/internal/tui"

	tea "github.com/charmbracelet/bubbletea"

	// Register the built-in adapters and format modules.
	_ "github.com/langhorst/waggle/internal/adapter/astm1381"
	_ "github.com/langhorst/waggle/internal/adapter/file"
	_ "github.com/langhorst/waggle/internal/adapter/httpin"
	_ "github.com/langhorst/waggle/internal/adapter/httpout"
	_ "github.com/langhorst/waggle/internal/adapter/mllp"
	_ "github.com/langhorst/waggle/internal/format/astm"
	_ "github.com/langhorst/waggle/internal/format/csvfmt"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
	_ "github.com/langhorst/waggle/internal/format/jsonfmt"
	_ "github.com/langhorst/waggle/internal/format/xmlfmt"
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
	case "tui":
		os.Exit(runTUI(args))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (expected: daemon, tui)\n", cmd)
		os.Exit(2)
	}
}

// runTUI starts the engine in-process from the same config and attaches the
// read-only observer TUI. Logs go to a file so they don't tear the screen.
func runTUI(args []string) int {
	fs := flag.NewFlagSet("tui", flag.ExitOnError)
	configPath := fs.String("config", "daemon.yaml", "path to daemon config")
	_ = fs.Parse(args)

	cfg, err := config.LoadDaemon(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading daemon config: %v\n", err)
		return 1
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "creating data dir: %v\n", err)
		return 1
	}
	logFile, err := os.OpenFile(filepath.Join(cfg.DataDir, "tui.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening log file: %v\n", err)
		return 1
	}
	defer logFile.Close()
	log := slog.New(slog.NewTextHandler(logFile, nil))
	slog.SetDefault(log)

	channels, err := config.LoadChannels(cfg.ChannelsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading channels: %v\n", err)
		return 1
	}
	st, err := store.Open(filepath.Join(cfg.DataDir, "messages.db"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening message store: %v\n", err)
		return 1
	}
	defer st.Close()
	scripts := script.New(script.Options{HotReload: cfg.HotReload, Log: log})
	defer scripts.Close()

	eng := engine.New(engine.Options{Log: log, Store: st, Scripts: scripts})
	for _, ch := range channels {
		if err := eng.LoadChannel(ch); err != nil {
			fmt.Fprintf(os.Stderr, "loading channel %s: %v\n", ch.ID, err)
			return 1
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eng.StartEnabled(ctx)
	defer eng.Shutdown()

	p := tea.NewProgram(tui.New(tui.EngineBackend{Eng: eng}), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "tui: %v\n", err)
		return 1
	}
	return 0
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

	apiServer := &api.Server{
		Eng:         eng,
		Scripts:     scripts,
		ScriptsRoot: cfg.ChannelsDir,
		Auth:        cfg.Auth,
		Log:         log,
	}
	if cfg.Auth.Disabled {
		log.Warn("http api authentication is disabled", "addr", cfg.Listen)
	}
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Info("http api listening", "addr", cfg.Listen)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "error", err)
			stop()
		}
	}()
	log.Info("daemon running", "channels", len(channels))

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	eng.Shutdown()
	return 0
}
