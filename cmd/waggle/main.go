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
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
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

// runTUI attaches the read-only observer TUI to a running daemon over its
// HTTP API and event stream. Credentials default to the daemon config, so
// `waggle tui` next to `waggle daemon` just works for both a token and a
// basicUser/basicPassword login; flags and the environment override it.
func runTUI(args []string) int {
	fs := flag.NewFlagSet("tui", flag.ExitOnError)
	configPath := fs.String("config", "daemon.yaml", "path to daemon config (for the listen address and credentials)")
	addr := fs.String("addr", "", "daemon API address, e.g. 127.0.0.1:8420 (default: the config's listen address)")
	token := fs.String("token", "", "API token (default: auth.token from the config, or $WAGGLE_TOKEN)")
	user := fs.String("user", "", "basic-auth user (default: auth.basicUser from the config, or $WAGGLE_USER)")
	password := fs.String("password", "", "basic-auth password (default: auth.basicPassword from the config, or $WAGGLE_PASSWORD)")
	_ = fs.Parse(args)

	// A config is a convenience here, not a requirement: the TUI needs only
	// an address and credentials, and may well be pointed at a daemon whose
	// config lives on another host. So a missing default config falls back
	// to the built-in defaults, while an explicit -config that is not there
	// is a typo worth reporting rather than silently ignoring.
	explicitConfig := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			explicitConfig = true
		}
	})
	cfg := config.DefaultDaemon()
	if _, statErr := os.Stat(*configPath); statErr != nil {
		if explicitConfig {
			fmt.Fprintf(os.Stderr, "reading daemon config: %v\n", statErr)
			return 1
		}
	} else {
		loaded, err := config.LoadDaemon(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "loading daemon config: %v\n", err)
			return 1
		}
		cfg = loaded
	}

	if *addr == "" {
		*addr = dialableAddr(cfg.Listen)
	}
	if *token == "" {
		*token = firstNonEmpty(cfg.Auth.Token, os.Getenv("WAGGLE_TOKEN"))
	}
	if *user == "" {
		*user = firstNonEmpty(cfg.Auth.BasicUser, os.Getenv("WAGGLE_USER"))
	}
	if *password == "" {
		*password = firstNonEmpty(cfg.Auth.BasicPassword, os.Getenv("WAGGLE_PASSWORD"))
	}

	backend := &tui.HTTPBackend{
		BaseURL:       "http://" + *addr,
		Token:         *token,
		BasicUser:     *user,
		BasicPassword: *password,
	}
	// Fail fast with a readable message rather than an empty screen.
	if _, err := backend.ChannelSummaries(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "connecting to the daemon at %s: %v\n", *addr, err)
		if errors.Is(err, tui.ErrUnauthorized) {
			fmt.Fprint(os.Stderr, credentialHelp(*token != "", *user != ""))
		}
		return 1
	}
	p := tea.NewProgram(tui.New(backend), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "tui: %v\n", err)
		return 1
	}
	return 0
}

// credentialHelp explains which credentials the TUI sent and how to supply
// the right ones, since a bare 401 does not say whether the problem is a
// missing token, a missing login, or a wrong password.
func credentialHelp(sentToken, sentUser bool) string {
	switch {
	case sentToken:
		return "\nThe token was rejected. Check auth.token in the daemon config, -token, or $WAGGLE_TOKEN.\n"
	case sentUser:
		return "\nThe basic-auth login was rejected. Check auth.basicUser and auth.basicPassword in the\ndaemon config, -user and -password, or $WAGGLE_USER and $WAGGLE_PASSWORD.\n"
	default:
		return "\nNo credentials were sent. Point -config at the daemon config, or pass -token\n(for auth.token) or -user and -password (for auth.basicUser/auth.basicPassword).\n"
	}
}

// firstNonEmpty returns the first non-empty argument, or "".
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// dialableAddr turns a listen address into one a client can connect to: a
// bare port or wildcard host means loopback.
func dialableAddr(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func runDaemon(args []string) int {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	configPath := fs.String("config", "daemon.yaml", "path to daemon config")
	logLevel := fs.String("log-level", "", "debug, info, warn or error (overrides the config's logLevel)")
	_ = fs.Parse(args)

	// Until the config is read there is nowhere to put a level, so config
	// failures are reported at the default and the real logger replaces
	// this one as soon as there is something to configure it from.
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	cfg, err := config.LoadDaemon(*configPath)
	if err != nil {
		log.Error("loading daemon config", "error", err)
		return 1
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	level, err := cfg.Level()
	if err != nil {
		log.Error("log level", "error", err)
		return 1
	}
	log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
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

	if errs := eng.StartEnabled(ctx); len(errs) > 0 {
		// Each failure was logged with its channel; the daemon keeps
		// running so the rest of the channels serve and the operator can
		// fix and reload the broken ones from the UI.
		log.Warn("some channels failed to start", "failed", len(errs), "total", len(channels))
	}

	apiServer := &api.Server{
		Eng:         eng,
		Scripts:     scripts,
		ScriptsRoot: cfg.ChannelsDir,
		Auth:        cfg.Auth,
		Log:         log,
	}
	if cfg.Auth.Disabled {
		if cfg.ServesLoopbackOnly() {
			log.Warn("http api authentication is disabled", "addr", cfg.Listen)
		} else {
			// Reachable from the network with no credentials: legitimate
			// only behind something that authenticates for us.
			log.Warn("http api authentication is disabled on a non-loopback address; "+
				"anyone who can reach this port can control every channel",
				"addr", cfg.Listen)
		}
	}
	// No WriteTimeout: the SSE endpoints stream indefinitely, and the API
	// applies a per-response deadline to everything else.
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
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
