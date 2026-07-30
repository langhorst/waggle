// Package engine is the composition root: it turns channel configurations
// into running channels, wiring adapters from the registry, format modules,
// the recorder, the event bus, and (once available) the script engine. Both
// the HTTP API and the embedded TUI operate on an Engine.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/langhorst/integration-channel/internal/adapter"
	"github.com/langhorst/integration-channel/internal/channel"
	"github.com/langhorst/integration-channel/internal/config"
	"github.com/langhorst/integration-channel/internal/events"
	"github.com/langhorst/integration-channel/internal/format"
)

// ScriptEngine compiles referenced script files into pipeline steps. The
// goja implementation arrives in phase 4; a nil ScriptEngine means script
// references are rejected at load time rather than silently skipped.
type ScriptEngine interface {
	CompileFilter(path string) (channel.FilterFunc, error)
	CompileTranslator(path string) (channel.TranslateFunc, error)
}

// Options configures a new Engine.
type Options struct {
	Bus      *events.Bus
	Log      *slog.Logger
	Recorder channel.Recorder
	Scripts  ScriptEngine
}

// Engine owns the set of configured channels.
type Engine struct {
	bus      *events.Bus
	log      *slog.Logger
	recorder channel.Recorder
	scripts  ScriptEngine

	mu       sync.Mutex
	channels map[string]*managed
	order    []string
}

type managed struct {
	cfg *config.Channel
	ch  *channel.Channel
}

// Info is a channel summary for UIs.
type Info struct {
	ID     string         `json:"id"`
	Name   string         `json:"name"`
	Status channel.Status `json:"status"`
}

func New(opts Options) *Engine {
	if opts.Bus == nil {
		opts.Bus = events.NewBus()
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Recorder == nil {
		opts.Recorder = channel.NewMemoryRecorder()
	}
	return &Engine{
		bus:      opts.Bus,
		log:      opts.Log,
		recorder: opts.Recorder,
		scripts:  opts.Scripts,
		channels: map[string]*managed{},
	}
}

// Bus returns the engine's event bus.
func (e *Engine) Bus() *events.Bus { return e.bus }

// LoadChannel builds a channel from config and registers it (initially
// stopped). Replacing an existing channel requires it to be stopped first.
func (e *Engine) LoadChannel(cfg *config.Channel) error {
	ch, err := e.build(cfg)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.channels[cfg.ID]; ok {
		if existing.ch.Status() != channel.StatusStopped {
			return fmt.Errorf("engine: channel %s is %s; stop it before reloading", cfg.ID, existing.ch.Status())
		}
		e.channels[cfg.ID] = &managed{cfg: cfg, ch: ch}
		return nil
	}
	e.channels[cfg.ID] = &managed{cfg: cfg, ch: ch}
	e.order = append(e.order, cfg.ID)
	return nil
}

func (e *Engine) build(cfg *config.Channel) (*channel.Channel, error) {
	inType, ok := format.Get(cfg.Source.DataType)
	if !ok {
		return nil, fmt.Errorf("engine: channel %s: unknown dataType %q", cfg.ID, cfg.Source.DataType)
	}
	source, err := adapter.NewInbound(cfg.Source.Type, cfg.Source.Settings)
	if err != nil {
		return nil, fmt.Errorf("engine: channel %s: source: %w", cfg.ID, err)
	}

	ch := &channel.Channel{
		ID:       cfg.ID,
		Name:     cfg.Name,
		InType:   inType,
		Source:   source,
		Recorder: e.recorder,
		Bus:      e.bus,
		Log:      e.log,
	}

	if ch.Filter, err = e.compileFilter(cfg, cfg.Filter); err != nil {
		return nil, fmt.Errorf("engine: channel %s: %w", cfg.ID, err)
	}
	if ch.Translate, err = e.compileTranslators(cfg, cfg.Transformers); err != nil {
		return nil, fmt.Errorf("engine: channel %s: %w", cfg.ID, err)
	}

	for i := range cfg.Destinations {
		dcfg := &cfg.Destinations[i]
		outType, ok := format.Get(dcfg.DataType)
		if !ok {
			return nil, fmt.Errorf("engine: channel %s: destination %s: unknown dataType %q", cfg.ID, dcfg.ID, dcfg.DataType)
		}
		out, err := adapter.NewOutbound(dcfg.Adapter.Type, dcfg.Adapter.Settings)
		if err != nil {
			return nil, fmt.Errorf("engine: channel %s: destination %s: %w", cfg.ID, dcfg.ID, err)
		}
		dest := &channel.Destination{
			ID:         dcfg.ID,
			OutType:    outType,
			Adapter:    out,
			WaitForAck: dcfg.WaitForAck,
		}
		if dest.Filter, err = e.compileFilter(cfg, dcfg.Filter); err != nil {
			return nil, fmt.Errorf("engine: channel %s: destination %s: %w", cfg.ID, dcfg.ID, err)
		}
		if dest.Translate, err = e.compileTranslators(cfg, dcfg.Transformers); err != nil {
			return nil, fmt.Errorf("engine: channel %s: destination %s: %w", cfg.ID, dcfg.ID, err)
		}
		ch.Destinations = append(ch.Destinations, dest)
	}
	return ch, nil
}

func (e *Engine) compileFilter(cfg *config.Channel, path string) (channel.FilterFunc, error) {
	if path == "" {
		return nil, nil
	}
	if e.scripts == nil {
		return nil, fmt.Errorf("filter %q configured but no script engine is available", path)
	}
	f, err := e.scripts.CompileFilter(cfg.ResolvePath(path))
	if err != nil {
		return nil, fmt.Errorf("filter %q: %w", path, err)
	}
	return f, nil
}

func (e *Engine) compileTranslators(cfg *config.Channel, paths []string) ([]channel.TranslateFunc, error) {
	var chain []channel.TranslateFunc
	for _, p := range paths {
		if e.scripts == nil {
			return nil, fmt.Errorf("transformer %q configured but no script engine is available", p)
		}
		t, err := e.scripts.CompileTranslator(cfg.ResolvePath(p))
		if err != nil {
			return nil, fmt.Errorf("transformer %q: %w", p, err)
		}
		chain = append(chain, t)
	}
	return chain, nil
}

// Channels lists all channels in configuration order.
func (e *Engine) Channels() []Info {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Info, 0, len(e.order))
	for _, id := range e.order {
		m := e.channels[id]
		out = append(out, Info{ID: m.cfg.ID, Name: m.cfg.Name, Status: m.ch.Status()})
	}
	return out
}

// Channel returns the runtime channel by ID.
func (e *Engine) Channel(id string) (*channel.Channel, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.channels[id]
	if !ok {
		return nil, false
	}
	return m.ch, true
}

// Config returns the configuration a channel was built from.
func (e *Engine) Config(id string) (*config.Channel, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.channels[id]
	if !ok {
		return nil, false
	}
	return m.cfg, true
}

// Start starts (or resumes) a channel.
func (e *Engine) Start(ctx context.Context, id string) error {
	ch, ok := e.Channel(id)
	if !ok {
		return fmt.Errorf("engine: unknown channel %q", id)
	}
	if ch.Status() == channel.StatusPaused {
		return ch.Resume(ctx)
	}
	return ch.Start(ctx)
}

// Stop stops a channel.
func (e *Engine) Stop(id string) error {
	ch, ok := e.Channel(id)
	if !ok {
		return fmt.Errorf("engine: unknown channel %q", id)
	}
	return ch.Stop()
}

// Pause pauses a channel's intake.
func (e *Engine) Pause(id string) error {
	ch, ok := e.Channel(id)
	if !ok {
		return fmt.Errorf("engine: unknown channel %q", id)
	}
	return ch.Pause()
}

// StartEnabled starts every enabled channel, collecting errors rather than
// aborting: one broken channel must not keep the rest down.
func (e *Engine) StartEnabled(ctx context.Context) []error {
	e.mu.Lock()
	var toStart []*managed
	for _, id := range e.order {
		if m := e.channels[id]; m.cfg.IsEnabled() {
			toStart = append(toStart, m)
		}
	}
	e.mu.Unlock()

	var errs []error
	for _, m := range toStart {
		if err := m.ch.Start(ctx); err != nil {
			e.log.Error("channel failed to start", "channel", m.cfg.ID, "error", err)
			errs = append(errs, err)
		} else {
			e.log.Info("channel started", "channel", m.cfg.ID)
		}
	}
	return errs
}

// Shutdown stops all channels.
func (e *Engine) Shutdown() {
	e.mu.Lock()
	var all []*managed
	for _, id := range e.order {
		all = append(all, e.channels[id])
	}
	e.mu.Unlock()
	for _, m := range all {
		if err := m.ch.Stop(); err != nil {
			e.log.Error("channel failed to stop cleanly", "channel", m.cfg.ID, "error", err)
		}
	}
}
