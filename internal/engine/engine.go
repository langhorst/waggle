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
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/queue"
	"github.com/langhorst/waggle/internal/store"
)

// ScriptEngine compiles referenced script files into pipeline steps
// (internal/script is the goja implementation). A nil ScriptEngine means
// script references are rejected at load time rather than silently skipped.
type ScriptEngine interface {
	CompileFilter(path string) (channel.FilterFunc, error)
	CompileTranslator(path string) (channel.TranslateFunc, error)
	// CompileErrors reports the last compile error per script path ("" when
	// healthy) for the UIs.
	CompileErrors() map[string]string
}

// Options configures a new Engine.
type Options struct {
	Bus *events.Bus
	Log *slog.Logger
	// Store enables persistence and Guaranteed Delivery: it becomes the
	// pipeline Recorder and Queuer, and per-destination delivery workers run
	// alongside each channel. Without it the engine falls back to Recorder
	// (or an in-memory recorder) with synchronous delivery.
	Store    *store.Store
	Recorder channel.Recorder
	Scripts  ScriptEngine
}

// Engine owns the set of configured channels.
type Engine struct {
	bus      *events.Bus
	log      *slog.Logger
	store    *store.Store
	recorder channel.Recorder
	scripts  ScriptEngine

	mu       sync.Mutex
	channels map[string]*managed
	order    []string
}

type managed struct {
	cfg     *config.Channel
	ch      *channel.Channel
	workers []*queue.Worker

	workersCancel context.CancelFunc
	workersDone   *sync.WaitGroup
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
	if opts.Store != nil {
		opts.Recorder = opts.Store
	} else if opts.Recorder == nil {
		opts.Recorder = channel.NewMemoryRecorder()
	}
	return &Engine{
		bus:      opts.Bus,
		log:      opts.Log,
		store:    opts.Store,
		recorder: opts.Recorder,
		scripts:  opts.Scripts,
		channels: map[string]*managed{},
	}
}

// Store returns the engine's store (nil when running without persistence).
func (e *Engine) Store() *store.Store { return e.store }

// Bus returns the engine's event bus.
func (e *Engine) Bus() *events.Bus { return e.bus }

// LoadChannel builds a channel from config and registers it (initially
// stopped). Replacing an existing channel requires it to be stopped first.
func (e *Engine) LoadChannel(cfg *config.Channel) error {
	ch, err := e.build(cfg)
	if err != nil {
		return err
	}
	m := &managed{cfg: cfg, ch: ch, workers: e.buildWorkers(cfg, ch)}
	if e.store != nil {
		e.store.SetRetention(cfg.ID, cfg.RetentionCount())
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.channels[cfg.ID]; ok {
		if existing.ch.Status() != channel.StatusStopped {
			return fmt.Errorf("engine: channel %s is %s; stop it before reloading", cfg.ID, existing.ch.Status())
		}
		e.channels[cfg.ID] = m
		return nil
	}
	e.channels[cfg.ID] = m
	e.order = append(e.order, cfg.ID)
	return nil
}

// buildWorkers creates one Guaranteed Delivery worker per queueing
// destination (waitForAck destinations deliver synchronously and get none).
func (e *Engine) buildWorkers(cfg *config.Channel, ch *channel.Channel) []*queue.Worker {
	if e.store == nil {
		return nil
	}
	var workers []*queue.Worker
	for i := range cfg.Destinations {
		dcfg := &cfg.Destinations[i]
		if dcfg.WaitForAck {
			continue
		}
		workers = append(workers, &queue.Worker{
			Store:        e.store,
			Adapter:      ch.Destinations[i].Adapter,
			ChannelID:    cfg.ID,
			DestID:       dcfg.ID,
			MaxAttempts:  dcfg.Queue.MaxAttemptCount(),
			BaseInterval: time.Duration(dcfg.Queue.RetryInterval),
			Bus:          e.bus,
			Log:          e.log,
		})
	}
	return workers
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
		ID:         cfg.ID,
		Name:       cfg.Name,
		InType:     inType,
		Source:     source,
		Recorder:   e.recorder,
		Bus:        e.bus,
		Log:        e.log,
		MaxPending: cfg.MaxPending,
	}
	if e.store != nil {
		ch.Queue = e.store
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

func (e *Engine) managed(id string) (*managed, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownChannel, id)
	}
	return m, nil
}

// Start starts (or resumes) a channel and its delivery workers.
func (e *Engine) Start(ctx context.Context, id string) error {
	m, err := e.managed(id)
	if err != nil {
		return err
	}
	if m.ch.Status() == channel.StatusPaused {
		return m.ch.Resume(ctx)
	}
	if err := m.ch.Start(ctx); err != nil {
		return err
	}
	e.startWorkers(ctx, m)
	return nil
}

// startWorkers launches a channel's Guaranteed Delivery workers (idempotent
// across pause/resume — they run from Start until Stop).
func (e *Engine) startWorkers(ctx context.Context, m *managed) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if m.workersCancel != nil || len(m.workers) == 0 {
		return
	}
	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	wg := &sync.WaitGroup{}
	m.workersCancel = cancel
	m.workersDone = wg
	for _, w := range m.workers {
		wg.Add(1)
		go func(w *queue.Worker) {
			defer wg.Done()
			w.Run(wctx)
		}(w)
	}
}

// Stop stops a channel: workers first (so no send races a closing adapter),
// then intake and adapters.
func (e *Engine) Stop(id string) error {
	m, err := e.managed(id)
	if err != nil {
		return err
	}
	e.stopWorkers(m)
	return m.ch.Stop()
}

func (e *Engine) stopWorkers(m *managed) {
	e.mu.Lock()
	cancel, done := m.workersCancel, m.workersDone
	m.workersCancel, m.workersDone = nil, nil
	e.mu.Unlock()
	if cancel != nil {
		cancel()
		done.Wait()
	}
}

// Pause pauses a channel's intake; queued deliveries keep draining.
func (e *Engine) Pause(id string) error {
	m, err := e.managed(id)
	if err != nil {
		return err
	}
	return m.ch.Pause()
}

// ReloadChannel re-reads a channel's YAML file (and recompiles its
// scripts), rebuilds the channel, and restores its previous run state.
func (e *Engine) ReloadChannel(ctx context.Context, id string) error {
	m, err := e.managed(id)
	if err != nil {
		return err
	}
	if m.cfg.Path == "" {
		return fmt.Errorf("engine: channel %s was not loaded from a file", id)
	}
	newCfg, err := config.LoadChannel(m.cfg.Path)
	if err != nil {
		return err
	}
	if newCfg.ID != id {
		return fmt.Errorf("engine: %s now defines channel %q; delete and re-add instead of reloading", m.cfg.Path, newCfg.ID)
	}
	prev := m.ch.Status()
	if err := e.Stop(id); err != nil {
		return err
	}
	if err := e.LoadChannel(newCfg); err != nil {
		return fmt.Errorf("engine: reloading %s: %w (channel left stopped)", id, err)
	}
	switch prev {
	case channel.StatusStarted:
		return e.Start(ctx, id)
	case channel.StatusPaused:
		if err := e.Start(ctx, id); err != nil {
			return err
		}
		return e.Pause(id)
	}
	return nil
}

// Replay re-processes a stored message. With destID empty the original raw
// bytes re-enter the pipeline as a new message (re-filter, re-transform,
// re-queue) sharing the original's correlation ID. With destID set, the
// already-transformed stored payload is requeued to that one destination
// without re-running the pipeline. Returns the new message ID (0 for
// destination requeue).
func (e *Engine) Replay(ctx context.Context, messageID int64, destID string) (int64, error) {
	if e.store == nil {
		return 0, fmt.Errorf("engine: replay requires persistence")
	}
	detail, err := e.store.GetMessage(ctx, messageID)
	if err != nil {
		return 0, err
	}
	m, err := e.managed(detail.ChannelID)
	if err != nil {
		return 0, fmt.Errorf("engine: message %d belongs to unloaded channel %q", messageID, detail.ChannelID)
	}
	if destID != "" {
		return 0, e.store.Requeue(ctx, messageID, destID)
	}
	replay, err := e.store.NewReplay(ctx, messageID)
	if err != nil {
		return 0, err
	}
	return m.ch.Inject(ctx, replay)
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
			continue
		}
		e.startWorkers(ctx, m)
		e.log.Info("channel started", "channel", m.cfg.ID)
	}
	return errs
}

// Shutdown stops all channels and their workers.
func (e *Engine) Shutdown() {
	e.mu.Lock()
	var all []*managed
	for _, id := range e.order {
		all = append(all, e.channels[id])
	}
	e.mu.Unlock()
	for _, m := range all {
		e.stopWorkers(m)
		if err := m.ch.Stop(); err != nil {
			e.log.Error("channel failed to stop cleanly", "channel", m.cfg.ID, "error", err)
		}
	}
}
