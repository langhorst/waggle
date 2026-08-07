// Package script is the JavaScript engine for Message Filters and Message
// Translators, built on goja (pure Go). Each script file gets its own
// runtime — sandboxed by construction, since goja ships no filesystem or
// network built-ins and only the bindings below are installed:
//
//	function filter(msg) { return msg.get('MSH-9.1') === 'ADT'; }
//	function transform(msg) { msg.set('PID-5.1', 'SMITH'); }
//
// Script API:
//   - msg.get(path) / msg.set(path, value) / msg.getAll(path) — the owning
//     format's path dialect (e.g. "PID-5.1", "R[2].3", "name[0].given");
//     for typed formats (JSON) set preserves JS number/boolean/null types
//   - msg.segments(name) — handles with .get/.set relative paths and .name
//   - msg.raw, msg.dataType, msg.id, msg.channel
//   - newMessage(dataType) — fresh empty message for format conversion; a
//     transformer that returns it replaces the pipeline message's tree
//   - meta — source metadata; writable, and writes flow to outbound
//     adapters (meta['http.path'] routes the http-sender per message)
//   - logger.info/warn/error(...)
//   - response.reject(code, text) — stop processing, Invalid Message
//     Channel, ACK exactly code/text in destination-ACK mode
//   - response.setAck(code, text) — override the ACK without stopping
//
// A transformer may mutate msg in place, return nothing, or return a
// message created with newMessage. Runaway scripts are interrupted after a
// configurable timeout. Script-global state persists across messages on the
// same channel (and resets on hot reload).
package script

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dop251/goja"

	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/message"
)

// Options configures the script engine.
type Options struct {
	// Timeout interrupts one script execution. Default 5s.
	Timeout time.Duration
	// HotReload polls script files for changes and recompiles them in
	// place; a failed compile keeps the previous program running.
	HotReload bool
	// ReloadInterval is the mtime poll period. Default 2s.
	ReloadInterval time.Duration
	Log            *slog.Logger
}

// Engine compiles script files into pipeline steps. Implements the engine
// package's ScriptEngine seam.
type Engine struct {
	opts Options

	mu      sync.Mutex
	scripts map[string]*Script // keyed by path

	watchCancel chan struct{}
	watchDone   chan struct{}
}

// New creates a script engine. Call Close to stop the hot-reload watcher.
func New(opts Options) *Engine {
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.ReloadInterval <= 0 {
		opts.ReloadInterval = 2 * time.Second
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	e := &Engine{opts: opts, scripts: map[string]*Script{}}
	if opts.HotReload {
		e.watchCancel = make(chan struct{})
		e.watchDone = make(chan struct{})
		go e.watch()
	}
	return e
}

// Close stops the hot-reload watcher.
func (e *Engine) Close() {
	if e.watchCancel != nil {
		close(e.watchCancel)
		<-e.watchDone
	}
}

// CompileFilter loads path as a Message Filter (must define
// `function filter(msg)` returning truthy to keep the message).
func (e *Engine) CompileFilter(path string) (channel.FilterFunc, error) {
	s, err := e.load(path, "filter")
	if err != nil {
		return nil, err
	}
	return func(m *message.Message) (bool, error) {
		v, err := s.run(m, e.opts.Timeout)
		if err != nil {
			return false, err
		}
		return v != nil && v.ToBoolean(), nil
	}, nil
}

// CompileTranslator loads path as a Message Translator step (must define
// `function transform(msg)`).
func (e *Engine) CompileTranslator(path string) (channel.TranslateFunc, error) {
	s, err := e.load(path, "transform")
	if err != nil {
		return nil, err
	}
	return func(m *message.Message) error {
		_, err := s.run(m, e.opts.Timeout)
		return err
	}, nil
}

// load compiles (or returns the already-compiled) script at path exposing
// fnName. The same file can back both a filter and a transformer only with
// distinct function names, so scripts are cached per (path, fnName).
func (e *Engine) load(path, fnName string) (*Script, error) {
	key := path + "\x00" + fnName
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.scripts[key]; ok {
		return s, nil
	}
	s := &Script{path: path, fnName: fnName, log: e.opts.Log}
	if err := s.compile(); err != nil {
		return nil, err
	}
	e.scripts[key] = s
	return s, nil
}

// Scripts returns the load state of every compiled script (path → last
// compile error, empty when healthy).
func (e *Engine) Scripts() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string]string{}
	for _, s := range e.scripts {
		s.mu.Lock()
		out[s.path] = s.lastError
		s.mu.Unlock()
	}
	return out
}

// Reload recompiles one script file immediately (used by the API's script
// editor after a PUT). Returns the compile error, if any; the previous
// program keeps running on failure.
func (e *Engine) Reload(path string) error {
	e.mu.Lock()
	var targets []*Script
	for _, s := range e.scripts {
		if s.path == path {
			targets = append(targets, s)
		}
	}
	e.mu.Unlock()
	if len(targets) == 0 {
		return fmt.Errorf("script: %s is not loaded", path)
	}
	for _, s := range targets {
		if err := s.compile(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) watch() {
	defer close(e.watchDone)
	ticker := time.NewTicker(e.opts.ReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.watchCancel:
			return
		case <-ticker.C:
		}
		e.mu.Lock()
		scripts := make([]*Script, 0, len(e.scripts))
		for _, s := range e.scripts {
			scripts = append(scripts, s)
		}
		e.mu.Unlock()
		for _, s := range scripts {
			s.reloadIfChanged()
		}
	}
}

// Script is one compiled script file bound to one entry function. Execution
// is serialized on its own runtime; hot reload swaps in a fresh runtime so
// stale script state never leaks across versions.
type Script struct {
	path   string
	fnName string
	log    *slog.Logger

	mu        sync.Mutex
	rt        *goja.Runtime
	fn        goja.Callable
	mtime     time.Time
	lastError string
}

// compile (re)loads the file into a brand-new runtime. On error the
// previous runtime keeps serving.
func (s *Script) compile() error {
	src, err := os.ReadFile(s.path)
	if err != nil {
		return s.fail(fmt.Errorf("script %s: %w", s.path, err))
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return s.fail(fmt.Errorf("script %s: %w", s.path, err))
	}
	prog, err := goja.Compile(s.path, string(src), false)
	if err != nil {
		return s.fail(fmt.Errorf("script %s: %w", s.path, err))
	}
	rt := goja.New()
	if _, err := rt.RunProgram(prog); err != nil {
		return s.fail(fmt.Errorf("script %s: %w", s.path, err))
	}
	fn, ok := goja.AssertFunction(rt.Get(s.fnName))
	if !ok {
		return s.fail(fmt.Errorf("script %s: must define function %s(msg)", s.path, s.fnName))
	}

	s.mu.Lock()
	s.rt = rt
	s.fn = fn
	s.mtime = info.ModTime()
	s.lastError = ""
	s.mu.Unlock()
	return nil
}

func (s *Script) fail(err error) error {
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
	return err
}

func (s *Script) reloadIfChanged() {
	info, err := os.Stat(s.path)
	if err != nil {
		return
	}
	s.mu.Lock()
	changed := !info.ModTime().Equal(s.mtime)
	s.mu.Unlock()
	if !changed {
		return
	}
	if err := s.compile(); err != nil {
		s.log.Error("script hot reload failed; previous version stays active",
			"script", s.path, "error", err)
		// Record the attempted mtime so a broken save doesn't retrigger
		// every tick; the next save retries.
		s.mu.Lock()
		s.mtime = info.ModTime()
		s.mu.Unlock()
		return
	}
	s.log.Info("script reloaded", "script", s.path)
}

// run executes the script's entry function against m. It installs the
// per-message bindings, enforces the timeout, translates response.reject
// into a channel.Rejection, and applies a returned newMessage() result.
func (s *Script) run(m *message.Message, timeout time.Duration) (goja.Value, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rt == nil {
		return nil, fmt.Errorf("script %s: not compiled", s.path)
	}

	env, err := newEnv(s.rt, m)
	if err != nil {
		return nil, err
	}
	timer := time.AfterFunc(timeout, func() {
		s.rt.Interrupt(fmt.Sprintf("script %s exceeded %v timeout", s.path, timeout))
	})
	defer func() {
		timer.Stop()
		s.rt.ClearInterrupt()
	}()

	v, err := s.fn(goja.Undefined(), env.msgValue)
	if err != nil {
		if rej := env.rejection; rej != nil {
			return nil, rej
		}
		return nil, fmt.Errorf("script %s: %w", s.path, err)
	}
	if rej := env.rejection; rej != nil {
		return nil, rej
	}
	if err := env.applyResult(v, m); err != nil {
		return nil, fmt.Errorf("script %s: %w", s.path, err)
	}
	env.syncMeta()
	return v, nil
}
