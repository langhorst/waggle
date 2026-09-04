// Package file provides the filesystem Channel Adapters: a Polling Consumer
// that reads message files from a directory, and a file writer destination.
package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	metakey "github.com/langhorst/waggle/internal/meta"
)

func init() {
	adapter.RegisterInbound("file-reader", func(settings map[string]any) (adapter.Inbound, error) {
		return NewReader(settings)
	})
	adapter.RegisterOutbound("file-writer", func(settings map[string]any) (adapter.Outbound, error) {
		return NewWriter(settings)
	})
}

// ReaderConfig configures the Polling Consumer. Each matching file is
// delivered as one message; on acceptance it moves to ProcessedDir, on
// rejection to ErrorDir, so a file is never delivered twice.
type ReaderConfig struct {
	Dir      string           `yaml:"dir"`
	Pattern  string           `yaml:"pattern"`  // glob against the base name; default "*"
	Interval adapter.Duration `yaml:"interval"` // poll interval; default 2s
	// MinAge skips files modified more recently than this, so half-written
	// files are not picked up. Default 1s.
	MinAge       adapter.Duration `yaml:"minAge"`
	ProcessedDir string           `yaml:"processedDir"` // default <dir>/processed
	ErrorDir     string           `yaml:"errorDir"`     // default <dir>/error
}

// Reader is the Polling Consumer inbound adapter.
type Reader struct {
	cfg ReaderConfig

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewReader(settings map[string]any) (*Reader, error) {
	cfg := ReaderConfig{Pattern: "*"}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("file-reader: dir is required")
	}
	if cfg.Pattern == "" {
		cfg.Pattern = "*"
	}
	if _, err := filepath.Match(cfg.Pattern, "probe"); err != nil {
		return nil, fmt.Errorf("file-reader: invalid pattern %q: %w", cfg.Pattern, err)
	}
	if cfg.Interval <= 0 {
		cfg.Interval = adapter.Duration(2 * time.Second)
	}
	if cfg.MinAge < 0 {
		cfg.MinAge = 0
	} else if cfg.MinAge == 0 {
		cfg.MinAge = adapter.Duration(time.Second)
	}
	if cfg.ProcessedDir == "" {
		cfg.ProcessedDir = filepath.Join(cfg.Dir, "processed")
	}
	if cfg.ErrorDir == "" {
		cfg.ErrorDir = filepath.Join(cfg.Dir, "error")
	}
	return &Reader{cfg: cfg}, nil
}

func (r *Reader) Start(ctx context.Context, deliver adapter.DeliverFunc) error {
	for _, dir := range []string{r.cfg.Dir, r.cfg.ProcessedDir, r.cfg.ErrorDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("file-reader: %w", err)
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	r.mu.Lock()
	r.cancel = cancel
	r.done = done
	r.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Duration(r.cfg.Interval))
		defer ticker.Stop()
		for {
			r.poll(runCtx, deliver)
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (r *Reader) Stop() error {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.cancel, r.done = nil, nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return nil
}

func (r *Reader) poll(ctx context.Context, deliver adapter.DeliverFunc) {
	entries, err := os.ReadDir(r.cfg.Dir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if ok, _ := filepath.Match(r.cfg.Pattern, e.Name()); !ok {
			continue
		}
		info, err := e.Info()
		if err != nil || time.Since(info.ModTime()) < time.Duration(r.cfg.MinAge) {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		path := filepath.Join(r.cfg.Dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		meta := map[string]string{metakey.SourceFile: name}
		if _, err := deliver(ctx, raw, meta); err != nil {
			moveTo(path, r.cfg.ErrorDir, name)
			continue
		}
		moveTo(path, r.cfg.ProcessedDir, name)
	}
}

// moveTo relocates a consumed file, suffixing the name if the target already
// exists so nothing is ever overwritten.
func moveTo(path, dir, name string) {
	target := filepath.Join(dir, name)
	for i := 1; ; i++ {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			break
		}
		target = filepath.Join(dir, fmt.Sprintf("%s.%d", name, i))
	}
	_ = os.Rename(path, target)
}
