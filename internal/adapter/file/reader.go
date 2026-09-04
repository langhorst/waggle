// Package file provides the filesystem Channel Adapters: a Polling Consumer
// that reads message files from a directory, and a file writer destination.
package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// Ack modes, as for the network sources: immediate moves the file to
// ProcessedDir once the engine has recorded it; destination waits for the
// pipeline outcome and moves a rejected file to ErrorDir instead.
const (
	AckImmediate   = adapter.AckImmediate
	AckDestination = adapter.AckDestination
)

// ReaderConfig configures the Polling Consumer. Each matching file is
// delivered as one message and then moved out of Dir, so a file is never
// delivered twice. When the engine refuses a file (channel paused, store
// unavailable) it stays where it is and is offered again on the next poll:
// refusal is transient by contract, not a verdict on the file.
type ReaderConfig struct {
	Dir      string           `yaml:"dir"`
	Pattern  string           `yaml:"pattern"`  // glob against the base name; default "*"
	Interval adapter.Duration `yaml:"interval"` // poll interval; default 2s
	// MinAge skips files modified more recently than this, so half-written
	// files are not picked up. Default 1s.
	MinAge       adapter.Duration `yaml:"minAge"`
	ProcessedDir string           `yaml:"processedDir"` // default <dir>/processed
	ErrorDir     string           `yaml:"errorDir"`     // default <dir>/error
	// AckMode is immediate (default) or destination; see adapter.AckMode.
	AckMode adapter.AckMode `yaml:"ackMode"`
	// HoldTimeout bounds the wait for a pipeline outcome in destination
	// mode; on expiry the file is treated as accepted (the engine owns it).
	// Default 30s.
	HoldTimeout adapter.Duration `yaml:"holdTimeout"`
}

// Reader is the Polling Consumer inbound adapter.
type Reader struct {
	cfg ReaderConfig
	log *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	// stuck holds files that were delivered but could not be moved out of
	// Dir. They must not be delivered again while this process lives; the
	// operator sees the error in the log and moves them by hand.
	stuck map[string]bool
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
	mode, err := adapter.ParseAckMode(string(cfg.AckMode))
	if err != nil {
		return nil, fmt.Errorf("file-reader: %w", err)
	}
	cfg.AckMode = mode
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = adapter.Duration(30 * time.Second)
	}
	return &Reader{cfg: cfg, log: slog.Default(), stuck: map[string]bool{}}, nil
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
		if r.stuck[name] {
			continue
		}
		path := filepath.Join(r.cfg.Dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		meta := map[string]string{metakey.SourceFile: name}
		rec, err := deliver(ctx, raw, meta)
		if err != nil {
			// Not accepted: the file stays put and is offered again next
			// poll. Whatever refused this one refuses the rest too, so
			// stop here rather than log once per file.
			r.log.Warn("file-reader: delivery refused; leaving file in place", "file", name, "error", err)
			return
		}
		r.settle(ctx, path, name, rec)
	}
}

// settle moves a delivered file out of Dir according to the ack mode.
func (r *Reader) settle(ctx context.Context, path, name string, rec adapter.Receipt) {
	dest := r.cfg.ProcessedDir
	if r.cfg.AckMode == adapter.AckDestination {
		d, outcome := adapter.AwaitDecision(ctx, rec, time.Duration(r.cfg.HoldTimeout))
		switch outcome {
		case adapter.Decided:
			if !d.Accepted() {
				r.log.Warn("file-reader: pipeline rejected file", "file", name, "message", rec.MessageID, "code", d.Code, "text", d.Text)
				dest = r.cfg.ErrorDir
			}
		case adapter.TimedOut:
			r.log.Warn("file-reader: no pipeline outcome within holdTimeout; treating as accepted", "file", name, "message", rec.MessageID)
		case adapter.Canceled:
			// Shutting down: the message is recorded, so the file is done.
		}
	}
	if err := moveTo(path, dest, name); err != nil {
		// Delivered but immovable: it would be redelivered every poll.
		r.stuck[name] = true
		r.log.Error("file-reader: delivered file could not be moved; it will not be re-read until restart",
			"file", name, "message", rec.MessageID, "target", dest, "error", err)
	}
}

// moveTo relocates a consumed file, suffixing the name if the target already
// exists so nothing is ever overwritten. A rename across filesystems fails
// with EXDEV; copy-and-remove covers that case.
func moveTo(path, dir, name string) error {
	target := filepath.Join(dir, name)
	for i := 1; ; i++ {
		// Only an existing target needs a suffix; any other Stat outcome
		// (missing, or dir is not a directory) is the rename's problem.
		if _, err := os.Stat(target); err != nil {
			break
		}
		target = filepath.Join(dir, fmt.Sprintf("%s.%d", name, i))
	}
	err := os.Rename(path, target)
	if err == nil {
		return nil
	}
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) {
		return err
	}
	if copyErr := copyFile(path, target); copyErr != nil {
		return fmt.Errorf("rename: %w; copy fallback: %w", err, copyErr)
	}
	return os.Remove(path)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	return out.Close()
}
