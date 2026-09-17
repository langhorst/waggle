package file

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	metakey "github.com/langhorst/waggle/internal/meta"
)

// WriterConfig configures the file writer destination.
//
// The default is one file per message: Pattern names it and may use the
// tokens {ts} (UTC timestamp), {id} (message ID), {channel} and {dest}.
//
// Setting File instead appends every message to that one file, which is how
// a destination produces a running CSV or log rather than a directory of
// fragments. The two are mutually exclusive.
type WriterConfig struct {
	Dir     string `yaml:"dir"`
	Pattern string `yaml:"pattern"` // default "{ts}-{id}.out"

	// File appends all output to this single name inside Dir.
	File string `yaml:"file"`
	// Header is written once, when File is created empty. A CSV needs its
	// column names, and they must not repeat on restart.
	Header string `yaml:"header"`
}

// Writer is the file writer outbound adapter.
//
// Per-message files are written atomically (temp file + rename) and never
// overwrite an existing file. In append mode the target is opened once and
// held open, with writes serialized: a partially interleaved line would
// corrupt a CSV that something else is tailing.
type Writer struct {
	cfg WriterConfig

	mu  sync.Mutex
	out *os.File
}

func NewWriter(settings map[string]any) (*Writer, error) {
	cfg := WriterConfig{Pattern: "{ts}-{id}.out"}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("file-writer: dir is required")
	}
	if cfg.File != "" {
		// Append mode. Pattern is meaningless here, and silently ignoring a
		// configured one would hide a mistake.
		if _, ok := settings["pattern"]; ok {
			return nil, fmt.Errorf("file-writer: file and pattern are mutually exclusive")
		}
		if strings.ContainsAny(cfg.File, `/\`) {
			return nil, fmt.Errorf("file-writer: file must not contain path separators")
		}
		return &Writer{cfg: cfg}, nil
	}
	if cfg.Header != "" {
		return nil, fmt.Errorf("file-writer: header only applies with file (append mode)")
	}
	if cfg.Pattern == "" {
		cfg.Pattern = "{ts}-{id}.out"
	}
	if strings.ContainsAny(cfg.Pattern, `/\`) {
		return nil, fmt.Errorf("file-writer: pattern must not contain path separators")
	}
	return &Writer{cfg: cfg}, nil
}

func (w *Writer) Open(ctx context.Context) error {
	if err := os.MkdirAll(w.cfg.Dir, 0o755); err != nil {
		return err
	}
	if w.cfg.File == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(w.cfg.Dir, w.cfg.File), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// The header belongs to a new file only: reopening on restart must
	// continue the existing one, not repeat its column names mid-file.
	if w.cfg.Header != "" {
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		if info.Size() == 0 {
			if _, err := f.WriteString(ensureNewline(w.cfg.Header)); err != nil {
				_ = f.Close()
				return err
			}
		}
	}
	w.out = f
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.out == nil {
		return nil
	}
	err := w.out.Close()
	w.out = nil
	return err
}

// appendOne writes payload as one record of the shared file, flushing it so
// a reviewer tailing the file sees each message as it lands.
func (w *Writer) appendOne(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.out == nil {
		return fmt.Errorf("file-writer: append target is not open")
	}
	if _, err := w.out.WriteString(ensureNewline(string(payload))); err != nil {
		return err
	}
	// Durability here is worth the cost: the point of this mode is a file
	// somebody reads while the channel runs.
	return w.out.Sync()
}

// ensureNewline gives s exactly one trailing newline, so records stay one
// to a line whether or not the format serializer supplied it.
func ensureNewline(s string) string {
	s = strings.TrimRight(s, "\r\n")
	return s + "\n"
}

func (w *Writer) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	if w.cfg.File != "" {
		return w.appendOne(payload)
	}
	name := w.cfg.Pattern
	name = strings.ReplaceAll(name, "{ts}", time.Now().UTC().Format("20060102T150405.000000000"))
	name = strings.ReplaceAll(name, "{id}", meta[metakey.MessageID])
	name = strings.ReplaceAll(name, "{channel}", meta[metakey.ChannelID])
	name = strings.ReplaceAll(name, "{dest}", meta[metakey.DestinationID])

	// Write to a temp file, fsync, then publish under the final name. The
	// publish step is os.Link rather than os.Rename: link fails with EEXIST
	// instead of silently replacing a file another channel wrote between
	// our existence check and our rename, so collisions get a suffix
	// without a race.
	tmp, err := os.CreateTemp(w.cfg.Dir, ".partial-*")
	if err != nil {
		return fmt.Errorf("file-writer: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return fmt.Errorf("file-writer: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("file-writer: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("file-writer: %w", err)
	}
	target := filepath.Join(w.cfg.Dir, name)
	for i := 1; ; i++ {
		err := os.Link(tmpName, target)
		if err == nil {
			return nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("file-writer: %w", err)
		}
		target = filepath.Join(w.cfg.Dir, fmt.Sprintf("%s.%d", name, i))
	}
}
