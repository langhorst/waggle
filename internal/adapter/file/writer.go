package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
)

// WriterConfig configures the file writer destination. Pattern names the
// output file and may use the tokens {ts} (UTC timestamp), {id} (message
// ID), {channel}, and {dest}.
type WriterConfig struct {
	Dir     string `yaml:"dir"`
	Pattern string `yaml:"pattern"` // default "{ts}-{id}.out"
}

// Writer is the file writer outbound adapter. Files are written atomically
// (temp file + rename) and never overwrite an existing file.
type Writer struct {
	cfg WriterConfig
}

func NewWriter(settings map[string]any) (*Writer, error) {
	cfg := WriterConfig{Pattern: "{ts}-{id}.out"}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("file-writer: dir is required")
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
	return os.MkdirAll(w.cfg.Dir, 0o755)
}

func (w *Writer) Close() error { return nil }

func (w *Writer) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	name := w.cfg.Pattern
	name = strings.ReplaceAll(name, "{ts}", time.Now().UTC().Format("20060102T150405.000000000"))
	name = strings.ReplaceAll(name, "{id}", meta["message.id"])
	name = strings.ReplaceAll(name, "{channel}", meta["channel.id"])
	name = strings.ReplaceAll(name, "{dest}", meta["destination.id"])

	target := filepath.Join(w.cfg.Dir, name)
	for i := 1; ; i++ {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			break
		}
		target = filepath.Join(w.cfg.Dir, fmt.Sprintf("%s.%d", name, i))
	}

	tmp, err := os.CreateTemp(w.cfg.Dir, ".partial-*")
	if err != nil {
		return fmt.Errorf("file-writer: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("file-writer: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("file-writer: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("file-writer: %w", err)
	}
	return nil
}
