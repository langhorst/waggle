// Package config defines the YAML configuration schema — one daemon file
// plus one file per channel — and its loading and validation. Filter and
// transformer scripts are referenced by path to separate .js files, keeping
// scripts editable with normal tooling.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/format"
)

// Daemon is the top-level daemon configuration.
type Daemon struct {
	// Listen is the HTTP API/web UI address, e.g. ":8420".
	Listen string `yaml:"listen"`
	// DataDir holds the SQLite database and other runtime state.
	DataDir string `yaml:"dataDir"`
	// ChannelsDir contains one YAML file per channel.
	ChannelsDir string `yaml:"channelsDir"`
	// HotReload enables watching channel configs and scripts for changes.
	HotReload bool `yaml:"hotReload"`
}

// DefaultDaemon returns the daemon config defaults.
func DefaultDaemon() Daemon {
	return Daemon{
		Listen:      ":8420",
		DataDir:     "data",
		ChannelsDir: "channels",
		HotReload:   true,
	}
}

// LoadDaemon reads the daemon YAML config; a missing file yields defaults.
func LoadDaemon(path string) (Daemon, error) {
	cfg := DefaultDaemon()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("config: %w", err)
	}
	if err := strictUnmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("config %s: %w", path, err)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8420"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if cfg.ChannelsDir == "" {
		cfg.ChannelsDir = "channels"
	}
	return cfg, nil
}

// Channel is one channel definition.
type Channel struct {
	ID      string `yaml:"id"`
	Name    string `yaml:"name"`
	Enabled *bool  `yaml:"enabled"` // default true
	// Retention caps stored messages for this channel; -1 = unlimited.
	// Default 1000.
	Retention    *int          `yaml:"retention"`
	Source       Source        `yaml:"source"`
	Filter       string        `yaml:"filter"`       // path to .js Message Filter
	Transformers []string      `yaml:"transformers"` // ordered .js Message Translator chain
	Destinations []Destination `yaml:"destinations"`

	// Path is where this channel was loaded from (set by LoadChannels; not
	// part of the YAML).
	Path string `yaml:"-"`
}

// Enabled reports whether the channel should start with the daemon.
func (c *Channel) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

// RetentionCount returns the effective retention (-1 = unlimited).
func (c *Channel) RetentionCount() int {
	if c.Retention == nil {
		return 1000
	}
	return *c.Retention
}

// Source is the inbound Channel Adapter reference.
type Source struct {
	Type     string         `yaml:"type"`
	DataType string         `yaml:"dataType"`
	Settings map[string]any `yaml:"settings"`
}

// Destination is one Recipient List entry.
type Destination struct {
	ID       string `yaml:"id"`
	DataType string `yaml:"dataType"` // output data type; default = source dataType
	// WaitForAck: this destination's synchronous delivery outcome drives the
	// source ACK in destination-ACK mode. WaitForAck destinations bypass the
	// retry queue (single attempt; upstream owns retry).
	WaitForAck   bool       `yaml:"waitForAck"`
	Filter       string     `yaml:"filter"`
	Transformers []string   `yaml:"transformers"`
	Adapter      AdapterRef `yaml:"adapter"`
	Queue        Queue      `yaml:"queue"`
}

// AdapterRef names a registered adapter type plus its settings block.
type AdapterRef struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

// Queue configures Guaranteed Delivery for one destination.
type Queue struct {
	// MaxAttempts before dead-lettering; -1 = retry forever. Default 10.
	MaxAttempts *int `yaml:"maxAttempts"`
	// RetryInterval is the base backoff (doubled per attempt, capped at 5m,
	// jittered). Default 1s.
	RetryInterval adapter.Duration `yaml:"retryInterval"`
}

func (q *Queue) MaxAttemptCount() int {
	if q.MaxAttempts == nil {
		return 10
	}
	return *q.MaxAttempts
}

// LoadChannels reads every *.yaml/*.yml in dir, sorted by filename.
func LoadChannels(dir string) ([]*Channel, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("config: channels dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if ext := strings.ToLower(filepath.Ext(e.Name())); ext == ".yaml" || ext == ".yml" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var channels []*Channel
	seen := map[string]string{}
	for _, name := range names {
		path := filepath.Join(dir, name)
		ch, err := LoadChannel(path)
		if err != nil {
			return nil, err
		}
		if prev, dup := seen[ch.ID]; dup {
			return nil, fmt.Errorf("config: duplicate channel id %q in %s (already defined in %s)", ch.ID, path, prev)
		}
		seen[ch.ID] = path
		channels = append(channels, ch)
	}
	return channels, nil
}

// LoadChannel reads and validates a single channel file.
func LoadChannel(path string) (*Channel, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var ch Channel
	if err := strictUnmarshal(raw, &ch); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	ch.Path = path
	if err := ch.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &ch, nil
}

// Validate checks structural correctness against the format and adapter
// registries, so typos fail at load time.
func (c *Channel) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("channel: id is required")
	}
	if c.Name == "" {
		c.Name = c.ID
	}
	if c.Source.Type == "" {
		return fmt.Errorf("channel %s: source.type is required", c.ID)
	}
	if c.Source.DataType == "" {
		return fmt.Errorf("channel %s: source.dataType is required", c.ID)
	}
	if _, ok := format.Get(c.Source.DataType); !ok {
		return fmt.Errorf("channel %s: unknown source dataType %q (registered: %v)",
			c.ID, c.Source.DataType, format.Names())
	}
	if len(c.Destinations) == 0 {
		return fmt.Errorf("channel %s: at least one destination is required", c.ID)
	}
	destIDs := map[string]bool{}
	for i := range c.Destinations {
		d := &c.Destinations[i]
		if d.ID == "" {
			return fmt.Errorf("channel %s: destination %d: id is required", c.ID, i+1)
		}
		if destIDs[d.ID] {
			return fmt.Errorf("channel %s: duplicate destination id %q", c.ID, d.ID)
		}
		destIDs[d.ID] = true
		if d.DataType == "" {
			d.DataType = c.Source.DataType
		}
		if _, ok := format.Get(d.DataType); !ok {
			return fmt.Errorf("channel %s: destination %s: unknown dataType %q (registered: %v)",
				c.ID, d.ID, d.DataType, format.Names())
		}
		if d.Adapter.Type == "" {
			return fmt.Errorf("channel %s: destination %s: adapter.type is required", c.ID, d.ID)
		}
		if ma := d.Queue.MaxAttemptCount(); ma == 0 || ma < -1 {
			return fmt.Errorf("channel %s: destination %s: queue.maxAttempts must be positive or -1", c.ID, d.ID)
		}
	}
	if r := c.RetentionCount(); r == 0 || r < -1 {
		return fmt.Errorf("channel %s: retention must be positive or -1 (unlimited)", c.ID)
	}
	return nil
}

// ScriptPaths returns every script file the channel references, resolved
// relative to the channel file's directory.
func (c *Channel) ScriptPaths() []string {
	var out []string
	add := func(p string) {
		if p != "" {
			out = append(out, c.ResolvePath(p))
		}
	}
	add(c.Filter)
	for _, t := range c.Transformers {
		add(t)
	}
	for _, d := range c.Destinations {
		add(d.Filter)
		for _, t := range d.Transformers {
			add(t)
		}
	}
	return out
}

// ResolvePath resolves a (possibly relative) path against the directory the
// channel file lives in.
func (c *Channel) ResolvePath(p string) string {
	if p == "" || filepath.IsAbs(p) || c.Path == "" {
		return p
	}
	return filepath.Join(filepath.Dir(c.Path), p)
}

func strictUnmarshal(raw []byte, out any) error {
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	return dec.Decode(out)
}
