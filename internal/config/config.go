// Package config defines the YAML configuration schema — one daemon file
// plus one file per channel — and its loading and validation. Filter and
// transformer scripts are referenced by path to separate .js files, keeping
// scripts editable with normal tooling.
package config

import (
	"fmt"
	"log/slog"
	"net"
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
	// Listen is the HTTP API/web UI address. Default "127.0.0.1:8420";
	// binding a non-loopback address requires auth to be configured.
	Listen string `yaml:"listen"`
	// DataDir holds the SQLite database and other runtime state.
	DataDir string `yaml:"dataDir"`
	// ChannelsDir contains one YAML file per channel.
	ChannelsDir string `yaml:"channelsDir"`
	// HotReload enables watching channel scripts for changes. Channel
	// YAML is reloaded on request (the reload action), not watched.
	HotReload bool `yaml:"hotReload"`
	// Auth protects the HTTP API and web UI.
	Auth Auth `yaml:"auth"`
	// LogLevel is debug, info (the default), warn or error.
	//
	// info logs a line per message through the pipeline, which is what
	// makes a channel followable in a terminal while it runs; a busy
	// production feed will want warn. debug adds the parse, filter and
	// transform steps within each message.
	LogLevel string `yaml:"logLevel"`
}

// Level maps LogLevel onto its slog level, reporting an unusable setting
// rather than silently picking one.
func (d *Daemon) Level() (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(d.LogLevel)) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logLevel %q is not debug, info, warn or error", d.LogLevel)
	}
}

// Auth is the HTTP API/web UI credential policy. With no credentials
// configured the daemon only agrees to listen on a loopback address; set
// Disabled to opt out of that check explicitly.
type Auth struct {
	// Token is accepted as `Authorization: Bearer <token>` and as the
	// password of HTTP basic auth with any user name (so a browser can use
	// it for the web UI).
	Token string `yaml:"token"`
	// BasicUser/BasicPassword configure a dedicated basic-auth login.
	BasicUser     string `yaml:"basicUser"`
	BasicPassword string `yaml:"basicPassword"`
	// Disabled turns authentication off entirely. Only appropriate behind
	// a reverse proxy that authenticates, or on an isolated host.
	Disabled bool `yaml:"disabled"`
}

// Enabled reports whether any credential is configured.
func (a Auth) Enabled() bool {
	return a.Token != "" || a.BasicUser != ""
}

// DefaultListen is the default HTTP API/web UI bind address.
const DefaultListen = "127.0.0.1:8420"

// DefaultDaemon returns the daemon config defaults.
func DefaultDaemon() Daemon {
	return Daemon{
		Listen:      DefaultListen,
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
			// Falling back to the defaults must not skip validation: the
			// defaults configure no credentials, and a daemon serving with
			// none rejects every request. Report that instead of starting
			// an unusable server.
			if verr := cfg.Validate(); verr != nil {
				return cfg, fmt.Errorf("no config file at %s: %w", path, verr)
			}
			return cfg, nil
		}
		return cfg, fmt.Errorf("config: %w", err)
	}
	if err := strictUnmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("config %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// applyDefaults fills in fields that were explicitly set to empty; a YAML
// file with `listen: ""` should behave like one without the key.
func (d *Daemon) applyDefaults() {
	def := DefaultDaemon()
	if d.Listen == "" {
		d.Listen = def.Listen
	}
	if d.DataDir == "" {
		d.DataDir = def.DataDir
	}
	if d.ChannelsDir == "" {
		d.ChannelsDir = def.ChannelsDir
	}
}

// Validate checks the daemon configuration for unsafe combinations.
func (d *Daemon) Validate() error {
	if d.Auth.Disabled && d.Auth.Enabled() {
		return fmt.Errorf("auth: disabled is set together with credentials; remove one")
	}
	if d.Auth.BasicUser != "" && d.Auth.BasicPassword == "" {
		return fmt.Errorf("auth: basicUser requires basicPassword")
	}
	if d.Auth.BasicPassword != "" && d.Auth.BasicUser == "" {
		return fmt.Errorf("auth: basicPassword requires basicUser")
	}
	if _, err := d.Level(); err != nil {
		return err
	}
	// No credentials at all is refused everywhere, not just off-loopback:
	// the API rejects every request when nothing is configured, so serving
	// that way is a lockout rather than an open door. Saying so at startup
	// beats a daemon that runs and 401s its own operator.
	if !d.Auth.Enabled() && !d.Auth.Disabled {
		return fmt.Errorf("auth: no credentials configured for %q: set auth.token, or auth.basicUser with auth.basicPassword, or auth.disabled: true to serve unauthenticated", d.Listen)
	}
	return nil
}

// ServesLoopbackOnly reports whether the daemon's listen address binds only
// a loopback interface. Auth policy no longer varies by address, but an
// unauthenticated daemon reachable from the network deserves a louder
// warning than one bound to localhost.
func (d *Daemon) ServesLoopbackOnly() bool { return isLoopback(d.Listen) }

// isLoopback reports whether addr binds only a loopback interface. An empty
// host (":8420") binds every interface and is not loopback.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Channel is one channel definition.
type Channel struct {
	ID      string `yaml:"id"`
	Name    string `yaml:"name"`
	Enabled *bool  `yaml:"enabled"` // default true
	// Retention caps stored messages for this channel; -1 = unlimited.
	// Default 1000.
	Retention *int `yaml:"retention"`
	// MaxPending bounds messages accepted but not yet processed; when the
	// buffer is full the source blocks (and its transport ACK waits).
	// Default 256.
	MaxPending int `yaml:"maxPending"`
	// LogFields names values to pull out of each message and attach to its
	// log lines, as label -> path in the source data type's dialect, e.g.
	// {mrn: "PID-3[1].1", ctrl: "MSH-10"}. Without them a log says only
	// which message number moved, which is hard to line up against another
	// system's log; with them every line names the patient.
	LogFields    map[string]string `yaml:"logFields"`
	Source       Source            `yaml:"source"`
	Filter       string            `yaml:"filter"`       // path to .js Message Filter
	Transformers []string          `yaml:"transformers"` // ordered .js Message Translator chain
	Destinations []Destination     `yaml:"destinations"`

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
	ch.Normalize()
	if err := ch.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &ch, nil
}

// Normalize fills in the defaults that derive from other fields: the name
// defaults to the id and a destination's data type to the source's.
// LoadChannel calls it before Validate; a Channel built in code should too.
func (c *Channel) Normalize() {
	if c.Name == "" {
		c.Name = c.ID
	}
	for i := range c.Destinations {
		if c.Destinations[i].DataType == "" {
			c.Destinations[i].DataType = c.Source.DataType
		}
	}
}

// Validate checks structural correctness against the format and adapter
// registries, so a typo in a channel file fails at load time rather than
// when the engine builds the channel. It does not modify the config.
func (c *Channel) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("channel: id is required")
	}
	if c.Source.Type == "" {
		return fmt.Errorf("channel %s: source.type is required", c.ID)
	}
	if !adapter.HasInbound(c.Source.Type) {
		return fmt.Errorf("channel %s: unknown source type %q (registered: %v)",
			c.ID, c.Source.Type, adapter.InboundTypes())
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
			return fmt.Errorf("channel %s: destination %s: dataType is required (Normalize fills it from the source)", c.ID, d.ID)
		}
		if _, ok := format.Get(d.DataType); !ok {
			return fmt.Errorf("channel %s: destination %s: unknown dataType %q (registered: %v)",
				c.ID, d.ID, d.DataType, format.Names())
		}
		if d.Adapter.Type == "" {
			return fmt.Errorf("channel %s: destination %s: adapter.type is required", c.ID, d.ID)
		}
		if !adapter.HasOutbound(d.Adapter.Type) {
			return fmt.Errorf("channel %s: destination %s: unknown adapter type %q (registered: %v)",
				c.ID, d.ID, d.Adapter.Type, adapter.OutboundTypes())
		}
		if ma := d.Queue.MaxAttemptCount(); ma == 0 || ma < -1 {
			return fmt.Errorf("channel %s: destination %s: queue.maxAttempts must be positive or -1", c.ID, d.ID)
		}
	}
	if r := c.RetentionCount(); r == 0 || r < -1 {
		return fmt.Errorf("channel %s: retention must be positive or -1 (unlimited)", c.ID)
	}
	if c.MaxPending < 0 {
		return fmt.Errorf("channel %s: maxPending must be positive", c.ID)
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
