package config

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/langhorst/waggle/internal/adapter/file"
	_ "github.com/langhorst/waggle/internal/adapter/mllp"
	_ "github.com/langhorst/waggle/internal/format/csvfmt"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
)

const validChannel = `
id: adt-feed
name: ADT feed
retention: 500
source:
  type: mllp-listener
  dataType: hl7v2
  settings:
    addr: ":6661"
destinations:
  - id: archive
    dataType: csv
    adapter:
      type: file-writer
      settings:
        dir: ./out
    queue:
      maxAttempts: 5
      retryInterval: 2s
`

func writeChannel(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadChannelValid(t *testing.T) {
	dir := t.TempDir()
	path := writeChannel(t, dir, "adt.yaml", validChannel)
	ch, err := LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	if ch.ID != "adt-feed" || ch.RetentionCount() != 500 || !ch.IsEnabled() {
		t.Errorf("channel = %+v", ch)
	}
	if ch.Destinations[0].Queue.MaxAttemptCount() != 5 {
		t.Errorf("maxAttempts = %d", ch.Destinations[0].Queue.MaxAttemptCount())
	}
	if ch.Destinations[0].DataType != "csv" {
		t.Errorf("dataType = %s", ch.Destinations[0].DataType)
	}
}

func TestLoadChannelDefaults(t *testing.T) {
	dir := t.TempDir()
	path := writeChannel(t, dir, "min.yaml", `
id: minimal
source:
  type: file-reader
  dataType: hl7v2
destinations:
  - id: out
    adapter:
      type: file-writer
`)
	ch, err := LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	if ch.Name != "minimal" {
		t.Errorf("name should default to id, got %q", ch.Name)
	}
	if ch.RetentionCount() != 1000 {
		t.Errorf("retention default = %d", ch.RetentionCount())
	}
	if ch.Destinations[0].DataType != "hl7v2" {
		t.Errorf("destination dataType should default to source dataType, got %q", ch.Destinations[0].DataType)
	}
	if ch.Destinations[0].Queue.MaxAttemptCount() != 10 {
		t.Errorf("maxAttempts default = %d", ch.Destinations[0].Queue.MaxAttemptCount())
	}
}

func TestLoadChannelErrors(t *testing.T) {
	cases := map[string]string{
		"unknown top-level key": strings.Replace(validChannel, "name:", "nome:", 1),
		"missing id":            strings.Replace(validChannel, "id: adt-feed", "", 1),
		"unknown dataType":      strings.Replace(validChannel, "dataType: hl7v2", "dataType: hl9", 1),
		"no destinations": `
id: x
source: {type: file-reader, dataType: hl7v2}
destinations: []
`,
		"duplicate destination": `
id: x
source: {type: file-reader, dataType: hl7v2}
destinations:
  - {id: d, adapter: {type: file-writer}}
  - {id: d, adapter: {type: file-writer}}
`,
		"zero retention":   strings.Replace(validChannel, "retention: 500", "retention: 0", 1),
		"zero maxAttempts": strings.Replace(validChannel, "maxAttempts: 5", "maxAttempts: 0", 1),
	}
	dir := t.TempDir()
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			path := writeChannel(t, dir, "bad.yaml", cases[name])
			if _, err := LoadChannel(path); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestLoadChannelsDuplicateID(t *testing.T) {
	dir := t.TempDir()
	writeChannel(t, dir, "a.yaml", validChannel)
	writeChannel(t, dir, "b.yaml", validChannel)
	if _, err := LoadChannels(dir); err == nil || !strings.Contains(err.Error(), "duplicate channel id") {
		t.Errorf("expected duplicate id error, got %v", err)
	}
}

func TestUnlimitedRetention(t *testing.T) {
	dir := t.TempDir()
	path := writeChannel(t, dir, "u.yaml", strings.Replace(validChannel, "retention: 500", "retention: -1", 1))
	ch, err := LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	if ch.RetentionCount() != -1 {
		t.Errorf("retention = %d, want -1", ch.RetentionCount())
	}
}

func TestScriptPathsResolveRelative(t *testing.T) {
	dir := t.TempDir()
	path := writeChannel(t, dir, "s.yaml", `
id: scripted
source: {type: file-reader, dataType: hl7v2}
filter: scripts/filter.js
transformers: [scripts/t1.js]
destinations:
  - id: out
    transformers: [scripts/d1.js]
    adapter: {type: file-writer}
`)
	ch, err := LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	paths := ch.ScriptPaths()
	if len(paths) != 3 {
		t.Fatalf("paths = %v", paths)
	}
	for _, p := range paths {
		if !strings.HasPrefix(p, dir) {
			t.Errorf("path %q not resolved against channel dir", p)
		}
	}
}

func TestLoadDaemonDefaults(t *testing.T) {
	cfg, err := LoadDaemon(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != DefaultListen || cfg.ChannelsDir != "channels" {
		t.Errorf("defaults = %+v", cfg)
	}
}

func TestLoadDaemonAuthPolicy(t *testing.T) {
	cases := map[string]struct {
		yaml    string
		wantErr string
	}{
		"loopback without auth":      {yaml: "listen: 127.0.0.1:9000\n"},
		"localhost without auth":     {yaml: "listen: localhost:9000\n"},
		"ipv6 loopback without auth": {yaml: "listen: \"[::1]:9000\"\n"},
		"all interfaces without auth": {
			yaml:    "listen: \":9000\"\n",
			wantErr: "no auth is configured",
		},
		"public address without auth": {
			yaml:    "listen: 0.0.0.0:9000\n",
			wantErr: "no auth is configured",
		},
		"public address with token": {yaml: "listen: 0.0.0.0:9000\nauth: {token: secret}\n"},
		"public address auth disabled": {
			yaml: "listen: 0.0.0.0:9000\nauth: {disabled: true}\n",
		},
		"disabled together with token": {
			yaml:    "auth: {disabled: true, token: x}\n",
			wantErr: "disabled is set together with credentials",
		},
		"basic user without password": {
			yaml:    "auth: {basicUser: ops}\n",
			wantErr: "basicUser requires basicPassword",
		},
		"basic password without user": {
			yaml:    "auth: {basicPassword: pw}\n",
			wantErr: "basicPassword requires basicUser",
		},
		"empty listen falls back to default": {yaml: "listen: \"\"\n"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "daemon.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadDaemon(path)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q, got config %+v", tc.wantErr, cfg)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			if err == nil && cfg.Listen == "" {
				t.Errorf("listen not defaulted: %+v", cfg)
			}
		})
	}
}

// TestValidateChecksAdapterTypes: adapter types are checked against the
// registry at load time, like data types, instead of when the engine builds
// the channel.
func TestValidateChecksAdapterTypes(t *testing.T) {
	base := func() *Channel {
		return &Channel{
			ID:     "c",
			Source: Source{Type: "file-reader", DataType: "hl7v2", Settings: map[string]any{"dir": "in"}},
			Destinations: []Destination{{
				ID:      "out",
				Adapter: AdapterRef{Type: "file-writer", Settings: map[string]any{"dir": "out"}},
			}},
		}
	}
	ok := base()
	ok.Normalize()
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid channel rejected: %v", err)
	}
	if ok.Name != "c" || ok.Destinations[0].DataType != "hl7v2" {
		t.Errorf("Normalize defaults: name %q, dest dataType %q", ok.Name, ok.Destinations[0].DataType)
	}

	badSource := base()
	badSource.Source.Type = "carrier-pigeon"
	badSource.Normalize()
	if err := badSource.Validate(); err == nil || !strings.Contains(err.Error(), "unknown source type") {
		t.Errorf("unknown source type: %v", err)
	}
	badDest := base()
	badDest.Destinations[0].Adapter.Type = "fax"
	badDest.Normalize()
	if err := badDest.Validate(); err == nil || !strings.Contains(err.Error(), "unknown adapter type") {
		t.Errorf("unknown adapter type: %v", err)
	}

	// Validate is pure: it reports a missing destination dataType rather
	// than filling it in.
	raw := base()
	if err := raw.Validate(); err == nil || !strings.Contains(err.Error(), "dataType is required") {
		t.Errorf("Validate without Normalize: %v", err)
	}
	if raw.Name != "" {
		t.Error("Validate modified the config")
	}
}
