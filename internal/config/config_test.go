package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	for name, content := range cases {
		path := writeChannel(t, dir, "bad.yaml", content)
		if _, err := LoadChannel(path); err == nil {
			t.Errorf("%s: expected error", name)
		}
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
	if cfg.Listen != ":8420" || cfg.ChannelsDir != "channels" {
		t.Errorf("defaults = %+v", cfg)
	}
}
