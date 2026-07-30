package adapter

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

// DecodeSettings maps a generic settings block from channel YAML onto an
// adapter's typed config struct. Unknown keys are an error — a typo in a
// channel file should fail loudly at load time, not silently fall back to a
// default.
func DecodeSettings(settings map[string]any, out any) error {
	raw, err := yaml.Marshal(settings)
	if err != nil {
		return fmt.Errorf("adapter settings: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("adapter settings: %w", err)
	}
	return nil
}
