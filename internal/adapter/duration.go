package adapter

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from YAML strings like "30s"
// or "5m". A bare number is rejected: it used to mean nanoseconds, so
// `holdTimeout: 30` silently configured 30ns and passed every "is it set"
// check.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!!int" || node.Tag == "!!float" {
		return fmt.Errorf("invalid duration %s: a unit is required (e.g. %ss)", node.Value, node.Value)
	}
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("invalid duration: %s", node.Value)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	if parsed < 0 {
		return fmt.Errorf("invalid duration %q: must not be negative", s)
	}
	*d = Duration(parsed)
	return nil
}
