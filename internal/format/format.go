// Package format defines the DataType contract every message format module
// (HL7 v2, ASTM, CSV, ...) implements, plus the compile-time registry the
// engine resolves configured data types from. Adding a format means
// implementing DataType and calling Register from an init function; no
// runtime plugins.
package format

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/langhorst/waggle/internal/message"
)

// DataType is a format module. Parsing and serialization happen exclusively
// in Go — transformer scripts only ever see the generic tree through a
// format's path dialect.
type DataType interface {
	// Name is the registry key used in channel configuration, e.g. "hl7v2".
	Name() string

	// Parse decodes raw bytes into the canonical tree.
	Parse(raw []byte) (*message.Node, error)
	// Serialize encodes a tree back to wire bytes.
	Serialize(root *message.Node) ([]byte, error)

	// Resolve evaluates a path in this format's dialect and returns all
	// matching nodes in document order. Omitted occurrence indexes act as
	// wildcards; an index defaults to the first occurrence only when written
	// explicitly by callers that want a single value (get = first of Resolve).
	// A path that matches nothing returns an empty slice and no error; an
	// error means the path itself is malformed for this dialect.
	Resolve(root *message.Node, path string) ([]*message.Node, error)
	// Set writes value at path, creating intermediate structure as needed
	// (missing segments, padded fields, leaf promotion). value is a Go
	// scalar: string, bool, nil, or any numeric type. Formats whose wire
	// model has types (JSON) keep it typed; the rest store String(value).
	Set(root *message.Node, path string, value any) error
	// Segments returns the top-level structural units matching name
	// (HL7/ASTM segments and records; CSV rows for name "R"; JSON keys,
	// fanned out over array elements; XML child elements).
	Segments(root *message.Node, name string) []*message.Node

	// ResolveFrom evaluates rel, a path in this format's dialect written
	// relative to one segment (seg.get('5.1') on a PID handle, row.get('3')
	// on a CSV row, el.get('id/@value') on an XML element, obj.get('family')
	// on a JSON object), against exactly that segment occurrence.
	ResolveFrom(root, seg *message.Node, rel string) ([]*message.Node, error)
	// SetFrom is Set relative to one segment occurrence.
	SetFrom(root, seg *message.Node, rel string, value any) error

	// Value renders a resolved node as a string: the leaf value, or for an
	// interior node the subtree re-serialized with this format's separators
	// (e.g. "DOE^JOHN" for a componentized HL7 field).
	Value(root, n *message.Node) string

	// Flatten renders every non-empty leaf as a path/value pair in this
	// format's dialect, in document order. Feeds message.Diff.
	Flatten(root *message.Node) []message.PathValue
}

// String renders a Set value for formats whose wire model is untyped text:
// nil is empty, booleans and numbers use their shortest exact form, and
// anything else falls back to fmt.
func String(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case fmt.Stringer:
		return t.String()
	}
	return fmt.Sprint(v)
}

var registry = map[string]DataType{}

// Register adds a DataType to the registry. It panics on a duplicate name;
// registration happens from init functions where a duplicate is a programming
// error.
func Register(dt DataType) {
	if _, dup := registry[dt.Name()]; dup {
		panic(fmt.Sprintf("format: duplicate registration of data type %q", dt.Name()))
	}
	registry[dt.Name()] = dt
}

// Get returns the DataType registered under name.
func Get(name string) (DataType, bool) {
	dt, ok := registry[name]
	return dt, ok
}

// Names returns all registered data type names, sorted.
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
