// Package jsonfmt is the JSON format module. Objects become named children
// (key order preserved), arrays become container nodes of kind "array", and
// every leaf is type-tagged (string/number/bool/null) so values round-trip
// exactly as typed — {"count":5} never mutates into {"count":"5"}.
//
// The path dialect follows JSON's own ecosystem (JavaScript, JSONPath, jq):
// dot-separated keys with 0-based array indexes — "patient.name[0].given".
// This deliberately differs from the 1-based HL7/ASTM/CSV dialects, which
// follow their specs; each dialect reads the way its format's tooling counts.
// A key segment applied to an array fans out across its elements, and a path
// ending on an array resolves to its elements — so getAll("entry.resource.id")
// walks a bundle the way getAll("PID-3.1") walks repetitions. Keys containing
// '.' or '[' are written as quoted segments: ["odd.key"].
package jsonfmt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
)

// Node kinds used in message.Node.Kind.
const (
	KindString = "string"
	KindNumber = "number"
	KindBool   = "bool"
	KindNull   = "null"
	KindObject = "object"
	KindArray  = "array"
)

func init() { format.Register(DataType{}) }

// DataType implements format.DataType (and format.TypedSetter) for JSON.
type DataType struct{}

func (DataType) Name() string { return "json" }

// MaxDepth bounds container nesting. Parsing recurses once per level, and
// a payload of nothing but "[[[[" within the transport's size limit would
// otherwise overflow the goroutine stack, which is a fatal error rather
// than a recoverable panic. Real documents nest a few dozen levels deep.
const MaxDepth = 512

func (DataType) Parse(raw []byte) (*message.Node, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	root := &message.Node{Name: "json"}
	if err := decodeValue(dec, root, 0); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("json: empty message")
		}
		return nil, fmt.Errorf("json: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("json: trailing data after document")
	}
	return root, nil
}

// decodeValue reads one JSON value from dec into n (name already set).
// depth is the container nesting level of n.
func decodeValue(dec *json.Decoder, n *message.Node, depth int) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch t := tok.(type) {
	case json.Delim:
		if depth >= MaxDepth {
			return fmt.Errorf("nesting deeper than %d levels", MaxDepth)
		}
		switch t {
		case '{':
			n.Kind = KindObject
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return err
				}
				child := &message.Node{Name: keyTok.(string)}
				if err := decodeValue(dec, child, depth+1); err != nil {
					return err
				}
				n.Children = append(n.Children, child)
			}
			_, err = dec.Token() // consume '}'
			return err
		case '[':
			n.Kind = KindArray
			for dec.More() {
				child := &message.Node{Name: elementName(len(n.Children))}
				if err := decodeValue(dec, child, depth+1); err != nil {
					return err
				}
				n.Children = append(n.Children, child)
			}
			_, err = dec.Token() // consume ']'
			return err
		}
		return fmt.Errorf("unexpected delimiter %v", t)
	case string:
		n.Kind, n.Value = KindString, t
	case json.Number:
		n.Kind, n.Value = KindNumber, t.String()
	case bool:
		n.Kind = KindBool
		n.Value = strconv.FormatBool(t)
	case nil:
		n.Kind = KindNull
	default:
		return fmt.Errorf("unexpected token %v", tok)
	}
	return nil
}

// elementName is the display name of array element i; paths address elements
// positionally, so names are cosmetic (tree explorer, TUI).
func elementName(i int) string { return "[" + strconv.Itoa(i) + "]" }

func (DataType) Serialize(root *message.Node) ([]byte, error) {
	if root == nil {
		return nil, fmt.Errorf("json: empty tree")
	}
	var buf bytes.Buffer
	if err := encodeValue(&buf, root); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}
	return buf.Bytes(), nil
}

// encodeValue writes n's value as compact JSON, preserving child order. Nodes
// without a Kind (hand-built trees) are inferred: children with names encode
// as an object, a bare value as a string.
func encodeValue(buf *bytes.Buffer, n *message.Node) error {
	switch effectiveKind(n) {
	case KindObject:
		buf.WriteByte('{')
		for i, c := range n.Children {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(quoteString(c.Name))
			buf.WriteByte(':')
			if err := encodeValue(buf, c); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case KindArray:
		buf.WriteByte('[')
		for i, c := range n.Children {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeValue(buf, c); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case KindNumber:
		if !numberLiteral.MatchString(n.Value) {
			return fmt.Errorf("invalid number literal %q", n.Value)
		}
		buf.WriteString(n.Value)
	case KindBool:
		if n.Value != "true" && n.Value != "false" {
			return fmt.Errorf("invalid bool literal %q", n.Value)
		}
		buf.WriteString(n.Value)
	case KindNull:
		buf.WriteString("null")
	default: // KindString
		buf.WriteString(quoteString(n.Value))
	}
	return nil
}

// numberLiteral is the JSON number grammar (RFC 8259 section 6).
var numberLiteral = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// effectiveKind returns n.Kind, inferring one for untagged nodes.
func effectiveKind(n *message.Node) string {
	if n.Kind != "" {
		return n.Kind
	}
	if len(n.Children) > 0 {
		return KindObject
	}
	return KindString
}

// quoteString JSON-encodes s without HTML escaping (json.Marshal would turn
// "<" into "<", breaking byte-exact round trips).
func quoteString(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s) // encoding a string cannot fail
	return strings.TrimSuffix(buf.String(), "\n")
}

func (DataType) Resolve(root *message.Node, pathExpr string) ([]*message.Node, error) {
	segs, err := parsePath(pathExpr)
	if err != nil {
		return nil, err
	}
	nodes := []*message.Node{root}
	for _, s := range segs {
		var next []*message.Node
		for _, n := range nodes {
			if s.isIndex {
				if n.Kind == KindArray && s.index < len(n.Children) {
					next = append(next, n.Children[s.index])
				}
				continue
			}
			next = append(next, lookupKey(n, s.key)...)
		}
		nodes = next
	}
	// A path ending on an array resolves to its elements, mirroring how a
	// repetition-less HL7 path resolves every repetition.
	var out []*message.Node
	for _, n := range nodes {
		if n.Kind == KindArray {
			out = append(out, n.Children...)
		} else {
			out = append(out, n)
		}
	}
	return out, nil
}

// lookupKey returns the children of n named key. When n is an array, the
// lookup fans out across its elements (one level), so "entry.resource"
// reaches into every element of the entry array.
func lookupKey(n *message.Node, key string) []*message.Node {
	if n.Kind == KindArray {
		var out []*message.Node
		for _, el := range n.Children {
			for _, c := range el.Children {
				if c.Name == key {
					out = append(out, c)
				}
			}
		}
		return out
	}
	var out []*message.Node
	for _, c := range n.Children {
		if c.Name == key {
			out = append(out, c)
		}
	}
	return out
}

func (d DataType) Set(root *message.Node, pathExpr, value string) error {
	return d.SetTyped(root, pathExpr, value)
}

// SetTyped implements format.TypedSetter: values set from scripts keep their
// dynamic type (JS numbers stay JSON numbers). Intermediate structure is
// autovivified — objects for key segments, arrays (null-padded) for index
// segments; a scalar or empty container in the way is converted to the
// needed container. A populated container is never silently replaced: a key
// step on a non-empty array descends into element 0 (get on the same path
// reads element 0 first), and an index step on a non-empty object is an
// error.
func (DataType) SetTyped(root *message.Node, pathExpr string, value any) error {
	segs, err := parsePath(pathExpr)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return fmt.Errorf("json: empty path")
	}
	cur := root
	for _, s := range segs {
		if s.isIndex {
			if cur.Kind == KindObject && len(cur.Children) > 0 {
				return fmt.Errorf("json: path %q: [%d] applied to an object with keys", pathExpr, s.index)
			}
			if cur.Kind != KindArray {
				cur.Kind, cur.Value, cur.Children = KindArray, "", nil
			}
			for len(cur.Children) <= s.index {
				cur.Children = append(cur.Children,
					&message.Node{Name: elementName(len(cur.Children)), Kind: KindNull})
			}
			cur = cur.Children[s.index]
			continue
		}
		if cur.Kind == KindArray && len(cur.Children) > 0 {
			cur = cur.Children[0]
			if cur.Kind == KindArray && len(cur.Children) > 0 {
				return fmt.Errorf("json: path %q: key %q applied to nested arrays", pathExpr, s.key)
			}
		}
		if cur.Kind != KindObject {
			cur.Kind, cur.Value, cur.Children = KindObject, "", nil
		}
		var child *message.Node
		for _, c := range cur.Children {
			if c.Name == s.key {
				child = c
				break
			}
		}
		if child == nil {
			child = &message.Node{Name: s.key}
			cur.Children = append(cur.Children, child)
		}
		cur = child
	}
	kind, text, err := leafValue(value)
	if err != nil {
		return err
	}
	cur.Kind, cur.Value, cur.Children = kind, text, nil
	return nil
}

// leafValue maps a dynamic value to a leaf kind and literal text.
func leafValue(v any) (kind, text string, err error) {
	switch t := v.(type) {
	case nil:
		return KindNull, "", nil
	case bool:
		return KindBool, strconv.FormatBool(t), nil
	case string:
		return KindString, t, nil
	case json.Number:
		return KindNumber, t.String(), nil
	case int:
		return KindNumber, strconv.FormatInt(int64(t), 10), nil
	case int64:
		return KindNumber, strconv.FormatInt(t, 10), nil
	case float64:
		b, err := json.Marshal(t)
		if err != nil {
			return "", "", fmt.Errorf("json: unrepresentable number %v", t)
		}
		return KindNumber, string(b), nil
	default:
		return "", "", fmt.Errorf("json: unsupported value type %T", v)
	}
}

// Segments returns the elements of the top-level array named name (so
// msg.segments('entry') iterates a bundle), or the matching top-level
// children themselves when they are not arrays.
func (DataType) Segments(root *message.Node, name string) []*message.Node {
	var out []*message.Node
	for _, c := range root.Children {
		if c.Name != name {
			continue
		}
		if c.Kind == KindArray {
			out = append(out, c.Children...)
		} else {
			out = append(out, c)
		}
	}
	return out
}

// Value renders a leaf as its text (null renders empty) and a container as
// its compact JSON.
func (DataType) Value(root, n *message.Node) string {
	switch n.Kind {
	case KindObject, KindArray:
		var buf bytes.Buffer
		if err := encodeValue(&buf, n); err != nil {
			return ""
		}
		return buf.String()
	case KindNull:
		return ""
	default:
		return n.Value
	}
}

func (DataType) Flatten(root *message.Node) []message.PathValue {
	var out []message.PathValue
	flatten(root, "", &out)
	return out
}

func flatten(n *message.Node, path string, out *[]message.PathValue) {
	switch n.Kind {
	case KindObject:
		for _, c := range n.Children {
			flatten(c, joinKey(path, c.Name), out)
		}
	case KindArray:
		for i, c := range n.Children {
			flatten(c, path+"["+strconv.Itoa(i)+"]", out)
		}
	default:
		if n.Value != "" {
			*out = append(*out, message.PathValue{Path: path, Value: n.Value})
		}
	}
}

// joinKey appends key to path in dialect form, quoting keys that contain
// path metacharacters.
func joinKey(path, key string) string {
	if key == "" || strings.ContainsAny(key, ".[]\"") {
		quoted := "[" + quoteString(key) + "]"
		return path + quoted
	}
	if path == "" {
		return key
	}
	return path + "." + key
}
