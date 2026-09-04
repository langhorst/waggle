// Package delimited implements the tree machinery shared by delimiter-based
// formats (HL7 v2, ASTM E1394): segment → field → repetition → component
// [→ subcomponent]. Format modules keep what genuinely differs — header
// detection, delimiter extraction, escape codecs, path syntax — and delegate
// structure handling here.
package delimited

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/langhorst/waggle/internal/message"
)

// Delims holds the separator characters in effect for one message. HL7 reads
// them from MSH-1/MSH-2, ASTM from H-1/H-2; both fall back to standard
// defaults.
type Delims struct {
	Field byte
	Rep   byte
	Comp  byte
	Sub   byte // unused when HasSub is false
	Esc   byte
	// HasSub enables the subcomponent level (HL7 yes, ASTM no).
	HasSub bool
}

// Tree depth levels under the message root.
const (
	levelSegment = 1
	levelField   = 2
	levelRep     = 3
	levelComp    = 4
	levelSub     = 5
)

// Node kinds. Every node this package (or a format module using it)
// creates carries its level as Kind, so Render can find a node's level in
// O(1) instead of searching the tree from the root. Trees built by hand
// without kinds still work; they fall back to the search.
const (
	KindSegment      = "segment"
	KindField        = "field"
	KindRepetition   = "repetition"
	KindComponent    = "component"
	KindSubcomponent = "subcomponent"
)

func levelForKind(kind string) (int, bool) {
	switch kind {
	case KindSegment:
		return levelSegment, true
	case KindField:
		return levelField, true
	case KindRepetition:
		return levelRep, true
	case KindComponent:
		return levelComp, true
	case KindSubcomponent:
		return levelSub, true
	}
	return 0, false
}

// NewSegment creates an empty segment node named name.
func NewSegment(name string) *message.Node {
	return &message.Node{Name: name, Kind: KindSegment}
}

// ByteString converts a single delimiter byte to a one-byte string. Never
// use string(b) on a delimiter: for bytes >= 0x80 Go performs a rune
// conversion producing two UTF-8 bytes, which corrupts splitting, joining,
// and stored header fields.
func ByteString(b byte) string { return string([]byte{b}) }

// SplitLines splits raw bytes into segment lines, accepting CR, LF, or CRLF
// terminators and discarding empty lines.
func SplitLines(raw []byte) []string {
	normalized := strings.ReplaceAll(string(raw), "\r\n", "\r")
	normalized = strings.ReplaceAll(normalized, "\n", "\r")
	var lines []string
	for _, l := range strings.Split(normalized, "\r") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// FieldNode parses one raw field body into a field node: repetitions split by
// dl.Rep, components by dl.Comp, subcomponents by dl.Sub. decode is applied
// to every leaf value (escape-sequence decoding).
func FieldNode(idx int, raw string, dl Delims, decode func(string) string) *message.Node {
	field := &message.Node{Name: strconv.Itoa(idx), Kind: KindField}
	for r, repRaw := range strings.Split(raw, ByteString(dl.Rep)) {
		rep := &message.Node{Name: strconv.Itoa(r + 1), Kind: KindRepetition}
		if strings.IndexByte(repRaw, dl.Comp) < 0 && (!dl.HasSub || strings.IndexByte(repRaw, dl.Sub) < 0) {
			rep.Value = decode(repRaw)
		} else {
			for c, compRaw := range strings.Split(repRaw, ByteString(dl.Comp)) {
				comp := &message.Node{Name: strconv.Itoa(c + 1), Kind: KindComponent}
				if !dl.HasSub || strings.IndexByte(compRaw, dl.Sub) < 0 {
					comp.Value = decode(compRaw)
				} else {
					for s, subRaw := range strings.Split(compRaw, ByteString(dl.Sub)) {
						comp.Children = append(comp.Children,
							&message.Node{Name: strconv.Itoa(s + 1), Kind: KindSubcomponent, Value: decode(subRaw)})
					}
				}
				rep.Children = append(rep.Children, comp)
			}
		}
		field.Children = append(field.Children, rep)
	}
	return field
}

// RawFieldNode builds a single-repetition leaf field whose value bypasses
// splitting and decoding — used for header fields that hold the delimiter
// characters themselves (MSH-1/MSH-2, H-1/H-2).
func RawFieldNode(idx int, value string) *message.Node {
	return &message.Node{
		Name:     strconv.Itoa(idx),
		Kind:     KindField,
		Children: []*message.Node{{Name: "1", Kind: KindRepetition, Value: value}},
	}
}

// SerializeSegment renders one segment node to its wire form. encode is
// applied to leaf values. The first rawFields fields are emitted verbatim
// from their leaf value with no encoding; for a header segment (rawFields
// >= 1) field 1 is the field separator itself and is emitted directly after
// the segment name, matching "MSH|^~\&|..." / "H|\^&|...".
func SerializeSegment(seg *message.Node, dl Delims, encode func(string) string, rawFields int) string {
	var b strings.Builder
	b.WriteString(seg.Name)
	for i, f := range seg.Children {
		idx := i + 1
		if idx <= rawFields {
			// Verbatim: field 1 is the field separator itself and field 2
			// holds the remaining delimiter chars, so neither is preceded by
			// a separator ("MSH|^~\&|...").
			b.WriteString(rawLeafValue(f))
			continue
		}
		b.WriteByte(dl.Field)
		b.WriteString(renderNode(f, levelField, dl, encode))
	}
	return b.String()
}

func rawLeafValue(f *message.Node) string {
	if f.IsLeaf() {
		return f.Value
	}
	if len(f.Children) > 0 && f.Children[0].IsLeaf() {
		return f.Children[0].Value
	}
	return ""
}

func renderNode(n *message.Node, level int, dl Delims, encode func(string) string) string {
	if n.IsLeaf() {
		return encode(n.Value)
	}
	sep := childSeparator(level, dl)
	parts := make([]string, len(n.Children))
	for i, c := range n.Children {
		parts[i] = renderNode(c, level+1, dl, encode)
	}
	return strings.Join(parts, ByteString(sep))
}

func childSeparator(level int, dl Delims) byte {
	switch level {
	case levelSegment:
		return dl.Field
	case levelField:
		return dl.Rep
	case levelRep:
		return dl.Comp
	default:
		return dl.Sub
	}
}

// Render returns the string form of any node found in root's tree: the
// decoded leaf value, or the subtree joined with this message's separators
// (values stay decoded — Render is for script/UI consumption, not the wire).
// A segment node renders with its name prefix, honoring headerRawFields for
// header segments. Returns "" when n is not in the tree.
func Render(root, n *message.Node, dl Delims, headerRawFields func(segName string) int) string {
	level, ok := levelForKind(n.Kind)
	if !ok {
		level, ok = depthOf(root, n, 0)
		if !ok {
			return ""
		}
	}
	identity := func(s string) string { return s }
	if level == levelSegment {
		raw := 0
		if headerRawFields != nil {
			raw = headerRawFields(n.Name)
		}
		return SerializeSegment(n, dl, identity, raw)
	}
	return renderNode(n, level, dl, identity)
}

func depthOf(cur, target *message.Node, level int) (int, bool) {
	if cur == target {
		return level, true
	}
	for _, c := range cur.Children {
		if d, ok := depthOf(c, target, level+1); ok {
			return d, true
		}
	}
	return 0, false
}

// Path is a parsed path expression. Zero means "unspecified": an unspecified
// occurrence index (SegOcc, FieldRep) acts as a wildcard in Resolve and as 1
// in Set; Field/Comp/Sub at zero mean the path stops above that level.
type Path struct {
	Seg      string
	SegOcc   int
	Field    int
	FieldRep int
	Comp     int
	Sub      int
}

// Resolve returns all nodes matching p in document order. Missing structure
// yields no matches (never an error). Reading one level below a leaf follows
// promotion semantics: component 1 of a leaf repetition is the repetition
// itself, subcomponent 1 of a leaf component is the component itself.
func Resolve(root *message.Node, p Path) []*message.Node {
	var out []*message.Node
	occ := 0
	for _, seg := range root.Children {
		if seg.Name != p.Seg {
			continue
		}
		occ++
		if p.SegOcc != 0 && occ != p.SegOcc {
			continue
		}
		if p.Field == 0 {
			out = append(out, seg)
			continue
		}
		if p.Field > len(seg.Children) {
			continue
		}
		field := seg.Children[p.Field-1]
		for r, rep := range field.Children {
			if p.FieldRep != 0 && r+1 != p.FieldRep {
				continue
			}
			if n := descend(rep, p.Comp, p.Sub); n != nil {
				out = append(out, n)
			}
		}
	}
	return out
}

func descend(rep *message.Node, comp, sub int) *message.Node {
	if comp == 0 {
		return rep
	}
	c := childOrPromoted(rep, comp)
	if c == nil {
		return nil
	}
	if sub == 0 {
		return c
	}
	return childOrPromoted(c, sub)
}

// childOrPromoted returns the idx-th (1-based) child of n, treating a leaf as
// implicitly holding its own value at index 1.
func childOrPromoted(n *message.Node, idx int) *message.Node {
	if n.IsLeaf() {
		if idx == 1 {
			return n
		}
		return nil
	}
	if idx > len(n.Children) {
		return nil
	}
	return n.Children[idx-1]
}

// Set writes value at p, creating structure as needed: missing segment
// occurrences are appended after the last segment of the same name (or at the
// end), fields and repetitions are padded with empties, and leaves are
// promoted when the path addresses below them (the old leaf value becomes
// child 1). The path must address at least a field.
func Set(root *message.Node, p Path, value string) error {
	if p.Field == 0 {
		return fmt.Errorf("path %s: set requires a field or deeper", p.Seg)
	}
	segOcc := p.SegOcc
	if segOcc == 0 {
		segOcc = 1
	}
	seg := ensureSegment(root, p.Seg, segOcc)

	for len(seg.Children) < p.Field {
		seg.Children = append(seg.Children, emptyField(len(seg.Children)+1))
	}
	field := seg.Children[p.Field-1]

	fieldRep := p.FieldRep
	if fieldRep == 0 {
		fieldRep = 1
	}
	for len(field.Children) < fieldRep {
		field.Children = append(field.Children, &message.Node{Name: strconv.Itoa(len(field.Children) + 1), Kind: KindRepetition})
	}
	rep := field.Children[fieldRep-1]

	target := rep
	if p.Comp != 0 {
		target = ensureChild(target, p.Comp, KindComponent)
		if p.Sub != 0 {
			target = ensureChild(target, p.Sub, KindSubcomponent)
		}
	}
	target.Value = value
	target.Children = nil
	return nil
}

func emptyField(idx int) *message.Node {
	return &message.Node{Name: strconv.Itoa(idx), Kind: KindField, Children: []*message.Node{{Name: "1", Kind: KindRepetition}}}
}

func ensureSegment(root *message.Node, name string, occ int) *message.Node {
	count := 0
	lastIdx := -1
	for i, seg := range root.Children {
		if seg.Name == name {
			count++
			lastIdx = i
			if count == occ {
				return seg
			}
		}
	}
	insertAt := len(root.Children)
	if lastIdx >= 0 {
		insertAt = lastIdx + 1
	}
	for count < occ {
		seg := NewSegment(name)
		root.Children = append(root.Children, nil)
		copy(root.Children[insertAt+1:], root.Children[insertAt:])
		root.Children[insertAt] = seg
		insertAt++
		count++
	}
	return root.Children[insertAt-1]
}

// ensureChild promotes a leaf (old value becomes child 1) and pads children
// up to idx, returning the idx-th (1-based) child. kind is the children's
// level.
func ensureChild(n *message.Node, idx int, kind string) *message.Node {
	if n.IsLeaf() {
		n.Children = []*message.Node{{Name: "1", Kind: kind, Value: n.Value}}
		n.Value = ""
	}
	for len(n.Children) < idx {
		n.Children = append(n.Children, &message.Node{Name: strconv.Itoa(len(n.Children) + 1), Kind: kind})
	}
	return n.Children[idx-1]
}

// Flatten renders every non-empty leaf as a path/value pair in document
// order. Occurrence and repetition indexes of 1 are omitted, as are
// component/subcomponent suffixes when the leaf sits higher up — producing
// paths like "PID-5.1", "PID-3[2]", "OBX[2]-5", "H-5".
func Flatten(root *message.Node, headerRawFields func(segName string) int) []message.PathValue {
	var out []message.PathValue
	occ := map[string]int{}
	for _, seg := range root.Children {
		occ[seg.Name]++
		segPrefix := seg.Name
		if occ[seg.Name] > 1 {
			segPrefix = fmt.Sprintf("%s[%d]", seg.Name, occ[seg.Name])
		}
		raw := 0
		if headerRawFields != nil {
			raw = headerRawFields(seg.Name)
		}
		for fi, field := range seg.Children {
			fieldPrefix := fmt.Sprintf("%s-%d", segPrefix, fi+1)
			if fi+1 <= raw {
				if v := rawLeafValue(field); v != "" {
					out = append(out, message.PathValue{Path: fieldPrefix, Value: v})
				}
				continue
			}
			for ri, rep := range field.Children {
				repPrefix := fieldPrefix
				if ri > 0 {
					repPrefix = fmt.Sprintf("%s[%d]", fieldPrefix, ri+1)
				}
				flattenRep(rep, repPrefix, &out)
			}
		}
	}
	return out
}

func flattenRep(rep *message.Node, prefix string, out *[]message.PathValue) {
	if rep.IsLeaf() {
		if rep.Value != "" {
			*out = append(*out, message.PathValue{Path: prefix, Value: rep.Value})
		}
		return
	}
	for ci, comp := range rep.Children {
		compPrefix := fmt.Sprintf("%s.%d", prefix, ci+1)
		if comp.IsLeaf() {
			if comp.Value != "" {
				*out = append(*out, message.PathValue{Path: compPrefix, Value: comp.Value})
			}
			continue
		}
		for si, sub := range comp.Children {
			if sub.Value != "" {
				*out = append(*out, message.PathValue{
					Path:  fmt.Sprintf("%s.%d", compPrefix, si+1),
					Value: sub.Value,
				})
			}
		}
	}
}
