// Package hl7v2 is the HL7 v2.x format module: a minimal, dependency-free
// parser and serializer over the generic message tree, with the 1-based path
// dialect "SEG[segOcc]-field[rep].component.subcomponent" (e.g. "PID-5.1",
// "OBX[2]-5", "PID-3[2].1"). Segment groups are not modeled: a message is an
// ordered list of segments, as in Mirth's default behavior.
package hl7v2

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/format/delimited"
	"github.com/langhorst/waggle/internal/message"
)

func init() { format.Register(DataType{}) }

// DataType implements format.DataType for HL7 v2.
type DataType struct{}

func (DataType) Name() string { return "hl7v2" }

var segNameRe = regexp.MustCompile(`^[A-Z][A-Z0-9]{2}$`)

// headerRawFields returns how many leading fields of a segment hold the
// delimiter characters verbatim: MSH-1/MSH-2 and the batch equivalents.
func headerRawFields(name string) int {
	switch name {
	case "MSH", "BHS", "FHS":
		return 2
	}
	return 0
}

func defaultDelims() delimited.Delims {
	return delimited.Delims{Field: '|', Rep: '~', Comp: '^', Sub: '&', Esc: '\\', HasSub: true}
}

// delimsFromHeader extracts the message's delimiters from the first
// segment's raw line ("MSH|^~\&..."), falling back to the standard set for
// any character the header does not define.
func delimsFromHeader(line string) delimited.Delims {
	dl := defaultDelims()
	if len(line) < 4 || headerRawFields(line[:3]) == 0 {
		return dl
	}
	dl.Field = line[3]
	enc := line[4:]
	if i := strings.IndexByte(enc, dl.Field); i >= 0 {
		enc = enc[:i]
	}
	if len(enc) > 0 {
		dl.Comp = enc[0]
	}
	if len(enc) > 1 {
		dl.Rep = enc[1]
	}
	if len(enc) > 2 {
		dl.Esc = enc[2]
	}
	if len(enc) > 3 {
		dl.Sub = enc[3]
	}
	return dl
}

// treeDelims recovers the delimiters from an already-parsed tree (MSH-1 and
// MSH-2 of the first header segment) so serialization honors the message's
// own separators.
func treeDelims(root *message.Node) delimited.Delims {
	dl := defaultDelims()
	if root == nil || len(root.Children) == 0 {
		return dl
	}
	seg := root.Children[0]
	if headerRawFields(seg.Name) == 0 || len(seg.Children) < 2 {
		return dl
	}
	if fs := leafValue(seg.Children[0]); len(fs) == 1 {
		dl.Field = fs[0]
	}
	enc := leafValue(seg.Children[1])
	if len(enc) > 0 {
		dl.Comp = enc[0]
	}
	if len(enc) > 1 {
		dl.Rep = enc[1]
	}
	if len(enc) > 2 {
		dl.Esc = enc[2]
	}
	if len(enc) > 3 {
		dl.Sub = enc[3]
	}
	return dl
}

func leafValue(f *message.Node) string {
	if f.IsLeaf() {
		return f.Value
	}
	if len(f.Children) > 0 && f.Children[0].IsLeaf() {
		return f.Children[0].Value
	}
	return ""
}

// Parse decodes an HL7 v2 message. Segment terminators may be CR, LF, or
// CRLF; the canonical serialized form uses CR. The first segment must be a
// header (MSH/BHS/FHS) so delimiters are known.
func (DataType) Parse(raw []byte) (*message.Node, error) {
	lines := delimited.SplitLines(raw)
	if len(lines) == 0 {
		return nil, fmt.Errorf("hl7v2: empty message")
	}
	first := lines[0]
	if len(first) < 4 || headerRawFields(first[:3]) == 0 {
		return nil, fmt.Errorf("hl7v2: message must start with MSH, BHS, or FHS segment")
	}
	dl := delimsFromHeader(first)
	decode := decoder(dl)

	root := &message.Node{Name: "hl7v2"}
	for i, line := range lines {
		if len(line) < 3 {
			return nil, fmt.Errorf("hl7v2: segment %d: too short: %q", i+1, line)
		}
		name := line[:3]
		if !segNameRe.MatchString(name) {
			return nil, fmt.Errorf("hl7v2: segment %d: invalid segment name %q", i+1, name)
		}
		seg := delimited.NewSegment(name)
		if rawFields := headerRawFields(name); rawFields > 0 {
			if len(line) < 4 || line[3] != dl.Field {
				return nil, fmt.Errorf("hl7v2: segment %d: header segment %s has inconsistent field separator", i+1, name)
			}
			enc := line[4:]
			if j := strings.IndexByte(enc, dl.Field); j >= 0 {
				enc = enc[:j]
			}
			seg.Children = append(seg.Children,
				delimited.RawFieldNode(1, delimited.ByteString(dl.Field)),
				delimited.RawFieldNode(2, enc))
			rest := line[4+len(enc):]
			if rest != "" {
				rest = rest[1:] // skip the separator that follows the encoding chars
				for fi, fieldRaw := range strings.Split(rest, delimited.ByteString(dl.Field)) {
					seg.Children = append(seg.Children, delimited.FieldNode(fi+3, fieldRaw, dl, decode))
				}
			}
		} else {
			if len(line) > 3 {
				if line[3] != dl.Field {
					return nil, fmt.Errorf("hl7v2: segment %d: expected %q after segment name %s", i+1, delimited.ByteString(dl.Field), name)
				}
				for fi, fieldRaw := range strings.Split(line[4:], delimited.ByteString(dl.Field)) {
					seg.Children = append(seg.Children, delimited.FieldNode(fi+1, fieldRaw, dl, decode))
				}
			}
		}
		root.Children = append(root.Children, seg)
	}
	return root, nil
}

// Serialize encodes the tree using the message's own delimiters (from
// MSH-1/MSH-2), CR segment terminators, and a trailing CR.
func (DataType) Serialize(root *message.Node) ([]byte, error) {
	if root == nil || len(root.Children) == 0 {
		return nil, fmt.Errorf("hl7v2: empty tree")
	}
	dl := treeDelims(root)
	encode := encoder(dl)
	var b strings.Builder
	for _, seg := range root.Children {
		if !segNameRe.MatchString(seg.Name) {
			return nil, fmt.Errorf("hl7v2: invalid segment name %q", seg.Name)
		}
		b.WriteString(delimited.SerializeSegment(seg, dl, encode, headerRawFields(seg.Name)))
		b.WriteByte('\r')
	}
	return []byte(b.String()), nil
}

// pathRe: SEG[occ]-field[rep].comp.sub, all indexes 1-based and optional
// below the segment name.
var pathRe = regexp.MustCompile(`^([A-Z][A-Z0-9]{2})(?:\[([1-9]\d*)\])?(?:-([1-9]\d*)(?:\[([1-9]\d*)\])?(?:\.([1-9]\d*)(?:\.([1-9]\d*))?)?)?$`)

func parsePath(path string) (delimited.Path, error) {
	m := pathRe.FindStringSubmatch(path)
	if m == nil {
		return delimited.Path{}, fmt.Errorf("hl7v2: invalid path %q (expected SEG[occ]-field[rep].component.subcomponent)", path)
	}
	return delimited.Path{
		Seg:      m[1],
		SegOcc:   atoiZero(m[2]),
		Field:    atoiZero(m[3]),
		FieldRep: atoiZero(m[4]),
		Comp:     atoiZero(m[5]),
		Sub:      atoiZero(m[6]),
	}, nil
}

func atoiZero(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

func (DataType) Resolve(root *message.Node, path string) ([]*message.Node, error) {
	p, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	return delimited.Resolve(root, p), nil
}

func (DataType) Set(root *message.Node, path, value string) error {
	p, err := parsePath(path)
	if err != nil {
		return err
	}
	return delimited.Set(root, p, value)
}

func (DataType) Segments(root *message.Node, name string) []*message.Node {
	var out []*message.Node
	for _, seg := range root.Children {
		if seg.Name == name {
			out = append(out, seg)
		}
	}
	return out
}

func (DataType) Value(root, n *message.Node) string {
	return delimited.Render(root, n, treeDelims(root), headerRawFields)
}

func (DataType) Flatten(root *message.Node) []message.PathValue {
	return delimited.Flatten(root, headerRawFields)
}
