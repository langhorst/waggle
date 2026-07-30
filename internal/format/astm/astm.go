// Package astm is the ASTM E1394 (LIS02-A2) record-format module: records →
// fields → repetitions → components over the generic message tree, with the
// path dialect "REC[occ]-field[rep].component" (e.g. "H-5", "R[2]-4.1",
// "P-3[2].2"). Delimiters come from the H record ("H|\^&": field, repeat,
// component, escape).
//
// Field numbering counts from the first delimiter-separated field after the
// record type — "P|1||PATID" is P-1="1", P-3="PATID" — mirroring the HL7
// dialect rather than E1394's convention of counting the record type itself
// as field 1. H-1 is the field separator and H-2 the remaining delimiter
// characters, exactly like MSH-1/MSH-2.
//
// This module covers the record format only; the E1381 checksummed-frame
// transport lives in internal/adapter/astm1381 (astm-listener/astm-sender),
// and file adapters carry ASTM equally well.
package astm

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/langhorst/integration-channel/internal/format"
	"github.com/langhorst/integration-channel/internal/format/delimited"
	"github.com/langhorst/integration-channel/internal/message"
)

func init() { format.Register(DataType{}) }

// DataType implements format.DataType for ASTM E1394.
type DataType struct{}

func (DataType) Name() string { return "astm" }

// headerRawFields: H-1 holds the field separator, H-2 the repeat/component/
// escape characters, both verbatim.
func headerRawFields(name string) int {
	if name == "H" {
		return 2
	}
	return 0
}

func defaultDelims() delimited.Delims {
	return delimited.Delims{Field: '|', Rep: '\\', Comp: '^', Esc: '&', HasSub: false}
}

func delimsFromHeader(line string) delimited.Delims {
	dl := defaultDelims()
	if len(line) < 2 || line[0] != 'H' {
		return dl
	}
	dl.Field = line[1]
	enc := line[2:]
	if i := strings.IndexByte(enc, dl.Field); i >= 0 {
		enc = enc[:i]
	}
	if len(enc) > 0 {
		dl.Rep = enc[0]
	}
	if len(enc) > 1 {
		dl.Comp = enc[1]
	}
	if len(enc) > 2 {
		dl.Esc = enc[2]
	}
	return dl
}

func treeDelims(root *message.Node) delimited.Delims {
	dl := defaultDelims()
	if root == nil || len(root.Children) == 0 {
		return dl
	}
	seg := root.Children[0]
	if seg.Name != "H" || len(seg.Children) < 2 {
		return dl
	}
	if fs := leafValue(seg.Children[0]); len(fs) == 1 {
		dl.Field = fs[0]
	}
	enc := leafValue(seg.Children[1])
	if len(enc) > 0 {
		dl.Rep = enc[0]
	}
	if len(enc) > 1 {
		dl.Comp = enc[1]
	}
	if len(enc) > 2 {
		dl.Esc = enc[2]
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

// decoder translates ASTM escape sequences using the message's delimiters:
// &F& field, &R& repeat, &S& component, &E& escape, &Xhh..& hex bytes.
func decoder(dl delimited.Delims) func(string) string {
	return delimited.Decoder(dl.Esc, func(seq string) (string, bool) {
		switch seq {
		case "F":
			return delimited.ByteString(dl.Field), true
		case "R":
			return delimited.ByteString(dl.Rep), true
		case "S":
			return delimited.ByteString(dl.Comp), true
		case "E":
			return delimited.ByteString(dl.Esc), true
		}
		if isHexSeq(seq) {
			raw, _ := hex.DecodeString(seq[1:])
			return string(raw), true
		}
		return "", false
	})
}

// decodableSeq reports whether the decoder expands seq — everything else
// (highlighting &H&/&N&, custom sequences) is preserved verbatim on both
// decode and encode.
func decodableSeq(seq string) bool {
	switch seq {
	case "F", "R", "S", "E":
		return true
	}
	return isHexSeq(seq)
}

func isHexSeq(seq string) bool {
	if len(seq) < 3 || seq[0] != 'X' || len(seq)%2 == 0 {
		return false
	}
	_, err := hex.DecodeString(seq[1:])
	return err == nil
}

func encoder(dl delimited.Delims) func(string) string {
	byteEncode := func(c byte) (string, bool) {
		switch c {
		case dl.Esc:
			return "E", true
		case dl.Field:
			return "F", true
		case dl.Rep:
			return "R", true
		case dl.Comp:
			return "S", true
		}
		if c < 0x20 {
			return "X" + strings.ToUpper(hex.EncodeToString([]byte{c})), true
		}
		return "", false
	}
	preserved := func(seq string) bool { return !decodableSeq(seq) }
	return delimited.Encoder(dl.Esc, byteEncode, preserved)
}

var recNameRe = regexp.MustCompile(`^[A-Z]$`)

// Parse decodes an ASTM E1394 message. Record terminators may be CR, LF, or
// CRLF; the canonical serialized form uses CR. The first record must be H so
// delimiters are known.
func (DataType) Parse(raw []byte) (*message.Node, error) {
	lines := delimited.SplitLines(raw)
	if len(lines) == 0 {
		return nil, fmt.Errorf("astm: empty message")
	}
	if lines[0][0] != 'H' {
		return nil, fmt.Errorf("astm: message must start with an H record")
	}
	dl := delimsFromHeader(lines[0])
	decode := decoder(dl)

	root := &message.Node{Name: "astm"}
	for i, line := range lines {
		name := line[:1]
		if !recNameRe.MatchString(name) {
			return nil, fmt.Errorf("astm: record %d: invalid record type %q", i+1, name)
		}
		rec := &message.Node{Name: name}
		if headerRawFields(name) > 0 {
			if len(line) < 2 || line[1] != dl.Field {
				return nil, fmt.Errorf("astm: record %d: H record has inconsistent field separator", i+1)
			}
			enc := line[2:]
			if j := strings.IndexByte(enc, dl.Field); j >= 0 {
				enc = enc[:j]
			}
			rec.Children = append(rec.Children,
				delimited.RawFieldNode(1, delimited.ByteString(dl.Field)),
				delimited.RawFieldNode(2, enc))
			rest := line[2+len(enc):]
			if rest != "" {
				rest = rest[1:] // skip the separator after the delimiter chars
				for fi, fieldRaw := range strings.Split(rest, delimited.ByteString(dl.Field)) {
					rec.Children = append(rec.Children, delimited.FieldNode(fi+3, fieldRaw, dl, decode))
				}
			}
		} else if len(line) > 1 {
			if line[1] != dl.Field {
				return nil, fmt.Errorf("astm: record %d: expected %q after record type %s", i+1, delimited.ByteString(dl.Field), name)
			}
			for fi, fieldRaw := range strings.Split(line[2:], delimited.ByteString(dl.Field)) {
				rec.Children = append(rec.Children, delimited.FieldNode(fi+1, fieldRaw, dl, decode))
			}
		}
		root.Children = append(root.Children, rec)
	}
	return root, nil
}

// Serialize encodes the tree using the message's own delimiters (from
// H-1/H-2), CR record terminators, and a trailing CR.
func (DataType) Serialize(root *message.Node) ([]byte, error) {
	if root == nil || len(root.Children) == 0 {
		return nil, fmt.Errorf("astm: empty tree")
	}
	dl := treeDelims(root)
	encode := encoder(dl)
	var b strings.Builder
	for _, rec := range root.Children {
		if !recNameRe.MatchString(rec.Name) {
			return nil, fmt.Errorf("astm: invalid record type %q", rec.Name)
		}
		b.WriteString(delimited.SerializeSegment(rec, dl, encode, headerRawFields(rec.Name)))
		b.WriteByte('\r')
	}
	return []byte(b.String()), nil
}

// pathRe: REC[occ]-field[rep].component — no subcomponent level in E1394.
var pathRe = regexp.MustCompile(`^([A-Z])(?:\[([1-9]\d*)\])?(?:-([1-9]\d*)(?:\[([1-9]\d*)\])?(?:\.([1-9]\d*))?)?$`)

func parsePath(path string) (delimited.Path, error) {
	m := pathRe.FindStringSubmatch(path)
	if m == nil {
		return delimited.Path{}, fmt.Errorf("astm: invalid path %q (expected REC[occ]-field[rep].component)", path)
	}
	return delimited.Path{
		Seg:      m[1],
		SegOcc:   atoiZero(m[2]),
		Field:    atoiZero(m[3]),
		FieldRep: atoiZero(m[4]),
		Comp:     atoiZero(m[5]),
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
	for _, rec := range root.Children {
		if rec.Name == name {
			out = append(out, rec)
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
