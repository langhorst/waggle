package delimited

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
)

// Role names one of the delimiter characters, for tables that describe a
// format in terms of "which delimiter" rather than "which byte".
type Role int

const (
	RoleField Role = iota + 1
	RoleRep
	RoleComp
	RoleSub
	RoleEsc
)

func (dl Delims) byRole(r Role) byte {
	switch r {
	case RoleField:
		return dl.Field
	case RoleRep:
		return dl.Rep
	case RoleComp:
		return dl.Comp
	case RoleSub:
		return dl.Sub
	case RoleEsc:
		return dl.Esc
	}
	return 0
}

func (dl *Delims) setRole(r Role, b byte) {
	switch r {
	case RoleField:
		dl.Field = b
	case RoleRep:
		dl.Rep = b
	case RoleComp:
		dl.Comp = b
	case RoleSub:
		dl.Sub = b
	case RoleEsc:
		dl.Esc = b
	}
}

// Spec is everything that distinguishes one delimiter-based format from
// another. HL7 v2 and ASTM E1394 share the whole tree model (segment,
// field, repetition, component, subcomponent), the header-carries-the-
// delimiters convention, the escape-sequence shape, and the path dialect;
// they differ only in the values below.
type Spec struct {
	// Name is the registry name and the root node's name.
	Name string
	// NamePattern is the regular expression a segment (record) name must
	// match, without anchors: `[A-Z][A-Z0-9]{2}` for HL7, `[A-Z]` for ASTM.
	NamePattern string
	// NameLen is the fixed length of a segment name.
	NameLen int
	// HeaderRaw reports how many leading fields of a segment hold the
	// delimiter characters verbatim (2 for MSH/BHS/FHS and H, 0 otherwise).
	// The first segment of a message must be a header.
	HeaderRaw func(name string) int
	// HeaderHint names the header segments for error messages.
	HeaderHint string
	// Defaults are the delimiters assumed for whatever the header does not
	// define; HasSub also decides whether the path dialect has a
	// subcomponent level.
	Defaults Delims
	// EncodingOrder lists, in order, which delimiter each character of the
	// header's encoding-characters field (MSH-2 / H-2) sets: HL7 "^~\&" is
	// component, repetition, escape, subcomponent; ASTM "\^&" is
	// repetition, component, escape.
	EncodingOrder []Role
	// Escapes maps escape-sequence bodies to the delimiter they stand for
	// (\F\ is the field separator, ...). Hex sequences (\Xhh..\) are built
	// in. Bodies outside this table are preserved verbatim through decode
	// and encode.
	Escapes map[string]Role
}

// Format is a complete format.DataType built from a Spec.
type Format struct {
	spec   Spec
	nameRe *regexp.Regexp
	pathRe *regexp.Regexp
	hint   string
}

// NewFormat compiles a Spec. It panics on an invalid spec: formats are
// declared at package level and a bad one is a programming error.
func NewFormat(spec Spec) *Format {
	if spec.Name == "" || spec.NamePattern == "" || spec.NameLen <= 0 || spec.HeaderRaw == nil {
		panic("delimited: incomplete format spec")
	}
	nameRe := regexp.MustCompile(`^` + spec.NamePattern + `$`)
	// SEG[occ]-field[rep].comp[.sub], all indexes 1-based, everything below
	// the segment name optional.
	path := `^(` + spec.NamePattern + `)(?:\[([1-9]\d*)\])?(?:-([1-9]\d*)(?:\[([1-9]\d*)\])?(?:\.([1-9]\d*)`
	hint := "SEG[occ]-field[rep].component"
	if spec.Defaults.HasSub {
		path += `(?:\.([1-9]\d*))?`
		hint += ".subcomponent"
	} else {
		path += `()` // keep group 6 present, always empty
	}
	path += `)?)?$`
	return &Format{spec: spec, nameRe: nameRe, pathRe: regexp.MustCompile(path), hint: hint}
}

// Name implements format.DataType.
func (f *Format) Name() string { return f.spec.Name }

// Delims returns the delimiters in effect for a parsed tree (from the
// header segment), or the defaults.
func (f *Format) Delims(root *message.Node) Delims {
	dl := f.spec.Defaults
	if root == nil || len(root.Children) == 0 {
		return dl
	}
	seg := root.Children[0]
	if f.spec.HeaderRaw(seg.Name) == 0 || len(seg.Children) < 2 {
		return dl
	}
	if fs := rawLeafValue(seg.Children[0]); len(fs) == 1 {
		dl.Field = fs[0]
	}
	f.applyEncoding(&dl, rawLeafValue(seg.Children[1]))
	return dl
}

// applyEncoding sets the delimiters named by the encoding-characters field.
func (f *Format) applyEncoding(dl *Delims, enc string) {
	for i, role := range f.spec.EncodingOrder {
		if i < len(enc) {
			dl.setRole(role, enc[i])
		}
	}
}

// delimsFromHeader extracts the delimiters from a raw header line.
func (f *Format) delimsFromHeader(line string) Delims {
	dl := f.spec.Defaults
	n := f.spec.NameLen
	if len(line) <= n || f.spec.HeaderRaw(line[:n]) == 0 {
		return dl
	}
	dl.Field = line[n]
	enc := line[n+1:]
	if i := strings.IndexByte(enc, dl.Field); i >= 0 {
		enc = enc[:i]
	}
	f.applyEncoding(&dl, enc)
	return dl
}

// Parse implements format.DataType. Segment terminators may be CR, LF, or
// CRLF; the canonical serialized form uses CR. The first segment must be a
// header so the delimiters are known.
func (f *Format) Parse(raw []byte) (*message.Node, error) {
	lines := SplitLines(raw)
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s: empty message", f.spec.Name)
	}
	n := f.spec.NameLen
	if len(lines[0]) <= n || f.spec.HeaderRaw(lines[0][:n]) == 0 {
		return nil, fmt.Errorf("%s: message must start with a header segment (%s)", f.spec.Name, f.spec.HeaderHint)
	}
	dl := f.delimsFromHeader(lines[0])
	decode := f.Decoder(dl)
	sep := ByteString(dl.Field)

	root := &message.Node{Name: f.spec.Name}
	for i, line := range lines {
		if len(line) < n {
			return nil, fmt.Errorf("%s: segment %d: too short: %q", f.spec.Name, i+1, line)
		}
		name := line[:n]
		if !f.nameRe.MatchString(name) {
			return nil, fmt.Errorf("%s: segment %d: invalid segment name %q", f.spec.Name, i+1, name)
		}
		seg := NewSegment(name)
		rest := line[n:]
		if rest != "" && rest[0] != dl.Field {
			return nil, fmt.Errorf("%s: segment %d: expected %q after segment name %s", f.spec.Name, i+1, sep, name)
		}
		firstField := 1
		if f.spec.HeaderRaw(name) > 0 {
			// Field 1 is the separator itself, field 2 the encoding
			// characters; neither is split or decoded.
			if rest == "" {
				return nil, fmt.Errorf("%s: segment %d: header segment %s has no field separator", f.spec.Name, i+1, name)
			}
			enc := rest[1:]
			if j := strings.IndexByte(enc, dl.Field); j >= 0 {
				enc = enc[:j]
			}
			seg.Children = append(seg.Children, RawFieldNode(1, sep), RawFieldNode(2, enc))
			rest = rest[1+len(enc):]
			firstField = 3
		}
		if rest != "" {
			for fi, fieldRaw := range strings.Split(rest[1:], sep) {
				seg.Children = append(seg.Children, FieldNode(firstField+fi, fieldRaw, dl, decode))
			}
		}
		root.Children = append(root.Children, seg)
	}
	return root, nil
}

// Serialize implements format.DataType: the tree's own delimiters, CR
// segment terminators, and a trailing CR.
func (f *Format) Serialize(root *message.Node) ([]byte, error) {
	if root == nil || len(root.Children) == 0 {
		return nil, fmt.Errorf("%s: empty tree", f.spec.Name)
	}
	dl := f.Delims(root)
	encode := f.Encoder(dl)
	var b strings.Builder
	for _, seg := range root.Children {
		if !f.nameRe.MatchString(seg.Name) {
			return nil, fmt.Errorf("%s: invalid segment name %q", f.spec.Name, seg.Name)
		}
		b.WriteString(SerializeSegment(seg, dl, encode, f.spec.HeaderRaw(seg.Name)))
		b.WriteByte('\r')
	}
	return []byte(b.String()), nil
}

// ParsePath parses a path in this format's dialect.
func (f *Format) ParsePath(path string) (Path, error) {
	m := f.pathRe.FindStringSubmatch(path)
	if m == nil {
		return Path{}, fmt.Errorf("%s: invalid path %q (expected %s)", f.spec.Name, path, f.hint)
	}
	return Path{
		Seg:      m[1],
		SegOcc:   atoiZero(m[2]),
		Field:    atoiZero(m[3]),
		FieldRep: atoiZero(m[4]),
		Comp:     atoiZero(m[5]),
		Sub:      atoiZero(m[6]),
	}, nil
}

// Resolve implements format.DataType.
func (f *Format) Resolve(root *message.Node, path string) ([]*message.Node, error) {
	p, err := f.ParsePath(path)
	if err != nil {
		return nil, err
	}
	return Resolve(root, p), nil
}

// Set implements format.DataType. Values are split on the separators below
// the addressed level (see Set); the header's delimiter fields are single
// raw values and reject paths below field level.
func (f *Format) Set(root *message.Node, path string, value any) error {
	p, err := f.ParsePath(path)
	if err != nil {
		return err
	}
	return f.set(root, root, p, path, format.String(value))
}

// set writes into target (root itself, or a single-segment scope) using the
// delimiters recorded in root.
func (f *Format) set(root, target *message.Node, p Path, path, value string) error {
	if p.Field <= f.spec.HeaderRaw(p.Seg) {
		if p.Comp != 0 || p.Sub != 0 {
			return fmt.Errorf("%s: path %s: %s-%d holds a delimiter and has no components", f.spec.Name, path, p.Seg, p.Field)
		}
		return Set(target, p, value, nil)
	}
	dl := f.Delims(root)
	return Set(target, p, value, &dl)
}

// ResolveFrom implements format.DataType: rel is "field[rep].comp.sub"
// evaluated against seg alone.
func (f *Format) ResolveFrom(root, seg *message.Node, rel string) ([]*message.Node, error) {
	p, err := f.ParsePath(seg.Name + "-" + rel)
	if err != nil {
		return nil, fmt.Errorf("%s: segment-relative path %q: %w", f.spec.Name, rel, err)
	}
	p.SegOcc = 0
	return Resolve(scope(root, seg), p), nil
}

// SetFrom implements format.DataType.
func (f *Format) SetFrom(root, seg *message.Node, rel string, value any) error {
	p, err := f.ParsePath(seg.Name + "-" + rel)
	if err != nil {
		return fmt.Errorf("%s: segment-relative path %q: %w", f.spec.Name, rel, err)
	}
	p.SegOcc = 1
	return f.set(root, scope(root, seg), p, rel, format.String(value))
}

// scope is a root holding exactly one segment; since it shares the node,
// writes through it land in the real tree.
func scope(root, seg *message.Node) *message.Node {
	return &message.Node{Name: root.Name, Children: []*message.Node{seg}}
}

// Segments implements format.DataType.
func (f *Format) Segments(root *message.Node, name string) []*message.Node {
	var out []*message.Node
	for _, seg := range root.Children {
		if seg.Name == name {
			out = append(out, seg)
		}
	}
	return out
}

// Value implements format.DataType.
func (f *Format) Value(root, n *message.Node) string {
	return Render(root, n, f.Delims(root), f.spec.HeaderRaw)
}

// Flatten implements format.DataType.
func (f *Format) Flatten(root *message.Node) []message.PathValue {
	return Flatten(root, f.spec.HeaderRaw)
}

// Decoder returns the escape-sequence decoder for dl: table sequences
// expand to their delimiter, \Xhh..\ to bytes, anything else is preserved.
func (f *Format) Decoder(dl Delims) func(string) string {
	return Decoder(dl.Esc, func(seq string) (string, bool) {
		if role, ok := f.spec.Escapes[seq]; ok {
			return ByteString(dl.byRole(role)), true
		}
		if isHexSeq(seq) {
			raw, _ := hex.DecodeString(seq[1:])
			return string(raw), true
		}
		return "", false
	})
}

// Encoder returns the escape-sequence encoder for dl: delimiter bytes and
// the escape character become table sequences, control characters become
// \Xhh\, and sequences the decoder preserved are re-emitted verbatim.
func (f *Format) Encoder(dl Delims) func(string) string {
	bodies := map[byte]string{}
	for body, role := range f.spec.Escapes {
		if b := dl.byRole(role); b != 0 {
			bodies[b] = body
		}
	}
	byteEncode := func(c byte) (string, bool) {
		if body, ok := bodies[c]; ok {
			return body, true
		}
		if c < 0x20 {
			return "X" + strings.ToUpper(hex.EncodeToString([]byte{c})), true
		}
		return "", false
	}
	preserved := func(seq string) bool {
		_, known := f.spec.Escapes[seq]
		return !known && !isHexSeq(seq)
	}
	return Encoder(dl.Esc, byteEncode, preserved)
}

func isHexSeq(seq string) bool {
	if len(seq) < 3 || seq[0] != 'X' || len(seq)%2 == 0 {
		return false
	}
	_, err := hex.DecodeString(seq[1:])
	return err == nil
}

func atoiZero(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
