// Package xmlfmt is the XML format module. Elements become nodes named by
// their tag exactly as written (namespace prefixes preserved verbatim —
// xmlns declarations are ordinary attributes that round-trip untouched),
// attributes become kind-"attr" children, and simple text content lives in
// the element's own Value. Mixed content keeps its interleaving through
// ordered "#text" children.
//
// The path dialect is XPath-flavored and, like XPath, 1-based:
// "Patient/name[1]/family", "Patient/@id", "entry/resource/id/@value". An
// omitted [n] fans out across same-name siblings the way a repetition-less
// HL7 path does. This deliberately differs from the 0-based JSON dialect —
// each dialect counts the way its format's native tooling counts.
//
// The canonical form is compact: whitespace-only text between elements,
// comments, processing instructions, and the DOCTYPE are dropped at parse;
// serialize emits no formatting whitespace, preserves attribute order,
// renders empty elements self-closed, and re-escapes CDATA as ordinary
// text. The XML declaration is kept. A document already in canonical form
// round-trips byte-exact.
package xmlfmt

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
)

// Node kinds used in message.Node.Kind.
const (
	KindElement = "element"
	KindAttr    = "attr"
	KindText    = "text"
	KindDecl    = "decl"
)

func init() { format.Register(DataType{}) }

// DataType implements format.DataType (and format.SegmentJoiner) for XML.
type DataType struct{}

func (DataType) Name() string { return "xml" }

// JoinSegmentPath implements format.SegmentJoiner: segment-relative script
// paths join with the dialect's step separator (seg.get('id/@value') on an
// "entry" handle resolves entry/id/@value against that occurrence).
func (DataType) JoinSegmentPath(segName, rel string) string {
	return segName + "/" + rel
}

// ---- parse ----

// nsFrame is one element's namespace declarations, innermost last on the
// stack: prefix "" is the default namespace.
type nsFrame []struct{ prefix, uri string }

type parser struct {
	dec   *xml.Decoder
	stack []nsFrame
}

// MaxDepth bounds element nesting. element recurses once per level, and a
// payload of nothing but "<a><a><a>" within the transport's size limit
// would otherwise overflow the goroutine stack, which is a fatal error
// rather than a recoverable panic.
const MaxDepth = 512

func (DataType) Parse(raw []byte) (*message.Node, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("xml: empty message")
	}
	p := &parser{dec: xml.NewDecoder(bytes.NewReader(raw))}
	p.dec.Strict = true
	root := &message.Node{Name: "xml"}

	sawElement := false
	for {
		tok, err := p.dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xml: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if sawElement {
				return nil, fmt.Errorf("xml: multiple document elements")
			}
			sawElement = true
			el, err := p.element(t)
			if err != nil {
				return nil, fmt.Errorf("xml: %w", err)
			}
			root.Children = append(root.Children, el)
		case xml.ProcInst:
			// Keep the declaration (before the document element); drop
			// other processing instructions (canonical form).
			if t.Target == "xml" && !sawElement {
				root.Children = append(root.Children,
					&message.Node{Name: "?xml", Kind: KindDecl, Value: string(t.Inst)})
			}
		case xml.CharData:
			if len(bytes.TrimSpace(t)) > 0 {
				return nil, fmt.Errorf("xml: text outside the document element")
			}
		case xml.Comment, xml.Directive:
			// Comments and <!DOCTYPE ...> are dropped (canonical form).
		}
	}
	if !sawElement {
		return nil, fmt.Errorf("xml: no document element")
	}
	return root, nil
}

// element consumes one element (start tag already read) into a node.
func (p *parser) element(start xml.StartElement) (*message.Node, error) {
	if len(p.stack) >= MaxDepth {
		return nil, fmt.Errorf("nesting deeper than %d levels", MaxDepth)
	}
	frame := nsFrame{}
	for _, a := range start.Attr {
		switch {
		case a.Name.Space == "" && a.Name.Local == "xmlns":
			frame = append(frame, struct{ prefix, uri string }{"", a.Value})
		case a.Name.Space == "xmlns":
			frame = append(frame, struct{ prefix, uri string }{a.Name.Local, a.Value})
		}
	}
	p.stack = append(p.stack, frame)
	defer func() { p.stack = p.stack[:len(p.stack)-1] }()

	n := &message.Node{Name: p.elementName(start.Name), Kind: KindElement}
	for _, a := range start.Attr {
		n.Children = append(n.Children,
			&message.Node{Name: p.attrName(a.Name), Kind: KindAttr, Value: a.Value})
	}

	// Collect content in order: text runs and child elements.
	type item struct {
		text string
		el   *message.Node
	}
	var items []item
	for {
		tok, err := p.dec.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			child, err := p.element(t)
			if err != nil {
				return nil, err
			}
			items = append(items, item{el: child})
		case xml.CharData:
			// Whitespace-only runs are formatting; drop them (canonical).
			if len(bytes.TrimSpace(t)) > 0 {
				items = append(items, item{text: string(t)})
			}
		case xml.EndElement:
			hasElements := false
			for _, it := range items {
				if it.el != nil {
					hasElements = true
					break
				}
			}
			if !hasElements {
				// Simple content: concatenated text lives in Value.
				var b strings.Builder
				for _, it := range items {
					b.WriteString(it.text)
				}
				n.Value = b.String()
				return n, nil
			}
			for _, it := range items {
				if it.el != nil {
					n.Children = append(n.Children, it.el)
					continue
				}
				n.Children = append(n.Children,
					&message.Node{Name: "#text", Kind: KindText, Value: it.text})
			}
			return n, nil
		case xml.Comment, xml.ProcInst, xml.Directive:
			// Dropped (canonical form).
		}
	}
}

// elementName renders a decoder name back to its as-written form: the
// decoder resolves prefixes to namespace URIs, so map the URI back to the
// innermost declared prefix. An unresolvable Space is a literal prefix the
// document never declared — encoding/xml passes those through verbatim.
func (p *parser) elementName(name xml.Name) string {
	if name.Space == "" {
		return name.Local
	}
	if prefix, ok := p.lookupPrefix(name.Space, true); ok {
		if prefix == "" {
			return name.Local // default namespace
		}
		return prefix + ":" + name.Local
	}
	return name.Space + ":" + name.Local
}

// attrName is elementName for attributes: they never use the default
// namespace, and xmlns declarations pass through as-is.
func (p *parser) attrName(name xml.Name) string {
	switch {
	case name.Space == "":
		return name.Local
	case name.Space == "xmlns":
		return "xmlns:" + name.Local
	}
	if prefix, ok := p.lookupPrefix(name.Space, false); ok {
		return prefix + ":" + name.Local
	}
	return name.Space + ":" + name.Local
}

// lookupPrefix finds the innermost prefix bound to uri. allowDefault
// includes the default-namespace binding (elements only).
func (p *parser) lookupPrefix(uri string, allowDefault bool) (string, bool) {
	for i := len(p.stack) - 1; i >= 0; i-- {
		frame := p.stack[i]
		for j := len(frame) - 1; j >= 0; j-- {
			if frame[j].uri != uri {
				continue
			}
			if frame[j].prefix == "" && !allowDefault {
				continue
			}
			return frame[j].prefix, true
		}
	}
	return "", false
}

// ---- serialize ----

func (DataType) Serialize(root *message.Node) ([]byte, error) {
	if root == nil {
		return nil, fmt.Errorf("xml: empty tree")
	}
	var buf bytes.Buffer
	sawElement := false
	for _, c := range root.Children {
		if c.Kind == KindDecl {
			buf.WriteString("<?xml")
			if c.Value != "" {
				buf.WriteByte(' ')
				buf.WriteString(c.Value)
			}
			buf.WriteString("?>")
			continue
		}
		if sawElement {
			return nil, fmt.Errorf("xml: multiple document elements")
		}
		sawElement = true
		if err := encodeElement(&buf, c); err != nil {
			return nil, fmt.Errorf("xml: %w", err)
		}
	}
	if !sawElement {
		return nil, fmt.Errorf("xml: empty document")
	}
	return buf.Bytes(), nil
}

func encodeElement(buf *bytes.Buffer, n *message.Node) error {
	if err := validName(n.Name); err != nil {
		return err
	}
	buf.WriteByte('<')
	buf.WriteString(n.Name)
	var content []*message.Node
	for _, c := range n.Children {
		if c.Kind == KindAttr {
			if err := validName(c.Name); err != nil {
				return err
			}
			buf.WriteByte(' ')
			buf.WriteString(c.Name)
			buf.WriteString(`="`)
			buf.WriteString(escapeAttr(c.Value))
			buf.WriteByte('"')
			continue
		}
		content = append(content, c)
	}
	if len(content) == 0 && n.Value == "" {
		buf.WriteString("/>")
		return nil
	}
	buf.WriteByte('>')
	if len(content) == 0 {
		buf.WriteString(escapeText(n.Value))
	}
	for _, c := range content {
		if c.Kind == KindText || c.Name == "#text" {
			buf.WriteString(escapeText(c.Value))
			continue
		}
		if err := encodeElement(buf, c); err != nil {
			return err
		}
	}
	buf.WriteString("</")
	buf.WriteString(n.Name)
	buf.WriteByte('>')
	return nil
}

func validName(name string) error {
	if name == "" || strings.ContainsAny(name, "<>&\"'=/ \t\n\r") {
		return fmt.Errorf("invalid element or attribute name %q", name)
	}
	return nil
}

// escapeText escapes text content: markup characters become entities and a
// bare CR becomes numeric (the parser would normalize it to LF), while LF
// and TAB stay literal so canonical documents round-trip byte-exact.
func escapeText(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '\r':
			b.WriteString("&#xD;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// escapeAttr additionally escapes the quote and whitespace controls, which
// attribute-value normalization would otherwise rewrite on reparse.
func escapeAttr(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		case '\n':
			b.WriteString("&#xA;")
		case '\t':
			b.WriteString("&#x9;")
		case '\r':
			b.WriteString("&#xD;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ---- paths ----

func (DataType) Resolve(root *message.Node, pathExpr string) ([]*message.Node, error) {
	steps, err := parsePath(pathExpr)
	if err != nil {
		return nil, err
	}
	nodes := []*message.Node{root}
	for _, s := range steps {
		var next []*message.Node
		for _, n := range nodes {
			if s.isAttr {
				for _, c := range n.Children {
					if c.Kind == KindAttr && c.Name == s.name {
						next = append(next, c)
					}
				}
				continue
			}
			next = append(next, matchChildren(n, s)...)
		}
		nodes = next
	}
	return nodes, nil
}

// matchChildren returns n's content children named s.name, filtered to the
// 1-based occurrence when one is given.
func matchChildren(n *message.Node, s step) []*message.Node {
	var out []*message.Node
	occ := 0
	for _, c := range n.Children {
		if c.Kind == KindAttr || c.Kind == KindDecl || c.Name != s.name {
			continue
		}
		occ++
		if s.occurrence == 0 || occ == s.occurrence {
			out = append(out, c)
		}
	}
	return out
}

func (DataType) Set(root *message.Node, pathExpr, value string) error {
	steps, err := parsePath(pathExpr)
	if err != nil {
		return err
	}
	if len(steps) == 0 {
		return fmt.Errorf("xml: empty path")
	}
	cur := root
	for i, s := range steps {
		if s.isAttr {
			if i != len(steps)-1 {
				return fmt.Errorf("xml: path %q: @%s must be the final step", pathExpr, s.name)
			}
			if cur == root {
				return fmt.Errorf("xml: path %q: cannot set an attribute on the document root", pathExpr)
			}
			setAttr(cur, s.name, value)
			return nil
		}
		occ := s.occurrence
		if occ == 0 {
			occ = 1
		}
		cur = childOccurrence(cur, s.name, occ)
	}
	// Setting an element's text replaces its content but keeps attributes.
	cur.Value = value
	var attrs []*message.Node
	for _, c := range cur.Children {
		if c.Kind == KindAttr {
			attrs = append(attrs, c)
		}
	}
	cur.Children = attrs
	return nil
}

// setAttr overwrites or appends an attribute, keeping attributes grouped
// before content children.
func setAttr(n *message.Node, name, value string) {
	lastAttr := -1
	for i, c := range n.Children {
		if c.Kind == KindAttr {
			lastAttr = i
			if c.Name == name {
				c.Value = value
				return
			}
		}
	}
	attr := &message.Node{Name: name, Kind: KindAttr, Value: value}
	n.Children = append(n.Children, nil)
	copy(n.Children[lastAttr+2:], n.Children[lastAttr+1:])
	n.Children[lastAttr+1] = attr
}

// childOccurrence finds the occ-th element child named name, appending
// empty siblings until it exists (autovivification).
func childOccurrence(n *message.Node, name string, occ int) *message.Node {
	seen := 0
	for _, c := range n.Children {
		if c.Kind == KindAttr || c.Name != name {
			continue
		}
		seen++
		if seen == occ {
			return c
		}
	}
	var last *message.Node
	for seen < occ {
		last = &message.Node{Name: name, Kind: KindElement}
		n.Children = append(n.Children, last)
		seen++
	}
	return last
}

// Segments returns the document element for its own name, or its child
// elements matching name (msg.segments('entry') on a Bundle).
func (DataType) Segments(root *message.Node, name string) []*message.Node {
	doc := documentElement(root)
	if doc == nil {
		return nil
	}
	if doc.Name == name {
		return []*message.Node{doc}
	}
	var out []*message.Node
	for _, c := range doc.Children {
		if c.Kind != KindAttr && c.Kind != KindText && c.Name == name {
			out = append(out, c)
		}
	}
	return out
}

func documentElement(root *message.Node) *message.Node {
	for _, c := range root.Children {
		if c.Kind == KindElement {
			return c
		}
	}
	return nil
}

// Value renders attributes and text as their value, a simple-content
// element as its text, and an element with child elements as compact XML.
func (DataType) Value(root, n *message.Node) string {
	switch n.Kind {
	case KindAttr, KindText, KindDecl:
		return n.Value
	}
	hasElements := false
	for _, c := range n.Children {
		if c.Kind != KindAttr {
			hasElements = true
			break
		}
	}
	if !hasElements {
		return n.Value
	}
	var buf bytes.Buffer
	if err := encodeElement(&buf, n); err != nil {
		return ""
	}
	return buf.String()
}

func (DataType) Flatten(root *message.Node) []message.PathValue {
	var out []message.PathValue
	if doc := documentElement(root); doc != nil {
		flattenElement(doc, doc.Name, &out)
	}
	return out
}

// flattenElement emits attribute and text leaves in document order. Like
// the other dialects' diff output, a first occurrence omits its [1].
func flattenElement(n *message.Node, path string, out *[]message.PathValue) {
	occ := map[string]int{}
	for _, c := range n.Children {
		switch c.Kind {
		case KindAttr:
			if c.Value != "" {
				*out = append(*out, message.PathValue{Path: path + "/@" + c.Name, Value: c.Value})
			}
		default:
			occ[c.Name]++
			childPath := path + "/" + c.Name
			if occ[c.Name] > 1 {
				childPath += "[" + strconv.Itoa(occ[c.Name]) + "]"
			}
			if c.Kind == KindText || c.Name == "#text" {
				if c.Value != "" {
					*out = append(*out, message.PathValue{Path: childPath, Value: c.Value})
				}
				continue
			}
			flattenElement(c, childPath, out)
		}
	}
	if n.Value != "" {
		*out = append(*out, message.PathValue{Path: path, Value: n.Value})
	}
}
