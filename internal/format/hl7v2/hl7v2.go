// Package hl7v2 is the HL7 v2.x format module: a minimal, dependency-free
// parser and serializer over the generic message tree, with the 1-based path
// dialect "SEG[segOcc]-field[rep].component.subcomponent" (e.g. "PID-5.1",
// "OBX[2]-5", "PID-3[2].1"). Segment groups are not modeled: a message is an
// ordered list of segments, as in Mirth's default behavior.
//
// All of the machinery lives in the delimited package; this package is the
// HL7 specifics: three-letter segment names, MSH/BHS/FHS headers carrying
// the delimiters in the order "^~\&", and the \F\ \S\ \T\ \R\ \E\ escapes.
package hl7v2

import (
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/format/delimited"
	"github.com/langhorst/waggle/internal/message"
)

func init() { format.Register(DataType{}) }

// Format is the HL7 v2 delimited format.
var Format = delimited.NewFormat(delimited.Spec{
	Name:        "hl7v2",
	NamePattern: `[A-Z][A-Z0-9]{2}`,
	NameLen:     3,
	HeaderRaw:   headerRawFields,
	HeaderHint:  "MSH, BHS, or FHS",
	Defaults:    delimited.Delims{Field: '|', Rep: '~', Comp: '^', Sub: '&', Esc: '\\', HasSub: true},
	// MSH-2 is "^~\&": component, repetition, escape, subcomponent.
	EncodingOrder: []delimited.Role{delimited.RoleComp, delimited.RoleRep, delimited.RoleEsc, delimited.RoleSub},
	Escapes: map[string]delimited.Role{
		"F": delimited.RoleField,
		"S": delimited.RoleComp,
		"T": delimited.RoleSub,
		"R": delimited.RoleRep,
		"E": delimited.RoleEsc,
	},
})

// headerRawFields returns how many leading fields of a segment hold the
// delimiter characters verbatim: MSH-1/MSH-2 and the batch equivalents.
func headerRawFields(name string) int {
	switch name {
	case "MSH", "BHS", "FHS":
		return 2
	}
	return 0
}

// DataType implements format.DataType by delegating to Format.
type DataType struct{}

func (DataType) Name() string                                   { return Format.Name() }
func (DataType) Parse(raw []byte) (*message.Node, error)        { return Format.Parse(raw) }
func (DataType) Serialize(root *message.Node) ([]byte, error)   { return Format.Serialize(root) }
func (DataType) Value(root, n *message.Node) string             { return Format.Value(root, n) }
func (DataType) Flatten(root *message.Node) []message.PathValue { return Format.Flatten(root) }

func (DataType) Resolve(root *message.Node, path string) ([]*message.Node, error) {
	return Format.Resolve(root, path)
}

func (DataType) Set(root *message.Node, path string, value any) error {
	return Format.Set(root, path, value)
}

func (DataType) Segments(root *message.Node, name string) []*message.Node {
	return Format.Segments(root, name)
}

func (DataType) ResolveFrom(root, seg *message.Node, rel string) ([]*message.Node, error) {
	return Format.ResolveFrom(root, seg, rel)
}

func (DataType) SetFrom(root, seg *message.Node, rel string, value any) error {
	return Format.SetFrom(root, seg, rel, value)
}
