// Package astm is the ASTM E1394 (LIS02-A2) record-format module: records,
// fields, repetitions, components over the generic message tree, with the
// path dialect "REC[occ]-field[rep].component" (e.g. "H-5", "R[2]-4.1",
// "P-3[2].2"). Delimiters come from the H record ("H|\^&": field, repeat,
// component, escape).
//
// Field numbering counts from the first delimiter-separated field after the
// record type: "P|1||PATID" is P-1="1", P-3="PATID", mirroring the HL7
// dialect rather than E1394's convention of counting the record type itself
// as field 1. H-1 is the field separator and H-2 the remaining delimiter
// characters, exactly like MSH-1/MSH-2.
//
// All of the machinery lives in the delimited package; this package is the
// ASTM specifics: one-letter record names, an H header carrying the
// delimiters in the order "\^&", no subcomponent level, and the &F& &R&
// &S& &E& escapes.
//
// This module covers the record format only; the E1381 checksummed-frame
// transport lives in internal/adapter/astm1381 (astm-listener/astm-sender),
// and file adapters carry ASTM equally well.
package astm

import (
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/format/delimited"
	"github.com/langhorst/waggle/internal/message"
)

func init() { format.Register(DataType{}) }

// Format is the ASTM E1394 delimited format.
var Format = delimited.NewFormat(delimited.Spec{
	Name:        "astm",
	NamePattern: `[A-Z]`,
	NameLen:     1,
	HeaderRaw:   headerRawFields,
	HeaderHint:  "H",
	Defaults:    delimited.Delims{Field: '|', Rep: '\\', Comp: '^', Esc: '&', HasSub: false},
	// H-2 is "\^&": repetition, component, escape.
	EncodingOrder: []delimited.Role{delimited.RoleRep, delimited.RoleComp, delimited.RoleEsc},
	Escapes: map[string]delimited.Role{
		"F": delimited.RoleField,
		"R": delimited.RoleRep,
		"S": delimited.RoleComp,
		"E": delimited.RoleEsc,
	},
})

// headerRawFields: H-1 holds the field separator, H-2 the repeat/component/
// escape characters, both verbatim.
func headerRawFields(name string) int {
	if name == "H" {
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
