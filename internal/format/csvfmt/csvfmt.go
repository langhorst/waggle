// Package csvfmt is the CSV format module: rows → columns over the generic
// message tree, with the path dialect "R[row].col" (1-based; "R.3" addresses
// column 3 of the first row when reading a single value, and of every row for
// getAll). Quoting follows RFC 4180 via encoding/csv; rows may have varying
// column counts. The canonical serialized form uses LF row terminators and
// minimal quoting.
package csvfmt

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
)

func init() { format.Register(DataType{}) }

// DataType implements format.DataType for CSV.
type DataType struct{}

func (DataType) Name() string { return "csv" }

func (DataType) Parse(raw []byte) (*message.Node, error) {
	r := csv.NewReader(bytes.NewReader(raw))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("csv: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("csv: empty message")
	}
	root := &message.Node{Name: "csv"}
	for _, row := range rows {
		rowNode := &message.Node{Name: "R"}
		for c, val := range row {
			// Normalize CR to LF inside field values: encoding/csv's reader
			// collapses CRLF inside quotes while its writer emits CR bytes
			// verbatim, so any surviving CR would make the canonical form
			// unstable across round trips.
			val = strings.ReplaceAll(val, "\r", "\n")
			rowNode.Children = append(rowNode.Children,
				&message.Node{Name: strconv.Itoa(c + 1), Value: val})
		}
		root.Children = append(root.Children, rowNode)
	}
	return root, nil
}

func (DataType) Serialize(root *message.Node) ([]byte, error) {
	if root == nil || len(root.Children) == 0 {
		return nil, fmt.Errorf("csv: empty tree")
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	for _, rowNode := range root.Children {
		row := make([]string, len(rowNode.Children))
		for i, col := range rowNode.Children {
			row[i] = col.Value
		}
		// A row that would serialize to a blank line (one empty field, or no
		// fields at all) must be written as `""`: csv readers skip blank
		// lines, which would silently drop the row on re-parse.
		if len(row) == 0 || (len(row) == 1 && row[0] == "") {
			w.Flush()
			buf.WriteString("\"\"\n")
			continue
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("csv: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("csv: %w", err)
	}
	return buf.Bytes(), nil
}

// pathRe: R[row].col — row omitted acts as a wildcard in Resolve and as row
// 1 in Set; the whole row is addressed when .col is omitted.
var pathRe = regexp.MustCompile(`^R(?:\[([1-9]\d*)\])?(?:\.([1-9]\d*))?$`)

type path struct{ row, col int }

func parsePath(p string) (path, error) {
	m := pathRe.FindStringSubmatch(p)
	if m == nil {
		return path{}, fmt.Errorf("csv: invalid path %q (expected R[row].col)", p)
	}
	return path{row: atoiZero(m[1]), col: atoiZero(m[2])}, nil
}

func atoiZero(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

func (DataType) Resolve(root *message.Node, pathExpr string) ([]*message.Node, error) {
	p, err := parsePath(pathExpr)
	if err != nil {
		return nil, err
	}
	var out []*message.Node
	for i, row := range root.Children {
		if p.row != 0 && i+1 != p.row {
			continue
		}
		if p.col == 0 {
			out = append(out, row)
			continue
		}
		if p.col <= len(row.Children) {
			out = append(out, row.Children[p.col-1])
		}
	}
	return out, nil
}

func (DataType) Set(root *message.Node, pathExpr, value string) error {
	p, err := parsePath(pathExpr)
	if err != nil {
		return err
	}
	if p.col == 0 {
		return fmt.Errorf("csv: path %q: set requires a column", pathExpr)
	}
	row := p.row
	if row == 0 {
		row = 1
	}
	for len(root.Children) < row {
		root.Children = append(root.Children, &message.Node{Name: "R"})
	}
	rowNode := root.Children[row-1]
	for len(rowNode.Children) < p.col {
		rowNode.Children = append(rowNode.Children,
			&message.Node{Name: strconv.Itoa(len(rowNode.Children) + 1)})
	}
	rowNode.Children[p.col-1].Value = value
	rowNode.Children[p.col-1].Children = nil
	return nil
}

// Segments returns the row nodes for name "R" (the only structural unit CSV
// has), matching the msg.segments('R') script idiom.
func (DataType) Segments(root *message.Node, name string) []*message.Node {
	if name != "R" {
		return nil
	}
	return append([]*message.Node(nil), root.Children...)
}

func (DataType) Value(root, n *message.Node) string {
	if n.IsLeaf() {
		return n.Value
	}
	// A row node renders as one CSV-encoded line without the terminator.
	row := make([]string, len(n.Children))
	for i, col := range n.Children {
		row[i] = col.Value
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(row); err != nil {
		return ""
	}
	w.Flush()
	return strings.TrimSuffix(buf.String(), "\n")
}

func (DataType) Flatten(root *message.Node) []message.PathValue {
	var out []message.PathValue
	for i, row := range root.Children {
		prefix := "R"
		if i > 0 {
			prefix = fmt.Sprintf("R[%d]", i+1)
		}
		for c, col := range row.Children {
			if col.Value != "" {
				out = append(out, message.PathValue{
					Path:  fmt.Sprintf("%s.%d", prefix, c+1),
					Value: col.Value,
				})
			}
		}
	}
	return out
}
