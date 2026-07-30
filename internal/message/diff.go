package message

// PathValue is one leaf of a flattened message tree, with the path rendered
// in the owning format's dialect (e.g. "PID-5.1", "R[2].3"). Format modules
// produce these via Flatten; the structural diff consumes them.
type PathValue struct {
	Path  string `json:"path"`
	Value string `json:"value"`
}

// DiffOp classifies one entry of a structural diff.
type DiffOp string

const (
	DiffAdded   DiffOp = "added"
	DiffRemoved DiffOp = "removed"
	DiffChanged DiffOp = "changed"
)

// DiffEntry is one difference between two flattened trees.
type DiffEntry struct {
	Path string `json:"path"`
	Op   DiffOp `json:"op"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// Diff computes the structural difference between two flattened trees.
// Entries follow document order: additions and changes in the order they
// appear in to, then removals in the order they appear in from. Paths present
// on both sides with equal values produce no entry.
func Diff(from, to []PathValue) []DiffEntry {
	fromVals := make(map[string]string, len(from))
	for _, pv := range from {
		fromVals[pv.Path] = pv.Value
	}
	toVals := make(map[string]string, len(to))
	for _, pv := range to {
		toVals[pv.Path] = pv.Value
	}

	var entries []DiffEntry
	for _, pv := range to {
		old, ok := fromVals[pv.Path]
		switch {
		case !ok:
			entries = append(entries, DiffEntry{Path: pv.Path, Op: DiffAdded, To: pv.Value})
		case old != pv.Value:
			entries = append(entries, DiffEntry{Path: pv.Path, Op: DiffChanged, From: old, To: pv.Value})
		}
	}
	for _, pv := range from {
		if _, ok := toVals[pv.Path]; !ok {
			entries = append(entries, DiffEntry{Path: pv.Path, Op: DiffRemoved, From: pv.Value})
		}
	}
	return entries
}
