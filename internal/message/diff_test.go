package message

import (
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	tests := []struct {
		name string
		from []PathValue
		to   []PathValue
		want []DiffEntry
	}{
		{
			name: "identical",
			from: []PathValue{{"PID-5.1", "DOE"}},
			to:   []PathValue{{"PID-5.1", "DOE"}},
			want: nil,
		},
		{
			name: "changed",
			from: []PathValue{{"PID-5.1", "DOE"}},
			to:   []PathValue{{"PID-5.1", "SMITH"}},
			want: []DiffEntry{{Path: "PID-5.1", Op: DiffChanged, From: "DOE", To: "SMITH"}},
		},
		{
			name: "added and removed",
			from: []PathValue{{"PID-5.1", "DOE"}, {"PID-7", "20200101"}},
			to:   []PathValue{{"PID-5.1", "DOE"}, {"PID-8", "M"}},
			want: []DiffEntry{
				{Path: "PID-8", Op: DiffAdded, To: "M"},
				{Path: "PID-7", Op: DiffRemoved, From: "20200101"},
			},
		},
		{
			name: "document order preserved",
			from: []PathValue{{"MSH-10", "A"}, {"PID-5.1", "DOE"}},
			to:   []PathValue{{"MSH-10", "B"}, {"PID-5.1", "SMITH"}, {"PV1-2", "I"}},
			want: []DiffEntry{
				{Path: "MSH-10", Op: DiffChanged, From: "A", To: "B"},
				{Path: "PID-5.1", Op: DiffChanged, From: "DOE", To: "SMITH"},
				{Path: "PV1-2", Op: DiffAdded, To: "I"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Diff(tt.from, tt.to)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Diff() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNodeClone(t *testing.T) {
	orig := &Node{Name: "root", Children: []*Node{
		{Name: "PID", Children: []*Node{{Name: "1", Value: "x"}}},
	}}
	c := orig.Clone()
	c.Children[0].Children[0].Value = "changed"
	if orig.Children[0].Children[0].Value != "x" {
		t.Error("Clone shares memory with original")
	}
	if (*Node)(nil).Clone() != nil {
		t.Error("Clone of nil should be nil")
	}
}
