package hl7v2

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/message"
)

var update = flag.Bool("update", false, "rewrite golden files")

var dt = DataType{}

// sampleADT is a canonical ADT^A01 with repeating fields (PID-3), repeating
// segments (OBX), components, subcomponents (OBX-3 of the second OBX), and an
// escape sequence (\T\ in OBX-5 of the first OBX).
func sampleADT() []byte {
	return []byte(strings.Join([]string{
		`MSH|^~\&|SENDAPP|SENDFAC|RECVAPP|RECVFAC|20260730120000||ADT^A01^ADT_A01|MSG00001|P|2.5.1`,
		`EVN|A01|20260730120000`,
		`PID|1||MRN12345^^^HOSP^MR~SSN987654^^^USA^SS||DOE^JOHN^Q^JR||20200101|M|||123 MAIN ST^^METROPOLIS^IL^62960`,
		`OBX|1|TX|NOTE^Progress Note||Patient admitted \T\ stable`,
		`OBX|2|NM|WT&BODY^Weight||72.5|kg`,
	}, "\r") + "\r")
}

func mustParse(t *testing.T, raw []byte) *message.Node {
	t.Helper()
	root, err := dt.Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return root
}

// get returns the first resolved value for path, or "" when missing.
func get(t *testing.T, root *message.Node, path string) string {
	t.Helper()
	nodes, err := dt.Resolve(root, path)
	if err != nil {
		t.Fatalf("Resolve(%q): %v", path, err)
	}
	if len(nodes) == 0 {
		return ""
	}
	return dt.Value(root, nodes[0])
}

func getAll(t *testing.T, root *message.Node, path string) []string {
	t.Helper()
	nodes, err := dt.Resolve(root, path)
	if err != nil {
		t.Fatalf("Resolve(%q): %v", path, err)
	}
	vals := make([]string, len(nodes))
	for i, n := range nodes {
		vals[i] = dt.Value(root, n)
	}
	return vals
}

func TestRoundTrip(t *testing.T) {
	raw := sampleADT()
	out, err := dt.Serialize(mustParse(t, raw))
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestParseAcceptsAnyLineEnding(t *testing.T) {
	canonical := sampleADT()
	for _, ending := range []string{"\n", "\r\n"} {
		variant := bytes.ReplaceAll(canonical, []byte("\r"), []byte(ending))
		out, err := dt.Serialize(mustParse(t, variant))
		if err != nil {
			t.Fatalf("Serialize (%q endings): %v", ending, err)
		}
		if !bytes.Equal(out, canonical) {
			t.Errorf("parse with %q endings did not normalize to canonical CR form", ending)
		}
	}
}

func TestGoldenTree(t *testing.T) {
	root := mustParse(t, sampleADT())
	got, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	golden := filepath.Join("..", "..", "..", "testdata", "hl7v2", "adt_a01.tree.json")
	if *update {
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("tree JSON differs from golden %s (run with -update to refresh)", golden)
	}
}

func TestResolve(t *testing.T) {
	root := mustParse(t, sampleADT())
	tests := []struct {
		path string
		want string
	}{
		{"MSH-9.1", "ADT"},
		{"MSH-9", "ADT^A01^ADT_A01"},
		{"MSH-1", "|"},
		{"MSH-2", `^~\&`},
		{"MSH-12", "2.5.1"},
		{"PID-5.2", "JOHN"},
		{"PID-5", "DOE^JOHN^Q^JR"},
		{"PID-3", "MRN12345^^^HOSP^MR"}, // first repetition
		{"PID-3[2].1", "SSN987654"},
		{"PID-7", "20200101"},
		{"PID-7.1", "20200101"},                // component 1 of a leaf is the leaf itself
		{"PID-7.2", ""},                        // beyond a leaf: missing
		{"OBX-5", "Patient admitted & stable"}, // \T\ decoded
		{"OBX[2]-5", "72.5"},
		{"OBX[2]-3.1.2", "BODY"}, // subcomponent
		{"OBX[2]-3.1.1", "WT"},
		{"OBX[3]-1", ""}, // no third OBX
		{"ZZZ-1", ""},    // segment absent
		{"PID-99", ""},   // field absent
	}
	for _, tt := range tests {
		if got := get(t, root, tt.path); got != tt.want {
			t.Errorf("get(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestResolveWildcards(t *testing.T) {
	root := mustParse(t, sampleADT())
	if got := getAll(t, root, "PID-3"); len(got) != 2 || got[0] != "MRN12345^^^HOSP^MR" || got[1] != "SSN987654^^^USA^SS" {
		t.Errorf("getAll(PID-3) = %v, want both repetitions", got)
	}
	// Segment occurrence omitted: one value per OBX segment.
	if got := getAll(t, root, "OBX-1"); len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Errorf("getAll(OBX-1) = %v, want [1 2]", got)
	}
}

func TestResolveInvalidPath(t *testing.T) {
	root := mustParse(t, sampleADT())
	for _, p := range []string{"pid-5", "PID-0", "PID-5.0", "PID", "-5", "PID-5.1.2.3", "PID[0]-1"} {
		if p == "PID" {
			continue // bare segment path is valid
		}
		if _, err := dt.Resolve(root, p); err == nil {
			t.Errorf("Resolve(%q): expected error", p)
		}
	}
	// Bare segment path resolves to the segment node.
	nodes, err := dt.Resolve(root, "PID")
	if err != nil || len(nodes) != 1 {
		t.Fatalf("Resolve(PID) = %v, %v; want one segment", nodes, err)
	}
	if v := dt.Value(root, nodes[0]); !strings.HasPrefix(v, "PID|1||MRN12345") {
		t.Errorf("Value(PID segment) = %q", v)
	}
}

func TestSet(t *testing.T) {
	root := mustParse(t, sampleADT())

	if err := dt.Set(root, "PID-5.1", "SMITH"); err != nil {
		t.Fatal(err)
	}
	if got := get(t, root, "PID-5"); got != "SMITH^JOHN^Q^JR" {
		t.Errorf("after set PID-5.1: PID-5 = %q", got)
	}

	// Promotion: setting component 2 of a leaf field keeps the old value as
	// component 1.
	if err := dt.Set(root, "PID-7.2", "EST"); err != nil {
		t.Fatal(err)
	}
	if got := get(t, root, "PID-7"); got != "20200101^EST" {
		t.Errorf("after set PID-7.2: PID-7 = %q", got)
	}

	// Auto-create a missing segment with padded fields.
	if err := dt.Set(root, "PV1-2", "I"); err != nil {
		t.Fatal(err)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("PV1||I\r")) {
		t.Errorf("serialized output missing created PV1 segment: %q", out)
	}

	// Auto-create a third OBX; it must sit adjacent to the existing OBX group.
	if err := dt.Set(root, "OBX[3]-1", "3"); err != nil {
		t.Fatal(err)
	}
	if got := getAll(t, root, "OBX-1"); len(got) != 3 || got[2] != "3" {
		t.Errorf("after set OBX[3]-1: getAll(OBX-1) = %v", got)
	}

	// Repetition create: second repetition of a currently single-rep field.
	if err := dt.Set(root, "PID-13[2]", "555-0102"); err != nil {
		t.Fatal(err)
	}
	if got := getAll(t, root, "PID-13"); len(got) != 2 || got[1] != "555-0102" {
		t.Errorf("after set PID-13[2]: getAll(PID-13) = %v", got)
	}

	// Setting at segment level is rejected.
	if err := dt.Set(root, "PID", "x"); err == nil {
		t.Error("Set at segment level: expected error")
	}
}

func TestEscapeRoundTrip(t *testing.T) {
	raw := []byte("MSH|^~\\&|APP|FAC|APP2|FAC2|20260730||ORU^R01|1|P|2.5\r" +
		"OBX|1|TX|N||a\\F\\b\\S\\c\\T\\d\\R\\e\\E\\f\\X0A\\g\r")
	root := mustParse(t, raw)
	want := "a|b^c&d~e\\f\ng"
	if got := get(t, root, "OBX-5"); got != want {
		t.Errorf("decoded OBX-5 = %q, want %q", got, want)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("escape round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestUnknownEscapePreserved(t *testing.T) {
	raw := []byte("MSH|^~\\&|A|B|C|D|1||ACK|1|P|2.5\rNTE|1||before \\H\\bold\\N\\ after\r")
	root := mustParse(t, raw)
	if got := get(t, root, "NTE-3"); got != `before \H\bold\N\ after` {
		t.Errorf("unknown escapes should be preserved verbatim, got %q", got)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("unknown-escape round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestCustomDelimiters(t *testing.T) {
	raw := []byte("MSH#*!?$#APP#FAC#R1#R2#20260730##ADT*A01#1#P#2.5\rPID#1##ID1!ID2##DOE*JOHN\r")
	root := mustParse(t, raw)
	if got := get(t, root, "PID-5.2"); got != "JOHN" {
		t.Errorf("custom delims: PID-5.2 = %q", got)
	}
	if got := getAll(t, root, "PID-3"); len(got) != 2 || got[1] != "ID2" {
		t.Errorf("custom delims: PID-3 repetitions = %v", got)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("custom-delimiter round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestSegments(t *testing.T) {
	root := mustParse(t, sampleADT())
	if got := len(dt.Segments(root, "OBX")); got != 2 {
		t.Errorf("Segments(OBX) = %d, want 2", got)
	}
	if got := len(dt.Segments(root, "ZZZ")); got != 0 {
		t.Errorf("Segments(ZZZ) = %d, want 0", got)
	}
}

func TestFlattenAndDiff(t *testing.T) {
	before := mustParse(t, sampleADT())
	after := before.Clone()
	if err := dt.Set(after, "PID-5.1", "SMITH"); err != nil {
		t.Fatal(err)
	}
	if err := dt.Set(after, "PV1-2", "I"); err != nil {
		t.Fatal(err)
	}
	diff := message.Diff(dt.Flatten(before), dt.Flatten(after))

	byPath := map[string]message.DiffEntry{}
	for _, e := range diff {
		byPath[e.Path] = e
	}
	if e := byPath["PID-5.1"]; e.Op != message.DiffChanged || e.From != "DOE" || e.To != "SMITH" {
		t.Errorf("diff PID-5.1 = %+v", e)
	}
	if e := byPath["PV1-2"]; e.Op != message.DiffAdded || e.To != "I" {
		t.Errorf("diff PV1-2 = %+v", e)
	}
	if len(diff) != 2 {
		t.Errorf("expected exactly 2 diff entries, got %+v", diff)
	}
}

func TestFlattenPaths(t *testing.T) {
	root := mustParse(t, sampleADT())
	flat := dt.Flatten(root)
	vals := map[string]string{}
	for _, pv := range flat {
		vals[pv.Path] = pv.Value
	}
	for path, want := range map[string]string{
		"MSH-9.1":      "ADT",
		"PID-3.1":      "MRN12345",
		"PID-3[2].1":   "SSN987654",
		"OBX-5":        "Patient admitted & stable",
		"OBX[2]-5":     "72.5",
		"OBX[2]-3.1.2": "BODY",
	} {
		if vals[path] != want {
			t.Errorf("flatten[%q] = %q, want %q", path, vals[path], want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for name, raw := range map[string]string{
		"empty":          "",
		"no header":      "PID|1|X\r",
		"short header":   "MS\r",
		"bad seg name":   "MSH|^~\\&|A|B|C|D|1||ACK|1|P|2.5\rp1d|x\r",
		"short segment":  "MSH|^~\\&|A|B|C|D|1||ACK|1|P|2.5\rPI\r",
		"wrong fieldsep": "MSH|^~\\&|A|B|C|D|1||ACK|1|P|2.5\rPID^1\r",
	} {
		if _, err := dt.Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected parse error", name)
		}
	}
}

func FuzzParse(f *testing.F) {
	f.Add(sampleADT())
	f.Add([]byte("MSH|^~\\&|A\r"))
	f.Add([]byte("MSH#*!?$#A#B\rPID#1\r"))
	f.Add([]byte("MSH|^~\\&|\\X0D\\\\E\\|B\r"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		root, err := dt.Parse(raw)
		if err != nil {
			return
		}
		out, err := dt.Serialize(root)
		if err != nil {
			t.Fatalf("Serialize after successful Parse: %v", err)
		}
		// Serialized output must itself re-parse.
		if _, err := dt.Parse(out); err != nil {
			t.Fatalf("re-Parse of serialized output failed: %v\ninput: %q\noutput: %q", err, raw, out)
		}
	})
}
