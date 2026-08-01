package astm

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

// sampleResult is a canonical E1394 result message: header, patient, order,
// two results (repeating record type), terminator. P-4 carries a repeating
// field, R-3 of the first R carries an escape sequence.
func sampleResult() []byte {
	return []byte(strings.Join([]string{
		`H|\^&|||LIS|||||||P|LIS2-A2|20260730120000`,
		`P|1||PATID123||DOE^JOHN||19800101|M`,
		`O|1|SPEC001||^^^GLU|R|20260730113000`,
		`R|1|^^^GLU|10&F&5|mg/dL||N||F||||20260730114500`,
		`R|2|^^^HBA1C|5.4|%||N||F||||20260730114500`,
		`L|1|N`,
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
	raw := sampleResult()
	out, err := dt.Serialize(mustParse(t, raw))
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestGoldenTree(t *testing.T) {
	root := mustParse(t, sampleResult())
	got, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	golden := filepath.Join("..", "..", "..", "testdata", "astm", "result.tree.json")
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
	root := mustParse(t, sampleResult())
	tests := []struct {
		path string
		want string
	}{
		{"H-1", "|"},
		{"H-2", `\^&`},
		{"H-5", "LIS"},
		{"H-13", "LIS2-A2"},
		{"P-5.2", "JOHN"},
		{"P-5", "DOE^JOHN"},
		{"O-4.4", "GLU"},
		{"R-3", "10|5"}, // &F& decoded to the field separator
		{"R[2]-3", "5.4"},
		{"R[2]-4", "%"},
		{"R-1.1", "1"}, // component 1 of a leaf is the leaf
		{"Q-1", ""},    // record absent
	}
	for _, tt := range tests {
		if got := get(t, root, tt.path); got != tt.want {
			t.Errorf("get(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
	// Record-occurrence wildcard: one value per R record.
	if got := getAll(t, root, "R-1"); len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Errorf("getAll(R-1) = %v, want [1 2]", got)
	}
}

func TestRepeatingField(t *testing.T) {
	raw := []byte("H|\\^&|||LIS\rP|1|||AID1\\AID2\r")
	root := mustParse(t, raw)
	if got := getAll(t, root, "P-4"); len(got) != 2 || got[0] != "AID1" || got[1] != "AID2" {
		t.Errorf("getAll(P-4) = %v, want both repeats", got)
	}
	if got := get(t, root, "P-4[2]"); got != "AID2" {
		t.Errorf("get(P-4[2]) = %q", got)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("repeat round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestSet(t *testing.T) {
	root := mustParse(t, sampleResult())

	if err := dt.Set(root, "P-5.1", "SMITH"); err != nil {
		t.Fatal(err)
	}
	if got := get(t, root, "P-5"); got != "SMITH^JOHN" {
		t.Errorf("after set P-5.1: P-5 = %q", got)
	}

	// Promotion on a leaf field.
	if err := dt.Set(root, "R-3.2", "H"); err != nil {
		t.Fatal(err)
	}
	if got := get(t, root, "R-3"); got != "10|5^H" {
		t.Errorf("after set R-3.2: R-3 = %q", got)
	}

	// Create a missing record type.
	if err := dt.Set(root, "C-2", "comment"); err != nil {
		t.Fatal(err)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("C||comment\r")) {
		t.Errorf("serialized output missing created C record: %q", out)
	}
}

func TestSubcomponentPathRejected(t *testing.T) {
	root := mustParse(t, sampleResult())
	if _, err := dt.Resolve(root, "R-3.1.2"); err == nil {
		t.Error("subcomponent path should be invalid for ASTM")
	}
}

func TestParseErrors(t *testing.T) {
	for name, raw := range map[string]string{
		"empty":     "",
		"no header": "P|1\r",
		"bad name":  "H|\\^&\r9|x\r",
	} {
		if _, err := dt.Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected parse error", name)
		}
	}
}

func FuzzParse(f *testing.F) {
	f.Add(sampleResult())
	f.Add([]byte("H|\\^&\rL|1|N\r"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		root, err := dt.Parse(raw)
		if err != nil {
			return
		}
		out, err := dt.Serialize(root)
		if err != nil {
			t.Fatalf("Serialize after successful Parse: %v", err)
		}
		if _, err := dt.Parse(out); err != nil {
			t.Fatalf("re-Parse of serialized output failed: %v\ninput: %q\noutput: %q", err, raw, out)
		}
	})
}
