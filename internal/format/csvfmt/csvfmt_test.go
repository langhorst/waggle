package csvfmt

import (
	"bytes"
	"testing"

	"github.com/langhorst/waggle/internal/message"
)

var dt = DataType{}

// sample uses a quoted field containing the delimiter and rows of varying
// width. Canonical form: LF terminators, minimal quoting.
func sample() []byte {
	return []byte("name,dob,mrn\nDOE JOHN,2020-01-01,MRN12345\n\"SMITH, JANE\",1980-05-05,MRN99\n")
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

func TestRoundTrip(t *testing.T) {
	raw := sample()
	out, err := dt.Serialize(mustParse(t, raw))
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("round trip mismatch:\n got: %q\nwant: %q", out, raw)
	}
}

func TestResolve(t *testing.T) {
	root := mustParse(t, sample())
	tests := []struct {
		path string
		want string
	}{
		{"R.1", "name"},
		{"R[2].3", "MRN12345"},
		{"R[3].1", "SMITH, JANE"}, // unquoted in the tree
		{"R[4].1", ""},            // row absent
		{"R.9", ""},               // column absent
		{"R[3]", `"SMITH, JANE",1980-05-05,MRN99`}, // whole row re-encoded
	}
	for _, tt := range tests {
		if got := get(t, root, tt.path); got != tt.want {
			t.Errorf("get(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}

	// Row wildcard: column 3 of every row.
	nodes, err := dt.Resolve(root, "R.3")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 || nodes[1].Value != "MRN12345" || nodes[2].Value != "MRN99" {
		t.Errorf("Resolve(R.3) wildcard = %d nodes", len(nodes))
	}

	if _, err := dt.Resolve(root, "R[0].1"); err == nil {
		t.Error("expected error for zero index")
	}
	if _, err := dt.Resolve(root, "PID-5"); err == nil {
		t.Error("expected error for foreign dialect path")
	}
}

func TestSet(t *testing.T) {
	root := mustParse(t, sample())

	if err := dt.Set(root, "R[2].2", "2021-02-02"); err != nil {
		t.Fatal(err)
	}
	if got := get(t, root, "R[2].2"); got != "2021-02-02" {
		t.Errorf("after set: R[2].2 = %q", got)
	}

	// Auto-create a new row with padded columns.
	if err := dt.Set(root, "R[5].2", "created"); err != nil {
		t.Fatal(err)
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(out, []byte("\n\"\"\n,created\n")) {
		t.Errorf("expected empty row 4 as \"\" and padded row 5, got %q", out)
	}

	if err := dt.Set(root, "R[2]", "x"); err == nil {
		t.Error("Set at row level: expected error")
	}
}

func TestSegments(t *testing.T) {
	root := mustParse(t, sample())
	if got := len(dt.Segments(root, "R")); got != 3 {
		t.Errorf("Segments(R) = %d, want 3", got)
	}
	if dt.Segments(root, "X") != nil {
		t.Error("Segments with unknown name should be nil")
	}
}

func TestFlatten(t *testing.T) {
	root := mustParse(t, sample())
	flat := dt.Flatten(root)
	vals := map[string]string{}
	for _, pv := range flat {
		vals[pv.Path] = pv.Value
	}
	for path, want := range map[string]string{
		"R.1":    "name",
		"R[2].3": "MRN12345",
		"R[3].1": "SMITH, JANE",
	} {
		if vals[path] != want {
			t.Errorf("flatten[%q] = %q, want %q", path, vals[path], want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	if _, err := dt.Parse([]byte("")); err == nil {
		t.Error("empty input: expected error")
	}
	if _, err := dt.Parse([]byte("a,\"unterminated\n")); err == nil {
		t.Error("bad quoting: expected error")
	}
}

func FuzzParse(f *testing.F) {
	f.Add(sample())
	f.Add([]byte("a,b\nc\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		root, err := dt.Parse(raw)
		if err != nil {
			return
		}
		out, err := dt.Serialize(root)
		if err != nil {
			t.Fatalf("Serialize after successful Parse: %v", err)
		}
		reparsed, err := dt.Parse(out)
		if err != nil {
			t.Fatalf("re-Parse of serialized output failed: %v\ninput: %q\noutput: %q", err, raw, out)
		}
		// CSV canonicalization is idempotent: a second round trip is stable.
		out2, err := dt.Serialize(reparsed)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out, out2) {
			t.Fatalf("canonical form not stable: %q vs %q", out, out2)
		}
	})
}
