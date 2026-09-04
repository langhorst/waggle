package jsonfmt

import (
	"bytes"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/format"
)

var dt = DataType{}

const patient = `{"resourceType":"Patient","id":"pat-1","active":true,` +
	`"identifier":[{"system":"urn:mrn","value":"12345"}],` +
	`"name":[{"family":"Doe","given":["John","Q"]},{"family":"Dough"}],` +
	`"birthDate":"1970-01-01","multipleBirthInteger":2,"deceasedBoolean":false,` +
	`"maritalStatus":null}`

func TestParseSerializeGolden(t *testing.T) {
	for _, raw := range []string{
		patient,
		`{}`,
		`[]`,
		`[1,2.5,-3e2,"x",true,null]`,
		`{"a":[1]}`,
		`{"a":1}`,
		`{"nested":{"deep":{"leaf":"v"}}}`,
		`{"esc":"line\nbreak \"quoted\" \\ <tag>"}`,
		`"top-level string"`,
		`42`,
		`{"big":9007199254740993,"exp":1e5,"neg":-0.25}`,
		`{"dup":1,"dup":2}`,
		`{"unicode":"héllo — 🐝"}`,
	} {
		root, err := dt.Parse([]byte(raw))
		if err != nil {
			t.Errorf("Parse(%s): %v", raw, err)
			continue
		}
		out, err := dt.Serialize(root)
		if err != nil {
			t.Errorf("Serialize(%s): %v", raw, err)
			continue
		}
		if string(out) != raw {
			t.Errorf("round trip:\n in  %s\n out %s", raw, out)
		}
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, raw := range []string{``, `   `, `{`, `{"a":}`, `{"a":1}extra`, `[1,]`, `nul`} {
		if _, err := dt.Parse([]byte(raw)); err == nil {
			t.Errorf("Parse(%q): expected error", raw)
		}
	}
}

// TestParseDepthLimit: nesting beyond MaxDepth is a parse error, not a
// stack overflow. The rejected input is far smaller than any transport's
// body limit, so this is the only thing standing between a hostile payload
// and a dead process.
func TestParseDepthLimit(t *testing.T) {
	deep := strings.Repeat("[", MaxDepth+1) + strings.Repeat("]", MaxDepth+1)
	if _, err := dt.Parse([]byte(deep)); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("Parse(%d levels): want nesting error, got %v", MaxDepth+1, err)
	}
	objects := strings.Repeat(`{"a":`, MaxDepth+1) + "1" + strings.Repeat("}", MaxDepth+1)
	if _, err := dt.Parse([]byte(objects)); err == nil {
		t.Fatalf("Parse(%d object levels): want error", MaxDepth+1)
	}
	// Exactly at the limit still parses.
	ok := strings.Repeat("[", MaxDepth) + strings.Repeat("]", MaxDepth)
	if _, err := dt.Parse([]byte(ok)); err != nil {
		t.Fatalf("Parse(%d levels): %v", MaxDepth, err)
	}
	// A megabyte of open brackets, the shape of the original crash.
	huge := strings.Repeat("[", 1<<20)
	if _, err := dt.Parse([]byte(huge)); err == nil {
		t.Fatal("Parse(1 MiB of '['): want error")
	}
}

func TestTypePreservation(t *testing.T) {
	root, err := dt.Parse([]byte(`{"count":5,"active":true,"note":"5","gone":null}`))
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := map[string]string{"count": KindNumber, "active": KindBool, "note": KindString, "gone": KindNull}
	for _, c := range root.Children {
		if c.Kind != wantKinds[c.Name] {
			t.Errorf("%s: kind %q, want %q", c.Name, c.Kind, wantKinds[c.Name])
		}
	}
	out, _ := dt.Serialize(root)
	if string(out) != `{"count":5,"active":true,"note":"5","gone":null}` {
		t.Errorf("serialized: %s", out)
	}
}

func TestResolve(t *testing.T) {
	root, err := dt.Parse([]byte(patient))
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) string {
		t.Helper()
		nodes, err := dt.Resolve(root, path)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", path, err)
		}
		if len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}
	getAll := func(path string) []string {
		t.Helper()
		nodes, err := dt.Resolve(root, path)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", path, err)
		}
		var vals []string
		for _, n := range nodes {
			vals = append(vals, dt.Value(root, n))
		}
		return vals
	}

	if v := get("id"); v != "pat-1" {
		t.Errorf("id = %q", v)
	}
	if v := get("active"); v != "true" {
		t.Errorf("active = %q", v)
	}
	if v := get("maritalStatus"); v != "" {
		t.Errorf("null value = %q", v)
	}
	if v := get("name[0].family"); v != "Doe" {
		t.Errorf("name[0].family = %q", v)
	}
	if v := get("name[1].family"); v != "Dough" {
		t.Errorf("name[1].family = %q", v)
	}
	if v := get("name[0].given[1]"); v != "Q" {
		t.Errorf("name[0].given[1] = %q", v)
	}
	// Key on an array fans out across elements.
	if got := getAll("name.family"); strings.Join(got, ",") != "Doe,Dough" {
		t.Errorf("name.family = %v", got)
	}
	// A path ending on an array resolves to its elements.
	if got := getAll("name[0].given"); strings.Join(got, ",") != "John,Q" {
		t.Errorf("given elements = %v", got)
	}
	// Interior nodes render as compact JSON.
	if v := get("identifier[0]"); v != `{"system":"urn:mrn","value":"12345"}` {
		t.Errorf("identifier[0] = %q", v)
	}
	// Misses are empty, not errors.
	if got := getAll("nope.deeper[3]"); len(got) != 0 {
		t.Errorf("miss = %v", got)
	}
	if v := get("name[9].family"); v != "" {
		t.Errorf("out of range = %q", v)
	}
	// Index on a non-array matches nothing.
	if got := getAll("id[0]"); len(got) != 0 {
		t.Errorf("index on scalar = %v", got)
	}
}

func TestResolveQuotedKeys(t *testing.T) {
	root, err := dt.Parse([]byte(`{"odd.key":{"a[b]":1},"":"empty"}`))
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		`["odd.key"]["a[b]"]`: "1",
		`['odd.key']['a[b]']`: "1",
		`[""]`:                "empty",
	} {
		nodes, err := dt.Resolve(root, path)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", path, err)
		}
		if len(nodes) != 1 || dt.Value(root, nodes[0]) != want {
			t.Errorf("Resolve(%s) = %v", path, nodes)
		}
	}
}

func TestPathErrors(t *testing.T) {
	root, _ := dt.Parse([]byte(`{}`))
	for _, path := range []string{"", ".", ".a", "a.", "a..b", "a[", "a[]", "a[-1]", "a[01]", `a["x`, "a[0]b"} {
		if _, err := dt.Resolve(root, path); err == nil {
			t.Errorf("Resolve(%q): expected error", path)
		}
	}
}

func TestSetTypedAndAutovivify(t *testing.T) {
	root, err := dt.Parse([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		path  string
		value any
	}{
		{"resourceType", "Patient"},
		{"active", true},
		{"multipleBirthInteger", int64(2)},
		{"name[0].family", "Doe"},
		{"name[0].given[0]", "John"},
		{"name[0].given[1]", "Q"},
		{"identifier[1].value", "MRN2"}, // pads identifier[0] with null
		{"maritalStatus", nil},
		{"score", 1.5},
	}
	for _, s := range steps {
		if err := dt.SetTyped(root, s.path, s.value); err != nil {
			t.Fatalf("SetTyped(%s): %v", s.path, err)
		}
	}
	out, err := dt.Serialize(root)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"resourceType":"Patient","active":true,"multipleBirthInteger":2,` +
		`"name":[{"family":"Doe","given":["John","Q"]}],` +
		`"identifier":[null,{"value":"MRN2"}],"maritalStatus":null,"score":1.5}`
	if string(out) != want {
		t.Errorf("serialized:\n got  %s\n want %s", out, want)
	}

	// Plain Set writes a string; overwriting converts a scalar to a container.
	if err := dt.Set(root, "active", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := dt.Set(root, "resourceType.sub", "x"); err != nil {
		t.Fatal(err)
	}
	out, _ = dt.Serialize(root)
	if !bytes.Contains(out, []byte(`"active":"yes"`)) || !bytes.Contains(out, []byte(`"resourceType":{"sub":"x"}`)) {
		t.Errorf("after overwrite: %s", out)
	}
}

func TestSegments(t *testing.T) {
	root, err := dt.Parse([]byte(patient))
	if err != nil {
		t.Fatal(err)
	}
	names := dt.Segments(root, "name")
	if len(names) != 2 {
		t.Fatalf("segments(name) = %d", len(names))
	}
	if dt.Value(root, names[1]) != `{"family":"Dough"}` {
		t.Errorf("segment[1] = %q", dt.Value(root, names[1]))
	}
	// A non-array child is returned as itself.
	if got := dt.Segments(root, "id"); len(got) != 1 || got[0].Value != "pat-1" {
		t.Errorf("segments(id) = %v", got)
	}
	if got := dt.Segments(root, "nope"); len(got) != 0 {
		t.Errorf("segments(nope) = %v", got)
	}
}

func TestFlatten(t *testing.T) {
	root, err := dt.Parse([]byte(`{"a":1,"b":[{"c":"x"},{"c":"y"}],"odd.key":true,"empty":"","null":null}`))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, pv := range dt.Flatten(root) {
		got = append(got, pv.Path+"="+pv.Value)
	}
	want := []string{"a=1", "b[0].c=x", "b[1].c=y", `["odd.key"]=true`}
	if strings.Join(got, ";") != strings.Join(want, ";") {
		t.Errorf("flatten = %v, want %v", got, want)
	}
	// Flattened paths must resolve back to their values.
	for _, pv := range dt.Flatten(root) {
		nodes, err := dt.Resolve(root, pv.Path)
		if err != nil || len(nodes) == 0 || dt.Value(root, nodes[0]) != pv.Value {
			t.Errorf("path %q does not resolve to %q (err %v)", pv.Path, pv.Value, err)
		}
	}
}

func TestRegistered(t *testing.T) {
	got, ok := format.Get("json")
	if !ok || got.Name() != "json" {
		t.Fatal("json not registered")
	}
	if _, ok := got.(format.TypedSetter); !ok {
		t.Fatal("json does not implement TypedSetter")
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		patient, `{}`, `[]`, `[1,2.5,-3e2,"x",true,null]`, `{"a":[1]}`,
		`{"esc":"a\nb\"c\\d"}`, `"s"`, `42`, `{"dup":1,"dup":2}`, `{"u":"héllo🐝"}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		root, err := dt.Parse(raw)
		if err != nil {
			return
		}
		out, err := dt.Serialize(root)
		if err != nil {
			t.Fatalf("Serialize after Parse(%q): %v", raw, err)
		}
		// The canonical form must be stable: parse(serialize(tree)) must
		// serialize to the identical bytes.
		root2, err := dt.Parse(out)
		if err != nil {
			t.Fatalf("reparse %q: %v", out, err)
		}
		out2, err := dt.Serialize(root2)
		if err != nil {
			t.Fatalf("reserialize: %v", err)
		}
		if !bytes.Equal(out, out2) {
			t.Fatalf("canonical form unstable: %q vs %q", out, out2)
		}
	})
}
