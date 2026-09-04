package xmlfmt

import (
	"bytes"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/format"
)

var dt = DataType{}

const patientXML = `<?xml version="1.0" encoding="UTF-8"?>` +
	`<Patient xmlns="http://hl7.org/fhir">` +
	`<id value="pat-9"/>` +
	`<identifier><system value="urn:waggle:mrn"/><value value="MRN9"/></identifier>` +
	`<active value="true"/>` +
	`<name use="official"><family value="Doe"/><given value="John"/><given value="Q"/></name>` +
	`<name use="maiden"><family value="Smith"/></name>` +
	`<birthDate value="1980-01-01"/>` +
	`<gender value="male"/>` +
	`</Patient>`

func TestParseSerializeGolden(t *testing.T) {
	for _, raw := range []string{
		patientXML,
		`<a/>`,
		`<a b="1" c="2"/>`,
		`<root><child>text</child><child>more</child></root>`,
		`<f:root xmlns:f="urn:f" f:kind="x"><f:child at="1">v</f:child></f:root>`,
		`<a n="&quot;q&amp;&lt;&gt;">&lt;body&gt; &amp; more</a>`,
		`<p>hello <b>bold</b> tail</p>`,
		`<a><b><c d="deep"/></b></a>`,
		`<?xml version="1.0"?><a>x</a>`,
		`<a>line1` + "\n" + `line2	tabbed</a>`,
		`<u>héllo — 🐝</u>`,
		`<ns1:a xmlns:ns1="urn:one" xmlns:ns2="urn:two"><ns2:b/></ns1:a>`,
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

func TestCanonicalization(t *testing.T) {
	for in, want := range map[string]string{
		// Pretty-printing collapses to compact.
		"<a>\n  <b>x</b>\n  <c/>\n</a>": `<a><b>x</b><c/></a>`,
		// Empty element forms unify to self-closing.
		`<a></a>`: `<a/>`,
		// CDATA re-escapes as ordinary text.
		`<a><![CDATA[5 < 6 & true]]></a>`: `<a>5 &lt; 6 &amp; true</a>`,
		// Comments, PIs, and DOCTYPE are dropped.
		`<!DOCTYPE a><a><!-- gone --><b/><?pi data?></a>`: `<a><b/></a>`,
		// Whitespace-only text in simple content is formatting too.
		"<a>  \n </a>": `<a/>`,
	} {
		root, err := dt.Parse([]byte(in))
		if err != nil {
			t.Errorf("Parse(%s): %v", in, err)
			continue
		}
		out, err := dt.Serialize(root)
		if err != nil || string(out) != want {
			t.Errorf("canonical(%s) = %s (err %v), want %s", in, out, err, want)
		}
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, raw := range []string{
		``, `   `, `<a>`, `<a></b>`, `<a/><b/>`, `<a/>trailing`, `plain text`, `<a b=></a>`,
	} {
		if _, err := dt.Parse([]byte(raw)); err == nil {
			t.Errorf("Parse(%q): expected error", raw)
		}
	}
}

// TestParseDepthLimit: element nesting beyond MaxDepth is a parse error,
// not a stack overflow.
func TestParseDepthLimit(t *testing.T) {
	deep := strings.Repeat("<a>", MaxDepth+1) + strings.Repeat("</a>", MaxDepth+1)
	if _, err := dt.Parse([]byte(deep)); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("Parse(%d levels): want nesting error, got %v", MaxDepth+1, err)
	}
	ok := strings.Repeat("<a>", MaxDepth) + strings.Repeat("</a>", MaxDepth)
	if _, err := dt.Parse([]byte(ok)); err != nil {
		t.Fatalf("Parse(%d levels): %v", MaxDepth, err)
	}
	// A megabyte of open tags, the shape of the original crash.
	huge := strings.Repeat("<a>", 350_000)
	if _, err := dt.Parse([]byte(huge)); err == nil {
		t.Fatal("Parse(1 MiB of '<a>'): want error")
	}
}

func TestResolve(t *testing.T) {
	root, err := dt.Parse([]byte(patientXML))
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

	if v := get("Patient/id/@value"); v != "pat-9" {
		t.Errorf("id = %q", v)
	}
	if v := get("Patient/@xmlns"); v != "http://hl7.org/fhir" {
		t.Errorf("xmlns = %q", v)
	}
	if v := get("Patient/name[1]/family/@value"); v != "Doe" {
		t.Errorf("name[1] family = %q", v)
	}
	if v := get("Patient/name[2]/family/@value"); v != "Smith" {
		t.Errorf("name[2] family = %q", v)
	}
	if v := get("/Patient/name[2]/@use"); v != "maiden" {
		t.Errorf("leading-slash path = %q", v)
	}
	if v := get("Patient/name[1]/given[2]/@value"); v != "Q" {
		t.Errorf("given[2] = %q", v)
	}
	// Omitted [n] fans out like an HL7 repetition.
	if got := getAll("Patient/name/family/@value"); strings.Join(got, ",") != "Doe,Smith" {
		t.Errorf("families = %v", got)
	}
	// An element with children renders as compact XML.
	if v := get("Patient/identifier"); v != `<identifier><system value="urn:waggle:mrn"/><value value="MRN9"/></identifier>` {
		t.Errorf("identifier = %q", v)
	}
	// Misses are empty, not errors.
	if got := getAll("Patient/nope/deeper"); len(got) != 0 {
		t.Errorf("miss = %v", got)
	}
	if v := get("Patient/name[9]/@use"); v != "" {
		t.Errorf("out of range = %q", v)
	}
	if v := get("Patient/name[1]/@nope"); v != "" {
		t.Errorf("missing attr = %q", v)
	}
}

func TestResolveSimpleContentAndMixed(t *testing.T) {
	root, err := dt.Parse([]byte(`<msg><subject>hello</subject><p>lead <b>x</b> tail</p></msg>`))
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) string {
		nodes, err := dt.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}
	if v := get("msg/subject"); v != "hello" {
		t.Errorf("subject = %q", v)
	}
	if v := get("msg/p/#text[2]"); v != " tail" {
		t.Errorf("#text[2] = %q", v)
	}
	if v := get("msg/p/b"); v != "x" {
		t.Errorf("b = %q", v)
	}
}

func TestPathErrors(t *testing.T) {
	root, _ := dt.Parse([]byte(`<a/>`))
	for _, path := range []string{
		"", "/", "a//b", "a/", "a[0]", "a[x]", "a[01]", "a[2", "@id/a", "a/@", "a/@b/c", "a/b@c",
	} {
		if _, err := dt.Resolve(root, path); err == nil {
			t.Errorf("Resolve(%q): expected error", path)
		}
	}
}

func TestSetAndAutovivify(t *testing.T) {
	tree, err := dt.Parse([]byte(`<Patient/>`))
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct{ path, value string }{
		{"Patient/@xmlns", "http://hl7.org/fhir"},
		{"Patient/id/@value", "p1"},
		{"Patient/name[1]/family/@value", "Doe"},
		{"Patient/name[1]/given[1]/@value", "John"},
		{"Patient/name[1]/given[2]/@value", "Q"},
		{"Patient/name[2]/@use", "maiden"}, // pads a second name element
		{"Patient/note", "plain text"},
	}
	for _, s := range steps {
		if err := dt.Set(tree, s.path, s.value); err != nil {
			t.Fatalf("Set(%s): %v", s.path, err)
		}
	}
	out, err := dt.Serialize(tree)
	if err != nil {
		t.Fatal(err)
	}
	want := `<Patient xmlns="http://hl7.org/fhir">` +
		`<id value="p1"/>` +
		`<name><family value="Doe"/><given value="John"/><given value="Q"/></name>` +
		`<name use="maiden"/>` +
		`<note>plain text</note>` +
		`</Patient>`
	if string(out) != want {
		t.Errorf("serialized:\n got  %s\n want %s", out, want)
	}

	// Setting text on an element with children replaces content, keeps attrs.
	if err := dt.Set(tree, "Patient/name[1]", "flattened"); err != nil {
		t.Fatal(err)
	}
	out, _ = dt.Serialize(tree)
	if !bytes.Contains(out, []byte(`<name>flattened</name>`)) {
		t.Errorf("text set did not replace content: %s", out)
	}
	// Overwriting an attribute in place.
	if err := dt.Set(tree, "Patient/name[2]/@use", "old"); err != nil {
		t.Fatal(err)
	}
	out, _ = dt.Serialize(tree)
	if !bytes.Contains(out, []byte(`<name use="old"/>`)) {
		t.Errorf("attr overwrite failed: %s", out)
	}
}

func TestSegments(t *testing.T) {
	root, err := dt.Parse([]byte(patientXML))
	if err != nil {
		t.Fatal(err)
	}
	names := dt.Segments(root, "name")
	if len(names) != 2 {
		t.Fatalf("segments(name) = %d", len(names))
	}
	if dt.Value(root, names[1]) != `<name use="maiden"><family value="Smith"/></name>` {
		t.Errorf("segment[1] = %q", dt.Value(root, names[1]))
	}
	if got := dt.Segments(root, "Patient"); len(got) != 1 || got[0].Name != "Patient" {
		t.Errorf("segments(Patient) = %v", got)
	}
	if got := dt.Segments(root, "nope"); len(got) != 0 {
		t.Errorf("segments(nope) = %v", got)
	}
	// Segment-relative paths join with the dialect separator.
	if p := dt.JoinSegmentPath("name", "family/@value"); p != "name/family/@value" {
		t.Errorf("JoinSegmentPath = %q", p)
	}
}

func TestFlatten(t *testing.T) {
	root, err := dt.Parse([]byte(
		`<r a="1"><x><y at="v">txt</y></x><x><y>two</y></x><empty/></r>`))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, pv := range dt.Flatten(root) {
		got = append(got, pv.Path+"="+pv.Value)
	}
	want := []string{"r/@a=1", "r/x/y/@at=v", "r/x/y=txt", "r/x[2]/y=two"}
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
	got, ok := format.Get("xml")
	if !ok || got.Name() != "xml" {
		t.Fatal("xml not registered")
	}
	// XML has no typed leaves; sets go through the plain string path.
	if _, ok := got.(format.TypedSetter); ok {
		t.Fatal("xml should not implement TypedSetter")
	}
	if _, ok := got.(format.SegmentJoiner); !ok {
		t.Fatal("xml should implement SegmentJoiner")
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		patientXML, `<a/>`, `<a b="1">t</a>`, `<p>x <b>y</b> z</p>`,
		`<f:r xmlns:f="u"><f:c/></f:r>`, `<?xml version="1.0"?><a>&amp;&lt;</a>`,
		`<a><![CDATA[<x>]]></a>`, `<u>héllo🐝</u>`,
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
		// Canonical stability: parse(serialize(tree)) must serialize to the
		// identical bytes.
		root2, err := dt.Parse(out)
		if err != nil {
			t.Fatalf("reparse %q (from %q): %v", out, raw, err)
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
