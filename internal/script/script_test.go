package script

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"

	_ "github.com/langhorst/waggle/internal/format/astm"
	_ "github.com/langhorst/waggle/internal/format/csvfmt"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
)

const sampleHL7 = "MSH|^~\\&|SEND|SFAC|RECV|RFAC|20260730||ADT^A01|CTRL001|P|2.5\r" +
	"PID|1||MRN1~MRN2||DOE^JOHN\r" +
	"OBX|1|NM|WT||70|kg\r" +
	"OBX|2|NM|HT||180|cm\r"

func writeScript(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "script.js")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func hl7Message(t *testing.T) *message.Message {
	t.Helper()
	dt, _ := format.Get("hl7v2")
	tree, err := dt.Parse([]byte(sampleHL7))
	if err != nil {
		t.Fatal(err)
	}
	return &message.Message{
		ID: 7, ChannelID: "test", Raw: []byte(sampleHL7), Tree: tree,
		DataType: "hl7v2", Meta: map[string]string{"source.file": "x.hl7"},
	}
}

func newTestEngine(t *testing.T, opts Options) *Engine {
	t.Helper()
	e := New(opts)
	t.Cleanup(e.Close)
	return e
}

func TestFilter(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `function filter(msg) { return msg.get('MSH-9.1') === 'ADT'; }`)
	f, err := e.CompileFilter(path)
	if err != nil {
		t.Fatal(err)
	}
	keep, err := f(hl7Message(t))
	if err != nil || !keep {
		t.Errorf("ADT should pass: keep=%v err=%v", keep, err)
	}

	drop := writeScript(t, `function filter(msg) { return msg.get('MSH-9.1') === 'ORU'; }`)
	f2, err := e.CompileFilter(drop)
	if err != nil {
		t.Fatal(err)
	}
	if keep, err := f2(hl7Message(t)); err != nil || keep {
		t.Errorf("ADT should be dropped by ORU filter: keep=%v err=%v", keep, err)
	}
}

func TestTransformMutatesInPlace(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `
function transform(msg) {
	msg.set('PID-5.1', msg.get('PID-5.1').toLowerCase());
	msg.set('PID-5.2', 'JAMES');
	msg.set('ZZZ-1', 'custom');
}`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	m := hl7Message(t)
	if err := tr(m); err != nil {
		t.Fatal(err)
	}
	dt, _ := format.Get("hl7v2")
	out, _ := dt.Serialize(m.Tree)
	if !strings.Contains(string(out), "doe^JAMES") || !strings.Contains(string(out), "ZZZ|custom") {
		t.Errorf("mutations not applied: %q", out)
	}
}

func TestGetAllAndSegments(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `
function transform(msg) {
	var reps = msg.getAll('PID-3');
	msg.set('ZID-1', reps.join(';'));
	var obx = msg.segments('OBX');
	msg.set('ZID-2', String(obx.length));
	for (var i = 0; i < obx.length; i++) {
		obx[i].set('1', String(i + 100));  // renumber via segment handle
	}
	msg.set('ZID-3', obx[1].get('5'));
}`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	m := hl7Message(t)
	if err := tr(m); err != nil {
		t.Fatal(err)
	}
	dt, _ := format.Get("hl7v2")
	get := func(p string) string {
		nodes, _ := dt.Resolve(m.Tree, p)
		if len(nodes) == 0 {
			return ""
		}
		return dt.Value(m.Tree, nodes[0])
	}
	if got := get("ZID-1"); got != "MRN1;MRN2" {
		t.Errorf("ZID-1 = %q", got)
	}
	if got := get("ZID-2"); got != "2" {
		t.Errorf("ZID-2 = %q", got)
	}
	if got := get("ZID-3"); got != "180" {
		t.Errorf("ZID-3 = %q (obx[1].get('5'))", got)
	}
	if got := get("OBX[2]-1"); got != "101" {
		t.Errorf("OBX[2]-1 = %q (segment handle set)", got)
	}
}

func TestFormatConversionViaNewMessage(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `
function transform(msg) {
	var out = newMessage('csv');
	out.set('R.1', msg.get('PID-3'));
	out.set('R.2', msg.get('PID-5.2') + ' ' + msg.get('PID-5.1'));
	return out;
}`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	m := hl7Message(t)
	if err := tr(m); err != nil {
		t.Fatal(err)
	}
	if m.DataType != "csv" {
		t.Fatalf("dataType = %q after conversion", m.DataType)
	}
	dt, _ := format.Get("csv")
	out, err := dt.Serialize(m.Tree)
	if err != nil || string(out) != "MRN1,JOHN DOE\n" {
		t.Errorf("csv = %q, %v", out, err)
	}
}

func TestResponseReject(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `
function filter(msg) {
	if (!msg.get('PID-18')) { response.reject('AR', 'missing PID-18 account number'); }
	return true;
}`)
	f, err := e.CompileFilter(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f(hl7Message(t))
	var rej *channel.Rejection
	if !errors.As(err, &rej) {
		t.Fatalf("expected Rejection, got %v", err)
	}
	if rej.Code != "AR" || !strings.Contains(rej.Text, "PID-18") {
		t.Errorf("rejection = %+v", rej)
	}
}

func TestResponseSetAck(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `function transform(msg) { response.setAck('AE', 'accepted with warnings'); }`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	m := hl7Message(t)
	if err := tr(m); err != nil {
		t.Fatal(err)
	}
	if m.AckCode != "AE" || m.AckText != "accepted with warnings" {
		t.Errorf("ack override = %q %q", m.AckCode, m.AckText)
	}
}

func TestScriptExceptionIsError(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `function transform(msg) { throw new Error('validation exploded'); }`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	err = tr(hl7Message(t))
	if err == nil || !strings.Contains(err.Error(), "validation exploded") {
		t.Errorf("err = %v", err)
	}
}

func TestBadPathThrows(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `function transform(msg) { msg.get('not a path'); }`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr(hl7Message(t)); err == nil || !strings.Contains(err.Error(), "invalid path") {
		t.Errorf("err = %v", err)
	}
}

func TestTimeoutInterruptsRunawayScript(t *testing.T) {
	e := newTestEngine(t, Options{Timeout: 100 * time.Millisecond})
	path := writeScript(t, `function transform(msg) { while (true) {} }`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = tr(hl7Message(t))
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("interrupt took too long")
	}
	// Runtime is reusable after an interrupt.
	ok := writeScript(t, `function transform(msg) { msg.set('ZZZ-1', 'fine'); }`)
	tr2, err := e.CompileTranslator(ok)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr2(hl7Message(t)); err != nil {
		t.Errorf("engine unusable after interrupt: %v", err)
	}
}

func TestCompileErrors(t *testing.T) {
	e := newTestEngine(t, Options{})
	if _, err := e.CompileFilter(filepath.Join(t.TempDir(), "missing.js")); err == nil {
		t.Error("missing file should fail")
	}
	bad := writeScript(t, `function filter(msg) { syntax error here`)
	if _, err := e.CompileFilter(bad); err == nil {
		t.Error("syntax error should fail")
	}
	wrongFn := writeScript(t, `function notFilter(msg) { return true; }`)
	if _, err := e.CompileFilter(wrongFn); err == nil || !strings.Contains(err.Error(), "must define function filter") {
		t.Errorf("missing entry function: %v", err)
	}
}

func TestHotReload(t *testing.T) {
	e := newTestEngine(t, Options{HotReload: true, ReloadInterval: 20 * time.Millisecond})
	path := writeScript(t, `function transform(msg) { msg.set('ZZZ-1', 'v1'); }`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	get := func(m *message.Message) string {
		dt, _ := format.Get("hl7v2")
		nodes, _ := dt.Resolve(m.Tree, "ZZZ-1")
		if len(nodes) == 0 {
			return ""
		}
		return dt.Value(m.Tree, nodes[0])
	}

	m := hl7Message(t)
	if err := tr(m); err != nil || get(m) != "v1" {
		t.Fatalf("v1: %v %q", err, get(m))
	}

	// Rewrite with a future mtime so the watcher sees the change.
	if err := os.WriteFile(path, []byte(`function transform(msg) { msg.set('ZZZ-1', 'v2'); }`), 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(path, future, future)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m2 := hl7Message(t)
		if err := tr(m2); err == nil && get(m2) == "v2" {
			// Reloaded. Now break it: previous version must stay active.
			if err := os.WriteFile(path, []byte(`function transform( { broken`), 0o644); err != nil {
				t.Fatal(err)
			}
			future = future.Add(2 * time.Second)
			_ = os.Chtimes(path, future, future)
			time.Sleep(100 * time.Millisecond)
			m3 := hl7Message(t)
			if err := tr(m3); err != nil || get(m3) != "v2" {
				t.Fatalf("broken reload must keep v2 active: %v %q", err, get(m3))
			}
			if errs := e.Scripts(); errs[path] == "" {
				t.Error("compile error should be recorded in Scripts()")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("hot reload never took effect")
}

func TestScriptStatePersistsAcrossMessages(t *testing.T) {
	e := newTestEngine(t, Options{})
	path := writeScript(t, `
var count = 0;
function transform(msg) { count++; msg.set('ZZZ-1', String(count)); }`)
	tr, err := e.CompileTranslator(path)
	if err != nil {
		t.Fatal(err)
	}
	dt, _ := format.Get("hl7v2")
	for i := 1; i <= 3; i++ {
		m := hl7Message(t)
		if err := tr(m); err != nil {
			t.Fatal(err)
		}
		nodes, _ := dt.Resolve(m.Tree, "ZZZ-1")
		if got := dt.Value(m.Tree, nodes[0]); got != strings.TrimSpace(string(rune('0'+i))) {
			t.Errorf("message %d: count = %q", i, got)
		}
	}
}
