package engine

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/adapter/httpin"
	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/format/jsonfmt"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"

	_ "github.com/langhorst/waggle/internal/adapter/httpout"
)

// apiHarness is the shared setup for the API E2E tests: a store, a script
// engine, an engine, and the example scripts directory.
type apiHarness struct {
	work       string
	scriptsDir string
	st         *store.Store
	eng        *Engine
}

func newAPIHarness(t *testing.T) *apiHarness {
	t.Helper()
	scriptsDir, err := filepath.Abs(filepath.Join("..", "..", "examples", "channels", "scripts"))
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	st, err := store.Open(filepath.Join(work, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	scripts := script.New(script.Options{Log: slog.New(slog.DiscardHandler)})
	t.Cleanup(scripts.Close)
	eng := New(Options{Store: st, Scripts: scripts, Log: slog.New(slog.DiscardHandler)})
	t.Cleanup(eng.Shutdown)
	return &apiHarness{work: work, scriptsDir: scriptsDir, st: st, eng: eng}
}

func (h *apiHarness) startChannel(t *testing.T, name, yaml string) {
	t.Helper()
	path := filepath.Join(h.work, name+".yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadChannel(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.eng.LoadChannel(cfg); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Start(context.Background(), name); err != nil {
		t.Fatal(err)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestADTToFHIREndToEnd drives the adt-to-fhir example: HL7 ADT over MLLP
// becomes a FHIR-shaped Patient resource delivered to a REST API through the
// Guaranteed Delivery queue — POST for admissions, PUT routed by the script
// via meta for A31 updates, types intact, and an API 404 dead-lettering.
func TestADTToFHIREndToEnd(t *testing.T) {
	h := newAPIHarness(t)
	ctx := context.Background()

	type call struct {
		method, path, body string
	}
	var mu sync.Mutex
	var calls []call
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		calls = append(calls, call{r.Method, r.URL.Path, string(body)})
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/MISSING") {
			http.Error(w, `{"issue":"no such patient"}`, http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer api.Close()

	h.startChannel(t, "adt-to-fhir", `
id: adt-to-fhir
source:
  type: mllp-listener
  dataType: hl7v2
  settings: {addr: "127.0.0.1:0", ackMode: immediate}
filter: "`+filepath.Join(h.scriptsDir, "only-adt.js")+`"
transformers: ["`+filepath.Join(h.scriptsDir, "adt-to-fhir-patient.js")+`"]
destinations:
  - id: fhir-api
    dataType: json
    adapter:
      type: http-sender
      settings: {url: "`+api.URL+`/fhir/Patient"}
    queue: {retryInterval: 20ms}
`)
	ch, _ := h.eng.Channel("adt-to-fhir")
	mllpAddr := ch.Source.(*mllp.Listener).Addr()

	sender, err := mllp.NewSender(map[string]any{"addr": mllpAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	admission := "MSH|^~\\&|HIS|WARD|||20260806||ADT^A01|C1|P|2.5.1\r" +
		"PID|1||MRN777||DOE^JANE||19851122|F\r"
	update := "MSH|^~\\&|HIS|WARD|||20260806||ADT^A31|C2|P|2.5.1\r" +
		"PID|1||MRN777||DOE^JANE||19851122|F\r"
	missing := "MSH|^~\\&|HIS|WARD|||20260806||ADT^A31|C3|P|2.5.1\r" +
		"PID|1||MISSING||GONE^GIRL||19700101|F\r"
	for _, m := range []string{admission, update, missing} {
		if err := sender.Send(ctx, []byte(m), nil); err != nil {
			t.Fatal(err)
		}
	}

	waitUntil(t, "three API calls", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 3
	})
	mu.Lock()
	defer mu.Unlock()

	// A01 admission → POST to the collection.
	if calls[0].method != "POST" || calls[0].path != "/fhir/Patient" {
		t.Errorf("admission = %s %s", calls[0].method, calls[0].path)
	}
	// A31 update → the script's meta routing: PUT /fhir/Patient/{id}.
	if calls[1].method != "PUT" || calls[1].path != "/fhir/Patient/MRN777" {
		t.Errorf("update = %s %s", calls[1].method, calls[1].path)
	}

	// The payload is FHIR-shaped, typed JSON.
	if !strings.Contains(calls[0].body, `"active":true`) {
		t.Errorf("active is not a JSON boolean: %s", calls[0].body)
	}
	dt, _ := format.Get("json")
	root, err := dt.Parse([]byte(calls[0].body))
	if err != nil {
		t.Fatalf("payload does not parse as JSON: %v", err)
	}
	get := func(path string) string {
		nodes, err := dt.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return dt.Value(root, nodes[0])
	}
	for path, want := range map[string]string{
		"resourceType":        "Patient",
		"id":                  "MRN777",
		"identifier[0].value": "MRN777",
		"name[0].family":      "DOE",
		"name[0].given[0]":    "JANE",
		"birthDate":           "1985-11-22",
		"gender":              "female",
	} {
		if got := get(path); got != want {
			t.Errorf("payload %s = %q, want %q", path, got, want)
		}
	}
	if nodes, _ := dt.Resolve(root, "active"); len(nodes) != 1 || nodes[0].Kind != jsonfmt.KindBool {
		t.Error("active did not stay a boolean")
	}

	// The 404 for MISSING is an application rejection: dead-lettered, with
	// the API's response body in the error.
	waitUntil(t, "dead letter", func() bool {
		dlq, err := h.st.DeadLetters(ctx, "adt-to-fhir", 10)
		return err == nil && len(dlq) == 1
	})
	dlq, _ := h.st.DeadLetters(ctx, "adt-to-fhir", 10)
	if !strings.Contains(dlq[0].Destination.LastError, "404") || !strings.Contains(dlq[0].Destination.LastError, "no such patient") {
		t.Errorf("dead letter error = %q", dlq[0].Destination.LastError)
	}
}

// TestFHIRWebhookToHL7 drives the fhir-webhook-to-hl7 example: a webhook
// POST is answered only after the downstream HIS acknowledged the converted
// HL7 message (destination ACK + waitForAck), and a script rejection of a
// non-Patient payload surfaces as HTTP 400.
func TestFHIRWebhookToHL7(t *testing.T) {
	h := newAPIHarness(t)
	ctx := context.Background()

	// Test-side HIS: an MLLP receiver that acknowledges everything.
	var mu sync.Mutex
	var received [][]byte
	his, err := mllp.NewListener(map[string]any{"addr": "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		mu.Lock()
		received = append(received, append([]byte(nil), raw...))
		mu.Unlock()
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: "AA"}
		return adapter.Receipt{Done: done}, nil
	}
	if err := his.Start(ctx, deliver); err != nil {
		t.Fatal(err)
	}
	defer his.Stop()

	h.startChannel(t, "fhir-webhook-to-hl7", `
id: fhir-webhook-to-hl7
source:
  type: http-listener
  dataType: json
  settings: {listen: "127.0.0.1:0", path: /fhir/Patient, ackMode: destination}
transformers: ["`+filepath.Join(h.scriptsDir, "fhir-patient-to-adt.js")+`"]
destinations:
  - id: to-his
    dataType: hl7v2
    waitForAck: true
    adapter:
      type: mllp-sender
      settings: {addr: "`+his.Addr()+`"}
`)
	ch, _ := h.eng.Channel("fhir-webhook-to-hl7")
	hookURL := "http://" + ch.Source.(*httpin.Listener).Addr() + "/fhir/Patient"

	patient := `{"resourceType":"Patient","id":"pat-9","active":true,` +
		`"identifier":[{"system":"urn:waggle:mrn","value":"MRN9"}],` +
		`"name":[{"family":"Doe","given":["John"]}],` +
		`"birthDate":"1980-01-01","gender":"male"}`
	resp, err := http.Post(hookURL, "application/json", bytes.NewReader([]byte(patient)))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"code":"AA"`) {
		t.Fatalf("webhook = %d %s", resp.StatusCode, body)
	}

	// The HIS got the converted ADT^A31 before the webhook was answered.
	mu.Lock()
	if len(received) != 1 {
		mu.Unlock()
		t.Fatalf("HIS received %d messages", len(received))
	}
	hl7 := string(received[0])
	mu.Unlock()
	for _, want := range []string{"ADT^A31", "PID|1||MRN9||Doe^John||19800101|M"} {
		if !strings.Contains(hl7, want) {
			t.Errorf("HIS message missing %q:\n%s", want, hl7)
		}
	}

	// A non-Patient payload is rejected by the script: HTTP 400, AR, and
	// nothing reaches the HIS.
	resp, err = http.Post(hookURL, "application/json", bytes.NewReader([]byte(`{"resourceType":"Observation"}`)))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "Patient resource") {
		t.Errorf("rejection = %d %s", resp.StatusCode, body)
	}
	mu.Lock()
	n := len(received)
	mu.Unlock()
	if n != 1 {
		t.Errorf("rejected message reached the HIS")
	}

	// The rejection is on the record: one TRANSFORMED→SENT flow and one ERROR.
	counts, err := h.st.MessageCounts(ctx, "fhir-webhook-to-hl7")
	if err != nil || counts[message.StateSent] != 1 || counts[message.StateError] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
}
