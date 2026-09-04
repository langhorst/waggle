package engine_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/format/jsonfmt"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/testutil"
)

// TestADTToFHIREndToEnd runs the shipped adt-to-fhir example: HL7 ADT over
// MLLP becomes a FHIR-shaped Patient resource delivered to a REST API
// through the Guaranteed Delivery queue (POST for admissions, PUT routed by
// the script via meta for A31 updates, types intact), and an API 404
// dead-letters with the response body.
func TestADTToFHIREndToEnd(t *testing.T) {
	f := testutil.NewFixture(t)
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

	ch := f.StartExample(t, "adt-to-fhir", map[string]string{
		`":6668"`:                              `"127.0.0.1:0"`,
		`"http://127.0.0.1:8600/fhir/Patient"`: `"` + api.URL + `/fhir/Patient"`,
		"retryInterval: 1s":                    "retryInterval: 20ms",
	})
	sender, err := mllp.NewSender(map[string]any{"addr": testutil.ListenAddr(t, ch)})
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

	testutil.Eventually(t, "three API calls", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 3
	})
	mu.Lock()
	defer mu.Unlock()

	// A01 admission: POST to the collection.
	if calls[0].method != "POST" || calls[0].path != "/fhir/Patient" {
		t.Errorf("admission = %s %s", calls[0].method, calls[0].path)
	}
	// A31 update: the script's meta routing, PUT /fhir/Patient/{id}.
	if calls[1].method != "PUT" || calls[1].path != "/fhir/Patient/MRN777" {
		t.Errorf("update = %s %s", calls[1].method, calls[1].path)
	}

	// The payload is FHIR-shaped, typed JSON.
	if !strings.Contains(calls[0].body, `"active":true`) {
		t.Errorf("active is not a JSON boolean: %s", calls[0].body)
	}
	get := testutil.Getter(t, "json", []byte(calls[0].body))
	for _, tc := range []struct{ path, want string }{
		{"resourceType", "Patient"},
		{"id", "MRN777"},
		{"identifier[0].value", "MRN777"},
		{"name[0].family", "DOE"},
		{"name[0].given[0]", "JANE"},
		{"birthDate", "1985-11-22"},
		{"gender", "female"},
	} {
		if got := get(tc.path); got != tc.want {
			t.Errorf("payload %s = %q, want %q", tc.path, got, tc.want)
		}
	}
	dt, _ := format.Get("json")
	root, _ := dt.Parse([]byte(calls[0].body))
	if nodes, _ := dt.Resolve(root, "active"); len(nodes) != 1 || nodes[0].Kind != jsonfmt.KindBool {
		t.Error("active did not stay a boolean")
	}

	// The 404 for MISSING is an application rejection: dead-lettered, with
	// the API's response body in the error.
	testutil.Eventually(t, "dead letter", func() bool {
		dlq, err := f.Store.DeadLetters(ctx, "adt-to-fhir", 10)
		return err == nil && len(dlq) == 1
	})
	dlq, _ := f.Store.DeadLetters(ctx, "adt-to-fhir", 10)
	if !strings.Contains(dlq[0].Destination.LastError, "404") || !strings.Contains(dlq[0].Destination.LastError, "no such patient") {
		t.Errorf("dead letter error = %q", dlq[0].Destination.LastError)
	}
}

// postWebhook posts body to url and returns the status and response body.
func postWebhook(t *testing.T, url, contentType, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, contentType, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// TestFHIRWebhookToHL7 runs the shipped fhir-webhook-to-hl7 example: a
// webhook POST is answered only after the downstream HIS acknowledged the
// converted HL7 message (destination ACK + waitForAck), and a script
// rejection of a non-Patient payload surfaces as HTTP 400.
func TestFHIRWebhookToHL7(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	his := testutil.AckingReceiver(t, "mllp")

	ch := f.StartExample(t, "fhir-webhook-to-hl7", map[string]string{
		`":8601"`:          `"127.0.0.1:0"`,
		`"127.0.0.1:6669"`: `"` + his.Addr() + `"`,
	})
	hookURL := "http://" + testutil.ListenAddr(t, ch) + "/fhir/Patient"

	patient := `{"resourceType":"Patient","id":"pat-9","active":true,` +
		`"identifier":[{"system":"urn:waggle:mrn","value":"MRN9"}],` +
		`"name":[{"family":"Doe","given":["John"]}],` +
		`"birthDate":"1980-01-01","gender":"male"}`
	if code, body := postWebhook(t, hookURL, "application/json", patient); code != http.StatusOK || !strings.Contains(body, `"code":"AA"`) {
		t.Fatalf("webhook = %d %s", code, body)
	}

	// The HIS got the converted ADT^A31 before the webhook was answered.
	if n := his.Count(); n != 1 {
		t.Fatalf("HIS received %d messages", n)
	}
	hl7 := string(his.Received()[0])
	for _, want := range []string{"ADT^A31", "PID|1||MRN9||Doe^John||19800101|M"} {
		if !strings.Contains(hl7, want) {
			t.Errorf("HIS message missing %q:\n%s", want, hl7)
		}
	}

	// A non-Patient payload is rejected by the script: HTTP 400, AR, and
	// nothing reaches the HIS.
	if code, body := postWebhook(t, hookURL, "application/json", `{"resourceType":"Observation"}`); code != http.StatusBadRequest || !strings.Contains(body, "Patient resource") {
		t.Errorf("rejection = %d %s", code, body)
	}
	if n := his.Count(); n != 1 {
		t.Errorf("rejected message reached the HIS")
	}

	// The rejection is on the record: one TRANSFORMED to SENT flow and one ERROR.
	counts, err := f.Store.MessageCounts(ctx, "fhir-webhook-to-hl7")
	if err != nil || counts[message.StateSent] != 1 || counts[message.StateError] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
}

// TestFHIRXMLWebhookToHL7 runs the shipped fhir-xml-webhook example: the
// same webhook contract as the JSON variant, but the Patient arrives as
// FHIR XML (value attributes, a default xmlns, XPath-flavored script
// paths).
func TestFHIRXMLWebhookToHL7(t *testing.T) {
	f := testutil.NewFixture(t)
	his := testutil.AckingReceiver(t, "mllp")

	ch := f.StartExample(t, "fhir-xml-webhook", map[string]string{
		`":8602"`:          `"127.0.0.1:0"`,
		`"127.0.0.1:6669"`: `"` + his.Addr() + `"`,
	})
	hookURL := "http://" + testutil.ListenAddr(t, ch) + "/fhir/Patient"

	patient := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<Patient xmlns="http://hl7.org/fhir">` +
		`<id value="pat-9"/>` +
		`<identifier><system value="urn:waggle:mrn"/><value value="MRN9"/></identifier>` +
		`<name><family value="Doe"/><given value="John"/></name>` +
		`<birthDate value="1980-01-01"/>` +
		`<gender value="male"/>` +
		`</Patient>`
	if code, body := postWebhook(t, hookURL, "application/fhir+xml", patient); code != http.StatusOK || !strings.Contains(body, `"code":"AA"`) {
		t.Fatalf("webhook = %d %s", code, body)
	}
	if n := his.Count(); n != 1 {
		t.Fatalf("HIS received %d messages", n)
	}
	hl7 := string(his.Received()[0])
	for _, want := range []string{"ADT^A31", "FHIR-XML", "PID|1||MRN9||Doe^John||19800101|M"} {
		if !strings.Contains(hl7, want) {
			t.Errorf("HIS message missing %q:\n%s", want, hl7)
		}
	}

	// A non-Patient document rejects with AR: HTTP 400; nothing delivered.
	if code, body := postWebhook(t, hookURL, "application/fhir+xml", `<Observation><id value="x"/></Observation>`); code != http.StatusBadRequest || !strings.Contains(body, "Patient resource") {
		t.Errorf("rejection = %d %s", code, body)
	}
	// Malformed XML fails to parse: pipeline error, HTTP 500.
	if code, _ := postWebhook(t, hookURL, "application/fhir+xml", `<Patient><unclosed>`); code != http.StatusInternalServerError {
		t.Errorf("malformed XML = %d", code)
	}
	if n := his.Count(); n != 1 {
		t.Errorf("rejected/malformed messages reached the HIS")
	}
}
