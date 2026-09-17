package engine_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/testutil"
)

// fhirStub stands in for the FHIR server: it records what was PUT where, so
// the test can assert the resource the channel actually produced rather than
// that a request merely happened.
type fhirStub struct {
	*httptest.Server

	mu       sync.Mutex
	patients map[string]map[string]any // id -> resource
	methods  []string
	badPaths []string
	requests int
}

func newFHIRStub(t *testing.T) *fhirStub {
	t.Helper()
	s := &fhirStub{patients: map[string]map[string]any{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/fhir/Patient/", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.requests++
		s.methods = append(s.methods, r.Method)
		id := strings.TrimPrefix(r.URL.Path, "/fhir/Patient/")
		if id == "" || strings.Contains(id, "/") {
			s.badPaths = append(s.badPaths, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.badPaths = append(s.badPaths, r.URL.Path+" (unparseable body)")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, existed := s.patients[id]
		s.patients[id] = body
		w.Header().Set("Content-Type", "application/fhir+json")
		if existed {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
		_ = json.NewEncoder(w).Encode(body)
	})
	// Anything else is a routing mistake worth failing on.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.badPaths = append(s.badPaths, r.Method+" "+r.URL.Path)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func (s *fhirStub) patient(id string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.patients[id]
}

func (s *fhirStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.patients)
}

func (s *fhirStub) problems() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.badPaths...)
}

const (
	admitA01 = "MSH|^~\\&|WAGGLESIM|SIMNET|WAGGLE|TEST|20260302143500||ADT^A01^ADT_A01|SIMT0001|T|2.5.1\r" +
		"EVN|A01|20260302143500\r" +
		"PID|1||MERC0000042^^^MERCY^MR~EID0000066^^^SIMNET^PI||THORNQUIST^IMOGEN^R||19580714|F|||418 ELDER LN^^SPRINGDALE^OH^45001^USA||(555)201-8834|||M||A000000123\r" +
		"PV1|1|I|3WEST^312^B^MERCY||||SIM0003^VANTERPOOL^MARGUERITE|||||||7|||||V000000123\r"

	// The same patient, name corrected. One Patient resource must result,
	// not two, and the CSV must show both rows.
	updateA08 = "MSH|^~\\&|WAGGLESIM|SIMNET|WAGGLE|TEST|20260302150000||ADT^A08^ADT_A01|SIMT0002|T|2.5.1\r" +
		"EVN|A08|20260302150000\r" +
		"PID|1||MERC0000042^^^MERCY^MR~EID0000066^^^SIMNET^PI||THORNQUIST-VALE^IMOGEN^R||19580714|F|||418 ELDER LN^^SPRINGDALE^OH^45001^USA||(555)201-8834|||M||A000000123\r" +
		"PV1|1|I|3WEST^312^B^MERCY||||SIM0003^VANTERPOOL^MARGUERITE|||||||7|||||V000000123\r"

	secondPatient = "MSH|^~\\&|WAGGLESIM|SIMNET|WAGGLE|TEST|20260302160000||ADT^A01^ADT_A01|SIMT0003|T|2.5.1\r" +
		"EVN|A01|20260302160000\r" +
		"PID|1||STLU0000007^^^STLUKE^MR~EID0000099^^^SIMNET^PI||OAKHURST^BRAM^||19910223|M|||12 QUARRY RD^^FAIRHAVEN^OH^45002^USA||(555)300-1122|||S||A000000200\r" +
		"PV1|1|E|ED^104^A^STLUKE||||SIM0001^HALLOWELL^ROSE|||||||7|||||V000000200\r"
)

// The workflow the channel exists for: ADT in over MLLP, out to a CSV a
// human reads and to a FHIR server they can query, with both agreeing.
func TestADTFansOutToCSVAndFHIR(t *testing.T) {
	f := testutil.NewFixture(t, testutil.InMemory())
	fhir := newFHIRStub(t)

	ch := f.StartExample(t, "adt-to-csv-and-fhir", map[string]string{
		"127.0.0.1:2575":              "127.0.0.1:0",
		"http://127.0.0.1:8080/fhir/": fhir.URL + "/fhir/",
		"dir: out":                    "dir: " + filepath.Join(f.Work, "out"),
	})
	addr := testutil.ListenAddr(t, ch)

	// The real MLLP sender: it returns an error on AE/AR, so a clean send
	// is the channel acknowledging that both destinations accepted.
	sender, err := mllp.NewSender(map[string]any{"addr": addr, "ackTimeout": "20s"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	ctx := context.Background()
	for i, raw := range []string{admitA01, updateA08, secondPatient} {
		if err := sender.Send(ctx, []byte(raw), nil); err != nil {
			t.Fatalf("message %d was not acknowledged, so the data did not land: %v", i+1, err)
		}
	}

	// Two patients, three messages: the A08 updates rather than duplicates.
	testutil.Eventually(t, "both patients in FHIR", func() bool { return fhir.count() == 2 })

	csvPath := filepath.Join(f.Work, "out", "patients.csv")
	testutil.Eventually(t, "three csv rows", func() bool {
		raw, err := os.ReadFile(csvPath)
		return err == nil && strings.Count(strings.TrimRight(string(raw), "\n"), "\n") == 3
	})

	raw, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	wantCSV := "id,last_name,first_name,dob\n" +
		"MERC0000042,THORNQUIST,IMOGEN,19580714\n" +
		"MERC0000042,THORNQUIST-VALE,IMOGEN,19580714\n" +
		"STLU0000007,OAKHURST,BRAM,19910223\n"
	if string(raw) != wantCSV {
		t.Errorf("csv =\n%s\nwant\n%s", raw, wantCSV)
	}

	// The FHIR resource must carry the corrected name: the A08 landed on the
	// same resource, which is the whole point of addressing it by MRN.
	p := fhir.patient("MERC0000042")
	if p == nil {
		t.Fatal("no Patient stored for MERC0000042")
	}
	names, _ := p["name"].([]any)
	if len(names) == 0 {
		t.Fatal("Patient has no name")
	}
	first, _ := names[0].(map[string]any)
	if got := first["family"]; got != "THORNQUIST-VALE" {
		t.Errorf("family = %v, want the corrected THORNQUIST-VALE", got)
	}
	given, _ := first["given"].([]any)
	if len(given) == 0 || given[0] != "IMOGEN" {
		t.Errorf("given = %v", given)
	}
	if got := p["birthDate"]; got != "1958-07-14" {
		t.Errorf("birthDate = %v, want the FHIR date form", got)
	}
	if got := p["gender"]; got != "female" {
		t.Errorf("gender = %v, want female", got)
	}
	if got := p["resourceType"]; got != "Patient" {
		t.Errorf("resourceType = %v", got)
	}
	if got := p["active"]; got != true {
		t.Errorf("active = %v (%T), want the JSON boolean true", got, got)
	}

	// Identifiers: the facility MRN qualified by its assigning facility, and
	// the enterprise id, so either finds the patient.
	ids, _ := p["identifier"].([]any)
	if len(ids) != 2 {
		t.Fatalf("identifier count = %d, want the MRN and the enterprise id", len(ids))
	}
	mrnID, _ := ids[0].(map[string]any)
	if got := mrnID["system"]; got != "urn:waggle:mrn:MERCY" {
		t.Errorf("MRN system = %v, want it qualified by the assigning facility", got)
	}
	if got := mrnID["value"]; got != "MERC0000042" {
		t.Errorf("MRN value = %v", got)
	}
	eid, _ := ids[1].(map[string]any)
	if got := eid["value"]; got != "EID0000066" {
		t.Errorf("enterprise id = %v", got)
	}

	// Address and phone should survive too.
	addrs, _ := p["address"].([]any)
	if len(addrs) == 0 {
		t.Fatal("Patient has no address")
	}
	a0, _ := addrs[0].(map[string]any)
	if got := a0["city"]; got != "SPRINGDALE" {
		t.Errorf("city = %v", got)
	}

	// The second facility's patient keeps its own assigning authority.
	p2 := fhir.patient("STLU0000007")
	if p2 == nil {
		t.Fatal("no Patient stored for STLU0000007")
	}
	ids2, _ := p2["identifier"].([]any)
	id2, _ := ids2[0].(map[string]any)
	if got := id2["system"]; got != "urn:waggle:mrn:STLUKE" {
		t.Errorf("second facility MRN system = %v", got)
	}

	if problems := fhir.problems(); len(problems) > 0 {
		t.Errorf("requests went somewhere unexpected: %v", problems)
	}
	for _, m := range fhir.methods {
		if m != http.MethodPut {
			t.Errorf("method %s; every write should be an idempotent PUT", m)
		}
	}

	f.WaitMessageState(t, "adt-to-csv-and-fhir", "fhir", message.StateSent)
}

// A message with no patient identifier must be filtered out before it can
// write a blank CSV row or a Patient with no identifier.
func TestNonPatientMessagesAreFiltered(t *testing.T) {
	f := testutil.NewFixture(t, testutil.InMemory())
	fhir := newFHIRStub(t)

	ch := f.StartExample(t, "adt-to-csv-and-fhir", map[string]string{
		"127.0.0.1:2575":              "127.0.0.1:0",
		"http://127.0.0.1:8080/fhir/": fhir.URL + "/fhir/",
		"dir: out":                    "dir: " + filepath.Join(f.Work, "out"),
	})
	addr := testutil.ListenAddr(t, ch)

	noPID := "MSH|^~\\&|WAGGLESIM|SIMNET|WAGGLE|TEST|20260302170000||ADT^A01^ADT_A01|SIMT0009|T|2.5.1\r" +
		"EVN|A01|20260302170000\r" +
		"PV1|1|I|3WEST^312^B^MERCY\r"
	sender, err := mllp.NewSender(map[string]any{"addr": addr, "ackTimeout": "20s"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.Send(context.Background(), []byte(noPID), nil); err != nil {
		t.Fatalf("a filtered message should still be acknowledged: %v", err)
	}
	f.WaitMessageState(t, "adt-to-csv-and-fhir", "", message.StateFiltered)

	if n := fhir.count(); n != 0 {
		t.Errorf("%d patients written for a message with no PID", n)
	}
	if _, err := os.Stat(filepath.Join(f.Work, "out", "patients.csv")); err == nil {
		raw, _ := os.ReadFile(filepath.Join(f.Work, "out", "patients.csv"))
		if strings.Count(strings.TrimRight(string(raw), "\n"), "\n") > 0 {
			t.Errorf("csv gained a row for a message with no PID:\n%s", raw)
		}
	}
}
