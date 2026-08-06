package httpout

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/langhorst/waggle/internal/adapter"
)

type captured struct {
	method, path, query, body, contentType, auth, apiKey string
}

// startServer runs a test API that records the last request and answers with
// the status set via *status.
func startServer(t *testing.T, status *int) (*httptest.Server, *captured) {
	t.Helper()
	var mu sync.Mutex
	got := &captured{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		*got = captured{
			method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
			body: string(body), contentType: r.Header.Get("Content-Type"),
			auth: r.Header.Get("Authorization"), apiKey: r.Header.Get("X-Api-Key"),
		}
		mu.Unlock()
		if *status >= 400 {
			http.Error(w, "the API said no", *status)
			return
		}
		w.WriteHeader(*status)
	}))
	t.Cleanup(ts.Close)
	return ts, got
}

func newSender(t *testing.T, settings map[string]any) *Sender {
	t.Helper()
	s, err := NewSender(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSendSuccess(t *testing.T) {
	status := 201
	ts, got := startServer(t, &status)
	s := newSender(t, map[string]any{
		"url":     ts.URL + "/fhir/Patient?tenant=lab",
		"headers": map[string]string{"X-Api-Key": "sekrit"},
	})
	if err := s.Send(context.Background(), []byte(`{"resourceType":"Patient"}`), nil); err != nil {
		t.Fatal(err)
	}
	if got.method != "POST" || got.path != "/fhir/Patient" || got.query != "tenant=lab" {
		t.Errorf("request = %s %s?%s", got.method, got.path, got.query)
	}
	if got.body != `{"resourceType":"Patient"}` || got.contentType != "application/json" || got.apiKey != "sekrit" {
		t.Errorf("body/headers = %+v", got)
	}
}

func TestBasicAuthAndContentType(t *testing.T) {
	status := 200
	ts, got := startServer(t, &status)
	s := newSender(t, map[string]any{
		"url": ts.URL, "basicUser": "u", "basicPass": "p", "contentType": "text/plain",
	})
	if err := s.Send(context.Background(), []byte("hi"), nil); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got.auth, "Basic ") || got.contentType != "text/plain" {
		t.Errorf("auth=%q ct=%q", got.auth, got.contentType)
	}
}

func TestMetaOverrides(t *testing.T) {
	status := 200
	ts, got := startServer(t, &status)
	s := newSender(t, map[string]any{"url": ts.URL + "/fhir/Patient"})

	meta := map[string]string{MetaMethod: "put", MetaPath: "/fhir/Patient/12345"}
	if err := s.Send(context.Background(), []byte(`{}`), meta); err != nil {
		t.Fatal(err)
	}
	if got.method != "PUT" || got.path != "/fhir/Patient/12345" {
		t.Errorf("override = %s %s", got.method, got.path)
	}

	// A relative path resolves against the configured URL.
	meta = map[string]string{MetaPath: "Patient/9"}
	if err := s.Send(context.Background(), []byte(`{}`), meta); err != nil {
		t.Fatal(err)
	}
	if got.path != "/fhir/Patient/9" {
		t.Errorf("relative override path = %s", got.path)
	}
}

func TestStatusClassification(t *testing.T) {
	status := 200
	ts, _ := startServer(t, &status)
	s := newSender(t, map[string]any{"url": ts.URL})
	send := func() error { return s.Send(context.Background(), []byte("x"), nil) }

	for _, transient := range []int{500, 502, 503, 408, 429} {
		status = transient
		err := send()
		if err == nil || adapter.IsPermanent(err) {
			t.Errorf("status %d: want transient error, got %v", transient, err)
		}
	}
	for _, permanent := range []int{400, 404, 409, 422} {
		status = permanent
		err := send()
		if err == nil || !adapter.IsPermanent(err) {
			t.Errorf("status %d: want permanent error, got %v", permanent, err)
		}
		if !strings.Contains(err.Error(), "the API said no") {
			t.Errorf("status %d: response body missing from error: %v", permanent, err)
		}
	}
	status = 204
	if err := send(); err != nil {
		t.Errorf("204 = %v", err)
	}
}

func TestConnectionRefusedIsTransient(t *testing.T) {
	s := newSender(t, map[string]any{"url": "http://127.0.0.1:1/nowhere", "timeout": "500ms"})
	err := s.Send(context.Background(), []byte("x"), nil)
	if err == nil || adapter.IsPermanent(err) {
		t.Fatalf("want transient error, got %v", err)
	}
}

func TestInvalidMetaPathIsPermanent(t *testing.T) {
	status := 200
	ts, _ := startServer(t, &status)
	s := newSender(t, map[string]any{"url": ts.URL})
	err := s.Send(context.Background(), []byte("x"), map[string]string{MetaPath: "::bad::url"})
	if err == nil || !adapter.IsPermanent(err) {
		t.Fatalf("want permanent error, got %v", err)
	}
}

func TestTLSWithCAFile(t *testing.T) {
	status := 200
	var mu sync.Mutex
	var gotPath string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotPath = r.URL.Path
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(ts.Close)

	certOut := filepath.Join(t.TempDir(), "ca.pem")
	pemData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw})
	if err := os.WriteFile(certOut, pemData, 0o600); err != nil {
		t.Fatal(err)
	}

	s := newSender(t, map[string]any{"url": ts.URL + "/secure", "caFile": certOut})
	if err := s.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/secure" {
		t.Errorf("path = %s", gotPath)
	}

	// Without the CA file the certificate is untrusted (transient error).
	plain := newSender(t, map[string]any{"url": ts.URL})
	if err := plain.Send(context.Background(), []byte("x"), nil); err == nil || adapter.IsPermanent(err) {
		t.Fatalf("want transient TLS error, got %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	for name, settings := range map[string]map[string]any{
		"missing url": {},
		"bad scheme":  {"url": "ftp://x"},
		"unparseable": {"url": "http://bad url with spaces"},
		"unknown key": {"url": "http://ok", "bogus": 1},
		"missing ca":  {"url": "https://ok", "caFile": "/does/not/exist.pem"},
	} {
		if _, err := NewSender(settings); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
