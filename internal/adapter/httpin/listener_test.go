package httpin

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

// startListener boots a listener on a loopback port with the given extra
// settings and a DeliverFunc, returning its base URL and a cleanup-registered
// stop.
func startListener(t *testing.T, settings map[string]any, deliver adapter.DeliverFunc) string {
	t.Helper()
	base := map[string]any{"listen": "127.0.0.1:0"}
	for k, v := range settings {
		base[k] = v
	}
	l, err := NewListener(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background(), deliver); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop() })
	return "http://" + l.Addr()
}

// accept is a DeliverFunc that records the message and resolves Done with
// the given decision.
func accept(id int64, code, text string) adapter.DeliverFunc {
	return func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		done := make(chan adapter.AckDecision, 1)
		done <- adapter.AckDecision{Code: code, Text: text}
		return adapter.Receipt{MessageID: id, Done: done}, nil
	}
}

func post(t *testing.T, url, body string, opt func(*http.Request)) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if opt != nil {
		opt(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &decoded)
	return resp.StatusCode, decoded
}

func TestImmediateMode(t *testing.T) {
	var gotRaw []byte
	var gotMeta map[string]string
	url := startListener(t, map[string]any{"path": "/intake"},
		func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
			gotRaw, gotMeta = raw, meta
			done := make(chan adapter.AckDecision, 1)
			return adapter.Receipt{MessageID: 42, Done: done}, nil
		})

	code, body := post(t, url+"/intake?src=lab&x=1", `{"hello":"world"}`, func(r *http.Request) {
		r.Header.Set("Content-Type", "application/json")
	})
	if code != http.StatusAccepted || body["messageId"] != float64(42) {
		t.Fatalf("immediate = %d %v", code, body)
	}
	if string(gotRaw) != `{"hello":"world"}` {
		t.Errorf("raw = %q", gotRaw)
	}
	for k, want := range map[string]string{
		"source.http.method": "POST", "source.http.path": "/intake",
		"source.http.query.src": "lab", "source.http.query.x": "1",
		"source.http.header.content-type": "application/json",
	} {
		if gotMeta[k] != want {
			t.Errorf("meta[%s] = %q, want %q", k, gotMeta[k], want)
		}
	}
	if gotMeta["source.remote"] == "" {
		t.Error("missing source.remote")
	}
	// Inbound context must never look like an http-sender routing hint.
	for k := range gotMeta {
		if !strings.HasPrefix(k, "source.") {
			t.Errorf("inbound meta key %q is outside the source.* namespace", k)
		}
	}
}

func TestDestinationMode(t *testing.T) {
	for _, tc := range []struct {
		ack        string
		wantStatus int
	}{
		{"AA", 200}, {"AR", 400}, {"AE", 500},
	} {
		url := startListener(t, map[string]any{"ackMode": "destination"}, accept(7, tc.ack, "detail"))
		code, body := post(t, url+"/", "payload", nil)
		if code != tc.wantStatus || body["code"] != tc.ack || body["text"] != "detail" {
			t.Errorf("%s: got %d %v", tc.ack, code, body)
		}
	}
}

func TestDestinationHoldTimeout(t *testing.T) {
	url := startListener(t, map[string]any{"ackMode": "destination", "holdTimeout": "50ms"},
		func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
			return adapter.Receipt{MessageID: 1, Done: make(chan adapter.AckDecision, 1)}, nil
		})
	code, body := post(t, url+"/", "payload", nil)
	if code != http.StatusGatewayTimeout || body["code"] != "AE" {
		t.Fatalf("timeout = %d %v", code, body)
	}
}

func TestDeliverErrorIs503(t *testing.T) {
	url := startListener(t, nil,
		func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
			return adapter.Receipt{}, fmt.Errorf("store down")
		})
	code, body := post(t, url+"/", "payload", nil)
	if code != http.StatusServiceUnavailable || body["error"] != "store down" {
		t.Fatalf("deliver error = %d %v", code, body)
	}
}

func TestRejections(t *testing.T) {
	url := startListener(t, map[string]any{
		"path": "/hook", "maxBodySize": 16,
		"basicUser": "u", "basicPass": "p",
		"requiredHeaders": map[string]string{"X-Api-Key": "sekrit"},
	}, accept(1, "AA", ""))

	withAuth := func(r *http.Request) {
		r.SetBasicAuth("u", "p")
		r.Header.Set("X-Api-Key", "sekrit")
	}

	if code, _ := post(t, url+"/nope", "x", withAuth); code != 404 {
		t.Errorf("wrong path = %d", code)
	}
	if code, _ := post(t, url+"/hook", "x", nil); code != 401 {
		t.Errorf("no auth = %d", code)
	}
	if code, _ := post(t, url+"/hook", "x", func(r *http.Request) { r.SetBasicAuth("u", "wrong"); r.Header.Set("X-Api-Key", "sekrit") }); code != 401 {
		t.Errorf("bad password = %d", code)
	}
	if code, _ := post(t, url+"/hook", "x", func(r *http.Request) { r.SetBasicAuth("u", "p") }); code != 401 {
		t.Errorf("missing header = %d", code)
	}
	req, _ := http.NewRequest(http.MethodGet, url+"/hook", nil)
	withAuth(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("GET = %d", resp.StatusCode)
	}
	if code, _ := post(t, url+"/hook", "this body is far too large", withAuth); code != 413 {
		t.Errorf("oversize = %d", code)
	}
	if code, _ := post(t, url+"/hook", "", withAuth); code != 400 {
		t.Errorf("empty body = %d", code)
	}
	if code, _ := post(t, url+"/hook", "ok body", withAuth); code != 202 {
		t.Errorf("valid = %d", code)
	}
}

func TestStopUnblocksHeldRequests(t *testing.T) {
	l, err := NewListener(map[string]any{
		"listen": "127.0.0.1:0", "ackMode": "destination", "holdTimeout": "1m",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = l.Start(context.Background(), func(ctx context.Context, raw []byte, meta map[string]string) (adapter.Receipt, error) {
		return adapter.Receipt{MessageID: 1, Done: make(chan adapter.AckDecision, 1)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = http.Post("http://"+l.Addr()+"/", "text/plain", bytes.NewReader([]byte("held")))
	}()
	time.Sleep(100 * time.Millisecond) // let the request arrive and block
	stopped := make(chan struct{})
	go func() { _ = l.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not unblock the held request")
	}
}

func TestTLS(t *testing.T) {
	certPEM, keyPEM := selfSigned(t)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	l, err := NewListener(map[string]any{
		"listen": "127.0.0.1:0", "certFile": certFile, "keyFile": keyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background(), accept(9, "AA", "")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop() })

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}}
	resp, err := client.Post("https://"+l.Addr()+"/", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("tls post = %d", resp.StatusCode)
	}
}

func TestConfigValidation(t *testing.T) {
	for name, settings := range map[string]map[string]any{
		"missing listen": {},
		"bad ackMode":    {"listen": ":0", "ackMode": "sometimes"},
		"cert only":      {"listen": ":0", "certFile": "x.pem"},
		"unknown key":    {"listen": ":0", "bogus": true},
	} {
		if _, err := NewListener(settings); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// selfSigned generates a throwaway certificate for 127.0.0.1.
func selfSigned(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "waggle-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
