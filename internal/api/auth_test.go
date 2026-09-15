package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/engine"
)

const testToken = "test-token-0123456789"

// authServer builds a Server with no channels so auth behaviour can be
// exercised without the full stack; every route sits behind protect.
func authServer(t *testing.T, auth AuthConfig) *httptest.Server {
	t.Helper()
	srv := &Server{Eng: engine.New(engine.Options{}), Auth: auth}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func request(t *testing.T, ts *httptest.Server, method, path string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp
}

func basic(user, pass string) string {
	req, _ := http.NewRequest("GET", "/", nil)
	req.SetBasicAuth(user, pass)
	return req.Header.Get("Authorization")
}

func TestAuthToken(t *testing.T) {
	ts := authServer(t, AuthConfig{Token: testToken})

	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"no credentials", nil, http.StatusUnauthorized},
		{"wrong bearer", map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
		{"bearer prefix only", map[string]string{"Authorization": "Bearer " + testToken[:5]}, http.StatusUnauthorized},
		{"bearer ok", map[string]string{"Authorization": "Bearer " + testToken}, http.StatusOK},
		{"basic with token as password", map[string]string{"Authorization": basic("anyone", testToken)}, http.StatusOK},
		{"basic wrong password", map[string]string{"Authorization": basic("anyone", "nope")}, http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := request(t, ts, "GET", "/api/status", tc.headers)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if tc.want == http.StatusUnauthorized && !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Basic") {
				t.Errorf("401 without a Basic challenge: %q", resp.Header.Get("WWW-Authenticate"))
			}
		})
	}
}

func TestAuthCoversEveryRoute(t *testing.T) {
	ts := authServer(t, AuthConfig{Token: testToken})
	for _, path := range []string{"/", "/static/htmx.min.js", "/api/channels", "/api/events", "/ui/channels"} {
		resp := request(t, ts, "GET", path, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s without credentials = %d, want 401", path, resp.StatusCode)
		}
	}
}

func TestAuthBasicUser(t *testing.T) {
	ts := authServer(t, AuthConfig{BasicUser: "ops", BasicPassword: "pw"})
	if resp := request(t, ts, "GET", "/api/status", map[string]string{"Authorization": basic("ops", "pw")}); resp.StatusCode != 200 {
		t.Errorf("correct basic creds = %d", resp.StatusCode)
	}
	for _, h := range []string{basic("ops", "wrong"), basic("other", "pw"), "Bearer pw"} {
		if resp := request(t, ts, "GET", "/api/status", map[string]string{"Authorization": h}); resp.StatusCode != 401 {
			t.Errorf("%q = %d, want 401", h, resp.StatusCode)
		}
	}
}

func TestAuthDisabled(t *testing.T) {
	ts := authServer(t, AuthConfig{Disabled: true})
	if resp := request(t, ts, "GET", "/api/status", nil); resp.StatusCode != 200 {
		t.Errorf("disabled auth = %d", resp.StatusCode)
	}
}

func TestCrossSiteGuard(t *testing.T) {
	ts := authServer(t, AuthConfig{Token: testToken})
	auth := map[string]string{"Authorization": "Bearer " + testToken}
	with := func(extra map[string]string) map[string]string {
		h := map[string]string{}
		for k, v := range auth {
			h[k] = v
		}
		for k, v := range extra {
			h[k] = v
		}
		return h
	}
	host := strings.TrimPrefix(ts.URL, "http://")

	// Unknown channel: 404 proves the request reached the handler.
	cases := []struct {
		name    string
		method  string
		headers map[string]string
		want    int
	}{
		{"no browser headers", "POST", auth, http.StatusNotFound},
		{"same-origin fetch", "POST", with(map[string]string{"Sec-Fetch-Site": "same-origin"}), http.StatusNotFound},
		{"navigation", "POST", with(map[string]string{"Sec-Fetch-Site": "none"}), http.StatusNotFound},
		{"matching origin", "POST", with(map[string]string{"Origin": "http://" + host}), http.StatusNotFound},
		{"cross-site fetch", "POST", with(map[string]string{"Sec-Fetch-Site": "cross-site"}), http.StatusForbidden},
		{"same-site subdomain", "POST", with(map[string]string{"Sec-Fetch-Site": "same-site"}), http.StatusForbidden},
		{"foreign origin", "POST", with(map[string]string{"Origin": "https://evil.example"}), http.StatusForbidden},
		{"foreign origin on GET is fine", "GET", with(map[string]string{"Origin": "https://evil.example"}), http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := "/api/channels/nope/start"
			if tc.method == "GET" {
				path = "/api/channels"
			}
			resp := request(t, ts, tc.method, path, tc.headers)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}
