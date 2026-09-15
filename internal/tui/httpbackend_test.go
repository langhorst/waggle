package tui_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/api"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
	"github.com/langhorst/waggle/internal/testutil"
	"github.com/langhorst/waggle/internal/tui"
)

const token = "tui-test-token"

// startDaemon runs a real engine behind a real API server and returns a
// backend pointed at it plus the fixture for feeding messages.
func startDaemon(t *testing.T) (*tui.HTTPBackend, *testutil.Fixture, string) {
	t.Helper()
	f := testutil.NewFixture(t)
	inDir := filepath.Join(f.Work, "in")
	outDir := filepath.Join(f.Work, "out")
	f.StartChannelYAML(t, "feed", `
id: feed
name: Feed
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 30ms, minAge: 1ms}
destinations:
  - id: out
    adapter:
      type: file-writer
      settings: {dir: `+outDir+`, pattern: "{id}.hl7"}
    queue: {retryInterval: 10ms}
`)
	srv := &api.Server{Eng: f.Eng, Scripts: f.Scripts, Auth: api.AuthConfig{Token: token}, Log: slog.New(slog.DiscardHandler)}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &tui.HTTPBackend{BaseURL: ts.URL, Token: token}, f, inDir
}

func TestHTTPBackendObservesDaemon(t *testing.T) {
	b, f, inDir := startDaemon(t)
	ctx := context.Background()

	channels, err := b.ChannelSummaries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 || channels[0].ID != "feed" || channels[0].Status != "STARTED" {
		t.Fatalf("channels = %+v", channels)
	}
	if _, err := b.ChannelSummary(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown channel err = %v, want not found", err)
	}

	// Subscribe before feeding so the event is not missed.
	evs, cancel := b.Subscribe(64)
	defer cancel()

	hl7 := "MSH|^~\\&|A|B|C|D|20260101||ADT^A01|X1|P|2.5\rPID|1||MRN1||DOE^JOHN\r"
	testutil.WriteFile(t, filepath.Join(inDir, "one.hl7"), hl7)
	id := f.WaitForDestinationState(t, "feed", "out", message.StateSent)

	deadline := time.After(testutil.Timeout)
	sawEvent := false
	for !sawEvent {
		select {
		case ev, ok := <-evs:
			if !ok {
				t.Fatal("event stream closed")
			}
			if ev.Type == events.TypeMessage && ev.ChannelID == "feed" && ev.MessageID == id {
				sawEvent = true
			}
		case <-deadline:
			t.Fatal("no message event arrived over SSE")
		}
	}

	list, err := b.ListMessages(ctx, "feed", store.ListQuery{Limit: 10})
	if err != nil || len(list) != 1 || list[0].ID != id {
		t.Fatalf("list = %+v, %v", list, err)
	}
	detail, err := b.GetMessage(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(detail.Raw) != hl7 || len(detail.Transformed) == 0 || len(detail.Destinations) != 1 {
		t.Errorf("detail = raw %q, transformed %d bytes, %d destinations", detail.Raw, len(detail.Transformed), len(detail.Destinations))
	}
	if got := engine.Stages(detail); strings.Join(got, ",") != "received,transformed,dest:out" {
		t.Errorf("stages = %v", got)
	}
	tree, dt, err := b.MessageTree(ctx, id, engine.StageReceived)
	if err != nil || dt != "hl7v2" || tree == nil || len(tree.Children) != 2 {
		t.Errorf("tree = %v, %q, %v", tree, dt, err)
	}
	if _, err := b.MessageDiff(ctx, id, ""); err != nil {
		t.Errorf("diff: %v", err)
	}
	summary, err := b.ChannelSummary(ctx, "feed")
	if err != nil || summary.Sent() != 1 {
		t.Errorf("summary = %+v, %v", summary, err)
	}

	// Cancelling the subscription returns promptly and closes the channel.
	done := make(chan struct{})
	go func() { cancel(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel hung")
	}
}

func TestHTTPBackendRejectedWithoutToken(t *testing.T) {
	b, _, _ := startDaemon(t)
	b.Token = "wrong"
	_, err := b.ChannelSummaries(context.Background())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v, want a 401", err)
	}
}

func TestHTTPBackendUnreachable(t *testing.T) {
	b := &tui.HTTPBackend{BaseURL: "http://127.0.0.1:1"}
	if _, err := b.ChannelSummaries(context.Background()); err == nil || !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("err = %v", err)
	}
}

// TestHTTPBackendBasicAuth: a daemon configured with basicUser/basicPassword
// is reachable by the TUI, which was previously token-only.
func TestHTTPBackendBasicAuth(t *testing.T) {
	var gotUser, gotPass, gotAuthHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		u, p, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="waggle"`)
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		gotUser, gotPass = u, p
		if u != "ops" || p != "pw" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	b := &tui.HTTPBackend{BaseURL: srv.URL, BasicUser: "ops", BasicPassword: "pw"}
	if _, err := b.ChannelSummaries(context.Background()); err != nil {
		t.Fatalf("basic auth should be accepted: %v", err)
	}
	if gotUser != "ops" || gotPass != "pw" {
		t.Errorf("sent user/pass = %q/%q", gotUser, gotPass)
	}
	if !strings.HasPrefix(gotAuthHeader, "Basic ") {
		t.Errorf("expected a Basic header, got %q", gotAuthHeader)
	}

	// Wrong password surfaces as tui.ErrUnauthorized so the CLI can explain it.
	bad := &tui.HTTPBackend{BaseURL: srv.URL, BasicUser: "ops", BasicPassword: "nope"}
	_, err := bad.ChannelSummaries(context.Background())
	if !errors.Is(err, tui.ErrUnauthorized) {
		t.Errorf("expected tui.ErrUnauthorized, got %v", err)
	}

	// No credentials at all is also tui.ErrUnauthorized, not a generic failure.
	none := &tui.HTTPBackend{BaseURL: srv.URL}
	if _, err := none.ChannelSummaries(context.Background()); !errors.Is(err, tui.ErrUnauthorized) {
		t.Errorf("expected tui.ErrUnauthorized with no credentials, got %v", err)
	}
}

// A token takes precedence over basic credentials: it satisfies the daemon
// on its own and is what the Bearer header is for.
func TestHTTPBackendTokenWins(t *testing.T) {
	var header string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	b := &tui.HTTPBackend{BaseURL: srv.URL, Token: "tok", BasicUser: "ops", BasicPassword: "pw"}
	if _, err := b.ChannelSummaries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if header != "Bearer tok" {
		t.Errorf("Authorization = %q, want the bearer token", header)
	}
}
