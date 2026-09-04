package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/langhorst/waggle/internal/config"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"

	_ "github.com/langhorst/waggle/internal/adapter/file"
	_ "github.com/langhorst/waggle/internal/format/csvfmt"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
)

const sampleHL7 = "MSH|^~\\&|SEND|SFAC|RECV|RFAC|20260730||ADT^A01|CTRL001|P|2.5\rPID|1||MRN1||DOE^JOHN\r"

type harness struct {
	t       *testing.T
	ts      *httptest.Server
	eng     *engine.Engine
	st      *store.Store
	work    string
	inDir   string
	scripts *script.Engine
}

// newHarness boots a full stack: store, script engine, one scripted channel
// (HL7 in from files, transformed, CSV out to files), and the HTTP server.
func newHarness(t *testing.T) *harness {
	t.Helper()
	work := t.TempDir()
	inDir := filepath.Join(work, "in")
	outDir := filepath.Join(work, "out")
	channelsDir := filepath.Join(work, "channels")
	scriptsDir := filepath.Join(channelsDir, "scripts")
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(inDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile := func(path, content string) {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(filepath.Join(scriptsDir, "upper.js"),
		`function transform(msg) { msg.set('PID-5.1', msg.get('PID-5.1').toUpperCase()); }`)
	writeFile(filepath.Join(channelsDir, "feed.yaml"), `
id: feed
source:
  type: file-reader
  dataType: hl7v2
  settings: {dir: `+inDir+`, interval: 30ms, minAge: 1ms}
transformers: [scripts/upper.js]
destinations:
  - id: out
    adapter:
      type: file-writer
      settings: {dir: `+outDir+`, pattern: "{id}.hl7"}
    queue: {retryInterval: 10ms}
`)

	st, err := store.Open(filepath.Join(work, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	scripts := script.New(script.Options{Log: slog.New(slog.DiscardHandler)})
	t.Cleanup(scripts.Close)

	eng := engine.New(engine.Options{Store: st, Scripts: scripts, Log: slog.New(slog.DiscardHandler)})
	channels, err := config.LoadChannels(channelsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range channels {
		if err := eng.LoadChannel(ch); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Start(context.Background(), "feed"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.Shutdown)

	srv := &Server{
		Eng:         eng,
		Scripts:     scripts,
		ScriptsRoot: channelsDir,
		Auth:        AuthConfig{Token: testToken},
		Log:         slog.New(slog.DiscardHandler),
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &harness{t: t, ts: ts, eng: eng, st: st, work: work, inDir: inDir, scripts: scripts}
}

func (h *harness) do(method, path string, body string) (int, []byte) {
	h.t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, h.ts.URL+path, rdr)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func (h *harness) decode(raw []byte, v any) {
	h.t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		h.t.Fatalf("bad JSON %q: %v", raw, err)
	}
}

// feedMessage drops a message file and waits until a NEW message (beyond
// whatever already exists) reaches SENT.
func (h *harness) feedMessage(content string) int64 {
	h.t.Helper()
	var baseline int64
	if list, err := h.st.ListMessages(context.Background(), "feed", store.ListQuery{Limit: 1}); err == nil && len(list) > 0 {
		baseline = list[0].ID
	}
	name := fmt.Sprintf("m-%d.hl7", time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(h.inDir, name), []byte(content), 0o644); err != nil {
		h.t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		list, err := h.st.ListMessages(context.Background(), "feed", store.ListQuery{Limit: 1})
		if err == nil && len(list) > 0 && list[0].ID > baseline {
			d, err := h.st.GetMessage(context.Background(), list[0].ID)
			if err == nil && len(d.Destinations) > 0 && d.Destinations[0].State == message.StateSent {
				return d.ID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatal("message never reached SENT")
	return 0
}

func TestStatusAndChannels(t *testing.T) {
	h := newHarness(t)

	code, raw := h.do("GET", "/api/status", "")
	if code != 200 {
		t.Fatalf("status = %d %s", code, raw)
	}
	var status map[string]any
	h.decode(raw, &status)
	if status["channels"].(float64) != 1 || status["channelsRunning"].(float64) != 1 {
		t.Errorf("status = %v", status)
	}

	code, raw = h.do("GET", "/api/channels", "")
	if code != 200 {
		t.Fatalf("channels = %d", code)
	}
	var channels []map[string]any
	h.decode(raw, &channels)
	if len(channels) != 1 || channels[0]["id"] != "feed" || channels[0]["status"] != "STARTED" {
		t.Errorf("channels = %v", channels)
	}
}

func TestLifecycleEndpoints(t *testing.T) {
	h := newHarness(t)

	code, raw := h.do("POST", "/api/channels/feed/pause", "")
	if code != 200 || !strings.Contains(string(raw), "PAUSED") {
		t.Errorf("pause = %d %s", code, raw)
	}
	code, raw = h.do("POST", "/api/channels/feed/start", "")
	if code != 200 || !strings.Contains(string(raw), "STARTED") {
		t.Errorf("resume = %d %s", code, raw)
	}
	code, raw = h.do("POST", "/api/channels/feed/stop", "")
	if code != 200 || !strings.Contains(string(raw), "STOPPED") {
		t.Errorf("stop = %d %s", code, raw)
	}
	code, raw = h.do("POST", "/api/channels/feed/reload", "")
	if code != 200 || !strings.Contains(string(raw), "STOPPED") {
		t.Errorf("reload should preserve stopped state = %d %s", code, raw)
	}
	code, _ = h.do("POST", "/api/channels/nope/start", "")
	if code != 404 {
		t.Errorf("unknown channel start = %d", code)
	}
	// Restart for other assertions.
	if code, _ := h.do("POST", "/api/channels/feed/start", ""); code != 200 {
		t.Fatal("restart failed")
	}
}

func TestMessageInspection(t *testing.T) {
	h := newHarness(t)
	id := h.feedMessage(sampleHL7)

	code, raw := h.do("GET", "/api/channels/feed/messages?limit=10", "")
	if code != 200 {
		t.Fatalf("messages = %d", code)
	}
	var list []map[string]any
	h.decode(raw, &list)
	if len(list) != 1 || int64(list[0]["id"].(float64)) != id {
		t.Fatalf("list = %v", list)
	}

	code, raw = h.do("GET", fmt.Sprintf("/api/messages/%d", id), "")
	if code != 200 {
		t.Fatalf("message = %d", code)
	}
	var detail map[string]any
	h.decode(raw, &detail)
	if !strings.Contains(detail["raw"].(string), "DOE^JOHN") ||
		!strings.Contains(detail["transformed"].(string), "DOE^JOHN") {
		t.Errorf("payloads missing: %v", detail)
	}

	// Tree: received vs transformed stages.
	code, raw = h.do("GET", fmt.Sprintf("/api/messages/%d/tree?stage=received", id), "")
	if code != 200 || !strings.Contains(string(raw), `"PID"`) {
		t.Errorf("tree = %d %s", code, raw)
	}
	code, _ = h.do("GET", fmt.Sprintf("/api/messages/%d/tree?stage=bogus", id), "")
	if code != 400 {
		t.Errorf("bad stage = %d", code)
	}

	// Diff: the transformer uppercased PID-5.1 (DOE was already uppercase,
	// so re-feed with a lowercase name to see a change).
	lower := strings.Replace(sampleHL7, "DOE^JOHN", "doe^JOHN", 1)
	lower = strings.Replace(lower, "CTRL001", "CTRL002", 1)
	id2 := h.feedMessage(lower)
	code, raw = h.do("GET", fmt.Sprintf("/api/messages/%d/diff", id2), "")
	if code != 200 {
		t.Fatalf("diff = %d %s", code, raw)
	}
	var diff []message.DiffEntry
	h.decode(raw, &diff)
	found := false
	for _, e := range diff {
		if e.Path == "PID-5.1" && e.Op == message.DiffChanged && e.From == "doe" && e.To == "DOE" {
			found = true
		}
	}
	if !found {
		t.Errorf("diff missing PID-5.1 change: %+v", diff)
	}

	code, _ = h.do("GET", "/api/messages/999999", "")
	if code != 404 {
		t.Errorf("missing message = %d", code)
	}
}

func TestReplayEndpoint(t *testing.T) {
	h := newHarness(t)
	id := h.feedMessage(sampleHL7)

	code, raw := h.do("POST", fmt.Sprintf("/api/messages/%d/replay", id), "")
	if code != 200 {
		t.Fatalf("replay = %d %s", code, raw)
	}
	var out map[string]int64
	h.decode(raw, &out)
	if out["replayId"] == 0 || out["replayId"] == id {
		t.Errorf("replayId = %d", out["replayId"])
	}
}

func TestDLQAndRequeue(t *testing.T) {
	h := newHarness(t)
	id := h.feedMessage(sampleHL7)
	ctx := context.Background()

	// Manufacture a dead-lettered delivery for a second destination.
	if err := h.st.SetDestinationState(ctx, id, "flaky", message.StateQueued, []byte("payload"), nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := h.st.Enqueue(ctx, "feed", "flaky", id); err != nil {
		t.Fatal(err)
	}
	head, err := h.st.Head(ctx, "feed", "flaky")
	if err != nil || head == nil {
		t.Fatal(err)
	}
	if err := h.st.DeadLetter(ctx, head, "flaky", "receiver rejected"); err != nil {
		t.Fatal(err)
	}

	code, raw := h.do("GET", "/api/channels/feed/dlq", "")
	if code != 200 || !strings.Contains(string(raw), "receiver rejected") {
		t.Fatalf("dlq = %d %s", code, raw)
	}

	code, _ = h.do("POST", fmt.Sprintf("/api/dlq/%d/flaky/requeue", id), "")
	if code != 200 {
		t.Errorf("requeue = %d", code)
	}
	code, _ = h.do("POST", fmt.Sprintf("/api/dlq/%d/flaky/requeue", id), "")
	if code != 409 {
		t.Errorf("double requeue = %d", code)
	}
}

func TestScriptEndpoints(t *testing.T) {
	h := newHarness(t)

	code, raw := h.do("GET", "/api/channels/feed/scripts", "")
	if code != 200 {
		t.Fatalf("channel scripts = %d", code)
	}
	var refs []scriptRef
	h.decode(raw, &refs)
	if len(refs) != 1 || refs[0].Role != "transformer" || refs[0].LastError != "" {
		t.Fatalf("refs = %+v", refs)
	}
	path := refs[0].Path

	code, raw = h.do("GET", "/api/scripts?path="+path, "")
	if code != 200 || !strings.Contains(string(raw), "toUpperCase") {
		t.Errorf("script read = %d %s", code, raw)
	}

	// Save a valid change: recompiled and applied.
	code, raw = h.do("PUT", "/api/scripts?path="+path,
		`function transform(msg) { msg.set('PID-5.1', 'REWRITTEN'); }`)
	if code != 200 {
		t.Fatalf("script write = %d %s", code, raw)
	}
	id := h.feedMessage(sampleHL7)
	_, raw = h.do("GET", fmt.Sprintf("/api/messages/%d", id), "")
	if !strings.Contains(string(raw), "REWRITTEN") {
		t.Error("edited script not applied to new messages")
	}

	// Save a broken change: 400 with the compile error, old program stays.
	code, raw = h.do("PUT", "/api/scripts?path="+path, `function transform( { nope`)
	if code != 400 || !strings.Contains(string(raw), "saved-with-errors") {
		t.Errorf("broken script write = %d %s", code, raw)
	}
	lower := strings.Replace(sampleHL7, "CTRL001", "CTRL003", 1)
	id2 := h.feedMessage(lower)
	_, raw = h.do("GET", fmt.Sprintf("/api/messages/%d", id2), "")
	if !strings.Contains(string(raw), "REWRITTEN") {
		t.Error("previous program should stay active after broken save")
	}

	// Confinement: paths outside the channels dir are rejected.
	outside := filepath.Join(h.work, "outside.js")
	code, _ = h.do("GET", "/api/scripts?path="+outside, "")
	if code != 400 {
		t.Errorf("outside path read = %d", code)
	}
	code, _ = h.do("PUT", "/api/scripts?path="+outside, "x")
	if code != 400 {
		t.Errorf("outside path write = %d", code)
	}
}

func TestSSEStream(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", h.ts.URL+"/api/channels/feed/events", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}

	// Drop the file directly rather than via feedMessage: that helper
	// calls t.Fatal, which must not run on a non-test goroutine.
	name := fmt.Sprintf("m-%d.hl7", time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(h.inDir, name), []byte(sampleHL7), 0o644); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(resp.Body)
	var sawMessageEvent bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"message"`) &&
			strings.Contains(line, `"feed"`) {
			sawMessageEvent = true
			break
		}
	}
	if !sawMessageEvent {
		t.Fatalf("no message event on SSE stream (scan err: %v)", scanner.Err())
	}
}
