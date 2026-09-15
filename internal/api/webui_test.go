package api

import (
	"context"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
	"time"

	"fmt"
	"strings"
	"testing"
)

func TestWebDashboard(t *testing.T) {
	h := newHarness(t)
	code, raw := h.do("GET", "/", "")
	if code != 200 {
		t.Fatalf("dashboard = %d", code)
	}
	page := string(raw)
	for _, want := range []string{"Waggle", "feed", "STARTED", "/static/htmx.min.js", "/static/flowbite.min.css", "EventSource"} {
		if !strings.Contains(page, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
}

func TestWebStaticAssets(t *testing.T) {
	h := newHarness(t)
	for _, asset := range []string{"htmx.min.js", "flowbite.min.css", "flowbite.min.js", "tailwind.js"} {
		code, raw := h.do("GET", "/static/"+asset, "")
		if code != 200 || len(raw) < 1000 {
			t.Errorf("asset %s = %d (%d bytes)", asset, code, len(raw))
		}
	}
}

func TestWebChannelAndMessagePages(t *testing.T) {
	h := newHarness(t)
	id := h.feedMessage(sampleHL7)

	code, raw := h.do("GET", "/channels/feed", "")
	if code != 200 || !strings.Contains(string(raw), "/ui/channels/feed/messages") {
		t.Errorf("channel page = %d", code)
	}
	code, raw = h.do("GET", "/ui/channels/feed/messages", "")
	if code != 200 || !strings.Contains(string(raw), fmt.Sprintf("/messages/%d", id)) {
		t.Errorf("messages partial = %d %s", code, raw)
	}

	code, raw = h.do("GET", fmt.Sprintf("/messages/%d", id), "")
	if code != 200 {
		t.Fatalf("message page = %d", code)
	}
	page := string(raw)
	for _, want := range []string{"DOE^JOHN", "Replay", "tab-tree", "tab-diff", "tab-dest"} {
		if !strings.Contains(page, want) {
			t.Errorf("message page missing %q", want)
		}
	}

	code, raw = h.do("GET", fmt.Sprintf("/ui/messages/%d/tree?stage=received", id), "")
	if code != 200 || !strings.Contains(string(raw), "PID") {
		t.Errorf("tree partial = %d", code)
	}
	code, raw = h.do("GET", fmt.Sprintf("/ui/messages/%d/destinations", id), "")
	if code != 200 || !strings.Contains(string(raw), "SENT") {
		t.Errorf("destinations partial = %d %s", code, raw)
	}

	code, _ = h.do("GET", "/channels/nope", "")
	if code != 404 {
		t.Errorf("unknown channel page = %d", code)
	}
}

func TestWebChannelActions(t *testing.T) {
	h := newHarness(t)
	code, raw := h.do("POST", "/ui/channels/feed/pause", "")
	if code != 200 || !strings.Contains(string(raw), "PAUSED") {
		t.Errorf("pause action = %d", code)
	}
	code, raw = h.do("POST", "/ui/channels/feed/start", "")
	if code != 200 || !strings.Contains(string(raw), "STARTED") {
		t.Errorf("start action = %d", code)
	}
}

func TestWebReplayAction(t *testing.T) {
	h := newHarness(t)
	id := h.feedMessage(sampleHL7)
	code, raw := h.do("POST", fmt.Sprintf("/ui/messages/%d/replay", id), "")
	if code != 200 || !strings.Contains(string(raw), "replayed as") {
		t.Errorf("replay action = %d %s", code, raw)
	}
}

func TestWebScriptsAndEditor(t *testing.T) {
	h := newHarness(t)
	code, raw := h.do("GET", "/channels/feed/scripts", "")
	if code != 200 || !strings.Contains(string(raw), "upper.js") {
		t.Fatalf("scripts page = %d", code)
	}
	// Pull the editor for the transformer.
	var refs []engine.ScriptRef
	_, apiRaw := h.do("GET", "/api/channels/feed/scripts", "")
	h.decode(apiRaw, &refs)
	code, raw = h.do("GET", "/scripts/edit?path="+refs[0].Path, "")
	if code != 200 || !strings.Contains(string(raw), "toUpperCase") || !strings.Contains(string(raw), "save-btn") {
		t.Errorf("editor page = %d", code)
	}
}

func TestWebDLQPage(t *testing.T) {
	h := newHarness(t)
	code, raw := h.do("GET", "/channels/feed/dlq", "")
	if code != 200 || !strings.Contains(string(raw), "/ui/channels/feed/dlq") {
		t.Errorf("dlq page = %d", code)
	}
	code, raw = h.do("GET", "/ui/channels/feed/dlq", "")
	if code != 200 || !strings.Contains(string(raw), "empty") {
		t.Errorf("dlq partial = %d %s", code, raw)
	}
}

// TestWebActionErrorIsShown: a failed channel action must be visible in the
// refreshed table, not only in the log.
func TestWebActionErrorIsShown(t *testing.T) {
	h := newHarness(t)
	// Pausing a channel that is not started fails.
	if code, _ := h.do("POST", "/ui/channels/feed/stop", ""); code != 200 {
		t.Fatalf("stop = %d", code)
	}
	code, raw := h.do("POST", "/ui/channels/feed/pause", "")
	if code != 200 || !strings.Contains(string(raw), "pause feed failed") {
		t.Fatalf("pause of a stopped channel = %d %s", code, raw)
	}
	if code, _ := h.do("POST", "/ui/channels/feed/explode", ""); code != 400 {
		t.Errorf("unknown action = %d", code)
	}
	if code, _ := h.do("GET", "/channels/nope/dlq", ""); code != 404 {
		t.Errorf("DLQ page for unknown channel = %d", code)
	}
}

// TestWebMessagesPagination: a full page offers an "older" link keyed on
// the oldest id shown, and following it yields the next page.
func TestWebMessagesPagination(t *testing.T) {
	h := newHarness(t)
	// Fill the store directly: one full page plus one.
	ctx := context.Background()
	for i := 0; i < webPageSize+1; i++ {
		m := &message.Message{ChannelID: "feed", CorrelationID: fmt.Sprint(i), Raw: []byte("x"), DataType: "hl7v2", State: message.StateReceived, ReceivedAt: time.Now()}
		if err := h.st.Record(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	code, raw := h.do("GET", "/ui/channels/feed/messages", "")
	if code != 200 || !strings.Contains(string(raw), "older messages") {
		t.Fatalf("first page = %d, missing older link:\n%s", code, raw)
	}
	list, _ := h.st.ListMessages(ctx, "feed", store.ListQuery{Limit: webPageSize})
	oldest := list[len(list)-1].ID
	if !strings.Contains(string(raw), fmt.Sprintf("before_id=%d", oldest)) {
		t.Errorf("older link does not carry before_id=%d", oldest)
	}
	code, raw = h.do("GET", fmt.Sprintf("/ui/channels/feed/messages?before_id=%d", oldest), "")
	if code != 200 || strings.Contains(string(raw), "older messages") {
		t.Fatalf("last page = %d, should have no older link:\n%s", code, raw)
	}
	if !strings.Contains(string(raw), fmt.Sprintf("/messages/%d\"", list[0].ID-webPageSize)) && !strings.Contains(string(raw), "#") {
		t.Errorf("last page shows no message")
	}
}
