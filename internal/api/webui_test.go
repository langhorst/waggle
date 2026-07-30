package api

import (
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
	for _, want := range []string{"integration-channel", "feed", "STARTED", "/static/htmx.min.js", "/static/flowbite.min.css", "EventSource"} {
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
	var refs []scriptRef
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
