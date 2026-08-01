package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

// fakeBackend serves canned data.
type fakeBackend struct {
	bus      *events.Bus
	channels []engine.Info
	messages map[string][]store.MessageSummary
	details  map[int64]*store.MessageDetail
	trees    map[string]*message.Node
	diffs    map[int64][]message.DiffEntry
}

func (f *fakeBackend) Channels() []engine.Info { return f.channels }
func (f *fakeBackend) ListMessages(ctx context.Context, channelID string, q store.ListQuery) ([]store.MessageSummary, error) {
	list := f.messages[channelID]
	if q.State == "" {
		return list, nil
	}
	var out []store.MessageSummary
	for _, m := range list {
		if m.State == q.State {
			out = append(out, m)
		}
	}
	return out, nil
}
func (f *fakeBackend) GetMessage(ctx context.Context, id int64) (*store.MessageDetail, error) {
	if d, ok := f.details[id]; ok {
		return d, nil
	}
	return nil, store.ErrNotFound
}
func (f *fakeBackend) MessageCounts(ctx context.Context, channelID string) (map[message.State]int, error) {
	counts := map[message.State]int{}
	for _, m := range f.messages[channelID] {
		counts[m.State]++
	}
	return counts, nil
}
func (f *fakeBackend) QueueDepth(ctx context.Context, channelID string) (map[string]int, error) {
	return map[string]int{"d1": 2}, nil
}
func (f *fakeBackend) MessageTree(ctx context.Context, id int64, stage string) (*message.Node, string, error) {
	if t, ok := f.trees[stage]; ok {
		return t, "hl7v2", nil
	}
	return nil, "", store.ErrNotFound
}
func (f *fakeBackend) MessageDiff(ctx context.Context, id int64, destID string) ([]message.DiffEntry, error) {
	return f.diffs[id], nil
}
func (f *fakeBackend) Subscribe(buf int) (<-chan events.Event, func()) {
	return f.bus.Subscribe(buf)
}

func newFake() *fakeBackend {
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	sentAt := now.Add(time.Second)
	return &fakeBackend{
		bus: events.NewBus(),
		channels: []engine.Info{
			{ID: "adt-feed", Name: "ADT Feed", Status: channel.StatusStarted},
			{ID: "lab-feed", Name: "Lab Feed", Status: channel.StatusStopped},
		},
		messages: map[string][]store.MessageSummary{
			"adt-feed": {
				{ID: 2, ChannelID: "adt-feed", State: message.StateError, ErrorText: "parse failed", ReceivedAt: now},
				{ID: 1, ChannelID: "adt-feed", State: message.StateTransformed, ReceivedAt: now},
			},
		},
		details: map[int64]*store.MessageDetail{
			1: {
				MessageSummary: store.MessageSummary{
					ID: 1, ChannelID: "adt-feed", CorrelationID: "corr-1",
					State: message.StateTransformed, DataType: "hl7v2", ReceivedAt: now,
				},
				Raw:                 []byte("MSH|^~\\&|A\rPID|1||MRN1\r"),
				Transformed:         []byte("MSH|^~\\&|A\rPID|1||MRN2\r"),
				TransformedDataType: "hl7v2",
				Destinations: []store.DestinationStatus{
					{DestinationID: "d1", State: message.StateSent, Attempts: 1, SentAt: &sentAt},
				},
			},
		},
		trees: map[string]*message.Node{
			"received": {Name: "hl7v2", Children: []*message.Node{
				{Name: "PID", Children: []*message.Node{{Name: "1", Value: "1"}}},
			}},
		},
		diffs: map[int64][]message.DiffEntry{
			1: {{Path: "PID-3", Op: message.DiffChanged, From: "MRN1", To: "MRN2"}},
		},
	}
}

func key(s string) tea.KeyMsg {
	switch s {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	case "tab":
		return tea.KeyMsg{Type: tea.KeyTab}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

func press(t *testing.T, m Model, keys ...string) Model {
	t.Helper()
	var mod tea.Model = m
	for _, k := range keys {
		mod, _ = mod.Update(key(k))
	}
	return mod.(Model)
}

func TestChannelListView(t *testing.T) {
	m := New(newFake())
	v := m.View()
	for _, want := range []string{"adt-feed", "ADT Feed", "STARTED", "lab-feed", "STOPPED"} {
		if !strings.Contains(v, want) {
			t.Errorf("channel view missing %q:\n%s", want, v)
		}
	}
}

func TestNavigateToMessages(t *testing.T) {
	m := press(t, New(newFake()), "enter") // open first channel
	v := m.View()
	for _, want := range []string{"channel adt-feed", "TRANSFORMED", "ERROR", "parse failed"} {
		if !strings.Contains(v, want) {
			t.Errorf("message view missing %q:\n%s", want, v)
		}
	}
	// Errors-only filter.
	m = press(t, m, "d")
	v = m.View()
	if !strings.Contains(v, "errors only") || strings.Contains(v, "TRANSFORMED") {
		t.Errorf("errors-only filter not applied:\n%s", v)
	}
	// Back to channels.
	m = press(t, m, "esc")
	if m.view != viewChannels {
		t.Error("esc should return to channel list")
	}
}

func TestMessageDetailTabs(t *testing.T) {
	// enter channel, move cursor to message #1 (second row), open it.
	m := press(t, New(newFake()), "enter", "down", "enter")
	if m.view != viewDetail {
		t.Fatalf("view = %d", m.view)
	}

	v := m.View() // Raw tab
	for _, want := range []string{"message #1", "corr-1", "raw inbound", "MRN1", "transformed", "MRN2"} {
		if !strings.Contains(v, want) {
			t.Errorf("raw tab missing %q:\n%s", want, v)
		}
	}

	m = press(t, m, "tab") // Tree
	v = m.View()
	for _, want := range []string{"stage: received", "PID"} {
		if !strings.Contains(v, want) {
			t.Errorf("tree tab missing %q:\n%s", want, v)
		}
	}

	m = press(t, m, "tab") // Diff
	v = m.View()
	for _, want := range []string{"received → transformed", "PID-3", "MRN1", "MRN2"} {
		if !strings.Contains(v, want) {
			t.Errorf("diff tab missing %q:\n%s", want, v)
		}
	}

	m = press(t, m, "tab") // Destinations
	v = m.View()
	for _, want := range []string{"DESTINATION", "d1", "SENT"} {
		if !strings.Contains(v, want) {
			t.Errorf("destinations tab missing %q:\n%s", want, v)
		}
	}

	m = press(t, m, "esc")
	if m.view != viewMessages {
		t.Error("esc should return to message list")
	}
}

func TestEventRefreshesMessageList(t *testing.T) {
	f := newFake()
	m := press(t, New(f), "enter")
	if len(m.messages) != 2 {
		t.Fatalf("messages = %d", len(m.messages))
	}
	// A new message arrives on the watched channel.
	now := time.Now()
	f.messages["adt-feed"] = append([]store.MessageSummary{
		{ID: 3, ChannelID: "adt-feed", State: message.StateReceived, ReceivedAt: now},
	}, f.messages["adt-feed"]...)

	mod, _ := m.Update(eventMsg{events.Event{
		Type: events.TypeMessage, ChannelID: "adt-feed", MessageID: 3, State: message.StateReceived,
	}})
	m = mod.(Model)
	if len(m.messages) != 3 {
		t.Errorf("event should refresh the list, have %d messages", len(m.messages))
	}
}

func TestQuitCancelsSubscription(t *testing.T) {
	f := newFake()
	m := New(f)
	_, cmd := m.Update(key("q"))
	if cmd == nil {
		t.Fatal("q should quit")
	}
	if msg := cmd(); msg != tea.Quit() {
		t.Errorf("expected quit message, got %v", msg)
	}
}
