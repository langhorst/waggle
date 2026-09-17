package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/langhorst/waggle/internal/format"
	_ "github.com/langhorst/waggle/internal/format/hl7v2"
	"github.com/langhorst/waggle/internal/message"
)

// captureLogs runs fn with a JSON logger and returns one decoded record per
// line, so assertions are about fields rather than substrings.
func captureLogs(t *testing.T, level slog.Level, fn func(log *slog.Logger)) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	fn(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})))

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, rec)
	}
	return out
}

const logTestHL7 = "MSH|^~\\&|SIM|SIMNET|WAGGLE|TEST|20260302143500||ADT^A01^ADT_A01|SIMT0001|T|2.5.1\r" +
	"PID|1||MERC0000042^^^MERCY^MR||THORNQUIST^IMOGEN||19580714|F\r"

// newLoggingChannel builds a channel with one no-op destination.
func newLoggingChannel(t *testing.T, log *slog.Logger, fields []LogField) *Channel {
	t.Helper()
	hl7, ok := format.Get("hl7v2")
	if !ok {
		t.Fatal("hl7v2 not registered")
	}
	return &Channel{
		ID:        "logtest",
		InType:    hl7,
		Recorder:  NewMemoryRecorder(),
		Log:       log,
		LogFields: fields,
		Destinations: []*Destination{{
			ID: "sink", OutType: hl7, Adapter: &nopOutbound{}, WaitForAck: true,
		}},
	}
}

type nopOutbound struct{}

func (nopOutbound) Open(context.Context) error                            { return nil }
func (nopOutbound) Send(context.Context, []byte, map[string]string) error { return nil }
func (nopOutbound) Close() error                                          { return nil }

// A healthy message must leave a trail: silence is indistinguishable from a
// hung channel when you are watching a terminal.
func TestPipelineLogsEveryMessage(t *testing.T) {
	recs := captureLogs(t, slog.LevelInfo, func(log *slog.Logger) {
		c := newLoggingChannel(t, log, nil)
		m := &message.Message{ChannelID: "logtest", Raw: []byte(logTestHL7), DataType: "hl7v2"}
		if err := c.Recorder.Record(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		if d := c.process(context.Background(), m); d.Code != "AA" {
			t.Fatalf("ack = %s %s", d.Code, d.Text)
		}
	})

	want := map[string]bool{"message received": false, "destination sent": false, "message processed": false}
	for _, r := range recs {
		msg, _ := r["msg"].(string)
		if _, ok := want[msg]; ok {
			want[msg] = true
		}
	}
	for msg, seen := range want {
		if !seen {
			t.Errorf("no %q line; got %v", msg, messages(recs))
		}
	}
}

// The labels are what let this log be lined up against another system's.
func TestLogFieldsLabelEveryLineAboutAMessage(t *testing.T) {
	recs := captureLogs(t, slog.LevelInfo, func(log *slog.Logger) {
		c := newLoggingChannel(t, log, []LogField{
			{Label: "ctrl", Path: "MSH-10"},
			{Label: "mrn", Path: "PID-3[1].1"},
			{Label: "missing", Path: "ZZZ-9"},
		})
		m := &message.Message{ChannelID: "logtest", Raw: []byte(logTestHL7), DataType: "hl7v2"}
		if err := c.Recorder.Record(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		c.process(context.Background(), m)
	})

	labelled := 0
	for _, r := range recs {
		msg, _ := r["msg"].(string)
		if msg == "message received" {
			// Labels need the parsed tree, so this one line precedes them.
			continue
		}
		if r["ctrl"] != "SIMT0001" {
			t.Errorf("%q line has ctrl=%v, want SIMT0001", msg, r["ctrl"])
		}
		if r["mrn"] != "MERC0000042" {
			t.Errorf("%q line has mrn=%v, want MERC0000042", msg, r["mrn"])
		}
		if _, present := r["missing"]; present {
			t.Errorf("%q line carries a label for a path that matched nothing", msg)
		}
		labelled++
	}
	if labelled == 0 {
		t.Fatal("no labelled lines at all")
	}
}

// A filtered message must say so: otherwise it looks identical to one that
// vanished.
func TestFilteredMessagesAreLogged(t *testing.T) {
	recs := captureLogs(t, slog.LevelInfo, func(log *slog.Logger) {
		c := newLoggingChannel(t, log, []LogField{{Label: "mrn", Path: "PID-3[1].1"}})
		c.Filter = func(*message.Message) (bool, error) { return false, nil }
		m := &message.Message{ChannelID: "logtest", Raw: []byte(logTestHL7), DataType: "hl7v2"}
		if err := c.Recorder.Record(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		c.process(context.Background(), m)
	})
	for _, r := range recs {
		if r["msg"] == "message filtered" {
			if r["mrn"] != "MERC0000042" {
				t.Errorf("filtered line is unlabelled: %v", r)
			}
			return
		}
	}
	t.Errorf("no filtered line; got %v", messages(recs))
}

// warn hides the per-message traffic but must keep the failures.
func TestWarnLevelKeepsFailuresOnly(t *testing.T) {
	recs := captureLogs(t, slog.LevelWarn, func(log *slog.Logger) {
		c := newLoggingChannel(t, log, nil)
		m := &message.Message{ChannelID: "logtest", Raw: []byte(logTestHL7), DataType: "hl7v2"}
		if err := c.Recorder.Record(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		c.process(context.Background(), m)
	})
	if len(recs) != 0 {
		t.Errorf("warn level should be silent for a healthy message, got %v", messages(recs))
	}
}

func messages(recs []map[string]any) []string {
	var out []string
	for _, r := range recs {
		s, _ := r["msg"].(string)
		out = append(out, s)
	}
	return out
}
