package engine_test

import (
	"context"
	"testing"

	"github.com/langhorst/waggle/internal/adapter/astm1381"
	"github.com/langhorst/waggle/internal/adapter/mllp"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/testutil"
)

// TestOrderDownloadEndToEnd runs the shipped orders-to-instrument example:
// an HL7 ORM^O01 with two ORC/OBR pairs arrives over MLLP and reaches the
// instrument as an ASTM order message over E1381, with order control codes
// mapped onto O-11 action codes.
func TestOrderDownloadEndToEnd(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	instrument := testutil.AckingReceiver(t, "astm")

	ch := f.StartExample(t, "orders-to-instrument", map[string]string{
		`":6665"`:           `"127.0.0.1:0"`,
		`"127.0.0.1:6664"`:  `"` + instrument.Addr() + `"`,
		"retryInterval: 1s": "retryInterval: 20ms",
	})

	orm := "MSH|^~\\&|HIS|HOSP|LIS|LAB|20260730120000||ORM^O01|ORD001|P|2.5.1\r" +
		"PID|1||MRN77||ROE^RICHARD||19751224|M\r" +
		"ORC|NW|PLACER001\r" +
		"OBR|1|PLACER001||GLU^Glucose|S||20260730120500\r" +
		"ORC|CA|PLACER002\r" +
		"OBR|2|PLACER002||CBC^Blood Count|R||20260730120600\r"
	sender, err := mllp.NewSender(map[string]any{"addr": testutil.ListenAddr(t, ch), "ackTimeout": "10s"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.Send(ctx, []byte(orm), nil); err != nil {
		t.Fatalf("ORM send: %v", err)
	}

	get := testutil.Getter(t, "astm", instrument.WaitFor(t, 1))
	for _, tc := range []struct{ path, want string }{
		{"H-5", "HIS"},
		{"P-3", "MRN77"},
		{"P-5.1", "ROE"},
		{"O-1", "1"},
		{"O-2", "PLACER001"},
		{"O-4.4", "GLU"},
		{"O-5", "S"},
		{"O-6", "20260730120500"},
		{"O-11", "N"}, // ORC-1 NW: new order
		{"O[2]-2", "PLACER002"},
		{"O[2]-4.4", "CBC"},
		{"O[2]-5", "R"},
		{"O[2]-11", "C"}, // ORC-1 CA: cancel
		{"L-1", "1"},
	} {
		if got := get(tc.path); got != tc.want {
			t.Errorf("order %s = %q, want %q", tc.path, got, tc.want)
		}
	}

	counts, err := f.Store.MessageCounts(ctx, "orders-to-instrument")
	if err != nil || counts[message.StateSent] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
}

// TestQueryResponseRoundTrip runs the shipped instrument-query example: the
// instrument queries for a specimen over E1381 and receives an order
// response in a second, opposite session. Non-query ASTM traffic is
// FILTERED and never reaches the responder.
func TestQueryResponseRoundTrip(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	instrument := testutil.AckingReceiver(t, "astm")

	ch := f.StartExample(t, "instrument-query", map[string]string{
		`":6666"`:           `"127.0.0.1:0"`,
		`"127.0.0.1:6667"`:  `"` + instrument.Addr() + `"`,
		"retryInterval: 1s": "retryInterval: 20ms",
	})
	sender, err := astm1381.NewSender(map[string]any{"addr": testutil.ListenAddr(t, ch)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	query := "H|\\^&|||INSTR|||||||P|LIS2-A2|20260730121500\r" +
		"Q|1|^SID999|^^^GLU|O\r" +
		"L|1|N\r"
	if err := sender.Send(ctx, []byte(query), nil); err != nil {
		t.Fatalf("query send: %v", err)
	}

	get := testutil.Getter(t, "astm", instrument.WaitFor(t, 1))
	for _, tc := range []struct{ path, want string }{
		{"O-2", "SID999"}, // the queried specimen
		{"O-4.4", "GLU"},  // the requested test
		{"O-11", "Q"},     // response to query
		{"L-2", "F"},      // final: no more data
		{"H-14", "20260730121500"},
	} {
		if got := get(tc.path); got != tc.want {
			t.Errorf("response %s = %q, want %q", tc.path, got, tc.want)
		}
	}

	// A result message (no Q record) on the same channel is FILTERED: the
	// transport accepts it, but the responder never fires. Its pipeline is
	// over once the FILTERED event is published, so the count is final.
	result := "H|\\^&|||INSTR\r" +
		"P|1||PATID123\r" +
		"R|1|^^^GLU|105|mg/dL\r" +
		"L|1|N\r"
	if err := sender.Send(ctx, []byte(result), nil); err != nil {
		t.Fatalf("result send: %v", err)
	}
	f.WaitMessageState(t, "instrument-query", "", message.StateFiltered)
	if n := instrument.Count(); n != 1 {
		t.Errorf("instrument received %d messages; filtered traffic must not produce responses", n)
	}
	counts, err := f.Store.MessageCounts(ctx, "instrument-query")
	if err != nil || counts[message.StateFiltered] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
}
