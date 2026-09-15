package engine_test

import (
	"context"
	"testing"

	"github.com/langhorst/waggle/internal/adapter/astm1381"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/testutil"
)

// TestASTMBridgeRoundTrip runs the shipped astm-to-mllp and mllp-to-astm
// example channels chained end to end: the test plays an instrument
// sending E1394 over E1381 into channel A (converted to HL7 ORU^R01, sent
// over MLLP), whose output feeds channel B (HL7 back to E1394, sent over
// E1381) into the test's own E1381 receiver. The fields that come out the
// far end must match what went in.
func TestASTMBridgeRoundTrip(t *testing.T) {
	f := testutil.NewFixture(t)
	ctx := context.Background()
	instrument := testutil.AckingReceiver(t, "astm")

	// Channel B first (its MLLP listen address feeds channel A's config):
	// HL7 over MLLP in, E1394 out over E1381 to the test instrument.
	chB := f.StartExample(t, "mllp-to-astm", map[string]string{
		`":6663"`:           `"127.0.0.1:0"`,
		`"127.0.0.1:6664"`:  `"` + instrument.Addr() + `"`,
		"retryInterval: 1s": "retryInterval: 20ms",
	})
	// Channel A: E1394 over E1381 in, HL7 ORU^R01 out over MLLP to B.
	chA := f.StartExample(t, "astm-to-mllp", map[string]string{
		`":6662"`:           `"127.0.0.1:0"`,
		`"127.0.0.1:6663"`:  `"` + testutil.ListenAddr(t, chB) + `"`,
		"retryInterval: 1s": "retryInterval: 20ms",
	})

	// The test plays the sending instrument.
	original := "H|\\^&|||LIS|||||||P|LIS2-A2|20260730120000\r" +
		"P|1||PATID123||DOE^JOHN||19800101|M\r" +
		"O|1|SPEC001||^^^GLU|R|20260730113000\r" +
		"R|1|^^^GLU|105|mg/dL||N||F\r" +
		"R|2|^^^HBA1C|5.4|%||N||F\r" +
		"L|1|N\r"
	sender, err := astm1381.NewSender(map[string]any{"addr": testutil.ListenAddr(t, chA)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.Send(ctx, []byte(original), nil); err != nil {
		t.Fatalf("instrument send: %v", err)
	}

	// The round-tripped E1394 carries the original clinical content.
	get := testutil.Getter(t, "astm", instrument.WaitFor(t, 1))
	for _, tc := range []struct{ path, want string }{
		{"H-5", "LIS"},
		{"P-3", "PATID123"},
		{"P-5.1", "DOE"},
		{"P-5.2", "JOHN"},
		{"P-7", "19800101"},
		{"O-2", "SPEC001"},
		{"O-4.4", "GLU"},
		{"R-2.4", "GLU"},
		{"R-3", "105"},
		{"R-4", "mg/dL"},
		{"R[2]-2.4", "HBA1C"},
		{"R[2]-3", "5.4"},
		{"L-1", "1"},
	} {
		if got := get(tc.path); got != tc.want {
			t.Errorf("round-tripped %s = %q, want %q", tc.path, got, tc.want)
		}
	}
	if n := instrument.Count(); n != 1 {
		t.Errorf("instrument received %d messages, want 1", n)
	}

	// Both channels recorded a successful delivery.
	for _, channelID := range []string{"astm-to-mllp", "mllp-to-astm"} {
		counts, err := f.Store.MessageCounts(ctx, channelID)
		if err != nil || counts[message.StateSent] != 1 {
			t.Errorf("channel %s counts = %v, %v", channelID, counts, err)
		}
	}
}
