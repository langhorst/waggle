package astm1381

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestChecksum(t *testing.T) {
	// Worked example: frame "1" + "H|\^&" + ETX.
	// '1'(0x31) + 'H'(0x48) + '|'(0x7C) + '\'(0x5C) + '^'(0x5E) + '&'(0x26)
	// + ETX(0x03) = 0x1D8 → 0xD8 → "D8".
	sum := checksum('1', []byte(`H|\^&`), etx)
	if sum != [2]byte{'D', '8'} {
		t.Errorf("checksum = %c%c, want D8", sum[0], sum[1])
	}
	// Empty text, ETB terminator: '1'(0x31)+ETB(0x17) = 0x48 → "48".
	sum = checksum('1', nil, etb)
	if sum != [2]byte{'4', '8'} {
		t.Errorf("checksum = %c%c, want 48", sum[0], sum[1])
	}
}

func TestFrameNumbers(t *testing.T) {
	// 1-based indexes map to 1..7,0,1...
	want := []byte{'1', '2', '3', '4', '5', '6', '7', '0', '1'}
	for i, w := range want {
		if got := frameNumber(i + 1); got != w {
			t.Errorf("frameNumber(%d) = %c, want %c", i+1, got, w)
		}
	}
	if nextFrameNumber('7') != '0' || nextFrameNumber('0') != '1' {
		t.Error("nextFrameNumber wraparound broken")
	}
}

func TestFrameRoundTrip(t *testing.T) {
	for _, tt := range []frame{
		{Number: '1', Text: []byte("H|\\^&\r"), Last: true},
		{Number: '2', Text: []byte("P|1||PATID\r"), Last: false},
		{Number: '0', Text: nil, Last: true},
	} {
		wire := encodeFrame(tt)
		got, err := readFrame(bufio.NewReader(bytes.NewReader(wire)), defaultFrameSize)
		if err != nil {
			t.Fatalf("readFrame(%q): %v", wire, err)
		}
		if got.Number != tt.Number || !bytes.Equal(got.Text, tt.Text) || got.Last != tt.Last {
			t.Errorf("round trip: got %+v, want %+v", got, tt)
		}
	}
}

func TestReadFrameErrors(t *testing.T) {
	good := encodeFrame(frame{Number: '1', Text: []byte("R|1\r"), Last: true})

	corrupt := func(mutate func([]byte)) []byte {
		c := append([]byte(nil), good...)
		mutate(c)
		return c
	}
	cases := map[string][]byte{
		"not stx":      corrupt(func(b []byte) { b[0] = 'X' }),
		"bad number":   corrupt(func(b []byte) { b[1] = '9' }),
		"bad checksum": corrupt(func(b []byte) { b[len(b)-4] = '0'; b[len(b)-3] = '0' }),
		"flipped text": corrupt(func(b []byte) { b[2] = 'X' }), // checksum no longer matches
		"missing crlf": corrupt(func(b []byte) { b[len(b)-2] = 'x' }),
		"truncated":    good[:len(good)-3],
		"empty":        {},
	}
	for name, wire := range cases {
		if _, err := readFrame(bufio.NewReader(bytes.NewReader(wire)), defaultFrameSize); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}

	// Oversized text is rejected.
	big := encodeFrame(frame{Number: '1', Text: []byte(strings.Repeat("A", 50)), Last: true})
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(big)), 10); err == nil {
		t.Error("oversized frame: expected error")
	}
}

func TestSplitFrames(t *testing.T) {
	// Three records, all short: one frame each, last is ETX.
	msg := []byte("H|\\^&\rP|1\rL|1\r")
	frames := splitFrames(msg, 240)
	if len(frames) != 3 {
		t.Fatalf("frames = %d", len(frames))
	}
	if string(frames[0].Text) != "H|\\^&\r" || frames[0].Number != '1' || frames[0].Last {
		t.Errorf("frame 1 = %+v", frames[0])
	}
	if string(frames[2].Text) != "L|1\r" || !frames[2].Last {
		t.Errorf("frame 3 = %+v", frames[2])
	}

	// Reassembly is the identity.
	var assembled []byte
	for _, f := range frames {
		assembled = append(assembled, f.Text...)
	}
	if !bytes.Equal(assembled, msg) {
		t.Errorf("reassembled %q != %q", assembled, msg)
	}
}

func TestSplitFramesLongRecord(t *testing.T) {
	// A 500-char record at frame size 240 → 240 + 240 + 21 (incl CR).
	long := strings.Repeat("R", 500) + "\r"
	msg := []byte("H|\\^&\r" + long)
	frames := splitFrames(msg, 240)
	if len(frames) != 4 {
		t.Fatalf("frames = %d", len(frames))
	}
	if len(frames[1].Text) != 240 || len(frames[2].Text) != 240 || len(frames[3].Text) != 21 {
		t.Errorf("split sizes = %d/%d/%d", len(frames[1].Text), len(frames[2].Text), len(frames[3].Text))
	}
	if frames[1].Last || frames[2].Last || !frames[3].Last {
		t.Error("only the final frame may be ETX")
	}
	var assembled []byte
	for _, f := range frames {
		assembled = append(assembled, f.Text...)
	}
	if !bytes.Equal(assembled, msg) {
		t.Error("reassembly mismatch")
	}
}

func TestSplitFramesEdges(t *testing.T) {
	// Exactly frame-size record: single frame, no empty continuation.
	exact := append(bytes.Repeat([]byte{'A'}, 239), cr)
	frames := splitFrames(exact, 240)
	if len(frames) != 1 || !frames[0].Last {
		t.Errorf("exact-size: %d frames", len(frames))
	}
	// No trailing CR still transmits.
	frames = splitFrames([]byte("L|1"), 240)
	if len(frames) != 1 || string(frames[0].Text) != "L|1" {
		t.Errorf("no-CR: %+v", frames)
	}
	// Empty message: one empty ETX frame.
	frames = splitFrames(nil, 240)
	if len(frames) != 1 || !frames[0].Last || len(frames[0].Text) != 0 {
		t.Errorf("empty: %+v", frames)
	}
	// Frame numbering wraps mod 8 across many records.
	var many []byte
	for i := 0; i < 10; i++ {
		many = append(many, []byte("C|x\r")...)
	}
	frames = splitFrames(many, 240)
	if frames[7].Number != '0' || frames[8].Number != '1' {
		t.Errorf("wraparound numbers = %c %c", frames[7].Number, frames[8].Number)
	}
}
