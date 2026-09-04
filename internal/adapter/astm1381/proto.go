// Package astm1381 implements the ASTM E1381 / CLSI LIS01-A2 low-level
// protocol as Channel Adapters: a TCP listener (inbound) and sender
// (outbound) moving ASTM E1394 messages in checksummed frames.
//
// Protocol shape: a sender opens a session with ENQ (receiver answers ACK,
// or NAK when busy), transfers the message as frames
//
//	<STX> FN text <ETB|ETX> C1 C2 <CR> <LF>
//
// (FN = frame number '0'–'7' starting at 1, ETB = more frames follow, ETX =
// last frame, C1C2 = mod-256 checksum of FN through ETB/ETX as two uppercase
// hex chars), and terminates with EOT. The receiver ACKs each good frame and
// NAKs a bad one, triggering retransmission. One ENQ…EOT session carries one
// E1394 message; each CR-terminated record travels in its own frame, split
// across ETB frames when longer than the frame size.
//
// Engine ACK mapping: the listener delivers the assembled message to the
// engine BEFORE acknowledging the final (ETX) frame — persist-before-ACK,
// the Guaranteed Delivery handoff; a recording failure NAKs that frame. In
// destination-ACK mode the final frame's response additionally waits for the
// pipeline outcome: success ACKs, anything else answers EOT (the receiver
// interrupt request — E1381 has no application-status channel, and a NAK
// would only trigger a retransmit loop).
package astm1381

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// E1381 control bytes.
const (
	stx = 0x02
	etx = 0x03
	eot = 0x04
	enq = 0x05
	ack = 0x06
	nak = 0x15
	etb = 0x17
	cr  = 0x0D
	lf  = 0x0A
)

// defaultFrameSize is the E1381 maximum frame text length.
const defaultFrameSize = 240

// frame is one decoded transfer frame.
type frame struct {
	Number byte // '0'..'7'
	Text   []byte
	Last   bool // ETX (true) vs ETB (false)
}

// checksum computes the E1381 frame checksum: the sum of the frame number,
// text, and terminator byte, mod 256, rendered as two uppercase hex chars.
func checksum(number byte, text []byte, terminator byte) [2]byte {
	sum := int(number)
	for _, b := range text {
		sum += int(b)
	}
	sum = (sum + int(terminator)) % 256
	const hexDigits = "0123456789ABCDEF"
	return [2]byte{hexDigits[sum>>4], hexDigits[sum&0x0F]}
}

// frameNumber returns the wire frame number for the 1-based frame index:
// 1,2,…,7,0,1,… as ASCII digits.
func frameNumber(index int) byte {
	return byte('0' + index%8)
}

// nextFrameNumber advances a wire frame number mod 8.
func nextFrameNumber(n byte) byte {
	return byte('0' + (int(n-'0')+1)%8)
}

// encodeFrame renders one frame to wire bytes.
func encodeFrame(f frame) []byte {
	terminator := byte(etb)
	if f.Last {
		terminator = etx
	}
	sum := checksum(f.Number, f.Text, terminator)
	out := make([]byte, 0, len(f.Text)+7)
	out = append(out, stx, f.Number)
	out = append(out, f.Text...)
	out = append(out, terminator, sum[0], sum[1], cr, lf)
	return out
}

// readFrame reads and validates one frame from r, whose next byte must be
// STX (the caller has already dispatched on ENQ/EOT/etc.). maxText bounds
// the text length. Checksum or structure violations return an error — the
// caller NAKs and the peer retransmits.
func readFrame(r *bufio.Reader, maxText int) (frame, error) {
	first, err := r.ReadByte()
	if err != nil {
		return frame{}, err
	}
	return readFrameAfter(first, r, maxText)
}

// readFrameAfter validates a frame whose first byte has already been read.
func readFrameAfter(first byte, r *bufio.Reader, maxText int) (frame, error) {
	if first != stx {
		return frame{}, fmt.Errorf("astm1381: expected STX, got 0x%02X", first)
	}
	number, err := r.ReadByte()
	if err != nil {
		return frame{}, unexpectedEOF(err)
	}
	if number < '0' || number > '7' {
		return frame{}, fmt.Errorf("astm1381: invalid frame number 0x%02X", number)
	}
	var text []byte
	var terminator byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return frame{}, unexpectedEOF(err)
		}
		if b == etb || b == etx {
			terminator = b
			break
		}
		text = append(text, b)
		if len(text) > maxText {
			return frame{}, fmt.Errorf("astm1381: frame text exceeds %d bytes", maxText)
		}
	}
	var sumAndEnd [4]byte
	if _, err := io.ReadFull(r, sumAndEnd[:]); err != nil {
		return frame{}, unexpectedEOF(err)
	}
	want := checksum(number, text, terminator)
	if sumAndEnd[0] != want[0] || sumAndEnd[1] != want[1] {
		return frame{}, fmt.Errorf("astm1381: checksum mismatch (got %c%c, want %c%c)",
			sumAndEnd[0], sumAndEnd[1], want[0], want[1])
	}
	if sumAndEnd[2] != cr || sumAndEnd[3] != lf {
		return frame{}, fmt.Errorf("astm1381: frame not terminated by CR LF")
	}
	return frame{Number: number, Text: text, Last: terminator == etx}, nil
}

func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// splitFrames slices a serialized E1394 message into frame texts: one
// CR-terminated record per frame, records longer than frameSize continuing
// across additional frames (the receiver just concatenates, so the split is
// transparent). The final returned frame is the message's ETX frame.
func splitFrames(msg []byte, frameSize int) []frame {
	if frameSize <= 0 {
		frameSize = defaultFrameSize
	}
	var texts [][]byte
	for len(msg) > 0 {
		record := msg
		if i := bytes.IndexByte(msg, cr); i >= 0 {
			record = msg[:i+1]
			msg = msg[i+1:]
		} else {
			msg = nil
		}
		for len(record) > frameSize {
			texts = append(texts, record[:frameSize])
			record = record[frameSize:]
		}
		if len(record) > 0 {
			texts = append(texts, record)
		}
	}
	if len(texts) == 0 {
		texts = [][]byte{{}}
	}
	frames := make([]frame, len(texts))
	for i, text := range texts {
		frames[i] = frame{
			Number: frameNumber(i + 1),
			Text:   text,
			Last:   i == len(texts)-1,
		}
	}
	return frames
}
