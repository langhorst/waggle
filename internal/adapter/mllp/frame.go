// Package mllp implements the HL7 Minimal Lower Layer Protocol Channel
// Adapters: a listener (inbound) supporting immediate and destination ACK
// modes, and a sender (outbound) that interprets the receiving system's ACK.
// Framing is <VT=0x0B> payload <FS=0x1C><CR=0x0D>, minimal LLP only.
package mllp

import (
	"bufio"
	"fmt"
	"io"
)

const (
	startByte = 0x0B // VT
	endByte   = 0x1C // FS
	crByte    = 0x0D // CR
)

// readFrame reads one MLLP frame. The stream must position a VT next
// (stray bytes between frames are a protocol violation and close the
// connection). Returns io.EOF unchanged when the peer closes cleanly between
// frames.
func readFrame(r *bufio.Reader, maxSize int) ([]byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if first != startByte {
		return nil, fmt.Errorf("mllp: expected frame start 0x0B, got 0x%02X", first)
	}
	payload := make([]byte, 0, 1024)
	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if b == endByte {
			break
		}
		payload = append(payload, b)
		if len(payload) > maxSize {
			return nil, fmt.Errorf("mllp: message exceeds maximum size %d", maxSize)
		}
	}
	cr, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if cr != crByte {
		return nil, fmt.Errorf("mllp: expected CR after frame end, got 0x%02X", cr)
	}
	return payload, nil
}

// writeFrame writes one MLLP frame.
func writeFrame(w io.Writer, payload []byte) error {
	_, err := w.Write(frame(payload))
	return err
}

// frame wraps payload in the MLLP start/end bytes.
func frame(payload []byte) []byte {
	buf := make([]byte, 0, len(payload)+3)
	buf = append(buf, startByte)
	buf = append(buf, payload...)
	buf = append(buf, endByte, crByte)
	return buf
}
