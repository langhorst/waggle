package hl7v2

import (
	"encoding/hex"
	"strings"

	"github.com/langhorst/waggle/internal/format/delimited"
)

// decoder translates HL7 escape sequences to literal bytes using the
// message's own delimiters: \F\ field, \S\ component, \T\ subcomponent,
// \R\ repetition, \E\ escape, \Xhh..\ hex bytes.
func decoder(dl delimited.Delims) func(string) string {
	return delimited.Decoder(dl.Esc, func(seq string) (string, bool) {
		switch seq {
		case "F":
			return delimited.ByteString(dl.Field), true
		case "S":
			return delimited.ByteString(dl.Comp), true
		case "T":
			return delimited.ByteString(dl.Sub), true
		case "R":
			return delimited.ByteString(dl.Rep), true
		case "E":
			return delimited.ByteString(dl.Esc), true
		}
		if isHexSeq(seq) {
			raw, _ := hex.DecodeString(seq[1:])
			return string(raw), true
		}
		return "", false
	})
}

// decodableSeq reports whether the decoder expands seq — the complement set
// (highlighting \H\/\N\, formatting \.br\, custom \Z..\, ...) is preserved
// verbatim by decode and must be re-emitted verbatim by encode.
func decodableSeq(seq string) bool {
	switch seq {
	case "F", "S", "T", "R", "E":
		return true
	}
	return isHexSeq(seq)
}

func isHexSeq(seq string) bool {
	if len(seq) < 3 || seq[0] != 'X' || len(seq)%2 == 0 {
		return false
	}
	_, err := hex.DecodeString(seq[1:])
	return err == nil
}

// encoder produces wire-safe field content: delimiter characters and the
// escape character become escape sequences, control characters become \Xhh\.
// Serialize(Parse(x)) is byte-identical for canonically escaped input.
func encoder(dl delimited.Delims) func(string) string {
	byteEncode := func(c byte) (string, bool) {
		switch c {
		case dl.Esc:
			return "E", true
		case dl.Field:
			return "F", true
		case dl.Comp:
			return "S", true
		case dl.Sub:
			return "T", true
		case dl.Rep:
			return "R", true
		}
		if c < 0x20 {
			return "X" + strings.ToUpper(hex.EncodeToString([]byte{c})), true
		}
		return "", false
	}
	preserved := func(seq string) bool { return !decodableSeq(seq) }
	return delimited.Encoder(dl.Esc, byteEncode, preserved)
}
