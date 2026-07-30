package delimited

import "strings"

// Decoder returns a leaf-value decoder for escape sequences of the form
// <esc>body<esc>. seqDecode maps a sequence body to its literal expansion;
// sequences it rejects — and unterminated sequences — are preserved verbatim,
// so parsing never loses data it does not understand.
func Decoder(esc byte, seqDecode func(string) (string, bool)) func(string) string {
	return func(s string) string {
		if strings.IndexByte(s, esc) < 0 {
			return s
		}
		var b strings.Builder
		b.Grow(len(s))
		for i := 0; i < len(s); {
			if s[i] != esc {
				b.WriteByte(s[i])
				i++
				continue
			}
			end := strings.IndexByte(s[i+1:], esc)
			if end < 0 {
				b.WriteString(s[i:])
				break
			}
			if lit, ok := seqDecode(s[i+1 : i+1+end]); ok {
				b.WriteString(lit)
			} else {
				b.WriteString(s[i : i+2+end])
			}
			i += end + 2
		}
		return b.String()
	}
}

// Encoder returns a leaf-value encoder. byteEncode maps a byte that cannot
// appear literally on the wire to its escape-sequence body; bytes it rejects
// pass through unchanged.
//
// preservedSeq identifies escape-sequence bodies the decoder kept verbatim
// (unknown sequences like \H\ or \.br\): when the value contains
// <esc>body<esc> with a preserved body, the whole sequence is emitted
// untouched instead of having its escape characters re-escaped — keeping
// decode/encode a faithful round trip. A preserved body is only honored when
// none of its bytes need encoding themselves; otherwise emitting it verbatim
// would smuggle separators past the parser.
func Encoder(esc byte, byteEncode func(byte) (string, bool), preservedSeq func(string) bool) func(string) string {
	return func(s string) string {
		needsEncoding := false
		for i := 0; i < len(s); i++ {
			if _, ok := byteEncode(s[i]); ok {
				needsEncoding = true
				break
			}
		}
		if !needsEncoding {
			return s
		}
		var b strings.Builder
		b.Grow(len(s) + 8)
		for i := 0; i < len(s); {
			c := s[i]
			if c == esc {
				if end := strings.IndexByte(s[i+1:], esc); end >= 0 {
					body := s[i+1 : i+1+end]
					if preservedSeq(body) && wireSafe(body, byteEncode) {
						b.WriteString(s[i : i+2+end])
						i += end + 2
						continue
					}
				}
			}
			if body, ok := byteEncode(c); ok {
				b.WriteByte(esc)
				b.WriteString(body)
				b.WriteByte(esc)
			} else {
				b.WriteByte(c)
			}
			i++
		}
		return b.String()
	}
}

func wireSafe(body string, byteEncode func(byte) (string, bool)) bool {
	for i := 0; i < len(body); i++ {
		if _, ok := byteEncode(body[i]); ok {
			return false
		}
	}
	return true
}
