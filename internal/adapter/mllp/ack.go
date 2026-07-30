package mllp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/langhorst/integration-channel/internal/format/hl7v2"
	"github.com/langhorst/integration-channel/internal/message"
)

var hl7 = hl7v2.DataType{}

// BuildAck constructs an HL7 ACK for the inbound raw message: sender and
// receiver swapped, MSH-9 = ACK^<trigger>^ACK, MSA-1 = code (AA/AE/AR),
// MSA-2 = the inbound control ID, MSA-3 = text. When the inbound MSH cannot
// be parsed, a minimal static NAK is returned so the peer always gets a
// framed response.
func BuildAck(inboundRaw []byte, code, text string) []byte {
	now := time.Now().Format("20060102150405")
	controlID := newControlID()

	in, err := hl7.Parse(inboundRaw)
	if err != nil {
		raw, _ := hl7.Serialize(staticAck(now, controlID, code, text))
		return raw
	}
	get := func(path string) string {
		nodes, err := hl7.Resolve(in, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return hl7.Value(in, nodes[0])
	}

	ack := &message.Node{Name: "hl7v2"}
	set := func(path, value string) { _ = hl7.Set(ack, path, value) }
	// Mirror the inbound delimiters so the peer reads the ACK with the same
	// separators it sent.
	fieldSep := get("MSH-1")
	if fieldSep == "" {
		fieldSep = "|"
	}
	encoding := get("MSH-2")
	if encoding == "" {
		encoding = `^~\&`
	}
	set("MSH-1", fieldSep)
	set("MSH-2", encoding)
	set("MSH-3", get("MSH-5"))
	set("MSH-4", get("MSH-6"))
	set("MSH-5", get("MSH-3"))
	set("MSH-6", get("MSH-4"))
	set("MSH-7", now)
	set("MSH-9.1", "ACK")
	if trigger := get("MSH-9.2"); trigger != "" {
		set("MSH-9.2", trigger)
		set("MSH-9.3", "ACK")
	}
	set("MSH-10", controlID)
	if v := get("MSH-11"); v != "" {
		set("MSH-11", v)
	} else {
		set("MSH-11", "P")
	}
	if v := get("MSH-12"); v != "" {
		set("MSH-12", v)
	} else {
		set("MSH-12", "2.5.1")
	}
	set("MSA-1", code)
	set("MSA-2", get("MSH-10"))
	if text != "" {
		set("MSA-3", text)
	}
	raw, err := hl7.Serialize(ack)
	if err != nil {
		raw, _ = hl7.Serialize(staticAck(now, controlID, code, text))
	}
	return raw
}

func staticAck(now, controlID, code, text string) *message.Node {
	ack := &message.Node{Name: "hl7v2"}
	set := func(path, value string) { _ = hl7.Set(ack, path, value) }
	set("MSH-1", "|")
	set("MSH-2", `^~\&`)
	set("MSH-7", now)
	set("MSH-9.1", "ACK")
	set("MSH-10", controlID)
	set("MSH-11", "P")
	set("MSH-12", "2.5.1")
	set("MSA-1", code)
	if text != "" {
		set("MSA-3", text)
	}
	return ack
}

// AckStatus extracts (MSA-1, MSA-3) from a raw ACK message.
func AckStatus(ackRaw []byte) (code, text string, err error) {
	root, err := hl7.Parse(ackRaw)
	if err != nil {
		return "", "", fmt.Errorf("mllp: unparseable ACK: %w", err)
	}
	get := func(path string) string {
		nodes, err := hl7.Resolve(root, path)
		if err != nil || len(nodes) == 0 {
			return ""
		}
		return hl7.Value(root, nodes[0])
	}
	code = get("MSA-1")
	if code == "" {
		return "", "", fmt.Errorf("mllp: ACK has no MSA-1")
	}
	return code, get("MSA-3"), nil
}

func newControlID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
