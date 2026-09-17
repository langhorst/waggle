// Package sink delivers a feed's messages: over MLLP to a running engine,
// as files for a directory-watching channel, or as a corpus on disk to be
// replayed later.
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/sim"

	// Registers the MLLP adapters this package builds on.
	_ "github.com/langhorst/waggle/internal/adapter/mllp"
)

// Outbound adapts any waggle outbound Channel Adapter into a sim.Sink.
//
// The engine's own adapters already solve what a simulator's transport needs
// -- MLLP framing, reconnection, and reading the ACK to tell AA from AE --
// and reusing them means the simulator and the engine cannot drift apart on
// the wire. It also means a sink reacts to acknowledgements for free: the
// sender returns a PermanentError on AE/AR, which stops the run rather than
// pretending a rejected message was delivered.
type Outbound struct {
	Adapter adapter.Outbound
	// Tolerate keeps the run going when the receiver rejects a message,
	// which is what the chaos profiles want: a rejection is the interesting
	// outcome, not a reason to stop.
	Tolerate bool
	// OnReject is called for a rejected message when Tolerate is set.
	OnReject func(m sim.Message, err error)

	once sync.Once
	open error
}

// NewMLLP returns a sink sending to addr over MLLP.
func NewMLLP(addr string, ackTimeout time.Duration) (*Outbound, error) {
	settings := map[string]any{"addr": addr}
	if ackTimeout > 0 {
		settings["ackTimeout"] = ackTimeout.String()
	}
	out, err := adapter.NewOutbound("mllp-sender", settings)
	if err != nil {
		return nil, err
	}
	return &Outbound{Adapter: out}, nil
}

func (o *Outbound) Send(ctx context.Context, m sim.Message) error {
	o.once.Do(func() { o.open = o.Adapter.Open(ctx) })
	if o.open != nil {
		return o.open
	}
	meta := map[string]string{
		"sim.feed":      m.Feed,
		"sim.trigger":   m.Trigger,
		"sim.controlId": m.ControlID,
		"sim.simulated": "true",
		"sim.eventKind": string(m.Cause.Kind),
	}
	err := o.Adapter.Send(ctx, m.Raw, meta)
	if err != nil && o.Tolerate {
		if o.OnReject != nil {
			o.OnReject(m, err)
		}
		return nil
	}
	return err
}

func (o *Outbound) Close() error { return o.Adapter.Close() }

// Dir writes each message as its own file, for a channel whose source is a
// watched directory. Files are named in emission order so a directory listing
// reads as the feed did.
type Dir struct {
	Path string
	// Ext defaults to ".hl7".
	Ext string

	mu sync.Mutex
	n  int
}

func (d *Dir) Send(_ context.Context, m sim.Message) error {
	d.mu.Lock()
	d.n++
	n := d.n
	d.mu.Unlock()

	if err := os.MkdirAll(d.Path, 0o755); err != nil {
		return err
	}
	ext := d.Ext
	if ext == "" {
		ext = ".hl7"
	}
	name := fmt.Sprintf("%08d-%s-%s%s", n, m.Feed, safe(m.Trigger), ext)
	// Written to a temporary name and renamed, so a directory-watching
	// source never reads a half-written message.
	tmp := filepath.Join(d.Path, "."+name+".part")
	if err := os.WriteFile(tmp, m.Raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(d.Path, name))
}

func (d *Dir) Close() error { return nil }

// Corpus writes a replayable run: every message in one file, with a
// line-per-message manifest carrying the provenance needed to replay it at
// the right simulated times.
type Corpus struct {
	Path string

	mu       sync.Mutex
	messages *os.File
	manifest *os.File
	n        int
}

// manifestEntry is one line of the corpus manifest.
type manifestEntry struct {
	Seq       int       `json:"seq"`
	Feed      string    `json:"feed"`
	Trigger   string    `json:"trigger"`
	ControlID string    `json:"controlId"`
	At        time.Time `json:"at"`
	Event     string    `json:"event"`
	Offset    int64     `json:"offset"`
	Length    int       `json:"length"`
}

func (c *Corpus) open() error {
	if c.messages != nil {
		return nil
	}
	if err := os.MkdirAll(c.Path, 0o755); err != nil {
		return err
	}
	var err error
	if c.messages, err = os.Create(filepath.Join(c.Path, "messages.hl7")); err != nil {
		return err
	}
	if c.manifest, err = os.Create(filepath.Join(c.Path, "manifest.jsonl")); err != nil {
		return err
	}
	return nil
}

func (c *Corpus) Send(_ context.Context, m sim.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.open(); err != nil {
		return err
	}
	offset, err := c.messages.Seek(0, 1)
	if err != nil {
		return err
	}
	if _, err := c.messages.Write(m.Raw); err != nil {
		return err
	}
	c.n++
	line, err := json.Marshal(manifestEntry{
		Seq: c.n, Feed: m.Feed, Trigger: m.Trigger, ControlID: m.ControlID,
		At: m.At, Event: string(m.Cause.Kind), Offset: offset, Length: len(m.Raw),
	})
	if err != nil {
		return err
	}
	_, err = c.manifest.Write(append(line, '\n'))
	return err
}

func (c *Corpus) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, f := range []*os.File{c.messages, c.manifest} {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.messages, c.manifest = nil, nil
	return first
}

// Multi fans one feed's messages out to several sinks, so a run can go to a
// live engine and to a corpus at the same time.
type Multi []sim.Sink

func (m Multi) Send(ctx context.Context, msg sim.Message) error {
	for _, s := range m {
		if err := s.Send(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (m Multi) Close() error {
	var first error
	for _, s := range m {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Discard throws messages away, for measuring the model without a receiver.
type Discard struct {
	mu sync.Mutex
	n  int
}

func (d *Discard) Send(context.Context, sim.Message) error {
	d.mu.Lock()
	d.n++
	d.mu.Unlock()
	return nil
}
func (d *Discard) Close() error { return nil }

// Count reports how many messages were discarded.
func (d *Discard) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}

// safe makes a trigger usable in a filename.
func safe(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
