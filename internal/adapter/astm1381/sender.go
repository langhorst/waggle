package astm1381

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

// SenderConfig configures the E1381 sender.
type SenderConfig struct {
	Addr           string           `yaml:"addr"`
	ConnectTimeout adapter.Duration `yaml:"connectTimeout"` // default 10s
	// AckTimeout bounds each wait for ENQ/frame acknowledgment (the E1381
	// sender timeout is 15s). Default 15s.
	AckTimeout adapter.Duration `yaml:"ackTimeout"`
	// MaxRetries is how many times one frame is sent before the session is
	// aborted (initial transmission plus retransmissions on NAK). Default 6:
	// E1381 aborts a frame rejected six times.
	MaxRetries int `yaml:"maxRetries"`
	// FrameSize is the maximum frame text length. Default 240 (the E1381
	// maximum).
	FrameSize int `yaml:"frameSize"`
}

// Sender is the E1381 outbound Channel Adapter. One Send runs one complete
// session: ENQ, frames with NAK-retransmit, EOT. Every failure is transient
// — E1381 has no application-level rejection, so a busy or interrupting
// receiver simply causes the delivery queue to retry with backoff (and
// dead-letter after maxAttempts).
type Sender struct {
	cfg SenderConfig

	mu   sync.Mutex
	conn net.Conn
	br   *bufio.Reader
}

func NewSender(settings map[string]any) (*Sender, error) {
	cfg := SenderConfig{
		ConnectTimeout: adapter.Duration(10 * time.Second),
		AckTimeout:     adapter.Duration(15 * time.Second),
		MaxRetries:     6,
		FrameSize:      defaultFrameSize,
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("astm-sender: addr is required")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = adapter.Duration(10 * time.Second)
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = adapter.Duration(15 * time.Second)
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 6
	}
	if cfg.FrameSize <= 0 || cfg.FrameSize > defaultFrameSize {
		cfg.FrameSize = defaultFrameSize
	}
	return &Sender{cfg: cfg}, nil
}

// Open is lazy like the MLLP sender: the connection is established on first
// Send so a down receiver delays messages instead of failing channel start.
func (s *Sender) Open(ctx context.Context) error { return nil }

func (s *Sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropConnLocked()
	return nil
}

func (s *Sender) dropConnLocked() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
		s.br = nil
	}
}

func (s *Sender) ensureConnLocked(ctx context.Context) error {
	if s.conn != nil {
		return nil
	}
	d := net.Dialer{Timeout: time.Duration(s.cfg.ConnectTimeout)}
	conn, err := d.DialContext(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("astm-sender: connect %s: %w", s.cfg.Addr, err)
	}
	s.conn = conn
	s.br = bufio.NewReader(conn)
	return nil
}

// awaitLocked reads one control byte within the ACK timeout.
func (s *Sender) awaitLocked(ctx context.Context) (byte, error) {
	deadline := time.Now().Add(time.Duration(s.cfg.AckTimeout))
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = s.conn.SetReadDeadline(deadline)
	b, err := s.br.ReadByte()
	_ = s.conn.SetReadDeadline(time.Time{})
	return b, err
}

func (s *Sender) writeLocked(p []byte) error {
	_ = s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := s.conn.Write(p)
	_ = s.conn.SetWriteDeadline(time.Time{})
	return err
}

func (s *Sender) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureConnLocked(ctx); err != nil {
		return err // receiver down: retry later
	}

	// Establishment.
	if err := s.writeLocked([]byte{enq}); err != nil {
		s.dropConnLocked()
		return fmt.Errorf("astm-sender: ENQ: %w", err)
	}
	switch b, err := s.awaitLocked(ctx); {
	case err != nil:
		s.dropConnLocked()
		return fmt.Errorf("astm-sender: awaiting ENQ response: %w", err)
	case b == ack:
		// Proceed to transfer.
	case b == nak:
		// Receiver busy: keep the connection, let the queue retry.
		return fmt.Errorf("astm-sender: receiver busy (NAK to ENQ)")
	default:
		s.dropConnLocked()
		return fmt.Errorf("astm-sender: unexpected ENQ response 0x%02X", b)
	}

	// Transfer: each frame is retransmitted on NAK up to MaxRetries times.
	for _, f := range splitFrames(payload, s.cfg.FrameSize) {
		wire := encodeFrame(f)
		accepted := false
		for attempt := 1; attempt <= s.cfg.MaxRetries; attempt++ {
			if err := s.writeLocked(wire); err != nil {
				s.dropConnLocked()
				return fmt.Errorf("astm-sender: frame %c: %w", f.Number, err)
			}
			b, err := s.awaitLocked(ctx)
			if err != nil {
				s.dropConnLocked()
				return fmt.Errorf("astm-sender: awaiting frame %c ACK: %w", f.Number, err)
			}
			switch b {
			case ack:
				accepted = true
			case nak:
				continue // retransmit
			case eot:
				// Receiver interrupt: stop transmitting immediately.
				_ = s.writeLocked([]byte{eot})
				return fmt.Errorf("astm-sender: receiver interrupted the transfer (EOT)")
			default:
				s.dropConnLocked()
				return fmt.Errorf("astm-sender: unexpected frame response 0x%02X", b)
			}
			break
		}
		if !accepted {
			// Retries exhausted: abort the session.
			_ = s.writeLocked([]byte{eot})
			return fmt.Errorf("astm-sender: frame %c rejected %d times", f.Number, s.cfg.MaxRetries)
		}
	}

	// Termination.
	if err := s.writeLocked([]byte{eot}); err != nil {
		s.dropConnLocked()
		return fmt.Errorf("astm-sender: EOT: %w", err)
	}
	return nil
}
