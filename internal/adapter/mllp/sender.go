package mllp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

// SenderConfig configures the MLLP sender.
type SenderConfig struct {
	Addr           string           `yaml:"addr"`
	ConnectTimeout adapter.Duration `yaml:"connectTimeout"` // default 10s
	AckTimeout     adapter.Duration `yaml:"ackTimeout"`     // default 30s
	WriteTimeout   adapter.Duration `yaml:"writeTimeout"`   // default 10s
	MaxAckSize     int              `yaml:"maxAckSize"`     // default 1 MiB
}

// Sender is the MLLP outbound Channel Adapter. It keeps one persistent
// connection, reconnecting on demand. Send blocks for the receiving system's
// ACK: AA/CA succeed, AE/AR/CE/CR return a Permanent error (application
// rejection → Dead Letter Channel), and transport failures return transient
// errors for the retry machinery.
type Sender struct {
	cfg SenderConfig

	mu   sync.Mutex
	conn net.Conn
	br   *bufio.Reader
}

func NewSender(settings map[string]any) (*Sender, error) {
	cfg := SenderConfig{
		ConnectTimeout: adapter.Duration(10 * time.Second),
		AckTimeout:     adapter.Duration(30 * time.Second),
		MaxAckSize:     1 << 20,
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("mllp-sender: addr is required")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = adapter.Duration(10 * time.Second)
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = adapter.Duration(10 * time.Second)
	}
	if cfg.MaxAckSize <= 0 {
		cfg.MaxAckSize = 1 << 20
	}
	return &Sender{cfg: cfg}, nil
}

// Open is intentionally lazy: the connection is established on first Send so
// a down receiver delays messages (retry/backoff) instead of failing channel
// startup.
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
		return fmt.Errorf("mllp-sender: connect %s: %w", s.cfg.Addr, err)
	}
	s.conn = conn
	s.br = bufio.NewReader(conn)
	return nil
}

// ctxOr reports the context's error when it is the reason an I/O call
// failed, so callers see "context canceled" rather than a bare timeout.
func ctxOr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// The read deadline may be the context's own deadline, in which case
	// the I/O timeout fires in the same instant the context expires.
	if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
		return context.DeadlineExceeded
	}
	return err
}

func (s *Sender) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureConnLocked(ctx); err != nil {
		return err // transient: receiver down
	}
	// Cancellation (shutdown) must unblock a send parked on a stalled
	// peer: expiring the deadline is the only way to interrupt a blocked
	// net.Conn read or write.
	conn := s.conn
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(s.cfg.WriteTimeout)))
	err := writeFrame(conn, payload)
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		s.dropConnLocked()
		return fmt.Errorf("mllp-sender: write: %w", ctxOr(ctx, err))
	}
	deadline := time.Now().Add(time.Duration(s.cfg.AckTimeout))
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetReadDeadline(deadline)
	ackRaw, err := readFrame(s.br, s.cfg.MaxAckSize)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		s.dropConnLocked()
		return fmt.Errorf("mllp-sender: waiting for ACK: %w", ctxOr(ctx, err))
	}

	code, text, err := AckStatus(ackRaw)
	if err != nil {
		s.dropConnLocked()
		return err // unparseable ACK: transient, resync the connection
	}
	switch code {
	case "AA", "CA":
		return nil
	case "AE", "AR", "CE", "CR":
		if text != "" {
			return adapter.Permanent(fmt.Errorf("mllp-sender: receiver rejected message: %s (%s)", code, text))
		}
		return adapter.Permanent(fmt.Errorf("mllp-sender: receiver rejected message: %s", code))
	default:
		return fmt.Errorf("mllp-sender: unrecognized ACK code %q", code)
	}
}
