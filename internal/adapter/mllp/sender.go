package mllp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/adapter/tcp"
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
// rejection, straight to the Dead Letter Channel), and transport failures
// return transient errors for the retry machinery.
type Sender struct {
	cfg SenderConfig

	mu   sync.Mutex
	conn tcp.Client
}

func NewSender(settings map[string]any) (*Sender, error) {
	cfg := SenderConfig{
		ConnectTimeout: adapter.Duration(10 * time.Second),
		AckTimeout:     adapter.Duration(30 * time.Second),
		WriteTimeout:   adapter.Duration(10 * time.Second),
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
	return &Sender{
		cfg: cfg,
		conn: tcp.Client{
			Addr:           cfg.Addr,
			ConnectTimeout: time.Duration(cfg.ConnectTimeout),
			WriteTimeout:   time.Duration(cfg.WriteTimeout),
		},
	}, nil
}

// Open is intentionally lazy: the connection is established on first Send so
// a down receiver delays messages (retry/backoff) instead of failing channel
// startup.
func (s *Sender) Open(ctx context.Context) error { return nil }

func (s *Sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Close()
}

func (s *Sender) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.Connect(ctx); err != nil {
		return fmt.Errorf("mllp-sender: connect %s: %w", s.cfg.Addr, err) // transient: receiver down
	}
	release := s.conn.Session(ctx)
	defer release()

	if err := s.conn.Write(frame(payload)); err != nil {
		s.conn.Drop()
		return fmt.Errorf("mllp-sender: write: %w", tcp.CtxErr(ctx, err))
	}
	clear := s.conn.ReadDeadline(ctx, time.Duration(s.cfg.AckTimeout))
	ackRaw, err := readFrame(s.conn.Reader(), s.cfg.MaxAckSize)
	clear()
	if err != nil {
		s.conn.Drop()
		return fmt.Errorf("mllp-sender: waiting for ACK: %w", tcp.CtxErr(ctx, err))
	}

	code, text, err := AckStatus(ackRaw)
	if err != nil {
		s.conn.Drop()
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
