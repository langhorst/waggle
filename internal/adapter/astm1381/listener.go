package astm1381

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/adapter/tcp"
	metakey "github.com/langhorst/waggle/internal/meta"
)

func init() {
	adapter.RegisterInbound("astm-listener", func(settings map[string]any) (adapter.Inbound, error) {
		return NewListener(settings)
	})
	adapter.RegisterOutbound("astm-sender", func(settings map[string]any) (adapter.Outbound, error) {
		return NewSender(settings)
	})
}

// ListenerConfig configures the E1381 listener.
type ListenerConfig struct {
	Addr string `yaml:"addr"`
	// AckMode is immediate (default) or destination; see adapter.AckMode.
	// In destination mode the final frame's response waits for the
	// pipeline: an accepting decision ACKs, a rejection or timeout answers
	// EOT (the E1381 receiver interrupt: there is no application-status
	// channel to carry a reason).
	AckMode adapter.AckMode `yaml:"ackMode"`
	// HoldTimeout bounds the destination-mode wait on the final frame.
	// Default 30s.
	HoldTimeout adapter.Duration `yaml:"holdTimeout"`
	// FrameTimeout bounds the wait for the next frame once a transfer has
	// begun (the E1381 receiver timeout). Default 30s.
	FrameTimeout adapter.Duration `yaml:"frameTimeout"`
	// MaxMessageSize bounds one assembled message. Default 10 MiB.
	MaxMessageSize int `yaml:"maxMessageSize"`
	// WriteTimeout bounds writing one control byte. Default 10s.
	WriteTimeout adapter.Duration `yaml:"writeTimeout"`
}

// Listener is the E1381 inbound Channel Adapter. Each connection is one
// half-duplex protocol peer: sessions (ENQ ... EOT) arrive sequentially,
// each carrying one E1394 message.
type Listener struct {
	cfg ListenerConfig
	srv tcp.Server
}

func NewListener(settings map[string]any) (*Listener, error) {
	cfg := ListenerConfig{
		HoldTimeout:    adapter.Duration(30 * time.Second),
		FrameTimeout:   adapter.Duration(30 * time.Second),
		MaxMessageSize: 10 << 20,
		WriteTimeout:   adapter.Duration(10 * time.Second),
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("astm-listener: addr is required")
	}
	mode, err := adapter.ParseAckMode(string(cfg.AckMode))
	if err != nil {
		return nil, fmt.Errorf("astm-listener: %w", err)
	}
	cfg.AckMode = mode
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.FrameTimeout <= 0 {
		cfg.FrameTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 10 << 20
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = adapter.Duration(10 * time.Second)
	}
	return &Listener{cfg: cfg}, nil
}

// Addr returns the bound listen address (useful when configured with port
// 0); empty until Start.
func (l *Listener) Addr() string { return l.srv.Addr() }

func (l *Listener) Start(ctx context.Context, deliver adapter.DeliverFunc) error {
	err := l.srv.Start(ctx, l.cfg.Addr, func(ctx context.Context, conn net.Conn) {
		l.serve(ctx, conn, deliver)
	})
	if err != nil {
		return fmt.Errorf("astm-listener: %w", err)
	}
	return nil
}

func (l *Listener) Stop() error { return l.srv.Stop() }

// session is the per-connection receiver state.
type session struct {
	inTransfer bool
	expected   byte // next expected frame number
	lastAcked  byte // last accepted frame number (0 = none); duplicates re-ACK
	buf        []byte
}

func (l *Listener) serve(ctx context.Context, conn net.Conn, deliver adapter.DeliverFunc) {
	br := bufio.NewReader(conn)
	meta := map[string]string{metakey.SourceRemote: conn.RemoteAddr().String()}
	s := &session{}

	for ctx.Err() == nil {
		// Idle connections may sit indefinitely between sessions; once a
		// transfer is underway the peer must keep frames coming.
		if s.inTransfer {
			_ = conn.SetReadDeadline(time.Now().Add(time.Duration(l.cfg.FrameTimeout)))
		} else {
			_ = conn.SetReadDeadline(time.Time{})
		}
		first, err := br.ReadByte()
		if err != nil {
			return // closed, or frame timeout: drop the connection
		}
		switch first {
		case enq:
			// New session (an ENQ mid-transfer restarts it).
			s.inTransfer = true
			s.expected = '1'
			s.lastAcked = 0
			s.buf = nil
			l.respond(conn, ack)

		case eot:
			// Sender terminated (or aborted) the session.
			s.inTransfer = false
			s.buf = nil

		case stx:
			if !s.inTransfer {
				return // frame outside a session: protocol violation
			}
			f, err := readFrameAfter(stx, br, defaultFrameSize)
			if err != nil {
				l.respond(conn, nak) // damaged frame: ask for retransmission
				continue
			}
			l.handleFrame(ctx, conn, s, f, meta, deliver)

		default:
			return // garbage on the wire: drop the connection
		}
	}
}

func (l *Listener) handleFrame(ctx context.Context, conn net.Conn, s *session, f frame, meta map[string]string, deliver adapter.DeliverFunc) {
	switch f.Number {
	case s.expected:
		// Accept below.
	case s.lastAcked:
		// Retransmission of a frame whose ACK was lost: re-ACK, discard.
		l.respond(conn, ack)
		return
	default:
		l.respond(conn, nak)
		return
	}

	if len(s.buf)+len(f.Text) > l.cfg.MaxMessageSize {
		// Message too large: interrupt the transfer entirely.
		l.respond(conn, eot)
		s.inTransfer = false
		s.buf = nil
		return
	}
	s.buf = append(s.buf, f.Text...)

	if !f.Last {
		s.lastAcked = f.Number
		s.expected = nextFrameNumber(f.Number)
		l.respond(conn, ack)
		return
	}

	// Final frame: the message is complete. Hand it to the engine BEFORE
	// acknowledging: once ACKed, the sender considers it delivered. The
	// handoff must not be cut short by Stop; only the hold below is.
	msg := s.buf
	s.buf = nil
	rec, err := deliver(context.WithoutCancel(ctx), msg, meta)
	if err != nil {
		// Not recorded: NAK so the sender retransmits this frame (delivery
		// is retried each time; nothing was persisted).
		s.buf = msg[:len(msg)-len(f.Text)] // keep earlier frames for the retry
		l.respond(conn, nak)
		return
	}
	s.lastAcked = f.Number
	s.expected = nextFrameNumber(f.Number)

	if l.cfg.AckMode == adapter.AckImmediate {
		l.respond(conn, ack)
		return
	}
	d, outcome := adapter.AwaitDecision(ctx, rec, time.Duration(l.cfg.HoldTimeout))
	switch outcome {
	case adapter.Decided:
		if d.Accepted() {
			l.respond(conn, ack)
		} else {
			l.respond(conn, eot) // pipeline rejected: interrupt
		}
	case adapter.TimedOut:
		l.respond(conn, eot)
	case adapter.Canceled:
		// Stopping: the message is recorded; the connection closes.
	}
}

func (l *Listener) respond(conn net.Conn, b byte) {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(l.cfg.WriteTimeout)))
	_, _ = conn.Write([]byte{b})
	_ = conn.SetWriteDeadline(time.Time{})
}
