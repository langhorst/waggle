package astm1381

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
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

// ACK modes (mirroring the MLLP listener).
const (
	// AckImmediate acknowledges the final frame as soon as the engine has
	// recorded the message.
	AckImmediate = "immediate"
	// AckDestination holds the final frame's response until the pipeline
	// finishes: success ACKs, rejection or timeout answers EOT (the E1381
	// receiver interrupt — there is no application-status channel to carry
	// a reason).
	AckDestination = "destination"
)

// ListenerConfig configures the E1381 listener.
type ListenerConfig struct {
	Addr    string `yaml:"addr"`
	AckMode string `yaml:"ackMode"` // default immediate
	// HoldTimeout bounds the destination-mode wait on the final frame.
	// Default 30s.
	HoldTimeout adapter.Duration `yaml:"holdTimeout"`
	// FrameTimeout bounds the wait for the next frame once a transfer has
	// begun (the E1381 receiver timeout). Default 30s.
	FrameTimeout adapter.Duration `yaml:"frameTimeout"`
	// MaxMessageSize bounds one assembled message. Default 10 MiB.
	MaxMessageSize int `yaml:"maxMessageSize"`
}

// Listener is the E1381 inbound Channel Adapter. Each connection is one
// half-duplex protocol peer: sessions (ENQ … EOT) arrive sequentially, each
// carrying one E1394 message.
type Listener struct {
	cfg ListenerConfig

	mu     sync.Mutex
	ln     net.Listener
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewListener(settings map[string]any) (*Listener, error) {
	cfg := ListenerConfig{
		AckMode:        AckImmediate,
		HoldTimeout:    adapter.Duration(30 * time.Second),
		FrameTimeout:   adapter.Duration(30 * time.Second),
		MaxMessageSize: 10 << 20,
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("astm-listener: addr is required")
	}
	if cfg.AckMode != AckImmediate && cfg.AckMode != AckDestination {
		return nil, fmt.Errorf("astm-listener: ackMode must be %q or %q", AckImmediate, AckDestination)
	}
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.FrameTimeout <= 0 {
		cfg.FrameTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 10 << 20
	}
	return &Listener{cfg: cfg}, nil
}

// Addr returns the bound listen address (useful when configured with port
// 0); empty until Start.
func (l *Listener) Addr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ln == nil {
		return ""
	}
	return l.ln.Addr().String()
}

func (l *Listener) Start(ctx context.Context, deliver adapter.DeliverFunc) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", l.cfg.Addr)
	if err != nil {
		return fmt.Errorf("astm-listener: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)

	l.mu.Lock()
	l.ln = ln
	l.cancel = cancel
	l.mu.Unlock()

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			l.wg.Add(1)
			go func() {
				defer l.wg.Done()
				defer conn.Close()
				// Stop must not hang on connections a peer keeps open:
				// closing them unblocks the reads below.
				unregister := context.AfterFunc(runCtx, func() { conn.Close() })
				defer unregister()
				l.serve(runCtx, conn, deliver)
			}()
		}
	}()
	go func() {
		<-runCtx.Done()
		ln.Close()
	}()
	return nil
}

func (l *Listener) Stop() error {
	l.mu.Lock()
	cancel, ln := l.cancel, l.ln
	l.cancel, l.ln = nil, nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ln != nil {
		ln.Close()
	}
	l.wg.Wait()
	return nil
}

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
	// acknowledging — once ACKed, the sender considers it delivered.
	msg := s.buf
	s.buf = nil
	rec, err := deliver(ctx, msg, meta)
	if err != nil {
		// Not recorded: NAK so the sender retransmits this frame (delivery
		// is retried each time; nothing was persisted).
		s.buf = msg[:len(msg)-len(f.Text)] // keep earlier frames for the retry
		l.respond(conn, nak)
		return
	}
	s.lastAcked = f.Number
	s.expected = nextFrameNumber(f.Number)

	if l.cfg.AckMode == AckImmediate {
		l.respond(conn, ack)
		return
	}
	timer := time.NewTimer(time.Duration(l.cfg.HoldTimeout))
	defer timer.Stop()
	select {
	case d := <-rec.Done:
		if d.Code == "AA" {
			l.respond(conn, ack)
		} else {
			l.respond(conn, eot) // pipeline rejected: interrupt
		}
	case <-timer.C:
		l.respond(conn, eot)
	case <-ctx.Done():
	}
}

func (l *Listener) respond(conn net.Conn, b byte) {
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, _ = conn.Write([]byte{b})
	_ = conn.SetWriteDeadline(time.Time{})
}
