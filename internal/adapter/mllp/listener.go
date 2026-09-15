package mllp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
	"github.com/langhorst/waggle/internal/adapter/tcp"
	metakey "github.com/langhorst/waggle/internal/meta"
)

func init() {
	adapter.RegisterInbound("mllp-listener", func(settings map[string]any) (adapter.Inbound, error) {
		return NewListener(settings)
	})
	adapter.RegisterOutbound("mllp-sender", func(settings map[string]any) (adapter.Outbound, error) {
		return NewSender(settings)
	})
}

// ListenerConfig configures the MLLP listener.
type ListenerConfig struct {
	Addr string `yaml:"addr"`
	// AckMode is immediate (default) or destination; see adapter.AckMode.
	AckMode adapter.AckMode `yaml:"ackMode"`
	// HoldTimeout bounds how long a destination-mode ACK is held; on expiry
	// FallbackAck is sent. Default 30s.
	HoldTimeout adapter.Duration `yaml:"holdTimeout"`
	FallbackAck string           `yaml:"fallbackAck"` // default AE
	// MaxMessageSize bounds one message's bytes. Default 10 MiB.
	MaxMessageSize int `yaml:"maxMessageSize"`
	// WriteTimeout bounds writing one ACK. Default 10s.
	WriteTimeout adapter.Duration `yaml:"writeTimeout"`
	// IdleTimeout drops a connection that sends nothing for this long
	// between messages. Zero (the default) keeps idle connections open,
	// which is what HL7 senders expect.
	IdleTimeout adapter.Duration `yaml:"idleTimeout"`
}

// Listener is the MLLP inbound Channel Adapter. Each connection is served by
// one goroutine and handled strictly in order: read frame, deliver, ACK,
// next frame. Multiple messages per connection are supported; a framing
// violation closes the connection.
type Listener struct {
	cfg ListenerConfig
	log *slog.Logger
	srv tcp.Server
}

func NewListener(settings map[string]any) (*Listener, error) {
	cfg := ListenerConfig{
		HoldTimeout:    adapter.Duration(30 * time.Second),
		FallbackAck:    "AE",
		MaxMessageSize: 10 << 20,
		WriteTimeout:   adapter.Duration(10 * time.Second),
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("mllp-listener: addr is required")
	}
	mode, err := adapter.ParseAckMode(string(cfg.AckMode))
	if err != nil {
		return nil, fmt.Errorf("mllp-listener: %w", err)
	}
	cfg.AckMode = mode
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.FallbackAck == "" {
		cfg.FallbackAck = "AE"
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 10 << 20
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = adapter.Duration(10 * time.Second)
	}
	return &Listener{cfg: cfg, log: slog.Default()}, nil
}

// Addr returns the bound listen address (useful when configured with port
// 0); empty until Start.
func (l *Listener) Addr() string { return l.srv.Addr() }

func (l *Listener) Start(ctx context.Context, deliver adapter.DeliverFunc) error {
	err := l.srv.Start(ctx, l.cfg.Addr, func(ctx context.Context, conn net.Conn) {
		l.serve(ctx, conn, deliver)
	})
	if err != nil {
		return fmt.Errorf("mllp-listener: %w", err)
	}
	return nil
}

func (l *Listener) Stop() error { return l.srv.Stop() }

func (l *Listener) serve(ctx context.Context, conn net.Conn, deliver adapter.DeliverFunc) {
	br := bufio.NewReader(conn)
	meta := map[string]string{metakey.SourceRemote: conn.RemoteAddr().String()}
	for ctx.Err() == nil {
		if l.cfg.IdleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(time.Duration(l.cfg.IdleTimeout)))
		}
		raw, err := readFrame(br, l.cfg.MaxMessageSize)
		if err != nil {
			// EOF is the peer hanging up; anything else is a framing
			// violation, truncated frame, or idle timeout. Either way,
			// drop the connection.
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				l.log.Warn("mllp-listener: dropping connection", "remote", conn.RemoteAddr(), "error", err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		l.handleMessage(ctx, conn, raw, meta, deliver)
	}
}

func (l *Listener) handleMessage(ctx context.Context, conn net.Conn, raw []byte, meta map[string]string, deliver adapter.DeliverFunc) {
	// The handoff must not be cut short by Stop; only the hold below is.
	rec, err := deliver(context.WithoutCancel(ctx), raw, meta)
	if err != nil {
		l.respond(conn, BuildAck(raw, "AE", err.Error()))
		return
	}
	if l.cfg.AckMode == adapter.AckImmediate {
		l.respond(conn, BuildAck(raw, "AA", ""))
		return
	}
	d, outcome := adapter.AwaitDecision(ctx, rec, time.Duration(l.cfg.HoldTimeout))
	switch outcome {
	case adapter.Decided:
		l.respond(conn, BuildAck(raw, d.Code, d.Text))
	case adapter.TimedOut:
		l.respond(conn, BuildAck(raw, l.cfg.FallbackAck, "processing timed out"))
	case adapter.Canceled:
		// Stopping: the message is recorded; the sender will see the
		// connection close and, if it retries, a duplicate is at-least-once
		// delivery doing its job.
	}
}

func (l *Listener) respond(conn net.Conn, ackMsg []byte) {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(l.cfg.WriteTimeout)))
	_ = writeFrame(conn, ackMsg)
	_ = conn.SetWriteDeadline(time.Time{})
}
