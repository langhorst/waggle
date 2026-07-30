package mllp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/langhorst/integration-channel/internal/adapter"
)

func init() {
	adapter.RegisterInbound("mllp-listener", func(settings map[string]any) (adapter.Inbound, error) {
		return NewListener(settings)
	})
	adapter.RegisterOutbound("mllp-sender", func(settings map[string]any) (adapter.Outbound, error) {
		return NewSender(settings)
	})
}

// AckMode selects when the listener acknowledges the sending system.
const (
	// AckImmediate acknowledges as soon as the engine records the message
	// (Guaranteed Delivery handoff): fire-and-forget downstream.
	AckImmediate = "immediate"
	// AckDestination holds the ACK until the pipeline finishes — filter,
	// transformers (which may reject via the response API), and delivery to
	// every waitForAck destination.
	AckDestination = "destination"
)

// ListenerConfig configures the MLLP listener.
type ListenerConfig struct {
	Addr    string `yaml:"addr"`
	AckMode string `yaml:"ackMode"` // default immediate
	// HoldTimeout bounds how long a destination-mode ACK is held; on expiry
	// FallbackAck is sent. Default 30s.
	HoldTimeout adapter.Duration `yaml:"holdTimeout"`
	FallbackAck string           `yaml:"fallbackAck"` // default AE
	// MaxMessageSize bounds one message's bytes. Default 10 MiB.
	MaxMessageSize int `yaml:"maxMessageSize"`
}

// Listener is the MLLP inbound Channel Adapter. Each connection is served by
// one goroutine and handled strictly in order: read frame, deliver, ACK,
// next frame. Multiple messages per connection are supported; a framing
// violation closes the connection.
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
		FallbackAck:    "AE",
		MaxMessageSize: 10 << 20,
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("mllp-listener: addr is required")
	}
	if cfg.AckMode != AckImmediate && cfg.AckMode != AckDestination {
		return nil, fmt.Errorf("mllp-listener: ackMode must be %q or %q", AckImmediate, AckDestination)
	}
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.FallbackAck == "" {
		cfg.FallbackAck = "AE"
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
	ln, err := net.Listen("tcp", l.cfg.Addr)
	if err != nil {
		return fmt.Errorf("mllp-listener: %w", err)
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
				// closing them unblocks the frame read below.
				unregister := context.AfterFunc(runCtx, func() { conn.Close() })
				defer unregister()
				l.serve(runCtx, conn, deliver)
			}()
		}
	}()
	// Close connections when the context ends so serve loops unblock.
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

func (l *Listener) serve(ctx context.Context, conn net.Conn, deliver adapter.DeliverFunc) {
	br := bufio.NewReader(conn)
	meta := map[string]string{"source.remote": conn.RemoteAddr().String()}
	for ctx.Err() == nil {
		raw, err := readFrame(br, l.cfg.MaxMessageSize)
		if err != nil {
			if err != io.EOF {
				// Framing violation or truncated frame: drop the connection.
			}
			return
		}
		l.handleMessage(ctx, conn, raw, meta, deliver)
	}
}

func (l *Listener) handleMessage(ctx context.Context, conn net.Conn, raw []byte, meta map[string]string, deliver adapter.DeliverFunc) {
	rec, err := deliver(ctx, raw, meta)
	if err != nil {
		_ = writeFrame(conn, BuildAck(raw, "AE", err.Error()))
		return
	}
	if l.cfg.AckMode == AckImmediate {
		_ = writeFrame(conn, BuildAck(raw, "AA", ""))
		return
	}
	timer := time.NewTimer(time.Duration(l.cfg.HoldTimeout))
	defer timer.Stop()
	select {
	case d := <-rec.Done:
		_ = writeFrame(conn, BuildAck(raw, d.Code, d.Text))
	case <-timer.C:
		_ = writeFrame(conn, BuildAck(raw, l.cfg.FallbackAck, "processing timed out"))
	case <-ctx.Done():
	}
}
