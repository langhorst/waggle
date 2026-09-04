// Package tcp holds the connection plumbing shared by the TCP-based
// Channel Adapters (MLLP, ASTM E1381): a listener that serves one goroutine
// per connection and stops cleanly, and a lazily dialed client connection
// that reconnects on demand. Protocol handling stays with each adapter.
package tcp

import (
	"bufio"
	"context"
	"net"
	"sync"
	"time"
)

// Server accepts connections and serves each on its own goroutine until
// Stop, which closes the listener and every connection and waits for the
// goroutines to finish. The zero value is ready to use.
type Server struct {
	mu     sync.Mutex
	ln     net.Listener
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Start binds addr and serves connections with serve. serve's context ends
// on Stop; the connection is closed at the same moment so blocked reads
// return, and closed again after serve returns.
func (s *Server) Start(ctx context.Context, addr string, serve func(ctx context.Context, conn net.Conn)) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)

	s.mu.Lock()
	s.ln, s.cancel = ln, cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer conn.Close()
				// Stop must not hang on a connection the peer keeps open:
				// closing it unblocks whatever serve is reading.
				unregister := context.AfterFunc(runCtx, func() { conn.Close() })
				defer unregister()
				serve(runCtx, conn)
			}()
		}
	}()
	// The parent context ending (channel stop without an explicit Stop)
	// also closes the listener so Accept returns.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-runCtx.Done()
		ln.Close()
	}()
	return nil
}

// Addr is the bound address, or "" before Start / after Stop.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Stop closes the listener and all connections and waits for every serve
// goroutine to return. Safe to call when not started.
func (s *Server) Stop() error {
	s.mu.Lock()
	cancel, ln := s.cancel, s.ln
	s.cancel, s.ln = nil, nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ln != nil {
		ln.Close()
	}
	s.wg.Wait()
	return nil
}

// Client is a lazily dialed connection to one peer. It is not safe for
// concurrent use: the owning adapter serializes sends with its own mutex,
// and the connection state below belongs to whoever holds it.
type Client struct {
	Addr           string
	ConnectTimeout time.Duration
	// WriteTimeout bounds each Write. Zero means no deadline.
	WriteTimeout time.Duration

	conn net.Conn
	br   *bufio.Reader
}

// Connect dials if not connected. Failure leaves the client disconnected.
func (c *Client) Connect(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	d := net.Dialer{Timeout: c.ConnectTimeout}
	conn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return err
	}
	c.conn = conn
	c.br = bufio.NewReader(conn)
	return nil
}

// Connected reports whether a connection is open.
func (c *Client) Connected() bool { return c.conn != nil }

// Reader is the buffered reader over the connection; nil when disconnected.
func (c *Client) Reader() *bufio.Reader { return c.br }

// Drop closes the connection so the next Connect redials. Use it after any
// I/O error: the stream state is unknown.
func (c *Client) Drop() {
	if c.conn != nil {
		c.conn.Close()
		c.conn, c.br = nil, nil
	}
}

// Close drops the connection. Implements the Outbound adapter's Close.
func (c *Client) Close() error {
	c.Drop()
	return nil
}

// Write writes p under WriteTimeout.
func (c *Client) Write(p []byte) error {
	if c.WriteTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}
	_, err := c.conn.Write(p)
	return err
}

// ReadDeadline arms a read deadline of timeout from now, or the context's
// deadline if that comes sooner. The returned func clears it.
func (c *Client) ReadDeadline(ctx context.Context, timeout time.Duration) (clear func()) {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.conn.SetReadDeadline(deadline)
	return func() { _ = c.conn.SetReadDeadline(time.Time{}) }
}

// Session binds the connection to ctx for the duration of one exchange:
// when ctx ends the connection's deadline is expired, which is the only way
// to interrupt a blocked net.Conn read or write. The returned func releases
// the binding and must be called before the exchange's result is decided.
func (c *Client) Session(ctx context.Context) (release func()) {
	conn := c.conn
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	return func() { stop() }
}

// CtxErr reports the context's error when it is the reason an I/O call
// failed (the deadline was the context's, or it was expired by Session),
// so callers surface "context canceled" rather than a bare I/O timeout.
func CtxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
		return context.DeadlineExceeded
	}
	return err
}
