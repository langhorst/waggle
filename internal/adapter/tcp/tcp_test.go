package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestServerServesAndStops(t *testing.T) {
	var srv Server
	served := make(chan string, 4)
	err := srv.Start(context.Background(), "127.0.0.1:0", func(ctx context.Context, conn net.Conn) {
		buf := make([]byte, 16)
		n, _ := conn.Read(buf)
		served <- string(buf[:n])
		_, _ = conn.Write([]byte("ok"))
		<-ctx.Done() // hold the connection open until Stop
	})
	if err != nil {
		t.Fatal(err)
	}
	if srv.Addr() == "" {
		t.Fatal("no bound address")
	}
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := <-served; got != "hello" {
		t.Errorf("served %q", got)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil || string(reply) != "ok" {
		t.Errorf("reply = %q, %v", reply, err)
	}

	stopped := make(chan struct{})
	go func() { _ = srv.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung on an open connection")
	}
	if srv.Addr() != "" {
		t.Error("Addr should be empty after Stop")
	}
	if _, err := net.DialTimeout("tcp", conn.RemoteAddr().String(), 200*time.Millisecond); err == nil {
		t.Error("listener still accepting after Stop")
	}
	// Idempotent.
	if err := srv.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestClientReconnectsAfterDrop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	c := &Client{Addr: ln.Addr().String(), ConnectTimeout: time.Second, WriteTimeout: time.Second}
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(ctx); err != nil { // no-op while connected
		t.Fatal(err)
	}
	first := <-accepted
	defer first.Close()
	if err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	c.Drop()
	if c.Connected() {
		t.Fatal("still connected after Drop")
	}
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	second := <-accepted
	defer second.Close()
	if second.RemoteAddr().String() == first.RemoteAddr().String() {
		t.Error("reconnect reused the dropped connection")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionInterruptsBlockedRead(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(io.Discard, c) // never answer
	}()
	c := &Client{Addr: ln.Addr().String(), ConnectTimeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	release := c.Session(ctx)
	defer release()
	clear := c.ReadDeadline(ctx, 30*time.Second)
	defer clear()
	start := time.Now()
	_, err = c.Reader().ReadByte()
	if err == nil {
		t.Fatal("read returned data from a silent peer")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("read was not interrupted: %v", time.Since(start))
	}
	if !errors.Is(CtxErr(ctx, err), context.DeadlineExceeded) {
		t.Errorf("CtxErr = %v, want deadline exceeded", CtxErr(ctx, err))
	}
}
