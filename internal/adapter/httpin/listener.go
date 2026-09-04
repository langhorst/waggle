// Package httpin is the HTTP inbound Channel Adapter: Waggle exposes an
// endpoint and every POST/PUT body becomes a message. The HTTP response is
// the transport acknowledgment, with the same two modes as the MLLP
// listener: ackMode "immediate" answers 202 once the message is durably
// recorded (the Guaranteed Delivery handoff), "destination" holds the
// response for the pipeline outcome so script rejections and waitForAck
// destination failures surface as meaningful HTTP statuses.
package httpin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

func init() {
	adapter.RegisterInbound("http-listener", func(settings map[string]any) (adapter.Inbound, error) {
		return NewListener(settings)
	})
}

// Ack modes, matching the MLLP listener's semantics.
const (
	AckImmediate   = "immediate"
	AckDestination = "destination"
)

// ListenerConfig configures the HTTP listener.
type ListenerConfig struct {
	Listen  string `yaml:"listen"`
	Path    string `yaml:"path"`    // default "/"
	AckMode string `yaml:"ackMode"` // default immediate
	// HoldTimeout bounds how long a destination-mode response is held;
	// on expiry the caller gets 504. Default 30s.
	HoldTimeout adapter.Duration `yaml:"holdTimeout"`
	// ReadTimeout bounds reading one request. Default 30s.
	ReadTimeout adapter.Duration `yaml:"readTimeout"`
	// MaxBodySize bounds one message's bytes. Default 10 MiB.
	MaxBodySize int64 `yaml:"maxBodySize"`

	// CertFile/KeyFile enable TLS termination when both are set.
	CertFile string `yaml:"certFile"`
	KeyFile  string `yaml:"keyFile"`

	// BasicUser/BasicPass require HTTP basic credentials when set.
	BasicUser string `yaml:"basicUser"`
	BasicPass string `yaml:"basicPass"`
	// RequiredHeaders must all be present with exactly these values
	// (shared-secret style: X-Api-Key: ...).
	RequiredHeaders map[string]string `yaml:"requiredHeaders"`
}

// Listener is the HTTP inbound Channel Adapter.
type Listener struct {
	cfg ListenerConfig

	mu     sync.Mutex
	ln     net.Listener
	srv    *http.Server
	cancel context.CancelFunc
	done   chan struct{}
}

func NewListener(settings map[string]any) (*Listener, error) {
	cfg := ListenerConfig{
		Path:        "/",
		AckMode:     AckImmediate,
		HoldTimeout: adapter.Duration(30 * time.Second),
		ReadTimeout: adapter.Duration(30 * time.Second),
		MaxBodySize: 10 << 20,
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.Listen == "" {
		return nil, fmt.Errorf("http-listener: listen is required")
	}
	if cfg.AckMode != AckImmediate && cfg.AckMode != AckDestination {
		return nil, fmt.Errorf("http-listener: ackMode must be %q or %q", AckImmediate, AckDestination)
	}
	if !strings.HasPrefix(cfg.Path, "/") {
		cfg.Path = "/" + cfg.Path
	}
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = adapter.Duration(30 * time.Second)
	}
	if cfg.MaxBodySize <= 0 {
		cfg.MaxBodySize = 10 << 20
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, fmt.Errorf("http-listener: certFile and keyFile must be set together")
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
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", l.cfg.Listen)
	if err != nil {
		return fmt.Errorf("http-listener: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &http.Server{
		Handler:     http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { l.handle(runCtx, w, r, deliver) }),
		ReadTimeout: time.Duration(l.cfg.ReadTimeout),
		BaseContext: func(net.Listener) context.Context { return runCtx },
	}
	done := make(chan struct{})

	l.mu.Lock()
	l.ln, l.srv, l.cancel, l.done = ln, srv, cancel, done
	l.mu.Unlock()

	go func() {
		defer close(done)
		if l.cfg.CertFile != "" {
			_ = srv.ServeTLS(ln, l.cfg.CertFile, l.cfg.KeyFile)
			return
		}
		_ = srv.Serve(ln)
	}()
	return nil
}

func (l *Listener) Stop() error {
	l.mu.Lock()
	srv, cancel, done := l.srv, l.cancel, l.done
	l.ln, l.srv, l.cancel, l.done = nil, nil, nil, nil
	l.mu.Unlock()
	if srv == nil {
		return nil
	}
	// Cancel first so handlers holding a destination-mode response unblock,
	// then drain gracefully; Close catches anything that outlives the grace.
	cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	_ = srv.Close()
	<-done
	return nil
}

func (l *Listener) handle(ctx context.Context, w http.ResponseWriter, r *http.Request, deliver adapter.DeliverFunc) {
	if r.URL.Path != l.cfg.Path {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	if !l.authorized(r) {
		if l.cfg.BasicUser != "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="waggle"`)
		}
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "POST, PUT")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	body, err := readBody(w, r, l.cfg.MaxBodySize)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "body too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unreadable body"})
		return
	}
	if len(body) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "empty body"})
		return
	}

	meta := map[string]string{
		"source.remote": r.RemoteAddr,
		"http.method":   r.Method,
		"http.path":     r.URL.Path,
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		meta["http.header.content-type"] = ct
	}
	for name, vals := range r.URL.Query() {
		if len(vals) > 0 {
			meta["http.query."+name] = vals[0]
		}
	}

	rec, err := deliver(ctx, body, meta)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	if l.cfg.AckMode == AckImmediate {
		writeJSON(w, http.StatusAccepted, map[string]any{"messageId": rec.MessageID})
		return
	}
	timer := time.NewTimer(time.Duration(l.cfg.HoldTimeout))
	defer timer.Stop()
	select {
	case d := <-rec.Done:
		writeJSON(w, statusForAck(d.Code), map[string]any{
			"code": d.Code, "text": d.Text, "messageId": rec.MessageID,
		})
	case <-timer.C:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{
			"code": "AE", "text": "processing timed out", "messageId": rec.MessageID,
		})
	case <-ctx.Done():
	}
}

func (l *Listener) authorized(r *http.Request) bool {
	if l.cfg.BasicUser != "" {
		user, pass, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(user), []byte(l.cfg.BasicUser)) != 1 ||
			subtle.ConstantTimeCompare([]byte(pass), []byte(l.cfg.BasicPass)) != 1 {
			return false
		}
	}
	for name, want := range l.cfg.RequiredHeaders {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get(name)), []byte(want)) != 1 {
			return false
		}
	}
	return true
}

// statusForAck maps the pipeline's HL7-flavored AckDecision codes onto HTTP:
// accept → 200, reject (validation) → 400, error → 500.
func statusForAck(code string) int {
	switch code {
	case "AA", "CA":
		return http.StatusOK
	case "AR", "CR":
		return http.StatusBadRequest
	default: // AE, CE, anything unrecognized
		return http.StatusInternalServerError
	}
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
