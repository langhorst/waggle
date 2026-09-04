// Package httpout is the HTTP outbound Channel Adapter: each delivery is one
// request to an external API. Failure classification drives the Guaranteed
// Delivery machinery — network errors, 408, 429, and 5xx are transient
// (retry with backoff), any other non-2xx answer is Permanent (the API
// rejected this message; straight to the Dead Letter Channel, with the
// response body preserved in the error so the DLQ shows why).
//
// Transformer scripts route per message through meta: meta['http.method']
// overrides the configured method and meta['http.path'] is resolved against
// the configured URL (absolute paths replace, relative paths append, query
// strings carry over).
package httpout

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/langhorst/waggle/internal/adapter"
)

func init() {
	adapter.RegisterOutbound("http-sender", func(settings map[string]any) (adapter.Outbound, error) {
		return NewSender(settings)
	})
}

// Meta keys scripts set to route a single message.
const (
	MetaMethod = "http.method"
	MetaPath   = "http.path"
)

// SenderConfig configures the HTTP sender.
type SenderConfig struct {
	URL         string            `yaml:"url"`
	Method      string            `yaml:"method"`      // default POST
	ContentType string            `yaml:"contentType"` // default application/json
	Headers     map[string]string `yaml:"headers"`     // static, e.g. Authorization
	BasicUser   string            `yaml:"basicUser"`
	BasicPass   string            `yaml:"basicPass"`
	Timeout     adapter.Duration  `yaml:"timeout"` // per request, default 30s
	// CAFile adds a PEM trust root for the target's TLS certificate
	// (self-signed or private CA); system roots stay valid.
	CAFile string `yaml:"caFile"`
}

// Sender is the HTTP outbound Channel Adapter.
type Sender struct {
	cfg    SenderConfig
	base   *url.URL
	client *http.Client
}

func NewSender(settings map[string]any) (*Sender, error) {
	cfg := SenderConfig{
		Method:      http.MethodPost,
		ContentType: "application/json",
		Timeout:     adapter.Duration(30 * time.Second),
	}
	if err := adapter.DecodeSettings(settings, &cfg); err != nil {
		return nil, err
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("http-sender: url is required")
	}
	base, err := url.Parse(cfg.URL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, fmt.Errorf("http-sender: invalid url %q", cfg.URL)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = adapter.Duration(30 * time.Second)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.CAFile != "" {
		pemBytes, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("http-sender: caFile: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("http-sender: caFile %s contains no certificates", cfg.CAFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &Sender{
		cfg:  cfg,
		base: base,
		client: &http.Client{
			Timeout:   time.Duration(cfg.Timeout),
			Transport: transport,
		},
	}, nil
}

func (s *Sender) Open(ctx context.Context) error { return nil }
func (s *Sender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

func (s *Sender) Send(ctx context.Context, payload []byte, meta map[string]string) error {
	target := s.base
	if p := meta[MetaPath]; p != "" {
		ref, err := url.Parse(p)
		if err != nil {
			// A script wrote an unparseable path; retrying cannot fix it.
			return adapter.Permanent(fmt.Errorf("http-sender: invalid %s %q: %w", MetaPath, p, err))
		}
		target = s.base.ResolveReference(ref)
	}
	method := s.cfg.Method
	if m := meta[MetaMethod]; m != "" {
		method = strings.ToUpper(m)
	}

	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(payload))
	if err != nil {
		return adapter.Permanent(fmt.Errorf("http-sender: %w", err))
	}
	req.Header.Set("Content-Type", s.cfg.ContentType)
	for k, v := range s.cfg.Headers {
		req.Header.Set(k, v)
	}
	if s.cfg.BasicUser != "" {
		req.SetBasicAuth(s.cfg.BasicUser, s.cfg.BasicPass)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http-sender: %s %s: %w", method, target, err) // transient
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body) // drain for connection reuse
		return nil
	}

	detail := fmt.Sprintf("http-sender: %s %s: %s%s", method, target, resp.Status, bodySnippet(resp.Body))
	if retryable(resp.StatusCode) {
		return fmt.Errorf("%s", detail)
	}
	return adapter.Permanent(fmt.Errorf("%s", detail))
}

// retryable reports whether a status is worth retrying: server-side trouble
// and throttling, but not application rejections.
func retryable(status int) bool {
	return status >= 500 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests
}

// bodySnippet renders the start of an error response for diagnostics.
func bodySnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 512))
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	return ": " + s
}
