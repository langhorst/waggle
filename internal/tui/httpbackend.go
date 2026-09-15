package tui

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

// HTTPBackend observes a daemon through its HTTP API and SSE event stream.
type HTTPBackend struct {
	// BaseURL is the daemon's API root, e.g. "http://127.0.0.1:8420".
	BaseURL string
	// Token is sent as a Bearer token; empty when the daemon has auth
	// disabled.
	Token string
	// Client defaults to one with a 15s timeout. The event stream uses a
	// separate client without a timeout.
	Client *http.Client
}

func (b *HTTPBackend) client() *http.Client {
	if b.Client != nil {
		return b.Client
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (b *HTTPBackend) newRequest(ctx context.Context, path string, query url.Values) (*http.Request, error) {
	u := strings.TrimRight(b.BaseURL, "/") + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if b.Token != "" {
		req.Header.Set("Authorization", "Bearer "+b.Token)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// get performs one JSON GET and decodes the body into out.
func (b *HTTPBackend) get(ctx context.Context, path string, query url.Values, out any) error {
	req, err := b.newRequest(ctx, path, query)
	if err != nil {
		return err
	}
	resp, err := b.client().Do(req)
	if err != nil {
		return fmt.Errorf("daemon unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return apiError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%s: decoding response: %w", path, err)
	}
	return nil
}

// apiError turns a non-2xx response into an error carrying the API's own
// message when it sent one.
func apiError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var payload struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &payload) == nil && payload.Error != "" {
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: %s", store.ErrNotFound, payload.Error)
		}
		return fmt.Errorf("daemon: %s (%s)", payload.Error, resp.Status)
	}
	return fmt.Errorf("daemon: %s", resp.Status)
}

func (b *HTTPBackend) ChannelSummaries(ctx context.Context) ([]engine.ChannelSummary, error) {
	var out []engine.ChannelSummary
	if err := b.get(ctx, "/api/channels", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (b *HTTPBackend) ChannelSummary(ctx context.Context, channelID string) (engine.ChannelSummary, error) {
	var out engine.ChannelSummary
	err := b.get(ctx, "/api/channels/"+url.PathEscape(channelID), nil, &out)
	return out, err
}

func (b *HTTPBackend) ListMessages(ctx context.Context, channelID string, q store.ListQuery) ([]store.MessageSummary, error) {
	query := url.Values{}
	if q.Limit > 0 {
		query.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.BeforeID > 0 {
		query.Set("before_id", strconv.FormatInt(q.BeforeID, 10))
	}
	if q.State != "" {
		query.Set("state", string(q.State))
	}
	var out []store.MessageSummary
	if err := b.get(ctx, "/api/channels/"+url.PathEscape(channelID)+"/messages", query, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// messagePayload mirrors the API's message document: the stored detail
// plus the raw and transformed payloads as strings.
type messagePayload struct {
	store.MessageDetail
	Raw         string `json:"raw"`
	Transformed string `json:"transformed"`
}

func (b *HTTPBackend) GetMessage(ctx context.Context, id int64) (*store.MessageDetail, error) {
	var payload messagePayload
	if err := b.get(ctx, "/api/messages/"+strconv.FormatInt(id, 10), nil, &payload); err != nil {
		return nil, err
	}
	detail := payload.MessageDetail
	detail.Raw = []byte(payload.Raw)
	detail.Transformed = []byte(payload.Transformed)
	return &detail, nil
}

func (b *HTTPBackend) MessageTree(ctx context.Context, id int64, stage string) (*message.Node, string, error) {
	query := url.Values{}
	if stage != "" {
		query.Set("stage", stage)
	}
	var out struct {
		DataType string        `json:"dataType"`
		Tree     *message.Node `json:"tree"`
	}
	if err := b.get(ctx, "/api/messages/"+strconv.FormatInt(id, 10)+"/tree", query, &out); err != nil {
		return nil, "", err
	}
	return out.Tree, out.DataType, nil
}

func (b *HTTPBackend) MessageDiff(ctx context.Context, id int64, destID string) ([]message.DiffEntry, error) {
	query := url.Values{}
	if destID != "" {
		query.Set("dest", destID)
	}
	var out []message.DiffEntry
	if err := b.get(ctx, "/api/messages/"+strconv.FormatInt(id, 10)+"/diff", query, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Subscribe follows the daemon's SSE stream. A dropped stream reconnects
// with backoff and delivers a Resync event first, since events were missed
// in between; a subscriber that falls behind also gets a Resync (the
// event's own semantics) rather than blocking the reader.
func (b *HTTPBackend) Subscribe(buf int) (<-chan events.Event, func()) {
	if buf < 1 {
		buf = 1
	}
	ch := make(chan events.Event, buf)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(ch)
		b.streamEvents(ctx, ch)
	}()
	return ch, func() {
		cancel()
		<-done
	}
}

func (b *HTTPBackend) streamEvents(ctx context.Context, ch chan<- events.Event) {
	client := &http.Client{} // no timeout: the stream is long-lived
	if b.Client != nil && b.Client.Transport != nil {
		client.Transport = b.Client.Transport
	}
	backoff := time.Second
	first := true
	for ctx.Err() == nil {
		if !first {
			// Reconnected: whatever happened meanwhile was missed.
			if !offer(ctx, ch, events.Event{Type: events.TypeResync}) {
				return
			}
		}
		first = false
		err := b.readStream(ctx, client, ch)
		if ctx.Err() != nil {
			return
		}
		_ = err // the next Resync tells the UI to refetch; nothing else to do
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// readStream consumes one SSE connection until it ends.
func (b *HTTPBackend) readStream(ctx context.Context, client *http.Client, ch chan<- events.Event) error {
	req, err := b.newRequest(ctx, "/api/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return apiError(resp)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var data strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			// Blank line ends one event.
			if data.Len() > 0 {
				var ev events.Event
				if json.Unmarshal([]byte(data.String()), &ev) == nil {
					if !offer(ctx, ch, ev) {
						return nil
					}
				}
				data.Reset()
			}
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		default:
			// "event:" names and ": comment" heartbeats carry nothing the
			// JSON does not.
		}
	}
	return scanner.Err()
}

// offer hands ev to the subscriber without ever blocking the stream
// reader: a full buffer drops the event and marks a Resync owed, exactly
// as the in-process bus does. It returns false when ctx ended.
func offer(ctx context.Context, ch chan<- events.Event, ev events.Event) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- ev:
		return true
	default:
	}
	// Full: replace the pending tail with a resync by draining one slot.
	select {
	case <-ctx.Done():
		return false
	case ch <- events.Event{Type: events.TypeResync}:
	default:
	}
	return true
}
