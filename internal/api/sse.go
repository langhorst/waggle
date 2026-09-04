package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/langhorst/waggle/internal/events"
)

const defaultMaxEventStreams = 64

// handleEvents streams the event bus over Server-Sent Events. With a {id}
// path segment present, only that channel's events pass (resyncs always
// pass — they mean "refetch", regardless of channel). Events carry IDs
// only; clients fetch details, which keeps fan-out cheap and overflow
// harmless.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	limit := s.MaxEventStreams
	if limit <= 0 {
		limit = defaultMaxEventStreams
	}
	if n := s.eventStreams.Add(1); n > int64(limit) {
		s.eventStreams.Add(-1)
		w.Header().Set("Retry-After", "5")
		s.writeError(w, http.StatusServiceUnavailable, fmt.Errorf("too many event streams (limit %d)", limit))
		return
	}
	defer s.eventStreams.Add(-1)
	channelFilter := r.PathValue("id")

	ch, cancel := s.Eng.Bus().Subscribe(64)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	// An immediate comment confirms the stream is live.
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		case ev, open := <-ch:
			if !open {
				return
			}
			if channelFilter != "" && ev.Type != events.TypeResync && ev.ChannelID != channelFilter {
				continue
			}
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		}
	}
}
