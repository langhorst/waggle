// Package api is the daemon's HTTP surface: a JSON REST API plus SSE event
// streams, shared by the embedded web UI and any external client. This is
// the full-control interface — channel lifecycle, message inspection,
// structural diffs, replay, DLQ requeue, and script editing.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/script"
	"github.com/langhorst/waggle/internal/store"
)

// Server wires the engine into HTTP handlers.
type Server struct {
	Eng *engine.Engine
	// Scripts enables the script read/write/reload endpoints (nil disables
	// them).
	Scripts *script.Engine
	// ScriptsRoot confines script file access: only files under this
	// directory are readable/writable via the API.
	ScriptsRoot string
	Log         *slog.Logger

	started time.Time
}

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	if s.Log == nil {
		s.Log = slog.Default()
	}
	s.started = time.Now()
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/channels", s.handleChannels)
	mux.HandleFunc("POST /api/channels/{id}/start", s.lifecycle("start"))
	mux.HandleFunc("POST /api/channels/{id}/stop", s.lifecycle("stop"))
	mux.HandleFunc("POST /api/channels/{id}/pause", s.lifecycle("pause"))
	mux.HandleFunc("POST /api/channels/{id}/reload", s.lifecycle("reload"))
	mux.HandleFunc("GET /api/channels/{id}/messages", s.handleMessages)
	mux.HandleFunc("GET /api/channels/{id}/dlq", s.handleDLQ)
	mux.HandleFunc("GET /api/channels/{id}/scripts", s.handleChannelScripts)
	mux.HandleFunc("GET /api/messages/{id}", s.handleMessage)
	mux.HandleFunc("GET /api/messages/{id}/tree", s.handleTree)
	mux.HandleFunc("GET /api/messages/{id}/diff", s.handleDiff)
	mux.HandleFunc("POST /api/messages/{id}/replay", s.handleReplay)
	mux.HandleFunc("POST /api/dlq/{id}/{dest}/requeue", s.handleRequeue)
	mux.HandleFunc("GET /api/scripts", s.handleScriptRead)
	mux.HandleFunc("PUT /api/scripts", s.handleScriptWrite)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/channels/{id}/events", s.handleEvents)

	s.registerWebUI(mux)
	return mux
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.Log.Error("encoding response", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, err error) {
	s.writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		s.writeError(w, http.StatusNotFound, err)
	default:
		s.writeError(w, http.StatusInternalServerError, err)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	channels := s.Eng.Channels()
	running := 0
	for _, ch := range channels {
		if ch.Status == "STARTED" {
			running++
		}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"uptimeSeconds":   int(time.Since(s.started).Seconds()),
		"channels":        len(channels),
		"channelsRunning": running,
	})
}

// channelInfo is the list payload: engine info enriched with counts.
type channelInfo struct {
	engine.Info
	Counts     map[message.State]int `json:"counts,omitempty"`
	QueueDepth map[string]int        `json:"queueDepth,omitempty"`
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	infos := s.Eng.Channels()
	out := make([]channelInfo, 0, len(infos))
	for _, info := range infos {
		ci := channelInfo{Info: info}
		if st := s.Eng.Store(); st != nil {
			if counts, err := st.MessageCounts(r.Context(), info.ID); err == nil {
				ci.Counts = counts
			}
			if depth, err := st.QueueDepth(r.Context(), info.ID); err == nil {
				ci.QueueDepth = depth
			}
		}
		out = append(out, ci)
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) lifecycle(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var err error
		switch action {
		case "start":
			err = s.Eng.Start(r.Context(), id)
		case "stop":
			err = s.Eng.Stop(id)
		case "pause":
			err = s.Eng.Pause(id)
		case "reload":
			err = s.Eng.ReloadChannel(r.Context(), id)
		}
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "unknown channel") {
				status = http.StatusNotFound
			}
			s.writeError(w, status, err)
			return
		}
		ch, _ := s.Eng.Channel(id)
		s.writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": string(ch.Status())})
	}
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	st := s.Eng.Store()
	if st == nil {
		s.writeError(w, http.StatusNotImplemented, errors.New("persistence disabled"))
		return
	}
	q := store.ListQuery{}
	if v := r.URL.Query().Get("limit"); v != "" {
		q.Limit, _ = strconv.Atoi(v)
	}
	if v := r.URL.Query().Get("before_id"); v != "" {
		q.BeforeID, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := r.URL.Query().Get("state"); v != "" {
		q.State = message.State(v)
	}
	list, err := st.ListMessages(r.Context(), r.PathValue("id"), q)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if list == nil {
		list = []store.MessageSummary{}
	}
	s.writeJSON(w, http.StatusOK, list)
}

// messagePayload extends the stored detail with printable payloads.
type messagePayload struct {
	*store.MessageDetail
	Raw         string `json:"raw"`
	Transformed string `json:"transformed,omitempty"`
}

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	st := s.Eng.Store()
	if st == nil {
		s.writeError(w, http.StatusNotImplemented, errors.New("persistence disabled"))
		return
	}
	detail, err := st.GetMessage(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, messagePayload{
		MessageDetail: detail,
		Raw:           string(detail.Raw),
		Transformed:   string(detail.Transformed),
	})
}

func (s *Server) handleTree(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	stage := r.URL.Query().Get("stage")
	tree, dt, err := s.Eng.MessageTree(r.Context(), id, stage)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, err)
		} else {
			s.writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"dataType": dt, "tree": tree})
}

func (s *Server) handleDiff(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	diff, err := s.Eng.MessageDiff(r.Context(), id, r.URL.Query().Get("dest"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, err)
		} else {
			s.writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	if diff == nil {
		diff = []message.DiffEntry{}
	}
	s.writeJSON(w, http.StatusOK, diff)
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	newID, err := s.Eng.Replay(r.Context(), id, r.URL.Query().Get("destination"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, err)
		} else {
			s.writeError(w, http.StatusConflict, err)
		}
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]int64{"replayId": newID})
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	st := s.Eng.Store()
	if st == nil {
		s.writeError(w, http.StatusNotImplemented, errors.New("persistence disabled"))
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	entries, err := st.DeadLetters(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if entries == nil {
		entries = []store.DLQEntry{}
	}
	s.writeJSON(w, http.StatusOK, entries)
}

func (s *Server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	st := s.Eng.Store()
	if st == nil {
		s.writeError(w, http.StatusNotImplemented, errors.New("persistence disabled"))
		return
	}
	if err := st.Requeue(r.Context(), id, r.PathValue("dest")); err != nil {
		s.writeError(w, http.StatusConflict, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "requeued"})
}

// scriptRef describes one script a channel references.
type scriptRef struct {
	Role string `json:"role"` // "filter", "transformer", "destination-filter", ...
	Path string `json:"path"` // resolved path, usable with /api/scripts
	// LastError is the most recent compile error ("" when healthy).
	LastError string `json:"lastError,omitempty"`
}

func (s *Server) handleChannelScripts(w http.ResponseWriter, r *http.Request) {
	cfg, ok := s.Eng.Config(r.PathValue("id"))
	if !ok {
		s.writeError(w, http.StatusNotFound, fmt.Errorf("unknown channel %q", r.PathValue("id")))
		return
	}
	var compileErrors map[string]string
	if s.Scripts != nil {
		compileErrors = s.Scripts.Scripts()
	}
	refs := []scriptRef{}
	add := func(role, p string) {
		if p == "" {
			return
		}
		resolved := cfg.ResolvePath(p)
		refs = append(refs, scriptRef{Role: role, Path: resolved, LastError: compileErrors[resolved]})
	}
	add("filter", cfg.Filter)
	for _, t := range cfg.Transformers {
		add("transformer", t)
	}
	for _, d := range cfg.Destinations {
		add("destination-filter:"+d.ID, d.Filter)
		for _, t := range d.Transformers {
			add("destination-transformer:"+d.ID, t)
		}
	}
	s.writeJSON(w, http.StatusOK, refs)
}

// confineScriptPath resolves p and ensures it stays inside ScriptsRoot.
func (s *Server) confineScriptPath(p string) (string, error) {
	if p == "" {
		return "", errors.New("path is required")
	}
	if s.ScriptsRoot == "" {
		return "", errors.New("script editing is not configured")
	}
	root, err := filepath.Abs(s.ScriptsRoot)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	if abs != root && !strings.HasPrefix(abs, root+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q is outside the configured scripts root", p)
	}
	return abs, nil
}

func (s *Server) handleScriptRead(w http.ResponseWriter, r *http.Request) {
	path, err := s.confineScriptPath(r.URL.Query().Get("path"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			s.writeError(w, http.StatusNotFound, err)
		} else {
			s.writeError(w, http.StatusInternalServerError, err)
		}
		return
	}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	_, _ = w.Write(raw)
}

func (s *Server) handleScriptWrite(w http.ResponseWriter, r *http.Request) {
	path, err := s.confineScriptPath(r.URL.Query().Get("path"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	body := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, readErr := r.Body.Read(buf)
		body = append(body, buf[:n]...)
		if len(body) > 1<<20 {
			s.writeError(w, http.StatusRequestEntityTooLarge, errors.New("script too large (1 MiB max)"))
			return
		}
		if readErr != nil {
			break
		}
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		s.writeError(w, http.StatusInternalServerError, err)
		return
	}
	// Recompile immediately so the editor gets compile feedback; a failed
	// compile keeps the previous program active in the pipeline.
	if s.Scripts != nil {
		if err := s.Scripts.Reload(path); err != nil && !strings.Contains(err.Error(), "not loaded") {
			s.writeJSON(w, http.StatusBadRequest, map[string]string{
				"status": "saved-with-errors",
				"error":  err.Error(),
			})
			return
		}
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}

func pathID(r *http.Request, key string) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue(key), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid message id %q", r.PathValue(key))
	}
	return id, nil
}
