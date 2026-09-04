// Package api is the daemon's HTTP surface: a JSON REST API plus SSE event
// streams, shared by the embedded web UI and any external client. This is
// the full-control interface — channel lifecycle, message inspection,
// structural diffs, replay, DLQ requeue, and script editing.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
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
	// Auth is the credential policy every route is checked against.
	Auth AuthConfig
	// MaxEventStreams caps concurrent SSE subscribers; each one costs a
	// goroutine, a bus buffer, and a heartbeat timer. Default 64.
	MaxEventStreams int
	Log             *slog.Logger

	started      time.Time
	eventStreams atomic.Int64
}

// Limits on list endpoints. The store defaults limit to 50 when unset;
// the API additionally caps it so a client cannot ask for the whole table.
const (
	defaultListLimit = 50
	maxListLimit     = 500
)

// responseDeadline bounds how long a non-streaming handler may take to
// write its response. SSE handlers are exempt (see protect).
const responseDeadline = 30 * time.Second

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
	return s.protect(mux)
}

var (
	errUnauthorized = errors.New("authentication required")
	errCrossSite    = errors.New("cross-site request rejected")
)

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

// writeInternalError logs the real error and answers with a generic
// message: internal errors carry filesystem paths and SQL detail that
// belong in the log, not in a response body.
func (s *Server) writeInternalError(w http.ResponseWriter, what string, err error) {
	s.Log.Error(what, "error", err)
	s.writeError(w, http.StatusInternalServerError, errors.New(what+" failed"))
}

// writeEngineError maps engine errors onto status codes.
func (s *Server) writeEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrUnknownChannel):
		s.writeError(w, http.StatusNotFound, err)
	case errors.Is(err, engine.ErrUnknownAction):
		s.writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, store.ErrNotFound):
		s.writeError(w, http.StatusNotFound, err)
	default:
		// Lifecycle failures (a port in use, a broken script) are the
		// caller's business to see.
		s.writeError(w, http.StatusInternalServerError, err)
	}
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		s.writeError(w, http.StatusNotFound, err)
	default:
		s.writeInternalError(w, "store query", err)
	}
}

// queryLimit parses the limit query parameter, defaulting and clamping it.
func queryLimit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultListLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid limit %q", v)
	}
	return min(n, maxListLimit), nil
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

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	out, err := s.Eng.ChannelSummaries(r.Context())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) lifecycle(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if err := s.Eng.Lifecycle(r.Context(), id, action); err != nil {
			s.writeEngineError(w, err)
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
	limit, err := queryLimit(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	q := store.ListQuery{Limit: limit}
	if v := r.URL.Query().Get("before_id"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil || id < 0 {
			s.writeError(w, http.StatusBadRequest, fmt.Errorf("invalid before_id %q", v))
			return
		}
		q.BeforeID = id
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
	id, err := pathID(r)
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
	id, err := pathID(r)
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
	id, err := pathID(r)
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
	id, err := pathID(r)
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
	limit, err := queryLimit(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
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
	id, err := pathID(r)
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

func (s *Server) handleChannelScripts(w http.ResponseWriter, r *http.Request) {
	refs, err := s.Eng.ScriptRefs(r.PathValue("id"))
	if err != nil {
		s.writeEngineError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, refs)
}

// errScriptPath is the one error the script endpoints return for a path
// they will not serve. The reason is logged, never echoed: the specific
// check that failed is of no use to a legitimate editor and of some use
// to anyone probing the filesystem.
var errScriptPath = errors.New("path is not an editable script of a loaded channel")

// confineScriptPath resolves p and decides whether the script endpoints may
// touch it. A path qualifies only when every check holds:
//
//   - it is a .js file that a loaded channel references (the editor only
//     ever links those; ScriptsRoot also holds the channel YAML, which
//     must stay out of reach of an endpoint that can rewrite files);
//   - after resolving symlinks it still lives under ScriptsRoot.
//
// The returned path is absolute with symlinks resolved, so reads and writes
// land on the real file.
func (s *Server) confineScriptPath(p string) (string, error) {
	real, err := s.resolveScriptPath(p)
	if err != nil {
		s.Log.Warn("script path rejected", "path", p, "reason", err)
		return "", errScriptPath
	}
	return real, nil
}

func (s *Server) resolveScriptPath(p string) (string, error) {
	if p == "" {
		return "", errors.New("path is required")
	}
	if s.ScriptsRoot == "" {
		return "", errors.New("script editing is not configured")
	}
	if strings.ToLower(filepath.Ext(p)) != ".js" {
		return "", errors.New("not a .js file")
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	if !s.isReferencedScript(abs) {
		return "", errors.New("not referenced by any loaded channel")
	}
	root, err := filepath.EvalSymlinks(s.ScriptsRoot)
	if err != nil {
		return "", fmt.Errorf("scripts root: %w", err)
	}
	if root, err = filepath.Abs(root); err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(real, root+string(filepath.Separator)) {
		return "", fmt.Errorf("resolves to %s, outside the scripts root", real)
	}
	return real, nil
}

// isReferencedScript reports whether abs is one of the script files a
// loaded channel configuration points at.
func (s *Server) isReferencedScript(abs string) bool {
	for _, info := range s.Eng.Channels() {
		cfg, ok := s.Eng.Config(info.ID)
		if !ok {
			continue
		}
		for _, sp := range cfg.ScriptPaths() {
			if resolved, err := filepath.Abs(sp); err == nil && resolved == abs {
				return true
			}
		}
	}
	return false
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
			s.writeError(w, http.StatusNotFound, errors.New("script not found"))
		} else {
			s.writeInternalError(w, "reading script", err)
		}
		return
	}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	_, _ = w.Write(raw)
}

// maxScriptSize bounds a PUT /api/scripts body.
const maxScriptSize = 1 << 20

func (s *Server) handleScriptWrite(w http.ResponseWriter, r *http.Request) {
	path, err := s.confineScriptPath(r.URL.Query().Get("path"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxScriptSize))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			s.writeError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("script too large (%d bytes max)", maxScriptSize))
			return
		}
		// A truncated upload must never reach the file: the hot-reload
		// watcher would compile the fragment.
		s.writeError(w, http.StatusBadRequest, fmt.Errorf("reading script body: %w", err))
		return
	}
	if err := writeFileAtomic(path, body, 0o644); err != nil {
		s.writeInternalError(w, "writing script", err)
		return
	}
	// Recompile immediately so the editor gets compile feedback; a failed
	// compile keeps the previous program active in the pipeline.
	if s.Scripts != nil {
		if err := s.Scripts.Reload(path); err != nil && !errors.Is(err, script.ErrNotLoaded) {
			s.writeJSON(w, http.StatusBadRequest, map[string]string{
				"status": "saved-with-errors",
				"error":  err.Error(),
			})
			return
		}
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}

// writeFileAtomic replaces path's contents via a temp file and rename so no
// reader (the script hot-reload watcher included) ever sees a partial
// write.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid message id %q", r.PathValue("id"))
	}
	return id, nil
}
