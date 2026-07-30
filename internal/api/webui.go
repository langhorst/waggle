package api

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/langhorst/integration-channel/internal/engine"
	"github.com/langhorst/integration-channel/internal/message"
	"github.com/langhorst/integration-channel/internal/store"
)

//go:embed web/templates/*.html
var templateFS embed.FS

//go:embed web/static
var staticFS embed.FS

// Vendored frontend assets (fully offline): htmx 1.9.12, Flowbite 2.5.2,
// and the Tailwind browser runtime that Flowbite's utility classes need.

var templateFuncs = template.FuncMap{
	"stateBadge": func(s message.State) template.HTML {
		classes := map[message.State]string{
			message.StateReceived:    "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
			message.StateTransformed: "bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-300",
			message.StateQueued:      "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300",
			message.StateSent:        "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
			message.StateError:       "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300",
			message.StateFiltered:    "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300",
		}
		c, ok := classes[s]
		if !ok {
			c = "bg-gray-100 text-gray-800"
		}
		return template.HTML(fmt.Sprintf(
			`<span class="text-xs font-medium me-2 px-2.5 py-0.5 rounded %s">%s</span>`, c, s))
	},
	"statusBadge": func(s any) template.HTML {
		classes := map[string]string{
			"STARTED": "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
			"PAUSED":  "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300",
			"STOPPED": "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300",
		}
		str := fmt.Sprint(s)
		c, ok := classes[str]
		if !ok {
			c = "bg-gray-100 text-gray-800"
		}
		return template.HTML(fmt.Sprintf(
			`<span class="text-xs font-medium me-2 px-2.5 py-0.5 rounded %s">%s</span>`, c, str))
	},
	"timefmt": func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.Format("2006-01-02 15:04:05.000")
	},
	"sum": func(m map[string]int) int {
		total := 0
		for _, v := range m {
			total += v
		}
		return total
	},
	"count": func(m map[message.State]int, states ...string) int {
		total := 0
		for _, s := range states {
			total += m[message.State(s)]
		}
		return total
	},
}

// pages maps a page name to its parsed template set (layout + page).
var pages = func() map[string]*template.Template {
	partials := []string{
		"web/templates/_channels.html",
		"web/templates/_messages.html",
		"web/templates/_tree.html",
		"web/templates/_diff.html",
		"web/templates/_destinations.html",
		"web/templates/_dlq.html",
	}
	out := map[string]*template.Template{}
	for _, page := range []string{"dashboard", "channel", "message", "dlq", "scripts", "editor"} {
		files := append([]string{"web/templates/layout.html", "web/templates/" + page + ".html"}, partials...)
		out[page] = template.Must(template.New("layout.html").Funcs(templateFuncs).ParseFS(templateFS, files...))
	}
	// Partials render standalone for HTMX swaps.
	for _, partial := range []string{"_channels", "_messages", "_tree", "_diff", "_destinations", "_dlq"} {
		out[partial] = template.Must(template.New(partial+".html").Funcs(templateFuncs).
			ParseFS(templateFS, "web/templates/"+partial+".html"))
	}
	return out
}()

func (s *Server) render(w http.ResponseWriter, page, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pages[page].ExecuteTemplate(w, name, data); err != nil {
		s.Log.Error("rendering template", "page", page, "error", err)
	}
}

func (s *Server) renderPage(w http.ResponseWriter, page string, data any) {
	s.render(w, page, "layout.html", data)
}

// registerWebUI mounts the embedded web UI onto mux.
func (s *Server) registerWebUI(mux *http.ServeMux) {
	static, _ := fs.Sub(staticFS, "web/static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))

	mux.HandleFunc("GET /{$}", s.pageDashboard)
	mux.HandleFunc("GET /channels/{id}", s.pageChannel)
	mux.HandleFunc("GET /channels/{id}/dlq", s.pageDLQ)
	mux.HandleFunc("GET /channels/{id}/scripts", s.pageScripts)
	mux.HandleFunc("GET /messages/{id}", s.pageMessage)
	mux.HandleFunc("GET /scripts/edit", s.pageEditor)

	// HTMX partials + actions (actions return refreshed partials).
	mux.HandleFunc("GET /ui/channels", s.partialChannels)
	mux.HandleFunc("POST /ui/channels/{id}/{action}", s.actionChannel)
	mux.HandleFunc("GET /ui/channels/{id}/messages", s.partialMessages)
	mux.HandleFunc("GET /ui/channels/{id}/dlq", s.partialDLQ)
	mux.HandleFunc("POST /ui/dlq/{id}/{dest}/requeue", s.actionRequeue)
	mux.HandleFunc("GET /ui/messages/{id}/tree", s.partialTree)
	mux.HandleFunc("GET /ui/messages/{id}/diff", s.partialDiff)
	mux.HandleFunc("GET /ui/messages/{id}/destinations", s.partialDestinations)
	mux.HandleFunc("POST /ui/messages/{id}/replay", s.actionReplay)
}

func (s *Server) channelInfos(ctx context.Context) []channelInfo {
	infos := s.Eng.Channels()
	out := make([]channelInfo, 0, len(infos))
	for _, info := range infos {
		ci := channelInfo{Info: info}
		if st := s.Eng.Store(); st != nil {
			if counts, err := st.MessageCounts(ctx, info.ID); err == nil {
				ci.Counts = counts
			}
			if depth, err := st.QueueDepth(ctx, info.ID); err == nil {
				ci.QueueDepth = depth
			}
		}
		out = append(out, ci)
	}
	return out
}

func (s *Server) pageDashboard(w http.ResponseWriter, r *http.Request) {
	s.renderPage(w, "dashboard", map[string]any{
		"Title":    "Channels",
		"Channels": s.channelInfos(r.Context()),
	})
}

func (s *Server) partialChannels(w http.ResponseWriter, r *http.Request) {
	s.render(w, "_channels", "_channels.html", map[string]any{
		"Channels": s.channelInfos(r.Context()),
	})
}

func (s *Server) actionChannel(w http.ResponseWriter, r *http.Request) {
	id, action := r.PathValue("id"), r.PathValue("action")
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
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	if err != nil {
		s.Log.Warn("channel action failed", "channel", id, "action", action, "error", err)
	}
	s.partialChannels(w, r)
}

type messagesQuery struct {
	ChannelID string
	State     string
	BeforeID  int64
}

func (s *Server) pageChannel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.Eng.Channel(id); !ok {
		http.NotFound(w, r)
		return
	}
	s.renderPage(w, "channel", map[string]any{
		"Title":     "Channel " + id,
		"ChannelID": id,
		"State":     r.URL.Query().Get("state"),
	})
}

func (s *Server) partialMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := store.ListQuery{Limit: 100, State: message.State(r.URL.Query().Get("state"))}
	if v := r.URL.Query().Get("before_id"); v != "" {
		q.BeforeID, _ = strconv.ParseInt(v, 10, 64)
	}
	var list []store.MessageSummary
	if st := s.Eng.Store(); st != nil {
		list, _ = st.ListMessages(r.Context(), id, q)
	}
	s.render(w, "_messages", "_messages.html", map[string]any{
		"ChannelID": id,
		"Messages":  list,
		"State":     r.URL.Query().Get("state"),
	})
}

func (s *Server) pageMessage(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	st := s.Eng.Store()
	if st == nil {
		http.Error(w, "persistence disabled", http.StatusNotImplemented)
		return
	}
	detail, err := st.GetMessage(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	stages := []string{engine.StageReceived}
	if len(detail.Transformed) > 0 {
		stages = append(stages, engine.StageTransformed)
	}
	for _, d := range detail.Destinations {
		stages = append(stages, "dest:"+d.DestinationID)
	}
	s.renderPage(w, "message", map[string]any{
		"Title":       fmt.Sprintf("Message #%d", id),
		"Detail":      detail,
		"Raw":         string(detail.Raw),
		"Transformed": string(detail.Transformed),
		"Stages":      stages,
	})
}

func (s *Server) actionReplay(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	newID, err := s.Eng.Replay(r.Context(), id, r.URL.Query().Get("destination"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err != nil {
		fmt.Fprintf(w, `<span class="text-sm text-red-600 dark:text-red-400">replay failed: %s</span>`,
			template.HTMLEscapeString(err.Error()))
		return
	}
	if newID != 0 {
		fmt.Fprintf(w, `<span class="text-sm text-green-600 dark:text-green-400">replayed as <a class="underline" href="/messages/%d">#%d</a></span>`, newID, newID)
		return
	}
	fmt.Fprint(w, `<span class="text-sm text-green-600 dark:text-green-400">requeued</span>`)
}

func (s *Server) partialTree(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stage := r.URL.Query().Get("stage")
	tree, dt, err := s.Eng.MessageTree(r.Context(), id, stage)
	data := map[string]any{"DataType": dt, "Tree": tree, "Error": ""}
	if err != nil {
		data["Error"] = err.Error()
	}
	s.render(w, "_tree", "_tree.html", data)
}

func (s *Server) partialDiff(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	diff, err := s.Eng.MessageDiff(r.Context(), id, r.URL.Query().Get("dest"))
	data := map[string]any{"Diff": diff, "Error": ""}
	if err != nil {
		data["Error"] = err.Error()
	}
	s.render(w, "_diff", "_diff.html", data)
}

func (s *Server) partialDestinations(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var dests []store.DestinationStatus
	if st := s.Eng.Store(); st != nil {
		if detail, err := st.GetMessage(r.Context(), id); err == nil {
			dests = detail.Destinations
		}
	}
	s.render(w, "_destinations", "_destinations.html", map[string]any{
		"MessageID":    id,
		"Destinations": dests,
	})
}

func (s *Server) pageDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.renderPage(w, "dlq", map[string]any{
		"Title":     "Dead Letters — " + id,
		"ChannelID": id,
	})
}

func (s *Server) partialDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var entries []store.DLQEntry
	if st := s.Eng.Store(); st != nil {
		entries, _ = st.DeadLetters(r.Context(), id, 100)
	}
	s.render(w, "_dlq", "_dlq.html", map[string]any{
		"ChannelID": id,
		"Entries":   entries,
	})
}

func (s *Server) actionRequeue(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if st := s.Eng.Store(); st != nil {
		if err := st.Requeue(r.Context(), id, r.PathValue("dest")); err != nil {
			s.Log.Warn("requeue failed", "message", id, "error", err)
		}
	}
	// Return the refreshed table; the channel id comes from the referer
	// query the partial passes along.
	r.SetPathValue("id", r.URL.Query().Get("channel"))
	s.partialDLQ(w, r)
}

func (s *Server) pageScripts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cfg, ok := s.Eng.Config(id)
	if !ok {
		http.NotFound(w, r)
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
	s.renderPage(w, "scripts", map[string]any{
		"Title":     "Scripts — " + id,
		"ChannelID": id,
		"Refs":      refs,
	})
}

func (s *Server) pageEditor(w http.ResponseWriter, r *http.Request) {
	path, err := s.confineScriptPath(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	s.renderPage(w, "editor", map[string]any{
		"Title":   "Edit script",
		"Path":    path,
		"Content": string(raw),
	})
}
