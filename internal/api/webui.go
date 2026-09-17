package api

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

//go:embed web/templates/*.html
var templateFS embed.FS

//go:embed web/static
var staticFS embed.FS

// Vendored frontend assets (fully offline): htmx 1.9.12, Flowbite 2.5.2,
// the Tailwind browser runtime that Flowbite's utility classes need, and
// two files of our own -- app.css, the stylesheet entry point, and
// codeedit.{js,css}, the script editor's JavaScript highlighter, written
// rather than vendored so the UI keeps needing no network.
//
// Pages link app.css, never flowbite.min.css directly: the two vendored
// stylesheets disagree about cascade layers (a v3 build that is entirely
// unlayered against a v4 runtime that layers everything), and app.css is
// what reconciles them. Its own comment has the details; the short version
// is that linking Flowbite directly silently disables every dark: variant
// and every text-*/font-* utility on a form control.

// badgeTmpl renders a state/status pill; going through html/template keeps
// the label contextually escaped even if it ever stops being a constant.
var badgeTmpl = template.Must(template.New("badge").Parse(
	`<span class="text-xs font-medium me-2 px-2.5 py-0.5 rounded {{.Class}}">{{.Label}}</span>`))

var (
	stateBadgeClasses = map[string]string{
		string(message.StateReceived):    "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
		string(message.StateTransformed): "bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-300",
		string(message.StateQueued):      "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300",
		string(message.StateSent):        "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
		string(message.StateError):       "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300",
		string(message.StateFiltered):    "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300",
	}
	statusBadgeClasses = map[string]string{
		"STARTED": "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
		"PAUSED":  "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300",
		"STOPPED": "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300",
	}
)

func badge(classes map[string]string, label string) template.HTML {
	c, ok := classes[label]
	if !ok {
		c = "bg-gray-100 text-gray-800"
	}
	var buf bytes.Buffer
	if err := badgeTmpl.Execute(&buf, struct{ Class, Label string }{c, label}); err != nil {
		return template.HTML(template.HTMLEscapeString(label)) //nolint:gosec // escaped just above
	}
	return template.HTML(buf.String()) //nolint:gosec // rendered by html/template
}

var templateFuncs = template.FuncMap{
	"stateBadge":  func(s message.State) template.HTML { return badge(stateBadgeClasses, string(s)) },
	"statusBadge": func(s any) template.HTML { return badge(statusBadgeClasses, fmt.Sprint(s)) },
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

// render executes a template into a buffer first: a template error then
// produces a clean 500 instead of a truncated 200 with the error logged
// and the client none the wiser.
func (s *Server) render(w http.ResponseWriter, page, name string, data any) {
	var buf bytes.Buffer
	if err := pages[page].ExecuteTemplate(&buf, name, data); err != nil {
		s.Log.Error("rendering template", "page", page, "error", err)
		http.Error(w, "rendering page failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
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

func (s *Server) pageDashboard(w http.ResponseWriter, r *http.Request) {
	channels, err := s.Eng.ChannelSummaries(r.Context())
	s.renderPage(w, "dashboard", map[string]any{
		"Title":    "Channels",
		"Channels": channels,
		"Error":    errText(err),
	})
}

func (s *Server) partialChannels(w http.ResponseWriter, r *http.Request) {
	s.renderChannels(w, r.Context(), "")
}

// renderChannels renders the channel table, with an operator-facing error
// line when an action or the store failed.
func (s *Server) renderChannels(w http.ResponseWriter, ctx context.Context, errMsg string) {
	channels, err := s.Eng.ChannelSummaries(ctx)
	if err != nil && errMsg == "" {
		errMsg = errText(err)
	}
	s.render(w, "_channels", "_channels.html", map[string]any{
		"Channels": channels,
		"Error":    errMsg,
	})
}

func (s *Server) actionChannel(w http.ResponseWriter, r *http.Request) {
	id, action := r.PathValue("id"), r.PathValue("action")
	err := s.Eng.Lifecycle(r.Context(), id, action)
	switch {
	case errors.Is(err, engine.ErrUnknownAction):
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	case err != nil:
		// The operator clicked a button; the outcome belongs on screen,
		// not only in the log.
		s.Log.Warn("channel action failed", "channel", id, "action", action, "error", err)
		s.renderChannels(w, r.Context(), fmt.Sprintf("%s %s failed: %v", action, id, err))
		return
	}
	s.renderChannels(w, r.Context(), "")
}

// errText renders an error for a template, "" for nil.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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

// webPageSize is how many messages one page of the web UI lists.
const webPageSize = 100

func (s *Server) partialMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := store.ListQuery{Limit: webPageSize, State: message.State(r.URL.Query().Get("state"))}
	if v := r.URL.Query().Get("before_id"); v != "" {
		q.BeforeID, _ = strconv.ParseInt(v, 10, 64)
	}
	var (
		list   []store.MessageSummary
		errMsg string
	)
	if st := s.Eng.Store(); st != nil {
		var err error
		if list, err = st.ListMessages(r.Context(), id, q); err != nil {
			s.Log.Error("listing messages", "channel", id, "error", err)
			errMsg = "loading messages failed"
		}
	} else {
		errMsg = "persistence disabled"
	}
	// A full page means there may be older messages; the cursor is the
	// oldest id shown.
	var nextBefore int64
	if len(list) == webPageSize {
		nextBefore = list[len(list)-1].ID
	}
	s.render(w, "_messages", "_messages.html", map[string]any{
		"ChannelID":  id,
		"Messages":   list,
		"State":      r.URL.Query().Get("state"),
		"Error":      errMsg,
		"NextBefore": nextBefore,
	})
}

func (s *Server) pageMessage(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
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
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		s.Log.Error("loading message", "message", id, "error", err)
		http.Error(w, "loading message failed", http.StatusInternalServerError)
		return
	}
	s.renderPage(w, "message", map[string]any{
		"Title":       fmt.Sprintf("Message #%d", id),
		"Detail":      detail,
		"Raw":         string(detail.Raw),
		"Transformed": string(detail.Transformed),
		"Stages":      engine.Stages(detail),
	})
}

func (s *Server) actionReplay(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
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
	id, err := pathID(r)
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
	id, err := pathID(r)
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
	id, err := pathID(r)
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
	if _, ok := s.Eng.Channel(id); !ok {
		http.NotFound(w, r)
		return
	}
	s.renderPage(w, "dlq", map[string]any{
		"Title":     "Dead Letters — " + id,
		"ChannelID": id,
	})
}

func (s *Server) partialDLQ(w http.ResponseWriter, r *http.Request) {
	s.renderDLQ(w, r.Context(), r.PathValue("id"), "")
}

func (s *Server) renderDLQ(w http.ResponseWriter, ctx context.Context, channelID, errMsg string) {
	var entries []store.DLQEntry
	if st := s.Eng.Store(); st != nil {
		var err error
		if entries, err = st.DeadLetters(ctx, channelID, webPageSize); err != nil {
			s.Log.Error("listing dead letters", "channel", channelID, "error", err)
			if errMsg == "" {
				errMsg = "loading dead letters failed"
			}
		}
	} else if errMsg == "" {
		errMsg = "persistence disabled"
	}
	s.render(w, "_dlq", "_dlq.html", map[string]any{
		"ChannelID": channelID,
		"Entries":   entries,
		"Error":     errMsg,
	})
}

func (s *Server) actionRequeue(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The refreshed table needs the channel, which the DLQ partial passes
	// along as a query parameter.
	channelID := r.URL.Query().Get("channel")
	var errMsg string
	if st := s.Eng.Store(); st != nil {
		if err := st.Requeue(r.Context(), id, r.PathValue("dest")); err != nil {
			s.Log.Warn("requeue failed", "message", id, "error", err)
			errMsg = fmt.Sprintf("requeue of #%d failed: %v", id, err)
		}
	}
	s.renderDLQ(w, r.Context(), channelID, errMsg)
}

func (s *Server) pageScripts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	refs, err := s.Eng.ScriptRefs(id)
	if err != nil {
		http.NotFound(w, r)
		return
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
