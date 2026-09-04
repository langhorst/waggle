package api

import (
	"crypto/subtle"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/langhorst/waggle/internal/config"
)

// protect wraps the route table with authentication and a cross-site
// request guard. Every route is covered, static assets included: the API
// is the full-control surface for a daemon that stores PHI, so nothing is
// worth serving anonymously.
//
// Authentication accepts either `Authorization: Bearer <token>` (API
// clients, scripts) or HTTP basic auth (browsers get the native prompt).
// The configured token doubles as a basic-auth password with any user
// name, so one secret serves both the API and the web UI; a dedicated
// basic user/password pair can be configured as well.
//
// The cross-site guard applies to every state-changing method. Basic auth
// is ambient (browsers replay it on cross-site form posts), so a page on
// any origin could otherwise start channels or trigger replays on a daemon
// the operator's browser can reach. Requests that carry an Origin must
// match the request Host; requests that carry Sec-Fetch-Site must be
// same-origin. Non-browser clients send neither header and pass.
func (s *Server) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.Disabled && !s.authorized(r) {
			// The Basic challenge is what makes browsers prompt; API
			// clients simply see the 401.
			w.Header().Set("WWW-Authenticate", `Basic realm="waggle", charset="UTF-8"`)
			s.writeError(w, http.StatusUnauthorized, errUnauthorized)
			return
		}
		if !safeMethod(r.Method) && crossSite(r) {
			s.writeError(w, http.StatusForbidden, errCrossSite)
			return
		}
		// A server-wide WriteTimeout would cut every SSE stream, so the
		// response deadline is applied per request and streams skip it.
		if !isEventStream(r) {
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(responseDeadline))
		}
		next.ServeHTTP(w, r)
	})
}

// authorized reports whether r carries acceptable credentials for the
// configured auth policy.
func (s *Server) authorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	if s.Auth.Token != "" {
		if tok, ok := strings.CutPrefix(header, "Bearer "); ok && equalString(strings.TrimSpace(tok), s.Auth.Token) {
			return true
		}
	}
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	if s.Auth.Token != "" && equalString(pass, s.Auth.Token) {
		return true
	}
	if s.Auth.BasicUser != "" {
		// Compare both halves unconditionally so timing does not reveal
		// which one was wrong.
		userOK := equalString(user, s.Auth.BasicUser)
		passOK := equalString(pass, s.Auth.BasicPassword)
		return userOK && passOK
	}
	return false
}

func equalString(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func isEventStream(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events")
}

func safeMethod(m string) bool {
	return m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
}

// crossSite reports whether a state-changing request originates from a
// browser context other than this daemon's own pages.
func crossSite(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" && origin != "null" {
		u, err := url.Parse(origin)
		if err != nil || !strings.EqualFold(u.Host, r.Host) {
			return true
		}
	}
	switch r.Header.Get("Sec-Fetch-Site") {
	case "", "same-origin", "none":
		return false
	default:
		return true
	}
}

// AuthConfig is re-exported so callers wiring a Server do not need to
// import config just for this.
type AuthConfig = config.Auth
