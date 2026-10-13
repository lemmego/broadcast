package provider

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// checkOrigin decides whether a request may open a realtime connection.
//
// This is the only defence between a cookie-authenticated stream and
// somebody else's page reading it, because of an asymmetry between the two
// transports:
//
//   - An event stream *is* subject to CORS. A cross-origin EventSource to a
//     response with no Access-Control-Allow-Origin is blocked by the browser
//     before any JavaScript sees a byte, and this package sends no CORS
//     headers at all — so that one is same-origin by default and for free.
//   - A WebSocket is *not* subject to CORS. A page on another origin can
//     open one and the browser will attach the cookies unless something
//     checks. The ws transport keeps the library's own Origin check, and this
//     runs in front of both so the rule is one rule.
//
// The order matters. Sec-Fetch-Site leads because it is a positive signal
// that page JavaScript cannot forge, where the absence of Origin is
// ambiguous. A request with neither header is not a browser, so it is not a
// cross-site attack — but it also does not need a cookie, since a non-browser
// client can set an Authorization header. Refusing the cookie in that case
// closes "an attacker-controlled native client replays a stolen cookie"
// without closing the legitimate server-to-server path.
func checkOrigin(r *http.Request, allowed []string) error {
	switch site := r.Header.Get("Sec-Fetch-Site"); site {
	case "same-origin", "same-site", "none":
		// "none" is a user-initiated navigation, which cannot be a
		// cross-site read of this endpoint.
		return nil
	case "cross-site":
		if originAllowed(r.Header.Get("Origin"), allowed) {
			return nil
		}
		return fmt.Errorf("broadcast: a cross-site request from %q is not allowed",
			r.Header.Get("Origin"))
	case "":
		// No Fetch Metadata. Fall through to Origin.
	default:
		return fmt.Errorf("broadcast: Sec-Fetch-Site %q is not allowed", site)
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		// Not a browser. See the note above: this is allowed, and the
		// transport's own cookie handling is what makes it safe.
		return nil
	}
	if sameOrigin(origin, r) || originAllowed(origin, allowed) {
		return nil
	}
	return fmt.Errorf("broadcast: the origin %q is not allowed", origin)
}

// sameOrigin reports whether an Origin header names this request's own host.
//
// Behind a proxy r.Host is the public host only if the proxy was configured
// to preserve it — nginx rewrites Host on proxy_pass unless told otherwise —
// so this check can silently become vacuous. That is why AllowedOrigins is
// required rather than optional: it is the half of the rule that does not
// depend on somebody else's configuration.
func sameOrigin(origin string, r *http.Request) bool {
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}

func originAllowed(origin string, allowed []string) bool {
	if origin == "" {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	for _, candidate := range allowed {
		if candidate == "*" {
			// Deliberate, greppable and loud in the configuration that set
			// it. There is no wildcard matching beyond this.
			return true
		}
		if strings.EqualFold(candidate, origin) {
			return true
		}
		// A bare host is accepted too, since that is how most people will
		// write it.
		if strings.EqualFold(candidate, parsed.Host) {
			return true
		}
	}
	return false
}
