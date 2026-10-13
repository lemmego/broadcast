package provider

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The origin rule is the only thing between a cookie-authenticated realtime
// endpoint and somebody else's page reading it, because a WebSocket is not
// subject to CORS.
func TestTheOriginRule(t *testing.T) {
	allowed := []string{"https://productshore.com"}

	cases := []struct {
		name    string
		headers map[string]string
		host    string
		allow   bool
		why     string
	}{
		{
			name:    "same-origin fetch metadata",
			headers: map[string]string{"Sec-Fetch-Site": "same-origin"},
			allow:   true,
			why:     "the ordinary case: our own page",
		},
		{
			name:    "same-site fetch metadata",
			headers: map[string]string{"Sec-Fetch-Site": "same-site"},
			allow:   true,
			why:     "a subdomain of the same site",
		},
		{
			name:    "cross-site from an unknown origin",
			headers: map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "https://evil.example"},
			allow:   false,
			why:     "this is the attack, and Sec-Fetch-Site cannot be forged by page JavaScript",
		},
		{
			name:    "cross-site from an allowed origin",
			headers: map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "https://productshore.com"},
			allow:   true,
			why:     "named in the allowlist on purpose",
		},
		{
			name:    "no fetch metadata, origin matches the host",
			headers: map[string]string{"Origin": "http://example.test"},
			host:    "example.test",
			allow:   true,
			why:     "an older browser making a same-origin request",
		},
		{
			name:    "no fetch metadata, foreign origin",
			headers: map[string]string{"Origin": "https://evil.example"},
			host:    "example.test",
			allow:   false,
			why:     "an older browser making a cross-origin request",
		},
		{
			name:    "no headers at all",
			headers: map[string]string{},
			allow:   true,
			why: "not a browser, so not a cross-site attack — and a non-browser " +
				"client can set an Authorization header rather than relying on a cookie",
		},
		{
			name:    "a user-initiated navigation",
			headers: map[string]string{"Sec-Fetch-Site": "none"},
			allow:   true,
			why:     "typed into the address bar; cannot be a cross-site read",
		},
		{
			name:    "an unrecognised fetch-site value",
			headers: map[string]string{"Sec-Fetch-Site": "sideways"},
			allow:   false,
			why:     "fails closed on a value this rule does not understand",
		},
	}

	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/broadcast/ws", nil)
			if one.host != "" {
				r.Host = one.host
			}
			for key, value := range one.headers {
				r.Header.Set(key, value)
			}

			err := checkOrigin(r, allowed)
			if one.allow && err != nil {
				t.Fatalf("refused, want allowed (%s): %v", one.why, err)
			}
			if !one.allow && err == nil {
				t.Fatalf("allowed, want refused (%s)", one.why)
			}
		})
	}
}

// A wildcard is honoured because somebody wrote it down, and it is one
// greppable string in one configuration file rather than a silent default.
func TestAWildcardAllowsAnyOrigin(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/broadcast/ws", nil)
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	r.Header.Set("Origin", "https://anywhere.example")

	if err := checkOrigin(r, []string{"*"}); err != nil {
		t.Fatalf("a wildcard allowlist refused an origin: %v", err)
	}
}

// A bare host is how most people will write an allowlist entry.
func TestAnAllowlistEntryMayBeABareHost(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/broadcast/ws", nil)
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	r.Header.Set("Origin", "https://productshore.com")

	if err := checkOrigin(r, []string{"productshore.com"}); err != nil {
		t.Fatalf("a bare host in the allowlist did not match: %v", err)
	}
}
