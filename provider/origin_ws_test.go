package provider_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/lemmego/api/app"
	"github.com/lemmego/api/config"
	"github.com/lemmego/auth"

	bprovider "github.com/lemmego/broadcast/provider"
)

// A cross-origin WebSocket from an allowed origin must be accepted.
//
// AllowedOrigins is honoured twice over: once by this package, which accepts
// either a full URL or a bare host, and once by coder/websocket, which runs
// filepath.Match against the Origin header's *host*. So an entry carrying a
// scheme is compared against "trusted.example" as "https://trusted.example"
// and can never match — the allowance is accepted by the first check and
// silently refused by the second.
//
// It is silent in the worst way: a same-origin connection passes, because
// Accept short-circuits when Origin's host equals r.Host, so everything works
// until somebody actually configures the cross-origin case this field exists
// for.
func TestAnAllowedCrossOriginWebSocketIsAccepted(t *testing.T) {
	p := &bprovider.Provider{
		// Written the way productshore writes it, and the way anybody
		// copying APP_URL into this field would: a URL, not a host.
		AllowedOrigins: []string{"http://trusted.example"},
	}

	engine := app.Configure()
	engine.WithConfig(config.M{
		"app": config.M{"name": "broadcast-test", "env": "test", "port": 0},
	})
	engine.WithMiddlewares([]app.Handler{
		func(c app.Context) error {
			auth.SetSubject(c, "42")
			return c.Next()
		},
	})
	engine.WithProviders([]app.Provider{p})

	server := httptest.NewServer(engine.Handler())
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if p.Hub() != nil {
			p.Hub().Close()
		}
	})

	socket, response, err := websocket.Dial(context.Background(),
		"ws"+strings.TrimPrefix(server.URL, "http")+"/broadcast/ws?channel=private-user.42",
		&websocket.DialOptions{
			HTTPHeader: http.Header{
				"Origin":         []string{"http://trusted.example"},
				"Sec-Fetch-Site": []string{"cross-site"},
			},
		})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		t.Fatalf("an allowed cross-origin WebSocket was refused with status %d: %v",
			status, err)
	}
	socket.Close(websocket.StatusNormalClosure, "")
}

// And a foreign origin is still refused, so the fix above does not widen the
// rule it was meant to make work.
func TestAForeignCrossOriginWebSocketIsStillRefused(t *testing.T) {
	p := &bprovider.Provider{AllowedOrigins: []string{"http://trusted.example"}}

	engine := app.Configure()
	engine.WithConfig(config.M{
		"app": config.M{"name": "broadcast-test", "env": "test", "port": 0},
	})
	engine.WithMiddlewares([]app.Handler{
		func(c app.Context) error {
			auth.SetSubject(c, "42")
			return c.Next()
		},
	})
	engine.WithProviders([]app.Provider{p})

	server := httptest.NewServer(engine.Handler())
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if p.Hub() != nil {
			p.Hub().Close()
		}
	})

	socket, _, err := websocket.Dial(context.Background(),
		"ws"+strings.TrimPrefix(server.URL, "http")+"/broadcast/ws?channel=private-user.42",
		&websocket.DialOptions{
			HTTPHeader: http.Header{
				"Origin":         []string{"http://evil.example"},
				"Sec-Fetch-Site": []string{"cross-site"},
			},
		})
	if err == nil {
		socket.Close(websocket.StatusNormalClosure, "")
		t.Fatal("a foreign origin opened an authenticated socket")
	}
}
