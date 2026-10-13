package provider_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lemmego/api/app"
	"github.com/lemmego/api/config"
	"github.com/lemmego/auth"

	"github.com/lemmego/broadcast"
	bprovider "github.com/lemmego/broadcast/provider"
)

// AllowedOrigins is required rather than defaulted, because a WebSocket is
// not subject to CORS and an unset allowlist is the difference between a
// private channel and a public one.
func TestProvideRefusesAnEmptyOriginAllowlist(t *testing.T) {
	engine := app.Configure()
	p := &bprovider.Provider{}

	err := p.Provide(engine)
	if err == nil {
		t.Fatal("a provider with no origin allowlist was accepted")
	}
	if !strings.Contains(err.Error(), "AllowedOrigins") {
		t.Fatalf("the refusal does not name the missing field: %v", err)
	}
}

// boot brings up a real application with the provider mounted, and a
// middleware that establishes a subject the way apptest does — through
// auth.SetSubject, so the application's own notion of identity is used rather
// than a stub.
func boot(t *testing.T, subject string) (*httptest.Server, *broadcast.Hub) {
	t.Helper()

	p := &bprovider.Provider{AllowedOrigins: []string{"https://productshore.com"}}

	engine := app.Configure()
	// Handler panics without an "app" section, which is the framework
	// asserting that a configuration was loaded rather than defaulted.
	engine.WithConfig(config.M{
		"app": config.M{"name": "broadcast-test", "env": "test", "port": 0},
	})
	engine.WithMiddlewares([]app.Handler{
		func(c app.Context) error {
			if subject != "" {
				auth.SetSubject(c, subject)
			}
			return c.Next()
		},
	})
	// Only WithProviders: Provider implements app.RouteProvider, so the
	// framework calls AddRoutes itself. Passing it to WithRoutes as well
	// registers the patterns twice, which ServeMux refuses with a panic —
	// which is how this test found out.
	engine.WithProviders([]app.Provider{p})

	server := httptest.NewServer(engine.Handler())
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if p.Hub() != nil {
			p.Hub().Close()
		}
	})
	return server, p.Hub()
}

// openStream connects an event stream and returns its reader.
func openStream(t *testing.T, server *httptest.Server, channel string) (*http.Response, *bufio.Reader) {
	t.Helper()

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		server.URL+"/broadcast/sse?channel="+channel, nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	request.Header.Set("Sec-Fetch-Site", "same-origin")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("opening the stream: %v", err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response, bufio.NewReader(response.Body)
}

// The default rule, installed with no configuration, is what lemmego/notify
// publishes to.
func TestSomebodyMayReadTheirOwnChannel(t *testing.T) {
	server, hub := boot(t, "42")

	response, reader := openStream(t, server, "private-user.42")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", response.StatusCode)
	}

	frame := nextFrame(t, reader)
	if frame.Kind != broadcast.FrameReady {
		t.Fatalf("got a %q frame first, want ready", frame.Kind)
	}

	event, err := broadcast.NewEvent("notification.created", map[string]int{"id": 1})
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := hub.Publish(context.Background(), "private-user.42", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	frame = nextFrame(t, reader)
	if frame.Event.Name != "notification.created" {
		t.Fatalf("got event %q", frame.Event.Name)
	}
}

// The case the whole authorization layer exists for.
func TestSomebodyMayNotReadAnotherPersonsChannel(t *testing.T) {
	server, _ := boot(t, "42")

	request, _ := http.NewRequest(http.MethodGet,
		server.URL+"/broadcast/sse?channel=private-user.99", nil)
	request.Header.Set("Sec-Fetch-Site", "same-origin")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("requesting: %v", err)
	}
	defer response.Body.Close()

	// A status, before any stream was established — not an accepted
	// connection that is then closed, which a browser would retry forever.
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("got status %d, want 403", response.StatusCode)
	}
}

func TestAnAnonymousVisitorIsAskedToSignIn(t *testing.T) {
	server, _ := boot(t, "")

	request, _ := http.NewRequest(http.MethodGet,
		server.URL+"/broadcast/sse?channel=private-user.42", nil)
	request.Header.Set("Sec-Fetch-Site", "same-origin")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("requesting: %v", err)
	}
	defer response.Body.Close()

	// 401 rather than 403: one is fixable by signing in and the other is
	// not, and a client retrying into a 401 forever is a self-inflicted
	// denial of service.
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", response.StatusCode)
	}
}

// No rule matched is refused the same way as "not yours", because a
// difference would be a channel-enumeration oracle.
func TestAChannelWithNoRuleIsRefused(t *testing.T) {
	server, _ := boot(t, "42")

	request, _ := http.NewRequest(http.MethodGet,
		server.URL+"/broadcast/sse?channel=private-whatever.1", nil)
	request.Header.Set("Sec-Fetch-Site", "same-origin")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("requesting: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("got status %d, want 403", response.StatusCode)
	}
}

func TestACrossSiteConnectionIsRefused(t *testing.T) {
	server, _ := boot(t, "42")

	request, _ := http.NewRequest(http.MethodGet,
		server.URL+"/broadcast/sse?channel=private-user.42", nil)
	request.Header.Set("Sec-Fetch-Site", "cross-site")
	request.Header.Set("Origin", "https://evil.example")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("requesting: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("got status %d, want 403 — a cross-site page must not be "+
			"able to read this with the viewer's cookies", response.StatusCode)
	}
}

// The diagnostic endpoint counts and does not enumerate: naming the channels
// would publish who is connected and what they are watching.
func TestStatsCountWithoutNamingChannels(t *testing.T) {
	server, _ := boot(t, "42")
	openStream(t, server, "private-user.42")

	request, _ := http.NewRequest(http.MethodGet, server.URL+"/broadcast/stats", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("requesting stats: %v", err)
	}
	defer response.Body.Close()

	body := make([]byte, 1024)
	n, _ := response.Body.Read(body)
	payload := string(body[:n])

	if strings.Contains(payload, "private-user.42") {
		t.Fatalf("the stats endpoint named a channel: %s", payload)
	}
	if !strings.Contains(payload, "connections") {
		t.Fatalf("the stats endpoint returned %s", payload)
	}
}

// nextFrame reads one frame from an event stream.
func nextFrame(t *testing.T, reader *bufio.Reader) broadcast.Frame {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	var data strings.Builder
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("reading the stream: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")

		switch {
		case line == "":
			if data.Len() == 0 {
				continue
			}
			var wire struct {
				Kind  broadcast.FrameKind `json:"kind"`
				Seq   uint64              `json:"seq"`
				Event *struct {
					Name string `json:"name"`
				} `json:"event"`
				Meta json.RawMessage `json:"meta"`
			}
			if err := json.Unmarshal([]byte(data.String()), &wire); err != nil {
				t.Fatalf("decoding %q: %v", data.String(), err)
			}
			frame := broadcast.Frame{Kind: wire.Kind, Seq: wire.Seq, Meta: wire.Meta}
			if wire.Event != nil {
				frame.Event = broadcast.Event{Name: wire.Event.Name}
			}
			return frame
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	t.Fatal("no frame arrived")
	return broadcast.Frame{}
}

// A bus that was asked for and cannot be reached must fail the boot. Falling
// back to the in-process bus passes staging, where there is one replica and
// every subscriber is local, and then loses half the events in production
// where there are two.
func TestAnUnreachableBusFailsTheBoot(t *testing.T) {
	engine := app.Configure()
	engine.WithConfig(config.M{
		"app": config.M{"name": "broadcast-test", "env": "test", "port": 0},
	})

	p := &bprovider.Provider{
		AllowedOrigins: []string{"https://productshore.test"},
		BusFor: func(app.App) (broadcast.Bus, error) {
			return unreachableBus{}, nil
		},
	}

	err := p.Provide(engine)
	if err == nil {
		t.Fatal("an unreachable bus was accepted, so this process would serve " +
			"connections it cannot deliver to")
	}
	if !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

// A bus that cannot be built at all is the same answer: a boot failure, not a
// silent downgrade.
func TestABusThatCannotBeBuiltFailsTheBoot(t *testing.T) {
	engine := app.Configure()
	engine.WithConfig(config.M{
		"app": config.M{"name": "broadcast-test", "env": "test", "port": 0},
	})

	p := &bprovider.Provider{
		AllowedOrigins: []string{"https://productshore.test"},
		BusFor: func(app.App) (broadcast.Bus, error) {
			return nil, errors.New("no redis address was configured")
		},
	}

	if err := p.Provide(engine); err == nil {
		t.Fatal("a bus that could not be built was accepted")
	}
}

// And no bus at all is the supported single-binary mode rather than an error.
func TestNoBusIsTheInProcessMode(t *testing.T) {
	engine := app.Configure()
	engine.WithConfig(config.M{
		"app": config.M{"name": "broadcast-test", "env": "test", "port": 0},
	})

	p := &bprovider.Provider{AllowedOrigins: []string{"https://productshore.test"}}
	if err := p.Provide(engine); err != nil {
		t.Fatalf("a provider with no bus failed to boot: %v", err)
	}
	t.Cleanup(func() { p.Hub().Close() })
}

// unreachableBus reports itself unreachable, which is what a Redis that is
// down does.
type unreachableBus struct{}

func (unreachableBus) Ping(context.Context) error {
	return errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
}

func (unreachableBus) Publish(context.Context, broadcast.Channel, broadcast.Event) error {
	return nil
}

func (unreachableBus) Subscribe(context.Context, broadcast.Channel) (<-chan broadcast.Delivery, func(), error) {
	return nil, func() {}, nil
}

func (unreachableBus) Close() error { return nil }
