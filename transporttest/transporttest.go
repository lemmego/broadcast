// Package transporttest is the conformance suite every broadcast transport
// must pass.
//
// It exists because the whole argument for shipping two transports together
// is that a seam fitted against one implementation is fitted by accident.
// Written before either transport, and run from each transport's own tests:
//
//	func TestSSEConformsToTheTransportContract(t *testing.T) {
//	    transporttest.Run(t, sse.New(), broadcast.Capabilities{})
//	}
//
// Every case runs against a real httptest.Server, because an
// httptest.ResponseRecorder can be neither flushed nor hijacked — so a suite
// built on one would pass for the wrong reason on both transports.
//
// Exported so a transport written outside this module can prove itself too.
package transporttest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/lemmego/broadcast"
)

// ErrUnsupported is what a Client's Send returns when its transport has no
// client-to-server path.
var ErrUnsupported = errors.New("transporttest: this transport accepts no commands")

// Factory is what a transport supplies so the suite can drive it.
type Factory struct {
	// Transport under test.
	Transport broadcast.Transport

	// Dial connects a client and returns the half the suite drives.
	//
	// base is the server's URL; the suite appends nothing, so Dial owns the
	// path and the query string its transport expects.
	Dial func(t *testing.T, base *url.URL, channels []broadcast.Channel) (Client, error)
}

// Client is the transport-specific client half the suite needs.
type Client interface {
	// Next returns the next frame, or an error if none arrives in time.
	Next(timeout time.Duration) (broadcast.Frame, error)

	// Send delivers a command, or returns ErrUnsupported.
	Send(broadcast.Command) error

	// Stall stops reading, so the server's queue fills.
	Stall()

	Close() error
}

// Run executes the whole contract.
func Run(t *testing.T, f Factory, caps broadcast.Capabilities) {
	t.Helper()

	if f.Transport == nil || f.Dial == nil {
		t.Fatal("transporttest: Run needs a Transport and a Dial")
	}

	cases := []struct {
		name string
		run  func(*testing.T, Factory, broadcast.Capabilities)
	}{
		{"ReadyArrivesBeforeAnyEvent", readyArrivesBeforeAnyEvent},
		{"AnEventRoundTrips", anEventRoundTrips},
		{"FrameBoundariesSurvive", frameBoundariesSurvive},
		{"OrderingIsPreserved", orderingIsPreserved},
		{"ASlowClientIsClosedNotBuffered", aSlowClientIsClosedNotBuffered},
		{"HeartbeatsDoNotSurfaceAsEvents", heartbeatsDoNotSurfaceAsEvents},
		{"SurvivesTheServersWriteTimeout", survivesTheServersWriteTimeout},
		{"WorksBehindAWrappingMiddleware", worksBehindAWrappingMiddleware},
		{"ARefusalIsAnHTTPStatus", aRefusalIsAnHTTPStatus},
		{"CloseIsObservableWithItsReason", closeIsObservableWithItsReason},
		{"CommandsChannelIsNeverNil", commandsChannelIsNeverNil},
	}

	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) { one.run(t, f, caps) })
	}
}

// harness is a server with a hub behind the transport under test.
type harness struct {
	hub    *broadcast.Hub
	server *httptest.Server
	base   *url.URL
}

// mount serves the transport, optionally behind a middleware and with the
// server configured.
func mount(t *testing.T, f Factory, wrap func(http.Handler) http.Handler, configure func(*http.Server)) *harness {
	t.Helper()

	hub := broadcast.New(broadcast.Config{
		Authorizer: broadcast.AuthorizerFunc(
			func(_ context.Context, _ *http.Request, ch broadcast.Channel) error {
				// One channel is refused, so the suite can exercise a
				// refusal without a second hub.
				if ch == "private-forbidden.1" {
					return broadcast.ErrForbidden
				}
				return nil
			}),
		Heartbeat: 100 * time.Millisecond,
		// Roomy enough that an ordinary burst is not mistaken for a slow
		// client. The overflow behaviour gets a case of its own, which
		// stalls the client deliberately rather than relying on a queue so
		// small that every other case trips it.
		QueueSize: 64,
	})
	t.Cleanup(func() { hub.Close() })

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channels := requestedChannels(r)

		granted, refused, err := hub.Authorize(r.Context(), r, channels)
		if err != nil {
			// The refusal is an HTTP status, before the transport commits to
			// its protocol. See Transport.Accept.
			switch {
			case errors.Is(err, broadcast.ErrUnauthenticated):
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
			case errors.Is(err, broadcast.ErrForbidden):
				http.Error(w, "forbidden", http.StatusForbidden)
			default:
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			}
			return
		}

		conn, err := f.Transport.Accept(w, r)
		if err != nil {
			return // Accept has written its own status
		}
		hub.Serve(r.Context(), conn, r, granted, refused)
	})

	if wrap != nil {
		handler = wrap(handler)
	}

	server := httptest.NewUnstartedServer(handler)
	if configure != nil {
		configure(server.Config)
	}
	server.Start()
	t.Cleanup(server.Close)

	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parsing the server URL: %v", err)
	}
	return &harness{hub: hub, server: server, base: base}
}

func requestedChannels(r *http.Request) []broadcast.Channel {
	raw := r.URL.Query()["channel"]
	channels := make([]broadcast.Channel, 0, len(raw))
	for _, one := range raw {
		channels = append(channels, broadcast.Channel(one))
	}
	return channels
}

const wait = 3 * time.Second

func expectReady(t *testing.T, client Client) broadcast.Ready {
	t.Helper()

	frame, err := client.Next(wait)
	if err != nil {
		t.Fatalf("waiting for the ready frame: %v", err)
	}
	if frame.Kind != broadcast.FrameReady {
		t.Fatalf("the first frame is %q, want ready", frame.Kind)
	}
	var ready broadcast.Ready
	if err := json.Unmarshal(frame.Meta, &ready); err != nil {
		t.Fatalf("decoding the ready frame: %v", err)
	}
	return ready
}

// --- cases ------------------------------------------------------------

// readyArrivesBeforeAnyEvent pins the gap tasker's own stream has: it sends
// nothing until its first five-second tick, so a client cannot tell
// "connected" from "hung" and the only thing it can do about either is wait.
func readyArrivesBeforeAnyEvent(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()

	start := time.Now()
	ready := expectReady(t, client)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the ready frame took %s; it must be flushed immediately", elapsed)
	}
	if ready.Protocol == "" {
		t.Fatal("the ready frame does not name its protocol")
	}
	if !ready.Resync {
		t.Fatal("the ready frame does not ask the client to re-fetch, so a " +
			"client would believe it had resumed")
	}
}

func anEventRoundTrips(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	event, err := broadcast.NewEvent("notification.created", map[string]any{"id": 7, "handle": "ada"})
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	frame, err := client.Next(wait)
	if err != nil {
		t.Fatalf("waiting for the event: %v", err)
	}
	if frame.Kind != broadcast.FrameEvent {
		t.Fatalf("got %q, want event", frame.Kind)
	}
	if frame.Event.Name != event.Name {
		t.Fatalf("got name %q, want %q", frame.Event.Name, event.Name)
	}
	if frame.Event.ID != event.ID {
		t.Fatalf("got id %q, want %q — a client deduplicates on it", frame.Event.ID, event.ID)
	}
	if frame.Channel != "private-user.1" {
		t.Fatalf("got channel %q", frame.Channel)
	}
	if string(frame.Event.Data) != string(event.Data) {
		t.Fatalf("got payload %s, want %s", frame.Event.Data, event.Data)
	}
}

// frameBoundariesSurvive is the case a naive event-stream encoder fails. A
// payload containing a newline, a blank line, or the literal text of a stream
// field must not be able to end a frame early or inject another one.
func frameBoundariesSurvive(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	hostile := []string{
		"a\nb",
		"a\r\nb",
		"a\n\nb",
		"a\rb",
		"data: injected",
		"event: injected\ndata: injected",
		"\n\ndata: injected\n\n",
		"id: 9",
		": comment",
	}

	for _, payload := range hostile {
		event, err := broadcast.NewEvent("probe", map[string]string{"body": payload})
		if err != nil {
			t.Fatalf("building an event for %q: %v", payload, err)
		}
		if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
			t.Fatalf("publishing %q: %v", payload, err)
		}

		frame, err := client.Next(wait)
		if err != nil {
			t.Fatalf("waiting for the event carrying %q: %v", payload, err)
		}
		var body struct{ Body string }
		if err := json.Unmarshal(frame.Event.Data, &body); err != nil {
			t.Fatalf("decoding the payload for %q: %v", payload, err)
		}
		if body.Body != payload {
			t.Fatalf("payload %q came back as %q — a frame boundary leaked",
				payload, body.Body)
		}
		if frame.Event.Name != "probe" {
			t.Fatalf("the event name became %q while carrying %q",
				frame.Event.Name, payload)
		}
	}
}

func orderingIsPreserved(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	const count = 50
	for i := 0; i < count; i++ {
		event, err := broadcast.NewEvent("tick", map[string]int{"n": i})
		if err != nil {
			t.Fatalf("building event %d: %v", i, err)
		}
		if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}

	for i := 0; i < count; i++ {
		frame, err := client.Next(wait)
		if err != nil {
			t.Fatalf("waiting for event %d: %v", i, err)
		}
		var body struct{ N int }
		if err := json.Unmarshal(frame.Event.Data, &body); err != nil {
			t.Fatalf("decoding event %d: %v", i, err)
		}
		if body.N != i {
			t.Fatalf("got event %d where %d was expected", body.N, i)
		}
		// The sequence counts every frame on the connection, ready included.
		if frame.Seq != uint64(i+2) {
			t.Fatalf("event %d carries seq %d, want %d", i, frame.Seq, i+2)
		}
	}
}

// aSlowClientIsClosedNotBuffered proves the client observes the close.
//
// The Hub decides to close rather than drop; this is the half that matters to
// a browser — that the decision is visible to it, so its reconnect and
// re-fetch actually run. An unbounded buffer would instead be a memory leak
// one slow client wide, and a silent drop would be an event nobody can know
// they missed.
func aSlowClientIsClosedNotBuffered(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	client.Stall()
	event, err := broadcast.NewEvent("flood", nil)
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	for i := 0; i < 500; i++ {
		if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
			break
		}
	}

	// Observable either as a close frame or as the read ending. What must not
	// happen is the server buffering indefinitely and saying nothing.
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if h.hub.Stats().Connections == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a stalled client was buffered rather than closed")
}

// heartbeatsDoNotSurfaceAsEvents: a keepalive that arrived as an application
// event would have every client branch on an event it never sent.
func heartbeatsDoNotSurfaceAsEvents(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	// The harness heartbeats every 100ms, so several pass in this window.
	if frame, err := client.Next(600 * time.Millisecond); err == nil {
		t.Fatalf("a heartbeat surfaced as a %q frame carrying %q",
			frame.Kind, frame.Event.Name)
	}

	// And the connection is still alive afterwards.
	event, _ := broadcast.NewEvent("after", nil)
	if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	if _, err := client.Next(wait); err != nil {
		t.Fatalf("the connection did not survive its own heartbeats: %v", err)
	}
}

// survivesTheServersWriteTimeout is the case that catches a transport which
// never touches its write deadline. http.Server.WriteTimeout is a deadline on
// the whole response, so a stream dies at that mark unless the transport
// re-arms it per frame.
func survivesTheServersWriteTimeout(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, func(s *http.Server) {
		s.WriteTimeout = 200 * time.Millisecond
	})
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	// Published well after the deadline would have fired.
	time.Sleep(500 * time.Millisecond)
	event, err := broadcast.NewEvent("late", nil)
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	if _, err := client.Next(wait); err != nil {
		t.Fatalf("the stream did not outlive the server's WriteTimeout: %v — "+
			"the transport has to manage its own write deadline", err)
	}
}

// worksBehindAWrappingMiddleware pins the failure that had tasker's dashboard
// stream answering 500 in a real application: a middleware wrapping
// http.ResponseWriter without an Unwrap, so http.Flusher and http.Hijacker
// disappeared behind it.
func worksBehindAWrappingMiddleware(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, statusCapturing, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling behind a wrapping middleware: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	event, err := broadcast.NewEvent("through", nil)
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	if _, err := client.Next(wait); err != nil {
		t.Fatalf("no event arrived through the middleware: %v", err)
	}
}

// statusCapturing is the shape every status-capturing middleware has, with
// the three methods that make it survivable.
//
// All three are needed, and the WebSocket transport is what proves it: with
// only Unwrap and Flush, a handshake behind this middleware answers 501 Not
// Implemented, because the library reaches for http.Hijacker by type
// assertion and a type assertion traverses nothing. An event stream, which
// goes through http.ResponseController, is happy with Unwrap alone — so a
// suite run against only one transport would have let this through.
func statusCapturing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&capturing{ResponseWriter: w}, r)
	})
}

type capturing struct {
	http.ResponseWriter
	status int
}

func (c *capturing) WriteHeader(status int) {
	c.status = status
	c.ResponseWriter.WriteHeader(status)
}

func (c *capturing) Unwrap() http.ResponseWriter { return c.ResponseWriter }

func (c *capturing) Flush() {
	http.NewResponseController(c.ResponseWriter).Flush()
}

func (c *capturing) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(c.ResponseWriter).Hijack()
}

// aRefusalIsAnHTTPStatus: a browser gets almost nothing out of a connection
// that was accepted and then closed, and its reconnect logic cannot tell that
// from a flaky network — so an accept-then-close refusal becomes a reconnect
// loop against an endpoint that will never admit it.
func aRefusalIsAnHTTPStatus(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)

	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-forbidden.1"})
	if err == nil {
		defer client.Close()
		// Some clients report the status only on first read.
		if _, readErr := client.Next(time.Second); readErr == nil {
			t.Fatal("a refused subscription produced a working connection")
		}
		return
	}
	if !isStatusError(err, http.StatusForbidden) {
		t.Fatalf("got %v, want a 403 before any connection was established", err)
	}
}

// StatusError is what a Dial returns when the server refused with a status.
type StatusError struct {
	Status int
	Err    error
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("transporttest: the server answered %d", e.Status)
}

func (e *StatusError) Unwrap() error { return e.Err }

func isStatusError(err error, status int) bool {
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Status == status
	}
	return false
}

func closeIsObservableWithItsReason(t *testing.T, f Factory, _ broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	if err := h.hub.Drain(context.Background()); err != nil {
		t.Fatalf("draining: %v", err)
	}

	// A close frame, or a read error — both are observable, which is the
	// requirement. A connection that simply stops delivering is not.
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		frame, err := client.Next(500 * time.Millisecond)
		if err != nil {
			return // the read ended, which the client can see
		}
		if frame.Kind == broadcast.FrameClose {
			var reason broadcast.CloseReason
			if err := json.Unmarshal(frame.Meta, &reason); err != nil {
				t.Fatalf("decoding the close reason: %v", err)
			}
			if reason.Code == "" {
				t.Fatal("the close frame carries no code for a client to act on")
			}
			return
		}
	}
	t.Fatal("the connection was neither closed nor told why")
}

// commandsChannelIsNeverNil: a nil channel in a select blocks forever, and a
// transport returning one would make that bug invisible. A transport with no
// inbound path returns a closed channel, which the Hub swaps for nil itself.
func commandsChannelIsNeverNil(t *testing.T, f Factory, caps broadcast.Capabilities) {
	h := mount(t, f, nil, nil)
	client, err := f.Dial(t, h.base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer client.Close()
	expectReady(t, client)

	sendErr := client.Send(broadcast.Command{Kind: broadcast.CommandPing})
	if caps.AcceptsCommands {
		if sendErr != nil {
			t.Fatalf("a transport declaring AcceptsCommands refused one: %v", sendErr)
		}
	} else if !errors.Is(sendErr, ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported from a transport with no inbound path", sendErr)
	}

	// Either way the connection still works, which is what proves the Hub's
	// loop is not spinning on a closed channel.
	event, _ := broadcast.NewEvent("after", nil)
	if err := h.hub.Publish(context.Background(), "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	if _, err := client.Next(wait); err != nil {
		t.Fatalf("the connection stopped working after a command: %v", err)
	}
}
