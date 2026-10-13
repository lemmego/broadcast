package broadcast

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeConn is a Conn that records frames, so the Hub can be tested without
// either real protocol. The transports get their own conformance suite.
type fakeConn struct {
	mu       sync.Mutex
	frames   []Frame
	closed   *CloseReason
	pings    int
	sendErr  error
	blockOn  chan struct{}
	commands chan Command
	done     chan struct{}
	caps     Capabilities
	protocol string
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		commands: make(chan Command, 4),
		done:     make(chan struct{}),
		protocol: "sse",
	}
}

func (c *fakeConn) Send(_ context.Context, f Frame) error {
	if c.blockOn != nil {
		<-c.blockOn
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sendErr != nil {
		return c.sendErr
	}
	c.frames = append(c.frames, f)
	return nil
}

func (c *fakeConn) Ping(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pings++
	return nil
}

func (c *fakeConn) Commands() <-chan Command { return c.commands }

func (c *fakeConn) Close(r CloseReason) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed == nil {
		c.closed = &r
		close(c.done)
	}
	return nil
}

func (c *fakeConn) Done() <-chan struct{}      { return c.done }
func (c *fakeConn) Capabilities() Capabilities { return c.caps }
func (c *fakeConn) RemoteAddr() string         { return "127.0.0.1:1" }
func (c *fakeConn) Protocol() string           { return c.protocol }

func (c *fakeConn) snapshot() ([]Frame, *CloseReason) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Frame(nil), c.frames...), c.closed
}

func (c *fakeConn) waitForFrames(t *testing.T, n int) []Frame {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		frames, _ := c.snapshot()
		if len(frames) >= n {
			return frames
		}
		select {
		case <-deadline:
			t.Fatalf("waited for %d frames, saw %d", n, len(frames))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (c *fakeConn) waitForClose(t *testing.T) CloseReason {
	t.Helper()
	select {
	case <-c.done:
		_, reason := c.snapshot()
		return *reason
	case <-time.After(2 * time.Second):
		t.Fatal("the connection was never closed")
		return CloseReason{}
	}
}

func permit() Authorizer {
	return AuthorizerFunc(func(context.Context, *http.Request, Channel) error { return nil })
}

func request() *http.Request {
	return httptest.NewRequest(http.MethodGet, "/broadcast/sse", nil)
}

// --- Authorize ---------------------------------------------------------

func TestAPrivateChannelIsRefusedWithoutAnAuthorizer(t *testing.T) {
	hub := New(Config{})
	defer hub.Close()

	// DenyAll is the zero value, because a hub that defaulted to allow would
	// be an open read channel onto every guessable private channel name.
	_, _, err := hub.Authorize(context.Background(), request(), []Channel{"private-user.1"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden", err)
	}
}

func TestAPublicChannelIsRefusedUnlessAllowed(t *testing.T) {
	hub := New(Config{Authorizer: permit()})
	defer hub.Close()

	_, _, err := hub.Authorize(context.Background(), request(), []Channel{"announcements"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden — an unauthenticated megaphone is "+
			"a decision, not a default", err)
	}

	allowed := New(Config{Authorizer: permit(), AllowPublic: true})
	defer allowed.Close()
	granted, _, err := allowed.Authorize(context.Background(), request(), []Channel{"announcements"})
	if err != nil || len(granted) != 1 {
		t.Fatalf("got %v / %v, want the channel granted", granted, err)
	}
}

// 401 and 403 are different answers: one is fixable by signing in and the
// other is not, and a client that retries into a 401 forever is a
// self-inflicted denial of service.
func TestAMissingCredentialIsDistinguishedFromAnInsufficientOne(t *testing.T) {
	unauthenticated := New(Config{Authorizer: AuthorizerFunc(
		func(context.Context, *http.Request, Channel) error { return ErrUnauthenticated })})
	defer unauthenticated.Close()

	_, _, err := unauthenticated.Authorize(context.Background(), request(), []Channel{"private-user.1"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("got %v, want ErrUnauthenticated", err)
	}
}

// An authorizer that could not reach the database has not decided. Reporting
// that as a denial would have the client retry forever against a transient
// fault, so it is a server error.
func TestAnAuthorizerThatCannotDecideIsAServerError(t *testing.T) {
	hub := New(Config{Authorizer: AuthorizerFunc(
		func(context.Context, *http.Request, Channel) error {
			return errors.New("the database is unreachable")
		})})
	defer hub.Close()

	_, _, err := hub.Authorize(context.Background(), request(), []Channel{"private-user.1"})
	if err == nil {
		t.Fatal("got no error")
	}
	if errors.Is(err, ErrForbidden) || errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("a failure to decide was reported as a denial: %v", err)
	}
}

func TestSomeChannelsGrantedIsStillAConnection(t *testing.T) {
	hub := New(Config{Authorizer: AuthorizerFunc(
		func(_ context.Context, _ *http.Request, ch Channel) error {
			if ch == "private-user.2" {
				return ErrForbidden
			}
			return nil
		})})
	defer hub.Close()

	granted, refused, err := hub.Authorize(context.Background(), request(),
		[]Channel{"private-user.1", "private-user.2"})
	if err != nil {
		t.Fatalf("got %v, want the partially granted connection served", err)
	}
	if len(granted) != 1 || granted[0] != "private-user.1" {
		t.Fatalf("granted %v", granted)
	}
	// Listed, because a connection silently carrying fewer channels than it
	// asked for is a client waiting for events that will never arrive.
	if len(refused) != 1 || refused[0].Channel != "private-user.2" {
		t.Fatalf("refused %v", refused)
	}
}

func TestAChannelNameThatCouldInjectFramesIsRefused(t *testing.T) {
	hub := New(Config{Authorizer: permit()})
	defer hub.Close()

	// A newline in a channel name would end a field in an event stream, and
	// a blank line would end the frame — letting one subscriber inject
	// frames at every other subscriber on that connection.
	_, refused, err := hub.Authorize(context.Background(), request(),
		[]Channel{"private-user.1\n\ndata: stolen"})
	if err == nil {
		t.Fatal("a channel name containing a newline was granted")
	}
	if len(refused) != 1 || refused[0].Reason != "bad_name" {
		t.Fatalf("refused %v", refused)
	}
}

func TestTooManyChannelsOnOneConnectionIsRefused(t *testing.T) {
	hub := New(Config{Authorizer: permit(), MaxChannelsPerConn: 2})
	defer hub.Close()

	_, _, err := hub.Authorize(context.Background(), request(),
		[]Channel{"private-a.1", "private-b.1", "private-c.1"})
	if !errors.Is(err, ErrTooManyChannels) {
		t.Fatalf("got %v, want ErrTooManyChannels", err)
	}
}

// --- Serve ------------------------------------------------------------

func TestTheFirstFrameIsReadyAndArrivesBeforeAnyEvent(t *testing.T) {
	hub := New(Config{Authorizer: permit()})
	defer hub.Close()
	conn := newFakeConn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)

	frames := conn.waitForFrames(t, 1)
	if frames[0].Kind != FrameReady {
		t.Fatalf("the first frame is %q, want ready — without it a client "+
			"cannot tell connected from hung", frames[0].Kind)
	}
	if frames[0].Seq != 1 {
		t.Fatalf("got seq %d, want 1", frames[0].Seq)
	}

	var ready Ready
	if err := json.Unmarshal(frames[0].Meta, &ready); err != nil {
		t.Fatalf("decoding the ready frame: %v", err)
	}
	// With no backlog the server cannot know nothing was missed, so it says
	// so rather than letting the client believe it resumed.
	if !ready.Resync || ready.Resumed {
		t.Fatalf("got resync=%v resumed=%v, want true/false", ready.Resync, ready.Resumed)
	}
	if len(ready.Channels) != 1 {
		t.Fatalf("ready lists %v", ready.Channels)
	}
}

func TestAPublishedEventReachesASubscribedConnection(t *testing.T) {
	hub := New(Config{Authorizer: permit()})
	defer hub.Close()
	conn := newFakeConn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
	conn.waitForFrames(t, 1)

	event, err := NewEvent("notification.created", map[string]int{"id": 7})
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := hub.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	frames := conn.waitForFrames(t, 2)
	if frames[1].Kind != FrameEvent {
		t.Fatalf("got %q, want event", frames[1].Kind)
	}
	if frames[1].Event.Name != "notification.created" {
		t.Fatalf("got event %q", frames[1].Event.Name)
	}
	if frames[1].Seq != 2 {
		t.Fatalf("got seq %d, want 2 — a client detects a gap by counting", frames[1].Seq)
	}
}

func TestAnEventOnAnotherChannelIsNotDelivered(t *testing.T) {
	hub := New(Config{Authorizer: permit()})
	defer hub.Close()
	conn := newFakeConn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
	conn.waitForFrames(t, 1)

	event, _ := NewEvent("notification.created", nil)
	hub.Publish(ctx, "private-user.2", event)

	time.Sleep(100 * time.Millisecond)
	frames, _ := conn.snapshot()
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want only the ready frame", len(frames))
	}
}

// A client that silently misses an event is worse than one that reconnects:
// with no backlog the miss is unrecoverable and invisible, whereas a closed
// connection is something the client already handles.
func TestASlowConnectionIsClosedRatherThanQuietlyMissingEvents(t *testing.T) {
	hub := New(Config{Authorizer: permit(), QueueSize: 2})
	defer hub.Close()

	conn := newFakeConn()
	// Blocked mid-write, so the queue fills behind it.
	conn.blockOn = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)

	event, _ := NewEvent("notification.created", nil)
	for i := 0; i < 50; i++ {
		hub.Publish(ctx, "private-user.1", event)
	}

	reason := conn.waitForClose(t)
	if reason.Code != CloseSlowConsumer {
		t.Fatalf("closed with %q, want %q", reason.Code, CloseSlowConsumer)
	}
	if !reason.Reconnect || !reason.Resync {
		t.Fatalf("got reconnect=%v resync=%v, want both true — the client has "+
			"to come back and re-fetch", reason.Reconnect, reason.Resync)
	}
	close(conn.blockOn)
}

func TestDrainClosesEveryConnectionWithAJitteredRetry(t *testing.T) {
	hub := New(Config{Authorizer: permit()})

	conns := []*fakeConn{newFakeConn(), newFakeConn(), newFakeConn()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, conn := range conns {
		go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
		conn.waitForFrames(t, 1)
	}

	if err := hub.Drain(context.Background()); err != nil {
		t.Fatalf("draining: %v", err)
	}

	delays := map[time.Duration]bool{}
	for i, conn := range conns {
		reason := conn.waitForClose(t)
		if reason.Code != CloseGoingAway {
			t.Fatalf("connection %d closed with %q, want %q", i, reason.Code, CloseGoingAway)
		}
		if !reason.Reconnect {
			t.Fatalf("connection %d was told not to come back", i)
		}
		delays[reason.After] = true
	}
	// Jittered, because ten thousand clients reconnecting in the same
	// hundred milliseconds is a thundering herd at a process that has not
	// finished booting. Three identical delays would mean no jitter at all.
	if len(delays) == 1 {
		t.Fatal("every connection was given the same retry delay")
	}

	// And new connections are refused while draining.
	if _, _, err := hub.Authorize(context.Background(), request(), []Channel{"private-user.1"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed while draining", err)
	}
}

// An open stream receiving nothing is the worst failure mode here, so a lost
// subscription closes the connection rather than going quiet.
func TestALostSubscriptionClosesTheConnection(t *testing.T) {
	bus := NewMemoryBus()
	hub := New(Config{Authorizer: permit(), Bus: bus})

	conn := newFakeConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
	conn.waitForFrames(t, 1)

	// The bus losing its backing closes its delivery channels, which is the
	// contract that makes this distinguishable from a quiet channel.
	bus.Close()

	reason := conn.waitForClose(t)
	if reason.Code != CloseBusUnavailable {
		t.Fatalf("closed with %q, want %q", reason.Code, CloseBusUnavailable)
	}
	if !reason.Resync {
		t.Fatal("the client was not told to re-fetch")
	}
}

func TestHeartbeatsArePingedOnTheConfiguredInterval(t *testing.T) {
	hub := New(Config{Authorizer: permit(), Heartbeat: 20 * time.Millisecond})
	defer hub.Close()

	conn := newFakeConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
	conn.waitForFrames(t, 1)

	deadline := time.After(2 * time.Second)
	for {
		conn.mu.Lock()
		pings := conn.pings
		conn.mu.Unlock()
		if pings >= 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("saw %d heartbeats, want at least 2 — an idle proxy "+
				"closes a connection that sends nothing", pings)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestStatsCountWithoutNamingChannels(t *testing.T) {
	hub := New(Config{Authorizer: permit()})
	defer hub.Close()

	conn := newFakeConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
	conn.waitForFrames(t, 1)

	stats := hub.Stats()
	if stats.Connections != 1 || stats.BySSE != 1 {
		t.Fatalf("got %+v", stats)
	}
	if stats.Channels != 1 {
		t.Fatalf("got %d channels, want 1", stats.Channels)
	}
	if stats.FramesSent < 1 {
		t.Fatalf("got %d frames sent", stats.FramesSent)
	}
}

func TestAClosedCommandChannelDoesNotSpinTheLoop(t *testing.T) {
	hub := New(Config{Authorizer: permit(), Heartbeat: time.Hour})
	defer hub.Close()

	conn := newFakeConn()
	// A transport with no inbound path. A closed channel is always ready in
	// a select, so without the nil swap this would spin at scheduler speed.
	close(conn.commands)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		hub.Serve(ctx, conn, request(), []Channel{"private-user.1"}, nil)
		close(done)
	}()
	conn.waitForFrames(t, 1)

	// If the loop were spinning, the sequence number would climb; it should
	// not move at all, because nothing is being written.
	time.Sleep(150 * time.Millisecond)
	frames, _ := conn.snapshot()
	if len(frames) != 1 {
		t.Fatalf("got %d frames while idle, want 1 — the loop is spinning", len(frames))
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after its context was cancelled")
	}
}
