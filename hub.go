package broadcast

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults for Config.
const (
	// DefaultHeartbeat is chosen to sit under the shortest idle timeout in
	// the usual path: nginx's proxy_read_timeout and an AWS load balancer
	// both default to 60 seconds, Cloudflare to 100.
	DefaultHeartbeat = 15 * time.Second

	// DefaultQueueSize is how many frames one connection may fall behind by
	// before it is closed.
	DefaultQueueSize = 64

	// DefaultWriteTimeout bounds one frame's write.
	//
	// Re-armed per frame rather than cleared, because a cleared deadline
	// means a stuck client holds a goroutine and a connection forever, while
	// a per-frame deadline reads correctly as "the peer's receive window is
	// full and it is not reading".
	DefaultWriteTimeout = 10 * time.Second

	// DefaultMaxChannelsPerConn bounds one connection's subscriptions.
	DefaultMaxChannelsPerConn = 10

	// DefaultMaxConns bounds a process. 10,000 connections is roughly
	// 200–400MB and 10–20k goroutines, which the scheduler does not notice;
	// the limit exists so the failure is a refusal rather than an OOM.
	DefaultMaxConns = 10_000
)

// Config configures a Hub.
type Config struct {
	// Bus carries events between processes. Nil means a memory bus, which
	// reaches only this process — a first-class mode for a single binary,
	// not a degraded one.
	Bus Bus

	// Authorizer decides who may subscribe to a private channel. Nil means
	// DenyAll, because a hub that defaulted to allow would be an open read
	// channel onto every private channel name an attacker can guess.
	Authorizer Authorizer

	// AllowPublic permits channels without the "private-" prefix.
	//
	// Off by default: an unauthenticated megaphone is a decision, not a
	// default.
	AllowPublic bool

	Heartbeat          time.Duration
	QueueSize          int
	WriteTimeout       time.Duration
	MaxChannelsPerConn int
	MaxConns           int

	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Bus == nil {
		c.Bus = NewMemoryBus()
	}
	if c.Authorizer == nil {
		c.Authorizer = DenyAll()
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = DefaultHeartbeat
	}
	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.MaxChannelsPerConn <= 0 {
		c.MaxChannelsPerConn = DefaultMaxChannelsPerConn
	}
	if c.MaxConns <= 0 {
		c.MaxConns = DefaultMaxConns
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Hub holds the open connections and moves events to them.
//
// Every transport's handler calls Authorize and then Serve, so the whole
// lifecycle — authorization, subscription, the ready frame, heartbeats,
// overflow, drain — lives here once rather than once per protocol. That is
// what keeps the two transports thin enough to be obviously equivalent.
type Hub struct {
	cfg Config

	mu       sync.RWMutex
	conns    map[*connState]struct{}
	draining bool
	closed   bool

	framesSent    atomic.Int64
	framesDropped atomic.Int64
	publishErrors atomic.Int64
}

// New builds a hub.
func New(cfg Config) *Hub {
	return &Hub{cfg: cfg.withDefaults(), conns: map[*connState]struct{}{}}
}

var _ Broadcaster = (*Hub)(nil)

// Publish hands an event to the bus, which delivers it to every subscriber in
// every process.
//
// There is one delivery path on purpose. An earlier design fanned out to
// local connections directly *and* published to the bus, so that local
// subscribers kept working while the bus was down — at the cost of an origin
// nonce on every event and two paths that have to agree about ordering and
// deduplication. One path is simpler to be correct about, and the case it
// gives up is a bus that is configured but unreachable, which a single-binary
// deployment avoids by using the default memory bus rather than configuring
// one it does not need.
func (h *Hub) Publish(ctx context.Context, channel Channel, event Event) error {
	if err := channel.Validate(); err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		return err
	}

	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()
	if closed {
		return ErrClosed
	}

	if err := h.cfg.Bus.Publish(ctx, channel, event); err != nil {
		h.publishErrors.Add(1)
		h.cfg.Logger.Error("broadcast: an event was not published",
			"channel", channel, "event", event.Name, "error", err)
		return fmt.Errorf("%w: %w", ErrBusUnavailable, err)
	}
	return nil
}

// Authorize decides which of the requested channels this request may have.
//
// It runs before a transport commits to its protocol — before a WebSocket
// handshake is answered and before an event stream's first byte — because a
// refusal has to be an HTTP status. A browser gets almost nothing from a
// connection that was accepted and then closed, and its reconnect logic
// cannot tell that from a flaky network, so an accept-then-close refusal
// becomes a reconnect loop against an endpoint that will never admit it.
//
// It returns an error only when nothing at all can be granted, so the caller
// can write a status. A connection that got some of what it asked for is
// served, and the ready frame lists what it did not get — a connection
// silently carrying fewer channels than it asked for is a client waiting for
// events that will never arrive.
func (h *Hub) Authorize(ctx context.Context, r *http.Request, requested []Channel) ([]Channel, []Refusal, error) {
	h.mu.RLock()
	draining, closed, open := h.draining, h.closed, len(h.conns)
	h.mu.RUnlock()

	if closed || draining {
		return nil, nil, ErrClosed
	}
	if open >= h.cfg.MaxConns {
		return nil, nil, ErrTooManyConnections
	}
	if len(requested) == 0 {
		return nil, nil, fmt.Errorf("broadcast: no channels were requested")
	}
	if len(requested) > h.cfg.MaxChannelsPerConn {
		return nil, nil, fmt.Errorf("%w: %d asked for, and the limit is %d",
			ErrTooManyChannels, len(requested), h.cfg.MaxChannelsPerConn)
	}

	var (
		granted []Channel
		refused []Refusal
		// Kept so a wholly refused connection can be answered with the right
		// status: 401 means the credential was missing, which a client can
		// fix by signing in, and 403 means it was not enough.
		sawUnauthenticated bool
	)
	for _, channel := range dedupeChannels(requested) {
		if err := channel.Validate(); err != nil {
			refused = append(refused, Refusal{Channel: channel, Reason: "bad_name"})
			continue
		}
		if !channel.IsPrivate() && !h.cfg.AllowPublic {
			refused = append(refused, Refusal{Channel: channel, Reason: "forbidden"})
			continue
		}
		if channel.IsPrivate() {
			switch err := h.cfg.Authorizer.Authorize(ctx, r, channel); {
			case err == nil:
			case errors.Is(err, ErrUnauthenticated):
				sawUnauthenticated = true
				refused = append(refused, Refusal{Channel: channel, Reason: "unauthenticated"})
				continue
			case errors.Is(err, ErrForbidden):
				refused = append(refused, Refusal{Channel: channel, Reason: "forbidden"})
				continue
			default:
				// An authorizer that could not reach the database has not
				// decided. Reporting that as a denial would have the client
				// retry forever against a transient fault, so it is a 500.
				h.cfg.Logger.Error("broadcast: an authorizer failed to decide",
					"channel", channel, "error", err)
				return nil, nil, fmt.Errorf("broadcast: authorizing %s: %w", channel, err)
			}
		}
		granted = append(granted, channel)
	}

	if len(granted) == 0 {
		if sawUnauthenticated {
			return nil, refused, ErrUnauthenticated
		}
		return nil, refused, ErrForbidden
	}
	return granted, refused, nil
}

// Serve runs one connection until it ends.
//
// It blocks, so a transport's handler calls it last. The connection is closed
// before this returns, whatever ended it.
func (h *Hub) Serve(ctx context.Context, conn Conn, r *http.Request, granted []Channel, refused []Refusal) error {
	if conn == nil {
		return errors.New("broadcast: Serve needs a connection")
	}

	state := &connState{
		conn:    conn,
		request: r, // carried so a mid-connection subscribe is authorized too
		queue:   make(chan Frame, h.cfg.QueueSize),
		stream:  newStreamID(),
	}
	for _, channel := range granted {
		state.remember(channel)
	}

	if err := h.register(state); err != nil {
		conn.Close(CloseReason{Code: CloseGoingAway, Message: err.Error(),
			Reconnect: true, After: time.Second, Resync: true})
		return err
	}
	defer h.unregister(state)

	// Subscribed before the ready frame, so a client that is told it is
	// subscribed is subscribed. One Subscribe per channel: a bus that wants
	// to serve many connections from one network subscription routes
	// internally, which is where that knowledge belongs.
	subscriptions, err := h.subscribe(ctx, state)
	if err != nil {
		conn.Close(CloseReason{Code: CloseBusUnavailable, Message: err.Error(),
			Reconnect: true, After: 2 * time.Second, Resync: true})
		return err
	}
	defer subscriptions.stop()

	if err := h.sendReady(ctx, state, refused); err != nil {
		conn.Close(CloseReason{Code: CloseServerError, Reconnect: true,
			After: time.Second, Resync: true})
		return err
	}

	return h.pump(ctx, state, subscriptions)
}

// pump is the connection's one loop.
func (h *Hub) pump(ctx context.Context, state *connState, subs *subscriptions) error {
	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()

	// Held locally rather than read from state on each turn, because this is
	// the one thing in the loop that gets replaced: a transport with no
	// inbound path hands over a closed channel, which is always ready in a
	// select and would spin this loop at the speed of the scheduler. Setting
	// the local to nil makes that case block forever instead.
	//
	// It was briefly done by replacing state.conn, which the race detector
	// caught at once: a bus forwarder reads state.conn from its own
	// goroutine in order to close the connection.
	commands := state.conn.Commands()

	for {
		select {
		case <-ctx.Done():
			state.conn.Close(CloseReason{Code: CloseGoingAway, Reconnect: true,
				After: time.Second, Resync: true})
			return ctx.Err()

		case <-state.conn.Done():
			// The transport's own answer, not an inference from a request
			// context — which a hijacked WebSocket cannot give.
			return nil

		case <-state.drain:
			// Jittered, because ten thousand clients reconnecting in the
			// same hundred milliseconds is a thundering herd aimed at a
			// replacement process that has not finished booting.
			state.conn.Close(CloseReason{
				Code:      CloseGoingAway,
				Message:   "the server is shutting down",
				Reconnect: true,
				After:     jitter(time.Second, 5*time.Second),
				Resync:    true,
			})
			return nil

		case frame, ok := <-state.queue:
			if !ok {
				return nil
			}
			if err := h.write(ctx, state, frame); err != nil {
				return err
			}

		case <-heartbeat.C:
			if err := state.conn.Ping(ctx); err != nil {
				h.cfg.Logger.Debug("broadcast: a heartbeat failed, so the "+
					"connection is gone", "stream", state.stream, "error", err)
				state.conn.Close(CloseReason{Code: CloseIdle, Reconnect: true,
					After: time.Second, Resync: true})
				return err
			}

		case command, ok := <-commands:
			if !ok {
				commands = nil
				continue
			}
			if err := h.handleCommand(ctx, state, subs, command); err != nil {
				return err
			}
		}
	}
}

func (h *Hub) write(ctx context.Context, state *connState, frame Frame) error {
	state.seq++
	frame.Seq = state.seq

	writeCtx, cancel := context.WithTimeout(ctx, h.cfg.WriteTimeout)
	defer cancel()

	if err := state.conn.Send(writeCtx, frame); err != nil {
		h.cfg.Logger.Debug("broadcast: a frame could not be written",
			"stream", state.stream, "error", err)
		state.conn.Close(CloseReason{Code: CloseClient, Reconnect: true,
			After: time.Second, Resync: true})
		return err
	}
	h.framesSent.Add(1)
	return nil
}

func (h *Hub) sendReady(ctx context.Context, state *connState, refused []Refusal) error {
	body, err := json.Marshal(Ready{
		Stream:      state.stream,
		Protocol:    state.conn.Protocol(),
		ServerTime:  time.Now().UTC(),
		HeartbeatMS: int(h.cfg.Heartbeat / time.Millisecond),
		Channels:    state.subscribed(),
		Refused:     refused,
		Resumed:     false,
		// Always true, because with no backlog the server cannot know that
		// nothing was missed. Claiming otherwise would have the client
		// believe it had resumed.
		Resync: true,
	})
	if err != nil {
		return fmt.Errorf("broadcast: building the ready frame: %w", err)
	}
	return h.write(ctx, state, Frame{Kind: FrameReady, Meta: body})
}

func (h *Hub) handleCommand(ctx context.Context, state *connState, subs *subscriptions, command Command) error {
	switch command.Kind {
	case CommandPing:
		return nil

	case CommandSubscribe:
		for _, channel := range command.Channels {
			h.addChannel(ctx, state, subs, channel)
		}
		return nil

	case CommandUnsubscribe:
		for _, channel := range command.Channels {
			subs.remove(channel)
			state.forget(channel)
		}
		return nil

	default:
		// Reported on the connection rather than closing it: a client
		// sending something unrecognised is a version skew, not an attack.
		return h.sendError(ctx, state, "", "unknown_command")
	}
}

// addChannel authorizes and subscribes to one more channel mid-connection.
//
// Authorized again here, not once at connect: a connection that can add
// channels later must not be able to add one it was never entitled to.
func (h *Hub) addChannel(ctx context.Context, state *connState, subs *subscriptions, channel Channel) {
	if len(state.subscribed()) >= h.cfg.MaxChannelsPerConn {
		h.sendError(ctx, state, channel, "too_many")
		return
	}
	if err := channel.Validate(); err != nil {
		h.sendError(ctx, state, channel, "bad_name")
		return
	}
	if !channel.IsPrivate() && !h.cfg.AllowPublic {
		h.sendError(ctx, state, channel, "forbidden")
		return
	}
	if channel.IsPrivate() {
		if err := h.cfg.Authorizer.Authorize(ctx, state.request, channel); err != nil {
			reason := "forbidden"
			if errors.Is(err, ErrUnauthenticated) {
				reason = "unauthenticated"
			}
			h.sendError(ctx, state, channel, reason)
			return
		}
	}
	if err := subs.add(ctx, channel); err != nil {
		h.sendError(ctx, state, channel, "bus_unavailable")
		return
	}
	state.remember(channel)
}

func (h *Hub) sendError(ctx context.Context, state *connState, channel Channel, reason string) error {
	body, err := json.Marshal(Refusal{Channel: channel, Reason: reason})
	if err != nil {
		return err
	}
	return h.write(ctx, state, Frame{Kind: FrameError, Channel: channel, Meta: body})
}

func (h *Hub) register(state *connState) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed || h.draining {
		return ErrClosed
	}
	if len(h.conns) >= h.cfg.MaxConns {
		return ErrTooManyConnections
	}
	state.drain = make(chan struct{})
	h.conns[state] = struct{}{}
	return nil
}

func (h *Hub) unregister(state *connState) {
	h.mu.Lock()
	delete(h.conns, state)
	h.mu.Unlock()
}

// Drain closes every open connection and refuses new ones.
//
// Called before http.Server.Shutdown, which waits for active requests — and
// an open stream is an active request for as long as somebody has the page
// open, so a server that shuts down without this waits for its entire
// timeout and then kills the streams anyway.
func (h *Hub) Drain(ctx context.Context) error {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		return nil
	}
	h.draining = true
	states := make([]*connState, 0, len(h.conns))
	for state := range h.conns {
		states = append(states, state)
	}
	h.mu.Unlock()

	for _, state := range states {
		close(state.drain)
	}

	// Waited for, but not indefinitely: an Air restart gives the whole
	// shutdown a hundred milliseconds, so Drain cannot be polite about it.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		h.mu.RLock()
		remaining := len(h.conns)
		h.mu.RUnlock()
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			h.cfg.Logger.Warn("broadcast: connections did not close in time",
				"remaining", remaining)
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Close drains and then releases the bus.
func (h *Hub) Close() error {
	drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drainErr := h.Drain(drainCtx)

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()

	if err := h.cfg.Bus.Close(); err != nil {
		return err
	}
	if errors.Is(drainErr, context.DeadlineExceeded) {
		return nil
	}
	return drainErr
}

// jitter returns a duration between low and high.
func jitter(low, high time.Duration) time.Duration {
	if high <= low {
		return low
	}
	return low + time.Duration(rand.Int64N(int64(high-low)))
}

func newStreamID() string {
	buffer := make([]byte, 8)
	if _, err := crand.Read(buffer); err != nil {
		return fmt.Sprintf("s%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buffer)
}

func dedupeChannels(in []Channel) []Channel {
	seen := make(map[Channel]struct{}, len(in))
	out := make([]Channel, 0, len(in))
	for _, channel := range in {
		if _, ok := seen[channel]; ok {
			continue
		}
		seen[channel] = struct{}{}
		out = append(out, channel)
	}
	return out
}
