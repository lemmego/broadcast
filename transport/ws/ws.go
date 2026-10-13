// Package ws carries broadcast frames over a WebSocket.
//
// It is the transport to use where server-sent events do not fit: a single
// binary with no proxy terminating HTTP/2, where a browser caps an origin at
// about six concurrent HTTP/1.1 connections and a stream permanently occupies
// one of them across every tab. Browsers do not apply that cap to WebSockets.
//
// It is also the only one of the two that can carry a message from the
// client, which is what makes the transport seam honest: a contract fitted
// only to an event stream would have had no inbound path at all.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/lemmego/broadcast"
)

// Name is this transport's protocol name, as it appears in a ready frame.
const Name = "ws"

// Defaults.
const (
	// DefaultReadLimit bounds an inbound message. Commands are tiny — a kind
	// and a few channel names — so this is generous and still refuses a
	// client trying to make the server allocate.
	DefaultReadLimit = 32 << 10

	// DefaultPongTimeout is how long a ping waits to be answered.
	DefaultPongTimeout = 10 * time.Second

	// DefaultWriteTimeout bounds one frame's write.
	DefaultWriteTimeout = 10 * time.Second
)

// Transport accepts WebSocket connections.
type Transport struct {
	originPatterns []string
	readLimit      int64
	pongTimeout    time.Duration
	writeTimeout   time.Duration
}

// Option configures a Transport.
type Option func(*Transport)

// WithOriginPatterns allows cross-origin connections from these hosts.
//
// Empty — the default — means same-origin only, which is what the underlying
// library does by default and the right answer for a cookie-authenticated
// endpoint.
//
// There is deliberately no option here for the library's InsecureSkipVerify,
// and no way to reach it from this package. A WebSocket is not subject to
// CORS, so a browser will attach cookies to a handshake from any origin
// unless the Origin header is checked — which makes disabling that check a
// cross-site hijack of every authenticated connection. The library's own
// documentation makes the same choice, telling callers not to use "*" as a
// pattern and to reach for the insecure flag instead "to bring attention to
// the danger of such a setting".
func WithOriginPatterns(patterns ...string) Option {
	return func(t *Transport) { t.originPatterns = patterns }
}

// WithReadLimit bounds an inbound message.
func WithReadLimit(bytes int64) Option {
	return func(t *Transport) { t.readLimit = bytes }
}

// WithPongTimeout bounds how long a heartbeat waits for its answer.
func WithPongTimeout(d time.Duration) Option {
	return func(t *Transport) { t.pongTimeout = d }
}

// WithWriteTimeout bounds one frame's write.
func WithWriteTimeout(d time.Duration) Option {
	return func(t *Transport) { t.writeTimeout = d }
}

// New returns the transport.
func New(opts ...Option) *Transport {
	t := &Transport{
		readLimit:    DefaultReadLimit,
		pongTimeout:  DefaultPongTimeout,
		writeTimeout: DefaultWriteTimeout,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

var _ broadcast.Transport = (*Transport)(nil)

func (t *Transport) Name() string { return Name }

// Accept completes the handshake.
//
// Origin verification is the library's, left on: Accept refuses a cross-origin
// request unless WithOriginPatterns names the origin. That check is this
// transport's only defence against a cookie-authenticated handshake from
// somebody else's page, because a WebSocket is not subject to CORS.
func (t *Transport) Accept(w http.ResponseWriter, r *http.Request) (broadcast.Conn, error) {
	socket, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: t.originPatterns,
		// Compression off: a stream of small JSON frames mostly burns CPU
		// compressing, and sharing a compression context between a secret
		// and attacker-influenced data on one connection is a side channel.
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		// Accept has already written its own status, including the 403 for a
		// cross-origin handshake.
		return nil, fmt.Errorf("ws: %w", err)
	}
	socket.SetReadLimit(t.readLimit)

	conn := &Conn{
		socket:       socket,
		request:      r,
		done:         make(chan struct{}),
		commands:     make(chan broadcast.Command, 8),
		pongTimeout:  t.pongTimeout,
		writeTimeout: t.writeTimeout,
	}
	go conn.readLoop()
	return conn, nil
}

// Conn is one WebSocket connection.
type Conn struct {
	socket  *websocket.Conn
	request *http.Request

	pongTimeout  time.Duration
	writeTimeout time.Duration

	mu     sync.Mutex
	closed bool

	done     chan struct{}
	commands chan broadcast.Command
	once     sync.Once
}

var _ broadcast.Conn = (*Conn)(nil)

func (c *Conn) Protocol() string { return Name }

func (c *Conn) RemoteAddr() string { return c.request.RemoteAddr }

func (c *Conn) Capabilities() broadcast.Capabilities {
	return broadcast.Capabilities{
		// The client can subscribe and unsubscribe over the same connection,
		// which an event stream cannot.
		AcceptsCommands: true,

		// A ping is answered by a pong, so a heartbeat here is evidence the
		// peer is still there. On an event stream a write succeeds into the
		// kernel's send buffer long after the peer has gone.
		VerifiesLiveness: true,

		MaxFrameBytes: int(DefaultReadLimit),
	}
}

func (c *Conn) Commands() <-chan broadcast.Command { return c.commands }

func (c *Conn) Done() <-chan struct{} { return c.done }

// Send writes one frame as a text message.
func (c *Conn) Send(ctx context.Context, frame broadcast.Frame) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return broadcast.ErrClosed
	}

	body, err := json.Marshal(wireFrame{
		Kind:    frame.Kind,
		Channel: frame.Channel,
		Seq:     frame.Seq,
		Event:   wireEvent(frame.Event),
		Meta:    frame.Meta,
	})
	if err != nil {
		return fmt.Errorf("ws: encoding a frame: %w", err)
	}

	// The library manages its own deadlines on the hijacked connection, so
	// this is a context rather than a SetWriteDeadline — and it is why this
	// transport is unaffected by http.Server.WriteTimeout.
	writeCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()
	return c.socket.Write(writeCtx, websocket.MessageText, body)
}

// Ping sends a ping and waits for the pong.
//
// Waiting is the point: unlike an event stream, this proves the peer is
// still there rather than only that the kernel accepted some bytes.
func (c *Conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return broadcast.ErrClosed
	}

	pingCtx, cancel := context.WithTimeout(ctx, c.pongTimeout)
	defer cancel()
	return c.socket.Ping(pingCtx)
}

// Close sends a final frame saying why, then closes the socket.
//
// The reason travels twice: as a close frame the client can parse, and in the
// WebSocket close reason. The first is the one with structure — the protocol
// caps a close reason at 123 bytes, which is not enough for a code, a
// message, a retry delay and a resync flag.
func (c *Conn) Close(reason broadcast.CloseReason) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// Best effort, with a short deadline: the peer may already be gone,
	// which is the ordinary case and not worth waiting on.
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if body, err := json.Marshal(reason); err == nil {
		frame, err := json.Marshal(wireFrame{Kind: broadcast.FrameClose, Meta: body})
		if err == nil {
			c.socket.Write(closeCtx, websocket.MessageText, frame)
		}
	}

	status := websocket.StatusNormalClosure
	if reason.Code == broadcast.CloseGoingAway {
		status = websocket.StatusGoingAway
	}
	// Truncated to what the protocol allows, so a long message does not turn
	// a clean close into a protocol error.
	err := c.socket.Close(status, truncateReason(reason.Code))
	c.finish()
	return err
}

// readLoop reads commands and is this connection's liveness signal.
//
// A WebSocket's death is learned here, from a failed read — not from the
// request context, which accepting the connection hijacked and which the
// library's own documentation warns misbehaves afterwards. That is the
// concrete reason Conn.Done exists on the seam at all.
func (c *Conn) readLoop() {
	defer c.finish()

	for {
		kind, data, err := c.socket.Read(context.Background())
		if err != nil {
			return
		}
		if kind != websocket.MessageText {
			continue
		}

		var command broadcast.Command
		if err := json.Unmarshal(data, &command); err != nil {
			// Version skew, not an attack: a client sending something
			// unrecognised keeps its connection.
			continue
		}
		select {
		case c.commands <- command:
		case <-c.done:
			return
		default:
			// The hub is not draining commands, which means it is busy
			// rather than broken. Dropping a subscribe is visible to the
			// client, which never sees it in its ready or error frames.
		}
	}
}

// finish closes done exactly once, whichever path got here first.
func (c *Conn) finish() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
	})
}

func truncateReason(code string) string {
	const maxWebSocketReason = 123
	if len(code) <= maxWebSocketReason {
		return code
	}
	return code[:maxWebSocketReason]
}

// wireFrame is the JSON shape of a frame, identical to the event-stream
// transport's so one client parser serves both.
type wireFrame struct {
	Kind    broadcast.FrameKind `json:"kind"`
	Channel broadcast.Channel   `json:"channel,omitempty"`
	Seq     uint64              `json:"seq"`
	Event   wireEvent           `json:"event,omitempty"`
	Meta    json.RawMessage     `json:"meta,omitempty"`
}

type wireEvent broadcast.Event

func (e wireEvent) MarshalJSON() ([]byte, error) {
	if e.Name == "" {
		return []byte("null"), nil
	}
	type wire struct {
		Name string          `json:"name"`
		Data json.RawMessage `json:"data,omitempty"`
		ID   string          `json:"id,omitempty"`
		At   string          `json:"at,omitempty"`
	}
	out := wire{Name: e.Name, Data: e.Data, ID: e.ID}
	if !e.At.IsZero() {
		out.At = e.At.UTC().Format(time.RFC3339Nano)
	}
	return json.Marshal(out)
}

// ErrNotWebSocket reports a handshake this transport refused.
var ErrNotWebSocket = errors.New("ws: not a WebSocket handshake")
