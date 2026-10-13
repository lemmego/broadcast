// Package sse carries broadcast frames as server-sent events.
//
// Standard library only. The browser supplies the client, reconnects on its
// own, and needs no library — which is why this is the transport to prefer
// where it fits. What it cannot do is carry a message from the client, so a
// client changes its channel set by reconnecting; see Capabilities.
package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/lemmego/broadcast"
)

// Name is this transport's protocol name, as it appears in a ready frame.
const Name = "sse"

// DefaultRetry is the reconnect delay suggested to the browser.
const DefaultRetry = 3 * time.Second

// Transport accepts server-sent event connections.
type Transport struct {
	retry        time.Duration
	writeTimeout time.Duration
}

// Option configures a Transport.
type Option func(*Transport)

// WithRetry sets the retry hint sent before the first frame.
//
// It is written first, so the browser's reconnect delay is set before
// anything can go wrong — including before the connection fails.
func WithRetry(d time.Duration) Option {
	return func(t *Transport) { t.retry = d }
}

// WithWriteTimeout bounds one frame's write.
//
// Accept clears http.Server.WriteTimeout outright, which is what lets a
// stream outlive it; this then re-arms a deadline around each individual
// write, so a client that accepts the connection and stops reading at the TCP
// level is abandoned rather than holding a goroutine and a socket forever.
//
// That second case is a backstop, and the conformance suite does not isolate
// it: a stalled client fills the Hub's frame queue first and is closed as a
// slow consumer, which is the path the suite covers. Isolating this one needs
// a client that stops reading its socket while the queue stays under its
// bound — worth writing if this deadline is ever changed, and not worth
// pretending is covered now.
func WithWriteTimeout(d time.Duration) Option {
	return func(t *Transport) { t.writeTimeout = d }
}

// New returns the transport.
func New(opts ...Option) *Transport {
	t := &Transport{retry: DefaultRetry, writeTimeout: 10 * time.Second}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

var _ broadcast.Transport = (*Transport)(nil)

func (t *Transport) Name() string { return Name }

// Accept writes the stream preamble and returns the connection.
//
// It refuses rather than degrading when it cannot flush or cannot control its
// write deadline, and the refusal names the likely cause. A stream that
// cannot flush delivers nothing until a buffer fills; one that cannot set a
// deadline dies at http.Server.WriteTimeout. Both look like an application
// bug and neither is.
func (t *Transport) Accept(w http.ResponseWriter, r *http.Request) (broadcast.Conn, error) {
	controller := http.NewResponseController(w)

	// Cleared outright, because http.Server.WriteTimeout is a deadline on
	// the whole response and a stream has no end. Send re-arms a per-frame
	// deadline afterwards, so clearing this is not the same as having none.
	if err := controller.SetWriteDeadline(time.Time{}); err != nil {
		http.Error(w, "streaming is not available", http.StatusInternalServerError)
		return nil, fmt.Errorf("sse: this response cannot carry a stream: %w — "+
			"a middleware is probably wrapping http.ResponseWriter without an "+
			"Unwrap() http.ResponseWriter method", err)
	}

	// text/event-stream, and the three headers that make it survive a proxy.
	header := w.Header()
	header.Set("Content-Type", "text/event-stream; charset=utf-8")
	// no-transform stops an intermediary gzipping or re-chunking the stream.
	header.Set("Cache-Control", "no-cache, no-transform")
	// nginx buffers text/event-stream by default, so without this the stream
	// looks dead through a proxy and nowhere else.
	header.Set("X-Accel-Buffering", "no")
	if r.ProtoMajor == 1 {
		// Connection is a connection-specific header and is illegal over
		// HTTP/2, where net/http rejects it.
		header.Set("Connection", "keep-alive")
	}
	w.WriteHeader(http.StatusOK)

	conn := &Conn{
		writer:       w,
		controller:   controller,
		request:      r,
		done:         make(chan struct{}),
		commands:     closedCommands(),
		writeTimeout: t.writeTimeout,
	}

	// Written before anything can block, so "connected" is observable. The
	// retry hint goes first, and the comment forces the headers out even
	// through a client that buffers until it sees body bytes.
	if _, err := fmt.Fprintf(w, "retry: %d\n\n: connected\n\n", t.retry.Milliseconds()); err != nil {
		return nil, fmt.Errorf("sse: writing the preamble: %w", err)
	}
	if err := controller.Flush(); err != nil {
		return nil, fmt.Errorf("sse: this response cannot be flushed: %w — "+
			"a middleware is probably wrapping http.ResponseWriter without an "+
			"Unwrap() http.ResponseWriter method", err)
	}

	// The request context is this transport's death signal. It is enough
	// here, and is exactly what a hijacked WebSocket cannot use.
	go func() {
		<-r.Context().Done()
		conn.Close(broadcast.CloseReason{Code: broadcast.CloseClient})
	}()

	return conn, nil
}

// Conn is one server-sent event stream.
type Conn struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
	request    *http.Request

	writeTimeout time.Duration

	mu       sync.Mutex
	closed   bool
	done     chan struct{}
	commands <-chan broadcast.Command
}

var _ broadcast.Conn = (*Conn)(nil)

func (c *Conn) Protocol() string { return Name }

func (c *Conn) RemoteAddr() string { return c.request.RemoteAddr }

func (c *Conn) Capabilities() broadcast.Capabilities {
	return broadcast.Capabilities{
		// A browser EventSource has no way to send anything, so a client
		// changes its channel set by reconnecting with a different query
		// string — which costs one round trip during which an event can be
		// missed, and is why every ready frame asks the client to re-fetch.
		AcceptsCommands: false,

		// A write into the kernel's send buffer succeeds long after the peer
		// has gone, so a successful heartbeat here proves nothing. Death is
		// noticed when that buffer fills, which on a dropped wifi connection
		// can take minutes. The difference is real and unfixable.
		VerifiesLiveness: false,
	}
}

func (c *Conn) Commands() <-chan broadcast.Command { return c.commands }

func (c *Conn) Done() <-chan struct{} { return c.done }

// Send writes one frame.
//
// The whole frame travels as JSON on a single data: line, with the frame kind
// in the event: field. That is the design decision in this file: JSON escapes
// every control character, so no payload can contain the newline that ends a
// field or the blank line that ends a frame — which is the injection a
// line-oriented encoder has to get right and usually does not. The event:
// field carries only a value from a closed set of four constants.
func (c *Conn) Send(_ context.Context, frame broadcast.Frame) error {
	body, err := json.Marshal(wireFrame{
		Kind:    frame.Kind,
		Channel: frame.Channel,
		Seq:     frame.Seq,
		Event:   wireEvent(frame.Event),
		Meta:    frame.Meta,
	})
	if err != nil {
		return fmt.Errorf("sse: encoding a frame: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return broadcast.ErrClosed
	}

	if err := c.arm(); err != nil {
		return err
	}
	// One Fprintf, so a frame is written whole or not at all: a partial frame
	// leaves the stream unparseable, which the client cannot detect.
	if _, err := fmt.Fprintf(c.writer, "event: %s\ndata: %s\n\n", frame.Kind, body); err != nil {
		return err
	}
	return c.controller.Flush()
}

// Ping writes a comment line.
//
// A comment rather than an event, so it can never reach the client's message
// handler. A keepalive that arrived as an application event would have every
// client branching on an event the server never meant to send.
func (c *Conn) Ping(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return broadcast.ErrClosed
	}
	if err := c.arm(); err != nil {
		return err
	}
	if _, err := io.WriteString(c.writer, ": hb\n\n"); err != nil {
		return err
	}
	return c.controller.Flush()
}

// Close writes a final frame saying why, then ends the stream.
func (c *Conn) Close(reason broadcast.CloseReason) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)

	// Best effort: the peer may already be gone, which is the ordinary case
	// and not worth reporting.
	if body, err := json.Marshal(reason); err == nil {
		if c.arm() == nil {
			fmt.Fprintf(c.writer, "event: %s\ndata: %s\n\n", broadcast.FrameClose, body)
			c.controller.Flush()
		}
	}
	c.mu.Unlock()
	return nil
}

// arm re-arms the write deadline. Called with the lock held.
func (c *Conn) arm() error {
	if c.writeTimeout <= 0 {
		return nil
	}
	if err := c.controller.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		// Accept already proved this works, so a failure here is a surprise
		// worth reporting rather than ignoring.
		if errors.Is(err, http.ErrNotSupported) {
			return nil
		}
		return fmt.Errorf("sse: arming the write deadline: %w", err)
	}
	return nil
}

// closedCommands returns an already-closed channel.
//
// Closed rather than nil, because the Conn contract says so: a nil channel in
// a select blocks forever, and a transport handing one over would make that
// bug invisible. The Hub swaps it for nil itself, once, deliberately.
func closedCommands() <-chan broadcast.Command {
	ch := make(chan broadcast.Command)
	close(ch)
	return ch
}

// wireFrame is the JSON shape of a frame on this transport.
type wireFrame struct {
	Kind    broadcast.FrameKind `json:"kind"`
	Channel broadcast.Channel   `json:"channel,omitempty"`
	Seq     uint64              `json:"seq"`
	Event   wireEvent           `json:"event,omitempty"`
	Meta    json.RawMessage     `json:"meta,omitempty"`
}

type wireEvent broadcast.Event

// MarshalJSON omits an empty event rather than sending a shell of nulls.
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
