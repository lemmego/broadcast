package broadcast

import (
	"context"
	"net/http"
)

// Conn is one client connection as the Hub sees it: a wire protocol reduced
// to five verbs.
//
// Concurrency: exactly one goroutine calls Send and Ping, and the Hub
// guarantees it. Close may be called from any goroutine at any time,
// including concurrently with a Send; it must be idempotent, and it must
// cause an in-flight Send to return. A transport that cannot promise that
// cannot be shut down, which makes it unusable here.
type Conn interface {
	// Send writes one frame and flushes it.
	//
	// It writes the frame whole or returns an error. A partial frame leaves
	// the stream unparseable, which is worse than disconnected, because the
	// client cannot detect it.
	Send(ctx context.Context, f Frame) error

	// Ping emits whatever keeps an idle path open: a comment line on an
	// event stream, a ping frame on a WebSocket. On a transport that can
	// verify liveness it waits for the answer, and says so through
	// Capabilities.
	Ping(ctx context.Context) error

	// Commands returns inbound commands from the client.
	//
	// A transport with no client-to-server path returns a channel that is
	// already closed — never nil, because a nil channel in a select blocks
	// forever, and that bug is invisible in review and in a test that
	// passes.
	Commands() <-chan Command

	// Close ends the connection, telling the client why. Idempotent.
	Close(r CloseReason) error

	// Done is closed when the connection is gone, for any reason.
	//
	// The transport owns this rather than the Hub watching a request
	// context, and that is the concrete reason a seam fitted to one
	// transport would have been wrong. An event stream learns its client has
	// gone from the request context; a WebSocket cannot, because accepting
	// one hijacks the connection and the library's own documentation warns
	// that using the request context afterwards misbehaves. Its liveness
	// comes from a read loop and a pong instead. So "the connection is gone"
	// is the transport's answer to give, not the Hub's to infer.
	Done() <-chan struct{}

	Capabilities() Capabilities

	// RemoteAddr and Protocol are for logs, metrics and rate limits.
	RemoteAddr() string
	Protocol() string
}

// Capabilities declares what a transport's *client* can do.
//
// Nothing here excuses a transport from any part of Conn: every transport
// must frame, flush, heartbeat, report death and close. There is no
// capability flag for opting out of those, which is what stops this struct
// becoming a list of excuses.
type Capabilities struct {
	// AcceptsCommands: the client can subscribe and unsubscribe over the
	// same connection.
	//
	// False for server-sent events, whose client changes its channel set by
	// reconnecting with a different query string. The cost of that is one
	// round trip during which an event can be missed, which is why a
	// reconnection always sets Resync.
	AcceptsCommands bool

	// VerifiesLiveness: Ping proves the peer is still there.
	//
	// True for WebSocket, whose ping is answered by a pong. False for an
	// event stream, where a write succeeds into the kernel's send buffer
	// long after the peer has gone, so death is noticed only when that
	// buffer fills — minutes, on a dropped wifi connection. The difference
	// is real and unfixable, and the Hub uses this to decide whether a
	// missed heartbeat is evidence of anything.
	VerifiesLiveness bool

	// MaxFrameBytes is the largest frame this transport will carry, or 0 for
	// no limit of its own.
	MaxFrameBytes int
}

// Transport turns an HTTP request into a Conn.
type Transport interface {
	// Accept negotiates the protocol.
	//
	// It is called only after authorization has passed, so it may write a
	// success status. It must never be the thing that authorizes: a refusal
	// has to be an HTTP status, because a browser gets almost nothing out of
	// a connection that was accepted and then closed, and its reconnect
	// logic cannot tell that from a flaky network — so an
	// accept-then-close refusal is a reconnect loop against an endpoint that
	// will never admit it.
	//
	// On error it has written nothing to w, and the caller writes the status.
	Accept(w http.ResponseWriter, r *http.Request) (Conn, error)

	Name() string
}
