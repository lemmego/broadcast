package broadcast

import (
	"encoding/json"
	"time"
)

// Frame is the unit a transport carries: everything the client needs in order
// to route one message, with no transport in it.
type Frame struct {
	Kind    FrameKind
	Channel Channel
	Event   Event

	// Seq counts frames on this connection from 1.
	//
	// It lets a client detect a gap the server created on its behalf without
	// the server promising a resumable log it does not keep. Nothing creates
	// such a gap today — an overflowing queue closes the connection rather
	// than dropping a frame — but a lossy channel would, and the wire format
	// is the thing that is expensive to change later.
	Seq uint64

	// Meta is the body of a non-event frame: the ready handshake, a close
	// reason, a per-channel refusal.
	Meta json.RawMessage
}

// FrameKind says what a frame is.
type FrameKind string

const (
	// FrameReady is the first frame on every connection, written and flushed
	// before anything can block.
	//
	// Without it a client cannot tell "connected" from "hung", and the only
	// thing it can do about either is wait. tasker's dashboard stream has
	// this gap: it sends nothing until its first five-second tick.
	FrameReady FrameKind = "ready"

	// FrameEvent carries an application event.
	FrameEvent FrameKind = "event"

	// FrameError reports a per-subscription failure — one channel refused on
	// a connection that stays open with its others.
	FrameError FrameKind = "error"

	// FrameClose is the last frame, carrying the reason and whether coming
	// back can succeed.
	FrameClose FrameKind = "close"
)

// Ready is the body of a FrameReady.
type Ready struct {
	// Stream is an opaque id for this connection, for correlating a client
	// report with a server log.
	Stream string `json:"stream"`

	Protocol string `json:"protocol"`

	// ServerTime lets a client detect a badly wrong local clock, which is
	// otherwise diagnosed as "the timestamps are wrong".
	ServerTime time.Time `json:"server_time"`

	HeartbeatMS int `json:"heartbeat_ms"`

	// Channels are the channels this connection is subscribed to, and
	// Refused are the ones it asked for and did not get. Both are listed,
	// because a connection that silently carries fewer channels than it
	// asked for is a client waiting for events that will never arrive.
	Channels []Channel `json:"channels"`
	Refused  []Refusal `json:"refused,omitempty"`

	// Resumed reports whether anything was replayed. Always false today.
	Resumed bool `json:"resumed"`

	// Resync tells the client to re-fetch authoritative state.
	//
	// True whenever the server cannot promise nothing was missed — which,
	// with no backlog, is every reconnection. Claiming otherwise would be
	// the dishonest default: the client would believe it had resumed.
	Resync bool `json:"resync"`
}

// Refusal is one channel a connection asked for and did not get.
type Refusal struct {
	Channel Channel `json:"channel"`
	// Reason is a stable code, not prose: "forbidden", "unauthenticated",
	// "bad_name", "too_many". It deliberately does not distinguish "not
	// yours" from "does not exist", which would be a channel-enumeration
	// oracle.
	Reason string `json:"reason"`
}

// Close reason codes.
const (
	// CloseGoingAway is a graceful server shutdown.
	CloseGoingAway = "going_away"
	// CloseSlowConsumer is a client that stopped reading.
	CloseSlowConsumer = "slow_consumer"
	// CloseUnauthorized is a credential that is not good enough, and is the
	// one code that must not be retried.
	CloseUnauthorized = "unauthorized"
	// CloseBusUnavailable is a subscription that can no longer be guaranteed.
	CloseBusUnavailable = "bus_unavailable"
	// CloseIdle is a connection that went quiet in both directions.
	CloseIdle = "idle"
	// CloseServerError is a bug.
	CloseServerError = "server_error"
	// CloseClient is the client's own doing.
	CloseClient = "client"
)

// CloseReason is why a connection ended, in terms a client can act on.
type CloseReason struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`

	// Reconnect says whether coming back can succeed.
	//
	// False for CloseUnauthorized, because a client that reconnects into a
	// 401 forever is a self-inflicted denial of service — and the browser's
	// own EventSource reconnect loop will do exactly that unless told.
	Reconnect bool `json:"reconnect"`

	// After is the earliest the client should retry, before its own jitter.
	After time.Duration `json:"after_ms"`

	// Resync says events were missed and state must be re-fetched.
	Resync bool `json:"resync"`
}

// MarshalJSON renders After as whole milliseconds.
//
// A time.Duration marshals as an integer count of nanoseconds, which a
// JavaScript client would pass to setTimeout and wait a million times too
// long. The field is named after_ms, so it carries milliseconds.
func (r CloseReason) MarshalJSON() ([]byte, error) {
	type wire struct {
		Code      string `json:"code"`
		Message   string `json:"message,omitempty"`
		Reconnect bool   `json:"reconnect"`
		AfterMS   int64  `json:"after_ms"`
		Resync    bool   `json:"resync"`
	}
	return json.Marshal(wire{
		Code:      r.Code,
		Message:   r.Message,
		Reconnect: r.Reconnect,
		AfterMS:   r.After.Milliseconds(),
		Resync:    r.Resync,
	})
}

// CommandKind is what a client asked for.
type CommandKind string

const (
	CommandSubscribe   CommandKind = "subscribe"
	CommandUnsubscribe CommandKind = "unsubscribe"
	CommandPing        CommandKind = "ping"
)

// Command is a message from the client.
//
// The set is closed, and small on purpose: everything an application wants to
// *do* goes through its ordinary HTTP routes, which already have CSRF,
// validation and authorization. A realtime connection is for receiving.
type Command struct {
	Kind     CommandKind `json:"kind"`
	Channels []Channel   `json:"channels,omitempty"`
}
