package broadcast

import (
	"context"
	"errors"
)

// Broadcaster publishes events.
//
// It is the whole of what a module that only produces events — lemmego/notify
// — needs to depend on.
type Broadcaster interface {
	// Publish delivers an event to every subscriber of a channel, in this
	// process and in every other process sharing the Bus.
	//
	// It returns once the event has been handed to local subscribers and
	// accepted by the bus. It does not wait for any client to receive it,
	// and a client too slow to receive is disconnected rather than waited
	// for. A nil error therefore means "accepted for delivery", never
	// "delivered" — and no API in this package ever means "delivered".
	//
	// An error wrapping ErrPartialPublish means local subscribers were
	// served and remote ones were not. It is not retried here: the caller
	// knows whether it sits somewhere that can retry — a queued job can, an
	// HTTP handler mostly cannot — and this package does not.
	Publish(ctx context.Context, channel Channel, event Event) error
}

// PublishFunc adapts a function to Broadcaster.
type PublishFunc func(ctx context.Context, channel Channel, event Event) error

func (f PublishFunc) Publish(ctx context.Context, ch Channel, e Event) error {
	return f(ctx, ch, e)
}

var (
	// ErrBusUnavailable reports that the cross-process bus could not be
	// reached.
	//
	// Never returned alone: it is wrapped by ErrPartialPublish or by a
	// subscription failure, so a caller can always tell which half failed.
	ErrBusUnavailable = errors.New("broadcast: bus unavailable")

	// ErrPartialPublish reports that local subscribers received an event and
	// remote ones did not.
	ErrPartialPublish = errors.New("broadcast: published locally only")

	// ErrUnauthenticated means "sign in". A transport turns it into 401.
	ErrUnauthenticated = errors.New("broadcast: unauthenticated")

	// ErrForbidden means "not yours". A transport turns it into 403, and
	// says nothing about whether the channel exists: a 403 and a 404 that
	// differ are a channel-enumeration oracle.
	ErrForbidden = errors.New("broadcast: forbidden")

	// ErrTooManyChannels reports a connection asking for more channels than
	// it is allowed.
	ErrTooManyChannels = errors.New("broadcast: too many channels on one connection")

	// ErrTooManyConnections reports a process at its connection limit.
	ErrTooManyConnections = errors.New("broadcast: too many connections")

	// ErrClosed reports a hub that is shutting down or has shut down.
	ErrClosed = errors.New("broadcast: hub closed")
)
