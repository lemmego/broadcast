package broadcast

import (
	"context"
	"net/http"
)

// Authorizer decides whether a request may subscribe to a channel.
//
// It runs once per channel per connection, after the transport has
// authenticated the request and before the transport has committed to the
// protocol — before a WebSocket handshake is answered and before an event
// stream's first byte. That ordering is the point; see Transport.Accept.
//
// Return nil to allow, ErrUnauthenticated for "sign in", ErrForbidden for
// "not yours". Any other error is a 500 and is logged: an authorizer that
// could not reach the database has not decided, and a failure to decide must
// not read as a denial the client will retry forever.
type Authorizer interface {
	Authorize(ctx context.Context, r *http.Request, ch Channel) error
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context, r *http.Request, ch Channel) error

func (f AuthorizerFunc) Authorize(ctx context.Context, r *http.Request, ch Channel) error {
	return f(ctx, r, ch)
}

// DenyAll refuses every channel.
//
// It is the zero-value behaviour of Config, because a hub with no authorizer
// that defaulted to allow would be an open read channel onto every private
// channel name an attacker can guess — and the symptom of that is nothing at
// all.
func DenyAll() Authorizer {
	return AuthorizerFunc(func(context.Context, *http.Request, Channel) error {
		return ErrForbidden
	})
}
