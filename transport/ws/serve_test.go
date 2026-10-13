package ws_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lemmego/broadcast"
)

// serve mounts a transport behind a hub on a real listener, for the cases
// that are specific to this transport rather than part of the shared
// contract.
func serve(t *testing.T, transport broadcast.Transport) (string, *broadcast.Hub) {
	t.Helper()

	hub := broadcast.New(broadcast.Config{
		Authorizer: broadcast.AuthorizerFunc(
			func(_ context.Context, _ *http.Request, ch broadcast.Channel) error {
				if ch == "private-forbidden.1" {
					return broadcast.ErrForbidden
				}
				return nil
			}),
		Heartbeat: time.Hour,
	})
	t.Cleanup(func() { hub.Close() })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channels := make([]broadcast.Channel, 0)
		for _, one := range r.URL.Query()["channel"] {
			channels = append(channels, broadcast.Channel(one))
		}

		granted, refused, err := hub.Authorize(r.Context(), r, channels)
		if err != nil {
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

		conn, err := transport.Accept(w, r)
		if err != nil {
			return
		}
		hub.Serve(r.Context(), conn, r, granted, refused)
	}))
	t.Cleanup(server.Close)

	return server.URL, hub
}
