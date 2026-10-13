// Package provider wires broadcast into a Lemmego application.
//
// It is a subpackage rather than part of broadcast, for the reason
// lemmego/log's 289 modules recorded: importing api/app pulls in api's own
// dependency closure, so a root package that offered the provider would
// charge every consumer for it — including lemmego/notify, which wants
// nothing but the Event type. cache put its provider in its root and
// consequently requires api; this does not.
package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lemmego/api/app"
	"github.com/lemmego/auth"

	"github.com/lemmego/broadcast"
	"github.com/lemmego/broadcast/transport/sse"
	"github.com/lemmego/broadcast/transport/ws"
)

// DefaultPrefix is where the endpoints mount.
const DefaultPrefix = "/broadcast"

// Provider mounts the realtime endpoints and wires the hub into the
// container.
type Provider struct {
	// Prefix is where the endpoints mount. Default "/broadcast".
	Prefix string

	// Transports to mount. Empty means both.
	Transports []string

	// AllowedOrigins is the cross-origin allowlist.
	//
	// Required: Provide returns an error when it is empty, because a
	// WebSocket is not subject to CORS and an unset allowlist is the
	// difference between a private channel and a public one. Pass
	// []string{"*"} to say so deliberately — it is one greppable string in
	// one configuration file, which is the point.
	AllowedOrigins []string

	// Bus is the bus to use. Nil means the in-process memory bus, which is a
	// supported mode for a single binary and not a degraded one.
	Bus broadcast.Bus

	// BusFor resolves the bus from the container, for a bus that needs
	// configuration or a connection the options cannot reach — which is
	// every bus that crosses a process boundary.
	//
	// A function rather than a value for the reason auth's SocialStore
	// records: a provider list is written before the container exists. It is
	// called once during Provide, and an error fails the boot — which is the
	// point. A bus that was asked for and cannot be reached must not quietly
	// become the in-process one.
	//
	// Takes precedence over Bus when both are set.
	BusFor func(a app.App) (broadcast.Bus, error)

	// DrainOnSignal installs its own SIGINT/SIGTERM watcher as a fallback.
	//
	// Needed only against an api that does not dispatch app.ServerStopping
	// before http.Server.Shutdown: a provider's own Shutdown runs *after*
	// Shutdown, far too late to release the connections holding it.
	// signal.Notify multicasts, so this does not steal the signal from the
	// application's own handler.
	DrainOnSignal bool

	// Config is passed to the hub, less the bus and authorizer, which this
	// provider supplies.
	Config broadcast.Config

	rules []rule
	hub   *broadcast.Hub
	once  sync.Once
}

var (
	_ app.Provider         = (*Provider)(nil)
	_ app.RouteProvider    = (*Provider)(nil)
	_ app.ShutdownProvider = (*Provider)(nil)
)

// authContext is what a channel rule is handed.
type authContext = app.Context

// Channel registers an authorization rule.
//
// The pattern is a literal channel name with {brace} placeholders, matched by
// exact segment on ".". The callback runs on the connection's own goroutine,
// once per channel per connection, and again for any channel added
// mid-connection — because a connection that can add channels later must not
// be able to add one it was never entitled to.
//
// It receives a real app.Context, so auth, the session and the container all
// work and auth.UserAs returns the project's own user type.
//
//	provider.Channel("private-post.{id}", func(c app.Context, p provider.Params) error {
//	    user, ok := auth.UserAs[*models.User](c)
//	    if !ok {
//	        return broadcast.ErrUnauthenticated
//	    }
//	    return policies.Post(c).ViewRealtime(user, p.Get("id"))
//	})
func (p *Provider) Channel(pattern string, authorize func(c app.Context, params Params) error) *Provider {
	p.rules = append(p.rules, rule{
		segments:  strings.Split(pattern, "."),
		authorize: authorize,
	})
	return p
}

// OwnSubject allows a channel whose {subject} placeholder is the signed-in
// user's own id.
//
// The comparison is against the string auth.UserID returns, which is the
// string UserProvider.GetID returns even where the column is an integer.
// Comparing an integer to a string with == is the mistake this helper exists
// to prevent.
func OwnSubject(c app.Context, params Params) error {
	subject, ok := auth.UserID(c)
	if !ok || subject == "" {
		return broadcast.ErrUnauthenticated
	}
	if params.Get("subject") != subject {
		return broadcast.ErrForbidden
	}
	return nil
}

func (p *Provider) Provide(a app.App) error {
	if len(p.AllowedOrigins) == 0 {
		return errors.New("broadcast: AllowedOrigins is required. A WebSocket " +
			"is not subject to CORS, so without an allowlist any page on the " +
			"internet can open an authenticated connection with the viewer's " +
			"cookies. Name your own origins, or pass []string{\"*\"} to say " +
			"deliberately that you mean any")
	}
	if p.Prefix == "" {
		p.Prefix = DefaultPrefix
	}
	if len(p.Transports) == 0 {
		p.Transports = []string{sse.Name, ws.Name}
	}

	// Installed by default, so private-user.<me> works with no configuration
	// — which is what lemmego/notify publishes to.
	p.Channel("private-user.{subject}", OwnSubject)

	bus, err := p.resolveBus(a)
	if err != nil {
		return err
	}

	cfg := p.Config
	cfg.Bus = bus
	cfg.Authorizer = p.authorizer(a)
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	p.hub = broadcast.New(cfg)

	a.AddService(p.hub)

	if bus == nil {
		slog.Info("broadcast: using the in-process bus — events reach only " +
			"the connections this process holds")
	}

	// Drained when the server starts stopping, not when providers shut down:
	// a provider's Shutdown runs after http.Server.Shutdown, which waits for
	// active requests, and an open stream is an active request for as long as
	// somebody has the page open.
	a.On(app.ServerStopping, func(any) error {
		p.drain()
		return nil
	})
	if p.DrainOnSignal {
		p.watchSignals()
	}

	return nil
}

// resolveBus builds the bus and proves it is reachable.
//
// A bus that implements Pinger is pinged here, so a Redis that was configured
// and is unreachable fails the boot. That is the whole reason this is not a
// plain field: a failure has to be able to stop the process from serving,
// and a value assembled before the container exists has nowhere to report
// one.
func (p *Provider) resolveBus(a app.App) (broadcast.Bus, error) {
	bus := p.Bus
	if p.BusFor != nil {
		resolved, err := p.BusFor(a)
		if err != nil {
			return nil, fmt.Errorf("broadcast: the bus could not be built: %w", err)
		}
		bus = resolved
	}
	if bus == nil {
		return nil, nil
	}

	if pinger, ok := bus.(broadcast.Pinger); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pinger.Ping(ctx); err != nil {
			return nil, fmt.Errorf("broadcast: the configured bus is "+
				"unreachable, so this process refuses to start rather than "+
				"falling back to an in-process bus that would lose every "+
				"event meant for another replica: %w", err)
		}
	}
	return bus, nil
}

// authorizer turns the registered rules into the hub's Authorizer.
func (p *Provider) authorizer(a app.App) broadcast.Authorizer {
	// Resolved once, by lookup rather than by auth.Get, which panics when
	// nothing is registered. An application with no auth provider — a
	// public dashboard, a test — must not panic on every subscribe, and
	// auth.Check reaches auth.Get internally.
	_, hasAuth := app.Lookup[*auth.Auth](a)
	if !hasAuth {
		slog.Warn("broadcast: no auth provider is registered, so every " +
			"channel rule sees an anonymous visitor")
	}

	return broadcast.AuthorizerFunc(func(_ context.Context, r *http.Request, channel broadcast.Channel) error {
		// A real context, so the rule can reach auth, the session and the
		// container. The writer is a discard: a rule answers, it does not
		// respond.
		c := app.NewContext(a, discardWriter{}, r)
		if hasAuth {
			if err := auth.Check(c); err != nil {
				// Not fatal: a rule may legitimately allow an anonymous
				// subscriber. The rule decides.
				slog.Debug("broadcast: no authenticated user on a subscribe",
					"channel", channel, "error", err)
			}
		}

		for _, one := range p.rules {
			params, ok := match(one.segments, string(channel))
			if !ok {
				continue
			}
			return one.authorize(c, params)
		}

		// No rule matched. Refused, and refused the same way as "not yours",
		// because a difference between "no such channel" and "not for you" is
		// a channel-enumeration oracle.
		return broadcast.ErrForbidden
	})
}

// AddRoutes mounts the endpoints as framework routes.
//
// Framework routes rather than Handle, so the session middleware and the
// application's handler chain run and auth.Check sees a loaded session. A raw
// mount bypasses the chain entirely, which is the queue dashboard's situation
// and the reason it has to build its own context and call its own guard.
func (p *Provider) AddRoutes() app.RouteCallback {
	return func(a app.App) {
		router := a.Router()
		for _, name := range p.Transports {
			switch name {
			case sse.Name:
				router.Get(p.Prefix+"/sse", p.handler(sse.New()))
			case ws.Name:
				router.Get(p.Prefix+"/ws", p.handler(ws.New(
					ws.WithOriginPatterns(p.originPatterns()...))))
			}
		}
		router.Get(p.Prefix+"/stats", p.stats)
	}
}

// originPatterns renders AllowedOrigins for the ws transport.
//
// "*" is dropped rather than passed through: the library refuses it as a
// pattern and says to use its insecure flag instead, so letting it through
// would turn a deliberate configuration into a confusing boot error. Our own
// check above has already allowed the request by then.
func (p *Provider) originPatterns() []string {
	out := make([]string, 0, len(p.AllowedOrigins))
	for _, origin := range p.AllowedOrigins {
		if origin == "*" {
			continue
		}
		out = append(out, originHost(origin))
	}
	return out
}

// originHost reduces an allowlist entry to the form a WebSocket handshake is
// matched against, which is the Origin header's host and not a URL.
//
// This conversion is the function's whole reason to exist. coder/websocket
// runs filepath.Match(pattern, u.Host), so an entry carrying a scheme is
// compared against "productshore.com" as "https://productshore.com" and can
// never match — while this package's own checkOrigin accepts either form. So
// a cross-origin allowance written the obvious way, by copying APP_URL, was
// accepted by the first check and silently refused by the second.
//
// Silent in the worst way, too: Accept short-circuits when Origin's host
// equals r.Host, so same-origin connections worked and nothing surfaced until
// somebody configured the cross-origin case the field exists for.
//
// A glob is left alone. url.Parse reads "*.productshore.com" as a path with
// no host, so the scheme test comes first; coder/websocket does support that
// pattern, which is more than checkOrigin's exact comparison allows, and the
// asymmetry is recorded in the README rather than smoothed over here.
func originHost(origin string) string {
	if !strings.Contains(origin, "://") {
		return strings.TrimSuffix(origin, "/")
	}
	if parsed, err := url.Parse(origin); err == nil && parsed.Host != "" {
		return parsed.Host
	}
	// Unparseable, so forward it verbatim and let the handshake refuse it.
	// Dropping it would widen the allowlist by making the entry disappear.
	return origin
}

// handler authorizes, then accepts, then serves.
func (p *Provider) handler(transport broadcast.Transport) app.Handler {
	return func(c app.Context) error {
		r := c.Request()
		w := c.ResponseWriter()

		if err := checkOrigin(r, p.AllowedOrigins); err != nil {
			slog.Warn("broadcast: a connection was refused on its origin",
				"error", err, "remote", r.RemoteAddr)
			return c.Forbidden(err)
		}

		channels := requestedChannels(r)
		granted, refused, err := p.hub.Authorize(r.Context(), r, channels)
		if err != nil {
			// A status, before the transport commits to its protocol. A
			// browser gets almost nothing from a connection that was
			// accepted and then closed, and cannot tell it from a flaky
			// network — so it would reconnect forever.
			switch {
			case errors.Is(err, broadcast.ErrUnauthenticated):
				return c.Unauthorized(err)
			case errors.Is(err, broadcast.ErrForbidden):
				return c.Forbidden(err)
			case errors.Is(err, broadcast.ErrTooManyChannels),
				errors.Is(err, broadcast.ErrTooManyConnections):
				w.Header().Set("Retry-After", "5")
				http.Error(w, "too many", http.StatusTooManyRequests)
				return nil
			case errors.Is(err, broadcast.ErrClosed):
				w.Header().Set("Retry-After", "1")
				http.Error(w, "shutting down", http.StatusServiceUnavailable)
				return nil
			default:
				return c.InternalServerError(err)
			}
		}

		conn, err := transport.Accept(w, r)
		if err != nil {
			// Accept has written its own status.
			slog.Debug("broadcast: a connection was not accepted", "error", err)
			return nil
		}

		return p.hub.Serve(r.Context(), conn, r, granted, refused)
	}
}

// stats answers with counts and no channel names.
//
// Enumerating the names would publish who is connected and what they are
// watching, which is a privacy leak wearing a diagnostic's clothes.
func (p *Provider) stats(c app.Context) error {
	// Guarded the same way, and for the same reason, as the authorizer.
	if _, hasAuth := app.Lookup[*auth.Auth](c.App()); hasAuth {
		if err := auth.Check(c); err != nil {
			return c.Unauthorized(err)
		}
	}
	stats := p.hub.Stats()
	return c.JSON(app.M{
		"connections":    stats.Connections,
		"sse":            stats.BySSE,
		"websocket":      stats.ByWebSocket,
		"channels":       stats.Channels,
		"frames_sent":    stats.FramesSent,
		"frames_dropped": stats.FramesDropped,
		"publish_errors": stats.PublishErrors,
		"draining":       stats.Draining,
	})
}

// Hub exposes the hub, for a test or a command.
func (p *Provider) Hub() *broadcast.Hub { return p.hub }

// Shutdown drains and releases the bus.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.hub == nil {
		return nil
	}
	if err := p.hub.Drain(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return p.hub.Close()
}

func (p *Provider) drain() {
	p.once.Do(func() {
		if p.hub == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.hub.Drain(ctx); err != nil {
			slog.Warn("broadcast: draining did not finish", "error", err)
		}
	})
}

func (p *Provider) watchSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		p.drain()
	}()
}

// Get resolves the broadcaster from the container. This is what
// lemmego/notify's broadcast channel is pointed at.
func Get(a app.App) (broadcast.Broadcaster, bool) {
	hub, ok := app.Lookup[*broadcast.Hub](a)
	if !ok {
		return nil, false
	}
	return hub, true
}

func requestedChannels(r *http.Request) []broadcast.Channel {
	raw := r.URL.Query()["channel"]
	channels := make([]broadcast.Channel, 0, len(raw))
	for _, one := range raw {
		channels = append(channels, broadcast.Channel(one))
	}
	return channels
}

// discardWriter satisfies http.ResponseWriter for a context built only to
// answer a question.
type discardWriter struct{}

func (discardWriter) Header() http.Header         { return http.Header{} }
func (discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (discardWriter) WriteHeader(int)             {}
