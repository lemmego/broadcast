// Package redis carries broadcast events between processes over Redis
// pub/sub.
//
// It is the bus for a deployment with more than one replica. One process is
// better served by the default in-process bus: it needs no Redis, and a
// single binary with no Redis is a supported mode rather than a degraded one.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"

	"github.com/lemmego/broadcast"
)

// Mode selects how this process subscribes.
type Mode int

const (
	// PatternSubscribe opens one subscription for the whole process and
	// routes incoming messages to local subscribers by name.
	//
	// The default, and for a per-user channel scheme the only workable
	// choice: go-redis dedicates a connection to each subscription for its
	// lifetime, so PerChannel with ten thousand connected users would hold
	// ten thousand Redis connections. The trade-off is that every process
	// receives every event on the bus and discards what it does not hold,
	// which costs CPU and bandwidth proportional to the total event rate
	// rather than to this process's share.
	PatternSubscribe Mode = iota

	// PerChannel opens one subscription per distinct channel.
	//
	// Right only for a few large channels. See above for why it is not the
	// default.
	PerChannel
)

// DefaultPoolSize is small on purpose: this client publishes and holds
// subscriptions, and nothing else.
const DefaultPoolSize = 4

// Config configures a Bus.
type Config struct {
	// Client is an existing client to use. When nil, one is built from Addr.
	//
	// Passing the application's cache client is possible and is a bad idea:
	// go-redis dedicates a connection to each subscription for its lifetime
	// and does not return it to the pool, so a bus sharing the cache's
	// client eats the cache's connections — and the symptom is the *cache*
	// blocking on a read. A pub/sub consumer owns its own pool.
	Client goredis.UniversalClient

	Addr     string
	Username string
	Password string
	DB       int
	PoolSize int

	// Prefix namespaces this application's channels. Required, with no
	// default.
	//
	// Redis pub/sub is not scoped to the selected database — a subscriber on
	// db 0 receives a publish made on db 5 — so DB provides no isolation
	// whatsoever and the namespace has to be in the name. Two applications
	// sharing a Redis with no prefix would deliver each other's events.
	Prefix string

	Mode   Mode
	Logger *slog.Logger
}

// Bus carries events between processes.
type Bus struct {
	client goredis.UniversalClient
	owned  bool // closed by Close only if this package opened it
	prefix string
	mode   Mode
	logger *slog.Logger

	mu          sync.Mutex
	closed      bool
	nextID      int
	subscribers map[broadcast.Channel]map[int]*subscriber
	pattern     *goredis.PubSub
	perChannel  map[broadcast.Channel]*goredis.PubSub
	routing     sync.WaitGroup
}

// subscriber owns the one close of its delivery channel, because both a stop
// and a bus-wide failure end a subscription and closing twice panics.
type subscriber struct {
	deliveries chan broadcast.Delivery
	once       sync.Once
}

func (s *subscriber) close() {
	s.once.Do(func() { close(s.deliveries) })
}

// bufferSize is how far one subscriber may fall behind.
//
// A hub forwarder moves events into its own per-connection queue and never
// blocks on a client, so falling behind here means the forwarder is stuck —
// which the hub treats as a reason to close that connection.
const bufferSize = 64

var _ broadcast.Bus = (*Bus)(nil)

// New builds a bus.
func New(cfg Config) (*Bus, error) {
	if cfg.Prefix == "" {
		return nil, errors.New("broadcast/redis: a Prefix is required — " +
			"pub/sub ignores the selected database, so two applications " +
			"sharing one Redis with no prefix deliver each other's events")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	bus := &Bus{
		prefix:      cfg.Prefix,
		mode:        cfg.Mode,
		logger:      cfg.Logger,
		subscribers: map[broadcast.Channel]map[int]*subscriber{},
		perChannel:  map[broadcast.Channel]*goredis.PubSub{},
	}

	switch {
	case cfg.Client != nil:
		bus.client = cfg.Client
	case cfg.Addr != "":
		poolSize := cfg.PoolSize
		if poolSize <= 0 {
			poolSize = DefaultPoolSize
		}
		bus.client = goredis.NewClient(&goredis.Options{
			Addr:     cfg.Addr,
			Username: cfg.Username,
			Password: cfg.Password,
			DB:       cfg.DB,
			PoolSize: poolSize,
		})
		bus.owned = true
	default:
		return nil, errors.New("broadcast/redis: New needs a Client or an Addr")
	}

	return bus, nil
}

// Ping reports whether Redis is reachable.
//
// Called at boot so a bus that was explicitly configured and cannot be
// reached fails the boot, rather than falling back to an in-process bus that
// passes staging and loses half the events in production with two replicas.
func (b *Bus) Ping(ctx context.Context) error {
	if err := b.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: %w", broadcast.ErrBusUnavailable, err)
	}
	return nil
}

// wireChannel is the Redis channel one broadcast channel maps to.
func (b *Bus) wireChannel(channel broadcast.Channel) string {
	return b.prefix + ":bc:" + string(channel)
}

// broadcastChannel is the reverse, for a message arriving on a pattern.
func (b *Bus) broadcastChannel(wire string) (broadcast.Channel, bool) {
	prefix := b.prefix + ":bc:"
	if !strings.HasPrefix(wire, prefix) {
		return "", false
	}
	return broadcast.Channel(strings.TrimPrefix(wire, prefix)), true
}

func (b *Bus) Publish(ctx context.Context, channel broadcast.Channel, event broadcast.Event) error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return broadcast.ErrClosed
	}

	body, err := json.Marshal(envelope{
		Name: event.Name,
		Data: event.Data,
		ID:   event.ID,
		At:   event.At,
	})
	if err != nil {
		return fmt.Errorf("broadcast/redis: encoding an event: %w", err)
	}

	if err := b.client.Publish(ctx, b.wireChannel(channel), body).Err(); err != nil {
		return fmt.Errorf("%w: %w", broadcast.ErrBusUnavailable, err)
	}
	return nil
}

func (b *Bus) Subscribe(ctx context.Context, channel broadcast.Channel) (<-chan broadcast.Delivery, func(), error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, nil, broadcast.ErrClosed
	}

	b.nextID++
	id := b.nextID
	sub := &subscriber{deliveries: make(chan broadcast.Delivery, bufferSize)}
	if b.subscribers[channel] == nil {
		b.subscribers[channel] = map[int]*subscriber{}
	}
	b.subscribers[channel][id] = sub

	// Started once, lazily, so a process that never subscribes holds no
	// subscription connection at all.
	var startErr error
	if b.mode == PatternSubscribe {
		startErr = b.ensurePattern()
	} else {
		startErr = b.ensureChannel(channel)
	}
	b.mu.Unlock()

	if startErr != nil {
		b.remove(channel, id)
		sub.close()
		return nil, nil, startErr
	}

	stop := func() {
		b.remove(channel, id)
		sub.close()
	}

	// Honoured here rather than left to the caller, so a cancelled request
	// cannot leak a subscription.
	go func() {
		<-ctx.Done()
		stop()
	}()

	return sub.deliveries, stop, nil
}

// ensurePattern opens the one process-wide subscription. Called with the lock.
func (b *Bus) ensurePattern() error {
	if b.pattern != nil {
		return nil
	}

	pattern := b.prefix + ":bc:*"
	pubsub := b.client.PSubscribe(context.Background(), pattern)
	// Receive forces the subscription to be established now, so a Redis that
	// is unreachable is reported to the caller subscribing rather than
	// discovered later as a channel that never delivers.
	if _, err := pubsub.Receive(context.Background()); err != nil {
		pubsub.Close()
		return fmt.Errorf("%w: subscribing to %s: %w",
			broadcast.ErrBusUnavailable, pattern, err)
	}

	b.pattern = pubsub
	b.routing.Add(1)
	go b.route(pubsub)
	return nil
}

// ensureChannel opens one subscription for one channel. Called with the lock.
func (b *Bus) ensureChannel(channel broadcast.Channel) error {
	if _, ok := b.perChannel[channel]; ok {
		return nil
	}

	wire := b.wireChannel(channel)
	pubsub := b.client.Subscribe(context.Background(), wire)
	if _, err := pubsub.Receive(context.Background()); err != nil {
		pubsub.Close()
		return fmt.Errorf("%w: subscribing to %s: %w",
			broadcast.ErrBusUnavailable, wire, err)
	}

	b.perChannel[channel] = pubsub
	b.routing.Add(1)
	go b.route(pubsub)
	return nil
}

// route moves messages from one Redis subscription to local subscribers.
//
// It reads with Receive rather than ranging over Channel, and that is the
// whole of this function's design. Channel reconnects and re-subscribes on
// its own and never closes — convenient for a cache, wrong here: events
// published while the connection was down are gone, and a subscriber left
// open would receive nothing with no way to know. The first version of this
// used Channel, and the test for losing Redis caught it sitting silent for
// five seconds, which is the exact failure this package exists to avoid.
//
// Receive returns an error when the connection fails, so a loss is an event
// rather than an absence. A re-subscription is treated the same way, because
// it means the same thing: there is a gap, so every subscriber is closed and
// the Hub turns that into a connection close telling the client to re-fetch.
func (b *Bus) route(pubsub *goredis.PubSub) {
	defer b.routing.Done()

	established := false
	for {
		received, err := pubsub.Receive(context.Background())
		if err != nil {
			b.mu.Lock()
			closed := b.closed
			b.mu.Unlock()
			if !closed {
				b.logger.Error("broadcast/redis: the subscription failed, so "+
					"every subscriber is being closed rather than left "+
					"receiving nothing", "error", err)
				b.closeAllSubscribers()
			}
			return
		}

		switch typed := received.(type) {
		case *goredis.Subscription:
			switch typed.Kind {
			case "subscribe", "psubscribe":
				if established {
					b.logger.Warn("broadcast/redis: the subscription was "+
						"re-established, so events published while it was "+
						"down are gone; subscribers are being closed to make "+
						"their clients re-fetch", "channel", typed.Channel)
					b.closeAllSubscribers()
				}
				established = true
			case "unsubscribe", "punsubscribe":
				// Our own teardown, or Redis finishing the subscription.
				// Nothing more will arrive on it either way.
				return
			}

		case *goredis.Message:
			channel, ok := b.broadcastChannel(typed.Channel)
			if !ok {
				continue
			}
			var env envelope
			if err := json.Unmarshal([]byte(typed.Payload), &env); err != nil {
				b.logger.Warn("broadcast/redis: a message on the bus was not "+
					"an event and was ignored",
					"channel", channel, "error", err)
				continue
			}
			b.deliver(channel, broadcast.Event{
				Name: env.Name, Data: env.Data, ID: env.ID, At: env.At,
			})

		case *goredis.Pong:
			// A health check answering. Nothing to route.
		}
	}
}

func (b *Bus) deliver(channel broadcast.Channel, event broadcast.Event) {
	b.mu.Lock()
	subscribers := make([]*subscriber, 0, len(b.subscribers[channel]))
	for _, sub := range b.subscribers[channel] {
		subscribers = append(subscribers, sub)
	}
	b.mu.Unlock()

	for _, sub := range subscribers {
		select {
		case sub.deliveries <- broadcast.Delivery{Channel: channel, Event: event}:
		default:
			b.logger.Warn("broadcast/redis: a subscriber is not draining, so "+
				"an event was dropped", "channel", channel, "event", event.Name)
		}
	}
}

func (b *Bus) closeAllSubscribers() {
	b.mu.Lock()
	subscribers := []*subscriber{}
	for channel, set := range b.subscribers {
		for id, sub := range set {
			subscribers = append(subscribers, sub)
			delete(set, id)
		}
		delete(b.subscribers, channel)
	}
	b.mu.Unlock()

	for _, sub := range subscribers {
		sub.close()
	}
}

func (b *Bus) remove(channel broadcast.Channel, id int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	set, ok := b.subscribers[channel]
	if !ok {
		return
	}
	delete(set, id)
	if len(set) > 0 {
		return
	}
	delete(b.subscribers, channel)

	// In per-channel mode the Redis subscription goes with the last local
	// subscriber. In pattern mode there is one subscription for everything
	// and it stays.
	if b.mode == PerChannel {
		if pubsub, ok := b.perChannel[channel]; ok {
			delete(b.perChannel, channel)
			pubsub.Close()
		}
	}
}

func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	pattern := b.pattern
	b.pattern = nil
	perChannel := b.perChannel
	b.perChannel = map[broadcast.Channel]*goredis.PubSub{}
	b.mu.Unlock()

	if pattern != nil {
		pattern.Close()
	}
	for _, pubsub := range perChannel {
		pubsub.Close()
	}
	b.closeAllSubscribers()

	// Waited for, so a Close that returns has no routing goroutine left
	// writing to a subscriber.
	b.routing.Wait()

	if b.owned {
		return b.client.Close()
	}
	return nil
}

// envelope is what travels on the wire.
//
// Short keys because every subscriber in every process parses it, and the
// field names are not an API: the Event struct is.
type envelope struct {
	Name string          `json:"n"`
	Data json.RawMessage `json:"d,omitempty"`
	ID   string          `json:"i,omitempty"`
	At   time.Time       `json:"t,omitempty"`
}
