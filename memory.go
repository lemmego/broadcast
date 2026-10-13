package broadcast

import (
	"context"
	"log/slog"
	"sync"
)

// memoryBus carries events between subscribers in one process.
//
// It is the default, and a single binary with no Redis is a first-class mode
// rather than a degraded one: for a deployment that runs one process, every
// subscriber is local and a network bus would add a dependency and a failure
// mode for nothing.
type memoryBus struct {
	mu          sync.RWMutex
	closed      bool
	nextID      int
	subscribers map[Channel]map[int]*memorySubscriber
}

// memorySubscriber owns the one close of its delivery channel.
//
// Both stop and Close end a subscription, and closing an already-closed
// channel panics — so the close goes through a once that both paths share
// rather than through a once held by only one of them. The first version had
// a once inside stop alone, which panicked whenever Close ran first.
type memorySubscriber struct {
	deliveries chan Delivery
	once       sync.Once
}

func (s *memorySubscriber) close() {
	s.once.Do(func() { close(s.deliveries) })
}

// NewMemoryBus returns a bus that reaches only this process.
func NewMemoryBus() Bus {
	return &memoryBus{subscribers: map[Channel]map[int]*memorySubscriber{}}
}

// memoryBufferSize is how many events one subscriber may fall behind by.
//
// A hub subscriber forwards into its own per-connection queue and does not
// block on a client, so falling behind here means the hub itself is stuck —
// which is a bug rather than a slow network. Hence the generous buffer and
// the loud complaint when it fills.
const memoryBufferSize = 64

func (b *memoryBus) Publish(_ context.Context, channel Channel, event Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrClosed
	}
	for _, subscriber := range b.subscribers[channel] {
		select {
		case subscriber.deliveries <- Delivery{Channel: channel, Event: event}:
		default:
			// Counted and complained about, never silent. A full buffer here
			// is the hub failing to drain, and the only thing worse than
			// dropping the event is dropping it quietly.
			slog.Error("broadcast: a local subscriber is not draining, so an "+
				"event was dropped",
				"channel", channel, "event", event.Name,
				"buffer", memoryBufferSize)
		}
	}
	return nil
}

func (b *memoryBus) Subscribe(ctx context.Context, channel Channel) (<-chan Delivery, func(), error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, nil, ErrClosed
	}

	b.nextID++
	id := b.nextID
	subscriber := &memorySubscriber{deliveries: make(chan Delivery, memoryBufferSize)}
	if b.subscribers[channel] == nil {
		b.subscribers[channel] = map[int]*memorySubscriber{}
	}
	b.subscribers[channel][id] = subscriber
	b.mu.Unlock()

	stop := func() {
		b.mu.Lock()
		if subscribers, ok := b.subscribers[channel]; ok {
			delete(subscribers, id)
			if len(subscribers) == 0 {
				delete(b.subscribers, channel)
			}
		}
		b.mu.Unlock()
		// Closed on the way out, so a subscriber's range ends rather than
		// blocking forever on a channel nobody will write to.
		subscriber.close()
	}

	// The context is honoured here rather than left to the caller, so a
	// cancelled request cannot leak a subscription.
	go func() {
		<-ctx.Done()
		stop()
	}()

	return subscriber.deliveries, stop, nil
}

func (b *memoryBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true
	for channel, subscribers := range b.subscribers {
		for id, subscriber := range subscribers {
			subscriber.close()
			delete(subscribers, id)
		}
		delete(b.subscribers, channel)
	}
	return nil
}
