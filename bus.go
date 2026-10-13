package broadcast

import "context"

// Bus carries published events between the processes that hold connections.
type Bus interface {
	// Publish hands an event to the bus.
	Publish(ctx context.Context, channel Channel, event Event) error

	// Subscribe delivers every event published on a channel by any process,
	// until the context is cancelled or stop is called.
	//
	// The returned channel is closed when the subscription ends, for any
	// reason — including the bus losing its backing. A bus that goes quiet
	// instead of closing is the failure this signature exists to make
	// impossible: a dead subscription and a channel with no traffic look
	// identical from the inside, which is the same mistake as a polling
	// stream that keeps answering after its source has gone.
	Subscribe(ctx context.Context, channel Channel) (deliveries <-chan Delivery, stop func(), err error)

	// Close releases the bus. A Hub closes the bus it was given only if it
	// opened it.
	Close() error
}

// Delivery is one event arriving from the bus.
type Delivery struct {
	Channel Channel
	Event   Event
}
