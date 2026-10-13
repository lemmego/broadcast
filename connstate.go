package broadcast

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// connState is one connection's bookkeeping, owned by the goroutine running
// Serve.
//
// Only channels and the queue are touched from elsewhere: a bus forwarder
// writes to the queue, and Drain closes drain. Everything else belongs to the
// pump loop, which is what lets the loop read seq without a lock.
type connState struct {
	conn    Conn
	request *http.Request
	stream  string
	seq     uint64

	queue chan Frame
	drain chan struct{}

	mu       sync.Mutex
	channels []Channel
}

func (s *connState) remember(channel Channel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.channels {
		if existing == channel {
			return
		}
	}
	s.channels = append(s.channels, channel)
}

func (s *connState) forget(channel Channel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, existing := range s.channels {
		if existing == channel {
			s.channels = append(s.channels[:i], s.channels[i+1:]...)
			return
		}
	}
}

func (s *connState) subscribed() []Channel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Channel(nil), s.channels...)
}

// withoutCommands wraps a Conn whose command channel has closed.
//
// A closed channel in a select is always ready, so a transport with no
// inbound path would spin the pump loop at the speed of the scheduler. This
// returns a nil channel instead, which blocks forever — the correct
// behaviour, and the reason Conn.Commands is documented to return a closed
// channel rather than a nil one: a nil channel handed over at the start would
// have made this bug invisible instead of noisy.
type withoutCommands struct{ Conn }

func (withoutCommands) Commands() <-chan Command { return nil }

// subscriptions holds one connection's bus subscriptions.
//
// One Subscribe per channel. A bus that wants to serve many connections from
// one network subscription routes internally — which is where that knowledge
// belongs, and why the redis bus pattern-subscribes once per process rather
// than once per user.
type subscriptions struct {
	hub   *Hub
	state *connState

	mu     sync.Mutex
	stops  map[Channel]func()
	closed bool
	wg     sync.WaitGroup
}

func (h *Hub) subscribe(ctx context.Context, state *connState) (*subscriptions, error) {
	subs := &subscriptions{hub: h, state: state, stops: map[Channel]func(){}}
	for _, channel := range state.subscribed() {
		if err := subs.add(ctx, channel); err != nil {
			subs.stop()
			return nil, err
		}
	}
	return subs, nil
}

func (s *subscriptions) add(ctx context.Context, channel Channel) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if _, already := s.stops[channel]; already {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	deliveries, stop, err := s.hub.cfg.Bus.Subscribe(ctx, channel)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		stop()
		return ErrClosed
	}
	s.stops[channel] = stop
	s.mu.Unlock()

	s.wg.Add(1)
	go s.forward(channel, deliveries)
	return nil
}

// forward moves one channel's deliveries into the connection's queue.
//
// On overflow the connection is closed rather than the frame dropped. A
// client that silently misses an event is worse than one that reconnects,
// because with no backlog the miss is unrecoverable and invisible — whereas a
// closed connection is an event the client already handles by reconnecting
// and re-fetching, which is a path that has to exist anyway for the
// laptop-wake case.
func (s *subscriptions) forward(channel Channel, deliveries <-chan Delivery) {
	defer s.wg.Done()

	for delivery := range deliveries {
		frame := Frame{Kind: FrameEvent, Channel: delivery.Channel, Event: delivery.Event}
		select {
		case s.state.queue <- frame:
		default:
			s.hub.framesDropped.Add(1)
			s.hub.cfg.Logger.Warn("broadcast: a connection is not keeping up, "+
				"so it is being closed rather than quietly missing events",
				"stream", s.state.stream, "channel", channel,
				"queue", s.hub.cfg.QueueSize)
			s.state.conn.Close(CloseReason{
				Code:      CloseSlowConsumer,
				Message:   "this connection fell behind",
				Reconnect: true,
				After:     2 * time.Second,
				Resync:    true,
			})
			return
		}
	}

	// The bus closed the subscription, which under the Bus contract means it
	// can no longer be guaranteed — not that the channel is quiet. An open
	// stream receiving nothing is the worst failure mode here, so the
	// connection goes rather than waiting silently.
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return
	}
	s.hub.cfg.Logger.Warn("broadcast: a subscription ended, so the connection "+
		"is being closed rather than left open receiving nothing",
		"stream", s.state.stream, "channel", channel)
	s.state.conn.Close(CloseReason{
		Code:      CloseBusUnavailable,
		Message:   "this subscription could no longer be guaranteed",
		Reconnect: true,
		After:     jitter(2*time.Second, 10*time.Second),
		Resync:    true,
	})
}

func (s *subscriptions) remove(channel Channel) {
	s.mu.Lock()
	stop, ok := s.stops[channel]
	delete(s.stops, channel)
	s.mu.Unlock()
	if ok {
		stop()
	}
}

func (s *subscriptions) stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	stops := make([]func(), 0, len(s.stops))
	for channel, stop := range s.stops {
		stops = append(stops, stop)
		delete(s.stops, channel)
	}
	s.mu.Unlock()

	for _, stop := range stops {
		stop()
	}
	s.wg.Wait()
}
