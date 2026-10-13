package broadcast

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTheMemoryBusFansOutToEverySubscriber(t *testing.T) {
	bus := NewMemoryBus()
	defer bus.Close()
	ctx := context.Background()

	first, stopFirst, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stopFirst()
	second, stopSecond, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stopSecond()
	// A different channel must not receive it.
	other, stopOther, err := bus.Subscribe(ctx, "private-user.2")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stopOther()

	event, err := NewEvent("notification.created", map[string]int{"id": 1})
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := bus.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	for i, deliveries := range []<-chan Delivery{first, second} {
		select {
		case delivery := <-deliveries:
			if delivery.Event.Name != "notification.created" {
				t.Fatalf("subscriber %d got %q", i, delivery.Event.Name)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d received nothing", i)
		}
	}

	select {
	case delivery := <-other:
		t.Fatalf("a subscriber to another channel received %q", delivery.Event.Name)
	case <-time.After(50 * time.Millisecond):
	}
}

// A dead subscription and a quiet channel look identical from the inside,
// which is why the contract is that the delivery channel closes.
func TestStoppingASubscriptionClosesItsChannel(t *testing.T) {
	bus := NewMemoryBus()
	defer bus.Close()

	deliveries, stop, err := bus.Subscribe(context.Background(), "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	stop()

	select {
	case _, open := <-deliveries:
		if open {
			t.Fatal("the channel delivered after being stopped")
		}
	case <-time.After(time.Second):
		t.Fatal("the channel was not closed, so a subscriber would block forever")
	}

	// Idempotent, because a stop runs from a defer and from a context watcher.
	stop()
}

func TestClosingTheBusEndsEverySubscription(t *testing.T) {
	bus := NewMemoryBus()

	deliveries, stop, err := bus.Subscribe(context.Background(), "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	select {
	case _, open := <-deliveries:
		if open {
			t.Fatal("a subscription survived the bus")
		}
	case <-time.After(time.Second):
		t.Fatal("closing the bus did not close its subscriptions")
	}

	// Close then stop closed the same channel twice in the first version of
	// this bus, which panicked. Both paths go through one closer now.
	stop()

	if err := bus.Publish(context.Background(), "private-user.1", Event{Name: "x"}); err == nil {
		t.Fatal("a closed bus accepted a publish")
	}
}

func TestACancelledContextEndsTheSubscription(t *testing.T) {
	bus := NewMemoryBus()
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	deliveries, stop, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	// Honoured by the bus rather than left to the caller, so a cancelled
	// request cannot leak a subscription.
	cancel()
	select {
	case _, open := <-deliveries:
		if open {
			t.Fatal("the subscription delivered after its context was cancelled")
		}
	case <-time.After(time.Second):
		t.Fatal("a cancelled context did not end the subscription")
	}
}

func TestConcurrentPublishesAndSubscriptionsAreRaceFree(t *testing.T) {
	bus := NewMemoryBus()
	defer bus.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deliveries, stop, err := bus.Subscribe(ctx, "private-user.1")
			if err != nil {
				return
			}
			defer stop()
			// Drained, so the buffer never fills and the publisher never
			// has to drop anything.
			go func() {
				for range deliveries {
				}
			}()
			for j := 0; j < 20; j++ {
				bus.Publish(ctx, "private-user.1", Event{Name: "tick"})
			}
		}()
	}
	wg.Wait()
}
