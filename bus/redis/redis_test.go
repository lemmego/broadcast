package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/lemmego/broadcast"
	redisbus "github.com/lemmego/broadcast/bus/redis"
)

func newBus(t *testing.T, mode redisbus.Mode) (*redisbus.Bus, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	bus, err := redisbus.New(redisbus.Config{
		Addr:   server.Addr(),
		Prefix: "productshore",
		Mode:   mode,
	})
	if err != nil {
		t.Fatalf("building the bus: %v", err)
	}
	t.Cleanup(func() { bus.Close() })
	return bus, server
}

// Pub/sub ignores the selected database, so two applications sharing one
// Redis with no prefix would deliver each other's events. Required rather
// than defaulted, because a default nobody chose is the same accident.
func TestAPrefixIsRequired(t *testing.T) {
	server := miniredis.RunT(t)

	_, err := redisbus.New(redisbus.Config{Addr: server.Addr()})
	if err == nil {
		t.Fatal("a bus with no prefix was accepted")
	}
}

func TestAnEventCrossesTheBus(t *testing.T) {
	bus, _ := newBus(t, redisbus.PatternSubscribe)
	ctx := context.Background()

	deliveries, stop, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	event, err := broadcast.NewEvent("notification.created", map[string]int{"id": 7})
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}
	if err := bus.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	select {
	case delivery := <-deliveries:
		if delivery.Channel != "private-user.1" {
			t.Fatalf("got channel %q", delivery.Channel)
		}
		if delivery.Event.Name != "notification.created" {
			t.Fatalf("got event %q", delivery.Event.Name)
		}
		if delivery.Event.ID != event.ID {
			t.Fatalf("the id did not survive: got %q, want %q",
				delivery.Event.ID, event.ID)
		}
		if string(delivery.Event.Data) != string(event.Data) {
			t.Fatalf("got payload %s, want %s", delivery.Event.Data, event.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing arrived")
	}
}

// Two buses on one Redis is two processes: the point of this package.
func TestAnEventReachesAnotherProcess(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()

	publisher, err := redisbus.New(redisbus.Config{Addr: server.Addr(), Prefix: "ps"})
	if err != nil {
		t.Fatalf("building the publisher: %v", err)
	}
	defer publisher.Close()

	subscriber, err := redisbus.New(redisbus.Config{Addr: server.Addr(), Prefix: "ps"})
	if err != nil {
		t.Fatalf("building the subscriber: %v", err)
	}
	defer subscriber.Close()

	deliveries, stop, err := subscriber.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	event, _ := broadcast.NewEvent("from.elsewhere", nil)
	if err := publisher.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	select {
	case delivery := <-deliveries:
		if delivery.Event.Name != "from.elsewhere" {
			t.Fatalf("got %q", delivery.Event.Name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("an event published by another process never arrived")
	}
}

func TestAnotherApplicationsEventsAreNotDelivered(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()

	ours, err := redisbus.New(redisbus.Config{Addr: server.Addr(), Prefix: "productshore"})
	if err != nil {
		t.Fatalf("building ours: %v", err)
	}
	defer ours.Close()

	theirs, err := redisbus.New(redisbus.Config{Addr: server.Addr(), Prefix: "someoneelse"})
	if err != nil {
		t.Fatalf("building theirs: %v", err)
	}
	defer theirs.Close()

	deliveries, stop, err := ours.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	event, _ := broadcast.NewEvent("not.ours", nil)
	if err := theirs.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	select {
	case delivery := <-deliveries:
		t.Fatalf("another application's event arrived: %q", delivery.Event.Name)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestAnEventOnAnotherChannelIsNotDelivered(t *testing.T) {
	bus, _ := newBus(t, redisbus.PatternSubscribe)
	ctx := context.Background()

	deliveries, stop, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	event, _ := broadcast.NewEvent("elsewhere", nil)
	bus.Publish(ctx, "private-user.2", event)

	select {
	case delivery := <-deliveries:
		t.Fatalf("an event on another channel arrived: %q", delivery.Event.Name)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestPerChannelModeDelivers(t *testing.T) {
	bus, _ := newBus(t, redisbus.PerChannel)
	ctx := context.Background()

	deliveries, stop, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	event, _ := broadcast.NewEvent("direct", nil)
	if err := bus.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	select {
	case delivery := <-deliveries:
		if delivery.Event.Name != "direct" {
			t.Fatalf("got %q", delivery.Event.Name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing arrived in per-channel mode")
	}
}

// A dead subscription and a quiet channel look identical from the inside, so
// the contract is that the delivery channel closes. This is the case that
// makes the Hub able to tell its clients to come back.
func TestClosingTheBusEndsEverySubscription(t *testing.T) {
	bus, _ := newBus(t, redisbus.PatternSubscribe)

	deliveries, stop, err := bus.Subscribe(context.Background(), "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	if err := bus.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	select {
	case _, open := <-deliveries:
		if open {
			t.Fatal("a subscription survived the bus")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closing the bus did not close its subscriptions, so a " +
			"subscriber would wait forever on a bus that is gone")
	}
}

// Redis going away must not look like a channel with no traffic.
func TestLosingRedisClosesSubscriptions(t *testing.T) {
	bus, server := newBus(t, redisbus.PatternSubscribe)

	deliveries, stop, err := bus.Subscribe(context.Background(), "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stop()

	server.Close()

	select {
	case _, open := <-deliveries:
		if open {
			t.Fatal("a delivery arrived from a Redis that is gone")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("losing Redis left the subscription open and silent, which " +
			"is indistinguishable from a quiet channel")
	}
}

// A bus that was explicitly configured and cannot be reached fails the boot,
// rather than falling back to something that passes staging and loses half
// the events in production with two replicas.
func TestAnUnreachableRedisIsReportedAtBoot(t *testing.T) {
	bus, err := redisbus.New(redisbus.Config{
		// A port nothing is listening on.
		Addr:   "127.0.0.1:1",
		Prefix: "ps",
	})
	if err != nil {
		t.Fatalf("building the bus: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := bus.Ping(ctx); err == nil {
		t.Fatal("an unreachable Redis reported itself healthy")
	}
}

func TestStoppingOneSubscriberLeavesTheOthers(t *testing.T) {
	bus, _ := newBus(t, redisbus.PatternSubscribe)
	ctx := context.Background()

	first, stopFirst, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	second, stopSecond, err := bus.Subscribe(ctx, "private-user.1")
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer stopSecond()

	stopFirst()
	select {
	case _, open := <-first:
		if open {
			t.Fatal("the stopped subscriber delivered")
		}
	case <-time.After(time.Second):
		t.Fatal("stopping did not close the channel")
	}

	event, _ := broadcast.NewEvent("still.here", nil)
	if err := bus.Publish(ctx, "private-user.1", event); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	select {
	case delivery := <-second:
		if delivery.Event.Name != "still.here" {
			t.Fatalf("got %q", delivery.Event.Name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stopping one subscriber took the other with it")
	}
}
