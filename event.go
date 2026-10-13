package broadcast

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MaxEventBytes caps one event's payload.
//
// A realtime frame is for telling a client that something happened, not for
// carrying it. Past this the right answer is an identifier the client
// fetches, and a cap says so at the one place that can.
const MaxEventBytes = 64 << 10

// ErrBadEvent reports an event that cannot be sent.
var ErrBadEvent = errors.New("broadcast: unusable event")

// Event is one message.
//
// Data is pre-encoded, which is the one surprising thing about this type. An
// event crosses a process boundary through the Bus, so it has to become bytes
// somewhere, and doing it at publish time means a payload that cannot be
// marshalled is reported to the one caller who can fix it, exactly once.
// Encoding lazily at fan-out would raise the same error on every subscriber's
// goroutine, N times, with nowhere to return it to.
type Event struct {
	// Name is what the client dispatches on, such as
	// "notification.created". Required, and subject to the same character
	// rules as a channel name, for the same framing reason.
	Name string

	// Data is the payload, already JSON. Nil is sent as null.
	Data json.RawMessage

	// ID identifies this event, for deduplication.
	//
	// Delivery is at-least-once: a publish that reaches local subscribers and
	// then fails against the bus is retried by its caller, and the local
	// subscribers see it twice. A client that is not idempotent needs this,
	// and the shipped client deduplicates on it.
	ID string

	// At is when the event was published, so a client reconnecting after a
	// laptop sleep can tell a fresh event from one that was in flight.
	At time.Time
}

// NewEvent encodes a payload and returns an event with an id and a timestamp.
//
// The only constructor, because an event assembled by hand with no id
// silently opts out of deduplication — and the duplicate it then delivers
// looks like an application bug rather than a missing field.
func NewEvent(name string, payload any) (Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("%w: encoding the payload for %q: %w",
			ErrBadEvent, name, err)
	}
	event := Event{Name: name, Data: data, ID: newEventID(), At: time.Now().UTC()}
	if err := event.Validate(); err != nil {
		return Event{}, err
	}
	return event, nil
}

// DataAs decodes this event's payload.
//
// A method with its own type parameter, which this Go allows on a concrete
// type — so a subscriber writes what it means:
//
//	payload, err := event.DataAs[NotificationCreated]()
//
// rather than declaring a variable and remembering to pass its address. It is
// the shape cache's GetAs uses, and the reason Event is a struct: a generic
// method cannot exist on an interface, which is also why Bus and Conn carry
// no generic methods of their own.
//
// Numbers decode as json.Number, so a large integer that a float64 could not
// hold survives — which it would not through a map[string]any, where
// 9007199254740993 comes back as the even number below it.
func (e Event) DataAs[T any]() (T, error) {
	var out T
	if len(e.Data) == 0 {
		return out, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(e.Data))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return out, fmt.Errorf("broadcast: decoding the payload of %q: %w",
			e.Name, err)
	}
	return out, nil
}

// Validate reports why an event cannot be sent.
func (e Event) Validate() error {
	if e.Name == "" {
		return fmt.Errorf("%w: it has no name, so a client has nothing to "+
			"dispatch on", ErrBadEvent)
	}
	// The same rules as a channel name: an event name is written into a
	// stream field too, so a newline in it ends the field early.
	for i := 0; i < len(e.Name); i++ {
		if !channelByteAllowed(e.Name[i]) {
			return fmt.Errorf("%w: the name %q contains %q, and a name may "+
				"hold only letters, digits and . _ - : @",
				ErrBadEvent, e.Name, string(e.Name[i]))
		}
	}
	if len(e.Data) > MaxEventBytes {
		return fmt.Errorf("%w: %d bytes of payload, and the limit is %d — "+
			"send an identifier the client can fetch instead",
			ErrBadEvent, len(e.Data), MaxEventBytes)
	}
	if len(e.Data) > 0 && !json.Valid(e.Data) {
		return fmt.Errorf("%w: the payload is not JSON", ErrBadEvent)
	}
	return nil
}

// newEventID returns an opaque, unguessable id.
//
// Random rather than sequential: an id is echoed to clients, and a counter
// would tell every subscriber how much traffic every other channel is
// carrying.
func newEventID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		// crypto/rand does not fail on any supported platform, and if it
		// did, a timestamp is still a usable deduplication key within one
		// connection. Panicking here would take down a web process over an
		// id for a notification.
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buffer)
}
