package broadcast

import (
	"encoding/json"
	"strings"
	"testing"
)

// DataAs is a generic method on a concrete type, which this Go allows — so a
// subscriber names the shape it wants rather than declaring a variable and
// passing its address.
func TestDataAsDecodesThePayload(t *testing.T) {
	event, err := NewEvent("notification.created", map[string]any{
		"type": "followed", "id": 7,
	})
	if err != nil {
		t.Fatalf("building the event: %v", err)
	}

	payload, err := event.DataAs[struct {
		Type string `json:"type"`
		ID   int    `json:"id"`
	}]()
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if payload.Type != "followed" || payload.ID != 7 {
		t.Fatalf("got %+v", payload)
	}
}

// A float64 cannot hold 9007199254740993 and silently returns the even number
// below it, which is what a map[string]any would give a subscriber.
func TestDataAsKeepsLargeNumbersExact(t *testing.T) {
	event := Event{Name: "probe", Data: json.RawMessage(`{"big":9007199254740993}`)}

	payload, err := event.DataAs[struct {
		Big json.Number `json:"big"`
	}]()
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if payload.Big.String() != "9007199254740993" {
		t.Fatalf("got %s, want 9007199254740993", payload.Big)
	}
}

func TestDataAsOnAnEmptyPayloadIsTheZeroValue(t *testing.T) {
	event := Event{Name: "probe"}

	payload, err := event.DataAs[map[string]any]()
	if err != nil {
		t.Fatalf("got an error for an empty payload: %v", err)
	}
	if payload != nil {
		t.Fatalf("got %v, want nil", payload)
	}
}

func TestDataAsReportsTheEventWhenDecodingFails(t *testing.T) {
	event := Event{Name: "notification.created", Data: json.RawMessage(`{"id":"seven"}`)}

	_, err := event.DataAs[struct {
		ID int `json:"id"`
	}]()
	if err == nil {
		t.Fatal("a payload that does not fit the shape was accepted")
	}
	// Named, because a subscriber handling several events needs to know which
	// one it could not read.
	if !strings.Contains(err.Error(), "notification.created") {
		t.Fatalf("the error does not name the event: %v", err)
	}
}

// NewEvent validates on the way through, so an unsendable event cannot be
// built by the only constructor.
func TestNewEventRefusesAnUnusableName(t *testing.T) {
	if _, err := NewEvent("bad\nname", nil); err == nil {
		t.Fatal("a name containing a newline was accepted, and it would end " +
			"a field in an event stream")
	}
	if _, err := NewEvent("", nil); err == nil {
		t.Fatal("an event with no name was accepted")
	}
}

func TestNewEventGeneratesAnIDForDeduplication(t *testing.T) {
	first, err := NewEvent("probe", nil)
	if err != nil {
		t.Fatalf("building: %v", err)
	}
	second, err := NewEvent("probe", nil)
	if err != nil {
		t.Fatalf("building: %v", err)
	}

	// Delivery is at-least-once, so an event with no id silently opts out of
	// the client's deduplication.
	if first.ID == "" {
		t.Fatal("NewEvent produced no id")
	}
	if first.ID == second.ID {
		t.Fatal("two events share an id, so one would be dropped as a duplicate")
	}
}
