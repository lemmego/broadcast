package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/lemmego/broadcast"
	"github.com/lemmego/broadcast/transport/ws"
	"github.com/lemmego/broadcast/transporttest"
)

// The same suite the event-stream transport passes. Two implementations held
// to one contract is the whole argument for shipping both at once.
func TestWebSocketConformsToTheTransportContract(t *testing.T) {
	transporttest.Run(t, transporttest.Factory{
		Transport: ws.New(),
		Dial:      dial,
	}, broadcast.Capabilities{AcceptsCommands: true, VerifiesLiveness: true})
}

// --- the client half ---------------------------------------------------

type client struct {
	socket *websocket.Conn

	mu      sync.Mutex
	stalled bool

	frames chan broadcast.Frame
	errs   chan error
	closed chan struct{}
	once   sync.Once
}

func dial(t *testing.T, base *url.URL, channels []broadcast.Channel) (transporttest.Client, error) {
	t.Helper()

	target := *base
	target.Scheme = "ws"
	target.Path = "/broadcast/ws"
	query := url.Values{}
	for _, channel := range channels {
		query.Add("channel", string(channel))
	}
	target.RawQuery = query.Encode()

	socket, response, err := websocket.Dial(context.Background(), target.String(), nil)
	if err != nil {
		if response != nil {
			return nil, &transporttest.StatusError{Status: response.StatusCode, Err: err}
		}
		return nil, err
	}
	socket.SetReadLimit(1 << 20)

	c := &client{
		socket: socket,
		frames: make(chan broadcast.Frame, 256),
		errs:   make(chan error, 1),
		closed: make(chan struct{}),
	}
	go c.read()
	t.Cleanup(func() { c.Close() })
	return c, nil
}

func (c *client) read() {
	for {
		_, data, err := c.socket.Read(context.Background())
		if err != nil {
			select {
			case c.errs <- err:
			default:
			}
			return
		}

		c.mu.Lock()
		stalled := c.stalled
		c.mu.Unlock()
		if stalled {
			continue
		}

		var wire struct {
			Kind    broadcast.FrameKind `json:"kind"`
			Channel broadcast.Channel   `json:"channel"`
			Seq     uint64              `json:"seq"`
			Event   *struct {
				Name string          `json:"name"`
				Data json.RawMessage `json:"data"`
				ID   string          `json:"id"`
			} `json:"event"`
			Meta json.RawMessage `json:"meta"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			select {
			case c.errs <- fmt.Errorf("ws: %s is not a frame: %w", data, err):
			default:
			}
			continue
		}

		frame := broadcast.Frame{
			Kind: wire.Kind, Channel: wire.Channel, Seq: wire.Seq, Meta: wire.Meta,
		}
		if wire.Event != nil {
			frame.Event = broadcast.Event{
				Name: wire.Event.Name, Data: wire.Event.Data, ID: wire.Event.ID,
			}
		}
		select {
		case c.frames <- frame:
		case <-c.closed:
			return
		}
	}
}

func (c *client) Next(timeout time.Duration) (broadcast.Frame, error) {
	select {
	case frame := <-c.frames:
		return frame, nil
	case err := <-c.errs:
		return broadcast.Frame{}, err
	case <-time.After(timeout):
		return broadcast.Frame{}, fmt.Errorf("ws: nothing arrived in %s", timeout)
	}
}

func (c *client) Send(command broadcast.Command) error {
	body, err := json.Marshal(command)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return c.socket.Write(ctx, websocket.MessageText, body)
}

func (c *client) Stall() {
	c.mu.Lock()
	c.stalled = true
	c.mu.Unlock()
}

func (c *client) Close() error {
	c.once.Do(func() {
		close(c.closed)
		c.socket.Close(websocket.StatusNormalClosure, "")
	})
	return nil
}

// --- what only this transport can be asked -----------------------------

// A WebSocket is not subject to CORS, so a browser attaches cookies to a
// handshake from any origin unless the Origin header is checked. This is the
// only thing standing between a cookie-authenticated realtime endpoint and
// somebody else's page reading it.
func TestACrossOriginHandshakeIsRefusedBeforeItIsAccepted(t *testing.T) {
	server, _ := serve(t, ws.New())

	target := strings.Replace(server, "http://", "ws://", 1) +
		"/broadcast/ws?channel=private-user.1"

	_, response, err := websocket.Dial(context.Background(), target,
		&websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
		})
	if err == nil {
		t.Fatal("a cross-origin handshake was accepted, so any page on the " +
			"internet could read this connection with the viewer's cookies")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("got %v, want a 403 before the handshake completed", response)
	}
}

func TestASameOriginHandshakeIsAccepted(t *testing.T) {
	server, _ := serve(t, ws.New())

	base, err := url.Parse(server)
	if err != nil {
		t.Fatalf("parsing the server URL: %v", err)
	}
	target := strings.Replace(server, "http://", "ws://", 1) +
		"/broadcast/ws?channel=private-user.1"

	socket, _, err := websocket.Dial(context.Background(), target,
		&websocket.DialOptions{
			// What a browser sends for a same-origin connection.
			HTTPHeader: http.Header{"Origin": []string{"http://" + base.Host}},
		})
	if err != nil {
		t.Fatalf("a same-origin handshake was refused: %v", err)
	}
	socket.Close(websocket.StatusNormalClosure, "")
}

func TestAnAllowedOriginIsAccepted(t *testing.T) {
	server, _ := serve(t, ws.New(ws.WithOriginPatterns("trusted.example")))

	target := strings.Replace(server, "http://", "ws://", 1) +
		"/broadcast/ws?channel=private-user.1"

	socket, _, err := websocket.Dial(context.Background(), target,
		&websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"https://trusted.example"}},
		})
	if err != nil {
		t.Fatalf("an allowed origin was refused: %v", err)
	}
	socket.Close(websocket.StatusNormalClosure, "")
}

// The inbound path is the half an event stream does not have, and the reason
// the seam admits commands at all.
func TestAClientCanSubscribeOverTheSameConnection(t *testing.T) {
	server, hub := serve(t, ws.New())

	base, err := url.Parse(server)
	if err != nil {
		t.Fatalf("parsing the server URL: %v", err)
	}
	c, err := dial(t, base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer c.Close()
	if _, err := c.Next(3 * time.Second); err != nil {
		t.Fatalf("waiting for ready: %v", err)
	}

	// Not subscribed yet, so nothing arrives.
	event, _ := broadcast.NewEvent("before", nil)
	hub.Publish(context.Background(), "private-user.2", event)
	if _, err := c.Next(300 * time.Millisecond); err == nil {
		t.Fatal("an event arrived on a channel this connection never asked for")
	}

	if err := c.Send(broadcast.Command{
		Kind:     broadcast.CommandSubscribe,
		Channels: []broadcast.Channel{"private-user.2"},
	}); err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	// Now it does, with no reconnection — which is the whole difference.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		after, _ := broadcast.NewEvent("after", nil)
		hub.Publish(context.Background(), "private-user.2", after)
		if frame, err := c.Next(200 * time.Millisecond); err == nil &&
			frame.Event.Name == "after" {
			return
		}
	}
	t.Fatal("a mid-connection subscribe never took effect")
}

// A connection that can add channels later must not be able to add one it was
// never entitled to, which is why the authorizer runs per subscribe and not
// once at connect.
func TestAMidConnectionSubscribeIsAuthorizedToo(t *testing.T) {
	server, hub := serve(t, ws.New())

	base, err := url.Parse(server)
	if err != nil {
		t.Fatalf("parsing the server URL: %v", err)
	}
	c, err := dial(t, base, []broadcast.Channel{"private-user.1"})
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer c.Close()
	c.Next(3 * time.Second)

	if err := c.Send(broadcast.Command{
		Kind:     broadcast.CommandSubscribe,
		Channels: []broadcast.Channel{"private-forbidden.1"},
	}); err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	frame, err := c.Next(3 * time.Second)
	if err != nil {
		t.Fatalf("waiting for the refusal: %v", err)
	}
	if frame.Kind != broadcast.FrameError {
		t.Fatalf("got a %q frame, want an error frame", frame.Kind)
	}

	var refusal broadcast.Refusal
	if err := json.Unmarshal(frame.Meta, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if refusal.Reason != "forbidden" {
		t.Fatalf("got reason %q", refusal.Reason)
	}

	// And the refused channel delivers nothing.
	event, _ := broadcast.NewEvent("leaked", nil)
	hub.Publish(context.Background(), "private-forbidden.1", event)
	if _, err := c.Next(300 * time.Millisecond); err == nil {
		t.Fatal("a refused channel delivered an event anyway")
	}
}
