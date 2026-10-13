package sse_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lemmego/broadcast"
	"github.com/lemmego/broadcast/transport/sse"
	"github.com/lemmego/broadcast/transporttest"
)

// The conformance suite, which was written before this transport existed so
// it could not be built to fit.
func TestSSEConformsToTheTransportContract(t *testing.T) {
	transporttest.Run(t, transporttest.Factory{
		Transport: sse.New(WithShortRetry()),
		Dial:      dial,
	}, broadcast.Capabilities{AcceptsCommands: false, VerifiesLiveness: false})
}

// WithShortRetry keeps a test's own reconnect hint small.
func WithShortRetry() sse.Option { return sse.WithRetry(50 * time.Millisecond) }

// --- the client half ---------------------------------------------------

type client struct {
	response *http.Response
	reader   *bufio.Reader

	mu      sync.Mutex
	stalled bool
	stall   chan struct{}

	frames chan broadcast.Frame
	errs   chan error
	closed chan struct{}
	once   sync.Once
}

func dial(t *testing.T, base *url.URL, channels []broadcast.Channel) (transporttest.Client, error) {
	t.Helper()

	target := *base
	target.Path = "/broadcast/sse"
	query := url.Values{}
	for _, channel := range channels {
		query.Add("channel", string(channel))
	}
	target.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(context.Background(),
		http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "text/event-stream")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, &transporttest.StatusError{Status: response.StatusCode}
	}
	// Asserted here rather than in a case of its own, because every case
	// depends on it: nginx buffers text/event-stream by default, and without
	// this header the stream looks dead through a proxy and nowhere else.
	if got := response.Header.Get("X-Accel-Buffering"); got != "no" {
		response.Body.Close()
		return nil, fmt.Errorf("sse: X-Accel-Buffering is %q, want no", got)
	}

	c := &client{
		response: response,
		reader:   bufio.NewReader(response.Body),
		frames:   make(chan broadcast.Frame, 128),
		errs:     make(chan error, 1),
		closed:   make(chan struct{}),
		stall:    make(chan struct{}),
	}
	go c.read()
	t.Cleanup(func() { c.Close() })
	return c, nil
}

// read parses the event stream.
//
// Comment lines are dropped on the floor, which is what proves a heartbeat
// cannot reach an application handler: this reader is the application.
func (c *client) read() {
	var (
		eventName string
		data      strings.Builder
	)
	for {
		// Stalling stops reading the socket entirely, rather than reading
		// and discarding.
		//
		// The first version discarded, which meant the server's writes always
		// succeeded and the hub's queue drained as fast as it filled — so the
		// overflow case passed only when the publish loop happened to outrun
		// the scheduler, and failed otherwise. A client that stops reading is
		// what a slow client actually is: the kernel buffer fills, the
		// server's write blocks, and the queue overflows for a reason rather
		// than by luck.
		select {
		case <-c.stall:
			return
		default:
		}

		line, err := c.reader.ReadString('\n')
		if err != nil {
			select {
			case c.errs <- err:
			default:
			}
			return
		}
		line = strings.TrimRight(line, "\r\n")

		switch {
		case line == "":
			// A blank line ends the frame.
			if eventName != "" && data.Len() > 0 {
				c.deliver(eventName, data.String())
			}
			eventName, data = "", strings.Builder{}
		case strings.HasPrefix(line, ":"):
			// A comment — a heartbeat. Never surfaced.
		case strings.HasPrefix(line, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		case strings.HasPrefix(line, "retry:"):
			// The reconnect hint, for the browser.
		}
	}
}

func (c *client) deliver(name, payload string) {
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
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		select {
		case c.errs <- fmt.Errorf("sse: %q is not a frame: %w", payload, err):
		default:
		}
		return
	}

	frame := broadcast.Frame{
		Kind: wire.Kind, Channel: wire.Channel, Seq: wire.Seq, Meta: wire.Meta,
	}
	if wire.Kind == broadcast.FrameClose {
		// The close reason travels as the frame body on this transport.
		frame.Meta = json.RawMessage(payload)
	}
	if wire.Event != nil {
		frame.Event = broadcast.Event{
			Name: wire.Event.Name, Data: wire.Event.Data, ID: wire.Event.ID,
		}
	}

	select {
	case c.frames <- frame:
	case <-c.closed:
	}
}

func (c *client) Next(timeout time.Duration) (broadcast.Frame, error) {
	select {
	case frame := <-c.frames:
		return frame, nil
	case err := <-c.errs:
		return broadcast.Frame{}, err
	case <-time.After(timeout):
		return broadcast.Frame{}, fmt.Errorf("sse: nothing arrived in %s", timeout)
	}
}

func (c *client) Send(broadcast.Command) error {
	// An EventSource has no way to send anything. Reported rather than
	// silently ignored, so the conformance suite can tell this transport
	// apart from one that accepted a command and dropped it.
	return transporttest.ErrUnsupported
}

func (c *client) Stall() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stalled {
		return
	}
	c.stalled = true
	close(c.stall)
}

func (c *client) Close() error {
	c.once.Do(func() {
		close(c.closed)
		c.response.Body.Close()
	})
	return nil
}
