package broadcast

// Stats is a snapshot of what a hub is doing.
//
// Counters rather than a metrics backend: wiring these to Prometheus or
// OpenTelemetry is the job of whichever module owns observability, and a
// realtime module that dragged in an exporter would be charging every
// consumer for a decision that is not its to make.
type Stats struct {
	// Connections open right now, in total and by protocol.
	Connections   int
	BySSE         int
	ByWebSocket   int
	Channels      int
	FramesSent    int64
	FramesDropped int64
	PublishErrors int64
	Draining      bool
}

// Stats returns a snapshot.
func (h *Hub) Stats() Stats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := Stats{
		Connections:   len(h.conns),
		FramesSent:    h.framesSent.Load(),
		FramesDropped: h.framesDropped.Load(),
		PublishErrors: h.publishErrors.Load(),
		Draining:      h.draining,
	}

	// Channel names are deliberately counted and not listed: a diagnostic
	// endpoint that enumerated them would publish who is connected and what
	// they are watching.
	channels := map[Channel]struct{}{}
	for state := range h.conns {
		switch state.conn.Protocol() {
		case "sse":
			stats.BySSE++
		case "ws":
			stats.ByWebSocket++
		}
		for _, channel := range state.subscribed() {
			channels[channel] = struct{}{}
		}
	}
	stats.Channels = len(channels)
	return stats
}
