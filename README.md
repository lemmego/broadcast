# lemmego/broadcast

Events, delivered to your clients over a live connection. Server-sent events
and WebSocket, behind one contract, over an in-process or a Redis bus.

Reach for it when something that happens on the server has to reach a browser
that is not asking: a notification badge, a live count, a job finishing.

## Register it

```go
// bootstrap/providers.go
bc := &provider.Provider{
    AllowedOrigins: []string{config.MustEnv("APP_URL", "http://localhost:8080")},
}
bc.Channel("private-post.{id}", func(c app.Context, p provider.Params) error {
    user, ok := auth.UserAs[*models.User](c)
    if !ok {
        return broadcast.ErrUnauthenticated
    }
    return policies.Post(c).ViewRealtime(user, p.Get("id"))
})

return []app.Provider{ …, bc }
```

`Channel` returns the provider, so you can chain as many as you have.

You get `private-user.{subject}` without configuring anything, so
`broadcast.UserChannel(id)` works out of the box — it is what `lemmego/notify`
publishes to.

## Publish an event

```go
event, err := broadcast.NewEvent("notification.created", map[string]any{"id": 7})
hub.Publish(ctx, broadcast.UserChannel("42"), event)
```

And in the browser:

```tsx
useBroadcast(`private-user.${viewer.id}`, {
    "notification.created": () => refetchUnread(),
}, { onResync: refetchUnread });
```

## Choosing a transport

Both transports pass the same conformance suite, so the choice is about your
deployment rather than your code.

| | `sse` | `ws` |
|---|---|---|
| client→server | no — reconnects to change channels | yes |
| liveness | a write succeeds into the kernel buffer long after the peer is gone | a ping is answered |
| dependencies | none | `coder/websocket` |
| browser connection cap | one of ~6 per origin on HTTP/1.1 | ~255 |

Behind nginx or Caddy terminating HTTP/2, prefer `sse`: one goroutine instead
of two, no dependency, and the browser's own reconnect does half the work.

In a single binary with no proxy there is no HTTP/2, so an event stream
permanently occupies one of about six connections per origin *across every
tab* — use `ws` there.

### If you wrap the response writer

If you wrap the `http.ResponseWriter` in middleware of your own, a WebSocket
upgrade needs `http.Hijacker` to survive the wrapper, and **a type assertion
traverses nothing**: an `Unwrap` method is not enough on its own, because the
WebSocket library asserts directly. Forward `Hijack` (and `Flush`, which an
event stream needs) from your wrapper, and add
`var _ http.Hijacker = (*yourWriter)(nil)` so the compiler holds you to it.
Without that, `ws` answers **501 Not Implemented** and `sse` may answer 500.

## Security

Both transports authenticate from a **cookie**, because neither a browser
`EventSource` nor a browser `WebSocket` can set an `Authorization` header.
That makes the origin check load-bearing, and the two protocols are not
equally exposed:

- **A WebSocket is not subject to CORS.** Any page can open one and the
  browser attaches your cookies unless `Origin` is checked. The `ws` transport
  keeps the library's own check, and this module gives you no way to turn it
  off.
- **An event stream *is* subject to CORS**, so it is same-origin by default
  and this module sends no CORS headers at all.

`AllowedOrigins` is **required** — an unset allowlist is the difference
between a private channel and a public one, so it fails the boot rather than
defaulting to something. Write entries as a URL
(`https://app.example.com`) or as a bare host (`app.example.com`); a WebSocket
handshake is matched against the host, so the scheme is stripped before it
gets there.

Two differences between the transports, both stricter on the WebSocket side:

- `[]string{"*"}` is honoured for an event stream, and means **same-origin
  only** for a WebSocket. Forwarding it as a handshake pattern would let any
  origin on the internet open an authenticated socket.
- A glob (`*.example.com`) works for a WebSocket, where the library supports
  it, and not for an event stream, whose check is an exact comparison. Write
  the hosts out if you need both.

Authorization runs **before the protocol is established**, and again for any
channel added mid-connection. A refusal is an HTTP status, because a browser
gets almost nothing out of a connection that was accepted and then closed and
its reconnect logic cannot tell that from a flaky network — so
accept-then-close is a reconnect loop against an endpoint that will never
admit it.

Channel patterns match **segment by segment**, never as regular expressions.
An unanchored pattern in an authorization matcher *grants* access rather than
skipping a check, so `private-post.{id}` can match nothing but one segment
where `{id}` is.

## What it promises

**`Publish` means "accepted for delivery", never "delivered."** No API here
means delivered. Delivery is at-least-once, which is why `Event.ID` exists and
why the client deduplicates on it.

**There is no backlog, so `Last-Event-ID` is not honoured** — and the stream
does not emit `id:` at all, rather than inviting the browser to send a header
the server cannot answer. Every ready frame carries `resync: true` instead and
the client calls `onResync`, so your application re-fetches. That is also the
path a laptop waking from sleep needs.

**A slow client is closed, not buffered.** An unbounded queue is a memory leak
one slow client wide, and a dropped frame is an event nobody can know they
missed. A closed connection is something your client already handles.

**A lost bus subscription closes the connections on it.** An open stream
receiving nothing is the worst failure here, and it is indistinguishable from
a quiet channel unless something says so.

## Buses

The **in-process bus is the default**, and a single binary with no Redis is a
supported mode rather than a degraded one.

The **Redis bus** is for more than one replica. It reads with `Receive` rather
than ranging over `Channel`, because `Channel` reconnects on its own and never
closes: events published while the connection was down are gone, and a
subscriber left open would receive nothing with no way to know. A
re-subscription is reported as a gap for the same reason.

`Prefix` is required, because pub/sub ignores the selected database — two
applications sharing one Redis with no prefix deliver each other's events.

The bus never borrows your cache's Redis client. go-redis dedicates a
connection to each subscription for its lifetime and does not return it, so
the symptom of sharing one is your *cache* blocking on a read.

## Capacity planning

About **10,000 connections per process** is comfortable: ~16–24KB each for an
event stream, ~32–40KB for a WebSocket, one goroutine against two. The wall
arrives at Redis before it arrives at HTTP, which is why `PatternSubscribe` is
the default — one subscription per process, routed locally, rather than one
Redis connection per connected user.

Past that: a dedicated fleet of connection-holding processes, then sharded
channels so Redis carries O(shards) rather than O(users), then Redis Streams
with consumer groups, which also buys the backlog that would make resume real.

## Limitations

Presence channels · arbitrary client→server application messages (v0 admits
three control commands; anything your application wants to *do* goes through
an HTTP route, which already has CSRF, validation and authorization) · a
message backlog and true resume · guaranteed delivery and acks (that is
`tasker`'s job, and a notification is already durable in Postgres) ·
per-event filtering beyond the channel · compression · long-polling · a lossy
per-channel mode, though `Frame.Seq` is already in the wire format for the day
there is one.
