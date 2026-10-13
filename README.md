# lemmego/broadcast

Events, delivered to clients over a live connection. Server-sent events and
WebSocket, behind one contract.

Named for the capability with the wire protocol as a driver, matching
`cache/store/*` and `tasker/driver/*`. `lemmego/websocket` would name a
protocol — the mistake calling the cache module `lemmego/redis` would have
been.

```go
&provider.Provider{
    AllowedOrigins: []string{"https://productshore.com"},
}.
Channel("private-post.{id}", func(c app.Context, p provider.Params) error {
    user, ok := auth.UserAs[*models.User](c)
    if !ok {
        return broadcast.ErrUnauthenticated
    }
    return policies.Post(c).ViewRealtime(user, p.Get("id"))
})
```

`private-user.{subject}` is installed by default, so `broadcast.UserChannel(id)`
works with no configuration — which is what `lemmego/notify` publishes to.

Publishing:

```go
event, err := broadcast.NewEvent("notification.created", map[string]any{"id": 7})
hub.Publish(ctx, broadcast.UserChannel("42"), event)
```

In the browser:

```tsx
useBroadcast(`private-user.${viewer.id}`, {
    "notification.created": () => refetchUnread(),
}, { onResync: refetchUnread });
```

## Both transports, and why that is not indulgence

A seam fitted against one implementation is fitted by accident. The
conformance suite in `transporttest` was written before either transport, and
the WebSocket run immediately failed one case with **501 Not Implemented**:
the library reaches for `http.Hijacker` by *type assertion*, and a type
assertion traverses nothing. An event stream goes through
`http.ResponseController` and was happy with `Unwrap` alone — so a suite run
against one transport would have passed, and that is the same gap that had
tasker's own dashboard stream answering 500 in a real application.

They also differ in two ways the contract has to admit:

| | `sse` | `ws` |
|---|---|---|
| client→server | no — reconnects to change channels | yes |
| liveness | a write succeeds into the kernel buffer long after the peer is gone | a ping is answered |
| dependencies | none | `coder/websocket` |
| browser connection cap | one of ~6 per origin on HTTP/1.1 | ~255 |

**Which to use.** Behind nginx or Caddy terminating HTTP/2, prefer `sse`: one
goroutine instead of two, no dependency, and the browser's own reconnect does
half the work. In a single binary with no proxy there is no HTTP/2, so an event
stream permanently occupies one of about six connections per origin *across
every tab* — use `ws` there.

## Security

Both transports authenticate from a **cookie**, because neither a browser
`EventSource` nor a browser `WebSocket` can set an `Authorization` header.
That makes the origin check load-bearing, and the two protocols are not
equally exposed:

- **A WebSocket is not subject to CORS.** Any page can open one and the
  browser attaches the cookies unless `Origin` is checked. The `ws` transport
  keeps the library's own check and this module exposes no way to disable it.
- **An event stream *is* subject to CORS**, so it is same-origin by default
  and this module sends no CORS headers at all.

`AllowedOrigins` is **required**. An unset allowlist is the difference between
a private channel and a public one, so it is a boot error rather than a
default. `[]string{"*"}` is honoured, because somebody wrote it down.

Authorization runs **before the protocol is established**, and again for any
channel added mid-connection. A refusal is an HTTP status: a browser gets
almost nothing out of a connection that was accepted and then closed, and its
reconnect logic cannot tell that from a flaky network, so accept-then-close is
a reconnect loop against an endpoint that will never admit it.

Channel patterns match **segment by segment**, deliberately not as regular
expressions. `api`'s CSRF `ExcludePatterns` were matched with
`regexp.MatchString`, which is an unanchored "contains", so `/api` also
excluded `/x/api/y`. That bug in an authorization matcher *grants* access
rather than skipping a check.

## What it promises, and what it does not

**`Publish` means "accepted for delivery", never "delivered."** No API here
ever means delivered. Delivery is at-least-once, which is why `Event.ID`
exists and why the client deduplicates on it.

**There is no backlog, so `Last-Event-ID` is not honoured** — and the stream
does not emit `id:` at all, rather than inviting the browser to send a header
the server cannot answer. Every ready frame carries `resync: true` instead, and
the client calls `onResync` so the application re-fetches. That is the honest
version of resume, and it is the path a laptop waking up needs anyway.

**A slow client is closed, not buffered.** An unbounded queue is a memory leak
one slow client wide; a dropped frame is an event nobody can know they missed.
A closed connection is something the client already handles.

**A lost bus subscription closes the connections on it.** An open stream
receiving nothing is the worst failure mode here, and it is indistinguishable
from a quiet channel unless somebody says so.

## Buses

The **in-process bus is the default**, and a single binary with no Redis is a
supported mode rather than a degraded one.

The **Redis bus** is for more than one replica. It reads with `Receive` rather
than ranging over `Channel`, because `Channel` reconnects on its own and never
closes — convenient for a cache, and wrong here: events published while the
connection was down are gone, and a subscriber left open would receive nothing
with no way to know. A re-subscription is treated as a gap for the same reason.

`Prefix` is required: pub/sub ignores the selected database, so two
applications sharing one Redis with no prefix deliver each other's events. And
the bus never borrows the application's cache client — go-redis dedicates a
connection to each subscription for its lifetime and does not return it, so
the symptom of sharing is the *cache* blocking on a read.

## Scale, honestly

About **10,000 connections per process** is comfortable: ~16–24KB each for an
event stream, ~32–40KB for a WebSocket, one goroutine against two. The wall
arrives earlier at Redis than at HTTP, which is why `PatternSubscribe` is the
default — one subscription per process, routed locally, rather than one Redis
connection per connected user.

Past that: a dedicated fleet of connection-holding processes, then sharded
channels so Redis carries O(shards) rather than O(users), then Redis Streams
with consumer groups — which also buys the backlog that would make resume
real.

## Not built

Presence channels (a distributed-systems feature wearing a UI feature's
clothes) · arbitrary client→server application messages (v0 admits three
control commands; everything an application wants to *do* goes through HTTP
routes that already have CSRF, validation and authorization) · a message
backlog and true resume · guaranteed delivery and acks (`tasker`'s job; a
notification is already durable in Postgres) · per-event filtering beyond the
channel (one more place for an authorization bug) · compression · long-polling
· a lossy per-channel mode, though `Frame.Seq` is already in the wire format
for the day one exists.
