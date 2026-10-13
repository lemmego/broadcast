// A React hook over the client.
//
// Every browser global it touches is read inside useEffect, which is the
// guarantee that matters: effects do not run under renderToString, so this is
// inert in a server renderer. Reading window during a render is the bug that
// has already cost this project a day — on a page that read
// window.location.search in a component body and answered 500 from the
// renderer.

import { useEffect, useRef } from "react";
import { connect, type BroadcastEvent, type ConnectOptions, type Connection, type Handlers } from "./broadcast";

/**
 * Subscribe to a channel for as long as the component is mounted.
 *
 * The handlers are kept in a ref and read through it, so a parent that
 * re-renders on every keystroke does not tear the subscription down and
 * build it again — which would reconnect the stream and ask the application
 * to re-fetch, repeatedly.
 */
export function useBroadcast(
  channel: string | null | undefined,
  handlers: Handlers,
  options: ConnectOptions = {},
): void {
  const latest = useRef(handlers);
  latest.current = handlers;

  const resync = useRef(options.onResync);
  resync.current = options.onResync;

  useEffect(() => {
    if (!channel) return;

    const connection = sharedConnection({
      ...options,
      onResync: () => resync.current?.(),
    });

    const unsubscribe = connection.subscribe(channel, {
      // One indirection so the identity of this object is stable across
      // renders while the functions behind it stay current.
      ...proxyHandlers(latest),
    });

    return unsubscribe;
    // Only the channel and the mount matter. The handlers are read through a
    // ref on purpose, so they are deliberately not dependencies.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [channel, options.prefix, options.transport]);
}

// proxyHandlers builds a handler map that forwards to whatever is current.
//
// The keys have to be known at subscribe time, so the map is built from the
// handlers present on the first render. A component that adds an event name
// later needs a different channel or a remount — which is a fair trade for
// not reconnecting on every parent render.
function proxyHandlers(latest: { current: Handlers }): Handlers {
  const forwarding: Handlers = {};
  for (const name of Object.keys(latest.current)) {
    forwarding[name] = (event: BroadcastEvent) => latest.current[name]?.(event);
  }
  return forwarding;
}

// One connection per tab, shared by every hook.
//
// A connection per component would open one HTTP connection per component,
// and a browser caps an origin at about six over HTTP/1.1 — so three
// components with a stream each would starve the page's own requests.
let shared: Connection | null = null;
let sharedKey = "";

function sharedConnection(options: ConnectOptions): Connection {
  const key = `${options.prefix ?? "/broadcast"}:${options.transport ?? "sse"}`;
  if (shared && sharedKey === key) return shared;
  shared?.close();
  shared = connect(options);
  sharedKey = key;
  return shared;
}
