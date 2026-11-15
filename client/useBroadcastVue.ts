// A Vue composable over the client.
//
// Every browser global it touches is reached inside onMounted, and in Vue that
// is the guarantee that matters rather than a tidiness rule: a component's
// setup() runs during server-side rendering and its mounted hook does not. So
// this is inert in a renderer. Reading window during setup is the bug that has
// already cost this project a day — on a page that read window.location.search
// while rendering and answered 500 from the renderer.

import { onMounted, onUnmounted, toValue, watch, type MaybeRefOrGetter } from "vue";
import { connect, type ConnectOptions, type Connection, type Handlers } from "./broadcast";

/**
 * Subscribe to a channel for as long as the component is mounted.
 *
 * The channel may be a string, a ref or a getter, so a component whose channel
 * comes from a prop resubscribes when that prop changes and stays subscribed
 * when anything else does.
 *
 * There is deliberately no equivalent of the React hook's ref indirection, and
 * that is a property of Vue rather than something left out: setup() runs once
 * per component, so the handlers object is already stable for the component's
 * lifetime. React keeps them in a ref because its component function re-runs
 * on every render and hands over a fresh object each time.
 *
 * `onResync` and `onStateChange` belong to the connection and not to the
 * subscription, and the connection is shared per tab — so when a second
 * component subscribes, the first one's callbacks are the ones that run. Put
 * anything that must happen per component in a handler.
 */
export function useBroadcast(
  channel: MaybeRefOrGetter<string | null | undefined>,
  handlers: Handlers,
  options: ConnectOptions = {},
): void {
  let unsubscribe: (() => void) | null = null;

  const open = (name: string | null | undefined) => {
    // Always drop the previous subscription first. A channel that changes
    // from one name to another would otherwise leave the old one delivering,
    // which looks like a page showing somebody else's events.
    unsubscribe?.();
    unsubscribe = null;
    if (!name) return;
    unsubscribe = sharedConnection(options).subscribe(name, handlers);
  };

  onMounted(() => {
    open(toValue(channel));
    // Created inside onMounted, so it is never set up in a server renderer,
    // and stopped with the component because this is still its effect scope.
    watch(() => toValue(channel), open);
  });

  onUnmounted(() => {
    unsubscribe?.();
    unsubscribe = null;
  });
}

// One connection per tab, shared by every composable.
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
