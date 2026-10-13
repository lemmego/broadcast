// A client for lemmego/broadcast, for an Inertia or any other React app.
//
// Three rules shape this file, and the first is the one that has already cost
// this project a day:
//
//  1. Nothing touches window, document, EventSource or WebSocket during a
//     render, and nothing at module scope touches them at all. A server-side
//     render runs this module in Node, where those do not exist — and the
//     failure is a 500 from the renderer, or worse a silent fallback to an
//     empty container. Read browser globals inside an effect, never in a
//     component body.
//  2. Delivery is at-least-once, so events are deduplicated on their id.
//  3. The server never promises a resumable log, so every reconnection asks
//     the application to re-fetch. onResync is not an error path; it is the
//     normal path after a laptop wakes up.

export type BroadcastEvent = {
  name: string;
  data: unknown;
  id: string;
  at?: string;
};

export type ConnectionState = "connecting" | "open" | "closed";

export type ConnectOptions = {
  /** "sse" (default) or "ws". See the note on transports below. */
  transport?: "sse" | "ws";
  /** Where the endpoints are mounted. Default "/broadcast". */
  prefix?: string;
  /** Called whenever the client may have missed events and should re-fetch. */
  onResync?: () => void;
  onStateChange?: (state: ConnectionState) => void;
};

export type Handlers = Record<string, (event: BroadcastEvent) => void>;

export type Connection = {
  /** Subscribe to a channel. Returns a function that unsubscribes. */
  subscribe(channel: string, handlers: Handlers): () => void;
  state(): ConnectionState;
  close(): void;
};

type Frame = {
  kind: "ready" | "event" | "error" | "close";
  channel?: string;
  seq: number;
  event?: BroadcastEvent | null;
  meta?: unknown;
};

type ReadyMeta = {
  stream: string;
  protocol: string;
  heartbeat_ms: number;
  channels: string[];
  refused?: { channel: string; reason: string }[];
  resumed: boolean;
  resync: boolean;
};

type CloseMeta = {
  code: string;
  message?: string;
  reconnect: boolean;
  after_ms: number;
  resync: boolean;
};

const MAX_SEEN = 256;
const BASE_DELAY = 1_000;
const MAX_DELAY = 30_000;

/**
 * Open a connection.
 *
 * Returns an inert connection when there is no window, so a careless caller
 * at module scope cannot throw inside a server renderer. That is the second
 * of three layers: no module-scope side effects, an inert connection here,
 * and useBroadcast doing all its work inside an effect.
 */
export function connect(options: ConnectOptions = {}): Connection {
  if (typeof window === "undefined") {
    return {
      subscribe: () => () => {},
      state: () => "closed",
      close: () => {},
    };
  }
  return new Client(options);
}

class Client implements Connection {
  private readonly prefix: string;
  private readonly transport: "sse" | "ws";
  private readonly onResync?: () => void;
  private readonly onStateChange?: (state: ConnectionState) => void;

  private channels = new Map<string, Set<Handlers>>();
  private source: EventSource | WebSocket | null = null;
  private currentState: ConnectionState = "closed";
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closedByUs = false;
  private hasConnectedBefore = false;

  // Bounded, because delivery is at-least-once and this would otherwise grow
  // for as long as the tab is open.
  private seen: string[] = [];
  private seenSet = new Set<string>();

  constructor(options: ConnectOptions) {
    this.prefix = options.prefix ?? "/broadcast";
    this.transport = options.transport ?? "sse";
    this.onResync = options.onResync;
    this.onStateChange = options.onStateChange;

    // The laptop-wake case, which is the common one rather than an edge: a
    // tab that was asleep reconnects at once instead of waiting out a
    // backoff that may have grown to thirty seconds.
    document.addEventListener("visibilitychange", this.onVisible);
  }

  subscribe(channel: string, handlers: Handlers): () => void {
    let set = this.channels.get(channel);
    if (!set) {
      set = new Set();
      this.channels.set(channel, set);
    }
    set.add(handlers);

    // A channel added after the connection is open needs the server to know.
    // An event stream has no inbound path, so it reconnects with the new
    // channel list; a WebSocket sends a command.
    this.ensureSubscribed(channel);

    return () => {
      const current = this.channels.get(channel);
      if (!current) return;
      current.delete(handlers);
      if (current.size > 0) return;
      this.channels.delete(channel);
      this.ensureUnsubscribed(channel);
    };
  }

  state(): ConnectionState {
    return this.currentState;
  }

  close(): void {
    this.closedByUs = true;
    document.removeEventListener("visibilitychange", this.onVisible);
    this.clearTimer();
    this.teardown();
    this.setState("closed");
  }

  // --- connecting ------------------------------------------------------

  private ensureSubscribed(channel: string): void {
    if (!this.source) {
      this.open();
      return;
    }
    if (this.transport === "ws" && this.source instanceof WebSocket) {
      if (this.source.readyState === WebSocket.OPEN) {
        this.source.send(JSON.stringify({ kind: "subscribe", channels: [channel] }));
      }
      return;
    }
    // An event stream carries no commands, so the channel set changes by
    // reconnecting. One round trip, during which an event can be missed —
    // which is why the ready frame that follows asks for a re-fetch.
    this.reopen();
  }

  private ensureUnsubscribed(channel: string): void {
    if (!this.source) return;
    if (this.transport === "ws" && this.source instanceof WebSocket) {
      if (this.source.readyState === WebSocket.OPEN) {
        this.source.send(JSON.stringify({ kind: "unsubscribe", channels: [channel] }));
      }
      return;
    }
    if (this.channels.size === 0) {
      this.teardown();
      this.setState("closed");
      return;
    }
    this.reopen();
  }

  private url(): string {
    const query = new URLSearchParams();
    for (const channel of this.channels.keys()) {
      query.append("channel", channel);
    }
    const path = `${this.prefix}/${this.transport}?${query.toString()}`;
    if (this.transport === "sse") return path;

    // A WebSocket needs an absolute URL with a ws scheme.
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${scheme}//${window.location.host}${path}`;
  }

  private open(): void {
    if (this.closedByUs || this.channels.size === 0) return;
    this.teardown();
    this.setState("connecting");

    if (this.transport === "ws") {
      const socket = new WebSocket(this.url());
      this.source = socket;
      socket.onopen = () => this.attempt = 0;
      socket.onmessage = (message) => this.onPayload(message.data);
      socket.onclose = () => this.onDisconnected();
      socket.onerror = () => {
        /* onclose follows, which is where the reconnection happens */
      };
      return;
    }

    // withCredentials so the session cookie travels, which is the only
    // mechanism an EventSource has: it cannot set an Authorization header.
    const stream = new EventSource(this.url(), { withCredentials: true });
    this.source = stream;
    // Named frame kinds, because the server puts the kind in the event:
    // field and all the variable content in the JSON on the data: line.
    for (const kind of ["ready", "event", "error", "close"]) {
      stream.addEventListener(kind, (e) => this.onPayload((e as MessageEvent).data));
    }
    stream.onerror = () => {
      // EventSource reconnects by itself, but only for a transport-level
      // failure — and it does so without telling the application, which
      // would leave a gap nobody re-fetched. Taken over here instead.
      this.onDisconnected();
    };
  }

  private reopen(): void {
    this.attempt = 0;
    this.open();
  }

  private onPayload(raw: unknown): void {
    if (typeof raw !== "string") return;

    let frame: Frame;
    try {
      frame = JSON.parse(raw) as Frame;
    } catch {
      return;
    }

    switch (frame.kind) {
      case "ready": {
        this.setState("open");
        this.attempt = 0;
        const meta = frame.meta as ReadyMeta | undefined;
        // Asked for on every reconnection, because with no backlog the
        // server cannot promise nothing was missed. The first connection of
        // a page load does not need it: the page just loaded its own state.
        if (this.hasConnectedBefore && (meta?.resync ?? true)) {
          this.onResync?.();
        }
        this.hasConnectedBefore = true;
        return;
      }

      case "event": {
        const event = frame.event;
        if (!event || !event.name) return;
        if (event.id && this.alreadySeen(event.id)) return;
        for (const handlers of this.channels.get(frame.channel ?? "") ?? []) {
          handlers[event.name]?.(event);
        }
        return;
      }

      case "error":
        // One channel refused on a connection that stays open with its
        // others. Reported rather than retried: retrying a refusal is how a
        // client ends up hammering an endpoint that will never admit it.
        console.warn("broadcast: a subscription was refused", frame.meta);
        return;

      case "close": {
        const meta = frame.meta as CloseMeta | undefined;
        if (meta && !meta.reconnect) {
          // "unauthorized", normally. Coming back would be a loop.
          this.closedByUs = true;
          this.teardown();
          this.setState("closed");
          return;
        }
        if (meta?.resync) this.onResync?.();
        this.scheduleReconnect(meta?.after_ms);
        return;
      }
    }
  }

  private alreadySeen(id: string): boolean {
    if (this.seenSet.has(id)) return true;
    this.seenSet.add(id);
    this.seen.push(id);
    if (this.seen.length > MAX_SEEN) {
      const oldest = this.seen.shift();
      if (oldest) this.seenSet.delete(oldest);
    }
    return false;
  }

  private onDisconnected(): void {
    if (this.closedByUs) return;
    this.setState("connecting");
    this.scheduleReconnect();
  }

  private scheduleReconnect(afterMS?: number): void {
    if (this.closedByUs || this.reconnectTimer) return;
    this.teardown();

    // Full jitter: a server restart disconnects every client at once, and
    // without jitter they all come back in the same instant, at a process
    // that has not finished booting.
    const ceiling = Math.min(MAX_DELAY, BASE_DELAY * 2 ** this.attempt);
    const base = afterMS && afterMS > 0 ? afterMS : 0;
    const delay = base + Math.random() * ceiling;
    this.attempt = Math.min(this.attempt + 1, 10);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.open();
    }, delay);
  }

  private onVisible = (): void => {
    if (document.visibilityState !== "visible") return;
    if (this.closedByUs || this.currentState === "open") return;
    this.clearTimer();
    this.reopen();
  };

  private clearTimer(): void {
    if (this.reconnectTimer === null) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private teardown(): void {
    const source = this.source;
    this.source = null;
    if (!source) return;
    if (source instanceof WebSocket) {
      source.onclose = null;
      source.onerror = null;
      source.close();
      return;
    }
    source.onerror = null;
    source.close();
  }

  private setState(state: ConnectionState): void {
    if (this.currentState === state) return;
    this.currentState = state;
    this.onStateChange?.(state);
  }
}
