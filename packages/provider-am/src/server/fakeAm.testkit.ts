// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - a raw HTTP stand-in for `am serve`, speaking plain JSON and SSE.
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";

import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

import type { AmMessageState, AmTranscriptEntry } from "./api.ts";

export type FakeEntry = Omit<AmTranscriptEntry, "index">;

export interface FakeAgent {
  status: string;
  statusReason?: string | null;
  dir: string;
  provider: string;
  entries: Array<FakeEntry>;
}

export interface FakeMessage {
  readonly agent: string;
  readonly text: string;
  readonly mode: string;
  state: AmMessageState;
  transcriptIndex: number | null;
  error?: string;
  /** am is mid-delivery: a cancel answers 409 `queued`, and the delivery then completes. */
  deliveringOnCancel?: boolean;
}

export interface FakeCall {
  readonly method: string;
  readonly path: string;
  readonly body: Record<string, unknown> | null;
  /** The receipt id a message POST answered with. */
  readonly messageId?: string;
}

export const FAKE_TOKEN = "fake-am-token";

/** An `am serve` stand-in on a loopback port, following the route contract in am's src/server.ts. */
export const makeFakeAm = Effect.fnUntraced(function* (
  options: { readonly hosts?: ReadonlyArray<{ name: string; reachable: boolean }> } = {},
) {
  const calls = yield* Queue.unbounded<FakeCall>();
  /** Keyed by am address: a bare name, or `host:name` for a fleet host's agent. */
  const agents = new Map<string, FakeAgent>();
  const messages = new Map<string, FakeMessage>();
  const log: Array<FakeCall> = [];
  let messageSeq = 0;
  const streams = new Set<NodeHttp.ServerResponse>();
  let markConnected!: () => void;
  const connected = new Promise<void>((resolve) => {
    markConnected = resolve;
  });
  const json = (res: NodeHttp.ServerResponse, status: number, body: unknown) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(body));
  };
  const handle = (req: NodeHttp.IncomingMessage, res: NodeHttp.ServerResponse, raw: string) => {
    const url = new URL(req.url ?? "/", "http://fake");
    const method = req.method ?? "GET";
    const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : null;
    if (req.headers.authorization !== `Bearer ${FAKE_TOKEN}`)
      return json(res, 401, { error: "unauthorized" });
    const parts = url.pathname
      .replace(/^\/api\/?/, "")
      .split("/")
      .filter(Boolean);
    const sendsMessage =
      method === "POST" && parts[0] === "agents" && parts[2] === "messages" && parts.length === 3;
    const messageId = sendsMessage ? `msg-${++messageSeq}` : undefined;
    const call = { method, path: url.pathname, body, ...(messageId ? { messageId } : {}) };
    log.push(call);
    Queue.offerUnsafe(calls, call);
    if (parts[0] === "health" && method === "GET") return json(res, 200, { ok: true, name: "am" });
    if (parts[0] === "hosts" && method === "GET")
      return json(res, 200, { local: "fake-local", hosts: options.hosts ?? [] });
    if (parts[0] === "events" && method === "GET") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write(`event: ready\ndata: ${JSON.stringify({ type: "ready" })}\n\n`);
      streams.add(res);
      res.on("close", () => streams.delete(res));
      markConnected();
      return;
    }
    if (parts[0] !== "agents") return json(res, 404, { error: "not found" });
    if (parts.length === 1 && method === "POST") {
      const host = typeof body?.host === "string" ? body.host : undefined;
      const name = host ? `${host}:${String(body?.name)}` : String(body?.name);
      if (agents.has(name)) return json(res, 409, { error: `agent "${name}" already exists` });
      agents.set(name, {
        status: "starting",
        dir: String(body?.dir),
        provider: typeof body?.provider === "string" ? body.provider : "codex",
        entries: [],
      });
      return json(res, 201, { ok: true, name });
    }
    const name = decodeURIComponent(parts[1] ?? "");
    const agent = agents.get(name);
    if (!agent) return json(res, 404, { error: `no local agent "${name}"` });
    if (parts.length === 2 && method === "GET")
      return json(res, 200, {
        name,
        status: agent.status,
        statusReason: agent.statusReason ?? null,
        provider: agent.provider,
        dir: agent.dir,
        queue: [],
        pane: null,
      });
    if (parts[2] === "transcript" && method === "GET") {
      const after = Number(url.searchParams.get("after") ?? "0");
      return json(res, 200, {
        provider: agent.provider,
        sessionId: null,
        status: agent.status,
        total: agent.entries.length,
        entries: agent.entries.slice(after).map((entry, offset) => ({
          index: after + offset,
          ...entry,
        })),
      });
    }
    if (parts[2] === "resume" && method === "POST") {
      agent.status = "starting";
      return json(res, 200, { ok: true });
    }
    if (sendsMessage && messageId) {
      const mode = typeof body?.mode === "string" ? body.mode : "queue";
      messages.set(messageId, {
        agent: name,
        text: String(body?.text),
        mode,
        state: "queued",
        transcriptIndex: null,
      });
      return json(res, 200, { ok: true, mode, id: messageId });
    }
    if (parts[2] === "messages" && parts.length === 4) {
      const id = decodeURIComponent(parts[3]!);
      const message = messages.get(id);
      if (!message || message.agent !== name) return json(res, 404, { error: "no such message" });
      if (method === "GET")
        return json(res, 200, {
          id,
          state: message.state,
          queuedAt: "2026-01-01T00:00:00.000Z",
          deliveredAt: message.transcriptIndex === null ? null : "2026-01-01T00:00:01.000Z",
          doneAt: message.state === "done" ? "2026-01-01T00:00:02.000Z" : null,
          transcriptIndex: message.transcriptIndex,
          ...(message.error ? { error: message.error } : {}),
        });
      if (method === "DELETE") {
        if (message.deliveringOnCancel) {
          message.deliveringOnCancel = false;
          message.state = "delivered";
          message.transcriptIndex = agent.entries.length;
          return json(res, 409, { error: "message is being delivered", state: "queued" });
        }
        if (message.state !== "queued")
          return json(res, 409, { error: "already delivered", state: message.state });
        message.state = "cancelled";
        return json(res, 200, { ok: true, state: "cancelled" });
      }
    }
    if (["interrupt", "stop"].includes(parts[2] ?? "") && method === "POST")
      return json(res, 200, { ok: true });
    return json(res, 404, { error: "not found" });
  };
  const server = NodeHttp.createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk) => (raw += chunk));
    req.on("end", () => handle(req, res, raw));
  });
  yield* Effect.acquireRelease(
    Effect.promise(() => new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))),
    () =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            for (const stream of streams) stream.end();
            server.closeAllConnections();
            server.close(() => resolve());
          }),
      ),
  );
  const { port } = server.address() as NodeNet.AddressInfo;
  const update = (name: string, change: (agent: FakeAgent) => void) =>
    Effect.sync(() => {
      const agent = agents.get(name);
      if (!agent) throw new Error(`no fake agent ${name}`);
      change(agent);
      const frame = `id: 1\nevent: fleet\ndata: ${JSON.stringify({ type: "fleet", event: "changed", agent: name })}\n\n`;
      for (const stream of streams) stream.write(frame);
    });
  return {
    url: `http://127.0.0.1:${port}`,
    agents,
    /** Resolves once a client holds the event stream open, so later wakes reach it. */
    connected: Effect.promise(() => connected),
    /** Mutates an agent, then wakes listeners the way am's daemon does after a state change. */
    update,
    messages,
    /** Every authorized call so far, in order. */
    log,
    /** Changes a message's receipt (and optionally its agent), then wakes listeners. */
    receipt: (id: string, change: (message: FakeMessage, agent: FakeAgent) => void) =>
      Effect.suspend(() => {
        const message = messages.get(id);
        if (!message) throw new Error(`no fake message ${id}`);
        return update(message.agent, (agent) => change(message, agent));
      }),
    takeCall: Effect.fnUntraced(function* (method: string, suffix: string) {
      while (true) {
        const call = yield* Queue.take(calls);
        if (call.method === method && call.path.endsWith(suffix)) return call;
      }
    }),
  };
});
