// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off globalDate:off - a raw HTTP stand-in for `am serve`, speaking plain JSON and SSE and expiring prompts by the wall clock as am does.
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";

import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

import type {
  AmMessageState,
  AmModelCatalog,
  AmPermissionRequest,
  AmTranscriptEntry,
} from "./api.ts";

export type FakeEntry = Omit<AmTranscriptEntry, "index">;

export interface FakeAgent {
  status: string;
  statusReason?: string | null;
  dir: string;
  provider: string;
  entries: Array<FakeEntry>;
  /** Messages am holds for delivery, shown on the agent's detail. */
  queue?: Array<unknown>;
  /** am's last state change, ISO; the fleet list reports it as the agent's activity. */
  updatedAt?: string;
  worktreePath?: string;
  worktreeBranch?: string;
  repoRoot?: string;
  /** The next conditional stop answers this 409, as when work arrives between T3's checks and am's. */
  stopRefusal?: { readonly error: string; readonly reason: string };
  /** Null or absent runs the provider's default. */
  model?: string | null;
  effort?: string | null;
  /** The next conditional restart answers this 409. */
  restartRefusal?: { readonly error: string; readonly reason: string };
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
  /** How many interrupts answer 409 because the delivered message's turn has not started. */
  unstartedInterrupts?: number;
  /** How many interrupts answer 409 because the mailbox is busy. */
  busyInterrupts?: number;
  /** How many cancels answer 409 because the mailbox is busy. */
  busyCancels?: number;
  /** am sent this message's turn an Esc. */
  escaped?: boolean;
  /** The pane never returns to an empty prompt after the Esc, so am leaves the turn running. */
  staysBusyAfterEsc?: boolean;
  /** On a done receipt: am ended its turn after an Esc. */
  interrupted?: boolean;
  /** On a done receipt: whether am found the final reply in the transcript. */
  transcriptComplete?: boolean;
  /** The next cancel or interrupt answers this 409 instead, as an older or remote am might. */
  conflict?: { readonly error: string; readonly reason?: string };
}

export interface FakePermission {
  readonly agent: string;
  readonly request: AmPermissionRequest;
  /** am's record states: `applied` once an answer reached the provider. */
  state: "pending" | "applied" | "stale" | "expired";
  optionId?: string;
  resolution?: string;
  /** The next answer gets this 409 instead, as a remote or an older am might. */
  conflict?: { readonly error: string; readonly reason?: string };
  /** The next answer is applied but its reply never arrives, and nothing wakes listeners. */
  loseResponse?: boolean;
}

export interface FakeCall {
  readonly method: string;
  readonly path: string;
  readonly search?: string;
  readonly body: Record<string, unknown> | null;
  /** The receipt id a message POST answered with. */
  readonly messageId?: string;
}

export const FAKE_TOKEN = "fake-am-token";

/** Shaped like a real `GET /api/models`: Claude's efforts are the provider's, Codex's per model. */
export const FAKE_MODEL_CATALOG: AmModelCatalog = [
  {
    provider: "claude",
    models: [
      { id: "opus", label: "Opus", efforts: [] },
      { id: "sonnet", efforts: [] },
      { id: "claude-fable-5-1[1m]", label: "Fable 5.1 (1M context)", efforts: [] },
    ],
    efforts: ["low", "medium", "high", "xhigh", "max"],
  },
  {
    provider: "codex",
    models: [
      { id: "gpt-6.1-sol", efforts: ["low", "medium", "high", "ultra"], defaultEffort: "medium" },
      { id: "gpt-6-luna", efforts: ["low", "high"], defaultEffort: "low" },
    ],
    efforts: [],
  },
  { provider: "gemini", models: [{ id: "pro", efforts: [] }], efforts: [] },
];

/** An `am serve` stand-in on a loopback port, following the route contract in am's src/server.ts. */
export const makeFakeAm = Effect.fnUntraced(function* (
  options: {
    readonly hosts?: ReadonlyArray<{ name: string; reachable: boolean }>;
    /** False plays an am from before message receipts, on health and on agent detail. */
    readonly messageReceipts?: boolean;
    /** False plays an am from before the conditional stop: it ignores `ifIdle` and stops regardless. */
    readonly stopIfIdle?: boolean;
    /** True plays an am that relays its agents' permission prompts; older ones have no routes. */
    readonly permissionRequests?: boolean;
    /**
     * Plays an am that lists its providers' models, with this list for each host (undefined for
     * its own machine) and restarts agents on another model. Older ones have neither route.
     */
    readonly models?: (host: string | undefined) => AmModelCatalog;
  } = {},
) {
  const receiptsFlag = options.messageReceipts === false ? {} : { messageReceipts: true };
  const stopIfIdle = options.stopIfIdle !== false;
  const capabilities = { ...receiptsFlag, ...(stopIfIdle ? { stopIfIdle: true } : {}) };
  const modelFlags = options.models ? { restartModel: true } : {};
  /** Keyed by am's request id. */
  const permissions = new Map<string, FakePermission>();
  let permissionSeq = 0;
  // Like am, a read is what expires a request whose deadline passed.
  const pendingOf = (name: string) =>
    [...permissions.values()].filter((permission) => {
      const { expiresAt } = permission.request;
      if (permission.state === "pending" && expiresAt && Date.parse(expiresAt) <= Date.now())
        permission.state = "expired";
      return permission.agent === name && permission.state === "pending";
    });
  const calls = yield* Queue.unbounded<FakeCall>();
  /** Keyed by am address: a bare name, or `host:name` for a fleet host's agent. */
  const agents = new Map<string, FakeAgent>();
  const messages = new Map<string, FakeMessage>();
  const log: Array<FakeCall> = [];
  /** Each Esc am sent to an agent's TUI, by agent address. */
  const escapes: Array<string> = [];
  let messageSeq = 0;
  const streams = new Set<NodeHttp.ServerResponse>();
  /** Fleet hosts the fleet list reports as unreachable. */
  const unreachable: Array<string> = [];
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
    const call = {
      method,
      path: url.pathname,
      ...(url.search ? { search: url.search } : {}),
      body,
      ...(messageId ? { messageId } : {}),
    };
    log.push(call);
    Queue.offerUnsafe(calls, call);
    if (parts[0] === "health" && method === "GET")
      return json(res, 200, {
        ok: true,
        name: "am",
        ...capabilities,
        ...(options.models ? { models: true } : {}),
      });
    if (parts[0] === "models" && method === "GET" && options.models)
      return json(res, 200, options.models(url.searchParams.get("host") ?? undefined));
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
    if (parts.length === 1 && method === "GET")
      return json(res, 200, {
        rows: [...agents].flatMap(([address, agent]) => {
          const colon = address.indexOf(":");
          const host = colon === -1 ? undefined : address.slice(0, colon);
          if (host && unreachable.includes(host)) return [];
          return [
            {
              name: colon === -1 ? address : address.slice(colon + 1),
              ...(host ? { host } : {}),
              status: agent.status,
              provider: agent.provider,
              queued: 0,
              dir: agent.dir,
              updatedAt: agent.updatedAt ?? "2026-01-01T00:00:00.000Z",
              ...(agent.worktreePath ? { worktreePath: agent.worktreePath } : {}),
              ...(agent.worktreeBranch ? { worktreeBranch: agent.worktreeBranch } : {}),
              ...(agent.repoRoot ? { repoRoot: agent.repoRoot } : {}),
            },
          ];
        }),
        unreachable,
      });
    if (parts.length === 1 && method === "POST") {
      const host = typeof body?.host === "string" ? body.host : undefined;
      const name = host ? `${host}:${String(body?.name)}` : String(body?.name);
      if (agents.has(name)) return json(res, 409, { error: `agent "${name}" already exists` });
      agents.set(name, {
        status: "starting",
        dir: String(body?.dir),
        provider: typeof body?.provider === "string" ? body.provider : "codex",
        entries: [],
        ...(typeof body?.model === "string" ? { model: body.model } : {}),
        ...(typeof body?.effort === "string" ? { effort: body.effort } : {}),
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
        queue: agent.queue ?? [],
        pane: null,
        ...capabilities,
        ...(options.models ? { model: agent.model ?? null, effort: agent.effort ?? null } : {}),
        ...modelFlags,
        ...(options.permissionRequests
          ? { permissionRequests: true, pendingPermissions: pendingOf(name).length }
          : {}),
      });
    if (parts[2] === "permissions" && options.permissionRequests) {
      if (parts.length === 3 && method === "GET")
        return json(res, 200, {
          requests: pendingOf(name).map((permission) => permission.request),
        });
      if (parts.length === 4 && method === "POST") {
        const permission = permissions.get(decodeURIComponent(parts[3]!));
        if (!permission || permission.agent !== name)
          return json(res, 404, { error: "no permission request" });
        const optionId = typeof body?.optionId === "string" ? body.optionId : "";
        if (!permission.request.options.some((option) => option.id === optionId))
          return json(res, 400, { error: "optionId must be one of the request's option ids" });
        if (permission.conflict) {
          const { conflict } = permission;
          delete permission.conflict;
          return json(res, 409, conflict);
        }
        if (permission.state !== "pending")
          return json(res, 409, {
            error: `permission request ${permission.request.id} is no longer pending`,
            reason:
              permission.state === "expired"
                ? "expired"
                : permission.state === "applied"
                  ? "already_resolved"
                  : "not_pending",
          });
        permission.state = "applied";
        permission.optionId = optionId;
        if (permission.loseResponse) {
          delete permission.loseResponse;
          return res.destroy();
        }
        json(res, 200, { ok: true, id: permission.request.id, optionId });
        return publish("permission-resolved", name);
      }
    }
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
          ...(message.state === "done" && message.interrupted ? { interrupted: true } : {}),
          ...(message.state === "done" && message.transcriptComplete !== undefined
            ? { transcriptComplete: message.transcriptComplete }
            : {}),
        });
      if (method === "DELETE") {
        if (message.conflict) {
          const { conflict } = message;
          delete message.conflict;
          return json(res, 409, conflict);
        }
        if (message.busyCancels) {
          message.busyCancels--;
          return json(res, 409, {
            error: `agent "${name}" mailbox is busy — retry the cancel`,
            state: "queued",
            reason: "mailbox_busy",
          });
        }
        if (message.deliveringOnCancel) {
          message.deliveringOnCancel = false;
          message.state = "delivered";
          message.transcriptIndex = agent.entries.length;
          return json(res, 409, {
            error: `message ${id} is being delivered`,
            state: "queued",
            reason: "already_delivered",
          });
        }
        if (message.state !== "queued")
          return json(res, 409, {
            error: `message ${id} is already ${message.state}`,
            state: message.state,
            reason: `already_${message.state}`,
          });
        message.state = "cancelled";
        return json(res, 200, { ok: true, state: "cancelled" });
      }
    }
    if (parts[2] === "interrupt" && method === "POST") {
      if (body?.messageId !== undefined && body.text !== undefined)
        return json(res, 400, { error: "messageId and text are mutually exclusive" });
      if (typeof body?.messageId !== "string") {
        escapes.push(name);
        return json(res, 200, { ok: true });
      }
      const id = body.messageId;
      const message = messages.get(id);
      if (!message || message.agent !== name) return json(res, 404, { error: "no such message" });
      const conflict = (error: string, reason: string) =>
        json(res, 409, { error, state: message.state, reason });
      if (message.conflict) {
        const refusal = message.conflict;
        delete message.conflict;
        return json(res, 409, refusal);
      }
      if (message.busyInterrupts) {
        message.busyInterrupts--;
        return conflict(`agent "${name}" mailbox is busy — retry`, "mailbox_busy");
      }
      if (message.state === "queued")
        return conflict(`message ${id} is not delivered yet`, "not_delivered");
      if (message.state !== "delivered")
        return conflict(`message ${id} is already ${message.state}`, `already_${message.state}`);
      if (message.unstartedInterrupts) {
        message.unstartedInterrupts--;
        return conflict(`message ${id} has not started yet`, "not_started");
      }
      if (message.escaped)
        return conflict(`message ${id} was already interrupted`, "already_interrupted");
      message.escaped = true;
      escapes.push(name);
      // am answers once the pane is back at its prompt: the turn's receipts are done and
      // interrupted, and the agent is idle, ready for its queue.
      if (!message.staysBusyAfterEsc) {
        for (const other of messages.values())
          if (other.agent === name && other.state === "delivered") {
            other.state = "done";
            other.interrupted = true;
          }
        agent.status = "idle";
      }
      return json(res, 200, { ok: true });
    }
    if (parts[2] === "restart" && method === "POST" && options.models) {
      if (body?.ifIdle !== true) return json(res, 400, { error: "T3 restarts only if idle" });
      const refuse = (error: string, reason: string) => json(res, 409, { error, reason });
      if (agent.restartRefusal) {
        const { error, reason } = agent.restartRefusal;
        delete agent.restartRefusal;
        return refuse(error, reason);
      }
      if (["working", "starting", "waiting", "needs-attention"].includes(agent.status))
        return refuse(`agent "${name}" is ${agent.status}`, "busy");
      if ((agent.queue?.length ?? 0) > 0)
        return refuse(`agent "${name}" has queued messages`, "queued");
      for (const message of messages.values())
        if (message.agent === name && message.state === "delivered")
          return refuse(`agent "${name}" has a turn that has not finished`, "in_turn");
      const colon = name.indexOf(":");
      const listed = options
        .models(colon === -1 ? undefined : name.slice(0, colon))
        .find((entry) => entry.provider === agent.provider);
      if (typeof body.model === "string" && !listed?.models.some((m) => m.id === body.model))
        return json(res, 400, { error: `unknown ${agent.provider} model "${body.model}"` });
      if (body.model !== undefined) agent.model = body.model as string | null;
      if (body.effort !== undefined) agent.effort = body.effort as string | null;
      agent.status = "starting";
      return json(res, 200, { ok: true, model: agent.model ?? null, effort: agent.effort ?? null });
    }
    if (parts[2] === "stop" && method === "POST") {
      if (stopIfIdle && body?.ifIdle === true) {
        const refuse = (error: string, reason: string) => json(res, 409, { error, reason });
        if (agent.stopRefusal) {
          const { error, reason } = agent.stopRefusal;
          delete agent.stopRefusal;
          return refuse(error, reason);
        }
        if (["working", "starting", "waiting", "needs-attention"].includes(agent.status))
          return refuse(`agent "${name}" is ${agent.status}`, "busy");
        if ((agent.queue?.length ?? 0) > 0)
          return refuse(`agent "${name}" has queued messages`, "queued");
        for (const message of messages.values())
          if (message.agent === name && message.state === "delivered")
            return refuse(
              `agent "${name}" has a delivered message whose turn has not finished`,
              "in_turn",
            );
      }
      agent.status = "exited";
      return json(res, 200, { ok: true });
    }
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
  function publish(event: string, agent: string) {
    const frame = `id: 1\nevent: fleet\ndata: ${JSON.stringify({ type: "fleet", event, agent })}\n\n`;
    for (const stream of streams) stream.write(frame);
  }
  const update = (name: string, change: (agent: FakeAgent) => void) =>
    Effect.sync(() => {
      const agent = agents.get(name);
      if (!agent) throw new Error(`no fake agent ${name}`);
      change(agent);
      publish("changed", name);
    });
  return {
    url: `http://127.0.0.1:${port}`,
    agents,
    unreachable,
    /** Resolves once a client holds the event stream open, so later wakes reach it. */
    connected: Effect.promise(() => connected),
    /** Mutates an agent, then wakes listeners the way am's daemon does after a state change. */
    update,
    messages,
    /** Every authorized call so far, in order. */
    log,
    escapes,
    /** Changes a message's receipt (and optionally its agent), then wakes listeners. */
    receipt: (id: string, change: (message: FakeMessage, agent: FakeAgent) => void) =>
      Effect.suspend(() => {
        const message = messages.get(id);
        if (!message) throw new Error(`no fake message ${id}`);
        return update(message.agent, (agent) => change(message, agent));
      }),
    permissions,
    /**
     * The agent's provider raises a permission prompt, as Claude's hook or Codex's app-server
     * would; `wake: false` records it without the event, as a fleet host's am does.
     */
    prompt: (
      name: string,
      request: Omit<AmPermissionRequest, "id" | "provider" | "createdAt" | "expiresAt"> &
        Partial<Pick<AmPermissionRequest, "provider" | "createdAt" | "expiresAt">>,
      wake = true,
    ) =>
      Effect.sync(() => {
        const id = `PERM${String(++permissionSeq).padStart(22, "0")}`;
        permissions.set(id, {
          agent: name,
          state: "pending",
          request: {
            provider: "claude",
            createdAt: "2026-01-01T00:00:03.000Z",
            expiresAt: null,
            ...request,
            id,
          },
        });
        if (wake) publish("permission-request", name);
        return id;
      }),
    /** Ends a prompt outside T3: answered in the pane (`stale`) or out of time (`expired`). */
    resolvePermission: (id: string, state: "stale" | "expired", wake = true) =>
      Effect.sync(() => {
        const permission = permissions.get(id);
        if (!permission) throw new Error(`no fake permission ${id}`);
        permission.state = state;
        if (wake) publish("permission-resolved", permission.agent);
      }),
    takeCall: Effect.fnUntraced(function* (method: string, suffix: string) {
      while (true) {
        const call = yield* Queue.take(calls);
        if (call.method === method && call.path.endsWith(suffix)) return call;
      }
    }),
  };
});
