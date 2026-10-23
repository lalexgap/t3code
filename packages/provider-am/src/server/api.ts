import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Sse from "effect/encoding/Sse";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";

/** The am HTTP API (`am serve`): bearer-token gated, one agent per tmux session. */

export const AmProvider = Schema.Literals(["claude", "codex"]);
export type AmProvider = typeof AmProvider.Type;

export const AmTranscriptEntry = Schema.Struct({
  index: Schema.Number,
  kind: Schema.Literals(["user", "assistant", "tool"]),
  text: Schema.optional(Schema.String),
  name: Schema.optional(Schema.String),
  input: Schema.optional(Schema.String),
  output: Schema.optional(Schema.String),
  error: Schema.optional(Schema.Boolean),
});
export type AmTranscriptEntry = typeof AmTranscriptEntry.Type;

export const AmTranscript = Schema.Struct({
  provider: Schema.String,
  sessionId: Schema.NullOr(Schema.String),
  status: Schema.String,
  total: Schema.Number,
  entries: Schema.Array(AmTranscriptEntry),
});
export type AmTranscript = typeof AmTranscript.Type;

export const AmAgentDetail = Schema.Struct({
  name: Schema.String,
  status: Schema.String,
  statusReason: Schema.optional(Schema.NullOr(Schema.String)),
  provider: Schema.String,
  dir: Schema.String,
  /** Messages waiting for delivery; a fleet host's detail may omit it. */
  queue: Schema.optional(Schema.Array(Schema.Unknown)),
  /** Set by an am whose `send --json` returns receipt ids; a fleet host's reflects that host's am. */
  messageReceipts: Schema.optional(Schema.Boolean),
  /** Set by an am that can stop an agent only while it is idle; a fleet host's reflects that host's am. */
  stopIfIdle: Schema.optional(Schema.Boolean),
  /** Set by an am that relays its agents' permission prompts; a fleet host's reflects that host's am. */
  permissionRequests: Schema.optional(Schema.Boolean),
  pendingPermissions: Schema.optional(Schema.Number),
  /** The agent's model and effort; null runs its provider's default. Absent from an older am. */
  model: Schema.optional(Schema.NullOr(Schema.String)),
  effort: Schema.optional(Schema.NullOr(Schema.String)),
  /** Set by an am that can restart an agent on another model; a fleet host's reflects that host's am. */
  restartModel: Schema.optional(Schema.Boolean),
});
export type AmAgentDetail = typeof AmAgentDetail.Type;

/** One row of `GET /api/agents`, am's cached fleet view; `host` is set on a fleet host's rows. */
export const AmFleetRow = Schema.Struct({
  name: Schema.String,
  host: Schema.optional(Schema.String),
  status: Schema.String,
  provider: Schema.String,
  dir: Schema.String,
  createdAt: Schema.optional(Schema.String),
  updatedAt: Schema.String,
  statusChangedAt: Schema.optional(Schema.String),
  /** Set only for agents am started in a worktree of its own. */
  worktreePath: Schema.optional(Schema.String),
  worktreeBranch: Schema.optional(Schema.String),
  repoRoot: Schema.optional(Schema.String),
});
export type AmFleetRow = typeof AmFleetRow.Type;

export const AmFleet = Schema.Struct({
  rows: Schema.Array(AmFleetRow),
  /** Fleet hosts whose rows am could not read; their agents are missing from `rows`. */
  unreachable: Schema.Array(Schema.String),
});
export type AmFleet = typeof AmFleet.Type;

export const AmMessageState = Schema.Literals([
  "queued",
  "delivered",
  "done",
  "cancelled",
  "failed",
]);
export type AmMessageState = typeof AmMessageState.Type;

/** am's delivery receipt for one message sent through `am serve`. */
export const AmMessageReceipt = Schema.Struct({
  id: Schema.String,
  state: AmMessageState,
  /** The transcript length when am delivered the message; its output starts here. */
  transcriptIndex: Schema.NullOr(Schema.Number),
  error: Schema.optional(Schema.NullOr(Schema.String)),
  /** Set on a `done` receipt whose turn was aborted with Esc rather than finished. */
  interrupted: Schema.optional(Schema.Boolean),
  /**
   * On a `done` receipt: whether the final reply was readable in the transcript (at or after
   * `transcriptIndex`) when am answered. Absent with no reply to check, or from an older am.
   */
  transcriptComplete: Schema.optional(Schema.Boolean),
});
export type AmMessageReceipt = typeof AmMessageReceipt.Type;

/** Why am refused a message or interrupt route (HTTP 409). */
export const AmConflictReason = Schema.Literals([
  "not_delivered",
  "already_done",
  "already_cancelled",
  "already_failed",
  "not_started",
  "already_interrupted",
  "no_session",
  "mailbox_busy",
  "already_delivered",
  "remote_too_old",
]);
export type AmConflictReason = typeof AmConflictReason.Type;
const isConflictReason = Schema.is(AmConflictReason);

/** Why am refused to stop an agent only while idle (HTTP 409). */
export const AmStopRefusalReason = Schema.Literals(["busy", "queued", "in_turn", "remote_too_old"]);
export type AmStopRefusalReason = typeof AmStopRefusalReason.Type;
const isStopRefusalReason = Schema.is(AmStopRefusalReason);

/** A choice the agent's own provider offers; its id goes back to am unchanged. */
export const AmPermissionOption = Schema.Struct({
  id: Schema.String,
  label: Schema.String,
  // A kind added after this was written is shown nowhere rather than failing the listing.
  kind: Schema.String,
});
export type AmPermissionOption = typeof AmPermissionOption.Type;

/** A permission prompt the agent's provider is holding open until someone answers it. */
export const AmPermissionRequest = Schema.Struct({
  id: Schema.String,
  provider: Schema.String,
  tool: Schema.String,
  summary: Schema.String,
  input: Schema.optional(Schema.Unknown),
  options: Schema.Array(AmPermissionOption),
  createdAt: Schema.String,
  /** Null while the provider waits with no deadline (Codex). */
  expiresAt: Schema.NullOr(Schema.String),
});
export type AmPermissionRequest = typeof AmPermissionRequest.Type;

const AmPermissions = Schema.Struct({ requests: Schema.Array(AmPermissionRequest) });

/** Why am refused an answer (HTTP 409). */
export const AmPermissionConflictReason = Schema.Literals([
  "expired",
  "already_resolved",
  "not_pending",
  "remote_too_old",
]);
export type AmPermissionConflictReason = typeof AmPermissionConflictReason.Type;
const isPermissionConflictReason = Schema.is(AmPermissionConflictReason);

export type AmPermissionAnswer =
  | { readonly answered: true }
  | {
      readonly answered: false;
      /** Undefined when am gave a reason T3 does not know. */
      readonly reason: AmPermissionConflictReason | undefined;
      readonly error: string;
    };

export const AmHosts = Schema.Struct({
  local: Schema.String,
  hosts: Schema.Array(Schema.Struct({ name: Schema.String, reachable: Schema.Boolean })),
});
export type AmHosts = typeof AmHosts.Type;

export const AmHealth = Schema.Struct({
  messageReceipts: Schema.optional(Schema.Boolean),
  stopIfIdle: Schema.optional(Schema.Boolean),
  /** Set by an am that lists its providers' models. */
  models: Schema.optional(Schema.Boolean),
});
export type AmHealth = typeof AmHealth.Type;

export const AmCatalogModel = Schema.Struct({
  id: Schema.String,
  label: Schema.optional(Schema.String),
  description: Schema.optional(Schema.String),
  /** Empty when the provider's own list applies to every model, as Claude's does. */
  efforts: Schema.Array(Schema.String),
  defaultEffort: Schema.optional(Schema.String),
});
export type AmCatalogModel = typeof AmCatalogModel.Type;

/** One provider's entry in `GET /api/models`; a provider T3 does not know is skipped later. */
export const AmProviderCatalog = Schema.Struct({
  provider: Schema.String,
  models: Schema.Array(AmCatalogModel),
  efforts: Schema.Array(Schema.String),
});
export type AmProviderCatalog = typeof AmProviderCatalog.Type;

export const AmModelCatalog = Schema.Array(AmProviderCatalog);
export type AmModelCatalog = typeof AmModelCatalog.Type;

/** A restart's model or effort: a value, null for the provider's default, or absent to keep it. */
export interface AmModelChange {
  readonly model?: string | null;
  readonly effort?: string | null;
}

export type AmRestartResult =
  | { readonly restarted: true; readonly model: string | null; readonly effort: string | null }
  | { readonly restarted: false; readonly reason: AmStopRefusalReason; readonly error: string };

const RestartedBody = Schema.Struct({
  model: Schema.optional(Schema.NullOr(Schema.String)),
  effort: Schema.optional(Schema.NullOr(Schema.String)),
});

const SentMessage = Schema.Struct({ id: Schema.String });
const ErrorBody = Schema.Struct({ error: Schema.String });
// A refusal relayed from a fleet host's am can lack a state, and an am older than reasons lacks one.
const ConflictBody = Schema.Struct({
  error: Schema.String,
  state: Schema.optional(AmMessageState),
  reason: Schema.optional(Schema.String),
});
// Starting an agent can wait on its provider (codex creates its thread up front), so allow
// more than a plain read needs; a hung am must still not hold a T3 session open forever.
const REQUEST_TIMEOUT_MS = 60_000;

export class AmApiError extends Schema.TaggedError<AmApiError>()("AmApiError", {
  operation: Schema.String,
  /** HTTP status, or null when am could not be reached. */
  status: Schema.NullOr(Schema.Number),
  detail: Schema.String,
}) {
  override get message(): string {
    return `Agent Motel ${this.operation} failed: ${this.detail}`;
  }
}

export interface CreateAgentInput {
  readonly name: string;
  /** A fleet host from am's `remotes`; absent starts the agent on am's own machine. */
  readonly host?: string;
  readonly dir: string;
  readonly provider?: AmProvider;
  /** Absent runs the provider's default. */
  readonly model?: string;
  readonly effort?: string;
}

/** am's 409. `reason` is undefined when am gave none T3 knows; `error` is am's own text. */
export interface AmConflict {
  readonly state: AmMessageState | undefined;
  readonly reason: AmConflictReason | undefined;
  readonly error: string;
}

export type AmCancelResult =
  | { readonly cancelled: true }
  | ({ readonly cancelled: false } & AmConflict);

export type AmStopResult =
  | { readonly stopped: true }
  | { readonly stopped: false; readonly reason: AmStopRefusalReason; readonly error: string };

export type AmInterruptResult =
  | { readonly interrupted: true }
  | ({ readonly interrupted: false } & AmConflict);

/** Agent routes take a bare name or `host:name` for an agent on a fleet host. */
export interface AmApi {
  readonly health: Effect.Effect<AmHealth, AmApiError>;
  readonly hosts: Effect.Effect<AmHosts, AmApiError>;
  /**
   * The providers' models, for a fleet host when `host` is set. `refresh` makes am re-read its
   * providers rather than answer from its cache.
   */
  readonly models: (input: {
    readonly host?: string;
    readonly refresh?: boolean;
  }) => Effect.Effect<AmModelCatalog, AmApiError>;
  /** am's cached fleet view; it never waits on ssh. */
  readonly agents: Effect.Effect<AmFleet, AmApiError>;
  /** None when am has no agent with this name. */
  readonly getAgent: (ref: string) => Effect.Effect<Option.Option<AmAgentDetail>, AmApiError>;
  readonly createAgent: (input: CreateAgentInput) => Effect.Effect<void, AmApiError>;
  /** Returns the message's receipt id. */
  readonly sendMessage: (
    ref: string,
    text: string,
    mode: "queue" | "now",
  ) => Effect.Effect<string, AmApiError>;
  readonly getMessage: (ref: string, id: string) => Effect.Effect<AmMessageReceipt, AmApiError>;
  /** Cancels a still-queued message; otherwise reports the state that prevented it. */
  readonly cancelMessage: (ref: string, id: string) => Effect.Effect<AmCancelResult, AmApiError>;
  /**
   * Sends Esc once, and only while this message's own turn runs, so it never stops other work.
   * There is deliberately no bare interrupt here.
   */
  readonly interruptMessage: (
    ref: string,
    id: string,
  ) => Effect.Effect<AmInterruptResult, AmApiError>;
  readonly resume: (name: string) => Effect.Effect<void, AmApiError>;
  /**
   * Ends the agent's tmux session only if am, under its delivery lock, finds it idle with nothing
   * queued and no turn left to finish; am keeps its state, conversation and worktree to resume.
   * An am without `stopIfIdle` ignores the condition and stops regardless, so check it first.
   */
  readonly stopIfIdle: (ref: string) => Effect.Effect<AmStopResult, AmApiError>;
  /**
   * Restarts the agent on another model or effort, resuming its conversation, only if am finds it
   * idle under the same checks as `stopIfIdle`. A model or effort am rejects fails with HTTP 400.
   */
  readonly restart: (
    ref: string,
    change: AmModelChange,
  ) => Effect.Effect<AmRestartResult, AmApiError>;
  readonly transcript: (name: string, after: number) => Effect.Effect<AmTranscript, AmApiError>;
  readonly permissions: (
    ref: string,
  ) => Effect.Effect<ReadonlyArray<AmPermissionRequest>, AmApiError>;
  /** Succeeds only once the provider took the decision; a 409 reports why it did not. */
  readonly answerPermission: (
    ref: string,
    id: string,
    optionId: string,
  ) => Effect.Effect<AmPermissionAnswer, AmApiError>;
  /** Every fleet event, as a bare wake signal; reconnecting is the caller's concern. */
  readonly events: Stream.Stream<Sse.Event, AmApiError>;
}

export const makeAmApi = (
  client: HttpClient.HttpClient,
  connection: { readonly baseUrl: string; readonly token: string },
): AmApi => {
  const authed = client.pipe(
    HttpClient.mapRequest((request) =>
      request.pipe(
        HttpClientRequest.prependUrl(`${connection.baseUrl}/api`),
        HttpClientRequest.bearerToken(connection.token),
      ),
    ),
  );
  const failure = (operation: string) => (cause: unknown) =>
    new AmApiError({
      operation,
      status: null,
      detail: cause instanceof Error && cause.message ? cause.message : "am is unreachable",
    });
  const send = (
    operation: string,
    request: HttpClientRequest.HttpClientRequest,
    timeout: boolean = true,
  ) =>
    authed
      .execute(request)
      .pipe(
        timeout ? Effect.timeout(REQUEST_TIMEOUT_MS) : (effect) => effect,
        Effect.mapError(failure(operation)),
      );
  const rejected = Effect.fnUntraced(function* (
    operation: string,
    response: HttpClientResponse.HttpClientResponse,
  ) {
    const body = yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(ErrorBody)),
      Effect.map((decoded) => decoded.error),
      Effect.orElseSucceed(() => `HTTP ${response.status}`),
    );
    return yield* new AmApiError({ operation, status: response.status, detail: body });
  });
  const decodeBody = <A, I>(
    operation: string,
    schema: Schema.Codec<A, I>,
    response: HttpClientResponse.HttpClientResponse,
    detail = "unreadable response",
  ) =>
    response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError(() => new AmApiError({ operation, status: response.status, detail })),
    );
  const decode = <A, I>(
    operation: string,
    schema: Schema.Codec<A, I>,
    response: HttpClientResponse.HttpClientResponse,
    detail?: string,
  ) =>
    response.status >= 200 && response.status < 300
      ? decodeBody(operation, schema, response, detail)
      : rejected(operation, response);
  const expectOk = (operation: string) => (response: HttpClientResponse.HttpClientResponse) =>
    response.status >= 200 && response.status < 300 ? Effect.void : rejected(operation, response);
  const conflict = (operation: string, response: HttpClientResponse.HttpClientResponse) =>
    decodeBody(operation, ConflictBody, response).pipe(
      Effect.map(({ error, state, reason }): AmConflict => ({
        error,
        state,
        reason: isConflictReason(reason) ? reason : undefined,
      })),
    );
  const agentPath = (ref: string, suffix = "") => `/agents/${encodeURIComponent(ref)}${suffix}`;
  const post = (operation: string, path: string, body: unknown) =>
    send(operation, HttpClientRequest.post(path).pipe(HttpClientRequest.bodyJsonUnsafe(body))).pipe(
      Effect.flatMap(expectOk(operation)),
    );

  return {
    health: send("health check", HttpClientRequest.get("/health")).pipe(
      Effect.flatMap((response) => decode("health check", AmHealth, response)),
    ),
    hosts: send("host list", HttpClientRequest.get("/hosts")).pipe(
      Effect.flatMap((response) => decode("host list", AmHosts, response)),
    ),
    models: ({ host, refresh }) =>
      send(
        "model list",
        HttpClientRequest.get("/models").pipe(
          host ? HttpClientRequest.setUrlParam("host", host) : (request) => request,
          refresh ? HttpClientRequest.setUrlParam("refresh", "1") : (request) => request,
        ),
      ).pipe(Effect.flatMap((response) => decode("model list", AmModelCatalog, response))),
    agents: send("agent list", HttpClientRequest.get("/agents")).pipe(
      Effect.flatMap((response) => decode("agent list", AmFleet, response)),
    ),
    getAgent: (ref) =>
      send("agent lookup", HttpClientRequest.get(agentPath(ref))).pipe(
        Effect.flatMap((response) =>
          response.status === 404
            ? Effect.succeedNone
            : decode("agent lookup", AmAgentDetail, response).pipe(Effect.map(Option.some)),
        ),
      ),
    createAgent: (input) =>
      post("agent start", "/agents", {
        name: input.name,
        dir: input.dir,
        // T3 already owns the thread's worktree.
        inPlace: true,
        ...(input.host ? { host: input.host } : {}),
        ...(input.provider ? { provider: input.provider } : {}),
        ...(input.model ? { model: input.model } : {}),
        ...(input.effort ? { effort: input.effort } : {}),
      }),
    sendMessage: (ref, text, mode) =>
      send(
        "message",
        HttpClientRequest.post(agentPath(ref, "/messages")).pipe(
          HttpClientRequest.bodyJsonUnsafe({ text, mode }),
        ),
      ).pipe(
        Effect.flatMap((response) =>
          decode(
            "message",
            SentMessage,
            response,
            "this am reports no message receipts; update it",
          ),
        ),
        Effect.map((sent) => sent.id),
      ),
    getMessage: (ref, id) =>
      send(
        "message lookup",
        HttpClientRequest.get(agentPath(ref, `/messages/${encodeURIComponent(id)}`)),
      ).pipe(Effect.flatMap((response) => decode("message lookup", AmMessageReceipt, response))),
    cancelMessage: (ref, id) =>
      send(
        "message cancel",
        HttpClientRequest.delete(agentPath(ref, `/messages/${encodeURIComponent(id)}`)),
      ).pipe(
        Effect.flatMap((response): Effect.Effect<AmCancelResult, AmApiError> =>
          response.status === 409
            ? conflict("message cancel", response).pipe(
                Effect.map((refusal) => ({ cancelled: false as const, ...refusal })),
              )
            : expectOk("message cancel")(response).pipe(Effect.as({ cancelled: true })),
        ),
      ),
    interruptMessage: (ref, id) =>
      send(
        "interrupt",
        HttpClientRequest.post(agentPath(ref, "/interrupt")).pipe(
          HttpClientRequest.bodyJsonUnsafe({ messageId: id }),
        ),
      ).pipe(
        Effect.flatMap((response): Effect.Effect<AmInterruptResult, AmApiError> =>
          response.status === 409
            ? conflict("interrupt", response).pipe(
                Effect.map((refusal) => ({ interrupted: false as const, ...refusal })),
              )
            : expectOk("interrupt")(response).pipe(Effect.as({ interrupted: true })),
        ),
      ),
    resume: (ref) => post("resume", agentPath(ref, "/resume"), {}),
    stopIfIdle: (ref) =>
      send(
        "stop",
        HttpClientRequest.post(agentPath(ref, "/stop")).pipe(
          HttpClientRequest.bodyJsonUnsafe({ ifIdle: true }),
        ),
      ).pipe(
        Effect.flatMap((response): Effect.Effect<AmStopResult, AmApiError> =>
          response.status === 409
            ? decodeBody("stop", ConflictBody, response).pipe(
                Effect.flatMap(({ error, reason }) =>
                  isStopRefusalReason(reason)
                    ? Effect.succeed({ stopped: false as const, reason, error })
                    : Effect.fail(
                        new AmApiError({ operation: "stop", status: 409, detail: error }),
                      ),
                ),
              )
            : expectOk("stop")(response).pipe(Effect.as({ stopped: true as const })),
        ),
      ),
    restart: (ref, change) =>
      send(
        "restart",
        HttpClientRequest.post(agentPath(ref, "/restart")).pipe(
          HttpClientRequest.bodyJsonUnsafe({ ...change, ifIdle: true }),
        ),
      ).pipe(
        Effect.flatMap((response): Effect.Effect<AmRestartResult, AmApiError> =>
          response.status === 409
            ? decodeBody("restart", ConflictBody, response).pipe(
                Effect.flatMap(({ error, reason }) =>
                  isStopRefusalReason(reason)
                    ? Effect.succeed({ restarted: false as const, reason, error })
                    : Effect.fail(
                        new AmApiError({ operation: "restart", status: 409, detail: error }),
                      ),
                ),
              )
            : decode("restart", RestartedBody, response).pipe(
                Effect.map((body) => ({
                  restarted: true as const,
                  model: body.model ?? null,
                  effort: body.effort ?? null,
                })),
              ),
        ),
      ),
    transcript: (ref, after) =>
      send(
        "transcript read",
        HttpClientRequest.get(agentPath(ref, "/transcript")).pipe(
          HttpClientRequest.setUrlParam("after", String(after)),
        ),
      ).pipe(Effect.flatMap((response) => decode("transcript read", AmTranscript, response))),
    permissions: (ref) =>
      send("permission list", HttpClientRequest.get(agentPath(ref, "/permissions"))).pipe(
        Effect.flatMap((response) => decode("permission list", AmPermissions, response)),
        Effect.map((listed) => listed.requests),
      ),
    answerPermission: (ref, id, optionId) =>
      send(
        "permission answer",
        HttpClientRequest.post(agentPath(ref, `/permissions/${encodeURIComponent(id)}`)).pipe(
          HttpClientRequest.bodyJsonUnsafe({ optionId }),
        ),
      ).pipe(
        Effect.flatMap((response): Effect.Effect<AmPermissionAnswer, AmApiError> =>
          response.status === 409
            ? decodeBody("permission answer", ConflictBody, response).pipe(
                Effect.map(({ error, reason }) => ({
                  answered: false as const,
                  error,
                  reason: isPermissionConflictReason(reason) ? reason : undefined,
                })),
              )
            : expectOk("permission answer")(response).pipe(Effect.as({ answered: true as const })),
        ),
      ),
    events: send(
      "event stream",
      HttpClientRequest.get("/events").pipe(HttpClientRequest.accept("text/event-stream")),
      false,
    ).pipe(
      Effect.flatMap((response) =>
        response.status === 200
          ? Effect.succeed(
              response.stream.pipe(
                Stream.decodeText(),
                Stream.pipeThroughChannel(Sse.decode()),
                Stream.mapError(failure("event stream")),
              ),
            )
          : rejected("event stream", response),
      ),
      Stream.unwrap,
    ),
  };
};
