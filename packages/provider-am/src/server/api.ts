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
  /** Set by an am whose `send --json` returns receipt ids; a fleet host's reflects that host's am. */
  messageReceipts: Schema.optional(Schema.Boolean),
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

export const AmHosts = Schema.Struct({
  local: Schema.String,
  hosts: Schema.Array(Schema.Struct({ name: Schema.String, reachable: Schema.Boolean })),
});
export type AmHosts = typeof AmHosts.Type;

export const AmHealth = Schema.Struct({ messageReceipts: Schema.optional(Schema.Boolean) });
export type AmHealth = typeof AmHealth.Type;

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

export type AmInterruptResult =
  | { readonly interrupted: true }
  | ({ readonly interrupted: false } & AmConflict);

/** Agent routes take a bare name or `host:name` for an agent on a fleet host. */
export interface AmApi {
  readonly health: Effect.Effect<AmHealth, AmApiError>;
  readonly hosts: Effect.Effect<AmHosts, AmApiError>;
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
  /** Ends the agent's tmux session; am keeps its state, conversation and worktree to resume. */
  readonly stop: (ref: string) => Effect.Effect<void, AmApiError>;
  readonly transcript: (name: string, after: number) => Effect.Effect<AmTranscript, AmApiError>;
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
    stop: (ref) => post("stop", agentPath(ref, "/stop"), {}),
    transcript: (ref, after) =>
      send(
        "transcript read",
        HttpClientRequest.get(agentPath(ref, "/transcript")).pipe(
          HttpClientRequest.setUrlParam("after", String(after)),
        ),
      ).pipe(Effect.flatMap((response) => decode("transcript read", AmTranscript, response))),
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
