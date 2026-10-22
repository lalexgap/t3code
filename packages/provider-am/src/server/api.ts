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
});
export type AmAgentDetail = typeof AmAgentDetail.Type;

const ErrorBody = Schema.Struct({ error: Schema.String });
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
  readonly task?: string;
  readonly dir: string;
  readonly provider?: AmProvider;
}

export interface AmApi {
  readonly health: Effect.Effect<void, AmApiError>;
  /** None when am has no local agent with this name. */
  readonly getAgent: (name: string) => Effect.Effect<Option.Option<AmAgentDetail>, AmApiError>;
  readonly createAgent: (input: CreateAgentInput) => Effect.Effect<void, AmApiError>;
  readonly sendMessage: (
    name: string,
    text: string,
    mode: "queue" | "now",
  ) => Effect.Effect<void, AmApiError>;
  readonly interrupt: (name: string) => Effect.Effect<void, AmApiError>;
  readonly resume: (name: string) => Effect.Effect<void, AmApiError>;
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
  const decode = <A, I>(
    operation: string,
    schema: Schema.Codec<A, I>,
    response: HttpClientResponse.HttpClientResponse,
  ) =>
    response.status >= 200 && response.status < 300
      ? response.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(schema)),
          Effect.mapError(
            () =>
              new AmApiError({ operation, status: response.status, detail: "unreadable response" }),
          ),
        )
      : rejected(operation, response);
  const expectOk = (operation: string) => (response: HttpClientResponse.HttpClientResponse) =>
    response.status >= 200 && response.status < 300 ? Effect.void : rejected(operation, response);
  const agentPath = (name: string, suffix = "") => `/agents/${encodeURIComponent(name)}${suffix}`;
  const post = (operation: string, path: string, body: unknown) =>
    send(operation, HttpClientRequest.post(path).pipe(HttpClientRequest.bodyJsonUnsafe(body))).pipe(
      Effect.flatMap(expectOk(operation)),
    );

  return {
    health: send("health check", HttpClientRequest.get("/health")).pipe(
      Effect.flatMap(expectOk("health check")),
    ),
    getAgent: (name) =>
      send("agent lookup", HttpClientRequest.get(agentPath(name))).pipe(
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
        ...(input.task ? { task: input.task } : {}),
        ...(input.provider ? { provider: input.provider } : {}),
      }),
    sendMessage: (name, text, mode) =>
      post("message", agentPath(name, "/messages"), { text, mode }),
    interrupt: (name) => post("interrupt", agentPath(name, "/interrupt"), {}),
    resume: (name) => post("resume", agentPath(name, "/resume"), {}),
    transcript: (name, after) =>
      send(
        "transcript read",
        HttpClientRequest.get(agentPath(name, "/transcript")).pipe(
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
