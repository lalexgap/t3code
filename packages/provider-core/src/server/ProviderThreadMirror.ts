import type {
  OrchestrationV2ProviderRef,
  OrchestrationV2ProviderThread,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

/**
 * Lets a provider whose agents also run outside T3 Code keep a T3 thread per native thread
 * without a session open.
 */

export interface MirroredProviderThread {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly archivedAt: DateTime.Utc | null;
  /** Deleted, or moved to another provider; still bound, so never adopted again. */
  readonly released: boolean;
}

export interface ProviderThreadHistoryMessage {
  readonly role: "user" | "assistant";
  readonly text: string;
}

export interface ProviderThreadAdoption {
  readonly driver: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly nativeThreadId: string;
  readonly title: string;
  readonly model: string;
  readonly runtimeMode: RuntimeMode;
  /** Absolute, or `~/`-relative to this machine's home. It must exist; nothing is created. */
  readonly workspaceRoot: string;
  /** Bound only when the path exists on this machine. */
  readonly worktree: { readonly path: string; readonly branch: string | null } | null;
  readonly createdAt: DateTime.Utc;
  readonly updatedAt: DateTime.Utc;
  readonly history: ReadonlyArray<ProviderThreadHistoryMessage>;
  /** Shown before the history, for example to say that older entries were left out. */
  readonly historyNotice: string | null;
  readonly conversationHeadRef: OrchestrationV2ProviderRef | null;
}

export type ProviderThreadAdoptionResult =
  | { readonly _tag: "Adopted"; readonly threadId: ThreadId }
  | { readonly _tag: "AlreadyOwned" }
  | { readonly _tag: "WorkspaceMissing"; readonly workspaceRoot: string };

export interface ProviderThreadMirrorUpdate {
  readonly threadId: ThreadId;
  /** Nothing changes unless the thread still runs on this provider thread. */
  readonly providerThreadId: ProviderThreadId;
  readonly archived?: boolean;
  /**
   * Sets, or with null clears, the fingerprint of the provider thread's current conversation head,
   * leaving where that head points as it is.
   */
  readonly headFingerprint?: string | null;
}

export class ProviderThreadMirrorError extends Schema.TaggedError<ProviderThreadMirrorError>()(
  "ProviderThreadMirrorError",
  {
    operation: Schema.Literals(["list", "project-roots", "adopt", "update"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Provider thread mirror ${this.operation} failed: ${this.detail}`;
  }
}

export interface ProviderThreadMirrorHandler {
  readonly list: (binding: {
    readonly driver: ProviderDriverKind;
    readonly providerInstanceId: ProviderInstanceId;
  }) => Effect.Effect<ReadonlyArray<MirroredProviderThread>, ProviderThreadMirrorError>;
  /** The workspace roots of this server's projects that are not deleted, as stored. */
  readonly projectRoots: Effect.Effect<ReadonlyArray<string>, ProviderThreadMirrorError>;
  readonly adopt: (
    input: ProviderThreadAdoption,
  ) => Effect.Effect<ProviderThreadAdoptionResult, ProviderThreadMirrorError>;
  readonly update: (
    input: ProviderThreadMirrorUpdate,
  ) => Effect.Effect<void, ProviderThreadMirrorError>;
}

const unavailable = (operation: ProviderThreadMirrorError["operation"]) =>
  Effect.fail(
    new ProviderThreadMirrorError({ operation, detail: "No thread mirror runs in this server." }),
  );

/**
 * The default reference is unavailable, keeping driver construction dependency-free in tests. The
 * live layer must be shared with the server service that installs the handler, like
 * `ProviderContinuationRequests`; calls wait until it has.
 */
export class ProviderThreadMirror extends Context.Reference<
  ProviderThreadMirrorHandler & {
    readonly available: boolean;
    readonly install: (handler: ProviderThreadMirrorHandler) => Effect.Effect<void>;
  }
>("@t3tools/provider-core/server/ProviderThreadMirror", {
  defaultValue: () => ({
    available: false,
    install: () => Effect.void,
    list: () => unavailable("list"),
    projectRoots: unavailable("project-roots"),
    adopt: () => unavailable("adopt"),
    update: () => unavailable("update"),
  }),
}) {}

export const layer = Layer.effect(
  ProviderThreadMirror,
  Effect.gen(function* () {
    const handler = yield* Deferred.make<ProviderThreadMirrorHandler>();
    const installed = Deferred.await(handler);
    return {
      available: true,
      install: (next: ProviderThreadMirrorHandler) =>
        Deferred.succeed(handler, next).pipe(Effect.asVoid),
      list: (binding) => installed.pipe(Effect.flatMap((live) => live.list(binding))),
      projectRoots: installed.pipe(Effect.flatMap((live) => live.projectRoots)),
      adopt: (input) => installed.pipe(Effect.flatMap((live) => live.adopt(input))),
      update: (input) => installed.pipe(Effect.flatMap((live) => live.update(input))),
    };
  }),
);
