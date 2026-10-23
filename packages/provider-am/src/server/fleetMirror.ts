import {
  ProviderDriverKind,
  ProviderThreadId,
  ThreadId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import type * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import type * as ProviderThreadMirror from "@t3tools/provider-core/server/ProviderThreadMirror";
import { amAutoImportDays, isAmApiUrlAllowed, type AmSettings } from "../settings.ts";
import type { AmApi, AmFleetRow } from "./api.ts";
import { AM_DEFAULT_MODEL, amProviderForModel, amTurnPhase } from "./mapping.ts";
import {
  AM_MIRROR_ENTRY_LIMIT,
  AM_REMOVED_MARK,
  amConversationHead,
  amHistory,
  amLocalPath,
  amMirroredThrough,
  amMirrorStatus,
  amRepoSpelledLike,
  amWorkspace,
  fleetHostPathHere,
  planMirror,
  type AmGitRepo,
  type AmLiveness,
  type AmUnsafeRootReason,
  type MirrorThreadView,
} from "./mirrorPlan.ts";

const AM_PROVIDER = ProviderDriverKind.make("am");
const TRANSCRIPT_END = 1_000_000_000;
// `/api/agents` is a cached local read, while a fleet host's transcript costs am an ssh round trip.
const LOCAL_INTERVAL_MS = 30_000;
const REMOTE_INTERVAL_MS = 60_000;
// With no client in the foreground, threads only need to be current when one connects.
const IDLE_INTERVAL_MS = 300_000;
const EVENT_DEBOUNCE_MS = 1_500;
const EVENT_RECONNECT_MS = 5_000;
// An observation request waits behind any T3 turn; ask again only if it never ran.
const OBSERVE_RETRY_MS = 120_000;
// One adoption commits up to ~400 events in one transaction. Importing one per step keeps a first
// sync of a large fleet from overflowing live subscribers' buffers or holding the writer.
const IMPORTS_PER_STEP = 1;
const IMPORT_PACE_MS = 1_000;

export class AmGitRootError extends Schema.TaggedError<AmGitRootError>()("AmGitRootError", {
  path: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Could not ask git for the repository of ${this.path}`;
  }
}

export interface AmFleetMirrorOptions {
  readonly instanceId: ProviderInstanceId;
  readonly settings: AmSettings;
  readonly api: AmApi;
  readonly mirror: Context.Service.Shape<typeof ProviderThreadMirror.ProviderThreadMirror>;
  readonly continuationRequests: Context.Service.Shape<
    typeof ProviderContinuationRequests.ProviderContinuationRequests
  >;
  /** Agents with a T3 turn running, which reads their transcript itself. */
  readonly busyAgents: ReadonlySet<string>;
  /** This machine's home directory. */
  readonly home: string;
  /** The home directory with its symlinks resolved. */
  readonly realHome: string;
  /**
   * The repository of a directory on this machine; null when it is not in one. Fails when git
   * could not answer, so the agent is tried again rather than kept out of its repository.
   */
  readonly gitRoot: (path: string) => Effect.Effect<AmGitRepo | null, AmGitRootError>;
  /** A path on this machine with its symlinks resolved; null when it cannot be resolved. */
  readonly realPath: (path: string) => Effect.Effect<string | null>;
  readonly intervalMs?: number;
}

/**
 * Keeps one T3 thread per agent in am's fleet: imports new agents, archives a thread whose agent
 * am removed, restores it when the agent comes back, and asks for a T3 run to follow activity that
 * did not come from T3. Reads never start or resume an agent.
 */
export const makeAmFleetMirror = Effect.fn("makeAmFleetMirror")(function* (
  options: AmFleetMirrorOptions,
) {
  const providerHost = yield* ProviderHost.ProviderHost;
  const { api, mirror, settings } = options;
  const host = settings.host;
  const days = amAutoImportDays(settings);
  const status = yield* SubscriptionRef.make<string | null>(null);
  const wake = yield* Queue.sliding<void>(1);
  const enabled =
    mirror.available &&
    settings.enabled &&
    days > 0 &&
    settings.apiToken !== "" &&
    isAmApiUrlAllowed(settings);

  let previous: ReadonlyMap<string, AmLiveness> | undefined;
  let previousOmitted: ReadonlySet<string> = new Set();
  /** am's `updatedAt` for each agent when T3 last compared its transcript with the thread. */
  const checkedAt = new Map<string, string>();
  /** When T3 last asked to follow each agent, and the thread's head then. */
  const observeRequested = new Map<string, { readonly at: number; readonly head?: number }>();
  /** Agents whose project has no copy here, until am reports a change to them. */
  const missingWorkspaces = new Map<string, string>();
  /** Agents whose project root is unsafe, until am reports a change to them. */
  const skipped = new Map<
    string,
    { readonly updatedAt: string; readonly reason: AmUnsafeRootReason }
  >();
  /** Tried since the imports last caught up, so a failing agent cannot hold up the rest. */
  const triedImports = new Set<string>();

  /** Reports whether the agent now has a thread. */
  const adopt = Effect.fnUntraced(function* (row: AmFleetRow & { readonly ref: string }) {
    if (missingWorkspaces.get(row.ref) === row.updatedAt) return false;
    const remote = host !== "";
    const dirHere = remote ? fleetHostPathHere(row.dir) : row.dir;
    const gitRoot =
      dirHere === null || (row.worktreePath && row.repoRoot)
        ? null
        : yield* options.gitRoot(amLocalPath(dirHere, options.home));
    const realPaths = new Map<string, string>();
    // Resolve the main root as amWorkspace will bind it, under the agent's own home spelling.
    const boundRoot =
      !remote && gitRoot && dirHere !== null
        ? amRepoSpelledLike(gitRoot, dirHere, options).root
        : null;
    if (!remote)
      for (const candidate of new Set([row.dir, row.repoRoot, boundRoot])) {
        if (!candidate) continue;
        const path = amLocalPath(candidate, options.home);
        const real = yield* options.realPath(path);
        if (real !== null) realPaths.set(path, real);
      }
    const workspace = amWorkspace(row, {
      remote,
      home: options.home,
      realHome: options.realHome,
      gitRoot,
      realPaths,
    });
    if (workspace._tag === "NoCopy") {
      skipped.delete(row.ref);
      missingWorkspaces.set(row.ref, row.updatedAt);
      return false;
    }
    if (workspace._tag === "Unsafe") {
      skipped.set(row.ref, { updatedAt: row.updatedAt, reason: workspace.reason });
      return false;
    }
    skipped.delete(row.ref);
    const total = (yield* api.transcript(row.ref, TRANSCRIPT_END)).total;
    const tail = yield* api.transcript(row.ref, Math.max(0, total - AM_MIRROR_ENTRY_LIMIT));
    const { history, notice } = amHistory(row.ref, tail.entries, tail.total);
    const updatedAt = DateTime.make(row.updatedAt).pipe(Option.getOrUndefined);
    const now = yield* DateTime.now;
    const result = yield* mirror.adopt({
      driver: AM_PROVIDER,
      providerInstanceId: options.instanceId,
      nativeThreadId: row.ref,
      title: row.name,
      model: amProviderForModel(row.provider) ?? AM_DEFAULT_MODEL,
      runtimeMode: "full-access",
      workspaceRoot: workspace.root,
      worktree: workspace.worktree,
      createdAt:
        DateTime.make(row.createdAt ?? row.updatedAt).pipe(Option.getOrUndefined) ??
        updatedAt ??
        now,
      updatedAt: updatedAt ?? now,
      history,
      historyNotice: notice,
      conversationHeadRef: amConversationHead(row.ref, tail.total),
    });
    if (result._tag === "WorkspaceMissing") {
      missingWorkspaces.set(row.ref, row.updatedAt);
      return false;
    }
    missingWorkspaces.delete(row.ref);
    return result._tag === "Adopted";
  });

  const follow = Effect.fnUntraced(function* (thread: MirrorThreadView, row: AmFleetRow) {
    const head = amMirroredThrough(thread.head, thread.ref);
    if (options.busyAgents.has(thread.ref)) {
      observeRequested.delete(thread.ref);
      return;
    }
    const now = yield* Clock.currentTimeMillis;
    const requested = observeRequested.get(thread.ref);
    // A moved head means the requested run already happened.
    if (requested && requested.head === head && now - requested.at < OBSERVE_RETRY_MS) return;
    const phase = amTurnPhase(row.status);
    let moved = phase === "running" || phase === "attention";
    if (!moved && checkedAt.get(thread.ref) !== row.updatedAt) {
      const total = (yield* api.transcript(thread.ref, TRANSCRIPT_END)).total;
      // Shorter than the head means a new session's transcript, which is all new.
      moved = total !== (head ?? total);
      checkedAt.set(thread.ref, row.updatedAt);
    }
    if (!moved) return;
    observeRequested.set(thread.ref, { at: now, ...(head === undefined ? {} : { head }) });
    yield* options.continuationRequests.offer({
      threadId: ThreadId.make(thread.threadId),
      providerThreadId: ProviderThreadId.make(thread.providerThreadId),
      driver: AM_PROVIDER,
      detail: "Activity outside T3 Code",
      notification: {
        source: { kind: "background_task" },
        outcome: "updated",
        summary: "Agent Motel activity",
      },
      delivery: "adapter_buffered",
    });
  });

  const reconcile = Effect.fn("AmFleetMirror.reconcile")(function* () {
    const fleet = yield* api.agents;
    const threads = (yield* mirror.list({
      driver: AM_PROVIDER,
      providerInstanceId: options.instanceId,
    })).flatMap((bound): Array<MirrorThreadView> => {
      const ref = bound.providerThread.nativeThreadRef?.nativeId;
      return ref
        ? [
            {
              threadId: bound.threadId,
              providerThreadId: bound.providerThread.id,
              ref,
              archived: bound.archivedAt !== null,
              released: bound.released,
              head: bound.providerThread.nativeConversationHeadRef,
            },
          ]
        : [];
    });
    const plan = planMirror({
      host,
      fleet,
      threads,
      autoImportDays: days,
      nowMs: yield* Clock.currentTimeMillis,
      previous,
      previousOmitted,
    });
    if (!plan.reachable) {
      yield* SubscriptionRef.set(status, `Agent Motel cannot reach ${host} to mirror its agents.`);
      return false;
    }
    const ignoreFailure = <A>(what: string, ref: string, fallback: A) =>
      Effect.catchCause((cause) =>
        Effect.logWarning(`Could not ${what} the Agent Motel agent ${ref}`, cause).pipe(
          Effect.as(fallback),
        ),
      );
    for (const thread of plan.archive)
      yield* mirror
        .update({
          threadId: ThreadId.make(thread.threadId),
          providerThreadId: ProviderThreadId.make(thread.providerThreadId),
          archived: true,
          headFingerprint: AM_REMOVED_MARK,
        })
        .pipe(ignoreFailure("archive the thread of", thread.ref, undefined));
    for (const thread of plan.unarchive)
      yield* mirror
        .update({
          threadId: ThreadId.make(thread.threadId),
          providerThreadId: ProviderThreadId.make(thread.providerThreadId),
          archived: false,
          headFingerprint: null,
        })
        .pipe(ignoreFailure("restore the thread of", thread.ref, undefined));
    let adopted = 0;
    let tried = 0;
    let importing = 0;
    for (const row of plan.imports) {
      if (
        triedImports.has(row.ref) ||
        missingWorkspaces.get(row.ref) === row.updatedAt ||
        skipped.get(row.ref)?.updatedAt === row.updatedAt
      )
        continue;
      if (tried === IMPORTS_PER_STEP) {
        importing++;
        continue;
      }
      tried++;
      triedImports.add(row.ref);
      if (yield* adopt(row).pipe(ignoreFailure("import", row.ref, false))) adopted++;
    }
    if (importing === 0) triedImports.clear();
    for (const { thread, row } of plan.watch)
      yield* follow(thread, row).pipe(ignoreFailure("follow", thread.ref, undefined));
    // A listing that confirmed nothing keeps its liveness, and the counts with it.
    const confirmed = plan.liveness !== previous;
    previous = plan.liveness;
    previousOmitted = plan.omitted;
    if (confirmed) {
      // An agent aged out of the imports no longer counts as missing or skipped; one left out of
      // a single listing still might come back.
      const refs = new Set([...plan.imports.map((row) => row.ref), ...plan.omitted]);
      for (const ref of missingWorkspaces.keys()) if (!refs.has(ref)) missingWorkspaces.delete(ref);
      for (const ref of skipped.keys()) if (!refs.has(ref)) skipped.delete(ref);
    }
    const skippedByReason = new Map<AmUnsafeRootReason, number>();
    for (const { reason } of skipped.values())
      skippedByReason.set(reason, (skippedByReason.get(reason) ?? 0) + 1);
    yield* SubscriptionRef.set(
      status,
      amMirrorStatus({
        mirrored: plan.mirrored + plan.unarchive.length + adopted,
        importing,
        missingWorkspaces: missingWorkspaces.size,
        skipped: skippedByReason,
        host,
      }),
    );
    return importing > 0;
  });

  if (enabled) {
    const interval = options.intervalMs ?? (host ? REMOTE_INTERVAL_MS : LOCAL_INTERVAL_MS);
    const hasDemand = Effect.all([
      providerHost.shouldRunBackgroundWork({ type: "provider-status" }),
      providerHost.shouldRunBackgroundWork({
        type: "provider-status",
        instanceId: options.instanceId,
      }),
    ]).pipe(Effect.map(([generic, instance]) => generic || instance));
    yield* Effect.gen(function* () {
      while (true) {
        const importing = yield* reconcile().pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Could not mirror the Agent Motel fleet", cause).pipe(
              Effect.as(false),
            ),
          ),
        );
        if (importing) {
          yield* Effect.sleep(IMPORT_PACE_MS);
          continue;
        }
        const wait = (yield* hasDemand) ? interval : Math.max(interval, IDLE_INTERVAL_MS);
        yield* Queue.take(wake).pipe(
          Effect.timeoutOrElse({ duration: wait, orElse: () => Effect.void }),
        );
      }
    }).pipe(Effect.forkScoped);
    // `/api/events` carries only the local daemon's events, so a fleet host is polled.
    if (!host) {
      const events = yield* Queue.sliding<void>(1);
      yield* Stream.fromQueue(events).pipe(
        Stream.runForEach(() =>
          Effect.sleep(EVENT_DEBOUNCE_MS).pipe(
            Effect.andThen(Queue.offer(wake, undefined)),
            Effect.asVoid,
          ),
        ),
        Effect.forkScoped,
      );
      yield* api.events.pipe(
        Stream.runForEach(() => Queue.offer(events, undefined).pipe(Effect.asVoid)),
        Effect.catch((error) => Effect.logDebug("Agent Motel event stream ended", error)),
        Effect.andThen(Effect.sleep(EVENT_RECONNECT_MS)),
        Effect.forever,
        Effect.forkScoped,
      );
    }
  }

  return {
    /** Runs a reconcile now, for example when the user refreshes provider status. */
    wake: Queue.offer(wake, undefined).pipe(Effect.asVoid),
    /** The status line for the provider, or null while mirroring is off or has not run. */
    status: SubscriptionRef.changes(status),
  };
});
