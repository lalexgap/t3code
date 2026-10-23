import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderThreadMirror from "@t3tools/provider-core/server/ProviderThreadMirror";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderThreadMirrorService from "./ProviderThreadMirrorService.ts";
import * as ThreadLifecycleService from "./ThreadLifecycleService.ts";

const AM = ProviderDriverKind.make("am");
const INSTANCE_ID = ProviderInstanceId.make("am_fleet");
const PROJECT_ID = ProjectId.make("project-mirror");
const now = DateTime.makeUnsafe("2026-10-22T12:00:00.000Z");
const existingRoots = new Set(["/work/app", "/work/app-wt"]);
const archives: Array<{ readonly threadId: ThreadId; readonly archived: boolean }> = [];
/** Holds each project lookup until this many are waiting, so adoptions overlap. */
let overlapProjectLookups: {
  readonly count: number;
  readonly arrived: Deferred.Deferred<void>;
} | null = null;
let waitingProjectLookups = 0;

const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const sink = EventSink.layer.pipe(Layer.provide(stores));
const testLayer = Layer.mergeAll(ProviderThreadMirror.layer, stores, sink, IdAllocator.layer).pipe(
  Layer.provideMerge(
    ProviderThreadMirrorService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          ProviderThreadMirror.layer,
          stores,
          sink,
          IdAllocator.layer,
          Layer.mock(ProjectService.ProjectService)({
            bootstrap: (input) =>
              Effect.gen(function* () {
                const overlap = overlapProjectLookups;
                if (overlap !== null) {
                  waitingProjectLookups++;
                  if (waitingProjectLookups === overlap.count)
                    yield* Deferred.succeed(overlap.arrived, undefined);
                  yield* Deferred.await(overlap.arrived);
                }
              }).pipe(
                Effect.as({
                  project: {
                    id: PROJECT_ID,
                    title: input.title,
                    workspaceRoot: input.workspaceRoot,
                    defaultModelSelection: null,
                    scripts: [],
                    createdAt: "2026-10-22T12:00:00.000Z",
                    updatedAt: "2026-10-22T12:00:00.000Z",
                    deletedAt: null,
                  },
                  created: true,
                }),
              ),
          }),
          Layer.mock(ThreadLifecycleService.ThreadLifecycleService)({
            archive: (input) =>
              Effect.sync(() => {
                archives.push({ threadId: input.threadId, archived: true });
                return { thread: {} as OrchestrationV2AppThread };
              }),
            unarchive: (input) =>
              Effect.sync(() => {
                archives.push({ threadId: input.threadId, archived: false });
                return { thread: {} as OrchestrationV2AppThread };
              }),
          }),
          Layer.mock(WorkspacePaths.WorkspacePaths)({
            normalizeWorkspaceRoot: (workspaceRoot) =>
              existingRoots.has(workspaceRoot)
                ? Effect.succeed(workspaceRoot)
                : Effect.fail(
                    new WorkspacePaths.WorkspaceRootNotExistsError({
                      workspaceRoot,
                      normalizedWorkspaceRoot: workspaceRoot,
                    }),
                  ),
          }),
        ),
      ),
    ),
  ),
);

const adoption = (
  nativeThreadId: string,
  patch: Partial<ProviderThreadMirror.ProviderThreadAdoption> = {},
): ProviderThreadMirror.ProviderThreadAdoption => ({
  driver: AM,
  providerInstanceId: INSTANCE_ID,
  nativeThreadId,
  title: nativeThreadId,
  model: "claude",
  runtimeMode: "full-access",
  workspaceRoot: "/work/app",
  worktree: null,
  createdAt: now,
  updatedAt: now,
  history: [
    { role: "user", text: "Fix the build" },
    { role: "assistant", text: "Fixed it." },
  ],
  historyNotice: "3 earlier transcript entries are not shown.",
  conversationHeadRef: { driver: AM, nativeId: nativeThreadId, strength: "weak", ordinal: 5 },
  ...patch,
});

/** A T3 thread that already drives this agent, as one T3 started would. */
const seedOwner = Effect.fn(function* (nativeThreadId: string) {
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threadId = ThreadId.make(`owner-${nativeThreadId}`);
  const providerThread: OrchestrationV2ProviderThread = {
    id: ids.derive.providerThread({
      driver: AM,
      providerInstanceId: INSTANCE_ID,
      nativeThreadId,
    }),
    driver: AM,
    providerInstanceId: INSTANCE_ID,
    providerSessionId: null,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver: AM, nativeId: nativeThreadId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  yield* eventSink.write({
    events: [
      {
        id: EventId.make(`${threadId}:created`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: PROJECT_ID,
          title: "T3 thread",
          providerInstanceId: INSTANCE_ID,
          modelSelection: { instanceId: INSTANCE_ID, model: "claude" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: providerThread.id,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          deletedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
        },
      },
      {
        id: EventId.make(`${threadId}:provider`),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  return threadId;
});

it.effect("adopts a native thread once, with its history, project and worktree", () =>
  Effect.gen(function* () {
    const mirror = yield* ProviderThreadMirror.ProviderThreadMirror;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const owner = yield* seedOwner("t3-owned-abc123");

    const adopted = yield* mirror.adopt(
      adoption("worker", { worktree: { path: "/work/app-wt", branch: "am/worker" } }),
    );
    assert.strictEqual(adopted._tag, "Adopted");
    const threadId = adopted._tag === "Adopted" ? adopted.threadId : ThreadId.make("none");
    const records = yield* projections.getThreadRecords(threadId, [
      "messages",
      "turnItems",
      "providerThreads",
    ]);
    assert.strictEqual(records.thread.projectId, PROJECT_ID);
    assert.strictEqual(records.thread.worktreePath, "/work/app-wt");
    assert.strictEqual(records.thread.branch, "am/worker");
    assert.isTrue(records.thread.externalWorktree);
    assert.isTrue((yield* projections.getThreadShell(threadId))?.externalWorktree);
    assert.deepStrictEqual(
      records.messages.map((message) => [message.role, message.text]),
      [
        ["user", "Fix the build"],
        ["assistant", "Fixed it."],
      ],
    );
    assert.deepStrictEqual(
      records.turnItems.map((item) => item.type),
      ["system_notice", "user_message", "assistant_message"],
    );
    const providerThread = records.providerThreads[0]!;
    assert.strictEqual(records.thread.activeProviderThreadId, providerThread.id);
    assert.strictEqual(providerThread.nativeThreadRef?.nativeId, "worker");
    assert.strictEqual(providerThread.nativeConversationHeadRef?.ordinal, 5);

    // Never twice, and never an agent another thread already drives.
    assert.strictEqual((yield* mirror.adopt(adoption("worker")))._tag, "AlreadyOwned");
    assert.strictEqual((yield* mirror.adopt(adoption("t3-owned-abc123")))._tag, "AlreadyOwned");
    assert.deepStrictEqual(
      yield* mirror.adopt(adoption("elsewhere", { workspaceRoot: "/missing/app" })),
      { _tag: "WorkspaceMissing", workspaceRoot: "/missing/app" },
    );
    // A worktree with no copy here leaves the thread on the project root.
    const plain = yield* mirror.adopt(
      adoption("remote", { worktree: { path: "/missing/wt", branch: "am/remote" } }),
    );
    const plainThread = yield* projections.getThread(
      plain._tag === "Adopted" ? plain.threadId : ThreadId.make("none"),
    );
    assert.isNull(plainThread.worktreePath);
    assert.isNull(plainThread.branch);
    assert.isUndefined(plainThread.externalWorktree);

    const listed = yield* mirror.list({ driver: AM, providerInstanceId: INSTANCE_ID });
    assert.deepStrictEqual(
      listed.map((thread) => thread.providerThread.nativeThreadRef?.nativeId).toSorted(),
      ["remote", "t3-owned-abc123", "worker"],
    );
    assert.isTrue(listed.every((thread) => !thread.released && thread.archivedAt === null));
    assert.isTrue(listed.some((thread) => thread.threadId === owner));
  }).pipe(Effect.provide(testLayer)),
);

it.effect("marks the provider's current head before archiving and clears it after restoring", () =>
  Effect.gen(function* () {
    const mirror = yield* ProviderThreadMirror.ProviderThreadMirror;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const adopted = yield* mirror.adopt(adoption("removed"));
    const threadId = adopted._tag === "Adopted" ? adopted.threadId : ThreadId.make("none");
    const providerThreadId = (yield* projections.getThread(threadId)).activeProviderThreadId!;
    const currentHead = () =>
      projections
        .getThreadRecords(threadId, ["providerThreads"])
        .pipe(Effect.map((records) => records.providerThreads[0]?.nativeConversationHeadRef));

    // A thread that moved to another provider thread is left alone.
    yield* mirror.update({
      threadId,
      providerThreadId: ProviderThreadId.make("provider-thread:switched-away"),
      archived: true,
      headFingerprint: "am-removed",
    });
    assert.isUndefined((yield* currentHead())?.fingerprint);
    assert.isEmpty(archives.filter((entry) => entry.threadId === threadId));

    // A turn moves the head on after the mirror listed the thread at ordinal 5.
    const { providerThreads } = yield* projections.getThreadRecords(threadId, ["providerThreads"]);
    const listed = providerThreads[0]!;
    yield* eventSink.write({
      events: [
        {
          id: EventId.make("removed:head-moved"),
          type: "provider-thread.updated",
          threadId,
          driver: listed.driver,
          providerInstanceId: listed.providerInstanceId,
          occurredAt: now,
          payload: {
            ...listed,
            nativeConversationHeadRef: { ...listed.nativeConversationHeadRef!, ordinal: 7 },
          },
        },
      ],
    });

    yield* mirror.update({
      threadId,
      providerThreadId,
      archived: true,
      headFingerprint: "am-removed",
    });
    assert.deepStrictEqual(yield* currentHead(), {
      driver: AM,
      nativeId: "removed",
      strength: "weak",
      ordinal: 7,
      fingerprint: "am-removed",
    });
    yield* mirror.update({ threadId, providerThreadId, archived: false, headFingerprint: null });
    assert.deepStrictEqual(yield* currentHead(), {
      driver: AM,
      nativeId: "removed",
      strength: "weak",
      ordinal: 7,
    });
    assert.deepStrictEqual(
      archives.filter((entry) => entry.threadId === threadId),
      [
        { threadId, archived: true },
        { threadId, archived: false },
      ],
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("lets one of two concurrent adoptions of a native thread win", () =>
  Effect.gen(function* () {
    const mirror = yield* ProviderThreadMirror.ProviderThreadMirror;
    overlapProjectLookups = { count: 2, arrived: yield* Deferred.make<void>() };
    waitingProjectLookups = 0;
    const results = yield* Effect.all(
      [mirror.adopt(adoption("race")), mirror.adopt(adoption("race"))],
      {
        concurrency: "unbounded",
      },
    );
    assert.deepStrictEqual(results.map((result) => result._tag).toSorted(), [
      "Adopted",
      "AlreadyOwned",
    ]);
  }).pipe(
    Effect.ensuring(Effect.sync(() => (overlapProjectLookups = null))),
    Effect.provide(testLayer),
  ),
);
