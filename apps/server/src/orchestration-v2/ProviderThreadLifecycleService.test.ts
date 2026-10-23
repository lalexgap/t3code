import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as ProviderThreadLifecycle from "@t3tools/provider-core/server/ProviderThreadLifecycle";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderThreadLifecycleService from "./ProviderThreadLifecycleService.ts";

const AM = ProviderDriverKind.make("am");
const AM_INSTANCE = ProviderInstanceId.make("am_fleet");
const CODEX_INSTANCE = ProviderInstanceId.make("codex");
const PROJECT_ID = ProjectId.make("project-lifecycle");
const now = DateTime.makeUnsafe("2026-10-22T12:00:00.000Z");

type Change = ProviderThreadLifecycle.ProviderThreadLifecycleChange;

const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const sink = EventSink.layer.pipe(Layer.provideMerge(stores));

/** Runs `body` with the service reading `domainEvents` and an am handler recording each change. */
const withService = <A, E>(
  body: (input: {
    readonly domainEvents: Queue.Queue<OrchestrationV2DomainEvent>;
    readonly changes: Queue.Queue<Change & { readonly stillAppliesNow: boolean }>;
  }) => Effect.Effect<A, E, EventSink.EventSinkV2 | ProjectionStore.ProjectionStoreV2>,
) =>
  Effect.gen(function* () {
    const domainEvents = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const changes = yield* Queue.unbounded<Change & { readonly stillAppliesNow: boolean }>();
    const layer = ProviderThreadLifecycleService.layer.pipe(
      Layer.provideMerge(
        Layer.mergeAll(
          sink,
          ProviderThreadLifecycle.layer,
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromQueue(domainEvents),
          }),
        ),
      ),
      Layer.provideMerge(NodeServices.layer),
    );
    return yield* Effect.gen(function* () {
      const lifecycle = yield* ProviderThreadLifecycle.ProviderThreadLifecycle;
      yield* lifecycle.register(AM_INSTANCE, (change) =>
        change.stillApplies.pipe(
          Effect.flatMap((stillAppliesNow) => Queue.offer(changes, { ...change, stillAppliesNow })),
          Effect.as(`Handled ${change.event} ${change.nativeThreadId}.`),
        ),
      );
      return yield* body({ domainEvents, changes });
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.scoped);

const seedThread = Effect.fn(function* (
  threadId: ThreadId,
  nativeThreadId: string,
  instanceId = AM_INSTANCE,
) {
  const eventSink = yield* EventSink.EventSinkV2;
  const providerThreadId = ProviderThreadId.make(`${threadId}:provider`);
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
          title: "Thread",
          providerInstanceId: instanceId,
          modelSelection: { instanceId, model: "claude" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: providerThreadId,
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
        payload: {
          id: providerThreadId,
          driver: instanceId === AM_INSTANCE ? AM : ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          providerSessionId: null,
          appThreadId: threadId,
          ownerNodeId: null,
          nativeThreadRef: {
            driver: instanceId === AM_INSTANCE ? AM : ProviderDriverKind.make("codex"),
            nativeId: nativeThreadId,
            strength: "strong",
          },
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        },
      },
    ],
  });
  return threadId;
});

/** Commits the change, as the orchestrator does before its domain event goes out. */
const commit = Effect.fn(function* (
  threadId: ThreadId,
  type: "thread.settled" | "thread.unsettled" | "thread.archived",
  at = now,
) {
  const eventSink = yield* EventSink.EventSinkV2;
  const thread = yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(threadId);
  const payload =
    type === "thread.settled"
      ? { ...thread, settledOverride: "settled" as const, settledAt: at, updatedAt: at }
      : type === "thread.unsettled"
        ? { ...thread, settledOverride: "active" as const, settledAt: null, updatedAt: at }
        : { ...thread, archivedAt: at, updatedAt: at };
  const event: OrchestrationV2DomainEvent = {
    id: EventId.make(`${threadId}:${type}:${DateTime.toEpochMillis(at)}`),
    type,
    threadId,
    providerInstanceId: thread.providerInstanceId,
    occurredAt: at,
    payload,
  };
  yield* eventSink.write({ events: [event] });
  return event;
});

it.effect("reports a settle once, with the agent's ref, and leaves its notice on the thread", () =>
  withService(({ domainEvents, changes }) =>
    Effect.gen(function* () {
      const threadId = yield* seedThread(ThreadId.make("thread-a"), "laptop:t3-a");
      const settled = yield* commit(threadId, "thread.settled");
      yield* Queue.offer(domainEvents, settled);
      const change = yield* Queue.take(changes);
      assert.strictEqual(change.event, "settled");
      assert.strictEqual(change.threadId, threadId);
      assert.strictEqual(change.nativeThreadId, "laptop:t3-a");
      assert.isFalse(change.sharedWithActiveThread);
      assert.isTrue(change.stillAppliesNow);

      // Settling a settled thread re-emits the same settle.
      yield* Queue.offer(domainEvents, settled);
      const next = yield* seedThread(ThreadId.make("thread-b"), "t3-b");
      yield* Queue.offer(domainEvents, yield* commit(next, "thread.settled"));
      assert.strictEqual((yield* Queue.take(changes)).threadId, next);

      const { turnItems } = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(
        threadId,
        ["turnItems"],
      );
      assert.deepStrictEqual(
        turnItems.map((item) => (item.type === "system_notice" ? item.message : item.type)),
        ["Handled settled laptop:t3-a."],
      );
    }),
  ),
);

it.effect("reports an archive", () =>
  withService(({ domainEvents, changes }) =>
    Effect.gen(function* () {
      const threadId = yield* seedThread(ThreadId.make("thread-a"), "t3-a");
      yield* Queue.offer(domainEvents, yield* commit(threadId, "thread.archived"));
      const change = yield* Queue.take(changes);
      assert.strictEqual(change.event, "archived");
      assert.isTrue(change.stillAppliesNow);
    }),
  ),
);

it.effect("skips a thread taken back before the report ran, and other providers' threads", () =>
  withService(({ domainEvents, changes }) =>
    Effect.gen(function* () {
      const unsettled = yield* seedThread(ThreadId.make("thread-unsettled"), "t3-a");
      const settled = yield* commit(unsettled, "thread.settled");
      yield* commit(unsettled, "thread.unsettled", DateTime.add(now, { seconds: 1 }));
      yield* Queue.offer(domainEvents, settled);
      const codex = yield* seedThread(
        ThreadId.make("thread-codex"),
        "codex-thread",
        CODEX_INSTANCE,
      );
      yield* Queue.offer(domainEvents, yield* commit(codex, "thread.settled"));

      const reported = yield* seedThread(ThreadId.make("thread-reported"), "t3-b");
      yield* Queue.offer(domainEvents, yield* commit(reported, "thread.settled"));
      assert.strictEqual((yield* Queue.take(changes)).threadId, reported);
      assert.strictEqual(yield* Queue.size(changes), 0);
    }),
  ),
);

it.effect("tells the provider while another unsettled thread shares the agent", () =>
  withService(({ domainEvents, changes }) =>
    Effect.gen(function* () {
      const first = yield* seedThread(ThreadId.make("thread-first"), "t3-shared");
      const second = yield* seedThread(ThreadId.make("thread-second"), "t3-shared");
      yield* Queue.offer(domainEvents, yield* commit(first, "thread.settled"));
      const shared = yield* Queue.take(changes);
      assert.isTrue(shared.sharedWithActiveThread);
      assert.isFalse(shared.stillAppliesNow);
      yield* Queue.offer(domainEvents, yield* commit(second, "thread.settled"));
      const last = yield* Queue.take(changes);
      assert.isFalse(last.sharedWithActiveThread);
      assert.isTrue(last.stillAppliesNow);
    }),
  ),
);
