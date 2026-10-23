import {
  EventId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type ThreadId,
} from "@t3tools/contracts";
import * as ProviderThreadLifecycle from "@t3tools/provider-core/server/ProviderThreadLifecycle";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const EVENT_PREFIX = "provider-thread-lifecycle";

type LifecycleEvent = ProviderThreadLifecycle.ProviderThreadLifecycleChange["event"];

const applies = (event: LifecycleEvent, thread: OrchestrationV2AppThread) =>
  thread.deletedAt === null &&
  (event === "archived"
    ? thread.archivedAt !== null
    : thread.archivedAt === null && thread.settledOverride === "settled");

/** When the thread became done, telling a new settle or archive from a re-emitted one. */
const doneAt = (event: LifecycleEvent, thread: OrchestrationV2AppThread) => {
  const at = event === "archived" ? thread.archivedAt : thread.settledAt;
  return `${event}:${at === null ? "" : DateTime.toEpochMillis(at)}`;
};

/**
 * Reports each committed settle and archive to the provider instance owning the thread's native
 * thread. thread.settled covers the sidebar, MCP tools and automatic settlement alike.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const lifecycle = yield* ProviderThreadLifecycle.ProviderThreadLifecycle;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const eventSink = yield* EventSink.EventSinkV2;
    // Settling a settled thread re-emits thread.settled with the same settledAt.
    const reported = new Map<ThreadId, string>();

    const stillApplies = (event: LifecycleEvent, threadId: ThreadId) =>
      projections.getThread(threadId).pipe(
        Effect.map((thread) => applies(event, thread)),
        Effect.orElseSucceed(() => false),
      );

    /** Another thread still at work on the same native thread keeps it running. */
    const sharedWithActiveThread = Effect.fnUntraced(function* (
      threadId: ThreadId,
      binding: Pick<OrchestrationV2ProviderThread, "driver" | "providerInstanceId">,
      nativeThreadId: string,
    ) {
      const bindings = yield* projections.getProviderThreadBindings({
        driver: binding.driver,
        providerInstanceId: binding.providerInstanceId,
      });
      for (const other of bindings) {
        if (
          other.threadId === threadId ||
          !other.active ||
          other.deletedAt !== null ||
          other.archivedAt !== null ||
          other.providerThread.nativeThreadRef?.nativeId !== nativeThreadId
        )
          continue;
        const thread = yield* projections.getThread(other.threadId);
        if (thread.settledOverride !== "settled") return true;
      }
      return false;
    });

    // In the latest run's band, so it follows the thread's work rather than leading it.
    const writeNotice = Effect.fnUntraced(function* (
      threadId: ThreadId,
      providerThreadId: OrchestrationV2AppThread["activeProviderThreadId"],
      message: string,
    ) {
      const { runs } = yield* projections.getThreadRecords(threadId, ["runs"]);
      const latestRun = runs.reduce<(typeof runs)[number] | null>(
        (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
        null,
      );
      const now = yield* DateTime.now;
      const id = yield* randomUuidV4;
      yield* eventSink.write({
        events: [
          {
            id: EventId.make(`${EVENT_PREFIX}:notice:${id}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make(`${EVENT_PREFIX}:notice:${id}`),
              threadId,
              runId: latestRun?.id ?? null,
              nodeId: null,
              providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              // The event sink assigns the position.
              ordinal: 0,
              status: "completed",
              title: null,
              type: "system_notice",
              message,
              startedAt: now,
              completedAt: now,
              updatedAt: now,
            },
          },
        ],
      });
    });

    const report = Effect.fn("ProviderThreadLifecycleService.report")(
      function* (event: LifecycleEvent, threadId: ThreadId) {
        const { thread, providerThreads } = yield* projections.getThreadRecords(threadId, [
          "providerThreads",
        ]);
        if (!applies(event, thread)) return;
        const key = doneAt(event, thread);
        if (reported.get(threadId) === key) return;
        const providerThread = providerThreads.find(
          (candidate) => candidate.id === thread.activeProviderThreadId,
        );
        const nativeThreadId = providerThread?.nativeThreadRef?.nativeId;
        if (providerThread === undefined || nativeThreadId == null) return;
        if (!(yield* lifecycle.handles(providerThread.providerInstanceId))) return;
        reported.set(threadId, key);
        const notice = yield* lifecycle.notify(providerThread.providerInstanceId, {
          event,
          threadId,
          providerThreadId: providerThread.id,
          nativeThreadId,
          sharedWithActiveThread: yield* sharedWithActiveThread(
            threadId,
            providerThread,
            nativeThreadId,
          ),
          stillApplies: stillApplies(event, threadId),
        });
        if (notice !== null) yield* writeNotice(threadId, providerThread.id, notice);
      },
      (effect, event, threadId) =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.failCause(cause)
              : Effect.logWarning("Could not report a thread lifecycle change to its provider", {
                  event,
                  threadId,
                  cause: Cause.pretty(cause),
                }),
          ),
        ),
    );

    const processEvent = (event: OrchestrationV2DomainEvent) => {
      switch (event.type) {
        case "thread.settled":
          return report("settled", event.threadId);
        case "thread.archived":
          return report("archived", event.threadId);
        case "thread.unsettled":
        case "thread.unarchived":
          return Effect.sync(() => reported.delete(event.threadId));
        default:
          return Effect.void;
      }
    };

    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, processEvent).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Thread lifecycle event stream failed", { cause }),
        ),
      ),
    );
  }),
);
