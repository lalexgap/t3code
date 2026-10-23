import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderThreadMirror from "@t3tools/provider-core/server/ProviderThreadMirror";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as ProjectService from "../project/ProjectService.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadLifecycleService from "./ThreadLifecycleService.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";

const EVENT_PREFIX = "provider-thread-mirror";
const HEAD_WRITE_ATTEMPTS = 3;

const failure =
  (operation: ProviderThreadMirror.ProviderThreadMirrorError["operation"], detail: string) =>
  (cause: unknown) =>
    new ProviderThreadMirror.ProviderThreadMirrorError({ operation, detail, cause });

const basename = (path: string) => path.split("/").findLast((part) => part !== "") ?? path;

function historyEvents(input: {
  readonly threadId: ThreadId;
  readonly at: DateTime.Utc;
  readonly adoption: ProviderThreadMirror.ProviderThreadAdoption;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const { threadId, at } = input;
  const common = (suffix: string, ordinal: number) => ({
    id: TurnItemId.make(`${EVENT_PREFIX}:turn-item:${threadId}:${suffix}`),
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  });
  const item = (suffix: string, turnItem: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
    id: EventId.make(`${EVENT_PREFIX}:turn-item:${threadId}:${suffix}`),
    type: "turn-item.updated",
    threadId,
    occurredAt: at,
    payload: turnItem,
  });
  const notice =
    input.adoption.historyNotice === null
      ? []
      : [
          item("notice", {
            ...common("notice", 1),
            type: "system_notice",
            title: "Earlier history not shown",
            message: input.adoption.historyNotice,
          }),
        ];
  return [
    ...notice,
    ...input.adoption.history.flatMap((entry, index) => {
      const suffix = String(index).padStart(6, "0");
      const messageId = MessageId.make(`${threadId}:${suffix}`);
      const message: OrchestrationV2ConversationMessage = {
        createdBy: entry.role === "user" ? "user" : "agent",
        creationSource: "provider",
        id: messageId,
        threadId,
        runId: null,
        nodeId: null,
        role: entry.role,
        text: entry.text,
        attachments: [],
        streaming: false,
        createdAt: at,
        updatedAt: at,
      };
      const base = common(suffix, index + 2);
      const turnItem: OrchestrationV2TurnItem =
        entry.role === "user"
          ? {
              ...base,
              createdBy: "user",
              creationSource: "provider",
              type: "user_message",
              messageId,
              inputIntent: "turn_start",
              text: entry.text,
              attachments: [],
            }
          : {
              ...base,
              type: "assistant_message",
              messageId,
              text: entry.text,
              streaming: false,
            };
      return [
        {
          id: EventId.make(`${EVENT_PREFIX}:message:${threadId}:${suffix}`),
          type: "message.updated" as const,
          threadId,
          occurredAt: at,
          payload: message,
        },
        item(suffix, turnItem),
      ];
    }),
  ];
}

/**
 * Adoption writes the thread, its history and its provider thread in one transaction, which
 * commits nothing when another thread already owns the native thread.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const mirror = yield* ProviderThreadMirror.ProviderThreadMirror;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const projects = yield* ProjectService.ProjectService;
    const lifecycle = yield* ThreadLifecycleService.ThreadLifecycleService;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;

    const list: ProviderThreadMirror.ProviderThreadMirrorHandler["list"] = Effect.fn(
      "ProviderThreadMirror.list",
    )(
      function* (binding) {
        const rows = yield* projections.getProviderThreadBindings(binding);
        return rows.map((row): ProviderThreadMirror.MirroredProviderThread => ({
          threadId: row.threadId,
          providerThread: row.providerThread,
          archivedAt: row.archivedAt,
          released: row.deletedAt !== null || !row.active,
        }));
      },
      Effect.mapError(failure("list", "Could not read the threads bound to this instance.")),
    );

    const resolveProject = Effect.fn("ProviderThreadMirror.resolveProject")(function* (
      workspaceRoot: string,
    ) {
      const id = yield* randomUuidV4;
      const bootstrapped = yield* projects
        .bootstrap({
          commandId: CommandId.make(`${EVENT_PREFIX}:project:${id}`),
          projectId: ProjectId.make(id),
          title: basename(workspaceRoot),
          workspaceRoot,
        })
        .pipe(
          // bootstrap looks the root up before taking the workspace lock, so a racing create
          // loses with a conflict that names the winner.
          Effect.map((result) => result.project.id),
          Effect.catchTags({
            ProjectConflictError: (conflict) => Effect.succeed(conflict.conflictingProjectId),
          }),
        );
      return bootstrapped;
    });

    const adopt: ProviderThreadMirror.ProviderThreadMirrorHandler["adopt"] = Effect.fn(
      "ProviderThreadMirror.adopt",
    )(
      function* (input) {
        const threadId = ThreadId.make(
          `${EVENT_PREFIX}:${input.providerInstanceId}:${input.nativeThreadId}`,
        );
        const existing = yield* projections
          .getThread(threadId)
          .pipe(
            Effect.asSome,
            Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeedNone }),
          );
        if (Option.isSome(existing)) return { _tag: "AlreadyOwned" } as const;
        // Never creates a directory: a fleet host's path that has no copy here stays unmirrored.
        const workspaceRoot = yield* workspacePaths
          .normalizeWorkspaceRoot(input.workspaceRoot)
          .pipe(Effect.option);
        if (Option.isNone(workspaceRoot))
          return { _tag: "WorkspaceMissing", workspaceRoot: input.workspaceRoot } as const;
        const worktreePath =
          input.worktree === null
            ? Option.none<string>()
            : yield* workspacePaths.normalizeWorkspaceRoot(input.worktree.path).pipe(Effect.option);
        const projectId = yield* resolveProject(workspaceRoot.value);
        const now = yield* DateTime.now;
        const modelSelection = { instanceId: input.providerInstanceId, model: input.model };
        const providerThreadId = idAllocator.derive.providerThread({
          driver: input.driver,
          providerInstanceId: input.providerInstanceId,
          nativeThreadId: input.nativeThreadId,
        });
        const appThread: OrchestrationV2AppThread = {
          createdBy: "system",
          creationSource: "provider",
          id: threadId,
          projectId,
          title: input.title,
          providerInstanceId: input.providerInstanceId,
          modelSelection,
          runtimeMode: input.runtimeMode,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          branch: Option.isSome(worktreePath) ? (input.worktree?.branch ?? null) : null,
          worktreePath: Option.getOrNull(worktreePath),
          ...(Option.isSome(worktreePath) ? { externalWorktree: true } : {}),
          linkedPullRequest: null,
          branchPullRequest: null,
          activeProviderThreadId: providerThreadId,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: input.createdAt,
          updatedAt: input.updatedAt,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          unsettledAt: null,
          snoozedUntil: null,
          snoozedAt: null,
          pinnedAt: null,
          pinOrderKey: null,
          activeOrderKey: null,
          lastVisitedAt: null,
          deletedAt: null,
        };
        const providerThread: OrchestrationV2ProviderThread = {
          id: providerThreadId,
          driver: input.driver,
          providerInstanceId: input.providerInstanceId,
          providerSessionId: null,
          appThreadId: threadId,
          ownerNodeId: null,
          nativeThreadRef: {
            driver: input.driver,
            nativeId: input.nativeThreadId,
            strength: "strong",
          },
          nativeConversationHeadRef: input.conversationHeadRef,
          status: "idle",
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          pendingBackgroundTasks: [],
          createdAt: now,
          updatedAt: now,
        };
        const write = eventSink
          .write({
            unlessNativeThreadOwned: providerThread,
            events: [
              {
                id: EventId.make(`${EVENT_PREFIX}:thread:${threadId}:created`),
                type: "thread.created",
                threadId,
                providerInstanceId: input.providerInstanceId,
                occurredAt: input.createdAt,
                payload: appThread,
              },
              ...historyEvents({ threadId, at: input.updatedAt, adoption: input }),
              {
                id: EventId.make(`${EVENT_PREFIX}:provider-thread:${providerThreadId}:adopted`),
                type: "provider-thread.updated",
                threadId,
                driver: input.driver,
                providerInstanceId: input.providerInstanceId,
                occurredAt: now,
                payload: providerThread,
              },
            ],
          })
          .pipe(
            // A concurrent adoption of the same native thread commits first: its thread id is ours.
            Effect.catchTags({
              EventSinkWriteError: (error) =>
                Effect.gen(function* () {
                  const winner = yield* projections.getThread(threadId).pipe(
                    Effect.asSome,
                    Effect.catchTags({
                      ProjectionStoreThreadNotFoundError: () => Effect.succeedNone,
                    }),
                  );
                  if (Option.isNone(winner)) return yield* error;
                  return [];
                }),
            }),
          );
        // Worktree removal checks ownership under this lease, so it cannot race this write.
        const committed = yield* Option.isSome(worktreePath)
          ? withWorkspaceLease(worktreePath.value, write)
          : write;
        return committed.length === 0
          ? ({ _tag: "AlreadyOwned" } as const)
          : ({ _tag: "Adopted", threadId } as const);
      },
      Effect.mapError(failure("adopt", "Could not create the thread.")),
    );

    /** The provider thread, while the thread still runs on it rather than on another provider. */
    const boundProviderThread = Effect.fnUntraced(function* (
      threadId: ThreadId,
      providerThreadId: OrchestrationV2ProviderThread["id"],
    ) {
      const records = yield* projections.getThreadRecords(threadId, ["providerThreads"]);
      if (records.thread.activeProviderThreadId !== providerThreadId) return undefined;
      return records.providerThreads.find((candidate) => candidate.id === providerThreadId);
    });

    /**
     * Reports whether the current head carries the fingerprint. A turn writing the row in between
     * wins, and the retry marks the head it moved to.
     */
    const setHeadFingerprint = Effect.fn("ProviderThreadMirror.setHeadFingerprint")(function* (
      threadId: ThreadId,
      providerThreadId: OrchestrationV2ProviderThread["id"],
      fingerprint: string | null,
    ) {
      for (let attempt = 0; attempt < HEAD_WRITE_ATTEMPTS; attempt++) {
        const current = yield* boundProviderThread(threadId, providerThreadId);
        if (!current) return false;
        const currentHead = current.nativeConversationHeadRef;
        if (currentHead === null) return fingerprint === null;
        if ((currentHead.fingerprint ?? null) === fingerprint) return true;
        const { fingerprint: _previous, ...position } = currentHead;
        const head = fingerprint === null ? position : { ...position, fingerprint };
        const now = yield* DateTime.now;
        const id = yield* randomUuidV4;
        const { committed } = yield* eventSink.writeIfProviderThreadUnchanged({
          expected: current,
          events: [
            {
              id: EventId.make(`${EVENT_PREFIX}:provider-thread:${current.id}:${id}`),
              type: "provider-thread.updated",
              threadId,
              driver: current.driver,
              providerInstanceId: current.providerInstanceId,
              occurredAt: now,
              payload: { ...current, nativeConversationHeadRef: head, updatedAt: now },
            },
          ],
        });
        if (committed) return true;
      }
      return false;
    });

    const setArchived = Effect.fn("ProviderThreadMirror.setArchived")(function* (
      threadId: ThreadId,
      archived: boolean,
    ) {
      const commandId = CommandId.make(`${EVENT_PREFIX}:archive:${yield* randomUuidV4}`);
      yield* archived
        ? lifecycle.archive({ commandId, threadId })
        : lifecycle.unarchive({ commandId, threadId });
    });

    const update: ProviderThreadMirror.ProviderThreadMirrorHandler["update"] = Effect.fn(
      "ProviderThreadMirror.update",
    )(
      function* (input) {
        if ((yield* boundProviderThread(input.threadId, input.providerThreadId)) === undefined)
          return;
        const mark = (fingerprint: string | null) =>
          setHeadFingerprint(input.threadId, input.providerThreadId, fingerprint);
        // The fingerprint is the provider's note about why it archived, so it lands before the
        // archive and is cleared only after the thread is back.
        if (input.archived === true) {
          if (input.headFingerprint === undefined || (yield* mark(input.headFingerprint)))
            yield* setArchived(input.threadId, true);
        } else if (input.archived === false) {
          yield* setArchived(input.threadId, false);
          if (input.headFingerprint !== undefined) yield* mark(input.headFingerprint);
        } else if (input.headFingerprint !== undefined) yield* mark(input.headFingerprint);
      },
      Effect.mapError(failure("update", "Could not update the thread.")),
    );

    yield* mirror.install({ list, adopt, update });
  }),
);
