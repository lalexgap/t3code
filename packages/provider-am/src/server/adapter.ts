import {
  ProviderDriverKind,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import type { AmSettings } from "../settings.ts";
import type { AmApi, AmApiError, AmTranscriptEntry } from "./api.ts";
import {
  amAgentName,
  amEntryStatus,
  amProviderForModel,
  amToolPresentation,
  amTurnPhase,
  fleetEventConcerns,
  isEntryOpen,
  isSentMessage,
  sameEntry,
} from "./mapping.ts";

const AM_PROVIDER = ProviderDriverKind.make("am");

/** Transcript indexes run past any real conversation; `after` beyond the end returns only the total. */
const TRANSCRIPT_END = 1_000_000_000;
const EVENT_RECONNECT_MS = 2_000;
// am launches agents with their provider's own full access; it has no restricted mode to map to.
const FULL_ACCESS_ONLY = "Agent Motel only runs in Full access";

const AmProviderCapabilitiesV2 = {
  // The agent's own provider enforces permissions; T3 never answers for it.
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "weak",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: false,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "weak",
    nativeRequestIds: "none",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface AmAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: AmSettings;
  readonly api: AmApi;
  /** Re-read cadence while a turn runs, in case the event stream is down. */
  readonly pollIntervalMs?: number;
}

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  /** The transcript length at send, then the index of T3's own prompt once am delivers it. */
  startIndex: number;
  /** Lowest transcript index that can still change. */
  cursor: number;
  readonly published: Map<number, { readonly entry: AmTranscriptEntry; readonly settled: boolean }>;
  readonly ordinals: Map<string, number>;
  nextOrdinal: number;
  readonly sent: Array<string>;
  delivered: boolean;
  /** am can list the delivered prompt while still reporting the idle it had before it. */
  workSeen: boolean;
  attentionEpisodes: number;
  inAttention: boolean;
  readonly done: Deferred.Deferred<void>;
}

const nativeRef = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
  driver: AM_PROVIDER,
  nativeId,
  strength,
});

export const makeAmAdapterV2 = Effect.fn("makeAmAdapterV2")(function* (
  options: AmAdapterV2Options,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const providerHost = yield* ProviderHost.ProviderHost;
  const { api } = options;
  const protocolError = (detail: string, cause?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: AM_PROVIDER,
      detail,
      ...(cause === undefined ? {} : { cause }),
    });
  const fromApi = <A>(effect: Effect.Effect<A, AmApiError>) =>
    effect.pipe(Effect.mapError((error) => protocolError(error.message, error)));

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: AM_PROVIDER,
    getCapabilities: () => Effect.succeed(AmProviderCapabilitiesV2),
    planSelectionTransition: (input) =>
      Effect.succeed(
        input.current.model === input.target.model
          ? { type: "apply_on_next_turn" as const }
          : {
              type: "reject" as const,
              reason: "An Agent Motel agent keeps its provider. Start a new thread to change it.",
            },
      ),
    openSession: Effect.fn("AmAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      if (!options.settings.enabled) return yield* protocolError("Agent Motel is disabled");
      if (input.runtimePolicy.interactionMode === "plan")
        return yield* protocolError("Agent Motel does not support Plan mode");
      if (input.runtimePolicy.runtimeMode !== "full-access")
        return yield* protocolError(FULL_ACCESS_ONLY);
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? providerHost.paths.cwd;
      const now = yield* DateTime.now;
      let session: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: AM_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: AmProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const wake = yield* Queue.sliding<void>(1);
      const lock = yield* Semaphore.make(1);
      let thread: OrchestrationV2ProviderThread | undefined;
      let agentName: string | undefined;
      let active: ActiveTurn | undefined;
      let closed = false;

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);
      const updateSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = null,
      ) {
        if (session.status === status && session.lastError === lastError) return;
        session = { ...session, status, lastError, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_session.updated",
          driver: AM_PROVIDER,
          providerSession: session,
        });
      });
      const updateThread = Effect.fnUntraced(function* (
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        if (!thread) return;
        thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_thread.updated",
          driver: AM_PROVIDER,
          providerThread: thread,
        });
      });

      const baseItem = (turn: ActiveTurn, key: string, time: DateTime.Utc) => {
        if (!turn.ordinals.has(key)) turn.ordinals.set(key, turn.nextOrdinal++);
        const identity = `${options.instanceId}:${agentName}:${key}`;
        return {
          id: idAllocator.derive.turnItemFromProviderItem({
            driver: AM_PROVIDER,
            nativeItemId: identity,
          }),
          nodeId: idAllocator.derive.nodeFromProviderItem({
            driver: AM_PROVIDER,
            nativeItemId: identity,
          }),
          identity,
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          providerThreadId: turn.input.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef(`${agentName}:${key}`, "weak"),
          parentItemId: null,
          ordinal: turn.ordinals.get(key)!,
          startedAt: time,
          updatedAt: time,
        };
      };
      const emitNotice = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        key: string,
        title: string,
        message: string,
      ) {
        const time = yield* DateTime.now;
        const { identity: _identity, ...base } = baseItem(turn, key, time);
        yield* emit({
          type: "turn_item.updated",
          driver: AM_PROVIDER,
          turnItem: {
            ...base,
            type: "system_notice",
            status: "completed",
            title,
            completedAt: time,
            message,
          },
        });
      });
      const publishEntry = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        entry: AmTranscriptEntry,
        settled: boolean,
      ) {
        if (entry.kind === "user") {
          if (isSentMessage(entry.text ?? "", turn.sent)) return;
          // Someone else reached the agent through am (another agent, or its terminal).
          if (entry.text?.trim())
            yield* emitNotice(turn, `${entry.index}`, "Message to the agent", entry.text);
          return;
        }
        const time = yield* DateTime.now;
        const status = amEntryStatus(entry, settled);
        const { identity, ...base } = baseItem(turn, `${entry.index}`, time);
        const completedAt = status === "running" ? null : time;
        let turnItem: OrchestrationV2TurnItem;
        if (entry.kind === "assistant") {
          const text = entry.text ?? "";
          const messageId = idAllocator.derive.messageFromProviderItem({
            driver: AM_PROVIDER,
            nativeItemId: identity,
          });
          const message: OrchestrationV2ConversationMessage = {
            id: messageId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            nodeId: base.nodeId,
            role: "assistant",
            text,
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: time,
            updatedAt: time,
          };
          yield* emit({ type: "message.updated", driver: AM_PROVIDER, message });
          turnItem = {
            ...base,
            type: "assistant_message",
            status,
            title: null,
            completedAt,
            messageId,
            text,
            streaming: false,
          };
        } else {
          turnItem = { ...base, ...amToolPresentation(entry), status, completedAt };
        }
        yield* emit({
          type: "node.updated",
          driver: AM_PROVIDER,
          node: {
            id: base.nodeId,
            threadId: base.threadId,
            runId: base.runId,
            parentNodeId: turn.input.rootNodeId,
            rootNodeId: turn.input.rootNodeId,
            kind: entry.kind === "assistant" ? "assistant_message" : "tool_call",
            status,
            countsForRun: false,
            providerThreadId: base.providerThreadId,
            providerTurnId: base.providerTurnId,
            nativeItemRef: base.nativeItemRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: base.startedAt,
            completedAt,
          },
        });
        yield* emit({ type: "turn_item.updated", driver: AM_PROVIDER, turnItem });
      });
      const applyEntries = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        entries: ReadonlyArray<AmTranscriptEntry>,
        total: number,
        settled: boolean,
      ) {
        for (const entry of entries) {
          if (entry.index < turn.startIndex) continue;
          // Until T3's prompt shows up, the transcript belongs to work that came before it.
          if (!turn.delivered) {
            if (entry.kind === "user" && isSentMessage(entry.text ?? "", turn.sent)) {
              turn.delivered = true;
              turn.startIndex = entry.index;
            }
            continue;
          }
          const previous = turn.published.get(entry.index);
          if (previous?.settled === settled && sameEntry(previous.entry, entry)) continue;
          turn.published.set(entry.index, { entry, settled });
          if (entry.kind !== "user") turn.workSeen = true;
          yield* publishEntry(turn, entry, settled);
        }
        const open = turn.delivered
          ? entries.filter((entry) => entry.index >= turn.startIndex && isEntryOpen(entry))
          : [];
        turn.cursor = Math.max(
          turn.startIndex,
          open.length > 0 ? Math.min(...open.map((entry) => entry.index)) : total,
        );
      });
      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "completed" | "interrupted" | "cancelled" | "failed",
        detail?: string,
      ) {
        if (active !== turn) return;
        // One last read so open tools settle and the closing reply lands in this run.
        const last = yield* api.transcript(agentName!, turn.cursor).pipe(Effect.option);
        if (last._tag === "Some")
          yield* applyEntries(turn, last.value.entries, last.value.total, true);
        else
          for (const { entry, settled } of turn.published.values())
            if (!settled && isEntryOpen(entry)) {
              turn.published.set(entry.index, { entry, settled: true });
              yield* publishEntry(turn, entry, true);
            }
        const completedAt = yield* DateTime.now;
        turn.providerTurn = { ...turn.providerTurn, status, completedAt };
        yield* emit({
          type: "provider_turn.updated",
          driver: AM_PROVIDER,
          threadId: turn.input.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateThread({ status: "idle" });
        yield* updateSession("ready", status === "failed" ? (detail ?? null) : null);
        active = undefined;
        if (status === "failed") {
          const failure = makeProviderFailure({
            class: "provider_error",
            message: detail ?? "The Agent Motel agent stopped.",
          });
          const { identity: _identity, ...base } = baseItem(
            turn,
            `failure:${turn.input.runId}`,
            completedAt,
          );
          yield* emit({
            type: "turn_item.updated",
            driver: AM_PROVIDER,
            turnItem: {
              ...base,
              type: "error",
              status: "failed",
              title: null,
              completedAt,
              failure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: AM_PROVIDER,
            providerThreadId: turn.providerTurn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure,
            failureItemOrdinal: base.ordinal,
            threadDisposition: "reusable",
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: AM_PROVIDER,
            providerThreadId: turn.providerTurn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure: null,
            threadDisposition: "reusable",
          });
        }
        yield* Deferred.succeed(turn.done, undefined);
      });
      /** Reads the agent's transcript and status, and settles the turn when am says it is done. */
      const sync = Effect.fnUntraced(function* () {
        const turn = active;
        if (!turn || !agentName || closed) return;
        const transcript = yield* api.transcript(agentName, turn.cursor);
        if (active !== turn) return;
        yield* applyEntries(turn, transcript.entries, transcript.total, false);
        const phase = amTurnPhase(transcript.status);
        if (turn.delivered && (phase === "running" || phase === "attention")) turn.workSeen = true;
        if (phase !== "attention") turn.inAttention = false;
        switch (phase) {
          case "running":
            return yield* updateSession("running");
          case "attention": {
            yield* updateSession("waiting");
            if (turn.inAttention) return;
            turn.inAttention = true;
            const detail = yield* api.getAgent(agentName).pipe(Effect.option);
            const reason = Option.getOrUndefined(Option.flatten(detail))?.statusReason;
            return yield* emitNotice(
              turn,
              `attention:${turn.input.runId}:${++turn.attentionEpisodes}`,
              "Waiting in its terminal",
              `${reason ? `${reason}. ` : ""}T3 Code cannot answer this agent's prompts. Answer it in its session with \`am j ${agentName}\`.`,
            );
          }
          case "idle":
            if (turn.workSeen) yield* finish(turn, "completed");
            return;
          case "gone":
            return yield* finish(
              turn,
              "failed",
              `The Agent Motel agent ${agentName} is ${transcript.status}. Send a message to resume it.`,
            );
        }
      });

      yield* Stream.fromQueue(wake).pipe(
        Stream.runForEach(() =>
          sync().pipe(
            lock.withPermits(1),
            Effect.catchCause((cause) =>
              Effect.logWarning("Could not read the Agent Motel agent", cause),
            ),
          ),
        ),
        Effect.forkIn(scope),
      );
      // The fleet event stream is only a wake signal; the transcript is the source of truth.
      yield* api.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (active && agentName && fleetEventConcerns(event.data, agentName))
              Queue.offerUnsafe(wake, undefined);
          }),
        ),
        Effect.catch((error) => Effect.logDebug("Agent Motel event stream ended", error)),
        Effect.andThen(Effect.sleep(EVENT_RECONNECT_MS)),
        Effect.forever,
        Effect.forkIn(scope),
      );
      yield* Effect.sleep(options.pollIntervalMs ?? 5_000).pipe(
        Effect.andThen(
          Effect.sync(() => {
            if (active) Queue.offerUnsafe(wake, undefined);
          }),
        ),
        Effect.forever,
        Effect.forkIn(scope),
      );
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          // The am agent outlives this session: T3 releases idle sessions, and the agent is
          // the user's to keep. Only the T3 run ends here.
          if (active) yield* finish(active, "cancelled", "T3 Code released the session");
          yield* Queue.shutdown(wake);
          yield* Queue.end(events);
        }).pipe(lock.withPermits(1)),
      );

      const register = Effect.fnUntraced(function* (
        args: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        const existing = args.existingProviderThread;
        if (
          existing &&
          (existing.driver !== AM_PROVIDER || existing.providerInstanceId !== options.instanceId)
        )
          return yield* protocolError("This thread belongs to another provider instance");
        const name =
          existing?.nativeThreadRef?.nativeId ??
          input.initialNativeThreadId ??
          amAgentName(args.threadId, args.title);
        if (thread && agentName === name && thread.appThreadId === args.threadId) return thread;
        if (thread) return yield* protocolError("This session already serves another agent");
        if (active) return yield* protocolError("Cannot change agents during an active turn");
        const time = yield* DateTime.now;
        agentName = name;
        thread = existing
          ? {
              ...existing,
              providerSessionId: input.providerSessionId,
              nativeThreadRef: nativeRef(name),
              status: "idle",
              updatedAt: time,
            }
          : {
              id: idAllocator.derive.providerThread({
                driver: AM_PROVIDER,
                providerInstanceId: options.instanceId,
                nativeThreadId: name,
              }),
              driver: AM_PROVIDER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: args.threadId,
              ownerNodeId: null,
              nativeThreadRef: nativeRef(name),
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: time,
              updatedAt: time,
            };
        yield* emit({
          type: "provider_thread.updated",
          driver: AM_PROVIDER,
          providerThread: thread,
        });
        return thread;
      });
      const validateThread = (candidate: OrchestrationV2ProviderThread) =>
        thread !== undefined &&
        candidate.driver === AM_PROVIDER &&
        candidate.providerInstanceId === options.instanceId &&
        candidate.nativeThreadRef?.nativeId === agentName;
      const promptText = (message: ProviderAdapter.ProviderAdapterV2TurnMessage) =>
        providerMessageTextWithAttachmentPaths({
          text: message.text,
          attachments: message.attachments,
          resolveAttachmentPath: providerHost.resolveAttachmentPath,
        }).trim();

      const start = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        if (closed || !agentName || !validateThread(turnInput.providerThread))
          return yield* protocolError("This thread is not attached to an Agent Motel agent");
        if (active)
          return yield* new ProviderAdapter.ProviderAdapterProtocolError({
            driver: AM_PROVIDER,
            detail: "The agent already has an active turn; queue through the orchestrator",
            reason: "previous_turn_active",
          });
        if (turnInput.runtimePolicy.interactionMode === "plan")
          return yield* protocolError("Agent Motel does not support Plan mode");
        if (turnInput.runtimePolicy.runtimeMode !== "full-access")
          return yield* protocolError(FULL_ACCESS_ONLY);
        const text = promptText(turnInput.message);
        if (!text) return yield* protocolError("Agent Motel needs a text message");
        const name = agentName;
        const detail = Option.getOrUndefined(yield* fromApi(api.getAgent(name)));
        let startIndex = 0;
        if (detail) {
          const phase = amTurnPhase(detail.status);
          if (phase === "gone") yield* fromApi(api.resume(name));
          const transcript = yield* fromApi(api.transcript(name, TRANSCRIPT_END));
          startIndex = transcript.total;
        }
        const startedAt = yield* DateTime.now;
        const providerTurn: OrchestrationV2ProviderTurn = {
          id: idAllocator.derive.providerTurn({
            driver: AM_PROVIDER,
            nativeTurnId: `${options.instanceId}:${name}:${turnInput.runId}:${startIndex}`,
          }),
          providerThreadId: turnInput.providerThread.id,
          nodeId: turnInput.rootNodeId,
          runAttemptId: turnInput.attemptId,
          nativeTurnRef: nativeRef(`${name}:${startIndex}`, "weak"),
          ordinal: turnInput.providerTurnOrdinal,
          status: "running",
          startedAt,
          completedAt: null,
        };
        const turn: ActiveTurn = {
          input: turnInput,
          providerTurn,
          startIndex,
          cursor: startIndex,
          published: new Map(),
          ordinals: new Map(),
          nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
          sent: [text],
          delivered: false,
          workSeen: false,
          attentionEpisodes: 0,
          inAttention: false,
          done: yield* Deferred.make<void>(),
        };
        thread = turnInput.providerThread;
        active = turn;
        yield* emit({
          type: "provider_turn.updated",
          driver: AM_PROVIDER,
          threadId: turnInput.threadId,
          providerTurn,
        });
        yield* updateThread({
          status: "active",
          firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
          lastRunOrdinal: turnInput.runOrdinal,
        });
        yield* updateSession("running");
        const provider = amProviderForModel(turnInput.modelSelection.model);
        yield* fromApi(
          detail
            ? api.sendMessage(name, text, "queue")
            : api.createAgent({
                name,
                dir: turnInput.runtimePolicy.cwd ?? cwd,
                task: text,
                ...(provider ? { provider } : {}),
              }),
        ).pipe(Effect.tapError((error) => finish(turn, "failed", error.detail)));
        Queue.offerUnsafe(wake, undefined);
      });

      const snapshot = Effect.fnUntraced(function* (current: OrchestrationV2ProviderThread) {
        const transcript = yield* api.transcript(agentName!, 0).pipe(
          Effect.map((value) => value.entries),
          Effect.catchIf(
            (error) => error.status === 404,
            () => Effect.succeed([] as ReadonlyArray<AmTranscriptEntry>),
          ),
          Effect.mapError((error) => protocolError(error.message, error)),
        );
        const time = yield* DateTime.now;
        return {
          providerThread: current,
          providerTurns: [],
          messages: transcript.flatMap((entry): OrchestrationV2ConversationMessage[] =>
            entry.kind === "tool" || !entry.text
              ? []
              : [
                  {
                    id: idAllocator.derive.messageFromProviderItem({
                      driver: AM_PROVIDER,
                      nativeItemId: `${options.instanceId}:${agentName}:history:${entry.index}`,
                    }),
                    threadId: current.appThreadId ?? input.threadId,
                    runId: null,
                    nodeId: null,
                    role: entry.kind,
                    text: entry.text,
                    attachments: [],
                    streaming: false,
                    createdBy: entry.kind === "assistant" ? "agent" : "user",
                    creationSource: "provider",
                    createdAt: time,
                    updatedAt: time,
                  },
                ],
          ),
          runtimeRequests: [],
        } satisfies ProviderAdapter.ProviderAdapterV2ThreadSnapshot;
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: AM_PROVIDER,
        providerSessionId: input.providerSessionId,
        providerSession: session,
        events: Stream.fromQueue(events),
        ensureThread: (args) =>
          register(args).pipe(
            lock.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: AM_PROVIDER,
                  threadId: args.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (args) =>
          register({
            threadId: args.threadId ?? args.providerThread.appThreadId ?? input.threadId,
            modelSelection: args.modelSelection ?? input.modelSelection,
            runtimePolicy: args.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: args.providerThread,
          }).pipe(
            lock.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: AM_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: args.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (args) =>
          start(args).pipe(
            lock.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: AM_PROVIDER,
                  threadId: args.threadId,
                  providerThreadId: args.providerThread.id,
                  runId: args.runId,
                  cause,
                }),
            ),
          ),
        // am's `now` mode types straight into the TUI, steering whatever runs there. Before the
        // turn's own prompt is delivered that is someone else's work, so queue behind the prompt.
        steerTurn: Effect.fn("AmAdapterV2.steerTurn")(
          function* (args) {
            const turn = active;
            if (
              !turn ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            )
              return yield* protocolError("This Agent Motel turn cannot be steered");
            const text = promptText(args.message);
            if (!text) return yield* protocolError("Agent Motel needs a text message");
            turn.sent.push(text);
            yield* fromApi(api.sendMessage(agentName!, text, turn.delivered ? "now" : "queue"));
          },
          (effect, args) =>
            effect.pipe(
              lock.withPermits(1),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: AM_PROVIDER,
                    providerThreadId: args.providerThread.id,
                    providerTurnId: args.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        // Esc stops the TUI's turn, but Claude Code runs no Stop hook for it, so am may keep
        // reporting `working`. The run ends here instead of waiting for a status that may not come.
        interruptTurn: Effect.fn("AmAdapterV2.interruptTurn")(
          function* (args) {
            const turn = active;
            if (
              !turn ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            )
              return yield* protocolError("This Agent Motel turn is no longer active");
            yield* fromApi(api.interrupt(agentName!));
            yield* finish(turn, "interrupted");
          },
          (effect, args) =>
            effect.pipe(
              lock.withPermits(1),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: AM_PROVIDER,
                    providerThreadId: args.providerThread.id,
                    providerTurnId: args.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        respondToRuntimeRequest: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
              driver: AM_PROVIDER,
              requestId: args.requestId,
              cause: "Agent Motel agents answer their own prompts in their terminal.",
            }),
          ),
        readThreadSnapshot: (args) =>
          (validateThread(args.providerThread)
            ? snapshot(args.providerThread)
            : Effect.fail(protocolError("Snapshot requested for a different Agent Motel agent"))
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: AM_PROVIDER,
                  providerThreadId: args.providerThread.id,
                  cause,
                }),
            ),
          ),
        rollbackThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: AM_PROVIDER,
              providerThreadId: args.providerThread.id,
              checkpointId: args.target.checkpointId,
              cause: "Agent Motel cannot rewind an agent's conversation.",
            }),
          ),
        forkThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: AM_PROVIDER,
              providerThreadId: args.sourceProviderThread.id,
              cause: "Agent Motel cannot fork an agent.",
            }),
          ),
      };
      return runtime;
    }),
  });
});
