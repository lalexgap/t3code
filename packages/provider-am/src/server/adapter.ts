import {
  ProviderDriverKind,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
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
import {
  AM_API_URL_RULE,
  isAmApiUrlAllowed,
  isAmLocalHostName,
  type AmSettings,
} from "../settings.ts";
import type {
  AmApi,
  AmApiError,
  AmConflict,
  AmMessageReceipt,
  AmMessageState,
  AmPermissionRequest,
  AmTranscriptEntry,
} from "./api.ts";
import {
  AM_MIRROR_ENTRY_LIMIT,
  AM_EMITTED_BYTE_LIMIT,
  amClipEntry,
  amConversationHead,
  amEntryEmittedBytes,
  amMirroredThrough,
} from "./mirrorPlan.ts";
import {
  amAgentName,
  amAgentRef,
  amApprovalChoices,
  amEntryStatus,
  amOptionForDecision,
  amPermissionPrompt,
  amPermissionRequestKind,
  amProviderForModel,
  amToolPresentation,
  amTurnPhase,
  fleetEventConcerns,
  isEntryOpen,
  parseAmAgentRef,
  sameEntry,
} from "./mapping.ts";

const AM_PROVIDER = ProviderDriverKind.make("am");

/** Transcript indexes run past any real conversation; `after` beyond the end returns only the total. */
const TRANSCRIPT_END = 1_000_000_000;
const EVENT_RECONNECT_MS = 2_000;
// am refuses a cancel while it is mid-delivery or its locks are busy, and an interrupt while the
// mailbox is busy or the message's turn has not started yet; both are worth retrying.
const CANCEL_RETRY_MS = 250;
const CANCEL_ATTEMPTS = 40;
// Claude flushes its session file in batches, so a turn's closing reply can reach the transcript
// after the Stop hook has marked the receipt done. A finished turn waits this long for it.
// Each read of a fleet host's agent is an ssh round trip, so a remote turn re-reads less often.
const SETTLE_GRACE_MS = 3_000;
const SETTLE_POLL_MS = 200;
const REMOTE_SETTLE_POLL_MS = 1_000;
const isSettled = (state: AmMessageState) =>
  state === "done" || state === "cancelled" || state === "failed";
const SETTLED_BY_REASON = {
  already_done: "done",
  already_cancelled: "cancelled",
  already_failed: "failed",
} as const;
// am launches agents with their provider's own full access; it has no restricted mode to map to.
const FULL_ACCESS_ONLY = "Agent Motel only runs in Full access";
// Claude can mark the agent as waiting just before its permission hook records the prompt, so a
// waiting agent of an am that relays prompts gets this long to list one before T3 says it cannot.
const ATTENTION_GRACE_MS = 2_000;
// am expires a request only when it is read, so T3 reads again once a deadline has passed.
const EXPIRY_SLACK_MS = 250;

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
  // T3 relays the prompts the agent's own provider raises under the mode am gave it, through an
  // am that lists them; T3 never asks on its own.
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
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
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

// A fleet-host agent edits that host's copy of the project, so T3's checkpoints of its own copy
// would not contain the agent's work.
const amCapabilities = (remote: boolean): OrchestrationV2ProviderCapabilities => ({
  ...AmProviderCapabilitiesV2,
  checkpointing: { ...AmProviderCapabilitiesV2.checkpointing, appCanCheckpointFilesystem: !remote },
});

export interface AmAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: AmSettings;
  readonly api: AmApi;
  /** Re-read cadence while a turn runs, in case the event stream is down. */
  readonly pollIntervalMs?: number;
  /** How long a finished turn waits for its closing reply to reach the transcript. */
  readonly settleGraceMs?: number;
  /** Shared with the fleet mirror, which leaves an agent alone while a T3 turn follows it. */
  readonly busyAgents?: Set<string>;
}

/** A message T3 sent, followed through am's delivery receipt. */
interface TrackedMessage {
  readonly id: string;
  readonly mode: "queue" | "now";
  state: AmMessageState;
  /** The transcript length when am delivered it. */
  transcriptIndex: number | null;
  /** The transcript user entry that shows this message, once seen. */
  shownAt: number | undefined;
  /** am's reason, once the receipt is `failed`. */
  error?: string | undefined;
  /** am ended its turn after an Esc, from T3's Stop or from anywhere else, rather than a finish. */
  interrupted?: boolean;
}

const isPending = (message: TrackedMessage) =>
  message.state === "queued" || message.state === "delivered";

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  /**
   * Follows work T3 did not send, started by the fleet mirror; it ends when the agent is idle
   * rather than on a receipt.
   */
  readonly observe: boolean;
  /** The thread's mirrored head when the turn started: later entries belong to this run. */
  from: number | undefined;
  /** An observing run skips a backlog this far back rather than replaying all of it. */
  readonly floor: number;
  /** Continues a run T3's restart cut off: an agent gone before it shows a reply fails it. */
  restartCheck: boolean;
  /** The prompt first, then any steering; the run ends when every one of them has. */
  readonly messages: Array<TrackedMessage>;
  /** Lowest transcript index that can still change; unset until am delivers the prompt. */
  cursor: number | undefined;
  readonly published: Map<number, { readonly entry: AmTranscriptEntry; readonly settled: boolean }>;
  readonly ordinals: Map<string, number>;
  nextOrdinal: number;
  attentionEpisodes: number;
  inAttention: boolean;
  /** When every message was first seen done while the transcript had not caught up. */
  settledAt: number | undefined;
  /** A re-read is already scheduled while the turn waits for its reply. */
  settleWakePending: boolean;
  /** Whether the agent's am lists its permission prompts; unknown until read once in the run. */
  permissions: "unknown" | "supported" | "unsupported";
  /** When the agent was first seen waiting with no prompt listed, while am relays prompts. */
  attentionSince: number | undefined;
  readonly done: Deferred.Deferred<void>;
}

/** One of the agent's permission prompts, shown as a T3 approval request. */
interface AmPendingRequest {
  readonly amId: string;
  readonly turn: ActiveTurn;
  readonly runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly item: OrchestrationV2TurnItem;
  readonly optionIds: ReadonlyMap<ProviderApprovalDecision, string>;
  readonly expiresAt: number | null;
  /** `answering` while T3's answer is on its way to am; `settled` once T3 showed its end. */
  state: "pending" | "answering" | "answered" | "settled";
  /** An answer whose reply was lost may have been applied, so am ending the request is no news. */
  maybeAnswered?: boolean;
}

const trimSlashes = (path: string) => path.replace(/\/+$/, "");

/** The fleet mirror asks for an observing run with a provider-created wake message. */
const isObserveMessage = (message: ProviderAdapter.ProviderAdapterV2TurnMessage) =>
  message.createdBy === "agent" && message.creationSource === "provider";

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
  const remote = options.settings.host !== "";
  const capabilities = amCapabilities(remote);
  const busyAgents = options.busyAgents ?? new Set<string>();
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
    getCapabilities: () => Effect.succeed(capabilities),
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
      if (!isAmApiUrlAllowed(options.settings)) return yield* protocolError(AM_API_URL_RULE);
      if (input.runtimePolicy.interactionMode === "plan")
        return yield* protocolError("Agent Motel does not support Plan mode");
      if (input.runtimePolicy.runtimeMode !== "full-access")
        return yield* protocolError(FULL_ACCESS_ONLY);
      // am resolves its own name to this machine, so such a "remote" agent would run here with
      // none of the checkpoints a local instance takes.
      const fleetHost = options.settings.host;
      if (
        fleetHost &&
        (isAmLocalHostName(fleetHost) ||
          isAmLocalHostName(fleetHost, (yield* fromApi(api.hosts)).local))
      )
        return yield* protocolError(
          `Fleet host "${fleetHost}" is the machine running \`am serve\`. Leave Fleet host blank to run agents there.`,
        );
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
        capabilities,
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
      /** The agent's am address: its name, or `host:name` on a fleet host. */
      let agentRef: string | undefined;
      let active: ActiveTurn | undefined;
      let closed = false;
      /** Whether am, or the fleet host's am, returns message receipts; checked before the first send. */
      let receiptsConfirmed = false;
      /** The agent's permission prompts T3 has shown, by runtime request id. */
      const requests = new Map<string, AmPendingRequest>();
      const forgetFinished = (request: AmPendingRequest) => {
        if (
          active !== request.turn &&
          (request.state === "answered" || request.state === "settled")
        )
          requests.delete(request.runtimeRequest.id);
      };
      const pendingRequestsOf = (turn: ActiveTurn) =>
        [...requests.values()].filter(
          (request) =>
            request.turn === turn && (request.state === "pending" || request.state === "answering"),
        );

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
        const identity = `${options.instanceId}:${agentRef}:${key}`;
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
          nativeItemRef: nativeRef(`${agentRef}:${key}`, "weak"),
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
      // am marks the transcript just before typing a message. A queued one reaches an idle agent,
      // so its entry comes before any reply; delivery through Claude's Stop-hook inbox shows no
      // entry, so once the reply starts it claims nothing later. A `now` message is typed into a
      // busy agent, which records it at its next step.
      const isOwnMessage = (turn: ActiveTurn, index: number) => {
        if (turn.messages.some((message) => message.shownAt === index)) return true;
        const owner = turn.messages.find((message) => {
          const from = message.transcriptIndex;
          if (message.shownAt !== undefined || from === null || index < from) return false;
          if (message.mode === "now") return true;
          for (let earlier = from; earlier < index; earlier++)
            if (turn.published.get(earlier)?.entry.kind !== "user") return false;
          return true;
        });
        if (owner) owner.shownAt = index;
        return owner !== undefined;
      };
      const publishEntry = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        entry: AmTranscriptEntry,
        settled: boolean,
      ) {
        if (entry.kind === "user") {
          if (isOwnMessage(turn, entry.index)) return;
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
      /**
       * The first transcript index this run shows. A T3 turn also shows what reached the agent
       * since the thread's mirrored head, so nothing between runs is lost or shown twice.
       */
      const turnStart = (turn: ActiveTurn) => {
        if (turn.observe)
          return turn.from === undefined ? undefined : Math.max(turn.from, turn.floor);
        const prompt = turn.messages[0]?.transcriptIndex;
        // Before am delivers the prompt, the transcript belongs to work that came before it.
        if (prompt === null || prompt === undefined) return undefined;
        if (turn.from === undefined) return prompt;
        return Math.max(Math.min(turn.from, prompt), prompt - AM_MIRROR_ENTRY_LIMIT);
      };
      const applyEntries = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        entries: ReadonlyArray<AmTranscriptEntry>,
        total: number,
        settled: boolean,
      ) {
        const start = turnStart(turn);
        if (start === undefined) return;
        if (turn.from !== undefined && start > turn.from) {
          const skipped = start - turn.from;
          turn.from = start;
          yield* emitNotice(
            turn,
            `gap:${turn.input.runId}`,
            "Earlier activity not shown",
            `${skipped} transcript ${skipped === 1 ? "entry" : "entries"} from before this run ${skipped === 1 ? "is" : "are"} not shown. Read them with \`am transcript ${agentRef}\`.`,
          );
        }
        // A transcript that grew a lot between reads shows only its newest entries, within an
        // entry and a text budget, so one read stays within what a live client can take.
        const clipped = entries.map(amClipEntry);
        const reading = Math.max(start, turn.cursor ?? start);
        let shown = Math.max(reading, total - AM_MIRROR_ENTRY_LIMIT);
        let budget = AM_EMITTED_BYTE_LIMIT;
        for (let index = clipped.length - 1; index >= 0; index--) {
          const entry = clipped[index]!;
          if (entry.index < shown) break;
          budget -= amEntryEmittedBytes(entry);
          if (budget < 0) {
            shown = entry.index + 1;
            break;
          }
        }
        if (shown > reading) {
          const skipped = shown - reading;
          for (const [index, { entry, settled: wasSettled }] of turn.published)
            if (index < shown && !wasSettled && isEntryOpen(entry)) {
              turn.published.set(index, { entry, settled: true });
              yield* publishEntry(turn, entry, true);
            }
          yield* emitNotice(
            turn,
            `gap:${turn.input.runId}:${shown}`,
            "Activity not shown",
            `${skipped} transcript ${skipped === 1 ? "entry" : "entries"} ${skipped === 1 ? "is" : "are"} not shown. Read them with \`am transcript ${agentRef}\`.`,
          );
        }
        for (const entry of clipped) {
          if (entry.index < shown) continue;
          const previous = turn.published.get(entry.index);
          if (previous?.settled === settled && sameEntry(previous.entry, entry)) continue;
          turn.published.set(entry.index, { entry, settled });
          yield* publishEntry(turn, entry, settled);
        }
        const open = clipped.filter((entry) => entry.index >= shown && isEntryOpen(entry));
        turn.cursor = Math.max(
          shown,
          open.length > 0 ? Math.min(...open.map((entry) => entry.index)) : total,
        );
      });
      /**
       * Whether the transcript shows the turn's end: its last entry is a reply. Delivery through
       * Claude's inbox writes no entry for the prompt, so this never relies on seeing it.
       */
      const showsReply = (turn: ActiveTurn) => {
        let last: AmTranscriptEntry | undefined;
        for (const { entry } of turn.published.values())
          if (!last || entry.index > last.index) last = entry;
        // A steer's reply comes after the steer, so an earlier message's reply does not count.
        const from = Math.max(...turn.messages.map((message) => message.transcriptIndex ?? 0));
        return last?.kind === "assistant" && last.index >= from;
      };
      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "completed" | "interrupted" | "cancelled" | "failed",
        detail?: string,
      ) {
        if (active !== turn) return;
        // One last read so open tools settle and the closing reply lands in this run.
        const last =
          turn.cursor === undefined
            ? Option.none()
            : yield* api.transcript(agentRef!, turn.cursor).pipe(Effect.option);
        if (last._tag === "Some")
          yield* applyEntries(turn, last.value.entries, last.value.total, true);
        else
          for (const { entry, settled } of turn.published.values())
            if (!settled && isEntryOpen(entry)) {
              turn.published.set(entry.index, { entry, settled: true });
              yield* publishEntry(turn, entry, true);
            }
        // A prompt still open goes with the run; the next run that follows the agent shows it again.
        // An answer on its way to am settles it itself.
        for (const request of pendingRequestsOf(turn))
          if (request.state === "pending")
            yield* settleRequest(request, request.maybeAnswered ? "resolved" : "cancelled");
        // Everything before the head is in the thread; a failed last read resumes at the cursor.
        const head = last._tag === "Some" ? last.value.total : turn.cursor;
        busyAgents.delete(agentRef!);
        const completedAt = yield* DateTime.now;
        turn.providerTurn = { ...turn.providerTurn, status, completedAt };
        yield* emit({
          type: "provider_turn.updated",
          driver: AM_PROVIDER,
          threadId: turn.input.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateThread({
          status: "idle",
          ...(head === undefined
            ? {}
            : {
                nativeConversationHeadRef: amConversationHead(agentRef!, head),
              }),
        });
        yield* updateSession("ready", status === "failed" ? (detail ?? null) : null);
        active = undefined;
        for (const request of requests.values()) if (request.turn === turn) forgetFinished(request);
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
      /** Ends a request T3 showed, without a decision: am says nothing of one taken elsewhere. */
      const settleRequest = Effect.fnUntraced(function* (
        request: AmPendingRequest,
        status: "resolved" | "expired" | "cancelled",
        note?: { readonly title: string; readonly message: string },
      ) {
        if (request.state === "settled") return;
        request.state = "settled";
        // The expiry says to answer in the terminal, so the waiting agent needs no second notice.
        if (status === "expired") request.turn.inAttention = true;
        const now = yield* DateTime.now;
        const itemStatus = status === "resolved" ? "completed" : "cancelled";
        const { decision: _decision, ...runtimeRequest } = request.runtimeRequest;
        yield* emit({
          type: "runtime_request.updated",
          driver: AM_PROVIDER,
          threadId: request.turn.input.threadId,
          runtimeRequest: { ...runtimeRequest, status, resolvedAt: now },
        });
        yield* emit({
          type: "node.updated",
          driver: AM_PROVIDER,
          node: { ...request.node, status: itemStatus, completedAt: now },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: AM_PROVIDER,
          turnItem: { ...request.item, status: itemStatus, completedAt: now, updatedAt: now },
        });
        if (note)
          yield* emitNotice(
            request.turn,
            `permission-note:${request.amId}`,
            note.title,
            note.message,
          );
      });
      const expiredNote = () => ({
        title: "Approval expired",
        message: `Nobody answered in time, so the agent's own prompt took over. Answer it in its session with \`am j ${agentRef}\`.`,
      });
      const showRequest = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        listed: AmPermissionRequest,
      ) {
        const nativeId = `${agentRef}:${listed.id}`;
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: AM_PROVIDER,
          providerTurnId: turn.providerTurn.id,
          nativeRequestId: nativeId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const now = yield* DateTime.now;
        const createdAt = DateTime.make(listed.createdAt).pipe(Option.getOrElse(() => now));
        const requestKind = amPermissionRequestKind(listed.tool);
        const { prompt, truncated } = amPermissionPrompt(listed);
        // Nobody may approve what the card cannot show in full.
        const choices = amApprovalChoices(listed.options, { denyOnly: truncated });
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId: turn.providerTurn.id,
          nativeRequestRef: nativeRef(nativeId),
          kind: requestKind,
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          parentNodeId: turn.input.rootNodeId,
          rootNodeId: turn.input.rootNodeId,
          kind: "approval_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId: turn.input.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef(nativeId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: now,
          completedAt: null,
        };
        const { identity: _identity, ...base } = baseItem(turn, `permission:${listed.id}`, now);
        const item: OrchestrationV2TurnItem = {
          ...base,
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          nodeId,
          nativeItemRef: nativeRef(nativeId),
          type: "approval_request",
          status: "waiting",
          title: listed.tool,
          completedAt: null,
          requestId,
          requestKind,
          prompt,
          ...(truncated
            ? {
                reason: `Input too long to review here — answer in its session: \`am j ${agentRef}\``,
              }
            : choices.hidden > 0
              ? {
                  reason: `The agent offers ${choices.hidden} more ${choices.hidden === 1 ? "choice" : "choices"} in its session: \`am j ${agentRef}\`.`,
                }
              : {}),
          options: choices.options,
        };
        const expiresAt =
          listed.expiresAt === null
            ? null
            : DateTime.make(listed.expiresAt).pipe(
                Option.map(DateTime.toEpochMillis),
                Option.getOrNull,
              );
        requests.set(requestId, {
          amId: listed.id,
          turn,
          runtimeRequest,
          node,
          item,
          optionIds: choices.optionIds,
          expiresAt,
          state: "pending",
        });
        yield* emit({ type: "node.updated", driver: AM_PROVIDER, node });
        yield* emit({
          type: "runtime_request.updated",
          driver: AM_PROVIDER,
          threadId: turn.input.threadId,
          runtimeRequest,
        });
        yield* emit({ type: "turn_item.updated", driver: AM_PROVIDER, turnItem: item });
        if (expiresAt !== null)
          yield* Effect.sleep(
            Math.max(0, expiresAt - (yield* Clock.currentTimeMillis)) + EXPIRY_SLACK_MS,
          ).pipe(
            Effect.andThen(Effect.sync(() => Queue.offerUnsafe(wake, undefined))),
            Effect.forkIn(scope),
          );
      });
      /**
       * Shows the prompts am lists for the agent and settles the ones it no longer does: answered
       * in the pane or by another client, expired, or dropped with the provider's wait.
       */
      const syncPermissions = Effect.fnUntraced(function* (turn: ActiveTurn, ref: string) {
        if (turn.permissions === "unknown") {
          const detail = yield* api.getAgent(ref).pipe(Effect.option);
          if (detail._tag === "None") return;
          turn.permissions =
            Option.getOrUndefined(detail.value)?.permissionRequests === true
              ? "supported"
              : "unsupported";
        }
        if (turn.permissions !== "supported") return;
        const listed = yield* api.permissions(ref).pipe(
          Effect.asSome,
          Effect.catch((error) => {
            // An am serve older than the routes, or a fleet host's am older than its own.
            if (error.status === 404 || error.status === 409) turn.permissions = "unsupported";
            // A failed read must not hold up the transcript; the next wake reads again.
            return Effect.logDebug("Could not list the Agent Motel agent's prompts", error).pipe(
              Effect.as(Option.none<ReadonlyArray<AmPermissionRequest>>()),
            );
          }),
        );
        if (listed._tag === "None" || active !== turn) return;
        const shown = new Set(
          [...requests.values()].filter((request) => request.turn === turn).map((r) => r.amId),
        );
        for (const request of listed.value)
          if (!shown.has(request.id)) yield* showRequest(turn, request);
        const open = new Set(listed.value.map((request) => request.id));
        const now = yield* Clock.currentTimeMillis;
        for (const request of pendingRequestsOf(turn))
          if (request.state === "pending" && !open.has(request.amId)) {
            if (!request.maybeAnswered && request.expiresAt !== null && now >= request.expiresAt)
              yield* settleRequest(request, "expired", expiredNote());
            else yield* settleRequest(request, "resolved");
          }
      });
      const answerRequest = Effect.fnUntraced(function* (
        request: AmPendingRequest,
        decision: ProviderApprovalDecision,
      ) {
        const optionId = amOptionForDecision(request.optionIds, decision);
        if (optionId === undefined)
          return yield* protocolError(`The agent offered no choice for "${decision}"`);
        request.state = "answering";
        const ref = agentRef!;
        const answer = yield* api.answerPermission(ref, request.amId, optionId).pipe(
          Effect.catchIf(
            (error) => error.status !== null && error.status < 500,
            (error) =>
              Effect.succeed({
                answered: false as const,
                reason: error.status === 404 ? ("not_pending" as const) : undefined,
                error: error.detail,
              }),
          ),
          // A transport failure is retried by T3, so the request is still T3's to answer.
          Effect.tapError(() =>
            Effect.sync(() => {
              request.maybeAnswered = true;
              if (request.state === "answering") request.state = "pending";
            }),
          ),
        );
        if (answer.answered) {
          request.state = "answered";
          return;
        }
        if (
          request.maybeAnswered &&
          (answer.reason === "already_resolved" || answer.reason === "not_pending")
        )
          return yield* settleRequest(request, "resolved", {
            title: "Answer may have been applied",
            message: `The reply to your earlier answer was lost, and Agent Motel no longer holds this request open, so that answer may already have been applied. Check the agent's session with \`am j ${ref}\`.`,
          });
        // Retrying cannot help: the prompt ended without this answer, so T3 shows what happened.
        switch (answer.reason) {
          case "expired":
            return yield* settleRequest(request, "expired", {
              title: "Approval expired",
              message: `Your answer arrived after the request expired, so it was not applied. Answer the agent's own prompt in its session with \`am j ${ref}\`.`,
            });
          case "already_resolved":
            return yield* settleRequest(request, "cancelled", {
              title: "Answered elsewhere",
              message: "Someone else answered this request first, so your answer was not applied.",
            });
          case "not_pending":
            return yield* settleRequest(request, "cancelled", {
              title: "Approval no longer open",
              message: `Your answer was not applied: ${answer.error}. It may have been answered in the agent's session.`,
            });
          case "remote_too_old":
            request.turn.permissions = "unsupported";
            return yield* settleRequest(request, "cancelled", {
              title: "Update Agent Motel",
              message: `${answer.error}. Update am there, or answer in the agent's session with \`am j ${ref}\`.`,
            });
          default:
            return yield* settleRequest(request, "cancelled", {
              title: "Approval not applied",
              message: `Agent Motel refused the answer: ${answer.error}. Answer in the agent's session with \`am j ${ref}\`.`,
            });
        }
      });
      const applyReceipt = (message: TrackedMessage, receipt: AmMessageReceipt) => {
        message.state = receipt.state;
        message.transcriptIndex = receipt.transcriptIndex ?? message.transcriptIndex;
        message.error = receipt.error ?? message.error;
        message.interrupted = receipt.state === "done" && receipt.interrupted === true;
      };
      /** Only the receipt says why a message failed, or whether a done turn was aborted. */
      const settle = Effect.fnUntraced(function* (message: TrackedMessage, state: AmMessageState) {
        message.state = state;
        if (state === "failed" ? message.error !== undefined : state !== "done") return;
        const receipt = yield* api.getMessage(agentRef!, message.id).pipe(Effect.option);
        if (receipt._tag === "Some" && receipt.value.state === state)
          applyReceipt(message, receipt.value);
      });
      /** A refusal T3 cannot act on, from an am too old to say why or with a reason it does not know. */
      const refused = (refusal: AmConflict) =>
        protocolError(`Agent Motel could not stop the agent: ${refusal.error}`);
      /** Cancels one message am has not delivered yet, waiting out a delivery in progress. */
      const withdrawMessage = Effect.fnUntraced(function* (message: TrackedMessage) {
        let withdrawn = false;
        for (let attempt = 1; message.state === "queued"; attempt++) {
          const result = yield* fromApi(api.cancelMessage(agentRef!, message.id));
          if (result.cancelled) {
            withdrawn = true;
            yield* settle(message, "cancelled");
            break;
          }
          switch (result.reason) {
            case "already_done":
            case "already_cancelled":
            case "already_failed":
              yield* settle(message, SETTLED_BY_REASON[result.reason]);
              return false;
            // Delivered already, or handed over and awaiting am's confirmation.
            case "already_delivered":
              if (result.state === "delivered") {
                message.state = "delivered";
                return false;
              }
              break;
            case "mailbox_busy":
              break;
            default:
              return yield* refused(result);
          }
          if (attempt === CANCEL_ATTEMPTS)
            return yield* protocolError(
              "Agent Motel is still handing the message to the agent. Try Stop again.",
            );
          yield* Effect.sleep(CANCEL_RETRY_MS);
          applyReceipt(message, yield* fromApi(api.getMessage(agentRef!, message.id)));
        }
        return withdrawn;
      });
      /** Cancels T3's messages am has not delivered yet; reports whether Stop withdrew any. */
      const withdraw = Effect.fnUntraced(function* (turn: ActiveTurn) {
        let withdrawn = false;
        for (const message of turn.messages.toReversed())
          if (yield* withdrawMessage(message)) withdrawn = true;
        return withdrawn;
      });
      /**
       * Esc for a delivered message, which am sends only while that message's own turn runs, so a
       * stale `delivered` can never stop whatever runs next. Reports whether this turn is stopped.
       */
      const interruptDelivered = Effect.fnUntraced(function* (message: TrackedMessage) {
        let reason = "the agent's mailbox stayed busy";
        for (let attempt = 1; ; attempt++) {
          if (attempt > CANCEL_ATTEMPTS)
            return yield* protocolError(`Agent Motel could not stop the agent: ${reason}`);
          if (message.state === "queued" && (yield* withdrawMessage(message))) return true;
          if (message.state !== "delivered") return false;
          const result = yield* fromApi(api.interruptMessage(agentRef!, message.id));
          if (result.interrupted) return true;
          switch (result.reason) {
            case "not_delivered":
              message.state = "queued";
              continue;
            case "already_done":
            case "already_cancelled":
            case "already_failed":
              yield* settle(message, SETTLED_BY_REASON[result.reason]);
              continue;
            // Another Stop already sent this turn its Esc; a second one would rewind the TUI.
            case "already_interrupted":
              return true;
            // The session ended with the turn; am fails the receipt rather than finishing it.
            case "no_session":
              return false;
            // A busy mailbox, or a turn queued behind earlier work in the agent.
            case "mailbox_busy":
            case "not_started":
              reason = result.error;
              yield* Effect.sleep(CANCEL_RETRY_MS);
              continue;
            default:
              return yield* refused(result);
          }
        }
      });
      /** Follows T3's messages through am's receipts and publishes the transcript they produced. */
      const sync = Effect.fnUntraced(function* () {
        const turn = active;
        if (!turn || !agentRef || closed) return;
        const ref = agentRef;
        for (const message of turn.messages) {
          if (!isPending(message)) continue;
          const receipt = yield* api.getMessage(ref, message.id).pipe(
            Effect.catchIf(
              (error) => error.status === 404,
              () =>
                Effect.succeed<AmMessageReceipt>({
                  id: message.id,
                  state: "failed",
                  transcriptIndex: null,
                  error:
                    "Agent Motel no longer has a receipt for this message, for example after `am move`.",
                }),
            ),
          );
          if (active !== turn) return;
          const wasDelivered = message.state === "delivered" || message.transcriptIndex !== null;
          applyReceipt(message, receipt);
          if (receipt.state !== "failed") continue;
          // A message that failed after delivery lost its session mid-turn, taking the run with it.
          if (
            (!turn.observe && message === turn.messages[0]) ||
            wasDelivered ||
            receipt.transcriptIndex !== null
          )
            return yield* finish(
              turn,
              "failed",
              receipt.error ??
                (message.transcriptIndex === null
                  ? "Agent Motel could not deliver the message."
                  : "The Agent Motel agent's session ended before it finished."),
            );
          yield* emitNotice(
            turn,
            `steer-failed:${message.id}`,
            "Message not delivered",
            receipt.error ?? "Agent Motel could not deliver a follow-up message.",
          );
        }
        // Someone else withdrew it, for example with `am queue --clear`.
        if (!turn.observe && turn.messages[0]!.state === "cancelled")
          return yield* finish(turn, "cancelled");
        turn.cursor ??= turnStart(turn);
        const read = yield* api.transcript(ref, turn.cursor ?? TRANSCRIPT_END).pipe(
          Effect.asSome,
          Effect.catchIf(
            (error) => error.status === 404,
            () => Effect.succeedNone,
          ),
        );
        if (active !== turn) return;
        // Withdraws a steer still queued, as the gone branch does, so a resume does not run it
        // outside any run.
        const stoppedOnRestart = () =>
          withdraw(turn).pipe(
            Effect.ignore,
            Effect.andThen(
              finish(
                turn,
                "failed",
                `The Agent Motel agent ${ref} stopped while T3 Code was restarting. Send a message to resume it.`,
              ),
            ),
          );
        if (read._tag === "None")
          return yield* turn.restartCheck
            ? stoppedOnRestart()
            : turn.observe
              ? finish(turn, "completed")
              : finish(turn, "failed", `Agent Motel no longer has the agent ${ref}.`);
        const transcript = read.value;
        yield* applyEntries(turn, transcript.entries, transcript.total, false);
        yield* syncPermissions(turn, ref);
        if (active !== turn) return;
        const phase = amTurnPhase(transcript.status);
        const prompting = pendingRequestsOf(turn).length > 0;
        // Until the continued run shows a reply, an agent that stops left it unfinished.
        if (turn.restartCheck) {
          if ([...turn.published.values()].some(({ entry }) => entry.kind === "assistant"))
            turn.restartCheck = false;
          else if (phase === "gone") return yield* stoppedOnRestart();
        }
        // An observing run lasts while the agent works or waits on a prompt; a T3 turn, while its
        // messages do.
        const observedWork =
          turn.observe && (phase === "running" || phase === "attention" || prompting);
        // am ends a turn aborted with Esc itself, whoever pressed it (`am interrupt` included).
        if (!observedWork && !turn.messages.some(isPending)) {
          const interrupted = turn.messages.some((message) => message.interrupted);
          if (!interrupted && !showsReply(turn)) {
            const now = yield* Clock.currentTimeMillis;
            turn.settledAt ??= now;
            if (now - turn.settledAt < (options.settleGraceMs ?? SETTLE_GRACE_MS)) {
              if (turn.settleWakePending) return;
              turn.settleWakePending = true;
              yield* Effect.sleep(remote ? REMOTE_SETTLE_POLL_MS : SETTLE_POLL_MS).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    turn.settleWakePending = false;
                    Queue.offerUnsafe(wake, undefined);
                  }),
                ),
                Effect.forkIn(scope),
              );
              return;
            }
          }
          return yield* finish(turn, interrupted ? "interrupted" : "completed");
        }
        turn.settledAt = undefined;
        if (phase !== "attention") {
          turn.inAttention = false;
          turn.attentionSince = undefined;
        }
        if (prompting) {
          turn.attentionSince = undefined;
          return yield* updateSession("waiting");
        }
        switch (phase) {
          case "running":
            return yield* updateSession("running");
          case "attention": {
            yield* updateSession("waiting");
            if (turn.inAttention) return;
            if (turn.permissions === "supported") {
              const now = yield* Clock.currentTimeMillis;
              turn.attentionSince ??= now;
              if (now - turn.attentionSince < ATTENTION_GRACE_MS) {
                yield* Effect.sleep(ATTENTION_GRACE_MS - (now - turn.attentionSince)).pipe(
                  Effect.andThen(Effect.sync(() => Queue.offerUnsafe(wake, undefined))),
                  Effect.forkIn(scope),
                );
                return;
              }
            }
            turn.inAttention = true;
            const detail = yield* api.getAgent(ref).pipe(Effect.option);
            const reason = Option.getOrUndefined(Option.flatten(detail))?.statusReason;
            return yield* emitNotice(
              turn,
              `attention:${turn.input.runId}:${++turn.attentionEpisodes}`,
              "Waiting in its terminal",
              `${reason ? `${reason}. ` : ""}${turn.permissions === "supported" ? "Agent Motel did not pass this prompt to T3 Code." : "T3 Code cannot answer this agent's prompts."} Answer it in its session with \`am j ${ref}\`.`,
            );
          }
          case "idle":
            return;
          case "gone":
            // A resumed agent would otherwise run the queued prompt outside any run.
            yield* withdraw(turn).pipe(Effect.ignore);
            return yield* finish(
              turn,
              "failed",
              `The Agent Motel agent ${ref} is ${transcript.status}. Send a message to resume it.`,
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
            if (active && agentRef && fleetEventConcerns(event.data, agentRef))
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
          amAgentRef(options.settings.host, amAgentName(args.threadId, args.title));
        // Capabilities follow the instance's Fleet host, so a thread whose agent lives elsewhere
        // would checkpoint the wrong copy.
        if (parseAmAgentRef(name).host !== (options.settings.host || undefined))
          return yield* protocolError(
            "This thread's Agent Motel agent is on another host than this instance's Fleet host. Restore the setting or start a new thread.",
          );
        if (thread && agentRef === name && thread.appThreadId === args.threadId) return thread;
        if (thread) return yield* protocolError("This session already serves another agent");
        if (active) return yield* protocolError("Cannot change agents during an active turn");
        const time = yield* DateTime.now;
        agentRef = name;
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
        candidate.nativeThreadRef?.nativeId === agentRef;
      const promptText = (message: ProviderAdapter.ProviderAdapterV2TurnMessage) =>
        providerMessageTextWithAttachmentPaths({
          text: message.text,
          attachments: message.attachments,
          resolveAttachmentPath: providerHost.resolveAttachmentPath,
        }).trim();

      const start = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        if (closed || !agentRef || !validateThread(turnInput.providerThread))
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
        const ref = agentRef;
        const head = amMirroredThrough(turnInput.providerThread.nativeConversationHeadRef, ref);
        // am's agents run on without T3, so a run T3 continues after its own restart follows the
        // agent rather than prompting it, which could also resume an agent stopped since.
        const restarted = turnInput.restartContinuationOfRunId !== undefined;
        if (isObserveMessage(turnInput.message) || restarted) {
          const { total } = yield* fromApi(
            api.transcript(ref, TRANSCRIPT_END).pipe(
              // The first read in the run reports an agent am no longer has.
              Effect.catchIf(
                (error) => restarted && error.status === 404,
                () => Effect.succeed({ total: 0 }),
              ),
            ),
          );
          return yield* begin(turnInput, {
            observe: true,
            restartCheck: restarted,
            nativeTurnId: `${ref}:observe:${turnInput.runId}`,
            // A transcript shorter than the head is a new session's, so it is all new.
            from: head === undefined ? total : head > total ? 0 : head,
            floor: total - AM_MIRROR_ENTRY_LIMIT,
            messages: [],
          });
        }
        const text = promptText(turnInput.message);
        if (!text) return yield* protocolError("Agent Motel needs a text message");
        const { host, name } = parseAmAgentRef(ref);
        const dir = turnInput.runtimePolicy.cwd ?? cwd;
        if (!receiptsConfirmed && !host) {
          if ((yield* fromApi(api.health)).messageReceipts !== true)
            return yield* protocolError(
              "This Agent Motel does not report message receipts, which T3 Code needs to follow a turn. Update am.",
            );
          receiptsConfirmed = true;
        }
        const detail = Option.getOrUndefined(yield* fromApi(api.getAgent(ref)));
        // A fleet host's copy lives at a mapped path, so only a local agent's dir is comparable.
        if (detail && !host && trimSlashes(detail.dir) !== trimSlashes(dir))
          return yield* protocolError(
            `The Agent Motel agent ${ref} works in ${detail.dir}, not this thread's ${dir}. Start a new thread, or remove the agent with \`am rm ${ref}\`.`,
          );
        if (detail && amTurnPhase(detail.status) === "gone") yield* fromApi(api.resume(ref));
        if (!detail) {
          const provider = amProviderForModel(turnInput.modelSelection.model);
          // No task: am would wrap it in launch instructions and give it no receipt.
          yield* fromApi(
            api.createAgent({
              name,
              ...(host ? { host } : {}),
              dir,
              ...(provider ? { provider } : {}),
            }),
          );
        }
        if (!receiptsConfirmed) {
          // The fleet host's own am answers the detail, so its flag covers the am that delivers.
          const remoteDetail = detail ?? Option.getOrUndefined(yield* fromApi(api.getAgent(ref)));
          if (remoteDetail?.messageReceipts !== true)
            return yield* protocolError(
              `Agent Motel on ${host} does not report message receipts, which T3 Code needs to follow a turn. Update am there.`,
            );
          receiptsConfirmed = true;
        }
        const id = yield* fromApi(api.sendMessage(ref, text, "queue"));
        yield* begin(turnInput, {
          observe: false,
          nativeTurnId: `${ref}:${id}`,
          from: head,
          floor: 0,
          // A new agent's detail was not read; the run reads it once.
          ...(detail
            ? { permissions: detail.permissionRequests ? "supported" : "unsupported" }
            : {}),
          messages: [
            { id, mode: "queue", state: "queued", transcriptIndex: null, shownAt: undefined },
          ],
        });
      });
      const begin = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
        init: Pick<ActiveTurn, "observe" | "from" | "floor" | "messages"> & {
          readonly nativeTurnId: string;
          readonly restartCheck?: boolean;
          readonly permissions?: ActiveTurn["permissions"];
        },
      ) {
        const startedAt = yield* DateTime.now;
        const providerTurn: OrchestrationV2ProviderTurn = {
          id: idAllocator.derive.providerTurn({
            driver: AM_PROVIDER,
            nativeTurnId: `${options.instanceId}:${init.nativeTurnId}`,
          }),
          providerThreadId: turnInput.providerThread.id,
          nodeId: turnInput.rootNodeId,
          runAttemptId: turnInput.attemptId,
          nativeTurnRef: nativeRef(init.nativeTurnId, "weak"),
          ordinal: turnInput.providerTurnOrdinal,
          status: "running",
          startedAt,
          completedAt: null,
        };
        const turn: ActiveTurn = {
          input: turnInput,
          providerTurn,
          observe: init.observe,
          from: init.from,
          floor: init.floor,
          restartCheck: init.restartCheck ?? false,
          messages: init.messages,
          cursor: undefined,
          published: new Map(),
          ordinals: new Map(),
          nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
          attentionEpisodes: 0,
          inAttention: false,
          settledAt: undefined,
          settleWakePending: false,
          permissions: init.permissions ?? "unknown",
          attentionSince: undefined,
          done: yield* Deferred.make<void>(),
        };
        thread = turnInput.providerThread;
        active = turn;
        busyAgents.add(agentRef!);
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
        Queue.offerUnsafe(wake, undefined);
      });

      const snapshot = Effect.fnUntraced(function* (current: OrchestrationV2ProviderThread) {
        const transcript = yield* api.transcript(agentRef!, 0).pipe(
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
                      nativeItemId: `${options.instanceId}:${agentRef}:history:${entry.index}`,
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
        // Either way the message joins this run rather than starting one.
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
            // An observing run follows someone else's work, so its steer waits for the agent.
            const mode = turn.observe || turn.messages[0]?.state === "queued" ? "queue" : "now";
            const id = yield* fromApi(api.sendMessage(agentRef!, text, mode));
            turn.settledAt = undefined;
            turn.messages.push({
              id,
              mode,
              state: "queued",
              transcriptIndex: null,
              shownAt: undefined,
            });
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
        // A message am has not delivered is withdrawn, and a delivered one is interrupted by its id,
        // so Esc never stops someone else's work. No provider runs a Stop hook for an Esc; am ends
        // the turn and drains its queue once the prompt is back, but may never see it return, so
        // the run ends here instead of waiting on the receipt.
        interruptTurn: Effect.fn("AmAdapterV2.interruptTurn")(
          function* (args) {
            const turn = active;
            if (
              !turn ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            )
              return yield* protocolError("This Agent Motel turn is no longer active");
            const withdrew = yield* withdraw(turn);
            // One Esc stops the turn; a steer absorbed into it is stopped with it.
            let stopped = false;
            for (const message of turn.messages)
              if (!stopped && message.state === "delivered")
                stopped = yield* interruptDelivered(message);
            const failed = turn.messages.find((message) => message.state === "failed");
            if (
              withdrew ||
              stopped ||
              turn.messages.some((message) => !isSettled(message.state) || message.interrupted)
            )
              return yield* finish(turn, "interrupted");
            // Everything had already ended before Stop reached am.
            if (failed)
              return yield* finish(
                turn,
                "failed",
                failed.error ?? "The Agent Motel agent's session ended before it finished.",
              );
            yield* finish(
              turn,
              turn.messages.every((message) => message.state === "cancelled")
                ? "cancelled"
                : "completed",
            );
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
        respondToRuntimeRequest: Effect.fn("AmAdapterV2.respondToRuntimeRequest")(
          function* (args) {
            const request = requests.get(args.requestId);
            if (!request || request.state === "answering" || request.state === "answered")
              return yield* protocolError(`No open Agent Motel request ${args.requestId}`);
            if (args.decision === undefined)
              return yield* protocolError("An Agent Motel approval needs a decision");
            // A request T3 already settled goes to am anyway: am says why it ended, and T3 shows it.
            if (request.state === "settled") request.state = "pending";
            yield* answerRequest(request, args.decision).pipe(
              Effect.ensuring(Effect.sync(() => forgetFinished(request))),
            );
          },
          (effect, args) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: AM_PROVIDER,
                    requestId: args.requestId,
                    cause,
                  }),
              ),
            ),
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
