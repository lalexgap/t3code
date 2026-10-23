import type {
  OrchestrationV2TurnItemStatus,
  ProviderApprovalDecision,
  ProviderApprovalOption,
  ProviderRequestKind,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import type {
  AmPermissionOption,
  AmPermissionRequest,
  AmProvider,
  AmTranscriptEntry,
} from "./api.ts";

/** Let am pick its configured default provider. Never sent to am. */
export const AM_DEFAULT_MODEL = "default";

export const amProviderForModel = (model: string): AmProvider | undefined =>
  model === "claude" || model === "codex" ? model : undefined;

const fnv1a = (value: string) => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index++) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(36).padStart(7, "0").slice(-6);
};

/**
 * am names are `[a-zA-Z0-9_-]+` and double as tmux session names, so keep them short and
 * readable in the fleet list. The hash keeps two threads with the same title apart; the name is
 * persisted as the provider thread's native ref, so later title changes never rename the agent.
 */
export const amAgentName = (threadId: string, title?: string) => {
  const slug = (title ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 24)
    .replace(/-+$/, "");
  return `t3-${slug ? `${slug}-` : ""}${fnv1a(threadId)}`;
};

/** am addresses an agent on a fleet host as `host:name`; names never contain a colon. */
export const amAgentRef = (host: string, name: string) => (host ? `${host}:${name}` : name);

export const parseAmAgentRef = (ref: string) => {
  const colon = ref.indexOf(":");
  return colon === -1
    ? { host: undefined, name: ref }
    : { host: ref.slice(0, colon), name: ref.slice(colon + 1) };
};

export type AmTurnPhase = "running" | "idle" | "attention" | "gone";

/**
 * am statuses: starting/working (busy), idle, waiting (idle at the prompt with background work),
 * needs-attention (blocked on a prompt in its terminal), exited, and dead (tmux session missing).
 */
export const amTurnPhase = (status: string): AmTurnPhase => {
  switch (status) {
    case "idle":
    case "waiting":
      return "idle";
    case "needs-attention":
      return "attention";
    case "exited":
    case "dead":
      return "gone";
    default:
      return "running";
  }
};

const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

const commandFromInput = (input: string) => {
  const parsed = decodeJson(input);
  if (parsed._tag === "None") return input;
  const value = parsed.value;
  if (Array.isArray(value)) return value.map(String).join(" ");
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of ["command", "cmd"]) {
      const field = record[key];
      if (typeof field === "string") return field;
      if (Array.isArray(field)) return field.map(String).join(" ");
    }
  }
  return input;
};

const SHELL_TOOLS = /^(bash|shell|exec_command|local_shell|container\.exec)$/i;

export const amEntryStatus = (
  entry: AmTranscriptEntry,
  turnSettled: boolean,
): OrchestrationV2TurnItemStatus => {
  if (entry.kind !== "tool") return "completed";
  if (entry.output !== undefined) return entry.error ? "failed" : "completed";
  if (entry.error) return "failed";
  // am never reports a result for some tools; a settled turn settles them too.
  return turnSettled ? "completed" : "running";
};

/** A tool entry stays open until am attaches its output, so it must be re-read. */
export const isEntryOpen = (entry: AmTranscriptEntry) =>
  entry.kind === "tool" && entry.output === undefined && !entry.error;

export const amToolPresentation = (entry: AmTranscriptEntry) => {
  const name = entry.name?.trim() || "tool";
  const input = entry.input ?? "";
  if (SHELL_TOOLS.test(name)) {
    return {
      type: "command_execution" as const,
      title: name,
      input: commandFromInput(input),
      ...(entry.output === undefined ? {} : { output: entry.output }),
      ...(entry.error ? { outputIndicatesFailure: true } : {}),
    };
  }
  const parsed = decodeJson(input);
  return {
    type: "dynamic_tool" as const,
    title: name,
    toolName: name,
    input: parsed._tag === "Some" ? parsed.value : input,
    ...(entry.output === undefined ? {} : { output: entry.output }),
  };
};

export const sameEntry = (left: AmTranscriptEntry, right: AmTranscriptEntry) =>
  left.index === right.index &&
  left.kind === right.kind &&
  left.text === right.text &&
  left.name === right.name &&
  left.input === right.input &&
  left.output === right.output &&
  left.error === right.error;

const decodeFleetEvent = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({ agent: Schema.optional(Schema.String), event: Schema.optional(Schema.String) }),
  ),
);

/** The agent a fleet event says raised a permission prompt. */
export const fleetEventPromptedAgent = (data: string) => {
  const event = decodeFleetEvent(data);
  return event._tag === "Some" && event.value.event === "permission-request"
    ? event.value.agent
    : undefined;
};

/** Fleet events name the agent they concern; file-watch events name none and may concern any. */
export const fleetEventConcerns = (data: string, agentRef: string) => {
  // `/api/events` carries only the local daemon's events, so a fleet host's agent never appears
  // there, and waking on local churn would cost an ssh round trip each time.
  if (parseAmAgentRef(agentRef).host !== undefined) return false;
  const event = decodeFleetEvent(data);
  return event._tag === "None" || event.value.agent === undefined || event.value.agent === agentRef;
};

// T3's approval card keys each choice by a decision, so each am option needs one of its kind's
// slots. Codex names its options after the same decisions; Claude offers one allow-always per
// suggestion it makes.
const DECISION_SLOTS = new Map<string, ReadonlyArray<ProviderApprovalDecision>>([
  ["allow", ["accept"]],
  ["allow_always", ["acceptForSession", "acceptAlways"]],
  ["deny", ["decline", "cancel"]],
]);

/**
 * am's options in am's order and with am's labels, each on a decision of its kind. Options beyond
 * their kind's slots, or of a kind T3 does not know, are left to the agent's terminal.
 */
export const amApprovalChoices = (options: ReadonlyArray<AmPermissionOption>) => {
  const assigned = new Map<AmPermissionOption, ProviderApprovalDecision>();
  const taken = new Set<ProviderApprovalDecision>();
  const take = (option: AmPermissionOption, decision: ProviderApprovalDecision) => {
    assigned.set(option, decision);
    taken.add(decision);
  };
  for (const option of options) {
    const named = DECISION_SLOTS.get(option.kind)?.find((decision) => decision === option.id);
    if (named && !taken.has(named)) take(option, named);
  }
  for (const option of options) {
    if (assigned.has(option)) continue;
    const free = DECISION_SLOTS.get(option.kind)?.find((decision) => !taken.has(decision));
    if (free) take(option, free);
  }
  const shown: Array<ProviderApprovalOption> = [];
  const optionIds = new Map<ProviderApprovalDecision, string>();
  for (const option of options) {
    const decision = assigned.get(option);
    if (!decision) continue;
    shown.push({ decision, label: option.label.trim() || option.id });
    optionIds.set(decision, option.id);
  }
  return { options: shown, optionIds, hidden: options.length - shown.length };
};

/**
 * The am option a T3 decision answers with. T3 itself declines with `cancel`, which falls back to
 * the agent's own decline when it offers no second one.
 */
export const amOptionForDecision = (
  optionIds: ReadonlyMap<ProviderApprovalDecision, string>,
  decision: ProviderApprovalDecision,
) =>
  optionIds.get(decision) ??
  (decision === "cancel"
    ? optionIds.get("decline")
    : decision === "decline"
      ? optionIds.get("cancel")
      : undefined);

const FILE_CHANGE_TOOLS = /^(edit|multiedit|write|notebookedit|apply_patch)$/i;
const FILE_READ_TOOLS = /^(read|glob|grep|ls|notebookread)$/i;

export const amPermissionRequestKind = (tool: string): ProviderRequestKind =>
  SHELL_TOOLS.test(tool)
    ? "command"
    : FILE_CHANGE_TOOLS.test(tool)
      ? "file-change"
      : FILE_READ_TOOLS.test(tool)
        ? "file-read"
        : "permission";

const PROMPT_INPUT_LIMIT = 4_000;

/** am's one-line summary, then as much of the tool's input as a card can show. */
export const amPermissionPrompt = (request: AmPermissionRequest) => {
  const summary = request.summary.trim() || request.tool;
  if (request.input === undefined || request.input === null) return summary;
  const input =
    typeof request.input === "string" ? request.input : (JSON.stringify(request.input) ?? "");
  if (input === "" || input === "{}") return summary;
  return `${summary}\n\n${input.length <= PROMPT_INPUT_LIMIT ? input : `${input.slice(0, PROMPT_INPUT_LIMIT - 3)}...`}`;
};
