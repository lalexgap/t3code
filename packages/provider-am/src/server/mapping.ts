import type { OrchestrationV2TurnItemStatus } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import type { AmProvider, AmTranscriptEntry } from "./api.ts";

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
  Schema.fromJsonString(Schema.Struct({ agent: Schema.optional(Schema.String) })),
);

/** Fleet events name the agent they concern; file-watch events name none and may concern any. */
export const fleetEventConcerns = (data: string, agentRef: string) => {
  const event = decodeFleetEvent(data);
  if (event._tag === "None" || event.value.agent === undefined) return true;
  // A remote agent's event may carry its bare name; a stray wake only costs a read.
  return event.value.agent === agentRef || event.value.agent === parseAmAgentRef(agentRef).name;
};
