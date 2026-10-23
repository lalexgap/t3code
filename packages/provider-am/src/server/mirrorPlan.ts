import { ProviderDriverKind, type OrchestrationV2ProviderRef } from "@t3tools/contracts";

import type { AmFleet, AmFleetRow, AmTranscriptEntry } from "./api.ts";
import { amAgentRef, amTurnPhase } from "./mapping.ts";

/**
 * Reconciles am's fleet against the T3 threads bound to one instance. Pure, so the archive and
 * import rules are testable without am or a server.
 */

/** Imported history and a catch-up after a gap both keep at most this many transcript entries. */
export const AM_MIRROR_ENTRY_LIMIT = 200;
const MESSAGE_TEXT_LIMIT = 16_000;
const DAY_MS = 86_400_000;
/** Marks a conversation head whose thread the mirror archived because am no longer has the agent. */
export const AM_REMOVED_MARK = "am-removed";

/** The thread's conversation head records how far T3 has mirrored the agent's transcript. */
export const amConversationHead = (ref: string, ordinal: number): OrchestrationV2ProviderRef => ({
  driver: ProviderDriverKind.make("am"),
  nativeId: ref,
  strength: "weak",
  ordinal,
});

export const amMirroredThrough = (head: OrchestrationV2ProviderRef | null, ref: string) =>
  head?.nativeId === ref ? head.ordinal : undefined;

export type AmLiveness = "live" | "gone";
const amLiveness = (status: string): AmLiveness =>
  amTurnPhase(status) === "gone" ? "gone" : "live";

export interface MirrorThreadView {
  readonly threadId: string;
  readonly providerThreadId: string;
  /** The agent's am address: its name, or `host:name` on a fleet host. */
  readonly ref: string;
  readonly archived: boolean;
  /** Deleted, or moved to another provider: bound, so never imported again, but left alone. */
  readonly released: boolean;
  /** Set once T3 has mirrored the agent's transcript up to `ordinal`; null for a fresh T3 thread. */
  readonly head: OrchestrationV2ProviderRef | null;
}

export interface MirrorPlanInput {
  /** The instance's Fleet host; empty for the machine running `am serve`. */
  readonly host: string;
  readonly fleet: AmFleet;
  readonly threads: ReadonlyArray<MirrorThreadView>;
  readonly autoImportDays: number;
  readonly nowMs: number;
  /** Each agent's liveness at the previous reconcile; undefined before the first one. */
  readonly previous: ReadonlyMap<string, AmLiveness> | undefined;
}

export interface MirrorWatch {
  readonly thread: MirrorThreadView;
  readonly row: AmFleetRow;
}

export interface MirrorPlan {
  /** False when am cannot read the fleet host, so its missing agents prove nothing. */
  readonly reachable: boolean;
  /** Live agents first, then the most recently active. */
  readonly imports: ReadonlyArray<AmFleetRow & { readonly ref: string }>;
  readonly archive: ReadonlyArray<MirrorThreadView>;
  readonly unarchive: ReadonlyArray<MirrorThreadView>;
  /** Visible threads whose agent am still has, to follow between T3 turns. */
  readonly watch: ReadonlyArray<MirrorWatch>;
  /** Visible threads whose agent am still has, including T3's own that have not run yet. */
  readonly mirrored: number;
  readonly liveness: ReadonlyMap<string, AmLiveness>;
}

const timeMs = (value: string | undefined) => {
  const parsed = value === undefined ? Number.NaN : Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
};

export const planMirror = (input: MirrorPlanInput): MirrorPlan => {
  const { host } = input;
  if (host && input.fleet.unreachable.includes(host))
    return {
      reachable: false,
      imports: [],
      archive: [],
      unarchive: [],
      watch: [],
      mirrored: 0,
      liveness: input.previous ?? new Map(),
    };
  const rows = new Map<string, AmFleetRow>();
  for (const row of input.fleet.rows)
    if ((row.host ?? "") === host) rows.set(amAgentRef(host, row.name), row);
  const liveness = new Map([...rows].map(([ref, row]) => [ref, amLiveness(row.status)] as const));
  const bound = new Set(input.threads.map((thread) => thread.ref));
  const windowMs = input.autoImportDays * DAY_MS;
  const imports = [...rows]
    .filter(
      ([ref, row]) =>
        !bound.has(ref) &&
        (liveness.get(ref) === "live" || input.nowMs - timeMs(row.updatedAt) <= windowMs),
    )
    .map(([ref, row]) => ({ ...row, ref }))
    .toSorted(
      (left, right) =>
        Number(liveness.get(right.ref) === "live") - Number(liveness.get(left.ref) === "live") ||
        timeMs(right.updatedAt) - timeMs(left.updatedAt),
    );
  const archive: Array<MirrorThreadView> = [];
  const unarchive: Array<MirrorThreadView> = [];
  const watch: Array<MirrorWatch> = [];
  let mirrored = 0;
  for (const thread of input.threads) {
    if (thread.released) continue;
    const row = rows.get(thread.ref);
    if (!row) {
      // A thread T3 never mirrored may belong to an agent its first message has yet to start.
      // One fleet listing that leaves an agent out proves nothing; two in a row do.
      const missingBefore = input.previous !== undefined && !input.previous.has(thread.ref);
      if (!thread.archived && thread.head !== null && missingBefore) archive.push(thread);
      continue;
    }
    if (!thread.archived) {
      mirrored++;
      if (thread.head !== null) watch.push({ thread, row });
      continue;
    }
    // A thread the user archived stays archived until its agent comes back: missing from am, or
    // stopped, at the previous reconcile, and running now.
    const cameBack =
      input.previous !== undefined &&
      liveness.get(thread.ref) === "live" &&
      input.previous.get(thread.ref) !== "live";
    if (thread.head?.fingerprint === AM_REMOVED_MARK || cameBack) unarchive.push(thread);
  }
  return { reachable: true, imports, archive, unarchive, watch, mirrored, liveness };
};

const HOME_PREFIX = /^(?:\/(?:Users|home)\/[^/]+|\/root)(?=\/|$)/;

/**
 * am runs a fleet host's agents at the same path under that host's home as this machine's copy,
 * so a fleet host path maps back under this home. Null when it is outside a home directory.
 */
export const fleetHostPathHere = (path: string) => {
  const home = HOME_PREFIX.exec(path);
  if (!home) return null;
  const rest = path.slice(home[0].length);
  // A `..` could climb out of this machine's home.
  return rest.split("/").includes("..") ? null : `~${rest}`;
};

export interface AmWorkspace {
  readonly root: string;
  readonly worktree: { readonly path: string; readonly branch: string | null } | null;
}

/**
 * The project an agent's thread belongs to, and the am worktree it runs in, if any. A fleet host's
 * worktree lives on that host, so its thread is never bound to a checkout here.
 */
export const amWorkspace = (row: AmFleetRow, remote: boolean): AmWorkspace | null => {
  const here = (path: string) => (remote ? fleetHostPathHere(path) : path);
  const dir = here(row.dir);
  if (dir === null) return null;
  if (row.worktreePath && row.repoRoot) {
    const root = here(row.repoRoot);
    if (root !== null)
      return {
        root,
        worktree: remote ? null : { path: row.worktreePath, branch: row.worktreeBranch ?? null },
      };
  }
  return { root: dir, worktree: null };
};

const clip = (text: string) =>
  text.length > MESSAGE_TEXT_LIMIT ? `${text.slice(0, MESSAGE_TEXT_LIMIT)}\n\n[…]` : text;

/** The tail of a transcript as plain history; tool calls are left to `am transcript`. */
export const amHistory = (
  ref: string,
  entries: ReadonlyArray<AmTranscriptEntry>,
  total: number,
) => {
  const omitted = entries[0]?.index ?? total;
  return {
    history: entries.flatMap((entry) =>
      entry.kind !== "tool" && entry.text?.trim()
        ? [{ role: entry.kind, text: clip(entry.text) }]
        : [],
    ),
    notice:
      omitted > 0
        ? `${omitted} earlier transcript ${omitted === 1 ? "entry is" : "entries are"} not shown. Read ${omitted === 1 ? "it" : "them"} with \`am transcript ${ref}\`.`
        : null,
  };
};

export const amMirrorStatus = (input: {
  readonly mirrored: number;
  readonly importing?: number;
  readonly missingWorkspaces: number;
  readonly host: string;
}) => {
  const agents = (count: number) => `${count} ${count === 1 ? "agent" : "agents"}`;
  const importing = input.importing ? ` Importing ${input.importing} more.` : "";
  const missing =
    input.missingWorkspaces > 0
      ? ` ${input.missingWorkspaces === 1 ? "1 agent works in a project" : `${input.missingWorkspaces} agents work in projects`} with no copy on this machine.`
      : "";
  return `Mirroring ${agents(input.mirrored)}${input.host ? ` on ${input.host}` : ""}.${importing}${missing}`;
};
