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
/**
 * What the text of one adoption or one transcript read may add to its events, serialized as JSON.
 * Either must stay well inside a live subscriber's 8MB buffer, envelopes included.
 */
export const AM_EMITTED_BYTE_LIMIT = 1_000_000;
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
  /** Agents the previous listing left out, their liveness carried over from the one before. */
  readonly previousOmitted?: ReadonlySet<string>;
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
  readonly omitted: ReadonlySet<string>;
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
      omitted: input.previousOmitted ?? new Set(),
    };
  const rows = new Map<string, AmFleetRow>();
  for (const row of input.fleet.rows)
    if ((row.host ?? "") === host) rows.set(amAgentRef(host, row.name), row);
  // An empty listing while threads are bound is likelier a failed read than every agent removed,
  // so it confirms nothing and leaves the last known state as it was.
  if (rows.size === 0 && input.threads.some((thread) => !thread.released))
    return {
      reachable: true,
      imports: [],
      archive: [],
      unarchive: [],
      watch: [],
      mirrored: input.threads.filter((thread) => !thread.released && !thread.archived).length,
      liveness: input.previous ?? new Map(),
      omitted: input.previousOmitted ?? new Set(),
    };
  const liveness = new Map([...rows].map(([ref, row]) => [ref, amLiveness(row.status)] as const));
  const previousOmitted = input.previousOmitted ?? new Set<string>();
  // One listing that leaves an agent out proves nothing, so it keeps its last liveness for one
  // more listing: returning then is not coming back.
  const omitted = new Set<string>();
  for (const [ref, last] of input.previous ?? [])
    if (!rows.has(ref) && !previousOmitted.has(ref)) {
      liveness.set(ref, last);
      omitted.add(ref);
    }
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
      // Two listings in a row that leave an agent out mean am removed it.
      const missingBefore =
        input.previous !== undefined &&
        (!input.previous.has(thread.ref) || previousOmitted.has(thread.ref));
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
  return { reachable: true, imports, archive, unarchive, watch, mirrored, liveness, omitted };
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

/** Why an agent's project root is never made a T3 project. */
export type AmUnsafeRootReason = "filesystem-root" | "home" | "non-git-outside-home";

export type AmWorkspaceResult =
  | ({ readonly _tag: "Workspace" } & AmWorkspace)
  | { readonly _tag: "Unsafe"; readonly reason: AmUnsafeRootReason }
  /** A fleet host's agent whose directory has no path under this machine's home. */
  | { readonly _tag: "NoCopy" };

/** The repository a directory on this machine is in. */
export interface AmGitRepo {
  /** The main repository's root. */
  readonly root: string;
  /** The linked worktree the directory is in, if it is not the main worktree. */
  readonly worktree: string | null;
}

export interface AmWorkspaceContext {
  /** True for a fleet host's agent, whose paths are on that host. */
  readonly remote: boolean;
  /** This machine's home directory. */
  readonly home: string;
  /** The home directory with its symlinks resolved, when that differs; am may record either. */
  readonly realHome?: string;
  /** The repository of the agent's directory on this machine, if it is in one. */
  readonly gitRoot: AmGitRepo | null;
  /**
   * A local agent's candidate roots with their symlinks resolved, keyed by the absolute path as
   * spelled. A root missing here is checked as spelled.
   */
  readonly realPaths?: ReadonlyMap<string, string>;
}

const trimSlashes = (path: string) => (path.length > 1 ? path.replace(/\/+$/, "") : path);

/** An absolute path with `.`, `..`, repeated and trailing slashes resolved. */
const normalizePath = (path: string) => {
  const parts: Array<string> = [];
  for (const part of path.split("/"))
    if (part === "..") parts.pop();
    else if (part !== "" && part !== ".") parts.push(part);
  return `/${parts.join("/")}`;
};

/** An absolute or `~`-relative path, absolute on this machine. */
export const amLocalPath = (path: string, home: string) =>
  path === "~" || path.startsWith("~/") ? `${trimSlashes(home)}${path.slice(1)}` : path;

const isUnder = (path: string, dir: string) => dir !== "/" && path.startsWith(`${dir}/`);

/**
 * A project root T3 may create or bind, as T3 should spell it, or why not: never the filesystem
 * root or the home directory, and outside home only a git repository's root. A local root is
 * checked both as bound and where its symlinks lead, since a link can point anywhere.
 */
const amSafeRoot = (
  root: string,
  git: boolean,
  context: AmWorkspaceContext,
): { readonly root: string } | { readonly reason: AmUnsafeRootReason } => {
  const homes = [context.home, context.realHome ?? context.home].map(normalizePath);
  const unsafe = (path: string): AmUnsafeRootReason | null => {
    if (path === "/") return "filesystem-root";
    if (homes.includes(path)) return "home";
    return git || homes.some((home) => isUnder(path, home)) ? null : "non-git-outside-home";
  };
  const local = amLocalPath(root, context.home);
  const real = context.remote ? undefined : context.realPaths?.get(local);
  // A `..` after a symlink climbs from where the link points, which only the real path knows.
  const bound = real !== undefined && local.split("/").includes("..") ? real : normalizePath(local);
  const reason = (real === undefined ? null : unsafe(real)) ?? unsafe(bound);
  if (reason) return { reason };
  return { root: context.remote ? root : bound };
};

/**
 * The project an agent's thread belongs to, and the worktree it runs in, if any: its am
 * worktree's repository, else the main repository its directory is in, else the directory. A
 * fleet host's worktree lives on that host, so its thread is never bound to a checkout here.
 */
export const amWorkspace = (row: AmFleetRow, context: AmWorkspaceContext): AmWorkspaceResult => {
  const { remote, home } = context;
  const here = (path: string) => (remote ? fleetHostPathHere(path) : path);
  const checked = (
    root: string,
    git: boolean,
    worktree: AmWorkspace["worktree"],
  ): AmWorkspaceResult => {
    const safe = amSafeRoot(root, git, context);
    return "reason" in safe
      ? { _tag: "Unsafe", reason: safe.reason }
      : { _tag: "Workspace", root: safe.root, worktree };
  };
  if (remote && normalizePath(row.dir) === "/")
    return { _tag: "Unsafe", reason: "filesystem-root" };
  const dir = here(row.dir);
  if (dir === null) return { _tag: "NoCopy" };
  const repoRoot = row.repoRoot === undefined ? null : here(row.repoRoot);
  if (row.worktreePath && repoRoot !== null)
    return checked(
      repoRoot,
      true,
      remote ? null : { path: row.worktreePath, branch: row.worktreeBranch ?? null },
    );
  const repo = context.gitRoot;
  const gitRoot =
    repo === null
      ? null
      : remote
        ? (homeRelative(repo.root, home) ??
          (context.realHome ? homeRelative(repo.root, context.realHome) : null) ??
          repo.root)
        : repo.root;
  // A repository rooted at home, such as dotfiles, groups nothing: the agent keeps its directory.
  const root = [repoRoot, gitRoot].find(
    (candidate): candidate is string =>
      candidate !== null && "root" in amSafeRoot(candidate, true, context),
  );
  if (root === undefined) return checked(dir, false, null);
  // A linked worktree joins its main repository's project, as an am worktree does.
  const worktree =
    !remote && repo?.worktree && root === gitRoot ? { path: repo.worktree, branch: null } : null;
  return checked(root, true, worktree);
};

/**
 * The toplevel of the repository `path` is in, from `git rev-parse --show-prefix`, spelled as
 * `path` is so a symlinked home still matches. Null when the prefix does not end `path`.
 */
export const amGitToplevel = (path: string, prefix: string) => {
  const dir = trimSlashes(path);
  const rest = trimSlashes(prefix.trim());
  if (rest === "") return dir;
  return dir.endsWith(`/${rest}`) ? dir.slice(0, -rest.length - 1) || "/" : null;
};

/**
 * The repository `path` is in, from `git rev-parse --show-prefix --path-format=absolute
 * --git-dir --git-common-dir`. A linked worktree's git dir differs from the common dir, whose
 * parent is the main worktree. A bare repository's worktree, or an unexpected answer, keeps
 * the toplevel as the root.
 */
export const amGitRepo = (path: string, stdout: string): AmGitRepo | null => {
  const [prefix = "", gitDir = "", commonDir = ""] = stdout.split("\n");
  const toplevel = amGitToplevel(path, prefix);
  if (toplevel === null) return null;
  const common = trimSlashes(commonDir.trim());
  const linked = gitDir.trim() !== "" && trimSlashes(gitDir.trim()) !== common;
  if (!linked || !common.startsWith("/") || !common.endsWith("/.git"))
    return { root: toplevel, worktree: null };
  return { root: common.slice(0, -"/.git".length) || "/", worktree: toplevel };
};

/** A path under a home directory as `~`-relative, as a fleet host's mapped paths are. */
const homeRelative = (path: string, home: string) => {
  const homeDir = trimSlashes(home);
  return path === homeDir
    ? "~"
    : path.startsWith(`${homeDir}/`)
      ? `~${path.slice(homeDir.length)}`
      : null;
};

const clip = (text: string) =>
  text.length > MESSAGE_TEXT_LIMIT ? `${text.slice(0, MESSAGE_TEXT_LIMIT)}\n\n[…]` : text;

/** A transcript entry with its text and tool fields clipped as history clips messages. */
export const amClipEntry = (entry: AmTranscriptEntry): AmTranscriptEntry => ({
  ...entry,
  ...(entry.text === undefined ? {} : { text: clip(entry.text) }),
  ...(entry.input === undefined ? {} : { input: clip(entry.input) }),
  ...(entry.output === undefined ? {} : { output: clip(entry.output) }),
});

const utf8 = new TextEncoder();

/** A field's size once serialized: JSON escapes control characters to six. */
const serializedBytes = (value: string | undefined) =>
  value === undefined ? 0 : utf8.encode(JSON.stringify(value)).length;

/**
 * What an entry's text and tool fields add to the events a run emits for it, serialized. A reply
 * is sent as a message and as a turn item.
 */
export const amEntryEmittedBytes = (entry: AmTranscriptEntry) =>
  entry.kind === "assistant"
    ? 2 * serializedBytes(entry.text)
    : serializedBytes(entry.text) +
      serializedBytes(entry.name) +
      serializedBytes(entry.input) +
      serializedBytes(entry.output);

/**
 * The newest entries of a transcript tail as plain history, within the entry and text budgets;
 * tool calls are left to `am transcript`. The tail may hold more than the entry limit when the
 * transcript grew while it was read.
 */
export const amHistory = (
  ref: string,
  entries: ReadonlyArray<AmTranscriptEntry>,
  total: number,
) => {
  const tail = entries.slice(-AM_MIRROR_ENTRY_LIMIT);
  let budget = AM_EMITTED_BYTE_LIMIT;
  let start = tail.length;
  for (let index = tail.length - 1; index >= 0; index--) {
    const text = tail[index]!.kind === "tool" ? undefined : tail[index]!.text?.trim();
    if (text) {
      // Each history entry is written as a message and as a turn item.
      const bytes = 2 * serializedBytes(clip(tail[index]!.text!));
      if (bytes > budget) break;
      budget -= bytes;
    }
    start = index;
  }
  const kept = tail.slice(start);
  const omitted = kept[0]?.index ?? total;
  return {
    history: kept.flatMap((entry) =>
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

const UNSAFE_ROOT_TEXT: Record<AmUnsafeRootReason, string> = {
  "filesystem-root": "at the filesystem root",
  home: "at your home directory",
  "non-git-outside-home": "outside your home directory that is not a git repository",
};

export const amMirrorStatus = (input: {
  readonly mirrored: number;
  readonly importing?: number;
  readonly missingWorkspaces: number;
  /** Agents left out because their project root is unsafe, by reason. */
  readonly skipped?: ReadonlyMap<AmUnsafeRootReason, number>;
  readonly host: string;
}) => {
  const agents = (count: number) => `${count} ${count === 1 ? "agent" : "agents"}`;
  const importing = input.importing ? ` Importing ${input.importing} more.` : "";
  const missing =
    input.missingWorkspaces > 0
      ? ` ${input.missingWorkspaces === 1 ? "1 agent works in a project" : `${input.missingWorkspaces} agents work in projects`} with no copy on this machine.`
      : "";
  const reasons = [...(input.skipped ?? [])].filter(([, count]) => count > 0);
  const skippedCount = reasons.reduce((sum, [, count]) => sum + count, 0);
  const skipped =
    skippedCount > 0
      ? ` Skipped ${agents(skippedCount)} with a project ${reasons
          .map(
            ([reason, count]) =>
              `${UNSAFE_ROOT_TEXT[reason]}${reasons.length > 1 ? ` (${count})` : ""}`,
          )
          .join(" or ")}.`
      : "";
  return `Mirroring ${agents(input.mirrored)}${input.host ? ` on ${input.host}` : ""}.${importing}${missing}${skipped}`;
};
