import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import type { AmFleet, AmFleetRow, AmTranscriptEntry } from "./api.ts";
import {
  AM_MIRROR_ENTRY_LIMIT,
  AM_REMOVED_MARK,
  amConversationHead,
  amClipEntry,
  amGitRepo,
  amGitToplevel,
  amHistory,
  amMirrorStatus,
  amRepoSpelledLike,
  amWorkspace,
  fleetHostPathHere,
  planMirror,
  type MirrorPlanInput,
  type MirrorThreadView,
} from "./mirrorPlan.ts";

const NOW = Date.parse("2026-10-22T12:00:00.000Z");
const daysAgo = (days: number) => DateTime.formatIso(DateTime.makeUnsafe(NOW - days * 86_400_000));

const row = (name: string, patch: Partial<AmFleetRow> = {}): AmFleetRow => ({
  name,
  status: "idle",
  provider: "claude",
  dir: `/home/me/code/${name}`,
  updatedAt: daysAgo(0),
  ...patch,
});

const thread = (ref: string, patch: Partial<MirrorThreadView> = {}): MirrorThreadView => ({
  threadId: `thread-${ref}`,
  providerThreadId: `provider-thread-${ref}`,
  ref,
  archived: false,
  released: false,
  head: amConversationHead(ref, 4),
  ...patch,
});

const plan = (
  rows: ReadonlyArray<AmFleetRow>,
  threads: ReadonlyArray<MirrorThreadView> = [],
  patch: Partial<MirrorPlanInput> = {},
) => {
  const fleet: AmFleet = { rows, unreachable: [] };
  return planMirror({
    host: "",
    fleet,
    threads,
    autoImportDays: 7,
    nowMs: NOW,
    previous: new Map(),
    ...patch,
  });
};

const refs = (items: ReadonlyArray<{ readonly ref: string }>) => items.map((item) => item.ref);

describe("planMirror", () => {
  it("imports running agents and stopped ones active within the window", () => {
    const result = plan([
      row("working", { status: "working", updatedAt: daysAgo(40) }),
      row("idle-long-ago", { status: "idle", updatedAt: daysAgo(40) }),
      row("stopped-recently", { status: "exited", updatedAt: daysAgo(6) }),
      row("stopped-long-ago", { status: "exited", updatedAt: daysAgo(8) }),
      row("dead-recently", { status: "dead", updatedAt: daysAgo(1) }),
    ]);
    assert.deepStrictEqual(refs(result.imports), [
      "working",
      "idle-long-ago",
      "dead-recently",
      "stopped-recently",
    ]);
  });

  it("never imports an agent a thread already owns, whatever that thread's state", () => {
    const result = plan(
      [row("t3-fix-login-4k2j9a"), row("archived"), row("deleted"), row("fresh")],
      [
        thread("t3-fix-login-4k2j9a", { head: null }),
        thread("archived", { archived: true }),
        thread("deleted", { released: true }),
      ],
    );
    assert.deepStrictEqual(refs(result.imports), ["fresh"]);
  });

  it("archives the thread of an agent am removed, only once T3 has mirrored it", () => {
    const result = plan(
      [row("kept")],
      [
        thread("kept"),
        thread("removed"),
        // T3's own thread before its first message has started the agent.
        thread("t3-new-abc123", { head: null }),
        thread("already-archived", { archived: true }),
        thread("deleted", { released: true }),
      ],
    );
    assert.deepStrictEqual(refs(result.archive), ["removed"]);
    assert.deepStrictEqual(refs(result.watch.map((watch) => watch.thread)), ["kept"]);
    assert.strictEqual(result.mirrored, 1);
  });

  it("archives only an agent missing from two fleet listings in a row", () => {
    const threads = [thread("kept"), thread("flaky")];
    // Nothing is known before the first reconcile.
    assert.deepStrictEqual(plan([], threads, { previous: undefined }).archive, []);
    // An empty or partial listing right after a full one archives nothing.
    const full = new Map([
      ["kept", "live" as const],
      ["flaky", "live" as const],
    ]);
    const partial = plan([row("kept")], threads, { previous: full });
    assert.deepStrictEqual(partial.archive, []);
    assert.deepStrictEqual(plan([], threads, { previous: full }).archive, []);
    // Still missing at the next one: am really removed it.
    const next = { previous: partial.liveness, previousOmitted: partial.omitted };
    assert.deepStrictEqual(refs(plan([row("kept")], threads, next).archive), ["flaky"]);
  });

  it("treats an empty listing as unconfirmed while threads are bound", () => {
    const threads = [thread("kept"), thread("other")];
    const previous = new Map([["kept", "live" as const]]);
    const previousOmitted = new Set(["other"]);
    for (let listing = 0; listing < 2; listing++) {
      const empty = plan([], threads, { previous, previousOmitted });
      assert.deepStrictEqual(empty.archive, []);
      assert.strictEqual(empty.liveness, previous);
      assert.strictEqual(empty.omitted, previousOmitted);
      assert.strictEqual(empty.mirrored, 2);
    }
  });

  it("does not take an agent one listing left out for one that came back", () => {
    const archived = [thread("busy", { archived: true })];
    const omitted = plan([], archived, { previous: new Map([["busy", "live"]]) });
    assert.strictEqual(omitted.liveness.get("busy"), "live");
    const back = plan([row("busy", { status: "working" })], archived, {
      previous: omitted.liveness,
      previousOmitted: omitted.omitted,
    });
    assert.deepStrictEqual(back.unarchive, []);
  });

  it("leaves a thread alone when its stopped agent ages out of the window", () => {
    const result = plan(
      [row("old", { status: "exited", updatedAt: daysAgo(90) })],
      [thread("old")],
    );
    assert.deepStrictEqual(result.archive, []);
    assert.deepStrictEqual(result.unarchive, []);
    assert.deepStrictEqual(refs(result.watch.map((watch) => watch.thread)), ["old"]);
  });

  it("restores a thread archived for a removed agent once the agent is back", () => {
    const head = { ...amConversationHead("restored", 4), fingerprint: AM_REMOVED_MARK };
    const result = plan(
      [row("restored", { status: "exited" })],
      [thread("restored", { archived: true, head })],
      { previous: undefined },
    );
    assert.deepStrictEqual(refs(result.unarchive), ["restored"]);
  });

  it("keeps a thread the user archived until its agent stops and comes back", () => {
    const archived = [thread("busy", { archived: true }), thread("resumed", { archived: true })];
    const rows = [row("busy", { status: "working" }), row("resumed", { status: "working" })];
    const still = plan(rows, archived, {
      previous: new Map([
        ["busy", "live"],
        ["resumed", "gone"],
      ]),
    });
    assert.deepStrictEqual(refs(still.unarchive), ["resumed"]);
    // Nothing is known about the agent before the first reconcile, so nothing counts as back.
    assert.deepStrictEqual(plan(rows, archived, { previous: undefined }).unarchive, []);
    // An agent missing at the last reconcile and running now came back too.
    assert.deepStrictEqual(
      refs(plan([row("busy")], [thread("busy", { archived: true })]).unarchive),
      ["busy"],
    );
  });

  it("follows only the instance's fleet host, by `host:name`", () => {
    const rows = [row("here"), row("there", { host: "laptop" }), row("other", { host: "desk" })];
    const remote = plan(rows, [thread("laptop:gone")], { host: "laptop" });
    assert.deepStrictEqual(refs(remote.imports), ["laptop:there"]);
    assert.deepStrictEqual(refs(remote.archive), ["laptop:gone"]);
    assert.deepStrictEqual(refs(plan(rows).imports), ["here"]);
  });

  it("changes nothing for a fleet host am cannot reach", () => {
    const previous = new Map([["laptop:gone", "live" as const]]);
    const result = planMirror({
      host: "laptop",
      fleet: { rows: [], unreachable: ["laptop"] },
      threads: [thread("laptop:gone")],
      autoImportDays: 7,
      nowMs: NOW,
      previous,
    });
    assert.isFalse(result.reachable);
    assert.deepStrictEqual(result.archive, []);
    assert.strictEqual(result.liveness, previous);
  });
});

const HOME = "/home/me";
const local = { remote: false, home: HOME, gitRoot: null } as const;
const remote = { remote: true, home: HOME, gitRoot: null } as const;

describe("amWorkspace", () => {
  it("puts an am worktree agent in its repository's project, bound to the worktree", () => {
    assert.deepStrictEqual(
      amWorkspace(
        row("wt", {
          dir: "/home/me/.agent-manager/worktrees/app/wt",
          worktreePath: "/home/me/.agent-manager/worktrees/app/wt",
          worktreeBranch: "am/wt",
          repoRoot: "/home/me/code/app",
        }),
        local,
      ),
      {
        _tag: "Workspace",
        root: "/home/me/code/app",
        worktree: { path: "/home/me/.agent-manager/worktrees/app/wt", branch: "am/wt" },
      },
    );
    // An agent working in a subdirectory still binds the worktree's root.
    const nested = amWorkspace(
      row("nested", {
        dir: "/home/me/repo-wt/packages/ui",
        worktreePath: "/home/me/repo-wt",
        repoRoot: "/home/me/repo",
      }),
      { ...local, gitRoot: { root: "/home/me/repo-wt", worktree: null } },
    );
    assert.deepStrictEqual(nested, {
      _tag: "Workspace",
      root: "/home/me/repo",
      worktree: { path: "/home/me/repo-wt", branch: null },
    });
    assert.deepStrictEqual(amWorkspace(row("plain", { dir: "/home/me/notes" }), local), {
      _tag: "Workspace",
      root: "/home/me/notes",
      worktree: null,
    });
  });

  it("groups an agent in a repository's subdirectory into the repository's project", () => {
    assert.deepStrictEqual(
      amWorkspace(row("ui", { dir: "/home/me/code/app/packages/ui" }), {
        ...local,
        gitRoot: { root: "/home/me/code/app", worktree: null },
      }),
      { _tag: "Workspace", root: "/home/me/code/app", worktree: null },
    );
    // am's repository root wins over the toplevel git reports.
    assert.deepStrictEqual(
      amWorkspace(
        row("ui", { dir: "/home/me/code/app/packages/ui", repoRoot: "/home/me/code/app" }),
        local,
      ),
      { _tag: "Workspace", root: "/home/me/code/app", worktree: null },
    );
    // A fleet host's agent groups by the repository of this machine's copy.
    assert.deepStrictEqual(
      amWorkspace(row("ui", { host: "laptop", dir: "/Users/alex/code/app/packages/ui" }), {
        ...remote,
        gitRoot: { root: "/home/me/code/app", worktree: null },
      }),
      { _tag: "Workspace", root: "~/code/app", worktree: null },
    );
    // A repository at home, such as dotfiles, leaves the agent in its own directory.
    assert.deepStrictEqual(
      amWorkspace(row("notes", { dir: "/home/me/notes" }), {
        ...local,
        gitRoot: { root: HOME, worktree: null },
      }),
      { _tag: "Workspace", root: "/home/me/notes", worktree: null },
    );
  });

  it("never makes a project of the filesystem root, the home directory, or a non-git folder outside it", () => {
    const unsafe = (patch: Partial<AmFleetRow>, context: Parameters<typeof amWorkspace>[1]) =>
      amWorkspace(row("agent", patch), context);
    const repo = (root: string) => ({ root, worktree: null });
    assert.deepStrictEqual(unsafe({ dir: "/" }, local), {
      _tag: "Unsafe",
      reason: "filesystem-root",
    });
    assert.deepStrictEqual(unsafe({ dir: "/home/me" }, local), { _tag: "Unsafe", reason: "home" });
    assert.deepStrictEqual(unsafe({ dir: "/home/me/" }, local), { _tag: "Unsafe", reason: "home" });
    assert.deepStrictEqual(unsafe({ dir: "/home/me/code/.." }, local), {
      _tag: "Unsafe",
      reason: "home",
    });
    assert.deepStrictEqual(unsafe({ dir: "/srv/app" }, local), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    assert.deepStrictEqual(unsafe({ dir: "/home/meme/app" }, local), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    // A git repository's root is allowed anywhere, from git or from am.
    assert.deepStrictEqual(
      unsafe(
        { dir: "/mnt/fastdata/code/producthunt/app" },
        { ...local, gitRoot: repo("/mnt/fastdata/code/producthunt") },
      ),
      { _tag: "Workspace", root: "/mnt/fastdata/code/producthunt", worktree: null },
    );
    assert.deepStrictEqual(unsafe({ dir: "/srv/app/sub", repoRoot: "/srv/app" }, local), {
      _tag: "Workspace",
      root: "/srv/app",
      worktree: null,
    });
    // But never one at the filesystem root or home, in either spelling.
    const linked = { ...local, realHome: "/data/me" };
    assert.deepStrictEqual(unsafe({ dir: "/etc" }, { ...local, gitRoot: repo("/") }), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    assert.deepStrictEqual(unsafe({ dir: "/data/me" }, { ...linked, gitRoot: repo("/data/me") }), {
      _tag: "Unsafe",
      reason: "home",
    });
    assert.deepStrictEqual(unsafe({ dir: "/home/me" }, { ...linked, gitRoot: repo(HOME) }), {
      _tag: "Unsafe",
      reason: "home",
    });
    // An am worktree of a repository at home is skipped, not moved to its directory.
    assert.deepStrictEqual(
      unsafe({ dir: "/home/me/wt", worktreePath: "/home/me/wt", repoRoot: "/home/me" }, local),
      { _tag: "Unsafe", reason: "home" },
    );
    assert.deepStrictEqual(
      unsafe({ dir: "/home/me/wt", worktreePath: "/home/me/wt", repoRoot: "/" }, local),
      { _tag: "Unsafe", reason: "filesystem-root" },
    );
    // The same rule holds for a fleet host's agents once mapped under this home.
    assert.deepStrictEqual(unsafe({ host: "laptop", dir: "/Users/alex" }, remote), {
      _tag: "Unsafe",
      reason: "home",
    });
    assert.deepStrictEqual(unsafe({ host: "laptop", dir: "/" }, remote), {
      _tag: "Unsafe",
      reason: "filesystem-root",
    });
    assert.deepStrictEqual(
      unsafe({ host: "laptop", dir: "/Users/alex/code/app" }, { ...remote, gitRoot: repo(HOME) }),
      { _tag: "Workspace", root: "~/code/app", worktree: null },
    );
    // This machine's copy may belong to a repository outside home.
    assert.deepStrictEqual(
      unsafe(
        { host: "laptop", dir: "/Users/alex/code/producthunt" },
        { ...remote, gitRoot: repo("/mnt/fastdata/code/producthunt") },
      ),
      { _tag: "Workspace", root: "/mnt/fastdata/code/producthunt", worktree: null },
    );
    assert.deepStrictEqual(
      unsafe({ host: "laptop", dir: "/Users/alex/code/app" }, { ...remote, gitRoot: repo("/") }),
      { _tag: "Workspace", root: "~/code/app", worktree: null },
    );
    // A fleet host path with no place under this home has no copy here.
    assert.deepStrictEqual(unsafe({ host: "laptop", dir: "/srv/app" }, remote), {
      _tag: "NoCopy",
    });
  });

  it("spells a linked worktree's main root under the agent's own home", () => {
    const linked = { ...local, realHome: "/data/me" };
    const main = amWorkspace(row("main", { dir: "/home/me/app/ui" }), {
      ...linked,
      gitRoot: { root: "/home/me/app", worktree: null },
    });
    const review = amWorkspace(row("review", { dir: "/home/me/wt" }), {
      ...linked,
      gitRoot: { root: "/data/me/app", worktree: "/home/me/wt" },
    });
    assert.deepStrictEqual(main, { _tag: "Workspace", root: "/home/me/app", worktree: null });
    assert.deepStrictEqual(review, {
      _tag: "Workspace",
      root: "/home/me/app",
      worktree: { path: "/home/me/wt", branch: null },
    });
    // So is a repoRoot am spelled under the real home.
    assert.deepStrictEqual(
      amWorkspace(row("review", { dir: "/home/me/wt", repoRoot: "/data/me/app" }), {
        ...linked,
        gitRoot: { root: "/data/me/app", worktree: "/home/me/wt" },
      }),
      review,
    );
    // An agent spelled by the real home keeps the real spelling.
    assert.deepStrictEqual(
      amWorkspace(row("review", { dir: "/data/me/wt" }), {
        ...linked,
        gitRoot: { root: "/home/me/app", worktree: "/data/me/wt" },
      }),
      { _tag: "Workspace", root: "/data/me/app", worktree: { path: "/data/me/wt", branch: null } },
    );
    // A root outside both homes is left as git spelled it.
    assert.deepStrictEqual(
      amRepoSpelledLike({ root: "/srv/app", worktree: "/home/me/wt" }, "/home/me/wt", linked),
      { root: "/srv/app", worktree: "/home/me/wt" },
    );
  });

  it("groups an agent in a linked worktree into its main repository's project", () => {
    const repo = {
      root: "/home/me/code/app",
      worktree: "/home/me/.agent-manager/worktrees/app/review",
    };
    assert.deepStrictEqual(
      amWorkspace(row("review", { dir: "/home/me/.agent-manager/worktrees/app/review/ui" }), {
        ...local,
        gitRoot: repo,
      }),
      {
        _tag: "Workspace",
        root: "/home/me/code/app",
        worktree: { path: "/home/me/.agent-manager/worktrees/app/review", branch: null },
      },
    );
    // A main repository outside home still groups its worktrees.
    assert.deepStrictEqual(
      amWorkspace(row("review", { dir: "/home/me/wt" }), {
        ...local,
        gitRoot: { root: "/mnt/fastdata/code/producthunt", worktree: "/home/me/wt" },
      }),
      {
        _tag: "Workspace",
        root: "/mnt/fastdata/code/producthunt",
        worktree: { path: "/home/me/wt", branch: null },
      },
    );
    // A fleet host's worktree is on that host, so its thread gets only the project.
    assert.deepStrictEqual(
      amWorkspace(row("review", { host: "laptop", dir: "/Users/alex/wt" }), {
        ...remote,
        gitRoot: { root: "/home/me/code/app", worktree: "/home/me/wt" },
      }),
      { _tag: "Workspace", root: "~/code/app", worktree: null },
    );
    // A main repository at home groups nothing: the agent keeps its directory.
    assert.deepStrictEqual(
      amWorkspace(row("dots", { dir: "/home/me/wt" }), {
        ...local,
        gitRoot: { root: HOME, worktree: "/home/me/wt" },
      }),
      { _tag: "Workspace", root: "/home/me/wt", worktree: null },
    );
  });

  it("accepts a path under either spelling of a symlinked home", () => {
    const linked = { ...local, realHome: "/data/me" };
    assert.deepStrictEqual(amWorkspace(row("app", { dir: "/data/me/code/app" }), linked), {
      _tag: "Workspace",
      root: "/data/me/code/app",
      worktree: null,
    });
    assert.deepStrictEqual(amWorkspace(row("app", { dir: "/home/me/code/app" }), linked), {
      _tag: "Workspace",
      root: "/home/me/code/app",
      worktree: null,
    });
    assert.deepStrictEqual(amWorkspace(row("home", { dir: "/data/me" }), linked), {
      _tag: "Unsafe",
      reason: "home",
    });
    assert.deepStrictEqual(amWorkspace(row("app", { dir: "/data/me/code/app" }), local), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
  });

  it("checks a local root where its symlinks lead", () => {
    const resolved = (
      dir: string,
      real: string,
      context: Parameters<typeof amWorkspace>[1] = local,
    ) => amWorkspace(row("agent", { dir }), { ...context, realPaths: new Map([[dir, real]]) });
    assert.deepStrictEqual(resolved("/home/me/root", "/"), {
      _tag: "Unsafe",
      reason: "filesystem-root",
    });
    assert.deepStrictEqual(resolved("/home/me/srv", "/srv/app"), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    assert.deepStrictEqual(resolved("/home/me/link/../..", "/home"), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    // A link outside home is bound by its spelling, so it is refused wherever it leads.
    assert.deepStrictEqual(resolved("/tmp/link", "/home/me/code/app"), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    // A repository may be anywhere, but not where its link leads to the filesystem root or home.
    const viaLink = (
      root: string,
      real: string,
      context: Parameters<typeof amWorkspace>[1] = local,
    ) =>
      amWorkspace(row("agent", { dir: `${root}/ui` }), {
        ...context,
        gitRoot: { root, worktree: null },
        realPaths: new Map([
          [root, real],
          [`${root}/ui`, `${real === "/" ? "" : real}/ui`],
        ]),
      });
    assert.deepStrictEqual(viaLink("/mnt/link", "/mnt/fastdata/app"), {
      _tag: "Workspace",
      root: "/mnt/link",
      worktree: null,
    });
    assert.deepStrictEqual(viaLink("/home/me/rootlink", "/"), {
      _tag: "Unsafe",
      reason: "non-git-outside-home",
    });
    assert.deepStrictEqual(viaLink("/home/me/homelink", HOME), {
      _tag: "Workspace",
      root: "/home/me/homelink/ui",
      worktree: null,
    });
    assert.deepStrictEqual(
      viaLink("/home/me/datalink", "/data/me", { ...local, realHome: "/data/me" }),
      { _tag: "Workspace", root: "/home/me/datalink/ui", worktree: null },
    );
    // A `..` after a symlink is where the link leads, not where its spelling suggests.
    assert.deepStrictEqual(resolved("/home/me/link/../app", "/home/me/code/app"), {
      _tag: "Workspace",
      root: "/home/me/code/app",
      worktree: null,
    });
    // Other spellings are kept, normalized, so projects opened by that path still match.
    assert.deepStrictEqual(
      resolved("/home/me/code/app/", "/data/me/code/app", {
        ...local,
        realHome: "/data/me",
      }),
      { _tag: "Workspace", root: "/home/me/code/app", worktree: null },
    );
    // A repository may lead outside home, and keeps its spelling.
    assert.deepStrictEqual(
      amWorkspace(row("ui", { dir: "/home/me/code/app/ui" }), {
        ...local,
        gitRoot: { root: "/home/me/code/app", worktree: null },
        realPaths: new Map([["/home/me/code/app", "/srv/app"]]),
      }),
      { _tag: "Workspace", root: "/home/me/code/app", worktree: null },
    );
    // A fleet host's paths are not on this machine, so nothing here resolves them.
    assert.deepStrictEqual(
      amWorkspace(row("app", { host: "laptop", dir: "/Users/alex/code/app" }), {
        ...remote,
        realPaths: new Map([["/home/me/code/app", "/srv/app"]]),
      }),
      { _tag: "Workspace", root: "~/code/app", worktree: null },
    );
  });

  it("maps a fleet host's paths under this machine's home", () => {
    assert.strictEqual(fleetHostPathHere("/Users/alex/code/app"), "~/code/app");
    assert.strictEqual(fleetHostPathHere("/home/alex"), "~");
    assert.strictEqual(fleetHostPathHere("/root/x"), "~/x");
    assert.isNull(fleetHostPathHere("/srv/app"));
    assert.isNull(fleetHostPathHere("/home/u/../.."));
    assert.isNull(fleetHostPathHere("/home/u/code/../../other/x"));
    assert.deepStrictEqual(
      amWorkspace(
        row("wt", {
          host: "laptop",
          dir: "/Users/alex/.agent-manager/worktrees/app/wt",
          worktreePath: "/Users/alex/.agent-manager/worktrees/app/wt",
          repoRoot: "/Users/alex/code/app",
        }),
        remote,
      ),
      // That worktree is on the fleet host; a checkout here may be unrelated.
      { _tag: "Workspace", root: "~/code/app", worktree: null },
    );
  });
});

describe("amGitToplevel", () => {
  it("strips git's prefix from the directory as it is spelled", () => {
    assert.strictEqual(amGitToplevel("/home/me/app/packages/ui", "packages/ui/\n"), "/home/me/app");
    assert.strictEqual(amGitToplevel("/home/me/app/", "\n"), "/home/me/app");
    assert.isNull(amGitToplevel("/home/me/elsewhere", "packages/ui/\n"));
  });
});

describe("amGitRepo", () => {
  it("finds the main repository of a linked worktree", () => {
    assert.deepStrictEqual(
      amGitRepo("/home/me/app/ui", "ui/\n/data/me/app/.git\n/data/me/app/.git\n"),
      { root: "/home/me/app", worktree: null },
    );
    assert.deepStrictEqual(
      amGitRepo(
        "/home/me/wt/ui",
        "ui/\n/mnt/fastdata/app/.git/worktrees/wt\n/mnt/fastdata/app/.git\n",
      ),
      { root: "/mnt/fastdata/app", worktree: "/home/me/wt" },
    );
    // A bare repository's worktree, or an odd common dir, keeps the toplevel.
    assert.deepStrictEqual(
      amGitRepo("/home/me/wt", "\n/home/me/app.git/worktrees/wt\n/home/me/app.git\n"),
      { root: "/home/me/wt", worktree: null },
    );
    assert.deepStrictEqual(amGitRepo("/home/me/wt", "\n"), { root: "/home/me/wt", worktree: null });
    assert.deepStrictEqual(amGitRepo("/home/me/wt", "\n/x/.git/worktrees/wt\nrelative/.git\n"), {
      root: "/home/me/wt",
      worktree: null,
    });
    assert.isNull(amGitRepo("/home/me/elsewhere", "ui/\n/x/.git\n/x/.git\n"));
  });

  it("resolves git's relative answers against the directory", () => {
    assert.deepStrictEqual(amGitRepo("/home/me/app", "\n.git\n.git\n"), {
      root: "/home/me/app",
      worktree: null,
    });
    // In a subdirectory git names the git dir by its real path but the common dir relatively.
    assert.deepStrictEqual(amGitRepo("/home/me/app/ui", "ui/\n/data/me/app/.git\n../.git\n"), {
      root: "/home/me/app",
      worktree: null,
    });
    assert.deepStrictEqual(
      amGitRepo("/home/me/wt", "\n/abs/main/.git/worktrees/x\n/abs/main/.git\n"),
      { root: "/abs/main", worktree: "/home/me/wt" },
    );
    assert.deepStrictEqual(
      amGitRepo("/home/me/wt/ui", "ui/\n/home/me/app/.git/worktrees/wt\n../../app/.git\n"),
      { root: "/home/me/app", worktree: "/home/me/wt" },
    );
  });

  it("keeps the toplevel for an answer of the wrong shape", () => {
    const toplevel = { root: "/home/me/wt", worktree: null };
    // A stray extra line, such as an old git echoing a flag it does not know.
    assert.deepStrictEqual(
      amGitRepo("/home/me/wt", "\n/abs/main/.git/worktrees/x\n/abs/main/.git\nextra\n"),
      toplevel,
    );
    assert.deepStrictEqual(
      amGitRepo(
        "/home/me/wt",
        "\n--path-format=absolute\n/abs/main/.git/worktrees/x\n/abs/main/.git\n",
      ),
      toplevel,
    );
    assert.deepStrictEqual(amGitRepo("/home/me/wt", "\n\n/abs/main/.git\n"), toplevel);
    // A git dir nested deeper than one worktree name is not a linked worktree's.
    assert.deepStrictEqual(
      amGitRepo("/home/me/wt", "\n/abs/main/.git/worktrees/x/y\n/abs/main/.git\n"),
      toplevel,
    );
    assert.isNull(amGitRepo("/home/me/elsewhere", "ui/\n.git\n.git\nextra\n"));
  });
});

describe("amMirrorStatus", () => {
  it("counts the agents it skipped, with why", () => {
    assert.strictEqual(
      amMirrorStatus({
        mirrored: 2,
        missingWorkspaces: 0,
        skipped: new Map([["non-git-outside-home", 1]]),
        host: "",
      }),
      "Mirroring 2 agents. Skipped 1 agent with a project outside your home directory that is not a git repository.",
    );
    assert.strictEqual(
      amMirrorStatus({
        mirrored: 1,
        missingWorkspaces: 0,
        skipped: new Map([
          ["home", 2],
          ["filesystem-root", 1],
        ]),
        host: "laptop",
      }),
      "Mirroring 1 agent on laptop. Skipped 3 agents with a project at your home directory (2) or at the filesystem root (1).",
    );
  });
});

describe("amHistory", () => {
  it("keeps the messages of the tail it was given and says how much is left out", () => {
    const total = 450;
    const entries: Array<AmTranscriptEntry> = Array.from(
      { length: AM_MIRROR_ENTRY_LIMIT },
      (_, offset) => {
        const index = total - AM_MIRROR_ENTRY_LIMIT + offset;
        return index % 3 === 2
          ? { index, kind: "tool", name: "Bash", input: "ls" }
          : { index, kind: index % 3 === 0 ? "user" : "assistant", text: `entry ${index}` };
      },
    );
    const { history, notice } = amHistory("worker", entries, total);
    assert.isAtMost(history.length, AM_MIRROR_ENTRY_LIMIT);
    assert.deepStrictEqual(history[0], { role: "assistant", text: "entry 250" });
    assert.strictEqual(
      notice,
      "250 earlier transcript entries are not shown. Read them with `am transcript worker`.",
    );
    assert.isNull(amHistory("worker", [{ index: 0, kind: "user", text: "hi" }], 1).notice);
  });

  it("keeps the newest entries when the transcript grew while its tail was read", () => {
    const entries: Array<AmTranscriptEntry> = Array.from({ length: 230 }, (_, index) => ({
      index: 20 + index,
      kind: "user",
      text: `entry ${20 + index}`,
    }));
    const { history, notice } = amHistory("worker", entries, 250);
    assert.strictEqual(history.length, AM_MIRROR_ENTRY_LIMIT);
    assert.deepStrictEqual(history[0], { role: "user", text: "entry 50" });
    assert.include(notice ?? "", "50 earlier transcript entries");
  });

  it("budgets history by its serialized size, which JSON escaping grows", () => {
    const entries: Array<AmTranscriptEntry> = Array.from({ length: 100 }, (_, index) => ({
      index,
      kind: "user",
      text: `${index} `.padEnd(16_000, "\u0000"),
    }));
    const { history } = amHistory("worker", entries, 100);
    // Every history entry is written as a message and as a turn item.
    const emitted = history.reduce(
      (sum, message) => sum + 2 * new TextEncoder().encode(JSON.stringify(message.text)).length,
      0,
    );
    assert.isAtMost(emitted, 1_000_000);
    assert.isTrue(history.at(-1)!.text.startsWith("99 "));
  });

  it("drops the oldest messages beyond the history's text budget", () => {
    // 100 clipped messages of 16k four-byte characters, far beyond the budget.
    const entries: Array<AmTranscriptEntry> = Array.from({ length: 100 }, (_, index) => ({
      index,
      kind: "assistant",
      text: `${index}`.padEnd(20_000, "😀"),
    }));
    const { history, notice } = amHistory("worker", entries, 100);
    const bytes = history.reduce(
      (sum, message) => sum + new TextEncoder().encode(message.text).length,
      0,
    );
    assert.isAtMost(bytes, 1_000_000);
    assert.isAbove(history.length, 0);
    assert.strictEqual(history.at(-1)?.text.slice(0, 2), "99");
    const shown = Number(history[0]!.text.match(/^\d+/)![0]);
    assert.strictEqual(
      notice,
      `${shown} earlier transcript entries are not shown. Read them with \`am transcript worker\`.`,
    );
  });
});

describe("amClipEntry", () => {
  it("clips a tool's input and output as it clips message text", () => {
    const clipped = amClipEntry({
      index: 0,
      kind: "tool",
      name: "Bash",
      input: "i".repeat(50_000),
      output: "o".repeat(50_000),
    });
    assert.isAtMost(clipped.input!.length, 16_010);
    assert.isAtMost(clipped.output!.length, 16_010);
    assert.strictEqual(clipped.name, "Bash");
  });
});
