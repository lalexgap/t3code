import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import type { AmFleet, AmFleetRow, AmTranscriptEntry } from "./api.ts";
import {
  AM_MIRROR_ENTRY_LIMIT,
  AM_REMOVED_MARK,
  amConversationHead,
  amClipEntry,
  amHistory,
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
        false,
      ),
      {
        root: "/home/me/code/app",
        worktree: { path: "/home/me/.agent-manager/worktrees/app/wt", branch: "am/wt" },
      },
    );
    // An agent working in a subdirectory still binds the worktree's root.
    assert.deepStrictEqual(
      amWorkspace(
        row("nested", {
          dir: "/repo-wt/packages/ui",
          worktreePath: "/repo-wt",
          repoRoot: "/repo",
        }),
        false,
      )?.worktree?.path,
      "/repo-wt",
    );
    assert.deepStrictEqual(amWorkspace(row("plain", { dir: "/srv/notes" }), false), {
      root: "/srv/notes",
      worktree: null,
    });
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
        true,
      ),
      // That worktree is on the fleet host; a checkout here may be unrelated.
      { root: "~/code/app", worktree: null },
    );
    assert.isNull(amWorkspace(row("srv", { host: "laptop", dir: "/srv/app" }), true));
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
