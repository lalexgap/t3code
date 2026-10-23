import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderRef,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";

import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import type * as ProviderThreadMirror from "@t3tools/provider-core/server/ProviderThreadMirror";
import type * as Context from "effect/Context";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { AmSettings } from "../settings.ts";
import { makeAmApi } from "./api.ts";
import { FAKE_TOKEN, makeFakeAm, type FakeAgent, type FakeEntry } from "./fakeAm.testkit.ts";
import { AmGitRootError, makeAmFleetMirror, type AmFleetMirrorOptions } from "./fleetMirror.ts";
import { AM_MIRROR_ENTRY_LIMIT, AM_REMOVED_MARK, amConversationHead } from "./mirrorPlan.ts";

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  FetchHttpClient.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const AM = ProviderDriverKind.make("am");
const INSTANCE_ID = ProviderInstanceId.make("am_fleet");
const decodeSettings = Schema.decodeSync(AmSettings);
/** Relative to the clock the mirror ages agents out by. */
const daysAgo = (now: DateTime.Utc, days: number) =>
  DateTime.formatIso(DateTime.subtract(now, { days }));

const agent = (entries: Array<FakeEntry> = [], patch: Partial<FakeAgent> = {}): FakeAgent => ({
  status: "idle",
  dir: "/work/app",
  provider: "claude",
  entries,
  ...patch,
});

/** An in-memory stand-in for the server's thread mirror. */
const makeFakeMirror = Effect.fnUntraced(function* (
  initial: ReadonlyArray<{
    readonly ref: string;
    readonly head: OrchestrationV2ProviderRef | null;
    readonly archived?: boolean;
  }>,
  /** Workspace roots of the server's existing projects; an adoption adds its own. */
  initialProjects: ReadonlyArray<string> = [],
) {
  const now = yield* DateTime.now;
  const threads = new Map<string, ProviderThreadMirror.MirroredProviderThread>();
  const bind = (ref: string, head: OrchestrationV2ProviderRef | null, archived = false) =>
    threads.set(`thread-${ref}`, {
      threadId: ThreadId.make(`thread-${ref}`),
      archivedAt: archived ? now : null,
      released: false,
      providerThread: {
        id: ProviderThreadId.make(`provider-thread-${ref}`),
        driver: AM,
        providerInstanceId: INSTANCE_ID,
        providerSessionId: null,
        appThreadId: ThreadId.make(`thread-${ref}`),
        ownerNodeId: null,
        nativeThreadRef: { driver: AM, nativeId: ref, strength: "strong" },
        nativeConversationHeadRef: head,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
  for (const thread of initial) bind(thread.ref, thread.head, thread.archived);
  const projects = new Set(initialProjects);
  const adoptions = yield* Queue.unbounded<ProviderThreadMirror.ProviderThreadAdoption>();
  const updates = yield* Queue.unbounded<ProviderThreadMirror.ProviderThreadMirrorUpdate>();
  const handler: Context.Service.Shape<typeof ProviderThreadMirror.ProviderThreadMirror> = {
    available: true,
    install: () => Effect.void,
    list: () => Effect.sync(() => [...threads.values()]),
    projectRoots: Effect.sync(() => [...projects]),
    adopt: (input) =>
      Effect.sync((): ProviderThreadMirror.ProviderThreadAdoptionResult => {
        Queue.offerUnsafe(adoptions, input);
        if (input.workspaceRoot.startsWith("/work/missing"))
          return { _tag: "WorkspaceMissing", workspaceRoot: input.workspaceRoot };
        projects.add(input.workspaceRoot);
        bind(input.nativeThreadId, input.conversationHeadRef);
        return { _tag: "Adopted", threadId: ThreadId.make(`thread-${input.nativeThreadId}`) };
      }),
    update: (input) =>
      Effect.sync(() => {
        const current = threads.get(input.threadId)!;
        const head = current.providerThread.nativeConversationHeadRef;
        const { fingerprint: _previous, ...position } = head ?? amConversationHead("", 0);
        threads.set(input.threadId, {
          ...current,
          ...(input.archived === undefined ? {} : { archivedAt: input.archived ? now : null }),
          providerThread: {
            ...current.providerThread,
            ...(input.headFingerprint === undefined || head === null
              ? {}
              : {
                  nativeConversationHeadRef:
                    input.headFingerprint === null
                      ? position
                      : { ...position, fingerprint: input.headFingerprint },
                }),
          },
        });
        Queue.offerUnsafe(updates, input);
      }),
  };
  return { handler, adoptions, updates };
});

const startMirror = Effect.fnUntraced(function* (
  fake: Effect.Success<ReturnType<typeof makeFakeAm>>,
  mirror: Effect.Success<ReturnType<typeof makeFakeMirror>>,
  host = "",
  overrides: Partial<AmFleetMirrorOptions> = {},
) {
  const requests =
    yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
  const fleetMirror = yield* makeAmFleetMirror({
    instanceId: INSTANCE_ID,
    settings: decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, apiUrl: fake.url, host }),
    api: makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN }),
    mirror: mirror.handler,
    continuationRequests: {
      offer: (request) => Queue.offer(requests, request).pipe(Effect.asVoid),
      take: Queue.take(requests),
    },
    busyAgents: new Set(),
    home: "/work",
    realHome: "/work",
    gitRoot: () => Effect.succeed(null),
    realPath: () => Effect.succeed(null),
    // Only explicit wakes and am's events reconcile during a test.
    intervalMs: 3_600_000,
    ...overrides,
  });
  const statuses = yield* Queue.unbounded<string>();
  yield* fleetMirror.status.pipe(
    Stream.runForEach((status) =>
      status === null ? Effect.void : Queue.offer(statuses, status).pipe(Effect.asVoid),
    ),
    Effect.forkScoped,
  );
  return { fleetMirror, requests, statuses };
});

const turnHistory = (count: number): Array<FakeEntry> =>
  Array.from({ length: count }, (_, index): FakeEntry =>
    index % 3 === 2
      ? { kind: "tool", name: "Bash", input: "ls", output: "ok" }
      : { kind: index % 3 === 0 ? "user" : "assistant", text: `entry ${index}` },
  );

describe("makeAmFleetMirror", () => {
  it.effect("imports new agents with bounded history and leaves owned or aged-out ones", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const now = yield* DateTime.now;
      fake.agents.set("worker", agent(turnHistory(450), { dir: "/work/worker" }));
      fake.agents.set("recent", agent([], { status: "exited", updatedAt: daysAgo(now, 2) }));
      fake.agents.set("old", agent([], { status: "exited", updatedAt: daysAgo(now, 30) }));
      fake.agents.set("t3-owned-abc123", agent());
      fake.agents.set("elsewhere", agent([], { dir: "/work/missing/project" }));
      const mirror = yield* makeFakeMirror([{ ref: "t3-owned-abc123", head: null }]);
      const { statuses } = yield* startMirror(fake, mirror);

      const adopted: Array<ProviderThreadMirror.ProviderThreadAdoption> = [];
      let status = "";
      for (let step = 0; step < 3; step++) {
        if (step > 0) yield* TestClock.adjust("1 second");
        status = yield* Queue.take(statuses);
        adopted.push(yield* Queue.take(mirror.adoptions));
      }
      assert.deepStrictEqual(adopted.map((adoption) => adoption.nativeThreadId).toSorted(), [
        "elsewhere",
        "recent",
        "worker",
      ]);
      const worker = adopted.find((adoption) => adoption.nativeThreadId === "worker")!;
      assert.strictEqual(worker.workspaceRoot, "/work/worker");
      assert.isNull(worker.worktree);
      assert.strictEqual(worker.model, "claude");
      assert.isAtMost(worker.history.length, AM_MIRROR_ENTRY_LIMIT);
      assert.deepStrictEqual(worker.history[0], { role: "assistant", text: "entry 250" });
      assert.include(worker.historyNotice ?? "", "250 earlier transcript entries");
      // The thread's head starts at the end, so mirroring picks up only new work.
      assert.strictEqual(worker.conversationHeadRef?.ordinal, 450);
      assert.strictEqual(
        status,
        "Mirroring 3 agents. 1 agent works in a project with no copy on this machine.",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("imports a large fleet one agent per step, live and recent agents first", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const fleet = 12;
      for (let index = 0; index < fleet; index++)
        fake.agents.set(
          `agent-${index}`,
          agent([{ kind: "user", text: `task ${index}` }], {
            status: index === 7 ? "working" : "exited",
            updatedAt: `2026-01-${String(index + 1).padStart(2, "0")}T00:00:00.000Z`,
          }),
        );
      const mirror = yield* makeFakeMirror([]);
      // The test clock starts in 1970, so no agent has aged out.
      const { statuses } = yield* startMirror(fake, mirror);

      const order: Array<string> = [];
      for (let step = 1; step <= fleet; step++) {
        if (step > 1) yield* TestClock.adjust("1 second");
        const status = yield* Queue.take(statuses);
        order.push((yield* Queue.take(mirror.adoptions)).nativeThreadId);
        assert.strictEqual(yield* Queue.size(mirror.adoptions), 0);
        assert.strictEqual(
          status,
          step < fleet
            ? `Mirroring ${step} ${step === 1 ? "agent" : "agents"}. Importing ${fleet - step} more.`
            : `Mirroring ${fleet} agents.`,
        );
      }
      assert.deepStrictEqual(order, [
        "agent-7",
        ...Array.from({ length: fleet }, (_, index) => `agent-${fleet - 1 - index}`).filter(
          (ref) => ref !== "agent-7",
        ),
      ]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("archives a removed agent's thread and restores it when the agent comes back", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set("kept", agent());
      const mirror = yield* makeFakeMirror([
        { ref: "kept", head: amConversationHead("kept", 0) },
        { ref: "gone", head: amConversationHead("gone", 3) },
        { ref: "parked", head: amConversationHead("parked", 0), archived: true },
      ]);
      fake.agents.set("parked", agent([], { status: "working" }));
      const { fleetMirror, statuses } = yield* startMirror(fake, mirror);

      // The first listing that misses an agent only notes it; the next one archives.
      yield* Queue.take(statuses);
      assert.strictEqual(yield* Queue.size(mirror.updates), 0);
      yield* fleetMirror.wake;
      yield* Queue.take(statuses);
      const archived = yield* Queue.take(mirror.updates);
      assert.strictEqual(archived.threadId, "thread-gone");
      assert.isTrue(archived.archived);
      assert.strictEqual(archived.headFingerprint, AM_REMOVED_MARK);

      fake.agents.set("gone", agent([], { status: "exited" }));
      yield* fleetMirror.wake;
      yield* Queue.take(statuses);
      const restored = yield* Queue.take(mirror.updates);
      assert.strictEqual(restored.threadId, "thread-gone");
      assert.isFalse(restored.archived);
      assert.isNull(restored.headFingerprint);
      // A thread the user archived stays archived while its agent keeps running.
      yield* fleetMirror.wake;
      yield* Queue.take(statuses);
      assert.strictEqual(yield* Queue.size(mirror.updates), 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("asks for a run when the agent works between T3 turns", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(
        "worker",
        agent([
          { kind: "user", text: "Hi" },
          { kind: "assistant", text: "Hello" },
        ]),
      );
      const mirror = yield* makeFakeMirror([
        { ref: "worker", head: amConversationHead("worker", 2) },
      ]);
      const { requests, statuses } = yield* startMirror(fake, mirror);
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 1 agent.");
      yield* fake.connected;
      assert.strictEqual(yield* Queue.size(requests), 0);

      // Someone types into the agent's terminal; am's event stream wakes the mirror.
      yield* fake.update("worker", (worker) => {
        worker.entries.push({ kind: "user", text: "From the terminal" });
        worker.entries.push({ kind: "assistant", text: "Done there." });
        worker.updatedAt = "2026-01-02T00:00:00.000Z";
      });
      const request = yield* Queue.take(requests);
      assert.strictEqual(request.threadId, "thread-worker");
      assert.strictEqual(request.providerThreadId, "provider-thread-worker");
      assert.strictEqual(request.delivery, "adapter_buffered");
      assert.strictEqual(request.notification?.summary, "Agent Motel activity");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("tries an agent again when git could not say which repository it is in", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set("ui", agent([], { dir: "/work/app/packages/ui" }));
      const mirror = yield* makeFakeMirror([]);
      let gitAnswers = false;
      const { fleetMirror, statuses } = yield* startMirror(fake, mirror, "", {
        gitRoot: () =>
          gitAnswers
            ? Effect.succeed({ root: "/work/app", worktree: null })
            : Effect.fail(
                new AmGitRootError({ path: "/work/app/packages/ui", cause: "timed out" }),
              ),
      });
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 0 agents.");
      assert.strictEqual(yield* Queue.size(mirror.adoptions), 0);

      gitAnswers = true;
      yield* fleetMirror.wake;
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 1 agent.");
      assert.strictEqual((yield* Queue.take(mirror.adoptions)).workspaceRoot, "/work/app");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("puts an agent in a linked worktree in its main repository's project", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set("review", agent([], { dir: "/work/wt/ui" }));
      fake.agents.set("outside", agent([], { dir: "/mnt/fastdata/app" }));
      const mirror = yield* makeFakeMirror([]);
      const { statuses } = yield* startMirror(fake, mirror, "", {
        gitRoot: (path) =>
          Effect.succeed(
            path === "/work/wt/ui"
              ? { root: "/srv/app", worktree: "/work/wt" }
              : { root: "/mnt/fastdata/app", worktree: null },
          ),
      });
      yield* Queue.take(statuses);
      yield* TestClock.adjust("1 second");
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 2 agents.");
      const adopted = new Map<string, ProviderThreadMirror.ProviderThreadAdoption>();
      for (let index = 0; index < 2; index++) {
        const adoption = yield* Queue.take(mirror.adoptions);
        adopted.set(adoption.nativeThreadId, adoption);
      }
      assert.strictEqual(adopted.get("review")?.workspaceRoot, "/srv/app");
      assert.deepStrictEqual(adopted.get("review")?.worktree, { path: "/work/wt", branch: null });
      assert.strictEqual(adopted.get("outside")?.workspaceRoot, "/mnt/fastdata/app");
      assert.isNull(adopted.get("outside")?.worktree);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("groups a main checkout and a linked worktree under a symlinked home", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set("main", agent([], { dir: "/work/app/ui" }));
      fake.agents.set("review", agent([], { dir: "/work/wt" }));
      const mirror = yield* makeFakeMirror([]);
      const resolved: Array<string> = [];
      const { statuses } = yield* startMirror(fake, mirror, "", {
        realHome: "/data/work",
        // Git names a linked worktree's common dir by its real path.
        gitRoot: (path) =>
          Effect.succeed(
            path === "/work/wt"
              ? { root: "/data/work/app", worktree: "/work/wt" }
              : { root: "/work/app", worktree: null },
          ),
        realPath: (path) => {
          resolved.push(path);
          return Effect.succeed(path.replace(/^\/work\//, "/data/work/"));
        },
      });
      yield* Queue.take(statuses);
      yield* TestClock.adjust("1 second");
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 2 agents.");
      const adopted = new Map<string, ProviderThreadMirror.ProviderThreadAdoption>();
      for (let index = 0; index < 2; index++) {
        const adoption = yield* Queue.take(mirror.adoptions);
        adopted.set(adoption.nativeThreadId, adoption);
      }
      assert.strictEqual(adopted.get("main")?.workspaceRoot, "/work/app");
      assert.strictEqual(adopted.get("review")?.workspaceRoot, "/work/app");
      assert.deepStrictEqual(adopted.get("review")?.worktree, { path: "/work/wt", branch: null });
      // The worktree's root is resolved as it is bound, not as git spelled it.
      assert.include(resolved, "/work/app");
      assert.notInclude(resolved, "/data/work/app");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("keeps one project for a repository am spells under home and by its real path", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const now = yield* DateTime.now;
      const linked = "/home/op/code/producthunt";
      const real = "/mnt/fastdata/code/producthunt";
      const worktrees = "/home/op/.agent-manager/worktrees/producthunt";
      // The agent spelled by its real path is the most recent, so it is imported first.
      fake.agents.set(
        "real-wt",
        agent([], {
          dir: `${worktrees}/real-wt`,
          worktreePath: `${worktrees}/real-wt`,
          worktreeBranch: "am/real-wt",
          repoRoot: real,
          updatedAt: DateTime.formatIso(now),
        }),
      );
      fake.agents.set(
        "home-wt",
        agent([], {
          dir: `${worktrees}/home-wt`,
          worktreePath: `${worktrees}/home-wt`,
          worktreeBranch: "am/home-wt",
          repoRoot: linked,
          updatedAt: daysAgo(now, 1),
        }),
      );
      fake.agents.set(
        "git-wt",
        agent([], { dir: `${worktrees}/git-wt`, updatedAt: daysAgo(now, 2) }),
      );
      fake.agents.set("main", agent([], { dir: linked, updatedAt: daysAgo(now, 3) }));
      const mirror = yield* makeFakeMirror([]);
      const { statuses } = yield* startMirror(fake, mirror, "", {
        home: "/home/op",
        realHome: "/home/op",
        // Git names a linked worktree's common dir by its real path.
        gitRoot: (path) =>
          Effect.succeed(
            path === linked ? { root: linked, worktree: null } : { root: real, worktree: path },
          ),
        realPath: (path) =>
          Effect.succeed(
            path === linked || path.startsWith(`${linked}/`)
              ? `${real}${path.slice(linked.length)}`
              : path.startsWith("/mnt/fastdata/") || path.startsWith(`${worktrees}/`)
                ? path
                : null,
          ),
      });
      yield* Queue.take(statuses);
      const adopted = new Map<string, ProviderThreadMirror.ProviderThreadAdoption>();
      for (let index = 0; index < 4; index++) {
        yield* TestClock.adjust("1 second");
        const adoption = yield* Queue.take(mirror.adoptions);
        adopted.set(adoption.nativeThreadId, adoption);
      }
      for (const name of ["real-wt", "home-wt", "git-wt", "main"])
        assert.strictEqual(adopted.get(name)?.workspaceRoot, linked, name);
      assert.deepStrictEqual(adopted.get("real-wt")?.worktree, {
        path: `${worktrees}/real-wt`,
        branch: "am/real-wt",
      });
      assert.deepStrictEqual(adopted.get("home-wt")?.worktree, {
        path: `${worktrees}/home-wt`,
        branch: "am/home-wt",
      });
      assert.deepStrictEqual(adopted.get("git-wt")?.worktree, {
        path: `${worktrees}/git-wt`,
        branch: null,
      });
      assert.isNull(adopted.get("main")?.worktree);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("joins an existing project at the same real path under any spelling", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const linked = "/home/op/code/producthunt";
      const real = "/mnt/fastdata/code/producthunt";
      fake.agents.set("main", agent([], { dir: linked }));
      const mirror = yield* makeFakeMirror([], ["/home/op/code/other", real]);
      const { statuses } = yield* startMirror(fake, mirror, "", {
        home: "/home/op",
        realHome: "/home/op",
        gitRoot: () => Effect.succeed({ root: linked, worktree: null }),
        realPath: (path) => Effect.succeed(path === linked ? real : path),
      });
      yield* Queue.take(statuses);
      assert.strictEqual((yield* Queue.take(mirror.adoptions)).workspaceRoot, real);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("counts a fleet host's agent with no path under this home as having no copy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set("laptop:far", agent([], { dir: "/mnt/fastdata/app" }));
      const mirror = yield* makeFakeMirror([]);
      const { statuses } = yield* startMirror(fake, mirror, "laptop");
      assert.strictEqual(
        yield* Queue.take(statuses),
        "Mirroring 0 agents on laptop. 1 agent works in a project with no copy on this machine.",
      );
      assert.strictEqual(yield* Queue.size(mirror.adoptions), 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("checks a local agent's project where its symlinks lead", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set("escaped", agent([], { dir: "/work/link" }));
      fake.agents.set("real", agent([], { dir: "/data/work/app" }));
      const mirror = yield* makeFakeMirror([]);
      const { statuses } = yield* startMirror(fake, mirror, "", {
        realHome: "/data/work",
        realPath: (path) => Effect.succeed(path === "/work/link" ? "/srv/app" : path),
      });
      yield* Queue.take(statuses);
      yield* TestClock.adjust("1 second");
      assert.strictEqual(
        yield* Queue.take(statuses),
        "Mirroring 1 agent. Skipped 1 agent with a project outside your home directory that is not a git repository.",
      );
      assert.strictEqual((yield* Queue.take(mirror.adoptions)).workspaceRoot, "/data/work/app");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("stops counting a skipped agent once it ages out of the imports", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const now = yield* DateTime.now;
      fake.agents.set(
        "outside",
        agent([], { dir: "/srv/app", status: "exited", updatedAt: daysAgo(now, 1) }),
      );
      fake.agents.set(
        "elsewhere",
        agent([], { dir: "/work/missing/app", status: "exited", updatedAt: daysAgo(now, 1) }),
      );
      fake.agents.set("t3-owned-abc123", agent());
      const mirror = yield* makeFakeMirror([{ ref: "t3-owned-abc123", head: null }]);
      const { fleetMirror, statuses } = yield* startMirror(fake, mirror);
      yield* Queue.take(statuses);
      yield* TestClock.adjust("1 second");
      assert.strictEqual(
        yield* Queue.take(statuses),
        "Mirroring 1 agent. 1 agent works in a project with no copy on this machine. Skipped 1 agent with a project outside your home directory that is not a git repository.",
      );

      // One listing that leaves an agent out proves nothing, so it still counts.
      const outside = fake.agents.get("outside")!;
      fake.agents.delete("outside");
      yield* fleetMirror.wake;
      assert.strictEqual(
        yield* Queue.take(statuses),
        "Mirroring 1 agent. 1 agent works in a project with no copy on this machine. Skipped 1 agent with a project outside your home directory that is not a git repository.",
      );
      fake.agents.set("outside", outside);

      // An empty listing while a thread is bound confirms nothing, so the counts stay.
      const listed = new Map(fake.agents);
      fake.agents.clear();
      yield* fleetMirror.wake;
      assert.strictEqual(
        yield* Queue.take(statuses),
        "Mirroring 1 agent. 1 agent works in a project with no copy on this machine. Skipped 1 agent with a project outside your home directory that is not a git repository.",
      );
      for (const [name, value] of listed) fake.agents.set(name, value);

      yield* TestClock.adjust("30 days");
      yield* fleetMirror.wake;
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 1 agent.");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("mirrors a fleet host by `host:name` and holds still while it is unreachable", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(
        "laptop:there",
        agent([], { dir: "/Users/alex/code/app", status: "working" }),
      );
      fake.agents.set("here", agent());
      const mirror = yield* makeFakeMirror([]);
      const { fleetMirror, statuses } = yield* startMirror(fake, mirror, "laptop");
      const adopted = yield* Queue.take(mirror.adoptions);
      assert.strictEqual(adopted.nativeThreadId, "laptop:there");
      assert.strictEqual(adopted.workspaceRoot, "~/code/app");
      assert.strictEqual(yield* Queue.take(statuses), "Mirroring 1 agent on laptop.");

      fake.unreachable.push("laptop");
      yield* fleetMirror.wake;
      assert.strictEqual(
        yield* Queue.take(statuses),
        "Agent Motel cannot reach laptop to mirror its agents.",
      );
      assert.strictEqual(yield* Queue.size(mirror.updates), 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
