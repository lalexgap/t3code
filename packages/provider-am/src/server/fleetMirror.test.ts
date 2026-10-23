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
import { makeAmFleetMirror } from "./fleetMirror.ts";
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
  const adoptions = yield* Queue.unbounded<ProviderThreadMirror.ProviderThreadAdoption>();
  const updates = yield* Queue.unbounded<ProviderThreadMirror.ProviderThreadMirrorUpdate>();
  const handler: Context.Service.Shape<typeof ProviderThreadMirror.ProviderThreadMirror> = {
    available: true,
    install: () => Effect.void,
    list: () => Effect.sync(() => [...threads.values()]),
    adopt: (input) =>
      Effect.sync((): ProviderThreadMirror.ProviderThreadAdoptionResult => {
        Queue.offerUnsafe(adoptions, input);
        if (input.workspaceRoot.startsWith("/work/missing"))
          return { _tag: "WorkspaceMissing", workspaceRoot: input.workspaceRoot };
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
    gitRoot: () => Effect.succeed(null),
    // Only explicit wakes and am's events reconcile during a test.
    intervalMs: 3_600_000,
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
