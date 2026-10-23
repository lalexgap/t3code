import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";

import type * as ProviderThreadLifecycle from "@t3tools/provider-core/server/ProviderThreadLifecycle";
import { AmSettings } from "../settings.ts";
import { makeAmApi } from "./api.ts";
import { FAKE_TOKEN, makeFakeAm, type FakeAgent } from "./fakeAm.testkit.ts";
import { makeAmThreadLifecycle } from "./lifecycle.ts";
import { amConversationHead, planMirror } from "./mirrorPlan.ts";

const testLayer = Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer);
const decodeSettings = Schema.decodeSync(AmSettings);
const AGENT = "t3-fix-login-abc123";

const agent = (status: string): FakeAgent => ({
  status,
  dir: "/work/app",
  provider: "claude",
  entries: [],
});

const change = (
  patch: Partial<ProviderThreadLifecycle.ProviderThreadLifecycleChange> = {},
): ProviderThreadLifecycle.ProviderThreadLifecycleChange => ({
  event: "settled",
  threadId: ThreadId.make("thread-am"),
  providerThreadId: ProviderThreadId.make("provider-thread-am"),
  nativeThreadId: AGENT,
  sharedWithActiveThread: false,
  stillApplies: Effect.succeed(true),
  ...patch,
});

const setup = Effect.fnUntraced(function* (
  settings: Partial<AmSettings> = {},
  busyAgents: ReadonlySet<string> = new Set(),
) {
  const fake = yield* makeFakeAm();
  const lifecycle = makeAmThreadLifecycle({
    settings: decodeSettings({
      enabled: true,
      apiToken: FAKE_TOKEN,
      apiUrl: fake.url,
      ...settings,
    }),
    api: makeAmApi(yield* HttpClient.HttpClient, {
      baseUrl: settings.apiUrl ?? fake.url,
      token: FAKE_TOKEN,
    }),
    busyAgents,
  });
  const stops = () => fake.log.filter((call) => call.path.endsWith("/stop"));
  return { fake, lifecycle, stops };
});

describe("makeAmThreadLifecycle", () => {
  it("stops on settle by default and never on archive", () => {
    const settings = decodeSettings({});
    assert.isTrue(settings.stopOnSettle);
    assert.isFalse(settings.stopOnArchive);
  });

  it.live.each([
    ["this machine", AGENT],
    ["a fleet host", `laptop:${AGENT}`],
  ] as const)("stops a settled thread's idle agent on %s", ([_where, ref]) =>
    Effect.gen(function* () {
      const { fake, lifecycle, stops } = yield* setup();
      fake.agents.set(ref, agent("idle"));
      const notice = yield* lifecycle(change({ nativeThreadId: ref }));
      assert.deepStrictEqual(
        stops().map((call) => [call.method, call.path]),
        [["POST", `/api/agents/${encodeURIComponent(ref)}/stop`]],
      );
      assert.strictEqual(fake.agents.get(ref)?.status, "exited");
      assert.strictEqual(
        notice,
        `Stopped the Agent Motel agent ${ref}. Send a message to resume it.`,
      );
      // Stopping keeps the agent: nothing removes it.
      assert.isFalse(fake.log.some((call) => call.method === "DELETE"));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live.each([
    ["working", "is working"],
    ["starting", "is working"],
    ["needs-attention", "is waiting on a prompt in its terminal"],
    ["waiting", "has scheduled or background work"],
  ] as const)("leaves a %s agent running and says so", ([status, reason]) =>
    Effect.gen(function* () {
      const { fake, lifecycle, stops } = yield* setup();
      fake.agents.set(AGENT, agent(status));
      const notice = yield* lifecycle(change());
      assert.deepStrictEqual(stops(), []);
      assert.strictEqual(
        notice,
        `The Agent Motel agent ${AGENT} ${reason}, so it was not stopped when this thread was settled. Stop it with \`am stop ${AGENT}\`.`,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("leaves an idle agent with queued messages running and says so", () =>
    Effect.gen(function* () {
      const { fake, lifecycle, stops } = yield* setup();
      fake.agents.set(AGENT, { ...agent("idle"), queue: [{ id: "msg-1" }] });
      const notice = yield* lifecycle(change());
      assert.deepStrictEqual(stops(), []);
      assert.strictEqual(
        notice,
        `The Agent Motel agent ${AGENT} has queued messages, so it was not stopped when this thread was settled. Stop it with \`am stop ${AGENT}\`.`,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("leaves an agent running a T3 turn alone without asking am", () =>
    Effect.gen(function* () {
      const { fake, lifecycle } = yield* setup({}, new Set([AGENT]));
      fake.agents.set(AGENT, agent("idle"));
      const notice = yield* lifecycle(change());
      assert.deepStrictEqual(fake.log, []);
      assert.include(notice ?? "", "was not stopped");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("does nothing when stopping on settle is off", () =>
    Effect.gen(function* () {
      const { fake, lifecycle } = yield* setup({ stopOnSettle: false });
      fake.agents.set(AGENT, agent("idle"));
      assert.isNull(yield* lifecycle(change()));
      assert.deepStrictEqual(fake.log, []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("stops on archive only when that is on", () =>
    Effect.gen(function* () {
      const off = yield* setup();
      off.fake.agents.set(AGENT, agent("idle"));
      assert.isNull(yield* off.lifecycle(change({ event: "archived" })));
      assert.deepStrictEqual(off.fake.log, []);

      const on = yield* setup({ stopOnArchive: true, stopOnSettle: false });
      on.fake.agents.set(AGENT, agent("idle"));
      const notice = yield* on.lifecycle(change({ event: "archived" }));
      assert.strictEqual(on.stops().length, 1);
      assert.include(notice ?? "", "Stopped");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("keeps an agent another active thread uses", () =>
    Effect.gen(function* () {
      const { fake, lifecycle } = yield* setup();
      fake.agents.set(AGENT, agent("idle"));
      const notice = yield* lifecycle(change({ sharedWithActiveThread: true }));
      assert.deepStrictEqual(fake.log, []);
      assert.strictEqual(
        notice,
        `Another active thread uses the Agent Motel agent ${AGENT}, so it was not stopped.`,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("does not stop once the thread was taken back", () =>
    Effect.gen(function* () {
      const { fake, lifecycle, stops } = yield* setup();
      fake.agents.set(AGENT, agent("idle"));
      assert.isNull(yield* lifecycle(change({ stillApplies: Effect.succeed(false) })));
      assert.deepStrictEqual(stops(), []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("says nothing for an agent that is already stopped or gone", () =>
    Effect.gen(function* () {
      const { fake, lifecycle, stops } = yield* setup();
      fake.agents.set(AGENT, agent("exited"));
      assert.isNull(yield* lifecycle(change()));
      assert.isNull(yield* lifecycle(change({ nativeThreadId: "t3-removed-abc123" })));
      assert.deepStrictEqual(stops(), []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("reports a failed stop on the thread", () =>
    Effect.gen(function* () {
      const { fake, lifecycle } = yield* setup({ apiUrl: "http://127.0.0.1:9" });
      fake.agents.set(AGENT, agent("idle"));
      const notice = yield* lifecycle(change());
      assert.include(notice ?? "", `Could not stop the Agent Motel agent ${AGENT}`);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});

describe("the fleet mirror and a stopped agent", () => {
  const row = (status: string) => ({
    name: AGENT,
    status,
    provider: "claude",
    dir: "/home/me/code/app",
    updatedAt: "2026-10-22T12:00:00.000Z",
  });
  const thread = (archived: boolean) => ({
    threadId: "thread-am",
    providerThreadId: "provider-thread-am",
    ref: AGENT,
    archived,
    released: false,
    head: amConversationHead(AGENT, 4),
  });
  const reconcile = (archived: boolean) =>
    planMirror({
      host: "",
      fleet: { rows: [row("exited")], unreachable: [] },
      threads: [thread(archived)],
      autoImportDays: 7,
      nowMs: Date.parse("2026-10-22T12:00:00.000Z"),
      previous: new Map([[AGENT, "live"]]),
    });

  it("keeps a settled thread whose agent it just stopped", () => {
    const plan = reconcile(false);
    assert.deepStrictEqual(plan.archive, []);
    assert.deepStrictEqual(plan.unarchive, []);
    assert.strictEqual(plan.mirrored, 1);
  });

  it("leaves an archived thread archived when its agent stops", () => {
    const plan = reconcile(true);
    assert.deepStrictEqual(plan.archive, []);
    assert.deepStrictEqual(plan.unarchive, []);
    assert.deepStrictEqual(plan.watch, []);
  });
});
