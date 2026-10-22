import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { AmSettings } from "../settings.ts";
import { makeAmAdapterV2 } from "./adapter.ts";
import { makeAmApi } from "./api.ts";
import { FAKE_TOKEN, makeFakeAm, type FakeEntry } from "./fakeAm.testkit.ts";
import { amAgentName } from "./mapping.ts";

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  FetchHttpClient.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const INSTANCE_ID = ProviderInstanceId.make("am_fleet");
const THREAD_ID = ThreadId.make("thread-am-test");
const TITLE = "Fix login";
const AGENT = amAgentName(THREAD_ID, TITLE);
const CWD = "/work/project";
const settings = Schema.decodeSync(AmSettings)({ enabled: true, apiToken: FAKE_TOKEN });
const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: CWD,
});
const modelSelection = { instanceId: INSTANCE_ID, model: "claude" };

type Fake = Effect.Success<ReturnType<typeof makeFakeAm>>;

const makeHarness = Effect.fnUntraced(function* (
  fake: Fake,
  existingProviderThread?: OrchestrationV2ProviderThread,
) {
  const api = makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN });
  // Turns must settle from am's event stream alone.
  const adapter = yield* makeAmAdapterV2({
    instanceId: INSTANCE_ID,
    settings,
    api,
    pollIntervalMs: 3_600_000,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: ProviderSessionId.make("session-am"),
    modelSelection,
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const allEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => {
        allEvents.push(event);
        Queue.offerUnsafe(emitted, event);
      }),
    ),
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    title: TITLE,
    modelSelection,
    runtimePolicy,
    ...(existingProviderThread ? { existingProviderThread } : {}),
  });
  yield* fake.connected;
  const takeEvent = Effect.fnUntraced(function* <
    T extends ProviderAdapter.ProviderAdapterV2Event["type"],
  >(
    type: T,
    predicate: (
      event: Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>,
    ) => boolean = () => true,
  ) {
    while (true) {
      const event = yield* Queue.take(emitted);
      if (
        event.type === type &&
        predicate(event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>)
      )
        return event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>;
    }
  });
  const takeItem = (
    predicate: (
      item: ProviderAdapter.ProviderAdapterV2Event & { type: "turn_item.updated" },
    ) => boolean,
  ) => takeEvent("turn_item.updated", predicate).pipe(Effect.map((event) => event.turnItem));
  return { runtime, providerThread, takeEvent, takeItem, allEvents };
});

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  text: string,
  ordinal = 1,
): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2TurnInput> {
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: THREAD_ID,
    projectId: ProjectId.make("project-am-test"),
    title: TITLE,
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: INSTANCE_ID,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  return {
    appThread,
    threadId: THREAD_ID,
    runId: RunId.make(`run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt-${ordinal}`),
    rootNodeId: NodeId.make(`root-${ordinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message-${ordinal}`),
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection,
    runtimePolicy,
  };
});

const user = (text: string): FakeEntry => ({ kind: "user", text });
const assistant = (text: string): FakeEntry => ({ kind: "assistant", text });

describe("AmAdapterV2", () => {
  it.live("starts the agent in place with the first prompt and settles when am goes idle", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const harness = yield* makeHarness(fake);
      assert.strictEqual(harness.providerThread.nativeThreadRef?.nativeId, AGENT);

      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Fix login"));
      const created = yield* fake.takeCall("POST", "/api/agents");
      assert.deepStrictEqual(created.body, {
        name: AGENT,
        dir: CWD,
        inPlace: true,
        task: "Fix login",
        provider: "claude",
      });

      // `starting` comes before the prompt lands; it must not end the turn when am idles next.
      yield* fake.update(AGENT, (agent) => {
        agent.status = "working";
        agent.entries.push(user("Fix login"), assistant("Looking at the login form."), {
          kind: "tool",
          name: "Bash",
          input: '{"command":"grep -r login src"}',
        });
      });
      const running = yield* harness.takeItem(
        (event) => event.turnItem.type === "command_execution",
      );
      assert.strictEqual(running.status, "running");
      assert.strictEqual(
        running.type === "command_execution" && running.input,
        "grep -r login src",
      );

      yield* fake.update(AGENT, (agent) => {
        agent.entries[2] = { ...agent.entries[2]!, output: "src/login.ts" };
        agent.entries.push(assistant("Fixed the redirect."));
        agent.status = "idle";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");

      const items = harness.allEvents.flatMap((event) =>
        event.type === "turn_item.updated" ? [event.turnItem] : [],
      );
      assert.deepStrictEqual(
        items
          .filter((item) => item.type === "assistant_message")
          .map((item) => item.type === "assistant_message" && item.text),
        ["Looking at the login form.", "Fixed the redirect."],
      );
      const command = items.findLast((item) => item.type === "command_execution");
      assert.strictEqual(command?.status, "completed");
      assert.strictEqual(command?.type === "command_execution" && command.output, "src/login.ts");
      // The prompt T3 sent is already the thread's user message.
      assert.isFalse(items.some((item) => item.type === "system_notice"));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("queues to a busy agent and waits for its own message before settling", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, {
        status: "working",
        dir: CWD,
        provider: "codex",
        entries: [user("Earlier work"), assistant("Earlier reply")],
      });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Next task"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(sent.body, { text: "Next task", mode: "queue" });

      // The earlier work wraps up and ends; am has not delivered the queued message yet.
      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(assistant("Earlier wrap-up"));
        agent.status = "idle";
      });
      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(user("Next task"), assistant("Next reply"));
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      const replies = harness.allEvents.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
          ? [event.turnItem.text]
          : [],
      );
      assert.deepStrictEqual(replies, ["Next reply"]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("surfaces a blocked agent without answering for it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { status: "idle", dir: CWD, provider: "claude", entries: [] });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Delete tmp"));
      yield* fake.update(AGENT, (agent) => {
        agent.status = "needs-attention";
        agent.statusReason = "Bash: rm -rf tmp";
        agent.entries.push(user("Delete tmp"));
      });
      yield* harness.takeEvent(
        "provider_session.updated",
        (event) => event.providerSession.status === "waiting",
      );
      const notice = yield* harness.takeItem((event) => event.turnItem.type === "system_notice");
      assert.include(notice.type === "system_notice" ? notice.message : "", "Bash: rm -rf tmp");
      assert.include(notice.type === "system_notice" ? notice.message : "", `am j ${AGENT}`);
      yield* fake.update(AGENT, (agent) => {
        agent.status = "idle";
        agent.statusReason = null;
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("completes only after its own prompt lands when another client got there first", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { status: "idle", dir: CWD, provider: "claude", entries: [] });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "My task"));
      yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);

      // Another client's prompt reached the agent between T3's status read and its send.
      yield* fake.update(AGENT, (agent) => {
        agent.status = "working";
        agent.entries.push(user("Other task"), assistant("Other reply"));
      });
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);
      yield* fake.update(AGENT, (agent) => {
        agent.status = "idle";
      });
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);

      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(user("My task"), assistant("My reply"));
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      const items = harness.allEvents.flatMap((event) =>
        event.type === "turn_item.updated" ? [event.turnItem] : [],
      );
      assert.deepStrictEqual(
        items.map((item) => (item.type === "assistant_message" ? item.text : item.type)),
        ["My reply"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("waits for work after its prompt lands while am still reports idle", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { status: "idle", dir: CWD, provider: "claude", entries: [] });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Quick check"));
      yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);

      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(user("Quick check"));
      });
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);
      // A second read is either another sync or an early finish's last read; both precede the reply.
      yield* fake.update(AGENT, () => {});
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);

      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(assistant("All good."));
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      assert.isTrue(
        harness.allEvents.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.text === "All good.",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("runs only in Full access", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const supervised = { ...runtimePolicy, runtimeMode: "approval-required" as const };
      const adapter = yield* makeAmAdapterV2({
        instanceId: INSTANCE_ID,
        settings,
        api: makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN }),
      });
      const opened = yield* adapter
        .openSession({
          threadId: THREAD_ID,
          providerSessionId: ProviderSessionId.make("session-am-supervised"),
          modelSelection,
          runtimePolicy: supervised,
        })
        .pipe(Effect.flip);
      assert.include(opened.message, "Full access");

      const harness = yield* makeHarness(fake);
      const started = yield* harness.runtime
        .startTurn({
          ...(yield* turnInput(harness.providerThread, "Fix login")),
          runtimePolicy: supervised,
        })
        .pipe(Effect.flip);
      assert.include(String(started.cause), "Full access");
      assert.isFalse(fake.agents.has(AGENT));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("steers behind its queued prompt, then with am's now mode, and interrupts with Esc", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { status: "idle", dir: CWD, provider: "claude", entries: [] });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Long job"));
      const started = yield* harness.takeEvent(
        "provider_turn.updated",
        (event) => event.providerTurn.status === "running",
      );
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run-1"),
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
        message: {
          messageId: MessageId.make("steer-1"),
          text: "Also update the docs",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const queued = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(queued.body, { text: "Long job", mode: "queue" });
      const behind = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(behind.body, { text: "Also update the docs", mode: "queue" });

      yield* fake.update(AGENT, (agent) => {
        agent.status = "working";
        agent.entries.push(user("Long job"), assistant("Starting the long job."));
      });
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run-1"),
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
        message: {
          messageId: MessageId.make("steer-2"),
          text: "Skip the tests",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const now = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(now.body, { text: "Skip the tests", mode: "now" });

      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(user("Also update the docs"), user("Skip the tests"));
      });
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* fake.takeCall("POST", `/api/agents/${AGENT}/interrupt`);
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      // Both messages came from T3, so neither shows as an outside message.
      assert.isFalse(
        harness.allEvents.some(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "system_notice",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("resumes a stopped agent from the thread's saved name and fails when it dies", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const saved = "t3-saved-abc123";
      fake.agents.set(saved, {
        status: "exited",
        dir: CWD,
        provider: "claude",
        entries: [user("Old"), assistant("Old reply")],
      });
      const now = yield* DateTime.now;
      const harness = yield* makeHarness(fake, {
        id: ProviderThreadId.make("provider-thread-saved"),
        driver: ProviderDriverKind.make("am"),
        providerInstanceId: INSTANCE_ID,
        providerSessionId: null,
        appThreadId: THREAD_ID,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("am"),
          nativeId: saved,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      });
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Continue", 2));
      yield* fake.takeCall("POST", `/api/agents/${saved}/resume`);
      yield* fake.takeCall("POST", `/api/agents/${saved}/messages`);
      yield* fake.update(saved, (agent) => {
        agent.status = "dead";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.include(terminal.failure?.message ?? "", saved);
      // History before this turn never re-renders into it.
      assert.isFalse(
        harness.allEvents.some(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
