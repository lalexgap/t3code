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
  type OrchestrationV2ProviderTurn,
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
import { FAKE_TOKEN, makeFakeAm, type FakeAgent, type FakeEntry } from "./fakeAm.testkit.ts";
import { amAgentName } from "./mapping.ts";
import { amConversationHead } from "./mirrorPlan.ts";

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
const decodeSettings = Schema.decodeSync(AmSettings);
const settings = decodeSettings({ enabled: true, apiToken: FAKE_TOKEN });
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
  instanceSettings: AmSettings = settings,
) {
  const api = makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN });
  // Turns must settle from am's event stream alone.
  const adapter = yield* makeAmAdapterV2({
    instanceId: INSTANCE_ID,
    settings: instanceSettings,
    api,
    pollIntervalMs: 3_600_000,
    settleGraceMs: 1_000,
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
  const items = () =>
    allEvents.flatMap((event) => (event.type === "turn_item.updated" ? [event.turnItem] : []));
  const replies = () =>
    items().flatMap((item) => (item.type === "assistant_message" ? [item.text] : []));
  const notices = () =>
    items().flatMap((item) => (item.type === "system_notice" ? [item.message] : []));
  return {
    adapter,
    runtime,
    providerThread,
    takeEvent,
    takeItem,
    allEvents,
    items,
    replies,
    notices,
  };
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
const idleAgent = (entries: Array<FakeEntry> = []): FakeAgent => ({
  status: "idle",
  dir: CWD,
  provider: "claude",
  entries,
});

/** am delivers the message: the transcript length is recorded before the agent writes more. */
const deliver = (fake: Fake, id: string, ...entries: Array<FakeEntry>) =>
  fake.receipt(id, (message, agent) => {
    message.state = "delivered";
    message.transcriptIndex = agent.entries.length;
    agent.status = "working";
    agent.entries.push(...entries);
  });
const complete = (fake: Fake, id: string, ...entries: Array<FakeEntry>) =>
  fake.receipt(id, (message, agent) => {
    agent.entries.push(...entries);
    message.state = "done";
    agent.status = "idle";
  });
const steer = (
  harness: Effect.Success<ReturnType<typeof makeHarness>>,
  providerTurnId: OrchestrationV2ProviderTurn["id"],
  text: string,
) =>
  harness.runtime.steerTurn({
    threadId: THREAD_ID,
    runId: RunId.make("run-1"),
    providerThread: harness.providerThread,
    providerTurnId,
    message: {
      messageId: MessageId.make(`steer-${text}`),
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
  });

describe("AmAdapterV2", () => {
  it.live("starts the agent with no task, sends the prompt, and settles on its receipt", () =>
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
        provider: "claude",
      });
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(sent.body, { text: "Fix login", mode: "queue" });

      yield* deliver(fake, sent.messageId!, user("Fix login"), assistant("Looking."), {
        kind: "tool",
        name: "Bash",
        input: '{"command":"grep -r login src"}',
      });
      const running = yield* harness.takeItem(
        (event) => event.turnItem.type === "command_execution",
      );
      assert.strictEqual(running.status, "running");
      assert.strictEqual(
        running.type === "command_execution" && running.input,
        "grep -r login src",
      );

      // An idle status alone settles nothing; the receipt does.
      yield* fake.update(AGENT, (agent) => {
        agent.entries[2] = { ...agent.entries[2]!, output: "src/login.ts" };
        agent.status = "idle";
      });
      yield* complete(fake, sent.messageId!, assistant("Fixed the redirect."));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");

      assert.deepStrictEqual(harness.replies(), ["Looking.", "Fixed the redirect."]);
      const command = harness.items().findLast((item) => item.type === "command_execution");
      assert.strictEqual(command?.status, "completed");
      assert.strictEqual(command?.type === "command_execution" && command.output, "src/login.ts");
      // The prompt T3 sent is already the thread's user message.
      assert.deepStrictEqual(harness.notices(), []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  // Replays a live run (am main e2c459b): Claude's inbox took the prompt with no transcript entry,
  // and its Stop hook marked the receipt done before Claude flushed the reply to its session
  // file. So the first receipt T3 sees is already `done` at index 0, over a transcript without
  // the reply, and "PONG" lands at index 0 a moment later with no wake. A typed prompt shows
  // its own entry first, putting the reply at index 1.
  it.live.each(["inbox", "typed"] as const)(
    "waits for a reply flushed after the receipt is done (%s delivery)",
    (shape) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeAm();
        const harness = yield* makeHarness(fake);
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Say PONG"));
        const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
        const transcriptRead = fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);
        yield* transcriptRead;
        yield* fake.receipt(sent.messageId!, (message, agent) => {
          message.state = "done";
          message.transcriptIndex = 0;
          agent.status = "idle";
          if (shape === "typed") agent.entries.push(user("Say PONG"));
        });
        // Two reads see the done receipt without the reply, which was once all the run got.
        yield* transcriptRead;
        yield* transcriptRead;
        fake.agents.get(AGENT)!.entries.push(assistant("PONG"));
        yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
        assert.deepStrictEqual(harness.replies(), ["PONG"]);
        assert.deepStrictEqual(harness.notices(), []);
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("waits for a steer's reply flushed after its receipt is done", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Long job"));
      const prompt = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* steer(harness, started.providerTurn.id, "Also update the docs");
      const behind = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, prompt.messageId!, user("Long job"), assistant("Halfway."));
      yield* complete(fake, prompt.messageId!);
      // The steer arrives through the inbox with no entry, and is done before its reply is on disk.
      const transcriptRead = fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);
      yield* deliver(fake, behind.messageId!);
      yield* complete(fake, behind.messageId!);
      yield* transcriptRead;
      yield* transcriptRead;
      fake.agents.get(AGENT)!.entries.push(assistant("Docs updated."));
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      assert.deepStrictEqual(harness.replies(), ["Halfway.", "Docs updated."]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("completes a delivered turn that produced no output when am reports it done", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent([user("Old"), assistant("Old reply")]));
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Quiet check"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, sent.messageId!);
      yield* complete(fake, sent.messageId!);
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");
      assert.deepStrictEqual(harness.items(), []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("follows a prompt a busy agent receives with no transcript entry for it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, {
        ...idleAgent([user("Earlier work"), assistant("Earlier reply")]),
        status: "working",
      });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Next task"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(sent.body, { text: "Next task", mode: "queue" });

      // The earlier work wraps up and idles while T3's message is still queued.
      yield* fake.update(AGENT, (agent) => {
        agent.entries.push(assistant("Earlier wrap-up"));
        agent.status = "idle";
      });
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/transcript`);
      // Claude's Stop-hook inbox hands the message over without a visible user entry.
      yield* deliver(fake, sent.messageId!, assistant("Next reply"));
      yield* complete(fake, sent.messageId!);
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      assert.deepStrictEqual(harness.replies(), ["Next reply"]);
      assert.deepStrictEqual(harness.notices(), []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("shows a peer's message inside the turn as a note, even with the prompt's text", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "yes"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, sent.messageId!, user("yes"), assistant("On it."));
      yield* complete(fake, sent.messageId!, user("[am · from lead] yes"), assistant("Noted."));
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      assert.deepStrictEqual(harness.notices(), ["[am · from lead] yes"]);
      assert.deepStrictEqual(harness.replies(), ["On it.", "Noted."]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("withdraws a still-queued prompt on Stop instead of interrupting other work", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { ...idleAgent(), status: "working" });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Later"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* fake.takeCall("DELETE", `/api/agents/${AGENT}/messages/${sent.messageId}`);
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.strictEqual(fake.messages.get(sent.messageId!)?.state, "cancelled");
      assert.isFalse(fake.log.some((call) => call.path.endsWith("/interrupt")));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("interrupts a delivered prompt on Stop by its message id", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Long job"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Long job"), assistant("Starting."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      const interrupt = yield* fake.takeCall("POST", `/api/agents/${AGENT}/interrupt`);
      assert.deepStrictEqual(interrupt.body, { messageId: sent.messageId });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.deepStrictEqual(fake.escapes, [AGENT]);
      assert.isFalse(fake.log.some((call) => call.method === "DELETE"));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("retries Stop while am's mailbox is busy or the turn has not started", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Behind"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Behind"));
      yield* harness.takeEvent("provider_session.updated");
      const message = fake.messages.get(sent.messageId!)!;
      message.busyInterrupts = 1;
      message.unstartedInterrupts = 1;
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.strictEqual(fake.log.filter((call) => call.path.endsWith("/interrupt")).length, 3);
      assert.deepStrictEqual(fake.escapes, [AGENT]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("interrupts when am delivered the prompt just before Stop withdrew it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Race"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/messages/${sent.messageId}`);
      // Delivered without a wake, so T3 still believes the prompt is queued.
      fake.messages.get(sent.messageId!)!.state = "delivered";
      fake.messages.get(sent.messageId!)!.transcriptIndex = 0;
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* fake.takeCall("DELETE", `/api/agents/${AGENT}/messages/${sent.messageId}`);
      const interrupt = yield* fake.takeCall("POST", `/api/agents/${AGENT}/interrupt`);
      assert.deepStrictEqual(interrupt.body, { messageId: sent.messageId });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("sends no Esc on Stop when am answers that the prompt is already done", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Quick"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Quick"), assistant("Working."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      // Done without a wake, while a peer's turn may already be running.
      fake.messages.get(sent.messageId!)!.state = "done";
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");
      assert.deepStrictEqual(
        fake.log.filter((call) => call.path.endsWith("/interrupt")).map((call) => call.body),
        [{ messageId: sent.messageId }],
      );
      assert.deepStrictEqual(fake.escapes, []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("waits out a cancel am answers mid-delivery, then interrupts", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Handing over"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/messages/${sent.messageId}`);
      fake.messages.get(sent.messageId!)!.deliveringOnCancel = true;
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      const path = `/api/agents/${AGENT}/messages/${sent.messageId}`;
      assert.deepStrictEqual(
        fake.log
          .filter((call) => call.method !== "GET" || call.path === path)
          .slice(-3)
          .map((call) => `${call.method} ${call.path}`),
        [`DELETE ${path}`, `GET ${path}`, `POST /api/agents/${AGENT}/interrupt`],
      );
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("retries a withdraw while am's mailbox is busy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { ...idleAgent(), status: "working" });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Later"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      fake.messages.get(sent.messageId!)!.busyCancels = 2;
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.strictEqual(fake.log.filter((call) => call.method === "DELETE").length, 3);
      assert.strictEqual(fake.messages.get(sent.messageId!)?.state, "cancelled");
      assert.isFalse(fake.log.some((call) => call.path.endsWith("/interrupt")));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("sends no second Esc when am says the turn was already interrupted", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Twice"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Twice"), assistant("Working."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      // Another client's Stop pressed Esc, and am has not seen the prompt return yet.
      fake.messages.get(sent.messageId!)!.escaped = true;
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.strictEqual(fake.log.filter((call) => call.path.endsWith("/interrupt")).length, 1);
      assert.deepStrictEqual(fake.escapes, []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("ends the run on Stop without an Esc when am says the session is gone", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Gone"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Gone"));
      yield* harness.takeEvent("provider_session.updated");
      fake.messages.get(sent.messageId!)!.conflict = {
        error: `agent "${AGENT}" has no live tmux session`,
        reason: "no_session",
      };
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.strictEqual(fake.log.filter((call) => call.path.endsWith("/interrupt")).length, 1);
      assert.deepStrictEqual(fake.escapes, []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails Stop at once with am's text on a refusal it cannot act on", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Refused"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Refused"));
      yield* harness.takeEvent("provider_session.updated");
      const refusals = [
        { error: "remote am too old on gapserver (unknown command)", reason: "remote_too_old" },
        // An am from before reasons.
        { error: "the agent is in a strange state" },
      ];
      for (const refusal of refusals) {
        fake.messages.get(sent.messageId!)!.conflict = refusal;
        const failure = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: started.providerTurn.id,
          })
          .pipe(Effect.flip);
        assert.include(String(failure.cause), refusal.error);
      }
      assert.strictEqual(fake.log.filter((call) => call.path.endsWith("/interrupt")).length, 2);
      assert.deepStrictEqual(fake.escapes, []);
      assert.isFalse(harness.allEvents.some((event) => event.type === "turn.terminal"));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails a withdraw at once when am refuses the cancel without a reason", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { ...idleAgent(), status: "working" });
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Later"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      fake.messages.get(sent.messageId!)!.conflict = { error: "cancel is not supported here" };
      const failure = yield* harness.runtime
        .interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: started.providerTurn.id,
        })
        .pipe(Effect.flip);
      assert.include(String(failure.cause), "cancel is not supported here");
      assert.strictEqual(fake.log.filter((call) => call.method === "DELETE").length, 1);
      assert.strictEqual(fake.messages.get(sent.messageId!)?.state, "queued");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("ends the run as interrupted when the turn is aborted outside T3", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Long job"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, sent.messageId!, user("Long job"), assistant("Starting."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      // `am interrupt` in a terminal: am ends the turn once the prompt is back.
      yield* fake.receipt(sent.messageId!, (message, agent) => {
        message.state = "done";
        message.interrupted = true;
        agent.status = "idle";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "interrupted");
      assert.deepStrictEqual(harness.replies(), ["Starting."]);
      assert.isFalse(fake.log.some((call) => call.path.endsWith("/interrupt")));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("reports Stop as an interrupt when am already ended the turn after an Esc", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Aborted"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, sent.messageId!, user("Aborted"), assistant("Working."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      // Aborted elsewhere without a wake reaching T3 first.
      const message = fake.messages.get(sent.messageId!)!;
      message.state = "done";
      message.interrupted = true;
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "interrupted");
      assert.deepStrictEqual(fake.escapes, []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("delivers a message sent after Stop once am drains the agent's queue", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "First try"));
      const first = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, first.messageId!, user("First try"), assistant("Going the wrong way."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "interrupted");
      assert.isTrue(fake.messages.get(first.messageId!)?.interrupted);
      assert.strictEqual(fake.agents.get(AGENT)?.status, "idle");

      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Second try", 2));
      const second = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(second.body, { text: "Second try", mode: "queue" });
      const running = yield* harness.takeEvent(
        "provider_turn.updated",
        (event) => event.providerTurn.status === "running",
      );
      assert.notStrictEqual(running.providerTurn.id, started.providerTurn.id);
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/messages/${second.messageId}`);
      // am drains the queue into the idle agent.
      yield* deliver(fake, second.messageId!, user("Second try"), assistant("Done right."));
      yield* complete(fake, second.messageId!);
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");
      assert.strictEqual(terminal.providerTurnId, running.providerTurn.id);
      assert.deepStrictEqual(harness.replies().slice(-1), ["Done right."]);
      assert.deepStrictEqual(fake.escapes, [AGENT]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails the run when am reports the message failed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Doomed"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* fake.receipt(sent.messageId!, (message) => {
        message.state = "failed";
        message.error = "the pane never accepted input";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.include(terminal.failure?.message ?? "", "the pane never accepted input");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails the run when the session ends after am delivered the prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Crashy"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, sent.messageId!, user("Crashy"), assistant("Starting."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      yield* fake.receipt(sent.messageId!, (message, agent) => {
        message.state = "failed";
        message.error = "the tmux session ended mid-turn";
        agent.status = "idle";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.include(terminal.failure?.message ?? "", "the tmux session ended mid-turn");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails the run when a delivered steering message fails", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Long job"));
      const prompt = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");
      yield* deliver(fake, prompt.messageId!, user("Long job"), assistant("Starting."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      yield* steer(harness, started.providerTurn.id, "Faster");
      const now = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, now.messageId!, user("Faster"));
      yield* fake.takeCall("GET", `/api/agents/${AGENT}/messages/${now.messageId}`);
      yield* fake.receipt(now.messageId!, (message) => {
        message.state = "failed";
        message.error = "the agent exited";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.include(terminal.failure?.message ?? "", "the agent exited");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses to send to an am without message receipts", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm({ messageReceipts: false });
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      const started = yield* harness.runtime
        .startTurn(yield* turnInput(harness.providerThread, "Fix login"))
        .pipe(Effect.flip);
      assert.include(String(started.cause), "Update am");
      assert.isFalse(fake.log.some((call) => call.path.endsWith("/messages")));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses to send to a fleet host whose am has no message receipts", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm({ messageReceipts: false });
      const remote = decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, host: "gapserver" });
      const harness = yield* makeHarness(fake, undefined, remote);
      const started = yield* harness.runtime
        .startTurn(yield* turnInput(harness.providerThread, "Remote job"))
        .pipe(Effect.flip);
      assert.include(String(started.cause), "gapserver");
      assert.include(String(started.cause), "Update am");
      assert.isFalse(fake.log.some((call) => call.path.endsWith("/messages")));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses a Fleet host that names the machine running am serve", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      for (const host of ["local", "fake-local"]) {
        const adapter = yield* makeAmAdapterV2({
          instanceId: INSTANCE_ID,
          settings: decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, host }),
          api: makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN }),
        });
        const opened = yield* adapter
          .openSession({
            threadId: THREAD_ID,
            providerSessionId: ProviderSessionId.make(`session-am-${host}`),
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.flip);
        assert.include(opened.message, "Leave Fleet host blank");
      }
      assert.strictEqual(fake.agents.size, 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails the run when am loses the receipt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Moved"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      // `am move` leaves the receipt behind on the old host.
      yield* fake.update(AGENT, () => fake.messages.delete(sent.messageId!));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.include(terminal.failure?.message ?? "", "no longer has a receipt");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("steers within the run and ends it only after every steering message is done", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Long job"));
      const prompt = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      const started = yield* harness.takeEvent("provider_turn.updated");

      // Before the prompt is delivered, steering queues behind it.
      yield* steer(harness, started.providerTurn.id, "Also update the docs");
      const behind = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(behind.body, { text: "Also update the docs", mode: "queue" });

      yield* deliver(fake, prompt.messageId!, user("Long job"), assistant("Starting."));
      yield* harness.takeItem((event) => event.turnItem.type === "assistant_message");
      yield* steer(harness, started.providerTurn.id, "Skip the tests");
      const now = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      assert.deepStrictEqual(now.body, { text: "Skip the tests", mode: "now" });
      // A `now` message is typed into the busy agent and recorded at its next step.
      yield* deliver(fake, now.messageId!, assistant("Halfway."), user("Skip the tests"));
      yield* complete(fake, prompt.messageId!);
      yield* complete(fake, now.messageId!);

      yield* deliver(fake, behind.messageId!, user("Also update the docs"));
      yield* complete(fake, behind.messageId!, assistant("Docs updated."));
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      assert.deepStrictEqual(harness.replies(), ["Starting.", "Halfway.", "Docs updated."]);
      // Every user entry came from T3, so none shows as an outside message.
      assert.deepStrictEqual(harness.notices(), []);
      assert.strictEqual(
        harness.allEvents.filter((event) => event.type === "turn.terminal").length,
        1,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("drives a fleet host's agent as host:name without workspace checkpoints", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const remote = decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, host: "gapserver" });
      const harness = yield* makeHarness(fake, undefined, remote);
      const ref = `gapserver:${AGENT}`;
      assert.strictEqual(harness.providerThread.nativeThreadRef?.nativeId, ref);
      const capabilities = yield* harness.adapter.getCapabilities();
      assert.isFalse(capabilities.checkpointing.appCanCheckpointFilesystem);

      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Remote job"));
      const created = yield* fake.takeCall("POST", "/api/agents");
      assert.deepStrictEqual(created.body, {
        name: AGENT,
        host: "gapserver",
        dir: CWD,
        inPlace: true,
        provider: "claude",
      });
      const sent = yield* fake.takeCall("POST", `/api/agents/${encodeURIComponent(ref)}/messages`);
      yield* deliver(fake, sent.messageId!, user("Remote job"), assistant("Done remotely."));
      yield* complete(fake, sent.messageId!);
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
      assert.deepStrictEqual(harness.replies(), ["Done remotely."]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses to adopt a local agent that works in another directory", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, { ...idleAgent(), dir: "/elsewhere" });
      const harness = yield* makeHarness(fake);
      const started = yield* harness.runtime
        .startTurn(yield* turnInput(harness.providerThread, "Fix login"))
        .pipe(Effect.flip);
      assert.include(String(started.cause), "/elsewhere");
      assert.isFalse(fake.log.some((call) => call.path.endsWith("/messages")));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("surfaces a blocked agent without answering for it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent());
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Delete tmp"));
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, sent.messageId!, user("Delete tmp"));
      yield* fake.update(AGENT, (agent) => {
        agent.status = "needs-attention";
        agent.statusReason = "Bash: rm -rf tmp";
      });
      yield* harness.takeEvent(
        "provider_session.updated",
        (event) => event.providerSession.status === "waiting",
      );
      const notice = yield* harness.takeItem((event) => event.turnItem.type === "system_notice");
      assert.include(notice.type === "system_notice" ? notice.message : "", "Bash: rm -rf tmp");
      assert.include(notice.type === "system_notice" ? notice.message : "", `am j ${AGENT}`);
      yield* complete(fake, sent.messageId!);
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");
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

  it.live("refuses an API URL that is not https or loopback http", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      for (const apiUrl of ["file:///tmp", "http://am.example:8787"]) {
        const adapter = yield* makeAmAdapterV2({
          instanceId: INSTANCE_ID,
          settings: decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, apiUrl }),
          api: makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN }),
        });
        const opened = yield* adapter
          .openSession({
            threadId: THREAD_ID,
            providerSessionId: ProviderSessionId.make("session-am-url"),
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.flip);
        assert.include(opened.message, "https://");
      }
      assert.deepStrictEqual(fake.log, []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses a saved thread whose agent is on another host than the Fleet host", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const now = yield* DateTime.now;
      const remote = decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, host: "gapserver" });
      const failure = yield* makeHarness(
        fake,
        {
          id: ProviderThreadId.make("provider-thread-local"),
          driver: ProviderDriverKind.make("am"),
          providerInstanceId: INSTANCE_ID,
          providerSessionId: null,
          appThreadId: THREAD_ID,
          ownerNodeId: null,
          nativeThreadRef: {
            driver: ProviderDriverKind.make("am"),
            nativeId: AGENT,
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
        },
        remote,
      ).pipe(Effect.flip);
      assert.include(String((failure as { cause?: unknown }).cause), "Fleet host");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("follows work T3 did not start, then a T3 turn adds only what is new", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      fake.agents.set(AGENT, idleAgent([user("Old"), assistant("Old reply")]));
      const now = yield* DateTime.now;
      const harness = yield* makeHarness(fake, {
        id: ProviderThreadId.make("provider-thread-imported"),
        driver: ProviderDriverKind.make("am"),
        providerInstanceId: INSTANCE_ID,
        providerSessionId: null,
        appThreadId: THREAD_ID,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("am"),
          nativeId: AGENT,
          strength: "strong",
        },
        // Imported with its history, so the thread already shows the first two entries.
        nativeConversationHeadRef: amConversationHead(AGENT, 2),
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      });
      const heads = () =>
        harness.allEvents.flatMap((event) =>
          event.type === "provider_thread.updated"
            ? [event.providerThread.nativeConversationHeadRef?.ordinal]
            : [],
        );
      const repliesById = () =>
        new Map(
          harness
            .items()
            .flatMap((item) => (item.type === "assistant_message" ? [[item.id, item.text]] : [])),
        );

      // A peer's message set the agent working, so the fleet mirror asked for a run to follow it:
      // a provider-created message, which T3 never sends to the agent.
      const working = fake.agents.get(AGENT)!;
      working.status = "working";
      working.entries.push(user("[am · from reviewer] check CI"), {
        kind: "tool",
        name: "Bash",
        input: '{"command":"gh pr checks"}',
      });
      const observed = yield* turnInput(harness.providerThread, "Activity outside T3 Code");
      yield* harness.runtime.startTurn({
        ...observed,
        message: { ...observed.message, createdBy: "agent", creationSource: "provider" },
      });
      yield* harness.takeItem((event) => event.turnItem.type === "command_execution");
      yield* fake.update(AGENT, (agent) => {
        agent.entries[3] = { ...agent.entries[3]!, output: "all green" };
        agent.entries.push(assistant("CI is green."));
        agent.status = "idle";
      });
      const observedEnd = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(observedEnd.status, "completed");
      assert.isFalse(fake.log.some((call) => call.method === "POST"));
      assert.deepStrictEqual([...repliesById().values()], ["CI is green."]);
      assert.deepStrictEqual(harness.notices(), ["[am · from reviewer] check CI"]);
      const afterObserve = harness.allEvents.findLast(
        (event) => event.type === "provider_thread.updated",
      );
      assert.strictEqual(heads().at(-1), 5);

      // Between runs someone types into the agent's terminal and it answers.
      fake.agents
        .get(AGENT)!
        .entries.push(user("Typed in the terminal"), assistant("Terminal reply."));
      yield* harness.runtime.startTurn(
        yield* turnInput(
          afterObserve?.type === "provider_thread.updated"
            ? afterObserve.providerThread
            : harness.providerThread,
          "Next step",
          2,
        ),
      );
      const sent = yield* fake.takeCall("POST", `/api/agents/${AGENT}/messages`);
      yield* deliver(fake, sent.messageId!, user("Next step"), assistant("Next reply."));
      yield* complete(fake, sent.messageId!);
      yield* harness.takeEvent("turn.terminal", (event) => event.status === "completed");

      // Every entry after the head shows once, in the run that saw it.
      assert.deepStrictEqual(
        [...repliesById().values()],
        ["CI is green.", "Terminal reply.", "Next reply."],
      );
      assert.deepStrictEqual(harness.notices(), [
        "[am · from reviewer] check CI",
        "Typed in the terminal",
      ]);
      assert.strictEqual(heads().at(-1), 9);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("resumes a stopped agent from the thread's saved name and fails when it dies", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const saved = "t3-saved-abc123";
      fake.agents.set(saved, {
        ...idleAgent([user("Old"), assistant("Old reply")]),
        status: "exited",
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
      const sent = yield* fake.takeCall("POST", `/api/agents/${saved}/messages`);
      yield* fake.update(saved, (agent) => {
        agent.status = "dead";
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.include(terminal.failure?.message ?? "", saved);
      // The queued prompt is withdrawn so a later resume does not run it outside any run.
      assert.strictEqual(fake.messages.get(sent.messageId!)?.state, "cancelled");
      // History before this turn never re-renders into it.
      assert.deepStrictEqual(harness.replies(), []);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
