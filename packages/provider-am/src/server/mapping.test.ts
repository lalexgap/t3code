import { assert, describe, it } from "@effect/vitest";

import {
  amAgentName,
  amAgentRef,
  amApprovalChoices,
  amEntryStatus,
  amOptionForDecision,
  amToolPresentation,
  amTurnPhase,
  fleetEventConcerns,
  parseAmAgentRef,
} from "./mapping.ts";

describe("amAgentName", () => {
  it("is an am-valid, stable name that tells same-titled threads apart", () => {
    const first = amAgentName("thread-1", "Fix the login bug!");
    assert.match(first, /^[a-zA-Z0-9_-]+$/);
    assert.match(first, /^t3-fix-the-login-bug-[0-9a-z]{6}$/);
    assert.strictEqual(amAgentName("thread-1", "Fix the login bug!"), first);
    assert.notStrictEqual(amAgentName("thread-2", "Fix the login bug!"), first);
  });

  it("keeps long and empty titles short and valid", () => {
    const long = amAgentName("thread-1", "a ".repeat(80));
    assert.isAtMost(long.length, 3 + 24 + 1 + 6);
    assert.match(long, /^[a-zA-Z0-9_-]+$/);
    assert.match(amAgentName("thread-1", "  !!  "), /^t3-[0-9a-z]{6}$/);
    assert.match(amAgentName("thread-1"), /^t3-[0-9a-z]{6}$/);
  });
});

describe("amTurnPhase", () => {
  it("maps every am status to a turn phase", () => {
    assert.deepStrictEqual(
      ["starting", "working", "idle", "waiting", "needs-attention", "exited", "dead"].map(
        amTurnPhase,
      ),
      ["running", "running", "idle", "idle", "attention", "gone", "gone"],
    );
  });
});

describe("amAgentRef", () => {
  it("addresses a fleet host's agent as host:name and round-trips", () => {
    assert.strictEqual(amAgentRef("", "t3-a"), "t3-a");
    assert.strictEqual(amAgentRef("gapserver", "t3-a"), "gapserver:t3-a");
    assert.deepStrictEqual(parseAmAgentRef("gapserver:t3-a"), { host: "gapserver", name: "t3-a" });
    assert.deepStrictEqual(parseAmAgentRef("t3-a"), { host: undefined, name: "t3-a" });
  });
});

describe("amToolPresentation", () => {
  it("shows shell tools as commands with their command line", () => {
    assert.deepInclude(
      amToolPresentation({
        index: 3,
        kind: "tool",
        name: "Bash",
        input: '{"command":"ls -la","description":"List"}',
        output: "total 0",
      }),
      { type: "command_execution", input: "ls -la", output: "total 0" },
    );
    assert.deepInclude(
      amToolPresentation({
        index: 4,
        kind: "tool",
        name: "exec_command",
        input: '{"cmd":["git","status"]}',
      }),
      { type: "command_execution", input: "git status" },
    );
  });

  it("shows other tools with their decoded input", () => {
    assert.deepInclude(
      amToolPresentation({ index: 5, kind: "tool", name: "Read", input: '{"file_path":"a.ts"}' }),
      { type: "dynamic_tool", toolName: "Read", input: { file_path: "a.ts" } },
    );
    assert.deepInclude(
      amToolPresentation({ index: 6, kind: "tool", name: "Weird", input: "not json" }),
      { type: "dynamic_tool", input: "not json" },
    );
  });
});

describe("amEntryStatus", () => {
  it("keeps a tool open until its output lands or the turn settles", () => {
    const tool = { index: 1, kind: "tool" as const, name: "Bash", input: "{}" };
    assert.strictEqual(amEntryStatus(tool, false), "running");
    assert.strictEqual(amEntryStatus(tool, true), "completed");
    assert.strictEqual(amEntryStatus({ ...tool, output: "ok" }, false), "completed");
    assert.strictEqual(amEntryStatus({ ...tool, output: "boom", error: true }, false), "failed");
    assert.strictEqual(
      amEntryStatus({ index: 2, kind: "assistant", text: "hi" }, false),
      "completed",
    );
  });
});

describe("fleetEventConcerns", () => {
  it("wakes a local agent for its own events and for unnamed fleet changes", () => {
    assert.isTrue(fleetEventConcerns('{"type":"fleet","event":"stop","agent":"t3-a"}', "t3-a"));
    assert.isFalse(fleetEventConcerns('{"type":"fleet","event":"stop","agent":"other"}', "t3-a"));
    assert.isTrue(fleetEventConcerns('{"type":"fleet","event":"changed"}', "t3-a"));
    assert.isTrue(fleetEventConcerns("garbled", "t3-a"));
  });

  it("never wakes a fleet host's agent, whose events the local stream does not carry", () => {
    assert.isFalse(fleetEventConcerns('{"agent":"t3-a"}', "gapserver:t3-a"));
    assert.isFalse(fleetEventConcerns('{"type":"fleet","event":"changed"}', "gapserver:t3-a"));
  });
});

describe("amApprovalChoices", () => {
  it("keeps Codex's decisions on their own slots and leaves extra choices to the terminal", () => {
    const choices = amApprovalChoices([
      { id: "accept", label: "Yes, proceed", kind: "allow" },
      {
        id: "acceptWithExecpolicyAmendment",
        label: "Yes, and don't ask again for `npm test`",
        kind: "allow_always",
      },
      {
        id: "acceptForSession",
        label: "Yes, and don't ask again this session",
        kind: "allow_always",
      },
      { id: "networkPolicy-3", label: "No, and always block network access", kind: "deny" },
      { id: "decline", label: "No, continue without it", kind: "deny" },
      { id: "cancel", label: "No, and tell Codex what to do differently", kind: "deny" },
      { id: "later", label: "Ask me later", kind: "snooze" },
    ]);
    assert.deepStrictEqual(choices.options, [
      { decision: "accept", label: "Yes, proceed" },
      { decision: "acceptAlways", label: "Yes, and don't ask again for `npm test`" },
      { decision: "acceptForSession", label: "Yes, and don't ask again this session" },
      { decision: "decline", label: "No, continue without it" },
      { decision: "cancel", label: "No, and tell Codex what to do differently" },
    ]);
    assert.strictEqual(choices.optionIds.get("acceptAlways"), "acceptWithExecpolicyAmendment");
    assert.strictEqual(choices.hidden, 2);
  });

  it("declines with the agent's one deny whichever decline T3 sends", () => {
    const { optionIds } = amApprovalChoices([
      { id: "allow", label: "Yes", kind: "allow" },
      { id: "deny", label: "No", kind: "deny" },
    ]);
    assert.strictEqual(amOptionForDecision(optionIds, "cancel"), "deny");
    assert.strictEqual(amOptionForDecision(optionIds, "decline"), "deny");
    assert.strictEqual(amOptionForDecision(optionIds, "acceptAlways"), undefined);
  });
});
