import { assert, describe, it } from "@effect/vitest";

import {
  amAgentName,
  amEntryStatus,
  amToolPresentation,
  amTurnPhase,
  fleetEventConcerns,
  isSentMessage,
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

describe("isSentMessage", () => {
  it("matches a long delivered message through wrapping and whitespace changes", () => {
    const sent = ["Fix the redirect bug\n\nin login.ts and add a test"];
    assert.isTrue(isSentMessage("<prompt>Fix the redirect bug in login.ts and add a test", sent));
    assert.isTrue(isSentMessage("Fix the redirect bug in login.ts", sent));
    assert.isFalse(isSentMessage("Something else entirely", sent));
    assert.isFalse(isSentMessage("   ", sent));
  });

  it("matches a short message only exactly", () => {
    assert.isTrue(isSentMessage(" yes\n", ["yes"]));
    assert.isFalse(isSentMessage("yesterday the build broke", ["yes"]));
    assert.isFalse(isSentMessage("yes", ["yesterday the build broke again"]));
  });

  it("sees through am's attribution envelope", () => {
    assert.isTrue(isSentMessage("[am · from lead] yes", ["yes"]));
    assert.isFalse(isSentMessage("[am · from lead] yesterday", ["yes"]));
    assert.isTrue(isSentMessage("[am · from lead] yes", ["[am · from lead] yes"]));
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
  it("wakes for this agent's events and for unnamed fleet changes", () => {
    assert.isTrue(fleetEventConcerns('{"type":"fleet","event":"stop","agent":"t3-a"}', "t3-a"));
    assert.isFalse(fleetEventConcerns('{"type":"fleet","event":"stop","agent":"other"}', "t3-a"));
    assert.isTrue(fleetEventConcerns('{"type":"fleet","event":"changed"}', "t3-a"));
    assert.isTrue(fleetEventConcerns("garbled", "t3-a"));
  });
});
