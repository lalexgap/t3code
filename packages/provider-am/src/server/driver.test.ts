import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { amGitRoot } from "./driver.ts";

const encoder = new TextEncoder();

const gitHandle = (input: { readonly stdout?: string; readonly exitCode?: number } | "hangs") =>
  ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_001),
    exitCode:
      input === "hangs"
        ? Effect.never
        : Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(input === "hangs"),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout:
      input !== "hangs" && input.stdout
        ? Stream.succeed(encoder.encode(input.stdout))
        : Stream.empty,
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });

const withGit = (
  path: string,
  answer: (
    args: ReadonlyArray<string>,
  ) => ReturnType<ChildProcessSpawner.ChildProcessSpawner["Service"]["spawn"]>,
) =>
  amGitRoot(path).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make((command) =>
        answer(ChildProcess.isStandardCommand(command) ? command.args : []),
      ),
    ),
  );

describe("amGitRoot", () => {
  it.effect("finds the repository in one call, with git's fsmonitor off", () =>
    Effect.gen(function* () {
      let seen: ReadonlyArray<string> = [];
      const repo = yield* withGit("/home/me/app/ui", (args) => {
        seen = args;
        return Effect.succeed(gitHandle({ stdout: "ui/\n/home/me/app/.git\n/home/me/app/.git\n" }));
      });
      assert.deepStrictEqual(repo, { root: "/home/me/app", worktree: null });
      assert.deepStrictEqual(seen, [
        "-c",
        "core.fsmonitor=false",
        "-C",
        "/home/me/app/ui",
        "rev-parse",
        "--show-prefix",
        "--path-format=absolute",
        "--git-dir",
        "--git-common-dir",
      ]);
    }),
  );

  it.effect("finds a linked worktree's main repository", () =>
    Effect.gen(function* () {
      const repo = yield* withGit("/home/me/wt", () =>
        Effect.succeed(
          gitHandle({ stdout: "\n/home/me/app/.git/worktrees/wt\n/home/me/app/.git\n" }),
        ),
      );
      assert.deepStrictEqual(repo, { root: "/home/me/app", worktree: "/home/me/wt" });
    }),
  );

  it.effect("reports no repository only when git exits cleanly saying so", () =>
    Effect.gen(function* () {
      assert.isNull(
        yield* withGit("/home/me/notes", () => Effect.succeed(gitHandle({ exitCode: 128 }))),
      );
    }),
  );

  it.effect("fails when git cannot be spawned", () =>
    Effect.gen(function* () {
      const exit = yield* withGit("/home/me/app/ui", () =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
          }),
        ),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
    }),
  );

  it.effect("fails when git does not answer in time", () =>
    Effect.gen(function* () {
      const fiber = yield* withGit("/home/me/app/ui", () =>
        Effect.succeed(gitHandle("hangs")),
      ).pipe(Effect.exit, Effect.forkChild);
      yield* TestClock.adjust("5 seconds");
      assert.isTrue(Exit.isFailure(yield* Fiber.join(fiber)));
    }),
  );
});
