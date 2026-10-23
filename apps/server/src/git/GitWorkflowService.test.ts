import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";

import * as ServerConfig from "../config.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as GitManager from "./GitManager.ts";
import * as GitWorkflowService from "./GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";

const testLayer = (externalWorktrees: Array<string> = []) =>
  GitWorkflowService.layer.pipe(
    Layer.provide(
      Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
        resolve: () => Effect.succeed({ kind: "git" } as VcsDriverRegistry.VcsDriverHandle),
      }),
    ),
    Layer.provide(Layer.mock(GitManager.GitManager)({})),
    Layer.provide(
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getExternalWorktreePaths: () => Effect.succeed(externalWorktrees),
      }),
    ),
    Layer.provideMerge(GitVcsDriver.layer),
    Layer.provideMerge(VcsProcess.layer),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-ref-named-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
    Layer.provideMerge(NodeServices.layer),
  );

it.layer(NodeServices.layer)("GitWorkflowService", (it) => {
  it.effect("finds a name among the refs of every namespace", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ref-named-" });
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const driver = yield* GitVcsDriver.GitVcsDriver;
      for (const args of [
        ["init", "-b", "main"],
        ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
        ["update-ref", "refs/remotes/upstream/feature/topic", "HEAD"],
        ["symbolic-ref", "refs/remotes/origin/alias", "refs/heads/main"],
        ["update-ref", "refs/mirror/origin/mirrored", "HEAD"],
      ]) {
        yield* driver.execute({ operation: "test.setupRepo", cwd, args });
      }
      const found = yield* Effect.forEach(
        ["main", "feature/topic", "alias", "mirrored", "t3/renamed", "feature/*"],
        (refName) => workflow.hasRefNamed({ cwd, refName }),
      );
      assert.deepEqual(found, [true, true, true, true, false, false]);
    }).pipe(Effect.scoped, Effect.provide(testLayer())),
  );

  it.effect("refuses to remove a worktree another tool created", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-external-wt-" });
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const refused = yield* Effect.flip(workflow.removeWorktree({ cwd, path: "/repo/agent-wt" }));
      assert.strictEqual(refused.reason, "worktree_owned_elsewhere");
      // Any other path still reaches git, which fails here on its own terms.
      const attempted = yield* Effect.flip(workflow.removeWorktree({ cwd, path: "/repo/own-wt" }));
      assert.notStrictEqual(attempted.reason, "worktree_owned_elsewhere");
    }).pipe(Effect.scoped, Effect.provide(testLayer(["/repo/agent-wt"]))),
  );

  const flagged: Array<string> = [];
  it.effect("refuses another tool's worktree reached through a symlinked parent", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.realPath(
        yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-external-alias-" }),
      );
      yield* fileSystem.makeDirectory(`${root}/real/repo/wt`, { recursive: true });
      yield* fileSystem.makeDirectory(`${root}/linked`);
      yield* fileSystem.symlink(`${root}/real/repo`, `${root}/linked/repo`);
      flagged.push(`${root}/real/repo/wt`);
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const refused = yield* Effect.flip(
        workflow.removeWorktree({ cwd: root, path: `${root}/linked/repo/wt` }),
      );
      assert.strictEqual(refused.reason, "worktree_owned_elsewhere");
    }).pipe(Effect.scoped, Effect.provide(testLayer(flagged))),
  );
});
