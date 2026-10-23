import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { worktreeLeaseKey } from "./externalWorktree.ts";

it.layer(NodeServices.layer)("worktreeLeaseKey", (it) => {
  it.effect("gives every spelling of one worktree the same lease", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped({ prefix: "lease-key-" }));
      yield* fs.makeDirectory(`${root}/real/wt`, { recursive: true });
      yield* fs.symlink(`${root}/real`, `${root}/alias`);
      const key = (target: string) => worktreeLeaseKey({ fs, path }, target);
      for (const spelling of [`${root}/real/wt`, `${root}/alias/wt`, `${root}/alias/wt/`])
        assert.strictEqual(yield* key(spelling), `${root}/real/wt`);
      // A worktree already gone keys on its resolved spelling.
      assert.strictEqual(yield* key(`${root}/gone/./wt/`), `${root}/gone/wt`);
    }).pipe(Effect.scoped),
  );
});
