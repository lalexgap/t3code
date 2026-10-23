import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import { normalizeWorktreePathForComparison } from "@t3tools/shared/path";

import type * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";

/**
 * The key a worktree's removal and an adoption that binds it share a workspace lease under: its
 * physical path, or its resolved spelling once it no longer exists.
 */
export const worktreeLeaseKey = Effect.fnUntraced(function* (
  services: { readonly fs: FileSystem.FileSystem; readonly path: Path.Path },
  target: string,
) {
  const resolved = services.path.resolve(target);
  return yield* services.fs.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
});

/**
 * Whether a thread marks `target` as another tool's worktree, under any spelling or symlinked
 * alias. A path that no longer resolves is compared by spelling alone.
 */
export const isExternalWorktreePath = Effect.fn("ExternalWorktree.isExternalWorktreePath")(
  function* (
    services: {
      readonly projections: ProjectionStore.ProjectionStoreV2["Service"];
      readonly fs: FileSystem.FileSystem;
      readonly path: Path.Path;
    },
    target: string,
  ) {
    const flagged = yield* services.projections.getExternalWorktreePaths();
    if (flagged.length === 0) return false;
    const resolved = services.path.resolve(target);
    const key = normalizeWorktreePathForComparison(resolved);
    const realPath = (candidate: string) =>
      services.fs.realPath(candidate).pipe(Effect.orElseSucceed(() => null));
    const real = yield* realPath(resolved);
    for (const candidate of flagged) {
      const flaggedPath = services.path.resolve(candidate);
      if (normalizeWorktreePathForComparison(flaggedPath) === key) return true;
      if (real !== null && (yield* realPath(flaggedPath)) === real) return true;
    }
    return false;
  },
);
