import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";

/** Require project worktrees so launches cannot target arbitrary host directories. */
export const assertProjectWorktree = Effect.fn("mcp.assertProjectWorktree")(function* (
  workspaceRoot: string,
  worktreePath: string,
) {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const fileSystem = yield* FileSystem.FileSystem;
  const real = (path: string) => fileSystem.realPath(path).pipe(Effect.orElseSucceed(() => null));
  const worktrees = yield* git.listWorktreePaths(workspaceRoot).pipe(
    Effect.flatMap((paths) => Effect.forEach(paths, real)),
    Effect.orElseSucceed((): ReadonlyArray<string | null> => []),
  );
  const requested = yield* real(worktreePath);
  if (requested === null || !worktrees.includes(requested))
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "worktreePath must be one of the project's git worktrees. t3_worktree_list shows them.",
    });
});
