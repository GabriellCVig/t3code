// @effect-diagnostics-next-line nodeBuiltinImport:off -- Resolve the fixture paths before the Effect tests run.
import * as NodePath from "node:path";
import * as Logger from "effect/Logger";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { AuthSessionId, ProjectId, ProviderInstanceId, RunId, ThreadId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import * as ConfigProvider from "effect/ConfigProvider";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as TestClock from "effect/testing/TestClock";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import { vi } from "vite-plus/test";

import * as McpBridge from "../mcp/AcpMcpStdioBridge.ts";
import {
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
} from "../serverRuntimeState.ts";
import {
  latestRunError,
  launchModelSelection,
  readLatestMessages,
  readThreadStatus,
  threadHealthClass,
  resolveProject,
  threadCommand,
  type ThreadTools,
  titleFromMessage,
  waitForRun,
} from "./thread.ts";

vi.mock("../auth/EnvironmentAuth.ts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../auth/EnvironmentAuth.ts")>();
  return {
    ...actual,
    layerRuntime: Layer.mock(actual.EnvironmentAuth)({
      issueMcpClientSession: () =>
        Effect.map(DateTime.now, (expiresAt) => ({
          sessionId: AuthSessionId.make("test-session"),
          token: "synthetic-token",
          expiresAt,
        })),
      revokeSession: () => Effect.succeed(true),
    }),
  };
});

const threadId = ThreadId.make("thread-1");
const runId = RunId.make("run-1");

it.effect("list includes delegated threads across pages and still leaves out subagents", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-thread-list-" });
    yield* persistServerRuntimeState({
      path: path.join(baseDir, "userdata", "server-runtime.json"),
      state: yield* makePersistedServerRuntimeState({
        config: { host: "127.0.0.1", devUrl: undefined },
        port: 3_773,
      }),
    });
    const projectId = ProjectId.make("project-1");
    const item = (id: string) => ({
      threadId: id,
      projectId,
      title: id,
      createdBy: "user",
      creationSource: "web",
      status: "idle",
      latestRunId: null,
      providerInstanceId: "codex",
      model: "test",
      runtimeMode: "full-access",
      interactionMode: "default",
      linkedPullRequest: null,
      settled: false,
      settledAt: null,
      snoozed: false,
      snoozedUntil: null,
      parentThreadId: null,
      relationshipToParent: null,
      itemCount: 0,
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    });
    const root = item("root");
    const delegated = {
      ...item("delegated"),
      delegatedFrom: { environmentId: "env-1", threadId: "remote", title: "Remote" },
    };
    const subagent = { ...item("child"), parentThreadId: "root", relationshipToParent: "subagent" };
    using _session = vi.spyOn(McpBridge, "mcpToolSession").mockReturnValue(
      Effect.succeed({
        call: (tool, args) => {
          if (tool === "t3_project_list")
            return Effect.succeed({
              structuredContent: {
                projects: [{ id: projectId, title: "Project", workspaceRoot: baseDir }],
                nextCursor: null,
              },
            });
          if (tool === "t3_thread_list") {
            const all = args.includeSubagents ? [root, subagent, delegated] : [root];
            const cursor = Number(args.cursor);
            return Effect.succeed({
              structuredContent: {
                projectId,
                currentThreadId: null,
                threads: all.slice(cursor, cursor + 2),
                nextCursor: cursor + 2 < all.length ? cursor + 2 : null,
                total: all.length,
              },
            });
          }
          return Effect.die(`unexpected ${tool} call`);
        },
      }),
    );
    yield* Command.runWith(threadCommand, { version: "0.0.0" })([
      "list",
      "--project",
      projectId,
      "--json",
      "--base-dir",
      baseDir,
    ]);
    const listed = JSON.parse(String((yield* TestConsole.logLines).at(-1))) as Array<{
      threadId: string;
    }>;
    assert.deepStrictEqual(
      listed.map((entry) => entry.threadId),
      ["root", "delegated"],
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        NetService.layer,
        ConfigProvider.layer(ConfigProvider.fromEnv({ env: { T3CODE_PORT: "3773" } })),
      ),
    ),
  ),
);

it.effect(
  "start resolves home-relative project paths while preserving ids and relative paths",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs
        .makeTempDirectoryScoped({ prefix: "t3-thread-project-path-" })
        .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
      const home = path.join(root, "home");
      const cwd = path.join(root, "current");
      const workspaceRoot = path.join(home, "My Project");
      const nestedRoot = path.join(workspaceRoot, "nested");
      const baseDir = path.join(root, "state");
      yield* fs.makeDirectory(nestedRoot, { recursive: true });
      yield* fs.makeDirectory(cwd);
      const projects = [
        { id: ProjectId.make("current-project"), title: "Current", workspaceRoot: cwd },
        { id: ProjectId.make("home-project"), title: "Home", workspaceRoot: home },
        { id: ProjectId.make("intended-project"), title: "Intended", workspaceRoot },
        { id: ProjectId.make("nested-project"), title: "Nested", workspaceRoot: nestedRoot },
        { id: ProjectId.make("~/literal-id"), title: "Exact id", workspaceRoot: cwd },
      ];
      yield* persistServerRuntimeState({
        path: path.join(baseDir, "userdata", "server-runtime.json"),
        state: yield* makePersistedServerRuntimeState({
          config: { host: "127.0.0.1", devUrl: undefined },
          port: 3_773,
        }),
      });
      using _session = vi.spyOn(McpBridge, "mcpToolSession").mockReturnValue(
        Effect.succeed({
          call: (tool, args) => {
            switch (tool) {
              case "t3_project_list":
                return Effect.succeed({ structuredContent: { projects, nextCursor: null } });
              case "t3_thread_launch":
                return Effect.succeed({
                  structuredContent: { threadId, projectId: args.projectId, runId: null },
                });
              default:
                return Effect.die(`unexpected ${tool} call`);
            }
          },
        }),
      );

      for (const [identifier, expectedProjectId] of [
        ["intended-project", "intended-project"],
        ["~/literal-id", "~/literal-id"],
        [workspaceRoot, "intended-project"],
        [path.relative(cwd, workspaceRoot), "intended-project"],
        [".", "current-project"],
        ["~/My Project", "intended-project"],
        ["~", "home-project"],
        ["~/My Project/nested", "nested-project"],
        ["~/My Project/missing", "intended-project"],
      ] as const) {
        yield* Command.runWith(threadCommand, { version: "0.0.0" })([
          "start",
          identifier,
          "Inspect this project",
          "--json",
          "--base-dir",
          baseDir,
        ]).pipe(
          Effect.provideService(HostProcess.HomeDirectory, home),
          Effect.provideService(HostProcess.WorkingDirectory, cwd),
        );
        assert.strictEqual(
          (yield* TestConsole.logLines).at(-1),
          JSON.stringify({ threadId, projectId: expectedProjectId, runId: null }),
          identifier,
        );
      }
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          NodeServices.layer,
          NodeCrypto.layer,
          NetService.layer,
          ConfigProvider.layer(ConfigProvider.fromEnv({ env: { T3CODE_PORT: "3773" } })),
        ),
      ),
    ),
);

it.effect("does not sign in to whatever took over a dead server's port", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-thread-dead-server-" });
    const state = yield* makePersistedServerRuntimeState({
      config: { host: "127.0.0.1", devUrl: undefined },
      port: 3_773,
    });
    yield* persistServerRuntimeState({
      path: path.join(baseDir, "userdata", "server-runtime.json"),
      // Above Linux's PID ceiling, as in pair.test.ts: no process can hold it.
      state: { ...state, pid: 4_194_305 },
    });
    using session = vi
      .spyOn(McpBridge, "mcpToolSession")
      .mockReturnValue(Effect.die("no session should open"));

    const error = yield* Command.runWith(threadCommand, { version: "0.0.0" })([
      "list",
      "--base-dir",
      baseDir,
    ]).pipe(Effect.flip);

    assert.strictEqual(error._tag, "ThreadServerNotRunningError");
    assert.strictEqual(session.mock.calls.length, 0);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        NetService.layer,
        ConfigProvider.layer(ConfigProvider.fromEnv({ env: { T3CODE_PORT: "3773" } })),
      ),
    ),
  ),
);

/** Answers each tool call with the next scripted response for that tool. */
const scriptedTools = (script: Record<string, Array<unknown>>) => {
  const calls: Array<{ readonly tool: string; readonly args: unknown }> = [];
  const call = ((tool: string, args: unknown) => {
    calls.push({ tool, args });
    const next = script[tool]?.shift();
    return next === undefined ? Effect.die(`unscripted ${tool} call`) : Effect.succeed(next);
  }) as ThreadTools["call"];
  return { tools: { call } satisfies ThreadTools, calls };
};

it.effect("--wait follows the sent run to its continuation's terminal status", () =>
  Effect.gen(function* () {
    const continuation = RunId.make("run-2");
    const { tools, calls } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "running", timedOut: true },
        { threadId, runId: continuation, status: "failed", timedOut: false },
      ],
      t3_thread_read: [{ thread: { pendingRequestCount: 0 } }],
    });

    const outcome = yield* waitForRun(tools, threadId, runId);

    assert.deepStrictEqual(outcome, { type: "ended", runId: continuation, status: "failed" });
    assert.deepStrictEqual(
      calls.filter((entry) => entry.tool === "t3_thread_wait").map((entry) => entry.args),
      [
        { threadId, runId, timeoutMs: 15_000 },
        { threadId, runId, timeoutMs: 15_000 },
      ],
    );
  }),
);

/** The reads that ask whether a run after `runId` holds a message someone sent. */
const laterRunReads = (
  laterRun: RunId,
  items: ReadonlyArray<{ readonly runId: RunId; readonly type: string }>,
) => [
  {
    recentRuns: [
      { runId, ordinal: 1 },
      { runId: laterRun, ordinal: 2 },
    ],
  },
  { thread: {}, items, nextPosition: null, hasMore: false },
];

it.effect("--wait lasts until background work the agent waits on wakes it and that run ends", () =>
  Effect.gen(function* () {
    const wakeRun = RunId.make("run-2");
    const { tools, calls } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "completed", timedOut: false },
        {
          threadId,
          runId,
          status: "completed",
          timedOut: false,
          resultRun: { runId: wakeRun, status: "completed" },
          // The wake run submits the result the first run owes, which the server stores there.
          output: { head: "abc" },
        },
      ],
      // The wake run starts from a notification, which the messages view leaves out.
      t3_thread_read: laterRunReads(wakeRun, []),
    });

    const outcome = yield* waitForRun(tools, threadId, runId, true);

    assert.deepStrictEqual(
      calls.filter((entry) => entry.tool === "t3_thread_wait").map((entry) => entry.args),
      [
        { threadId, runId, timeoutMs: 15_000 },
        { threadId, runId, timeoutMs: 15_000, untilDelegatedWorkSettled: true },
      ],
    );
    assert.deepStrictEqual(outcome, {
      type: "ended",
      runId: wakeRun,
      status: "completed",
      output: { head: "abc" },
    });
  }),
);

it.effect(
  "--wait waits out the moment between background work ending and its wake run starting",
  () =>
    Effect.gen(function* () {
      const wakeRun = RunId.make("run-2");
      const ownOnly = { recentRuns: [{ runId, ordinal: 1, status: "completed" }] };
      const withWake = (status: string) => [
        {
          recentRuns: [
            { runId, ordinal: 1, status: "completed" },
            { runId: wakeRun, ordinal: 2, status },
          ],
        },
        { thread: {}, items: [], nextPosition: null, hasMore: false },
      ];
      const { tools } = scriptedTools({
        t3_thread_wait: [
          { threadId, runId, status: "completed", timedOut: false },
          { threadId, runId, status: "completed", timedOut: true },
          // The work ended, and the run it wakes is not recorded yet.
          {
            threadId,
            runId,
            status: "completed",
            timedOut: false,
            resultRun: { runId, status: "completed" },
          },
          {
            threadId,
            runId,
            status: "completed",
            timedOut: false,
            resultRun: { runId: wakeRun, status: "completed" },
          },
        ],
        t3_thread_read: [
          ownOnly,
          { thread: { pendingRequestCount: 0 } },
          ownOnly,
          ...withWake("running"),
          ...withWake("completed"),
        ],
      });

      const fiber = yield* Effect.forkChild(waitForRun(tools, threadId, runId, true));
      yield* TestClock.adjust("500 millis");

      assert.deepStrictEqual(yield* Fiber.join(fiber), {
        type: "ended",
        runId: wakeRun,
        status: "completed",
      });
    }),
);

it.effect(
  "--wait keeps its own run's result when another client's later run settles the thread",
  () =>
    Effect.gen(function* () {
      const sentRun = RunId.make("run-2");
      const { tools } = scriptedTools({
        t3_thread_wait: [
          { threadId, runId, status: "completed", timedOut: false },
          {
            threadId,
            runId,
            status: "completed",
            timedOut: false,
            resultRun: { runId: sentRun, status: "failed" },
            output: { head: "abc" },
          },
        ],
        t3_thread_read: laterRunReads(sentRun, [{ runId: sentRun, type: "user_message" }]),
      });

      assert.deepStrictEqual(yield* waitForRun(tools, threadId, runId, true), {
        type: "ended",
        runId,
        status: "completed",
        output: { head: "abc" },
      });
    }),
);

it.effect("--wait stops when the thread waits for an answer instead of polling forever", () =>
  Effect.gen(function* () {
    const { tools } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "queued", timedOut: true },
        { threadId, runId, status: "queued", timedOut: true },
      ],
      t3_thread_read: [{ thread: { pendingRequestCount: 1 } }],
    });

    assert.deepStrictEqual(yield* waitForRun(tools, threadId, runId), { type: "needs_input" });
  }),
);

it.effect("--wait reports its run's result when a later run is the one waiting", () =>
  Effect.gen(function* () {
    const { tools } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "running", timedOut: true },
        { threadId, runId, status: "completed", timedOut: false },
      ],
      t3_thread_read: [{ thread: { pendingRequestCount: 1 } }],
    });

    assert.deepStrictEqual(yield* waitForRun(tools, threadId, runId), {
      type: "ended",
      runId,
      status: "completed",
    });
  }),
);

it.effect("start sends each --option as a typed model option", () =>
  Effect.gen(function* () {
    const selection = yield* launchModelSelection({
      provider: Option.some("codex"),
      model: Option.some("gpt-6.1-sol"),
      option: ["reasoningEffort=high", " fastMode = false ", "note=a=b"],
    });

    assert.deepStrictEqual(selection, {
      instanceId: "codex",
      model: "gpt-6.1-sol",
      options: [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: false },
        { id: "note", value: "a=b" },
      ],
    });
  }),
);

it.effect("start rejects a repeated --option before reaching the server", () =>
  Effect.gen(function* () {
    const error = yield* Command.runWith(threadCommand, { version: "0.0.0" })([
      "start",
      ".",
      "hi",
      "--provider",
      "codex",
      "--model",
      "gpt-6.1-sol",
      "--option",
      "reasoningEffort=high",
      "--option",
      "reasoningEffort=low",
    ]).pipe(Effect.flip);

    assert.strictEqual(error.message, "--option reasoningEffort is given more than once.");
  }).pipe(
    Effect.provide(
      Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, NetService.layer, TestConsole.layer),
    ),
  ),
);

/** Serves t3_thread_read pages the way the server does: positions count every item. */
const timelineTools = (types: ReadonlyArray<string>) => {
  const items = types.map((type, position) => ({ position, itemId: `item-${position}`, type }));
  const served: Array<number> = [];
  const call = ((_tool: string, input: unknown) => {
    const args = input as { readonly afterPosition?: number; readonly limit: number };
    // Scanning one match past the page keeps reads of huge fixtures linear.
    const matching: typeof items = [];
    for (let position = (args.afterPosition ?? -1) + 1; position < items.length; position++) {
      const item = items[position]!;
      if (["user_message", "assistant_message", "proposed_plan"].includes(item.type)) {
        matching.push(item);
        if (matching.length > args.limit) break;
      }
    }
    const page = matching.slice(0, args.limit);
    served.push(page.length);
    return Effect.succeed({
      thread: { itemCount: items.length },
      items: page,
      nextPosition: page.at(-1)?.position ?? null,
      hasMore: matching.length > page.length,
    });
  }) as ThreadTools["call"];
  const latest = (count: number) =>
    items
      .filter((item) => item.type === "user_message" || item.type === "assistant_message")
      .slice(-count)
      .map((item) => item.itemId);
  return { tools: { call } satisfies ThreadTools, served, latest };
};

const tail = (tools: number) => [...Array<string>(tools).fill("tool_call"), "assistant_message"];

it.effect("show retains pull request metadata when reading a later message window", () =>
  Effect.gen(function* () {
    const link = {
      host: "github.com",
      repository: "owner/repo",
      number: 1,
      url: "https://github.com/owner/repo/pull/1",
      source: "manual",
      state: "open",
      watching: false,
      watchStartedAt: null,
    } as const;
    const call = ((_tool: string, input: unknown) => {
      const args = input as { afterPosition?: number; limit: number };
      const first = args.afterPosition === undefined && args.limit === 1;
      return Effect.succeed({
        thread: {
          itemCount: 200,
          status: first ? "running" : "completed",
          pullRequestCount: 1,
          ...(first ? { pullRequests: [link], openPullRequestCount: 1 } : {}),
        },
        items: first ? [] : [{ position: 199, itemId: "message", type: "assistant_message" }],
        nextPosition: 199,
        hasMore: false,
      });
    }) as ThreadTools["call"];
    const { thread } = yield* readLatestMessages({ call }, threadId, 1);
    assert.strictEqual(thread.status, "completed");
    assert.deepStrictEqual(thread.pullRequests, [link]);
    assert.strictEqual(thread.openPullRequestCount, 1);
  }),
);

it.effect("show reads the end of a long thread, not its whole history", () =>
  Effect.gen(function* () {
    const { tools, served, latest } = timelineTools([
      ...Array.from({ length: 1000 }, (_, index) =>
        index % 2 === 0 ? "user_message" : "assistant_message",
      ),
      ...Array.from({ length: 10 }, () => tail(30)).flat(),
      "proposed_plan",
    ]);

    const { messages } = yield* readLatestMessages(tools, threadId, 10);

    assert.deepStrictEqual(
      messages.map((item) => item.itemId),
      latest(10),
    );
    assert.isBelow(
      served.reduce((sum, count) => sum + count, 0),
      100,
    );
  }),
);

it.effect("show widens back to the first item when the thread has fewer messages", () =>
  Effect.gen(function* () {
    const { tools, latest } = timelineTools([
      "user_message",
      ...Array.from({ length: 4 }, () => tail(150)).flat(),
    ]);

    const { messages } = yield* readLatestMessages(tools, threadId, 10);

    assert.deepStrictEqual(
      messages.map((item) => item.itemId),
      latest(10),
    );
    assert.lengthOf(messages, 5);
  }),
);

it.effect("show returns a --messages count larger than one call can spread", () =>
  Effect.gen(function* () {
    const { tools, latest } = timelineTools(Array<string>(200_000).fill("assistant_message"));

    const { messages } = yield* readLatestMessages(tools, threadId, 200_000);

    assert.deepStrictEqual(
      messages.map((item) => item.itemId),
      latest(200_000),
    );
  }),
);

it.effect("show gives the latest run's last error only when that run did not complete", () =>
  Effect.gen(function* () {
    const page = {
      thread: {},
      items: [
        { itemId: "item-0", runId: RunId.make("run-0"), type: "error", text: "earlier run" },
        { itemId: "item-1", runId, type: "error", text: "first try" },
        { itemId: "item-2", runId, type: "error", text: "Usage limit reached" },
      ],
      nextPosition: null,
      hasMore: false,
    };
    const { tools, calls } = scriptedTools({ t3_thread_read: [page] });

    assert.strictEqual(
      yield* latestRunError(tools, { threadId, status: "failed", latestRunId: runId }),
      "Usage limit reached",
    );
    assert.deepInclude(calls[0]?.args, { view: "activity" });
    for (const status of ["completed", "running", "queued", "idle"] as const) {
      assert.isNull(yield* latestRunError(tools, { threadId, status, latestRunId: runId }), status);
    }
    assert.lengthOf(calls, 1);
  }),
);

it("titles from a long first line without splitting an emoji", () => {
  const title = titleFromMessage(`${"a".repeat(78)}😀 and the rest of a long first line`);
  assert.strictEqual(title, `${"a".repeat(78)}…`);
});

const git = Effect.fn("test.git")(function* (cwd: string, args: ReadonlyArray<string>) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const exitCode = yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }));
  assert.strictEqual(Number(exitCode), 0, `git ${args.join(" ")}`);
});

const commitAll = (cwd: string) =>
  Effect.andThen(
    git(cwd, ["add", "."]),
    git(cwd, [
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.test",
      "commit",
      "--quiet",
      "-m",
      "initial",
    ]),
  );

it.live("resolves a path inside a git worktree of the project's repository to that project", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-thread-worktree-" });
    const repo = path.join(root, "sample");
    const worktree = path.join(root, "worktrees", "sample", "t3-8ff64c24");
    const feature = path.join(root, "wt", "feature");
    const featureWorktree = path.join(root, "worktrees", "feature", "t3-1a2b3c4d");
    yield* fileSystem.makeDirectory(path.join(repo, "pkg"), { recursive: true });
    yield* fileSystem.writeFileString(path.join(repo, "pkg", "index.ts"), "export {};\n");
    yield* git(repo, ["init", "--quiet", "-b", "main"]);
    yield* commitAll(repo);
    yield* git(repo, ["worktree", "add", "--quiet", "-b", "t3/8ff64c24", worktree]);
    yield* git(repo, ["worktree", "add", "--quiet", "-b", "feature", feature]);
    yield* git(feature, ["worktree", "add", "--quiet", "-b", "t3/1a2b3c4d", featureWorktree]);

    const project = (id: string, workspaceRoot: string) => ({
      id: ProjectId.make(id),
      title: path.basename(workspaceRoot),
      workspaceRoot,
    });
    const sample = project("project-sample", repo);
    const featureProject = project("project-feature", feature);
    const resolveFrom = (
      projects: ReadonlyArray<ReturnType<typeof project>>,
      workingDirectory: string,
      identifier = ".",
    ) =>
      resolveProject(
        scriptedTools({ t3_project_list: [{ projects, nextCursor: null }] }).tools,
        identifier,
      ).pipe(Effect.provideService(HostProcess.WorkingDirectory, workingDirectory));

    assert.deepStrictEqual(yield* resolveFrom([sample], worktree), sample);
    assert.deepStrictEqual(yield* resolveFrom([sample], path.join(worktree, "pkg")), sample);
    assert.deepStrictEqual(yield* resolveFrom([sample], worktree, "pkg/index.ts"), sample);
    assert.deepStrictEqual(yield* resolveFrom([featureProject], featureWorktree), featureProject);
    const ambiguous = yield* Effect.flip(resolveFrom([sample, featureProject], featureWorktree));
    assert.strictEqual(ambiguous._tag, "ThreadUsageError");
    assert.include(ambiguous.message, "sample (project-sample)");
    assert.include(ambiguous.message, "feature (project-feature)");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const runThread = Command.runWith(threadCommand, { version: "test" });

it.layer(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, NetService.layer))(
  "thread commands",
  (it) => {
    it.effect.each(["--worktree", "--branch"])("rejects --worktree-path with %s", (flag) =>
      Effect.gen(function* () {
        const error = yield* runThread([
          "start",
          ".",
          "Review",
          "--worktree-path",
          "/repo/review",
          flag,
          "feature",
        ]).pipe(Effect.flip);
        assert.include(error, {
          _tag: "ThreadUsageError",
          detail: "--worktree-path cannot be combined with --worktree or --branch.",
        });
      }),
    );

    it.effect.each(["", "   "])("rejects an empty worktree path %j", (value) =>
      Effect.gen(function* () {
        const error = yield* runThread(["start", ".", "Review", "--worktree-path", value]).pipe(
          Effect.flip,
        );
        assert.include(error, { _tag: "ThreadUsageError", detail: "The worktree path is empty." });
      }),
    );

    const projectId = "project-1";
    const message = "Review";
    const cliCwd = NodePath.resolve("/cli/repo");
    const worktreePath = NodePath.resolve("/repo/review");
    const launchArgs = { projectId, title: message, message };
    const interruptResult = { threadId, runId, status: "interrupt_requested" };

    it.effect.each([
      ...(
        [
          [" .worktrees/foo ", NodePath.join(cliCwd, ".worktrees", "foo")],
          [".", cliCwd],
        ] as const
      ).map(([arg, resolved]) => ({
        args: ["start", projectId, message, "--worktree-path", arg],
        tool: "t3_thread_launch",
        input: {
          ...launchArgs,
          workspaceStrategy: { type: "existing_worktree", worktreePath: resolved },
        },
        result: { threadId, runId, projectId },
        output: threadId,
      })),
      {
        args: [
          "start",
          projectId,
          message,
          "--worktree-path",
          worktreePath,
          "--client-request-id",
          "launch-id",
        ],
        tool: "t3_thread_launch",
        input: {
          ...launchArgs,
          workspaceStrategy: { type: "existing_worktree", worktreePath },
          clientRequestId: "launch-id",
        },
        result: { threadId, runId, projectId },
        output: threadId,
      },
      {
        args: ["start", projectId, message, "--worktree", "main", "--branch", "feature"],
        tool: "t3_thread_launch",
        input: {
          ...launchArgs,
          workspaceStrategy: { type: "worktree", baseRef: "main", branch: "feature" },
        },
        result: { threadId, runId, projectId },
        output: threadId,
      },
      {
        args: ["send", threadId, message, "--client-request-id", "send-id"],
        tool: "t3_thread_send",
        input: { threadId, message, mode: "queue", clientRequestId: "send-id" },
        result: { threadId, runId, messageId: "message-1", status: "queued", delivery: "queued" },
        output: "Queued.",
      },
      {
        args: ["archive", threadId],
        tool: "t3_thread_organize",
        input: { threadId, action: "archive" },
        result: { sequence: 1 },
        output: "Archived.",
      },
      {
        args: ["archive", threadId, "--unarchive", "--json"],
        tool: "t3_thread_organize",
        input: { threadId, action: "unarchive" },
        result: { sequence: 1 },
        output: JSON.stringify({ sequence: 1 }),
      },
      {
        args: ["interrupt", threadId],
        tool: "t3_thread_interrupt",
        input: { threadId },
        result: interruptResult,
        output: "interrupt_requested",
      },
      {
        args: ["interrupt", threadId, "--run", runId, "--reason", "Done", "--stop", "--json"],
        tool: "t3_thread_interrupt",
        input: { threadId, runId, reason: "Done", stop: true },
        result: interruptResult,
        output: JSON.stringify(interruptResult),
      },
    ])("thread $args", ({ args, tool, input, result, output }) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-thread-cli-" });
        const path = yield* Path.Path;
        yield* persistServerRuntimeState({
          path: path.join(baseDir, "userdata", "server-runtime.json"),
          state: yield* makePersistedServerRuntimeState({
            config: { host: "127.0.0.1", devUrl: undefined },
            port: 3773,
          }),
        });
        const call = vi.fn((name: string, _args: Readonly<Record<string, unknown>>) =>
          Effect.succeed({
            structuredContent:
              name === "t3_project_list"
                ? {
                    projects: [{ id: projectId, title: "Project", workspaceRoot: "/repo" }],
                    nextCursor: null,
                  }
                : result,
          }),
        );
        using _session = vi
          .spyOn(McpBridge, "mcpToolSession")
          .mockReturnValue(Effect.succeed({ call }));
        const lines = yield* runThread([...args, "--base-dir", baseDir]).pipe(
          Effect.provideService(HostProcess.WorkingDirectory, cliCwd),
          Effect.andThen(TestConsole.logLines),
          Effect.provide(
            Layer.merge(
              Layer.fresh(TestConsole.layer),
              Logger.layer([], { mergeWithExisting: false }),
            ),
          ),
        );
        assert.deepEqual(call.mock.lastCall, [tool, input]);
        assert.deepEqual(lines, [output]);
      }),
    );
  },
);

const projectId = "project-1";
const threadDetail = (overrides: Record<string, unknown> = {}) => ({
  threadId,
  projectId,
  title: "Lead",
  status: "running",
  activeRunId: runId,
  latestRunId: runId,
  providerInstanceId: "claudeAgent",
  model: "claude-opus-5-5",
  branch: "main",
  worktreePath: "/work/repo",
  pendingRequestCount: 0,
  blockedOnThreadWait: false,
  usageLimit: null,
  ...overrides,
});
const listItem = (id: string, parentThreadId: string | null, status = "running") => ({
  threadId: ThreadId.make(id),
  title: id,
  status,
  latestRunId: RunId.make(`${id}-run`),
  providerInstanceId: "codex",
  model: "gpt-6.2-astra",
  parentThreadId,
  relationshipToParent: "subagent",
});

it.effect("status reads passively and lists only this thread's active children", () =>
  Effect.gen(function* () {
    const { tools, calls } = scriptedTools({
      t3_thread_read: [
        {
          thread: threadDetail({
            status: "waiting",
            blockedOnThreadWait: true,
            pendingBackgroundTasks: [
              { taskId: "task-1", kind: "subagent", childThreadId: "child-1" },
            ],
          }),
          recentRuns: [{ runId, status: "waiting" }],
        },
      ],
      t3_thread_list: [
        {
          threads: [
            listItem("child-1", threadId),
            listItem("other", "thread-2"),
            { ...listItem("fork", threadId), relationshipToParent: "fork" },
          ],
          nextCursor: 1,
        },
        { threads: [listItem("child-2", threadId, "queued")], nextCursor: null },
      ],
    });

    const status = yield* readThreadStatus(tools, threadId);

    // Never task_status: that would acknowledge a child result the parent has not seen.
    assert.deepStrictEqual(
      calls.map((entry) => entry.tool),
      ["t3_thread_read", "t3_thread_list", "t3_thread_list"],
    );
    assert.deepStrictEqual(calls[1]?.args, {
      projectId,
      includeSubagents: true,
      statuses: ["preparing", "queued", "starting", "running", "waiting"],
      limit: 100,
      cursor: 0,
    });
    assert.deepStrictEqual(status, {
      threadId,
      title: "Lead",
      status: "waiting",
      healthClass: "running",
      activeRunId: runId,
      latestRunId: runId,
      latestRunStatus: "waiting",
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-opus-5-5",
      branch: "main",
      worktreePath: "/work/repo",
      pendingRequestCount: 0,
      pendingRequests: [],
      blockedOnThreadWait: true,
      pendingBackgroundTasks: [
        { taskId: "task-1", kind: "subagent", childThreadId: ThreadId.make("child-1") },
      ],
      activeChildren: [
        {
          threadId: ThreadId.make("child-1"),
          title: "child-1",
          status: "running",
          latestRunId: RunId.make("child-1-run"),
          providerInstanceId: "codex",
          model: "gpt-6.2-astra",
        },
        {
          threadId: ThreadId.make("child-2"),
          title: "child-2",
          status: "queued",
          latestRunId: RunId.make("child-2-run"),
          providerInstanceId: "codex",
          model: "gpt-6.2-astra",
        },
      ],
      usageLimit: null,
    });
  }),
);

it.effect("status reports a usage-limit hold as rate_limited with its reset time", () =>
  Effect.gen(function* () {
    const usageLimit = {
      rateLimited: true as const,
      resetsAt: "2026-11-22T06:00:00.000Z",
      instanceId: ProviderInstanceId.make("claudeAgent_claude2"),
      detail: "Claude usage limit reached.",
    };
    const { tools } = scriptedTools({
      t3_thread_read: [
        {
          thread: threadDetail({ status: "queued", activeRunId: null, usageLimit }),
          recentRuns: [{ runId, status: "failed" }],
        },
      ],
      t3_thread_list: [{ threads: [], nextCursor: null }],
    });

    const status = yield* readThreadStatus(tools, threadId);

    assert.equal(status.healthClass, "rate_limited");
    assert.deepStrictEqual(status.usageLimit, usageLimit);
  }),
);

it.effect(
  "status reports the interrupted turn when the recent run page contains only held work",
  () =>
    Effect.gen(function* () {
      const { tools } = scriptedTools({
        t3_thread_read: [
          {
            thread: threadDetail({ status: "interrupted", activeRunId: null }),
            recentRuns: [{ runId: "held", status: "queued" }],
          },
        ],
        t3_thread_list: [{ threads: [], nextCursor: null }],
      });
      const status = yield* readThreadStatus(tools, threadId);
      assert.equal(status.latestRunStatus, "interrupted");
      assert.equal(status.healthClass, "interrupted");
    }),
);

it("health class puts a blocked human ahead of a hold and a hold ahead of a run", () => {
  const base = {
    status: "idle",
    latestRunStatus: null,
    pendingRequestCount: 0,
    pendingRequests: [],
    activeChildren: [],
    pendingBackgroundTasks: [],
    usageLimit: null,
  } as const;
  const limit = {
    rateLimited: true,
    resetsAt: null,
    instanceId: ProviderInstanceId.make("codex"),
    detail: null,
  } as const;
  const cases: Array<[Parameters<typeof threadHealthClass>[0], string]> = [
    [base, "idle"],
    [
      {
        ...base,
        status: "completed",
        latestRunStatus: "completed",
        activeChildren: [listItem("child", threadId)],
      },
      "waiting_children",
    ],
    [
      {
        ...base,
        status: "waiting",
        latestRunStatus: "completed",
        pendingBackgroundTasks: [{ kind: "command", awaited: true }],
      },
      "waiting_children",
    ],
    [
      {
        ...base,
        status: "completed",
        latestRunStatus: "completed",
        pendingBackgroundTasks: [{ kind: "command" }],
      },
      "idle",
    ],
    [{ ...base, status: "running" }, "running"],
    [{ ...base, status: "failed", latestRunStatus: "failed" }, "failed"],
    [{ ...base, status: "failed", latestRunStatus: "failed", usageLimit: limit }, "rate_limited"],
    [{ ...base, latestRunStatus: "cancelled" }, "interrupted"],
    [{ ...base, latestRunStatus: "interrupted" }, "interrupted"],
    [{ ...base, status: "queued", usageLimit: limit }, "rate_limited"],
    [
      {
        ...base,
        status: "waiting",
        usageLimit: limit,
        pendingRequestCount: 1,
        pendingRequests: [{ kind: "command" }],
      },
      "waiting_approval",
    ],
    [
      {
        ...base,
        status: "waiting",
        pendingRequestCount: 2,
        pendingRequests: [{ kind: "command" }, { kind: "user_input" }],
      },
      "waiting_user",
    ],
    [
      {
        ...base,
        status: "running",
        pendingRequestCount: 1,
        pendingRequests: [{ kind: "user_input", blocking: false }],
      },
      "running",
    ],
    // An older server sends only the count.
    [{ ...base, status: "waiting", pendingRequestCount: 1 }, "waiting_user"],
  ];
  for (const [input, expected] of cases) {
    assert.equal(threadHealthClass(input), expected, JSON.stringify(input));
  }
});

it.effect("show keeps each message's server-assigned provenance", () =>
  Effect.gen(function* () {
    const item = (index: number, overrides: Record<string, unknown>) => ({
      position: index,
      itemId: `item-${index}`,
      runId,
      type: "user_message",
      text: `message ${index}`,
      textTruncated: false,
      ...overrides,
    });
    const { tools } = scriptedTools({
      t3_thread_read: [
        {
          thread: threadDetail(),
          recentRuns: [],
          items: [],
        },
        {
          thread: threadDetail(),
          items: [
            item(0, { createdBy: "user", creationSource: "web" }),
            item(1, { createdBy: "agent", creationSource: "mcp" }),
            item(2, { type: "reasoning", createdBy: null, creationSource: null }),
            item(3, { type: "assistant_message", createdBy: null, creationSource: null }),
          ],
          nextPosition: 3,
          hasMore: false,
        },
      ],
    });

    const { messages } = yield* readLatestMessages(tools, threadId, 10);

    assert.deepStrictEqual(
      messages.map(({ type, createdBy, creationSource, text }) => ({
        role: type === "user_message" ? "user" : "assistant",
        createdBy,
        creationSource,
        text,
      })),
      [
        {
          role: "user",
          createdBy: "user",
          creationSource: "web",
          text: "message 0",
        },
        {
          role: "user",
          createdBy: "agent",
          creationSource: "mcp",
          text: "message 1",
        },
        {
          role: "assistant",
          createdBy: null,
          creationSource: null,
          text: "message 3",
        },
      ],
    );
  }),
);
