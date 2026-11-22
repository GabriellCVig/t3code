import * as TaskFolders from "../workspace/TaskFolders.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as NodeURL from "node:url";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  isProviderNativeSubagentThread,
  MessageId,
  type ModelSelection,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpThreadInterruptResult,
  OrchestratorMcpThreadSendResult,
  OrchestratorMcpThreadReadResult,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderReplayTranscript,
  type ProviderOptionDescriptor,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import { McpSchema, McpServer } from "effect/ai";

import { CodexOrchestratorReplayHarness } from "../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ServerConfig from "../config.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  decodeProviderReplayNdjson,
  materializeReplayTranscriptWorkspace,
} from "@t3tools/provider-testing/replayTranscript";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinkRequests from "../peer/PeerLinkRequests.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";

const layerGit = GitVcsDriver.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-delegate-worktree-" })),
  Layer.provide(NodeServices.layer),
);

/** A task delegated here always has a local child thread. */
const localChild = (childThreadId: ThreadId | null): ThreadId => {
  if (childThreadId === null) throw new Error("expected a local delegated child");
  return childThreadId;
};

// Effect returns a declared tool failure as `isError` with its encoded payload
// as JSON text, never as `structuredContent`.

const parentThreadId = ThreadId.make("thread:mcp-orchestrator-parent");
const projectId = ProjectId.make("project:mcp-orchestrator");
const codexInstanceId = ProviderInstanceId.make("codex");
const codexModel = "gpt-5.4";
const parentPrompt = "Keep this parent turn active while orchestration tools are tested.";
const delegatedPrompt = "Inspect the delegated API boundary and return the result.";
const delegatedResult = "Delegated API boundary inspected.";
const cancellationPrompt = "Remain active until the parent cancels this delegated task.";
const queuedFollowupPrompt = "Complete the queued follow-up and return the final result.";
const queuedFollowupResult = "Queued delegated follow-up completed.";
const decodeThreadRead = Schema.decodeUnknownEffect(OrchestratorMcpThreadReadResult);
const longFollowupResult = "Long delegated follow-up line. ".repeat(1_500);

const decodeDelegateTaskResult = Schema.decodeUnknownEffect(OrchestratorMcpDelegateTaskResult);
const decodeThreadInterruptResult = Schema.decodeUnknownEffect(
  OrchestratorMcpThreadInterruptResult,
);
const decodeThreadSendResult = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult);

const codexSelection = {
  instanceId: codexInstanceId,
  model: codexModel,
} satisfies ModelSelection;

const delegatedTaskStatusTranscriptFile = new URL(
  "../orchestration-v2/testkit/fixtures/delegated_task_status/codex_transcript.ndjson",
  import.meta.url,
);

const readDelegatedTaskStatusTranscript = Effect.fn("readDelegatedTaskStatusTranscript")(
  function* (followupResult: string) {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs.readFileString(NodeURL.fileURLToPath(delegatedTaskStatusTranscriptFile));
    // The recording names the parent, then the child, after the fixture's titles; this test's differ.
    const threadNames = ["MCP replay parent", delegatedPrompt];
    const transcript = yield* decodeProviderReplayNdjson(
      text
        .replaceAll(queuedFollowupResult, followupResult)
        .replace(
          /"name":"Replay fixture: delegated_task_status"/g,
          () => `"name":${JSON.stringify(threadNames.shift())}`,
        ),
    );
    // The recording ends when the recorder closes the app-server while the parent
    // turn is still open. This test keeps acting as that parent afterwards, and
    // replaying the exit would kill the shared process and fail the parent run.
    return {
      ...transcript,
      entries: transcript.entries.filter((entry) => entry.type !== "runtime_exit"),
    };
  },
  Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

function makeProviderSnapshot(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly model: string;
  readonly optionDescriptors?: ReadonlyArray<ProviderOptionDescriptor>;
}): ServerProvider {
  return {
    instanceId: input.instanceId,
    driver: input.driver,
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-06-17T00:00:00.000Z",
    models: [
      {
        slug: input.model,
        name: input.model,
        isCustom: false,
        capabilities:
          input.optionDescriptors === undefined
            ? null
            : { optionDescriptors: input.optionDescriptors },
      },
    ],
    slashCommands: [],
    skills: [],
  };
}

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "orchestrator-mcp-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "orchestrator-mcp-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const layerMemorySecretStore = Layer.sync(ServerSecretStore.ServerSecretStore, () => {
  const stored = new Map<string, Uint8Array>();
  return ServerSecretStore.ServerSecretStore.of({
    get: (name) => Effect.succeed(Option.fromNullishOr(stored.get(name))),
    set: (name, value) => Effect.sync(() => void stored.set(name, value)),
    create: (name, value) => Effect.sync(() => void stored.set(name, value)),
    getOrCreateRandom: (name, bytes) =>
      Effect.sync(() => {
        const existing = stored.get(name);
        if (existing) return existing;
        const value = new Uint8Array(bytes).fill(7);
        stored.set(name, value);
        return value;
      }),
    remove: (name) => Effect.sync(() => void stored.delete(name)),
  });
});

const layerUnusedScheduledTaskStub = Layer.succeed(
  ScheduledTaskService.ScheduledTaskService,
  ScheduledTaskService.ScheduledTaskService.of({
    list: () => Effect.succeed({ tasks: [] }),
    subscribeList: () => Stream.succeed({ tasks: [] }),
    upsert: () => Effect.die("ScheduledTaskService.upsert is unused in this test"),
    setEnabled: () => Effect.die("ScheduledTaskService.setEnabled is unused in this test"),
    delete: () => Effect.die("ScheduledTaskService.delete is unused in this test"),
    deleteForThread: () => Effect.die("unused in this test"),
    moveThreadTasks: () => Effect.die("unused in this test"),
    runNow: () => Effect.die("ScheduledTaskService.runNow is unused in this test"),
    rotateWebhookToken: () => Effect.die("unused in this test"),
    listWebhookDeliveries: () => Effect.die("unused in this test"),
    getWebhookDelivery: () => Effect.die("unused in this test"),
    triggerWebhook: () => Effect.die("unused in this test"),
  }),
);

const replayLayer = Effect.fn("replayLayer")(function* (
  cwd: string,
  rawTranscript: ProviderReplayTranscript,
) {
  const transcript = yield* CodexOrchestratorReplayHarness.decodeTranscript(
    materializeReplayTranscriptWorkspace(rawTranscript, cwd),
  );
  const layerOrchestrator = ProviderReplayHarness.layerProviderReplay(
    {
      name: "delegated-task-status/codex",
      transcript,
      commands: [],
    },
    CodexOrchestratorReplayHarness,
  );
  // SQL spans carry no bound parameters, so this records which thread each tool read.
  const threadRecordReads: Array<{
    readonly threadId: ThreadId;
    readonly fields: ReadonlyArray<string>;
  }> = [];
  const layerOrchestration = Layer.merge(
    layerOrchestrator,
    Layer.effect(
      ThreadManagementService.ThreadManagementService,
      Effect.gen(function* () {
        const service = yield* ThreadManagementService.ThreadManagementService;
        return ThreadManagementService.ThreadManagementService.of({
          ...service,
          getThreadRecords: (threadId, fields, filter) =>
            Effect.sync(() => threadRecordReads.push({ threadId, fields })).pipe(
              Effect.andThen(service.getThreadRecords(threadId, fields, filter)),
            ),
        });
      }),
    ).pipe(
      Layer.provide(
        ThreadManagementService.layer.pipe(
          Layer.provide(layerOrchestrator),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(
            CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          ),
        ),
      ),
    ),
  );
  const layerProviderRegistry = ProviderRegistryMock.layer([
    makeProviderSnapshot({
      instanceId: codexInstanceId,
      driver: ProviderDriverKind.make("codex"),
      model: codexModel,
    }),
  ]);
  const layerProject = Layer.mock(ProjectService.ProjectService)({
    workspaceScanDirectory: Effect.succeed,
    getById: () =>
      Effect.succeedSome({
        id: projectId,
        title: "Replay project",
        workspaceRoot: cwd,
        defaultModelSelection: codexSelection,
        scripts: [],
        createdAt: "2026-06-20T00:00:00.000Z",
        updatedAt: "2026-06-20T00:00:00.000Z",
        deletedAt: null,
      }),
  });
  const layerLaunch = ThreadLaunchService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerProject,
        layerOrchestration,
        layerProviderRegistry,
        ServerSettings.layerTest(),
        WorktreeSetupTracker.layer,
        IdAllocator.layer,
        CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
        Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: cwd }),
        Layer.mock(GitWorkflow.GitWorkflowService)({}),
        Layer.mock(TerminalManager.TerminalManager)({}),
        Layer.mock(TextGeneration.TextGeneration)({}),
        Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
          runForThread: () => Effect.succeed({ status: "no-script" }),
        }),
      ),
    ),
  );
  const layer = Layer.merge(
    McpHttpServer.layerOrchestratorToolkit,
    McpHttpServer.layerThreadToolkit,
  ).pipe(
    Layer.provide(layerLaunch),
    Layer.provide(Layer.mock(TaskFolders.TaskFolders)({})),
    Layer.provide(Layer.mock(CheckpointStore.CheckpointStore)({})),
    Layer.provide(layerGit),
    Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
    Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
    Layer.provideMerge(layerOrchestration),
    Layer.provide(
      CodexOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(transcript).pipe(
        Layer.provide(McpProviderSessions.layer),
      ),
    ),
    Layer.provide(layerProviderRegistry),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(layerUnusedScheduledTaskStub),
    Layer.provide(layerProject),
    Layer.provideMerge(
      SecretRequests.layer.pipe(
        Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
        Layer.provide(layerMemorySecretStore),
        Layer.provide(layerOrchestration),
      ),
    ),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );
  return { layer, threadRecordReads };
});

const startReplayParent = Effect.fn("startReplayParent")(function* (cwd: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const server = yield* McpServer.McpServer;
  const parentCreate = yield* orchestrator.dispatch({
    type: "thread.create",
    createdBy: "user",
    creationSource: "web",
    commandId: CommandId.make("command:mcp-replay-parent:create"),
    threadId: parentThreadId,
    projectId,
    title: "MCP replay parent",
    modelSelection: codexSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: cwd,
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    createdBy: "user",
    creationSource: "web",
    commandId: CommandId.make("command:mcp-replay-parent:start"),
    threadId: parentThreadId,
    messageId: MessageId.make("message:mcp-replay-parent:start"),
    text: parentPrompt,
    attachments: [],
    modelSelection: codexSelection,
    dispatchMode: { type: "start_immediately" },
  });
  yield* orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: parentCreate.sequence,
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "provider-turn.updated" &&
          stored.event.payload.status === "running",
      ),
      Stream.runHead,
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.die("Parent provider turn did not start."),
          onSome: () => Effect.void,
        }),
      ),
    );

  const invocation: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment:mcp-replay"),
    requestNamespace: "mcp-provider-session-replay-parent",
    thread: {
      threadId: parentThreadId,
      providerSessionId: "mcp-provider-session-replay-parent",
      providerInstanceId: codexInstanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 1,
  };
  const invoke = (name: string, args: Record<string, unknown>, scope = invocation) =>
    server
      .callTool({ name, arguments: args })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, client),
      );

  return { orchestrator, invocation, invoke };
});

describe("orchestrator MCP toolkit", () => {
  it.live.each([
    { label: "distinct", result: queuedFollowupResult, latestSummary: queuedFollowupResult },
    { label: "identical", result: delegatedResult, latestSummary: null },
    { label: "emoji", result: "😀", latestSummary: "😀" },
    { label: "long", result: longFollowupResult, latestSummary: longFollowupResult },
  ])("reports $label child results", ({ result, latestSummary }) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("delegated-task-status");
      const rawTranscript = yield* readDelegatedTaskStatusTranscript(result);
      const { layer: layerTest, threadRecordReads } = yield* replayLayer(cwd, rawTranscript);
      yield* Effect.gen(function* () {
        const { orchestrator, invocation, invoke } = yield* startReplayParent(cwd);
        const delegationStartSequence = yield* orchestrator.getThreadEventSequence(parentThreadId);
        const delegatedCall = yield* invoke("delegate_task", {
          task: delegatedPrompt,
          target: {
            providerInstanceId: codexInstanceId,
            model: codexModel,
          },
          mode: "async",
          clientRequestId: "delegate-codex-replay-1",
        });
        const delegatedStart = yield* decodeDelegateTaskResult(
          delegatedCall.structuredContent,
        ).pipe(Effect.orDie);
        expect(delegatedStart.childRunId).not.toBeNull();
        yield* orchestrator
          .streamStoredEventsFrom({
            threadId: parentThreadId,
            afterSequence: delegationStartSequence,
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "context-transfer.created" &&
                stored.event.payload.type === "subagent_result" &&
                stored.event.payload.sourceThreadId === delegatedStart.childThreadId &&
                stored.event.payload.sourcePoint.runId === delegatedStart.childRunId,
            ),
            Stream.runHead,
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.die("Delegated result transfer was not created."),
                onSome: () => Effect.void,
              }),
            ),
          );
        const delegatedStatusCall = yield* invoke("task_status", {
          taskId: delegatedStart.taskId,
        });
        const delegated = yield* decodeDelegateTaskResult(
          delegatedStatusCall.structuredContent,
        ).pipe(Effect.orDie);
        expect(delegated).toMatchObject({
          status: "completed",
          summary: delegatedResult,
          providerInstanceId: codexInstanceId,
          hasPendingChildRuns: false,
          latestTerminalRunId: delegated.childRunId,
          latestTerminalStatus: "completed",
          latestTerminalSummary: null,
          latestTerminalResultContextTransferId: null,
        });
        expect(delegated.resultContextTransferId).not.toBeNull();

        const delegatedChild = yield* orchestrator.getThreadProjection(
          localChild(delegated.childThreadId),
        );
        expect(delegatedChild.thread.lineage.relationshipToParent).toBe("subagent");
        expect(isProviderNativeSubagentThread(delegatedChild.thread)).toBe(false);
        const followupStartSequence = yield* orchestrator.getThreadEventSequence(
          localChild(delegated.childThreadId),
        );
        const runningFollowupCall = yield* invoke("t3_thread_send", {
          threadId: localChild(delegated.childThreadId),
          message: cancellationPrompt,
          clientRequestId: "delegated-child-replay-running-1",
        });
        const runningFollowup = yield* decodeThreadSendResult(
          runningFollowupCall.structuredContent,
        ).pipe(Effect.orDie);
        yield* orchestrator
          .streamStoredEventsFrom({
            threadId: localChild(delegated.childThreadId),
            afterSequence: followupStartSequence,
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "provider-turn.updated" &&
                stored.event.payload.status === "running",
            ),
            Stream.runHead,
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.die("Follow-up provider turn did not start."),
                onSome: () => Effect.void,
              }),
            ),
          );

        const queuedFollowupCall = yield* invoke("t3_thread_send", {
          threadId: localChild(delegated.childThreadId),
          message: queuedFollowupPrompt,
          mode: "queue",
          clientRequestId: "delegated-child-replay-queued-1",
        });
        const queuedFollowup = yield* decodeThreadSendResult(
          queuedFollowupCall.structuredContent,
        ).pipe(Effect.orDie);
        expect(queuedFollowup).toMatchObject({
          status: "queued",
          delivery: "queued",
        });

        const pendingProjection = yield* orchestrator.getThreadProjection(
          localChild(delegated.childThreadId),
        );
        const followupTasks = (yield* orchestrator.getThreadProjection(
          parentThreadId,
        )).subagents.filter(
          (task) => task.childThreadId === delegated.childThreadId && task.id !== delegated.taskId,
        );
        expect(followupTasks).toHaveLength(2);
        expect(followupTasks.every((task) => task.result === null)).toBe(true);
        expect(followupTasks.map((task) => task.id)).toEqual([
          runningFollowup.taskId,
          queuedFollowup.taskId,
        ]);
        expect(pendingProjection.runs.find((run) => run.id === delegated.childRunId)?.status).toBe(
          "completed",
        );
        expect(pendingProjection.runs.find((run) => run.id === runningFollowup.runId)?.status).toBe(
          "running",
        );
        expect(pendingProjection.runs.find((run) => run.id === queuedFollowup.runId)?.status).toBe(
          "queued",
        );
        expect(
          (yield* orchestrator.getThreadProjection(parentThreadId)).runs.some(
            (run) => run.status === "running",
          ),
        ).toBe(true);

        const pendingStatusCall = yield* invoke("task_status", {
          taskId: delegated.taskId,
        });
        const pendingStatus = yield* decodeDelegateTaskResult(
          pendingStatusCall.structuredContent,
        ).pipe(Effect.orDie);
        expect(pendingStatus).toMatchObject({
          childRunId: delegated.childRunId,
          status: "completed",
          summary: delegatedResult,
          resultContextTransferId: delegated.resultContextTransferId,
          hasPendingChildRuns: true,
          latestTerminalRunId: delegated.childRunId,
          latestTerminalStatus: "completed",
          latestTerminalSummary: null,
          latestTerminalResultContextTransferId: null,
        });

        const finalSequence = yield* orchestrator.getThreadEventSequence(
          localChild(delegated.childThreadId),
        );
        const interruptCall = yield* invoke("t3_thread_interrupt", {
          threadId: localChild(delegated.childThreadId),
          runId: runningFollowup.runId,
          reason: "Allow the queued replay follow-up to run.",
          clientRequestId: "interrupt-delegated-child-replay-1",
        });
        const interrupt = yield* decodeThreadInterruptResult(interruptCall.structuredContent).pipe(
          Effect.orDie,
        );
        expect(interrupt).toMatchObject({
          runId: runningFollowup.runId,
          status: "interrupt_requested",
        });
        yield* orchestrator
          .streamStoredEventsFrom({
            threadId: localChild(delegated.childThreadId),
            afterSequence: finalSequence,
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" &&
                stored.event.payload.id === queuedFollowup.runId &&
                stored.event.payload.status === "completed",
            ),
            Stream.runHead,
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.die("Queued follow-up did not complete."),
                onSome: () => Effect.void,
              }),
            ),
          );

        const finalProjection = yield* orchestrator.getThreadProjection(
          localChild(delegated.childThreadId),
        );
        expect(finalProjection.runs.find((run) => run.id === runningFollowup.runId)?.status).toBe(
          "interrupted",
        );
        expect(finalProjection.runs.find((run) => run.id === queuedFollowup.runId)?.status).toBe(
          "completed",
        );
        expect(
          finalProjection.turnItems.find(
            (item) => item.runId === runningFollowup.runId && item.type === "run_interrupt_result",
          ),
        ).toMatchObject({
          message: "Stopped by an agent: Allow the queued replay follow-up to run.",
        });
        yield* orchestrator
          .streamStoredEventsFrom({
            threadId: parentThreadId,
            afterSequence: finalSequence,
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === followupTasks[1]!.id &&
                stored.event.payload.status === "completed",
            ),
            Stream.runHead,
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.die("Follow-up task did not settle."),
                onSome: () => Effect.void,
              }),
            ),
          );
        const finishedParent = yield* orchestrator.getThreadProjection(parentThreadId);
        expect(
          finishedParent.subagents.find((task) => task.id === followupTasks[0]!.id)?.status,
        ).toBe("interrupted");
        expect(
          finishedParent.subagents.find((task) => task.id === followupTasks[1]!.id),
        ).toMatchObject({
          status: "completed",
          result,
        });
        expect(
          finishedParent.contextTransfers.filter(
            (transfer) =>
              transfer.type === "subagent_result" &&
              transfer.sourceThreadId === delegated.childThreadId,
          ),
        ).toHaveLength(3);
        expect(
          finishedParent.subagents.find((task) => task.id === followupTasks[1]!.id)
            ?.completionDelivery?.state,
        ).toBe("claimed");
        const passive = {
          ...invocation,
          thread: undefined,
          client: {
            sessionId: "cli",
            label: "t3 thread cli",
            access: "read-only" as const,
            creationSource: "cli" as const,
          },
        };
        for (const [name, args] of [
          ["t3_thread_read", { threadId: delegated.childThreadId, limit: 100 }],
          ["t3_thread_list", { projectId, includeSubagents: true }],
        ] as const) {
          const result = yield* invoke(name, args, passive);
          expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
        }
        expect((yield* orchestrator.getThreadProjection(parentThreadId)).subagents).toEqual(
          finishedParent.subagents,
        );
        const finalStatusCall = yield* invoke("task_status", {
          taskId: delegated.taskId,
        });
        const finalStatus = yield* decodeDelegateTaskResult(finalStatusCall.structuredContent).pipe(
          Effect.orDie,
        );
        expect(finalStatus).toMatchObject({
          childRunId: delegated.childRunId,
          status: "completed",
          summary: delegatedResult,
          resultContextTransferId: delegated.resultContextTransferId,
          hasPendingChildRuns: false,
          latestTerminalRunId: queuedFollowup.runId,
          latestTerminalStatus: "completed",
          latestTerminalSummary: latestSummary,
        });
        const followupTransfer = finishedParent.contextTransfers.find(
          (transfer) =>
            transfer.type === "subagent_result" &&
            transfer.sourcePoint.runId === queuedFollowup.runId,
        );
        expect(followupTransfer).toBeDefined();
        expect(finalStatus.latestTerminalResultContextTransferId).toBe(followupTransfer?.id);
        expect(finalStatus.latestTerminalResultContextTransferId).not.toBe(
          delegated.resultContextTransferId,
        );
        const transferReads: Array<string> = [];
        const messageReads: Array<string> = [];
        const runReads: Array<string> = [];
        const projectionTableReads: Array<string> = [];
        const acknowledgementReads: Array<string> = [];
        const tracer = Tracer.make({
          span(options) {
            const span = new Tracer.NativeSpan(options);
            const end = span.end.bind(span);
            span.end = (endTime, exit) => {
              end(endTime, exit);
              const query = span.attributes.get("db.query.text");
              if (typeof query !== "string") return;
              let parent = Option.getOrUndefined(span.parent);
              while (parent?._tag === "Span") {
                if (
                  parent.name === "Orchestrator.dispatchDelegatedTaskCompletionDeliveryResolution"
                ) {
                  acknowledgementReads.push(query);
                  break;
                }
                parent = Option.getOrUndefined(parent.parent);
              }
              for (const [, table] of query.matchAll(/FROM orchestration_v2_projection_(\w+)/gi)) {
                projectionTableReads.push(table!);
              }
              if (/SELECT[\s\S]*FROM orchestration_v2_projection_context_transfers/i.test(query)) {
                transferReads.push(query);
              }
              if (/SELECT\s+payload_json\s+FROM orchestration_v2_projection_runs/i.test(query)) {
                runReads.push(query);
              }
              if (
                /SELECT\s+payload_json\s+FROM orchestration_v2_projection_messages/i.test(query)
              ) {
                messageReads.push(query);
              }
            };
            return span;
          },
        });
        const wait = yield* invoke(
          "t3_thread_wait",
          { threadId: delegated.childThreadId, runId: queuedFollowup.runId },
          { ...invocation, thread: undefined },
        ).pipe(Effect.withTracer(tracer));
        expect(wait.isError).not.toBe(true);
        const expectedWait = {
          threadId: delegated.childThreadId,
          runId: queuedFollowup.runId,
          status: "completed",
          timedOut: false,
        };
        expect(wait.structuredContent).toEqual(expectedWait);
        expect(messageReads).toEqual([]);
        expect(transferReads).toHaveLength(0);
        const settledWait = yield* invoke(
          "t3_thread_wait",
          {
            threadId: delegated.childThreadId,
            runId: queuedFollowup.runId,
            untilDelegatedWorkSettled: true,
          },
          { ...invocation, thread: undefined },
        ).pipe(Effect.withTracer(tracer));
        expect(settledWait.isError).not.toBe(true);
        expect(settledWait.structuredContent).toMatchObject({
          ...expectedWait,
          resultRun: { runId: queuedFollowup.runId, status: "completed" },
        });
        expect(messageReads.length).toBeGreaterThan(0);
        expect(
          messageReads.filter((query) => query.includes("$.notification.source.kind")),
        ).toHaveLength(1);
        // The orchestrator's restart-continuation guard still reads its own records.
        expect(
          messageReads.filter((query) => !query.includes("$.notification.source.kind")),
        ).toHaveLength(1);
        const parentlessRead = yield* invoke(
          "t3_thread_read",
          {
            threadId: delegated.childThreadId,
            limit: 100,
            maxCharsPerItem: result === "😀" ? 1 : 50_000,
          },
          { ...invocation, thread: undefined },
        ).pipe(Effect.withTracer(tracer));
        expect(parentlessRead.isError).not.toBe(true);
        expect(transferReads).toHaveLength(0);
        const afterParentlessRead = yield* orchestrator.getThreadProjection(parentThreadId);
        expect(
          afterParentlessRead.subagents.find((task) => task.id === queuedFollowup.taskId)
            ?.completionDelivery?.state,
        ).toBe("claimed");

        const read = yield* invoke("t3_thread_read", {
          threadId: delegated.childThreadId,
          limit: 100,
          ...(result === "😀" ? { maxCharsPerItem: 1 } : {}),
        }).pipe(Effect.withTracer(tracer));
        expect(read.isError).not.toBe(true);
        expect(transferReads.length).toBeGreaterThan(0);
        const firstPage = yield* decodeThreadRead(read.structuredContent);
        const finalItem = firstPage.items.find((item) => item.text === result.slice(0, 500));
        expect(finalItem).toBeDefined();
        if (result.length > 500) {
          yield* invoke("t3_thread_read", {
            threadId: delegated.childThreadId,
            itemId: finalItem!.itemId,
            textOffset: result.length,
          });
        } else {
          expect(read.structuredContent).toEqual(parentlessRead.structuredContent);
        }
        let text = finalItem!.text ?? "";
        let nextTextOffset = finalItem?.nextTextOffset ?? null;
        while (nextTextOffset !== null) {
          const beforeContinuation = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(
            beforeContinuation.subagents.find((task) => task.id === queuedFollowup.taskId)
              ?.completionDelivery?.state,
          ).toBe("claimed");
          const continuation = yield* invoke("t3_thread_read", {
            threadId: delegated.childThreadId,
            itemId: finalItem!.itemId,
            textOffset: nextTextOffset,
            maxCharsPerItem: 50_000,
          }).pipe(Effect.withTracer(tracer));
          const nextPage = yield* decodeThreadRead(continuation.structuredContent);
          text += nextPage.items[0]?.text ?? "";
          nextTextOffset = nextPage.items[0]?.nextTextOffset ?? null;
        }
        expect(text).toBe(result);
        // The acknowledgement reads the task and its spawning run by id, not the parent's history.
        expect(acknowledgementReads.length).toBeGreaterThan(0);
        expect(
          acknowledgementReads.filter(
            (query) =>
              /FROM orchestration_v2_projection_(nodes|run_attempts)\b/.test(query) ||
              (/FROM orchestration_v2_projection_runs\b/.test(query) &&
                !query.includes("AND run_id IN")) ||
              (/FROM orchestration_v2_projection_subagents\b/.test(query) &&
                !query.includes("AND subagent_id IN")),
          ),
        ).toEqual([]);
        const afterRead = yield* orchestrator.getThreadProjection(parentThreadId);
        expect(
          afterRead.subagents.find((task) => task.id === queuedFollowup.taskId)?.completionDelivery
            ?.state,
        ).toBe("acknowledged");

        const messageReadsBeforeCapabilities = messageReads.length;
        const tableReadsBeforeCapabilities = projectionTableReads.length;
        const capabilities = yield* invoke("orchestrator_capabilities", {}).pipe(
          Effect.withTracer(tracer),
        );
        expect(capabilities.isError).not.toBe(true);
        expect(messageReads).toHaveLength(messageReadsBeforeCapabilities);
        // The caller check reads only the thread row, never its history.
        expect(new Set(projectionTableReads.slice(tableReadsBeforeCapabilities))).toEqual(
          new Set(["threads"]),
        );
        const runReadsBeforeStatus = runReads.length;
        const transferReadsBeforeStatus = transferReads.length;
        const threadRecordReadsBeforeStatus = threadRecordReads.length;
        const statusAgain = yield* invoke("task_status", { taskId: delegated.taskId }).pipe(
          Effect.withTracer(tracer),
        );
        expect(statusAgain.isError).not.toBe(true);
        // A task wait reads result transfers and runs from the child; the parent's stay unread.
        expect(runReads).toHaveLength(runReadsBeforeStatus + 1);
        expect(transferReads).toHaveLength(transferReadsBeforeStatus + 1);
        expect(
          threadRecordReads
            .slice(threadRecordReadsBeforeStatus)
            .filter(({ fields }) => fields.includes("runs") || fields.includes("contextTransfers"))
            .map(({ threadId }) => threadId),
        ).toEqual([delegated.childThreadId]);
      }).pipe(Effect.provide(layerTest));
    }),
  );
  it.live("reads a child result delivered after the parent was interrupted", () =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("late-child-result");
      const raw = yield* readDelegatedTaskStatusTranscript(queuedFollowupResult);
      // Hold the child response in the provider replay until it receives the parent interrupt.
      const start = raw.entries.findIndex((entry) => entry.label === "child.original.item/started");
      const end = raw.entries.findIndex((entry) => entry.label === "child.original.turn/completed");
      expect(start).toBeGreaterThan(0);
      expect(end).toBeGreaterThan(start);
      const providerThread = "019f1000-0000-7000-8000-000000000001";
      const providerTurn = "019f1000-0000-7000-8000-000000000002";
      const { layer } = yield* replayLayer(cwd, {
        ...raw,
        entries: [
          ...raw.entries.slice(0, start),
          {
            type: "expect_outbound",
            label: "parent.turn/interrupt",
            frame: {
              id: 12,
              method: "turn/interrupt",
              params: { threadId: providerThread, turnId: providerTurn },
            },
          },
          { type: "emit_inbound", label: "parent.turn/interrupt", frame: { id: 12, result: {} } },
          {
            type: "emit_inbound",
            label: "parent.turn/completed",
            frame: {
              method: "turn/completed",
              params: {
                threadId: providerThread,
                turn: {
                  id: providerTurn,
                  items: [],
                  status: "interrupted",
                  error: null,
                  startedAt: 1776739467,
                  completedAt: 1776739469,
                  durationMs: 2000,
                },
              },
            },
          },
          ...raw.entries.slice(start, end + 1),
          { type: "runtime_exit", status: "success" },
        ],
      });
      yield* Effect.gen(function* () {
        const { orchestrator, invoke } = yield* startReplayParent(cwd);
        const worktreePath = cwd;
        const rejected = yield* invoke("delegate_task", {
          task: delegatedPrompt,
          workerControls: { maxTurns: 2 },
        });
        expect(rejected.isError).toBe(true);
        const content = rejected.content[0];
        expect(content?.type === "text" && JSON.parse(content.text)).toMatchObject({
          code: "invalid_request",
          message: "Worker controls require a Claude provider.",
        });
        for (const invalid of [
          { isolation: "existing_worktree" },
          { isolation: "shared", worktreePath },
          { isolation: "worktree", worktreePath },
          { worktreePath },
          { isolation: "existing_worktree", worktreePath: `${cwd}/.git` },
        ]) {
          const rejected = yield* invoke("delegate_task", {
            task: delegatedPrompt,
            ...invalid,
          });
          expect(rejected.isError).toBe(true);
          const part = rejected.content[0];
          expect(part?.type === "text" && JSON.parse(part.text)).toMatchObject({
            code: "invalid_request",
          });
        }
        expect((yield* orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(0);
        const request = {
          clientRequestId: "late-child",
          isolation: "existing_worktree",
          worktreePath,
          task: delegatedPrompt,
          target: { providerInstanceId: codexInstanceId, model: codexModel },
          mode: "async",
        };
        const start = yield* invoke("delegate_task", request);
        const task = yield* decodeDelegateTaskResult(start.structuredContent);
        yield* orchestrator
          .streamStoredEventsFrom({ threadId: localChild(task.childThreadId), afterSequence: 0 })
          .pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "provider-turn.updated" && event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        const child = yield* orchestrator.getThreadProjection(localChild(task.childThreadId));
        expect(child.thread.worktreePath).toBe(worktreePath);
        const retry = yield* invoke("delegate_task", request);
        expect(retry.structuredContent).toMatchObject({
          taskId: task.taskId,
          childThreadId: task.childThreadId,
        });
        const parent = yield* orchestrator.getThreadProjection(parentThreadId);
        expect(parent.subagents.find((row) => row.id === task.taskId)?.result).toBeNull();
        const interrupted = yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("interrupt-before-child-result"),
          threadId: parentThreadId,
          runId: parent.runs[0]!.id,
        });
        yield* orchestrator
          .streamStoredEventsFrom({ threadId: parentThreadId, afterSequence: interrupted.sequence })
          .pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "subagent.updated" &&
                event.payload.id === task.taskId &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        const beforeRead = yield* orchestrator.getThreadProjection(parentThreadId);
        const status = yield* invoke("task_status", { taskId: task.taskId });
        expect(status.isError, JSON.stringify(status.content)).not.toBe(true);
        const result = yield* decodeDelegateTaskResult(status.structuredContent);
        const finished = yield* orchestrator.getThreadProjection(parentThreadId);
        expect(finished.runs[0]?.status).toBe("interrupted");
        expect(finished.subagents).toEqual(beforeRead.subagents);
        const transfer = finished.contextTransfers.find(
          (t) => t.type === "subagent_result" && t.sourcePoint.runId === task.childRunId,
        );
        expect(transfer).toBeDefined();
        expect(result).toMatchObject({
          status: "completed",
          summary: delegatedResult,
          resultContextTransferId: transfer!.id,
        });
      }).pipe(Effect.provide(layer));
    }),
  );

  it.live("enforces edit attribution on agent and human queue entries", () =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("queue-attribution");
      const { layer } = yield* replayLayer(
        cwd,
        yield* readDelegatedTaskStatusTranscript(queuedFollowupResult),
      );
      yield* Effect.gen(function* () {
        const { orchestrator, invoke } = yield* startReplayParent(cwd);
        for (const human of [true, false]) {
          const messageId = MessageId.make(human ? "user-queue" : "agent-queue");
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(messageId),
            threadId: parentThreadId,
            messageId,
            text: "Original",
            createdBy: human ? "user" : "system",
            creationSource: human ? "desktop" : "server",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
          });
          const before = yield* orchestrator.getThreadProjection(parentThreadId);
          const queued = before.runs.find((run) => run.userMessageId === messageId)!;
          const edit = yield* invoke("t3_queue", {
            action: "edit",
            threadId: parentThreadId,
            queuedRunId: queued.id,
            text: "Revised",
          });
          const after = yield* orchestrator.getThreadProjection(parentThreadId);
          const message = after.messages.find((row) => row.id === messageId);
          if (human) {
            expect(edit.isError).toBe(true);
            expect(edit.content[0]).toMatchObject({ text: expect.stringContaining("human") });
            expect(message).toEqual(before.messages.find((row) => row.id === messageId));
          } else {
            expect(edit.isError).not.toBe(true);
            expect(message).toMatchObject({
              text: "Revised",
              createdBy: "agent",
              creationSource: "mcp",
            });
          }
        }
      }).pipe(Effect.provide(layer));
    }),
  );
});
