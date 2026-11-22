import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpThreadWaitResult,
} from "./orchestratorMcp.ts";

const decodeDelegateTaskInput = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const decodeDelegateTaskResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);
const decodeThreadWaitResult = Schema.decodeUnknownSync(OrchestratorMcpThreadWaitResult);

describe("orchestrator MCP contracts", () => {
  it("accepts a delegated task in an existing worktree", () => {
    const request = {
      task: "Review the branch",
      isolation: "existing_worktree",
      worktreePath: "/repo/review",
    };
    expect(decodeDelegateTaskInput(request)).toEqual(request);
  });

  it("decodes Claude worker controls and rejects unknown environment keys", () => {
    const workerControls = {
      agent: "reader",
      maxTurns: 4,
      allowedTools: ["Read", "Grep", "Glob"],
      disallowedTools: ["Edit"],
      exposeT3Mcp: false,
      env: { GCV_OWNER: "runner", TMPDIR: "/tmp/worker", CLAUDE_CODE_PROMPT_CACHE_TTL: "5m" },
    };
    expect(decodeDelegateTaskInput({ task: "Inspect", workerControls }).workerControls).toEqual(
      workerControls,
    );
    for (const key of ["PATH", "LD_PRELOAD", "ANTHROPIC_API_KEY", "T3CODE_TURN_SOURCE"]) {
      expect(() =>
        decodeDelegateTaskInput({
          task: "Inspect",
          workerControls: { env: { ...workerControls.env, [key]: "invalid" } },
        }),
      ).toThrow(`Unsupported Claude worker environment key: ${key}`);
    }
    expect(() =>
      decodeDelegateTaskInput({ task: "Inspect", workerControls: { maxTurns: 0 } }),
    ).toThrow();
  });

  it.each([
    { agent: "-reader" },
    { allowedTools: ["default"] },
    { allowedTools: ["mcp__t3__read"] },
    { allowedTools: [], exposeT3Mcp: true },
  ])("rejects invalid Claude worker controls %j", (workerControls) => {
    expect(() => decodeDelegateTaskInput({ task: "Inspect", workerControls })).toThrow();
  });

  it("decodes cross-provider delegated task requests and durable results", () => {
    const request = decodeDelegateTaskInput({
      task: "Inspect the workspace and report the result.",
      target: {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
      },
      mode: "wait",
      timeoutMs: 5_000,
      clientRequestId: "delegate-1",
      runtimeMode: "inherit",
      interactionMode: "inherit",
    });
    const result = decodeDelegateTaskResult({
      taskId: "node-task-1",
      childThreadId: "thread-child-1",
      childRunId: "run-child-1",
      childNodeId: "node-task-1",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      providerInstanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
      summary: "Workspace inspected.",
      resultContextTransferId: "context-transfer-result-1",
      latestTerminalRunId: "run-child-1",
      latestTerminalStatus: "completed",
      latestTerminalSummary: "Workspace inspected.",
      latestTerminalResultContextTransferId: "context-transfer-result-1",
      hasPendingQuestion: false,
      pendingApproval: null,
      waitTimedOut: false,
    });

    expect(request.target?.providerInstanceId).toBe("claudeAgent");
    expect(result.status).toBe("completed");
    expect(result.summary).toBe("Workspace inspected.");
    expect(result.pendingApproval).toBeNull();
    const pendingApproval = {
      requestId: "request-approval-1",
      kind: "command",
      runId: "run-child-1",
    };
    expect(
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, input: { command: "private command" } },
      }).pendingApproval,
    ).toEqual(pendingApproval);
    expect(() =>
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, kind: "user_input" },
      }),
    ).toThrow();
  });

  it("decodes a running-task next written by another version", () => {
    const next = "Completion wakes this thread, so end your turn. Do not wait on it or poll it.";
    expect(
      decodeThreadWaitResult({
        threadId: "thread-child-1",
        runId: "run-child-1",
        status: "running",
        timedOut: true,
        next,
      }).next,
    ).toBe(next);
  });
});
