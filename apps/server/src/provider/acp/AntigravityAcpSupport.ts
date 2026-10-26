import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  ANTIGRAVITY_DEFAULT_MODEL,
  type AntigravityAuthMethod,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  type ProviderSendTurnInput,
  type RuntimeMode,
} from "@t3tools/contracts";
import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import {
  collectSessionConfigOptionValues,
  extractModelConfigId,
  findModelConfigOption,
  findSessionConfigOption,
  parseSessionModeState,
  type AcpSessionModeState,
} from "@t3tools/provider-acp/server/runtimeModel";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/compat";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import {
  makeAntigravityStderrHandler,
  makeAntigravityStdoutTransform,
} from "../antigravityAuthSupport.ts";
import * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { normalizeAntigravitySessionUpdate } from "./AntigravityProtocol.ts";

export interface AntigravityAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  | "authMethodId"
  | "authenticateEagerly"
  | "cancelBehavior"
  | "clientCapabilities"
  | "onStderr"
  | "resumeMethod"
  | "transformSessionUpdate"
  | "transformStdout"
> {
  readonly agentDeviceEnvironment?: Readonly<Record<string, string>>;
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly onAuthorizationUrl?: (url: string) => Effect.Effect<void, EffectAcpErrors.AcpError>;
  readonly clientFileSystem?: boolean;
  readonly authMethod?: AntigravityAuthMethod;
}

export interface AgyModelCatalogEntry {
  readonly id: string;
  readonly label: string;
}

export const FALLBACK_AGY_MODELS: ReadonlyArray<AgyModelCatalogEntry> = [
  { id: "gemini-4-argon-medium", label: "Gemini 4 Argon (Medium)" },
  { id: "gemini-4-argon-high", label: "Gemini 4 Argon (High)" },
  { id: "gemini-4-argon-low", label: "Gemini 4 Argon (Low)" },
  { id: "gemini-3.8-flash-high", label: "Gemini 3.8 Flash (High)" },
  { id: "gemini-3.8-flash-medium", label: "Gemini 3.8 Flash (Medium)" },
  { id: "gemini-3.8-flash-low", label: "Gemini 3.8 Flash (Low)" },
  { id: "claude-opus-5-5-low", label: "Claude Opus 5.5 (Low)" },
  { id: "claude-opus-5-5-medium", label: "Claude Opus 5.5 (Medium)" },
  { id: "claude-opus-5-5-high", label: "Claude Opus 5.5 (High)" },
  { id: "claude-sonnet-5-5-low", label: "Claude Sonnet 5.5 (Low)" },
  { id: "claude-sonnet-5-5-medium", label: "Claude Sonnet 5.5 (Medium)" },
  { id: "claude-sonnet-5-5-high", label: "Claude Sonnet 5.5 (High)" },
  { id: "gpt-oss-120b-medium", label: "GPT-OSS 120B (Medium)" },
];

const AGY_MODEL_CACHE_TTL_MS = 60_000;
let cachedAgyCatalog: {
  readonly models: ReadonlyArray<AgyModelCatalogEntry>;
  readonly defaultModelId: string;
  readonly fetchedAtMs: number;
} | null = null;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function extractTrailingJsonObject(raw: string): Record<string, unknown> | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  const lines = trimmed.split(/\r?\n/);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i]!;
    const braceIndex = line.indexOf("{");
    if (braceIndex === -1) continue;
    try {
      const parsed: unknown = JSON.parse(line.slice(braceIndex));
      if (isRecord(parsed)) return parsed;
    } catch {
      // Continue scanning earlier lines or fall back to full-buffer brace search.
    }
  }
  const firstBrace = trimmed.indexOf("{");
  const lastBrace = trimmed.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace > firstBrace) {
    try {
      const parsed: unknown = JSON.parse(trimmed.slice(firstBrace, lastBrace + 1));
      if (isRecord(parsed)) return parsed;
    } catch {
      return undefined;
    }
  }
  return undefined;
}

export const probeAgyCatalog = Effect.fn("probeAgyCatalog")(function* (input: {
  readonly command: string;
  readonly cwd: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly forceRefresh?: boolean;
}): Effect.fn.Return<
  {
    readonly models: ReadonlyArray<AgyModelCatalogEntry>;
    readonly defaultModelId: string;
  },
  EffectAcpErrors.AcpError
> {
  const now = Date.now();
  if (
    !input.forceRefresh &&
    cachedAgyCatalog !== null &&
    now - cachedAgyCatalog.fetchedAtMs < AGY_MODEL_CACHE_TTL_MS
  ) {
    return {
      models: cachedAgyCatalog.models,
      defaultModelId: cachedAgyCatalog.defaultModelId,
    };
  }

  const runAgyCommand = (args: ReadonlyArray<string>, timeoutMs: number) =>
    Effect.scoped(
      Effect.gen(function* () {
        const child = yield* input.spawner.spawn(
          ChildProcess.make(input.command, [...args], {
            cwd: input.cwd,
            ...(input.env ? { env: input.env } : {}),
            extendEnv: true,
            shell: false,
          }),
        );
        const [stdout, stderr, exitCode] = yield* Effect.all(
          [
            collectUint8StreamText({ stream: child.stdout, maxBytes: 256 * 1024 }),
            collectUint8StreamText({ stream: child.stderr, maxBytes: 64 * 1024 }),
            child.exitCode,
          ],
          { concurrency: "unbounded" },
        );
        return {
          stdout: stdout.text,
          stderr: stderr.text,
          exitCode: Number(exitCode),
        };
      }).pipe(Effect.timeoutOption(timeoutMs)),
    ).pipe(
      Effect.mapError(
        (cause) =>
          new EffectAcpErrors.AcpSpawnError({
            command: input.command,
            cause,
          }),
      ),
    );

  // Fast local check (~30ms) for current default model and binary health.
  const currentModelRes = yield* runAgyCommand(
    ["--output-format", "json", "--print=/model"],
    10_000,
  );
  let defaultModelId = cachedAgyCatalog?.defaultModelId ?? FALLBACK_AGY_MODELS[0]!.id;
  if (Option.isSome(currentModelRes) && currentModelRes.value.exitCode === 0) {
    const parsed = extractTrailingJsonObject(currentModelRes.value.stdout);
    const command = isRecord(parsed?.command) ? parsed.command : undefined;
    const data = isRecord(command?.data) ? command.data : undefined;
    if (typeof data?.id === "string" && data.id.trim().length > 0) {
      defaultModelId = data.id.trim();
    }
  } else if (Option.isSome(currentModelRes) && currentModelRes.value.exitCode !== 0) {
    const detail =
      currentModelRes.value.stderr.trim() ||
      currentModelRes.value.stdout.trim() ||
      `agy exited with code ${currentModelRes.value.exitCode}`;
    return yield* new EffectAcpErrors.AcpTransportError({
      detail,
      cause: currentModelRes.value,
    });
  }

  const modelsRes = yield* runAgyCommand(["--output-format", "json", "models"], 15_000);
  let models: ReadonlyArray<AgyModelCatalogEntry> = cachedAgyCatalog?.models ?? FALLBACK_AGY_MODELS;
  if (Option.isSome(modelsRes) && modelsRes.value.exitCode === 0) {
    const parsed = extractTrailingJsonObject(modelsRes.value.stdout);
    const command = isRecord(parsed?.command) ? parsed.command : undefined;
    const data = isRecord(command?.data) ? command.data : undefined;
    const rawModels = Array.isArray(data?.models) ? data.models : [];
    const parsedModels = rawModels.flatMap((entry): ReadonlyArray<AgyModelCatalogEntry> => {
      if (!isRecord(entry) || typeof entry.id !== "string") return [];
      const id = entry.id.trim();
      if (!id) return [];
      const label =
        typeof entry.label === "string" && entry.label.trim().length > 0 ? entry.label.trim() : id;
      return [{ id, label }];
    });
    if (parsedModels.length > 0) {
      models = parsedModels;
    }
  }

  if (!models.some((entry) => entry.id === defaultModelId)) {
    models = [{ id: defaultModelId, label: defaultModelId }, ...models];
  }

  cachedAgyCatalog = {
    models,
    defaultModelId,
    fetchedAtMs: Date.now(),
  };
  return { models, defaultModelId };
});

export function isDirectAgyCliCommand(command: string): boolean {
  return /^(?:agy|agy\.exe)$/i.test(NodePath.basename(command));
}

export function buildAgyConfigOptions(
  models: ReadonlyArray<AgyModelCatalogEntry>,
  currentModelId: string,
  currentModeId: string,
): ReadonlyArray<EffectAcpSchema.SessionConfigOption> {
  return [
    {
      type: "select",
      id: "model",
      name: "Model",
      category: "model",
      currentValue: currentModelId,
      options: models.map((model) => ({
        value: model.id,
        name: model.label,
      })),
    },
    {
      type: "select",
      id: "mode",
      name: "Mode",
      category: "mode",
      currentValue: currentModeId,
      options: [
        { value: "default", name: "Default" },
        { value: "auto_edit", name: "Auto Edit" },
        { value: "plan", name: "Plan" },
        { value: "yolo", name: "Full Access" },
      ],
    },
  ];
}

function buildAgySessionModes(currentModeId: string) {
  return {
    currentModeId,
    availableModes: [
      { id: "default", name: "Default" },
      { id: "auto_edit", name: "Auto Edit" },
      { id: "plan", name: "Plan" },
      { id: "yolo", name: "Full Access" },
    ],
  };
}

function classifyAgyToolKind(
  toolName: string,
): "execute" | "read" | "edit" | "delete" | "search" | "fetch" | "other" {
  switch (toolName) {
    case "run_command":
    case "command_status":
    case "send_command_input":
    case "manage_task":
    case "notebook_execution":
      return "execute";
    case "view_file":
    case "read_file":
    case "list_dir":
    case "read_resource":
    case "list_resources":
      return "read";
    case "write_to_file":
    case "write_file":
    case "replace_file_content":
    case "multi_replace_file_content":
    case "edit_file":
    case "sed_file":
    case "notebook_edit":
      return "edit";
    case "delete_knowledge":
      return "delete";
    case "grep_search":
    case "find_by_name":
    case "search_marketplace":
      return "search";
    case "search_web":
    case "read_url_content":
    case "open_browser_url":
    case "read_browser_page":
      return "fetch";
    default:
      return "other";
  }
}

function formatAgyToolTitle(
  toolName: string,
  parameters: Record<string, unknown> | undefined,
): string {
  if (toolName === "invoke_subagent" || toolName === "browser_subagent") {
    return "Running start_subagent";
  }
  if (toolName === "run_command") {
    const cmd =
      typeof parameters?.CommandLine === "string"
        ? parameters.CommandLine.trim()
        : typeof parameters?.command === "string"
          ? parameters.command.trim()
          : "";
    return cmd ? `Running \`${cmd}\`` : "Running command";
  }
  const fileCandidate =
    parameters?.TargetFile ??
    parameters?.AbsolutePath ??
    parameters?.Path ??
    parameters?.DirectoryPath ??
    parameters?.SearchPath;
  if (typeof fileCandidate === "string" && fileCandidate.trim().length > 0) {
    return `${toolName}: ${fileCandidate.trim()}`;
  }
  return `Running ${toolName}`;
}

function enrichAgyToolRawInput(
  parameters: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!parameters) return undefined;
  const pathCandidate =
    parameters.path ??
    parameters.TargetFile ??
    parameters.AbsolutePath ??
    parameters.Path ??
    parameters.DirectoryPath ??
    parameters.SearchPath;
  return {
    ...parameters,
    ...(typeof pathCandidate === "string" && pathCandidate.trim().length > 0
      ? { path: pathCandidate.trim() }
      : {}),
  };
}

function parseAgyCommandExitCode(output: string): number | undefined {
  const match = /Command exited with code (-?\d+)/.exec(output);
  if (!match?.[1]) return undefined;
  const parsed = Number.parseInt(match[1], 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function resolveAgyConfigDir(env?: NodeJS.ProcessEnv): string {
  const home = env?.HOME ?? process.env.HOME ?? NodeOS.homedir();
  return NodePath.join(home, ".gemini", "antigravity-cli");
}

function readPersistedAgyConversationId(
  sessionId: string,
  env?: NodeJS.ProcessEnv,
): string | undefined {
  try {
    const agyDir = resolveAgyConfigDir(env);
    const directDb = NodePath.join(agyDir, "conversations", `${sessionId}.db`);
    if (NodeFS.existsSync(directDb)) {
      return sessionId;
    }
    const mappingPath = NodePath.join(agyDir, "t3-sessions", `${sessionId}.json`);
    if (!NodeFS.existsSync(mappingPath)) {
      return undefined;
    }
    const parsed: unknown = JSON.parse(NodeFS.readFileSync(mappingPath, "utf8"));
    if (
      isRecord(parsed) &&
      typeof parsed.conversationId === "string" &&
      parsed.conversationId.trim().length > 0
    ) {
      const conversationId = parsed.conversationId.trim();
      const mappedDb = NodePath.join(agyDir, "conversations", `${conversationId}.db`);
      if (NodeFS.existsSync(mappedDb)) {
        return conversationId;
      }
    }
  } catch {
    // Ignore filesystem errors during session lookup.
  }
  return undefined;
}

function persistAgyConversationId(
  sessionId: string,
  conversationId: string,
  env?: NodeJS.ProcessEnv,
): void {
  try {
    const mappingDir = NodePath.join(resolveAgyConfigDir(env), "t3-sessions");
    NodeFS.mkdirSync(mappingDir, { recursive: true, mode: 0o700 });
    const mappingPath = NodePath.join(mappingDir, `${sessionId}.json`);
    NodeFS.writeFileSync(
      mappingPath,
      `${JSON.stringify({ sessionId, conversationId, updatedAt: new Date().toISOString() })}\n`,
      { mode: 0o600 },
    );
  } catch {
    // Ignore persistence errors.
  }
}

export const makeAntigravityAcpRuntime = Effect.fn("makeAntigravityAcpRuntime")(function* (
  input: AntigravityAcpRuntimeInput,
): Effect.fn.Return<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> {
  if (!isDirectAgyCliCommand(input.spawn.command)) {
    const context = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        authMethodId: input.authMethod ?? "oauth-personal",
        authenticateEagerly: true,
        resumeMethod: "resume",
        cancelBehavior: "wait-for-prompt",
        clientCapabilities: {
          fs: {
            readTextFile: input.clientFileSystem === true,
            writeTextFile: input.clientFileSystem === true,
          },
          terminal: false,
        },
        transformStdout: makeAntigravityStdoutTransform(
          input.onAuthorizationUrl ? { onAuthorizationUrl: input.onAuthorizationUrl } : {},
        ),
        onStderr: makeAntigravityStderrHandler(
          input.onAuthorizationUrl ? { onAuthorizationUrl: input.onAuthorizationUrl } : {},
        ),
        transformSessionUpdate: normalizeAntigravitySessionUpdate,
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(Effect.provide(context));
  }

  const crypto = yield* Crypto.Crypto;
  const runtimeScope = yield* Scope.fork(yield* Scope.Scope);
  const eventQueue = yield* Queue.unbounded<AcpSessionRuntime.AcpSessionRuntimeEvent>();
  const sessionUpdateHandlersRef = yield* Ref.make<
    ReadonlyArray<
      (
        notification: EffectAcpSchema.SessionNotification,
      ) => Effect.Effect<void, EffectAcpErrors.AcpError>
    >
  >([]);
  const modelsRef = yield* Ref.make<ReadonlyArray<AgyModelCatalogEntry>>(FALLBACK_AGY_MODELS);
  const currentModelRef = yield* Ref.make<string>(FALLBACK_AGY_MODELS[0]!.id);
  const currentModeRef = yield* Ref.make<string>("default");
  const modeStateRef = yield* Ref.make<AcpSessionModeState | undefined>(
    parseSessionModeState({ modes: buildAgySessionModes("default") }),
  );
  const configOptionsRef = yield* Ref.make<ReadonlyArray<EffectAcpSchema.SessionConfigOption>>(
    buildAgyConfigOptions(FALLBACK_AGY_MODELS, FALLBACK_AGY_MODELS[0]!.id, "default"),
  );
  const initializeResultRef = yield* Ref.make<Option.Option<EffectAcpSchema.InitializeResponse>>(
    Option.none(),
  );
  const startedStateRef = yield* Ref.make<
    Option.Option<AcpSessionRuntime.AcpSessionRuntimeStartResult>
  >(Option.none());
  const activePromptFiberRef = yield* Ref.make<
    Option.Option<Fiber.Fiber<EffectAcpSchema.PromptResponse, EffectAcpErrors.AcpError>>
  >(Option.none());
  const assistantItemRuntimeId = yield* crypto
    .randomUUIDv4()
    .pipe(Effect.orElseSucceed(() => "agy-runtime"));
  const assistantSegmentRef = yield* Ref.make<{
    readonly nextSegmentIndex: number;
    readonly activeItemId?: string;
  }>({ nextSegmentIndex: 0 });
  const conversationMap = new Map<string, string>();

  const emitSessionNotification = Effect.fnUntraced(function* (
    rawNotification: EffectAcpSchema.SessionNotification,
  ) {
    const notification = normalizeAntigravitySessionUpdate(rawNotification);
    const handlers = yield* Ref.get(sessionUpdateHandlersRef);
    for (const handler of handlers) {
      yield* handler(notification).pipe(Effect.ignore);
    }
  });

  const ensureAssistantSegment = (sessionId: string) =>
    Ref.modify(
      assistantSegmentRef,
      (
        current,
      ): readonly [
        { readonly itemId: string; readonly started: boolean },
        { readonly nextSegmentIndex: number; readonly activeItemId?: string },
      ] => {
        if (current.activeItemId) {
          return [{ itemId: current.activeItemId, started: false }, current];
        }
        const itemId = `assistant:${sessionId}:runtime:${assistantItemRuntimeId}:segment:${current.nextSegmentIndex}`;
        return [
          { itemId, started: true },
          { nextSegmentIndex: current.nextSegmentIndex + 1, activeItemId: itemId },
        ];
      },
    ).pipe(
      Effect.tap((segment) =>
        segment.started
          ? Queue.offer(eventQueue, { _tag: "AssistantItemStarted", itemId: segment.itemId })
          : Effect.void,
      ),
      Effect.map((segment) => segment.itemId),
    );

  const closeAssistantSegment = Ref.modify(
    assistantSegmentRef,
    (
      current,
    ): readonly [
      string | undefined,
      { readonly nextSegmentIndex: number; readonly activeItemId?: string },
    ] => {
      if (!current.activeItemId) {
        return [undefined, current];
      }
      return [current.activeItemId, { nextSegmentIndex: current.nextSegmentIndex }];
    },
  ).pipe(
    Effect.flatMap((itemId) =>
      itemId
        ? Queue.offer(eventQueue, { _tag: "AssistantItemCompleted", itemId }).pipe(Effect.asVoid)
        : Effect.void,
    ),
  );

  const initialize = Effect.gen(function* () {
    const existing = yield* Ref.get(initializeResultRef);
    if (Option.isSome(existing)) {
      return existing.value;
    }
    const catalog = yield* probeAgyCatalog({
      command: input.spawn.command,
      cwd: input.spawn.cwd ?? input.cwd,
      ...(input.spawn.env ? { env: input.spawn.env } : {}),
      spawner: input.childProcessSpawner,
    });
    yield* Ref.set(modelsRef, catalog.models);
    yield* Ref.set(currentModelRef, catalog.defaultModelId);
    const modeId = yield* Ref.get(currentModeRef);
    const configOptions = buildAgyConfigOptions(catalog.models, catalog.defaultModelId, modeId);
    yield* Ref.set(configOptionsRef, configOptions);

    const initResponse: EffectAcpSchema.InitializeResponse = {
      protocolVersion: 1,
      agentInfo: {
        name: "antigravity-acp",
        title: "Google Antigravity (agy)",
        version: "1.4.7",
      },
      agentCapabilities: {
        loadSession: true,
        promptCapabilities: {
          image: true,
          audio: true,
          embeddedContext: true,
        },
        mcpCapabilities: {
          http: true,
          sse: true,
        },
        sessionCapabilities: {
          list: {},
          resume: {},
        },
        auth: {
          logout: {},
        },
      },
      authMethods: [
        {
          id: "oauth-personal",
          name: "Log in with Google",
          description: "Log in with your Google account",
        },
        {
          id: "oauth-business",
          name: "Log in with Gemini Enterprise",
          description: "Log in with your Gemini Enterprise account",
        },
        {
          id: "gemini-api-key",
          name: "Gemini API key",
          description: "Use an API key with Gemini Developer API",
        },
        {
          id: "agent-platform",
          name: "Gemini Enterprise Agent Platform",
          description: "Use Gemini Enterprise Agent Platform",
        },
      ],
    };
    yield* Ref.set(initializeResultRef, Option.some(initResponse));
    return initResponse;
  });

  const buildStartResult = Effect.fnUntraced(function* (sessionId: string) {
    const initializeResult = yield* initialize;
    const models = yield* Ref.get(modelsRef);
    const currentModelId = yield* Ref.get(currentModelRef);
    const currentModeId = yield* Ref.get(currentModeRef);
    const configOptions = buildAgyConfigOptions(models, currentModelId, currentModeId);
    const modes = buildAgySessionModes(currentModeId);
    yield* Ref.set(configOptionsRef, configOptions);
    yield* Ref.set(modeStateRef, parseSessionModeState({ configOptions, modes }));
    const sessionSetupResult: EffectAcpSchema.NewSessionResponse = {
      sessionId,
      configOptions,
      modes,
    };
    const result: AcpSessionRuntime.AcpSessionRuntimeStartResult = {
      sessionId,
      initializeResult,
      sessionSetupResult,
      modelConfigId: extractModelConfigId(sessionSetupResult) ?? "model",
    };
    yield* Ref.set(startedStateRef, Option.some(result));
    return result;
  });

  const start = Effect.gen(function* () {
    const existing = yield* Ref.get(startedStateRef);
    if (Option.isSome(existing)) {
      return existing.value;
    }
    const sessionId =
      input.resumeSessionId ??
      (yield* crypto.randomUUIDv4().pipe(
        Effect.mapError(
          (cause) =>
            new EffectAcpErrors.AcpTransportError({
              detail: "Failed to generate an Antigravity session ID.",
              cause,
            }),
        ),
      ));
    return yield* buildStartResult(sessionId);
  });

  const syncConfigOptions = Effect.gen(function* () {
    const models = yield* Ref.get(modelsRef);
    const currentModelId = yield* Ref.get(currentModelRef);
    const currentModeId = yield* Ref.get(currentModeRef);
    const configOptions = buildAgyConfigOptions(models, currentModelId, currentModeId);
    const modes = buildAgySessionModes(currentModeId);
    yield* Ref.set(configOptionsRef, configOptions);
    yield* Ref.set(modeStateRef, parseSessionModeState({ configOptions, modes }));
    const started = yield* Ref.get(startedStateRef);
    if (Option.isSome(started)) {
      const updatedSetup: EffectAcpSchema.NewSessionResponse = {
        sessionId: started.value.sessionId,
        configOptions,
        modes,
      };
      yield* Ref.set(
        startedStateRef,
        Option.some({
          ...started.value,
          sessionSetupResult: updatedSetup,
        }),
      );
    }
    yield* Queue.offer(eventQueue, {
      _tag: "ConfigOptionsUpdated",
      configOptions,
      rawPayload: { configOptions },
    });
    return configOptions;
  });

  const setConfigOption = Effect.fn("AntigravityAcpRuntime.setConfigOption")(function* (
    configId: string,
    value: string | boolean,
  ): Effect.fn.Return<EffectAcpSchema.SetSessionConfigOptionResponse, EffectAcpErrors.AcpError> {
    const configOptions = yield* Ref.get(configOptionsRef);
    const target = findSessionConfigOption(configOptions, configId);
    if (target && target.type === "select") {
      if (typeof value !== "string") {
        return yield* EffectAcpErrors.AcpRequestError.invalidParams(
          `Expected string value for config option '${configId}'.`,
        );
      }
      const allowed = collectSessionConfigOptionValues(target);
      if (
        !allowed.includes(value) &&
        !(configId === "model" && input.customModels?.includes(value))
      ) {
        return yield* EffectAcpErrors.AcpRequestError.invalidParams(
          `Unsupported value '${value}' for config option '${configId}'.`,
        );
      }
    }
    if (configId === "model" && typeof value === "string") {
      yield* Ref.set(currentModelRef, value);
    } else if (configId === "mode" && typeof value === "string") {
      yield* Ref.set(currentModeRef, value);
      yield* Queue.offer(eventQueue, { _tag: "ModeChanged", modeId: value });
    }
    const updated = yield* syncConfigOptions;
    return { configOptions: updated };
  });

  const drainEvents = Effect.gen(function* () {
    const acknowledge = yield* Deferred.make<void>();
    yield* closeAssistantSegment;
    yield* Queue.offer(eventQueue, { _tag: "EventStreamBarrier", acknowledge });
    yield* Deferred.await(acknowledge).pipe(Effect.timeoutOption("2 seconds"), Effect.ignore);
  });

  const cancel = Effect.gen(function* () {
    const active = yield* Ref.get(activePromptFiberRef);
    if (Option.isSome(active)) {
      yield* Fiber.interrupt(active.value).pipe(Effect.ignore);
      yield* Ref.set(activePromptFiberRef, Option.none());
    }
    yield* closeAssistantSegment;
  });

  const prompt: AcpSessionRuntime.AcpSessionRuntime["Service"]["prompt"] = (
    payload,
    promptOptions,
  ) =>
    Effect.gen(function* () {
      const started = yield* start;
      const sessionId = started.sessionId;
      const modelId = yield* Ref.get(currentModelRef);
      const modeId = yield* Ref.get(currentModeRef);
      yield* closeAssistantSegment;

      const tempAttachmentFiles: string[] = [];
      const promptParts: string[] = [];
      for (const block of payload.prompt) {
        if (block.type === "text") {
          if (block.text.trim().length > 0) {
            promptParts.push(block.text);
          }
        } else if (block.type === "resource" && "text" in block.resource) {
          const uri = block.resource.uri ?? "attached-file";
          promptParts.push(`File (${uri}):\n\`\`\`\n${block.resource.text}\n\`\`\``);
        } else if (block.type === "resource_link") {
          promptParts.push(`Attached file: ${block.name ?? block.uri} (${block.uri})`);
        } else if (block.type === "image" || block.type === "audio") {
          const ext = block.mimeType.split("/")[1]?.split(";")[0] ?? "bin";
          const fileId = yield* crypto
            .randomUUIDv4()
            .pipe(Effect.orElseSucceed(() => `${Date.now()}`));
          const tempPath = NodePath.join(NodeOS.tmpdir(), `t3-agy-${fileId}.${ext}`);
          try {
            NodeFS.writeFileSync(tempPath, Buffer.from(block.data, "base64"), { mode: 0o600 });
            tempAttachmentFiles.push(tempPath);
            promptParts.push(`Attached ${block.type} file at ${tempPath}`);
          } catch {
            // Ignore temp write errors.
          }
        }
      }
      const promptText = promptParts.join("\n\n").trim() || "Continue.";

      const existingConversationId =
        conversationMap.get(sessionId) ??
        readPersistedAgyConversationId(sessionId, input.spawn.env);
      if (existingConversationId) {
        conversationMap.set(sessionId, existingConversationId);
      }

      const args: string[] = ["--input-format", "stream-json", "--output-format", "stream-json"];
      if (existingConversationId) {
        args.push("--conversation", existingConversationId);
      }
      if (modelId && modelId !== ANTIGRAVITY_DEFAULT_MODEL) {
        args.push("--model", modelId);
      }
      if (modeId === "yolo") {
        args.push("--mode", "auto", "--dangerously-skip-permissions");
      } else if (modeId === "auto_edit") {
        args.push("--mode", "accept-edits");
      } else if (modeId === "plan") {
        args.push("--mode", "plan");
      } else {
        args.push("--mode", "auto");
      }
      const extraDirs = new Set<string>(input.additionalDirectories ?? []);
      if (tempAttachmentFiles.length > 0) {
        extraDirs.add(NodeOS.tmpdir());
      }
      for (const dir of extraDirs) {
        if (dir.trim().length > 0) {
          args.push("--add-dir", dir);
        }
      }
      args.push("--print=");

      const runTurn = Effect.scoped(
        Effect.gen(function* () {
          const child = yield* input.childProcessSpawner
            .spawn(
              ChildProcess.make(input.spawn.command, args, {
                cwd: input.spawn.cwd ?? input.cwd,
                ...(input.spawn.env ? { env: input.spawn.env } : {}),
                extendEnv: true,
                shell: false,
              }),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new EffectAcpErrors.AcpSpawnError({
                    command: input.spawn.command,
                    cause,
                  }),
              ),
            );

          const stdinMessage = `${JSON.stringify({
            event: "user",
            message: { content: promptText },
          })}\n`;
          yield* Stream.fromIterable([new TextEncoder().encode(stdinMessage)]).pipe(
            Stream.run(child.stdin),
            Effect.mapError(
              (cause) =>
                new EffectAcpErrors.AcpTransportError({
                  detail: "Failed to write prompt to agy stdin.",
                  cause,
                }),
            ),
          );

          const seenToolSteps = new Set<number>();
          let resultError: string | undefined;
          let lineBuffer = "";

          const handleNdjsonLine = Effect.fnUntraced(function* (rawLine: string) {
            const trimmed = rawLine.trim();
            if (!trimmed.startsWith("{")) return;
            let parsed: unknown;
            try {
              parsed = JSON.parse(trimmed);
            } catch {
              return;
            }
            if (!isRecord(parsed)) return;

            if (parsed.event === "init" && typeof parsed.conversation_id === "string") {
              const convId = parsed.conversation_id.trim();
              if (convId.length > 0) {
                conversationMap.set(sessionId, convId);
                persistAgyConversationId(sessionId, convId, input.spawn.env);
              }
              return;
            }

            if (parsed.event === "step_update" && isRecord(parsed.step_update)) {
              const su = parsed.step_update;
              if (typeof su.conversation_id === "string" && su.conversation_id.trim().length > 0) {
                const convId = su.conversation_id.trim();
                if (!conversationMap.has(sessionId)) {
                  conversationMap.set(sessionId, convId);
                  persistAgyConversationId(sessionId, convId, input.spawn.env);
                }
              }
              const stepIndex = typeof su.step_index === "number" ? su.step_index : 0;
              const stepType = typeof su.step_type === "string" ? su.step_type : "";
              const stepState = typeof su.state === "string" ? su.state : "";

              if (stepType === "agent_response") {
                if (typeof su.thinking_delta === "string" && su.thinking_delta.length > 0) {
                  yield* Queue.offer(eventQueue, {
                    _tag: "ThoughtDelta",
                    text: su.thinking_delta,
                    rawPayload: parsed,
                  });
                  yield* emitSessionNotification({
                    sessionId,
                    update: {
                      sessionUpdate: "agent_thought_chunk",
                      content: { type: "text", text: su.thinking_delta },
                    },
                  });
                }
                if (typeof su.text_delta === "string" && su.text_delta.length > 0) {
                  const itemId = yield* ensureAssistantSegment(sessionId);
                  yield* Queue.offer(eventQueue, {
                    _tag: "ContentDelta",
                    itemId,
                    text: su.text_delta,
                    rawPayload: parsed,
                  });
                  yield* emitSessionNotification({
                    sessionId,
                    update: {
                      sessionUpdate: "agent_message_chunk",
                      content: { type: "text", text: su.text_delta },
                    },
                  });
                }
              } else if (stepType === "tool") {
                yield* closeAssistantSegment;
                const toolInfo = isRecord(su.tool_info) ? su.tool_info : undefined;
                const toolName =
                  (typeof su.tool_name === "string" && su.tool_name.trim()) ||
                  (typeof toolInfo?.name === "string" && toolInfo.name.trim()) ||
                  "tool";
                const parameters = isRecord(toolInfo?.parameters) ? toolInfo.parameters : undefined;
                const output = typeof toolInfo?.output === "string" ? toolInfo.output : undefined;
                const toolCallId = `agy-tool-${stepIndex}`;
                const kind = classifyAgyToolKind(toolName);
                const title = formatAgyToolTitle(toolName, parameters);
                const status =
                  stepState === "DONE"
                    ? ("completed" as const)
                    : stepState === "ERROR" || stepState === "FAILED"
                      ? ("failed" as const)
                      : ("inProgress" as const);
                const rawInput = enrichAgyToolRawInput(parameters);
                const exitCode =
                  kind === "execute" && output !== undefined
                    ? parseAgyCommandExitCode(output)
                    : undefined;
                const rawOutput =
                  output !== undefined
                    ? kind === "execute"
                      ? {
                          combinedOutput: output,
                          ...(exitCode !== undefined ? { exitCode } : {}),
                        }
                      : output
                    : undefined;
                const content: ReadonlyArray<EffectAcpSchema.ToolCallContent> | undefined =
                  output !== undefined
                    ? [{ type: "content", content: { type: "text", text: output } }]
                    : undefined;
                const isFirst = !seenToolSteps.has(stepIndex);
                seenToolSteps.add(stepIndex);

                yield* emitSessionNotification({
                  sessionId,
                  update: {
                    sessionUpdate: isFirst ? "tool_call" : "tool_call_update",
                    toolCallId,
                    name: toolName,
                    title,
                    kind,
                    status,
                    ...(rawInput !== undefined ? { rawInput } : {}),
                    ...(rawOutput !== undefined ? { rawOutput } : {}),
                    ...(content !== undefined ? { content } : {}),
                  },
                });
              }

              if (isRecord(su.usage) && typeof su.usage.total_tokens === "number") {
                yield* emitSessionNotification({
                  sessionId,
                  update: {
                    sessionUpdate: "usage_update",
                    used: su.usage.total_tokens,
                    size: 0,
                  },
                });
              }
              return;
            }

            if (parsed.event === "result" && isRecord(parsed.result)) {
              const res = parsed.result;
              if (
                typeof res.conversation_id === "string" &&
                res.conversation_id.trim().length > 0
              ) {
                const convId = res.conversation_id.trim();
                conversationMap.set(sessionId, convId);
                persistAgyConversationId(sessionId, convId, input.spawn.env);
              }
              if (isRecord(res.usage) && typeof res.usage.total_tokens === "number") {
                yield* emitSessionNotification({
                  sessionId,
                  update: {
                    sessionUpdate: "usage_update",
                    used: res.usage.total_tokens,
                    size: 0,
                  },
                });
              }
              if (res.status === "ERROR") {
                resultError =
                  typeof res.error === "string" && res.error.trim().length > 0
                    ? res.error.trim()
                    : "Antigravity CLI reported an error.";
              }
            }
          });

          const stdoutEffect = child.stdout.pipe(
            Stream.decodeText(),
            Stream.runForEach((chunk) =>
              Effect.gen(function* () {
                lineBuffer += chunk;
                const lines = lineBuffer.split("\n");
                lineBuffer = lines.pop() ?? "";
                for (const line of lines) {
                  yield* handleNdjsonLine(line);
                }
              }),
            ),
            Effect.andThen(
              Effect.suspend(() =>
                lineBuffer.trim().length > 0 ? handleNdjsonLine(lineBuffer) : Effect.void,
              ),
            ),
            Effect.mapError(
              (cause) =>
                new EffectAcpErrors.AcpTransportError({
                  detail: "Failed while reading agy stream output.",
                  cause,
                }),
            ),
          );

          const [stderr, exitCode] = yield* Effect.all(
            [
              stdoutEffect.pipe(
                Effect.andThen(
                  collectUint8StreamText({ stream: child.stderr, maxBytes: 64 * 1024 }).pipe(
                    Effect.orElseSucceed(() => ({ text: "", bytes: 0, truncated: false })),
                  ),
                ),
              ),
              child.exitCode.pipe(
                Effect.mapError(
                  (cause) =>
                    new EffectAcpErrors.AcpTransportError({
                      detail: "Failed waiting for agy exit code.",
                      cause,
                    }),
                ),
              ),
            ],
            { concurrency: "unbounded" },
          );

          yield* closeAssistantSegment;

          if (resultError !== undefined || Number(exitCode) !== 0) {
            const detail =
              resultError ??
              (stderr.text.trim() || `Antigravity CLI exited with code ${Number(exitCode)}.`);
            return yield* new EffectAcpErrors.AcpTransportError({
              detail,
              cause: { exitCode: Number(exitCode), stderr: stderr.text },
            });
          }

          return { stopReason: "end_turn" } satisfies EffectAcpSchema.PromptResponse;
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              for (const tempFile of tempAttachmentFiles) {
                try {
                  NodeFS.rmSync(tempFile, { force: true });
                } catch {
                  // Ignore cleanup failure.
                }
              }
            }),
          ),
        ),
      );

      const fiber = yield* runTurn.pipe(Effect.forkIn(runtimeScope));
      yield* Ref.set(activePromptFiberRef, Option.some(fiber));
      if (promptOptions?.dispatched) {
        yield* Deferred.succeed(promptOptions.dispatched, undefined);
      }
      return yield* Fiber.join(fiber).pipe(
        Effect.catchCauseIf(
          (cause) => Cause.hasInterruptsOnly(cause),
          () =>
            Effect.succeed({
              stopReason: "cancelled",
            } satisfies EffectAcpSchema.PromptResponse),
        ),
        Effect.ensuring(
          Ref.set(activePromptFiberRef, Option.none()).pipe(Effect.andThen(closeAssistantSegment)),
        ),
      );
    });

  const unsupportedMethod = (method: string) =>
    Effect.fail(
      new EffectAcpErrors.AcpRequestError({
        code: -32601,
        errorMessage: `Method '${method}' is not supported by the direct agy runtime.`,
      }),
    );

  return {
    processContainment: "none",
    handleRequestPermission: () => Effect.void,
    handleElicitation: () => Effect.void,
    handleMcpConnect: () => Effect.void,
    handleMcpMessage: () => Effect.void,
    handleMcpDisconnect: () => Effect.void,
    handleMcpNotification: () => Effect.void,
    handleReadTextFile: () => Effect.void,
    handleWriteTextFile: () => Effect.void,
    handleCreateTerminal: () => Effect.void,
    handleTerminalOutput: () => Effect.void,
    handleTerminalWaitForExit: () => Effect.void,
    handleTerminalKill: () => Effect.void,
    handleTerminalRelease: () => Effect.void,
    handleSessionUpdate: (handler) =>
      Ref.update(sessionUpdateHandlersRef, (current) => [...current, handler]),
    handleElicitationComplete: () => Effect.void,
    handleUnknownExtRequest: () => Effect.void,
    handleUnknownExtNotification: () => Effect.void,
    handleExtRequest: () => Effect.void,
    handleExtNotification: () => Effect.void,
    initialize: () => initialize,
    authenticate: () => initialize.pipe(Effect.asVoid),
    start: () => start,
    getEvents: () => Stream.fromQueue(eventQueue),
    drainEvents,
    getModeState: Ref.get(modeStateRef),
    getConfigOptions: Ref.get(configOptionsRef),
    loadSession: (sessionId) => buildStartResult(sessionId),
    resumeSession: (sessionId) => buildStartResult(sessionId),
    forkSession: (sessionId) => buildStartResult(sessionId),
    listSessions: () => Effect.succeed({ sessions: [] }),
    closeSession: () => Effect.succeed({}),
    deleteSession: () => Effect.succeed({}),
    listProviders: unsupportedMethod("providers/list"),
    setProvider: () => unsupportedMethod("providers/set"),
    disableProvider: () => unsupportedMethod("providers/disable"),
    logout: Effect.succeed({}),
    prompt,
    cancel,
    setMode: (modeId) =>
      setConfigOption("mode", modeId).pipe(
        Effect.map(() => ({}) satisfies EffectAcpSchema.SetSessionModeResponse),
      ),
    setSessionModel: (modelId) =>
      setConfigOption("model", modelId).pipe(
        Effect.map(() => ({}) satisfies EffectAcpSchema.SetSessionModelResponse),
      ),
    setConfigOption,
    setModel: (model) => setConfigOption("model", model).pipe(Effect.asVoid),
    request: (method) => unsupportedMethod(method),
    notify: () => Effect.void,
  } satisfies AcpSessionRuntime.AcpSessionRuntime["Service"];
});

export function antigravityPermissionMode(runtimeMode: RuntimeMode): string {
  switch (runtimeMode) {
    case "full-access":
      return "yolo";
    case "auto-accept-edits":
      return "auto_edit";
    case "auto":
    case "approval-required":
      return "default";
  }
}

function antigravityModelOptions(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
) {
  const model = configOptions.find((option) => option.id === "model");
  if (model?.type !== "select") return [];
  return model.options.flatMap((entry) => ("value" in entry ? [entry] : entry.options));
}

function resolveAntigravityModel(input: {
  readonly configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>;
  readonly model: string | null | undefined;
  readonly defaultModel?: string | undefined;
}): string | undefined {
  const modelConfig = input.configOptions.find((option) => option.id === "model");
  const current = modelConfig?.type === "select" ? modelConfig.currentValue : undefined;
  if (input.model && input.model !== ANTIGRAVITY_DEFAULT_MODEL) return input.model;
  const options = antigravityModelOptions(input.configOptions);
  return input.defaultModel && options.some((option) => option.value === input.defaultModel)
    ? input.defaultModel
    : current;
}

/** Never replace a saved selection with the default returned by a cold resume. */
export const applyAntigravityAcpModelSelection = Effect.fn("applyAntigravityAcpModelSelection")(
  function* <E>(input: {
    readonly runtime: Pick<
      AcpSessionRuntime.AcpSessionRuntime["Service"],
      "getConfigOptions" | "setModel"
    >;
    readonly model: string | null | undefined;
    readonly defaultModel?: string | undefined;
    readonly mapError: (cause: EffectAcpErrors.AcpError) => E;
  }): Effect.fn.Return<string | undefined, E> {
    const configOptions = yield* input.runtime.getConfigOptions;
    const modelConfig = configOptions.find((option) => option.id === "model");
    const current = modelConfig?.type === "select" ? modelConfig.currentValue : undefined;
    const resolved = resolveAntigravityModel({
      configOptions,
      model: input.model,
      defaultModel: input.defaultModel,
    });
    const explicit = Boolean(input.model) && input.model !== ANTIGRAVITY_DEFAULT_MODEL;
    if (resolved === undefined || (!explicit && resolved === current)) return current;
    const options = antigravityModelOptions(configOptions);
    if (!options.some((option) => option.value === resolved)) {
      return yield* Effect.fail(
        input.mapError(
          EffectAcpErrors.AcpRequestError.invalidParams(
            `Antigravity model '${resolved}' is unavailable for this Google account. Select an available model.`,
          ),
        ),
      );
    }
    yield* input.runtime.setModel(resolved).pipe(Effect.mapError(input.mapError));
    return resolved;
  },
);

const IMAGE_MIME_TYPES = new Set(["image/bmp", "image/jpeg", "image/png", "image/webp"]);
const AUDIO_MIME_TYPES = new Set([
  "audio/aac",
  "audio/flac",
  "audio/mp3",
  "audio/mpeg",
  "audio/mp4",
  "audio/m4a",
  "audio/x-m4a",
  "audio/ogg",
  "audio/wav",
  "audio/x-wav",
  "audio/webm",
]);
const ANTIGRAVITY_MAX_AUDIO_ATTACHMENT_BYTES = 20 * 1024 * 1024;
const TEXT_MIME_TYPES = new Set([
  "application/json",
  "application/ld+json",
  "application/javascript",
  "application/typescript",
  "application/xml",
  "application/yaml",
  "application/x-yaml",
  "application/x-sh",
]);
const TEXT_FILE_EXTENSIONS = new Set([
  ".txt",
  ".md",
  ".mdx",
  ".json",
  ".jsonl",
  ".yaml",
  ".yml",
  ".toml",
  ".xml",
  ".csv",
  ".tsv",
  ".js",
  ".jsx",
  ".mjs",
  ".cjs",
  ".ts",
  ".tsx",
  ".html",
  ".css",
  ".scss",
  ".less",
  ".py",
  ".rs",
  ".go",
  ".java",
  ".kt",
  ".swift",
  ".c",
  ".h",
  ".cc",
  ".cpp",
  ".hpp",
  ".cs",
  ".rb",
  ".php",
  ".sh",
  ".bash",
  ".zsh",
  ".sql",
  ".graphql",
  ".svelte",
  ".vue",
  ".log",
  ".diff",
  ".patch",
  ".ini",
  ".conf",
]);
const ANTIGRAVITY_MAX_TEXT_ATTACHMENT_BYTES = 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES = PROVIDER_SEND_TURN_MAX_FILE_BYTES;

export const buildAntigravityPrompt = Effect.fn("buildAntigravityPrompt")(function* (input: {
  readonly input: ProviderSendTurnInput["input"];
  readonly attachments: ProviderSendTurnInput["attachments"];
  readonly attachmentsDir: string;
  readonly promptCapabilities: EffectAcpSchema.AgentCapabilities["promptCapabilities"];
}): Effect.fn.Return<
  ReadonlyArray<EffectAcpSchema.ContentBlock>,
  EffectAcpErrors.AcpError,
  FileSystem.FileSystem | Path.Path
> {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const blocks: Array<EffectAcpSchema.ContentBlock> = [];
  const text = input.input?.trim();
  if (text) blocks.push({ type: "text", text });
  let totalBytes = 0;

  for (const attachment of input.attachments ?? []) {
    const isPastedText =
      attachment.type === "file" &&
      "source" in attachment &&
      attachment.source?._tag === "pasted-text";
    // The text already carries the path; keep folded pastes lazy so agents can search without embedding the whole file.
    const mimeType = attachment.mimeType.toLowerCase().split(";", 1)[0] ?? "";
    const image = attachment.type === "image" && IMAGE_MIME_TYPES.has(mimeType);
    const audio = attachment.type === "file" && AUDIO_MIME_TYPES.has(mimeType);
    const pdf = attachment.type === "file" && mimeType === "application/pdf";
    const textFile =
      attachment.type === "file" &&
      (mimeType.startsWith("text/") ||
        TEXT_MIME_TYPES.has(mimeType) ||
        TEXT_FILE_EXTENSIONS.has(path.extname(attachment.name).toLowerCase()));
    const isPathOnly =
      (attachment.type === "image" && (!image || input.promptCapabilities?.image !== true)) ||
      (attachment.type === "file" &&
        (isPastedText ||
          (audio
            ? input.promptCapabilities?.audio !== true
            : pdf || textFile
              ? input.promptCapabilities?.embeddedContext !== true
              : true)));
    const attachmentPath = resolveAttachmentPath({
      attachmentsDir: input.attachmentsDir,
      attachment,
    });
    if (!attachmentPath) {
      return yield* EffectAcpErrors.AcpRequestError.invalidParams(
        `Invalid attachment '${attachment.name}'.`,
      );
    }
    const info = yield* fileSystem
      .stat(attachmentPath)
      .pipe(
        Effect.mapError(() =>
          EffectAcpErrors.AcpRequestError.invalidParams(
            `Could not read attachment '${attachment.name}'.`,
          ),
        ),
      );
    if (info.type !== "File") {
      return yield* EffectAcpErrors.AcpRequestError.invalidParams(
        `Could not read attachment '${attachment.name}'.`,
      );
    }
    if (isPathOnly) continue;
    const size = Number(info.size);
    const limit = image
      ? PROVIDER_SEND_TURN_MAX_IMAGE_BYTES
      : audio
        ? ANTIGRAVITY_MAX_AUDIO_ATTACHMENT_BYTES
        : pdf
          ? MAX_TOTAL_ATTACHMENT_BYTES
          : ANTIGRAVITY_MAX_TEXT_ATTACHMENT_BYTES;
    if (
      attachment.type === "file" &&
      (size > limit || totalBytes + size > MAX_TOTAL_ATTACHMENT_BYTES)
    ) {
      continue;
    }
    totalBytes += size;
    if (size > limit || totalBytes > MAX_TOTAL_ATTACHMENT_BYTES) {
      return yield* EffectAcpErrors.AcpRequestError.invalidParams(
        `Image '${attachment.name}' is too large. Antigravity accepts images up to 10 MiB and 50 MiB of native attachments per message.`,
      );
    }
    const uri = yield* path.toFileUrl(attachmentPath).pipe(
      Effect.map((url) => url.href),
      Effect.mapError(() =>
        EffectAcpErrors.AcpRequestError.invalidParams(`Invalid attachment '${attachment.name}'.`),
      ),
    );
    if (pdf) {
      blocks.push({ type: "resource_link", uri, name: attachment.name, mimeType });
      continue;
    }
    const bytes = yield* fileSystem.stream(attachmentPath, { bytesToRead: limit + 1 }).pipe(
      Stream.runCollect,
      Effect.map((chunks) => Buffer.concat(chunks)),
      Effect.mapError(() =>
        EffectAcpErrors.AcpRequestError.invalidParams(
          `Could not read attachment '${attachment.name}'.`,
        ),
      ),
    );
    totalBytes += bytes.length - size;
    if (bytes.length > limit || totalBytes > MAX_TOTAL_ATTACHMENT_BYTES) {
      return yield* EffectAcpErrors.AcpRequestError.invalidParams(
        `Attachment '${attachment.name}' changed while being read and is too large.`,
      );
    }
    if (image) {
      blocks.push({ type: "image", data: Buffer.from(bytes).toString("base64"), mimeType });
    } else if (audio) {
      blocks.push({ type: "audio", data: Buffer.from(bytes).toString("base64"), mimeType });
    } else {
      const decoded = yield* Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        catch: () =>
          EffectAcpErrors.AcpRequestError.invalidParams(
            `Attachment '${attachment.name}' is not a UTF-8 text file.`,
          ),
      });
      if (decoded.includes("\0")) {
        return yield* EffectAcpErrors.AcpRequestError.invalidParams(
          `Attachment '${attachment.name}' contains binary data.`,
        );
      }
      blocks.push({ type: "resource", resource: { uri, mimeType, text: decoded } });
    }
  }
  if (blocks.length === 0) {
    return yield* EffectAcpErrors.AcpRequestError.invalidParams(
      "A turn requires text or supported attachments.",
    );
  }
  return blocks;
});
