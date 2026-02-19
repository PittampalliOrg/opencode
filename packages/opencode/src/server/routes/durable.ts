import { DaprWorkflowClient, WorkflowRuntime, type WorkflowActivityContext, type WorkflowContext } from "@dapr/dapr"
import { Hono } from "hono"
import { describeRoute, resolver, validator } from "hono-openapi"
import z from "zod"
import { Agent } from "@/agent/agent"
import { Global } from "@/global"
import { InstanceBootstrap } from "@/project/bootstrap"
import { Instance } from "@/project/instance"
import { Provider } from "@/provider/provider"
import { SessionPrompt } from "@/session/prompt"
import { Session } from "@/session"
import { MessageV2 } from "@/session/message-v2"
import { Filesystem } from "@/util/filesystem"
import { lazy } from "@/util/lazy"
import { Log } from "@/util/log"
import fs from "fs/promises"
import path from "path"

const log = Log.create({ service: "server.durable" })
const PLAN_OPEN = "<proposed_plan>"
const PLAN_CLOSE = "</proposed_plan>"
const PLAN_PATH = path.join(Global.Path.state, "durable-plan-artifacts")
const WORKFLOW_RUNNING = 0
const WORKFLOW_COMPLETED = 1
const WORKFLOW_CONTINUED_AS_NEW = 2
const WORKFLOW_FAILED = 3
const WORKFLOW_TERMINATED = 5
const WORKFLOW_PENDING = 6
const WORKFLOW_SUSPENDED = 7
const STARTUP_INIT_RETRY_MS = Number.parseInt(process.env.DURABLE_STARTUP_INIT_RETRY_MS ?? "15000", 10)
const REQUIRE_STARTUP_INIT = String(process.env.DURABLE_REQUIRE_STARTUP_INIT ?? "false").toLowerCase() === "true"
const WORKSPACE_SESSION_TTL_MS = Number.parseInt(process.env.WORKSPACE_SESSION_TTL_MS ?? `${30 * 60 * 1000}`, 10)
const WORKSPACE_SWEEP_MS = Number.parseInt(process.env.WORKSPACE_SESSION_SWEEP_MS ?? `${60 * 1000}`, 10)
const WORKSPACE_COMMAND_TIMEOUT_MS = Number.parseInt(process.env.WORKSPACE_COMMAND_TIMEOUT_MS ?? "30000", 10)
const WORKSPACE_CLONE_TIMEOUT_MS = Number.parseInt(process.env.WORKSPACE_CLONE_TIMEOUT_MS ?? "120000", 10)
const WORKSPACE_STRIP_CLONE_GIT_DIR = String(process.env.WORKSPACE_CLONE_STRIP_GIT_DIR ?? "true").toLowerCase() !== "false"
const WORKSPACE_STORE_PATH = path.join(Global.Path.state, "durable-workspaces", "sessions.json")

const AgentConfig = z
  .object({
    name: z.string().optional(),
    instructions: z.string().optional(),
    modelSpec: z.string().optional(),
    tools: z.array(z.string()).optional(),
    maxTurns: z.number().int().positive().optional(),
    timeoutMinutes: z.number().int().positive().optional(),
  })
  .partial()

const RunInput = z.object({
  prompt: z.string().optional(),
  model: z.string().optional(),
  tools: z.union([z.array(z.string()), z.record(z.string(), z.boolean()), z.string()]).optional(),
  instructions: z.string().optional(),
  maxTurns: z.coerce.number().int().positive().optional(),
  cwd: z.string().optional(),
  executionId: z.string().optional(),
  dbExecutionId: z.string().optional(),
  parentExecutionId: z.string().optional(),
  workflowId: z.string().optional(),
  nodeId: z.string().optional(),
  nodeName: z.string().optional(),
  agentConfig: AgentConfig.optional(),
  plan: z.any().optional(),
  planJson: z.any().optional(),
  artifactRef: z.string().optional(),
})

const RunStarted = z.object({
  success: z.boolean(),
  workflow_id: z.string(),
  dapr_instance_id: z.string(),
})

const RunStatus = z.object({
  success: z.boolean(),
  workflow_id: z.string(),
  status: z.enum(["running", "completed", "failed"]),
  result: z.record(z.string(), z.any()).optional(),
  error: z.string().optional(),
})

const PlanResponse = z.object({
  success: z.boolean(),
  artifactRef: z.string(),
  planMarkdown: z.string(),
  plan: z.record(z.string(), z.any()),
  tasks: z.array(z.string()),
  workflow_id: z.string(),
  daprPlanningInstanceId: z.string(),
})

const WorkspaceToolName = z.enum([
  "read",
  "write",
  "edit",
  "list",
  "bash",
])

const WorkspaceProfileInput = z.object({
  executionId: z.string().optional(),
  name: z.string().optional(),
  rootPath: z.string().optional(),
  enabledTools: z.array(WorkspaceToolName).optional(),
  requireReadBeforeWrite: z.boolean().optional(),
  commandTimeoutMs: z.number().int().positive().optional(),
})

const WorkspaceCloneInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
  durableInstanceId: z.string().optional(),
  repositoryOwner: z.string().optional(),
  repositoryRepo: z.string().optional(),
  repositoryBranch: z.string().optional(),
  targetDir: z.string().optional(),
  repositoryToken: z.string().optional(),
  githubToken: z.string().optional(),
  timeoutMs: z.number().int().positive().optional(),
})

const WorkspaceCommandInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
  durableInstanceId: z.string().optional(),
  command: z.string().optional(),
  timeoutMs: z.number().int().positive().optional(),
})

const WorkspaceFileInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
  durableInstanceId: z.string().optional(),
  operation: z.string().optional(),
  path: z.string().optional(),
  content: z.string().optional(),
  old_string: z.string().optional(),
  new_string: z.string().optional(),
})

const CleanupInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
})

const CleanupResponse = z.object({
  success: z.boolean(),
  cleaned: z.boolean(),
  cleanedWorkspaceRefs: z.array(z.string()).optional(),
  executionId: z.string().optional(),
})

const WorkspaceProfileResponse = z.object({
  success: z.literal(true),
  workspaceRef: z.string(),
  executionId: z.string(),
  name: z.string(),
  rootPath: z.string(),
  clonePath: z.string().optional(),
  backend: z.literal("local"),
  enabledTools: z.array(WorkspaceToolName),
  requireReadBeforeWrite: z.boolean(),
  commandTimeoutMs: z.number().int().positive(),
  createdAt: z.string(),
  sandbox: z.object({
    backend: z.literal("local"),
    rootPath: z.string(),
    workingDirectory: z.string(),
    details: z.record(z.string(), z.any()),
  }),
})

const WorkspaceActionResponse = z.object({
  success: z.boolean(),
  result: z.record(z.string(), z.any()).optional(),
  error: z.string().optional(),
})

type ModelRef = { providerID: string; modelID: string }

type DurableRunPayload = {
  workflowID: string
  parentExecutionID?: string
  prompt: string
  cwd?: string
  agent?: string
  model?: ModelRef
  tools?: Record<string, boolean>
  instructions?: string
}

type DurableRunResult = {
  success: boolean
  workflow_id: string
  result?: Record<string, unknown>
  error?: string
}

type DurablePlanPayload = {
  workflowID: string
  prompt: string
  cwd?: string
  agent?: string
  model?: ModelRef
  tools?: Record<string, boolean>
  instructions?: string
}

type DurablePlanResult = {
  success: boolean
  artifactRef?: string
  planMarkdown?: string
  plan?: Record<string, unknown>
  tasks?: string[]
  workflow_id: string
  daprPlanningInstanceId: string
  error?: string
}

type WorkflowStateLike = {
  runtimeStatus: number
  serializedOutput?: string
  workflowFailureDetails?: {
    getErrorMessage: () => string
  }
}

type WorkspaceTool = z.infer<typeof WorkspaceToolName>

type WorkspaceSessionRecord = {
  workspaceRef: string
  executionId: string
  name: string
  rootPath: string
  clonePath?: string
  backend: "local"
  enabledTools: WorkspaceTool[]
  requireReadBeforeWrite: boolean
  commandTimeoutMs: number
  createdAt: number
  lastAccessedAt: number
  durableInstanceId?: string
  readPaths: string[]
}

type WorkspaceSession = Omit<WorkspaceSessionRecord, "readPaths"> & {
  readPaths: Set<string>
}

type WorkspaceActionInput = {
  workspaceRef?: string
  executionId?: string
  durableInstanceId?: string
}

let durableRuntime: WorkflowRuntime | undefined
let durableClient: DaprWorkflowClient | undefined
let durableStarting: Promise<void> | undefined
const workspaceSessions = new Map<string, WorkspaceSession>()
const executionToWorkspace = new Map<string, string>()
const durableToWorkspace = new Map<string, string>()
let workspaceStoreReady = false
let workspaceStoreLoading: Promise<void> | undefined
let workspaceStorePersisting = Promise.resolve()

function rid(prefix: string) {
  return `${prefix}-${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`
}

const workspaceTools = WorkspaceToolName.options
const workspaceFileOperations = ["read_file", "write_file", "edit_file", "list_files", "delete_file", "mkdir", "file_stat"] as const

function workspaceBaseRoot() {
  const configured = process.env.WORKSPACE_SESSIONS_ROOT?.trim()
  if (!configured) return path.join(Global.Path.state, "durable-workspaces", "runs")
  if (path.isAbsolute(configured)) return configured
  return path.resolve(configured)
}

function sanitizeWorkspaceSegment(input: string) {
  return input.replace(/[^a-zA-Z0-9._-]/g, "-")
}

function parseWorkspaceBoolean(input: unknown) {
  if (typeof input === "boolean") return input
  return false
}

function parseWorkspaceTimeout(input: unknown) {
  if (typeof input === "number" && Number.isFinite(input) && input > 0) return Math.floor(input)
  return
}

function parseWorkspaceEnabledTools(input: unknown): WorkspaceTool[] {
  if (!input) return [...workspaceTools]
  if (!Array.isArray(input)) return [...workspaceTools]
  const tools = input.flatMap((item) => {
    if (typeof item !== "string") return []
    const value = item.trim()
    if (!value || !workspaceTools.includes(value as WorkspaceTool)) return []
    return [value as WorkspaceTool]
  })
  if (!tools.length) return [...workspaceTools]
  return [...new Set(tools)]
}

function workspaceRecord(session: WorkspaceSession): WorkspaceSessionRecord {
  return {
    workspaceRef: session.workspaceRef,
    executionId: session.executionId,
    name: session.name,
    rootPath: session.rootPath,
    clonePath: session.clonePath,
    backend: session.backend,
    enabledTools: [...session.enabledTools],
    requireReadBeforeWrite: session.requireReadBeforeWrite,
    commandTimeoutMs: session.commandTimeoutMs,
    createdAt: session.createdAt,
    lastAccessedAt: session.lastAccessedAt,
    durableInstanceId: session.durableInstanceId,
    readPaths: [...session.readPaths],
  }
}

function workspaceSandbox(session: WorkspaceSession) {
  return {
    backend: "local" as const,
    rootPath: session.rootPath,
    workingDirectory: session.clonePath ?? session.rootPath,
    details: {},
  }
}

function workspaceProfile(session: WorkspaceSession) {
  return {
    success: true as const,
    workspaceRef: session.workspaceRef,
    executionId: session.executionId,
    name: session.name,
    rootPath: session.rootPath,
    clonePath: session.clonePath,
    backend: "local" as const,
    enabledTools: [...session.enabledTools],
    requireReadBeforeWrite: session.requireReadBeforeWrite,
    commandTimeoutMs: session.commandTimeoutMs,
    createdAt: new Date(session.createdAt).toISOString(),
    sandbox: workspaceSandbox(session),
  }
}

async function ensureWorkspaceStore() {
  if (workspaceStoreReady) return
  if (!workspaceStoreLoading) {
    workspaceStoreLoading = (async () => {
      const file = await Filesystem.readJson<{ sessions?: WorkspaceSessionRecord[] }>(WORKSPACE_STORE_PATH).catch(() => undefined)
      if (!file?.sessions || !Array.isArray(file.sessions)) {
        workspaceStoreReady = true
        return
      }
      for (const record of file.sessions) {
        if (!record || typeof record !== "object") continue
        if (typeof record.workspaceRef !== "string" || !record.workspaceRef.trim()) continue
        if (typeof record.executionId !== "string" || !record.executionId.trim()) continue
        if (typeof record.rootPath !== "string" || !record.rootPath.trim()) continue
        const session: WorkspaceSession = {
          workspaceRef: record.workspaceRef.trim(),
          executionId: record.executionId.trim(),
          name: typeof record.name === "string" && record.name.trim() ? record.name : `workspace-${record.executionId}`,
          rootPath: record.rootPath.trim(),
          clonePath: typeof record.clonePath === "string" && record.clonePath.trim() ? record.clonePath.trim() : undefined,
          backend: "local",
          enabledTools: parseWorkspaceEnabledTools(record.enabledTools),
          requireReadBeforeWrite: Boolean(record.requireReadBeforeWrite),
          commandTimeoutMs:
            typeof record.commandTimeoutMs === "number" && record.commandTimeoutMs > 0
              ? Math.floor(record.commandTimeoutMs)
              : WORKSPACE_COMMAND_TIMEOUT_MS,
          createdAt: typeof record.createdAt === "number" && record.createdAt > 0 ? record.createdAt : Date.now(),
          lastAccessedAt: typeof record.lastAccessedAt === "number" && record.lastAccessedAt > 0 ? record.lastAccessedAt : Date.now(),
          durableInstanceId:
            typeof record.durableInstanceId === "string" && record.durableInstanceId.trim()
              ? record.durableInstanceId.trim()
              : undefined,
          readPaths: new Set(Array.isArray(record.readPaths) ? record.readPaths.filter((item): item is string => typeof item === "string") : []),
        }
        workspaceSessions.set(session.workspaceRef, session)
        executionToWorkspace.set(session.executionId, session.workspaceRef)
        if (session.durableInstanceId) {
          durableToWorkspace.set(session.durableInstanceId, session.workspaceRef)
        }
      }
      workspaceStoreReady = true
    })().catch((error: unknown) => {
      log.warn("failed loading workspace store", {
        error: error instanceof Error ? error.message : String(error),
      })
      workspaceStoreReady = true
    })
  }
  await workspaceStoreLoading
}

async function persistWorkspaceStore() {
  await ensureWorkspaceStore()
  workspaceStorePersisting = workspaceStorePersisting
    .catch(() => undefined)
    .then(async () => {
      await Filesystem.writeJson(WORKSPACE_STORE_PATH, {
        version: 1,
        sessions: [...workspaceSessions.values()].map((session) => workspaceRecord(session)),
      })
    })
  return workspaceStorePersisting
}

function touchWorkspace(session: WorkspaceSession) {
  session.lastAccessedAt = Date.now()
}

async function bindWorkspaceDurableInstance(session: WorkspaceSession, durableInstanceID: string | undefined) {
  const id = durableInstanceID?.trim()
  if (!id) return
  if (session.durableInstanceId === id) return
  if (session.durableInstanceId) durableToWorkspace.delete(session.durableInstanceId)
  session.durableInstanceId = id
  durableToWorkspace.set(id, session.workspaceRef)
  await persistWorkspaceStore()
}

function parseDurableInstanceID(input: WorkspaceActionInput) {
  return input.durableInstanceId?.trim() || ""
}

async function resolveWorkspaceFromInput(input: WorkspaceActionInput) {
  await ensureWorkspaceStore()
  const byRef = input.workspaceRef?.trim()
  if (byRef) {
    const session = workspaceSessions.get(byRef)
    if (session) {
      touchWorkspace(session)
      return session
    }
  }
  const byDurable = parseDurableInstanceID(input)
  if (byDurable) {
    const ref = durableToWorkspace.get(byDurable)
    if (ref) {
      const session = workspaceSessions.get(ref)
      if (session) {
        touchWorkspace(session)
        return session
      }
    }
  }
  const byExecution = input.executionId?.trim() || ""
  if (byExecution) {
    const ref = executionToWorkspace.get(byExecution)
    if (ref) {
      const session = workspaceSessions.get(ref)
      if (session) {
        touchWorkspace(session)
        return session
      }
    }
  }
  throw new Error("Workspace session not found (provide workspaceRef or executionId)")
}

function resolveWorkspaceRoot(executionID: string, requested: string | undefined) {
  const base = workspaceBaseRoot()
  const value = requested?.trim()
  if (!value) return path.resolve(base, sanitizeWorkspaceSegment(executionID))
  if (path.isAbsolute(value)) return path.resolve(value)
  return path.resolve(base, value)
}

async function createOrGetWorkspaceProfile(input: z.infer<typeof WorkspaceProfileInput>) {
  await ensureWorkspaceStore()
  const executionID = input.executionId?.trim() || ""
  if (!executionID) throw new Error("executionId is required")
  const existingRef = executionToWorkspace.get(executionID)
  if (existingRef) {
    const existing = workspaceSessions.get(existingRef)
    if (existing) {
      touchWorkspace(existing)
      await persistWorkspaceStore()
      return existing
    }
    executionToWorkspace.delete(executionID)
  }
  const rootPath = resolveWorkspaceRoot(executionID, input.rootPath)
  await fs.mkdir(rootPath, { recursive: true })
  const now = Date.now()
  const session: WorkspaceSession = {
    workspaceRef: `ws_${rid("session").replace(/^session-/, "")}`,
    executionId: executionID,
    name: input.name?.trim() || `workspace-${executionID}`,
    rootPath,
    clonePath: undefined,
    backend: "local",
    enabledTools: parseWorkspaceEnabledTools(input.enabledTools),
    requireReadBeforeWrite: parseWorkspaceBoolean(input.requireReadBeforeWrite),
    commandTimeoutMs: parseWorkspaceTimeout(input.commandTimeoutMs) ?? WORKSPACE_COMMAND_TIMEOUT_MS,
    createdAt: now,
    lastAccessedAt: now,
    durableInstanceId: undefined,
    readPaths: new Set<string>(),
  }
  workspaceSessions.set(session.workspaceRef, session)
  executionToWorkspace.set(executionID, session.workspaceRef)
  await persistWorkspaceStore()
  return session
}

function assertWorkspaceTool(session: WorkspaceSession, tool: WorkspaceTool) {
  if (session.enabledTools.includes(tool)) return
  throw new Error(`Tool "${tool}" is disabled for workspace ${session.workspaceRef}`)
}

function resolveCloneTargetDir(repo: string, targetDir: string | undefined) {
  const candidate = (targetDir?.trim() || repo.trim()).replace(/\\/g, "/")
  if (!candidate) throw new Error("targetDir could not be resolved")
  const normalized = path.posix.normalize(candidate)
  if (!normalized || normalized === "." || normalized.startsWith("..") || normalized.startsWith("/")) {
    throw new Error("targetDir must be a relative path inside workspace root")
  }
  return normalized.replace(/^\.\/+/, "")
}

async function runProcess(input: { cmd: string[]; cwd: string; timeoutMs: number; env?: Record<string, string> }) {
  const startedAt = Date.now()
  const proc = Bun.spawn(input.cmd, {
    cwd: input.cwd,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    env: input.env ? { ...process.env, ...input.env } : process.env,
  })
  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    try {
      proc.kill()
    } catch {}
  }, input.timeoutMs)
  const [exitCode, stdout, stderr] = await Promise.all([
    proc.exited.catch(() => 1),
    proc.stdout ? new Response(proc.stdout).text() : Promise.resolve(""),
    proc.stderr ? new Response(proc.stderr).text() : Promise.resolve(""),
  ])
  clearTimeout(timer)
  return {
    stdout,
    stderr,
    exitCode,
    success: exitCode === 0 && !timedOut,
    executionTimeMs: Date.now() - startedAt,
    timedOut: timedOut || undefined,
  }
}

async function runShell(command: string, cwd: string, timeoutMs: number) {
  return await runProcess({
    cmd: ["sh", "-lc", command],
    cwd,
    timeoutMs,
  })
}

function resolveWorkspacePath(session: WorkspaceSession, value: string | undefined, operation: string) {
  const input = value?.trim() || ""
  if (!input) throw new Error(`path is required for ${operation}`)
  const full = path.resolve(session.rootPath, input)
  if (!Filesystem.contains(session.rootPath, full)) {
    throw new Error(`path "${input}" escapes workspace root`)
  }
  return full
}

function assertCloneScope(session: WorkspaceSession, fullPath: string, operation: string) {
  if (!session.clonePath) return
  if (fullPath === session.clonePath) return
  if (Filesystem.contains(session.clonePath, fullPath)) return
  throw new Error(`${operation} path is outside clone root "${session.clonePath}"`)
}

async function enforceReadBeforeWrite(session: WorkspaceSession, fullPath: string) {
  if (!session.requireReadBeforeWrite) return
  if (!(await Filesystem.exists(fullPath))) return
  if (session.readPaths.has(fullPath)) return
  throw new Error(`Write blocked by read-before-write policy for "${fullPath}" in workspace ${session.workspaceRef}`)
}

async function runWorkspaceClone(input: z.infer<typeof WorkspaceCloneInput>) {
  const session = await resolveWorkspaceFromInput(input)
  assertWorkspaceTool(session, "bash")
  await bindWorkspaceDurableInstance(session, parseDurableInstanceID(input))
  const repositoryOwner = input.repositoryOwner?.trim() || ""
  const repositoryRepo = input.repositoryRepo?.trim() || ""
  if (!repositoryOwner || !repositoryRepo) {
    throw new Error("repositoryOwner and repositoryRepo are required")
  }
  const branch = input.repositoryBranch?.trim() || "main"
  const timeoutMs = parseWorkspaceTimeout(input.timeoutMs) ?? Math.max(session.commandTimeoutMs, WORKSPACE_CLONE_TIMEOUT_MS)
  const cloneDir = resolveCloneTargetDir(repositoryRepo, input.targetDir)
  const clonePath = path.resolve(session.rootPath, cloneDir)
  if (!Filesystem.contains(session.rootPath, clonePath)) {
    throw new Error("targetDir must stay inside workspace root")
  }
  await fs.rm(clonePath, { recursive: true, force: true })
  const token = input.repositoryToken?.trim() || input.githubToken?.trim() || ""
  const repositoryURL = token
    ? `https://${token}@github.com/${repositoryOwner}/${repositoryRepo}.git`
    : `https://github.com/${repositoryOwner}/${repositoryRepo}.git`
  const gitCheck = await runProcess({
    cmd: ["git", "--version"],
    cwd: session.rootPath,
    timeoutMs: Math.min(timeoutMs, 15000),
  })
  if (!gitCheck.success) {
    throw new Error("git is not installed in the durable agent runtime")
  }
  const clone = await runProcess({
    cmd: ["git", "clone", "--depth", "1", "--branch", branch, repositoryURL, cloneDir],
    cwd: session.rootPath,
    timeoutMs,
  })
  if (!clone.success) {
    const sanitized = token ? clone.stderr.replaceAll(token, "***") : clone.stderr
    throw new Error(`git clone failed: ${sanitized || "unknown clone error"}`)
  }
  const rev = await runProcess({
    cmd: ["git", "rev-parse", "HEAD"],
    cwd: clonePath,
    timeoutMs: Math.min(timeoutMs, 30000),
  })
  const commitHash = rev.success ? rev.stdout.trim() || "unknown" : "unknown"
  const files = await runProcess({
    cmd: ["git", "ls-files", "--cached"],
    cwd: clonePath,
    timeoutMs: Math.min(timeoutMs, 30000),
  })
  const fileCount = files.success
    ? files.stdout
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean).length
    : 0
  let strippedGitDir = false
  if (WORKSPACE_STRIP_CLONE_GIT_DIR) {
    await fs.rm(path.join(clonePath, ".git"), { recursive: true, force: true })
    strippedGitDir = true
  }
  session.clonePath = clonePath
  touchWorkspace(session)
  await persistWorkspaceStore()
  return {
    success: true,
    clonePath,
    repository: `${repositoryOwner}/${repositoryRepo}`,
    branch,
    commitHash,
    fileCount,
    gitMetadataStripped: strippedGitDir,
    sandbox: workspaceSandbox(session),
  }
}

async function runWorkspaceCommand(input: z.infer<typeof WorkspaceCommandInput>) {
  const session = await resolveWorkspaceFromInput(input)
  assertWorkspaceTool(session, "bash")
  await bindWorkspaceDurableInstance(session, parseDurableInstanceID(input))
  const command = input.command?.trim() || ""
  if (!command) throw new Error("command is required")
  const timeoutMs = parseWorkspaceTimeout(input.timeoutMs) ?? session.commandTimeoutMs
  const cwd = session.clonePath ?? session.rootPath
  const result = await runShell(command, cwd, timeoutMs)
  touchWorkspace(session)
  await persistWorkspaceStore()
  return {
    ...result,
    sandbox: workspaceSandbox(session),
  }
}

function isWorkspaceFileOperation(input: string): input is (typeof workspaceFileOperations)[number] {
  return workspaceFileOperations.includes(input as (typeof workspaceFileOperations)[number])
}

function workspaceToolForOperation(operation: (typeof workspaceFileOperations)[number]): WorkspaceTool {
  if (operation === "read_file") return "read"
  if (operation === "write_file") return "write"
  if (operation === "edit_file") return "edit"
  if (operation === "list_files") return "list"
  return "bash"
}

async function runWorkspaceFileOperation(input: z.infer<typeof WorkspaceFileInput>) {
  const session = await resolveWorkspaceFromInput(input)
  const operation = input.operation?.trim() || ""
  if (!isWorkspaceFileOperation(operation)) {
    throw new Error(
      "operation is required and must be one of read_file, write_file, edit_file, list_files, delete_file, mkdir, file_stat",
    )
  }
  assertWorkspaceTool(session, workspaceToolForOperation(operation))
  await bindWorkspaceDurableInstance(session, parseDurableInstanceID(input))

  if (operation === "read_file") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    const file = Bun.file(fullPath)
    if (!(await file.exists())) throw new Error(`path not found: ${input.path}`)
    const content = await file.text()
    session.readPaths.add(fullPath)
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { content }
  }

  if (operation === "write_file") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await enforceReadBeforeWrite(session, fullPath)
    await Filesystem.write(fullPath, input.content ?? "")
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { path: input.path ?? "" }
  }

  if (operation === "edit_file") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await enforceReadBeforeWrite(session, fullPath)
    const oldString = input.old_string ?? ""
    if (!oldString) throw new Error("old_string is required for edit_file")
    const file = Bun.file(fullPath)
    if (!(await file.exists())) throw new Error(`path not found: ${input.path}`)
    const current = await file.text()
    if (!current.includes(oldString)) throw new Error(`old_string not found in ${input.path}`)
    await Filesystem.write(fullPath, current.replace(oldString, input.new_string ?? ""))
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { path: input.path ?? "" }
  }

  if (operation === "list_files") {
    const fullPath = path.resolve(session.rootPath, input.path?.trim() || ".")
    if (!Filesystem.contains(session.rootPath, fullPath)) throw new Error(`path "${input.path}" escapes workspace root`)
    const entries = await fs.readdir(fullPath, { withFileTypes: true })
    touchWorkspace(session)
    await persistWorkspaceStore()
    return {
      files: entries.map((entry) => ({
        name: entry.name,
        type: entry.isDirectory() ? "directory" : "file",
      })),
    }
  }

  if (operation === "delete_file") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await fs.rm(fullPath, { recursive: true, force: true })
    touchWorkspace(session)
    await persistWorkspaceStore()
    return {
      deleted: true,
      path: input.path ?? "",
    }
  }

  if (operation === "mkdir") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await fs.mkdir(fullPath, { recursive: true })
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { path: input.path ?? "" }
  }

  const fullPath = resolveWorkspacePath(session, input.path, operation)
  const stat = await fs.stat(fullPath)
  touchWorkspace(session)
  await persistWorkspaceStore()
  return {
    size: stat.size,
    isFile: stat.isFile(),
    isDirectory: stat.isDirectory(),
    modified: stat.mtime.toISOString(),
    created: stat.birthtime.toISOString(),
  }
}

async function cleanupWorkspaceRef(workspaceRef: string) {
  await ensureWorkspaceStore()
  const ref = workspaceRef.trim()
  if (!ref) return false
  const session = workspaceSessions.get(ref)
  if (!session) return false
  workspaceSessions.delete(ref)
  executionToWorkspace.delete(session.executionId)
  if (session.durableInstanceId) {
    durableToWorkspace.delete(session.durableInstanceId)
  }
  const rootPath = path.resolve(session.rootPath)
  const safeRoot = path.resolve(workspaceBaseRoot())
  if (Filesystem.contains(safeRoot, rootPath)) {
    await fs.rm(rootPath, { recursive: true, force: true }).catch(() => undefined)
  } else {
    log.warn("skipping workspace root deletion outside durable root", {
      workspaceRef: ref,
      rootPath,
      safeRoot,
    })
  }
  await persistWorkspaceStore()
  return true
}

async function cleanupWorkspaceInput(input: z.infer<typeof CleanupInput>) {
  await ensureWorkspaceStore()
  const refs = new Set<string>()
  const workspaceRef = input.workspaceRef?.trim() || ""
  const executionId = input.executionId?.trim() || ""
  if (workspaceRef) refs.add(workspaceRef)
  if (executionId) {
    const ref = executionToWorkspace.get(executionId)
    if (ref) refs.add(ref)
  }
  if (!refs.size) {
    throw new Error("workspaceRef or executionId is required")
  }
  const cleanedWorkspaceRefs = (
    await Promise.all(
      [...refs].map(async (ref) => {
        if (!(await cleanupWorkspaceRef(ref))) return
        return ref
      }),
    )
  ).filter((value): value is string => Boolean(value))
  return cleanedWorkspaceRefs
}

async function sweepWorkspaceSessions() {
  await ensureWorkspaceStore()
  const now = Date.now()
  const expired = [...workspaceSessions.values()].flatMap((session) => {
    if (now - session.lastAccessedAt <= WORKSPACE_SESSION_TTL_MS) return []
    return [session.workspaceRef]
  })
  if (!expired.length) return
  await Promise.all(expired.map((ref) => cleanupWorkspaceRef(ref)))
}

const workspaceSweepTimer = setInterval(() => {
  void sweepWorkspaceSessions().catch((error: unknown) => {
    log.warn("workspace session sweep failed", {
      error: error instanceof Error ? error.message : String(error),
    })
  })
}, WORKSPACE_SWEEP_MS)
workspaceSweepTimer.unref()

function parseTools(input: z.infer<typeof RunInput>): Record<string, boolean> | undefined {
  if (!input.tools) return input.agentConfig?.tools ? Object.fromEntries(input.agentConfig.tools.map((x) => [x, true])) : undefined
  if (Array.isArray(input.tools)) {
    return Object.fromEntries(input.tools.filter((x): x is string => typeof x === "string").map((x) => [x, true]))
  }
  if (typeof input.tools === "string") {
    let parsed: unknown
    try {
      parsed = JSON.parse(input.tools)
    } catch {
      return undefined
    }
    if (Array.isArray(parsed)) {
      return Object.fromEntries(parsed.filter((x): x is string => typeof x === "string").map((x) => [x, true]))
    }
    if (parsed && typeof parsed === "object") {
      return Object.fromEntries(
        Object.entries(parsed).flatMap(([key, value]) =>
          typeof value === "boolean" ? [[key, value] as const] : [],
        ),
      )
    }
    return undefined
  }
  return input.tools
}

function parseModel(input: z.infer<typeof RunInput>) {
  const model = input.agentConfig?.modelSpec ?? input.model
  if (!model) return undefined
  return Provider.parseModel(model)
}

function extractProposedPlanText(text: string): string | undefined {
  const lines = text.match(/[^\r\n]*\r?\n|[^\r\n]+$/g) ?? []
  let inPlan = false
  let seen = false
  let current = ""
  let last = ""
  for (const line of lines) {
    if (!inPlan) {
      if (line.trim() === PLAN_OPEN) {
        inPlan = true
        seen = true
        current = ""
      }
      continue
    }
    if (line.trim() === PLAN_CLOSE) {
      last = current
      inPlan = false
      continue
    }
    current += line
  }
  if (inPlan) {
    last = current
  }
  if (!seen) return
  return last
}

function extractTasks(text: string) {
  return text
    .split("\n")
    .flatMap((line) => {
      const item = line.match(/^\s*(?:[-*]|\d+\.)\s+(.+)$/)
      if (!item?.[1]) return []
      return [item[1].trim()]
    })
    .filter((line) => line.length > 0)
}

async function readPlanArtifact(ref: string) {
  const file = path.join(PLAN_PATH, `${ref}.json`)
  if (!(await Filesystem.exists(file))) return
  return await Filesystem.readJson<{
    artifactRef: string
    planMarkdown: string
    plan: Record<string, unknown>
    tasks: string[]
  }>(file)
}

async function writePlanArtifact(input: {
  artifactRef: string
  planMarkdown: string
  plan: Record<string, unknown>
  tasks: string[]
}) {
  const file = path.join(PLAN_PATH, `${input.artifactRef}.json`)
  await Filesystem.writeJson(file, {
    ...input,
    createdAt: new Date().toISOString(),
  })
}

function toResult(message: MessageV2.WithParts) {
  const final = message.parts.findLast((part) => part.type === "text")?.text ?? ""
  const tools = message.parts.flatMap((part): Array<Record<string, unknown>> => {
    if (part.type !== "tool") return []
    if (part.state.status === "error") {
      return [
        {
          toolName: part.tool,
          toolArgs: part.state.input,
          state: part.state.status,
          error: part.state.error,
        },
      ]
    }
    if (part.state.status === "completed") {
      return [
        {
          toolName: part.tool,
          toolArgs: part.state.input,
          state: part.state.status,
          output: part.state.output,
          title: part.state.title,
          metadata: part.state.metadata,
        },
      ]
    }
    return [
      {
        toolName: part.tool,
        toolArgs: part.state.input,
        state: part.state.status,
      },
    ]
  })
  return {
    final_answer: final,
    content: final,
    toolCalls: tools,
    sessionID: message.info.sessionID,
    messageID: message.info.id,
  }
}

async function publishCompletion(input: {
  workflowID: string
  parentExecutionID?: string
  success: boolean
  result?: Record<string, unknown>
  error?: string
}) {
  const parent = input.parentExecutionID?.trim()
  if (!parent) return

  const host = process.env.DAPR_HOST ?? "localhost"
  const port = process.env.DAPR_HTTP_PORT ?? "3500"
  const appID = process.env.ORCHESTRATOR_APP_ID ?? "workflow-orchestrator"
  const url = `http://${host}:${port}/v1.0/invoke/${appID}/method/api/v2/workflows/${parent}/events`
  const body = {
    eventName: `agent_completed_${input.workflowID}`,
    eventData: {
      workflow_id: input.workflowID,
      phase: "agent",
      success: input.success,
      result: input.result ?? {},
      error: input.error,
      timestamp: new Date().toISOString(),
    },
  }
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })
  if (response.ok) return
  const details = await response.text()
  log.warn("failed to publish completion", {
    workflowID: input.workflowID,
    parentExecutionID: parent,
    status: response.status,
    details,
  })
}

async function withDir<T>(cwd: string | undefined, fn: () => Promise<T>) {
  const trimmed = cwd?.trim()
  if (!trimmed) return await fn()
  return await Instance.provide({
    directory: trimmed,
    init: InstanceBootstrap,
    fn,
  })
}

async function runPrompt(input: {
  prompt: string
  cwd?: string
  agent?: string
  model?: ModelRef
  tools?: Record<string, boolean>
  instructions?: string
}) {
  return await withDir(input.cwd, async () => {
    const agentName = input.agent ?? (await Agent.defaultAgent())
    const agent = await Agent.get(agentName)
    if (!agent) {
      throw new Error(`Agent "${agentName}" not found`)
    }
    return await SessionPrompt.prompt({
      sessionID: (await Session.create({ title: `Durable ${agentName}` })).id,
      agent: agentName,
      model: input.model,
      tools: input.tools,
      system: input.instructions,
      parts: [
        {
          type: "text",
          text: input.prompt,
        },
      ],
    })
  })
}

function parseWorkflowOutput(serialized: string | undefined): Record<string, unknown> | undefined {
  if (!serialized) return
  try {
    const parsed = JSON.parse(serialized)
    if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>
    return { value: parsed } as Record<string, unknown>
  } catch {
    return { raw: serialized } as Record<string, unknown>
  }
}

function executePrompt(input: z.infer<typeof RunInput>) {
  const plan = input.planJson ?? input.plan
  if (!plan) return input.prompt?.trim() ?? ""
  const body = typeof plan === "string" ? plan : JSON.stringify(plan, null, 2)
  const base = input.prompt?.trim() ? `${input.prompt.trim()}\n\n` : ""
  return `${base}Execute the following approved plan exactly.\n\n<plan>\n${body}\n</plan>`
}

async function durableRunActivity(_ctx: WorkflowActivityContext, input: DurableRunPayload) {
  const msg = await runPrompt({
    prompt: input.prompt,
    cwd: input.cwd,
    model: input.model,
    tools: input.tools,
    instructions: input.instructions,
    agent: input.agent,
  })
  return toResult(msg)
}

async function durablePublishCompletionActivity(
  _ctx: WorkflowActivityContext,
  input: {
    workflowID: string
    parentExecutionID?: string
    success: boolean
    result?: Record<string, unknown>
    error?: string
  },
) {
  await publishCompletion(input)
}

async function durablePlanActivity(_ctx: WorkflowActivityContext, input: DurablePlanPayload): Promise<DurablePlanResult> {
  const msg = await runPrompt({
    prompt: input.prompt,
    cwd: input.cwd,
    model: input.model,
    tools: input.tools,
    instructions: input.instructions,
    agent: input.agent ?? "plan",
  })
  const result = toResult(msg)
  const final = typeof result.final_answer === "string" ? result.final_answer : ""
  const planMarkdown = extractProposedPlanText(final)?.trim() || final.trim()
  const tasks = extractTasks(planMarkdown)
  const artifactRef = rid("plan")
  await writePlanArtifact({
    artifactRef,
    planMarkdown,
    plan: {
      format: "markdown",
      content: planMarkdown,
    },
    tasks,
  })
  return {
    success: true,
    artifactRef,
    planMarkdown,
    plan: {
      format: "markdown",
      content: planMarkdown,
    },
    tasks,
    workflow_id: input.workflowID,
    daprPlanningInstanceId: input.workflowID,
  }
}

async function* durableRunWorkflow(
  ctx: WorkflowContext,
  input: DurableRunPayload,
): AsyncGenerator<unknown, DurableRunResult, unknown> {
  try {
    const result = (yield ctx.callActivity(durableRunActivity, input)) as Record<string, unknown>
    yield ctx.callActivity(durablePublishCompletionActivity, {
      workflowID: input.workflowID,
      parentExecutionID: input.parentExecutionID,
      success: true,
      result,
    })
    return {
      success: true,
      workflow_id: input.workflowID,
      result,
    }
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    yield ctx.callActivity(durablePublishCompletionActivity, {
      workflowID: input.workflowID,
      parentExecutionID: input.parentExecutionID,
      success: false,
      error: message,
    })
    return {
      success: false,
      workflow_id: input.workflowID,
      error: message,
    }
  }
}

async function* durablePlanWorkflow(
  ctx: WorkflowContext,
  input: DurablePlanPayload,
): AsyncGenerator<unknown, DurablePlanResult, unknown> {
  try {
    return (yield ctx.callActivity(durablePlanActivity, input)) as DurablePlanResult
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    return {
      success: false,
      error: message,
      workflow_id: input.workflowID,
      daprPlanningInstanceId: input.workflowID,
    }
  }
}

async function ensureDurableRuntime() {
  if (durableRuntime && durableClient) return
  if (!durableStarting) {
    durableStarting = (async () => {
      const runtime = new WorkflowRuntime()
      runtime.registerActivity(durableRunActivity)
      runtime.registerActivity(durablePublishCompletionActivity)
      runtime.registerActivity(durablePlanActivity)
      runtime.registerWorkflow(durableRunWorkflow)
      runtime.registerWorkflow(durablePlanWorkflow)
      await runtime.start()
      durableRuntime = runtime
      durableClient = new DaprWorkflowClient()
      log.info("durable workflow runtime started")
    })().catch((error: unknown) => {
      durableStarting = undefined
      throw error
    })
  }
  await durableStarting
}

function scheduleRuntimeBootstrap() {
  if (!process.env.DAPR_GRPC_PORT && !process.env.DAPR_HTTP_PORT) return
  void ensureDurableRuntime().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error)
    if (REQUIRE_STARTUP_INIT) {
      log.error("durable runtime startup failed; exiting", { error: message })
      process.exit(1)
    }
    log.warn("durable runtime startup failed; retrying", {
      error: message,
      retryMS: STARTUP_INIT_RETRY_MS,
    })
    setTimeout(() => scheduleRuntimeBootstrap(), STARTUP_INIT_RETRY_MS)
  })
}

function workflowFailure(state: WorkflowStateLike, output: Record<string, unknown> | undefined) {
  if (typeof output?.error === "string" && output.error.trim()) return output.error
  const failure = state.workflowFailureDetails
  if (!failure) return "workflow failed"
  return failure.getErrorMessage()
}

async function withWorkflowClient<T>(fn: (client: DaprWorkflowClient) => Promise<T>) {
  await ensureDurableRuntime()
  if (!durableClient) throw new Error("durable workflow client not initialized")
  return await fn(durableClient)
}

scheduleRuntimeBootstrap()

export const DurableRoutes = lazy(() =>
  new Hono()
    .get(
      "/health",
      describeRoute({
        summary: "Durable compatibility health",
        operationId: "durable.health",
        responses: {
          200: {
            description: "health",
            content: {
              "application/json": {
                schema: resolver(z.object({ status: z.literal("ok"), service: z.string() })),
              },
            },
          },
        },
      }),
      async (c) => {
        return c.json({
          status: "ok",
          service: "opencode-durable",
        })
      },
    )
    .get(
      "/ready",
      describeRoute({
        summary: "Durable compatibility readiness",
        operationId: "durable.ready",
        responses: {
          200: {
            description: "ready",
            content: {
              "application/json": {
                schema: resolver(z.object({ status: z.literal("ready"), service: z.string() })),
              },
            },
          },
        },
      }),
      async (c) => {
        return c.json({
          status: "ready",
          service: "opencode-durable",
        })
      },
    )
    .post(
      "/run",
      describeRoute({
        summary: "Start durable-compatible run",
        operationId: "durable.run",
        responses: {
          200: {
            description: "run started",
            content: {
              "application/json": {
                schema: resolver(RunStarted),
              },
            },
          },
        },
      }),
      validator("json", RunInput),
      async (c) => {
        const body = c.req.valid("json")
        const prompt = body.prompt?.trim() ?? ""
        if (!prompt) {
          c.status(400)
          return c.json({
            success: false,
            error: "prompt is required",
          })
        }
        try {
          const id = rid("durable-run")
          const workflowInput: DurableRunPayload = {
            workflowID: id,
            parentExecutionID: body.parentExecutionId?.trim() || "",
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: body.agentConfig?.name?.trim() || "build",
            model: parseModel(body),
            tools: parseTools(body),
            instructions: body.agentConfig?.instructions ?? body.instructions,
          }
          const instanceID = await withWorkflowClient((client) =>
            client.scheduleNewWorkflow(durableRunWorkflow, workflowInput, id),
          )
          return c.json({
            success: true,
            workflow_id: id,
            dapr_instance_id: instanceID,
          })
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error)
          c.status(503)
          return c.json({
            success: false,
            error: `durable runtime unavailable: ${message}`,
          })
        }
      },
    )
    .post(
      "/execute-plan",
      describeRoute({
        summary: "Start durable-compatible plan execution",
        operationId: "durable.executePlan",
        responses: {
          200: {
            description: "plan execution started",
            content: {
              "application/json": {
                schema: resolver(RunStarted),
              },
            },
          },
        },
      }),
      validator("json", RunInput),
      async (c) => {
        const body = c.req.valid("json")
        let prompt = executePrompt(body)
        if (!prompt && body.artifactRef) {
          const artifact = await readPlanArtifact(body.artifactRef)
          if (artifact) {
            prompt = `Execute the following approved plan exactly.\n\n<plan>\n${artifact.planMarkdown}\n</plan>`
          }
        }
        if (!prompt) {
          c.status(400)
          return c.json({
            success: false,
            error: "prompt or planJson is required",
          })
        }
        try {
          const id = rid("durable-exec")
          const workflowInput: DurableRunPayload = {
            workflowID: id,
            parentExecutionID: body.parentExecutionId?.trim() || "",
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: body.agentConfig?.name?.trim() || "build",
            model: parseModel(body),
            tools: parseTools(body),
            instructions: body.agentConfig?.instructions ?? body.instructions,
          }
          const instanceID = await withWorkflowClient((client) =>
            client.scheduleNewWorkflow(durableRunWorkflow, workflowInput, id),
          )
          return c.json({
            success: true,
            workflow_id: id,
            dapr_instance_id: instanceID,
          })
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error)
          c.status(503)
          return c.json({
            success: false,
            error: `durable runtime unavailable: ${message}`,
          })
        }
      },
    )
    .post(
      "/plan",
      describeRoute({
        summary: "Generate plan synchronously",
        operationId: "durable.plan",
        responses: {
          200: {
            description: "generated plan",
            content: {
              "application/json": {
                schema: resolver(PlanResponse),
              },
            },
          },
        },
      }),
      validator("json", RunInput),
      async (c) => {
        const body = c.req.valid("json")
        const prompt = body.prompt?.trim() ?? ""
        if (!prompt) {
          c.status(400)
          return c.json({
            success: false,
            error: "prompt is required",
          })
        }
        try {
          const id = rid("durable-plan")
          const timeoutMinutes = body.agentConfig?.timeoutMinutes ?? 10
          const timeoutSeconds = Math.min(Math.max(timeoutMinutes * 60 + 30, 90), 3600)
          const workflowInput: DurablePlanPayload = {
            workflowID: id,
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: body.agentConfig?.name?.trim() || "plan",
            model: parseModel(body),
            tools: parseTools(body),
            instructions: body.agentConfig?.instructions ?? body.instructions,
          }
          const state = await withWorkflowClient(async (client) => {
            const instanceID = await client.scheduleNewWorkflow(durablePlanWorkflow, workflowInput, id)
            return await client.waitForWorkflowCompletion(instanceID, true, timeoutSeconds)
          })
          if (!state) {
            c.status(504)
            return c.json({
              success: false,
              error: "planning timed out before workflow state was available",
            })
          }
          const typed = state as unknown as WorkflowStateLike
          if (typed.runtimeStatus !== WORKFLOW_COMPLETED) {
            c.status(500)
            return c.json({
              success: false,
              error: workflowFailure(typed, parseWorkflowOutput(typed.serializedOutput)),
            })
          }
          const output = parseWorkflowOutput(typed.serializedOutput)
          if (!output) {
            c.status(500)
            return c.json({
              success: false,
              error: "planning workflow returned empty output",
            })
          }
          if (output.success === false) {
            c.status(500)
            return c.json({
              success: false,
              error: typeof output.error === "string" ? output.error : "planning workflow failed",
            })
          }
          return c.json(output as z.infer<typeof PlanResponse>)
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error)
          c.status(503)
          return c.json({
            success: false,
            error: `durable runtime unavailable: ${message}`,
          })
        }
      },
    )
    .get(
      "/run/:workflowID",
      describeRoute({
        summary: "Get durable-compatible run status",
        operationId: "durable.runStatus",
        responses: {
          200: {
            description: "status",
            content: {
              "application/json": {
                schema: resolver(RunStatus),
              },
            },
          },
        },
      }),
      validator(
        "param",
        z.object({
          workflowID: z.string(),
        }),
      ),
      async (c) => {
        const workflowID = c.req.valid("param").workflowID
        try {
          const state = await withWorkflowClient((client) => client.getWorkflowState(workflowID, true))
          if (!state) {
            return c.json({
              success: false,
              workflow_id: workflowID,
              status: "failed",
              error: "workflow not found",
            })
          }
          const typed = state as unknown as WorkflowStateLike
          if (
            typed.runtimeStatus === WORKFLOW_RUNNING ||
            typed.runtimeStatus === WORKFLOW_PENDING ||
            typed.runtimeStatus === WORKFLOW_SUSPENDED ||
            typed.runtimeStatus === WORKFLOW_CONTINUED_AS_NEW
          ) {
            return c.json({
              success: false,
              workflow_id: workflowID,
              status: "running",
            })
          }
          const output = parseWorkflowOutput(typed.serializedOutput)
          if (typed.runtimeStatus === WORKFLOW_COMPLETED) {
            if (output?.success === false) {
              return c.json({
                success: false,
                workflow_id: workflowID,
                status: "failed",
                error: workflowFailure(typed, output),
              })
            }
            return c.json({
              success: true,
              workflow_id: workflowID,
              status: "completed",
              result: output?.result ?? output,
            })
          }
          if (typed.runtimeStatus === WORKFLOW_FAILED || typed.runtimeStatus === WORKFLOW_TERMINATED) {
            return c.json({
              success: false,
              workflow_id: workflowID,
              status: "failed",
              error: workflowFailure(typed, output),
              result: output?.result,
            })
          }
          return c.json({
            success: false,
            workflow_id: workflowID,
            status: "running",
          })
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error)
          c.status(503)
          return c.json({
            success: false,
            workflow_id: workflowID,
            status: "failed",
            error: `durable runtime unavailable: ${message}`,
          })
        }
      },
    )
    .post(
      "/workspaces/profile",
      describeRoute({
        summary: "Create/get workspace profile",
        operationId: "durable.workspaceProfile",
        responses: {
          200: {
            description: "workspace profile",
            content: {
              "application/json": {
                schema: resolver(WorkspaceProfileResponse),
              },
            },
          },
        },
      }),
      validator("json", WorkspaceProfileInput),
      async (c) => {
        const body = c.req.valid("json")
        try {
          const profile = await createOrGetWorkspaceProfile(body)
          return c.json(workspaceProfile(profile))
        } catch (error: unknown) {
          c.status(400)
          return c.json({
            success: false,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      },
    )
    .post(
      "/workspaces/clone",
      describeRoute({
        summary: "Clone repository in workspace",
        operationId: "durable.workspaceClone",
        responses: {
          200: {
            description: "clone result",
            content: {
              "application/json": {
                schema: resolver(WorkspaceActionResponse),
              },
            },
          },
        },
      }),
      validator("json", WorkspaceCloneInput),
      async (c) => {
        const body = c.req.valid("json")
        try {
          const result = await runWorkspaceClone(body)
          return c.json({
            success: true,
            result,
          })
        } catch (error: unknown) {
          c.status(400)
          return c.json({
            success: false,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      },
    )
    .post(
      "/workspaces/command",
      describeRoute({
        summary: "Execute command in workspace",
        operationId: "durable.workspaceCommand",
        responses: {
          200: {
            description: "command result",
            content: {
              "application/json": {
                schema: resolver(WorkspaceActionResponse),
              },
            },
          },
        },
      }),
      validator("json", WorkspaceCommandInput),
      async (c) => {
        const body = c.req.valid("json")
        try {
          const result = await runWorkspaceCommand(body)
          return c.json({
            success: true,
            result,
          })
        } catch (error: unknown) {
          c.status(400)
          return c.json({
            success: false,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      },
    )
    .post(
      "/workspaces/file",
      describeRoute({
        summary: "Workspace file operations",
        operationId: "durable.workspaceFile",
        responses: {
          200: {
            description: "file operation result",
            content: {
              "application/json": {
                schema: resolver(WorkspaceActionResponse),
              },
            },
          },
        },
      }),
      validator("json", WorkspaceFileInput),
      async (c) => {
        const body = c.req.valid("json")
        try {
          const result = await runWorkspaceFileOperation(body)
          return c.json({
            success: true,
            result,
          })
        } catch (error: unknown) {
          c.status(400)
          return c.json({
            success: false,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      },
    )
    .post(
      "/workspaces/cleanup",
      describeRoute({
        summary: "Cleanup workspace sessions",
        operationId: "durable.workspaceCleanup",
        responses: {
          200: {
            description: "cleanup done",
            content: {
              "application/json": {
                schema: resolver(CleanupResponse),
              },
            },
          },
        },
      }),
      validator("json", CleanupInput),
      async (c) => {
        const body = c.req.valid("json")
        try {
          const cleanedWorkspaceRefs = await cleanupWorkspaceInput(body)
          return c.json({
            success: true,
            cleaned: cleanedWorkspaceRefs.length > 0,
            cleanedWorkspaceRefs,
            executionId: body.executionId,
          })
        } catch (error: unknown) {
          c.status(400)
          return c.json({
            success: false,
            cleaned: false,
            cleanedWorkspaceRefs: [],
            executionId: body.executionId,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      },
    ),
)
