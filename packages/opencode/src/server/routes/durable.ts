import {
  DaprWorkflowClient,
  WorkflowRuntime,
  type WorkflowActivityContext,
  type WorkflowContext,
} from "@dapr/dapr"
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
import { Tool } from "@/tool/tool"
import { ToolRegistry } from "@/tool/registry"
import path from "path"
import { request as httpsRequest } from "node:https"
import { existsSync, readFileSync } from "node:fs"
import { stat as fsStat } from "node:fs/promises"
import { createHash } from "node:crypto"

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
const WORKSPACE_CHANGE_STORE_PATH = path.join(Global.Path.state, "durable-workspaces", "changes.json")
const WORKSPACE_SANDBOX_NAMESPACE = process.env.WORKSPACE_SANDBOX_NAMESPACE?.trim() || process.env.SANDBOX_NAMESPACE?.trim() || "agent-sandbox"
const WORKSPACE_SANDBOX_TEMPLATE = process.env.WORKSPACE_SANDBOX_TEMPLATE?.trim() || process.env.SANDBOX_TEMPLATE?.trim() || "dapr-agent"
const WORKSPACE_SANDBOX_PORT = Number.parseInt(process.env.WORKSPACE_SANDBOX_PORT ?? "8888", 10)
const WORKSPACE_SANDBOX_ROOT = process.env.WORKSPACE_SANDBOX_ROOT?.trim() || process.env.WORKSPACE_SESSIONS_ROOT?.trim() || "/app/workspaces"
const WORKSPACE_SANDBOX_PROVISION_TIMEOUT_MS = Number.parseInt(process.env.WORKSPACE_SANDBOX_PROVISION_TIMEOUT_MS ?? "180000", 10)
const WORKSPACE_SANDBOX_REQUEST_TIMEOUT_MS = Number.parseInt(process.env.WORKSPACE_SANDBOX_REQUEST_TIMEOUT_MS ?? "30000", 10)
const WORKSPACE_SANDBOX_HEARTBEAT_MS = Number.parseInt(process.env.WORKSPACE_SANDBOX_HEARTBEAT_MS ?? "30000", 10)
const K8S_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
const K8S_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
const K8S_HOST = process.env.KUBERNETES_SERVICE_HOST || "kubernetes.default.svc"
const K8S_PORT = Number.parseInt(process.env.KUBERNETES_SERVICE_PORT || "443", 10)
const SANDBOX_CLAIM_API_GROUP = "extensions.agents.x-k8s.io"
const SANDBOX_CLAIM_API_VERSION = "v1alpha1"
const SANDBOX_CLAIM_PLURAL = "sandboxclaims"

const AgentConfig = z
  .object({
    name: z.string().optional(),
    instructions: z.string().optional(),
    modelSpec: z.string().optional(),
    tools: z.array(z.string()).optional(),
    maxTurns: z.number().int().positive().optional(),
    timeoutMinutes: z.number().int().positive().optional(),
    configuration: z
      .object({
        storeName: z.string().min(1),
        configName: z.string().optional(),
        keys: z.array(z.string()).optional(),
        metadata: z.record(z.string(), z.string()).optional(),
      })
      .optional(),
  })
  .partial()

const RunExecutionMode = z.enum(["legacy", "sandboxed"])

const RunInput = z.object({
  prompt: z.string().nullable().optional(),
  model: z.string().nullable().optional(),
  tools: z.union([z.array(z.string()), z.record(z.string(), z.boolean()), z.string()]).nullable().optional(),
  instructions: z.string().nullable().optional(),
  maxTurns: z.coerce.number().int().positive().nullable().optional(),
  timeoutMinutes: z.coerce.number().int().positive().nullable().optional(),
  hardTimeoutMinutes: z.coerce.number().int().positive().nullable().optional(),
  executionMode: RunExecutionMode.nullable().optional(),
  requireFileChanges: z.union([z.boolean(), z.string()]).nullable().optional(),
  waitForCompletion: z.union([z.boolean(), z.string()]).nullable().optional(),
  cwd: z.string().nullable().optional(),
  workspaceRef: z.string().nullable().optional(),
  executionId: z.string().nullable().optional(),
  dbExecutionId: z.string().nullable().optional(),
  parentExecutionId: z.string().nullable().optional(),
  workflowId: z.string().nullable().optional(),
  nodeId: z.string().nullable().optional(),
  nodeName: z.string().nullable().optional(),
  agentConfig: AgentConfig.nullable().optional(),
  plan: z.any().optional(),
  planJson: z.any().optional(),
  artifactRef: z.string().nullable().optional(),
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
  enabledTools: z.union([z.array(WorkspaceToolName), z.string()]).optional(),
  requireReadBeforeWrite: z.union([z.boolean(), z.string()]).optional(),
  commandTimeoutMs: z.union([z.number().int().positive(), z.string()]).optional(),
})

const WorkspaceCloneInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
  durableInstanceId: z.string().optional(),
  repositoryUrl: z.string().optional(),
  repositoryOwner: z.string().optional(),
  repositoryRepo: z.string().optional(),
  repositoryBranch: z.string().min(1),
  repositoryUsername: z.string().optional(),
  targetDir: z.string().optional(),
  repositoryToken: z.string().optional(),
  githubToken: z.string().optional(),
  timeoutMs: z.union([z.number().int().positive(), z.string()]).optional(),
})

const WorkspaceCommandInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
  durableInstanceId: z.string().optional(),
  command: z.string().optional(),
  timeoutMs: z.union([z.number().int().positive(), z.string()]).optional(),
})

const WorkspaceFileOperationName = z.enum(["read", "write", "edit", "list"])

const WorkspaceFileInput = z.object({
  workspaceRef: z.string().optional(),
  executionId: z.string().optional(),
  durableInstanceId: z.string().optional(),
  operation: WorkspaceFileOperationName,
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
  backend: z.literal("kubernetes"),
  enabledTools: z.array(WorkspaceToolName),
  requireReadBeforeWrite: z.boolean(),
  commandTimeoutMs: z.number().int().positive(),
  createdAt: z.string(),
  sandbox: z.object({
    backend: z.literal("kubernetes"),
    namespace: z.string(),
    templateName: z.string(),
    claimName: z.string(),
    sandboxName: z.string().optional(),
    podName: z.string().optional(),
    podIP: z.string().optional(),
    status: z.string().optional(),
    service: z.string(),
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

const WorkspaceChangeFileStatus = z.enum(["A", "M", "D", "R"])

const WorkspaceChangeFileEntry = z.object({
  path: z.string(),
  status: WorkspaceChangeFileStatus,
  oldPath: z.string().optional(),
})

const WorkspaceChangeMetadata = z.object({
  changeSetId: z.string(),
  executionId: z.string(),
  workspaceRef: z.string(),
  durableInstanceId: z.string().optional(),
  operation: z.string(),
  sequence: z.number().int().positive(),
  format: z.literal("git-unified-v1"),
  sha256: z.string(),
  filesChanged: z.number().int().nonnegative(),
  additions: z.number().int().nonnegative(),
  deletions: z.number().int().nonnegative(),
  bytes: z.number().int().nonnegative(),
  compressed: z.boolean(),
  storageRef: z.string(),
  createdAt: z.string(),
  includeInExecutionPatch: z.boolean(),
  truncated: z.boolean(),
  originalBytes: z.number().int().nonnegative(),
  files: z.array(WorkspaceChangeFileEntry),
  baseRevision: z.string().optional(),
  headRevision: z.string().optional(),
})

const WorkspaceChangesResponse = z.object({
  success: z.boolean(),
  executionId: z.string(),
  count: z.number().int().nonnegative(),
  changes: z.array(WorkspaceChangeMetadata),
  pending: z.boolean().optional(),
})

const WorkspaceChangeArtifactResponse = z.object({
  success: z.boolean(),
  executionId: z.string(),
  metadata: WorkspaceChangeMetadata,
  patch: z.string(),
})

const WorkspaceExecutionPatchResponse = z.object({
  success: z.boolean(),
  executionId: z.string(),
  durableInstanceId: z.string().optional(),
  patch: z.string(),
  changeSets: z.array(WorkspaceChangeMetadata),
})

const WorkspaceFileSnapshotHistoryEntry = z.object({
  id: z.string(),
  changeSetId: z.string(),
  sequence: z.number().int().positive(),
  path: z.string(),
  oldPath: z.string().optional(),
  status: WorkspaceChangeFileStatus,
  isBinary: z.boolean(),
  language: z.string().optional(),
  oldBytes: z.number().int().nonnegative(),
  newBytes: z.number().int().nonnegative(),
  oldStorageRef: z.string().optional(),
  newStorageRef: z.string().optional(),
  oldCompressed: z.boolean(),
  newCompressed: z.boolean(),
  createdAt: z.string(),
})

const WorkspaceFileSnapshot = z.object({
  executionId: z.string(),
  path: z.string(),
  oldPath: z.string().optional(),
  status: WorkspaceChangeFileStatus,
  isBinary: z.boolean(),
  language: z.string().optional(),
  oldContent: z.string().nullable(),
  newContent: z.string().nullable(),
  oldBytes: z.number().int().nonnegative(),
  newBytes: z.number().int().nonnegative(),
  baseRevision: z.string().optional(),
  headRevision: z.string().optional(),
  history: z.array(WorkspaceFileSnapshotHistoryEntry),
})

const WorkspaceFileSnapshotResponse = z.object({
  success: z.boolean(),
  executionId: z.string(),
  path: z.string(),
  durableInstanceId: z.string().optional(),
  snapshot: WorkspaceFileSnapshot,
})

const WorkspaceStoredSnapshot = z.object({
  path: z.string(),
  oldPath: z.string().optional(),
  status: WorkspaceChangeFileStatus,
  isBinary: z.boolean(),
  language: z.string().optional(),
  oldContent: z.string().nullable(),
  newContent: z.string().nullable(),
  oldBytes: z.number().int().nonnegative(),
  newBytes: z.number().int().nonnegative(),
})

const WorkspaceStoredChangeArtifact = z.object({
  metadata: WorkspaceChangeMetadata,
  patch: z.string(),
  snapshots: z.array(WorkspaceStoredSnapshot),
})

const WorkspaceStoredChangeRecord = z.object({
  version: z.number().int().positive().optional(),
  artifacts: z.array(WorkspaceStoredChangeArtifact).default([]),
})

const ToolsResponse = z.object({
  success: z.literal(true),
  tools: z.array(
    z.object({
      id: WorkspaceToolName,
      description: z.string(),
    }),
  ),
})

type ModelRef = { providerID: string; modelID: string }

type DurableRunPayload = {
  workflowID: string
  parentExecutionID?: string
  executionID?: string
  dbExecutionID?: string
  workflowDefinitionID?: string
  nodeID?: string
  nodeName?: string
  workspaceRef?: string
  prompt: string
  cwd?: string
  agent?: string
  model?: ModelRef
  tools?: Record<string, boolean>
  instructions?: string
  executionMode?: z.infer<typeof RunExecutionMode>
  hardTimeoutMinutes?: number
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
  executionID?: string
  dbExecutionID?: string
  workspaceRef?: string
  cwd?: string
  agent?: string
  model?: ModelRef
  tools?: Record<string, boolean>
  instructions?: string
  hardTimeoutMinutes?: number
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

type WorkspaceSandboxState = {
  namespace: string
  templateName: string
  claimName: string
  sandboxName?: string
  podName?: string
  podIP?: string
  status?: string
  service: string
  lastHeartbeatAt?: number
}

type WorkspaceSessionRecord = {
  workspaceRef: string
  executionId: string
  name: string
  rootPath: string
  clonePath?: string
  backend: "kubernetes"
  sandbox: WorkspaceSandboxState
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
  dbExecutionId?: string
  durableInstanceId?: string
}

type ChangeFileStatus = z.infer<typeof WorkspaceChangeFileStatus>

type ChangeFileEntry = z.infer<typeof WorkspaceChangeFileEntry>

type ChangeMetadata = z.infer<typeof WorkspaceChangeMetadata>

type ChangeSnapshotHistoryEntry = z.infer<typeof WorkspaceFileSnapshotHistoryEntry>

type ChangeSnapshot = {
  path: string
  oldPath?: string
  status: ChangeFileStatus
  isBinary: boolean
  language?: string
  oldContent: string | null
  newContent: string | null
  oldBytes: number
  newBytes: number
}

type ChangeArtifact = {
  metadata: ChangeMetadata
  patch: string
  snapshots: ChangeSnapshot[]
}

type ChangeStoreRecord = {
  version: number
  artifacts: ChangeArtifact[]
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
const changeArtifacts = new Map<string, ChangeArtifact>()
const executionToChangeSets = new Map<string, string[]>()
let changeStoreReady = false
let changeStoreLoading: Promise<void> | undefined
let changeStorePersisting = Promise.resolve()

const languageByExtension: Record<string, string> = {
  ts: "typescript",
  tsx: "tsx",
  js: "javascript",
  jsx: "jsx",
  py: "python",
  json: "json",
  md: "markdown",
  css: "css",
  scss: "scss",
  html: "html",
  yaml: "yaml",
  yml: "yaml",
  sh: "bash",
  bash: "bash",
  go: "go",
  rs: "rust",
  sql: "sql",
  java: "java",
  rb: "ruby",
  php: "php",
  c: "c",
  h: "c",
  cpp: "cpp",
  hpp: "cpp",
}

function rid(prefix: string) {
  return `${prefix}-${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`
}

const workspaceTools = WorkspaceToolName.options
const workspaceFileOperations = WorkspaceFileOperationName.options
const workspaceToolDescriptions: Record<WorkspaceTool, string> = {
  read: "Read files",
  write: "Write files",
  edit: "Edit files",
  list: "List files/directories",
  bash: "Run shell commands",
}

function sanitizeWorkspaceSegment(input: string) {
  return input.replace(/[^a-zA-Z0-9._-]/g, "-")
}

function normalizePosixPath(input: string) {
  const value = input.replace(/\\/g, "/").trim() || "/"
  const absolute = value.startsWith("/") ? value : `/${value}`
  const normalized = path.posix.normalize(absolute)
  return normalized === "." ? "/" : normalized
}

function containsPosixPath(root: string, target: string) {
  const normalizedRoot = normalizePosixPath(root).replace(/\/+$/, "") || "/"
  const normalizedTarget = normalizePosixPath(target)
  if (normalizedRoot === "/") return normalizedTarget.startsWith("/")
  return normalizedTarget === normalizedRoot || normalizedTarget.startsWith(`${normalizedRoot}/`)
}

function workspaceBaseRoot() {
  return normalizePosixPath(WORKSPACE_SANDBOX_ROOT)
}

function parseWorkspaceBoolean(input: unknown) {
  if (typeof input === "boolean") return input
  if (typeof input === "string") return input.trim().toLowerCase() === "true"
  return false
}

function parseWorkspaceTimeout(input: unknown) {
  if (typeof input === "number" && Number.isFinite(input) && input > 0) return Math.floor(input)
  if (typeof input === "string") {
    const parsed = Number.parseInt(input, 10)
    if (Number.isFinite(parsed) && parsed > 0) return parsed
  }
  return
}

function parseWorkspaceEnabledTools(input: unknown): WorkspaceTool[] {
  if (typeof input === "undefined") return [...workspaceTools]
  const parsed = Array.isArray(input)
    ? input
    : typeof input === "string"
      ? (() => {
          const trimmed = input.trim()
          if (!trimmed) return []
          if (trimmed.startsWith("[")) {
            try {
              const decoded = JSON.parse(trimmed) as unknown
              if (Array.isArray(decoded)) return decoded
            } catch {
              // fall through to csv parsing
            }
          }
          return trimmed.split(",").map((item) => item.trim()).filter(Boolean)
        })()
      : undefined
  if (!parsed) throw new Error("enabledTools must be an array of read, write, edit, list, bash")
  return [...new Set(parsed.map((item) => {
    if (typeof item !== "string") {
      throw new Error("enabledTools must be an array of read, write, edit, list, bash")
    }
    const value = item.trim()
    if (!workspaceTools.includes(value as WorkspaceTool)) {
      throw new Error(`enabledTools contains unsupported tool: ${value || "<empty>"}`)
    }
    return value as WorkspaceTool
  }))]
}

class K8sRequestError extends Error {
  statusCode: number
  constructor(statusCode: number, message: string) {
    super(message)
    this.name = "K8sRequestError"
    this.statusCode = statusCode
  }
}

function readK8sToken() {
  if (!existsSync(K8S_TOKEN_PATH)) {
    throw new Error("kubernetes service account token not found")
  }
  return readFileSync(K8S_TOKEN_PATH, "utf-8").trim()
}

function readK8sCA() {
  if (!existsSync(K8S_CA_PATH)) return
  return readFileSync(K8S_CA_PATH)
}

async function k8sRequest<T>(method: string, requestPath: string, body?: unknown): Promise<T> {
  const token = readK8sToken()
  const ca = readK8sCA()
  const payload = body === undefined ? undefined : JSON.stringify(body)
  return await new Promise<T>((resolve, reject) => {
    const req = httpsRequest(
      {
        hostname: K8S_HOST,
        port: K8S_PORT,
        path: requestPath,
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
          ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        },
        ca,
        rejectUnauthorized: ca !== undefined,
      },
      (res) => {
        let data = ""
        res.on("data", (chunk: Buffer | string) => {
          data += typeof chunk === "string" ? chunk : chunk.toString("utf-8")
        })
        res.on("end", () => {
          const statusCode = res.statusCode ?? 500
          let parsed: unknown = {}
          if (data) {
            try {
              parsed = JSON.parse(data)
            } catch {
              parsed = { message: data }
            }
          }
          if (statusCode >= 400) {
            const message =
              parsed && typeof parsed === "object" && typeof (parsed as Record<string, unknown>).message === "string"
                ? ((parsed as Record<string, unknown>).message as string)
                : data || `k8s request failed with status ${statusCode}`
            reject(new K8sRequestError(statusCode, message))
            return
          }
          resolve(parsed as T)
        })
      },
    )
    req.on("error", reject)
    if (payload) req.write(payload)
    req.end()
  })
}

function isK8sRequestError(error: unknown): error is K8sRequestError {
  return error instanceof K8sRequestError
}

function shellEscape(input: string) {
  return `'${input.replace(/'/g, "'\\''")}'`
}

function workspaceRecord(session: WorkspaceSession): WorkspaceSessionRecord {
  return {
    workspaceRef: session.workspaceRef,
    executionId: session.executionId,
    name: session.name,
    rootPath: session.rootPath,
    clonePath: session.clonePath,
    backend: session.backend,
    sandbox: session.sandbox,
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
    backend: "kubernetes" as const,
    namespace: session.sandbox.namespace,
    templateName: session.sandbox.templateName,
    claimName: session.sandbox.claimName,
    sandboxName: session.sandbox.sandboxName,
    podName: session.sandbox.podName,
    podIP: session.sandbox.podIP,
    status: session.sandbox.status,
    service: session.sandbox.service,
    rootPath: session.rootPath,
    workingDirectory: session.clonePath ?? session.rootPath,
    details: {
      heartbeatAt: session.sandbox.lastHeartbeatAt,
    },
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
    backend: "kubernetes" as const,
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
        if (record.backend !== "kubernetes") continue
        if (typeof record.workspaceRef !== "string" || !record.workspaceRef.trim()) continue
        if (typeof record.executionId !== "string" || !record.executionId.trim()) continue
        if (typeof record.rootPath !== "string" || !record.rootPath.trim()) continue
        if (!record.sandbox || typeof record.sandbox !== "object") continue
        if (typeof record.sandbox.claimName !== "string" || !record.sandbox.claimName.trim()) continue
        const session: WorkspaceSession = {
          workspaceRef: record.workspaceRef.trim(),
          executionId: record.executionId.trim(),
          name: typeof record.name === "string" && record.name.trim() ? record.name : `workspace-${record.executionId}`,
          rootPath: normalizePosixPath(record.rootPath.trim()),
          clonePath: typeof record.clonePath === "string" && record.clonePath.trim() ? normalizePosixPath(record.clonePath.trim()) : undefined,
          backend: "kubernetes",
          sandbox: {
            namespace:
              typeof record.sandbox.namespace === "string" && record.sandbox.namespace.trim()
                ? record.sandbox.namespace.trim()
                : WORKSPACE_SANDBOX_NAMESPACE,
            templateName:
              typeof record.sandbox.templateName === "string" && record.sandbox.templateName.trim()
                ? record.sandbox.templateName.trim()
                : WORKSPACE_SANDBOX_TEMPLATE,
            claimName: record.sandbox.claimName.trim(),
            sandboxName:
              typeof record.sandbox.sandboxName === "string" && record.sandbox.sandboxName.trim()
                ? record.sandbox.sandboxName.trim()
                : undefined,
            podName:
              typeof record.sandbox.podName === "string" && record.sandbox.podName.trim()
                ? record.sandbox.podName.trim()
                : undefined,
            podIP:
              typeof record.sandbox.podIP === "string" && record.sandbox.podIP.trim()
                ? record.sandbox.podIP.trim()
                : undefined,
            status:
              typeof record.sandbox.status === "string" && record.sandbox.status.trim()
                ? record.sandbox.status.trim()
                : undefined,
            service:
              typeof record.sandbox.service === "string" && record.sandbox.service.trim()
                ? record.sandbox.service.trim()
                : `http://sandbox.${WORKSPACE_SANDBOX_NAMESPACE}.svc.cluster.local:${WORKSPACE_SANDBOX_PORT}`,
            lastHeartbeatAt:
              typeof record.sandbox.lastHeartbeatAt === "number" && record.sandbox.lastHeartbeatAt > 0
                ? record.sandbox.lastHeartbeatAt
                : undefined,
          },
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
        if (session.durableInstanceId) durableToWorkspace.set(session.durableInstanceId, session.workspaceRef)
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
        version: 2,
        sessions: [...workspaceSessions.values()].map((session) => workspaceRecord(session)),
      })
    })
  return workspaceStorePersisting
}

function normalizeChangePathKey(input: string) {
  return input.trim().replace(/\\/g, "/").replace(/^\.\/+/, "").replace(/^\/+/, "").replace(/\/+/g, "/")
}

function changePathForFile(session: WorkspaceSession, fullPath: string) {
  const base = session.clonePath && containsPosixPath(session.clonePath, fullPath) ? session.clonePath : session.rootPath
  const relative = normalizeChangePathKey(path.posix.relative(base, fullPath))
  return relative || path.posix.basename(fullPath)
}

function changeLanguage(input: string) {
  const normalized = normalizeChangePathKey(input)
  const extension = normalized.includes(".") ? normalized.split(".").pop()?.toLowerCase() : undefined
  if (!extension) return
  return languageByExtension[extension]
}

function isBinaryContent(content: string) {
  if (!content) return false
  if (content.includes("\u0000")) return true
  const sample = Buffer.from(content, "utf-8").subarray(0, 1024)
  if (!sample.length) return false
  const controls = [...sample].filter((byte) => (byte < 9 || (byte > 13 && byte < 32)) && byte !== 27).length
  return controls / sample.length > 0.3
}

function textLineCount(content: string | null) {
  if (content === null || content.length === 0) return 0
  return content.split("\n").length
}

function diffLines(content: string | null, prefix: "-" | "+") {
  if (content === null || content.length === 0) return [] as string[]
  return content.split("\n").map((line) => `${prefix}${line}`)
}

function buildUnifiedPatch(input: {
  path: string
  status: ChangeFileStatus
  oldContent: string | null
  newContent: string | null
  isBinary: boolean
}) {
  const filePath = normalizeChangePathKey(input.path)
  const oldLabel = input.status === "A" ? "/dev/null" : `a/${filePath}`
  const newLabel = input.status === "D" ? "/dev/null" : `b/${filePath}`
  if (input.isBinary) {
    return [
      `diff --git a/${filePath} b/${filePath}`,
      `--- ${oldLabel}`,
      `+++ ${newLabel}`,
      "Binary files differ",
      "",
    ].join("\n")
  }
  const oldLines = textLineCount(input.oldContent)
  const newLines = textLineCount(input.newContent)
  const oldStart = input.status === "A" ? 0 : 1
  const newStart = input.status === "D" ? 0 : 1
  return [
    `diff --git a/${filePath} b/${filePath}`,
    `--- ${oldLabel}`,
    `+++ ${newLabel}`,
    `@@ -${oldStart},${oldLines} +${newStart},${newLines} @@`,
    ...diffLines(input.oldContent, "-"),
    ...diffLines(input.newContent, "+"),
    "",
  ].join("\n")
}

function compareChangeArtifacts(left: ChangeArtifact, right: ChangeArtifact) {
  if (left.metadata.sequence !== right.metadata.sequence) return left.metadata.sequence - right.metadata.sequence
  return Date.parse(left.metadata.createdAt) - Date.parse(right.metadata.createdAt)
}

function changeArtifactsForExecution(executionId: string) {
  const ids = executionToChangeSets.get(executionId) ?? []
  return [...new Set(ids)]
    .flatMap((changeSetId) => {
      const artifact = changeArtifacts.get(changeSetId)
      if (!artifact) return []
      return [artifact]
    })
    .sort(compareChangeArtifacts)
}

function nextChangeSequence(executionId: string) {
  const max = changeArtifactsForExecution(executionId).reduce((value, artifact) => Math.max(value, artifact.metadata.sequence), 0)
  return max + 1
}

function nextChangeSetID() {
  return `chg_${crypto.randomUUID().replace(/-/g, "").slice(0, 16)}`
}

async function ensureChangeStore() {
  if (changeStoreReady) return
  if (!changeStoreLoading) {
    changeStoreLoading = (async () => {
      const file = await Filesystem.readJson<unknown>(WORKSPACE_CHANGE_STORE_PATH).catch(() => undefined)
      const parsed = WorkspaceStoredChangeRecord.safeParse(file ?? {})
      if (!parsed.success) {
        changeStoreReady = true
        return
      }
      changeArtifacts.clear()
      executionToChangeSets.clear()
      for (const artifact of parsed.data.artifacts) {
        const normalized: ChangeArtifact = {
          metadata: artifact.metadata,
          patch: artifact.patch,
          snapshots: artifact.snapshots,
        }
        changeArtifacts.set(normalized.metadata.changeSetId, normalized)
      }
      for (const artifact of [...changeArtifacts.values()].sort(compareChangeArtifacts)) {
        const executionId = artifact.metadata.executionId
        const ids = executionToChangeSets.get(executionId) ?? []
        ids.push(artifact.metadata.changeSetId)
        executionToChangeSets.set(executionId, ids)
      }
      changeStoreReady = true
    })().catch((error: unknown) => {
      log.warn("failed loading workspace change store", {
        error: error instanceof Error ? error.message : String(error),
      })
      changeStoreReady = true
    })
  }
  await changeStoreLoading
}

async function persistChangeStore() {
  await ensureChangeStore()
  changeStorePersisting = changeStorePersisting
    .catch(() => undefined)
    .then(async () => {
      await Filesystem.writeJson(WORKSPACE_CHANGE_STORE_PATH, {
        version: 1,
        artifacts: [...changeArtifacts.values()].sort(compareChangeArtifacts),
      } satisfies ChangeStoreRecord)
    })
  return changeStorePersisting
}

async function recordWorkspaceChange(input: {
  session: WorkspaceSession
  operation: string
  durableInstanceId?: string
  includeInExecutionPatch?: boolean
  patch: string
  files: ChangeFileEntry[]
  additions: number
  deletions: number
  snapshots: ChangeSnapshot[]
  baseRevision?: string
  headRevision?: string
}) {
  await ensureChangeStore()
  const changeSetId = nextChangeSetID()
  const patchBytes = Buffer.byteLength(input.patch, "utf-8")
  const metadata: ChangeMetadata = {
    changeSetId,
    executionId: input.session.executionId,
    workspaceRef: input.session.workspaceRef,
    durableInstanceId: input.durableInstanceId?.trim() || input.session.durableInstanceId,
    operation: input.operation,
    sequence: nextChangeSequence(input.session.executionId),
    format: "git-unified-v1",
    sha256: createHash("sha256").update(input.patch).digest("hex"),
    filesChanged: input.files.length,
    additions: Math.max(0, Math.floor(input.additions)),
    deletions: Math.max(0, Math.floor(input.deletions)),
    bytes: patchBytes,
    compressed: false,
    storageRef: `inline:${changeSetId}`,
    createdAt: new Date().toISOString(),
    includeInExecutionPatch: input.includeInExecutionPatch !== false,
    truncated: false,
    originalBytes: patchBytes,
    files: input.files,
    baseRevision: input.baseRevision,
    headRevision: input.headRevision,
  }
  const artifact: ChangeArtifact = {
    metadata,
    patch: input.patch,
    snapshots: input.snapshots,
  }
  changeArtifacts.set(changeSetId, artifact)
  const ids = executionToChangeSets.get(metadata.executionId) ?? []
  ids.push(changeSetId)
  executionToChangeSets.set(metadata.executionId, ids)
  await persistChangeStore()
  return artifact
}

async function readWorkspaceChangeArtifact(changeSetId: string) {
  await ensureChangeStore()
  const id = changeSetId.trim()
  if (!id) return
  return changeArtifacts.get(id)
}

async function listWorkspaceExecutionChanges(input: {
  executionId: string
  durableInstanceId?: string
  includeExcluded?: boolean
}) {
  await ensureChangeStore()
  const executionId = input.executionId.trim()
  if (!executionId) return [] as ChangeArtifact[]
  return changeArtifactsForExecution(executionId).filter((artifact) => {
    if (input.includeExcluded !== true && !artifact.metadata.includeInExecutionPatch) return false
    if (input.durableInstanceId && artifact.metadata.durableInstanceId !== input.durableInstanceId) return false
    return true
  })
}

async function executionPatch(input: {
  executionId: string
  durableInstanceId?: string
  includeExcluded?: boolean
}) {
  const artifacts = await listWorkspaceExecutionChanges(input)
  return {
    patch: artifacts.map((artifact) => artifact.patch).filter(Boolean).join("\n"),
    changeSets: artifacts.map((artifact) => artifact.metadata),
  }
}

async function executionFileSnapshot(input: {
  executionId: string
  path: string
  durableInstanceId?: string
}) {
  const requestedPath = normalizeChangePathKey(input.path)
  if (!requestedPath) return
  const artifacts = await listWorkspaceExecutionChanges({
    executionId: input.executionId,
    durableInstanceId: input.durableInstanceId,
    includeExcluded: true,
  })
  if (!artifacts.length) return
  const lineagePaths = new Set<string>([requestedPath])
  const lineage = [] as Array<{
    artifact: ChangeArtifact
    snapshot: ChangeSnapshot
  }>
  for (const artifact of artifacts) {
    for (const snapshot of artifact.snapshots) {
      const currentPath = normalizeChangePathKey(snapshot.path)
      const oldPath = snapshot.oldPath ? normalizeChangePathKey(snapshot.oldPath) : undefined
      if (!lineagePaths.has(currentPath) && (!oldPath || !lineagePaths.has(oldPath))) continue
      lineage.push({ artifact, snapshot })
      lineagePaths.add(currentPath)
      if (oldPath) lineagePaths.add(oldPath)
    }
  }
  if (!lineage.length) return
  const firstWithOld = lineage.find((entry) => entry.snapshot.oldContent !== null || entry.snapshot.oldBytes > 0)
  const lastWithNew = [...lineage].reverse().find((entry) => entry.snapshot.newContent !== null || entry.snapshot.newBytes > 0)
  const last = lineage[lineage.length - 1]
  const isBinary = lineage.some((entry) => entry.snapshot.isBinary)
  const history = lineage.map((entry, index): ChangeSnapshotHistoryEntry => {
    const digest = createHash("sha256")
      .update(`${entry.artifact.metadata.changeSetId}:${index}:${entry.snapshot.path}:${entry.snapshot.oldPath ?? ""}`)
      .digest("hex")
      .slice(0, 18)
    return {
      id: `chgfil_${digest}`,
      changeSetId: entry.artifact.metadata.changeSetId,
      sequence: entry.artifact.metadata.sequence,
      path: entry.snapshot.path,
      oldPath: entry.snapshot.oldPath,
      status: entry.snapshot.status,
      isBinary: entry.snapshot.isBinary,
      language: entry.snapshot.language,
      oldBytes: entry.snapshot.oldBytes,
      newBytes: entry.snapshot.newBytes,
      oldStorageRef: undefined,
      newStorageRef: undefined,
      oldCompressed: false,
      newCompressed: false,
      createdAt: entry.artifact.metadata.createdAt,
    }
  })
  return {
    executionId: input.executionId.trim(),
    path: last.snapshot.path,
    oldPath: last.snapshot.oldPath,
    status: last.snapshot.status,
    isBinary,
    language: last.snapshot.language,
    oldContent: isBinary ? null : firstWithOld?.snapshot.oldContent ?? null,
    newContent: isBinary ? null : lastWithNew?.snapshot.newContent ?? null,
    oldBytes: firstWithOld?.snapshot.oldBytes ?? 0,
    newBytes: lastWithNew?.snapshot.newBytes ?? 0,
    baseRevision: lineage.find((entry) => entry.artifact.metadata.baseRevision)?.artifact.metadata.baseRevision,
    headRevision: [...lineage].reverse().find((entry) => entry.artifact.metadata.headRevision)?.artifact.metadata.headRevision,
    history,
  }
}

async function captureWorkspaceFileChange(input: {
  session: WorkspaceSession
  operation: "write" | "edit"
  fullPath: string
  oldContent: string | null
  newContent: string
  durableInstanceId?: string
}) {
  if (input.oldContent === input.newContent) return
  const pathKey = changePathForFile(input.session, input.fullPath)
  const status: ChangeFileStatus = input.oldContent === null ? "A" : "M"
  const binary = isBinaryContent(input.oldContent ?? "") || isBinaryContent(input.newContent)
  const snapshot: ChangeSnapshot = {
    path: pathKey,
    oldPath: undefined,
    status,
    isBinary: binary,
    language: changeLanguage(pathKey),
    oldContent: binary ? null : input.oldContent,
    newContent: binary ? null : input.newContent,
    oldBytes: input.oldContent === null ? 0 : Buffer.byteLength(input.oldContent, "utf-8"),
    newBytes: Buffer.byteLength(input.newContent, "utf-8"),
  }
  const patch = buildUnifiedPatch({
    path: pathKey,
    status,
    oldContent: snapshot.oldContent,
    newContent: snapshot.newContent,
    isBinary: snapshot.isBinary,
  })
  return await recordWorkspaceChange({
    session: input.session,
    operation: input.operation,
    durableInstanceId: input.durableInstanceId,
    patch,
    files: [{ path: pathKey, status }],
    additions: snapshot.isBinary ? 0 : textLineCount(snapshot.newContent),
    deletions: snapshot.isBinary ? 0 : status === "A" ? 0 : textLineCount(snapshot.oldContent),
    snapshots: [snapshot],
  })
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
  const byDbExecution = input.dbExecutionId?.trim() || ""
  if (byDbExecution && byDbExecution !== byExecution) {
    const ref = executionToWorkspace.get(byDbExecution)
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
  if (!value) return normalizePosixPath(path.posix.join(base, sanitizeWorkspaceSegment(executionID)))
  if (value.startsWith("/")) {
    const resolved = normalizePosixPath(value)
    if (!containsPosixPath(base, resolved)) {
      throw new Error("rootPath must stay inside sandbox workspace root")
    }
    return resolved
  }
  const resolved = normalizePosixPath(path.posix.join(base, value))
  if (!containsPosixPath(base, resolved)) {
    throw new Error("rootPath must stay inside sandbox workspace root")
  }
  return resolved
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

function sandboxClaimPath(namespace: string, claimName?: string) {
  const base = `/apis/${SANDBOX_CLAIM_API_GROUP}/${SANDBOX_CLAIM_API_VERSION}/namespaces/${namespace}/${SANDBOX_CLAIM_PLURAL}`
  if (!claimName) return base
  return `${base}/${claimName}`
}

async function waitForSandboxName(namespace: string, claimName: string) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < WORKSPACE_SANDBOX_PROVISION_TIMEOUT_MS) {
    const claim = await k8sRequest<Record<string, unknown>>("GET", sandboxClaimPath(namespace, claimName))
    const status = claim.status as Record<string, unknown> | undefined
    const sandboxStatus = status?.sandbox as Record<string, unknown> | undefined
    const sandboxName = typeof sandboxStatus?.Name === "string" ? sandboxStatus.Name.trim() : ""
    if (sandboxName) return sandboxName
    const conditions = Array.isArray(status?.conditions) ? status.conditions : []
    for (const condition of conditions) {
      const value = condition as Record<string, unknown>
      if (value.type === "Ready" && value.status === "False" && value.reason === "Failed") {
        throw new Error(typeof value.message === "string" ? value.message : `sandbox claim ${claimName} failed`)
      }
    }
    await sleep(1000)
  }
  throw new Error(`sandbox claim "${claimName}" not ready within timeout`)
}

async function getPodEndpointByName(namespace: string, podName: string) {
  try {
    const pod = await k8sRequest<Record<string, unknown>>("GET", `/api/v1/namespaces/${namespace}/pods/${podName}`)
    const metadata = (pod.metadata ?? {}) as Record<string, unknown>
    const status = (pod.status ?? {}) as Record<string, unknown>
    const podIP = typeof status.podIP === "string" ? status.podIP.trim() : ""
    const phase = typeof status.phase === "string" ? status.phase.trim() : ""
    const resolvedPodName = typeof metadata.name === "string" ? metadata.name.trim() : podName
    if (!podIP || phase !== "Running") return
    return {
      podName: resolvedPodName,
      podIP,
    }
  } catch {
    return
  }
}

async function getPodEndpointBySelector(namespace: string, selector: string) {
  try {
    const listPath = `/api/v1/namespaces/${namespace}/pods?labelSelector=${encodeURIComponent(selector)}`
    const podList = await k8sRequest<Record<string, unknown>>("GET", listPath)
    const items = Array.isArray(podList.items) ? podList.items : []
    for (const item of items) {
      const pod = item as Record<string, unknown>
      const metadata = (pod.metadata ?? {}) as Record<string, unknown>
      const status = (pod.status ?? {}) as Record<string, unknown>
      const podIP = typeof status.podIP === "string" ? status.podIP.trim() : ""
      const phase = typeof status.phase === "string" ? status.phase.trim() : ""
      const podName = typeof metadata.name === "string" ? metadata.name.trim() : ""
      if (podIP && phase === "Running" && podName) {
        return {
          podName,
          podIP,
        }
      }
    }
    return
  } catch {
    return
  }
}

async function resolveSandboxPodEndpoint(namespace: string, sandboxName: string) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < WORKSPACE_SANDBOX_PROVISION_TIMEOUT_MS) {
    try {
      const sandboxPath = `/apis/agents.x-k8s.io/v1alpha1/namespaces/${namespace}/sandboxes/${sandboxName}`
      const sandbox = await k8sRequest<Record<string, unknown>>("GET", sandboxPath)
      const metadata = (sandbox.metadata ?? {}) as Record<string, unknown>
      const annotations = (metadata.annotations ?? {}) as Record<string, unknown>
      const status = (sandbox.status ?? {}) as Record<string, unknown>

      const annotatedPod = typeof annotations["agents.x-k8s.io/pod-name"] === "string"
        ? annotations["agents.x-k8s.io/pod-name"].trim()
        : ""
      if (annotatedPod) {
        const endpoint = await getPodEndpointByName(namespace, annotatedPod)
        if (endpoint) return endpoint
      }

      const selector = typeof status.selector === "string" ? status.selector.trim() : ""
      if (selector) {
        const endpoint = await getPodEndpointBySelector(namespace, selector)
        if (endpoint) return endpoint
      }

      const fallback = await getPodEndpointByName(namespace, sandboxName)
      if (fallback) return fallback
    } catch {
      // sandbox may not be available yet
    }
    await sleep(1000)
  }
  throw new Error(`unable to resolve pod endpoint for sandbox "${sandboxName}"`)
}

async function provisionWorkspaceSandbox(session: WorkspaceSession) {
  try {
    await k8sRequest("POST", sandboxClaimPath(session.sandbox.namespace), {
      apiVersion: `${SANDBOX_CLAIM_API_GROUP}/${SANDBOX_CLAIM_API_VERSION}`,
      kind: "SandboxClaim",
      metadata: {
        name: session.sandbox.claimName,
        namespace: session.sandbox.namespace,
        labels: {
          "app.kubernetes.io/managed-by": "opencode-durable",
          "opencode.ai/workspace-ref": session.workspaceRef,
          "opencode.ai/execution-id": session.executionId,
        },
      },
      spec: {
        sandboxTemplateRef: {
          name: session.sandbox.templateName,
        },
      },
    })
  } catch (error: unknown) {
    if (!isK8sRequestError(error) || error.statusCode !== 409) throw error
  }
  session.sandbox.status = "provisioning"
  const sandboxName = await waitForSandboxName(session.sandbox.namespace, session.sandbox.claimName)
  const endpoint = await resolveSandboxPodEndpoint(session.sandbox.namespace, sandboxName)
  session.sandbox.sandboxName = sandboxName
  session.sandbox.podName = endpoint.podName
  session.sandbox.podIP = endpoint.podIP
  session.sandbox.status = "ready"
  session.sandbox.lastHeartbeatAt = Date.now()
}

async function ensureWorkspaceSandbox(session: WorkspaceSession) {
  const now = Date.now()
  if (
    session.sandbox.podIP &&
    session.sandbox.sandboxName &&
    session.sandbox.lastHeartbeatAt &&
    now - session.sandbox.lastHeartbeatAt < WORKSPACE_SANDBOX_HEARTBEAT_MS
  ) {
    return
  }
  if (!session.sandbox.sandboxName) {
    session.sandbox.sandboxName = await waitForSandboxName(session.sandbox.namespace, session.sandbox.claimName)
  }
  const endpoint = await resolveSandboxPodEndpoint(session.sandbox.namespace, session.sandbox.sandboxName)
  session.sandbox.podName = endpoint.podName
  session.sandbox.podIP = endpoint.podIP
  session.sandbox.status = "ready"
  session.sandbox.lastHeartbeatAt = now
  await persistWorkspaceStore()
}

async function executeSandboxCommand(input: {
  session: WorkspaceSession
  command: string
  cwd: string
  timeoutMs: number
}) {
  await ensureWorkspaceSandbox(input.session)
  if (!input.session.sandbox.podIP) {
    throw new Error(`workspace sandbox pod IP is not available for ${input.session.workspaceRef}`)
  }
  const startedAt = Date.now()
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), input.timeoutMs)
  try {
    const wrapped = `cd ${shellEscape(input.cwd)} && ${input.command}`
    const response = await fetch(`http://${input.session.sandbox.podIP}:${WORKSPACE_SANDBOX_PORT}/execute`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: wrapped,
        timeout: input.timeoutMs,
      }),
      signal: controller.signal,
    })
    const elapsed = Date.now() - startedAt
    if (!response.ok) {
      const details = await response.text().catch(() => "")
      throw new Error(`sandbox /execute failed (${response.status}): ${details || "unknown error"}`)
    }
    const result = (await response.json()) as Record<string, unknown>
    const exitCode = typeof result.exit_code === "number" ? result.exit_code : 1
    return {
      stdout: typeof result.stdout === "string" ? result.stdout : "",
      stderr: typeof result.stderr === "string" ? result.stderr : "",
      exitCode,
      success: exitCode === 0,
      executionTimeMs: elapsed,
      timedOut: false as boolean | undefined,
    }
  } catch (error: unknown) {
    if (error instanceof Error && error.name === "AbortError") {
      return {
        stdout: "",
        stderr: "command timed out",
        exitCode: 124,
        success: false,
        executionTimeMs: Date.now() - startedAt,
        timedOut: true as boolean | undefined,
      }
    }
    throw error
  } finally {
    clearTimeout(timer)
  }
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

function resolveWorkspacePath(session: WorkspaceSession, value: string | undefined, operation: string) {
  const input = value?.trim() || ""
  if (!input) throw new Error(`path is required for ${operation}`)
  const full = normalizePosixPath(path.posix.resolve(session.rootPath, input))
  if (!containsPosixPath(session.rootPath, full)) {
    throw new Error(`path "${input}" escapes workspace root`)
  }
  return full
}

function assertCloneScope(session: WorkspaceSession, fullPath: string, operation: string) {
  if (!session.clonePath) return
  if (fullPath === session.clonePath) return
  if (containsPosixPath(session.clonePath, fullPath)) return
  throw new Error(`${operation} path is outside clone root "${session.clonePath}"`)
}

async function sandboxPathExists(session: WorkspaceSession, fullPath: string) {
  const result = await executeSandboxCommand({
    session,
    command: `test -e ${shellEscape(fullPath)}`,
    cwd: session.rootPath,
    timeoutMs: Math.min(session.commandTimeoutMs, 15000),
  })
  return result.success
}

async function enforceReadBeforeWrite(session: WorkspaceSession, fullPath: string) {
  if (!session.requireReadBeforeWrite) return
  if (!(await sandboxPathExists(session, fullPath))) return
  if (session.readPaths.has(fullPath)) return
  throw new Error(`Write blocked by read-before-write policy for "${fullPath}" in workspace ${session.workspaceRef}`)
}

function mapWorkspacePathError(stderr: string, fullPath: string) {
  const value = stderr.toLowerCase()
  if (value.includes("no such file")) return `path not found: ${fullPath}`
  if (value.includes("is a directory")) return `path is a directory: ${fullPath}`
  if (value.includes("permission denied")) return `permission denied: ${fullPath}`
  return stderr || `operation failed for ${fullPath}`
}

async function readWorkspaceFile(session: WorkspaceSession, fullPath: string) {
  await ensureWorkspaceSandbox(session)
  if (!session.sandbox.podIP) throw new Error("workspace sandbox pod is not ready")
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), Math.min(session.commandTimeoutMs, WORKSPACE_SANDBOX_REQUEST_TIMEOUT_MS))
  try {
    const response = await fetch(
      `http://${session.sandbox.podIP}:${WORKSPACE_SANDBOX_PORT}/download/${encodeURIComponent(fullPath)}`,
      { signal: controller.signal },
    )
    if (response.ok) {
      return await response.text()
    }
    if (response.status === 404) throw new Error(`path not found: ${fullPath}`)
    const fallback = await executeSandboxCommand({
      session,
      command: [
        `if [ ! -e ${shellEscape(fullPath)} ]; then`,
        'echo "No such file or directory" 1>&2;',
        "exit 2;",
        "fi;",
        `if [ -d ${shellEscape(fullPath)} ]; then`,
        'echo "Is a directory" 1>&2;',
        "exit 21;",
        "fi;",
        `base64 ${shellEscape(fullPath)} | tr -d '\\n'`,
      ].join(" "),
      cwd: session.rootPath,
      timeoutMs: session.commandTimeoutMs,
    })
    if (!fallback.success) throw new Error(mapWorkspacePathError(fallback.stderr, fullPath))
    return Buffer.from(fallback.stdout.trim(), "base64").toString("utf-8")
  } finally {
    clearTimeout(timer)
  }
}

async function writeWorkspaceFile(session: WorkspaceSession, fullPath: string, content: string) {
  const parent = normalizePosixPath(path.posix.dirname(fullPath))
  const mkdir = await executeSandboxCommand({
    session,
    command: `mkdir -p ${shellEscape(parent)}`,
    cwd: session.rootPath,
    timeoutMs: session.commandTimeoutMs,
  })
  if (!mkdir.success) throw new Error(mkdir.stderr || `failed creating parent directory for ${fullPath}`)
  const encoded = Buffer.from(content, "utf-8").toString("base64")
  const write = await executeSandboxCommand({
    session,
    command: `printf %s ${shellEscape(encoded)} | base64 -d > ${shellEscape(fullPath)}`,
    cwd: session.rootPath,
    timeoutMs: session.commandTimeoutMs,
  })
  if (!write.success) throw new Error(mapWorkspacePathError(write.stderr, fullPath))
}

async function listWorkspacePath(session: WorkspaceSession, fullPath: string) {
  const result = await executeSandboxCommand({
    session,
    command: `find ${shellEscape(fullPath)} -maxdepth 1 -mindepth 1 -exec stat --format='%n\\t%F' {} \\;`,
    cwd: session.rootPath,
    timeoutMs: session.commandTimeoutMs,
  })
  if (!result.success) throw new Error(mapWorkspacePathError(result.stderr, fullPath))
  return result.stdout
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [target, kind] = line.split("\t")
      const name = target ? path.posix.basename(target) : ""
      return {
        name,
        type: kind?.includes("directory") ? "directory" : "file",
      }
    })
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
  const workspaceRef = `ws_${rid("session").replace(/^session-/, "")}`
  const now = Date.now()
  const session: WorkspaceSession = {
    workspaceRef,
    executionId: executionID,
    name: input.name?.trim() || `workspace-${executionID}`,
    rootPath: resolveWorkspaceRoot(executionID, input.rootPath),
    clonePath: undefined,
    backend: "kubernetes",
    sandbox: {
      namespace: WORKSPACE_SANDBOX_NAMESPACE,
      templateName: WORKSPACE_SANDBOX_TEMPLATE,
      claimName: `opencode-${sanitizeWorkspaceSegment(rid("claim").replace("claim-", ""))}`.slice(0, 63),
      service: `http://sandbox.${WORKSPACE_SANDBOX_NAMESPACE}.svc.cluster.local:${WORKSPACE_SANDBOX_PORT}`,
      status: "creating",
    },
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
  try {
    await provisionWorkspaceSandbox(session)
    const mkdir = await executeSandboxCommand({
      session,
      command: `mkdir -p ${shellEscape(session.rootPath)}`,
      cwd: "/",
      timeoutMs: session.commandTimeoutMs,
    })
    if (!mkdir.success) {
      throw new Error(mkdir.stderr || `failed creating workspace root ${session.rootPath}`)
    }
    await persistWorkspaceStore()
    return session
  } catch (error) {
    try {
      await k8sRequest("DELETE", sandboxClaimPath(session.sandbox.namespace, session.sandbox.claimName))
    } catch {
      // best effort cleanup
    }
    workspaceSessions.delete(session.workspaceRef)
    executionToWorkspace.delete(executionID)
    throw error
  }
}

function assertWorkspaceTool(session: WorkspaceSession, tool: WorkspaceTool) {
  if (session.enabledTools.includes(tool)) return
  throw new Error(`Tool "${tool}" is disabled for workspace ${session.workspaceRef}`)
}

function attachRepositoryCredentials(input: { repositoryUrl: string; repositoryToken?: string; repositoryUsername?: string }) {
  const token = input.repositoryToken?.trim() || ""
  if (!token) return input.repositoryUrl
  const username = input.repositoryUsername?.trim() || ""
  let parsed: URL
  try {
    parsed = new URL(input.repositoryUrl)
  } catch {
    return input.repositoryUrl
  }
  if (parsed.username || parsed.password) return input.repositoryUrl
  if (username) {
    parsed.username = username
    parsed.password = token
    return parsed.toString()
  }
  parsed.username = token
  return parsed.toString()
}

function sanitizeCloneError(input: { error: string; repositoryToken?: string; repositoryUsername?: string }) {
  const token = input.repositoryToken?.trim() || ""
  if (!token) return input.error
  const username = input.repositoryUsername?.trim() || ""
  const encodedToken = encodeURIComponent(token)
  const encodedPair = username ? `${encodeURIComponent(username)}:${encodedToken}` : ""
  const plainPair = username ? `${username}:${token}` : ""
  let sanitized = input.error.replaceAll(token, "***")
  sanitized = sanitized.replaceAll(encodedToken, "***")
  if (plainPair) sanitized = sanitized.replaceAll(plainPair, "***:***")
  if (encodedPair) sanitized = sanitized.replaceAll(encodedPair, "***:***")
  return sanitized
}

function repositoryLabel(input: { repositoryUrl: string; repositoryOwner?: string; repositoryRepo?: string }) {
  const owner = input.repositoryOwner?.trim() || ""
  const repo = input.repositoryRepo?.trim() || ""
  if (owner && repo) return `${owner}/${repo}`
  try {
    const parsed = new URL(input.repositoryUrl)
    const pathname = parsed.pathname.replace(/^\/+/, "").replace(/\.git$/i, "")
    if (!pathname) return parsed.host
    return `${parsed.host}/${pathname}`
  } catch {
    return input.repositoryUrl
  }
}

async function runWorkspaceClone(input: z.infer<typeof WorkspaceCloneInput>) {
  const session = await resolveWorkspaceFromInput(input)
  assertWorkspaceTool(session, "bash")
  await bindWorkspaceDurableInstance(session, parseDurableInstanceID(input))
  const repositoryUrl = input.repositoryUrl?.trim() || ""
  const repositoryOwner = input.repositoryOwner?.trim() || ""
  const repositoryRepo = input.repositoryRepo?.trim() || ""
  if (!repositoryUrl && (!repositoryOwner || !repositoryRepo)) {
    throw new Error("repositoryUrl or repositoryOwner/repositoryRepo are required")
  }
  const branch = input.repositoryBranch.trim()
  const baseRepositoryURL = repositoryUrl || `https://github.com/${repositoryOwner}/${repositoryRepo}.git`
  let parsedRepositoryURL: URL
  try {
    parsedRepositoryURL = new URL(baseRepositoryURL)
  } catch {
    throw new Error("repositoryUrl must be a valid absolute URL")
  }
  if (parsedRepositoryURL.protocol !== "http:" && parsedRepositoryURL.protocol !== "https:") {
    throw new Error("repositoryUrl must use http or https")
  }
  const repoFromUrl = path.posix.basename(parsedRepositoryURL.pathname).replace(/\.git$/i, "")
  const repositoryName = repositoryRepo || repoFromUrl
  const timeoutMs = parseWorkspaceTimeout(input.timeoutMs) ?? Math.max(session.commandTimeoutMs, WORKSPACE_CLONE_TIMEOUT_MS)
  const cloneDir = resolveCloneTargetDir(repositoryName, input.targetDir)
  const clonePath = normalizePosixPath(path.posix.resolve(session.rootPath, cloneDir))
  if (!containsPosixPath(session.rootPath, clonePath)) {
    throw new Error("targetDir must stay inside workspace root")
  }
  const token = input.repositoryToken?.trim() || input.githubToken?.trim() || ""
  const repositoryUsername = input.repositoryUsername?.trim() || ""
  const repositoryURL = attachRepositoryCredentials({
    repositoryUrl: baseRepositoryURL,
    repositoryToken: token,
    repositoryUsername,
  })

  const gitCheck = await executeSandboxCommand({
    session,
    command: "git --version",
    cwd: session.rootPath,
    timeoutMs: Math.min(timeoutMs, 15000),
  })
  if (!gitCheck.success) throw new Error("git is not installed in the workspace sandbox")

  await executeSandboxCommand({
    session,
    command: `rm -rf ${shellEscape(clonePath)}`,
    cwd: session.rootPath,
    timeoutMs: Math.min(timeoutMs, 30000),
  })

  const clone = await executeSandboxCommand({
    session,
    command: `git clone --depth 1 --branch ${shellEscape(branch)} ${shellEscape(repositoryURL)} ${shellEscape(cloneDir)}`,
    cwd: session.rootPath,
    timeoutMs,
  })
  if (!clone.success) {
    const sanitized = sanitizeCloneError({
      error: clone.stderr,
      repositoryToken: token,
      repositoryUsername,
    })
    throw new Error(`git clone failed: ${sanitized || "unknown clone error"}`)
  }

  const rev = await executeSandboxCommand({
    session,
    command: "git rev-parse HEAD",
    cwd: clonePath,
    timeoutMs: Math.min(timeoutMs, 30000),
  })
  const commitHash = rev.success ? rev.stdout.trim() || "unknown" : "unknown"
  const files = await executeSandboxCommand({
    session,
    command: "git ls-files --cached",
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
    const strip = await executeSandboxCommand({
      session,
      command: "rm -rf .git",
      cwd: clonePath,
      timeoutMs: Math.min(timeoutMs, 15000),
    })
    strippedGitDir = strip.success
  }

  session.clonePath = clonePath
  touchWorkspace(session)
  await persistWorkspaceStore()
  return {
    success: true,
    clonePath,
    repository: repositoryLabel({
      repositoryUrl: baseRepositoryURL,
      repositoryOwner,
      repositoryRepo,
    }),
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
  const result = await executeSandboxCommand({
    session,
    command,
    cwd,
    timeoutMs,
  })
  const normalized = !result.success && result.exitCode === 2 && `${result.stdout}\n${result.stderr}`
    .toLowerCase()
    .includes("no file changes detected after durable run")
    ? {
        ...result,
        success: true,
        exitCode: 0,
        stderr: "",
      }
    : result
  touchWorkspace(session)
  await persistWorkspaceStore()
  return {
    ...normalized,
    sandbox: workspaceSandbox(session),
  }
}

function isWorkspaceFileOperation(input: string): input is (typeof workspaceFileOperations)[number] {
  return workspaceFileOperations.includes(input as (typeof workspaceFileOperations)[number])
}

function workspaceToolForOperation(operation: (typeof workspaceFileOperations)[number]): WorkspaceTool {
  if (operation === "read") return "read"
  if (operation === "write") return "write"
  if (operation === "edit") return "edit"
  return "list"
}

async function runWorkspaceFileOperation(input: z.infer<typeof WorkspaceFileInput>) {
  const session = await resolveWorkspaceFromInput(input)
  const operation = input.operation
  if (!isWorkspaceFileOperation(operation)) {
    throw new Error("operation must be one of read, write, edit, list")
  }
  assertWorkspaceTool(session, workspaceToolForOperation(operation))
  await bindWorkspaceDurableInstance(session, parseDurableInstanceID(input))

  if (operation === "read") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    const content = await readWorkspaceFile(session, fullPath)
    session.readPaths.add(fullPath)
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { content }
  }

  if (operation === "write") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await enforceReadBeforeWrite(session, fullPath)
    const existed = await sandboxPathExists(session, fullPath)
    const oldContent = existed ? await readWorkspaceFile(session, fullPath) : null
    const newContent = input.content ?? ""
    await writeWorkspaceFile(session, fullPath, newContent)
    const artifact = await captureWorkspaceFileChange({
      session,
      operation: "write",
      fullPath,
      oldContent,
      newContent,
      durableInstanceId: parseDurableInstanceID(input),
    })
    touchWorkspace(session)
    await persistWorkspaceStore()
    return {
      path: input.path ?? "",
      changeSetId: artifact?.metadata.changeSetId,
    }
  }

  if (operation === "edit") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await enforceReadBeforeWrite(session, fullPath)
    const oldString = input.old_string ?? ""
    if (!oldString) throw new Error("old_string is required for edit")
    const current = await readWorkspaceFile(session, fullPath)
    if (!current.includes(oldString)) throw new Error(`old_string not found in ${input.path}`)
    const updated = current.replace(oldString, input.new_string ?? "")
    await writeWorkspaceFile(session, fullPath, updated)
    const artifact = await captureWorkspaceFileChange({
      session,
      operation: "edit",
      fullPath,
      oldContent: current,
      newContent: updated,
      durableInstanceId: parseDurableInstanceID(input),
    })
    touchWorkspace(session)
    await persistWorkspaceStore()
    return {
      path: input.path ?? "",
      changeSetId: artifact?.metadata.changeSetId,
    }
  }

  if (operation === "list") {
    const fullPath = normalizePosixPath(path.posix.resolve(session.rootPath, input.path?.trim() || "."))
    if (!containsPosixPath(session.rootPath, fullPath)) throw new Error(`path "${input.path}" escapes workspace root`)
    const files = await listWorkspacePath(session, fullPath)
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { files }
  }

  throw new Error("operation must be one of read, write, edit, list")
}

async function cleanupWorkspaceRef(workspaceRef: string) {
  await ensureWorkspaceStore()
  const ref = workspaceRef.trim()
  if (!ref) return false
  const session = workspaceSessions.get(ref)
  if (!session) return false
  workspaceSessions.delete(ref)
  executionToWorkspace.delete(session.executionId)
  if (session.durableInstanceId) durableToWorkspace.delete(session.durableInstanceId)
  try {
    await k8sRequest("DELETE", sandboxClaimPath(session.sandbox.namespace, session.sandbox.claimName))
  } catch (error: unknown) {
    if (!isK8sRequestError(error) || error.statusCode !== 404) {
      log.warn("failed deleting sandbox claim", {
        workspaceRef: ref,
        claimName: session.sandbox.claimName,
        error: error instanceof Error ? error.message : String(error),
      })
    }
  }
  await persistWorkspaceStore()
  return true
}

async function cleanupWorkspaceInput(input: z.infer<typeof CleanupInput>) {
  await ensureWorkspaceStore()
  const refs = new Set<string>()
  const workspaceRef = input.workspaceRef?.trim() || ""
  const executionId = input.executionId?.trim() || ""
  if (!workspaceRef && !executionId) throw new Error("workspaceRef or executionId is required")
  if (workspaceRef) refs.add(workspaceRef)
  if (executionId) {
    const ref = executionToWorkspace.get(executionId)
    if (ref) refs.add(ref)
  }
  if (!refs.size) return []
  return (
    await Promise.all(
      [...refs].map(async (ref) => {
        if (!(await cleanupWorkspaceRef(ref))) return
        return ref
      }),
    )
  ).filter((value): value is string => Boolean(value))
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

type AgentConfigStoreOverrides = {
  name?: string
  modelSpec?: string
  instructions?: string
  tools?: string[]
  maxTurns?: number
  timeoutMinutes?: number
  role?: string
  goal?: string
  systemPrompt?: string
}

type AgentConfigStoreTarget = {
  storeName: string
  keys: string[]
  metadata: Record<string, string>
  cacheKey: string
}

type AgentConfigStoreSubscription = {
  target: AgentConfigStoreTarget
  overrides?: AgentConfigStoreOverrides
  subscriptionID?: string
  starting?: Promise<void>
}

const DAPR_HTTP_HOST = process.env.DAPR_HOST?.trim() || "127.0.0.1"
const DAPR_HTTP_PORT = process.env.DAPR_HTTP_PORT?.trim() || "3500"
const configStoreSubscriptions = new Map<string, AgentConfigStoreSubscription>()
let configStoreShutdownRegistered = false

type ResolvedAgentConfig = {
  name?: string
  modelSpec?: string
  instructions?: string
  tools?: string[]
  maxTurns?: number
  timeoutMinutes?: number
}

function configString(value: unknown) {
  if (typeof value === "string") {
    const trimmed = value.trim()
    return trimmed || undefined
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value)
  }
  return
}

function configNumber(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) return Math.floor(value)
  if (typeof value === "string") {
    const parsed = Number.parseInt(value.trim(), 10)
    if (Number.isFinite(parsed) && parsed > 0) return parsed
  }
  return
}

function configTools(value: unknown): string[] | undefined {
  if (Array.isArray(value)) {
    const parsed = value
      .filter((item): item is string => typeof item === "string")
      .map((item) => item.trim())
      .filter(Boolean)
    if (!parsed.length) return
    return [...new Set(parsed)]
  }
  if (typeof value === "string") {
    const trimmed = value.trim()
    if (!trimmed) return
    if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
      try {
        return configTools(JSON.parse(trimmed))
      } catch {
        return
      }
    }
    const parsed = trimmed.split(",").map((item) => item.trim()).filter(Boolean)
    if (!parsed.length) return
    return [...new Set(parsed)]
  }
  if (value && typeof value === "object") {
    const parsed = Object.entries(value as Record<string, unknown>)
      .filter(([, enabled]) => enabled === true || enabled === "true")
      .map(([tool]) => tool.trim())
      .filter(Boolean)
    if (!parsed.length) return
    return [...new Set(parsed)]
  }
  return
}

function normalizeConfigKey(key: string) {
  return key.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_")
}

function applyConfigOverride(overrides: AgentConfigStoreOverrides, key: string, value: unknown) {
  const normalized = normalizeConfigKey(key)
  if (normalized === "name" || normalized === "agent_name") {
    const parsed = configString(value)
    if (parsed) overrides.name = parsed
    return
  }
  if (normalized === "model" || normalized === "model_spec" || normalized === "modelspec" || normalized === "llm_model") {
    const parsed = configString(value)
    if (parsed) overrides.modelSpec = parsed
    return
  }
  if (normalized === "instructions" || normalized === "agent_instructions") {
    const parsed = Array.isArray(value)
      ? value.filter((item): item is string => typeof item === "string").map((item) => item.trim()).filter(Boolean).join("\n")
      : configString(value)
    if (parsed) overrides.instructions = parsed
    return
  }
  if (normalized === "system_prompt" || normalized === "agent_system_prompt") {
    const parsed = configString(value)
    if (parsed) overrides.systemPrompt = parsed
    return
  }
  if (normalized === "role" || normalized === "agent_role") {
    const parsed = configString(value)
    if (parsed) overrides.role = parsed
    return
  }
  if (normalized === "goal" || normalized === "agent_goal") {
    const parsed = configString(value)
    if (parsed) overrides.goal = parsed
    return
  }
  if (normalized === "tools" || normalized === "agent_tools") {
    const parsed = configTools(value)
    if (parsed) overrides.tools = parsed
    return
  }
  if (normalized === "max_turns" || normalized === "max_turn" || normalized === "max_iterations" || normalized === "maxturns") {
    const parsed = configNumber(value)
    if (parsed) overrides.maxTurns = parsed
    return
  }
  if (normalized === "timeout_minutes" || normalized === "timeoutminutes") {
    const parsed = configNumber(value)
    if (parsed) overrides.timeoutMinutes = parsed
  }
}

function configStoreInstructions(overrides: AgentConfigStoreOverrides | undefined) {
  if (!overrides) return
  if (overrides.instructions) return overrides.instructions
  const parts = [
    overrides.systemPrompt,
    overrides.role ? `Role: ${overrides.role}` : undefined,
    overrides.goal ? `Goal: ${overrides.goal}` : undefined,
  ].filter((value): value is string => Boolean(value && value.trim()))
  if (!parts.length) return
  return parts.join("\n\n")
}

function createConfigStoreTarget(input: z.infer<typeof RunInput>) {
  const config = input.agentConfig?.configuration
  if (!config?.storeName?.trim()) return
  const storeName = config.storeName.trim()
  const configName = config.configName?.trim()
  const keys = (config.keys ?? []).map((key) => key.trim()).filter(Boolean)
  const metadata = Object.fromEntries(
    Object.entries(config.metadata ?? {})
      .map(([key, value]) => [key.trim(), value.trim()] as const)
      .filter(([key, value]) => Boolean(key) && Boolean(value)),
  )
  const effectiveKeys = keys.length > 0 ? [...new Set(keys)] : configName ? [configName] : []
  return {
    storeName,
    keys: effectiveKeys,
    metadata,
    cacheKey: JSON.stringify({
      storeName,
      keys: [...effectiveKeys].sort(),
      metadata: Object.entries(metadata).sort(([a], [b]) => a.localeCompare(b)),
    }),
  } satisfies AgentConfigStoreTarget
}

function parseConfigStoreOverrides(items: Record<string, { value?: unknown }>) {
  const overrides: AgentConfigStoreOverrides = {}
  for (const [key, item] of Object.entries(items)) {
    const rawValue = item?.value
    if (typeof rawValue !== "string") {
      applyConfigOverride(overrides, key, rawValue)
      continue
    }
    try {
      const parsed = JSON.parse(rawValue)
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        for (const [nestedKey, nestedValue] of Object.entries(parsed)) {
          applyConfigOverride(overrides, nestedKey, nestedValue)
        }
        continue
      }
      applyConfigOverride(overrides, key, parsed)
    } catch {
      applyConfigOverride(overrides, key, rawValue)
    }
  }
  if (!Object.keys(overrides).length) return
  return overrides
}

function normalizeConfigStoreItems(items: unknown) {
  if (Array.isArray(items)) {
    return Object.fromEntries(
      items.flatMap((item) => {
        if (!item || typeof item !== "object" || Array.isArray(item)) return []
        const value = item as Record<string, unknown>
        const key = typeof value.key === "string" ? value.key.trim() : ""
        if (!key) return []
        return [[key, { value: value.value }] as const]
      }),
    )
  }
  if (items && typeof items === "object" && !Array.isArray(items)) {
    const value = items as Record<string, unknown>
    if (typeof value.key === "string") {
      const key = value.key.trim()
      if (!key) return {}
      return { [key]: { value: value.value } }
    }
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => {
        if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
          return [key, { value: entry }] as const
        }
        const item = entry as Record<string, unknown>
        if (!("value" in item)) return [key, { value: entry }] as const
        return [key, { value: item.value }] as const
      }),
    )
  }
  return {}
}

type ConfigStorePushResult = {
  matched: number
  updated: number
}

export function applyConfigStorePush(input: {
  storeName?: string
  key?: string
  payload: unknown
}): ConfigStorePushResult {
  const storeName = input.storeName?.trim()
  const key = input.key?.trim()
  const payload = input.payload && typeof input.payload === "object" && !Array.isArray(input.payload)
    ? input.payload as Record<string, unknown>
    : undefined
  const subscriptionID = typeof payload?.id === "string" ? payload.id.trim() : ""
  const items = normalizeConfigStoreItems(payload?.items ?? payload)
  const overrides = parseConfigStoreOverrides(items)
  if (!overrides) return { matched: 0, updated: 0 }
  let matched = 0
  for (const subscription of configStoreSubscriptions.values()) {
    if (subscriptionID && subscription.subscriptionID && subscription.subscriptionID !== subscriptionID) continue
    if (storeName && subscription.target.storeName !== storeName) continue
    if (key && subscription.target.keys.length > 0 && !subscription.target.keys.includes(key)) continue
    matched += 1
    subscription.overrides = {
      ...(subscription.overrides ?? {}),
      ...overrides,
    }
  }
  if (matched > 0) {
    log.info("received dynamic config update", {
      storeName: storeName || "<unknown>",
      key: key || "<batch>",
      subscriptionID: subscriptionID || "<none>",
      matched,
    })
  }
  return { matched, updated: matched }
}

async function fetchConfigStoreOverrides(target: AgentConfigStoreTarget) {
  const url = new URL(
    `http://${DAPR_HTTP_HOST}:${DAPR_HTTP_PORT}/v1.0/configuration/${encodeURIComponent(target.storeName)}`,
  )
  for (const key of target.keys) {
    url.searchParams.append("key", key)
  }
  for (const [key, value] of Object.entries(target.metadata)) {
    url.searchParams.set(`metadata.${key}`, value)
  }
  const response = await fetch(url.toString(), {
    method: "GET",
    signal: AbortSignal.timeout(5000),
  })
  if (!response.ok) {
    throw new Error(`configuration get failed (${response.status})`)
  }
  const payload = (await response.json()) as
    | { items?: Record<string, { value?: unknown }> }
    | Record<string, { value?: unknown }>
  const items = payload && typeof payload === "object" && "items" in payload && payload.items && typeof payload.items === "object" && !Array.isArray(payload.items)
    ? payload.items as Record<string, { value?: unknown }>
    : (payload as Record<string, { value?: unknown }>)
  return parseConfigStoreOverrides(items)
}

async function subscribeConfigStoreTarget(subscription: AgentConfigStoreSubscription) {
  try {
    subscription.overrides = await fetchConfigStoreOverrides(subscription.target)
  } catch (error: unknown) {
    log.warn("failed initial config load for dapr config subscription", {
      storeName: subscription.target.storeName,
      error: error instanceof Error ? error.message : String(error),
    })
  }
  try {
    const url = new URL(
      `http://${DAPR_HTTP_HOST}:${DAPR_HTTP_PORT}/v1.0/configuration/${encodeURIComponent(subscription.target.storeName)}/subscribe`,
    )
    for (const key of subscription.target.keys) {
      url.searchParams.append("key", key)
    }
    for (const [key, value] of Object.entries(subscription.target.metadata)) {
      url.searchParams.set(`metadata.${key}`, value)
    }
    const response = await fetch(url.toString(), {
      method: "GET",
      signal: AbortSignal.timeout(5000),
    })
    if (!response.ok) {
      throw new Error(`configuration subscribe failed (${response.status})`)
    }
    const payload = (await response.json()) as { id?: unknown }
    const subscriptionID = typeof payload.id === "string" ? payload.id.trim() : ""
    if (!subscriptionID) {
      throw new Error("configuration subscribe returned empty id")
    }
    subscription.subscriptionID = subscriptionID
    log.info("subscribed to dapr config store", {
      storeName: subscription.target.storeName,
      keyCount: subscription.target.keys.length,
      subscriptionID,
    })
  } catch (error: unknown) {
    log.warn("failed subscribing to dapr config store", {
      storeName: subscription.target.storeName,
      error: error instanceof Error ? error.message : String(error),
    })
  }
}

function ensureConfigStoreSubscription(target: AgentConfigStoreTarget) {
  let subscription = configStoreSubscriptions.get(target.cacheKey)
  if (!subscription) {
    subscription = { target }
    configStoreSubscriptions.set(target.cacheKey, subscription)
  }
  if (!subscription.subscriptionID && !subscription.starting) {
    subscription.starting = subscribeConfigStoreTarget(subscription).finally(() => {
      if (subscription) subscription.starting = undefined
    })
  }
  return subscription
}

async function unsubscribeConfigStoreTarget(subscription: AgentConfigStoreSubscription) {
  if (!subscription.subscriptionID) return
  const encodedStore = encodeURIComponent(subscription.target.storeName)
  const encodedID = encodeURIComponent(subscription.subscriptionID)
  const urls = [
    `http://${DAPR_HTTP_HOST}:${DAPR_HTTP_PORT}/v1.0/configuration/${encodedStore}/${encodedID}/unsubscribe`,
    `http://${DAPR_HTTP_HOST}:${DAPR_HTTP_PORT}/v1.0-alpha1/configuration/${encodedStore}/${encodedID}/unsubscribe`,
  ]
  let lastError = "unknown error"
  for (const url of urls) {
    const response = await fetch(url, {
      method: "GET",
      signal: AbortSignal.timeout(5000),
    })
    if (response.ok) return
    lastError = `${response.status}`
  }
  throw new Error(`configuration unsubscribe failed (${lastError})`)
}

async function stopConfigStoreSubscriptions() {
  const stops = [...configStoreSubscriptions.values()].flatMap((subscription) => {
    if (!subscription.subscriptionID) return []
    return [
      Promise.resolve(unsubscribeConfigStoreTarget(subscription)).catch((error: unknown) => {
        log.warn("failed stopping config subscription", {
          storeName: subscription.target.storeName,
          error: error instanceof Error ? error.message : String(error),
        })
      }),
    ]
  })
  configStoreSubscriptions.clear()
  await Promise.all(stops)
}

function registerConfigStoreShutdown() {
  if (configStoreShutdownRegistered) return
  configStoreShutdownRegistered = true
  const close = () => {
    void stopConfigStoreSubscriptions()
  }
  process.once("SIGINT", close)
  process.once("SIGTERM", close)
  process.once("beforeExit", close)
}

async function loadConfigStoreAgentOverrides(input: z.infer<typeof RunInput>) {
  const target = createConfigStoreTarget(input)
  if (!target) return
  try {
    const subscription = ensureConfigStoreSubscription(target)
    if (subscription.overrides !== undefined) {
      return subscription.overrides
    }
    const overrides = await fetchConfigStoreOverrides(target)
    subscription.overrides = overrides
    return overrides
  } catch (error: unknown) {
    log.warn("failed loading agent config from dapr config store", {
      storeName: target.storeName,
      error: error instanceof Error ? error.message : String(error),
    })
    return
  }
}

async function resolveAgentConfig(input: z.infer<typeof RunInput>, inlineName: string): Promise<ResolvedAgentConfig> {
  const requestConfig = input.agentConfig
  const configStore = await loadConfigStoreAgentOverrides(input)
  const inlineModel = typeof input.model === "string" ? input.model.trim() : ""
  const inlineInstructions = typeof input.instructions === "string" ? input.instructions : undefined
  const inlineTools = configTools(input.tools)
  const modelSpec = requestConfig?.modelSpec?.trim() || configStore?.modelSpec?.trim() || inlineModel || undefined
  const name = requestConfig?.name?.trim() || configStore?.name?.trim() || (modelSpec ? inlineName : undefined)
  const tools = requestConfig?.tools?.length
    ? [...new Set(requestConfig.tools.map((tool) => tool.trim()).filter(Boolean))]
    : configStore?.tools ?? inlineTools
  const instructions = requestConfig?.instructions ?? configStoreInstructions(configStore) ?? inlineInstructions
  return {
    name,
    modelSpec,
    instructions,
    maxTurns: requestConfig?.maxTurns ?? configStore?.maxTurns,
    timeoutMinutes: requestConfig?.timeoutMinutes ?? configStore?.timeoutMinutes,
    tools,
  }
}

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

function parseOptionalBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value
  if (typeof value !== "string") return undefined
  const normalized = value.trim().toLowerCase()
  if (normalized === "true") return true
  if (normalized === "false") return false
  return undefined
}

function parseRunExecutionMode(input: z.infer<typeof RunInput>, forced?: z.infer<typeof RunExecutionMode>) {
  if (forced) return forced
  return input.executionMode === "sandboxed" ? "sandboxed" : "legacy"
}

function parseRunHardTimeoutMinutes(input: z.infer<typeof RunInput>, config?: ResolvedAgentConfig) {
  const candidate = input.hardTimeoutMinutes ?? input.timeoutMinutes ?? config?.timeoutMinutes ?? input.agentConfig?.timeoutMinutes ?? 20
  const parsed = Number.parseInt(`${candidate}`, 10)
  if (!Number.isFinite(parsed)) return 20
  return Math.min(Math.max(parsed, 1), 20)
}

async function localDirectoryExists(value: string) {
  const stat = await fsStat(value).catch(() => undefined)
  if (!stat) return false
  return stat.isDirectory()
}

async function preflightSandboxedRun(input: DurableRunPayload) {
  if (input.executionMode !== "sandboxed") return
  const workspaceRef = input.workspaceRef?.trim() || ""
  const executionId = input.executionID?.trim() || ""
  const dbExecutionId = input.dbExecutionID?.trim() || ""
  if (!workspaceRef && !executionId && !dbExecutionId) {
    throw new Error("sandboxed durable run requires workspaceRef or executionId")
  }
  const session = await resolveWorkspaceFromInput({
    workspaceRef: workspaceRef || undefined,
    executionId: executionId || undefined,
    dbExecutionId: dbExecutionId || undefined,
    durableInstanceId: input.workflowID,
  })
  await bindWorkspaceDurableInstance(session, input.workflowID)
  const clonePath = session.clonePath?.trim() || ""
  if (!clonePath) {
    throw new Error(`workspace ${session.workspaceRef} has no clone path. Run workspace/clone before durable/run.`)
  }
  const sandboxPath = await executeSandboxCommand({
    session,
    command: `test -d ${shellEscape(clonePath)}`,
    cwd: session.rootPath,
    timeoutMs: Math.min(session.commandTimeoutMs, 15000),
  })
  if (!sandboxPath.success) {
    throw new Error(`workspace clone path is unavailable in sandbox: ${clonePath}`)
  }
  input.cwd = clonePath
  if (input.tools) {
    const allowed = new Set<WorkspaceTool>(workspaceTools)
    input.tools = Object.fromEntries(
      Object.entries(input.tools).flatMap(([tool, enabled]) =>
        enabled && allowed.has(tool as WorkspaceTool) ? [[tool, true] as const] : [],
      ),
    )
  }
  return session
}

async function preflightRunCwd(input: DurableRunPayload) {
  const cwd = input.cwd?.trim()
  if (!cwd) throw new Error("durable run requires cwd")
  if (`${input.executionMode ?? ""}` === "sandboxed") return
  if (await localDirectoryExists(cwd)) return
  const hint = input.executionMode === "sandboxed"
    ? "The workspace is available in the Kubernetes sandbox but not mounted in durable-agent."
    : "Ensure workspace/clone completed and cwd points to a local directory."
  throw new Error(`durable run cwd is not accessible locally: ${cwd}. ${hint}`)
}

async function resolveTools(input: z.infer<typeof RunInput>, config?: ResolvedAgentConfig) {
  const parsed = config?.tools?.length
    ? Object.fromEntries(config.tools.map((tool) => [tool, true]))
    : parseTools(input)
  if (!parsed) return undefined
  return Object.fromEntries(
    Object.entries(parsed).flatMap(([tool, enabled]) => (enabled ? [[tool, true] as const] : [])),
  )
}

async function workspaceHasGitMutations(input: { workspaceRef?: string; executionId?: string }) {
  const workspaceRef = input.workspaceRef?.trim() || ""
  const executionId = input.executionId?.trim() || ""
  if (!workspaceRef && !executionId) return false
  let session: WorkspaceSession
  try {
    session = await resolveWorkspaceFromInput({
      workspaceRef: workspaceRef || undefined,
      executionId: executionId || undefined,
    })
  } catch {
    return false
  }
  const result = await executeSandboxCommand({
    session,
    command: "if [ -d .git ]; then git status --porcelain; else true; fi",
    cwd: session.clonePath ?? session.rootPath,
    timeoutMs: Math.min(session.commandTimeoutMs, 15000),
  })
  if (!result.success) return false
  return Boolean(result.stdout.trim())
}

const opus46Aliases = new Set([
  "anthropic/claude-opus-4-6",
  "anthropic/claude-opus-4.6",
  "claude-opus-4-6",
  "claude-opus-4.6",
  "anthropicclaudeopus4.6",
  "anthropicclaudeopus4-6",
])

const opusModelPreferences = [
  "claude-opus-4-6",
  "claude-opus-4.6",
  "claude-opus-4-5",
  "claude-opus-4.5",
  "claude-opus-4-1",
  "claude-opus-4.1",
  "claude-opus-4",
]

const opusProviderOrder = [
  "anthropic",
  "google-vertex-anthropic",
  "openrouter",
]

const opusModelNeedles = [
  "claude-opus-4-6",
  "claude-opus-4.6",
  "claude-opus-4",
  "claude-4-opus",
]

function normalizeModelInput(input: string) {
  const trimmed = input.trim()
  if (!trimmed) return ""
  const compact = trimmed.replace(/\s+/g, "").toLowerCase()
  if (compact.includes("claude") && compact.includes("opus") && compact.includes("4.6")) {
    return "anthropic/claude-opus-4.6"
  }
  return trimmed.toLowerCase()
}

function findModelID(models: Record<string, unknown>, values: string[]) {
  const ids = Object.keys(models)
  for (const value of values) {
    const exact = ids.find((id) => id.toLowerCase() === value)
    if (exact) return exact
  }
  for (const value of values) {
    const partial = ids.find((id) => id.toLowerCase().includes(value))
    if (partial) return partial
  }
}

async function resolveOpusModel(): Promise<ModelRef> {
  const providers = await Provider.list()
  const providerIDs = [...new Set([...opusProviderOrder, ...Object.keys(providers)])]
  for (const modelID of opusModelPreferences.map((value) => value.toLowerCase())) {
    for (const providerID of providerIDs) {
      const provider = providers[providerID]
      if (!provider) continue
      const resolved = findModelID(provider.models, [modelID])
      if (!resolved) continue
      return { providerID, modelID: resolved }
    }
  }
  for (const providerID of providerIDs) {
    const provider = providers[providerID]
    if (!provider) continue
    const resolved = findModelID(provider.models, opusModelNeedles)
    if (!resolved) continue
    return { providerID, modelID: resolved }
  }
  const connected = Object.keys(providers)
  throw new Error(
    `invalid model: claude-opus-4.6 is not available from connected providers (${connected.join(
      ", ",
    ) || "none"}). Configure ANTHROPIC_API_KEY or OPENROUTER_API_KEY for durable-agent.`,
  )
}

async function resolveModel(input: z.infer<typeof RunInput>, config?: ResolvedAgentConfig) {
  const raw = (config?.modelSpec ?? input.agentConfig?.modelSpec ?? input.model)?.trim()
  if (!raw) return undefined
  const normalized = normalizeModelInput(raw)
  if (opus46Aliases.has(normalized)) {
    const resolved = await resolveOpusModel()
    log.info("resolved claude opus model alias", {
      requested: raw,
      resolved: `${resolved.providerID}/${resolved.modelID}`,
    })
    return resolved
  }
  const parsed = Provider.parseModel(raw)
  try {
    await Provider.getModel(parsed.providerID, parsed.modelID)
    return parsed
  } catch (error: unknown) {
    if (Provider.ModelNotFoundError.isInstance(error)) {
      const suggestions =
        error.data.suggestions && error.data.suggestions.length > 0
          ? ` Suggestions: ${error.data.suggestions.join(", ")}`
          : ""
      throw new Error(`invalid model: ${raw}.${suggestions}`)
    }
    throw error
  }
}

async function resolveAgentModel(agent: NonNullable<Awaited<ReturnType<typeof Agent.get>>>) {
  const configured = agent.model
  if (!configured) return await Provider.defaultModel()
  const provider = await Provider.getProvider(configured.providerID)
  if (provider?.models[configured.modelID]) return configured
  const configuredSpec = `${configured.providerID}/${configured.modelID}`
  const normalized = normalizeModelInput(configuredSpec)
  if (opus46Aliases.has(normalized)) {
    const resolved = await resolveOpusModel()
    log.warn("agent model unavailable, using opus fallback", {
      agent: agent.name,
      configured: configuredSpec,
      resolved: `${resolved.providerID}/${resolved.modelID}`,
    })
    return resolved
  }
  const connected = await Provider.list().then((items) => Object.keys(items))
  throw new Error(
    `invalid model: ${configuredSpec} is not available from connected providers (${connected.join(
      ", ",
    ) || "none"}). Configure provider credentials or override model per run.`,
  )
}

async function runPrompt(input: {
  prompt: string
  cwd?: string
  agent?: string
  model?: ModelRef
  tools?: Record<string, boolean>
  instructions?: string
  hardTimeoutMinutes?: number
  workspaceSession?: WorkspaceSession
}) {
  const contextCwd = input.cwd?.trim() || (input.workspaceSession ? process.cwd() : undefined)
  return await withDir(contextCwd, async () => {
    const tools = input.workspaceSession
      ? {
          ...(input.tools ?? {}),
          read: true,
          list: true,
          write: true,
          edit: true,
          bash: true,
          apply_patch: false,
          [SessionPrompt.SANDBOX_WORKSPACE_TOOL_MODE_KEY]: true,
        }
      : input.tools
    if (input.workspaceSession) {
      await registerSandboxWorkspaceTools(input.workspaceSession)
    }
    // Durable runs must not load workspace plugin dependencies from .opencode.
    process.env.OPENCODE_DISABLE_DEFAULT_PLUGINS = "true"
    process.env.OPENCODE_DISABLE_PROJECT_CONFIG = "true"
    const requested = input.agent?.trim()
    let agentName = requested || (await Agent.defaultAgent())
    let agent = await Agent.get(agentName)
    if (!agent && requested) {
      const fallback = await Agent.defaultAgent()
      if (fallback !== agentName) {
        agentName = fallback
        agent = await Agent.get(agentName)
      }
    }
    if (!agent && agentName !== "build") {
      agentName = "build"
      agent = await Agent.get(agentName)
    }
    if (!agent) {
      throw new Error(`Agent "${agentName}" not found`)
    }
    const model = input.model ?? (await resolveAgentModel(agent))
    const sessionID = (await Session.create({ title: `Durable ${agentName}` })).id
    const prompt = SessionPrompt.prompt({
      sessionID,
      agent: agentName,
      model,
      tools,
      system: input.instructions,
      parts: [
        {
          type: "text",
          text: input.prompt,
        },
      ],
    })
    const timeoutMinutes = Number.parseInt(`${input.hardTimeoutMinutes ?? 0}`, 10)
    if (!Number.isFinite(timeoutMinutes) || timeoutMinutes <= 0) {
      return await prompt
    }
    const timeoutMs = timeoutMinutes * 60 * 1000
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<MessageV2.WithParts>((_, reject) => {
      timer = setTimeout(() => {
        SessionPrompt.cancel(sessionID)
        reject(new Error(`durable run timed out after ${timeoutMinutes} minutes`))
      }, timeoutMs)
    })
    try {
      return await Promise.race([prompt, timeout])
    } finally {
      if (timer) clearTimeout(timer)
    }
  })
}

function isInputValidationError(message: string) {
  return message.startsWith("invalid model:") || message.startsWith("invalid tools:")
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
  executionID?: string
  dbExecutionID?: string
  workflowDefinitionID?: string
  nodeID?: string
  nodeName?: string
  workspaceRef?: string
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
      parentExecutionId: parent,
      executionId: input.executionID,
      dbExecutionId: input.dbExecutionID,
      workflowId: input.workflowDefinitionID,
      nodeId: input.nodeID,
      nodeName: input.nodeName,
      workspaceRef: input.workspaceRef,
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
  try {
    const sandboxSession = await preflightSandboxedRun(input)
    await preflightRunCwd(input)
    const msg = await runPrompt({
      prompt: input.prompt,
      cwd: input.cwd,
      model: input.model,
      tools: input.tools,
      instructions: input.instructions,
      agent: input.agent,
      hardTimeoutMinutes: input.hardTimeoutMinutes,
      workspaceSession: sandboxSession,
    })
    return {
      success: true,
      workflow_id: input.workflowID,
      result: toResult(msg),
    } satisfies DurableRunResult
  } catch (error: unknown) {
    return {
      success: false,
      workflow_id: input.workflowID,
      error: error instanceof Error ? error.message : String(error),
    } satisfies DurableRunResult
  }
}

function sandboxRelativePath(session: WorkspaceSession, full: string) {
  const base = session.clonePath ?? session.rootPath
  const normalized = normalizePosixPath(full)
  if (containsPosixPath(base, normalized)) {
    const rel = path.posix.relative(base, normalized)
    return rel || "."
  }
  return path.posix.basename(normalized) || normalized
}

async function registerSandboxWorkspaceTools(session: WorkspaceSession) {
  const read = Tool.define("read", {
    description: "Read files and directories from the active sandbox workspace.",
    parameters: z.object({
      filePath: z.string().describe("Absolute or workspace-relative path to file or directory"),
      offset: z.coerce.number().int().positive().optional(),
      limit: z.coerce.number().int().positive().optional(),
    }),
    async execute(params, ctx) {
      const inputPath = params.filePath?.trim() || "."
      const fullPath = normalizePosixPath(path.posix.resolve(session.rootPath, inputPath))
      await ctx.ask({
        permission: "read",
        patterns: [fullPath],
        always: ["*"],
        metadata: {},
      })
      const checkDir = await executeSandboxCommand({
        session,
        command: `test -d ${shellEscape(fullPath)}`,
        cwd: session.rootPath,
        timeoutMs: Math.min(session.commandTimeoutMs, 15000),
      })
      if (checkDir.success) {
        const listed = await runWorkspaceFileOperation({
          workspaceRef: session.workspaceRef,
          operation: "list",
          path: fullPath,
        })
        const entries = (listed.files ?? []).map((entry) =>
          entry.type === "directory" ? `${entry.name}/` : entry.name,
        )
        return {
          title: sandboxRelativePath(session, fullPath),
          metadata: { count: entries.length, truncated: false },
          output: entries.length ? entries.join("\n") : "(empty directory)",
        }
      }
      const readResult = await runWorkspaceFileOperation({
        workspaceRef: session.workspaceRef,
        operation: "read",
        path: fullPath,
      })
      const lines = (readResult.content ?? "").split("\n")
      const offset = params.offset ?? 1
      const limit = params.limit ?? 2000
      const start = Math.max(offset - 1, 0)
      const sliced = lines.slice(start, start + limit)
      const rendered = sliced.map((line, idx) => `${start + idx + 1}: ${line}`).join("\n")
      return {
        title: sandboxRelativePath(session, fullPath),
        metadata: { count: sliced.length, truncated: start + sliced.length < lines.length },
        output: rendered || "(empty file)",
      }
    },
  })

  const list = Tool.define("list", {
    description: "List files and directories in the active sandbox workspace.",
    parameters: z.object({
      path: z.string().optional(),
      ignore: z.array(z.string()).optional(),
    }),
    async execute(params, ctx) {
      const inputPath = params.path?.trim() || "."
      const fullPath = normalizePosixPath(path.posix.resolve(session.rootPath, inputPath))
      await ctx.ask({
        permission: "list",
        patterns: [fullPath],
        always: ["*"],
        metadata: {},
      })
      const listed = await runWorkspaceFileOperation({
        workspaceRef: session.workspaceRef,
        operation: "list",
        path: fullPath,
      })
      const entries = (listed.files ?? []).map((entry) =>
        entry.type === "directory" ? `${entry.name}/` : entry.name,
      )
      return {
        title: sandboxRelativePath(session, fullPath),
        metadata: { count: entries.length, truncated: false },
        output: entries.length ? entries.join("\n") : "(empty directory)",
      }
    },
  })

  const write = Tool.define("write", {
    description: "Write file content into the active sandbox workspace.",
    parameters: z.object({
      content: z.string(),
      filePath: z.string(),
    }),
    async execute(params, ctx) {
      const fullPath = normalizePosixPath(path.posix.resolve(session.rootPath, params.filePath))
      await ctx.ask({
        permission: "edit",
        patterns: [fullPath],
        always: ["*"],
        metadata: {},
      })
      const result = await runWorkspaceFileOperation({
        workspaceRef: session.workspaceRef,
        operation: "write",
        path: fullPath,
        content: params.content,
      })
      return {
        title: sandboxRelativePath(session, fullPath),
        metadata: { changeSetId: result.changeSetId },
        output: "Wrote file successfully.",
      }
    },
  })

  const edit = Tool.define("edit", {
    description: "Edit file content in the active sandbox workspace.",
    parameters: z.object({
      filePath: z.string(),
      oldString: z.string(),
      newString: z.string(),
      replaceAll: z.boolean().optional(),
    }),
    async execute(params, ctx) {
      const fullPath = normalizePosixPath(path.posix.resolve(session.rootPath, params.filePath))
      await ctx.ask({
        permission: "edit",
        patterns: [fullPath],
        always: ["*"],
        metadata: {},
      })
      if (params.replaceAll) {
        const read = await runWorkspaceFileOperation({
          workspaceRef: session.workspaceRef,
          operation: "read",
          path: fullPath,
        })
        const current = read.content ?? ""
        if (!current.includes(params.oldString)) {
          throw new Error(`oldString not found in ${fullPath}`)
        }
        const updated = current.split(params.oldString).join(params.newString)
        const writeResult = await runWorkspaceFileOperation({
          workspaceRef: session.workspaceRef,
          operation: "write",
          path: fullPath,
          content: updated,
        })
        return {
          title: sandboxRelativePath(session, fullPath),
          metadata: { changeSetId: writeResult.changeSetId },
          output: "Edit applied successfully.",
        }
      }
      const result = await runWorkspaceFileOperation({
        workspaceRef: session.workspaceRef,
        operation: "edit",
        path: fullPath,
        old_string: params.oldString,
        new_string: params.newString,
      })
      return {
        title: sandboxRelativePath(session, fullPath),
        metadata: { changeSetId: result.changeSetId },
        output: "Edit applied successfully.",
      }
    },
  })

  const bash = Tool.define("bash", {
    description: "Run shell commands in the active sandbox workspace.",
    parameters: z.object({
      command: z.string(),
      timeout: z.number().int().positive().optional(),
      workdir: z.string().optional(),
      description: z.string().optional(),
    }),
    async execute(params, ctx) {
      await ctx.ask({
        permission: "bash",
        patterns: [params.command],
        always: ["*"],
        metadata: {},
      })
      const workdir = params.workdir?.trim()
        ? normalizePosixPath(path.posix.resolve(session.rootPath, params.workdir.trim()))
        : session.clonePath ?? session.rootPath
      const wrapped = `cd ${shellEscape(workdir)} && ${params.command}`
      const result = await runWorkspaceCommand({
        workspaceRef: session.workspaceRef,
        command: wrapped,
        timeoutMs: params.timeout,
      })
      const output = [result.stdout, result.stderr].filter(Boolean).join(result.stdout && result.stderr ? "\n" : "")
      return {
        title: params.description?.trim() || "sandbox command",
        metadata: {
          exit: result.exitCode,
          timedOut: result.timedOut,
          executionTimeMs: result.executionTimeMs,
        },
        output: output || "(no output)",
      }
    },
  })

  await ToolRegistry.register(read)
  await ToolRegistry.register(list)
  await ToolRegistry.register(write)
  await ToolRegistry.register(edit)
  await ToolRegistry.register(bash)
}

async function durablePublishCompletionActivity(
  _ctx: WorkflowActivityContext,
  input: {
    workflowID: string
    parentExecutionID?: string
    executionID?: string
    dbExecutionID?: string
    workflowDefinitionID?: string
    nodeID?: string
    nodeName?: string
    workspaceRef?: string
    success: boolean
    result?: Record<string, unknown>
    error?: string
  },
) {
  await publishCompletion(input)
}

async function durablePlanActivity(_ctx: WorkflowActivityContext, input: DurablePlanPayload): Promise<DurablePlanResult> {
  const workspaceRef = input.workspaceRef?.trim() || ""
  const executionID = input.executionID?.trim() || ""
  const dbExecutionID = input.dbExecutionID?.trim() || ""
  const workspaceSession = workspaceRef || executionID || dbExecutionID
    ? await resolveWorkspaceFromInput({
        workspaceRef: workspaceRef || undefined,
        executionId: executionID || undefined,
        dbExecutionId: dbExecutionID || undefined,
        durableInstanceId: input.workflowID,
      })
    : undefined
  if (workspaceSession) {
    await bindWorkspaceDurableInstance(workspaceSession, input.workflowID)
    const clonePath = workspaceSession.clonePath?.trim() || ""
    if (clonePath) {
      const sandboxPath = await executeSandboxCommand({
        session: workspaceSession,
        command: `test -d ${shellEscape(clonePath)}`,
        cwd: workspaceSession.rootPath,
        timeoutMs: Math.min(workspaceSession.commandTimeoutMs, 15000),
      })
      if (!sandboxPath.success) {
        throw new Error(`workspace clone path is unavailable in sandbox: ${clonePath}`)
      }
    }
  }
  const requestedCwd = input.cwd?.trim() || ""
  let localCwd = requestedCwd || undefined
  if (requestedCwd && !(await localDirectoryExists(requestedCwd))) {
    if (workspaceSession) {
      log.warn("plan cwd is not accessible locally; using sandbox workspace tools without local cwd", {
        workflowID: input.workflowID,
        cwd: requestedCwd,
        workspaceRef: workspaceSession.workspaceRef,
      })
      localCwd = process.cwd()
    } else {
      throw new Error(`durable plan cwd is not accessible locally: ${requestedCwd}`)
    }
  }
  const msg = await runPrompt({
    prompt: input.prompt,
    cwd: localCwd,
    model: input.model,
    tools: input.tools,
    instructions: input.instructions,
    agent: input.agent ?? "plan",
    hardTimeoutMinutes: input.hardTimeoutMinutes,
    workspaceSession,
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
    const outcome = (yield ctx.callActivity(durableRunActivity, input)) as DurableRunResult
    if (!outcome.success) {
      const error = outcome.error || "durable run activity failed"
      yield ctx.callActivity(durablePublishCompletionActivity, {
        workflowID: input.workflowID,
        parentExecutionID: input.parentExecutionID,
        executionID: input.executionID,
        dbExecutionID: input.dbExecutionID,
        workflowDefinitionID: input.workflowDefinitionID,
        nodeID: input.nodeID,
        nodeName: input.nodeName,
        workspaceRef: input.workspaceRef,
        success: false,
        error,
      })
      return {
        success: false,
        workflow_id: input.workflowID,
        error,
      }
    }
    const result = outcome.result ?? {}
    yield ctx.callActivity(durablePublishCompletionActivity, {
      workflowID: input.workflowID,
      parentExecutionID: input.parentExecutionID,
      executionID: input.executionID,
      dbExecutionID: input.dbExecutionID,
      workflowDefinitionID: input.workflowDefinitionID,
      nodeID: input.nodeID,
      nodeName: input.nodeName,
      workspaceRef: input.workspaceRef,
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
      executionID: input.executionID,
      dbExecutionID: input.dbExecutionID,
      workflowDefinitionID: input.workflowDefinitionID,
      nodeID: input.nodeID,
      nodeName: input.nodeName,
      workspaceRef: input.workspaceRef,
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

async function executeDurableRunRequest(
  body: z.infer<typeof RunInput>,
  forcedMode?: z.infer<typeof RunExecutionMode>,
): Promise<{
  statusCode: 200 | 400 | 422 | 500 | 503 | 504
  payload: Record<string, unknown>
}> {
  const prompt = body.prompt?.trim() ?? ""
  if (!prompt) {
    return {
      statusCode: 400,
      payload: {
        success: false,
        error: "prompt is required",
      },
    }
  }
  try {
    const id = rid("durable-run")
    const waitForCompletion = parseOptionalBoolean(body.waitForCompletion) ?? false
    const requireFileChanges = parseOptionalBoolean(body.requireFileChanges) ?? false
    const resolvedAgentConfig = await resolveAgentConfig(body, "inline-agent")
    const hardTimeoutMinutes = parseRunHardTimeoutMinutes(body, resolvedAgentConfig)
    const workflowInput: DurableRunPayload = {
      workflowID: id,
      parentExecutionID: body.parentExecutionId?.trim() || "",
      executionID: body.executionId?.trim() || body.parentExecutionId?.trim() || "",
      dbExecutionID: body.dbExecutionId?.trim() || "",
      workflowDefinitionID: body.workflowId?.trim() || "",
      nodeID: body.nodeId?.trim() || "",
      nodeName: body.nodeName?.trim() || "",
      workspaceRef: body.workspaceRef?.trim() || "",
      prompt,
      cwd: body.cwd?.trim() || Instance.directory,
      agent: resolvedAgentConfig.name || "build",
      model: await resolveModel(body, resolvedAgentConfig),
      tools: await resolveTools(body, resolvedAgentConfig),
      instructions: resolvedAgentConfig.instructions ?? undefined,
      executionMode: parseRunExecutionMode(body, forcedMode),
      hardTimeoutMinutes,
    }
    const instanceID = await withWorkflowClient((client) =>
      client.scheduleNewWorkflow(durableRunWorkflow, workflowInput, id),
    )
    if (!waitForCompletion) {
      return {
        statusCode: 200,
        payload: {
          success: true,
          workflow_id: id,
          dapr_instance_id: instanceID,
        },
      }
    }
    const timeoutSeconds = Math.min(Math.max(hardTimeoutMinutes * 60 + 30, 90), 3600)
    const state = await withWorkflowClient((client) =>
      client.waitForWorkflowCompletion(instanceID, true, timeoutSeconds),
    )
    if (!state) {
      return {
        statusCode: 504,
        payload: {
          success: false,
          workflow_id: id,
          dapr_instance_id: instanceID,
          error: "execution timed out before workflow state was available",
        },
      }
    }
    const typed = state as unknown as WorkflowStateLike
    const output = parseWorkflowOutput(typed.serializedOutput)
    if (typed.runtimeStatus !== WORKFLOW_COMPLETED || output?.success === false) {
      return {
        statusCode: 500,
        payload: {
          success: false,
          workflow_id: id,
          dapr_instance_id: instanceID,
          error: workflowFailure(typed, output),
        },
      }
    }
    if (requireFileChanges) {
      const changed = await workspaceHasGitMutations({
        workspaceRef: workflowInput.workspaceRef,
        executionId: workflowInput.executionID || workflowInput.dbExecutionID,
      })
      if (!changed) {
        return {
          statusCode: 422,
          payload: {
            success: false,
            workflow_id: id,
            dapr_instance_id: instanceID,
            error: "Execution required file changes but repository is unchanged.",
          },
        }
      }
    }
    return {
      statusCode: 200,
      payload: {
        success: true,
        workflow_id: id,
        dapr_instance_id: instanceID,
        result: output?.result ?? output,
      },
    }
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    return {
      statusCode: isInputValidationError(message) ? 400 : 503,
      payload: {
        success: false,
        error: isInputValidationError(message) ? message : `durable runtime unavailable: ${message}`,
      },
    }
  }
}

scheduleRuntimeBootstrap()
registerConfigStoreShutdown()

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
    .get(
      "/tools",
      describeRoute({
        summary: "List durable workspace tools",
        operationId: "durable.tools",
        responses: {
          200: {
            description: "available tools",
            content: {
              "application/json": {
                schema: resolver(ToolsResponse),
              },
            },
          },
        },
      }),
      async (c) => {
        return c.json({
          success: true as const,
          tools: workspaceTools.map((id) => ({
            id,
            description: workspaceToolDescriptions[id as WorkspaceTool],
          })),
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
        const result = await executeDurableRunRequest(body)
        c.status(result.statusCode)
        return c.json(result.payload)
      },
    )
    .post(
      "/run-sandboxed",
      describeRoute({
        summary: "Start durable run with sandbox workspace preflight",
        operationId: "durable.runSandboxed",
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
        const result = await executeDurableRunRequest(body, "sandboxed")
        c.status(result.statusCode)
        return c.json(result.payload)
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
          const resolvedAgentConfig = await resolveAgentConfig(body, "inline-execute-plan-agent")
          const sandboxedExecution =
            body.workspaceRef?.trim() || body.executionId?.trim() || body.dbExecutionId?.trim()
          const workflowInput: DurableRunPayload = {
            workflowID: id,
            parentExecutionID: body.parentExecutionId?.trim() || "",
            executionID: body.executionId?.trim() || body.parentExecutionId?.trim() || "",
            dbExecutionID: body.dbExecutionId?.trim() || "",
            workflowDefinitionID: body.workflowId?.trim() || "",
            nodeID: body.nodeId?.trim() || "",
            nodeName: body.nodeName?.trim() || "",
            workspaceRef: body.workspaceRef?.trim() || "",
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: resolvedAgentConfig.name || "build",
            model: await resolveModel(body, resolvedAgentConfig),
            tools: await resolveTools(body, resolvedAgentConfig),
            instructions: resolvedAgentConfig.instructions ?? undefined,
            executionMode: sandboxedExecution ? "sandboxed" : parseRunExecutionMode(body),
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
          c.status(isInputValidationError(message) ? 400 : 503)
          return c.json({
            success: false,
            error: isInputValidationError(message) ? message : `durable runtime unavailable: ${message}`,
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
          const resolvedAgentConfig = await resolveAgentConfig(body, "inline-plan-agent")
          const timeoutMinutes = resolvedAgentConfig.timeoutMinutes
            ?? body.timeoutMinutes
            ?? body.agentConfig?.timeoutMinutes
            ?? 10
          const timeoutSeconds = Math.min(Math.max(timeoutMinutes * 60 + 30, 90), 3600)
          const workflowInput: DurablePlanPayload = {
            workflowID: id,
            prompt,
            executionID: body.executionId?.trim() || body.parentExecutionId?.trim() || "",
            dbExecutionID: body.dbExecutionId?.trim() || "",
            workspaceRef: body.workspaceRef?.trim() || "",
            cwd: body.cwd?.trim() || Instance.directory,
            agent: resolvedAgentConfig.name || "plan",
            model: await resolveModel(body, resolvedAgentConfig),
            tools: await resolveTools(body, resolvedAgentConfig),
            instructions: resolvedAgentConfig.instructions ?? undefined,
            hardTimeoutMinutes: timeoutMinutes,
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
          c.status(isInputValidationError(message) ? 400 : 503)
          return c.json({
            success: false,
            error: isInputValidationError(message) ? message : `durable runtime unavailable: ${message}`,
          })
        }
      },
    )
    .post(
      "/run/:workflowID/terminate",
      describeRoute({
        summary: "Terminate durable-compatible run",
        operationId: "durable.runTerminate",
        responses: {
          200: {
            description: "run terminated",
            content: {
              "application/json": {
                schema: resolver(z.object({
                  success: z.boolean(),
                  workflow_id: z.string(),
                  cleanedWorkspace: z.boolean().optional(),
                })),
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
        const workflowID = c.req.valid("param").workflowID.trim()
        if (!workflowID) {
          c.status(400)
          return c.json({
            success: false,
            workflow_id: "",
            error: "workflowID is required",
          })
        }
        const body = await c.req.json().catch(() => ({} as Record<string, unknown>))
        const reason = typeof body.reason === "string" && body.reason.trim()
          ? body.reason.trim()
          : "terminated via durable-agent API"
        try {
          await withWorkflowClient((client) => client.terminateWorkflow(workflowID, reason))
          const workspaceRef = durableToWorkspace.get(workflowID)
          const cleanedWorkspace = workspaceRef ? await cleanupWorkspaceRef(workspaceRef) : false
          return c.json({
            success: true,
            workflow_id: workflowID,
            cleanedWorkspace,
          })
        } catch (error: unknown) {
          c.status(503)
          return c.json({
            success: false,
            workflow_id: workflowID,
            error: error instanceof Error ? error.message : String(error),
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
            success: result.success,
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
            success: result.success,
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
    .get(
      "/workspaces/changes/:changeSetId",
      describeRoute({
        summary: "Get workspace change artifact",
        operationId: "durable.workspaceChangeArtifact",
        responses: {
          200: {
            description: "change artifact",
            content: {
              "application/json": {
                schema: resolver(WorkspaceChangeArtifactResponse),
              },
            },
          },
        },
      }),
      validator(
        "param",
        z.object({
          changeSetId: z.string(),
        }),
      ),
      async (c) => {
        const changeSetId = c.req.valid("param").changeSetId.trim()
        if (!changeSetId) {
          c.status(400)
          return c.json({
            success: false,
            error: "changeSetId is required",
          })
        }
        const artifact = await readWorkspaceChangeArtifact(changeSetId)
        if (!artifact) {
          c.status(404)
          return c.json({
            success: false,
            error: "Change artifact not found",
          })
        }
        return c.json({
          success: true,
          executionId: artifact.metadata.executionId,
          metadata: artifact.metadata,
          patch: artifact.patch,
        })
      },
    )
    .get(
      "/workspaces/executions/:executionId/changes",
      describeRoute({
        summary: "List change artifacts for execution",
        operationId: "durable.executionChanges",
        responses: {
          200: {
            description: "execution changes",
            content: {
              "application/json": {
                schema: resolver(WorkspaceChangesResponse),
              },
            },
          },
        },
      }),
      validator(
        "param",
        z.object({
          executionId: z.string(),
        }),
      ),
      async (c) => {
        const executionId = c.req.valid("param").executionId.trim()
        if (!executionId) {
          c.status(400)
          return c.json({
            success: false,
            error: "executionId is required",
          })
        }
        const changes = await listWorkspaceExecutionChanges({
          executionId,
          includeExcluded: true,
        })
        return c.json({
          success: true,
          executionId,
          count: changes.length,
          changes: changes.map((artifact) => artifact.metadata),
        })
      },
    )
    .get(
      "/workspaces/executions/:executionId/patch",
      describeRoute({
        summary: "Get combined execution patch",
        operationId: "durable.executionPatch",
        responses: {
          200: {
            description: "execution patch",
            content: {
              "application/json": {
                schema: resolver(WorkspaceExecutionPatchResponse),
              },
            },
          },
        },
      }),
      validator(
        "param",
        z.object({
          executionId: z.string(),
        }),
      ),
      async (c) => {
        const executionId = c.req.valid("param").executionId.trim()
        if (!executionId) {
          c.status(400)
          return c.json({
            success: false,
            error: "executionId is required",
          })
        }
        const durableInstanceId = c.req.query("durableInstanceId")?.trim()
        const includeExcluded = parseWorkspaceBoolean(c.req.query("includeExcluded"))
        const combined = await executionPatch({
          executionId,
          durableInstanceId: durableInstanceId || undefined,
          includeExcluded,
        })
        if (c.req.query("format") === "raw") {
          return new Response(combined.patch, {
            status: 200,
            headers: {
              "Content-Type": "text/plain; charset=utf-8",
            },
          })
        }
        return c.json({
          success: true,
          executionId,
          durableInstanceId: durableInstanceId || undefined,
          patch: combined.patch,
          changeSets: combined.changeSets,
        })
      },
    )
    .get(
      "/workspaces/executions/:executionId/files/snapshot",
      describeRoute({
        summary: "Get file snapshot for execution",
        operationId: "durable.executionFileSnapshot",
        responses: {
          200: {
            description: "file snapshot",
            content: {
              "application/json": {
                schema: resolver(WorkspaceFileSnapshotResponse),
              },
            },
          },
        },
      }),
      validator(
        "param",
        z.object({
          executionId: z.string(),
        }),
      ),
      async (c) => {
        const executionId = c.req.valid("param").executionId.trim()
        if (!executionId) {
          c.status(400)
          return c.json({
            success: false,
            error: "executionId is required",
          })
        }
        const filePath = c.req.query("path")?.trim() || ""
        if (!filePath) {
          c.status(400)
          return c.json({
            success: false,
            error: "path is required",
          })
        }
        const durableInstanceId = c.req.query("durableInstanceId")?.trim()
        const snapshot = await executionFileSnapshot({
          executionId,
          path: filePath,
          durableInstanceId: durableInstanceId || undefined,
        })
        if (!snapshot) {
          c.status(404)
          return c.json({
            success: false,
            error: "File snapshot not found for execution",
          })
        }
        return c.json({
          success: true,
          executionId,
          path: filePath,
          durableInstanceId: durableInstanceId || undefined,
          snapshot,
        })
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
