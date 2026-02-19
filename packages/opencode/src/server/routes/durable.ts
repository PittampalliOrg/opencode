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
import { ToolRegistry } from "@/tool/registry"
import { Filesystem } from "@/util/filesystem"
import { lazy } from "@/util/lazy"
import { Log } from "@/util/log"
import path from "path"
import { request as httpsRequest } from "node:https"
import { existsSync, readFileSync } from "node:fs"

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
  })
  .partial()

const RunInput = z.object({
  prompt: z.string().nullable().optional(),
  model: z.string().nullable().optional(),
  tools: z.union([z.array(z.string()), z.record(z.string(), z.boolean()), z.string()]).nullable().optional(),
  instructions: z.string().nullable().optional(),
  maxTurns: z.coerce.number().int().positive().nullable().optional(),
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
  if (!Array.isArray(input)) throw new Error("enabledTools must be an array of read, write, edit, list, bash")
  return [...new Set(input.map((item) => {
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
  const clonePath = normalizePosixPath(path.posix.resolve(session.rootPath, cloneDir))
  if (!containsPosixPath(session.rootPath, clonePath)) {
    throw new Error("targetDir must stay inside workspace root")
  }
  const token = input.repositoryToken?.trim() || input.githubToken?.trim() || ""
  const repositoryURL = token
    ? `https://${token}@github.com/${repositoryOwner}/${repositoryRepo}.git`
    : `https://github.com/${repositoryOwner}/${repositoryRepo}.git`

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
    const sanitized = token ? clone.stderr.replaceAll(token, "***") : clone.stderr
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
  const result = await executeSandboxCommand({
    session,
    command,
    cwd,
    timeoutMs,
  })
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
    await writeWorkspaceFile(session, fullPath, input.content ?? "")
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { path: input.path ?? "" }
  }

  if (operation === "edit") {
    const fullPath = resolveWorkspacePath(session, input.path, operation)
    assertCloneScope(session, fullPath, operation)
    await enforceReadBeforeWrite(session, fullPath)
    const oldString = input.old_string ?? ""
    if (!oldString) throw new Error("old_string is required for edit")
    const current = await readWorkspaceFile(session, fullPath)
    if (!current.includes(oldString)) throw new Error(`old_string not found in ${input.path}`)
    await writeWorkspaceFile(session, fullPath, current.replace(oldString, input.new_string ?? ""))
    touchWorkspace(session)
    await persistWorkspaceStore()
    return { path: input.path ?? "" }
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

async function resolveTools(input: z.infer<typeof RunInput>) {
  const parsed = parseTools(input)
  if (!parsed) return undefined
  const available = new Set(await ToolRegistry.ids())
  const unknown = Object.keys(parsed).filter((tool) => !available.has(tool))
  if (unknown.length > 0) {
    throw new Error(`invalid tools: ${unknown.join(", ")}`)
  }
  return parsed
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

function normalizeModelInput(input: string) {
  const trimmed = input.trim()
  if (!trimmed) return ""
  const compact = trimmed.replace(/\s+/g, "").toLowerCase()
  if (compact.includes("claude") && compact.includes("opus") && compact.includes("4.6")) {
    return "anthropic/claude-opus-4.6"
  }
  return trimmed.toLowerCase()
}

async function resolveOpusModel(): Promise<ModelRef> {
  const provider = await Provider.getProvider("anthropic")
  if (!provider) {
    throw new Error("invalid model: anthropic provider is not configured")
  }
  for (const modelID of opusModelPreferences) {
    if (provider.models[modelID]) return { providerID: "anthropic", modelID }
  }
  const candidates = Object.keys(provider.models).filter((modelID) => modelID.includes("claude-opus-4"))
  if (candidates.length > 0) return { providerID: "anthropic", modelID: candidates[0]! }
  throw new Error("invalid model: anthropic provider has no claude opus model available")
}

async function resolveModel(input: z.infer<typeof RunInput>) {
  const raw = (input.agentConfig?.modelSpec ?? input.model)?.trim()
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
            executionID: body.executionId?.trim() || "",
            dbExecutionID: body.dbExecutionId?.trim() || "",
            workflowDefinitionID: body.workflowId?.trim() || "",
            nodeID: body.nodeId?.trim() || "",
            nodeName: body.nodeName?.trim() || "",
            workspaceRef: body.workspaceRef?.trim() || "",
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: body.agentConfig?.name?.trim() || "build",
            model: await resolveModel(body),
            tools: await resolveTools(body),
            instructions: body.agentConfig?.instructions ?? body.instructions ?? undefined,
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
            executionID: body.executionId?.trim() || "",
            dbExecutionID: body.dbExecutionId?.trim() || "",
            workflowDefinitionID: body.workflowId?.trim() || "",
            nodeID: body.nodeId?.trim() || "",
            nodeName: body.nodeName?.trim() || "",
            workspaceRef: body.workspaceRef?.trim() || "",
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: body.agentConfig?.name?.trim() || "build",
            model: await resolveModel(body),
            tools: await resolveTools(body),
            instructions: body.agentConfig?.instructions ?? body.instructions ?? undefined,
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
          const timeoutMinutes = body.agentConfig?.timeoutMinutes ?? 10
          const timeoutSeconds = Math.min(Math.max(timeoutMinutes * 60 + 30, 90), 3600)
          const workflowInput: DurablePlanPayload = {
            workflowID: id,
            prompt,
            cwd: body.cwd?.trim() || Instance.directory,
            agent: body.agentConfig?.name?.trim() || "plan",
            model: await resolveModel(body),
            tools: await resolveTools(body),
            instructions: body.agentConfig?.instructions ?? body.instructions ?? undefined,
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
