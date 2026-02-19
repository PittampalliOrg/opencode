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

const CleanupInput = z.object({
  executionId: z.string().optional(),
  dbExecutionId: z.string().optional(),
})

const CleanupResponse = z.object({
  success: z.boolean(),
  cleaned: z.boolean(),
  executionId: z.string().optional(),
  dbExecutionId: z.string().optional(),
})

const UnsupportedWorkspaceResponse = z.object({
  success: z.literal(false),
  error: z.string(),
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

let durableRuntime: WorkflowRuntime | undefined
let durableClient: DaprWorkflowClient | undefined
let durableStarting: Promise<void> | undefined

function rid(prefix: string) {
  return `${prefix}-${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`
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
        summary: "Workspace profile (unsupported)",
        operationId: "durable.workspaceProfile",
        responses: {
          501: {
            description: "unsupported",
            content: {
              "application/json": {
                schema: resolver(UnsupportedWorkspaceResponse),
              },
            },
          },
        },
      }),
      validator("json", z.any()),
      async (c) => {
        c.status(501)
        return c.json({
          success: false as const,
          error: "workspace profile is not implemented in opencode-durable-agent",
        })
      },
    )
    .post(
      "/workspaces/clone",
      describeRoute({
        summary: "Workspace clone (unsupported)",
        operationId: "durable.workspaceClone",
        responses: {
          501: {
            description: "unsupported",
            content: {
              "application/json": {
                schema: resolver(UnsupportedWorkspaceResponse),
              },
            },
          },
        },
      }),
      validator("json", z.any()),
      async (c) => {
        c.status(501)
        return c.json({
          success: false as const,
          error: "workspace clone is not implemented in opencode-durable-agent",
        })
      },
    )
    .post(
      "/workspaces/command",
      describeRoute({
        summary: "Workspace command (unsupported)",
        operationId: "durable.workspaceCommand",
        responses: {
          501: {
            description: "unsupported",
            content: {
              "application/json": {
                schema: resolver(UnsupportedWorkspaceResponse),
              },
            },
          },
        },
      }),
      validator("json", z.any()),
      async (c) => {
        c.status(501)
        return c.json({
          success: false as const,
          error: "workspace command is not implemented in opencode-durable-agent",
        })
      },
    )
    .post(
      "/workspaces/file",
      describeRoute({
        summary: "Workspace file operations (unsupported)",
        operationId: "durable.workspaceFile",
        responses: {
          501: {
            description: "unsupported",
            content: {
              "application/json": {
                schema: resolver(UnsupportedWorkspaceResponse),
              },
            },
          },
        },
      }),
      validator("json", z.any()),
      async (c) => {
        c.status(501)
        return c.json({
          success: false as const,
          error: "workspace file operations are not implemented in opencode-durable-agent",
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
        return c.json({
          success: true,
          cleaned: true,
          executionId: body.executionId,
          dbExecutionId: body.dbExecutionId,
        })
      },
    ),
)
