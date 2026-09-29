import { AsyncLocalStorage } from "node:async_hooks"
import { parentPort, workerData } from "node:worker_threads"

import { Actor, findActorDefinition, registerActorClass } from "../actor/actor.js"
import type { ActorClass } from "../actor/actor.js"
import { ACTOR_ARTIFACT_VERSION } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"
import type { SocketConnection, SocketEffect } from "../actor/socketProtocol.js"
import { ActorConfigurationError, ActorDefinitionError, errorMessage } from "../errors.js"

import { ActorRuntime } from "./actor-runtime.js"
import { failedReply } from "./protocol.js"
import type { ActorWorkerData, ActorWorkerMessage, ActorWorkerRequest } from "./protocol.js"

const port = parentPort
if (port === null) throw new Error("actor Worker requires a parent message port")

let exiting = false
// Bun 1.3 can drain microtasks after process.exit().
process.once("exit", () => {
    exiting = true
})

let assigned = false
const invocation = new AsyncLocalStorage<number>()
const publishing = new Map<number, { resolve: () => void; reject: (error: Error) => void }>()
const loadingConnections = new Map<
    number,
    { resolve: (connections: readonly SocketConnection[]) => void; reject: (error: Error) => void }
>()
if (workerData !== undefined && workerData !== null) void initialize(workerData as ActorWorkerData)
else {
    post({ type: "warm" })
    port.once("message", (message: ActorWorkerRequest) => {
        if (message.type !== "load") throw new Error("generic executor requires a code assignment")
        void initialize(message.data)
    })
}

async function initialize(data: ActorWorkerData): Promise<void> {
    try {
        if (assigned) throw new Error("customer code already assigned")
        assigned = true
        Object.assign(process.env, data.environment)
        const actorNames = await loadActorEntrypoint(data.moduleUrl)
        let runtime: ActorRuntime | undefined

        port!.on("message", (message: ActorWorkerRequest) => {
            if (message.type === "load") throw new Error("customer code already assigned")
            if (message.type === "socket_connections") {
                const pending = loadingConnections.get(message.messageId)
                loadingConnections.delete(message.messageId)
                if (message.error === undefined) pending?.resolve(message.connections)
                else pending?.reject(new Error(message.error))
                return
            }
            if (message.type === "socket_effects_published") {
                const pending = publishing.get(message.messageId)
                publishing.delete(message.messageId)
                if (message.error === undefined) pending?.resolve()
                else pending?.reject(new Error(message.error))
                return
            }
            const definition = findActorDefinition(message.command.actor.actor_name)
            if (definition === undefined) {
                post({
                    type: "reply",
                    messageId: message.messageId,
                    reply: failedReply(
                        "actor_name_not_found",
                        `actor entrypoint ${data.moduleUrl} does not export ${message.command.actor.actor_name}`
                    )
                })
                return
            }
            runtime ??= new ActorRuntime(definition, allowNextInvocation, publish, getConnections)
            invocation.run(message.messageId, () => {
                void runtime!.handle(message.command).then(
                    reply => post({ type: "reply", messageId: message.messageId, reply }),
                    error =>
                        post({
                            type: "reply",
                            messageId: message.messageId,
                            reply: failedReply("actor_worker_failed", errorMessage(error))
                        })
                )
            })
        })
        post({ type: "ready", actorNames })
    } catch (error) {
        post(failedReply("actor_worker_failed", errorMessage(error)))
    }
}

function post(message: ActorWorkerMessage): void {
    if (!exiting) port!.postMessage(message)
}

function allowNextInvocation(): void {
    const messageId = invocation.getStore()
    if (messageId === undefined) throw new Error("invocation admission has no active invocation")
    post({ type: "ready_for_invocation", messageId })
}

function publish(effects: readonly SocketEffect[]): Promise<void> {
    return new Promise((resolve, reject) => {
        const messageId = invocation.getStore()
        if (messageId === undefined) throw new Error("socket output has no active invocation")
        if (publishing.has(messageId)) throw new Error("actor socket output is already being published")
        publishing.set(messageId, { resolve, reject })
        post({ type: "socket_effects", messageId, effects })
    })
}

function getConnections(): Promise<readonly SocketConnection[]> {
    return new Promise((resolve, reject) => {
        const messageId = invocation.getStore()
        if (messageId === undefined) throw new Error("connection lookup has no active invocation")
        if (loadingConnections.has(messageId)) throw new Error("actor connections are already being loaded")
        loadingConnections.set(messageId, { resolve, reject })
        post({ type: "get_connections", messageId })
    })
}

async function loadActorEntrypoint(moduleUrl: string): Promise<string[]> {
    const artifact = await import(moduleUrl)
    if (artifact.version !== ACTOR_ARTIFACT_VERSION || !Array.isArray(artifact.schemas) || !artifact.actors)
        throw new ActorConfigurationError("invalid actor artifact; redeploy using matching SDK and runtime versions")
    return registerActors(artifact.actors, artifact.schemas)
}

function registerActors(actorModule: Record<string, unknown>, schemas: readonly ActorSchema[]): string[] {
    const actorNames: string[] = []
    for (const [exportName, value] of Object.entries(actorModule)) {
        if (!isActorClass(value)) continue
        if (exportName === "default") {
            throw new ActorDefinitionError("actor entrypoint must use named exports, not a default export")
        }
        if (Object.getPrototypeOf(value.prototype) !== Actor.prototype) {
            throw new ActorDefinitionError(
                `actor entrypoint export ${exportName} must be a class that directly extends Actor`
            )
        }
        if (value.name !== exportName) {
            throw new ActorDefinitionError(`actor entrypoint export ${exportName} must have the same class name`)
        }
        const schema = schemas.find(schema => schema.actorName === exportName)
        if (schema === undefined)
            throw new ActorDefinitionError(`actor ${exportName} has no validated schema; restart the actor host`)
        actorNames.push(registerActorClass(value, schema).actorName)
    }
    if (actorNames.length === 0) throw new ActorDefinitionError("actor entrypoint has no named actor exports")
    if (actorNames.length !== schemas.length)
        throw new ActorDefinitionError("actor exports do not match validated schemas; restart the actor host")
    actorNames.sort()
    return actorNames
}

function isActorClass(value: unknown): value is ActorClass {
    return typeof value === "function" && value.prototype instanceof Actor
}
