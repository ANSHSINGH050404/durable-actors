import { stat } from "node:fs/promises"
import { type Socket, createConnection } from "node:net"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { z } from "zod"

import type { ActorIdentity } from "../actor/identity.js"
import type { SocketEffect, SocketLookup, SocketQuery } from "../actor/socketProtocol.js"
import { ActorConfigurationError, ActorProtocolError, ActorSessionError } from "../errors.js"

import { parseActorSessionServerMessage } from "./protocol.js"
import type { ActorExecutorCommand, ActorExecutorReply, ActorSessionClientMessage } from "./protocol.js"
import type { ActorCommandHandler, ActorHostSettings, ActorWorkerSupervisorFactory } from "./types.js"
import { ActorWorkerSupervisor } from "./worker-supervisor.js"

async function runActorHost(): Promise<never> {
    const session = new ActorSession()
    await session.start()

    await session.waitUntilDisconnected()
    throw new ActorSessionError("Rust host disconnected from actor session")
}

class ActorSession {
    private startup: Promise<void> | undefined
    private connection: ActorSessionConnection | undefined

    constructor(
        private readonly settings: ActorHostSettings = parseHostSettings(process.env),
        private readonly createSupervisor: ActorWorkerSupervisorFactory = options => new ActorWorkerSupervisor(options),
        private readonly socket?: Socket
    ) {}

    start(): Promise<void> {
        this.startup ??= this.initialize()
        return this.startup
    }

    waitUntilDisconnected(): Promise<void> {
        if (this.connection === undefined) throw new ActorSessionError("actor session has not started")
        return this.connection.closed()
    }

    private async initialize(): Promise<void> {
        const actorEntrypointUrl = await resolveActorEntrypoint(this.settings.actorEntrypoint)
        const supervisor = this.createSupervisor({ actorEntrypointUrl })
        const commandHandler: ActorCommandHandler = (command, allowNextInvocation, publish, connections) =>
            supervisor.handle(command, allowNextInvocation, publish, connections)
        try {
            const actorNames = await discoverActorNames(supervisor, this.settings.startupTimeoutMs)
            this.connection = await ActorSessionConnection.open(
                this.settings.socketPath,
                actorNames,
                commandHandler,
                this.settings.startupTimeoutMs,
                supervisor.activeActors.bind(supervisor),
                supervisor.onActiveActorsChange.bind(supervisor),
                this.socket
            )
            void this.connection.closed().then(() => supervisor.close())
        } catch (error) {
            supervisor.close()
            throw error
        }
    }
}

async function discoverActorNames(
    supervisor: Pick<ActorWorkerSupervisor, "ready">,
    timeoutMs: number
): Promise<readonly string[]> {
    let timer: NodeJS.Timeout | undefined
    try {
        return await Promise.race([
            supervisor.ready(),
            new Promise<never>((_, reject) => {
                timer = setTimeout(
                    () => reject(new ActorSessionError(`actor module loading timed out after ${timeoutMs}ms`)),
                    timeoutMs
                )
            })
        ])
    } finally {
        clearTimeout(timer)
    }
}

class ActorSessionConnection {
    private unsubscribeActivity: (() => void) | undefined
    private activityTimer: NodeJS.Timeout | undefined
    private buffer = ""
    private attachedResolve: (() => void) | undefined
    private attachedReject: ((error: Error) => void) | undefined
    private readonly attachedPromise: Promise<void>
    private closedResolve: (() => void) | undefined
    private readonly closedPromise: Promise<void>
    private readonly publishing = new Map<number, { resolve: () => void; reject: (error: Error) => void }>()
    private readonly loadingConnections = new Map<
        number,
        { resolve: (connections: SocketLookup) => void; reject: (error: Error) => void }
    >()

    static async open(
        socketPath: string,
        actorNames: readonly string[],
        commandHandler: ActorCommandHandler,
        timeoutMs: number,
        activeActors: () => readonly ActorIdentity[],
        watchActiveActors: (listener: () => void) => () => void,
        connectedSocket?: Socket
    ): Promise<ActorSessionConnection> {
        if (actorNames.length === 0)
            throw new ActorSessionError("the actor entrypoint does not export any actor classes")
        const socket = connectedSocket ?? (await connectSocket(socketPath))
        const connection = new ActorSessionConnection(socket, commandHandler, activeActors, watchActiveActors)
        connection.send({
            type: "attach",
            protocol: 23,
            actor_names: actorNames
        })
        await connection.waitUntilAttached(timeoutMs)
        return connection
    }

    closed(): Promise<void> {
        return this.closedPromise
    }

    private constructor(
        private readonly socket: Socket,
        private readonly commandHandler: ActorCommandHandler,
        private readonly activeActors: () => readonly ActorIdentity[],
        private readonly watchActiveActors: (listener: () => void) => () => void
    ) {
        this.attachedPromise = new Promise<void>((resolve, reject) => {
            this.attachedResolve = resolve
            this.attachedReject = reject
        })
        this.closedPromise = new Promise<void>(resolve => {
            this.closedResolve = resolve
        })
        this.bindSocket()
    }

    private waitUntilAttached(timeoutMs: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => {
                const error = new ActorSessionError(`actor session attachment timed out after ${timeoutMs}ms`)
                this.fail(error)
                reject(error)
            }, timeoutMs)
            void this.attachedPromise.then(
                () => {
                    clearTimeout(timeout)
                    resolve()
                },
                error => {
                    clearTimeout(timeout)
                    reject(error)
                }
            )
        })
    }

    private bindSocket(): void {
        this.socket.setEncoding("utf8")
        this.socket.on("data", (chunk: string) => this.acceptChunk(chunk))
        this.socket.once("error", error => this.fail(error))
        this.socket.once("close", () => this.close())
    }

    private acceptChunk(chunk: string): void {
        this.buffer += chunk
        let newline = this.buffer.indexOf("\n")
        while (newline !== -1) {
            const rawMessage = this.buffer.slice(0, newline)
            this.buffer = this.buffer.slice(newline + 1)
            void this.handle(rawMessage)
            newline = this.buffer.indexOf("\n")
        }
    }

    private async handle(rawMessage: string): Promise<void> {
        try {
            const message = parseActorSessionServerMessage(rawMessage)
            switch (message.type) {
                case "attached":
                    if (message.supports_residency && !this.activityTimer) {
                        const report = () => {
                            try {
                                this.send({ type: "residency", actors: this.activeActors() })
                            } catch (error) {
                                this.fail(sessionError(error))
                            }
                        }
                        this.unsubscribeActivity = this.watchActiveActors(report)
                        report()
                        this.activityTimer = setInterval(report, 1_000)
                        this.activityTimer.unref()
                    }
                    this.attachedResolve?.()
                    this.attachedResolve = undefined
                    this.attachedReject = undefined
                    break
                case "command":
                    await this.reply(
                        message.message_id,
                        await this.commandHandler(
                            message.command,
                            () => this.send({ type: "ready_for_invocation", message_id: message.message_id }),
                            effects => this.publish(message.message_id, effects),
                            query => this.getConnections(message.message_id, query)
                        )
                    )
                    break
                case "socket_connections": {
                    const pending = this.loadingConnections.get(message.message_id)
                    if (pending === undefined)
                        throw new ActorProtocolError("Rust host replied to unknown connection lookup")
                    this.loadingConnections.delete(message.message_id)
                    if (message.error === undefined) pending.resolve(message.connections)
                    else pending.reject(new ActorSessionError(message.error))
                    break
                }
                case "socket_effects_published": {
                    const pending = this.publishing.get(message.message_id)
                    if (pending === undefined)
                        throw new ActorProtocolError("Rust host acknowledged unknown socket output")
                    this.publishing.delete(message.message_id)
                    if (message.error === undefined) pending.resolve()
                    else pending.reject(new ActorSessionError(message.error))
                    break
                }
                default:
                    throw message satisfies never
            }
        } catch (error) {
            this.fail(sessionError(error))
        }
    }

    private publish(messageId: number, effects: readonly SocketEffect[]): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.publishing.has(messageId))
                throw new ActorProtocolError("actor socket output is already being published")
            this.publishing.set(messageId, { resolve, reject })
            try {
                this.send({ type: "socket_effects", message_id: messageId, effects })
            } catch (error) {
                this.publishing.delete(messageId)
                reject(sessionError(error))
            }
        })
    }

    private getConnections(messageId: number, query?: SocketQuery): Promise<SocketLookup> {
        return new Promise((resolve, reject) => {
            if (this.loadingConnections.has(messageId))
                throw new ActorProtocolError("actor connections are already being loaded")
            this.loadingConnections.set(messageId, { resolve, reject })
            try {
                this.send({ type: "get_connections", message_id: messageId, query })
            } catch (error) {
                this.loadingConnections.delete(messageId)
                reject(sessionError(error))
            }
        })
    }

    private reply(messageId: number, reply: ActorExecutorReply): void {
        this.send({ type: "reply", message_id: messageId, reply })
    }

    private send(message: ActorSessionClientMessage): void {
        this.socket.write(`${JSON.stringify(message)}\n`)
    }

    private fail(error: Error): void {
        this.attachedReject?.(error)
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.socket.destroy()
    }

    private close(): void {
        clearInterval(this.activityTimer)
        this.unsubscribeActivity?.()
        this.unsubscribeActivity = undefined
        for (const pending of this.loadingConnections.values())
            pending.reject(new ActorSessionError("Rust host disconnected while loading connections"))
        this.loadingConnections.clear()
        for (const pending of this.publishing.values())
            pending.reject(new ActorSessionError("Rust host disconnected while publishing socket output"))
        this.publishing.clear()
        this.attachedReject?.(new ActorSessionError("Rust host disconnected from actor session"))
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.closedResolve?.()
        this.closedResolve = undefined
    }
}

function connectSocket(socketPath: string): Promise<Socket> {
    return new Promise((resolve, reject) => {
        const socket = createConnection(socketPath)
        const onError = (error: Error): void => {
            socket.off("connect", onConnect)
            socket.destroy()
            reject(new ActorSessionError(`could not attach to Rust host at ${socketPath}`, { cause: error }))
        }
        const onConnect = (): void => {
            socket.off("error", onError)
            resolve(socket)
        }
        socket.once("error", onError)
        socket.once("connect", onConnect)
    })
}

function sessionError(error: unknown): Error {
    return error instanceof Error ? error : new ActorSessionError(String(error))
}

async function resolveActorEntrypoint(configured: string | undefined): Promise<string> {
    const entrypointPath = path.resolve(configured ?? DEFAULT_ACTOR_ENTRYPOINT)
    await requireFile(
        entrypointPath,
        configured === undefined
            ? `default actor entrypoint ${DEFAULT_ACTOR_ENTRYPOINT}`
            : `configured actor entrypoint ${configured}`
    )
    return pathToFileURL(entrypointPath).href
}

async function requireFile(filePath: string, label: string): Promise<void> {
    if (!(await isFile(filePath))) throw new ActorConfigurationError(`${label} is not a file`)
}

async function isFile(filePath: string): Promise<boolean> {
    try {
        return (await stat(filePath)).isFile()
    } catch {
        return false
    }
}

function parseHostSettings(environment: NodeJS.ProcessEnv): ActorHostSettings {
    const result = actorSessionSettingsSchema.safeParse(environment)
    if (!result.success)
        throw new ActorConfigurationError(`actor-host session settings are invalid: ${result.error.message}`)
    return {
        socketPath: result.data.DURABLE_ACTORS_EXECUTOR_SOCKET,
        actorEntrypoint: result.data.DURABLE_ACTORS_ENTRYPOINT,
        startupTimeoutMs: parseStartupTimeout(environment.DURABLE_ACTORS_HOST_STARTUP_MS)
    }
}

function parseStartupTimeout(value: string | undefined): number {
    if (value === undefined) return DEFAULT_ACTOR_STARTUP_TIMEOUT_MS
    const parsed = Number(value)
    if (!Number.isInteger(parsed) || parsed <= 0)
        throw new ActorConfigurationError("DURABLE_ACTORS_HOST_STARTUP_MS must be a positive integer")
    return parsed
}

const DEFAULT_ACTOR_STARTUP_TIMEOUT_MS = 10_000

const actorSessionSettingsSchema = z.object({
    DURABLE_ACTORS_EXECUTOR_SOCKET: z.string().trim().min(1),
    DURABLE_ACTORS_ENTRYPOINT: z.string().trim().min(1).optional()
})

const DEFAULT_ACTOR_ENTRYPOINT = "dist/actors.mjs"

export { ActorSession, connectSocket, parseHostSettings, resolveActorEntrypoint, runActorHost }
