import { Worker } from "node:worker_threads"

import type { ActorIdentity } from "../actor/identity.js"
import { actorKey } from "../actor/identity.js"
import type { SocketQuery } from "../actor/socketProtocol.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import { errorMessage } from "../errors.js"

import { failedReply } from "./protocol.js"
import type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorWorkerData,
    ActorWorkerMessage,
    ActorWorkerRequest,
    EvictCommand,
    HydrateCommand,
    InvokeCommand,
    WebSocketEventCommand
} from "./protocol.js"
import type {
    ActorWorkerFactory,
    ActorWorkerHandle,
    ActorWorkerState,
    ActorWorkerSupervisorOptions,
    ResidentActorWorkerOptions,
    SocketPublisher,
    SocketSource
} from "./types.js"

class ActorWorkerSupervisor {
    private readonly actorEntrypointUrl: string
    private readonly createWorker: ActorWorkerFactory
    private resident: ResidentActorWorker | undefined
    private lastActiveActors = "[]"
    private readonly activeActorListeners = new Set<() => void>()
    private speculativeWorker: ActorWorkerHandle | undefined
    private identity: string | undefined
    private closed = false
    private actorNames: readonly string[] | undefined
    private lastSequence = 0

    constructor(options: ActorWorkerSupervisorOptions) {
        this.actorEntrypointUrl = options.actorEntrypointUrl
        this.createWorker = options.createWorker ?? ((data, onStateChange) => new ActorWorker(data, onStateChange))
        this.preload()
    }

    async ready(): Promise<readonly string[]> {
        if (this.closed) throw new Error("actor supervisor is closed")
        if (this.actorNames !== undefined) return this.actorNames
        const worker = this.speculativeWorker ?? this.preload()
        this.actorNames = await worker.ready()
        return this.actorNames
    }

    async handle(
        command: ActorExecutorCommand,
        allowNextInvocation: () => void,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.closed) return failedReply("actor_worker_terminated", "actor supervisor is closed")
        const identity = actorKey(command.actor)
        if (this.identity !== undefined && this.identity !== identity)
            return failedReply("actor_identity_mismatch", "sandbox is permanently assigned to another actor")
        if (command.type !== "evict") this.identity ??= identity
        switch (command.type) {
            case "hydrate":
            case "invoke":
            case "websocket_event":
                try {
                    if (this.actorNames === undefined) await this.ready()
                    return await this.execute(command, allowNextInvocation, publish, connections)
                } catch (error) {
                    return failedReply("actor_worker_failed", errorMessage(error))
                }
            case "evict":
                return this.evict(command)
            default:
                throw command satisfies never
        }
    }

    onActiveActorsChange(listener: () => void): () => void {
        this.activeActorListeners.add(listener)
        return () => {
            this.activeActorListeners.delete(listener)
        }
    }

    private notifyActiveActorsChange(): void {
        const current = JSON.stringify(this.activeActors())
        if (current === this.lastActiveActors) return
        this.lastActiveActors = current
        for (const listener of this.activeActorListeners) listener()
    }

    activeActors(): readonly ActorIdentity[] {
        return this.resident?.isActive() ? [this.resident.identity] : []
    }

    close(): void {
        this.closed = true
        this.takeSpeculativeWorker()?.terminate("actor supervisor is closed")
        this.resident?.terminate("actor supervisor is closed")
        this.resident = undefined
    }

    private preload(): ActorWorkerHandle {
        const worker = this.createWorker({ moduleUrl: this.actorEntrypointUrl }, () => this.notifyActiveActorsChange())
        this.speculativeWorker = worker
        void worker.ready().catch(() => this.discardPreload(worker))
        return worker
    }

    private discardPreload(worker: ActorWorkerHandle): void {
        if (this.speculativeWorker !== worker) return
        this.takeSpeculativeWorker()?.terminate("actor preload failed")
    }

    private execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
        allowNextInvocation: () => void,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (!this.actorNames?.includes(command.actor.actor_name)) {
            return Promise.resolve(
                failedReply(
                    "actor_name_not_found",
                    `actor name ${command.actor.actor_name} is not loaded in this customer process`
                )
            )
        }
        let actor = this.resident
        if (actor === undefined) {
            if (command.resident_only) return Promise.resolve({ type: "state_required" })
            actor = new ResidentActorWorker({
                identity: command.actor,
                sequenceBase: this.lastSequence,
                moduleUrl: this.actorEntrypointUrl,
                worker: this.takeSpeculativeWorker(),
                createWorker: this.createWorker,
                onActiveActorsChange: () => this.notifyActiveActorsChange()
            })
            this.resident = actor
        }
        const base = actor.sequenceBase
        return actor.execute(command, allowNextInvocation, publish, connections).then(reply => {
            if ((reply.type === "invoked" || reply.type === "websocket_handled") && reply.sequence !== undefined) {
                const sequence = base + reply.sequence
                this.lastSequence = Math.max(this.lastSequence, sequence)
                return { ...reply, sequence }
            }
            return reply
        })
    }

    private evict(_command: EvictCommand): ActorExecutorReply {
        this.resident?.terminate("resident actor was evicted by the Rust host")
        this.resident = undefined
        return { type: "evicted" }
    }

    private takeSpeculativeWorker(): ActorWorkerHandle | undefined {
        const worker = this.speculativeWorker
        this.speculativeWorker = undefined
        return worker
    }
}

class ResidentActorWorker {
    readonly identity: ActorIdentity
    readonly sequenceBase: number
    readonly moduleUrl: string
    readonly createWorker: ActorWorkerFactory
    private readonly onActiveActorsChange: () => void
    private worker: ActorWorkerHandle | undefined

    constructor(options: ResidentActorWorkerOptions) {
        this.identity = { ...options.identity }
        this.sequenceBase = options.sequenceBase
        this.moduleUrl = options.moduleUrl
        this.createWorker = options.createWorker
        this.onActiveActorsChange = options.onActiveActorsChange
        this.worker = options.worker
    }

    async execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
        allowNextInvocation: () => void,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.worker === undefined && command.resident_only) return { type: "state_required" }
        this.worker ??= this.createWorker({ moduleUrl: this.moduleUrl }, this.onActiveActorsChange)
        const worker = this.worker
        this.onActiveActorsChange()
        let reply: ActorExecutorReply
        try {
            reply = await worker.execute(command, allowNextInvocation, publish, connections)
        } catch (error) {
            reply = failedReply(
                error instanceof ActorWorkerTerminatedError ? "actor_worker_terminated" : "actor_worker_failed",
                errorMessage(error)
            )
        }
        if (this.worker !== worker)
            return failedReply("actor_worker_terminated", "resident actor was terminated during invocation")
        if (
            reply.type === "failed" &&
            !["actor_method_failed", "actor_socket_failed", "method_not_found", "method_not_callable"].includes(
                reply.code
            )
        ) {
            worker.terminate("actor invocation failed")
            this.worker = undefined
            this.onActiveActorsChange()
        }
        return reply
    }

    isActive(): boolean {
        return this.worker?.state === "ready"
    }

    terminate(reason: string): void {
        this.worker?.terminate(reason)
        this.worker = undefined
        this.onActiveActorsChange()
    }
}

class ActorWorker implements ActorWorkerHandle {
    private readonly worker: Worker
    private readonly readyPromise: Promise<readonly string[]>
    private readyResolve: ((actorNames: readonly string[]) => void) | undefined
    private readyReject: ((error: Error) => void) | undefined
    private readonly pending = new Map<
        number,
        {
            resolve: (reply: ActorExecutorReply) => void
            reject: (error: Error) => void
            allowNextInvocation: (() => void) | undefined
            publish: SocketPublisher | undefined
            connections: SocketSource | undefined
        }
    >()
    private nextMessageId = 0
    private lifecycleState: ActorWorkerState = "starting"
    private terminalError: Error | undefined

    private readonly warmPromise: Promise<void>
    private warmResolve: (() => void) | undefined
    private warmReject: ((error: Error) => void) | undefined
    private assigned: boolean

    constructor(
        data?: ActorWorkerData,
        private onStateChange: () => void = () => {}
    ) {
        this.assigned = data !== undefined
        this.warmPromise = new Promise((resolve, reject) => {
            this.warmResolve = resolve
            this.warmReject = reject
        })
        void this.warmPromise.catch(() => undefined)
        this.readyPromise = new Promise<readonly string[]>((resolve, reject) => {
            this.readyResolve = resolve
            this.readyReject = reject
        })
        void this.readyPromise.catch(() => undefined)
        this.worker = new Worker(new URL("./actor-worker.js", import.meta.url), { workerData: data })
        this.worker.on("message", (message: ActorWorkerMessage) => this.receive(message))
        this.worker.once("error", error => this.stop(error))
        this.worker.once("exit", code => this.exited(code))
        this.worker.unref()
    }

    warm(): Promise<void> {
        this.worker.ref()
        return this.warmPromise
    }

    load(data: ActorWorkerData, onStateChange: () => void): void {
        if (this.assigned) throw new Error("customer code already assigned")
        this.onStateChange = onStateChange
        this.assigned = true
        this.post({ type: "load", data })
    }

    get state(): ActorWorkerState {
        return this.lifecycleState
    }

    ready(): Promise<readonly string[]> {
        if (this.lifecycleState === "stopping" || this.lifecycleState === "stopped")
            return Promise.reject(this.terminalError)
        this.worker.ref()
        return this.readyPromise.finally(() => {
            if (this.pending.size === 0) this.worker.unref()
        })
    }

    async execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
        allowNextInvocation: () => void,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.terminalError !== undefined) throw this.terminalError
        this.worker.ref()
        try {
            await this.readyPromise
            if (this.terminalError !== undefined) throw this.terminalError
            return await new Promise<ActorExecutorReply>((resolve, reject) => {
                const messageId = ++this.nextMessageId
                this.pending.set(messageId, { resolve, reject, allowNextInvocation, publish, connections })
                this.post({ type: "execute", messageId, command })
            })
        } finally {
            if (this.pending.size === 0) this.worker.unref()
        }
    }

    terminate(reason: string): void {
        this.stop(new ActorWorkerTerminatedError(reason))
    }

    private receive(message: ActorWorkerMessage): void {
        if (this.lifecycleState === "stopping" || this.lifecycleState === "stopped") return
        if (message.type === "warm") {
            this.warmResolve?.()
            this.warmResolve = undefined
            this.warmReject = undefined
            return
        }
        if (message.type === "ready_for_invocation") {
            const pending = this.pending.get(message.messageId)
            if (pending?.allowNextInvocation === undefined)
                return this.stop(new Error("actor sent an unexpected invocation admission"))
            const allowNextInvocation = pending.allowNextInvocation
            pending.allowNextInvocation = undefined
            allowNextInvocation()
            return
        }
        if (message.type === "get_connections") {
            void this.loadConnections(message.messageId, message.query)
            return
        }
        if (message.type === "socket_effects") {
            void this.publishEffects(message.messageId, message.effects)
            return
        }
        if (message.type === "ready") {
            if (this.lifecycleState !== "starting") return
            this.lifecycleState = "ready"
            this.readyResolve?.(message.actorNames)
            this.readyResolve = undefined
            this.readyReject = undefined
            this.onStateChange()
            return
        }
        if (this.readyResolve !== undefined) {
            this.stop(
                new Error(
                    message.type === "failed" ? message.message : `actor Worker sent ${message.type} before ready`
                )
            )
            return
        }
        if (message.type === "failed") this.stop(new Error(message.message))
        else this.reply(message.messageId, message.reply)
    }

    private async publishEffects(messageId: number, effects: readonly SocketEffect[]): Promise<void> {
        try {
            const publish = this.pending.get(messageId)?.publish
            if (publish === undefined) throw new Error("actor socket publishing is unavailable")
            await publish(effects)
            this.post({ type: "socket_effects_published", messageId })
        } catch (error) {
            this.post({ type: "socket_effects_published", messageId, error: errorMessage(error) })
        }
    }

    private async loadConnections(messageId: number, query?: SocketQuery): Promise<void> {
        try {
            const source = this.pending.get(messageId)?.connections
            if (source === undefined) throw new Error("actor connection lookup is unavailable")
            const connections = await source(query)
            this.post({ type: "socket_connections", messageId, connections })
        } catch (error) {
            this.post({ type: "socket_connections", messageId, connections: [], error: errorMessage(error) })
        }
    }

    private reply(messageId: number, reply: ActorExecutorReply): void {
        const pending = this.pending.get(messageId)
        if (pending === undefined) return this.stop(new Error("actor replied to an unknown invocation"))
        this.pending.delete(messageId)
        pending.resolve(reply)
        if (this.pending.size === 0) this.worker.unref()
    }

    private post(message: ActorWorkerRequest): void {
        this.worker.postMessage(message)
    }

    private stop(error: Error): void {
        if (this.lifecycleState === "stopping" || this.lifecycleState === "stopped") return
        this.lifecycleState = "stopping"
        this.fail(error)
        void this.worker.terminate()
        this.onStateChange()
    }

    private exited(code: number): void {
        this.lifecycleState = "stopped"
        this.fail(new Error(`actor Worker exited with code ${code}`))
        this.onStateChange()
    }

    private fail(error: Error): void {
        this.warmReject?.(error)
        this.terminalError ??= error
        this.readyReject?.(this.terminalError)
        this.readyResolve = undefined
        this.readyReject = undefined
        for (const pending of this.pending.values()) pending.reject(this.terminalError)
        this.pending.clear()
        this.worker.unref()
    }
}

class ActorWorkerTerminatedError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "ActorWorkerTerminatedError"
    }
}

export { ActorWorker, ActorWorkerSupervisor }
