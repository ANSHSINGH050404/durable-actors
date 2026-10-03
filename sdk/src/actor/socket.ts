import { AsyncLocalStorage } from "node:async_hooks"

import { ActorProtocolError } from "../errors.js"
import type { JsonValue } from "../json.js"

import type {
    SocketConnection,
    SocketEffect,
    SocketLookup,
    SocketMessage,
    SocketQuery,
    SocketSource
} from "./socketProtocol.js"
import { incomingMessage, outgoingMessage, socketMetadata, socketTags } from "./socketValidation.js"
import type { ActorSchemas, ActorStateMessage, ActorStateUpdate } from "./socketValidation.js"

type ActorSocketState = "connecting" | "open" | "closed"
/** A JSON value, encoded automatically when sent. */
type ActorSocketMessage = JsonValue

/** Actor-side socket. Metadata and tags last only for this connection. */
interface ActorSocket<Metadata = JsonValue, Outgoing = JsonValue, Tag extends string = string> {
    readonly id: string
    /** Assign the whole value to update it. Maximum: 16 KiB of JSON. */
    metadata: Metadata
    readonly tags: readonly Tag[]
    readonly state: ActorSocketState
    /** Sends JSON. Throws if closed. Delivery does not confirm saved state. */
    send(message: Outgoing): void
    /**
     * @param code - Defaults to 1000; accepts 1000 or application codes 3000–4999.
     * @param reason - Defaults to an empty string; at most 123 UTF-8 bytes.
     */
    close(code?: number, reason?: string): void
    /**
     * Rejects a connection during `onConnect`. Pass an explicit code, such as 4003.
     * @param code - Use 3000–4999; the default 1008 is currently rejected.
     * @param reason - Defaults to "connection rejected"; at most 123 UTF-8 bytes.
     */
    reject(code?: number, reason?: string): void
    /** Replaces tags. Maximum: 10 unique tags, 1–256 Unicode characters each. */
    setTags(...tags: Tag[]): void
}

/** Filters connections for a broadcast. */
interface ActorBroadcastOptions<Tag extends string = string> {
    /** Excludes the selected connections. */
    readonly except?: Pick<ActorSocket, "id"> | readonly Pick<ActorSocket, "id">[]
    /** Empty or omitted selects all connections. */
    readonly tags?: readonly Tag[]
    /** Defaults to "all". */
    readonly tagMatch?: "all" | "any"
}

/**
 * Backend socket with automatic JSON encoding. Attach listeners after `connect()`.
 * Reconnect in your application; messages are not replayed.
 */
interface ActorConnection<Send = JsonValue, Receive = Send, State = JsonValue> {
    /** WebSocket state: 0 connecting, 1 open, 2 closing, 3 closed. */
    readonly readyState: number
    /** Sends JSON while open. Pass objects directly. */
    send(data: Send): void
    /** Reasons are limited to 123 UTF-8 bytes. */
    close(code?: number, reason?: string): void
    /** The `open` event normally fires before `connect()` resolves. */
    addEventListener<Type extends keyof ActorConnectionEventMap<Receive, State>>(
        type: Type,
        listener: (event: ActorConnectionEventMap<Receive, State>[Type]) => void
    ): void
    removeEventListener<Type extends keyof ActorConnectionEventMap<Receive, State>>(
        type: Type,
        listener: (event: ActorConnectionEventMap<Receive, State>[Type]) => void
    ): void
}

interface ActorConnectionEventMap<Receive = JsonValue, State = JsonValue> {
    readonly open: { readonly type: "open" }
    readonly message: {
        readonly type: "message"
        readonly data: Receive | ActorStateMessage<State> | ActorStateUpdate<State>
    }
    readonly close: {
        readonly type: "close"
        readonly code: number
        readonly reason: string
        readonly wasClean: boolean
    }
    readonly error: { readonly type: "error" }
}

const scopes = new AsyncLocalStorage<{ instance: object; scope: ActorSocketScope; active: boolean }>()

async function actorConnections<Metadata, Outgoing, Tag extends string>(
    instance: object,
    tag?: string
): Promise<readonly ActorSocket<Metadata, Outgoing, Tag>[]> {
    return (await socketScope(instance).getConnections(tag)) as unknown as readonly ActorSocket<
        Metadata,
        Outgoing,
        Tag
    >[]
}

function broadcastActor(instance: object, message: unknown, options?: ActorBroadcastOptions): void {
    socketScope(instance).broadcast(message, options)
}

async function runWithActorSockets<T>(
    instance: object,
    connections: readonly SocketConnection[] | SocketSource,
    operation: (scope: ActorSocketScope) => Promise<T>,
    publish?: (effects: readonly SocketEffect[]) => Promise<void>,
    schemas: ActorSchemas = {},
    eventConnections: readonly SocketConnection[] = []
): Promise<{ readonly value: T; readonly effects: readonly SocketEffect[] }> {
    const effects: SocketEffect[] = []
    const output = publish === undefined ? undefined : new SocketOutput(publish)
    const scope = new ActorSocketScope(connections, output ?? effects, schemas, eventConnections)
    const context = { instance, scope, active: true }
    return scopes.run(context, async () => {
        try {
            return { value: await operation(scope), effects }
        } finally {
            context.active = false
            await scope.settle()
            await output?.flush()
        }
    })
}

class ActorSocketScope {
    private readonly byId = new Map<string, RuntimeActorSocket>()
    private readonly loading = new Map<string | undefined, Promise<readonly RuntimeActorSocket[]>>()
    private querying: Promise<unknown> = Promise.resolve()

    constructor(
        private readonly connections: readonly SocketConnection[] | SocketSource,
        readonly effects: Pick<SocketEffect[], "push">,
        private readonly schemas: ActorSchemas,
        eventConnections: readonly SocketConnection[]
    ) {
        this.wrap(eventConnections)
        if (typeof connections !== "function") this.loading.set(undefined, Promise.resolve(this.wrap(connections)))
    }

    getConnections(tag?: string): Promise<readonly RuntimeActorSocket[]> {
        if (tag !== undefined) socketTags([tag], this.schemas)
        let loading = this.loading.get(tag)
        if (loading !== undefined) return loading
        loading = this.query(tag === undefined ? undefined : { tag }).then(connections => {
            if (typeof connections === "number") throw new ActorProtocolError("expected socket list")
            const sockets = this.wrap(connections)
            const ids = new Set(sockets.map(socket => socket.id))
            return [
                ...sockets,
                ...[...this.byId.values()].filter(
                    socket =>
                        socket.state === "connecting" &&
                        !ids.has(socket.id) &&
                        (tag === undefined || socket.tags.includes(tag))
                )
            ]
        })
        this.loading.set(tag, loading)
        return loading
    }

    async getConnectionCount(): Promise<number> {
        const count = await this.query({ countOnly: true })
        if (typeof count !== "number") throw new ActorProtocolError("expected socket count")
        return count + [...this.byId.values()].filter(socket => socket.state === "connecting").length
    }

    setWebSocketAutoResponse(pair?: { readonly request: string; readonly response: string }): void {
        if (
            pair !== undefined &&
            (typeof pair.request !== "string" ||
                typeof pair.response !== "string" ||
                [...pair.request].length > 2048 ||
                [...pair.response].length > 2048)
        )
            throw new ActorProtocolError(
                "automatic response request and response must be strings of at most 2048 characters"
            )
        this.effects.push({
            type: "set_auto_response",
            request: pair?.request ?? null,
            response: pair?.response ?? null
        })
    }

    async settle(): Promise<void> {
        await Promise.all([...this.loading.values()].map(promise => promise.catch(() => undefined)))
        await this.querying.catch(() => undefined)
    }

    private query(query?: SocketQuery): Promise<SocketLookup> {
        const next = this.querying.then(() => {
            if (typeof this.connections === "function") return this.connections(query)
            const connections = this.connections.filter(
                connection => query?.tag === undefined || connection.tags.includes(query.tag)
            )
            return query?.countOnly ? connections.length : connections
        })
        this.querying = next.catch(() => undefined)
        return next
    }

    eventSocket(connection: SocketConnection, state: ActorSocketState): RuntimeActorSocket {
        const socket = this.byId.get(connection.id)
        if (socket !== undefined) {
            socket.setState(state)
            return socket
        }
        const created = new RuntimeActorSocket(connection, this.effects, this.schemas, state)
        this.byId.set(connection.id, created)
        return created
    }

    connection(connectionId: string): RuntimeActorSocket {
        const socket = this.byId.get(connectionId)
        if (socket === undefined)
            throw new ActorProtocolError(`socket connection ${connectionId} is not attached to the actor`)
        return socket
    }

    broadcast(message: unknown, options: ActorBroadcastOptions = {}): void {
        if (options.tagMatch !== undefined && options.tagMatch !== "all" && options.tagMatch !== "any")
            throw new ActorProtocolError('broadcast tagMatch must be "all" or "any"')
        this.effects.push({
            type: "broadcast",
            message: socketMessage(message, this.schemas),
            except_connection_ids: excludedSocketIds(options.except),
            tags: socketTags(options.tags ?? [], this.schemas),
            ...(options.tagMatch === undefined ? {} : { tag_match: options.tagMatch })
        })
    }

    private wrap(connections: readonly SocketConnection[]): readonly RuntimeActorSocket[] {
        const sockets = connections.map(
            connection => this.byId.get(connection.id) ?? new RuntimeActorSocket(connection, this.effects, this.schemas)
        )
        for (const socket of sockets) this.byId.set(socket.id, socket)
        return sockets
    }
}

class RuntimeActorSocket<Metadata = JsonValue> implements ActorSocket<Metadata> {
    private metadataValue: Metadata
    private tagsValue: readonly string[]

    constructor(
        connection: SocketConnection,
        private readonly effects: Pick<SocketEffect[], "push">,
        private readonly schemas: ActorSchemas,
        private stateValue: ActorSocketState = "open"
    ) {
        this.id = connection.id
        this.metadataValue = socketMetadata(connection.metadata, schemas) as Metadata
        this.tagsValue = socketTags(connection.tags, schemas)
    }

    readonly id: string

    get state(): ActorSocketState {
        return this.stateValue
    }

    get metadata(): Metadata {
        return this.metadataValue
    }

    set metadata(value: Metadata) {
        const metadata = socketMetadata(value, this.schemas)
        this.metadataValue = metadata as Metadata
        this.effects.push({ type: "set_metadata", connection_id: this.id, metadata })
    }

    get tags(): readonly string[] {
        return this.tagsValue
    }

    send(message: ActorSocketMessage): void {
        if (this.stateValue === "closed") throw new ActorProtocolError("cannot send on a closed actor socket")
        this.effects.push({ type: "send", connection_id: this.id, message: socketMessage(message, this.schemas) })
    }

    close(code = 1000, reason = ""): void {
        validateClose(code, reason)
        if (this.stateValue === "closed") return
        this.stateValue = "closed"
        this.effects.push({ type: "close", connection_id: this.id, code, reason })
    }

    reject(code = 1008, reason = "connection rejected"): void {
        if (this.stateValue !== "connecting")
            throw new ActorProtocolError("only a connecting actor socket can be rejected")
        validateClose(code, reason)
        this.stateValue = "closed"
        this.effects.push({ type: "reject", connection_id: this.id, code, reason })
    }

    setTags(...tags: string[]): void {
        const unique = socketTags(tags, this.schemas)
        this.tagsValue = unique
        this.effects.push({ type: "set_tags", connection_id: this.id, tags: unique })
    }

    setState(state: ActorSocketState): void {
        this.stateValue = state
    }
}

class SocketOutput {
    private pending: Promise<void> | undefined
    private queued: SocketEffect[] = []
    private failure: unknown

    constructor(private readonly publish: (effects: readonly SocketEffect[]) => Promise<void>) {}

    push(...effects: SocketEffect[]): number {
        if (this.failure !== undefined) throw this.failure
        this.queued.push(...effects)
        this.pending ??= this.drain()
        void this.pending.catch(() => undefined)
        return this.queued.length
    }

    async flush(): Promise<void> {
        await this.pending
        if (this.failure !== undefined) throw this.failure
    }

    private async drain(): Promise<void> {
        try {
            while (this.queued.length > 0) {
                const batch = this.queued
                this.queued = []
                await this.publish(batch)
            }
        } catch (error) {
            this.failure = error
            throw error
        } finally {
            this.pending = undefined
        }
    }
}

function socketScope(instance: object): ActorSocketScope {
    const context = scopes.getStore()
    if (context === undefined || context.instance !== instance || !context.active)
        throw new ActorProtocolError("actor connections are available only during an actor invocation")
    return context.scope
}

function socketMessage(message: unknown, schemas: ActorSchemas = {}): SocketMessage {
    if (ArrayBuffer.isView(message) || message instanceof ArrayBuffer)
        throw new ActorProtocolError("socket messages must be JSON values, not bytes")
    return { type: "text", data: JSON.stringify(outgoingMessage(message, schemas)) }
}

function decodeSocketMessage(message: SocketMessage, schemas: ActorSchemas = {}): ActorSocketMessage {
    if (message.type !== "text") throw new ActorProtocolError("socket messages must be JSON text frames")
    try {
        return incomingMessage(JSON.parse(message.data), schemas)
    } catch (error) {
        throw new ActorProtocolError("socket message is not valid JSON", { cause: error })
    }
}

function validateClose(code: number, reason: string): void {
    if (!Number.isInteger(code) || (code !== 1000 && (code < 3000 || code > 4999)))
        throw new ActorProtocolError("socket close codes must be 1000 or between 3000 and 4999")
    if (Buffer.byteLength(reason) > 123)
        throw new ActorProtocolError("socket close reasons must not exceed 123 UTF-8 bytes")
}

function excludedSocketIds(except: ActorBroadcastOptions["except"]): readonly string[] {
    if (except === undefined) return []
    return Array.isArray(except) ? except.map(socket => socket.id) : [(except as ActorSocket).id]
}

function actorConnectionCount(instance: object): Promise<number> {
    return socketScope(instance).getConnectionCount()
}
function setActorAutoResponse(instance: object, pair?: { readonly request: string; readonly response: string }): void {
    socketScope(instance).setWebSocketAutoResponse(pair)
}

export {
    actorConnectionCount,
    setActorAutoResponse,
    actorConnections,
    broadcastActor,
    decodeSocketMessage,
    runWithActorSockets,
    socketMessage
}
export type {
    ActorSocketScope,
    ActorBroadcastOptions,
    ActorConnection,
    ActorConnectionEventMap,
    ActorSocket,
    ActorSocketMessage,
    ActorSocketState
}
