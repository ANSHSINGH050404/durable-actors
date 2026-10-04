import { isDeepStrictEqual } from "node:util"

import type { ActorDefinition, AnyActor } from "../actor/actor.js"
import { Actor, bindActorIdentity } from "../actor/actor.js"
import { bindActorDatabase, runWithActorDatabase } from "../actor/database.js"
import { actorKey } from "../actor/identity.js"
import type { ActorIdentity } from "../actor/identity.js"
import { Persistence } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"
import type { ActorSocketScope } from "../actor/socket.js"
import { decodeSocketMessage, runWithActorSockets } from "../actor/socket.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import type { ActorSchemas } from "../actor/socketValidation.js"
import { ActorDefinitionError, ActorProtocolError, ActorSerializationError, errorMessage } from "../errors.js"
import { cloneJson, cloneJsonObject, isJsonObject } from "../json.js"
import type { JsonObject, JsonValue } from "../json.js"

import { failedReply } from "./protocol.js"
import type { ActorExecutorReply, HydrateCommand, InvokeCommand, WebSocketEventCommand } from "./protocol.js"
import { SqliteCaptureError } from "./sqlite.js"
import type { ActorDatabaseStorage, SqliteState } from "./sqlite.js"
import type { SocketPublisher, SocketSource } from "./types.js"

class ActorRuntime {
    private instance: AnyActor | undefined
    private identity: ActorIdentity | undefined
    private readonly schemas: ActorSchemas
    private serial: Promise<unknown> = Promise.resolve()
    private completion: Promise<unknown> = Promise.resolve()
    private sequence = 0
    private lastCompletedState: JsonObject | undefined
    private fatal: ActorExecutorReply | undefined

    constructor(
        private readonly definition: ActorDefinition,
        private readonly allowNextInvocation: () => void,
        private readonly database: ActorDatabaseStorage,
        private readonly publish?: SocketPublisher,
        private readonly connections: SocketSource = async () => {
            throw new Error("actor connection lookup is unavailable")
        }
    ) {
        this.schemas = { ...definition.schemas, contract: definition.state.contract }
    }

    async handle(command: InvokeCommand | WebSocketEventCommand | HydrateCommand): Promise<ActorExecutorReply> {
        if (command.type === "hydrate") return this.execute(command)
        const method = command.type === "invoke" ? command.method : lifecycleMethod(command)
        const reentrant = this.definition.state.reentrantMethods?.includes(method) ?? false
        const operation = this.serial.then(() => this.execute(command))
        if (!reentrant) this.serial = operation.catch(() => undefined)
        return operation
    }

    close(): void {
        this.reset()
    }

    private async execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand
    ): Promise<ActorExecutorReply> {
        if (this.fatal !== undefined) return this.fatal
        try {
            if (command.type === "hydrate") {
                const prepared = this.prepare(command)
                return prepared instanceof Actor ? { type: "hydrated" } : prepared
            }
            return await (command.type === "invoke" ? this.invoke(command) : this.handleSocketEvent(command))
        } catch (error) {
            this.reset()
            const failure = failedReply("invalid_actor_state", errorMessage(error))
            if (this.interleaved) this.fatal = failure
            return failure
        }
    }

    private get interleaved(): boolean {
        return (this.definition.state.reentrantMethods?.length ?? 0) > 0
    }

    private completionOrder(): { sequence?: number } {
        return this.interleaved ? { sequence: ++this.sequence } : {}
    }

    private async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        const prepared = this.prepare(command)
        if (!(prepared instanceof Actor)) return prepared
        const instance = prepared
        if (!this.definition.methods.has(command.method)) {
            return failedReply(
                "method_not_found",
                `actor method ${this.definition.actorName}.${command.method} was not found`
            )
        }
        const method: unknown = Reflect.get(instance, command.method)
        if (typeof method !== "function") {
            return failedReply(
                "method_not_callable",
                `actor method ${this.definition.actorName}.${command.method} is not callable`
            )
        }

        const before = snapshotActorState(instance, this.definition.state)
        try {
            this.admitNext(command.method)
            const operation = await runWithActorSockets(
                instance,
                this.connections,
                async () =>
                    runWithActorDatabase(
                        instance,
                        async () => Reflect.apply(method, instance, command.args) as Promise<unknown>
                    ),
                this.publish,
                this.schemas
            )
            const result: JsonValue = operation.value === undefined ? null : cloneJson(operation.value, "actor result")
            return await this.complete(async () => {
                const state = snapshotActorState(instance, this.definition.state)
                const effects = [...operation.effects, ...this.stateUpdates(before, state)]
                return {
                    type: "invoked",
                    result,
                    sqlite: await this.databaseState(state),
                    ...this.completionOrder(),
                    ...(effects.length === 0 ? {} : { effects })
                }
            })
        } catch (error) {
            if (error instanceof SqliteCaptureError) {
                this.fatal = failedReply("actor_database_failed", errorMessage(error))
                return this.fatal
            }
            if (!this.interleaved) this.restoreInstance(command.actor, before)
            return failedReply("actor_method_failed", errorMessage(error))
        }
    }

    private async handleSocketEvent(command: WebSocketEventCommand): Promise<ActorExecutorReply> {
        const prepared = this.prepare(command)
        if (!(prepared instanceof Actor)) return prepared
        const instance = prepared
        const methodName = lifecycleMethod(command)
        const method: unknown = Reflect.get(instance, methodName)
        const before = snapshotActorState(instance, this.definition.state)
        try {
            this.admitNext(methodName)
            const operation = await runWithActorSockets(
                instance,
                this.connections,
                async scope => {
                    const args = lifecycleArguments(command, scope, this.schemas)
                    if (method === undefined) return
                    if (typeof method !== "function")
                        throw new ActorProtocolError(
                            `actor lifecycle hook ${this.definition.actorName}.${methodName} is not callable`
                        )
                    await runWithActorDatabase(
                        instance,
                        async () => Reflect.apply(method, instance, args) as Promise<unknown>
                    )
                },
                command.event.type === "connect" ? undefined : this.publish,
                this.schemas,
                command.connections
            )
            return await this.complete(async () => {
                const state = snapshotActorState(instance, this.definition.state)
                const effects = [
                    ...operation.effects,
                    ...this.stateUpdates(
                        before,
                        state,
                        command.event.type === "connect" ? command.event.connection.id : undefined
                    )
                ]
                return {
                    type: "websocket_handled",
                    sqlite: await this.databaseState(state),
                    ...this.completionOrder(),
                    effects: socketEffects(command, state, effects, this.definition.state)
                }
            })
        } catch (error) {
            if (error instanceof SqliteCaptureError) {
                this.fatal = failedReply("actor_database_failed", errorMessage(error))
                return this.fatal
            }
            if (!this.interleaved) this.restoreInstance(command.actor, before)
            return failedReply("actor_socket_failed", errorMessage(error))
        }
    }

    private admitNext(method: string): void {
        if (this.definition.state.reentrantMethods?.includes(method)) this.allowNextInvocation()
    }

    private prepare(command: InvokeCommand | WebSocketEventCommand | HydrateCommand): AnyActor | ActorExecutorReply {
        const identity = command.actor
        if (identity.actor_name !== this.definition.actorName) {
            return failedReply(
                "actor_name_not_found",
                `actor name ${identity.actor_name} is not loaded in this customer process`
            )
        }
        if (this.identity !== undefined && actorKey(this.identity) !== actorKey(identity)) {
            return failedReply(
                "actor_identity_mismatch",
                "resident actor Worker received an invocation for a different actor"
            )
        }
        if (this.instance !== undefined) return this.instance
        if (command.resident_only) return { type: "state_required" }
        this.database.restore(command.sqlite)
        return this.createInstance(identity, this.database.fields())
    }

    private complete(operation: () => Promise<ActorExecutorReply>): Promise<ActorExecutorReply> {
        const completed = this.completion.then(async () => {
            if (this.fatal !== undefined) return this.fatal
            try {
                return await operation()
            } catch (error) {
                if (error instanceof SqliteCaptureError)
                    this.fatal = failedReply("actor_database_failed", errorMessage(error))
                throw error
            }
        })
        this.completion = completed.catch(() => undefined)
        return completed
    }

    private async databaseState(fields: JsonObject): Promise<SqliteState> {
        this.database.persistFields(fields)
        return await this.database.snapshot()
    }

    private restoreInstance(identity: ActorIdentity, state: JsonObject): void {
        this.database.rollback()
        this.createInstance(identity, state)
    }

    private stateUpdates(before: JsonObject, state: JsonObject, except?: string): SocketEffect[] {
        const previous = this.interleaved ? (this.lastCompletedState ?? before) : before
        if (this.interleaved) this.lastCompletedState = state
        return stateUpdates(previous, state, this.definition.state, except)
    }

    private reset(): void {
        this.database.close()
        this.instance = undefined
        this.identity = undefined
    }

    private createInstance(identity: ActorIdentity, state: JsonValue | null): AnyActor {
        const instance = Reflect.construct(this.definition.actorClass, []) as AnyActor
        bindActorIdentity(instance, identity.actor_id)
        bindActorDatabase(instance, this.database)
        validateActorState(instance, this.definition.state)
        if (state !== null) hydrateActorState(instance, persistedState(state), this.definition.state)
        if (this.interleaved) this.lastCompletedState = snapshotActorState(instance, this.definition.state)
        this.identity = { ...identity }
        this.instance = instance
        return instance
    }
}

function socketEffects(
    command: WebSocketEventCommand,
    state: JsonObject,
    effects: readonly SocketEffect[],
    schema: ActorSchema
): readonly SocketEffect[] {
    if (command.event.type !== "connect" || connectionWasRejected(command.event.connection.id, effects)) return effects
    const fields = emittableFields(schema)
    if (fields.length === 0) return effects
    return [
        ...effects,
        {
            type: "state_snapshot",
            connection_id: command.event.connection.id,
            state: Object.fromEntries(
                fields.filter(field => Object.hasOwn(state, field.name)).map(field => [field.name, state[field.name]])
            )
        }
    ]
}

function publicState(state: JsonObject, schema: ActorSchema): JsonObject {
    return Object.fromEntries(
        schema.fields
            .filter(
                field =>
                    field.persistence === Persistence.Persisted &&
                    !field.private &&
                    !field.visibility &&
                    Object.hasOwn(state, field.name)
            )
            .map(field => [field.name, state[field.name]])
    )
}

function stateUpdates(before: JsonObject, after: JsonObject, schema: ActorSchema, except?: string): SocketEffect[] {
    const changed = emittableFields(schema).filter(
        field =>
            Object.hasOwn(before, field.name) !== Object.hasOwn(after, field.name) ||
            !isDeepStrictEqual(before[field.name], after[field.name])
    )
    if (changed.length === 0) return []
    return [
        {
            type: "state_update",
            changes: Object.fromEntries(
                changed.filter(field => Object.hasOwn(after, field.name)).map(field => [field.name, after[field.name]])
            ),
            removed: changed.filter(field => !Object.hasOwn(after, field.name)).map(field => field.name),
            ...(except === undefined ? {} : { except_connection_ids: [except] })
        }
    ]
}

function emittableFields(schema: ActorSchema) {
    return schema.fields.filter(
        field => field.emittable && !field.visibility && !field.private && field.persistence === Persistence.Persisted
    )
}

function connectionWasRejected(connectionId: string, effects: readonly SocketEffect[]): boolean {
    return effects.some(effect => effect.type === "reject" && effect.connection_id === connectionId)
}

function lifecycleMethod(command: WebSocketEventCommand): "onConnect" | "onMessage" | "onDisconnect" {
    switch (command.event.type) {
        case "connect":
            return "onConnect"
        case "message":
            return "onMessage"
        case "disconnect":
            return "onDisconnect"
    }
}

function lifecycleArguments(
    command: WebSocketEventCommand,
    scope: ActorSocketScope,
    schemas: ActorDefinition["schemas"]
): readonly unknown[] {
    switch (command.event.type) {
        case "connect":
            return [scope.eventSocket(command.event.connection, "connecting")]
        case "message":
            return [scope.connection(command.event.connection_id), decodeSocketMessage(command.event.message, schemas)]
        case "disconnect":
            return [
                scope.eventSocket(command.event.connection, "closed"),
                command.event.code,
                command.event.reason,
                command.event.was_clean
            ]
    }
}

function persistedState(value: JsonValue): JsonObject {
    if (!isJsonObject(value)) {
        throw new ActorProtocolError("persisted actor state must be a JSON object")
    }
    return value
}

function snapshotActorState(instance: object, schema: ActorSchema): JsonObject {
    validateActorState(instance, schema)
    const state = Object.fromEntries(
        schema.fields
            .filter(field => field.persistence === Persistence.Persisted && Object.hasOwn(instance, field.name))
            .map(field => [field.name, Reflect.get(instance, field.name)])
    )
    return cloneJsonObject(state, "actor state")
}

function hydrateActorState(instance: object, state: JsonObject, schema: ActorSchema): void {
    validateActorState(instance, schema)
    const restored = cloneJsonObject(
        Object.fromEntries(
            schema.fields
                .filter(field => field.persistence === Persistence.Persisted && Object.hasOwn(state, field.name))
                .map(field => [field.name, state[field.name]])
        ),
        "actor state"
    )
    for (const [key, value] of Object.entries(restored)) {
        if (!Reflect.defineProperty(instance, key, { configurable: true, enumerable: true, writable: true, value }))
            throw new ActorSerializationError(`actor field ${key} cannot be restored`)
    }
}

function validateActorState(instance: object, schema: ActorSchema): void {
    const fields = new Set(schema.fields.filter(field => !field.private).map(field => field.name))
    for (const key of Reflect.ownKeys(instance)) {
        if (typeof key !== "string" || !fields.has(key))
            throw new ActorDefinitionError(
                `actor field ${schema.actorName}.${String(key)} must declare @Persisted or @Ephemeral`
            )
        const descriptor = Object.getOwnPropertyDescriptor(instance, key)!
        if (!("value" in descriptor))
            throw new ActorDefinitionError(`actor field ${schema.actorName}.${key} must be a data property`)
    }
}

export { ActorRuntime, hydrateActorState, snapshotActorState }
