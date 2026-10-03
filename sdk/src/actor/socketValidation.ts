import { z } from "zod"

import { ActorProtocolError, ActorSerializationError, ActorValidationError } from "../errors.js"
import { jsonValueSchema } from "../json.js"
import type { JsonObject, JsonValue } from "../json.js"
import type { SocketContract } from "../wire/contract.js"

import { socketTagsSchema } from "./socketProtocol.js"

/** Optional Zod validators for `static schemas`. Transformations are not applied. */
interface ActorSchemas<Metadata = unknown, Incoming = unknown, Outgoing = Incoming, Tag extends string = string> {
    readonly contract?: SocketContract
    readonly metadata?: z.ZodType<Metadata, Metadata>
    readonly incoming?: z.ZodType<Incoming, Incoming>
    readonly outgoing?: z.ZodType<Outgoing, Outgoing>
    readonly tag?: z.ZodType<Tag, Tag>
}

/** Initial snapshot of public `@Emittable` fields. */
interface ActorStateMessage<State = JsonObject> {
    readonly type: "state"
    readonly state: State
    readonly version?: number
}

/** Saved state update. Merge `changes` and delete fields listed in `removed`. */
interface ActorStateUpdate<State = JsonObject> {
    readonly type: "state_update"
    readonly changes: Partial<State>
    readonly removed: readonly string[]
    readonly version: number
}

const stateMessageSchema = z.discriminatedUnion("type", [
    z.object({
        type: z.literal("state"),
        state: z.record(z.string(), jsonValueSchema),
        version: z.number().int().nonnegative().safe().optional()
    }),
    z.object({
        type: z.literal("state_update"),
        changes: z.record(z.string(), jsonValueSchema),
        removed: z.array(z.string()),
        version: z.number().int().nonnegative().safe()
    })
])

function socketMetadata(value: unknown, schemas: ActorSchemas = {}): JsonValue {
    return validateValue(value, "socket metadata", schemas.metadata, 16 * 1024)
}

function incomingMessage(value: unknown, schemas: ActorSchemas = {}): JsonValue {
    return validateValue(value, "incoming socket message", schemas.incoming)
}

function outgoingMessage(value: unknown, schemas: ActorSchemas = {}): JsonValue {
    const message = validateValue(value, "outgoing socket message", schemas.outgoing)
    if (isStateMessage(message))
        throw new ActorProtocolError('socket message types "state" and "state_update" are reserved for actor state')
    return message
}

function receivedMessage(value: unknown, schemas: ActorSchemas = {}): JsonValue {
    if (!isStateMessage(value)) return outgoingMessage(value, schemas)
    const result = stateMessageSchema.safeParse(value)
    if (!result.success) throw new ActorProtocolError(`invalid actor state message: ${result.error.message}`)
    return result.data
}

function socketTags(tags: readonly string[], schemas: ActorSchemas = {}): string[] {
    const result = socketTagsSchema.safeParse([...new Set(tags)])
    if (!result.success) throw new ActorProtocolError(`invalid socket tags: ${result.error.message}`)
    for (const tag of result.data) {
        const validated = schemas.tag?.safeParse(tag)
        if (validated?.success === false)
            throw new ActorValidationError(`invalid socket tag: ${validated.error.message}`)
    }
    return result.data
}

function validateValue(value: unknown, label: string, schema?: z.ZodType, maximumBytes = Infinity): JsonValue {
    let json: JsonValue
    let bytes: number
    try {
        json = jsonValueSchema.parse(value)
        bytes = Buffer.byteLength(JSON.stringify(json))
    } catch (error) {
        throw new ActorSerializationError(`${label} must be a JSON value`, { cause: error })
    }
    if (bytes > maximumBytes) throw new ActorValidationError(`${label} exceeds ${maximumBytes} bytes`)
    const result = schema?.safeParse(json)
    if (result?.success === false) throw new ActorValidationError(`${label} is invalid: ${result.error.message}`)
    return json
}

function isStateMessage(value: unknown): boolean {
    return (
        typeof value === "object" &&
        value !== null &&
        "type" in value &&
        (value.type === "state" || value.type === "state_update")
    )
}

export { socketMetadata, incomingMessage, outgoingMessage, receivedMessage, socketTags }
export type { ActorSchemas, ActorStateMessage, ActorStateUpdate }
