import { z } from "zod"

import type { ActorIdentity } from "../actor/identity.js"
import { actorComponentSchema, actorIdentitySchema } from "../actor/identity.js"
import { socketConnectionSchema, socketEventSchema } from "../actor/socketProtocol.js"
import type { SocketConnection, SocketEffect } from "../actor/socketProtocol.js"
import { ActorProtocolError } from "../errors.js"
import { jsonValueSchema } from "../json.js"
import type { JsonValue } from "../json.js"

import type { SqliteState } from "./sqlite.js"

function parseActorSessionServerMessage(document: string): ActorSessionServerMessage {
    let value: unknown
    try {
        value = JSON.parse(document)
    } catch (error) {
        throw new ActorProtocolError("actor session message is not valid JSON", { cause: error })
    }
    const result = actorSessionServerMessageSchema.safeParse(value)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    return result.data
}

function failedReply(code: string, message: string): FailedReply {
    return { type: "failed", code, message }
}

const sqliteStateSchema = z.object({
    txid: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
    path: z.string().optional(),
    socket: z.string().optional()
})

const invokeCommandSchema = z.object({
    type: z.literal("invoke"),
    request_id: actorComponentSchema,
    actor: actorIdentitySchema,
    method: actorComponentSchema,
    args: z.array(jsonValueSchema),
    sqlite: sqliteStateSchema.optional(),
    resident_only: z.boolean().optional()
})

const websocketEventCommandSchema = z.object({
    type: z.literal("websocket_event"),
    request_id: actorComponentSchema,
    actor: actorIdentitySchema,
    event: socketEventSchema,
    connections: z.array(socketConnectionSchema),
    sqlite: sqliteStateSchema.optional(),
    resident_only: z.boolean().optional()
})

const evictCommandSchema = z.object({
    type: z.literal("evict"),
    actor: actorIdentitySchema
})

const hydrateCommandSchema = z.object({
    type: z.literal("hydrate"),
    actor: actorIdentitySchema,
    sqlite: sqliteStateSchema.optional(),
    resident_only: z.boolean().optional()
})

const executorCommandSchema = z.discriminatedUnion("type", [
    invokeCommandSchema,
    websocketEventCommandSchema,
    evictCommandSchema,
    hydrateCommandSchema
])

const actorSessionServerMessageSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("attached"), protocol: z.literal(21), supports_residency: z.boolean().optional() }),
    z.object({
        type: z.literal("socket_connections"),
        message_id: z.number().int().nonnegative(),
        connections: z.array(socketConnectionSchema),
        error: z.string().optional()
    }),
    z.object({
        type: z.literal("socket_effects_published"),
        message_id: z.number().int().nonnegative(),
        error: z.string().optional()
    }),
    z.object({
        type: z.literal("command"),
        message_id: z.number().int().nonnegative(),
        command: executorCommandSchema
    })
])

type HydrateCommand = z.infer<typeof hydrateCommandSchema>
type InvokeCommand = z.infer<typeof invokeCommandSchema>
type EvictCommand = z.infer<typeof evictCommandSchema>
type ActorExecutorCommand = z.infer<typeof executorCommandSchema>
type ActorSessionServerMessage = z.infer<typeof actorSessionServerMessageSchema>
type ActorExecutorReply =
    | InvokedReply
    | WebSocketHandledReply
    | FailedReply
    | EvictedReply
    | { readonly type: "hydrated" }
    | { readonly type: "state_required" }
type ActorSessionClientMessage =
    | { readonly type: "residency"; readonly actors: readonly ActorIdentity[] }
    | AttachMessage
    | { readonly type: "ready_for_invocation"; readonly message_id: number }
    | ReplyMessage
    | { readonly type: "get_connections"; readonly message_id: number }
    | { readonly type: "socket_effects"; readonly message_id: number; readonly effects: readonly SocketEffect[] }

interface AttachMessage {
    readonly type: "attach"
    readonly protocol: 21
    readonly actor_names: readonly string[]
}

interface ReplyMessage {
    readonly type: "reply"
    readonly message_id: number
    readonly reply: ActorExecutorReply
}

interface InvokedReply {
    readonly type: "invoked"
    readonly sequence?: number
    readonly result: JsonValue
    readonly sqlite: SqliteState
    readonly effects?: readonly SocketEffect[]
}

interface WebSocketHandledReply {
    readonly type: "websocket_handled"
    readonly sequence?: number
    readonly sqlite: SqliteState
    readonly effects: readonly SocketEffect[]
}

interface FailedReply {
    readonly type: "failed"
    readonly code: string
    readonly message: string
}

interface EvictedReply {
    readonly type: "evicted"
}

interface ActorWorkerData {
    readonly moduleUrl: string
    readonly environment?: Readonly<Record<string, string>>
}

type ActorWorkerRequest =
    | { readonly type: "load"; readonly data: ActorWorkerData }
    | {
          readonly type: "execute"
          readonly messageId: number
          readonly command: InvokeCommand | WebSocketEventCommand | HydrateCommand
      }
    | { readonly type: "socket_effects_published"; readonly messageId: number; readonly error?: string }
    | {
          readonly type: "socket_connections"
          readonly messageId: number
          readonly connections: readonly SocketConnection[]
          readonly error?: string
      }
type ActorWorkerMessage =
    | { readonly type: "warm" }
    | {
          readonly type: "ready"
          readonly actorNames: readonly string[]
      }
    | { readonly type: "reply"; readonly messageId: number; readonly reply: ActorExecutorReply }
    | FailedReply
    | { readonly type: "ready_for_invocation"; readonly messageId: number }
    | { readonly type: "socket_effects"; readonly messageId: number; readonly effects: readonly SocketEffect[] }
    | { readonly type: "get_connections"; readonly messageId: number }

type WebSocketEventCommand = z.infer<typeof websocketEventCommandSchema>
export { failedReply, parseActorSessionServerMessage }
export type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorSessionClientMessage,
    ActorSessionServerMessage,
    ActorWorkerData,
    ActorWorkerMessage,
    ActorWorkerRequest,
    EvictCommand,
    InvokeCommand,
    HydrateCommand,
    WebSocketEventCommand
}
