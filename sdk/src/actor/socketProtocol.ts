import { z } from "zod"

import { ActorProtocolError } from "../errors.js"
import { jsonValueSchema } from "../json.js"
import type { JsonObject, JsonValue } from "../json.js"

import { actorComponentSchema } from "./identity.js"

function parseSocketEffects(value: unknown): readonly SocketEffect[] {
    const result = socketEffectsSchema.safeParse(value)
    if (!result.success) throw new ActorProtocolError(`actor socket effects are invalid: ${result.error.message}`)
    return result.data
}

const socketConnectionIdSchema = actorComponentSchema.max(128)
const socketMetadataSchema = jsonValueSchema.refine(
    value => Buffer.byteLength(JSON.stringify(value)) <= 16 * 1024,
    "socket metadata must not exceed 16 KiB"
)
const socketTagSchema = z
    .string()
    .min(1)
    .refine(tag => [...tag].length <= 256, "socket tag must not exceed 256 characters")
const socketTagsSchema = z.array(socketTagSchema).max(10)
const socketTextSchema = z.string()
const socketBinarySchema = z
    .string()
    .regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u, "socket binary message must be base64")
const socketCloseCodeSchema = z
    .number()
    .int()
    .refine(
        code => code === 1000 || (code >= 3000 && code <= 4999),
        "socket close code must be 1000 or between 3000 and 4999"
    )
const socketCloseReasonSchema = z
    .string()
    .refine(reason => Buffer.byteLength(reason) <= 123, "socket close reason must not exceed 123 bytes")

const socketConnectionSchema = z.object({
    id: socketConnectionIdSchema,
    metadata: socketMetadataSchema,
    tags: socketTagsSchema
})

const socketMessageSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("text"), data: socketTextSchema }),
    z.object({ type: z.literal("binary"), data: socketBinarySchema })
])

const socketEffectSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("set_auto_response"), request: z.string().nullable(), response: z.string().nullable() }),
    z.object({
        type: z.literal("state_snapshot"),
        connection_id: socketConnectionIdSchema,
        state: z.record(z.string(), jsonValueSchema),
        version: z.number().int().nonnegative().safe().optional()
    }),
    z.object({
        type: z.literal("state_update"),
        changes: z.record(z.string(), jsonValueSchema),
        removed: z.array(z.string()),
        except_connection_ids: z.array(socketConnectionIdSchema).optional(),
        version: z.number().int().nonnegative().safe().optional()
    }),
    z.object({ type: z.literal("send"), connection_id: socketConnectionIdSchema, message: socketMessageSchema }),
    z.object({
        type: z.literal("broadcast"),
        message: socketMessageSchema,
        except_connection_ids: z.array(socketConnectionIdSchema),
        tags: socketTagsSchema,
        tag_match: z.enum(["all", "any"]).optional()
    }),
    z.object({
        type: z.literal("close"),
        connection_id: socketConnectionIdSchema,
        code: socketCloseCodeSchema,
        reason: socketCloseReasonSchema
    }),
    z.object({
        type: z.literal("reject"),
        connection_id: socketConnectionIdSchema,
        code: socketCloseCodeSchema,
        reason: socketCloseReasonSchema
    }),
    z.object({
        type: z.literal("set_metadata"),
        connection_id: socketConnectionIdSchema,
        metadata: socketMetadataSchema
    }),
    z.object({ type: z.literal("set_tags"), connection_id: socketConnectionIdSchema, tags: socketTagsSchema })
])

const socketEffectsSchema = z.array(socketEffectSchema)

const socketEventSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("connect"), connection: socketConnectionSchema }),
    z.object({ type: z.literal("message"), connection_id: actorComponentSchema, message: socketMessageSchema }),
    z.object({
        type: z.literal("disconnect"),
        connection: socketConnectionSchema,
        code: z.number().int().min(0).max(65535),
        reason: z.string(),
        was_clean: z.boolean()
    })
])

type SocketConnection = z.infer<typeof socketConnectionSchema>
type SocketMessage = z.infer<typeof socketMessageSchema>
type SocketEvent = z.infer<typeof socketEventSchema>
type SocketQuery = { readonly tag?: string; readonly countOnly?: boolean }
type SocketLookup = readonly SocketConnection[] | number
type SocketSource = (query?: SocketQuery) => Promise<SocketLookup>

type SocketEffect =
    | { readonly type: "set_auto_response"; readonly request: string | null; readonly response: string | null }
    | {
          readonly type: "state_snapshot"
          readonly connection_id: string
          readonly state: JsonObject
          readonly version?: number
      }
    | {
          readonly type: "state_update"
          readonly changes: JsonObject
          readonly removed: readonly string[]
          readonly except_connection_ids?: readonly string[]
          readonly version?: number
      }
    | { readonly type: "send"; readonly connection_id: string; readonly message: SocketMessage }
    | {
          readonly type: "broadcast"
          readonly message: SocketMessage
          readonly except_connection_ids: readonly string[]
          readonly tags: readonly string[]
          readonly tag_match?: "all" | "any"
      }
    | {
          readonly type: "close" | "reject"
          readonly connection_id: string
          readonly code: number
          readonly reason: string
      }
    | { readonly type: "set_metadata"; readonly connection_id: string; readonly metadata: JsonValue }
    | { readonly type: "set_tags"; readonly connection_id: string; readonly tags: readonly string[] }

export { parseSocketEffects, socketConnectionSchema, socketEventSchema, socketTagsSchema }
export type { SocketConnection, SocketEffect, SocketEvent, SocketMessage }

export type { SocketQuery, SocketLookup, SocketSource }
