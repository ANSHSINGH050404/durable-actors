import { ActorProtocolError } from "./errors.js"
import type { JsonValue } from "./json.js"
import { projectActorPath, validateOrigin } from "./settings.js"

export class HttpActorHostTransport implements ActorHostTransport {
    constructor(private readonly fetchRequest: typeof globalThis.fetch = globalThis.fetch) {}

    async invoke(target: ActorHostTarget, invocation: ActorInvocation): Promise<ActorHostReply> {
        let response: Response
        try {
            response = await this.post(target, invocation, "invoke", {
                requestId: invocation.requestId,
                ownerEpoch: target.ownerEpoch,
                method: invocation.method,
                args: invocation.args
            })
        } catch (error) {
            if (connectionRefused(error)) return { type: "not_executed", reason: "upstream_not_reached" }
            throw error
        }
        // A 401 is issued before dispatch, so refreshing this ticket cannot repeat actor code.
        if (response.status === 401) return { type: "unauthenticated" }
        if (!response.ok) throw new Error(`actor host returned HTTP ${response.status}`)
        return parseActorHostReply(await responseDocument(response))
    }

    async publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void> {
        const response = await this.post(target, actor, "socket-effects", { ownerEpoch: target.ownerEpoch, effects })
        if (!response.ok) throw new Error(`socket effects returned HTTP ${response.status}`)
    }

    private post(target: ActorHostTarget, actor: ActorAddress, endpoint: string, body: unknown): Promise<Response> {
        return this.fetchRequest(
            `${validateOrigin(target.route)}${projectActorPath(actor.projectId, actor.actorName, actor.actorId)}/${endpoint}`,
            {
                method: "POST",
                redirect: "manual",
                headers: {
                    authorization: `Bearer ${target.token}`,
                    "content-type": "application/json",
                    accept: "application/json"
                },
                body: JSON.stringify(body)
            }
        )
    }
}

export function parseActorHostReply(reply: unknown): ActorHostReply {
    if (isRecord(reply)) {
        const metadata = parseMetadata(reply)
        if (reply.type === "completed" && Object.hasOwn(reply, "result"))
            return { type: "completed", result: reply.result, ...metadata }
        if (
            reply.type === "failed" &&
            typeof reply.code === "string" &&
            reply.code.length > 0 &&
            typeof reply.message === "string"
        )
            return { type: "failed", code: reply.code, message: reply.message, ...metadata }
        if (reply.type === "not_executed" && rejectionReason(reply.reason))
            return { type: "not_executed", reason: reply.reason, ...metadata }
    }
    throw new ActorProtocolError("actor host response did not contain a valid outcome")
}

function parseMetadata(reply: Record<string, unknown>): { metadata?: ActorResponseMetadata } {
    if (!Object.hasOwn(reply, "metadata")) return {}
    const metadata = reply.metadata
    if (
        !isRecord(metadata) ||
        !nonnegativeFiniteNumber(metadata.routingMs) ||
        !nonnegativeFiniteNumber(metadata.durationMs) ||
        (metadata.queueWaitMs !== null &&
            (!nonnegativeFiniteNumber(metadata.queueWaitMs) || metadata.queueWaitMs > metadata.durationMs)) ||
        (metadata.hostState !== "cold" && metadata.hostState !== "warm")
    )
        throw new ActorProtocolError("actor host response contained invalid latency metadata")
    return {
        metadata: {
            routingMs: metadata.routingMs,
            durationMs: metadata.durationMs,
            queueWaitMs: metadata.queueWaitMs,
            hostState: metadata.hostState
        }
    }
}

function nonnegativeFiniteNumber(value: unknown): value is number {
    return typeof value === "number" && Number.isFinite(value) && value >= 0
}

function rejectionReason(value: unknown): value is ActorRejectionReason {
    return value === "stale_owner" || value === "host_unavailable" || value === "upstream_not_reached"
}

export function connectionRefused(error: unknown, ancestors = new Set<unknown>()): boolean {
    if (!isRecord(error) || ancestors.has(error)) return false
    const visited = new Set(ancestors).add(error)
    if (error.code !== undefined && error.code !== "ECONNREFUSED") return false
    if (error.errors !== undefined)
        return (
            Array.isArray(error.errors) &&
            error.errors.length > 0 &&
            error.errors.every(cause => connectionRefused(cause, visited))
        )
    if (error.code === "ECONNREFUSED") return error.syscall === "connect"
    return connectionRefused(error.cause, visited)
}

export async function responseDocument(response: Response): Promise<unknown> {
    try {
        return await response.json()
    } catch (error) {
        throw new ActorProtocolError(`HTTP ${response.status} response was not valid JSON`, { cause: error })
    }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

export interface ActorAddress {
    readonly projectId: string
    readonly actorName: string
    readonly actorId: string
}
export interface ActorHostTarget {
    readonly route: string
    readonly token: string
    readonly ownerEpoch: number
    readonly expiresAtMs: number
}
export interface ActorInvocation extends ActorAddress {
    readonly requestId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}
type ActorRejectionReason = "stale_owner" | "host_unavailable" | "upstream_not_reached"

export interface ActorResponseMetadata {
    readonly routingMs: number
    readonly durationMs: number
    readonly queueWaitMs: number | null
    readonly hostState: "cold" | "warm"
}

export type ActorHostReply = (
    | { readonly type: "completed"; readonly result: unknown }
    | { readonly type: "failed"; readonly code: string; readonly message: string }
    | { readonly type: "unauthenticated" }
    | { readonly type: "not_executed"; readonly reason: ActorRejectionReason }
) & { readonly metadata?: ActorResponseMetadata }
export interface ActorHostTransport {
    invoke(target: ActorHostTarget, invocation: ActorInvocation): Promise<ActorHostReply>
    publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void>
}
