import type { ActorInventory } from "./client.js"

export type SocketSessionStatus = "open" | "closed" | "lost" | "unknown"

export interface SocketSession {
    connectionId: string
    actorName: string
    actorId: string
    hostId: string | null
    openedAtMs: number | null
    closedAtMs: number | null
    lastSeenMs: number | null
    messages: number
    failures: number
    status: SocketSessionStatus
    metadata?: unknown
    estimatedStart?: boolean
}

export type SocketSessionRow = Omit<SocketSession, "status">

export interface SocketDuration {
    ms: number
    lowerBound: boolean
}

export const sessionLimit = 500

export function sessionRows(value: unknown): SocketSessionRow[] {
    if (!Array.isArray(value) || !value.every(row => !!row && typeof row === "object" && isSessionRow(row))) throw new Error("Invalid WebSocket session response")
    return value
}

export function connectionKey(actorName: string, actorId: string, connectionId: string): string {
    return JSON.stringify([actorName, actorId, connectionId])
}

// Connections the inventory reports before their connect trace is saved borrow the time they were first seen.
export function socketSessions(rows: SocketSessionRow[], inventory: ActorInventory | undefined, firstSeen: ReadonlyMap<string, number> = new Map()): SocketSession[] {
    const live = new Map<string, unknown>()
    for (const actor of inventory?.actors ?? [])
        for (const instance of actor.instances) for (const connection of instance.connections) live.set(JSON.stringify([actor.actorName, instance.actorId, connection.id]), connection.metadata)
    const sessions: SocketSession[] = rows.map(row => {
        const key = JSON.stringify([row.actorName, row.actorId, row.connectionId])
        const status: SocketSessionStatus = row.closedAtMs !== null ? "closed" : live.has(key) ? "open" : inventory?.connectionsComplete === false ? "unknown" : "lost"
        return status === "open" ? { ...row, status, metadata: live.get(key) } : { ...row, status }
    })
    const known = new Set(sessions.map(session => JSON.stringify([session.actorName, session.actorId, session.connectionId])))
    for (const actor of inventory?.actors ?? [])
        for (const instance of actor.instances)
            for (const connection of instance.connections) {
                const key = connectionKey(actor.actorName, instance.actorId, connection.id)
                if (known.has(key)) continue
                const seen = firstSeen.get(key)
                sessions.unshift({
                    connectionId: connection.id,
                    actorName: actor.actorName,
                    actorId: instance.actorId,
                    hostId: null,
                    openedAtMs: seen ?? null,
                    estimatedStart: seen !== undefined,
                    closedAtMs: null,
                    lastSeenMs: null,
                    messages: 0,
                    failures: 0,
                    status: "open",
                    metadata: connection.metadata
                })
            }
    return sessions
}

export function sessionDuration(session: Pick<SocketSession, "status" | "openedAtMs" | "closedAtMs" | "lastSeenMs">, now: number): SocketDuration | null {
    if (session.openedAtMs === null) return null
    const end = session.status === "closed" ? session.closedAtMs! : session.status === "open" ? Math.max(now, session.openedAtMs) : session.lastSeenMs!
    return { ms: Math.max(0, end - session.openedAtMs), lowerBound: session.status === "lost" || session.status === "unknown" }
}

export function sessionSummary(sessions: SocketSession[], now: number) {
    const durations = sessions.map(session => sessionDuration(session, now)).filter((duration): duration is SocketDuration => duration !== null && !duration.lowerBound)
    const messages = sessions.reduce((sum, session) => sum + session.messages, 0)
    return {
        total: sessions.length,
        open: sessions.filter(session => session.status === "open").length,
        lost: sessions.filter(session => session.status === "lost").length,
        closed: sessions.filter(session => session.status === "closed").length,
        median: percentile(
            durations.map(duration => duration.ms),
            0.5
        ),
        p95: percentile(
            durations.map(duration => duration.ms),
            0.95
        ),
        messages,
        messagesPerSession: sessions.length ? messages / sessions.length : null
    }
}

export function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms.toLocaleString(undefined, { maximumFractionDigits: 0 })} ms`
    if (ms < 60_000) return `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 })} s`
    const minutes = Math.floor(ms / 60_000)
    if (minutes < 60) return `${minutes}m ${Math.floor((ms % 60_000) / 1000)}s`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ${minutes % 60}m`
    return `${Math.floor(hours / 24)}d ${hours % 24}h`
}

// A short "which connection is this" label: the top-level metadata entries, most useful first.
export function metadataSummary(metadata: unknown, limit = 3): string | null {
    if (metadata === undefined || metadata === null) return null
    if (typeof metadata !== "object") return String(metadata)
    const entries = Array.isArray(metadata) ? metadata.map((value, index) => [String(index), value] as const) : Object.entries(metadata)
    if (!entries.length) return null
    const shown = entries.slice(0, limit).map(([key, value]) => `${key}: ${typeof value === "string" ? value : JSON.stringify(value)}`)
    return entries.length > limit ? `${shown.join(" · ")} · +${entries.length - limit}` : shown.join(" · ")
}

function percentile(values: number[], fraction: number) {
    if (!values.length) return null
    return values.sort((a, b) => a - b)[Math.max(0, Math.ceil(values.length * fraction) - 1)]!
}

function isSessionRow(value: Record<string, unknown>): value is SocketSessionRow & Record<string, unknown> {
    return (
        typeof value.connectionId === "string" &&
        typeof value.actorName === "string" &&
        typeof value.actorId === "string" &&
        (value.hostId === null || typeof value.hostId === "string") &&
        [value.openedAtMs, value.closedAtMs, value.lastSeenMs].every(time => time === null || (Number.isSafeInteger(time) && Number(time) >= 0)) &&
        [value.messages, value.failures].every(count => Number.isSafeInteger(count) && Number(count) >= 0) &&
        (value.openedAtMs !== null || value.lastSeenMs !== null)
    )
}
