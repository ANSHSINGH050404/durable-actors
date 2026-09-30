import type { RequestTrace } from "./client.js"

export function requestTimeline(records: RequestTrace[]) {
    const ordered = [...records].sort((a, b) => a.startedAtMs - b.startedAtMs || a.sequence - b.sequence)
    const start = ordered[0]?.startedAtMs ?? 0
    let end = start
    const finishes = new Map<string, number>()
    const rows = ordered.map(record => {
        const key = JSON.stringify([record.projectId, record.actorName, record.actorId])
        const previousEnd = finishes.get(key)
        const finish = record.startedAtMs + record.durationMs
        finishes.set(key, Math.max(previousEnd ?? finish, finish))
        end = Math.max(end, finish)
        return { record, offsetMs: record.startedAtMs - start, gapMs: previousEnd === undefined ? null : record.startedAtMs - previousEnd }
    })
    return { start, span: end - start, rows }
}

export function duration(ms: number): string {
    if (ms >= 60_000) return `${(ms / 60_000).toLocaleString(undefined, { maximumFractionDigits: 2 })} min`
    return ms >= 1000 ? `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })} s` : `${ms.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}

export function gapLabel(gapMs: number | null): string {
    if (gapMs === null) return "First in view"
    if (gapMs < 0) return "Overlapping"
    return gapMs === 0 ? "No gap" : `${duration(gapMs)} gap`
}
