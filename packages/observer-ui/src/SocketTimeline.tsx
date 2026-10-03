import { useState } from "react"
import type { FocusEvent as ReactFocusEvent, MouseEvent as ReactMouseEvent } from "react"

import { formatDuration, metadataSummary, sessionDuration } from "./socket-sessions.js"
import type { SocketSession } from "./socket-sessions.js"

export interface SocketTimelineProps {
    sessions: SocketSession[]
    start: number
    end: number
    endLabel?: string
    selected?: SocketSession
    onSelect: (session: SocketSession, trigger: HTMLElement) => void
}

interface Lane {
    key: string
    actorName: string
    actorId: string
    rows: Placed[][]
}

interface Placed {
    session: SocketSession
    from: number
    to: number
    openEnded: boolean
    unknownStart: boolean
}

const laneHeight = 18
const lanePadding = 8
const maxLanes = 40
const minute = 60_000
const hour = 60 * minute
const day = 24 * hour
const tickSteps = [
    1_000,
    2_000,
    5_000,
    10_000,
    15_000,
    30_000,
    minute,
    2 * minute,
    5 * minute,
    10 * minute,
    15 * minute,
    30 * minute,
    hour,
    2 * hour,
    3 * hour,
    6 * hour,
    12 * hour,
    day,
    2 * day,
    7 * day
]

export function SocketTimeline({ sessions, start, end, endLabel = "now", selected, onSelect }: SocketTimelineProps) {
    const [hover, setHover] = useState<{ session: SocketSession; left: number; top: number; below: boolean }>()
    const lanes = packLanes(sessions, start, end)
    const visible = lanes.slice(0, maxLanes)
    const span = Math.max(1, end - start)
    const ticks = timelineTicks(start, end)
    const step = tickStep(span)
    const position = (time: number) => `${((Math.min(Math.max(time, start), end) - start) / span) * 100}%`
    const show = (session: SocketSession, target: HTMLElement) => {
        const bar = target.getBoundingClientRect()
        const frame = target.closest(".socket-timeline")!.getBoundingClientRect()
        const below = bar.top - frame.top < 150
        setHover({
            session,
            left: Math.min(Math.max(bar.left - frame.left + bar.width / 2, 120), frame.width - 120),
            top: below ? bar.bottom - frame.top : bar.top - frame.top,
            below
        })
    }
    return (
        <div className="socket-timeline" role="group" aria-label="Connection timeline" onMouseLeave={() => setHover(undefined)}>
            <div className="socket-timeline-axis" aria-hidden="true">
                <span />
                <div>
                    {ticks.map(tick => (
                        <span key={tick} style={{ left: position(tick) }}>
                            {tickLabel(tick, step)}
                        </span>
                    ))}
                    <span className="socket-timeline-now">{endLabel}</span>
                </div>
            </div>
            <div className="socket-timeline-body">
                <div className="socket-timeline-grid" aria-hidden="true">
                    {ticks.map(tick => (
                        <i key={tick} style={{ left: position(tick) }} />
                    ))}
                </div>
                {visible.map(lane => (
                    <div key={lane.key} className="socket-timeline-row">
                        <div className="socket-timeline-label" title={`${lane.actorName} / ${lane.actorId}`}>
                            <span>{lane.actorName}</span>
                            <strong>{lane.actorId}</strong>
                        </div>
                        <div className="socket-timeline-lanes" style={{ height: lane.rows.length * laneHeight + lanePadding * 2 }}>
                            {lane.rows.flatMap((row, index) =>
                                row.map(placed => (
                                    <button
                                        key={placed.session.connectionId}
                                        type="button"
                                        className={`socket-bar socket-bar-${placed.session.status}${placed.openEnded ? " socket-bar-open-ended" : ""}${placed.unknownStart ? " socket-bar-unknown-start" : ""}`}
                                        data-state={selected === placed.session ? "selected" : undefined}
                                        style={{ left: position(placed.from), width: `${((placed.to - placed.from) / span) * 100}%`, top: lanePadding + index * laneHeight }}
                                        aria-label={barLabel(placed.session, end)}
                                        onClick={event => onSelect(placed.session, event.currentTarget)}
                                        onMouseEnter={(event: ReactMouseEvent<HTMLElement>) => show(placed.session, event.currentTarget)}
                                        onFocus={(event: ReactFocusEvent<HTMLElement>) => show(placed.session, event.currentTarget)}
                                        onBlur={() => setHover(undefined)}
                                    />
                                ))
                            )}
                        </div>
                    </div>
                ))}
                {lanes.length > maxLanes && (
                    <p className="socket-timeline-more">
                        {(lanes.length - maxLanes).toLocaleString()} more {lanes.length - maxLanes === 1 ? "instance is" : "instances are"} listed in the table below.
                    </p>
                )}
            </div>
            {hover && (
                <div role="tooltip" className={`socket-tooltip${hover.below ? " socket-tooltip-below" : ""}`} style={{ left: hover.left, top: hover.top }}>
                    <TooltipMetadata metadata={hover.session.metadata} />
                    <dl>
                        <dt>Connection</dt>
                        <dd>{shortId(hover.session.connectionId)}</dd>
                        <dt>Instance</dt>
                        <dd>
                            {hover.session.actorName} / {hover.session.actorId}
                        </dd>
                        <dt>Status</dt>
                        <dd>{statusLabel(hover.session.status)}</dd>
                        <dt>Opened</dt>
                        <dd>
                            {hover.session.openedAtMs === null
                                ? "Before retained history"
                                : `${hover.session.estimatedStart ? "≈ " : ""}${new Date(hover.session.openedAtMs).toLocaleTimeString([], { hour12: false })}`}
                        </dd>
                        <dt>Duration</dt>
                        <dd>{durationLabel(hover.session, end)}</dd>
                        <dt>Messages</dt>
                        <dd>{hover.session.messages.toLocaleString()}</dd>
                    </dl>
                </div>
            )}
        </div>
    )
}

const tooltipEntries = 4

// Metadata identifies the person behind a connection, so it leads the tooltip one entry per line.
function TooltipMetadata({ metadata }: { metadata: unknown }) {
    if (metadata === undefined || metadata === null) return null
    const entries: [string, unknown][] =
        typeof metadata === "object" ? (Array.isArray(metadata) ? metadata.map((value, index) => [String(index), value]) : Object.entries(metadata)) : [["value", metadata]]
    if (!entries.length) return null
    return (
        <dl className="socket-tooltip-metadata" aria-label="Connection metadata">
            {entries.slice(0, tooltipEntries).map(([key, value]) => (
                <div key={key}>
                    <dt>{key}</dt>
                    <dd>{typeof value === "string" ? value : JSON.stringify(value)}</dd>
                </div>
            ))}
            {entries.length > tooltipEntries && <p>+{entries.length - tooltipEntries} more in details</p>}
        </dl>
    )
}

export function packLanes(sessions: SocketSession[], start: number, end: number): Lane[] {
    const lanes = new Map<string, Lane>()
    const placed = sessions
        .map(session => place(session, start, end))
        .filter(item => item.to > start)
        .sort((a, b) => a.from - b.from || a.to - b.to)
    for (const item of placed) {
        const key = `${item.session.actorName} ${item.session.actorId}`
        const lane = lanes.get(key) ?? { key, actorName: item.session.actorName, actorId: item.session.actorId, rows: [] }
        lanes.set(key, lane)
        const row = lane.rows.find(row => row.at(-1)!.to + minimumGap(start, end) <= item.from)
        if (row) row.push(item)
        else lane.rows.push([item])
    }
    return [...lanes.values()].sort((a, b) => latest(b) - latest(a))
}

function place(session: SocketSession, start: number, end: number): Placed {
    const from = session.openedAtMs ?? (session.status === "open" ? start : (session.lastSeenMs ?? start))
    const to = session.status === "closed" ? session.closedAtMs! : session.status === "open" ? end : (session.lastSeenMs ?? from)
    return {
        session,
        from: Math.max(start, from),
        to: Math.max(Math.max(start, from) + minimumGap(start, end), Math.min(end, to)),
        openEnded: session.status !== "closed",
        unknownStart: session.openedAtMs === null
    }
}

function minimumGap(start: number, end: number) {
    return Math.max(1, (end - start) / 160)
}

function latest(lane: Lane) {
    return Math.max(...lane.rows.flat().map(item => item.to))
}

export function timelineTicks(start: number, end: number): number[] {
    const step = tickStep(Math.max(1, end - start))
    const ticks: number[] = []
    for (let tick = Math.floor(start / step) * step + step; tick < end - step * 0.35; tick += step) ticks.push(tick)
    return ticks
}

function tickStep(span: number) {
    return tickSteps.find(step => span / step <= 9) ?? tickSteps.at(-1)!
}

function tickLabel(tick: number, step: number) {
    const date = new Date(tick)
    if (step >= day) return date.toLocaleDateString([], { month: "short", day: "numeric" })
    if (step >= minute) return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })
    return date.toLocaleTimeString([], { hour12: false })
}

export function statusLabel(status: SocketSession["status"]) {
    return status === "open" ? "Open" : status === "closed" ? "Closed" : status === "unknown" ? "Unknown" : "Lost"
}

export function durationLabel(session: SocketSession, now: number) {
    const duration = sessionDuration(session, now)
    if (!duration) return "Unknown"
    return `${duration.lowerBound ? "≥ " : ""}${formatDuration(duration.ms)}`
}

export function shortId(id: string) {
    return id.length > 14 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id
}

function barLabel(session: SocketSession, now: number) {
    const summary = metadataSummary(session.metadata)
    return `${statusLabel(session.status)} connection ${shortId(session.connectionId)}${summary === null ? "" : ` (${summary})`} on ${session.actorName} ${session.actorId}, ${durationLabel(session, now)}`
}
