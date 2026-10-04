import { TimelineAxis, TimelineZoomControls, timelineTicks, useTimelineZoom } from "./TimelineZoom.js"
import type { TimelineWindow } from "./TimelineZoom.js"
import type { RequestTrace } from "./client.js"
import { duration, gapLabel, requestTimeline } from "./request-timeline.js"

export interface RequestTimelineProps {
    records: RequestTrace[]
    selected?: RequestTrace
    onSelect: (record: RequestTrace, trigger: HTMLButtonElement) => void
}

export function RequestTimeline({ records, selected, onSelect }: RequestTimelineProps) {
    const { start, span, rows } = requestTimeline(records)
    const bounds = { start, end: start + Math.max(1, span) }
    const { range, zoomed, setWindow } = useTimelineZoom(bounds)
    const visible = rows.map(row => ({ ...row, calls: row.calls.filter(call => inWindow(call.record, range)) })).filter(row => row.calls.length > 0)
    return (
        <div className="request-waterfall" role="group" aria-label="Invocation waterfall">
            <TimelineHeading count={records.length} span={span} />
            <TimelineZoomControls bounds={bounds} range={range} zoomed={zoomed} count={visible.reduce((total, row) => total + row.calls.length, 0)} onChange={setWindow} />
            <div className="request-waterfall-scroll" tabIndex={0} aria-label="Invocation timeline, scroll for more calls">
                <TimelineAxis start={start} range={range} onChange={setWindow} />
                {visible.map(row => (
                    <TimelineRow key={row.key} row={row} range={range} selected={selected} onSelect={onSelect} />
                ))}
                {!visible.length && <div className="la-request-empty">No calls in this time range. Pan or zoom out to find calls.</div>}
            </div>
            <div className="request-waterfall-caption">
                <span>
                    Relative to <time dateTime={new Date(start).toISOString()}>{new Date(start).toLocaleString([], { hour12: false })}</time>
                </span>
                <span>Gaps use preceding loaded calls on the same instance.</span>
            </div>
        </div>
    )
}

function TimelineHeading({ count, span }: { count: number; span: number }) {
    return (
        <div className="request-waterfall-heading">
            <div>
                <strong>Invocation waterfall</strong>
                <span>
                    {count.toLocaleString()} calls · {duration(span)} elapsed
                </span>
            </div>
            <ul className="request-waterfall-legend" aria-label="Timing legend">
                <li>
                    <i className="request-waterfall-method" />
                    Method
                </li>
                <li>
                    <i className="request-waterfall-websocket" />
                    WebSocket
                </li>
                <li>
                    <i className="request-waterfall-queue" />
                    Queue wait
                </li>
                <li>
                    <i className="request-waterfall-failed" />
                    Failed / rejected
                </li>
                <li>
                    <i className="request-waterfall-interrupted" />
                    Interrupted / rerouted
                </li>
            </ul>
        </div>
    )
}

function TimelineRow({
    row: { record, calls },
    range,
    selected,
    onSelect
}: {
    row: ReturnType<typeof requestTimeline>["rows"][number]
    range: TimelineWindow
    selected: RequestTimelineProps["selected"]
    onSelect: RequestTimelineProps["onSelect"]
}) {
    return (
        <div
            className="request-waterfall-row"
            role="group"
            aria-label={`${record.operation} ${record.kind} calls on ${record.actorName} / ${record.actorId}`}
            data-state={calls.some(call => call.record === selected) ? "selected" : undefined}
        >
            <span className="request-waterfall-label">
                <strong>{record.operation}</strong>
                <span>
                    {record.actorName} / {record.actorId}
                </span>
            </span>
            <span className="request-waterfall-track">
                {timelineTicks.map(tick => (
                    <i className="request-waterfall-gridline" aria-hidden="true" key={tick} style={{ left: `${tick * 100}%` }} />
                ))}
                {calls.map(call => (
                    <TimelineCall key={call.record.eventId ?? call.record.sequence} call={call} range={range} selected={selected === call.record} onSelect={onSelect} />
                ))}
            </span>
            <span className="request-waterfall-timing">
                <strong>
                    {calls.length.toLocaleString()} {calls.length === 1 ? "call" : "calls"}
                </strong>
                <span>{duration(calls.reduce((total, call) => total + call.record.durationMs, 0))} total</span>
            </span>
        </div>
    )
}

function TimelineCall({
    call: { record, offsetMs, gapMs },
    range,
    selected,
    onSelect
}: {
    call: ReturnType<typeof requestTimeline>["calls"][number]
    range: TimelineWindow
    selected: boolean
    onSelect: RequestTimelineProps["onSelect"]
}) {
    const scale = range.end - range.start
    const start = Math.max(range.start, record.startedAtMs)
    const end = Math.min(range.end, record.startedAtMs + record.durationMs)
    const queue = Math.max(0, Math.min(end, record.startedAtMs + (record.queueWaitMs ?? 0)) - start)
    const gapStart = Math.max(range.start, record.startedAtMs - (gapMs ?? 0))
    const gapEnd = Math.min(range.end, record.startedAtMs)
    return (
        <>
            {gapEnd > gapStart && (
                <span className="request-waterfall-gap" aria-hidden="true" style={{ left: `${((gapStart - range.start) / scale) * 100}%`, width: `${((gapEnd - gapStart) / scale) * 100}%` }} />
            )}
            <button
                type="button"
                className={`request-waterfall-bar request-waterfall-${record.kind} request-waterfall-${record.outcome}`}
                style={{ left: `${((start - range.start) / scale) * 100}%`, width: `${((end - start) / scale) * 100}%` }}
                data-state={selected ? "selected" : undefined}
                aria-label={`Inspect ${record.operation} request on ${record.actorName} / ${record.actorId}, ${record.outcome}, ${duration(record.durationMs)}, starts +${duration(offsetMs)}, ${gapLabel(gapMs)}`}
                aria-haspopup="dialog"
                title={`${record.actorName} / ${record.actorId}\n${record.operation} · ${record.outcome}\nStart: ${new Date(record.startedAtMs).toLocaleString()} (+${duration(offsetMs)})\nTotal: ${duration(record.durationMs)}\nQueue wait: ${record.queueWaitMs === null ? "Did not begin processing" : duration(record.queueWaitMs)}\n${gapLabel(gapMs)}${gapMs === null ? "" : " relative to preceding calls on this instance"}`}
                onClick={event => onSelect(record, event.currentTarget)}
            >
                {queue > 0 && <span className="request-waterfall-queue" style={{ width: `${(queue / (end - start)) * 100}%` }} />}
            </button>
        </>
    )
}

function inWindow(record: RequestTrace, range: TimelineWindow) {
    return record.durationMs === 0 ? record.startedAtMs >= range.start && record.startedAtMs <= range.end : record.startedAtMs < range.end && record.startedAtMs + record.durationMs > range.start
}
