import type { RequestTrace } from "./client.js"
import { duration, gapLabel, requestTimeline } from "./request-timeline.js"

export interface RequestTimelineProps {
    records: RequestTrace[]
    selected?: RequestTrace
    onSelect: (record: RequestTrace, trigger: HTMLButtonElement) => void
}

const ticks = [0, 0.25, 0.5, 0.75, 1]

export function RequestTimeline({ records, selected, onSelect }: RequestTimelineProps) {
    const { start, span, rows } = requestTimeline(records)
    const scale = Math.max(1, span)
    return (
        <div className="request-waterfall" role="group" aria-label="Invocation waterfall">
            <TimelineHeading count={records.length} span={span} />
            <div className="request-waterfall-scroll" tabIndex={0} aria-label="Invocation timeline, scroll for more calls">
                <div className="request-waterfall-axis" aria-hidden="true">
                    <span>Operation / instance</span>
                    <div>
                        {ticks.map(tick => (
                            <span key={tick} style={{ left: `${tick * 100}%` }}>
                                {duration(scale * tick)}
                            </span>
                        ))}
                    </div>
                    <span>Total / gap</span>
                </div>
                {rows.map(row => (
                    <TimelineRow key={row.record.eventId ?? row.record.sequence} row={row} scale={scale} selected={selected === row.record} onSelect={onSelect} />
                ))}
            </div>
            <div className="request-waterfall-caption">
                <span>
                    Relative to <time dateTime={new Date(start).toISOString()}>{new Date(start).toLocaleString([], { hour12: false })}</time>
                </span>
                <span>Gaps use preceding calls on the same instance in this view.</span>
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
    row: { record, offsetMs, gapMs },
    scale,
    selected,
    onSelect
}: {
    row: ReturnType<typeof requestTimeline>["rows"][number]
    scale: number
    selected: boolean
    onSelect: RequestTimelineProps["onSelect"]
}) {
    return (
        <button
            type="button"
            className="request-waterfall-row"
            data-state={selected ? "selected" : undefined}
            aria-label={`Inspect ${record.operation} request on ${record.actorName} / ${record.actorId}, ${record.outcome}, ${duration(record.durationMs)}, starts +${duration(offsetMs)}, ${gapLabel(gapMs)}`}
            aria-haspopup="dialog"
            title={`${record.actorName} / ${record.actorId}\n${record.operation} · ${record.outcome}\nStart: ${new Date(record.startedAtMs).toLocaleString()} (+${duration(offsetMs)})\nTotal: ${duration(record.durationMs)}\nQueue wait: ${record.queueWaitMs === null ? "Did not begin processing" : duration(record.queueWaitMs)}\n${gapLabel(gapMs)}${gapMs === null ? "" : " relative to preceding calls on this instance"}`}
            onClick={event => onSelect(record, event.currentTarget)}
        >
            <span className="request-waterfall-label">
                <strong>{record.operation}</strong>
                <span>
                    {record.actorName} / {record.actorId}
                </span>
            </span>
            <span className="request-waterfall-track" aria-hidden="true">
                {ticks.map(tick => (
                    <i className="request-waterfall-gridline" key={tick} style={{ left: `${tick * 100}%` }} />
                ))}
                {gapMs !== null && gapMs > 0 && <span className="request-waterfall-gap" style={{ left: `${((offsetMs - gapMs) / scale) * 100}%`, width: `${(gapMs / scale) * 100}%` }} />}
                <span
                    className={`request-waterfall-bar request-waterfall-${record.kind} request-waterfall-${record.outcome}`}
                    style={{ left: `${(offsetMs / scale) * 100}%`, width: `${(record.durationMs / scale) * 100}%` }}
                >
                    {record.queueWaitMs !== null && record.queueWaitMs > 0 && <span className="request-waterfall-queue" style={{ width: `${(record.queueWaitMs / record.durationMs) * 100}%` }} />}
                </span>
            </span>
            <span className="request-waterfall-timing">
                <strong>{duration(record.durationMs)}</strong>
                <span>{gapLabel(gapMs)}</span>
            </span>
        </button>
    )
}
