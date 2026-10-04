import { useEffect, useRef, useState } from "react"
import type { PointerEvent } from "react"

import { ChevronLeft, ChevronRight, RotateCcw, ZoomIn, ZoomOut } from "lucide-react"

import { Button } from "./components/ui/button.js"
import { duration } from "./request-timeline.js"

export interface TimelineWindow {
    start: number
    end: number
}

export const timelineTicks = [0, 0.25, 0.5, 0.75, 1]

export function useTimelineZoom(bounds: TimelineWindow) {
    const [window, setWindow] = useState<TimelineWindow | null>(null)
    const outside = window !== null && (window.end <= bounds.start || window.start >= bounds.end)
    const zoomed = window !== null && !outside
    const range = zoomed ? window : bounds
    useEffect(() => {
        if (outside) setWindow(null)
    }, [outside])
    const update = (next: TimelineWindow | null) => {
        const fitted = next && fitWindow(next, bounds)
        setWindow(fitted?.start === bounds.start && fitted.end === bounds.end ? null : fitted)
    }
    return { range, zoomed, setWindow: update }
}

export function TimelineZoomControls({
    bounds,
    range,
    zoomed,
    count,
    onChange
}: {
    bounds: TimelineWindow
    range: TimelineWindow
    zoomed: boolean
    count: number
    onChange: (range: TimelineWindow | null) => void
}) {
    const span = range.end - range.start
    const fullSpan = Math.max(bounds.end - bounds.start, span)
    const zoom = (width: number) => {
        const center = (range.start + range.end) / 2
        onChange({ start: center - width / 2, end: center + width / 2 })
    }
    const pan = (direction: number) => onChange({ start: range.start + (direction * span) / 2, end: range.end + (direction * span) / 2 })
    return (
        <div className="request-waterfall-zoom">
            <div role="group" aria-label="Timeline zoom">
                <Button variant="outline" size="icon-sm" aria-label="Zoom out" title="Zoom out" disabled={!zoomed} onClick={() => zoom(span * 2)}>
                    <ZoomOut aria-hidden="true" />
                </Button>
                <label className="request-waterfall-zoom-slider">
                    <span>Zoom</span>
                    <input
                        type="range"
                        min={0}
                        max={100}
                        step={1}
                        value={fullSpan <= 1 ? 0 : Math.round((Math.log(fullSpan / span) / Math.log(fullSpan)) * 100)}
                        aria-valuetext={`${duration(span)} window`}
                        disabled={fullSpan <= 1}
                        onChange={event => {
                            const value = event.currentTarget.valueAsNumber
                            if (value === 0) onChange(null)
                            else zoom(fullSpan ** (1 - value / 100))
                        }}
                    />
                </label>
                <Button variant="outline" size="icon-sm" aria-label="Zoom in" title="Zoom in" disabled={span <= 1} onClick={() => zoom(span / 2)}>
                    <ZoomIn aria-hidden="true" />
                </Button>
            </div>
            <div role="group" aria-label="Timeline navigation">
                <Button variant="outline" size="icon-sm" aria-label="Pan earlier" title="Pan earlier" disabled={range.start <= bounds.start} onClick={() => pan(-1)}>
                    <ChevronLeft aria-hidden="true" />
                </Button>
                <Button variant="outline" size="icon-sm" aria-label="Pan later" title="Pan later" disabled={range.end >= bounds.end} onClick={() => pan(1)}>
                    <ChevronRight aria-hidden="true" />
                </Button>
                <Button variant="ghost" size="sm" aria-label="Reset zoom" disabled={!zoomed} onClick={() => onChange(null)}>
                    <RotateCcw aria-hidden="true" />
                    Reset
                </Button>
            </div>
            <span role="status">
                {duration(span)} window · {count.toLocaleString()} {count === 1 ? "call" : "calls"} in view
            </span>
            <span>Scroll horizontally to pan · Drag across the time axis to zoom</span>
        </div>
    )
}

export function TimelineAxis({ start, range, onChange }: { start: number; range: TimelineWindow; onChange: (range: TimelineWindow) => void }) {
    const [selection, setSelection] = useState<{ from: number; to: number } | null>(null)
    const drag = useRef<{ pointerId: number; x: number; left: number; width: number; range: TimelineWindow } | null>(null)
    const span = range.end - range.start
    const begin = (event: PointerEvent<HTMLDivElement>) => {
        if (event.button !== 0 || !event.isPrimary || span <= 1) return
        const { left, width } = event.currentTarget.getBoundingClientRect()
        if (!width) return
        drag.current = { pointerId: event.pointerId, x: event.clientX, left, width, range }
        event.currentTarget.setPointerCapture(event.pointerId)
        setSelection({ from: fraction(event.clientX, left, width), to: fraction(event.clientX, left, width) })
    }
    const move = (event: PointerEvent<HTMLDivElement>) => {
        const active = drag.current
        if (!active || active.pointerId !== event.pointerId) return
        setSelection({ from: fraction(active.x, active.left, active.width), to: fraction(event.clientX, active.left, active.width) })
    }
    const finish = (event: PointerEvent<HTMLDivElement>) => {
        const active = drag.current
        if (!active || active.pointerId !== event.pointerId) return
        drag.current = null
        setSelection(null)
        event.currentTarget.releasePointerCapture(event.pointerId)
        if (Math.abs(event.clientX - active.x) < 4) return
        const from = fraction(active.x, active.left, active.width)
        const to = fraction(event.clientX, active.left, active.width)
        const width = active.range.end - active.range.start
        onChange({ start: active.range.start + Math.min(from, to) * width, end: active.range.start + Math.max(from, to) * width })
    }
    const cancel = () => {
        drag.current = null
        setSelection(null)
    }
    return (
        <div className="request-waterfall-axis">
            <span>Operation / instance</span>
            <div role="group" aria-label="Select time range" onPointerDown={begin} onPointerMove={move} onPointerUp={finish} onPointerCancel={cancel} onLostPointerCapture={cancel}>
                {timelineTicks.map(tick => (
                    <span key={tick} style={{ left: `${tick * 100}%` }}>
                        {axisOffset(range.start - start + span * tick, span)}
                    </span>
                ))}
                {selection && (
                    <i className="request-waterfall-selection" style={{ left: `${Math.min(selection.from, selection.to) * 100}%`, width: `${Math.abs(selection.to - selection.from) * 100}%` }} />
                )}
            </div>
            <span>Calls / total</span>
        </div>
    )
}

function fitWindow(range: TimelineWindow, bounds: TimelineWindow): TimelineWindow {
    const span = Math.min(bounds.end - bounds.start, Math.max(1, range.end - range.start))
    const start = Math.max(bounds.start, Math.min(range.start, bounds.end - span))
    return { start, end: start + span }
}

function fraction(x: number, left: number, width: number) {
    return Math.max(0, Math.min(1, (x - left) / width))
}

function axisOffset(ms: number, span: number) {
    if (span >= 60_000) return duration(ms)
    return ms >= 1000 ? `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 5 })} s` : `${ms.toLocaleString(undefined, { maximumFractionDigits: 2 })} ms`
}
