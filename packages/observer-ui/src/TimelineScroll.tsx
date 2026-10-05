import { useEffect, useLayoutEffect, useRef } from "react"
import type { ReactNode } from "react"

import { useTimelineWheelZoom } from "./TimelineWheelZoom.js"
import type { TimelineWindow } from "./TimelineZoom.js"

export function TimelineScroll({ bounds, range, onChange, children }: { bounds: TimelineWindow; range: TimelineWindow; onChange: (range: TimelineWindow) => void; children: ReactNode }) {
    const viewport = useRef<HTMLDivElement>(null)
    const scrollbar = useRef<HTMLDivElement>(null)
    const position = useRef(0)
    const start = Math.min(bounds.start, range.start)
    const end = Math.max(bounds.end, range.end)
    const span = range.end - range.start
    const travel = end - start - span
    useTimelineWheelZoom(viewport, bounds, range, onChange)

    useLayoutEffect(() => {
        const element = scrollbar.current!
        const sync = () => {
            element.scrollLeft = travel > 0 ? ((range.start - start) / travel) * (element.scrollWidth - element.clientWidth) : 0
            position.current = element.scrollLeft
        }
        sync()
        const observer = new ResizeObserver(sync)
        observer.observe(element)
        return () => observer.disconnect()
    }, [range.start, start, span, travel])

    useEffect(() => {
        const element = viewport.current!
        const wheel = (event: WheelEvent) => {
            const delta = event.deltaX || (event.shiftKey ? event.deltaY : 0)
            if (!delta || event.ctrlKey || event.metaKey || travel <= 0 || element.scrollWidth > element.clientWidth) return
            const width = element.querySelector(".request-waterfall-axis > div")!.getBoundingClientRect().width
            if (!width) return
            const unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? width : 1
            const next = Math.max(bounds.start, Math.min(range.start + (delta * unit * span) / width, bounds.end - span))
            if (next === range.start) return
            event.preventDefault()
            onChange({ start: next, end: next + span })
        }
        element.addEventListener("wheel", wheel, { passive: false })
        return () => element.removeEventListener("wheel", wheel)
    }, [bounds.start, bounds.end, range.start, span, travel, onChange])

    return (
        <>
            <div ref={viewport} className="request-waterfall-scroll" tabIndex={0} aria-label="Invocation timeline, scroll for more calls">
                {children}
            </div>
            <div
                ref={scrollbar}
                className="request-waterfall-pan"
                role="region"
                aria-label="Scroll request timeline"
                tabIndex={0}
                hidden={travel <= 0}
                onScroll={event => {
                    const element = event.currentTarget
                    const distance = element.scrollWidth - element.clientWidth
                    if (distance <= 0 || element.scrollLeft === position.current) return
                    position.current = element.scrollLeft
                    const next = start + (element.scrollLeft / distance) * travel
                    onChange({ start: next, end: next + span })
                }}
            >
                <div style={{ width: `${((end - start) / span) * 100}%` }} />
            </div>
        </>
    )
}
