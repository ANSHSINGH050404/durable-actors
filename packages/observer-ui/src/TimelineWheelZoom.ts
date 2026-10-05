import { useEffect, useLayoutEffect, useRef } from "react"
import type { RefObject } from "react"

import type { TimelineWindow } from "./TimelineZoom.js"

export function useTimelineWheelZoom(viewport: RefObject<HTMLDivElement | null>, bounds: TimelineWindow, range: TimelineWindow, onChange: (range: TimelineWindow) => void) {
    const current = useRef(range)
    const held = useRef({ control: false, command: false })
    useLayoutEffect(() => {
        current.current = range
    }, [range])

    useEffect(() => {
        const element = viewport.current!
        const document = element.ownerDocument
        const window = document.defaultView!
        const axis = () => element.querySelector(".request-waterfall-axis > div")!.getBoundingClientRect()
        const zoom = (factor: number, clientX: number) => {
            const { left, width } = axis()
            if (!width) return false
            current.current = anchoredWindow(current.current, bounds, factor, Math.max(0, Math.min(1, (clientX - left) / width)))
            onChange(current.current)
            return true
        }
        const wheel = (event: WheelEvent) => {
            // Track the keyboard separately: browsers also mark trackpad pinches as Ctrl+wheel.
            if (!((event.ctrlKey && held.current.control) || (event.metaKey && held.current.command)) || !event.deltaY) return
            const unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? axis().width : 1
            if (zoom(Math.exp(event.deltaY * unit * 0.01), event.clientX)) event.preventDefault()
        }
        const keys = (event: KeyboardEvent) => {
            held.current = { control: event.ctrlKey, command: event.metaKey }
        }
        const clearKeys = () => {
            held.current = { control: false, command: false }
        }
        element.addEventListener("wheel", wheel, { passive: false })
        window.addEventListener("keydown", keys, true)
        window.addEventListener("keyup", keys, true)
        window.addEventListener("blur", clearKeys)
        document.addEventListener("visibilitychange", clearKeys)
        return () => {
            element.removeEventListener("wheel", wheel)
            window.removeEventListener("keydown", keys, true)
            window.removeEventListener("keyup", keys, true)
            window.removeEventListener("blur", clearKeys)
            document.removeEventListener("visibilitychange", clearKeys)
        }
    }, [viewport, bounds.start, bounds.end, onChange])
}

function anchoredWindow(range: TimelineWindow, bounds: TimelineWindow, factor: number, anchor: number): TimelineWindow {
    const span = range.end - range.start
    const width = Math.max(1, Math.min(bounds.end - bounds.start, span * factor))
    const start = Math.max(bounds.start, Math.min(range.start + (span - width) * anchor, bounds.end - width))
    return { start, end: start + width }
}
