import React from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { RequestTrace } from "../src/client.js"

import "./dom.js"

const { cleanup, fireEvent, render, within } = await import("@testing-library/react")
const { RequestTimeline } = await import("../src/RequestTimeline.js")
afterEach(cleanup)

const trace: RequestTrace = {
    sequence: 1,
    projectId: "local",
    requestId: "short",
    hostId: "host",
    sessionId: "session",
    actorName: "Room",
    actorId: "lobby",
    kind: "method",
    operation: "load",
    connectionId: null,
    startedAtMs: 1500,
    durationMs: 4,
    queueWaitMs: 1,
    hostState: "warm",
    outcome: "completed"
}
const records = [{ ...trace, sequence: 0, startedAtMs: 1000, operation: "early" }, trace, { ...trace, sequence: 2, startedAtMs: 1996, operation: "late" }]

test("the zoom slider continuously scales the centered window and resets to all calls", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    const slider = view.getByRole("slider", { name: "Zoom" }) as HTMLInputElement
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    assert.equal(slider.value, "0")
    assert.equal(slider.getAttribute("aria-valuetext"), "1 s window")
    fireEvent.change(slider, { target: { value: "50" } })
    assert.ok(Math.abs(parseFloat(bar.style.width) - (4 / Math.sqrt(1000)) * 100) < 0.001)
    assert.equal(bar.style.left, "50%")
    assert.equal(view.getAllByRole("button", { name: /Inspect .* request/ }).length, 1)
    fireEvent.change(slider, { target: { value: "100" } })
    assert.equal(view.getByRole("status").textContent, "1 ms window · 1 call in view")
    assert.equal(view.getByRole("button", { name: "Zoom in" }).hasAttribute("disabled"), true)
    fireEvent.click(view.getByRole("button", { name: "Zoom out" }))
    assert.ok(Number(slider.value) < 100)
    fireEvent.change(slider, { target: { value: "0" } })
    assert.equal(view.getAllByRole("button", { name: /Inspect .* request/ }).length, 3)
    assert.equal(view.getByRole("button", { name: "Reset zoom" }).hasAttribute("disabled"), true)
})

test("slider zoom preserves timestamps across history updates and reset resumes fitting", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    const slider = view.getByRole("slider", { name: "Zoom" }) as HTMLInputElement
    fireEvent.change(slider, { target: { value: "50" } })
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    const width = bar.style.width
    view.rerender(<RequestTimeline records={[...records, { ...trace, sequence: 3, startedAtMs: 5000 }]} onSelect={() => {}} />)
    assert.equal(bar.style.width, width)
    assert.equal(bar.style.left, "50%")
    assert.ok(Number(slider.value) > 50)
    view.rerender(<RequestTimeline records={[trace]} onSelect={() => {}} />)
    assert.equal(bar.style.width, width)
    fireEvent.click(view.getByRole("button", { name: "Reset zoom" }))
    view.rerender(<RequestTimeline records={records} onSelect={() => {}} />)
    assert.equal(bar.style.width, "0.4%")
    assert.equal(slider.value, "0")
})

test("the zoom slider is disabled for a one millisecond history", () => {
    const view = render(<RequestTimeline records={[{ ...trace, durationMs: 0 }]} onSelect={() => {}} />)
    const slider = view.getByRole("slider", { name: "Zoom" }) as HTMLInputElement
    assert.equal(slider.disabled, true)
    assert.equal(slider.value, "0")
    assert.equal(slider.getAttribute("aria-valuetext"), "1 ms window")
})

test("scrolling the timeline pans to either end and keeps calls selectable", () => {
    let selected: RequestTrace | undefined
    const view = render(<RequestTimeline records={records} onSelect={record => (selected = record)} />)
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    const scroll = view.getByRole("region", { name: "Scroll request timeline" })
    Object.defineProperties(scroll, { clientWidth: { value: 1000 }, scrollWidth: { value: 2000 } })
    fireEvent.scroll(scroll, { target: { scrollLeft: 1000 } })
    const late = view.getByRole("button", { name: /Inspect late request/ })
    assert.equal(late.style.left, "99.2%")
    fireEvent.click(late)
    assert.equal(selected, records[2])
    assert.equal(view.getByRole("status").textContent, "500 ms window · 2 calls in view")
    fireEvent.scroll(scroll, { target: { scrollLeft: 0 } })
    assert.ok(view.getByRole("button", { name: /Inspect early request/ }))
    assert.equal(view.queryByRole("button", { name: /Inspect late request/ }), null)
    fireEvent.click(view.getByRole("button", { name: "Reset zoom" }))
    assert.equal(view.queryByRole("region", { name: "Scroll request timeline" }), null)
    assert.equal(view.getAllByRole("button", { name: /Inspect .* request/ }).length, 3)
})

test("horizontal and Shift-wheel scrolling pan the window without consuming vertical scroll or pinch zoom", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    const viewport = view.getByLabelText("Invocation timeline, scroll for more calls")
    const axis = view.getByRole("group", { name: "Select time range" })
    axis.getBoundingClientRect = () => ({ width: 1000 }) as DOMRect
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    assert.equal(fireEvent.wheel(viewport, { deltaX: 500, cancelable: true }), false)
    assert.ok(view.getByRole("button", { name: /Inspect late request/ }))
    assert.equal(fireEvent.wheel(viewport, { deltaY: -1000, shiftKey: true, cancelable: true }), false)
    assert.ok(view.getByRole("button", { name: /Inspect early request/ }))
    const early = view.getByRole("button", { name: /Inspect early request/ })
    assert.equal(fireEvent.wheel(viewport, { deltaY: 500, cancelable: true }), true)
    assert.equal(fireEvent.wheel(viewport, { deltaX: 500, ctrlKey: true, cancelable: true }), true)
    assert.equal(early.style.left, "0%")
    assert.equal(fireEvent.wheel(viewport, { deltaX: -500, cancelable: true }), true)
    assert.equal(fireEvent.wheel(viewport, { deltaX: 1, deltaMode: 2, cancelable: true }), false)
    assert.ok(view.getByRole("button", { name: /Inspect late request/ }))
})

test("scroll position follows zoom and live history without moving the chosen timestamps", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    const scroll = view.getByRole("region", { name: "Scroll request timeline" })
    Object.defineProperties(scroll, { clientWidth: { value: 1000 }, scrollWidth: { value: 2000 } })
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    assert.equal(scroll.scrollLeft, 500)
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    view.rerender(<RequestTimeline records={[{ ...trace, sequence: -1, startedAtMs: 0 }, ...records]} onSelect={() => {}} />)
    assert.ok(scroll.scrollLeft > 500)
    fireEvent.scroll(scroll)
    assert.equal(bar.style.left, "50%")
    assert.equal(bar.style.width, "1.6%")
})

test("zoom enlarges short requests, keeps them selectable, pans and resets the full range", () => {
    let selected: RequestTrace | undefined
    const view = render(<RequestTimeline records={records} onSelect={record => (selected = record)} />)
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    assert.equal(bar.style.width, "0.4%")
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    assert.equal(bar.style.width, "0.8%")
    assert.equal(bar.style.left, "50%")
    assert.equal(view.getByRole("status").textContent, "500 ms window · 1 call in view")
    assert.equal(view.queryByRole("button", { name: /Inspect early request/ }), null)
    fireEvent.click(bar)
    assert.equal(selected, trace)
    fireEvent.click(view.getByRole("button", { name: "Pan later" }))
    assert.ok(view.getByRole("button", { name: /Inspect late request/ }))
    assert.equal(view.getByRole("button", { name: "Pan later" }).hasAttribute("disabled"), true)
    fireEvent.click(view.getByRole("button", { name: "Pan earlier" }))
    fireEvent.click(view.getByRole("button", { name: "Zoom out" }))
    assert.equal(bar.style.width, "0.4%")
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    fireEvent.click(view.getByRole("button", { name: "Reset zoom" }))
    assert.equal(view.getAllByRole("button", { name: /Inspect .* request/ }).length, 3)
    assert.equal(bar.style.width, "0.4%")
})

test("a zoomed window stays on the same timestamps as newer and older records arrive", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    view.rerender(<RequestTimeline records={[{ ...trace, sequence: -1, operation: "older", startedAtMs: 0 }, ...records, { ...trace, sequence: 3, startedAtMs: 5000 }]} onSelect={() => {}} />)
    assert.equal(bar.style.left, "50%")
    assert.equal(bar.style.width, "0.8%")
    assert.equal(view.getAllByRole("button", { name: /Inspect .* request/ }).length, 1)
    view.rerender(<RequestTimeline records={[{ ...trace, startedAtMs: 9000 }]} onSelect={() => {}} />)
    assert.ok(view.getByRole("button", { name: /Inspect load request/ }))
    assert.equal(view.getByRole("button", { name: "Reset zoom" }).hasAttribute("disabled"), true)
})

test("zoom clips calls and queue wait to the window without changing reported durations", () => {
    const crossing = { ...trace, startedAtMs: 1100, durationMs: 700, queueWaitMs: 300 }
    const view = render(<RequestTimeline records={[records[0]!, crossing, records[2]!]} onSelect={() => {}} />)
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    const bar = view.getByRole("button", { name: /Inspect load request.*700 ms/ })
    assert.equal(bar.style.left, "0%")
    assert.equal(bar.style.width, "100%")
    assert.equal(bar.querySelector<HTMLElement>(".request-waterfall-queue")!.style.width, "30%")
    fireEvent.click(view.getByRole("button", { name: "Pan later" }))
    assert.equal(bar.style.width, "60%")
    assert.equal(bar.querySelector(".request-waterfall-queue"), null)
})

test("retention changes preserve a zoomed window while any of it still overlaps loaded history", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    for (const retained of [[trace, records[2]!], [trace], [trace, { ...records[2]!, startedAtMs: 5000 }]]) {
        view.rerender(<RequestTimeline records={retained} onSelect={() => {}} />)
        assert.equal(bar.style.left, "50%")
        assert.equal(bar.style.width, "0.8%")
        assert.equal(view.getByRole("status").textContent, "500 ms window · 1 call in view")
        assert.equal(view.getByRole("button", { name: "Reset zoom" }).hasAttribute("disabled"), false)
    }
})

for (const control of ["Reset zoom", "Zoom out"]) {
    test(`${control} restores automatic fitting when retained history matches the selected window`, () => {
        const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
        fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
        view.rerender(<RequestTimeline records={[{ ...records[0]!, startedAtMs: 1250 }, trace, { ...records[2]!, startedAtMs: 1746 }]} onSelect={() => {}} />)
        const button = view.getByRole("button", { name: control })
        assert.equal(button.hasAttribute("disabled"), false)
        fireEvent.click(button)
        view.rerender(<RequestTimeline records={records} onSelect={() => {}} />)
        assert.equal(view.getByRole("button", { name: /Inspect load request/ }).style.width, "0.4%")
        assert.equal(view.getByRole("button", { name: "Reset zoom" }).hasAttribute("disabled"), true)
    })
}

test("dragging the axis in either direction zooms precisely and cancelled drags leave it unchanged", () => {
    const view = render(<RequestTimeline records={records} onSelect={() => {}} />)
    const axis = view.getByRole("group", { name: "Select time range" })
    axis.getBoundingClientRect = () => ({ left: 100, width: 1000 }) as DOMRect
    axis.setPointerCapture = () => {}
    axis.releasePointerCapture = () => {}
    pointer(axis, "pointerdown", 580)
    pointer(axis, "pointermove", 620)
    pointer(axis, "pointerup", 620)
    const bar = view.getByRole("button", { name: /Inspect load request/ })
    assert.equal(bar.style.width, "10%")
    assert.equal(bar.style.left, "50%")
    pointer(axis, "pointerdown", 400)
    pointer(axis, "pointermove", 600)
    pointer(axis, "pointercancel", 600)
    assert.equal(bar.style.width, "10%")
    fireEvent.click(view.getByRole("button", { name: "Reset zoom" }))
    pointer(axis, "pointerdown", 620)
    pointer(axis, "pointermove", 580)
    pointer(axis, "pointerup", 580)
    assert.equal(bar.style.width, "10%")
})

test("deep zoom has distinct axis labels and a one millisecond limit, including instant requests", () => {
    const view = render(
        <RequestTimeline
            records={[
                { ...trace, startedAtMs: 0 },
                { ...trace, sequence: 2, startedAtMs: 600_000, durationMs: 0, queueWaitMs: null }
            ]}
            onSelect={() => {}}
        />
    )
    for (let i = 0; i < 24; i++) fireEvent.click(view.getByRole("button", { name: "Zoom in" }))
    assert.equal(view.getByRole("button", { name: "Zoom in" }).hasAttribute("disabled"), true)
    const axis = view.getByRole("group", { name: "Select time range" })
    const labels = [...axis.querySelectorAll("span")].map(label => label.textContent)
    assert.equal(new Set(labels).size, 5)
    assert.ok(view.getByText("No calls in this time range. Pan or zoom out to find calls."))
    assert.doesNotMatch(view.container.innerHTML, /NaN|Infinity/)
    view.rerender(<RequestTimeline records={[{ ...trace, startedAtMs: 0, durationMs: 0, queueWaitMs: null }]} onSelect={() => {}} />)
    assert.equal(within(view.container).getAllByRole("button", { name: /Inspect .* request/ }).length, 1)
    assert.equal(view.getByRole("button", { name: "Zoom in" }).hasAttribute("disabled"), true)
})

function pointer(element: HTMLElement, type: string, clientX: number) {
    const event = new MouseEvent(type, { bubbles: true, clientX, button: 0 })
    Object.assign(event, { pointerId: 1, isPrimary: true })
    fireEvent(element, event)
}
