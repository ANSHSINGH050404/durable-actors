import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { RequestTrace, RequestTracePage } from "../src/client.js"

import "./dom.js"

const { cleanup, fireEvent, render, within, waitFor } = await import("@testing-library/react")
const { RequestObserver } = await import("../src/RequestObserver.js")
afterEach(cleanup)

const trace: RequestTrace = {
    sequence: 1,
    projectId: "local",
    requestId: "first",
    hostId: "host",
    sessionId: "session",
    actorName: "Room",
    actorId: "lobby",
    kind: "method",
    operation: "load",
    connectionId: null,
    startedAtMs: 1000,
    durationMs: 100,
    queueWaitMs: 20,
    outcome: "completed"
}

function fixture(records: RequestTrace[]) {
    let publish!: (page: RequestTracePage) => void
    const page: RequestTracePage = { epoch: "one", cursor: 100, capacity: 500, evicted: 0, dropped: 0, records }
    const client = {
        watchRequests: async (receive: typeof publish, signal: AbortSignal) => {
            publish = receive
            receive(page)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        listRequests: async () => page
    }
    return { client, publish: (records: RequestTrace[]) => publish({ ...page, records }) }
}

test("repeated methods share a row while interleaved calls remain individually selectable", async () => {
    const source = fixture([{ ...trace, sequence: 3, requestId: "third", startedAtMs: 1300 }, trace, { ...trace, sequence: 2, requestId: "second", operation: "save", startedAtMs: 1050 }])
    const view = render(<RequestObserver client={source.client} />)
    const timeline = await view.findByRole("group", { name: "Invocation waterfall" })
    const loads = within(timeline).getAllByRole("button", { name: /Inspect load request/ })
    const row = loads[0]!.closest(".request-waterfall-row")!
    assert.ok(loads[1]!.closest(".request-waterfall-row") === row, "Repeated calls share one row")
    assert.equal(timeline.querySelectorAll(".request-waterfall-row").length, 2)
    assert.ok(within(row as HTMLElement).getByText("2 calls"))
    assert.equal((loads[0] as HTMLElement).style.left, "0%")
    assert.equal((loads[1] as HTMLElement).style.left, "75%")
    assert.match(loads[1]!.getAttribute("aria-label")!, /150 ms gap/)
    for (const [index, requestId] of ["first", "third"].entries()) {
        fireEvent.click(loads[index]!)
        const details = await view.findByRole("dialog", { name: "Request details" })
        assert.ok(within(details).getByText(requestId))
        assert.equal(loads[index]!.getAttribute("data-state"), "selected")
        fireEvent.click(view.getByRole("button", { name: "Close" }))
        await waitFor(() => assert.ok(document.activeElement === loads[index], "Closing details restores focus to the selected call"))
    }
    await act(async () => source.publish([{ ...trace, sequence: 4, requestId: "fourth", startedAtMs: 1400 }]))
    assert.equal(timeline.querySelectorAll(".request-waterfall-row").length, 2)
    assert.equal(within(row as HTMLElement).getAllByRole("button").length, 3)
    assert.ok(within(row as HTMLElement).getByText("3 calls"))
})

test("method rows keep different projects, actor classes, instances and event kinds separate", async () => {
    const { client } = fixture([
        trace,
        { ...trace, sequence: 2, projectId: "other" },
        { ...trace, sequence: 3, actorName: "Workspace" },
        { ...trace, sequence: 4, actorId: "other" },
        { ...trace, sequence: 5, kind: "websocket" },
        { ...trace, sequence: 6, requestId: "overlap", startedAtMs: 1050 }
    ])
    const view = render(<RequestObserver client={client} />)
    const timeline = await view.findByRole("group", { name: "Invocation waterfall" })
    const rows = timeline.querySelectorAll(".request-waterfall-row")
    assert.equal(rows.length, 5)
    assert.equal(within(rows[0] as HTMLElement).getAllByRole("button").length, 2)
    assert.ok(within(rows[0] as HTMLElement).getByRole("button", { name: /Overlapping/ }))
})

test("request details open a right-side drawer from the waterfall and table, then restore focus on Escape", async () => {
    const { client } = fixture([trace])
    const view = render(<RequestObserver client={client} />)
    await view.findByRole("group", { name: "Invocation waterfall" })
    for (const layout of ["Waterfall", "Table"]) {
        fireEvent.click(view.getByRole("button", { name: layout }))
        const trigger = view.getByRole("button", { name: /Inspect load request/ })
        fireEvent.click(trigger)
        const drawer = await view.findByRole("dialog", { name: "Request details" })
        assert.equal(drawer.getAttribute("data-vaul-drawer-direction"), "right")
        assert.equal(drawer.getAttribute("data-slot"), "drawer-content")
        assert.ok(view.getByRole("region", { name: "Request observer", hidden: true }).contains(drawer), "The portal retains the observer's theme scope")
        assert.ok(within(drawer).getByText("first"))
        await waitFor(() => assert.ok(drawer.contains(document.activeElement), "Opening the drawer moves focus inside"))
        fireEvent.keyDown(drawer, { key: "Escape" })
        await waitFor(() => assert.equal(view.queryByRole("dialog"), null))
        await waitFor(() => assert.ok(document.activeElement === trigger, "Escape returns focus to the invocation"))
    }
})

test("waterfall places calls chronologically on one scale with queue time and gaps", async () => {
    const { client } = fixture([{ ...trace, sequence: 2, requestId: "second", operation: "save", startedAtMs: 1300, durationMs: 100, outcome: "failed" }, trace])
    const view = render(<RequestObserver client={client} />)
    const timeline = await view.findByRole("group", { name: "Invocation waterfall" })
    const calls = within(timeline).getAllByRole("button", { name: /Inspect .* request/ })
    assert.deepEqual(
        [...timeline.querySelectorAll(".request-waterfall-label strong")].map(label => label.textContent),
        ["load", "save"]
    )
    const bars = timeline.querySelectorAll<HTMLElement>(".request-waterfall-bar")
    assert.equal(bars[0]!.style.left, "0%")
    assert.equal(bars[0]!.style.width, "25%")
    assert.equal(bars[1]!.style.left, "75%")
    assert.equal(timeline.querySelector<HTMLElement>(".request-waterfall-bar > .request-waterfall-queue")!.style.width, "20%")
    assert.match(calls[1]!.getAttribute("aria-label")!, /200 ms gap/)
    assert.match(calls[1]!.getAttribute("aria-label")!, /failed/)
    fireEvent.click(calls[1]!)
    assert.ok(await view.findByRole("dialog", { name: "Request details" }))
    assert.ok(view.getByText("second"))
    assert.ok(within(view.getByRole("dialog", { name: "Request details" })).getByText("300 ms"))
    fireEvent.click(view.getByRole("button", { name: "Close" }))
    await waitFor(() => assert.ok(document.activeElement === calls[1], "Closing details restores focus to the call"))
})

test("gaps follow the latest finish on the same actor, including overlapping requests", async () => {
    const { client } = fixture([
        { ...trace, durationMs: 500 },
        { ...trace, sequence: 2, operation: "overlap", startedAtMs: 1100, durationMs: 50 },
        { ...trace, sequence: 3, actorId: "other", operation: "another actor", startedAtMs: 1200 },
        { ...trace, sequence: 4, operation: "after", startedAtMs: 1600 }
    ])
    const view = render(<RequestObserver client={client} />)
    const timeline = await view.findByRole("group", { name: "Invocation waterfall" })
    assert.ok(within(timeline).getByRole("button", { name: /Overlapping/ }))
    assert.ok(within(timeline).getByRole("button", { name: /100 ms gap/ }))
    assert.equal(within(timeline).getAllByRole("button", { name: /First in view/ }).length, 2)
})

test("waterfall handles instant requests, unavailable queue timings and every outcome", async () => {
    const outcomes: RequestTrace["outcome"][] = ["completed", "failed", "rejected", "rerouted", "interrupted"]
    const { client } = fixture(outcomes.map((outcome, sequence) => ({ ...trace, sequence, outcome, durationMs: 0, queueWaitMs: null })))
    const view = render(<RequestObserver client={client} />)
    const timeline = await view.findByRole("group", { name: "Invocation waterfall" })
    assert.equal(within(timeline).getAllByRole("button").length, 5)
    assert.doesNotMatch(timeline.innerHTML, /NaN|Infinity/)
    assert.equal(timeline.querySelectorAll(".request-waterfall-bar > .request-waterfall-queue").length, 0)
    for (const outcome of outcomes) assert.ok(within(timeline).getByRole("button", { name: new RegExp(`${outcome},`) }))
})

test("actor waterfall respects scope, pause, history and the table toggle", async () => {
    const source = fixture([trace, { ...trace, sequence: 2, actorId: "elsewhere", operation: "other" }])
    const view = render(<RequestObserver client={source.client} actor={{ actorName: "Room", actorId: "lobby" }} />)
    await view.findByRole("group", { name: "Invocation waterfall" })
    assert.equal(view.queryByText("other"), null)
    fireEvent.click(view.getByRole("button", { name: "Pause" }))
    await act(async () => source.publish([{ ...trace, sequence: 3, operation: "update", startedAtMs: 1200 }]))
    assert.equal(view.queryByText("update"), null)
    fireEvent.click(view.getByRole("button", { name: "Resume" }))
    assert.ok(view.getByText("update"))
    fireEvent.click(view.getByRole("button", { name: "Table" }))
    assert.ok(view.getByRole("table", { name: "Recent requests" }))
    fireEvent.click(view.getByRole("button", { name: "Waterfall" }))
    fireEvent.click(view.getByRole("button", { name: "History" }))
    await view.findByText("load")
    assert.equal(view.queryByText("update"), null)
    assert.ok(view.getByRole("group", { name: "Invocation waterfall" }))
})
