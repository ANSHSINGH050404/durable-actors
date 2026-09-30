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

test("waterfall places calls chronologically on one scale with queue time and gaps", async () => {
    const { client } = fixture([{ ...trace, sequence: 2, requestId: "second", operation: "save", startedAtMs: 1300, durationMs: 100, outcome: "failed" }, trace])
    const view = render(<RequestObserver client={client} />)
    const timeline = await view.findByRole("group", { name: "Invocation waterfall" })
    const calls = within(timeline).getAllByRole("button", { name: /Inspect .* request/ })
    assert.deepEqual(
        calls.map(call => call.querySelector("strong")?.textContent),
        ["load", "save"]
    )
    const bars = timeline.querySelectorAll<HTMLElement>(".request-waterfall-bar")
    assert.equal(bars[0]!.style.left, "0%")
    assert.equal(bars[0]!.style.width, "25%")
    assert.equal(bars[1]!.style.left, "75%")
    assert.equal(timeline.querySelector<HTMLElement>(".request-waterfall-bar > .request-waterfall-queue")!.style.width, "20%")
    assert.ok(within(timeline).getByText("200 ms gap"))
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
    assert.ok(within(timeline).getByText("Overlapping"))
    assert.ok(within(timeline).getByText("100 ms gap"))
    assert.equal(within(timeline).getAllByText("First in view").length, 2)
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
