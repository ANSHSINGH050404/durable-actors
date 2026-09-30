import React from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { ObserverClient, RequestTracePage } from "../src/client.js"

import "./dom.js"

const { cleanup, fireEvent, render, within } = await import("@testing-library/react")
const { ActorObserver } = await import("../src/ActorObserver.js")
afterEach(cleanup)

const page: RequestTracePage = {
    epoch: "one",
    cursor: 1,
    capacity: 500,
    evicted: 0,
    dropped: 0,
    records: [
        {
            sequence: 1,
            projectId: "local",
            requestId: "request-one",
            hostId: "host",
            sessionId: "session",
            actorName: "Room",
            actorId: "general",
            kind: "method",
            operation: "save",
            connectionId: null,
            startedAtMs: 1000,
            durationMs: 40,
            queueWaitMs: 10,
            outcome: "completed"
        }
    ]
}

function client(): ObserverClient {
    return {
        checkConnection: async () => {},
        listActors: async () => ({
            actors: [{ actorName: "Room", live: 0, dormant: 1, unknown: 0, instances: [{ actorId: "general", status: "dormant", connections: [{ id: "socket-one", metadata: null }], waiting: [] }] }]
        }),
        listQueueWaits: async () => [{ actorName: "Room", actorId: "general", admitted: 2, averageMs: 10, maxMs: 15 }],
        watchRequests: async (receive, signal) => {
            receive(page)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        listRequests: async () => page,
        getState: async () => ({
            snapshot: { stateVersion: 1, ownerEpoch: 1, requestId: "request-one", state: { count: 1 }, attribution: { operation: "save", committedAtMs: 1000, interleaved: false } },
            schema: null
        }),
        listStateHistory: async () => ({ records: [], nextBefore: null })
    }
}

async function inspect(source = client()) {
    const view = render(<ActorObserver client={source} initialActorName="Room" />)
    fireEvent.click(await view.findByRole("button", { name: "general" }))
    return view
}

test("instance header identifies and focuses the instance, with breadcrumb navigation and queue metrics", async () => {
    const view = await inspect()
    const heading = view.getByRole("heading", { name: "general", level: 1 })
    assert.ok(document.activeElement === heading)
    assert.ok(within(heading.parentElement!).getByText("Dormant"))
    const metrics = await view.findByRole("region", { name: "Queue wait" })
    assert.equal(within(metrics).getByLabelText("Average queue wait").textContent, "10 ms")
    assert.equal(within(metrics).getByLabelText("Longest queue wait").textContent, "15 ms")
    assert.equal(within(metrics).getByLabelText("Admitted requests").textContent, "2")
    assert.ok(view.getByText("No requests waiting."))
    fireEvent.click(within(view.getByRole("navigation", { name: "Breadcrumb" })).getByRole("button", { name: "Back to instances" }))
    assert.ok(view.getByRole("table", { name: "Room instances" }))
    assert.ok(document.activeElement === view.getByRole("heading", { name: "Room", level: 1 }))
})

test("instance views open on requests and provide state and connection inspection", async () => {
    const view = await inspect()
    assert.ok(await view.findByRole("group", { name: "Invocation waterfall" }))
    const controls = within(view.getByRole("group", { name: "Instance view" }))
    assert.equal(controls.getByRole("button", { name: "Requests" }).getAttribute("aria-pressed"), "true")
    fireEvent.click(controls.getByRole("button", { name: "State" }))
    assert.ok(await view.findByRole("region", { name: "Persisted state" }))
    assert.ok(await view.findByText("count"))
    fireEvent.click(controls.getByRole("button", { name: /^WebSockets/ }))
    assert.ok(view.getByRole("table", { name: "general WebSocket connections" }))
    fireEvent.click(controls.getByRole("button", { name: "Requests" }))
    assert.ok(await view.findByRole("group", { name: "Invocation waterfall" }))
})

test("a state attribution link opens the corresponding request history", async () => {
    const source = client()
    const queries: unknown[] = []
    source.listRequests = async query => {
        queries.push(query)
        return page
    }
    const view = await inspect(source)
    const controls = within(view.getByRole("group", { name: "Instance view" }))
    fireEvent.click(controls.getByRole("button", { name: "State" }))
    fireEvent.click(await view.findByRole("button", { name: "request-one" }))
    assert.ok(await view.findByText("Showing request request-one"))
    assert.ok(queries.some(query => (query as { requestId: string }).requestId === "request-one"))
    assert.ok(document.activeElement === controls.getByRole("button", { name: "Requests" }))
})

test("switching instance views preserves request inspection controls", async () => {
    const view = await inspect()
    await view.findByRole("group", { name: "Invocation waterfall" })
    fireEvent.click(view.getByRole("button", { name: "Pause" }))
    fireEvent.click(view.getByRole("button", { name: "Table" }))
    const controls = within(view.getByRole("group", { name: "Instance view" }))
    fireEvent.click(controls.getByRole("button", { name: "State" }))
    fireEvent.click(controls.getByRole("button", { name: "Requests" }))
    assert.ok(view.getByRole("button", { name: "Resume" }))
    assert.ok(view.getByRole("table", { name: "Recent requests" }))
})
