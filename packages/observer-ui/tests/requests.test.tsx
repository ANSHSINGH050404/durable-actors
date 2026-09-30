import React, { act } from "react"

import { cleanup, fireEvent, render, waitFor } from "@testing-library/react"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { HttpObserverClient, isTrace } from "../src/client.js"
import type { RequestHistoryQuery, RequestTracePage } from "../src/client.js"

import "./dom.js"

const { RequestObserver } = await import("../src/RequestObserver.js")
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
            projectId: "default",
            requestId: "request-one",
            hostId: "host-one",
            sessionId: "session-one",
            actorName: "Room",
            actorId: "lobby",
            kind: "method",
            operation: "post",
            connectionId: null,
            startedAtMs: 1000,
            durationMs: 25,
            queueWaitMs: 10,
            outcome: "completed"
        }
    ]
}

test("request traces require an explicit valid project ID", () => {
    for (const projectId of [undefined, "", "bad/project"]) assert.equal(isTrace({ ...page.records[0], projectId }), false)
    assert.equal(isTrace({ ...page.records[0], projectId: "hosted-project" }), true)
})

test("request history shows both timings, deduplicates replay, and can pause for inspection", async () => {
    let publish!: (page: RequestTracePage) => void
    const client = {
        watchRequests: async (receive: typeof publish, signal: AbortSignal) => {
            publish = receive
            receive(page)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<RequestObserver client={client} />)
    fireEvent.click(view.getByRole("button", { name: "Table" }))
    assert.ok(await view.findByText("25 ms"))
    assert.ok(view.getByText("10 ms"))
    await act(async () => publish(page))
    assert.equal(view.getAllByRole("button", { name: "Inspect post request" }).length, 1)
    fireEvent.click(view.getByRole("button", { name: "Pause" }))
    const next = {
        ...page,
        cursor: 2,
        dropped: 3,
        records: [{ ...page.records[0]!, sequence: 2, requestId: "request-two", operation: "onMessage", kind: "websocket" as const, outcome: "failed" as const }]
    }
    await act(async () => publish(next))
    assert.equal(view.queryByText("onMessage"), null)
    fireEvent.click(view.getByRole("button", { name: "Resume" }))
    assert.ok(view.getByText("onMessage"))
    assert.match(view.getByRole("alert").textContent!, /3.*not delivered/u)
    fireEvent.click(view.getByRole("button", { name: "Inspect onMessage request" }))
    assert.ok(view.getByText("request-two"))
    await act(async () => publish({ ...page, epoch: "restarted" }))
    assert.equal(view.queryByText("request-two"), null)
    assert.equal(view.getAllByRole("button", { name: "Inspect post request" }).length, 1)
})

test("persistence failures warn without hiding live requests", async () => {
    const client = {
        watchRequests: async (receive: (page: RequestTracePage) => void, signal: AbortSignal) => {
            receive({ ...page, persistenceFailed: true })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<RequestObserver client={client} />)
    fireEvent.click(view.getByRole("button", { name: "Table" }))
    assert.ok(await view.findByText("25 ms"))
    assert.match(view.getByRole("alert").textContent!, /could not be saved.*incomplete/u)
})

test("request streams validate records and use the trace endpoint", async () => {
    const controller = new AbortController()
    const client = new HttpObserverClient("/api/observe", async url => {
        assert.equal(url, "/api/observe/requests/events")
        return new Response(`event: requests\ndata: ${JSON.stringify(page)}\n\n`, { headers: { "content-type": "text/event-stream" } })
    })
    await client.watchRequests(value => {
        assert.deepEqual(value, page)
        controller.abort()
    }, controller.signal)
    const invalid = { ...page, records: [{ ...page.records[0], queueWaitMs: 100 }] }
    const broken = new HttpObserverClient("/api/observe", async () => new Response(`event: requests\ndata: ${JSON.stringify(invalid)}\n\n`, { headers: { "content-type": "text/event-stream" } }))
    await assert.rejects(broken.watchRequests(() => assert.fail("invalid trace accepted"), new AbortController().signal))
})

function historyPage(records = page.records, nextCursor?: string): RequestTracePage {
    return { ...page, cursor: 200, records, nextCursor }
}

test("saved history sends typed filters and loads older pages without mixing live rows", async () => {
    const queries: RequestHistoryQuery[] = []
    const client = {
        watchRequests: async (receive: (page: RequestTracePage) => void, signal: AbortSignal) => {
            receive(page)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        listRequests: async (query: RequestHistoryQuery) => {
            queries.push(query)
            if (queries.length === 1)
                return historyPage(
                    Array.from({ length: 100 }, (_, i) => ({ ...page.records[0]!, sequence: 200 - i, eventId: `saved-${i}`, operation: `saved call ${i}` })),
                    "older-page"
                )
            return historyPage([{ ...page.records[0]!, sequence: 100, eventId: "older", operation: "older call" }])
        }
    }
    const view = render(<RequestObserver client={client} />)
    await view.findByText("post")
    fireEvent.click(view.getByRole("button", { name: "History" }))
    await view.findByText("saved call 0")
    assert.equal(view.queryByText("post"), null)
    fireEvent.click(view.getByRole("button", { name: "Load older" }))
    await view.findByText("older call")
    assert.ok(view.getByText("saved call 0"))
    assert.ok(queries[0]!.fromMs! >= Date.now() - 62 * 60_000 && queries[0]!.fromMs! <= Date.now() - 60 * 60_000)
    assert.equal(queries[1]!.cursor, "older-page")
    assert.equal(queries[1]!.fromMs, queries[0]!.fromMs)
    fireEvent.click(view.getByRole("button", { name: "Live" }))
    await view.findByText("post")
    assert.equal(view.queryByText("saved call 0"), null)
})

test("HTTP history encodes filters without exposing credentials", async () => {
    const client = new HttpObserverClient("/api/observe", async (url, options) => {
        assert.equal(url, "/api/observe/requests?actorId=a%2Fb&limit=100&cursor=page%2Btoken")
        assert.equal(options?.method, "GET")
        assert.equal(new Headers(options?.headers).get("authorization"), null)
        return Response.json(page)
    })
    assert.deepEqual(await client.listRequests({ actorId: "a/b", limit: 100, cursor: "page+token" }), page)
})

test("live reconnect resumes after the last received cursor and preserves distinct events", async () => {
    const cursors: (string | undefined)[] = []
    const client = {
        watchRequests: async (receive: (page: RequestTracePage) => void, signal: AbortSignal, after?: string) => {
            cursors.push(after)
            receive({ ...page, resumeCursor: "saved-cursor", records: [{ ...page.records[0]!, eventId: "one" }] })
            if (cursors.length === 1) throw new Error("disconnected")
            receive({ ...page, cursor: 9, evicted: 8, records: [{ ...page.records[0]!, eventId: "two", sequence: 9, operation: "reconnected call" }] })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<RequestObserver client={client} />)
    await view.findByText("reconnected call", {}, { timeout: 3000 })
    assert.deepEqual(cursors, [undefined, "saved-cursor"])
    assert.ok(view.getByText("post"))
})

test("changing history filters cancels the old query and ignores a late response", async () => {
    let resolveFirst!: (page: ReturnType<typeof historyPage>) => void
    let firstSignal: AbortSignal | undefined
    const queries: unknown[] = []
    const client = {
        listRequests: async (query: unknown, signal?: AbortSignal) => {
            queries.push(query)
            if (queries.length === 1) {
                firstSignal = signal
                return new Promise<ReturnType<typeof historyPage>>(resolve => {
                    resolveFirst = resolve
                })
            }
            return historyPage([{ ...page.records[0]!, operation: "filtered call" }])
        }
    }
    const view = render(<RequestObserver client={client} />)
    fireEvent.click(view.getByRole("button", { name: "History" }))
    // A new query can be submitted from the form while a request is outstanding.
    const actor = view.getByLabelText("Actor ID") as HTMLInputElement
    actor.value = "lobby"
    fireEvent.submit(actor.closest("form")!)
    await view.findByText("filtered call")
    assert.equal(firstSignal?.aborted, true)
    assert.equal((queries[1] as RequestHistoryQuery).actorId, "lobby")
    await act(async () => resolveFirst(historyPage()))
    assert.equal(view.queryByText("post"), null)
})

test("instance requests filter both class and ID in live and saved history", async () => {
    const queries: RequestHistoryQuery[] = []
    const client = {
        watchRequests: async (receive: (value: RequestTracePage) => void, signal: AbortSignal) => {
            receive({
                ...page,
                records: [
                    page.records[0]!,
                    { ...page.records[0]!, sequence: 2, actorName: "Counter", operation: "wrong class" },
                    { ...page.records[0]!, sequence: 3, actorId: "other", operation: "wrong instance" }
                ]
            })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        listRequests: async (query: RequestHistoryQuery) => {
            queries.push(query)
            return historyPage()
        }
    }
    const view = render(<RequestObserver client={client} actor={{ actorName: "Room", actorId: "lobby" }} />)
    fireEvent.click(view.getByRole("button", { name: "Table" }))
    await view.findByText("post")
    assert.equal(view.queryByText("wrong class"), null)
    assert.equal(view.queryByText("wrong instance"), null)
    fireEvent.click(view.getByRole("button", { name: "History" }))
    await view.findByRole("table", { name: "Saved requests" })
    assert.equal(queries[0]!.actorName, "Room")
    assert.equal(queries[0]!.actorId, "lobby")
    assert.equal(view.queryByLabelText("Actor ID"), null)
    fireEvent.click(view.getByRole("button", { name: "Time range: Last hour" }))
    fireEvent.click(view.getByRole("button", { name: "All retained" }))
    await waitFor(() => assert.equal(queries.length, 2))
    assert.equal(queries[1]!.fromMs, undefined, "all retained history drops the time bound")
})

test("request inspection keeps table rows intact and opens a separate details drawer", async () => {
    const view = render(
        <RequestObserver
            client={{
                watchRequests: async (receive, signal) => {
                    receive(page)
                    await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
                }
            }}
        />
    )
    await view.findByText("post")
    fireEvent.click(view.getByRole("button", { name: "Table" }))
    const table = view.getByRole("table", { name: "Recent requests" })
    fireEvent.click(view.getByRole("button", { name: "Inspect post request" }))
    assert.ok(await view.findByRole("dialog", { name: "Request details" }))
    assert.equal(table.querySelectorAll("tbody tr").length, 1)
    assert.equal(table.textContent!.includes("request-one"), false)
    assert.ok(view.getByText("request-one"))
    fireEvent.click(view.getByRole("button", { name: "Close" }))
    assert.equal(view.queryByRole("dialog"), null)
})
