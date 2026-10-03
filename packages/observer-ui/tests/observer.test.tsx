import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import "./dom.js"

// React DOM and Radix must see the jsdom globals when they load.
const { cleanup, fireEvent, render, waitFor, within } = await import("@testing-library/react")
const { ActorObserver, HttpObserverClient } = await import("../src/index.js")
afterEach(cleanup)

const inventory = {
    connectionsComplete: true,
    actors: [
        {
            actorName: "Room",
            live: 2,
            dormant: 1,
            unknown: 1,
            instances: [
                {
                    actorId: "general",
                    status: "live" as const,
                    connections: [
                        { id: "socket-a", metadata: { userId: "ada", role: "moderator" } },
                        { id: "socket-b", metadata: { userId: "grace" } },
                        { id: "socket-c", metadata: null }
                    ]
                },
                { actorId: "quiet", status: "dormant" as const, connections: [] },
                { actorId: "waiting", status: "unknown" as const, connections: [{ id: "socket-d", metadata: { userId: "linus" } }] }
            ]
        },
        { actorName: "Counter", live: 0, dormant: 0, unknown: 0, instances: [] }
    ]
}

test("the actor inventory renders counts, preserves stale data on failure, and retries", async () => {
    let calls = 0
    const client = {
        checkConnection: async () => {},
        listActors: async () => {
            if (++calls === 2) throw new Error("private server detail")
            return inventory
        }
    }
    const view = render(<ActorObserver client={client} />)
    assert.equal(view.getByRole("button").hasAttribute("disabled"), true)
    await waitFor(() => assert.ok(view.getByRole("cell", { name: "Room" })))
    const rows = view.getAllByRole("row")
    assert.match(rows[1]!.textContent!, /Room2114/u)
    assert.match(rows[2]!.textContent!, /Counter000/u)
    assert.equal(view.getByRole("button", { name: "Refresh" }).getAttribute("data-slot"), "button")
    fireEvent.click(view.getByRole("button", { name: "Refresh" }))
    await waitFor(() => assert.match(view.getByRole("alert").textContent!, /out of date/u))
    assert.ok(view.getByRole("cell", { name: "Room" }))
    assert.doesNotMatch(view.container.textContent!, /private server detail/u)
    fireEvent.click(view.getByRole("button", { name: "Try again" }))
    await waitFor(() => assert.equal(view.queryByRole("alert"), null))
    assert.equal(calls, 3)
})

test("opening an actor replaces the inventory with a dedicated page and returns to the filtered list", async () => {
    const client = { checkConnection: async () => {}, listActors: async () => inventory }
    const view = render(<ActorObserver client={client} />)
    const room = await view.findByRole("button", { name: "Room" })

    fireEvent.change(view.getByRole("combobox", { name: "Search actors and instances" }), { target: { value: "room" } })
    assert.equal(room.hasAttribute("aria-expanded"), false)
    fireEvent.click(room)

    assert.ok(view.getByRole("heading", { name: "Room", level: 1 }))
    assert.equal(view.queryByRole("table", { name: "Actor instance counts" }), null)
    assert.equal(view.queryByRole("combobox", { name: "Search actors and instances" }), null)
    assert.equal(document.activeElement, view.getByRole("heading", { name: "Room", level: 1 }))
    assert.ok(view.getByRole("heading", { name: "Room instances" }))
    assert.match(view.getByRole("row", { name: /general/i }).textContent!, /generalLive3/u)
    assert.match(view.getByRole("row", { name: /quiet/i }).textContent!, /quietDormant0/u)
    assert.match(view.getByRole("row", { name: /^waiting/i }).textContent!, /waitingUnknown1/u)
    assert.match(view.getByText(/connections, not unique people/i).textContent!, /WebSocket/u)

    fireEvent.click(view.getByRole("button", { name: "general" }))
    fireEvent.click(within(view.getByRole("group", { name: "Instance view" })).getByRole("button", { name: /^WebSockets/ }))
    assert.ok(view.getByRole("heading", { name: "general WebSockets" }))
    assert.match(view.getByRole("row", { name: /socket-a/i }).textContent!, /"userId": "ada"/u)
    assert.match(view.getByRole("row", { name: /socket-c/i }).textContent!, /null/u)

    fireEvent.click(view.getByRole("button", { name: "Back to actors" }))
    assert.equal(view.queryByRole("heading", { name: "Room instances" }), null)
    assert.equal((view.getByRole("combobox", { name: "Search actors and instances" }) as HTMLInputElement).value, "room")
    assert.equal(view.queryByRole("button", { name: "Counter" }), null)
    assert.equal(document.activeElement, view.getByRole("heading", { name: "Actors", level: 1 }))
})

test("a deployed actor name with no instances has an instructive instance empty state", async () => {
    const client = { checkConnection: async () => {}, listActors: async () => inventory }
    const view = render(<ActorObserver client={client} />)
    fireEvent.click(await view.findByRole("button", { name: "Counter" }))
    assert.match(view.getByRole("status").textContent!, /No Counter instances have been created yet/u)
})

test("switching clients cancels the old request and never displays its late inventory", async () => {
    let signal: AbortSignal | undefined
    let finish: (value: typeof inventory) => void = () => {}
    const first = {
        checkConnection: async () => {},
        listActors: async (incoming?: AbortSignal) => {
            signal = incoming
            return await new Promise<typeof inventory>(resolve => {
                finish = resolve
            })
        }
    }
    const second = { checkConnection: async () => {}, listActors: async () => ({ connectionsComplete: true, actors: [] }) }
    const view = render(<ActorObserver client={first} />)
    view.rerender(<ActorObserver client={second} />)
    await waitFor(() => assert.match(view.container.textContent!, /No actors yet/u))
    assert.equal(signal?.aborted, true)
    await act(async () => finish(inventory))
    assert.equal(view.queryByRole("cell", { name: "Room" }), null)
})

test("the HTTP adapter uses a configurable backend prefix and the existing session", async () => {
    const controller = new AbortController()
    const client = new HttpObserverClient("/api/projects/project-1/observe/", async (url, options) => {
        assert.equal(url, "/api/projects/project-1/observe/connection")
        assert.equal(options?.credentials, "same-origin")
        assert.equal(options?.signal, controller.signal)
        assert.equal(options?.redirect, "error")
        assert.equal(new Headers(options?.headers).has("authorization"), false)
        return Response.json({ connected: true })
    })
    await client.checkConnection(controller.signal)
})

test("the HTTP adapter rejects denied and invalid connection responses", async () => {
    for (const response of [Response.json({}, { status: 403 }), Response.json({ connected: false }), Response.json(null), new Response("<html>Login</html>")]) {
        const client = new HttpObserverClient("/api/observe", async () => response)
        await assert.rejects(client.checkConnection())
    }
})

test("the default HTTP adapter preserves the browser fetch receiver", async () => {
    const original = globalThis.fetch
    globalThis.fetch = async function (this: typeof globalThis) {
        assert.ok(this === globalThis, "fetch must receive the browser global")
        return Response.json({ connected: true })
    }
    try {
        await new HttpObserverClient().checkConnection()
    } finally {
        globalThis.fetch = original
    }
})

test("the inventory adapter uses the configured backend and validates counts", async () => {
    const controller = new AbortController()
    const client = new HttpObserverClient("/api/projects/one/observe", async (url, options) => {
        assert.equal(url, "/api/projects/one/observe/actors")
        assert.equal(options?.signal, controller.signal)
        assert.equal(options?.credentials, "same-origin")
        return Response.json(inventory)
    })
    assert.deepEqual(await client.listActors(controller.signal), inventory)
    for (const value of [
        {},
        { ...inventory, actors: [{ actorName: "Room", live: -1, dormant: 0, unknown: 0, instances: [] }] },
        { ...inventory, actors: [{ actorName: "Room", live: 1.5, dormant: 0, unknown: 0, instances: [] }] },
        { ...inventory, actors: [{ actorName: "Room", live: 1, dormant: 0, unknown: 0, instances: [{ actorId: "one", status: "missing", connections: [] }] }] },
        { ...inventory, actors: [{ actorName: "Room", live: 1, dormant: 0, unknown: 0, instances: [{ actorId: "one", status: "live", connections: [{ id: 1, metadata: {} }] }] }] }
    ]) {
        await assert.rejects(new HttpObserverClient("/api/observe", async () => Response.json(value)).listActors())
    }
})

test("actor search filters the inventory and can recover from no matches", async () => {
    const view = render(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => inventory }} />)
    const search = await view.findByRole("combobox", { name: "Search actors and instances" })
    fireEvent.change(search, { target: { value: "room" } })
    assert.ok(view.getByRole("button", { name: "Room" }))
    assert.equal(view.queryByRole("button", { name: "Counter" }), null)
    fireEvent.change(search, { target: { value: "missing" } })
    assert.ok(view.getByText("No matching actors"))
    fireEvent.click(view.getByRole("button", { name: "Clear search" }))
    assert.ok(view.getByRole("button", { name: "Counter" }))
})

test("an exact actor class search keeps its keyboard navigation option", async () => {
    const view = render(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => inventory }} />)
    const search = await view.findByRole("combobox", { name: "Search actors and instances" })
    fireEvent.change(search, { target: { value: "room" } })
    assert.ok(view.getByRole("option", { name: "Room 3 instances" }))
    fireEvent.keyDown(search, { key: "Enter" })
    assert.equal(document.activeElement, view.getByRole("heading", { name: "Room", level: 1 }))
    assert.ok(view.getByRole("heading", { name: "Room instances" }))
})

test("actor search finds instance IDs and opens their requests and WebSockets directly", async () => {
    const searchable = {
        connectionsComplete: true,
        actors: [inventory.actors[0]!, { ...inventory.actors[1]!, live: 1, instances: [{ actorId: "general", status: "live" as const, connections: [] }] }]
    }
    const view = render(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => searchable }} />)
    const search = await view.findByRole("combobox", { name: "Search actors and instances" })
    fireEvent.change(search, { target: { value: "Room general" } })
    assert.ok(view.getByRole("button", { name: "Room" }), "an instance match keeps its actor class in the table")
    assert.equal(view.queryByRole("button", { name: "Counter" }), null)
    const result = view.getByRole("option", { name: /general.*Room.*Live.*3 connections/iu })
    fireEvent.click(result)
    assert.ok(view.getByRole("region", { name: "Room / general" }))
    assert.ok(document.activeElement === view.getByRole("heading", { name: "general", level: 1 }), "the opened instance receives focus")
    assert.ok(view.getByRole("heading", { name: "Requests" }))
    fireEvent.click(within(view.getByRole("group", { name: "Instance view" })).getByRole("button", { name: /^WebSockets/ }))
    assert.ok(view.getByRole("heading", { name: "general WebSockets" }))
    fireEvent.click(view.getByRole("button", { name: "Back to instances" }))
    assert.ok(document.activeElement === view.getByRole("heading", { name: "Room", level: 1 }))
})

test("instance search and residency filtering combine without changing inventory totals", async () => {
    const view = render(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => inventory }} />)
    fireEvent.click(await view.findByRole("button", { name: "Room" }))
    fireEvent.change(view.getByRole("combobox", { name: "Instance state" }), { target: { value: "live" } })
    assert.ok(view.getByRole("button", { name: "general" }))
    assert.equal(view.queryByRole("button", { name: "quiet" }), null)
    fireEvent.input(view.getByRole("searchbox", { name: "Search instances" }), { target: { value: "quiet" } })
    assert.ok(view.getByText("No matching instances"))
    fireEvent.click(view.getByRole("button", { name: "Clear filters" }))
    assert.ok(view.getByRole("button", { name: "quiet" }))
    assert.equal(view.getByLabelText("Total instances").textContent, "4")
    assert.equal(view.getByLabelText("Live instances").textContent, "2")
})

test("switching clients clears actor selection and search", async () => {
    const view = render(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => inventory }} />)
    await view.findByRole("button", { name: "Room" })
    fireEvent.change(view.getByRole("combobox", { name: "Search actors and instances" }), { target: { value: "Room" } })
    fireEvent.click(view.getByRole("button", { name: "Room" }))
    view.rerender(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => ({ ...inventory }) }} />)
    await view.findByRole("button", { name: "Room" })
    assert.equal(view.queryByRole("heading", { name: "Room instances" }), null)
    assert.equal((view.getByRole("combobox", { name: "Search actors and instances" }) as HTMLInputElement).value, "")
})

test("a live subscription updates inventory without polling and is aborted on unmount", async () => {
    let publish: ((value: typeof inventory) => void) | undefined
    let signal: AbortSignal | undefined
    let reads = 0
    const client = {
        checkConnection: async () => {},
        listActors: async () => {
            reads++
            return inventory
        },
        watchActors: async (onInventory: (value: typeof inventory) => void, incoming: AbortSignal) => {
            publish = onInventory
            signal = incoming
            onInventory(inventory)
            await new Promise<void>(resolve => incoming.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<ActorObserver client={client} />)
    await view.findByText("Live updates")
    await act(async () => publish!({ ...inventory, actors: [] }))
    assert.ok(view.getByText("No actors yet"))
    assert.equal(reads, 0)
    view.unmount()
    assert.equal(signal?.aborted, true)
})

test("the HTTP stream parses fragmented SSE and rejects malformed inventories", async () => {
    let aborted = false
    const controller = new AbortController()
    const bytes = new TextEncoder().encode(`: heartbeat\n\nevent: inventory\ndata: ${JSON.stringify(inventory)}\n\n`)
    const client = new HttpObserverClient("/api/custom", async (url, options) => {
        assert.equal(url, "/api/custom/events")
        assert.equal(options?.credentials, "same-origin")
        assert.equal(options?.signal, controller.signal)
        return new Response(
            new ReadableStream({
                start(stream) {
                    stream.enqueue(bytes.slice(0, 27))
                    stream.enqueue(bytes.slice(27))
                },
                cancel() {
                    aborted = true
                }
            }),
            { headers: { "content-type": "text/event-stream" } }
        )
    })
    const snapshots: unknown[] = []
    await client
        .watchActors(value => {
            snapshots.push(value)
            controller.abort()
        }, controller.signal)
        .catch(() => {})
    assert.deepEqual(snapshots, [inventory])
    assert.equal(aborted, true)
    const invalid = new HttpObserverClient("/api/observe", async () => new Response("event: inventory\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } }))
    await assert.rejects(invalid.watchActors(() => assert.fail("invalid snapshot delivered"), new AbortController().signal))
})

test("a dropped stream keeps its last snapshot and automatically reconnects", async () => {
    let attempts = 0
    const client = {
        checkConnection: async () => {},
        listActors: async () => inventory,
        watchActors: async (publish: (value: typeof inventory) => void, signal: AbortSignal) => {
            attempts++
            publish(inventory)
            if (attempts === 1) throw new Error("upstream secret")
            publish({ ...inventory, actors: [] })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<ActorObserver client={client} />)
    await view.findByRole("alert")
    assert.ok(view.getByRole("button", { name: "Room" }))
    assert.doesNotMatch(view.container.textContent!, /upstream secret/u)
    await waitFor(() => assert.ok(view.getByText("No actors yet")), { timeout: 2000 })
    assert.equal(view.queryByRole("alert"), null)
    assert.equal(attempts, 2)
})

test("SSE snapshots and heartbeats preserve the selected instance and its live state", async () => {
    let stream: ReadableStreamDefaultController<Uint8Array> | undefined
    let requests = 0
    const client = new HttpObserverClient("/api/observe", async url => {
        if (String(url).includes("/requests/")) return new Response(new ReadableStream(), { headers: { "content-type": "text/event-stream" } })
        if (String(url).includes("/queue-waits")) return Response.json([])
        if (String(url).includes("/state/history")) return Response.json({ records: [], retention: null, nextBefore: null })
        if (String(url).includes("/state")) return Response.json({ snapshot: null, schema: null })
        requests++
        return new Response(
            new ReadableStream<Uint8Array>({
                start(controller) {
                    stream = controller
                }
            }),
            {
                headers: { "content-type": "text/event-stream" }
            }
        )
    })
    const encoder = new TextEncoder()
    const publish = async (text: string) =>
        act(async () => {
            stream!.enqueue(encoder.encode(text))
        })
    const view = render(<ActorObserver client={client} />)
    await publish(`event: inventory\ndata: ${JSON.stringify(inventory)}\n\n`)
    fireEvent.click(view.getByRole("button", { name: "Room" }))
    fireEvent.click(view.getByRole("button", { name: "general" }))
    fireEvent.click(within(view.getByRole("group", { name: "Instance view" })).getByRole("button", { name: /^WebSockets/ }))
    const detail = view.getByRole("region", { name: "Room / general" })
    for (let update = 0; update < 10; update++) {
        await publish(": heartbeat\n\n")
        assert.equal(view.getByRole("region", { name: "Room / general" }), detail)
        await publish(`event: inventory\ndata: ${JSON.stringify(inventory)}\n\n`)
        assert.equal(view.getByRole("region", { name: "Room / general" }), detail)
        assert.ok(view.getByRole("heading", { name: "general WebSockets" }))
        assert.equal(view.queryByRole("status", { name: "Loading actors" }), null)
    }
    assert.equal(requests, 1)
})

test("an initial actor opens its page with class-specific totals and handles removal from inventory", async () => {
    let publish: (value: typeof inventory) => void = () => {}
    const client = {
        checkConnection: async () => {},
        listActors: async () => inventory,
        watchActors: async (receive: typeof publish) => {
            publish = receive
            receive(inventory)
            await new Promise<void>(() => {})
        }
    }
    const view = render(<ActorObserver client={client} initialActorName="Counter" />)
    await view.findByRole("heading", { name: "Counter", level: 1 })
    assert.equal(view.getByLabelText("Total instances").textContent, "0")
    assert.equal(view.queryByRole("table", { name: "Actor instance counts" }), null)
    await act(async () => publish({ connectionsComplete: true, actors: [inventory.actors[0]!] }))
    assert.ok(view.getByText("Actor class unavailable"))
    fireEvent.click(view.getByRole("button", { name: "Back to actors" }))
    assert.ok(view.getByRole("button", { name: "Room" }))
})

test("live actors and instances come first and the entire row opens inspection", async () => {
    const client = {
        checkConnection: async () => {},
        listActors: async () => ({ connectionsComplete: true, actors: [inventory.actors[1]!, { ...inventory.actors[0]!, instances: [...inventory.actors[0]!.instances].reverse() }] })
    }
    const view = render(<ActorObserver client={client} />)
    await view.findByRole("button", { name: "Room" })
    assert.match(view.getAllByRole("row")[1]!.textContent!, /^Room/u)
    fireEvent.click(view.getByRole("button", { name: "Room" }).closest("tr")!.lastElementChild!)
    await view.findByRole("table", { name: "Room instances" })
    assert.match(view.getAllByRole("row")[1]!.textContent!, /^general/u)
    fireEvent.click(view.getByRole("button", { name: "general" }).closest("tr")!.lastElementChild!)
    assert.ok(view.getByRole("heading", { name: "Requests" }))
    assert.ok(view.getByRole("button", { name: "Back to instances" }))
})

test("instance queues show operation bubbles and update while inspecting an instance", async () => {
    const waiting = ["sendMessage", "save", "sendMessage", "close"].map((operation, i) => ({ id: String(i), operation }))
    const current = {
        connectionsComplete: true,
        actors: [{ actorName: "Room", live: 1, dormant: 0, unknown: 0, instances: [{ actorId: "general", status: "live" as const, connections: [], waiting }] }]
    }
    let update: (inventory: typeof current) => void = () => {}
    const client = {
        checkConnection: async () => {},
        listActors: async () => current,
        watchActors: async (onInventory: typeof update, signal: AbortSignal) => {
            update = onInventory
            onInventory(current)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<ActorObserver client={client} initialActorName="Room" />)
    await view.findByRole("button", { name: "general" })
    assert.ok(view.getByRole("columnheader", { name: "Waiting" }))
    assert.equal(view.getAllByText("sendMessage").length, 2)
    assert.ok(view.getByText("+1"))
    fireEvent.click(view.getByRole("button", { name: "general" }))
    assert.match(view.getByRole("region", { name: "Waiting requests" }).textContent!, /Waiting 4/u)
    assert.ok(view.getByText("close"))
    await act(async () => update({ connectionsComplete: true, actors: [{ ...current.actors[0]!, instances: [{ ...current.actors[0]!.instances[0]!, waiting: [] }] }] }))
    assert.ok(view.getByText("No requests waiting."))
    assert.equal(view.queryByText("sendMessage"), null)
})

test("actor pages show the average queue wait per class and instance from retained history", async () => {
    const queries: { fromMs?: number; toMs?: number; actorName?: string }[] = []
    const client = {
        checkConnection: async () => {},
        listActors: async () => inventory,
        listQueueWaits: async (query: { fromMs?: number; toMs?: number; actorName?: string }) => {
            queries.push(query)
            const rows = [
                { actorName: "Room", actorId: "general", admitted: 3, averageMs: 12.5, maxMs: 40 },
                { actorName: "Room", actorId: "quiet", admitted: 1, averageMs: 1500, maxMs: 1500 },
                { actorName: "Counter", actorId: "one", admitted: 2, averageMs: 2, maxMs: 3 }
            ]
            return rows.filter(row => !query.actorName || row.actorName === query.actorName)
        }
    }
    const view = render(<ActorObserver client={client} />)
    await waitFor(() => assert.equal(view.getByLabelText("Average queue wait").textContent, "256.9 ms"))
    assert.match(view.container.textContent!, /Max 1.5 s · 6 admitted in the last hour/u)
    assert.match(view.getByRole("cell", { name: "Room" }).closest("tr")!.textContent!, /384.4 msmax 1.5 s/u, "the class average weights instances by admitted requests")
    assert.match(view.getByRole("cell", { name: "Counter" }).closest("tr")!.textContent!, /2 msmax 3 ms/u)
    assert.equal(typeof queries[0]!.fromMs, "number")
    fireEvent.click(view.getByRole("button", { name: "Room" }))
    await waitFor(() => assert.equal(queries.at(-1)!.actorName, "Room"))
    await waitFor(() => assert.match(view.container.textContent!, /Max 1.5 s · 4 admitted/u))
    assert.equal(view.getByLabelText("Average queue wait").textContent, "384.4 ms")
    const general = view.getByRole("button", { name: "general" }).closest("tr")!
    assert.match(general.textContent!, /12.5 msmax 40 ms/u)
    assert.match(view.getByRole("button", { name: "waiting" }).closest("tr")!.textContent!, /—/u)
    fireEvent.click(view.getByRole("button", { name: "general" }))
    const section = await view.findByRole("region", { name: "Queue wait" })
    assert.equal(within(section).getByLabelText("Average queue wait").textContent, "12.5 ms")
    assert.equal(within(section).getByLabelText("Longest queue wait").textContent, "40 ms")
    assert.equal(within(section).getByLabelText("Admitted requests").textContent, "3")
    view.unmount()
    const plain = render(<ActorObserver client={{ checkConnection: async () => {}, listActors: async () => inventory }} />)
    await plain.findByRole("cell", { name: "Room" })
    assert.equal(plain.queryByLabelText("Average queue wait"), null)
    assert.equal(plain.queryByRole("columnheader", { name: "Avg queue wait" }), null)
})
