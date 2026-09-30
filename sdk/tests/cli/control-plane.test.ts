import assert from "node:assert/strict"
import { test } from "node:test"

import { ControlPlaneClient } from "../../src/cli/control-plane.js"

const connection = { projectId: "default", controlPlaneUrl: "https://control.example", credential: "admin-key" }

test("observability reads and streams use the configured project", async () => {
    const requests: string[] = []
    const client = new ControlPlaneClient({ ...connection, projectId: "hosted-project" }, async (url, options) => {
        requests.push(String(url))
        return options?.headers && new Headers(options.headers).get("accept") === "text/event-stream"
            ? new Response("event: inventory\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } })
            : Response.json({ actors: [] })
    })
    const signal = new AbortController().signal
    await client.checkConnection()
    await client.listActors()
    await client.listRequests(new URLSearchParams({ limit: "1" }))
    await client.getMetrics(new URLSearchParams())
    await client.listQueueWaits(new URLSearchParams())
    await client.listWebSockets(new URLSearchParams())
    await client.openActorStream(signal)
    await client.openRequestStream(signal, "cursor")
    assert.deepEqual(
        requests,
        [
            "actors",
            "actors",
            "requests?limit=1",
            "metrics",
            "queue-waits",
            "websockets",
            "events",
            "requests/events?after=cursor"
        ].map(path => `https://control.example/v1/projects/hosted-project/observe/${path}`)
    )
})

test("connection checks work before any deployment exists", async () => {
    const client = new ControlPlaneClient(connection, async input => {
        if (String(input) === "https://control.example/v1/projects/default/observe/actors")
            return Response.json({ actors: [] })
        return Response.json({ error: { message: "deployment not found" } }, { status: 404 })
    })
    await client.checkConnection()
})

test("contract reads use the active deployment", async () => {
    const requests: string[] = []
    const client = new ControlPlaneClient(connection, async (input, init) => {
        requests.push(String(input))
        assert.equal(init?.method, "GET")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(new Headers(init?.headers).get("content-type"), null)
        assert.equal(init?.body, undefined)
        assert.equal(init?.redirect, "error")
        assert.ok(init?.signal instanceof AbortSignal)
        return Response.json({ received: true })
    })
    assert.deepEqual(await client.getContract(), { received: true })
    assert.deepEqual(requests, ["https://control.example/v1/projects/default/deployment/contract"])
})

test("HTTP failures retain their status with JSON, non-JSON, or malformed error documents", async () => {
    for (const [body, message] of [
        [JSON.stringify({ error: { message: "Contract conflict" } }), "Contract conflict"],
        ["upstream unavailable", "Conflict"],
        ["null", "Conflict"],
        [JSON.stringify({ error: { message: {} } }), "Conflict"]
    ]) {
        const client = new ControlPlaneClient(
            connection,
            async () => new Response(body, { status: 409, statusText: "Conflict" })
        )
        await assert.rejects(client.getContract(), {
            message: `Control-plane request failed (HTTP 409): ${message}\nGET https://control.example/v1/projects/default/deployment/contract`
        })
    }
})

test("missing contract endpoints identify the server and project without exposing credentials", async () => {
    const client = new ControlPlaneClient(
        connection,
        async () => new Response(null, { status: 404, statusText: "Not Found" })
    )
    await assert.rejects(client.getContract(), error => {
        const message = (error as Error).message
        assert.match(message, /HTTP 404.*Not Found/u)
        assert.match(message, /GET https:\/\/control\.example\/v1\/projects\/default\/deployment\/contract/u)
        assert.match(message, /durable-actors dev/u)
        assert.match(message, /DURABLE_ACTORS_PROJECT_ID/u)
        assert.doesNotMatch(message, /admin-key/u)
        return true
    })
})

test("successful responses must contain JSON", async () => {
    const client = new ControlPlaneClient(connection, async () => new Response("not JSON"))
    await assert.rejects(client.getContract(), /invalid JSON \(HTTP 200\)/u)
})

test("transport failures do not retry writes and warn that their outcome is unknown", async () => {
    let requests = 0
    const client = new ControlPlaneClient(connection, async () => {
        requests++
        throw new TypeError("fetch failed")
    })
    await assert.rejects(
        client.registerDeployment({ localSource: { workingDirectory: "/project", actorEntrypoint: "actors.ts" } }),
        /Cannot complete PUT.*may have reached the server/u
    )
    assert.equal(requests, 1)
    await assert.rejects(client.getContract(), error => {
        assert.match((error as Error).message, /Cannot complete GET/u)
        assert.doesNotMatch((error as Error).message, /may have reached|admin-key/u)
        return true
    })
    assert.equal(requests, 2)
})

test("live inventory streams carry server-side credentials and cancellation", async () => {
    const controller = new AbortController()
    const client = new ControlPlaneClient(connection, async (url, options) => {
        assert.equal(url, "https://control.example/v1/projects/default/observe/events")
        assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(new Headers(options?.headers).get("accept"), "text/event-stream")
        assert.equal(options?.signal, controller.signal)
        assert.equal(options?.redirect, "error")
        return new Response("event: inventory\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } })
    })
    assert.match(await (await client.openActorStream(controller.signal)).text(), /event: inventory/u)
})

test("live inventory rejects denied responses and non-streaming upstreams", async () => {
    for (const response of [new Response("secret", { status: 403 }), Response.json({})]) {
        const client = new ControlPlaneClient(connection, async () => response)
        await assert.rejects(client.openActorStream(new AbortController().signal), {
            message: "Live inventory is unavailable"
        })
    }
})

test("request traces stream through the authenticated control-plane client", async () => {
    const controller = new AbortController()
    const client = new ControlPlaneClient(connection, async (url, options) => {
        assert.equal(url, "https://control.example/v1/projects/default/observe/requests/events")
        assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(options?.signal, controller.signal)
        return new Response("event: requests\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } })
    })
    assert.match(await (await client.openRequestStream(controller.signal)).text(), /event: requests/u)
})

test("request history uses bounded filters with server-side credentials", async () => {
    const query = new URLSearchParams({ actorId: "one", outcome: "failed", limit: "100", cursor: "opaque+cursor" })
    const controller = new AbortController()
    const client = new ControlPlaneClient(connection, async (url, options) => {
        assert.equal(
            url,
            "https://control.example/v1/projects/default/observe/requests?actorId=one&outcome=failed&limit=100&cursor=opaque%2Bcursor"
        )
        assert.equal(options?.method, "GET")
        assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(options?.body, undefined)
        assert.ok(options?.signal)
        return Response.json({ records: [], nextCursor: null })
    })
    assert.deepEqual(await client.listRequests(query, controller.signal), { records: [], nextCursor: null })
})

for (const [method, path] of [
    ["getMetrics", "metrics"],
    ["listQueueWaits", "queue-waits"],
    ["listWebSockets", "websockets"],
    ["getState", "state"],
    ["listStateHistory", "state/history"]
] as const) {
    test(`${path} reads use typed routes and server-side credentials`, async () => {
        const client = new ControlPlaneClient(connection, async (url, options) => {
            assert.equal(url, `https://control.example/v1/projects/default/observe/${path}?fromMs=10&toMs=20`)
            assert.equal(options?.method, "GET")
            assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
            return Response.json({ saved: true })
        })
        assert.deepEqual(await client[method](new URLSearchParams({ fromMs: "10", toMs: "20" })), { saved: true })
    })
}
