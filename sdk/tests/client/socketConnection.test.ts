import assert from "node:assert/strict"
import { once } from "node:events"
import { test } from "node:test"
import type { TestContext } from "node:test"
import { WebSocketServer } from "ws"
import type WebSocket from "ws"
import { z } from "zod"

import type { ActorConnection } from "../../src/actor/socket.js"
import type { ActorSchemas } from "../../src/actor/socketValidation.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorValidationError } from "../../src/errors.js"

test(
    "connections send JSON values and deliver parsed messages with removable listeners",
    { timeout: 5_000 },
    async t => {
        const received: unknown[] = []
        const timings: Record<string, boolean | number | string | undefined>[] = []
        const connection = await connect(
            t,
            socket => {
                socket.on("message", (data, binary) => {
                    assert.equal(binary, false)
                    const value: unknown = JSON.parse(data.toString())
                    received.push(value)
                    socket.send(JSON.stringify(value))
                })
            },
            {},
            event => timings.push(event)
        )
        const removed = () => assert.fail("removed message listener ran")
        connection.addEventListener("message", removed)
        connection.removeEventListener("message", removed)
        for (const value of [
            { type: "chat", payload: { text: "hello 🌍", flags: [true, null] } },
            [1, false],
            "hello",
            0,
            false,
            null
        ]) {
            const reply = nextMessage(connection)
            connection.send(value)
            assert.deepEqual(await reply, value)
        }
        assert.equal(received.length, 6)
        assert.deepEqual(
            timings.map(event => event.event),
            ["actor_client_socket_connect", "actor_client_socket_first_message"]
        )
        assert.ok(Number(timings[1]!.completed_at_ms) >= Number(timings[0]!.socket_opened_at_ms))
        assert.equal(connection.readyState, 1)
        const closed = new Promise(resolve =>
            connection.addEventListener("close", ({ type, code, reason, wasClean }) =>
                resolve({ type, code, reason, wasClean })
            )
        )
        connection.close(3001, "done")
        assert.deepEqual(await closed, { type: "close", code: 3001, reason: "done", wasClean: true })
        assert.equal(connection.readyState, 3)
    }
)

test("connections close on malformed JSON and unsupported binary messages", { timeout: 5_000 }, async t => {
    for (const [payload, expectedCode] of [
        ["not JSON", 1007],
        [Buffer.from("{}"), 1003]
    ] as const) {
        let peer!: WebSocket
        const connection = await connect(t, socket => {
            peer = socket
        })
        let errors = 0
        connection.addEventListener("error", () => errors++)
        const closed = new Promise<{ code: number }>(resolve => connection.addEventListener("close", resolve))
        peer.send(payload)
        assert.equal((await closed).code, expectedCode)
        assert.equal(errors, 1)
    }
})

test("connections validate both message directions while accepting the initial state", { timeout: 5_000 }, async t => {
    const schemas = {
        metadata: z.object({ userId: z.string() }),
        incoming: z.object({ type: z.literal("post"), text: z.string() }),
        outgoing: z.object({ type: z.literal("posted"), text: z.string() })
    }
    const received: unknown[] = []
    let peer!: WebSocket
    const connection = await connect(
        t,
        socket => {
            peer = socket
            socket.on("message", data => {
                const value = JSON.parse(data.toString()) as { type: string; text?: string }
                received.push(value)
                socket.send(JSON.stringify({ type: "posted", text: 123 }))
            })
        },
        schemas
    )
    const initial = nextMessage(connection)
    peer.send(JSON.stringify({ type: "state", state: { history: [] } }))
    assert.deepEqual(await initial, { type: "state", state: { history: [] } })
    assert.throws(() => connection.send({ type: "post", text: 123 }), ActorValidationError)
    assert.equal(received.length, 0)
    const closed = new Promise<{ code: number }>(resolve => connection.addEventListener("close", resolve))
    connection.send({ type: "post", text: "hello" })
    assert.equal((await closed).code, 1007)
})

test("connection metadata is validated before opening a transport", async () => {
    const client = new RemoteActorClient(
        { projectId: "default", apiKey: "key", controlPlaneUrl: "https://example.com" },
        { connectWebSocket: async () => assert.fail("invalid metadata opened a socket") }
    )
    await assert.rejects(
        client.connect("Room", "one", { userId: 123 }, { metadata: z.object({ userId: z.string() }) }),
        ActorValidationError
    )
})

async function connect(
    t: TestContext,
    connected: (socket: WebSocket) => void,
    schemas?: ActorSchemas,
    telemetry?: (event: Record<string, boolean | number | string | undefined>) => void
): Promise<ActorConnection> {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 })
    t.after(async () => {
        for (const socket of server.clients) socket.terminate()
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
    })
    server.on("connection", (socket, request) => {
        assert.equal(request.headers.authorization, undefined)
        assert.equal(new URL(request.url!, "http://localhost").searchParams.get("key"), "token")
        connected(socket)
    })
    await once(server, "listening")
    const address = server.address()
    assert.ok(address && typeof address !== "string")
    const client = new RemoteActorClient(
        {
            projectId: "default",
            apiKey: "key",
            controlPlaneUrl: `http://127.0.0.1:${address.port}`
        },
        {
            telemetry,
            fetch: async () =>
                Response.json({ websocketUrl: `ws://127.0.0.1:${address.port}/v1/socket?key=token`, key: "token" })
        }
    )
    return client.connect("Room", "one", { userId: "user-1" }, schemas)
}

function nextMessage(connection: ActorConnection): Promise<unknown> {
    return new Promise(resolve => {
        const listener = (event: { data: unknown }) => {
            connection.removeEventListener("message", listener)
            resolve(event.data)
        }
        connection.addEventListener("message", listener)
    })
}
