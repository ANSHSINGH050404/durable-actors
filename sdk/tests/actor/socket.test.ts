import assert from "node:assert/strict"
import { test } from "node:test"

import { actorConnections, decodeSocketMessage, runWithActorSockets } from "../../src/actor/socket.js"
import { parseSocketEffects } from "../../src/actor/socketProtocol.js"
import { ActorProtocolError, ActorSerializationError } from "../../src/errors.js"

test("ordinary invocations and broadcasts do not enumerate connections", async () => {
    let lookups = 0
    const load = async () => {
        lookups++
        throw new Error("gateway unavailable")
    }
    const result = await runWithActorSockets({}, load, async scope => {
        scope.broadcast({ hello: "world" })
        return 42
    })
    assert.equal(result.value, 42)
    assert.equal(result.effects.length, 1)
    assert.equal(lookups, 0)
})

test("connection lookup is lazy and shared only within the current invocation", async () => {
    const actor = {}
    let lookups = 0
    const load = async () => {
        lookups++
        return [{ id: "socket-1", metadata: { revision: lookups }, tags: [] }]
    }
    await runWithActorSockets(actor, load, async () => {
        assert.equal(lookups, 0)
        const [first, concurrent] = await Promise.all([actorConnections(actor), actorConnections(actor)])
        assert.equal(lookups, 1)
        assert.strictEqual(first, concurrent)
        first[0]!.metadata = { revision: 100 }
        assert.deepEqual((await actorConnections(actor))[0]!.metadata, { revision: 100 })
    })
    await runWithActorSockets(actor, load, async () => {
        assert.deepEqual((await actorConnections(actor))[0]!.metadata, { revision: 2 })
    })
    assert.equal(lookups, 2)
})

test("an actor can handle a failed explicit connection lookup", async () => {
    const actor = {}
    const load = async () => {
        throw new Error("gateway unavailable")
    }
    const result = await runWithActorSockets(actor, load, async () => {
        await assert.rejects(async () => actorConnections(actor), /gateway unavailable/)
        return "handled"
    })
    assert.equal(result.value, "handled")
})

test("broadcast tag matching modes survive socket transport", async () => {
    const published: unknown[] = []
    await runWithActorSockets(
        {},
        [],
        async scope => {
            for (const tagMatch of ["all", "any"] as const) {
                scope.broadcast({ type: "files-ready" }, { tags: ["file:a", "file:b"], tagMatch })
            }
        },
        async effects => {
            published.push(...parseSocketEffects(effects))
        }
    )
    assert.deepEqual(
        published,
        ["all", "any"].map(tag_match => ({
            type: "broadcast",
            message: { type: "text", data: '{"type":"files-ready"}' },
            except_connection_ids: [],
            tags: ["file:a", "file:b"],
            tag_match
        }))
    )
})

test("invalid broadcast tag matching modes fail before publishing", async () => {
    const result = await runWithActorSockets({}, [], async scope => {
        assert.throws(() => scope.broadcast({}, { tagMatch: "either" as never }), ActorProtocolError)
    })
    assert.deepEqual(result.effects, [])
    assert.throws(
        () =>
            parseSocketEffects([
                {
                    type: "broadcast",
                    message: { type: "text", data: "{}" },
                    except_connection_ids: [],
                    tags: [],
                    tag_match: "either"
                }
            ]),
        ActorProtocolError
    )
})

test("actor sends and broadcasts snapshot JSON values without caller serialization", async () => {
    const message = { type: "delta", payload: { text: "hello", flags: [true, null, 3] } }
    const result = await runWithActorSockets({}, [{ id: "socket-1", metadata: {}, tags: [] }], async scope => {
        scope.connection("socket-1").send(message)
        scope.broadcast(message, { tags: ["members"] })
        message.payload.text = "changed"
    })
    const encoded = { type: "text", data: '{"type":"delta","payload":{"text":"hello","flags":[true,null,3]}}' }
    assert.deepEqual(result.effects, [
        { type: "send", connection_id: "socket-1", message: encoded },
        { type: "broadcast", message: encoded, except_connection_ids: [], tags: ["members"] }
    ])
})

test("actor messages decode JSON and reject raw text and binary frames", () => {
    for (const value of [{ text: "hello" }, [1, true, null], "hello", 3, false, null]) {
        assert.deepEqual(decodeSocketMessage({ type: "text", data: JSON.stringify(value) }), value)
    }
    assert.throws(() => decodeSocketMessage({ type: "text", data: "hello" }), ActorProtocolError)
    assert.throws(() => decodeSocketMessage({ type: "binary", data: "e30=" }), ActorProtocolError)
})

test("invalid actor messages fail before queuing socket output", async () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    const result = await runWithActorSockets({}, [{ id: "socket-1", metadata: {}, tags: [] }], async scope => {
        const socket = scope.connection("socket-1")
        for (const value of [undefined, 1n, circular]) {
            assert.throws(() => socket.send(value as never), ActorSerializationError)
        }
        assert.throws(() => socket.send(new Uint8Array([1]) as never), ActorProtocolError)
    })
    assert.deepEqual(result.effects, [])
})

test("metadata and tags enforce size limits", async () => {
    await runWithActorSockets({}, [{ id: "socket", metadata: null, tags: [] }], async scope => {
        const socket = scope.connection("socket")
        socket.metadata = "x".repeat(16382)
        assert.throws(() => {
            socket.metadata = "x".repeat(16383)
        })
        socket.setTags(...Array.from({ length: 10 }, (_, i) => String(i)))
        assert.throws(() => socket.setTags(...Array.from({ length: 11 }, (_, i) => String(i))))
    })
})

test("count and tagged lookup query the gateway without loading the full list", async () => {
    const queries: unknown[] = []
    const source = async (query?: { tag?: string; countOnly?: boolean }) => {
        queries.push(query)
        if (query?.countOnly) return 32768
        assert.equal(query?.tag, "blue")
        return [{ id: "blue-one", metadata: null, tags: ["blue"] }]
    }
    await runWithActorSockets({}, source, async scope => {
        assert.equal(await scope.getConnectionCount(), 32768)
        assert.deepEqual(
            (await scope.getConnections("blue")).map(socket => socket.id),
            ["blue-one"]
        )
    })
    assert.deepEqual(queries, [{ countOnly: true }, { tag: "blue" }])
})

test("automatic heartbeat responses are transported and can be cleared", async () => {
    const result = await runWithActorSockets({}, [], async scope => {
        scope.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        scope.setWebSocketAutoResponse()
        assert.throws(() => scope.setWebSocketAutoResponse({ request: "x".repeat(2049), response: "pong" }), /2048/)
    })
    assert.deepEqual(parseSocketEffects(result.effects), [
        { type: "set_auto_response", request: "ping", response: "pong" },
        { type: "set_auto_response", request: null, response: null }
    ])
})
