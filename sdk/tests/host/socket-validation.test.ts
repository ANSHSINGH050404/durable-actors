import assert from "node:assert/strict"
import { test } from "node:test"
import { z } from "zod"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import type { ActorMessageOf, ActorSocketOf } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import type { SocketConnection } from "../../src/actor/socketProtocol.js"
import type { WebSocketEventCommand } from "../../src/host/protocol.js"
import { ActorRuntime } from "../fixtures/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"

const metadata = z.object({ userId: z.string().min(1) })
const incoming = z.object({ type: z.literal("post"), text: z.string().min(1) })
const outgoing = z.object({ type: z.literal("posted"), text: z.string(), userId: z.string() })
const tag = z.enum(["member", "moderator"])
const calls: string[] = []

class ValidatedRoom extends Actor<
    z.infer<typeof metadata>,
    z.infer<typeof incoming>,
    z.infer<typeof outgoing>,
    z.infer<typeof tag>
> {
    static schemas = { metadata, incoming, outgoing, tag }

    async onConnect(): Promise<void> {
        calls.push("connect")
    }

    async onMessage(socket: ActorSocketOf<ValidatedRoom>, message: ActorMessageOf<ValidatedRoom>): Promise<void> {
        calls.push("message")
        socket.send({ type: "posted", text: message.text, userId: socket.metadata.userId })
    }

    async invalidOutput(): Promise<void> {
        this.broadcast({ type: "posted", text: 123, userId: "one" } as never)
    }

    async invalidMetadata(): Promise<void> {
        ;(await this.getConnections())[0]!.metadata = { userId: 123 } as never
    }

    async invalidTags(): Promise<void> {
        ;(await this.getConnections())[0]!.setTags("admin" as never)
    }

    async invalidFilter(): Promise<void> {
        this.broadcast({ type: "posted", text: "hello", userId: "one" }, { tags: ["admin" as never] })
    }

    async joinMembers(): Promise<readonly string[]> {
        const socket = (await this.getConnections())[0]!
        socket.setTags("member", "member")
        this.broadcast({ type: "posted", text: "joined", userId: socket.metadata.userId }, { tags: ["member"] })
        return socket.tags
    }
}

const definition = registerActorClass(ValidatedRoom, { actorName: "ValidatedRoom", fields: [] })
const actor = { project_id: "default", actor_name: "ValidatedRoom", actor_id: "one" }
const connection: SocketConnection = { id: "socket-1", metadata: { userId: "one" }, tags: [] }

test("deployment contracts do not impose runtime AJV validation", async () => {
    let handled = 0
    class ContractRoom extends Actor<{ userId: string }, { count: number }> {
        count = 0
        async onConnect() {
            handled++
        }
        async invalidState() {
            this.count = "invalid" as never
        }
        async onMessage() {
            handled++
        }
        async invalidOutput() {
            this.broadcast({ count: "invalid" } as never)
        }
    }
    const definition = registerActorClass(ContractRoom, {
        actorName: "ContractRoom",
        fields: [{ name: "count", persistence: Persistence.Persisted }],
        contract: {
            version: 1,
            actorName: "ContractRoom",
            emittable: [],
            schema: {
                definitions: {
                    Metadata: { type: "object", properties: { userId: { type: "string" } }, required: ["userId"] },
                    State: { type: "object", properties: { count: { type: "number" } }, required: ["count"] },
                    Incoming: { type: "object", properties: { count: { type: "number" } }, required: ["count"] },
                    Outgoing: { type: "object", properties: { count: { type: "number" } }, required: ["count"] }
                }
            }
        }
    })
    const runtime = new ActorRuntime(definition, () => {})
    const request = await event({
        type: "message",
        connection_id: connection.id,
        message: { type: "text", data: '{"count":"wrong"}' }
    })
    const identity = { ...actor, actor_name: "ContractRoom" }
    const invalidConnection = { ...connection, metadata: { userId: 123 } }
    const metadataReply = await runtime.handle({
        ...(await event({ type: "connect", connection: invalidConnection }, [invalidConnection])),
        actor: identity
    })
    assert.equal(metadataReply.type, "websocket_handled")
    const messageReply = await runtime.handle({ ...request, actor: identity })
    assert.equal(messageReply.type, "websocket_handled")
    assert.equal(handled, 2)
    for (const method of ["invalidOutput", "invalidState"]) {
        const reply = await runtime.handle({
            type: "invoke",
            request_id: "invalid",
            actor: identity,
            sqlite: await seed(null),
            method: method!,
            args: []
        })
        assert.equal(reply.type, "invoked")
    }
    const valid = {
        ...request,
        actor: identity,
        event: {
            type: "message" as const,
            connection_id: connection.id,
            message: { type: "text" as const, data: '{"count":1,"newField":true}' }
        }
    }
    assert.equal((await runtime.handle(valid)).type, "websocket_handled")
    assert.equal(handled, 3)
})

test("connection metadata is validated before actor hooks run", async () => {
    calls.length = 0
    const runtime = new ActorRuntime(definition, () => {})
    const invalid = { ...connection, metadata: { userId: 123 } }
    const reply = await runtime.handle(await event({ type: "connect", connection: invalid }, [invalid]))
    assert.equal(reply.type, "failed")
    assert.deepEqual(calls, [])
    assert.equal((await runtime.handle(await event({ type: "connect", connection }))).type, "websocket_handled")
    assert.deepEqual(calls, ["connect"])
})

test("incoming JSON is validated before onMessage and outgoing values are encoded automatically", async () => {
    calls.length = 0
    const runtime = new ActorRuntime(definition, () => {})
    const message = async (value: unknown) =>
        await event({
            type: "message",
            connection_id: connection.id,
            message: { type: "text", data: JSON.stringify(value) }
        })
    assert.equal((await runtime.handle(await message({ type: "post", text: 123 }))).type, "failed")
    assert.deepEqual(calls, [])
    const reply = await runtime.handle(await message({ type: "post", text: "hello" }))
    assertReply(reply, {
        type: "websocket_handled",

        effects: [
            {
                type: "send",
                connection_id: "socket-1",
                message: { type: "text", data: '{"type":"posted","text":"hello","userId":"one"}' }
            }
        ]
    })
    assert.deepEqual(calls, ["message"])
})

test("invalid actor output, metadata, and tags cannot reach the gateway", async () => {
    const published: unknown[] = []
    const runtime = new ActorRuntime(
        definition,
        () => {},
        async effects => {
            published.push(...effects)
        },
        async () => [connection]
    )
    for (const method of ["invalidOutput", "invalidMetadata", "invalidTags", "invalidFilter"]) {
        const reply = await runtime.handle({
            type: "invoke",
            request_id: method,
            actor,
            method,
            args: [],
            sqlite: await seed(null)
        })
        assert.equal(reply.type, "failed")
    }
    assert.deepEqual(published, [])
})

test("schema-validated tags persist and select broadcast recipients", async () => {
    const runtime = new ActorRuntime(
        definition,
        () => {},
        undefined,
        async () => [connection]
    )
    const reply = await runtime.handle({
        type: "invoke",
        request_id: "join",
        actor,
        method: "joinMembers",
        args: [],
        sqlite: await seed(null)
    })
    assert.equal(reply.type, "invoked")
    if (reply.type !== "invoked") return
    assert.deepEqual(reply.result, ["member"])
    assert.deepEqual(reply.effects, [
        { type: "set_tags", connection_id: connection.id, tags: ["member"] },
        {
            type: "broadcast",
            message: { type: "text", data: '{"type":"posted","text":"joined","userId":"one"}' },
            except_connection_ids: [],
            tags: ["member"]
        }
    ])
})

test("restored connection tags are validated before a hibernated actor resumes", async () => {
    calls.length = 0
    const invalid = { ...connection, tags: ["admin"] }
    const runtime = new ActorRuntime(definition, () => {})
    const reply = await runtime.handle(
        await event(
            {
                type: "message",
                connection_id: invalid.id,
                message: { type: "text", data: '{"type":"post","text":"hello"}' }
            },
            [invalid]
        )
    )
    assert.equal(reply.type, "failed")
    assert.deepEqual(calls, [])
})

async function event(
    value: WebSocketEventCommand["event"],
    connections = [connection]
): Promise<WebSocketEventCommand> {
    return { type: "websocket_event", request_id: "event", actor, event: value, connections, sqlite: await seed(null) }
}
