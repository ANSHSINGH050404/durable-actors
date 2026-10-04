import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persisted } from "../../src/actor/decorators.js"
import { Persistence } from "../../src/actor/schema.js"
import type { InvokeCommand } from "../../src/host/protocol.js"
import { ActorRuntime } from "../fixtures/actor-runtime.js"
import { seed, fields as storedFields } from "../fixtures/litestream.js"

class ObservableRoom extends Actor {
    @Persisted messages: string[] = []
    @Persisted title = "Room"
    @Persisted private secret = "secret"
    @Persisted protected internal = "internal"
    @Persisted status?: string = "online"

    async change() {
        this.messages.push("first")
        this.messages.push("second")
        this.title = "Renamed"
        this.secret = "changed"
        delete this.status
    }

    async unchanged() {
        this.messages.push("temporary")
        this.messages.pop()
    }

    async fail() {
        this.messages.push("failed")
        throw new Error("failed")
    }
}

const fields = [
    { name: "messages", persistence: Persistence.Persisted, emittable: true },
    { name: "title", persistence: Persistence.Persisted },
    { name: "secret", persistence: Persistence.Persisted, visibility: "private" as const },
    { name: "internal", persistence: Persistence.Persisted, visibility: "protected" as const },
    { name: "status", persistence: Persistence.Persisted, emittable: true }
]
const definition = registerActorClass(ObservableRoom, { actorName: "ObservableRoom", fields })
const actor = { project_id: "default", actor_name: "ObservableRoom", actor_id: "room" }

test("initial snapshots expose only emittable fields while still saving all persisted state", async () => {
    const runtime = new ActorRuntime(definition, () => {})
    const sqlite = await seed()
    const connection = { id: "connection", metadata: {}, tags: [] }
    const reply = await runtime.handle({
        type: "websocket_event",
        request_id: "connect",
        actor,
        sqlite,
        connections: [connection],
        event: { type: "connect", connection }
    })
    assert.equal(reply.type, "websocket_handled")
    if (reply.type !== "websocket_handled") return
    assert.deepEqual(storedFields(sqlite), {
        messages: [],
        title: "Room",
        secret: "secret",
        internal: "internal",
        status: "online"
    })
    assert.deepEqual(reply.effects, [
        {
            type: "state_snapshot",
            connection_id: "connection",
            state: { messages: [], status: "online" }
        }
    ])
})

test("coalesces nested mutations and removals into one final update without live publication", async () => {
    const runtime = new ActorRuntime(
        definition,
        () => {},
        async () => assert.fail("automatic changes must wait for commit")
    )
    const command = await invocation("change")
    const reply = await runtime.handle(command)
    assert.equal(reply.type, "invoked")
    if (reply.type !== "invoked") return
    assert.deepEqual(reply.effects, [
        {
            type: "state_update",
            changes: { messages: ["first", "second"] },
            removed: ["status"]
        }
    ])
    assert.deepEqual(storedFields(command.sqlite!), {
        messages: ["first", "second"],
        title: "Renamed",
        secret: "changed",
        internal: "internal"
    })
})

test("does not emit intermediate values, unchanged fields, or failed operations", async () => {
    const runtime = new ActorRuntime(
        definition,
        () => {},
        async () => assert.fail("unexpected live output")
    )
    const unchanged = await runtime.handle(await invocation("unchanged"))
    assert.equal(unchanged.type, "invoked")
    if (unchanged.type === "invoked") assert.equal(unchanged.effects, undefined)
    const failed = await runtime.handle(await invocation("fail"))
    assert.equal(failed.type, "failed")
    assert.equal("effects" in failed, false)
})

async function invocation(method: string): Promise<InvokeCommand> {
    return { type: "invoke", request_id: method, actor, sqlite: await seed(null), method, args: [] }
}
