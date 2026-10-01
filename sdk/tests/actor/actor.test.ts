import assert from "node:assert/strict"
import { test } from "node:test"
import { z } from "zod"

import { Actor, findActorDefinition, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorDefinitionError, ActorValidationError } from "../../src/errors.js"
import { runWithActorClientForTests } from "../fixtures/actorClient.js"

class ChatRoom extends Actor {
    async history(): Promise<readonly string[]> {
        return []
    }
}

test("actor methods cannot replace the reserved database handle", () => {
    class DatabaseMethod extends Actor {}
    Object.defineProperty(DatabaseMethod.prototype, "db", { value: async () => [] })
    assert.throws(
        () => registerActorClass(DatabaseMethod, { actorName: "DatabaseMethod", fields: [] }),
        /reserved method db/
    )
})

test("creating and invoking a reference does not register an executable actor", async () => {
    class RemoteCounter extends Actor {
        count = 0

        async increment(): Promise<number> {
            assert.fail("reference executed the actor implementation")
        }
    }
    await runWithActorClientForTests(
        {
            invoke: async request => {
                assert.equal(request.actorName, "RemoteCounter")
                assert.equal(request.actorId, "one")
                assert.equal(request.method, "increment")
                return 42
            }
        },
        async () => {
            const reference = RemoteCounter.get("one")
            assert.equal(findActorDefinition("RemoteCounter"), undefined)
            assert.equal(await reference.increment(), 42)
            assert.equal(findActorDefinition("RemoteCounter"), undefined)
        }
    )
})

test("runtime registration requires an explicit schema and references preserve it", () => {
    class RegisteredCounter extends Actor {
        count = 0
    }
    // @ts-expect-error runtime registration requires a persistence schema
    const missingSchema: Parameters<typeof registerActorClass> = [RegisteredCounter]
    assert.equal(missingSchema.length, 1)
    RegisteredCounter.get("before-registration")
    assert.equal(findActorDefinition("RegisteredCounter"), undefined)
    const state = {
        actorName: "RegisteredCounter",
        fields: [{ name: "count", persistence: Persistence.Persisted }]
    }
    const definition = registerActorClass(RegisteredCounter, state)
    RegisteredCounter.get("after-registration")
    assert.equal(findActorDefinition("RegisteredCounter"), definition)
    assert.equal(definition.state, state)
})

test("reference classes do not claim runtime names but duplicate runtime registrations fail", () => {
    const RemoteRoom = class SharedRoom extends Actor {}
    const LocalRoom = class SharedRoom extends Actor {}
    const schema = { actorName: "SharedRoom", fields: [] }
    RemoteRoom.get("remote")
    const definition = registerActorClass(LocalRoom, schema)
    RemoteRoom.get("another")
    assert.equal(findActorDefinition("SharedRoom"), definition)
    assert.throws(() => registerActorClass(RemoteRoom, schema), ActorDefinitionError)
})

test("actor references broadcast without invoking a customer actor method", async () => {
    const broadcasts: unknown[] = []
    await runWithActorClientForTests(
        {
            invoke: async () => assert.fail("broadcast invoked a customer actor method"),
            broadcast: async request => {
                broadcasts.push(request)
            },
            requestId: () => "request-1"
        },
        () => ChatRoom.get("room-1").broadcast({ text: "hello" })
    )
    assert.deepEqual(broadcasts, [
        {
            requestId: "request-1",
            actorName: "ChatRoom",
            actorId: "room-1",
            message: { type: "text", data: JSON.stringify({ text: "hello" }) }
        }
    ])
})

test("actor references enforce declared metadata and broadcast schemas before dispatch", async () => {
    class SchemaReferenceRoom extends Actor<{ userId: string }, { text: string }> {
        static schemas = { metadata: z.object({ userId: z.string() }), outgoing: z.object({ text: z.string() }) }
    }
    await runWithActorClientForTests(
        {
            invoke: async () => assert.fail("unexpected actor invocation"),
            connect: async () => assert.fail("invalid metadata was dispatched"),
            broadcast: async () => assert.fail("invalid broadcast was dispatched")
        },
        async () => {
            const room = SchemaReferenceRoom.get("one")
            assert.throws(() => room.connect({ userId: 123 } as never), ActorValidationError)
            assert.throws(() => room.broadcast({ text: 123 } as never), ActorValidationError)
        }
    )
})
