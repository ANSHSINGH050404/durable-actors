import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import type { ActorConnection } from "../../src/actor/socket.js"
import { runWithActorClient } from "../../src/client/client.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorRuntime } from "../../src/host/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"

class Destination extends Actor {
    async increment(amount: number): Promise<number> {
        return amount
    }
}

class Relay extends Actor {
    async forward(): Promise<number> {
        const target = Destination.get("destination")
        const count = await target.increment(3)
        await target.broadcast("updated")
        const socket = await target.connect(null)
        socket.close()
        return count
    }
}

test("actors invoke, broadcast, and open sockets through the remote client", async () => {
    const events: string[] = []
    const target = { route: "https://host.test", token: "ticket", ownerEpoch: 1, expiresAtMs: 4_000_000_000_000 }
    const transport = new RemoteActorClient(
        { controlPlaneUrl: "https://control.test", projectId: "test" },
        {
            telemetry: () => {},
            fetch: async (url, options) => {
                if (String(url).endsWith("/invoke")) {
                    assert.equal(JSON.parse(String(options?.body)).method, "increment")
                    events.push("invoke")
                    return Response.json({ target, outcome: { type: "completed", result: 3 } })
                }
                assert.ok(String(url).endsWith("/find-websocket"))
                events.push("connect")
                return Response.json({ websocketUrl: "wss://host.test/socket" })
            },
            actorHost: {
                async invoke() {
                    return assert.fail("unexpected direct invocation")
                },
                async publish(_target, actor, effects) {
                    assert.equal(actor.actorId, "destination")
                    assert.equal(effects.length, 1)
                    events.push("broadcast")
                }
            },
            connectWebSocket: async (): Promise<ActorConnection> => ({
                readyState: 1,
                send() {
                    assert.fail("unexpected socket send")
                },
                close() {
                    events.push("close")
                },
                addEventListener() {
                    assert.fail("unexpected socket listener")
                },
                removeEventListener() {
                    assert.fail("unexpected socket listener removal")
                }
            })
        }
    )
    const definition = registerActorClass(Relay, { actorName: "Relay", fields: [] })
    const reply = await runWithActorClient(transport, async () =>
        new ActorRuntime(definition, () => {}).handle({
            type: "invoke",
            request_id: "parent",
            actor: { project_id: "test", actor_name: "Relay", actor_id: "one" },
            method: "forward",
            args: [],
            sqlite: await seed(null)
        })
    )
    assertReply(reply, { type: "invoked", result: 3 })
    assert.deepEqual(events, ["invoke", "broadcast", "connect", "close"])
})
