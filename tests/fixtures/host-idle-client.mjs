import assert from "node:assert/strict"
import { setTimeout } from "node:timers/promises"

const { RemoteActorClient } = await import(process.argv[2])
const client = new RemoteActorClient()
assert.equal(await client.invoke("Counter", "one", "increment", []), 1)
let previous = await client.invoke("Counter", "one", "processId", [])
for (const [method, expected] of [
    ["read", 1],
    ["increment", 2]
]) {
    const target = await fetch(`${process.env.DURABLE_ACTORS_CONTROL_PLANE_URL}/v1/projects/default/actors/Counter/one/find-actor`, {
        method: "POST",
        headers: { authorization: `Bearer ${process.env.DURABLE_ACTORS_SECRET}`, "content-type": "application/json" },
        body: "{}"
    })
    assert.equal(target.status, 200)
    const { route } = await target.json()
    await setTimeout(2_000)
    await assert.rejects(fetch(route), error => error.cause?.code === "ECONNREFUSED")
    assert.equal(await client.invoke("Counter", "one", method, []), expected)
    const current = await client.invoke("Counter", "one", "processId", [])
    assert.notEqual(current, previous, "an idle local host should be replaced")
    previous = current
}
