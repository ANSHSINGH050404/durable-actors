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
    await setTimeout(2_000)
    assert.throws(
        () => process.kill(previous, 0),
        error => error.code === "ESRCH",
        "the idle sandbox process exits"
    )
    assert.equal((await fetch(`${process.env.DURABLE_ACTORS_CONTROL_PLANE_URL}/healthz`)).status, 200)
    assert.equal(await client.invoke("Counter", "one", method, []), expected)
    const current = await client.invoke("Counter", "one", "processId", [])
    assert.notEqual(current, previous, "an idle local host should be replaced")
    previous = current
}
