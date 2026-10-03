import assert from "node:assert/strict"
import { setTimeout } from "node:timers/promises"

const { RemoteActorClient } = await import(process.argv[2])
const client = new RemoteActorClient()
const socket = await client.connect("Counter", "one", null)
const messages = []
socket.addEventListener("message", event => messages.push(event.data))
const inspect = () => client.invoke("Counter", "one", "inspect", [])
async function message() {
    socket.send({ text: "wake up" })
    const deadline = Date.now() + 5_000
    while (messages.length === 0 && Date.now() < deadline) await setTimeout(10)
    const reply = messages.shift()
    assert.ok(reply, "the existing WebSocket receives a reply")
    return reply
}
try {
    const deadline = Date.now() + 5000
    let first = await inspect()
    while ((first.count === 0 || first.sockets.length === 0) && Date.now() < deadline) {
        await setTimeout(10)
        first = await inspect()
    }
    assert.equal(first.count, 1)
    assert.equal(first.sockets.length, 1)
    assert.deepEqual(first.sockets[0].metadata, { user: "restored" })
    assert.deepEqual(first.sockets[0].tags, ["room"])
    for (let i = 0; i < 4; i++) {
        await setTimeout(200)
        assert.equal((await inspect()).instance, first.instance, "method calls reset the idle timer")
    }
    assert.equal((await client.invoke("Counter", "one", "hold", [])).instance, first.instance, "running handlers are not evicted")
    await setTimeout(1_500)
    assert.equal(socket.readyState, 1)
    const second = await message()
    assert.notEqual(second.instance, first.instance, "an open WebSocket must not pin the actor instance")
    assert.notEqual(second.pid, first.pid, "the actor sandbox shuts down while the gateway keeps sockets alive")
    assert.equal(second.count, 2)
    assert.deepEqual(second.sockets, first.sockets)
    for (let i = 0; i < 4; i++) {
        await setTimeout(200)
        assert.equal((await message()).instance, second.instance, "WebSocket messages reset the idle timer")
    }
    await setTimeout(1_500)
    const third = await inspect()
    assert.notEqual(third.instance, second.instance, "a direct method also rehydrates the actor")
    assert.equal(third.count, 6)
    assert.deepEqual(third.sockets, first.sockets)
} finally {
    socket.close()
}
