import assert from "node:assert/strict"
import { once } from "node:events"
import { mkdtemp, rm, unlink, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import type { Socket } from "node:net"
import { createInterface } from "node:readline"
import { before, test } from "node:test"
import { fileURLToPath } from "node:url"

import { buildActor } from "../../src/compiler/actor-build.js"
import { ActorSession, parseHostSettings } from "../../src/host/actor-host.js"
import type { ActorExecutorReply } from "../../src/host/protocol.js"
import { ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"
import { commit, seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"

before(
    async () => {
        await actorBundle()
    },
    { timeout: 30_000 }
)

test("loads a prepared JavaScript artifact only inside the first execution Worker", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-discovery-")
    const entrypoint = `${root}/actors.js`
    const fixture = await actorBundle()
    await writeFile(`${root}/package.json`, JSON.stringify({ type: "module" }))
    await writeFile(
        entrypoint,
        `import { isMainThread } from "node:worker_threads"; if (isMainThread) throw new Error("customer code loaded in supervisor"); export * from ${JSON.stringify(fixture)};`
    )
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.once("line", line => {
            assert.deepEqual(JSON.parse(line).actor_names, ["SessionCounter"])
            socket.write(`${JSON.stringify({ type: "attached", protocol: 24 })}\n`)
            socket.end()
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: entrypoint
        })
    )
    try {
        await session.start()
        await session.waitUntilDisconnected()
    } finally {
        server.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("a stalled actor import times out and closes the Worker", { timeout: 5_000 }, async () => {
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `/tmp/ta-unused-${process.pid}.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle(),
            DURABLE_ACTORS_HOST_STARTUP_MS: "20"
        }),
        () => ({
            ready: () => new Promise(() => {}),
            async handle() {
                return { type: "evicted" }
            },
            close() {
                closed += 1
            },
            activeActors: () => [],
            onActiveActorsChange: () => () => {}
        })
    )
    await assert.rejects(session.start(), /actor module loading timed out/)
    assert.equal(closed, 1)
})

test("the actor session carries only owned execution commands", async t => {
    const socketPath = `/tmp/ta-session-${process.pid}.sock`
    await removeSocket(socketPath)
    const server = createServer()
    const customerSocketPromise = new Promise<Socket>(resolve => {
        server.once("connection", resolve)
    })
    server.listen(socketPath)
    await once(server, "listening")
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        options => {
            const supervisor = new ActorWorkerSupervisor(options)
            const close = supervisor.close.bind(supervisor)
            t.mock.method(supervisor, "close", () => {
                closed += 1
                close()
            })
            return supervisor
        }
    )
    const startup = session.start()

    try {
        const customerSocket = await Promise.race([customerSocketPromise, startup.then(() => customerSocketPromise)])
        const lines = createInterface({ input: customerSocket, crlfDelay: Infinity })
        const iterator = lines[Symbol.asyncIterator]()

        assert.deepEqual(await readMessage(iterator), {
            type: "attach",
            protocol: 24,
            actor_names: ["SessionCounter"]
        })
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 24 })}\n`)
        await startup

        const sqlite = await seed(null)
        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 1,
                command: {
                    type: "invoke",
                    request_id: "request-1",
                    actor: {
                        project_id: "default",
                        actor_name: "SessionCounter",
                        actor_id: "counter-1"
                    },
                    method: "increment",
                    args: [4],
                    sqlite
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), { type: "commit_sqlite", message_id: 1 })
        customerSocket.write(
            `${JSON.stringify({ type: "sqlite_committed", message_id: 1, txid: await commit(sqlite) })}\n`
        )
        assertSessionReply(await readMessage(iterator), 1, { type: "invoked", result: 4 })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 100,
                command: {
                    type: "invoke",
                    request_id: "stream",
                    actor: { project_id: "default", actor_name: "SessionCounter", actor_id: "counter-1" },
                    method: "stream",
                    args: [],
                    sqlite: await seed({ count: 4 })
                }
            })}\n`
        )
        for (const data of ["first", "last"]) {
            assert.deepEqual(await readMessage(iterator), {
                type: "socket_effects",
                message_id: 100,
                effects: [
                    {
                        type: "broadcast",
                        message: { type: "text", data: JSON.stringify({ delta: data }) },
                        except_connection_ids: [],
                        tags: []
                    }
                ]
            })
            customerSocket.write(`${JSON.stringify({ type: "socket_effects_published", message_id: 100 })}\n`)
        }
        assertSessionReply(await readMessage(iterator), 100, { type: "invoked", result: 4 })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 3,
                command: {
                    type: "invoke",
                    request_id: "request-3",
                    actor: actorIdentity(),
                    method: "increment",
                    args: [1],
                    sqlite: await seed({ count: 4 })
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), { type: "commit_sqlite", message_id: 3 })
        customerSocket.write(
            `${JSON.stringify({ type: "sqlite_committed", message_id: 3, txid: await commit(sqlite) })}\n`
        )
        assertSessionReply(await readMessage(iterator), 3, { type: "invoked", result: 5 })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 4,
                command: {
                    type: "evict",
                    actor: actorIdentity()
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 4,
            reply: { type: "evicted" }
        })

        customerSocket.end()
        await session.waitUntilDisconnected()
        assert.equal(closed, 1)
        lines.close()
        server.close()
        await once(server, "close")
    } finally {
        if (server.listening) server.close()
        await removeSocket(socketPath)
    }
})

test("a failed session connection cleans up the speculative Worker", async () => {
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `/tmp/ta-missing-${process.pid}.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        () => ({
            async ready() {
                return ["SessionCounter"]
            },
            async handle() {
                return { type: "evicted" }
            },
            close() {
                closed += 1
            },
            activeActors: () => [],
            onActiveActorsChange: () => () => {}
        })
    )
    await assert.rejects(session.start(), /could not attach to Rust host/)
    assert.equal(closed, 1)
})

function actorIdentity(): Record<string, string> {
    return {
        project_id: "default",
        actor_name: "SessionCounter",
        actor_id: "counter-1"
    }
}

function assertSessionReply(message: unknown, id: number, expected: object): void {
    const { reply, ...envelope } = message as { reply: ActorExecutorReply; type: string; message_id: number }
    assert.deepEqual(envelope, { type: "reply", message_id: id })
    assertReply(reply, expected)
}

async function readMessage(iterator: AsyncIterator<string>): Promise<unknown> {
    const next = await iterator.next()
    assert.equal(next.done, false)
    const message: unknown = JSON.parse(next.value ?? "")
    return message
}

async function removeSocket(socketPath: string): Promise<void> {
    try {
        await unlink(socketPath)
    } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error
    }
}

test("reports resident instances when the Rust host advertises support", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-residency-")
    const actor = { project_id: "default", actor_name: "SessionCounter", actor_id: "one" }
    let received: unknown
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.on("line", line => {
            const message = JSON.parse(line)
            if (message.type === "attach")
                socket.write(`${JSON.stringify({ type: "attached", protocol: 24, supports_residency: true })}\n`)
            else if (message.type === "residency") {
                received = message.actors
                socket.end()
            }
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        () => ({
            ready: async () => ["SessionCounter"],
            handle: async () => ({ type: "evicted" }),
            close() {},
            activeActors: () => [actor],
            onActiveActorsChange: () => () => {}
        })
    )
    try {
        await session.start()
        await session.waitUntilDisconnected()
        assert.deepEqual(received, [actor])
    } finally {
        server.close()
        await rm(root, { recursive: true, force: true })
    }
})

let preparedFixture: Promise<string> | undefined

function actorBundle(): Promise<string> {
    return (preparedFixture ??= (async () => {
        const source = fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        const output = fileURLToPath(new URL("../fixtures/actorSession.mjs", import.meta.url))
        await buildActor(source, output, { local: true })
        return output
    })())
}
