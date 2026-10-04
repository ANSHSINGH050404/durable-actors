import assert from "node:assert/strict"
import { EventEmitter, once } from "node:events"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"

import type { SocketEffect } from "../../src/actor/socketProtocol.js"
import { buildActor } from "../../src/compiler/actor-build.js"
import { ActorWorker, ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"
import { fields, recover, seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"
import { execute, handle } from "../fixtures/worker-supervisor.js"

const actorIdentity = {
    project_id: "default",
    actor_name: "SessionCounter",
    actor_id: "counter-1"
}

test("worker eviction restores fields and SQL from the SQLite replica", { timeout: 30000 }, async context => {
    const root = await createTypeScriptConsumer("SqliteWorker")
    const file = path.join(root, "src/actors.ts")
    await writeFile(
        file,
        `import { Actor, Persisted } from ${JSON.stringify(fileURLToPath(new URL("../../src/index.js", import.meta.url)))}
        export class SqliteWorker extends Actor {
            @Persisted count = 0
            async increment(): Promise<number> {
                this.db.exec("CREATE TABLE IF NOT EXISTS entries (count INTEGER)")
                this.db.exec("INSERT INTO entries VALUES (?)", ++this.count)
                return this.count
            }
            async read(): Promise<{count: number}[]> {
                return this.db.exec<{count: number}>("SELECT count FROM entries ORDER BY count")
            }
        }`
    )
    const supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: await buildConsumer(root) })
    try {
        const command = await invokeCommand("one", "SqliteWorker")
        const first = await handle(supervisor, command, () => {})
        assert.equal(first.type, "invoked")
        if (first.type !== "invoked") return
        assert.equal(typeof first.sqlite, "object")
        assert.deepEqual(fields(command.sqlite), { count: 1 })
        await handle(supervisor, { type: "evict", actor: command.actor }, () => {})
        const restored = await handle(
            supervisor,
            {
                ...command,
                method: "read",
                sqlite: await recover(command.sqlite, first.sqlite)
            },
            () => {}
        )
        assert.equal(restored.type, "invoked")
        if (restored.type !== "invoked") return
        assert.deepEqual(restored.result, [{ count: 1 }])
        assert.ok(restored.sqlite.txid > 0)
    } finally {
        supervisor.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("keeps actor state resident between requests until the host closes", async context => {
    context.mock.timers.enable({ apis: ["setTimeout"] })
    let created = 0
    let terminated = 0
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        createWorker: () => {
            created++
            let count = 0
            return {
                state: "ready",
                ready: async () => ["SessionCounter"],
                execute: async () => ({ type: "invoked", result: ++count, sqlite: { txid: 1 } }),
                terminate() {
                    terminated++
                }
            }
        }
    })
    try {
        const command = await invokeCommand("counter-1", "SessionCounter")
        assert.deepEqual(await handle(supervisor, command, () => {}), {
            type: "invoked",
            result: 1,
            sqlite: { txid: 1 }
        })
        context.mock.timers.tick(300_000)
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
        assert.deepEqual(await handle(supervisor, { ...command, resident_only: true, sqlite: undefined }, () => {}), {
            type: "invoked",
            result: 2,
            sqlite: { txid: 1 }
        })
        assert.equal(created, 1)
        assert.equal(terminated, 0)
    } finally {
        supervisor.close()
    }
    assert.equal(terminated, 1)
    assert.deepEqual(supervisor.activeActors(), [])
})

test("correlates overlapping worker replies and socket lookups", { timeout: 10000 }, async () => {
    const root = await createTypeScriptConsumer("InterleavedWorker")
    const file = path.join(root, "src/actors.ts")
    await writeFile(
        file,
        `import { Actor, Interleave, Persisted } from ${JSON.stringify(fileURLToPath(new URL("../../src/index.js", import.meta.url)))}
        export class InterleavedWorker extends Actor {
            @Persisted count = 0
            @Interleave async hold() {
                const sockets = await this.getConnections()
                this.broadcast("started:" + sockets[0]?.id)
                await new Promise(resolve => setTimeout(resolve, 150))
                this.broadcast("ended:" + sockets[0]?.id)
                return ++this.count
            }
            async increment() { this.broadcast("increment"); return ++this.count }
        }`
    )
    const entrypoint = await buildConsumer(root)
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint
    })
    const events: string[] = []
    let started!: () => void
    const start = new Promise<void>(resolve => {
        started = resolve
    })
    try {
        const holding = handle(
            runtime,
            { ...(await invokeCommand("one", "InterleavedWorker")), method: "hold" },
            () => {},
            async effects => {
                for (const effect of effects) if (effect.type === "broadcast") events.push(effect.message.data)
                started()
            },
            async () => [{ id: "one", metadata: {}, tags: [] }]
        )
        await start
        const second = await handle(
            runtime,
            { ...(await invokeCommand("one", "InterleavedWorker")), request_id: "second" },
            () => {},
            async effects => {
                assert.deepEqual(
                    effects.map(effect => effect.type === "broadcast" && effect.message.data),
                    ['"increment"']
                )
            }
        )
        assert.equal("result" in second && second.result, 1)
        const first = await holding
        assert.equal("result" in first && first.result, 2)
        assert.deepEqual(events, ['"started:one"', '"ended:one"'])
        await handle(
            runtime,
            { type: "evict", actor: (await invokeCommand("one", "InterleavedWorker")).actor },
            () => {}
        )
        assert.deepEqual(runtime.activeActors(), [])
        const resumed = await handle(
            runtime,
            { ...(await invokeCommand("one", "InterleavedWorker")), sqlite: await seed({ count: 2 }) },
            () => {},
            async () => {}
        )
        assert.ok(resumed.type === "invoked")
        assert.equal(resumed.result, 3)
        assert.equal(resumed.sequence, 3)
    } finally {
        runtime.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("a generic Bun worker is warm before customer code is assigned", async () => {
    const worker = new ActorWorker()
    try {
        await worker.warm()
        const root = await createTypeScriptConsumer(
            "WarmCounter",
            'if (process.env.TEST_WARM_SECRET !== "warm-secret") throw new Error("secret missing at import")'
        )
        try {
            const moduleUrl = await buildConsumer(root)
            worker.load({ moduleUrl, environment: { TEST_WARM_SECRET: "warm-secret" } }, () => {})
            assert.deepEqual(await worker.ready(), ["WarmCounter"])
            assert.throws(() => worker.load({ moduleUrl }, () => {}), /already assigned/)
            const command = await invokeCommand("one", "WarmCounter")
            assert.deepEqual(
                await execute(
                    worker,
                    { type: "hydrate", actor: command.actor, sqlite: await seed({ count: 41 }) },
                    () => {}
                ),
                {
                    type: "hydrated"
                }
            )
            assertReply(await execute(worker, { ...command, resident_only: true, sqlite: undefined }, () => {}), {
                type: "invoked",
                result: 42
            })
        } finally {
            await rm(root, { recursive: true, force: true })
        }
    } finally {
        worker.terminate("test finished")
    }
})

test("an executor never accepts a second actor identity, even after eviction", async () => {
    const root = await createTypeScriptConsumer("BoundCounter")
    const entrypoint = await buildConsumer(root)
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint
    })
    try {
        const first = await invokeCommand("first", "BoundCounter")
        assert.equal((await handle(runtime, first, () => {})).type, "invoked")
        await handle(runtime, { type: "evict", actor: first.actor }, () => {})
        const reply = await handle(runtime, await invokeCommand("second", "BoundCounter"), () => {})
        assert.equal(reply.type, "failed")
        if (reply.type === "failed") assert.equal(reply.code, "actor_identity_mismatch")
    } finally {
        runtime.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("keeps an actor resident until Rust explicitly evicts it", async () => {
    const consumerRoot = await createTypeScriptConsumer()
    const entrypoint = await buildConsumer(consumerRoot)
    try {
        await exerciseActiveActors(entrypoint)
        await exerciseSocketRecovery(entrypoint)
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

test("reports a new actor only after its Worker is ready", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer()
    const entrypoint = await buildConsumer(root)
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint
    })
    try {
        const starting = handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(supervisor.activeActors(), [])
        assert.equal((await starting).type, "invoked")
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
    } finally {
        supervisor.close()
        await rm(root, { recursive: true, force: true })
    }
})

for (const [reason, action] of [
    ["exit", "process.exit(0)"],
    ["uncaught error", 'throw new Error("worker crashed")']
]) {
    test(`publishes an idle Worker's ${reason} without another invocation`, { timeout: 10_000 }, async context => {
        const root = await createTypeScriptConsumer(
            "SessionCounter",
            `
import { watch } from "node:fs"
watch(new URL(".", import.meta.url), (_, name) => {
    if (name === "stop") { ${action} }
})`
        )
        const entrypoint = await buildConsumer(root)
        const supervisor = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint
        })
        try {
            await handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
            const changes = new EventEmitter()
            supervisor.onActiveActorsChange(() => changes.emit("change"))
            const stopped = once(changes, "change", { signal: context.signal })
            // Idle Workers are unreferenced, so the deadline also keeps the test alive.
            const deadline = setTimeout(() => changes.emit("error", new Error("Worker exit was not reported")), 5_000)
            context.after(() => clearTimeout(deadline))
            await writeFile(path.join(root, "src/stop"), "")
            await stopped
            assert.deepEqual(supervisor.activeActors(), [])
        } finally {
            supervisor.close()
            await rm(root, { recursive: true, force: true })
        }
    })
}

test("starts one speculative Worker and gives it to the first actor", async () => {
    const consumerRoot = await createTypeScriptConsumer("PreloadedCounter")
    const entrypoint = await buildConsumer(consumerRoot)
    const created: number[] = []
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            createWorker: () => {
                created.push(created.length + 1)
                return {
                    state: "ready",

                    async ready() {
                        return ["PreloadedCounter"]
                    },
                    async execute() {
                        return { type: "invoked", result: null, sqlite: { txid: 1 } }
                    },
                    terminate() {}
                }
            }
        })

        assert.equal(created.length, 1)
        await handle(runtime, await invokeCommand("counter-1", "PreloadedCounter"), () => {})
        assert.equal(created.length, 1)
        const second = await handle(runtime, await invokeCommand("counter-2", "PreloadedCounter"), () => {})
        assert.equal(second.type, "failed")
        assert.equal(created.length, 1)
        runtime.close()
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

test("thrown methods and socket handlers roll back state without restarting the worker", async () => {
    const root = await createTypeScriptConsumer()
    const entrypoint = await buildConsumer(root)
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint
    })
    const seen: number[] = []
    supervisor.onActiveActorsChange(() => seen.push(supervisor.activeActors().length))
    const command = await invokeCommand("counter-1", "SessionCounter")
    try {
        await handle(supervisor, command, () => {})
        const workerId = await handle(supervisor, { ...command, method: "workerId" }, () => {})
        for (const failure of [
            { ...command, method: "explode", resident_only: true, sqlite: undefined },
            {
                type: "websocket_event" as const,
                request_id: "socket-failure",
                actor: actorIdentity,
                resident_only: true,
                event: {
                    type: "message" as const,
                    connection_id: "socket-1",
                    message: { type: "text" as const, data: JSON.stringify({ text: "fail" }) }
                },
                connections: [{ id: "socket-1", metadata: { userId: "user-1" }, tags: [] }]
            }
        ]) {
            assert.equal((await handle(supervisor, failure, () => {})).type, "failed")
            assert.deepEqual(supervisor.activeActors(), [actorIdentity])
            assert.deepEqual(seen, [1])
            assertReply(
                await handle(
                    supervisor,
                    { ...command, method: "getCount", resident_only: true, sqlite: undefined },
                    () => {}
                ),
                {
                    type: "invoked",
                    result: 1
                }
            )
            assert.deepEqual(await handle(supervisor, { ...command, method: "workerId" }, () => {}), workerId)
        }
    } finally {
        supervisor.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("keeps the preloaded Worker available until the first request", async context => {
    context.mock.timers.enable({ apis: ["setTimeout"] })
    let created = 0
    let terminated = 0
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        createWorker: () => {
            created++
            return {
                state: "ready",
                ready: async () => ["SessionCounter"],
                execute: async () => ({ type: "invoked", result: null, sqlite: { txid: 1 } }),
                terminate() {
                    terminated++
                }
            }
        }
    })
    try {
        await supervisor.ready()
        context.mock.timers.tick(300_000)
        await handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.equal(created, 1)
        assert.equal(terminated, 0)
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
    } finally {
        supervisor.close()
    }
    assert.equal(terminated, 1)
})

test("eviction during Worker startup settles the invocation and allows recovery", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer("CancelledCounter")
    const entrypoint = await buildConsumer(root)
    const command = await invokeCommand("counter-1", "CancelledCounter")
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint
        })
        await runtime.ready()
        const pending = handle(runtime, command, () => {})
        await handle(runtime, { type: "evict", actor: command.actor }, () => {})
        const reply = await pending
        assert.equal(reply.type, "failed")
        if (reply.type === "failed") assert.equal(reply.code, "actor_worker_terminated")
        assertReply(await handle(runtime, { ...command, sqlite: await seed({ count: 9 }) }, () => {}), {
            type: "invoked",
            result: 10
        })
        await handle(runtime, { type: "evict", actor: command.actor }, () => {})
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("discards a failed preload before accepting the first actor", async () => {
    const root = await createTypeScriptConsumer("RetryPreloadCounter")
    const entrypoint = await buildConsumer(root)
    let created = 0
    let terminated = 0
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            createWorker: () => {
                const failed = created++ === 0
                return {
                    state: failed ? "stopped" : "ready",

                    ready: () =>
                        failed ? Promise.reject(new Error("preload failed")) : Promise.resolve(["RetryPreloadCounter"]),
                    async execute() {
                        if (failed) throw new Error("preload failed")
                        return { type: "invoked", result: 1, sqlite: { txid: 1 } }
                    },
                    terminate() {
                        terminated += 1
                    }
                }
            }
        })
        await new Promise(resolve => setImmediate(resolve))
        assert.equal(terminated, 1)
        assert.deepEqual(await handle(runtime, await invokeCommand("counter-1", "RetryPreloadCounter"), () => {}), {
            type: "invoked",
            result: 1,
            sqlite: { txid: 1 }
        })
        assert.equal(created, 2)
        runtime.close()
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("closing the supervisor terminates an unused Worker and rejects new work", async () => {
    let terminated = 0
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        createWorker: () => ({
            state: "ready",

            async ready() {
                return ["UnusedCounter"]
            },
            async execute() {
                throw new Error("closed supervisor must not execute")
            },
            terminate() {
                terminated += 1
            }
        })
    })
    runtime.close()
    runtime.close()
    assert.equal(terminated, 1)
    const reply = await handle(runtime, await invokeCommand("counter-1", "UnusedCounter"), () => {})
    assert.equal(reply.type, "failed")
    if (reply.type === "failed") assert.equal(reply.code, "actor_worker_terminated")
})

test("an actor module that fails inside a Worker returns a failure without hanging", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer(
        "FailedImportCounter",
        'import { isMainThread } from "node:worker_threads"\nif (!isMainThread) throw new Error("worker import failed")'
    )
    const entrypoint = await buildConsumer(root)
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint
        })
        try {
            const reply = await handle(runtime, await invokeCommand("counter-1", "FailedImportCounter"), () => {})
            assert.equal(reply.type, "failed")
            if (reply.type === "failed") assert.match(reply.message, /worker import failed/)
        } finally {
            runtime.close()
        }
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

async function exerciseActiveActors(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint
    })
    assert.deepEqual(await runtime.ready(), ["SessionCounter"])

    assertReply(
        await handle(
            runtime,
            {
                type: "invoke",
                request_id: "request-1",
                actor: actorIdentity,
                method: "increment",
                args: [2],
                sqlite: await seed(null)
            },
            () => {}
        ),
        { type: "invoked", result: 2 }
    )
    assertReply(
        await handle(
            runtime,
            {
                type: "invoke",
                request_id: "request-2",
                actor: actorIdentity,
                method: "increment",
                args: [3],
                sqlite: await seed({ count: 0 })
            },
            () => {}
        ),
        { type: "invoked", result: 5 }
    )
    assert.deepEqual(await handle(runtime, { type: "evict", actor: actorIdentity }, () => {}), { type: "evicted" })
    assertReply(
        await handle(
            runtime,
            {
                type: "invoke",
                request_id: "request-3",
                actor: actorIdentity,
                method: "getCount",
                args: [],
                sqlite: await seed({ count: 2 })
            },
            () => {}
        ),
        { type: "invoked", result: 2 }
    )
    runtime.close()
}

async function exerciseSocketRecovery(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint
    })
    const connection = { id: "socket-1", metadata: { userId: "user-1" }, tags: [] }
    assertReply(
        await handle(
            runtime,
            {
                type: "websocket_event",
                request_id: "socket-request-1",
                actor: actorIdentity,
                event: { type: "connect", connection },
                connections: [connection],
                sqlite: await seed(null)
            },
            () => {}
        ),
        {
            type: "websocket_handled",

            effects: []
        }
    )
    assert.deepEqual(runtime.activeActors(), [actorIdentity])
    await handle(runtime, { type: "evict", actor: actorIdentity }, () => {})
    assert.deepEqual(runtime.activeActors(), [])
    const published: SocketEffect[] = []
    assertReply(
        await handle(
            runtime,
            {
                type: "websocket_event",
                request_id: "socket-request-2",
                actor: actorIdentity,
                event: {
                    type: "message",
                    connection_id: "socket-1",
                    message: { type: "text", data: JSON.stringify({ text: "hello" }) }
                },
                connections: [connection],
                sqlite: await seed({ count: 1 })
            },
            () => {},
            async effects => {
                published.push(...effects)
            }
        ),
        {
            type: "websocket_handled",

            effects: []
        }
    )
    assert.deepEqual(published, [
        {
            type: "send",
            connection_id: "socket-1",
            message: { type: "text", data: JSON.stringify({ text: "user-1:hello" }) }
        }
    ])
    runtime.close()
}

async function buildConsumer(root: string): Promise<string> {
    const entrypoint = path.join(root, "src/actors.ts")
    const output = path.join(root, "src/actors.mjs")
    await buildActor(entrypoint, output, { local: true })
    return pathToFileURL(output).href
}

async function createTypeScriptConsumer(actorName = "SessionCounter", preamble = ""): Promise<string> {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-actors-worker-"))
    const source = path.join(root, "src")
    await mkdir(source)
    const compiledSdkRoot = fileURLToPath(new URL("../../src/", import.meta.url))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(source, "actors.ts"),
        `import { Actor, Persisted, Ephemeral } from ${JSON.stringify(path.join(compiledSdkRoot, "index.js"))}
import { threadId } from "node:worker_threads"
${preamble}

export class ${actorName} extends Actor<{ userId: string }, { text: string }> {
    @Persisted count = 0
    @Ephemeral cache = new Map<string, number>()

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async getCount(): Promise<number> {
        return this.count
    }

    async workerId(): Promise<number> {
        return threadId
    }

    async explode(): Promise<void> {
        this.count = 999
        throw new Error("failed")
    }

    async onConnect(): Promise<void> {
        this.count += 1
    }

    async onMessage(socket: { metadata: { userId: string }, send(message: { text: string }): void }, message: { text: string }): Promise<void> {
        this.count += 1
        if (message.text === "fail") throw new Error("failed")
        socket.send({ text: \`${"${socket.metadata.userId}"}:${"${message.text}"}\` })
    }
}
`
    )
    return root
}

async function invokeCommand(actorId: string, actorName: string) {
    return {
        type: "invoke" as const,
        request_id: `request-${actorId}`,
        actor: { ...actorIdentity, actor_name: actorName, actor_id: actorId },
        method: "increment",
        args: [],
        sqlite: await seed(null)
    }
}

test("residency reports actual workers and drops evicted and failed instances", async () => {
    let fail = false
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        createWorker: () => ({
            state: "ready",

            ready: async () => ["SessionCounter"],
            execute: async () =>
                fail
                    ? { type: "failed", code: "test", message: "failed" }
                    : { type: "invoked", result: null, sqlite: { txid: 1 } },
            terminate() {}
        })
    })
    try {
        assert.deepEqual(supervisor.activeActors(), [])
        await handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
        await handle(supervisor, { type: "evict", actor: actorIdentity }, () => {})
        assert.deepEqual(supervisor.activeActors(), [])
        fail = true
        await handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(supervisor.activeActors(), [])
    } finally {
        supervisor.close()
    }
})

test("residency subscribers see worker creation and eviction immediately", async () => {
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        createWorker: () => ({
            state: "ready",

            ready: async () => ["SessionCounter"],
            execute: async () => ({ type: "invoked", result: null, sqlite: { txid: 1 } }),
            terminate() {}
        })
    })
    const seen: number[] = []
    try {
        const unsubscribe = supervisor.onActiveActorsChange(() => seen.push(supervisor.activeActors().length))
        await handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
        await handle(supervisor, { type: "evict", actor: actorIdentity }, () => {})
        assert.deepEqual(seen, [1, 0])
        unsubscribe()
        await handle(supervisor, await invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(seen, [1, 0])
    } finally {
        supervisor.close()
    }
})
