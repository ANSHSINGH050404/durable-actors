import assert from "node:assert/strict"
import { copyFile, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import WebSocket from "ws"

import { createActorTransport } from "../../dist/backend.js"
import { startLocalActors } from "../../dist/localRuntime.js"
import { SocketProxy } from "../../dist/proxy.js"

const sdk = fileURLToPath(new URL("../..", import.meta.url))

test("reentrant RPC and socket lifecycle survive overlap and restart", { timeout: 120_000 }, async t => {
    const root = await mkdtemp(path.join(os.tmpdir(), "reentrant-e2e-"))
    const sockets = new Set()
    let runtime
    t.after(async () => {
        for (const socket of sockets) socket.terminate()
        await runtime?.stop()
        await rm(root, { recursive: true, force: true })
    })
    await mkdir(path.join(root, "node_modules"))
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await copyFile(new URL("../fixtures/reentrant-actor.ts", import.meta.url), path.join(root, "actors.ts"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(root, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                typeRoots: [path.join(sdk, "node_modules/@types")]
            }
        })
    )
    const entrypoint = "actors.ts"
    const start = () => startLocalActors({ projectId: "reentrant-test", project: root, entrypoint, quiet: process.env.REENTRANT_TEST_LOGS !== "1" })
    runtime = await start()
    const transport = createActorTransport(connectionSettings(runtime))
    const invoke = (method, ...args) => transport.invoke("ReentrantProbe", "room", method, args)
    const proxy = new SocketProxy({ ReentrantProbe: {}, SerialProbe: {} }, runtime.connection)
    const connect = async actorName => {
        const grant = await proxy.handle({ actorName, actorId: "room", metadata: {} })
        const socket = new WebSocket(grant.websocketUrl)
        sockets.add(socket)
        return messages(socket, t.signal)
    }
    const observer = await connect("ReentrantProbe")
    await observer.next(value => value.event === "connected")
    const holding = invoke("hold", "first")
    void holding.catch(() => {})
    await Promise.race([
        observer.next(value => value.event === "started"),
        holding.then(() => {
            throw new Error("hold completed before release")
        })
    ])
    assert.deepEqual(await invoke("read"), { count: 1, waiting: 1 })
    const newcomer = await connect("ReentrantProbe")
    assert.equal((await newcomer.next(value => value.event === "connected")).waiting, 1)
    newcomer.socket.send(JSON.stringify("ping"))
    assert.equal((await newcomer.next(value => value.event === "heartbeat")).waiting, 1)
    newcomer.socket.close()
    assert.equal((await observer.next(value => value.event === "disconnected")).waiting, 1)

    const failing = invoke("hold", "failure", true)
    const failed = assert.rejects(failing, /generation failed/)
    await observer.next(value => value.event === "started" && value.label === "failure")
    const increments = await Promise.all(Array.from({ length: 8 }, () => invoke("increment")))
    assert.deepEqual(
        increments.sort((a, b) => a - b),
        [3, 4, 5, 6, 7, 8, 9, 10]
    )
    await invoke("release", "failure")
    await failed
    assert.deepEqual(await invoke("read"), { count: 10, waiting: 1 })
    await invoke("release", "first")
    assert.equal(await holding, 10)
    assert.deepEqual(await invoke("read"), { count: 10, waiting: 0 })

    const ordinary = invoke("ordinaryHold")
    await observer.next(value => value.event === "ordinary-started")
    assert.equal(await invoke("observeOrdinary"), false)
    await ordinary

    const serial = await connect("SerialProbe")
    await serial.next(value => value === "connected")
    const serialHold = transport.invoke("SerialProbe", "room", "hold", [])
    await serial.next(value => value === "started")
    assert.equal(await transport.invoke("SerialProbe", "room", "read", []), false)
    await serialHold
    const interrupted = invoke("hold", "interrupted")
    const lost = assert.rejects(interrupted, error => error.code === "outcome_unknown")
    await observer.next(value => value.event === "started" && value.label === "interrupted")
    assert.deepEqual(await invoke("read"), { count: 11, waiting: 1 })
    await assert.rejects(invoke("crash"), error => error.code === "outcome_unknown")
    await lost
    const recovered = await recover(runtime)
    assert.deepEqual(recovered, { count: 11, waiting: 1 })
    for (const socket of sockets) socket.terminate()
    sockets.clear()
    await runtime.stop()
    runtime = await start()
    assert.deepEqual(await createActorTransport(connectionSettings(runtime)).invoke("ReentrantProbe", "room", "read", []), recovered)
})

async function recover(runtime) {
    const deadline = Date.now() + 10000
    while (true) {
        try {
            return await createActorTransport(connectionSettings(runtime)).invoke("ReentrantProbe", "room", "read", [])
        } catch (error) {
            if (Date.now() >= deadline) throw error
            await new Promise(resolve => setTimeout(resolve, 100))
        }
    }
}

function connectionSettings(runtime) {
    const { projectId, controlPlaneUrl, apiKey } = runtime.connection
    return { projectId, controlPlaneUrl, apiKey }
}

function messages(socket, signal) {
    const queued = []
    const waiting = []
    let failure
    const fail = error => {
        failure = error
        for (const pending of waiting.splice(0)) pending.reject(error)
    }
    socket.on("error", fail)
    socket.on("message", data => {
        const value = JSON.parse(data.toString())
        const index = waiting.findIndex(pending => pending.matches(value))
        if (index === -1) queued.push(value)
        else waiting.splice(index, 1)[0].resolve(value)
    })
    signal.addEventListener("abort", () => fail(new Error("test aborted")), { once: true })
    return {
        socket,
        next(matches) {
            const index = queued.findIndex(matches)
            if (index !== -1) return Promise.resolve(queued.splice(index, 1)[0])
            if (failure) return Promise.reject(failure)
            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error(`socket message timed out: ${matches}`)), 10000)
                waiting.push({
                    matches,
                    resolve: value => {
                        clearTimeout(timer)
                        resolve(value)
                    },
                    reject: error => {
                        clearTimeout(timer)
                        reject(error)
                    }
                })
            })
        }
    }
}
