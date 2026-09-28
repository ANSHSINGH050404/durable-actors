import { FSWatcher } from "chokidar"
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { promisify } from "node:util"

import { ActorSourceWatcher, watchActorSources } from "../../src/cli/actor-source-watcher.js"

for (const code of ["EMFILE", "ENFILE", "ENOSPC"]) {
    test(`${code} disables reload and cancels pending updates`, async t => {
        const source = new FSWatcher()
        const close = t.mock.method(source, "close")
        const refresh = t.mock.fn(async () => {})
        const warnings: string[] = []
        const watcher = new ActorSourceWatcher(
            { projectDirectory: "/project" },
            refresh,
            () => source,
            message => warnings.push(message)
        )
        const started = watcher.start()
        const error = Object.assign(new Error("watch limit reached"), { code })
        source.emit("ready")
        await started
        source.emit("all", "change", "/project/src/actors.ts")
        source.emit("error", error)
        await watcher.close()
        source.emit("error", error)
        assert.equal(close.mock.callCount(), 1)
        assert.equal(refresh.mock.callCount(), 0)
        assert.equal(warnings.length, 1)
        assert.match(warnings[0]!, new RegExp(code))
        assert.match(warnings[0]!, /automatic reload.*disabled/i)
        assert.match(warnings[0]!, /restart.*changes/i)
        assert.match(warnings[0]!, /--no-watch/)
    })
}

test("unexpected watcher startup failures still surface", async t => {
    const source = new FSWatcher()
    const close = t.mock.method(source, "close")
    const watcher = new ActorSourceWatcher(
        { projectDirectory: "/project" },
        async () => {},
        () => source,
        assert.fail
    )
    const started = watcher.start()
    const error = Object.assign(new Error("permission denied"), { code: "EACCES" })
    source.emit("error", error)
    await assert.rejects(started, error)
    assert.equal(close.mock.callCount(), 1)
})

test("reloads nested sources in projects containing sockets and FIFOs", { timeout: 10_000 }, async t => {
    const root = await mkdtemp(path.join(tmpdir(), "aw-"))
    const server = createServer()
    let watcher: Awaited<ReturnType<typeof watchActorSources>> | undefined
    t.after(async () => {
        await watcher?.close()
        if (server.listening)
            await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
        await rm(root, { recursive: true, force: true })
    })
    await mkdir(path.join(root, ".config"))
    server.listen(path.join(root, ".config", "client.sock"))
    await once(server, "listening")
    await promisify(execFile)("mkfifo", [path.join(root, ".config", "events.ts")])
    let refresh!: () => void
    const refreshed = new Promise<void>(resolve => {
        refresh = resolve
    })
    watcher = await watchActorSources({ projectDirectory: root }, async () => refresh())
    await mkdir(path.join(root, "src"))
    await writeFile(path.join(root, "src", "actors.ts"), "export {}")
    await refreshed
})

test("rebuilds for configuration and dependency changes", { timeout: 15_000 }, async t => {
    const root = await mkdtemp(path.join(tmpdir(), "actor-watch-"))
    let changed: (() => void) | undefined
    const watcher = await watchActorSources({ projectDirectory: root }, async () => changed?.())
    t.after(async () => {
        await watcher.close()
        await rm(root, { recursive: true, force: true })
    })
    for (const file of ["tsconfig.json", "package.json", "pnpm-lock.yaml", "helper.js"]) {
        const refreshed = new Promise<void>((resolve, reject) => {
            const deadline = setTimeout(() => reject(new Error(`${file} did not trigger a rebuild`)), 2500)
            changed = () => {
                clearTimeout(deadline)
                resolve()
            }
        })
        await writeFile(path.join(root, file), "{}")
        await refreshed
    }
})
