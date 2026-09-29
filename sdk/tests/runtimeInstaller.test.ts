import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { c } from "tar"

import { RuntimeInstaller } from "../src/runtimeInstaller.js"

test("installs the runtime from a verified release and reuses the cache offline", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const archive = await fixture(directory)
    const requests: string[] = []
    const download = async (url: string) => {
        requests.push(url)
        return url.endsWith(".sha256") ? Buffer.from(createHash("sha256").update(archive).digest("hex")) : archive
    }
    const options = {
        version: "1.2.3",
        platform: "darwin",
        arch: "arm64",
        cacheDirectory: path.join(directory, "cache")
    }
    const binary = await new RuntimeInstaller(options, download).install()
    assert.equal(await readFile(binary, "utf8"), "runtime")
    assert.equal(requests.length, 2)
    assert.deepEqual(requests, [
        "https://github.com/TerseAI/durable-actors/releases/download/v1.2.3/durable-actors-darwin-arm64.tar.gz",
        "https://github.com/TerseAI/durable-actors/releases/download/v1.2.3/durable-actors-darwin-arm64.tar.gz.sha256"
    ])
    const offline = new RuntimeInstaller(options, async () => {
        throw new Error("offline")
    })
    assert.equal(await offline.install(), binary)
})

test("rejects a damaged archive before exposing executables", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "linux", arch: "x64", cacheDirectory: directory },
        async url => (url.endsWith(".sha256") ? Buffer.from("0".repeat(64)) : Buffer.from("corrupt"))
    )
    await assert.rejects(installer.install(), /checksum/i)
})

test("unsupported systems get an actionable error without downloading", async () => {
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "win32", arch: "x64", cacheDirectory: "unused" },
        async () => {
            throw new Error("unexpected download")
        }
    )
    await assert.rejects(installer.install(), /WSL/)
})

async function fixture(directory: string): Promise<Buffer> {
    await writeFile(path.join(directory, "durable-actors"), "runtime")
    const archive = path.join(directory, "runtime.tar.gz")
    await c({ gzip: true, file: archive, cwd: directory }, ["durable-actors"])
    return readFile(archive)
}
