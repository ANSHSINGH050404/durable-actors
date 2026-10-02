import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { promisify } from "node:util"

import { RuntimeBuilder } from "../../scripts/build-runtime.mjs"

const execute = promisify(execFile)

test("native builds package the runtime and replication tools with a matching checksum", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-bundle-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "target/release"), { recursive: true })
    await mkdir(path.join(root, "third_party/terse-ltx"), { recursive: true })
    await writeFile(path.join(root, "third_party/terse-ltx/LICENSE"), "license")
    const run = async (command, args, options) => {
        if (command === "cargo") {
            await mkdir(path.join(root, "target/release"), { recursive: true })
            await writeFile(path.join(root, "target/release/durable-actors"), "runtime", { mode: 0o755 })
        } else {
            assert.equal(command, "tar")
            return execute(command, args, options)
        }
    }
    const installLitestream = async directory => {
        await writeFile(path.join(directory, "litestream"), "litestream", { mode: 0o755 })
        await writeFile(path.join(directory, "LICENSE.litestream"), "license")
    }
    const archive = await new RuntimeBuilder({ root, platform: "linux", arch: "arm64" }, run, installLitestream).build()
    assert.equal(path.basename(archive), "durable-actors-linux-arm64.tar.gz")
    const checksum = createHash("sha256")
        .update(await readFile(archive))
        .digest("hex")
    assert.equal(await readFile(`${archive}.sha256`, "utf8"), `${checksum}  ${path.basename(archive)}\n`)
    const extracted = path.join(root, "extracted")
    await mkdir(extracted)
    await execute("tar", ["-xzf", archive, "-C", extracted])
    assert.equal(await readFile(path.join(extracted, "durable-actors"), "utf8"), "runtime")
    await execute("test", ["-x", path.join(extracted, "durable-actors")])
    assert.equal(await readFile(path.join(extracted, "litestream"), "utf8"), "litestream")
    await execute("test", ["-x", path.join(extracted, "litestream")])
    assert.equal(await readFile(path.join(extracted, "LICENSE.terse-ltx"), "utf8"), "license")
})

test("a compiler failure does not publish a native bundle", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-bundle-failure-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const builder = new RuntimeBuilder({ root }, async () => {
        throw new Error("compiler failed")
    })
    await assert.rejects(builder.build(), /compiler failed/)
    await assert.rejects(readFile(path.join(root, `dist-runtime/durable-actors-${process.platform}-${process.arch}.tar.gz`)), { code: "ENOENT" })
})
