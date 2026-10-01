import assert from "node:assert/strict"
import { mkdtemp, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"

import { installLitestream } from "../../scripts/litestream.mjs"

test("Litestream packaging rejects altered release bytes before extraction", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "terse-litestream-package-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await assert.rejects(
        installLitestream(directory, { platform: "linux", arch: "x64" }, async url => {
            assert.equal(url, "https://github.com/benbjohnson/litestream/releases/download/v0.5.17/litestream-0.5.17-linux-x86_64.tar.gz")
            return Buffer.from("altered")
        }),
        /checksum/
    )
    assert.deepEqual(await readdir(directory), [])
})

test("unsupported Litestream platforms fail before downloading", async () => {
    await assert.rejects(
        installLitestream("unused", { platform: "win32", arch: "x64" }, async () => {
            assert.fail("unexpected download")
        }),
        /No Litestream release/
    )
})
