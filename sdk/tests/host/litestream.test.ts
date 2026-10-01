import assert from "node:assert/strict"
import { mkdtempSync, rmSync } from "node:fs"
import { createServer } from "node:http"
import { join } from "node:path"
import { test } from "node:test"

import { syncLitestream } from "../../src/host/litestream.js"

test("Litestream sync waits for replication and validates the database and position", async context => {
    const directory = mkdtempSync("/tmp/terse-ipc-test-")
    const socket = join(directory, "control.sock")
    let response: object = { path: "/actor.sqlite", txid: 7, replicated_txid: 7 }
    const server = createServer(async (request, reply) => {
        assert.equal(request.url, "/sync")
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.from(chunk))
        assert.deepEqual(JSON.parse(Buffer.concat(chunks).toString()), {
            path: "/actor.sqlite",
            wait: true,
            timeout: 30
        })
        reply.end(JSON.stringify(response))
    })
    await new Promise<void>(resolve => server.listen(socket, resolve))
    context.after(async () => {
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
        rmSync(directory, { recursive: true, force: true })
    })
    assert.equal(await syncLitestream({ path: "/actor.sqlite", socket }), 7)
    response = { path: "/actor.sqlite", txid: 7, replicated_txid: 6 }
    await assert.rejects(syncLitestream({ path: "/actor.sqlite", socket }), /replicated/)
    response = { path: "/another.sqlite", txid: 7, replicated_txid: 7 }
    await assert.rejects(syncLitestream({ path: "/actor.sqlite", socket }), /database/)
    response = { path: "/actor.sqlite", txid: 0, replicated_txid: 0 }
    await assert.rejects(syncLitestream({ path: "/actor.sqlite", socket }), /position/)
})
