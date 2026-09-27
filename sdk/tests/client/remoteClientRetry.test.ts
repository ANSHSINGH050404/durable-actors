import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"

import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError } from "../../src/errors.js"

for (const warm of [false, true])
    test(`${warm ? "warm" : "cold"} mutations with a lost HTTP response execute once and report outcome_unknown`, async t => {
        let executions = 0
        let origin = ""
        const host = createServer(async (request, response) => {
            const chunks: Buffer[] = []
            for await (const chunk of request) chunks.push(Buffer.from(chunk))
            const body = JSON.parse(Buffer.concat(chunks).toString())
            if (warm && body.method === "read") {
                response.setHeader("content-type", "application/json")
                response.end(
                    JSON.stringify({
                        target: { route: origin, token: "ticket", ownerEpoch: 1, expiresAtMs: Date.now() + 60_000 },
                        outcome: { type: "completed", result: null }
                    })
                )
                return
            }
            executions++
            request.socket.destroy()
        })
        t.after(() => host.close())
        host.listen(0, "127.0.0.1")
        await once(host, "listening")
        const address = host.address()
        assert.ok(address && typeof address !== "string")
        origin = `http://127.0.0.1:${address.port}`
        const client = new RemoteActorClient(
            { controlPlaneUrl: `http://127.0.0.1:${address.port}` },
            { telemetry: () => {} }
        )
        if (warm) await client.invoke("Counter", "one", "read", [])
        await assert.rejects(
            client.invoke("Counter", "one", "increment", [1]),
            error => error instanceof ActorInvocationError && error.code === "outcome_unknown"
        )
        assert.equal(executions, 1)
    })
