import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { SocketEffect } from "../../src/actor/socketProtocol.js"
import type { ActorWorkerSupervisor as Supervisor } from "../../src/host/worker-supervisor.js"

import { commit, seed } from "./litestream.js"

const [sdk, artifact] = process.argv.slice(2)
const { ActorWorkerSupervisor } = await import(pathToFileURL(path.join(sdk!, "dist/host/worker-supervisor.js")).href)
const supervisor: Supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: pathToFileURL(artifact!).href })
try {
    assert.deepEqual(await supervisor.ready(), ["Counter"])
    const effects: SocketEffect[] = []
    const sqlite = await seed(null)
    const reply = await supervisor.handle(
        {
            type: "invoke",
            request_id: "request",
            actor: { project_id: "local", actor_name: "Counter", actor_id: "counter-1" },
            method: "read",
            args: [],
            sqlite
        },
        () => {},
        () => commit(sqlite),
        async published => {
            effects.push(...published)
        },
        async () => []
    )
    assert.equal(reply.type, "invoked")
    if (reply.type === "invoked") assert.equal(reply.result, "counter-1:0")
    assert.equal(effects[0]?.type, "broadcast")
    console.log("invocation passed")
} finally {
    supervisor.close()
}
