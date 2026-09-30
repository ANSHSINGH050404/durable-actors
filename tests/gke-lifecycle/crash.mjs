import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { readFileSync, writeFileSync } from "node:fs"
import { setTimeout as delay } from "node:timers/promises"
import { promisify } from "node:util"

import { RemoteActorClient } from "../../sdk/dist/client/remoteClient.js"

assert.ok(process.env.TERSE_LIFECYCLE_DIRECTORY && process.env.TERSE_LIFECYCLE_SETTINGS)
const exec = promisify(execFile),
    root = process.env.TERSE_LIFECYCLE_DIRECTORY.replace(/\/$/, "") + "/"
const resources = JSON.parse(readFileSync(root + "resources.json"))
const settings = JSON.parse(readFileSync(process.env.TERSE_LIFECYCLE_SETTINGS))
const environment = { ...process.env, KUBECONFIG: settings.kubeconfig }
const run = (command, args) => exec(command, args, { env: environment })
const id = "crash-" + Date.now(),
    pending = []
let activation
function client() {
    return new RemoteActorClient(
        { controlPlaneUrl: settings.url, projectId: settings.project_id, apiKey: settings.api_key, homeRegion: "north-america-west" },
        {
            fetch: async (url, init) => {
                const response = await fetch(url, init)
                if (response.ok && String(url).endsWith("/invoke"))
                    pending.push(
                        response
                            .clone()
                            .json()
                            .then(body => {
                                if (body.target) activation = { host: JSON.parse(Buffer.from(body.target.token.split(".")[1], "base64url")).sub, epoch: body.target.ownerEpoch }
                            })
                    )
                return response
            }
        }
    )
}
const first = client()
for (let count = 1; count <= 5; count++) assert.equal(await first.invoke("Counter", id, "increment", []), count)
await Promise.all(pending.splice(0))
assert.match(activation.host, /^[a-zA-Z0-9._-]+$/)
const initial = activation
const keepalive = setInterval(() => first.invoke("Counter", id, "read", []).catch(() => {}), 500)
const { stdout } = await run("kubectl", [
    "-n",
    resources.namespace,
    "exec",
    "postgres",
    "--",
    "psql",
    "-U",
    "postgres",
    "-XAt",
    "-c",
    "SELECT name FROM durable_actors_spares WHERE host_id='" + activation.host + "'"
])
const pod = stdout.trim()
assert.match(pod, /^do-spare-[a-f0-9]+$/)
const { stdout: logs } = await run("kubectl", ["-n", resources.sandbox_namespace, "logs", pod])
writeFileSync(root + "crash-before-host.log", logs)
await run("kubectl", ["-n", resources.sandbox_namespace, "exec", pod, "--", "/usr/local/bin/python3", "-c", "import os; os.kill(1, 9)"]).catch(() => {})
clearInterval(keepalive)
const enc = value => Buffer.from(value).toString("base64url")
const actorPath =
    createHash("sha256")
        .update("object.v4." + settings.project_id + ":Counter:" + id)
        .digest("hex")
        .slice(0, 2) +
    "/" +
    enc(settings.project_id) +
    "/" +
    enc("Counter") +
    "/" +
    enc(id)
const ownerKey = "gs://" + resources.buckets.owner + "/durable-actors/v3/owners/" + actorPath + ".json"
let owner
for (let attempt = 0; attempt < 60; attempt++) {
    const { stdout } = await run("gcloud", ["storage", "cat", ownerKey, "--project=" + resources.project])
    owner = JSON.parse(stdout)
    assert.equal(owner.sealed, false, "host shutdown was graceful, so this is not a crash test")
    if (owner.lease.expires_at_ms <= Date.now()) break
    await delay(1000)
}
assert.ok(owner.lease.expires_at_ms <= Date.now(), "old lease did not expire")
const second = client(),
    start = performance.now()
assert.equal(await second.invoke("Counter", id, "increment", []), 6)
const recoveryMs = performance.now() - start
await Promise.all(pending.splice(0))
assert.notEqual(activation.host, initial.host)
assert.ok(activation.epoch > initial.epoch)
assert.equal(await second.invoke("Counter", id, "read", []), 6)
const result = { actor: id, killedPod: pod, initial, recovered: activation, acknowledgedBeforeCrash: 5, valueAfterRecovery: 6, recoveryMs, passed: true }
writeFileSync(root + "crash-result.json", JSON.stringify(result, null, 2))
console.log(JSON.stringify(result))
