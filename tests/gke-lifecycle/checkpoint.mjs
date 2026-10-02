import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { readFileSync, writeFileSync } from "node:fs"
import { setTimeout as delay } from "node:timers/promises"
import { promisify } from "node:util"

import { RemoteActorClient } from "../../sdk/dist/client/remoteClient.js"

assert.ok(process.env.TERSE_LIFECYCLE_DIRECTORY && process.env.TERSE_LIFECYCLE_SETTINGS)
const root = process.env.TERSE_LIFECYCLE_DIRECTORY.replace(/\/$/, "") + "/"
const resources = JSON.parse(readFileSync(root + "resources.json"))
const settings = JSON.parse(readFileSync(process.env.TERSE_LIFECYCLE_SETTINGS))
const client = new RemoteActorClient({ controlPlaneUrl: settings.url, projectId: settings.project_id, apiKey: settings.api_key, homeRegion: "north-america-west" })
const actor = "checkpoint-" + Date.now()
console.log(JSON.stringify({ event: "start", actor }))
for (let value = 1; value <= 131; value++) {
    assert.equal(await client.invoke("Counter", actor, "increment", []), value)
    await delay(1000)
}
const encode = value => Buffer.from(value).toString("base64url")
const shard = createHash("sha256").update(`object.v4.${settings.project_id}:Counter:${actor}`).digest("hex").slice(0, 2)
const actorPath = [shard, settings.project_id, "Counter", actor].map((value, index) => (index ? encode(value) : value)).join("/")
const physical = "durable-actors-v3-snapshots-" + createHash("sha256").update(actorPath).digest("base64url") + "~"
const { stdout } = await promisify(execFile)("gcloud", ["storage", "ls", `gs://${resources.buckets.archive}/${physical}*`, `--project=${resources.project}`])
const objects = stdout.trim().split("\n")
assert.ok(
    objects.some(object => object.endsWith(".batch")),
    "background archiving did not publish a batch"
)
assert.ok(objects.filter(object => object.endsWith(".manifest")).length >= 2, "writes did not open a new segment after checkpoint")
assert.ok(
    objects.some(object => object.endsWith(".checkpoint")),
    "background compaction did not publish a checkpoint"
)
assert.equal(await client.invoke("Counter", actor, "read", []), 131)
await delay(45000)
const resumeStarted = performance.now()
assert.equal(await client.invoke("Counter", actor, "increment", []), 132)
const resumeMs = performance.now() - resumeStarted
const result = { actor, writesBeforeResume: 131, valueAfterResume: 132, resumeMs, objects, passed: true }
writeFileSync(root + "checkpoint-result.json", JSON.stringify(result, null, 2))
console.log(JSON.stringify(result))
