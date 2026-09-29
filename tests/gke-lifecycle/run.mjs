import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { setTimeout as delay } from "node:timers/promises"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { RemoteActorClient } from "../../sdk/dist/client/remoteClient.js"

const source = fileURLToPath(new URL(".", import.meta.url))
assert.ok(process.env.TERSE_LIFECYCLE_DIRECTORY, "TERSE_LIFECYCLE_DIRECTORY is required")
assert.ok(process.env.TERSE_LIFECYCLE_SETTINGS, "TERSE_LIFECYCLE_SETTINGS is required")
const root = process.env.TERSE_LIFECYCLE_DIRECTORY.replace(/\/$/, "") + "/"
const exec = promisify(execFile)
const settings = JSON.parse(readFileSync(process.env.TERSE_LIFECYCLE_SETTINGS, "utf8"))
const count = Number(process.argv[2] ?? 20),
    warmups = Number(process.argv[3] ?? 2)
const prefix = `live-${Date.now()}`
const output = root + prefix + "/"
mkdirSync(output)
const samples = [],
    actors = []
for (let index = -warmups; index < count; index++) {
    for (const method of ["increment", "read"]) {
        const actor = makeActor(`${prefix}-${method}-${index + warmups}`, index < 0)
        actor.readyBefore = await ops("ready")
        actor.value = method === "increment" ? 1 : 0
        await call(actor, method === "increment" ? "cold_write" : "cold_read", method, actor.value)
        actor.initial = actor.activation
        await verifyPrewarmed(actor)
        for (let i = 0; i < 5; i++) {
            await call(actor, "hot_write", "increment", ++actor.value)
            await call(actor, "hot_read", "read", actor.value)
        }
        actor.resumeMethod = method
        actors.push(actor)
    }
}
await delay(13000)
for (const actor of actors) {
    actor.release = await ops("released", actor.id)
    assert.equal(actor.release.epoch, actor.initial.epoch)
    assert.equal(actor.release.host, actor.initial.host)
}
writeFileSync(output + "prefixes.json", JSON.stringify(actors.map(a => a.release.prefix)))
const archive = await ops("archive", output + "prefixes.json")
writeFileSync(output + "archive-evidence.json", JSON.stringify(archive, null, 2))
for (const actor of actors) {
    actor.readyBefore = await ops("ready")
    if (actor.resumeMethod === "increment") actor.value++
    await call(actor, actor.resumeMethod === "increment" ? "resume_write" : "resume_read", actor.resumeMethod, actor.value)
    assert.notEqual(actor.activation.host, actor.initial.host, "resume reused old host")
    assert.ok(actor.activation.epoch > actor.initial.epoch, "resume did not advance epoch")
    await verifyPrewarmed(actor)
}
writeFileSync(output + "summary.json", JSON.stringify(summary(), null, 2))
console.log(JSON.stringify({ event: "complete", output, summary: summary() }))

function makeActor(id, warmup) {
    const actor = { id, warmup, pending: [], activation: null }
    actor.client = new RemoteActorClient(
        { controlPlaneUrl: settings.url, projectId: settings.project_id, apiKey: settings.api_key, homeRegion: "north-america-west" },
        {
            telemetry: event => {
                actor.telemetry = event
            },
            fetch: async (url, init) => {
                const response = await fetch(url, { ...init, signal: AbortSignal.timeout(120000) })
                if (response.ok && String(url).endsWith("/invoke"))
                    actor.pending.push(
                        response
                            .clone()
                            .json()
                            .then(body => {
                                if (!body.target) return
                                const claims = JSON.parse(Buffer.from(body.target.token.split(".")[1], "base64url").toString())
                                actor.activation = { host: claims.sub, epoch: body.target.ownerEpoch }
                            })
                    )
                return response
            }
        }
    )
    return actor
}
async function call(actor, category, method, expected) {
    const row = { actor: actor.id, category, method, expected, warmup: actor.warmup, startedAt: Date.now() }
    const start = performance.now()
    try {
        const result = await actor.client.invoke("Counter", actor.id, method, [])
        row.ms = performance.now() - start
        row.result = result
        row.completedAt = Date.now()
        assert.equal(result, expected, "persisted state mismatch")
        await Promise.all(actor.pending.splice(0))
        Object.assign(row, actor.activation, { telemetry: actor.telemetry })
    } catch (error) {
        row.error = String(error)
        save(row)
        throw error
    }
    save(row)
    console.log(JSON.stringify({ category, warmup: actor.warmup, ms: row.ms, host: row.host, epoch: row.epoch }))
}
function save(row) {
    samples.push(row)
    appendFileSync(output + "samples.jsonl", JSON.stringify(row) + "\n")
}
async function verifyPrewarmed(actor) {
    const evidence = await ops("evidence", actor.activation.host)
    const before = actor.readyBefore.find(p => p.uid === evidence.pod.uid)
    assert.ok(before, "activation did not use a previously ready pod")
    assert.equal(evidence.pod.runtimeClass, "gvisor")
    const startup = evidence.startup.find(row => row.outcome === "ready")
    assert.ok(startup, "missing successful Rust host startup evidence")
    appendFileSync(output + "activation-evidence.jsonl", JSON.stringify({ actor: actor.id, activation: actor.activation, readyBefore: before, release: actor.release, evidence }) + "\n")
}
async function ops(action, ...args) {
    const { stdout } = await exec("python3", [source + "ops.py", action, ...args], { timeout: 240000, maxBuffer: 16 * 1024 * 1024 })
    return JSON.parse(stdout)
}
function summary() {
    return Object.fromEntries(
        ["cold_write", "cold_read", "hot_write", "hot_read", "resume_write", "resume_read"].map(category => {
            const values = samples
                    .filter(s => s.category === category && !s.warmup && !s.error)
                    .map(s => s.ms)
                    .sort((a, b) => a - b),
                n = values.length
            return [category, { n, p50: (values[Math.floor((n - 1) / 2)] + values[Math.floor(n / 2)]) / 2, p95: values[Math.ceil(n * 0.95) - 1], min: values[0], max: values[n - 1] }]
        })
    )
}
