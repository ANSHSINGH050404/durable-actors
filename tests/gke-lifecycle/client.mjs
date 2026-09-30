import assert from "node:assert/strict"
import { setTimeout as delay } from "node:timers/promises"
import { pathToFileURL } from "node:url"

const { RemoteActorClient } = await import(pathToFileURL(process.env.BENCH_SDK_CLIENT).href)
const base = process.env.BENCH_INTERNAL_URL
const projectId = process.env.BENCH_PROJECT_ID
assert.ok(base && projectId && process.env.BENCH_API_KEY)
const actor = "regional-" + Date.now()
const client = new RemoteActorClient(
    { controlPlaneUrl: base, projectId, apiKey: process.env.BENCH_API_KEY, homeRegion: "north-america-west" },
    {
        fetch: (url, options) => {
            const original = new URL(url)
            return fetch(new URL(original.pathname + original.search, base), options)
        }
    }
)
let value = 0
console.log(JSON.stringify({ event: "start", actor, mode: process.env.BENCH_MODE }))
await measure("cold_write", "increment", ++value, false)
for (let sample = 0; sample < 105; sample++) {
    await measure("hot_write", "increment", ++value, sample < 5)
    await measure("hot_read", "read", value, sample < 5)
}
await delay(15000)
await measure("resume_write", "increment", ++value, false)
assert.equal(await client.invoke("Counter", actor, "read", []), value)
console.log(JSON.stringify({ event: "complete", actor, mode: process.env.BENCH_MODE, value, passed: true }))

async function measure(category, method, expected, warmup) {
    const started = performance.now()
    const result = await client.invoke("Counter", actor, method, [])
    const ms = performance.now() - started
    assert.equal(result, expected)
    console.log(JSON.stringify({ actor, mode: process.env.BENCH_MODE, category, warmup, ms, result }))
}
