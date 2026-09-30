import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorSessionRejectedError, ActorSessionTransport } from "../../src/client-runtime/session.js"

function fixture(lifetimeMs = 300_000) {
    let now = 1_000_000
    let exchanges = 0
    let transports = 0
    let calls = 0
    let failure: Error | undefined
    const scheduled: (() => void)[] = []
    const client = new ActorSessionTransport(
        {
            projectId: "project",
            getSession: async () => {
                exchanges++
                if (failure) throw failure
                return {
                    projectId: "project",
                    controlPlaneUrl: "https://actors.example",
                    token: `session-${exchanges}`,
                    expiresAtMs: now + lifetimeMs
                }
            }
        },
        {
            now: () => now,
            schedule: callback => {
                scheduled.push(callback)
                return () => {}
            },
            createTransport: options => {
                transports++
                assert.equal(options.apiKey, `session-${exchanges}`)
                return {
                    async invoke() {
                        calls++
                        return 42
                    }
                }
            }
        }
    )
    return {
        client,
        scheduled,
        stats: () => ({ exchanges, transports, calls }),
        advance: (ms: number) => {
            now += ms
        },
        fail: (error: Error) => {
            failure = error
        }
    }
}

test("concurrent and warm invocations share a session and transport", async () => {
    const f = fixture()
    await Promise.all(Array.from({ length: 5 }, () => f.client.invoke("Counter", "one", "read", [])))
    assert.deepEqual(f.stats(), { exchanges: 1, transports: 1, calls: 5 })
    await f.client.invoke("Counter", "two", "read", [])
    assert.deepEqual(f.stats(), { exchanges: 1, transports: 1, calls: 6 })
    f.client.dispose()
})

test("active sessions renew in the background and an explicit denial clears authorization", async () => {
    const f = fixture(60_000)
    await f.client.invoke("Counter", "one", "read", [])
    f.advance(46_000)
    f.scheduled[0]()
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(f.stats(), { exchanges: 2, transports: 2, calls: 1 })
    f.fail(new ActorSessionRejectedError("membership removed"))
    f.scheduled[1]()
    await new Promise(resolve => setImmediate(resolve))
    await assert.rejects(f.client.invoke("Counter", "one", "read", []), /membership removed/)
    assert.equal(f.stats().calls, 1)
    f.client.dispose()
})

test("transient renewal failure never extends authorization expiry", async () => {
    const f = fixture(60_000)
    await f.client.invoke("Counter", "one", "read", [])
    f.advance(46_000)
    f.fail(new Error("auth service unavailable"))
    f.scheduled[0]()
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(await f.client.invoke("Counter", "one", "read", []), 42)
    f.advance(15_000)
    await assert.rejects(f.client.invoke("Counter", "one", "read", []), /unavailable/)
    assert.equal(f.stats().calls, 2)
    f.client.dispose()
})

test("idle or disposed clients do not keep renewing", async () => {
    const f = fixture()
    await f.client.invoke("Counter", "one", "read", [])
    f.advance(61_000)
    f.scheduled[0]()
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(f.stats().exchanges, 1)
    f.client.dispose()
    await assert.rejects(f.client.invoke("Counter", "one", "read", []), /disposed/)
})

test("session responses cannot cross projects, use insecure remote origins, or extend the lifetime", async () => {
    for (const change of [
        { projectId: "other" },
        { controlPlaneUrl: "http://actors.example" },
        { token: "" },
        { expiresAtMs: Date.now() + 306_000 }
    ]) {
        const client = new ActorSessionTransport(
            {
                projectId: "project",
                getSession: async () => ({
                    projectId: "project",
                    controlPlaneUrl: "https://actors.example",
                    token: "session",
                    expiresAtMs: Date.now() + 60_000,
                    ...change
                })
            },
            { createTransport: () => assert.fail("Invalid sessions must not reach the runtime") }
        )
        await assert.rejects(client.invoke("Counter", "one", "read", []), /session/i)
        client.dispose()
    }
})

test("session transport never replays an invocation failure", async () => {
    let calls = 0
    const client = new ActorSessionTransport(
        {
            projectId: "project",
            getSession: async () => ({
                projectId: "project",
                controlPlaneUrl: "https://actors.example",
                token: "session",
                expiresAtMs: Date.now() + 60_000
            })
        },
        {
            createTransport: () => ({
                async invoke() {
                    calls++
                    throw new Error("outcome_unknown")
                }
            })
        }
    )
    await assert.rejects(client.invoke("Counter", "one", "increment", []), /outcome_unknown/)
    assert.equal(calls, 1)
    client.dispose()
})

test("a five-minute session survives the customer's 135-second idle return without authorization", async () => {
    const f = fixture()
    await f.client.invoke("Counter", "one", "read", [])
    f.advance(135_000)
    await f.client.invoke("Counter", "one", "read", [])
    assert.deepEqual(f.stats(), { exchanges: 1, transports: 1, calls: 2 })
    f.advance(161_000)
    await f.client.invoke("Counter", "one", "read", [])
    assert.deepEqual(f.stats(), { exchanges: 2, transports: 2, calls: 3 })
    f.client.dispose()
})
