import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpActorClient } from "../../src/client-runtime/client.js"
import { ActorInvocationError } from "../../src/client-runtime/errors.js"
import { ActorSessionTransport } from "../../src/client-runtime/session.js"

for (const reason of ["host_unavailable", "stale_owner", "upstream_not_reached"]) {
    test(`session invocations recover from ${reason} without renewing the session`, async t => {
        const f = fixture(async attempt =>
            attempt === 1
                ? Response.json({ type: "not_executed", reason })
                : Response.json({ type: "completed", result: 1 })
        )
        t.after(() => f.client.dispose())
        assert.equal(await f.client.invoke("Counter", "one", "increment", [1]), 1)
        assert.deepEqual(f.stats(), { exchanges: 1, discoveries: 2, attempts: 2 })
        assert.deepEqual(f.invocations[0], { ...f.invocations[1], ownerEpoch: 1 })
        assert.equal(f.invocations[1].ownerEpoch, 2)
    })
}

test("session transport cannot add another retry after host recovery is exhausted", async t => {
    const f = fixture(async attempt =>
        attempt === 1
            ? new Response(null, { status: 401 })
            : Response.json({ type: "not_executed", reason: "host_unavailable" })
    )
    t.after(() => f.client.dispose())
    await assert.rejects(f.client.invoke("Counter", "one", "increment", []), invocationError("unavailable"))
    assert.deepEqual(f.stats(), { exchanges: 1, discoveries: 2, attempts: 2 })
})

test("session invocations never replay permission denials or unknown outcomes", async () => {
    for (const code of ["forbidden", "outcome_unknown"]) {
        const f = fixture(async () => {
            if (code === "outcome_unknown") throw new Error("response lost")
            return Response.json({ type: "failed", code, message: "method is outside the grant" })
        })
        try {
            await assert.rejects(f.client.invoke("Counter", "one", "increment", []), invocationError(code))
            assert.deepEqual(f.stats(), { exchanges: 1, discoveries: 1, attempts: 1 })
        } finally {
            f.client.dispose()
        }
    }
})

test("session renewal replaces actor tickets before the next invocation", async t => {
    const f = fixture(async () => Response.json({ type: "completed", result: 1 }))
    t.after(() => f.client.dispose())
    await f.client.invoke("Counter", "one", "increment", [])
    f.expireSession()
    await f.client.invoke("Counter", "one", "increment", [])
    assert.deepEqual(f.stats(), { exchanges: 2, discoveries: 2, attempts: 2 })
    assert.equal(f.invocations[1].ownerEpoch, 2)
})

function fixture(invoke: (attempt: number) => Promise<Response>) {
    let now = 0
    let exchanges = 0
    let discoveries = 0
    let attempts = 0
    const invocations: Record<string, unknown>[] = []
    const client = new ActorSessionTransport(
        {
            projectId: "project",
            getSession: async () => ({
                projectId: "project",
                controlPlaneUrl: "https://actors.example",
                token: `session-${++exchanges}`,
                expiresAtMs: now + 60_000
            })
        },
        {
            now: () => now,
            schedule: () => () => {},
            createTransport: options =>
                new HttpActorClient(options, {
                    now: () => now,
                    fetch: async (url, init) => {
                        assert.equal(init?.method, "POST")
                        const authorization = new Headers(init.headers).get("authorization")
                        if (String(url).endsWith("/find-actor")) {
                            assert.equal(authorization, `Bearer session-${exchanges}`)
                            return Response.json({
                                route: `https://host-${++discoveries}.example`,
                                token: `ticket-${discoveries}`,
                                ownerEpoch: discoveries,
                                expiresAtMs: now + 60_000
                            })
                        }
                        assert.equal(authorization, `Bearer ticket-${discoveries}`)
                        invocations.push(JSON.parse(String(init.body)))
                        return invoke(++attempts)
                    }
                })
        }
    )
    return {
        client,
        invocations,
        stats: () => ({ exchanges, discoveries, attempts }),
        expireSession: () => {
            now += 60_000
        }
    }
}

function invocationError(code: string): (error: unknown) => boolean {
    return error => error instanceof ActorInvocationError && error.code === code
}
