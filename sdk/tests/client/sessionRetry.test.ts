import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpActorClient } from "../../src/client-runtime/client.js"
import { ActorInvocationError } from "../../src/client-runtime/errors.js"
import { ActorSessionTransport } from "../../src/client-runtime/session.js"

test("session transport cannot add another retry after host recovery is exhausted", async t => {
    const f = fixture(async () => Response.json({ type: "not_executed", reason: "host_unavailable" }))
    t.after(() => f.client.dispose())
    await assert.rejects(f.client.invoke("Counter", "one", "increment", []), invocationError("unavailable"))
    assert.deepEqual(f.stats(), { exchanges: 1, attempts: 2 })
})

test("session invocations never replay permission denials or unknown outcomes", async () => {
    for (const code of ["forbidden", "outcome_unknown"]) {
        const f = fixture(async () => {
            if (code === "outcome_unknown") throw new Error("response lost")
            return Response.json({ type: "failed", code, message: "method is outside the grant" })
        })
        try {
            await assert.rejects(f.client.invoke("Counter", "one", "increment", []), invocationError(code))
            assert.deepEqual(f.stats(), { exchanges: 1, attempts: 1 })
        } finally {
            f.client.dispose()
        }
    }
})

test("session renewal updates the credential on the next invocation", async t => {
    const f = fixture(async () => Response.json({ type: "completed", result: 1 }))
    t.after(() => f.client.dispose())
    await f.client.invoke("Counter", "one", "increment", [])
    f.expireSession()
    await f.client.invoke("Counter", "one", "increment", [])
    assert.deepEqual(f.stats(), { exchanges: 2, attempts: 2 })
    assert.deepEqual(f.authorizations, ["Bearer session-1", "Bearer session-2"])
})

function fixture(invoke: (attempt: number) => Promise<Response>) {
    let now = 0
    let exchanges = 0
    let attempts = 0
    const authorizations: (string | null)[] = []
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
                        assert.equal(
                            String(url),
                            "https://actors.example/v1/projects/project/actors/Counter/one/invoke"
                        )
                        assert.equal(authorization, `Bearer session-${exchanges}`)
                        authorizations.push(authorization)
                        const response = await invoke(++attempts)
                        if (!response.ok) return response
                        return Response.json({
                            target: {
                                route: "https://host.example",
                                token: "ticket",
                                ownerEpoch: 1,
                                expiresAtMs: now + 60_000
                            },
                            outcome: await response.json()
                        })
                    }
                })
        }
    )
    return {
        client,
        authorizations,
        stats: () => ({ exchanges, attempts }),
        expireSession: () => {
            now += 60_000
        }
    }
}

function invocationError(code: string): (error: unknown) => boolean {
    return error => error instanceof ActorInvocationError && error.code === code
}
