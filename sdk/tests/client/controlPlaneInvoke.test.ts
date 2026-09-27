import assert from "node:assert/strict"
import { test } from "node:test"

import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError, ActorProtocolError } from "../../src/errors.js"

const options = {
    projectId: "project",
    apiKey: "backend-key",
    controlPlaneUrl: "https://control-plane.example.com"
}
const target = { route: "https://host.example.com", token: "ticket", ownerEpoch: 3, expiresAtMs: 60_000 }

test("a cold call resolves and invokes once, then warm calls go directly to the host", async () => {
    const requests: { url: string; method: string | undefined; body: unknown; authorization: string | null }[] = []
    let now = 0
    let requestId = 0
    const client = new RemoteActorClient(
        { ...options, homeRegion: "north-america-west" },
        {
            telemetry: () => {},
            now: () => now,
            requestId: () => `request-${++requestId}`,
            fetch: async (url, init) => {
                requests.push({
                    url: String(url),
                    method: init?.method,
                    body: JSON.parse(String(init?.body)),
                    authorization: new Headers(init?.headers).get("authorization")
                })
                const outcome = { type: "completed", result: 7 }
                return Response.json(
                    String(url).startsWith(target.route)
                        ? outcome
                        : { target: { ...target, expiresAtMs: now + 60_000 }, outcome }
                )
            }
        }
    )
    for (const amount of [2, 3]) assert.equal(await client.invoke("Counter", "one", "increment", [amount]), 7)
    now = 60_000
    assert.equal(await client.invoke("Counter", "one", "increment", [4]), 7)
    assert.deepEqual(requests, [
        {
            url: `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
            method: "POST",
            body: { requestId: "request-1", method: "increment", args: [2], homeRegion: "north-america-west" },
            authorization: "Bearer backend-key"
        },
        {
            url: `${target.route}/v1/projects/project/actors/Counter/one/invoke`,
            method: "POST",
            body: { requestId: "request-2", ownerEpoch: 3, method: "increment", args: [3] },
            authorization: "Bearer ticket"
        },
        {
            url: `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
            method: "POST",
            body: { requestId: "request-3", method: "increment", args: [4], homeRegion: "north-america-west" },
            authorization: "Bearer backend-key"
        }
    ])
})

test("host idle validity sends the first call after idle through the control plane", async () => {
    for (const delay of [4_999, 5_000, 31_918]) {
        const urls: string[] = []
        let now = 0
        const client = new RemoteActorClient(options, {
            now: () => now,
            telemetry: () => {},
            fetch: async url => {
                urls.push(String(url))
                const outcome = { type: "completed", result: urls.length }
                if (String(url).startsWith(target.route)) {
                    assert.ok(now < 10_000, "the idle host has already stopped")
                    return Response.json(outcome)
                }
                return Response.json({ target: { ...target, expiresAtMs: now + 10_000 }, outcome })
            }
        })
        assert.equal(await client.invoke("Counter", "one", "increment", [1]), 1)
        now = delay
        assert.equal(await client.invoke("Counter", "one", "increment", [1]), 2)
        assert.deepEqual(urls, [
            `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
            `${delay < 5_000 ? target.route : options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`
        ])
    }
})

test("combined invocation preserves placement and does not hide a structured not-found error", async () => {
    let calls = 0
    const client = new RemoteActorClient(
        { ...options, homeRegion: "north-america-west" },
        {
            telemetry: () => {},
            fetch: async (url, init) => {
                calls++
                assert.ok(String(url).endsWith("/invoke"))
                assert.equal(JSON.parse(String(init?.body)).homeRegion, "north-america-west")
                return Response.json(
                    { error: { code: "not_found", message: "actor contract not found" } },
                    { status: 404 }
                )
            }
        }
    )
    for (const actor of ["one", "two"]) {
        await assert.rejects(
            client.invoke("Counter", actor, "increment", []),
            (error: unknown) => error instanceof ActorInvocationError && error.code === "not_found"
        )
    }
    assert.equal(calls, 2)
})

test("invocation replies require an explicit valid outcome", async () => {
    for (const response of [
        ...[{}, { type: "completed" }, { type: "failed", code: 3 }, { type: "not_executed", reason: "unknown" }].map(
            outcome => Response.json({ target, outcome })
        ),
        Response.json({ target: { ...target, ownerEpoch: 0 }, outcome: { type: "completed", result: 7 } }),
        new Response("unavailable", { status: 502 })
    ]) {
        let calls = 0
        const client = new RemoteActorClient(options, {
            telemetry: () => {},
            fetch: async () => {
                calls++
                return response.clone()
            }
        })
        await assert.rejects(client.invoke("Counter", "one", "increment", []), ActorProtocolError)
        assert.equal(calls, 1)
    }
})

test(
    "an unfinished cold call does not block other calls or replace a newer cached target",
    { timeout: 2000 },
    async t => {
        let finish!: (response: Response) => void
        const held = new Promise<Response>(resolve => {
            finish = resolve
        })
        t.after(() => finish(Response.json({ target, outcome: { type: "completed", result: 1 } })))
        const urls: string[] = []
        const fresh = { ...target, route: "https://fresh.example.com", token: "fresh", ownerEpoch: 4 }
        const client = new RemoteActorClient(options, {
            now: () => 0,
            telemetry: () => {},
            fetch: async url => {
                urls.push(String(url))
                if (urls.length === 1) return held
                const outcome = { type: "completed", result: 2 }
                return Response.json(String(url).startsWith(fresh.route) ? outcome : { target: fresh, outcome })
            }
        })
        const first = client.invoke("Counter", "one", "hold", [])
        assert.equal(await client.invoke("Counter", "one", "release", []), 2)
        finish(Response.json({ target, outcome: { type: "completed", result: 1 } }))
        assert.equal(await first, 1)
        assert.equal(await client.invoke("Counter", "one", "read", []), 2)
        assert.deepEqual(urls, [
            `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
            `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
            `${fresh.route}/v1/projects/project/actors/Counter/one/invoke`
        ])
    }
)

test("cold and warm calls share one retry budget for explicit pre-execution rejections", async () => {
    for (const warm of [false, true]) {
        for (const recovered of [false, true]) {
            for (const rejection of [
                { type: "unauthenticated" },
                ...["stale_owner", "host_unavailable", "upstream_not_reached"].map(reason => ({
                    type: "not_executed",
                    reason
                }))
            ]) {
                const requests: { url: string; requestId: string }[] = []
                let priming = warm
                let nextRequestId = 0
                const fresh = { ...target, route: "https://fresh.example.com", token: "fresh", ownerEpoch: 4 }
                const client = new RemoteActorClient(options, {
                    now: () => 0,
                    telemetry: () => {},
                    requestId: () => `request-${++nextRequestId}`,
                    fetch: async (url, init) => {
                        if (priming) return Response.json({ target, outcome: { type: "completed", result: 0 } })
                        requests.push({ url: String(url), requestId: JSON.parse(String(init?.body)).requestId })
                        const outcome =
                            recovered && requests.length === 2 ? { type: "completed", result: 7 } : rejection
                        if (String(url).startsWith(target.route)) {
                            return outcome.type === "unauthenticated"
                                ? new Response(null, { status: 401 })
                                : Response.json(outcome)
                        }
                        return Response.json({ target: fresh, outcome })
                    }
                })
                if (priming) {
                    await client.invoke("Counter", "one", "read", [])
                    priming = false
                }
                const invocation = client.invoke("Counter", "one", "increment", [1])
                if (recovered) assert.equal(await invocation, 7)
                else
                    await assert.rejects(
                        invocation,
                        error =>
                            error instanceof ActorInvocationError &&
                            error.code === (rejection.type === "unauthenticated" ? "unauthenticated" : "unavailable")
                    )
                assert.deepEqual(requests, [
                    {
                        url: `${warm ? target.route : options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
                        requestId: warm ? "request-2" : "request-1"
                    },
                    {
                        url: `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
                        requestId: warm ? "request-2" : "request-1"
                    }
                ])
            }
        }
    }
})

test("invoke refreshes a stale broadcast target after the actor moves", async () => {
    const fresh = { ...target, route: "https://fresh.example.com", token: "fresh", ownerEpoch: 4 }
    const requests: string[] = []
    const published: unknown[] = []
    const client = new RemoteActorClient(options, {
        now: () => 0,
        telemetry: () => {},
        fetch: async url => {
            requests.push(String(url))
            return Response.json(
                String(url).endsWith("/find-actor")
                    ? target
                    : { target: fresh, outcome: { type: "completed", result: 7 } }
            )
        },
        actorHost: {
            async invoke(cached) {
                assert.deepEqual(cached, target)
                return { type: "not_executed", reason: "stale_owner" }
            },
            async publish(cached) {
                published.push(cached)
            }
        }
    })
    await client.broadcast("Counter", "one", "before")
    assert.equal(await client.invoke("Counter", "one", "increment", [1]), 7)
    await client.broadcast("Counter", "one", "after")
    assert.deepEqual(published, [target, fresh])
    assert.deepEqual(requests, [
        `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/find-actor`,
        `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`
    ])
})

test("an actor failure still supplies a target for the next warm call", async () => {
    const urls: string[] = []
    const client = new RemoteActorClient(options, {
        now: () => 0,
        telemetry: () => {},
        fetch: async url => {
            urls.push(String(url))
            return Response.json(
                urls.length === 1
                    ? { target, outcome: { type: "failed", code: "actor_error", message: "failed" } }
                    : { type: "completed", result: 7 }
            )
        }
    })
    await assert.rejects(
        client.invoke("Counter", "one", "fail", []),
        error => error instanceof ActorInvocationError && error.code === "actor_error"
    )
    assert.equal(await client.invoke("Counter", "one", "read", []), 7)
    assert.deepEqual(urls, [
        `${options.controlPlaneUrl}/v1/projects/project/actors/Counter/one/invoke`,
        `${target.route}/v1/projects/project/actors/Counter/one/invoke`
    ])
})

test("the next separate invocation resolves through the control plane after a direct transport failure", async () => {
    const fresh = { ...target, route: "https://fresh.example.com", token: "fresh", ownerEpoch: 4 }
    const requests: { url: string; requestId: string }[] = []
    let nextRequestId = 0
    const client = new RemoteActorClient(options, {
        now: () => 0,
        telemetry: () => {},
        requestId: () => `request-${++nextRequestId}`,
        fetch: async (url, init) => {
            requests.push({ url: String(url), requestId: JSON.parse(String(init?.body)).requestId })
            if (String(url).startsWith(target.route)) throw new TypeError("fetch failed")
            const outcome = { type: "completed", result: 7 }
            return Response.json(
                String(url).startsWith(fresh.route)
                    ? outcome
                    : { target: requests.length === 1 ? target : fresh, outcome }
            )
        }
    })
    assert.equal(await client.invoke("Counter", "one", "increment", [1]), 7)
    await assert.rejects(
        client.invoke("Counter", "one", "increment", [1]),
        error =>
            error instanceof ActorInvocationError && error.code === "outcome_unknown" && error.requestId === "request-2"
    )
    assert.equal(requests.length, 2)
    assert.equal(await client.invoke("Counter", "one", "increment", [1]), 7)
    assert.equal(await client.invoke("Counter", "one", "increment", [1]), 7)
    assert.deepEqual(
        requests,
        [options.controlPlaneUrl, target.route, options.controlPlaneUrl, fresh.route].map((origin, index) => ({
            url: `${origin}/v1/projects/project/actors/Counter/one/invoke`,
            requestId: `request-${index + 1}`
        }))
    )
})

test("a late failure from the old host cannot erase a refreshed target", { timeout: 2000 }, async t => {
    let fail!: (error: Error) => void
    const held = new Promise<Response>((_resolve, reject) => {
        fail = reject
    })
    t.after(() => fail(new Error("test finished")))
    const fresh = { ...target, route: "https://fresh.example.com", token: "fresh", ownerEpoch: 4 }
    const urls: string[] = []
    let cold = true
    const client = new RemoteActorClient(options, {
        now: () => 0,
        telemetry: () => {},
        fetch: async (url, init) => {
            urls.push(String(url))
            const body = JSON.parse(String(init?.body))
            if (String(url).startsWith(target.route)) {
                if (body.method === "hold") return held
                return Response.json({ type: "not_executed", reason: "stale_owner" })
            }
            const outcome = { type: "completed", result: 7 }
            return Response.json(
                String(url).startsWith(fresh.route) ? outcome : { target: cold ? target : fresh, outcome }
            )
        }
    })
    await client.invoke("Counter", "one", "read", [])
    cold = false
    const pending = assert.rejects(
        client.invoke("Counter", "one", "hold", []),
        error => error instanceof ActorInvocationError && error.code === "outcome_unknown"
    )
    assert.equal(await client.invoke("Counter", "one", "read", []), 7)
    fail(new Error("response lost"))
    await pending
    assert.equal(await client.invoke("Counter", "one", "read", []), 7)
    assert.deepEqual(
        urls,
        [options.controlPlaneUrl, target.route, target.route, options.controlPlaneUrl, fresh.route].map(
            origin => `${origin}/v1/projects/project/actors/Counter/one/invoke`
        )
    )
})
