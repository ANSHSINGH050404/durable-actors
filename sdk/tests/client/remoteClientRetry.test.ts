import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { setTimeout } from "node:timers/promises"

import type { DirectActorInvocation } from "../../src/client-runtime/http.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError } from "../../src/errors.js"

for (const method of ["read", "increment"]) {
    test(`a cached connection refusal rediscovers and retries ${method} once`, async () => {
        let count = 0
        let retired = false
        const { client, discoveries, requests } = retryClient(async request => {
            if (retired && request.url.includes("host-1")) throw refused()
            if (request.body.method === "increment") count += Number(request.body.args[0])
            return completed(count)
        })
        assert.equal(await client.invoke("Counter", "one", "increment", [1]), 1)
        retired = true
        assert.equal(await client.invoke("Counter", "one", method, [1]), method === "read" ? 1 : 2)
        assert.equal(discoveries.length, 2)
        assert.equal(requests.length, 3)
        assert.deepEqual(requests[1].body, { ...requests[2].body, ownerEpoch: 1 })
        assert.equal(requests[2].body.ownerEpoch, 2)
        assert.equal(requests[2].authorization, "Bearer ticket-2")
        assert.equal(count, method === "read" ? 1 : 2)
    })
}

test("all pre-execution rejections share one recovery attempt", async () => {
    const rejections = [
        async () => {
            throw refused()
        },
        async () => new Response(null, { status: 401 }),
        ...["host_unavailable", "stale_owner", "upstream_not_reached"].map(
            reason => async () => Response.json({ type: "not_executed", reason })
        )
    ]
    for (const first of rejections) {
        for (const [index, second] of rejections.entries()) {
            let calls = 0
            const { client, discoveries, requests } = retryClient(() => (++calls === 1 ? first() : second()))
            await assert.rejects(
                client.invoke("Counter", "one", "increment", [1]),
                invocationError(index === 1 ? "unauthenticated" : "unavailable")
            )
            assert.equal(discoveries.length, 2)
            assert.equal(requests.length, 2)
            assert.equal(requests[0].body.requestId, requests[1].body.requestId)
        }
    }
})

for (const reason of ["host_unavailable", "stale_owner", "upstream_not_reached"]) {
    test(`a ${reason} rejection rediscovers and executes the mutation once`, async () => {
        let count = 0
        const { client, discoveries, requests } = retryClient(async request => {
            if (request.body.ownerEpoch === 1) return Response.json({ type: "not_executed", reason })
            return completed(++count)
        })
        assert.equal(await client.invoke("Counter", "one", "increment", [1]), 1)
        assert.equal(count, 1)
        assert.equal(discoveries.length, 2)
        assert.equal(requests.length, 2)
        assert.deepEqual(requests[0].body, { ...requests[1].body, ownerEpoch: 1 })
        assert.equal(requests[1].authorization, "Bearer ticket-2")
    })
}

test("gateway errors and actor failures never authorize replay", async () => {
    for (const response of [
        ...[502, 503, 504].map(status => () => new Response(null, { status })),
        ...["unavailable", "not_executed", "host_unavailable"].map(
            code => () => Response.json({ type: "failed", code, message: "actor method failed" })
        )
    ]) {
        let executions = 0
        const { client, discoveries, requests } = retryClient(async () => {
            executions++
            return response()
        })
        await assert.rejects(client.invoke("Counter", "one", "increment", [1]), ActorInvocationError)
        assert.equal(executions, 1)
        assert.equal(discoveries.length, 1)
        assert.equal(requests.length, 1)
    }
})

test("ambiguous failure after rediscovery is not retried", async () => {
    let calls = 0
    const { client, discoveries } = retryClient(async () => {
        if (++calls === 1) throw refused()
        throw new Error("response lost")
    })
    await assert.rejects(client.invoke("Counter", "one", "increment", [1]), invocationError("outcome_unknown"))
    assert.equal(calls, 2)
    assert.equal(discoveries.length, 2)
})

test("failed rediscovery remains unavailable when the host never received the invocation", async () => {
    let discoveries = 0
    let calls = 0
    const client = new RemoteActorClient(undefined, {
        environment: {},
        telemetry: () => {},
        fetch: async (url, init) => {
            if (String(url).endsWith("/find-actor")) {
                if (++discoveries === 2) throw new Error("discovery disconnected")
                return target(1)
            }
            calls++
            throw refused()
        }
    })
    await assert.rejects(client.invoke("Counter", "one", "increment", [1]), invocationError("unavailable"))
    assert.equal(calls, 1)
    assert.equal(discoveries, 2)
})

test("concurrent stale failures share rediscovery and cannot erase a refreshed target", async () => {
    const failures = Array.from({ length: 3 }, () => deferred<Response>())
    const started = deferred<void>()
    let staleCalls = 0
    const { client, discoveries } = retryClient(async request => {
        if (!request.url.includes("host-1") || request.body.method === "read") return completed(1)
        const failure = failures[staleCalls++]
        if (staleCalls === failures.length) started.resolve()
        return failure.promise
    })
    await client.invoke("Counter", "one", "read", [])
    const calls = failures.map(() => client.invoke("Counter", "one", "increment", [1]))
    await started.promise
    failures[0].reject(refused())
    failures[1].reject(refused())
    await Promise.all(calls.slice(0, 2))
    failures[2].reject(refused())
    await calls[2]
    assert.equal(await client.invoke("Counter", "one", "read", []), 1)
    assert.equal(discoveries.length, 2)
})

test("concurrent callers share refresh of an expired target", async () => {
    let now = 0
    const { client, discoveries } = retryClient(
        async () => completed(1),
        () => now
    )
    await client.invoke("Counter", "one", "read", [])
    now = 60_000
    await Promise.all(Array.from({ length: 3 }, () => client.invoke("Counter", "one", "read", [])))
    assert.equal(discoveries.length, 2)
})

test("an ambiguous stale failure does not wait for another caller's rediscovery", async () => {
    const rediscovery = deferred<Response>()
    const resolving = deferred<void>()
    const ambiguous = deferred<Response>()
    const dispatched = deferred<void>()
    let discoveries = 0
    let calls = 0
    const client = new RemoteActorClient(undefined, {
        environment: {},
        telemetry: () => {},
        now: () => 0,
        fetch: async (url, init) => {
            if (String(url).endsWith("/find-actor")) {
                if (++discoveries === 1) return target(1)
                resolving.resolve()
                return rediscovery.promise
            }
            if (++calls === 1) return completed(1)
            if (calls === 2) {
                dispatched.resolve()
                return ambiguous.promise
            }
            if (calls === 3) throw refused()
            return completed(1)
        }
    })
    await client.invoke("Counter", "one", "read", [])
    const unknown = assert.rejects(
        client.invoke("Counter", "one", "increment", [1]),
        invocationError("outcome_unknown")
    )
    await dispatched.promise
    const retry = client.invoke("Counter", "one", "read", [])
    await resolving.promise
    ambiguous.reject(new Error("response lost"))
    const deadline = new AbortController()
    try {
        assert.equal(
            await Promise.race([
                unknown.then(() => "rejected"),
                setTimeout(1_000, "waiting", { signal: deadline.signal })
            ]),
            "rejected"
        )
    } finally {
        deadline.abort()
        rediscovery.resolve(target(2))
        await Promise.all([unknown, retry])
    }
})

test("a mutation with a lost HTTP response is executed once and reports outcome_unknown", async t => {
    let executions = 0
    const host = createServer(async (request, response) => {
        request.resume()
        await once(request, "end")
        executions++
        request.socket.destroy()
    })
    t.after(() => host.close())
    host.listen(0, "127.0.0.1")
    await once(host, "listening")
    const address = host.address()
    assert.ok(address && typeof address !== "string")
    const client = new RemoteActorClient(undefined, {
        environment: {},
        telemetry: () => {},
        fetch: async (url, init) =>
            String(url).endsWith("/find-actor")
                ? Response.json({
                      route: `http://127.0.0.1:${address.port}`,
                      token: "ticket",
                      ownerEpoch: 1,
                      expiresAtMs: Date.now() + 60_000
                  })
                : fetch(url, init)
    })
    await assert.rejects(client.invoke("Counter", "one", "increment", [1]), invocationError("outcome_unknown"))
    assert.equal(executions, 1)
})

function retryClient(invoke: (request: HostRequest) => Promise<Response>, now = () => 0) {
    const discoveries: string[] = []
    const requests: HostRequest[] = []
    let requestId = 0
    const client = new RemoteActorClient(undefined, {
        environment: {},
        telemetry: () => {},
        now,
        requestId: () => `request-${++requestId}`,
        fetch: async (url, init) => {
            if (String(url).endsWith("/find-actor")) {
                discoveries.push(String(url))
                return target(discoveries.length, now())
            }
            const request = {
                url: String(url),
                body: JSON.parse(String(init?.body)),
                authorization: new Headers(init?.headers).get("authorization")
            }
            requests.push(request)
            return invoke(request)
        }
    })
    return { client, discoveries, requests }
}

function target(generation: number, now = 0): Response {
    return Response.json({
        route: `http://host-${generation}.example`,
        token: `ticket-${generation}`,
        ownerEpoch: generation,
        expiresAtMs: now + 60_000
    })
}

function completed(result: number): Response {
    return Response.json({ type: "completed", result })
}

function refused(): Error {
    return new TypeError("fetch failed", {
        cause: Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    })
}

function invocationError(code: string): (error: unknown) => boolean {
    return error => error instanceof ActorInvocationError && error.code === code
}

function deferred<T>() {
    let resolve!: (value: T) => void
    let reject!: (reason: unknown) => void
    const promise = new Promise<T>((yes, no) => {
        resolve = yes
        reject = no
    })
    return { promise, resolve, reject }
}

interface HostRequest {
    readonly url: string
    readonly body: Pick<DirectActorInvocation, "requestId" | "method" | "args"> & { ownerEpoch: number }
    readonly authorization: string | null
}
