import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpActorHostTransport } from "../../src/client-runtime/http.js"
import { ActorProtocolError } from "../../src/errors.js"

const target = { route: "https://host.example", token: "ticket", ownerEpoch: 3, expiresAtMs: 4000000000000 }
const invocation = {
    projectId: "team",
    actorName: "Counter",
    actorId: "one",
    requestId: "request-1",
    method: "increment",
    args: [2]
}

test("HTTP invocation sends its ticket, epoch, method and arguments in one POST", async () => {
    const methods: string[] = []
    const transport = new HttpActorHostTransport(async (url, init) => {
        assert.equal(url, "https://host.example/v1/projects/team/actors/Counter/one/invoke")
        assert.ok(init?.method)
        methods.push(init.method)
        assert.equal(init?.method, "POST")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer ticket")
        assert.equal(init?.redirect, "manual")
        assert.deepEqual(JSON.parse(String(init?.body)), {
            requestId: "request-1",
            ownerEpoch: 3,
            method: "increment",
            args: [2]
        })
        return Response.json({ type: "completed", result: 7 })
    })
    assert.deepEqual(await transport.invoke(target, invocation), { type: "completed", result: 7 })
    assert.deepEqual(methods, ["POST"])
})

test("HTTP replies preserve host latency metadata for every outcome", async () => {
    for (const outcome of [
        { type: "completed", result: 7 },
        { type: "failed", code: "actor_error", message: "failed" },
        { type: "not_executed", reason: "host_unavailable" }
    ]) {
        const reply = {
            ...outcome,
            metadata: {
                durationMs: 12.5,
                queueWaitMs: outcome.type === "not_executed" ? null : 2.5,
                hostState: outcome.type === "completed" ? "cold" : "warm"
            }
        }
        const transport = new HttpActorHostTransport(async () => Response.json(reply))
        assert.deepEqual(await transport.invoke(target, invocation), reply)
    }
})

test("HTTP replies reject invalid host latency metadata", async () => {
    for (const metadata of [
        null,
        {},
        { durationMs: -1, queueWaitMs: null, hostState: "warm" },
        { durationMs: "12", queueWaitMs: null, hostState: "warm" },
        { durationMs: 12, hostState: "warm" },
        { durationMs: 12, queueWaitMs: -1, hostState: "warm" },
        { durationMs: 12, queueWaitMs: 13, hostState: "warm" },
        { durationMs: 12, queueWaitMs: "2", hostState: "warm" },
        { durationMs: 12, queueWaitMs: 2, hostState: "unknown" },
        { durationMs: 12, queueWaitMs: 2, hostState: null },
        { durationMs: 12, queueWaitMs: 2, hostState: true },
        { durationMs: 12, queueWaitMs: 2 }
    ]) {
        const transport = new HttpActorHostTransport(async () =>
            Response.json({ type: "completed", result: 7, metadata })
        )
        await assert.rejects(transport.invoke(target, invocation), ActorProtocolError)
    }
})

test("only a pre-dispatch HTTP 401 is an authentication refresh signal", async () => {
    for (const status of [401, 403, 500, 503, 504]) {
        const transport = new HttpActorHostTransport(async () => new Response("rejected", { status }))
        if (status === 401) assert.deepEqual(await transport.invoke(target, invocation), { type: "unauthenticated" })
        else await assert.rejects(transport.invoke(target, invocation))
    }
    const failure = { type: "failed", code: "unauthenticated", message: "method failed" }
    const transport = new HttpActorHostTransport(async () => Response.json(failure))
    assert.deepEqual(await transport.invoke(target, invocation), failure)
})

test("HTTP replies require an explicit valid outcome", async () => {
    for (const document of [
        {},
        { type: "completed" },
        { type: "failed", code: 3 },
        { type: "unauthenticated" },
        { type: "not_executed" },
        { type: "not_executed", reason: "" },
        { type: "not_executed", reason: "unknown" },
        { type: "not_executed", reason: 1 }
    ]) {
        const transport = new HttpActorHostTransport(async () => Response.json(document))
        await assert.rejects(transport.invoke(target, invocation), ActorProtocolError)
    }
})

test("connection refusals identify requests that were never dispatched", async () => {
    const refused = () => Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    for (const error of [
        refused(),
        new TypeError("fetch failed", { cause: refused() }),
        new TypeError("fetch failed", { cause: new AggregateError([refused(), refused()]) })
    ]) {
        const transport = new HttpActorHostTransport(async () => {
            throw error
        })
        assert.deepEqual(await transport.invoke(target, invocation), {
            type: "not_executed",
            reason: "upstream_not_reached"
        })
    }
})

test("unproven transport failures preserve ambiguity", async () => {
    const refused = Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    const cyclic = new Error("cycle")
    cyclic.cause = cyclic
    for (const error of [
        new TypeError("fetch failed"),
        new Error("connect ECONNREFUSED"),
        Object.assign(new Error("refused"), { code: "ECONNREFUSED" }),
        Object.assign(new Error("reset"), { code: "ECONNRESET", syscall: "read", cause: refused }),
        Object.assign(new Error("timeout"), { code: "ETIMEDOUT" }),
        new AggregateError([refused, new Error("connection lost")]),
        Object.assign(new AggregateError([refused, cyclic]), { code: "ECONNREFUSED" }),
        new AggregateError([]),
        cyclic
    ]) {
        const transport = new HttpActorHostTransport(async () => {
            throw error
        })
        await assert.rejects(transport.invoke(target, invocation), failure => failure === error)
    }
})

test("socket broadcasts use actor-scoped HTTP with the same ownership ticket", async () => {
    const effects = [
        { type: "broadcast", message: { type: "text", data: "hello" }, except_connection_ids: [], tags: [] }
    ]
    const transport = new HttpActorHostTransport(async (url, init) => {
        assert.equal(url, "https://host.example/v1/projects/team/actors/Counter/one/socket-effects")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer ticket")
        assert.deepEqual(JSON.parse(String(init?.body)), { ownerEpoch: 3, effects })
        return new Response(null, { status: 204 })
    })
    await transport.publish(target, invocation, effects)
})

test("explicit pre-execution rejections preserve their reason", async () => {
    for (const reason of ["host_unavailable", "stale_owner", "upstream_not_reached"]) {
        const reply = { type: "not_executed", reason }
        const transport = new HttpActorHostTransport(async () => Response.json(reply))
        assert.deepEqual(await transport.invoke(target, invocation), reply)
    }
})
