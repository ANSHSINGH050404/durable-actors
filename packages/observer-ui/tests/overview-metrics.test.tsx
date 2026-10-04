import assert from "node:assert/strict"
import { test } from "node:test"

import type { RequestTrace } from "../src/client.js"
import { liveOverviewMetrics } from "../src/overview-metrics.js"
import { rangeLabel, rangePhrase, resolveRange, sameRange, toLocalInput } from "../src/time-range.js"

let counter = 0
function trace(overrides: Partial<RequestTrace>): RequestTrace {
    counter++
    return {
        sequence: counter,
        projectId: "default",
        requestId: `request-${counter}`,
        hostId: "host",
        sessionId: "session",
        actorName: "Room",
        actorId: "lobby",
        kind: "method",
        operation: "post",
        connectionId: null,
        startedAtMs: 5000,
        durationMs: 50,
        queueWaitMs: 10,
        hostState: "warm",
        outcome: "completed",
        ...overrides
    }
}
test("live overview metrics include p95 ranks, reroutes, and the deployment-wide row", () => {
    const records = [
        ...Array.from({ length: 21 }, (_, index) => trace({ durationMs: index + 1, queueWaitMs: (index + 1) * 2 })),
        trace({ outcome: "failed", durationMs: 100, queueWaitMs: 5 }),
        trace({ outcome: "rerouted", durationMs: 900, queueWaitMs: 900 }),
        trace({ outcome: "rejected", durationMs: 3, queueWaitMs: null }),
        trace({ actorName: "Counter", actorId: "one", durationMs: 7, queueWaitMs: 1 }),
        trace({ actorName: "Counter", actorId: "one", outcome: "rerouted", durationMs: 8, queueWaitMs: 1 }),
        trace({ actorName: "Idle", actorId: "one", outcome: "rerouted", durationMs: 1, queueWaitMs: 1 }),
        trace({ startedAtMs: 100, durationMs: 5000, queueWaitMs: 5000 })
    ]
    const saved = liveOverviewMetrics(records.filter(record => record.startedAtMs >= 1000))
    assert.equal(saved.total.count, 27)
    assert.equal(saved.total.success, (100 * 22) / 24)
    assert.equal(saved.classes.find(row => row.actorName === "Room")!.p95, 21, "p95 of 23 attempts is the 22nd smallest duration")
    assert.equal(saved.classes.find(row => row.actorName === "Room")!.queueP95, 40, "queue p95 ignores attempts that never began processing")
    assert.deepEqual(
        saved.classes.find(row => row.actorName === "Idle"),
        { actorName: "Idle", count: 1, success: null, p95: null, queueP95: null }
    )
})

test("time ranges resolve to minute-aligned bounds and describe themselves", () => {
    const now = new Date(2026, 8, 21, 15, 30, 45).getTime()
    assert.deepEqual(resolveRange({ kind: "relative", minutes: 60 }, now), { fromMs: new Date(2026, 8, 21, 14, 30).getTime() })
    assert.deepEqual(resolveRange({ kind: "all" }, now), {})
    assert.deepEqual(resolveRange({ kind: "absolute", fromMs: 1, toMs: 2 }, now), { fromMs: 1, toMs: 2 })
    assert.equal(rangeLabel({ kind: "relative", minutes: 60 }), "Last hour")
    assert.equal(rangePhrase({ kind: "relative", minutes: 15 }), "in the last 15 minutes")
    assert.equal(rangePhrase({ kind: "all" }), "in retained history")
    const from = new Date(2026, 8, 21, 9, 0).getTime()
    assert.match(rangeLabel({ kind: "absolute", fromMs: from, toMs: from + 90 * 60_000 }), /^Sep 21 09:00 – 10:30$/u)
    assert.match(rangeLabel({ kind: "absolute", fromMs: from, toMs: from + 25 * 3_600_000 }), /^Sep 21 09:00 – Sep 22 10:00$/u)
    assert.equal(toLocalInput(from), "2026-09-21T09:00")
    assert.ok(sameRange({ kind: "relative", minutes: 5 }, { kind: "relative", minutes: 5 }))
    assert.ok(!sameRange({ kind: "relative", minutes: 5 }, { kind: "relative", minutes: 15 }))
    assert.ok(!sameRange({ kind: "all" }, { kind: "relative", minutes: 15 }))
})
