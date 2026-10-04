import assert from "node:assert/strict"
import { test } from "node:test"

import type { RequestTrace } from "../src/client.js"
import { requestSummary } from "../src/overview-data.js"

const trace = (durationMs: number, outcome: RequestTrace["outcome"]): RequestTrace => ({
    sequence: 1,
    projectId: "default",
    requestId: "request",
    hostId: "host",
    sessionId: "session",
    actorName: "Room",
    actorId: "one",
    kind: "method",
    operation: "post",
    connectionId: null,
    startedAtMs: 1000,
    durationMs,
    queueWaitMs: 0,
    hostState: "warm",
    outcome
})

test("overview metrics use retained execution attempts, without counting reroutes as successful executions", () => {
    assert.deepEqual(requestSummary([]), { count: 0, success: null, p95: null })
    assert.deepEqual(requestSummary([trace(10, "completed"), trace(30, "failed"), trace(100, "rerouted")]), { count: 3, success: 50, p95: 30 })
})
