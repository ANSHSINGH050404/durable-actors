import assert from "node:assert/strict"
import { test } from "node:test"

import type { ActorInventory } from "../src/client.js"
import { formatDuration, metadataSummary, sessionDuration, sessionRows, sessionSummary, socketSessions } from "../src/socket-sessions.js"

test("live inventory decides whether unfinished sessions are open or lost and adds unrecorded connections", () => {
    const inventory: ActorInventory = {
        connectionsComplete: true,
        actors: [
            {
                actorName: "Room",
                live: 1,
                dormant: 0,
                unknown: 0,
                instances: [
                    {
                        actorId: "random",
                        status: "live",
                        connections: [
                            { id: "c2", metadata: { name: "Ada" } },
                            { id: "c9", metadata: null }
                        ]
                    }
                ]
            }
        ]
    }
    const rows = [
        { connectionId: "c2", actorName: "Room", actorId: "random", hostId: "host-2", openedAtMs: 6000, closedAtMs: null, lastSeenMs: 6000, messages: 0, failures: 0, metadata: { name: "Stale" } },
        { connectionId: "c1", actorName: "Room", actorId: "lobby", hostId: "host-1", openedAtMs: 1000, closedAtMs: 4000, lastSeenMs: 4000, messages: 2, failures: 1, metadata: { name: "Grace" } },
        { connectionId: "c0", actorName: "Room", actorId: "lobby", hostId: "host-1", openedAtMs: 100, closedAtMs: null, lastSeenMs: 300, messages: 1, failures: 0 }
    ]
    const sessions = socketSessions(rows, inventory)
    assert.equal(socketSessions(rows, { ...inventory, connectionsComplete: false }).find(session => session.connectionId === "c0")?.status, "unknown")
    assert.deepEqual(
        sessions.map(session => [session.connectionId, session.status, session.metadata]),
        [
            ["c9", "open", null],
            ["c2", "open", { name: "Ada" }],
            ["c1", "closed", { name: "Grace" }],
            ["c0", "lost", undefined]
        ],
        "live inventory metadata wins for open connections; saved connect metadata serves closed ones"
    )
    assert.deepEqual(sessionDuration(sessions[0]!, 10_000), null, "a connection without a retained connect event has no duration")
    const seen = socketSessions(rows, inventory, new Map([[JSON.stringify(["Room", "random", "c9"]), 9_000]]))[0]!
    assert.deepEqual(
        [seen.openedAtMs, seen.estimatedStart, sessionDuration(seen, 10_000)],
        [9_000, true, { ms: 1_000, lowerBound: false }],
        "an inventory-only connection borrows the time it was first seen"
    )
    assert.deepEqual(sessionDuration(sessions[1]!, 10_000), { ms: 4000, lowerBound: false })
    assert.deepEqual(sessionDuration(sessions[2]!, 10_000), { ms: 3000, lowerBound: false })
    assert.deepEqual(sessionDuration(sessions[3]!, 10_000), { ms: 200, lowerBound: true })
    assert.deepEqual(
        socketSessions(rows, undefined).map(session => session.status),
        ["lost", "closed", "lost"]
    )
    const summary = sessionSummary(sessions, 10_000)
    assert.deepEqual(summary, { total: 4, open: 2, lost: 1, closed: 1, median: 3000, p95: 4000, messages: 3, messagesPerSession: 0.75 })
    assert.deepEqual(sessionSummary([], 10_000), { total: 0, open: 0, lost: 0, closed: 0, median: null, p95: null, messages: 0, messagesPerSession: null })
})

test("invalid session rows are rejected and durations format by magnitude", () => {
    assert.throws(() => sessionRows([{ connectionId: "c1", actorName: "Room", actorId: "lobby", hostId: null, openedAtMs: null, closedAtMs: null, lastSeenMs: null, messages: 0, failures: 0 }]))
    assert.throws(() => sessionRows([{ connectionId: 5, actorName: "Room", actorId: "lobby", hostId: null, openedAtMs: 1, closedAtMs: null, lastSeenMs: 1, messages: 0, failures: 0 }]))
    assert.equal(formatDuration(820), "820 ms")
    assert.equal(formatDuration(4210), "4.2 s")
    assert.equal(formatDuration(192_000), "3m 12s")
    assert.equal(formatDuration(2 * 3_600_000 + 5 * 60_000), "2h 5m")
    assert.equal(formatDuration(27 * 3_600_000), "1d 3h")
    assert.equal(metadataSummary({ name: "Ada", role: "moderator", team: "core", seat: 4 }), "name: Ada · role: moderator · team: core · +1")
    assert.equal(metadataSummary({ user: { id: 7 } }), 'user: {"id":7}')
    assert.equal(metadataSummary("guest-token"), "guest-token")
    assert.equal(metadataSummary(null), null)
    assert.equal(metadataSummary({}), null)
    assert.equal(metadataSummary(undefined), null)
})
