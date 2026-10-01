import assert from "node:assert/strict"

import type { ActorExecutorReply } from "../../src/host/protocol.js"

export function assertReply(actual: ActorExecutorReply, expected: object): void {
    if (actual.type === "invoked" || actual.type === "websocket_handled") {
        const { sqlite, ...reply } = actual
        assert.ok(sqlite !== undefined && Number.isSafeInteger(sqlite.txid) && sqlite.txid > 0)
        assert.deepEqual(reply, expected)
    } else assert.deepEqual(actual, expected)
}
