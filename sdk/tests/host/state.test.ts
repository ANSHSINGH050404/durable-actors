import assert from "node:assert/strict"
import { test } from "node:test"

import { Persistence } from "../../src/actor/schema.js"
import { ActorDefinitionError, ActorSerializationError } from "../../src/errors.js"
import { hydrateActorState, snapshotActorState } from "../fixtures/actor-runtime.js"

const schema = {
    actorName: "Counter",
    fields: [
        { name: "count", persistence: Persistence.Persisted },
        { name: "nested", persistence: Persistence.Persisted },
        { name: "cache", persistence: Persistence.Ephemeral }
    ]
}

test("snapshots save only persisted fields and detach nested state", () => {
    const instance = { count: 1, nested: { enabled: false }, cache: {} }
    instance.cache = instance
    const snapshot = snapshotActorState(instance, schema)
    instance.nested.enabled = true
    assert.deepEqual(snapshot, { count: 1, nested: { enabled: false } })
})

test("hydration overlays persisted values and preserves ephemeral values and new defaults", () => {
    const cache = new Map([["live", 1]])
    const instance = { count: 5, nested: { count: 0 }, cache }
    const state = JSON.parse('{"nested":{"count":1},"cache":"obsolete","__proto__":{"polluted":true},"retired":42}')
    hydrateActorState(instance, state, schema)
    state.nested.count = 2
    assert.equal(Object.getPrototypeOf(instance), Object.prototype)
    assert.equal(instance.cache, cache)
    assert.deepEqual(snapshotActorState(instance, schema), { count: 5, nested: { count: 1 } })
    assert.equal(Object.hasOwn(instance, "retired"), false)
})

test("restoring empty state retains persisted initializer defaults", () => {
    const instance = { count: 5 }
    hydrateActorState(instance, {}, schema)
    assert.deepEqual(snapshotActorState(instance, schema), { count: 5 })
})

test("persisted state retains JSON conversion and serialization failures", () => {
    assert.deepEqual(snapshotActorState({ count: undefined, nested: [undefined] }, schema), { nested: [null] })
    assert.throws(() => snapshotActorState({ count: 1n }, schema), ActorSerializationError)
})

test("undeclared own properties cannot silently become actor state", () => {
    for (const key of ["undeclared", Symbol("undeclared")]) {
        const instance = Object.defineProperty({ count: 1 }, key, { value: 42 })
        assert.throws(() => snapshotActorState(instance, schema), ActorDefinitionError)
        assert.throws(() => hydrateActorState(instance, {}, schema), ActorDefinitionError)
    }
})

test("hydration rejects fields that cannot be replaced", () => {
    const instance = Object.defineProperty({}, "count", { enumerable: true, value: 1 })
    assert.throws(() => hydrateActorState(instance, { count: 2 }, schema), ActorSerializationError)
})
