import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, type ActorClass, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime } from "../../src/host/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"

function deferred() {
    let resolve!: () => void
    const promise = new Promise<void>(done => {
        resolve = done
    })
    return { promise, resolve }
}

async function setup(actorClass: ActorClass, reentrantMethods: string[], allowNextInvocation = () => {}) {
    const definition = registerActorClass(actorClass, {
        actorName: actorClass.name,
        fields: [{ name: "count", persistence: Persistence.Persisted }],
        ...{ reentrantMethods }
    })
    const runtime = new ActorRuntime(definition, allowNextInvocation)
    const sqlite = await seed()
    const invoke = (method: string) =>
        runtime.handle({
            type: "invoke",
            request_id: method,
            actor: { project_id: "test", actor_name: actorClass.name, actor_id: "one" },
            method,
            args: [],
            sqlite
        })
    return { runtime, invoke }
}

test("only entering a reentrant invocation grants admission; nested calls inherit their caller", async () => {
    const gate = deferred()
    let admissions = 0
    class AdmissionProbe extends Actor {
        count = 0
        async reentrant() {
            await gate.promise
        }
        async ordinary() {
            await this.reentrant()
        }
    }
    const { invoke } = await setup(AdmissionProbe, ["reentrant"], () => {
        admissions++
    })
    const ordinary = invoke("ordinary")
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(admissions, 0)
    const reentrant = invoke("reentrant")
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(admissions, 0)
    gate.resolve()
    await Promise.all([ordinary, reentrant])
    assert.equal(admissions, 1)
})

test("a failed reentrant invocation cannot erase an overlapping successful mutation", async () => {
    const gate = deferred()
    class ReentrantFailure extends Actor {
        count = 0
        async hold() {
            this.count++
            await gate.promise
            throw new Error("model failed")
        }
        async increment() {
            return ++this.count
        }
        async read() {
            return this.count
        }
    }
    const { invoke } = await setup(ReentrantFailure, ["hold"])
    const holding = invoke("hold")
    await invoke("increment")
    gate.resolve()
    assert.equal((await holding).type, "failed")
    const reply = await invoke("read")
    assert.equal("result" in reply && reply.result, 2)
})

test("undecorated invocations still serialize with each other on an opted-in actor", async () => {
    const gate = deferred()
    const entered: string[] = []
    class ReentrantSerial extends Actor {
        count = 0
        async hold() {
            entered.push("hold")
            await gate.promise
            this.count++
        }
        async read() {
            entered.push("read")
            return this.count
        }
        async background() {}
    }
    const { invoke } = await setup(ReentrantSerial, ["background"])
    const holding = invoke("hold")
    const reading = invoke("read")
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(entered, ["hold"])
    gate.resolve()
    await holding
    await reading
    assert.deepEqual(entered, ["hold", "read"])
})

test("an ordinary invocation blocks newly arriving reentrant calls across awaits", async () => {
    const gate = deferred()
    const entered = deferred()
    const calls: string[] = []
    class ExclusiveAdmission extends Actor {
        count = 0
        async ordinary() {
            calls.push("ordinary:start")
            entered.resolve()
            await gate.promise
            calls.push("ordinary:end")
        }
        async reentrant() {
            calls.push("reentrant")
            return ++this.count
        }
    }
    const { invoke } = await setup(ExclusiveAdmission, ["reentrant"])
    const ordinary = invoke("ordinary")
    await entered.promise
    const reentrant = invoke("reentrant")
    try {
        await new Promise(resolve => setImmediate(resolve))
        assert.deepEqual(calls, ["ordinary:start"])
    } finally {
        gate.resolve()
        await Promise.all([ordinary, reentrant])
    }
    assert.deepEqual(calls, ["ordinary:start", "ordinary:end", "reentrant"])
})

test("socket context survives another invocation finishing during a reentrant await", async () => {
    const gate = deferred()
    class ReentrantSockets extends Actor {
        count = 0
        async hold() {
            this.broadcast("before")
            await gate.promise
            this.broadcast("after")
        }
        async read() {
            this.broadcast("other")
            return this.count
        }
    }
    const { invoke } = await setup(ReentrantSockets, ["hold"])
    const holding = invoke("hold")
    await invoke("read")
    gate.resolve()
    const reply = await holding
    assert.notEqual(reply.type, "failed")
    assert.ok("effects" in reply)
    assert.deepEqual(
        reply.effects?.map(effect => effect.type === "broadcast" && effect.message.data),
        ['"before"', '"after"']
    )
})

test("reentrant continuations may resume during an undecorated await and nested calls inherit their caller", async () => {
    const generation = deferred()
    const settings = deferred()
    const entered = deferred()
    class NativeInterleaving extends Actor {
        count = 0
        async helper() {
            await generation.promise
            this.count++
        }
        async hold() {
            await this.helper()
            return this.count
        }
        async update() {
            entered.resolve()
            await settings.promise
            return this.count
        }
    }
    const { invoke } = await setup(NativeInterleaving, ["hold"])
    const holding = invoke("hold")
    const updating = invoke("update")
    await entered.promise
    generation.resolve()
    const result = await holding
    assert.equal("result" in result && result.result, 1)
    settings.resolve()
    await updating
})

test("an undecorated failure on an opted-in actor cannot roll back a reentrant success", async () => {
    const release = deferred()
    const entered = deferred()
    class OrdinaryFailure extends Actor {
        count = 0
        async background() {
            await entered.promise
            return ++this.count
        }
        async fail() {
            entered.resolve()
            await release.promise
            throw new Error("failed")
        }
        async read() {
            return this.count
        }
    }
    const { invoke } = await setup(OrdinaryFailure, ["background"])
    const background = invoke("background")
    const failed = invoke("fail")
    await background
    release.resolve()
    assert.equal((await failed).type, "failed")
    const result = await invoke("read")
    assert.equal("result" in result && result.result, 1)
})

test("emittable changes compare consecutive completed snapshots, including a value restored by a continuation", async () => {
    const gate = deferred()
    class ReentrantEmission extends Actor {
        count = 0
        async hold() {
            await gate.promise
            this.count = 0
        }
        async increment() {
            this.count++
        }
    }
    const definition = registerActorClass(ReentrantEmission, {
        actorName: "ReentrantEmission",
        reentrantMethods: ["hold"],
        fields: [{ name: "count", persistence: Persistence.Persisted, emittable: true }]
    })
    const runtime = new ActorRuntime(definition, () => {})
    const command = {
        type: "invoke" as const,
        request_id: "hold",
        actor: { project_id: "test", actor_name: "ReentrantEmission", actor_id: "one" },
        args: [],
        sqlite: await seed()
    }
    const holding = runtime.handle({ ...command, method: "hold" })
    await runtime.handle({ ...command, method: "increment" })
    gate.resolve()
    const reply = await holding
    assert.ok("effects" in reply)
    assert.deepEqual(reply.effects, [{ type: "state_update", changes: { count: 0 }, removed: [] }])
})
