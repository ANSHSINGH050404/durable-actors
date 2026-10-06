import { Ajv } from "ajv"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "../../src/compiler/actor-compiler.js"
import { generateClientArtifacts } from "../../src/compiler/generators/client-artifacts.js"
import { parsePublicContract } from "../../src/compiler/validate-public-contract.js"

test("compute overrides survive compilation and public contract transport", async t => {
    const project = await createProject(t)
    await project.write(`import { Compute as S } from "durable-actors"
        @S({ cpu: 1.5, memoryMiB: 2048, regions: ["canada"], idleTimeoutMs: 60000 })
        export class Room extends Actor { async read() { return 1 } }
        export class Plain extends Actor { async read() { return 2 } }
        throw new Error("must not execute actor code")`)
    const deployment = new ActorCompiler().compileDeployment(project.entrypoint)
    const transported = parsePublicContract(JSON.parse(JSON.stringify(deployment.contract)))
    const options = { cpu: 1.5, memoryMiB: 2048, regions: ["canada"], idleTimeoutMs: 60000 }
    assert.deepEqual(transported.actors.find(actor => actor.actorName === "Room")?.sandbox, options)
    assert.equal(transported.actors.find(actor => actor.actorName === "Plain")?.sandbox, undefined)
})

test("public contracts survive JSON transport without actor source and with explicit dependency imports", async t => {
    const project = await createProject(t)
    await mkdir(path.join(project.root, "node_modules/@example/private-data"), { recursive: true })
    await writeFile(
        path.join(project.root, "node_modules/@example/private-data/package.json"),
        JSON.stringify({
            name: "@example/private-data",
            version: "1.0.0",
            type: "module",
            exports: { "./message": { types: "./index.d.ts", default: "./index.js" } },
            main: "index.js"
        })
    )
    await writeFile(
        path.join(project.root, "node_modules/@example/private-data/index.d.ts"),
        `export interface Message { id: string; text: string; reply?: Message; status: "sent" | "pending" }`
    )
    await writeFile(
        path.join(project.root, "node_modules/@example/private-data/index.js"),
        'throw new Error("never load")'
    )
    await writeFile(
        path.join(project.root, "message.ts"),
        'export type { Message } from "@example/private-data/message"'
    )
    await project.write(`
        import type { Message } from "./message.js"
        export class Room extends Actor<{ userId: string }, { text: string }, Message> {
            @Persisted @Emittable messages: Message[] = []
            @Persisted private secret = "hidden"
            async send(message: Message): Promise<Message> { return message }
            async clear(): Promise<void> {}
            private async internal(): Promise<Date> { return new Date() }
            protected async helper() { return 1n }
            async #hidden() {}
            static async utility() {}
            async onConnect() {}
            async onMessage() {}
            async onDisconnect() {}
        }
        throw new Error("generation must not execute actor code")
    `)
    const contract = new ActorCompiler().compileContract(project.entrypoint)
    const serialized = JSON.stringify(contract)
    assert.deepEqual(JSON.parse(serialized), contract)
    assert.equal(contract.version, 1)
    assert.deepEqual(contract.typescript.dependencies, { "@example/private-data": "1.0.0" })
    const [actor] = contract.actors
    assert.equal(actor.actorName, "Room")
    assert.deepEqual(
        actor.rpc.methods.map(method => method.name),
        ["clear", "send"]
    )
    assert.deepEqual(actor.rpc.methods[0].result, { kind: "void" })
    assert.deepEqual(actor.socket.emittable, ["messages"])
    for (const absent of [project.root, "secret", "hidden", "internal", "onConnect"])
        assert.equal(serialized.includes(absent), false, absent)
    await rm(project.root, { recursive: true, force: true })
    const fetched = JSON.parse(serialized)
    const method = fetched.actors[0].rpc.methods.find((method: { name: string }) => method.name === "send")
    const validate = new Ajv().compile({ ...fetched.actors[0].rpc.schema, ...method.parameters[0].type })
    assert.equal(
        validate({ id: "1", text: "hello", status: "sent", reply: { id: "0", text: "hi", status: "pending" } }),
        true
    )
    assert.equal(validate({ id: "1", text: 42, status: "sent" }), false)
    assert.equal(validate({ id: "1", text: "hello", status: "wrong" }), false)
    const validateResult = new Ajv().compile({ ...fetched.actors[0].rpc.schema, ...method.result.type })
    assert.equal(validateResult({ id: "1", text: "hello", status: "sent" }), true)
    assert.equal(validateResult({ id: 1, text: "hello", status: "sent" }), false)
})

test("bundles local type dependencies through the project's path aliases", async t => {
    const project = await createProject(t)
    await mkdir(path.join(project.root, "types"))
    await writeFile(
        path.join(project.root, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                paths: { "@model/*": ["./types/*"] }
            }
        })
    )
    await writeFile(path.join(project.root, "types/value.ts"), "export interface Value { readonly text: string }")
    await writeFile(
        path.join(project.root, "types/message.ts"),
        'import type { Value } from "@model/value.js"; export interface Message { values: readonly Value[] }'
    )
    await project.write(`
        import type { Message } from "@model/message.js"
        export class Room extends Actor<{}, Message, Message> {
            async echo(message: Message): Promise<Message> { return message }
        }
    `)
    const contract = new ActorCompiler().compileContract(project.entrypoint)
    assert.deepEqual(contract.typescript.dependencies, {})
    await rm(path.join(project.root, "types"), { recursive: true })
    await rm(project.entrypoint)
    for (const [name, contents] of await generateClientArtifacts(contract)) {
        const file = path.join(project.root, "generated", name)
        await mkdir(path.dirname(file), { recursive: true })
        await writeFile(file, contents)
    }
    const consumer = path.join(project.root, "consumer.ts")
    await writeFile(
        consumer,
        `
        import { actors } from "./generated/index.js"
        const message: actors.Room.Incoming = { values: [{ text: "hello" }] }
        const returned: actors.Room.Outgoing = await actors.Room.get("one").echo(message)
        // @ts-expect-error nested readonly fields survive bundling
        returned.values[0].text = "changed"
    `
    )
    const program = ts.createProgram([consumer], {
        strict: true,
        noEmit: true,
        skipLibCheck: false,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext
    })
    assert.deepEqual(
        ts.getPreEmitDiagnostics(program).map(d => ts.flattenDiagnosticMessageText(d.messageText, "\n")),
        []
    )
})

test("declaration bundling accepts separate actor exports and omits unused implementation declarations", async t => {
    const project = await createProject(t)
    await project.write(`
        class Room extends Actor<{}, never, never> {
            async echo(value: string) { return value }
            private async cache() { return new Map<string, Date>() }
        }
        export { Room }
        export function createRoomType(): typeof Room { return Room }
    `)
    const contract = new ActorCompiler().compileContract(project.entrypoint)
    assert.match(contract.typescript.declarations, /echo\(value: string\): Promise<string>/)
    assert.doesNotMatch(contract.typescript.declarations, /createRoomType|cache|Map|Date|class Room/)
})

test("captures optional, default, rest and nullable parameters and inferred promise results", async t => {
    const project = await createProject(t)
    await project.write(`
        export class Counter extends Actor<{}, never, never> {
            async add(amount: number, label?: string | null, scale = 1, ...tags: string[]) { return amount * scale }
            async unpack({ value }: { value: number }) { return value }
        }
    `)
    const [actor] = new ActorCompiler().compileContract(project.entrypoint).actors
    const [add, unpack] = actor.rpc.methods
    assert.deepEqual(
        add.parameters.map(({ name, optional, rest }) => ({ name, optional, rest })),
        [
            { name: "amount", optional: false, rest: false },
            { name: "label", optional: true, rest: false },
            { name: "scale", optional: true, rest: false },
            { name: "tags", optional: false, rest: true }
        ]
    )
    assert.equal(unpack.parameters[0].name, "arg0")
    assert.equal(add.result.kind, "value")
    if (add.result.kind !== "value") assert.fail("expected result schema")
    const ajv = new Ajv()
    const result = ajv.compile({ ...actor.rpc.schema, ...add.result.type })
    assert.equal(result(42), true)
    assert.equal(result("42"), false)
    const label = ajv.compile({ ...actor.rpc.schema, ...add.parameters[1].type })
    assert.equal(label(null), true)
    assert.equal(label("label"), true)
    const tags = ajv.compile({ ...actor.rpc.schema, ...add.parameters[3].type })
    assert.equal(tags(["one", "two"]), true)
    assert.equal(tags([1]), false)
})

test("extracts named re-exports and orders actors and methods deterministically", async t => {
    const project = await createProject(t)
    await project.write(`
        export class Zebra extends Actor<{}, never, never> { async z() {} async a() {} }
        export class Alpha extends Actor<{}, never, never> {}
    `)
    const entrypoint = path.join(project.root, "index.ts")
    await writeFile(entrypoint, 'export { Zebra, Alpha } from "./actors.js"')
    const compiler = new ActorCompiler()
    const first = compiler.compileContract(entrypoint)
    await writeFile(entrypoint, 'export { Alpha, Zebra } from "./actors.js"')
    assert.deepEqual(compiler.compileContract(entrypoint), first)
    assert.deepEqual(
        first.actors.map(actor => actor.actorName),
        ["Alpha", "Zebra"]
    )
    assert.deepEqual(
        first.actors[1].rpc.methods.map(method => method.name),
        ["a", "z"]
    )
})

test("explicit recursive JSON types support varied payloads in contracts", async t => {
    const project = await createProject(t)
    await project.write(`
        type Json = null | boolean | number | string | Json[] | { [key: string]: Json }
        interface Message {
            id: string
            metadata?: Json
            parts: { [key: string]: Json }[]
        }
        export class Room extends Actor<{}, never, never> {
            async send(message: Message): Promise<Message> { return message }
        }
    `)
    const { rpc } = new ActorCompiler().compileContract(project.entrypoint).actors[0]
    const [send] = rpc.methods
    assert.equal(send.result.kind, "value")
    if (send.result.kind !== "value") assert.fail("expected result schema")
    const ajv = new Ajv({ allowUnionTypes: true })
    for (const type of [send.parameters[0].type, send.result.type]) {
        const validate = ajv.compile({ ...rpc.schema, ...type })
        for (const value of [null, true, 42, "hello", [1, null], { nested: [false, { text: "hello" }] }]) {
            assert.equal(
                validate({
                    id: "1",
                    metadata: value,
                    parts: [{ type: "tool", input: value, output: value, data: value }]
                }),
                true
            )
        }
        assert.equal(validate({ id: "1", parts: [{ type: "text", text: "hello" }] }), true)
        assert.equal(validate({ id: 1, parts: [] }), false)
        assert.equal(validate({ id: "1", parts: "invalid" }), false)
    }
})

test("unknown accepts varied JSON values throughout public contracts", async t => {
    const project = await createProject(t)
    const cases: [string, unknown[]][] = [
        ["unknown", [null, true, 42, "hello", [1, null], { nested: false }]],
        ["{ metadata?: unknown }", [{}, { metadata: null }, { metadata: 42 }]],
        ["Record<string, unknown>", [{ text: "hello", number: 42, array: [null] }]],
        ["readonly unknown[]", [[null, 42, "hello", { nested: false }]]],
        [
            "[string, unknown]",
            [
                ["label", null],
                ["label", 42]
            ]
        ],
        ['{ kind: "text"; value: string } | { kind: "raw"; value: unknown }', [{ kind: "raw", value: 42 }]],
        ["{ id: string } & Record<string, unknown>", [{ id: "one", payload: null }]]
    ]
    for (const [type, values] of cases) {
        await t.test(type, async () => {
            await project.write(`export class Room extends Actor<{}, never, never> {
                async send(value: ${type}): Promise<${type}> { return value }
            }`)
            const { rpc } = new ActorCompiler().compileContract(project.entrypoint).actors[0]
            const [send] = rpc.methods
            if (send.result.kind !== "value") assert.fail("expected result schema")
            for (const reference of [send.parameters[0].type, send.result.type]) {
                const validate = new Ajv().compile({ ...rpc.schema, ...reference })
                for (const value of values) assert.equal(validate(value), true)
            }
        })
    }
})

test("JSON dictionaries allow omitted entries", async t => {
    const project = await createProject(t)
    await project.write(`
        type Json = null | boolean | number | string | Json[] | { [key: string]: Json | undefined }
        export class Room extends Actor<{}, never, never> {
            async send(value: { [key: string]: Json | undefined }) { return value }
        }
    `)
    const { rpc } = new ActorCompiler().compileContract(project.entrypoint).actors[0]
    const [send] = rpc.methods
    if (send.result.kind !== "value") assert.fail("expected result schema")
    for (const type of [send.parameters[0].type, send.result.type]) {
        const validate = new Ajv({ allowUnionTypes: true }).compile({ ...rpc.schema, ...type })
        const value = { omitted: undefined, nested: { omitted: undefined, retained: [null, true, 42, "hello"] } }
        assert.equal(validate(JSON.parse(JSON.stringify(value))), true)
        assert.equal(validate({}), true)
        assert.equal(validate(["invalid"]), false)
    }
})

test("rejects RPC types that cannot preserve their meaning across JSON", async t => {
    const project = await createProject(t)
    for (const type of [
        "Date",
        "bigint",
        "any",
        "() => void",
        "string | undefined",
        "(string | undefined)[]",
        "{ nested: Date }",
        "Record<string, Date | undefined>"
    ]) {
        await project.write(`export class Room extends Actor<{}, never, never> {
            async send(value: ${type}): Promise<void> {}
        }`)
        assert.throws(() => new ActorCompiler().compileContract(project.entrypoint), /Room\.send.*JSON/, type)
    }
    await project.write(`export class Room extends Actor<{}, never, never> {
        async send(): Promise<Date> { return new Date() }
    }`)
    assert.throws(() => new ActorCompiler().compileContract(project.entrypoint), /Room\.send.*JSON/)
})

test("contracts are independent of checkout paths and preserve distinct types with the same name", async t => {
    const contracts = []
    for (let index = 0; index < 2; index++) {
        const project = await createProject(t)
        await writeFile(path.join(project.root, "first.ts"), "export interface Item { value: string }")
        await writeFile(path.join(project.root, "second.ts"), "export interface Item { value: number }")
        await project.write(`
            import type { Item as First } from "./first.js"
            import type { Item as Second } from "./second.js"
            export class Room extends Actor<First, Second, never> {
                async first(value: First): Promise<First> { return value }
                async second(value: Second): Promise<Second> { return value }
            }
        `)
        contracts.push(new ActorCompiler().compileContract(project.entrypoint))
    }
    assert.deepEqual(contracts[0], contracts[1])
    const { rpc } = contracts[0].actors[0]
    const first = new Ajv().compile({ ...rpc.schema, ...rpc.methods[0].parameters[0].type })
    const second = new Ajv().compile({ ...rpc.schema, ...rpc.methods[1].parameters[0].type })
    assert.equal(first({ value: "one" }), true)
    assert.equal(first({ value: 1 }), false)
    assert.equal(second({ value: "one" }), false)
    assert.equal(second({ value: 1 }), true)
})

test("rejects unsupported callable shapes instead of publishing incomplete signatures", async t => {
    const project = await createProject(t)
    for (const [method, message] of [
        ["async send<T>(value: T): Promise<T> { return value }", /Room\.send.*generic/],
        [
            "async send(value: string): Promise<string>; async send(value: number): Promise<number>; async send(value: string | number) { return value }",
            /Room\.send.*overload/
        ],
        ["send() { return Promise.resolve(1) }", /Room\.send.*async/],
        ["async then() {}", /Room\.then.*reserved/],
        ["async connect() {}", /Room\.connect.*reserved/],
        ["get value() { return 1 }", /Room\.value.*accessor/],
        ["async send(this: Room) {}", /Room\.send.*this/],
        ["async send(...values: [string, number?]) {}", /Room\.send.*rest/],
        ["async send(value = 1, required: string) {}", /Room\.send.*default/]
    ] as const) {
        await project.write(`export class Room extends Actor<{}, never, never> { ${method} }`)
        assert.throws(() => new ActorCompiler().compileContract(project.entrypoint), message, method)
    }
})

async function createProject(t: { after(fn: () => Promise<void>): void }) {
    const root = await mkdtemp(path.join(os.tmpdir(), "public-contract-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    const location = fileURLToPath(new URL("../../", import.meta.url))
    const sdk = location.endsWith(`${path.sep}.test-dist${path.sep}`) ? path.dirname(location.slice(0, -1)) : location
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    const entrypoint = path.join(root, "actors.ts")
    return {
        root,
        entrypoint,
        write: (source: string) =>
            writeFile(entrypoint, `import { Actor, Persisted, Emittable } from "durable-actors"\n${source}`)
    }
}
