import assert from "node:assert/strict"
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import { ActorCompiler, Persistence, analyzeActors, resolveSdkSymbols } from "../../src/compiler/actor-compiler.js"

test("rejects invalid or dynamic compute overrides", () => {
    for (const settings of ["{ cpu: 0 }", "{ regions: [] }", "{ cpu: Math.random() }", "{ ...defaults }"]) {
        const result = analyze(`import { Actor, Compute } from "./sdk.js"
            const defaults = { cpu: 1 }
            @Compute(${settings}) export class Room extends Actor { async read() {} }`)
        assert.ok(result.diagnostics.length > 0, settings)
    }
    for (const declaration of [
        "@Compute({}) @Compute({}) export class Room extends Actor {}",
        "export class Room extends Actor { @Compute({}) async read() {} }"
    ])
        assert.ok(analyze(`import { Actor, Compute } from "./sdk.js"; ${declaration}`).diagnostics.length > 0)
})

test("db is reserved for the actor SQLite database", () => {
    const result = analyze(`import { Actor, Persisted } from "./sdk.js"
        export class Room extends Actor { @Persisted db = 1 }`)
    assert.ok(result.diagnostics.some(diagnostic => String(diagnostic.messageText).includes("reserved")))
})

test("recognizes aliased interleave async methods", () => {
    const result = analyze(`import { Actor, Interleave as R } from "./sdk.js"
        export class Room extends Actor { @R async stream() {} async read() {} }`)
    assert.deepEqual(result.diagnostics, [])
    assert.deepEqual(result.schemas, [{ actorName: "Room", fields: [], reentrantMethods: ["stream"] }])
})

test("continues compiling undecorated actors against SDKs without reentrancy", () => {
    const result = analyze(`import { Actor } from "./sdk.js"; export class Room extends Actor { async read() {} }`, {
        "sdk.ts": `export abstract class Actor { protected constructor() {} }
            export function Persisted(...args: unknown[]) {}
            export function Emittable(...args: unknown[]) {}
            export function Ephemeral(...args: unknown[]) {}`
    })
    assert.deepEqual(result.diagnostics, [])
    assert.deepEqual(result.schemas, [{ actorName: "Room", fields: [] }])
})

test("rejects invalid interleave declarations", () => {
    for (const member of [
        "@Interleave value = 1",
        "@Interleave sync() {}",
        "@Interleave static async run() {}",
        "@Interleave private async run() {}",
        "@Interleave() async run() {}",
        "@Interleave @Interleave async run() {}"
    ]) {
        const result = analyze(`import { Actor, Interleave } from "./sdk.js"
            export class Room extends Actor { ${member} }`)
        assert.ok(result.diagnostics.length > 0, member)
    }
})

test("retains state visibility and stacked emission annotations", () => {
    const result = analyze(`import { Actor, Persisted, Emittable } from "./sdk.js"
        export class Room extends Actor {
            @Persisted @Emittable messages: string[] = []
            @Persisted private secret = "secret"
            @Persisted protected internal = 1
        }`)
    assert.deepEqual(result.diagnostics, [])
    assert.deepEqual(result.schemas[0]?.fields, [
        { name: "messages", persistence: Persistence.Persisted, emittable: true },
        { name: "secret", persistence: Persistence.Persisted, visibility: "private" },
        { name: "internal", persistence: Persistence.Persisted, visibility: "protected" }
    ])
})

test("emittable fields must be public persisted fields with one emission annotation", () => {
    for (const field of [
        "@Emittable value = 0",
        "@Persisted @Emittable private value = 0",
        "@Persisted @Emittable protected value = 0",
        "@Ephemeral @Emittable value = 0",
        "@Persisted @Emittable @Emittable value = 0",
        "@Persisted @Emittable() value = 0"
    ]) {
        const result = analyze(`import { Actor, Persisted, Ephemeral, Emittable } from "./sdk.js"
            export class Room extends Actor { ${field} }`)
        assert.ok(result.diagnostics.length > 0, field)
    }
})

test("validates actors without writing files", async () => {
    const root = await createProject()
    try {
        const compiler = new ActorCompiler({
            ...ts.sys,
            writeFile: () => assert.fail("actor validation must not write files")
        })
        const schemas = compiler.check(path.join(root, "src/actors.ts"))
        assert.equal(schemas[0]?.actorName, "Counter")
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("compiles socket validation schemas without importing actor implementation", async () => {
    const root = await createProject()
    try {
        const entrypoint = path.join(root, "src/actors.ts")
        await writeFile(
            entrypoint,
            `import { Actor, Persisted, Emittable } from "durable-actors"
            type Incoming = { type: "post"; text: string }
            type Outgoing = { type: "posted"; text: string }
            export class Room extends Actor<{ userId: string }, Incoming, Outgoing> {
                @Persisted @Emittable messages: string[] = []
                @Persisted private secret = "hidden"
                @Persisted label?: string
            }
            throw new Error("must not execute during generation")`
        )
        const [actor] = new ActorCompiler().compile(entrypoint)
        assert.equal(actor.contract.actorName, "Room")
        assert.deepEqual(actor.contract.emittable, ["messages"])
        const state = actor.contract.schema.definitions?.State
        assert.ok(state && typeof state === "object")
        assert.deepEqual(Object.keys(state.properties ?? {}), ["messages", "label"])
        assert.deepEqual(state.required, ["messages"])
        assert.equal(JSON.stringify(actor.contract).includes("hidden"), false)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects non-JSON socket types and public state with useful actor diagnostics", async () => {
    const root = await createProject()
    try {
        const entrypoint = path.join(root, "src/actors.ts")
        for (const type of [
            "Date",
            "bigint",
            "() => void",
            "Map<string, number>",
            "any",
            "{ first?: string; required: string | undefined }"
        ]) {
            await writeFile(
                entrypoint,
                `import { Actor } from "durable-actors"
                export class Room extends Actor<{}, { value: ${type} }, never> {}`
            )
            assert.throws(() => new ActorCompiler().compile(entrypoint), /Room.*JSON/)
        }
        await writeFile(
            entrypoint,
            `import { Actor, Persisted } from "durable-actors"
            export class Room extends Actor { @Persisted value: string | undefined = "initial" }`
        )
        assert.throws(() => new ActorCompiler().compile(entrypoint), /Room.*JSON/)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("checks source actors and returns persistence schemas without generating files", async () => {
    const root = await createProject()
    try {
        const compiler = new ActorCompiler()
        const entrypoint = path.join(root, "src/actors.ts")
        const schemas = compiler.check(entrypoint)
        assert.deepEqual(schemas, [
            {
                actorName: "Counter",
                fields: [
                    { name: "count", persistence: Persistence.Persisted, visibility: "private" },
                    { name: "cache", persistence: Persistence.Ephemeral },
                    { name: "callback", persistence: Persistence.Ephemeral }
                ]
            }
        ])
        await assert.rejects(readFile(path.join(root, "dist/actors.js")), { code: "ENOENT" })
        assert.deepEqual(await readdir(path.join(root, "src")), ["actors.ts"])
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("checks actors and rejects invalid fields without executing the module", async () => {
    const root = await createProject()
    try {
        const compiler = new ActorCompiler()
        const entrypoint = path.join(root, "src/actors.ts")
        await writeFile(
            entrypoint,
            `${await readFile(entrypoint, "utf8")}\nthrow new Error("must not execute during analysis")\n`
        )
        compiler.check(entrypoint)
        await writeFile(
            entrypoint,
            'import { Actor } from "durable-actors"; export class Counter extends Actor { count = 0 }'
        )
        assert.throws(() => compiler.check(entrypoint), /must declare exactly one/)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects JavaScript and declaration entrypoints without modifying them", async () => {
    const root = await createProject()
    try {
        for (const extension of ["js", "mjs", "cjs", "d.ts"]) {
            const entrypoint = path.join(root, `src/actors.${extension}`)
            const source = "export const value = 1"
            await writeFile(entrypoint, source)
            assert.throws(() => new ActorCompiler().check(entrypoint), /TypeScript source/)
            assert.equal(await readFile(entrypoint, "utf8"), source)
        }
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("actor loading validates its dependency graph without type checking unrelated application files", async () => {
    const root = await createProject()
    try {
        await writeFile(path.join(root, "src/unrelated.ts"), "const invalid: string = 123")
        const schemas = new ActorCompiler().check(path.join(root, "src/actors.ts"))
        assert.equal(schemas[0]?.actorName, "Counter")
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects TypeScript errors in actor dependencies", async () => {
    const root = await createProject()
    try {
        await writeFile(path.join(root, "src/invalid.ts"), "export const value: string = 123")
        const entrypoint = path.join(root, "src/actors.ts")
        await writeFile(entrypoint, `import "./invalid.js"\n${await readFile(entrypoint, "utf8")}`)
        assert.throws(() => new ActorCompiler().check(entrypoint), /Type 'number' is not assignable to type 'string'/)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("source loading respects noEmit configurations with TypeScript extension imports", async () => {
    const root = await createProject()
    try {
        const configFile = path.join(root, "tsconfig.json")
        const config = JSON.parse(await readFile(configFile, "utf8"))
        Object.assign(config.compilerOptions, { noEmit: true, allowImportingTsExtensions: true })
        await writeFile(configFile, JSON.stringify(config))
        assert.equal(new ActorCompiler().check(path.join(root, "src/actors.ts"))[0]?.actorName, "Counter")
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("checks private and optional computed fields on re-exported actors", async () => {
    const root = await createProject()
    try {
        await mkdir(path.join(root, "src/nested"))
        await writeFile(path.join(root, "src/actors.ts"), 'export { Counter } from "./nested/counter.js"')
        await writeFile(
            path.join(root, "src/nested/counter.ts"),
            `import { Actor, Persisted, Ephemeral } from "durable-actors"
            type Count = { value: number }
            export class Counter extends Actor<{}, never, never> {
                @Persisted private count: Count = { value: 0 }
                @Persisted ["label"]?: string
                @Ephemeral cache = new Map()
                async increment() { return ++this.count.value }
            }`
        )
        const compiler = new ActorCompiler()
        assert.deepEqual(compiler.check(path.join(root, "src/actors.ts")), [
            {
                actorName: "Counter",
                fields: [
                    { name: "count", persistence: Persistence.Persisted, visibility: "private" },
                    { name: "label", persistence: Persistence.Persisted },
                    { name: "cache", persistence: Persistence.Ephemeral }
                ]
            }
        ])
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("resolves actor and decorator aliases through re-exports and namespaces", () => {
    const result = analyze(
        `import { Base, Saved } from "./barrel.js"
         import * as sdk from "./sdk.js"
         class Counter extends Base {
             @Saved private count = 0
             @sdk.Ephemeral cache = new Map()
             @sdk.Ephemeral #resource = 1
             static config = {}
             async increment() { return ++this.count }
         }
         export { Counter }`,
        { "barrel.ts": 'export { Actor as Base, Persisted as Saved } from "./sdk.js"' }
    )
    assert.deepEqual(
        result.diagnostics.map(diagnostic => diagnostic.messageText),
        []
    )
    assert.deepEqual(result.schemas, [
        {
            actorName: "Counter",
            fields: [
                { name: "count", persistence: Persistence.Persisted, visibility: "private" },
                { name: "cache", persistence: Persistence.Ephemeral },
                { name: "#resource", persistence: Persistence.Ephemeral, private: true }
            ]
        }
    ])
})

test("discovers actors alongside unrelated runtime and type exports", () => {
    const result = analyze(`import { Actor, Persisted } from "./sdk.js"
        export { Actor } from "./sdk.js"
        export const limit = 10
        export const empty = null
        export const callback = () => 1
        export function helper() { return limit }
        export class Utility { value = 1 }
        export enum Status { Ready }
        export interface Options { limit: number }
        export type { Actor as ActorName } from "./sdk.js"
        export default { limit }
        export class Counter extends Actor { @Persisted count = 0 }`)
    assert.deepEqual(
        result.diagnostics.map(diagnostic => diagnostic.messageText),
        []
    )
    assert.deepEqual(result.schemas, [
        { actorName: "Counter", fields: [{ name: "count", persistence: Persistence.Persisted }] }
    ])
})

test("requires at least one actor even when the entrypoint exports other values or types", () => {
    for (const source of ["export {}", "export const value = 1", "export interface Options {}"])
        assert.match(String(analyze(source).diagnostics[0]?.messageText), /named actor exports/)
})

test("requires annotations on uninitialized, private and ordinary instance fields", () => {
    const result = analyze(`import { Actor } from "./sdk.js"
        export class Counter extends Actor {
            count!: number
            private cache = 0
            #resource = 0
        }`)
    assert.equal(result.diagnostics.length, 3)
    for (const diagnostic of result.diagnostics) {
        assert.match(String(diagnostic.messageText), /must declare exactly one of @Persisted or @Ephemeral/)
        assert.ok(diagnostic.file)
        assert.ok(diagnostic.start! > 0)
    }
    assert.deepEqual(result.schemas, [])
})

test("unrelated decorators with the same name do not count as SDK annotations", () => {
    const result = analyze(`import { Actor } from "./sdk.js"
        function Persisted(...args: unknown[]) {}
        export class Counter extends Actor { @Persisted count = 0 }`)
    assert.match(String(result.diagnostics[0]?.messageText), /must declare exactly one/)
})

test("retains duplicate annotations to report conflicts with both source locations", () => {
    for (const decorators of ["@Persisted @Ephemeral", "@Persisted @Persisted"]) {
        const result = analyze(`import { Actor, Persisted, Ephemeral } from "./sdk.js"
            export class Counter extends Actor { ${decorators} count = 0 }`)
        assert.match(String(result.diagnostics[0]?.messageText), /exactly one/)
        assert.equal(result.diagnostics[0]?.relatedInformation?.length, 2)
    }
})

test("rejects unsupported persistence targets and syntax", () => {
    for (const member of [
        "@Persisted static count = 0",
        "@Persisted async count() {}",
        "@Persisted get count() { return 0 }",
        "@Persisted accessor count = 0",
        "@Persisted #count = 0",
        "@Persisted [Symbol.iterator] = 0",
        "@Persisted() count = 0",
        "constructor(public count = 0) { super() }",
        "@Persisted declare count: number"
    ]) {
        const result = analyze(`import { Actor, Persisted } from "./sdk.js"
            export class Counter extends Actor { ${member} }`)
        assert.ok(result.diagnostics.length > 0, member)
        assert.deepEqual(result.schemas, [], member)
    }
})

test("checks class decorators and entrypoint exports", () => {
    for (const [declaration, message] of [
        ["@Persisted export class Counter extends Actor {}", /instance field/],
        ["export default class Counter extends Actor {}", /named exports/],
        ["class Counter extends Actor {}; export { Counter as Renamed }", /same class name/],
        ["class Base extends Actor {}; export class Counter extends Base {}", /directly extends Actor/],
        ["class Base<T> extends Actor {}; export class Counter extends Base<string> {}", /directly extends Actor/],
        ["export abstract class Counter extends Actor {}", /cannot be abstract/],
        ["export class Counter<T> extends Actor {}", /cannot have type parameters/],
        ["export class $Counter extends Actor {}", /actor name may contain only ASCII/],
        ["export class Café extends Actor {}", /actor name may contain only ASCII/]
    ] as const) {
        const result = analyze(`import { Actor, Persisted } from "./sdk.js";
            export const helper = 1
            export class Valid extends Actor {}
            ${declaration}`)
        assert.ok(
            result.diagnostics.some(diagnostic => message.test(String(diagnostic.messageText))),
            declaration
        )
    }
})

async function createProject() {
    const root = await mkdtemp(path.join(os.tmpdir(), "actor-compiler-"))
    await mkdir(path.join(root, "src"))
    await mkdir(path.join(root, "node_modules"))
    const sdkRoot = fileURLToPath(new URL("../../", import.meta.url))
    const packageRoot = sdkRoot.endsWith(`${path.sep}.test-dist${path.sep}`)
        ? path.dirname(sdkRoot.slice(0, -1))
        : sdkRoot
    await symlink(packageRoot, path.join(root, "node_modules/durable-actors"), "dir")
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(root, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                rootDir: "src",
                outDir: "dist"
            },
            include: ["src"]
        })
    )
    await writeFile(
        path.join(root, "src/actors.ts"),
        `import { Actor, Persisted, Ephemeral } from "durable-actors"
        export const defaultCount = 0
        export function formatCount(value: number) { return String(value) }
        export default { name: "counter" }
        export class Counter extends Actor<{}, never, never> {
            @Persisted private count = 0
            @Ephemeral cache = new Map<string, number>()
            @Ephemeral callback = () => 1
            async increment() { return ++this.count }
        }`
    )
    return root
}

function analyze(source: string, extra: Record<string, string> = {}) {
    const files = new Map(
        Object.entries({
            "actors.ts": source,
            "sdk.ts": `export abstract class Actor { protected constructor() {} }
            export function Persisted(...args: unknown[]) {}
            export function Emittable(...args: unknown[]) {}
            export function Interleave(...args: unknown[]) {}
            export function Compute(...args: unknown[]) {}
            export function Ephemeral(...args: unknown[]) {}`,
            ...extra
        }).map(([name, content]) => [`/virtual/${name}`, content])
    )
    const options = { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.NodeNext }
    const host = ts.createCompilerHost(options)
    const originalRead = host.readFile
    const originalExists = host.fileExists
    host.readFile = file => files.get(file) ?? originalRead(file)
    host.fileExists = file => files.has(file) || originalExists(file)
    const originalDirectoryExists = host.directoryExists!
    host.directoryExists = directory => directory === "/virtual" || originalDirectoryExists(directory)
    host.getSourceFile = (file, languageVersion) => {
        const text = host.readFile(file)
        return text === undefined ? undefined : ts.createSourceFile(file, text, languageVersion, true)
    }
    const program = ts.createProgram([...files.keys()], options, host)
    const sdk = resolveSdkSymbols(program.getTypeChecker(), program.getSourceFile("/virtual/sdk.ts")!)
    return analyzeActors(program, program.getSourceFile("/virtual/actors.ts")!, sdk)
}
