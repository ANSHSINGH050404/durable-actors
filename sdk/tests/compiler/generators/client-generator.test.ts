import { build } from "esbuild"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "../../../src/compiler/actor-compiler.js"
import { declarations } from "../../fixtures/declarations.js"

test("generates an actor-specific proxy from backend metadata types", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-proxy-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, "node_modules"))
    await symlink(
        fileURLToPath(new URL("../../../../", import.meta.url)),
        path.join(directory, "node_modules/durable-actors")
    )
    await writeFile(path.join(directory, "package.json"), JSON.stringify({ type: "module" }))
    const entrypoint = path.join(directory, "actors.ts")
    await writeFile(
        entrypoint,
        `import { Actor } from "durable-actors"
        interface Member { userId: string; profile?: { displayName: string } }
        export class Room extends Actor<Member, { type: "post"; text: string }, never> {}
        export class Counter extends Actor<{ tenantId: number; role: "viewer" | "editor" }, number, never> {}
        throw new Error("generation must not execute backend code")`
    )
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient(new ActorCompiler().compileContract(entrypoint), directory)
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `import { ActorProxy } from "./index.js"
        import type { ActorAuthorization } from "./index.js"
        import { actors } from "./index.js"
        const grant: Promise<{ websocketUrl: string; homeRegion: string; connectByMs: number }> = actors.Room.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } })
        actors.Counter.prepareWebsocket({ actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        // @ts-expect-error unknown actor
        actors.Missing.prepareWebsocket({ actorId: "one", metadata: {} })
        // @ts-expect-error actor name is selected by the helper
        actors.Room.prepareWebsocket({ actorName: "Counter", actorId: "one", metadata: { userId: "alice" } })
        // @ts-expect-error metadata belongs to another actor
        actors.Room.prepareWebsocket({ actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        // @ts-expect-error required metadata is missing
        actors.Room.prepareWebsocket({ actorId: "one" })
        // @ts-expect-error nested metadata is typed
        actors.Room.prepareWebsocket({ actorId: "one", metadata: { userId: "alice", profile: { displayName: 1 } } })
        // @ts-expect-error metadata literals are preserved
        actors.Counter.prepareWebsocket({ actorId: "one", metadata: { tenantId: 1, role: "admin" } })
        ActorProxy.handle({ actorName: "Room", actorId: "lobby", metadata: { userId: "alice" } })
        ActorProxy.handle({ actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        const proxy = new ActorProxy({ controlPlaneUrl: "https://actors.example.com", apiKey: "secret" })
        proxy.handle({ actorName: "Room", actorId: "lobby", metadata: { userId: "alice", profile: { displayName: "Alice" } } })
        // @ts-expect-error unknown actor
        ActorProxy.handle({ actorName: "Missing", actorId: "one", metadata: {} })
        // @ts-expect-error metadata belongs to another actor
        ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        // @ts-expect-error required metadata is missing
        ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: {} })
        // @ts-expect-error nested metadata is typed
        ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: { userId: "alice", profile: { displayName: 1 } } })
        // @ts-expect-error metadata literals are preserved
        proxy.handle({ actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "admin" } })
        function authorize(value: ActorAuthorization) {
            if (value.actorName === "Room") value.metadata.userId.toUpperCase()
            else value.metadata.tenantId.toFixed()
        }
`
    )
    checkTypes(consumer)
    const proxyFile = path.join(directory, "proxy.mjs")
    await build({
        entryPoints: [path.join(directory, "index.js")],
        bundle: true,
        platform: "node",
        format: "esm",
        outfile: proxyFile,
        logLevel: "silent"
    })
    const { actors, ActorProxy } = await import(pathToFileURL(proxyFile).href)
    const requests: { url: string; metadata: unknown }[] = []
    const fetch = async (url: unknown, init?: RequestInit) => {
        requests.push({ url: String(url), metadata: JSON.parse(init!.body as string).metadata })
        return Response.json({
            websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
            homeRegion: "north-america-east",
            connectByMs: 1000
        })
    }
    const options = { projectId: "default", controlPlaneUrl: "https://actors.example.com", apiKey: "secret" }
    const proxy = new ActorProxy(options, { fetch })
    for (const authorization of [
        { actorName: "Room", actorId: "one", metadata: { userId: "alice" } },
        { actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "editor" } }
    ])
        assert.equal((await proxy.handle(authorization)).websocketUrl, "wss://actors.example.com/v1/socket?key=ticket")
    assert.deepEqual(
        requests.map(request => request.metadata),
        [{ userId: "alice" }, { tenantId: 1, role: "editor" }]
    )
    assert.match(requests[1]!.url, /actors\/Counter\/one\/find-websocket$/)
    for (const authorization of [
        { actorName: "Room", actorId: "one", metadata: {} },
        { actorName: "Room", actorId: "one", metadata: { userId: 1 } },
        { actorName: "Room", actorId: "one", metadata: { userId: "alice", profile: { displayName: 1 } } },
        { actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "admin" } }
    ])
        assert.equal((await proxy.handle(authorization)).websocketUrl, "wss://actors.example.com/v1/socket?key=ticket")
    for (const authorization of [
        { actorName: "Missing", actorId: "one", metadata: {} },
        { actorName: "toString", actorId: "one", metadata: {} }
    ])
        await assert.rejects(proxy.handle(authorization), /metadata|actor name/i)
    assert.equal(requests.length, 6, "unknown actors must fail before issuing a ticket")
    const originalFetch = globalThis.fetch
    globalThis.fetch = fetch
    t.after(() => {
        globalThis.fetch = originalFetch
    })
    const original = {
        project: process.env.DURABLE_ACTORS_PROJECT_ID,
        url: process.env.DURABLE_ACTORS_CONTROL_PLANE_URL,
        key: process.env.DURABLE_ACTORS_SECRET
    }
    t.after(() => {
        for (const [key, value] of Object.entries({
            DURABLE_ACTORS_PROJECT_ID: original.project,
            DURABLE_ACTORS_CONTROL_PLANE_URL: original.url,
            DURABLE_ACTORS_SECRET: original.key
        })) {
            if (value === undefined) delete process.env[key]
            else process.env[key] = value
        }
    })
    process.env.DURABLE_ACTORS_PROJECT_ID = options.projectId
    process.env.DURABLE_ACTORS_CONTROL_PLANE_URL = options.controlPlaneUrl
    process.env.DURABLE_ACTORS_SECRET = options.apiKey
    assert.deepEqual(await actors.Room.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } }), {
        websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
        homeRegion: "north-america-east",
        connectByMs: 1000
    })
    assert.equal(
        requests.at(-1)!.url,
        "https://actors.example.com/v1/projects/default/actors/Room/lobby/find-websocket"
    )
    const issued: { url: string; headers: Headers; body: unknown }[] = []
    await actors.Counter.prepareWebsocket(
        { actorId: "one", metadata: { tenantId: 1, role: "viewer" } },
        { ...options },
        {
            fetch: async (url: unknown, init: RequestInit) => {
                issued.push({
                    url: String(url),
                    headers: new Headers(init.headers),
                    body: JSON.parse(init.body as string)
                })
                return Response.json({
                    websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
                    homeRegion: "north-america-east",
                    connectByMs: 1000
                })
            }
        }
    )
    assert.equal(issued[0]!.url, "https://actors.example.com/v1/projects/default/actors/Counter/one/find-websocket")
    assert.equal(issued[0]!.headers.get("authorization"), "Bearer secret")
    assert.deepEqual(issued[0]!.body, {
        metadata: { tenantId: 1, role: "viewer" }
    })
    await assert.rejects(
        actors.Room.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } }, options, {
            fetch: async () => new Response(null, { status: 403 })
        }),
        /HTTP 403/
    )
    assert.equal(
        (await ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: { userId: "alice" } })).websocketUrl,
        "wss://actors.example.com/v1/socket?key=ticket"
    )
})

test("generates backend contracts for actors without outgoing application messages", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-never-"))
    try {
        const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
        await generateClient(
            {
                version: 1,
                typescript: declarations({
                    Counter: "Metadata: {}; Incoming: string; Outgoing: never; State: {}; Methods: {}"
                }),
                actors: [
                    {
                        actorName: "Counter",
                        socket: {
                            version: 1,
                            actorName: "Counter",
                            emittable: [],
                            schema: {
                                definitions: {
                                    Metadata: { type: "object" },
                                    Incoming: { type: "string" },
                                    Outgoing: false,
                                    State: { type: "object" }
                                }
                            }
                        },
                        rpc: { schema: { definitions: {} }, methods: [] }
                    }
                ]
            },
            directory
        )
        assert.match(await readFile(path.join(directory, "types.d.ts"), "utf8"), /Outgoing: never/)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test("actor names cannot collide with generated entrypoint or helper bindings", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-names-"))
    try {
        const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
        const names = [
            "actors",
            "ActorProxy",
            "ActorAuthorization",
            "createActorTransport",
            "ActorInvocationError",
            "ActorRpcTransport",
            "DurableActorsClientOptions",
            "SocketGrant",
            "SocketProxyDependencies",
            "SocketProxyOptions",
            "ProxyActor",
            "Connection",
            "Authorization",
            "Metadata",
            "Incoming",
            "Outgoing",
            "State",
            "Methods",
            "Stub"
        ]
        await generateClient(
            {
                version: 1,
                typescript: declarations(
                    Object.fromEntries(
                        names.map(name => [
                            name,
                            "Metadata: {}; Incoming: string; Outgoing: string; State: {}; Methods: {}"
                        ])
                    )
                ),
                actors: names.map(actorName => ({
                    actorName,
                    socket: {
                        version: 1,
                        actorName,
                        emittable: [],
                        schema: {
                            definitions: {
                                Metadata: { type: "object" },
                                Incoming: { type: "string" },
                                Outgoing: { type: "string" },
                                State: { type: "object" }
                            }
                        }
                    },
                    rpc: { schema: { definitions: {} }, methods: [] }
                }))
            },
            directory
        )
        checkTypes(path.join(directory, "index.d.ts"))
        await build({
            entryPoints: [path.join(directory, "index.js")],
            bundle: true,
            platform: "browser",
            format: "esm",
            write: false,
            logLevel: "silent"
        })
        await build({
            entryPoints: [path.join(directory, "index.js")],
            bundle: true,
            platform: "node",
            format: "esm",
            write: false,
            logLevel: "silent"
        })
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test("generates typed descriptors that can be bundled for browsers", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-client-"))
    try {
        const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
        await generateClient(
            {
                version: 1,
                typescript: declarations({
                    Room: "Metadata: {}; Incoming: { amount: number }; Outgoing: string; State: { count: number }; Methods: {}"
                }),
                actors: [
                    {
                        actorName: "Room",
                        socket: {
                            version: 1,
                            actorName: "Room",
                            emittable: ["count"],
                            schema: {
                                definitions: {
                                    Metadata: { type: "object" },
                                    Incoming: {
                                        type: "object",
                                        properties: { amount: { type: "number" } },
                                        required: ["amount"]
                                    },
                                    Outgoing: { type: "string" },
                                    State: {
                                        type: "object",
                                        properties: { count: { type: "number" } },
                                        required: ["count"]
                                    }
                                }
                            }
                        },
                        rpc: { schema: { definitions: {} }, methods: [] }
                    }
                ]
            },
            directory
        )
        const source = await readFile(path.join(directory, "index.d.ts"), "utf8")
        assert.match(await readFile(path.join(directory, "types.d.ts"), "utf8"), /amount: number/)
        assert.doesNotMatch(source, /node:|\/host|actor-compiler/)
        assert.deepEqual((await readdir(directory)).sort(), [
            "index.d.ts",
            "index.js",
            "package.json",
            "runtime",
            "types.d.ts"
        ])
        assert.match(await readFile(path.join(directory, "index.d.ts"), "utf8"), /export declare const actors/)
        const consumer = path.join(directory, "consumer.ts")
        await writeFile(
            consumer,
            `import { actors } from "./index.js"
            const metadata: actors.Room.Metadata = {}
            const incoming: actors.Room.Incoming = { amount: 1 }
            const state: actors.Room.State = { count: 1 }
            actors.Room.prepareWebsocket({ actorId: "lobby", metadata })
            // @ts-expect-error wrong incoming message
            const invalid: actors.Room.Incoming = { amount: "invalid" }
            // @ts-expect-error private field is absent
            state.secret
            // @ts-expect-error the contract declares no RPC methods
            actors.Room.get("lobby").missingMethod()`
        )
        checkTypes(consumer)
        const bundle = await build({
            entryPoints: [path.join(directory, "index.js")],
            bundle: true,
            platform: "browser",
            format: "esm",
            write: false,
            metafile: true
        })
        assert.equal(
            Object.keys(bundle.metafile!.inputs).some(file =>
                /\/host\/|\/compiler\/|\/client\/|proxy|node:|ajv|validators/.test(file)
            ),
            false
        )
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test("each actor module exposes complete unprefixed contract types", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-readable-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient(
        {
            version: 1,
            typescript: declarations(
                Object.fromEntries(
                    ["Counter", "Room"].map(name => [
                        name,
                        "Metadata: { userId: string }; Incoming: { by: number }; Outgoing: { count: number }; State: { count: number }; Methods: {}"
                    ])
                )
            ),
            actors: ["Counter", "Room"].map(actorName => ({
                actorName,
                socket: {
                    version: 1 as const,
                    actorName,
                    emittable: ["count"],
                    schema: {
                        definitions: {
                            Metadata: {
                                type: "object",
                                properties: { userId: { type: "string" } },
                                required: ["userId"]
                            },
                            Incoming: { type: "object", properties: { by: { type: "number" } }, required: ["by"] },
                            Outgoing: {
                                type: "object",
                                properties: { count: { type: "number" } },
                                required: ["count"]
                            },
                            Field_count: { type: "number" },
                            State: {
                                type: "object",
                                properties: { count: { $ref: "#/definitions/Field_count" } },
                                required: ["count"]
                            }
                        }
                    }
                },
                rpc: { schema: { definitions: {} }, methods: [] }
            }))
        },
        directory
    )
    const source = await readFile(path.join(directory, "index.d.ts"), "utf8")
    for (const name of ["Counter", "Room"]) assert.ok(source.includes(`namespace ${name} {`))
    for (const type of ["Metadata", "Incoming", "Outgoing", "State"]) assert.ok(source.includes(`type ${type} =`))
    assert.match(await readFile(path.join(directory, "types.d.ts"), "utf8"), /count: number/)
    assert.doesNotMatch(source, /ActorNames|FieldCount/)
    assert.match(source, /metadata: Metadata/)
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `
        import type { actors } from "./index.js"
        type Metadata = actors.Counter.Metadata
        type Incoming = actors.Counter.Incoming
        type Outgoing = actors.Counter.Outgoing
        type State = actors.Counter.State
        type RoomMetadata = actors.Room.Metadata
        type Authorization = actors.Room.Authorization
        const metadata: Metadata = { userId: "alice" }
        const other: RoomMetadata = metadata
        const incoming: Incoming = { by: 1 }
        const outgoing: Outgoing = { count: 1 }
        const state: State = outgoing
        const authorization: Authorization = { actorName: "Room", actorId: "lobby", metadata: other }
        // @ts-expect-error invalid metadata
        const invalid: Metadata = { userId: 1 }
        // @ts-expect-error wrong actor identity
        const wrong: Authorization = { actorName: "Counter", actorId: "one", metadata }
    `
    )
    checkTypes(consumer)
})

test("readable contract types preserve recursive metadata and helper-name collisions", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-recursive-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient(
        {
            version: 1,
            typescript: {
                declarations: `interface Connection { id: string; parent?: Connection }
                export interface ActorTypes {
                    Room: { Metadata: { connection: Connection }; Incoming: null; Outgoing: unknown; State: {}; Methods: {} }
                }`,
                dependencies: {}
            },
            actors: [
                {
                    actorName: "Room",
                    socket: {
                        version: 1,
                        actorName: "Room",
                        emittable: [],
                        schema: {
                            definitions: {
                                Metadata: {
                                    type: "object",
                                    properties: { connection: { $ref: "#/definitions/Connection" } },
                                    required: ["connection"]
                                },
                                Connection: {
                                    type: "object",
                                    properties: {
                                        parent: { $ref: "#/definitions/Connection" },
                                        id: { type: "string" }
                                    },
                                    required: ["id"]
                                },
                                Incoming: { type: "null" },
                                Outgoing: true,
                                State: { type: "object" }
                            }
                        }
                    },
                    rpc: { schema: { definitions: {} }, methods: [] }
                }
            ]
        },
        directory
    )
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `
        import type { actors } from "./index.js"
        type Metadata = actors.Room.Metadata
        type Incoming = actors.Room.Incoming
        type Outgoing = actors.Room.Outgoing
        type Authorization = actors.Room.Authorization
        const metadata: Metadata = { connection: { id: "a", parent: { id: "b", parent: { id: "c" } } } }
        const authorization: Authorization = { actorName: "Room", actorId: "one", metadata }
        const incoming: Incoming = null
        const outgoing: Outgoing = { anything: true }
        // @ts-expect-error recursive metadata keeps its required fields
        const invalid: Metadata = { connection: { id: "a", parent: {} } }
        // @ts-expect-error null input accepts no payload
        const wrong: Incoming = "wrong"
    `
    )
    checkTypes(consumer)
})

function checkTypes(consumer: string): void {
    const options: ts.CompilerOptions = {
        strict: true,
        noEmit: true,
        skipLibCheck: false,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        types: []
    }
    const program = ts.createProgram([consumer], options)
    assert.deepEqual(
        ts
            .getPreEmitDiagnostics(program)
            .map(diagnostic => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")),
        []
    )
}

test("regeneration replaces managed runtime files and preserves application files", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-regenerate-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient({ version: 1, actors: [], typescript: declarations({}) }, directory)
    const runtime = path.join(directory, "runtime")
    const files = await readdir(runtime)
    await writeFile(path.join(runtime, "stale.js"), "throw new Error('stale runtime')")
    await writeFile(path.join(runtime, "client.js"), "throw new Error('edited runtime')")
    await writeFile(path.join(directory, "backend.ts"), "export const application = true")
    await generateClient({ version: 1, actors: [], typescript: declarations({}) }, directory)
    assert.deepEqual(await readdir(runtime), files)
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        'import { actors, ActorProxy } from "./index.js"; const proxy: typeof ActorProxy = ActorProxy'
    )
    checkTypes(consumer)
    assert.match(await readFile(path.join(runtime, "client.js"), "utf8"), /class HttpActorClient/)
    assert.equal(await readFile(path.join(directory, "backend.ts"), "utf8"), "export const application = true")
})
