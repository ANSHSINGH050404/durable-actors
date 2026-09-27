import { build } from "esbuild"
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { pathToFileURL } from "node:url"
import { promisify } from "node:util"
import ts from "typescript"

import { generateClient } from "../../dist/compiler/generators/client-generator.js"

const run = promisify(execFile)

test("generated clients typecheck and run with or without bundling in an application with no dependencies", { timeout: 30_000 }, async t => {
    const directory = await standaloneProject(t)
    await checkArtifacts(directory)
    await checkTypes(directory)
    await checkServerCalls(t, directory)
    await checkBrowser(directory)
})

test("a generated client rediscovers a retired tunnel before sending the next mutation", { timeout: 30_000 }, async t => {
    const directory = await standaloneProject(t)
    const calls = []
    const oldHost = actorHost(calls)
    const newHost = actorHost(calls)
    for (const host of [oldHost, newHost]) {
        t.after(() => host.close())
        host.listen(0, "127.0.0.1")
        await once(host, "listening")
    }
    let port = oldHost.address().port
    const requests = []
    const origin = await controlPlaneServer(t, () => port, requests)
    const clientPath = path.join(directory, "generated/index.js")
    const { actors, createActorTransport } = await import(pathToFileURL(clientPath).href)
    const transport = createActorTransport({ projectId: "team-a", apiKey: "app-key", controlPlaneUrl: origin })
    const room = actors.ChatRoom.get("lobby", transport)
    assert.deepEqual(await room.sendMessage({ text: "hello" }), { id: "1", text: "hello" })
    const retiredRequests = []
    oldHost.removeAllListeners("request")
    oldHost.on("request", request => {
        retiredRequests.push(request.method)
        request.socket.destroy()
    })
    port = newHost.address().port
    assert.deepEqual(await room.sendMessage({ text: "hello" }), { id: "1", text: "hello" })
    assert.deepEqual(calls, ["sendMessage", "sendMessage"])
    assert.deepEqual(retiredRequests, ["HEAD"])
    assert.equal(requests.length, 2)
})

async function standaloneProject(t) {
    const directory = await mkdtemp(path.join(os.tmpdir(), "standalone-actors-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await writeFile(path.join(directory, "package.json"), '{"type":"module"}')
    const contract = JSON.parse(await readFile(new URL("../fixtures/public-contract.json", import.meta.url), "utf8"))
    await generateClient(contract, path.join(directory, "generated"))
    return directory
}

async function checkArtifacts(directory) {
    const generated = path.join(directory, "generated")
    assert.deepEqual((await readdir(generated)).sort(), ["index.d.ts", "index.js", "package.json", "runtime", "types.d.ts"])
    const files = await readdir(path.join(generated, "runtime"))
    for (const file of files) {
        if (file.endsWith(".js")) assert.ok(files.includes(file.replace(/\.js$/, ".d.ts")), file)
        else assert.ok(file.endsWith(".d.ts") || ["LICENSE.md", "package.json"].includes(file), file)
    }
}

async function checkTypes(directory) {
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `
        import { actors, createActorTransport, createActorSessionTransport, ActorSessionRejectedError, ActorInvocationError, type ActorSession, type SocketGrant } from "./generated/index.js"
        const transport = createActorTransport({ controlPlaneUrl: "http://localhost:7100" })
        const room = actors.ChatRoom.get("lobby", transport)
        const message: Promise<{ id: string; text: string }> = room.sendMessage({ text: "hello" })
        const grant: Promise<SocketGrant> = actors.ChatRoom.prepareWebsocket({ actorId: "lobby", metadata: {} })
        // @ts-expect-error the generated stub preserves argument types
        room.sendMessage({ text: 42 })
        const error: string = new ActorInvocationError("actor_error", "request", "failed").code
        const session: ActorSession = { projectId: "team-a", token: "session", controlPlaneUrl: "https://actors.example", expiresAtMs: Date.now() + 60000 }
        const sessions = createActorSessionTransport({ projectId: "team-a", getSession: async () => session })
        sessions.dispose()
        const denial: Error = new ActorSessionRejectedError("access revoked")
    `
    )
    const program = ts.createProgram([consumer], {
        strict: true,
        noEmit: true,
        skipLibCheck: false,
        types: [],
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext
    })
    assert.deepEqual(
        ts.getPreEmitDiagnostics(program).map(d => ts.flattenDiagnosticMessageText(d.messageText, "\n")),
        []
    )
}

async function checkServerCalls(t, directory) {
    const calls = []
    const host = actorHost(calls)
    t.after(() => host.close())
    host.listen(0, "127.0.0.1")
    await once(host, "listening")
    const port = host.address().port
    const requests = []
    const origin = await controlPlaneServer(t, () => port, requests)
    for (const format of ["node", "esm", "cjs", "commonjs-project"]) {
        if (format === "commonjs-project") await writeFile(path.join(directory, "package.json"), '{"type":"commonjs"}')
        calls.length = 0
        requests.length = 0
        await invokeClient(directory, origin, format)
        assert.deepEqual(calls, ["sendMessage", "clear", "sendMessage"])
        assert.deepEqual(
            requests.map(request => request.url),
            ["/v1/projects/team-a/actors/ChatRoom/lobby/find-actor", "/v1/projects/team-a/actors/ChatRoom/lobby/find-websocket"]
        )
        assert.deepEqual(requests[1].body, { metadata: { userId: "alice" }, authorizationLifetimeMs: 900000 })
    }
}

async function controlPlaneServer(t, port, requests) {
    const controlPlane = createServer(async (request, response) => {
        assert.equal(request.headers.authorization, "Bearer app-key")
        const chunks = []
        for await (const chunk of request) chunks.push(chunk)
        requests.push({ url: request.url, body: JSON.parse(Buffer.concat(chunks).toString()) })
        response.setHeader("content-type", "application/json")
        response.end(
            JSON.stringify(
                request.url.endsWith("/find-actor")
                    ? { route: `http://127.0.0.1:${port()}`, token: "host-ticket", ownerEpoch: 3, expiresAtMs: Date.now() + 60_000 }
                    : { websocketUrl: "wss://example.com/socket?key=ticket", homeRegion: "us-east", connectByMs: 1000, authorizedUntilMs: 900000 }
            )
        )
    })
    t.after(() => controlPlane.close())
    controlPlane.listen(0, "127.0.0.1")
    await once(controlPlane, "listening")
    return `http://127.0.0.1:${controlPlane.address().port}`
}

async function invokeClient(directory, origin, format) {
    const bundled = format === "esm" || format === "cjs"
    const file = bundled ? `client.${format === "cjs" ? "cjs" : "mjs"}` : "generated/index.js"
    if (bundled)
        await build({
            entryPoints: [path.join(directory, "generated/index.js")],
            outfile: path.join(directory, file),
            bundle: true,
            platform: "node",
            format: format === "cjs" ? "cjs" : "esm",
            logLevel: "silent"
        })
    const script = path.join(directory, "invoke.mjs")
    await writeFile(
        script,
        `
        import assert from "node:assert/strict"
        import { actors, createActorSessionTransport, ActorInvocationError } from ${JSON.stringify(`./${file}`)}
        let exchanges = 0
        const transport = createActorSessionTransport({ projectId: "team-a", getSession: async () => {
            exchanges++
            return { projectId: "team-a", token: "app-key", controlPlaneUrl: ${JSON.stringify(origin)}, expiresAtMs: Date.now() + 60000 }
        } })
        const room = actors.ChatRoom.get("lobby", transport)
        const input = { text: "hello" }
        assert.deepEqual(await room.sendMessage(input), { id: "1", text: "hello" })
        assert.equal(await room.clear(), undefined)
        await assert.rejects(room.sendMessage({ text: "fail" }), error => error instanceof ActorInvocationError && error.code === "actor_error")
        const grant = await actors.ChatRoom.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } })
        assert.equal(grant.websocketUrl, "wss://example.com/socket?key=ticket")
        assert.equal(exchanges, 1)
        transport.dispose()
    `
    )
    await run(process.execPath, [script], {
        cwd: directory,
        timeout: 20_000,
        env: {
            ...process.env,
            NODE_PATH: "",
            DURABLE_ACTORS_PROJECT_ID: "team-a",
            DURABLE_ACTORS_CONTROL_PLANE_URL: origin,
            DURABLE_ACTORS_SECRET: "app-key"
        }
    })
}

async function checkBrowser(directory) {
    const browser = await build({ entryPoints: [path.join(directory, "generated/index.js")], bundle: true, platform: "browser", format: "esm", write: false, logLevel: "silent" })
    const module = await import(`data:text/javascript;base64,${Buffer.from(browser.outputFiles[0].text).toString("base64")}`)
    assert.throws(() => module.actors.ChatRoom.get("lobby"), /server/)
    assert.throws(() => new module.ActorProxy(), /server/)
    assert.throws(() => module.createActorTransport({ controlPlaneUrl: "http://localhost:7100" }), /server/)
    assert.throws(() => module.createActorSessionTransport({ projectId: "team-a", getSession: async () => null }), /server/)
    assert.equal(new module.ActorSessionRejectedError("access revoked").name, "ActorSessionRejectedError")
    assert.ok(browser.outputFiles[0].contents.length < 10_000)
}

function actorHost(calls) {
    return createServer(async (request, response) => {
        assert.equal(request.headers.authorization, "Bearer host-ticket")
        assert.equal(request.url, "/v1/projects/team-a/actors/ChatRoom/lobby/invoke")
        if (request.method === "HEAD") {
            response.writeHead(204).end()
            return
        }
        const chunks = []
        for await (const chunk of request) chunks.push(chunk)
        const body = JSON.parse(Buffer.concat(chunks).toString())
        assert.equal(body.ownerEpoch, 3)
        assert.ok(body.requestId)
        const { method, args } = body
        calls.push(method)
        response.setHeader("content-type", "application/json")
        response.end(
            JSON.stringify(
                args[0]?.text === "fail" ? { type: "failed", code: "actor_error", message: "failed" } : { type: "completed", result: method === "clear" ? null : { id: "1", text: "hello" } }
            )
        )
    })
}
