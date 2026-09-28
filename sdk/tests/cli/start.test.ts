import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))
const cli = path.join(sdk, "dist/cli.js")

test("start launches the configured runtime without a local actor project and propagates its exit code", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-start-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const executable = path.join(directory, "runtime.mjs")
    await writeFile(
        executable,
        `#!/usr/bin/env node
console.log(JSON.stringify({ args: process.argv.slice(2), role: process.env.DURABLE_ACTORS_PROCESS_ROLE }))
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = {
        ...process.env,
        DURABLE_ACTORS_BINARY: executable,
        DURABLE_ACTORS_PROJECT_ID: "",
        DURABLE_ACTORS_PROJECT: path.join(directory, "missing-project"),
        DURABLE_ACTORS_PROCESS_ROLE: "control_plane"
    }
    const { stdout } = await run(process.execPath, [cli, "start"], { cwd: directory, env })
    assert.deepEqual(JSON.parse(stdout), { args: [], role: "control_plane" })
    await assert.rejects(
        run(process.execPath, [cli, "start"], { cwd: directory, env: { ...env, TEST_RUNTIME_EXIT_CODE: "7" } }),
        { code: 7 }
    )
})

test("dev validates its configured storage and port before launching", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-dev-options-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    for (const settings of [{ DURABLE_ACTORS_STORAGE: "invalid" }, { DURABLE_ACTORS_PORT: "65536" }])
        await assert.rejects(
            run(process.execPath, [cli, "dev"], {
                cwd: directory,
                env: { ...process.env, DURABLE_ACTORS_BINARY: "missing-runtime", ...settings }
            }),
            /DURABLE_ACTORS_STORAGE|Port must be an integer/
        )
})

test("dev passes actor sources to the runtime and redeploys watched changes", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-dev-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "actor project")
    await mkdir(path.join(project, "node_modules"), { recursive: true })
    await symlink(sdk, path.join(project, "node_modules/durable-actors"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    const source = path.join(project, "actors.ts")
    await writeFile(
        source,
        'import { Actor } from "durable-actors"; export class Room extends Actor { async hello(): Promise<string> { return "hi" } }\nthrow new Error("must not execute actor source")'
    )
    const executable = path.join(directory, "runtime.mjs")
    await writeFile(
        executable,
        `#!/usr/bin/env node
import { createWriteStream, appendFileSync } from "node:fs"
import { createServer } from "node:http"
const args = process.argv.slice(2)
const updates = []
const server = createServer(async (request, response) => {
    let body = ""
    for await (const chunk of request) body += chunk
    updates.push({ method: request.method, path: request.url, authorization: request.headers.authorization ?? null, body: JSON.parse(body) })
    response.end("{}")
})
let controlPlaneUrl = "http://127.0.0.1:7100"
if (process.env.TEST_WATCH_SOURCE) {
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve))
    controlPlaneUrl = "http://127.0.0.1:" + server.address().port
}
createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    projectId: process.env.DURABLE_ACTORS_PROJECT_ID ?? "local",
    pid: process.pid,
    controlPlaneUrl,
    apiKey: process.env.DURABLE_ACTORS_SECRET ?? null,
    storageRegion: "local"
}))
if (process.env.TEST_WATCH_SOURCE) {
    setTimeout(() => appendFileSync(process.env.TEST_WATCH_SOURCE, "\\n// source changed"), 400)
    setTimeout(() => { server.close(); console.log(JSON.stringify({ updates })) }, 2500)
} else {
    console.log(JSON.stringify({ args }))
}
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_BINARY: executable,
        DURABLE_ACTORS_SECRET: "test-key",
        DURABLE_ACTORS_PROJECT: project,
        DURABLE_ACTORS_ENTRYPOINT: "actors.ts"
    }
    const args = [cli, "dev", "--port", "0"]
    const { stdout } = await run(process.execPath, args, { cwd: directory, env })
    const result = JSON.parse(stdout)
    assert.equal(result.args[result.args.indexOf("--project") + 1], project)
    assert.equal(result.args[result.args.indexOf("--entrypoint") + 1], "actors.ts")
    const sdkHostIndex = result.args.indexOf("--sdk-host")
    assert.notEqual(sdkHostIndex, -1, "development mode must provide its host module")
    assert.equal(result.args[sdkHostIndex + 1], path.join(sdk, "dist/host.js"))

    const { DURABLE_ACTORS_PROJECT_ID, ...withoutProjectId } = env
    const local = await run(process.execPath, args, { cwd: directory, env: withoutProjectId })
    const localArgs = JSON.parse(local.stdout).args
    assert.equal(localArgs[localArgs.indexOf("--project-id") + 1], "local")
    const configuredArgs = result.args
    assert.equal(configuredArgs[configuredArgs.indexOf("--project-id") + 1], "default")

    const failure = await run(process.execPath, args, {
        cwd: directory,
        env: { ...env, TEST_RUNTIME_EXIT_CODE: "7" }
    }).then(
        () => assert.fail("runtime failure should propagate"),
        (error: Error & { code: number; stdout: string }) => error
    )
    assert.equal(failure.code, 7)
    const watching = await run(process.execPath, args, {
        cwd: directory,
        env: { ...env, TEST_WATCH_SOURCE: source }
    })
    const updates = JSON.parse(watching.stdout.trim().split("\n").at(-1)!).updates
    assert.ok(updates.length > 0, "development mode watches sources by default")
    assert.deepEqual(updates[0], {
        method: "PUT",
        path: "/v1/projects/default/deployment",
        authorization: "Bearer test-key",
        body: {
            imageRef: "local",
            workingDirectory: await realpath(project),
            actorEntrypoint: "actors.ts",
            secretRefs: []
        }
    })
    const unauthenticated = await run(process.execPath, args, {
        cwd: directory,
        env: {
            ...env,
            DURABLE_ACTORS_PROJECT_ID: undefined,
            DURABLE_ACTORS_SECRET: undefined,
            DURABLE_ACTORS_API_KEY: undefined,
            TEST_WATCH_SOURCE: source
        }
    })
    const localUpdates = JSON.parse(unauthenticated.stdout.trim().split("\n").at(-1)!).updates
    assert.ok(localUpdates.length > 0)
    assert.equal(localUpdates[0].path, "/v1/projects/local/deployment")
    assert.equal(localUpdates[0].authorization, null)
    const notWatching = await run(process.execPath, [...args, "--no-watch"], {
        cwd: directory,
        env: { ...env, TEST_WATCH_SOURCE: source }
    })
    assert.deepEqual(JSON.parse(notWatching.stdout).updates, [], "--no-watch prevents source-triggered redeployments")

    const watchLimit = path.join(directory, "watch-limit.mjs")
    await writeFile(
        watchLimit,
        `import fs from "node:fs"
import { syncBuiltinESMExports } from "node:module"
fs.watch = () => { throw Object.assign(new Error("too many open files"), { code: "EMFILE" }) }
syncBuiltinESMExports()
`
    )
    const limited = await run(process.execPath, ["--import", watchLimit, ...args], {
        cwd: directory,
        env: { ...env, TEST_WATCH_SOURCE: source }
    })
    assert.deepEqual(JSON.parse(limited.stdout).updates, [], "watch exhaustion must keep the runtime running")
    assert.match(limited.stderr, /EMFILE.*Automatic reload is disabled/)
})
