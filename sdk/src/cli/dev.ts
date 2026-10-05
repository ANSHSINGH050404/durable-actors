import { Command, InvalidArgumentError, Option } from "commander"
import { realpath, stat } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { z } from "zod"

import { projectIdSchema } from "../actor/identity.js"
import { configuredSettings } from "../client/clientSettings.js"
import { projectSdkModule } from "../projectSdk.js"
import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { type ActorSourceWatcher, watchActorSources } from "./actor-source-watcher.js"
import { ControlPlaneClient } from "./control-plane.js"
import { checkPython, pythonExecutable } from "./python.js"
import { runtimeConnection, runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

interface DevOptions {
    /** Module URL from which to resolve the SDK dependency when embedded in a wrapper. */
    sdkResolveFrom?: string
    projectId: string
    apiKey?: string
    port: number
    project: string
    entrypoint: string
    storage: "local" | "gcs"
    dataDir?: string
    watch: boolean
}

export function registerDevCommand(program: Command): void {
    program
        .command("dev")
        .description("Run local actors and reload code changes")
        .option("--no-watch", "disable automatic code reload")
        .addOption(
            new Option("--port <number>", "localhost port (0 selects a free port)")
                .env("DURABLE_ACTORS_PORT")
                .argParser(portNumber)
                .default(7100)
        )
        .addHelpText(
            "after",
            `
Run from your actor project directory; dev loads src/actors.ts, or src/actors.py when only it exists.
No configuration is required. Optional overrides in .env:
  DURABLE_ACTORS_PROJECT     project directory (default: current directory)
  DURABLE_ACTORS_ENTRYPOINT  actor source file, relative to the project (default: src/actors.ts or src/actors.py)

Python projects: durable-actors init my-project --template python
DURABLE_ACTORS_PYTHON selects an interpreter; otherwise dev uses the project .venv.

Create a project with: durable-actors init my-project`
        )
        .action(async (options: { watch: boolean; port: number }) => {
            process.exitCode = await runDev(await developmentOptions(options, process.env))
        })
}

async function developmentOptions(
    options: { watch: boolean; port: number },
    environment: NodeJS.ProcessEnv
): Promise<DevOptions> {
    const env = developmentEnvironment.parse(environment)
    return {
        ...options,
        projectId: env.DURABLE_ACTORS_PROJECT_ID,
        apiKey: env.DURABLE_ACTORS_SECRET,
        project: env.DURABLE_ACTORS_PROJECT,
        entrypoint: env.DURABLE_ACTORS_ENTRYPOINT ?? (await defaultEntrypoint(env.DURABLE_ACTORS_PROJECT)),
        dataDir: env.DURABLE_ACTORS_DATA_DIR,
        storage: env.DURABLE_ACTORS_STORAGE
    }
}

async function defaultEntrypoint(project: string): Promise<string> {
    for (const entrypoint of DEFAULT_ENTRYPOINTS)
        if ((await developmentPathStats(path.resolve(project, entrypoint)))?.isFile()) return entrypoint
    return DEFAULT_ENTRYPOINTS[0]
}

const DEFAULT_ENTRYPOINTS = ["src/actors.ts", "src/actors.py"] as const

function portNumber(value: string): number {
    const port = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isSafeInteger(port) || port < 0 || port > 65535)
        throw new InvalidArgumentError("Port must be an integer from 0 to 65535.")
    return port
}

const developmentEnvironment = z.object({
    DURABLE_ACTORS_PROJECT_ID: projectIdSchema.default("local"),
    DURABLE_ACTORS_SECRET: z.string().optional(),
    DURABLE_ACTORS_PROJECT: z.string().min(1).default("."),
    DURABLE_ACTORS_ENTRYPOINT: z.string().min(1).optional(),
    DURABLE_ACTORS_DATA_DIR: z.string().min(1).optional(),
    DURABLE_ACTORS_STORAGE: z.enum(["local", "gcs"]).default("local")
})

async function runDev(options: DevOptions): Promise<number> {
    const project = await developmentProject(options)
    if (options.entrypoint.endsWith(".py")) return runDevRuntime(options, project)
    const local = await projectSdkModule(project, "./cli/dev.js", import.meta.url, options.sdkResolveFrom)
    if (local !== undefined) return (await import(local)).runDev({ ...options, project })
    return runDevRuntime(options, project)
}

async function developmentProject(options: DevOptions): Promise<string> {
    const directory = path.resolve(options.project)
    if (!(await developmentPathStats(directory))?.isDirectory())
        throw new Error(
            `No actor project directory found at ${directory}.\n` +
                "Run from your actor project directory, or set DURABLE_ACTORS_PROJECT in .env.\n" +
                "Create a project with: durable-actors init my-project"
        )
    const project = await realpath(directory)
    const entrypoint = path.resolve(project, options.entrypoint)
    if (!(await developmentPathStats(entrypoint))?.isFile())
        throw new Error(
            `No actor source file found at ${entrypoint}.\n` +
                "Run from your actor project directory, or set DURABLE_ACTORS_PROJECT and DURABLE_ACTORS_ENTRYPOINT in .env.\n" +
                "Create a project with: durable-actors init my-project"
        )
    return project
}

async function developmentPathStats(candidate: string) {
    return stat(candidate).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT" || error.code === "ENOTDIR") return undefined
        throw error
    })
}

async function runDevRuntime(options: DevOptions, project: string): Promise<number> {
    const python = options.entrypoint.endsWith(".py") ? await pythonExecutable(project) : undefined
    if (python) await checkPython(project, [options.entrypoint], python)
    const executable = await fetchRuntimeExecutablePath()
    const runtime = startRustRuntime(
        executable,
        [...devArguments(options), "--ready-fd", "3"],
        { ...runtimeEnvironment(executable), ...(python ? { DURABLE_ACTORS_PYTHON: python } : {}) },
        true,
        true
    )
    const connection = runtimeConnection(runtime.readiness!, runtime.exited)
    const settings = connection.then(value =>
        configuredSettings(
            z
                .object({
                    projectId: z.string(),
                    controlPlaneUrl: z.string(),
                    apiKey: z
                        .string()
                        .nullish()
                        .transform(value => value ?? undefined)
                })
                .parse(value)
        )
    )
    const client = settings.then(settings => new ControlPlaneClient(settings, fetch))
    void client.catch(() => {})
    let watcher: ActorSourceWatcher | undefined
    try {
        if (options.watch) {
            watcher = await watchActorSources({ projectDirectory: project, dataDirectory: options.dataDir }, async () =>
                publishLocalCode(options, project, await client)
            )
        }
        await client
        return await runtime.exited
    } catch (error) {
        runtime.child.kill("SIGTERM")
        await runtime.exited.catch(() => {})
        throw error
    } finally {
        await watcher?.close()
    }
}

async function publishLocalCode(
    options: DevOptions,
    project: string,
    client: Pick<ControlPlaneClient, "registerDeployment">
): Promise<void> {
    if (options.entrypoint.endsWith(".py")) await checkPython(project, [options.entrypoint])
    await client.registerDeployment({
        localSource: { workingDirectory: project, actorEntrypoint: options.entrypoint },
        secretRefs: []
    })
    console.log("Updated local actors.")
}

function devArguments(options: DevOptions): string[] {
    const args = [
        "dev",
        "--project-id",
        options.projectId,
        "--project",
        options.project,
        "--port",
        String(options.port),
        "--entrypoint",
        options.entrypoint,
        "--storage",
        options.storage,
        "--sdk-host",
        fileURLToPath(new URL("../host.js", import.meta.url))
    ]
    if (options.apiKey) args.push("--api-key", options.apiKey)
    if (options.dataDir) args.push("--data-dir", options.dataDir)
    return args
}

export { type DevOptions, runDev }
