import { Command, Option } from "commander"
import path from "node:path"
import { z } from "zod"

import { connectionHelp } from "./connection.js"
import { createControlPlaneClient } from "./control-plane.js"
import { compilePythonContract, generatePythonClient } from "./python.js"

interface GenerateOptions {
    language?: "python" | "typescript"
    outDir: string
    config?: string
    controlPlaneUrl?: string
}

function registerGenerateCommand(program: Command): void {
    program
        .command("generate")
        .argument("[entrypoint]", "actor source file to compile instead of fetching from the server")
        .description("Generate actor clients from the running server or an explicit source file")
        .option("--out-dir <directory>", "generated client directory", "generated")
        .addOption(
            new Option("--language <language>", "client language (inferred from the contract)").choices([
                "typescript",
                "python"
            ])
        )
        .option("--config <file>", "TypeScript configuration file (local source only)")
        .option("--control-plane-url <url>", "control-plane origin (overrides DURABLE_ACTORS_CONTROL_PLANE_URL)")
        .addHelpText("after", connectionHelp)
        .action(generate)
}

async function generate(entrypoint: string | undefined, options: GenerateOptions): Promise<void> {
    validateOptions(entrypoint, options)
    const source = entrypoint
        ? await localContract(entrypoint, options.config)
        : await remoteContract(options.controlPlaneUrl)
    const directory = path.resolve(options.outDir)
    const summary = z.object({ actors: z.array(z.unknown()), typescript: z.unknown().optional() }).parse(source)
    const language = options.language ?? (summary.typescript === undefined ? "python" : "typescript")
    if (language === "python") await generatePythonClient(source, directory)
    else await generateTypeScriptClient(source, directory)
    console.log(`Generated ${summary.actors.length} actor contract(s) in ${directory}.`)
}

async function generateTypeScriptClient(source: unknown, directory: string): Promise<void> {
    const { generateClient } = await import("../compiler/generators/client-generator.js")
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    const contract = parsePublicContract(source)
    await generateClient(contract, directory)
    const dependencies = Object.entries(contract.typescript.dependencies)
    if (dependencies.length)
        console.log(
            `Type dependencies: ${dependencies.map(([name, version]) => `${name}@${version}`).join(", ")}. Install compatible versions in the calling project.`
        )
}

function validateOptions(entrypoint: string | undefined, options: GenerateOptions): void {
    if (entrypoint && options.controlPlaneUrl !== undefined)
        throw new Error("--control-plane-url cannot be combined with a source entrypoint.")
    if (options.config && entrypoint?.endsWith(".py")) throw new Error("--config applies to TypeScript source only.")
    if (options.config && !entrypoint) throw new Error("--config requires a source entrypoint.")
}

async function localContract(entrypoint: string, configFile?: string) {
    if (entrypoint.endsWith(".py")) return compilePythonContract(entrypoint)
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    return new ActorCompiler().compileContract(entrypoint, { configFile })
}

async function remoteContract(controlPlaneUrl?: string) {
    const client = createControlPlaneClient(
        {
            ...process.env,
            DURABLE_ACTORS_CONTROL_PLANE_URL: controlPlaneUrl ?? process.env.DURABLE_ACTORS_CONTROL_PLANE_URL
        },
        fetch
    )
    const publication = publicationSchema.parse(await client.getContract())
    return publication.contract
}

const publicationSchema = z.strictObject({
    contractHash: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    contract: z.unknown()
})

export { registerGenerateCommand }
