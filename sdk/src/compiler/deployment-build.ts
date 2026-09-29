import { execFile } from "node:child_process"
import { stat } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { errorMessage } from "../errors.js"

import { buildActor } from "./actor-build.js"
import { parsePublicContract } from "./validate-public-contract.js"

async function main(): Promise<void> {
    const [directory, entrypoint, output, mode] = process.argv.slice(2)
    if (!directory || !entrypoint || !output)
        throw new Error("Expected project directory, actor entrypoint, and output directory")
    if (entrypoint.endsWith(".py")) return buildPython(directory, entrypoint, output, mode)
    process.chdir(directory)
    const artifact = path.join(output, "actors.mjs")
    const contract = parsePublicContract(await buildActor(entrypoint, artifact, { local: mode === "local" }))
    const document = JSON.stringify(contract)
    if (Buffer.byteLength(document) > 4 * 1024 * 1024) throw new Error("Public actor contract exceeds 4 MiB")
    const { size } = await stat(artifact)
    if (size === 0 || size > 32 * 1024 * 1024) throw new Error("Compiled customer code must contain 1–33554432 bytes")
    if (mode === "local") await validateArtifact(artifact, entrypoint)
    process.stdout.write(document)
}

async function buildPython(directory: string, entrypoint: string, output: string, mode?: string): Promise<void> {
    const { stdout, stderr } = await promisify(execFile)(
        process.env.DURABLE_ACTORS_PYTHON ?? "python3",
        ["-m", "durable_actors.build", directory, entrypoint, output, ...(mode === "local" ? [mode] : [])],
        { maxBuffer: 8 * 1024 * 1024 }
    )
    process.stderr.write(stderr)
    process.stdout.write(stdout)
}

async function validateArtifact(artifact: string, entrypoint: string): Promise<void> {
    const validator = fileURLToPath(new URL("../host/validate-artifact.js", import.meta.url))
    try {
        await promisify(execFile)(process.execPath, [validator, artifact], { timeout: 30_000 })
    } catch (error) {
        throw new Error(`Cannot load actor entrypoint ${entrypoint} using ${validator}: ${errorMessage(error)}`)
    }
}

await main()
