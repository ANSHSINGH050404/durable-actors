import { spawn } from "node:child_process"
import { createHash } from "node:crypto"
import { copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

export class RuntimeBuilder {
    constructor({ root, platform = process.platform, arch = process.arch }, run = runCommand) {
        this.root = root
        this.platform = platform
        this.arch = arch
        this.run = run
    }

    async build() {
        const output = path.join(this.root, "dist-runtime")
        await mkdir(output, { recursive: true })
        const staging = await mkdtemp(path.join(output, ".build-"))
        try {
            await this.compile(staging)
            return await this.package(staging, output)
        } finally {
            await rm(staging, { recursive: true, force: true })
        }
    }

    async compile(staging) {
        await this.run("cargo", ["build", "--locked", "--release", "--bin", "durable-actors"], { cwd: this.root })
        await copyFile(path.join(this.root, "target/release/durable-actors"), path.join(staging, "durable-actors"))
    }

    async package(staging, output) {
        const name = `durable-actors-${this.platform}-${this.arch}.tar.gz`
        const archive = path.join(staging, name)
        await this.run("tar", ["-czf", archive, "-C", staging, "durable-actors"])
        const checksum = createHash("sha256")
            .update(await readFile(archive))
            .digest("hex")
        const destination = path.join(output, name)
        await writeFile(`${destination}.sha256`, `${checksum}  ${name}\n`)
        await rename(archive, destination)
        return destination
    }
}

function runCommand(command, args, options) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, { ...options, stdio: "inherit" })
        child.once("error", reject)
        child.once("exit", code => (code === 0 ? resolve() : reject(new Error(`${command} exited with ${code}`))))
    })
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    console.log(await new RuntimeBuilder({ root: fileURLToPath(new URL("../", import.meta.url)) }).build())
}
