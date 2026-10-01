import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { mkdir, rename, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const version = "0.5.17"
const checksums = {
    "darwin-arm64": "e211f68ff7658d19f193f2914417afdf8f89a053ff8f263e5d6b3b1d3bbc7b08",
    "darwin-x64": "891875af09db152e93a4b31a8a79f538ce7ce702c132803cfe0a831e7cb1b7db",
    "linux-arm64": "f8ca4a050095c1efbda2c4365172e61bf9d955ea0d9ac42f448b52e51819baa5",
    "linux-x64": "cfb371176d164437ae869f8351cfde49bd1804ae71c61923f75c9cba9c9c006d"
}

export async function installLitestream(directory, { platform = process.platform, arch = process.arch } = {}, download = downloadRelease) {
    const checksum = checksums[`${platform}-${arch}`]
    if (!checksum) throw new Error(`No Litestream release for ${platform}/${arch}`)
    const name = `litestream-${version}-${platform}-${arch === "x64" ? "x86_64" : arch}.tar.gz`
    const archive = await download(`https://github.com/benbjohnson/litestream/releases/download/v${version}/${name}`)
    if (createHash("sha256").update(archive).digest("hex") !== checksum) throw new Error("Litestream archive checksum does not match")
    await mkdir(directory, { recursive: true })
    const file = path.join(directory, ".litestream.tar.gz")
    try {
        await writeFile(file, archive)
        await promisify(execFile)("tar", ["-xzf", file, "-C", directory, "litestream", "LICENSE"])
        await rename(path.join(directory, "LICENSE"), path.join(directory, "LICENSE.litestream"))
    } finally {
        await rm(file, { force: true })
    }
}

async function downloadRelease(url) {
    const response = await fetch(url, { signal: AbortSignal.timeout(120_000) })
    if (!response.ok) throw new Error(`Cannot download Litestream: HTTP ${response.status}`)
    return Buffer.from(await response.arrayBuffer())
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    if (!process.argv[2]) throw new Error("Litestream output directory required")
    await installLitestream(path.resolve(process.argv[2]))
}
