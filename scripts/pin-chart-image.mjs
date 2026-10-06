import { readFileSync, writeFileSync } from "node:fs"
import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function pinChartImage(values, digest) {
    if (!/^sha256:[a-f0-9]{64}$/u.test(digest ?? "")) throw new Error("Expected an immutable runtime digest")
    if ([...values.matchAll(/^  digest: ""$/gmu)].length !== 1) throw new Error("Chart values must contain one image digest placeholder")
    return values.replace(/^  digest: ""$/mu, `  digest: "${digest.slice(7)}"`)
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    const values = new URL("../charts/durable-actors/values.yaml", import.meta.url)
    writeFileSync(values, pinChartImage(readFileSync(values, "utf8"), process.argv[2]))
}
