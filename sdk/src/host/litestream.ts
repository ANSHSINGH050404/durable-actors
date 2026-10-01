import { request } from "node:http"

interface LitestreamDatabase {
    readonly path: string
    readonly socket: string
}

async function syncLitestream(database: LitestreamDatabase): Promise<number> {
    const reply = JSON.parse(await sync(database)) as Record<string, unknown>
    if (reply.path !== database.path) throw new Error("Litestream synced another database")
    if (!Number.isSafeInteger(reply.txid) || Number(reply.txid) < 1)
        throw new Error("Litestream returned an invalid position")
    if (!Number.isSafeInteger(reply.replicated_txid) || Number(reply.replicated_txid) < Number(reply.txid))
        throw new Error("Litestream has not replicated the commit")
    return Number(reply.txid)
}

function sync(database: LitestreamDatabase): Promise<string> {
    return new Promise((resolve, reject) => {
        const body = JSON.stringify({ path: database.path, wait: true, timeout: 30 })
        const operation = request(
            {
                socketPath: database.socket,
                path: "/sync",
                method: "POST",
                signal: AbortSignal.timeout(35_000),
                headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) }
            },
            response => {
                let document = ""
                response.setEncoding("utf8")
                response.on("data", chunk => {
                    document += String(chunk)
                })
                response.on("error", reject)
                response.on("end", () => {
                    if (response.statusCode !== 200) reject(new Error(`Litestream sync failed: ${response.statusCode}`))
                    else resolve(document)
                })
            }
        )
        operation.on("error", reject)
        operation.end(body)
    })
}

export { syncLitestream }
export type { LitestreamDatabase }
