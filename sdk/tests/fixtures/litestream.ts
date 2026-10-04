import { execFile, spawn } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { request } from "node:http"
import { createRequire } from "node:module"
import { join } from "node:path"
import { promisify } from "node:util"

import type { SqliteState } from "../../src/host/sqlite.js"
import type { JsonObject } from "../../src/json.js"

const directory = mkdtempSync("/tmp/sdk-litestream-")
const socket = join(directory, "control.sock")
let daemon: ReturnType<typeof spawn> | undefined
let ready: Promise<void> | undefined
let count = 0
const replicas = new Map<string, string>()

process.once("exit", () => {
    daemon?.kill()
    rmSync(directory, { recursive: true, force: true })
})

export async function seed(fields: JsonObject | null = {}): Promise<SqliteState> {
    await (ready ??= start())
    const path = join(directory, `${++count}.sqlite`)
    const database = connect(path)
    try {
        database.exec(
            "PRAGMA journal_mode=WAL; CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        for (const [name, value] of Object.entries(fields ?? {}))
            database.prepare("INSERT INTO __terse_fields VALUES (?, ?)").run(name, JSON.stringify(value))
    } finally {
        database.close()
    }
    return register(path)
}

export async function commit(state: SqliteState): Promise<number> {
    const reply = await ipc("sync", { path: state.path, wait: true, timeout: 10 })
    return Number(reply.txid)
}

export async function recover(source: SqliteState, position: SqliteState): Promise<SqliteState> {
    const path = join(directory, `${++count}.sqlite`)
    await promisify(execFile)("litestream", [
        "restore",
        "-txid",
        position.txid.toString(16).padStart(16, "0"),
        "-o",
        path,
        `file://${replicas.get(source.path!)!}`
    ])
    return register(path)
}

export function fields(state: SqliteState): JsonObject {
    const database = connect(state.path!)
    try {
        return Object.fromEntries(
            database
                .prepare("SELECT name, value FROM __terse_fields")
                .all()
                .map(row => [row.name, JSON.parse(row.value)])
        )
    } finally {
        database.close()
    }
}

async function register(path: string): Promise<SqliteState> {
    const replica = `${path}.replica`
    replicas.set(path, replica)
    await ipc("register", { path, replica_url: `file://${replica}` })
    const reply = await ipc("sync", { path, wait: true, timeout: 10 })
    return { txid: Number(reply.txid), path }
}

async function start(): Promise<void> {
    const config = join(directory, "config.json")
    writeFileSync(config, JSON.stringify({ socket: { enabled: true, path: socket }, levels: [] }))
    daemon = spawn("litestream", ["replicate", "-config", config], { stdio: "ignore" })
    daemon.unref()
    let failure: Error | undefined
    daemon.on("error", error => {
        failure = error
    })
    for (let attempt = 0; attempt < 200; attempt++) {
        if (failure) throw failure
        try {
            await ipc("info")
            return
        } catch {
            await new Promise(resolve => setTimeout(resolve, 20))
        }
    }
    throw new Error("test Litestream did not start")
}

function ipc(endpoint: string, body?: object): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
        const operation = request(
            { socketPath: socket, path: `/${endpoint}`, method: body ? "POST" : "GET" },
            response => {
                let text = ""
                response.setEncoding("utf8")
                response.on("data", chunk => {
                    text += String(chunk)
                })
                response.on("error", reject)
                response.on("end", () => {
                    if (response.statusCode !== 200) reject(new Error(text))
                    else {
                        try {
                            resolve(JSON.parse(text))
                        } catch (error) {
                            reject(error)
                        }
                    }
                })
            }
        )
        operation.on("error", reject)
        operation.end(body ? JSON.stringify(body) : undefined)
    })
}

function connect(path: string) {
    const require = createRequire(import.meta.url)
    const Database = process.versions.bun ? require("bun:sqlite").Database : require("node:sqlite").DatabaseSync
    return new Database(path) as {
        exec(sql: string): void
        prepare(sql: string): { run(...values: string[]): unknown; all(): { name: string; value: string }[] }
        close(): void
    }
}
