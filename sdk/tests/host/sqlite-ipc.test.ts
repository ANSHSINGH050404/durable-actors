import assert from "node:assert/strict"
import { mkdtempSync, rmSync } from "node:fs"
import { createRequire } from "node:module"
import { join } from "node:path"
import { test } from "node:test"

import { SqliteActorDatabase, SqliteCaptureError } from "../../src/host/sqlite.js"

test("actor SQLite commits the host database before waiting for the executor commit acknowledgement", async context => {
    const fixture = sqliteFile()
    context.after(fixture.close)
    let release!: (txid: number) => void
    let synced = false
    const database = new SqliteActorDatabase(async () => {
        assert.deepEqual(fixture.connection.prepare("SELECT value FROM entries").all(), [{ value: 7 }])
        assert.deepEqual(fixture.connection.prepare("SELECT name, value FROM __terse_fields").all(), [
            { name: "count", value: "7" }
        ])
        return await new Promise<number>(resolve => {
            release = resolve
        })
    })
    context.after(() => database.close())
    database.restore({ txid: 1, path: fixture.path })
    database.exec("CREATE TABLE entries (value INTEGER)")
    database.exec("INSERT INTO entries VALUES (7)")
    database.persistFields({ count: 7 })
    const pending = database.snapshot().then(state => {
        synced = true
        return state
    })
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(synced, false)
    release(2)
    assert.deepEqual(await pending, { txid: 2 })
    database.persistFields({ count: 7 })
    assert.deepEqual(await database.snapshot(), { txid: 2 })
})

test("a failed replication acknowledgement fences subsequent database writes", async context => {
    const fixture = sqliteFile()
    context.after(fixture.close)
    const database = new SqliteActorDatabase(async () => {
        throw new Error("host disconnected")
    })
    context.after(() => database.close())
    database.restore({ txid: 1, path: fixture.path })
    database.persistFields({ count: 1 })
    await assert.rejects(database.snapshot(), SqliteCaptureError)
    assert.throws(() => database.exec("CREATE TABLE unacknowledged (value INTEGER)"), SqliteCaptureError)
})

function sqliteFile() {
    const directory = mkdtempSync("/tmp/actor-ipc-test-")
    const path = join(directory, "actor.sqlite")
    const require = createRequire(import.meta.url)
    const Database = process.versions.bun ? require("bun:sqlite").Database : require("node:sqlite").DatabaseSync
    const connection = new Database(path) as {
        exec(sql: string): void
        prepare(sql: string): { all(): object[] }
        close(): void
    }
    connection.exec(
        "PRAGMA journal_mode=WAL; CREATE TABLE __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
    )
    return {
        path,
        connection,
        close() {
            connection.close()
            rmSync(directory, { recursive: true, force: true })
        }
    }
}
