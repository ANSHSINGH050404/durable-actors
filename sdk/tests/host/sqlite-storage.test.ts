import assert from "node:assert/strict"
import { test } from "node:test"

import { SqliteActorDatabase } from "../../src/host/sqlite.js"
import { recover, seed } from "../fixtures/litestream.js"

test("commit errors reject the asynchronous snapshot operation", async context => {
    const database = new SqliteActorDatabase(() => ({
        isTransaction: true,
        exec(sql) {
            if (sql === "COMMIT") throw new Error("disk full")
        },
        prepare() {
            return { all: () => [], get: () => ({ quick_check: "ok" }) }
        },
        close() {}
    }))
    context.after(() => database.close())
    database.restore(await seed())
    database.exec("SELECT 1")
    await assert.rejects(database.snapshot(), /failed to replicate/)
})

test("SQLite replication returns a durable transaction position", async context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.restore(await seed())
    database.exec("CREATE TABLE entries (value BLOB)")
    database.exec("INSERT INTO entries VALUES (zeroblob(?))", 2 * 1024 * 1024)
    const captured = await database.snapshot()
    assert.equal(typeof captured, "object")
    assert.ok(captured.txid > 1)
})

test("SQLite snapshots retain blobs, bound parameters, and schema metadata", async context => {
    const database = new SqliteActorDatabase()
    const restored = new SqliteActorDatabase()
    const source = await seed()
    database.restore(source)
    context.after(() => {
        database.close()
        restored.close()
    })
    database.exec("CREATE TABLE entries (name TEXT, data BLOB)")
    database.exec("INSERT INTO entries VALUES (?, ?)", "'); DROP TABLE entries; --", new Uint8Array([0, 128, 255]))
    database.exec("PRAGMA user_version = 2")
    const image = await database.snapshot()
    restored.restore(await recover(source, image))
    const [row] = restored.exec<{ name: string; data: Uint8Array }>("SELECT name, data FROM entries")
    assert.equal(row!.name, "'); DROP TABLE entries; --")
    assert.deepEqual([...row!.data], [0, 128, 255])
    assert.equal(restored.exec<{ user_version: number }>("PRAGMA user_version")[0]!.user_version, 2)
    assert.ok((await restored.snapshot()).txid > 0)
    restored.close()
    database.exec("DELETE FROM entries")
    const deleted = await database.snapshot()
    assert.notEqual(deleted, image)
    restored.close()
    restored.restore(await recover(source, deleted))
    assert.deepEqual(restored.exec("SELECT * FROM entries"), [])
})

test("rollback discards pending SQL and retains the previous snapshot", async context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.restore(await seed())
    database.exec("CREATE TABLE entries (value TEXT)")
    const image = await database.snapshot()
    database.exec("INSERT INTO entries VALUES (?)", "discarded")
    database.rollback()
    assert.deepEqual(database.exec("SELECT * FROM entries"), [])
    assert.deepEqual(await database.snapshot(), { txid: image!.txid })
})

test("the runtime owns transaction and persistence configuration", async context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.restore(await seed())
    for (const sql of [
        "DROP TABLE __terse_fields",
        "DROP TABLE _litestream_seq",
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT x",
        "RELEASE x",
        "ATTACH ':memory:' AS other",
        "PRAGMA journal_mode = WAL",
        "/* comment */ COMMIT",
        "-- comment\nCOMMIT"
    ])
        assert.throws(() => database.exec(sql), /runtime|invocations|reserved/)
    assert.deepEqual(
        database.exec<{ value: number }>("SELECT 1 AS value").map(row => row.value),
        [1]
    )
    assert.ok((await database.snapshot()).txid > 0)
})

test("malformed SQLite recovery state is rejected", () => {
    const database = new SqliteActorDatabase()
    assert.throws(() => database.restore({ txid: 1 }), /invalid actor SQLite recovery state/)
    assert.throws(() => database.restore({ txid: 0, path: "/unused" }), /invalid actor SQLite recovery state/)
    database.close()
})

test("exec rejects multiple statements before applying writes and accepts trigger bodies", async context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.restore(await seed())
    database.exec("CREATE TABLE entries (value TEXT)")
    assert.throws(
        () => database.exec("INSERT INTO entries VALUES ('first'); INSERT INTO entries VALUES ('second')"),
        /one SQL statement/
    )
    assert.deepEqual(database.exec("SELECT * FROM entries"), [])
    database.exec("CREATE TABLE audit (value TEXT)")
    database.exec(
        "CREATE TRIGGER record_entry AFTER INSERT ON entries BEGIN INSERT INTO audit VALUES (NEW.value); INSERT INTO audit VALUES ('trigger; value'); END;"
    )
    database.exec("INSERT INTO entries VALUES (?); /* trailing ; comment */", "bound; value")
    assert.deepEqual(
        database.exec<{ value: string }>("SELECT value FROM audit; -- trailing comment").map(row => row.value),
        ["bound; value", "trigger; value"]
    )
    const [row] = database.exec<{ value: string; "a;b": number }>("SELECT ';' AS value, 1 AS \"a;b\"")
    assert.equal(row!.value, ";")
    assert.equal(row!["a;b"], 1)
})
