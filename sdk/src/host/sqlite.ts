import { createRequire } from "node:module"

import type { ActorDatabase, SqliteValue } from "../actor/database.js"
import type { JsonObject, JsonValue } from "../json.js"

import { syncLitestream } from "./litestream.js"
import type { LitestreamDatabase } from "./litestream.js"

interface SqliteState {
    readonly txid: number
    readonly path?: string
    readonly socket?: string
}

class SqliteCaptureError extends Error {}

interface ActorDatabaseStorage extends ActorDatabase {
    fields(): JsonObject
    persistFields(fields: JsonObject): void
    restore(state: SqliteState | undefined): void
    snapshot(): Promise<SqliteState>
    rollback(): void
    close(): void
}

class SqliteActorDatabase implements ActorDatabaseStorage {
    private connection: SqliteConnection | undefined
    private seed: LitestreamDatabase | undefined
    private txid = 0
    private version = ""
    private failure: SqliteCaptureError | undefined

    constructor(
        private readonly connect: (path: string) => SqliteConnection = openSqlite,
        private readonly sync: (database: LitestreamDatabase) => Promise<number> = syncLitestream
    ) {}

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        validateStatement(sql)
        const database = this.open()
        validateSingleStatement(database, sql)
        const statement = database.prepare(sql)
        if (!database.isTransaction) database.exec("BEGIN IMMEDIATE")
        return statement.all(...bindings) as Row[]
    }

    fields(): JsonObject {
        const rows = this.fieldDatabase().prepare("SELECT name, value FROM __terse_fields").all() as {
            name: string
            value: string
        }[]
        return Object.fromEntries(rows.map(row => [row.name, JSON.parse(row.value) as JsonValue]))
    }

    persistFields(fields: JsonObject): void {
        const database = this.fieldDatabase()
        const put = database.prepare(
            "INSERT INTO __terse_fields (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value WHERE value <> excluded.value"
        )
        for (const [name, value] of Object.entries(fields)) put.all(name, JSON.stringify(value))
        database
            .prepare("DELETE FROM __terse_fields WHERE name NOT IN (SELECT value FROM json_each(?))")
            .all(JSON.stringify(Object.keys(fields)))
    }

    restore(state: SqliteState | undefined): void {
        this.close()
        if (state === undefined || !Number.isSafeInteger(state.txid) || state.txid < 1 || !state.path || !state.socket)
            throw new Error("invalid actor SQLite recovery state")
        this.seed = { path: state.path, socket: state.socket }
        this.txid = state.txid
        this.version = this.changeToken()
    }

    async snapshot(): Promise<SqliteState> {
        try {
            const database = this.open()
            const version = this.changeToken()
            if (database.isTransaction) database.exec("COMMIT")
            if (version !== this.version) {
                const txid = await this.sync(this.seed!)
                if (!Number.isSafeInteger(txid) || txid < this.txid) throw new Error("invalid Litestream transaction")
                this.txid = txid
                this.version = version
            }
            return { txid: this.txid }
        } catch (cause) {
            this.failure = new SqliteCaptureError("failed to replicate actor SQLite commit", { cause })
            throw this.failure
        }
    }

    rollback(): void {
        if (this.connection?.isTransaction) this.connection.exec("ROLLBACK")
        if (this.connection !== undefined) this.version = this.changeToken()
    }

    close(): void {
        try {
            this.connection?.close()
        } finally {
            this.connection = undefined
            this.seed = undefined
            this.failure = undefined
        }
    }

    private changeToken(): string {
        const database = this.open()
        return JSON.stringify([
            database.prepare("SELECT total_changes() AS changes").get(),
            database.prepare("PRAGMA schema_version").get(),
            database.prepare("PRAGMA user_version").get()
        ])
    }

    private fieldDatabase(): SqliteConnection {
        const database = this.open()
        if (!database.isTransaction) database.exec("BEGIN IMMEDIATE")
        database.exec(
            "CREATE TABLE IF NOT EXISTS __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        return database
    }

    private open(): SqliteConnection {
        if (this.failure !== undefined) throw this.failure
        if (this.connection !== undefined) return this.connection
        if (this.seed === undefined) throw new Error("actor SQLite database has not been restored")
        const database = (this.connection = this.connect(this.seed.path))
        database.exec(
            "PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0; PRAGMA synchronous = FULL"
        )
        if (database.prepare("PRAGMA quick_check").get()!.quick_check !== "ok")
            throw new Error("invalid actor SQLite database")
        return database
    }
}

interface SqliteConnection {
    readonly isTransaction: boolean
    exec(sql: string): void
    prepare(sql: string): {
        all(...bindings: SqliteValue[]): object[]
        get(): Record<string, SqliteValue> | undefined
    }
    close(): void
}

function openSqlite(path: string): SqliteConnection {
    const require = createRequire(import.meta.url)
    if (!process.versions.bun) {
        const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite")
        return new DatabaseSync(path)
    }
    const { Database } = require("bun:sqlite") as {
        Database: new (
            path: string,
            options: { strict: boolean }
        ) => Omit<SqliteConnection, "isTransaction"> & {
            readonly inTransaction: boolean
        }
    }
    const database = new Database(path, { strict: true })
    return {
        get isTransaction() {
            return database.inTransaction
        },
        exec: sql => database.exec(sql),
        prepare: sql => database.prepare(sql),
        close: () => database.close()
    }
}

function validateStatement(sql: string): void {
    const statement = withoutComments(sql)
    if (/(?:__terse_|_litestream_)/i.test(statement)) throw new Error("SQLite runtime table names are reserved")
    if (/^(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE|ATTACH|DETACH|VACUUM)\b/i.test(statement))
        throw new Error("actor invocations own SQLite transactions and database files")
    if (
        /^PRAGMA\b/i.test(statement) &&
        !/^PRAGMA\s+(?:main\.)?(?:table_info|table_xinfo|index_info|index_xinfo|index_list|foreign_key_list|foreign_key_check|integrity_check|quick_check|user_version)\b/i.test(
            statement
        )
    )
        throw new Error("this SQLite pragma is managed by the actor runtime")
}

function validateSingleStatement(database: SqliteConnection, sql: string): void {
    const tokens = sql.matchAll(
        /--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/|'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|;/g
    )
    for (const token of tokens) {
        if (token[0] !== ";" || !withoutComments(sql.slice(token.index + 1))) continue
        try {
            // SQLite distinguishes a statement boundary from a semicolon inside a trigger body.
            database.prepare(sql.slice(0, token.index + 1))
        } catch (error) {
            if (!(error instanceof Error) || !error.message.includes("incomplete input")) throw error
            continue
        }
        throw new Error("actor database exec accepts one SQL statement")
    }
}

function withoutComments(sql: string): string {
    return sql.replace(/^(?:\s|;|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*/, "").trim()
}

export { SqliteActorDatabase, SqliteCaptureError }
export type { ActorDatabaseStorage, SqliteState }
