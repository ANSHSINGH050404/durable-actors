import { createRequire } from "node:module"

const require = createRequire(import.meta.url)
const Database = process.versions.bun ? require("bun:sqlite").Database : require("node:sqlite").DatabaseSync
const database = new Database(process.argv[2]) as { exec(sql: string): void; close(): void }
database.exec("PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE")
process.send!("locked")
process.once("message", () => {
    setTimeout(() => {
        database.exec("COMMIT")
        database.close()
        process.disconnect!()
    }, 100)
})
