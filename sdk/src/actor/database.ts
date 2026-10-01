import { AsyncLocalStorage } from "node:async_hooks"

import { ActorDefinitionError } from "../errors.js"

type SqliteValue = string | number | bigint | Uint8Array | null

interface ActorDatabase {
    /** Executes one SQL statement with positional bindings and returns its rows. */
    exec<Row extends object = Record<string, SqliteValue>>(sql: string, ...bindings: SqliteValue[]): Row[]
}

const databases = new WeakMap<object, ActorDatabase>()
const invocation = new AsyncLocalStorage<{ actor: object; active: boolean }>()

function actorDatabase(actor: object): ActorDatabase {
    const database = databases.get(actor)
    if (database === undefined) throw new ActorDefinitionError("actor database is unavailable during construction")
    return database
}

function bindActorDatabase(actor: object, database: ActorDatabase): void {
    databases.set(actor, {
        exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
            const context = invocation.getStore()
            if (context?.actor !== actor || !context.active)
                throw new ActorDefinitionError("actor database is unavailable outside its invocation")
            return database.exec<Row>(sql, ...bindings)
        }
    })
}

async function runWithActorDatabase<T>(actor: object, operation: () => Promise<T>): Promise<T> {
    const context = { actor, active: true }
    try {
        return await invocation.run(context, operation)
    } finally {
        context.active = false
    }
}

export { actorDatabase, bindActorDatabase, runWithActorDatabase }
export type { ActorDatabase, SqliteValue }
