import type { SocketContract } from "../wire/contract.js"

import type { ComputeOptions } from "./compute.js"

const ACTOR_ARTIFACT_VERSION = 1

enum Persistence {
    Persisted = "persisted",
    Ephemeral = "ephemeral"
}

interface ActorFieldSchema {
    readonly name: string
    readonly persistence: Persistence
    readonly private?: boolean
    readonly visibility?: "private" | "protected"
    readonly emittable?: boolean
}

interface ActorSchema {
    readonly actorName: string
    readonly fields: readonly ActorFieldSchema[]
    readonly sandbox?: ComputeOptions
    readonly reentrantMethods?: readonly string[]
    readonly contract?: SocketContract
}

export { ACTOR_ARTIFACT_VERSION, Persistence }
export type { ActorFieldSchema, ActorSchema }
