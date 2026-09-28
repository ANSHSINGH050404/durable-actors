import type { JSONSchema7 } from "json-schema"

import type { SandboxOptions } from "../actor/sandbox.js"

import type { SocketContract } from "./contract.js"

interface PublicActorContract {
    readonly version: 1
    readonly actors: readonly ActorApi[]
    readonly typescript: TypeScriptContract
}

interface TypeScriptContract {
    readonly declarations: string
    readonly dependencies: Readonly<Record<string, string>>
}

interface ActorApi {
    readonly actorName: string
    readonly description?: string
    readonly sandbox?: SandboxOptions
    readonly socket: SocketContract
    readonly rpc: RpcContract
}

interface RpcContract {
    readonly schema: JSONSchema7
    readonly methods: readonly RpcMethod[]
}

interface RpcMethod {
    readonly name: string
    readonly description?: string
    readonly parameters: readonly RpcParameter[]
    readonly result: { readonly kind: "void" } | { readonly kind: "value"; readonly type: TypeReference }
}

interface RpcParameter {
    readonly name: string
    readonly optional: boolean
    readonly rest: boolean
    readonly type: TypeReference
}

interface TypeReference {
    readonly $ref: string
}

export type { ActorApi, TypeReference, PublicActorContract, RpcContract, RpcMethod, RpcParameter, TypeScriptContract }
