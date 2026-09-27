/** @module durable-actors/backend */
import { ActorSessionTransport } from "./client-runtime/session.js"
import type { ActorSessionTransportOptions } from "./client-runtime/session.js"
import { createActorStub as stub } from "./client-runtime/stub.js"
import type { ActorRpcMethod, ActorRpcTransport } from "./client-runtime/stub.js"
import { actorClient } from "./client/client.js"
import type { DurableActorsClientOptions } from "./client/remoteClient.js"

export function createActorStub<Stub extends object>(
    actorName: string,
    actorId: string,
    methods: readonly ActorRpcMethod[],
    transport?: ActorRpcTransport
): Stub {
    return stub(
        actorName,
        actorId,
        methods,
        transport ?? {
            async invoke(...args) {
                return (await actorClient()).invoke(...args)
            }
        }
    )
}
/** Connects backend clients using explicit settings. Keep the API key on the server. */
export function createActorTransport(options: DurableActorsClientOptions): ActorRpcTransport {
    let client: Promise<ActorRpcTransport> | undefined
    return {
        async invoke(...args) {
            client ??= import("./client/remoteClient.js").then(
                ({ RemoteActorClient }) => new RemoteActorClient(options)
            )
            return (await client).invoke(...args)
        }
    }
}
export function createActorSessionTransport(options: ActorSessionTransportOptions): ActorSessionTransport {
    return new ActorSessionTransport(options, { createTransport: createActorTransport })
}
export { ActorSessionRejectedError } from "./client-runtime/session.js"
export type { ActorSession, ActorSessionTransportOptions, ActorSessionTransport } from "./client-runtime/session.js"
export type { DurableActorsClientOptions }

export type { ActorRpcMethod, ActorRpcTransport }
