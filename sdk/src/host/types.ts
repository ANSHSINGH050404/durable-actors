import type { ActorIdentity } from "../actor/identity.js"
import type { SocketEffect, SocketSource } from "../actor/socketProtocol.js"

import type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorWorkerData,
    HydrateCommand,
    InvokeCommand,
    WebSocketEventCommand
} from "./protocol.js"
import type { SqliteCommitter } from "./sqlite.js"
import type { ActorWorkerSupervisor } from "./worker-supervisor.js"

interface ActorHostSettings {
    readonly socketPath: string
    readonly actorEntrypoint: string | undefined
    readonly startupTimeoutMs: number
}

type SocketPublisher = (effects: readonly SocketEffect[]) => Promise<void>

type ActorCommandHandler = (
    command: ActorExecutorCommand,
    allowNextInvocation: () => void,
    commit: SqliteCommitter,
    publish?: SocketPublisher,
    connections?: SocketSource
) => Promise<ActorExecutorReply>

type ActorWorkerSupervisorFactory = (
    options: ActorWorkerSupervisorOptions
) => Pick<ActorWorkerSupervisor, "ready" | "handle" | "close" | "activeActors" | "onActiveActorsChange">

interface ActorWorkerSupervisorOptions {
    readonly actorEntrypointUrl: string
    readonly createWorker?: ActorWorkerFactory
}

interface ResidentActorWorkerOptions {
    readonly identity: ActorIdentity
    readonly sequenceBase: number
    readonly onActiveActorsChange: () => void
    readonly moduleUrl: string
    readonly worker?: ActorWorkerHandle
    readonly createWorker: ActorWorkerFactory
}

type ActorWorkerState = "starting" | "ready" | "stopping" | "stopped"

interface ActorWorkerHandle {
    readonly state: ActorWorkerState
    ready(): Promise<readonly string[]>
    execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
        allowNextInvocation: () => void,
        commit: SqliteCommitter,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply>
    terminate(reason: string): void
}

type ActorWorkerFactory = (data: ActorWorkerData, onStateChange: () => void) => ActorWorkerHandle

export type {
    ActorCommandHandler,
    ActorHostSettings,
    ActorWorkerFactory,
    ActorWorkerHandle,
    ActorWorkerState,
    ActorWorkerSupervisorFactory,
    ActorWorkerSupervisorOptions,
    ResidentActorWorkerOptions,
    SocketPublisher,
    SocketSource
}
