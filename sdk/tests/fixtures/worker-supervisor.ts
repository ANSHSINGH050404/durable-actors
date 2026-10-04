import type { ActorExecutorCommand, ActorExecutorReply } from "../../src/host/protocol.js"
import type { SqliteState } from "../../src/host/sqlite.js"
import type { ActorWorker, ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"

import { commit } from "./litestream.js"

type Arguments = Parameters<ActorWorkerSupervisor["handle"]>
const states = new WeakMap<object, SqliteState>()

export function handle(
    host: ActorWorkerSupervisor,
    command: Arguments[0],
    allowNext: Arguments[1],
    publish?: Arguments[3],
    connections?: Arguments[4]
): Promise<ActorExecutorReply> {
    return host.handle(command, allowNext, capture(host, command), publish, connections)
}

export function execute(
    worker: ActorWorker,
    command: Parameters<ActorWorker["execute"]>[0],
    allowNext: Arguments[1],
    publish?: Arguments[3],
    connections?: Arguments[4]
): Promise<ActorExecutorReply> {
    return worker.execute(command, allowNext, capture(worker, command), publish, connections)
}

function capture(host: object, command: ActorExecutorCommand): () => Promise<number> {
    if ("sqlite" in command && command.sqlite && !states.has(host)) states.set(host, command.sqlite)
    if (command.type === "evict") states.delete(host)
    const state = states.get(host)
    return () => {
        if (!state) throw new Error("host has no registered SQLite database")
        return commit(state)
    }
}
