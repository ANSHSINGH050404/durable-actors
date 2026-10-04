import { access } from "node:fs/promises"
import type { Socket } from "node:net"
import { isAbsolute } from "node:path"
import { z } from "zod"

import { ActorSession, connectSocket, parseHostSettings } from "./actor-host.js"
import { ActorWorker, ActorWorkerSupervisor } from "./worker-supervisor.js"

async function runGenericHost(): Promise<never> {
    const worker = new ActorWorker()
    try {
        await worker.warm()
        const settings = parseHostSettings(process.env)
        const socket = await connectSocket(settings.socketPath)
        const assignment = readAssignment(socket)
        socket.write(`${JSON.stringify({ type: "warm", protocol: 24 })}\n`)
        const { entrypoint, environment } = await assignment
        await access(entrypoint)
        let available = true
        const session = new ActorSession(
            { ...settings, actorEntrypoint: entrypoint },
            options =>
                new ActorWorkerSupervisor({
                    ...options,
                    createWorker: (data, onStateChange) => {
                        const assigned = { ...data, environment }
                        if (!available) return new ActorWorker(assigned, onStateChange)
                        available = false
                        worker.load(assigned, onStateChange)
                        return worker
                    }
                }),
            socket
        )
        await session.start()
        await session.waitUntilDisconnected()
        throw new Error("Rust host disconnected")
    } finally {
        worker.terminate("generic executor stopped")
    }
}

function readAssignment(socket: Socket): Promise<z.infer<typeof assignmentSchema>> {
    return new Promise((resolve, reject) => {
        let buffer = ""
        const fail = (error: Error) => {
            cleanup()
            socket.destroy()
            reject(error)
        }
        const closed = () => fail(new Error("Rust host disconnected before assignment"))
        const read = (chunk: Buffer) => {
            buffer += chunk.toString("utf8")
            if (!buffer.includes("\n")) return
            try {
                const assignment = assignmentSchema.parse(JSON.parse(buffer))
                cleanup()
                resolve(assignment)
            } catch (error) {
                fail(error instanceof Error ? error : new Error(String(error)))
            }
        }
        const cleanup = () => {
            socket.off("data", read)
            socket.off("error", fail)
            socket.off("close", closed)
        }
        socket.on("data", read).once("error", fail).once("close", closed)
    })
}

const assignmentSchema = z.object({
    type: z.literal("load"),
    entrypoint: z.string().refine(isAbsolute, "entrypoint must be absolute"),
    environment: z.record(z.string(), z.string())
})

export { runGenericHost }
