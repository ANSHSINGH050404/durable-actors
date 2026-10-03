import { Actor, type ActorSocket, Ephemeral, Interleave, Persisted } from "durable-actors"

type Event = { event: string; label?: string; count: number; waiting: number }

export class ReentrantProbe extends Actor<Record<string, never>, string, Event> {
    @Persisted count = 0
    @Persisted waiting = 0
    @Ephemeral private ordinaryRunning = false
    @Ephemeral private releases = new Map<string, () => void>()

    @Interleave
    async hold(label: string, fail = false): Promise<number> {
        this.count++
        this.waiting++
        const gate = new Promise<void>(resolve => this.releases.set(label, resolve))
        this.broadcast({ event: "started", label, count: this.count, waiting: this.waiting })
        await gate
        this.waiting--
        this.broadcast({ event: "finished", label, count: this.count, waiting: this.waiting })
        if (fail) throw new Error("generation failed")
        return this.count
    }

    async ordinaryHold(): Promise<void> {
        this.ordinaryRunning = true
        this.broadcast({ event: "ordinary-started", count: this.count, waiting: this.waiting })
        await new Promise(resolve => setTimeout(resolve, 300))
        this.ordinaryRunning = false
    }

    @Interleave
    async observeOrdinary(): Promise<boolean> {
        return this.ordinaryRunning
    }

    async release(label: string): Promise<void> {
        this.releases.get(label)?.()
        this.releases.delete(label)
    }

    async increment(): Promise<number> {
        return ++this.count
    }

    async read(): Promise<{ count: number; waiting: number }> {
        return { count: this.count, waiting: this.waiting }
    }

    async crash(): Promise<void> {
        process.exit(1)
    }

    async onConnect(socket: ActorSocket<Record<string, never>, Event>): Promise<void> {
        socket.send({ event: "connected", count: this.count, waiting: this.waiting })
    }

    async onMessage(socket: ActorSocket<Record<string, never>, Event>, _message: string): Promise<void> {
        socket.send({ event: "heartbeat", count: this.count, waiting: this.waiting })
    }

    async onDisconnect(): Promise<void> {
        this.broadcast({ event: "disconnected", count: this.count, waiting: this.waiting })
    }
}

export class SerialProbe extends Actor<Record<string, never>, string, string> {
    @Persisted waiting = false

    async onConnect(socket: ActorSocket<Record<string, never>, string>): Promise<void> {
        socket.send("connected")
    }

    async hold(): Promise<void> {
        this.waiting = true
        this.broadcast("started")
        await new Promise(resolve => setTimeout(resolve, 150))
        this.waiting = false
    }

    async read(): Promise<boolean> {
        return this.waiting
    }
}
