import { Actor, type ActorSocket, Compute, Emittable, Ephemeral, Interleave, Persisted } from "durable-actors"

@Compute({ cpu: 0.5, memoryMiB: 256, idleTimeoutMs: 60_000 })
export class BankAccount extends Actor<Metadata, Incoming, Outgoing> {
    @Persisted @Emittable balance = 0
    @Ephemeral private depositsSinceWake = 0
    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        this.balance += amount
        this.depositsSinceWake += 1
        this.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        this.broadcast({ type: "balance", balance: this.balance }, { tags: ["customer"] })
        return this.balance
    }
    async onConnect(socket: Socket): Promise<void> {
        socket.setTags("customer")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        socket.send({ type: "balance", balance: this.balance })
    }
    async onMessage(socket: Socket): Promise<void> {
        socket.send({ type: "balance", balance: this.balance })
    }
    async onDisconnect(socket: Socket): Promise<void> {
        this.broadcast({ type: "balance", balance: this.balance }, { except: socket })
    }
}

const clearingNetwork = {
    async submit(reference: string): Promise<string> {
        return `receipt:${reference}`
    }
}

export class Wire extends Actor {
    @Persisted @Emittable status = "requested"
    async getStatus(): Promise<string> {
        return this.status
    }
    @Interleave
    async submit(reference: string): Promise<string> {
        const receipt = await clearingNetwork.submit(reference)
        this.status = "submitted"
        return receipt
    }
}

type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
