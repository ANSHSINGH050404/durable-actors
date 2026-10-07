# TypeScript guide

## Before you start

```bash
pnpm install
pnpm --dir sdk build
cd examples/bank-typescript
cp .env.example .env
```

## Defining an actor

Each bank account is an actor. An actor has fields and methods. The `@Persisted` property decorator saves a field after each successful method call.

To track a bank balance, define `src/actors.ts`:

```typescript
import { Actor, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted private balance = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
}
```

## Generating a client

Start the local dev server from `examples/bank-typescript`. The first run downloads the runtime:

```bash
pnpm dev
```

Wait for `Ready`, then generate the client in a second terminal:

```bash
cd examples/bank-typescript
pnpm generate
```

This creates a `generated` folder with typed stubs for your actors. Each actor instance is addressed by an ID. Create a `client.ts` that gets the `demo` account and calls a method:

```typescript
import { actors } from "./generated/index.js"

const account = actors.BankAccount.get("demo")
console.log(await account.getBalance())
```

Run it with `pnpm client`, which runs `bun client.ts`. Bun loads `.env`, so the client connects to port 7111.

An actor starts when one of its methods is called, stays in memory while it is busy, and shuts down after sitting idle. Regenerate the client after changing an actor's methods.

## Ephemeral fields

Next, count the deposits made since the actor last woke up. The `@Ephemeral` decorator keeps a field in memory only while the actor is awake. It resets when the actor shuts down.

```typescript
import { Actor, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted private balance = 0
    @Ephemeral private depositsSinceWake = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        return this.balance
    }
}
```

## WebSocket messages

Actors handle WebSocket connections directly. The three type parameters of `Actor` describe each connection. Payloads are JSON.

- **Metadata** identifies the client. In this example, it is the `userId` passed when the client connects.
- **Incoming** messages are what a client can send. Here, `{ type: "ping" }`, which `onMessage` answers with the balance.
- **Outgoing** messages are what the actor sends, through either `socket.send` or `broadcast`. Here, `{ type: "balance", balance }`.

The example also uses:

- **Tags.** `setTags` labels one socket. `onConnect` tags each socket `"customer"`.
- **Automatic responses.** The gateway answers a raw text `ping` with `pong` without waking the actor. `onConnect` registers the pair. A JSON `{ type: "ping" }` still runs `onMessage`.
- **Broadcasts.** `broadcast` sends to every connected socket. `deposit` limits it to sockets tagged `"customer"`, and `onDisconnect` uses `except` to skip the socket that just left.
- **Individual sends.** `socket.send` sends to one socket. `onConnect` and `onMessage` use it to send that client the current balance.

```typescript
import { Actor, type ActorSocket, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor<Metadata, Incoming, Outgoing> {
    @Persisted private balance = 0
    @Ephemeral private depositsSinceWake = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
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

type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
```

## Emitting state

`@Emittable` publishes a public `@Persisted` field to every connected client, so `balance` becomes public:

- When a client connects, it receives a `state` message with the current emitted fields, here `message.state.balance`.
- After a successful call changes the field, clients receive a `state_update` message with the changes.

```typescript
import { Actor, type ActorSocket, Emittable, Ephemeral, Persisted } from "durable-actors"

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
        this.balance += amount
        this.depositsSinceWake += 1
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

type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
```

To connect, your backend prepares a WebSocket grant for the actor, and the client opens its `websocketUrl`. A real backend checks the user's access before issuing a grant; the [chatroom example](../../examples/chat/README.md) shows a browser doing this.

Open the connection before the grant's `connectByMs` deadline. Authorization is checked on connection; an accepted socket remains authorized until it closes, including while the actor sleeps. Reconnects require a valid grant. To revoke an existing connection, explicitly close it.

```typescript
import { actors } from "./generated/index.js"

const { websocketUrl } = await actors.BankAccount.prepareWebsocket({ actorId: "demo", metadata: { userId: "ada" } })
const socket = new WebSocket(websocketUrl)
socket.addEventListener("message", event => {
    const message = JSON.parse(event.data)
    if (message.type === "state") console.log(message.state.balance)
    else if (message.type === "state_update") console.log(message.changes.balance)
    else console.log(message)
})
```

## Interleaving calls

An actor runs one call at a time, and a call holds the actor until it finishes, including across an `await`. `@Interleave` lets other calls run while that method awaits.

Add a second actor for wire transfers to `src/actors.ts`:

```typescript
import { Actor, Emittable, Interleave, Persisted } from "durable-actors"

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
```

`getStatus` can run while `submit` waits on the clearing network.

Interleaving changes failure handling for the whole class. A failed call normally rolls back its field and SQLite changes. Once any method in a class uses `@Interleave`, no call in that class rolls back, because overlapping calls may already have used the changes.

## Using SQLite

Each actor has its own SQLite database at `this.db`. `this.db.exec(sql, ...bindings)` runs one statement and returns its rows. Here, `deposit` records each deposit in a ledger table:

```typescript
import { Actor, Emittable, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted @Emittable balance = 0

    async deposit(amount: number): Promise<number> {
        this.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        this.balance += amount
        this.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        return this.balance
    }
}
```

- SQL writes and `@Persisted` fields commit together when the method or socket hook succeeds.
- Bind values with `?` placeholders. Values can be strings, numbers, bigints, byte arrays, or `null`.
- `this.db` is available only while a method or socket hook runs, not in the constructor.
- The runtime owns transactions and the database file, so `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `ATTACH`, `DETACH`, and `VACUUM` are rejected.
- The only allowed PRAGMAs are `table_info`, `table_xinfo`, `index_info`, `index_xinfo`, `index_list`, `foreign_key_list`, `foreign_key_check`, `integrity_check`, `quick_check`, and `user_version`.
- Names beginning with `__terse_` or `_litestream_` are reserved.

## Configuring actor resources

`@Compute` overrides the deployment's default resources for one actor class:

```typescript
import { Actor, Compute, Emittable, Persisted } from "durable-actors"

@Compute({ cpu: 0.5, memoryMiB: 256, idleTimeoutMs: 60_000 })
export class BankAccount extends Actor {
    @Persisted @Emittable balance = 0
}
```
