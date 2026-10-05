# Durable Actors for TypeScript

Durable actors and typed clients, backed by the Rust runtime.

[Quickstart](#quickstart) · [Reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/typescript-guide.md) · [Runnable example](https://github.com/TerseAI/durable-actors/blob/main/examples/chat/README.md)

## Quickstart

Requires Node.js 22.19+, pnpm, and Bun 1.3.9+.

```sh
npx durable-actors init my-actors
cd my-actors
pnpm install
```

Define `src/actors.ts`. Fields marked `@Persisted` survive restarts. They share one SQLite database and transaction with `this.db` SQL. Litestream is installed and managed automatically with the runtime; local development persists to the filesystem.

```ts
import { Actor, Persisted } from "durable-actors"

export class Counter extends Actor {
    @Persisted private value = 0

    async increment() {
        return ++this.value
    }
}
```

```sh
pnpm exec durable-actors dev
```

## Call an actor

In another terminal in the same directory, generate the client:

```sh
pnpm exec durable-actors generate
```

Save as `client.ts` and run `bun client.ts`:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("one")
console.log(await counter.increment())
```

For WebSockets and browser integration, see the [chatroom example](https://github.com/TerseAI/durable-actors/blob/main/examples/chat/README.md).
