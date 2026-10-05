# Chatroom

An Express + React chatroom with shared messages and persistent history.

[Quickstart](../../sdk/README.md#quickstart) · [Reference](../../docs/reference/typescript-guide.md)

## Run locally

Requires Node.js 22.19+ and Bun 1.3.9+.

```sh
npx durable-actors init chat-example --template chat
cd chat-example
npm install
cp .env.example .env
npm run dev:actors
```

Already in the example directory? Start at `npm install`.

Wait for `Ready`. In another terminal in the same directory:

```sh
npm run dev
```

Open [localhost:3000](http://127.0.0.1:3000) in two tabs; messages stay saved after an actor server restart.

## Define the actor

[ChatRoom](src/actors.ts) saves and broadcasts messages; each room ID has its own history:

```ts
import { Actor, Persisted } from "durable-actors"
import type { ActorSocket } from "durable-actors"

export class ChatRoom extends Actor<Member, string, ChatMessage[]> {
    @Persisted history: ChatMessage[] = []

    async onConnect(socket: ActorSocket<Member, ChatMessage[]>) {
        socket.send(this.history)
    }

    async onMessage(socket: ActorSocket<Member, ChatMessage[]>, text: string) {
        this.history.push({ name: socket.metadata.name, text })
        this.broadcast(this.history)
    }
}

type Member = { name: string }
type ChatMessage = { name: string; text: string }
```

## Connect the app

The [Express backend](src/backend.ts) issues a WebSocket URL with `actors.ChatRoom.prepareWebsocket`. The [React client](src/Chat.tsx) connects and sends JSON messages:

```js
const response = await fetch("/api/socket/ChatRoom/lobby", { method: "POST" })
if (!response.ok) throw new Error("Connection denied")
const { websocketUrl } = await response.json()
const socket = new WebSocket(websocketUrl)

socket.onopen = () => socket.send(JSON.stringify("Hello"))
socket.onmessage = event => console.log(JSON.parse(event.data))
```

The demo joins as a guest; add authentication and room access checks before issuing URLs in your app. Reload to reconnect.

## Development

Actors reload automatically; restart `npm run dev` after changing public actor types. See [port settings](../README.md#run-the-examples-together) to run multiple examples.

```sh
npm run build
```
