# Chatroom

An Express + React chatroom backed by a durable actor. Messages appear in every connected tab, and history survives server restarts.

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

Open [localhost:3000](http://127.0.0.1:3000) in two tabs. Send a message, then restart the actor server and reload to see the saved history.

## Define the actor

[ChatRoom](src/actors.ts) saves messages and sends the updated history to everyone in the room:

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

Each room ID has its own history.

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

Everyone joins as a guest. Add authentication and room access checks before issuing URLs in your app. Reload to reconnect after a disconnect.

## Development

Both processes read `.env`; saved state lives in `.durable-actors/`. Actor code reloads automatically. After changing public actor types, restart `npm run dev` to regenerate the client. For multiple examples, set distinct `PORT`, `DURABLE_ACTORS_PORT`, and matching control-plane URLs; see [Run the examples together](https://github.com/TerseAI/durable-actors/tree/main/examples#run-the-examples-together).

`npm run build` generates clients, checks TypeScript, and builds the frontend.
