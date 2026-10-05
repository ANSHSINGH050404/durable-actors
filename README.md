# Durable Actors

> _an open-source alternative to Cloudflare Durable Objects... with no vendor lock-in, memory limits, and observability built in._

Durable Actors help you **build real-time applications** like chat systems (e.g. ChatGPT, Codex), collaboration tools (e.g. Notion), and agent swarms (e.g. Devin).

They provide _stateful serverless functions_, a foundational building block that abstracts away persistence, coordination, and infrastructure challenges in distributed systems.

[Live demo](https://demo.useterse.ai) · [TypeScript documentation](docs/reference/typescript-guide.md) · [Python documentation](docs/reference/python-guide.md)

## How it works

1. Define an _actor_, a class with _durable state_ (i.e. data survives interruptions, errors, and restarts) and _serialized execution_ (i.e. concurrent callers can update it safely).
   For example:

- **If you were building ChatGPT...** a chat actor can store conversations that survive LLM flakiness and server crashes (durable state)
- **If you were building Notion...** a document actor can coordinate concurrent edits from several people and agents (serialized execution)

2. Generate type-safe clients automatically with the Durable Actors SDK. For now, it supports Python and TypeScript.
3. Develop locally with one command and later self-host the Durable Actors runtime for production.

## Quickstart: Multiplayer AI Chat

![Teammates share one TeamAgent chat across regions; prompts queue, replies stream to everyone, and conversation state is durably persisted.](.github/assets/team-agent.gif)

### 1. Create your project

Install Node.js 22.19+ and Bun 1.3.9+.

```sh
npx durable-actors init my-actors
cd my-actors
npm install
npx durable-actors dev # Run the server locally on your machine
```

### 2. Define an _actor_

Define and export actors in your actor project’s `src/actors.ts`, the default entrypoint loaded by `durable-actors dev`. The runtime loads actors on demand and persists fields marked `@Persisted`.

For example, a chat history actor:

```ts
import { openai } from "@ai-sdk/openai"
import { streamText } from "ai"
import { Actor, type ActorSocket, Interleave, Persisted } from "durable-actors"

type Message = { role: "user" | "assistant"; content: string }

export class ChatHistory extends Actor<null, string, Message[]> {
    @Persisted messages: Message[] = []

    async onConnect(socket: ActorSocket<null, Message[]>) {
        socket.send(this.messages)
    }

    @Interleave // Let other calls run during await, e.g. new connections while a reply streams.
    async onMessage(_socket: ActorSocket<null, Message[]>, text: string) {
        this.messages.push({ role: "user", content: text })
        const result = streamText({ model: openai("gpt-5-mini"), messages: [...this.messages] })
        const reply: Message = { role: "assistant", content: "" }
        this.messages.push(reply)
        this.broadcast(this.messages)
        for await (const chunk of result.textStream) {
            reply.content += chunk
            this.broadcast(this.messages)
        }
    }
}
```

### 3. Connect your backend

We make it super easy to integrate the actors into your existing tech stack. Just generate the client and you get a fully type safe contract to interact with.

```sh
npx durable-actors generate
```

Create a WebSocket connection to one shared chat:

```ts
import express from "express"

import { actors } from "../generated/index.js"

export const app = express()

app.post("/api/chat/socket", async (_req, res) => {
    const grant = await actors.ChatHistory.prepareWebsocket({
        actorId: "lobby",
        metadata: null
    })
    res.set("Cache-Control", "no-store").json(grant)
})
```

### 4. Connect the frontend

```tsx
import { useEffect, useRef, useState } from "react"
import { createRoot } from "react-dom/client"

import type { actors } from "../generated/index.js"

function Chat() {
    const socket = useRef<WebSocket>(null)
    const [messages, setMessages] = useState<actors.ChatHistory.Outgoing>([])

    useEffect(() => {
        let active = true
        async function connect() {
            const response = await fetch("/api/chat/socket", { method: "POST" })
            const { websocketUrl } = await response.json()
            if (!active) return
            socket.current = new WebSocket(websocketUrl)
            socket.current.onmessage = event => setMessages(JSON.parse(event.data))
        }
        void connect()
        return () => {
            active = false
            socket.current?.close()
        }
    }, [])

    function send(form: FormData) {
        const text = String(form.get("message")).trim()
        if (!text || socket.current?.readyState !== WebSocket.OPEN) return
        socket.current.send(JSON.stringify(text))
    }

    return (
        <main>
            <div role="log" aria-label="Messages">
                {messages.map((message, index) => (
                    <p key={index}><strong>{message.role}:</strong> {message.content}</p>
                ))}
            </div>
            <form action={send}>
                <input name="message" aria-label="Message" required />
                <button>Send</button>
            </form>
        </main>
    )
}

createRoot(document.getElementById("root")!).render(<Chat />)
```

For a complete app with error handling and retries, see the [AI Chat example](examples/ai-chat).

### 5. Monitor and debug

Durable Agents provides built-in observability features:

![o11y-screenshot](https://github.com/user-attachments/assets/ac390257-8534-4911-830c-0e9155b36831)

## Examples

For complete sample applications, see [AI Chat](examples/ai-chat), [Collaborative Documents](examples/documents), and [Chatroom](examples/chat).

## Community

Bug reports, feature requests, documentation fixes, and code contributions are welcome. See the [contributing guide](CONTRIBUTING.md) for repository setup and checks, and follow our [code of conduct](CODE_OF_CONDUCT.md).

Use [GitHub Issues](https://github.com/TerseAI/durable-actors/issues) for bugs, ideas, and questions. Report vulnerabilities privately using our [security policy](SECURITY.md).

Follow development and release notes on [GitHub Releases](https://github.com/TerseAI/durable-actors/releases).

## License

[MIT](LICENSE.md) © 2026 Terse
