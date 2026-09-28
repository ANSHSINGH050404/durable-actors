<div align="left">
  <a href="https://github.com/TerseAI/durable-actors/blob/main/.github/assets/team-agent.gif">
    <picture>
      <source media="(prefers-reduced-motion: reduce)" srcset="https://raw.githubusercontent.com/TerseAI/durable-actors/main/.github/assets/team-agent.png">
      <img alt="Teammates share one TeamAgent chat across regions; prompts queue, replies stream to everyone, and conversation state is durably persisted." src="https://raw.githubusercontent.com/TerseAI/durable-actors/main/.github/assets/team-agent.gif" width="1000">
    </picture>
  </a>

  <h1>Durable Actors</h1>

  <p><strong>Durable state for collaborative apps and AI agents.</strong></p>
  <p>TypeScript actors. Rust runtime.</p>

  <p>
    <a href="https://github.com/TerseAI/durable-actors/stargazers"><img alt="GitHub stars" src="https://img.shields.io/github/stars/TerseAI/durable-actors?style=flat&amp;logo=github&amp;color=f5a623"></a>
    <a href="https://www.npmjs.com/package/durable-actors"><img alt="durable-actors on npm" src="https://img.shields.io/npm/v/durable-actors?label=npm&amp;logo=npm&amp;color=cb3837"></a>
    <a href="https://crates.io/crates/durable-actors"><img alt="durable-actors on crates.io" src="https://img.shields.io/crates/v/durable-actors?logo=rust&amp;color=dea584"></a>
    <a href="https://github.com/TerseAI/durable-actors/actions/workflows/ci.yml"><img alt="CI status on main" src="https://img.shields.io/github/actions/workflow/status/TerseAI/durable-actors/ci.yml?branch=main&amp;event=push&amp;label=CI&amp;logo=githubactions"></a>
    <a href="https://github.com/TerseAI/durable-actors/blob/main/LICENSE.md"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue"></a>
  </p>

  <p>
    <a href="#local-development"><img alt="Quickstart" src="https://img.shields.io/badge/quickstart-local%20development-22c55e?logo=rocket&amp;logoColor=white"></a>
    <a href="https://github.com/TerseAI/durable-actors/tree/main/docs"><img alt="Documentation" src="https://img.shields.io/badge/docs-Durable%20Actors-2563eb?logo=readthedocs&amp;logoColor=white"></a>
    <a href="https://useterse.ai"><img alt="Terse website" src="https://img.shields.io/badge/website-useterse.ai-000000"></a>
    <a href="https://www.linkedin.com/company/terse-inc"><img alt="Terse on LinkedIn" src="https://img.shields.io/badge/LinkedIn-Terse-0a66c2"></a>
  </p>

  <p>
    <a href="#local-development">Quickstart</a> ·
    <a href="https://github.com/TerseAI/durable-actors/blob/main/docs/reference/typescript.md">SDK</a> ·
    <a href="https://github.com/TerseAI/durable-actors/blob/main/docs/reference/openapi.md">HTTP API</a> ·
    <a href="https://github.com/TerseAI/durable-actors/tree/main/examples">Examples</a> ·
    <a href="https://github.com/TerseAI/durable-actors/blob/main/CONTRIBUTING.md">Contributing</a>
  </p>
</div>

---

Managing state is hard! Back in the pre-agent era, building a multiplayer app showed just how hard this could be. You had to lock resources, deal with websockets at scale, handle peak loads etc...

Now with AI, we've got agents working with agents and agents working with people to worry about. Furthermore, we have agent swarms coming!

Durable Actors is a primitive to help developers build the next generation of collaborative software. We provide a mechanism to serve shared, concurrency safe state to your app.

Based on the Actor principle from Erlang, all state is durably persisted for you. Only one Agent/person can be in the actor at a time, protecting you from race conditions.

We are fully horizontally scalable, and instances go dormant when not in use. Only pay for what your users are using.

We offer a clean API to manage webSocket connections, Swift inspired syntax for building your actor and full observability into your deployed actors.

## Local development

Install Node.js 22.19+, pnpm, and Bun 1.3.9+.

### Create your actor project in your directory of choice

```sh
npx durable-actors init my-actors
cd my-actors
pnpm install
# Or with npm:
npm install
npx durable-actors dev # Run the server locally on your machine
```

Running dev will also start a watch, every-time you make a change to an actor and save, metadata changes will be stored automatically.

### Connect your application

In your separate application project's directory (ex: node server), install the generator as a development dependency:

```sh
pnpm install --save-dev durable-actors
# Or with npm:
npm install --save-dev durable-actors
```

Local CLI commands and backend clients default to project `local` at `http://127.0.0.1:7100`. No project ID or secret is required, and local authentication is disabled unless you set `DURABLE_ACTORS_SECRET` on the actor server and backend.

`durable-actors observe` never requires a secret when connecting to `durable-actors dev`, even when application routes use one. The dev runtime binds only to localhost; hosted observability still uses the server's configured authentication.

Then generate your client from the same application directory:

```sh
npx durable-actors generate
```

Now you may call your actor and access the state.

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

For complete sample applications, see [AI Chat](https://github.com/TerseAI/durable-actors/tree/main/examples/ai-chat), [Collaborative documents](https://github.com/TerseAI/durable-actors/tree/main/examples/documents), and [Chatroom](https://github.com/TerseAI/durable-actors/tree/main/examples/chat).

## Define an actor

Install `ai` in your actor project, then define actors in `src/actors.ts`. This chat history stores completed exchanges in one persisted array:

```ts
import type { UIMessage } from "ai"
import { Actor, Persisted } from "durable-actors"

export class ChatHistory extends Actor {
    @Persisted private messages: UIMessage[] = []

    async load() {
        return this.messages
    }

    async append(...messages: UIMessage[]) {
        this.messages.push(...messages)
    }
}
```

## Stream from your backend

Generate a type-safe client with `npx durable-actors generate`. In your Express backend, load the conversation, stream a reply, and save the prompt and reply together when the stream succeeds:

```ts
import { openai } from "@ai-sdk/openai"
import { convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
import express from "express"

import { actors } from "./generated/index.js"

const app = express()
app.use(express.json())

app.get("/api/chat/:id", async (request, response) => {
    response.json(await actors.ChatHistory.get(request.params.id).load())
})

app.post("/api/chat", async (request, response) => {
    const [message] = await validateUIMessages({ messages: [request.body.messages.at(-1)] })
    if (message.role !== "user") return response.sendStatus(400)
    const chat = actors.ChatHistory.get(request.body.id)
    const messages = [...(await chat.load()), message]
    let failed = false
    const result = streamText({
        model: openai("gpt-5-mini"),
        messages: await convertToModelMessages(messages)
    })
    await pipeUIMessageStreamToResponse({
        response,
        stream: toUIMessageStream({
            stream: result.stream,
            originalMessages: messages,
            generateMessageId: generateId,
            onError: () => {
                failed = true
                return "Reply failed. Check the API key and try again."
            },
            onEnd: async ({ responseMessage, outcome }) => {
                if (!failed && outcome.status === "completed") await chat.append(message, responseMessage)
            }
        })
    })
})
```

Failed or interrupted attempts are not saved. Each completed exchange is persisted in one actor call.

## Connect the frontend

The AI SDK's `useChat` handles streaming and retries:

```tsx
import { useChat } from "@ai-sdk/react"
import type { UIMessage } from "ai"

const history: UIMessage[] = await fetch("/api/chat/lobby").then(response => response.json())

function Chat() {
    const { messages, sendMessage, regenerate, status, error } = useChat({ id: "lobby", messages: history })
    const busy = status === "submitted" || status === "streaming"

    return (
        <>
            {messages.map(message => (
                <p key={message.id}>
                    {message.role}: {message.parts.map(part => (part.type === "text" ? part.text : "")).join("")}
                </p>
            ))}
            <form action={async form => { await sendMessage({ text: String(form.get("message")) }) }}>
                <input name="message" aria-label="Message" required disabled={busy} />
                <button disabled={busy}>Send</button>
            </form>
            {error && <p role="alert">{error.message} <button onClick={() => regenerate()} disabled={busy}>Retry</button></p>}
        </>
    )
}
```

## Host it yourself

Follow the [self-hosting guide](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/self-hosting.md) to connect your backend with an API key and deploy your actors.

## Community

Bug reports, feature requests, documentation fixes, and code contributions are welcome. See the [contributing guide](https://github.com/TerseAI/durable-actors/blob/main/CONTRIBUTING.md) for repository setup and checks, and follow our [code of conduct](https://github.com/TerseAI/durable-actors/blob/main/CODE_OF_CONDUCT.md).

Use [GitHub Issues](https://github.com/TerseAI/durable-actors/issues) for bugs, ideas, and questions. Report vulnerabilities privately using our [security policy](https://github.com/TerseAI/durable-actors/blob/main/SECURITY.md).

Follow development and release notes on [GitHub Releases](https://github.com/TerseAI/durable-actors/releases).

## License

[MIT](https://github.com/TerseAI/durable-actors/blob/main/LICENSE.md) © 2026 Terse
