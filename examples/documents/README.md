# Collaborative documents

A Tiptap editor with Yjs for concurrent edits and durable actors for saved documents. Edit together in multiple tabs, then reload to pick up where you left off.

## Run locally

Requires Node.js 22.19+ and Bun 1.3.9+.

```sh
npx durable-actors init documents-example --template documents
cd documents-example
npm install
cp .env.example .env
npm run dev:actors
```

Already in the example directory? Start at `npm install`.

Wait for `Ready`. In another terminal in the same directory:

```sh
npm run dev
```

Open [localhost:3000](http://127.0.0.1:3000) in two tabs. Edit **Welcome** from both, add another document, and switch between them. Restart the servers and reload to restore saved documents.

## Save and share documents

[src/actors.ts](src/actors.ts) defines two actors:

- `Workspace` saves the document list.
- `Document` merges Yjs updates, saves the merged content, and broadcasts it to connected editors. Each document ID has its own actor.

A document joins through the [Express backend](src/backend.ts):

```ts
import { actors } from "../generated/index.js"

const grant = await actors.Document.prepareWebsocket({
    actorId: "welcome",
    metadata: null
})
```

The [collaboration client](src/collaboration.ts) opens the returned `websocketUrl` and exchanges Yjs updates. [App.tsx](src/App.tsx) handles document navigation; [Editor.tsx](src/Editor.tsx) connects Tiptap to the shared document.

Editing pauses while disconnected. Reload to reconnect; unsaved edits are not stored in the browser. This demo shares one workspace without authentication. Add user authentication and document access checks before issuing WebSocket URLs in your app.

## Development

Both processes read `.env`; saved state lives in `.durable-actors/`. Actor code reloads automatically. After changing public actor types, restart `npm run dev` to regenerate the client. For multiple examples, set distinct `PORT`, `DURABLE_ACTORS_PORT`, and matching control-plane URLs; see [Run the examples together](https://github.com/TerseAI/durable-actors/tree/main/examples#run-the-examples-together).

`npm run build` generates clients, checks TypeScript, and builds the frontend.
