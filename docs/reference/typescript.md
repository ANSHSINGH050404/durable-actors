# TypeScript reference

[Quickstart](../../sdk/README.md#quickstart) · [Runnable example](../../examples/chat/README.md)

Hover over SDK exports for API documentation, or build the full reference from the repository root:

```sh
pnpm install
pnpm docs:build
```

Open `.artifacts/api/index.html` for actor, client, backend, proxy, and local runtime APIs.

## SQLite

Each actor has its own database through protected `this.db.exec<Row>(sql, ...bindings)`. Calls synchronously execute one statement and return rows. Bind values with `?` placeholders; supported values are strings, numbers, bigints, byte arrays, and `null`. Actor method results must satisfy the JSON result contract.

SQL and `@Persisted` fields commit together after successful methods or socket hooks. Fields occupy JSON values in the reserved `__terse_fields` table; names beginning with `__terse_` or `_litestream_` are reserved. Failed ordinary calls roll both back. Overlapping `@Interleave` calls share state, and a failed call cannot roll back another call's changes.

Database access is available during actor invocations, after construction. The runtime owns transactions and database files; transaction control, attached databases, vacuuming, and storage-related pragmas are unavailable. SQLite on Node.js requires 22.19+. Deploy matching SDK and runtime versions.

## Hibernating WebSockets

Open connections remain at the gateway when an idle actor sandbox shuts down. A message activates a new sandbox with the connection's metadata and tags intact. Inside an actor call:

```ts
const count = await this.getConnectionCount()
const members = await this.getConnections("member")
this.setWebSocketAutoResponse({ request: JSON.stringify("ping"), response: JSON.stringify("pong") })
```

Counts read one maintained integer without enumerating sockets or transferring their metadata. Fetch a connection list only when you need the connections themselves, optionally filtered by tag.

An automatic response is one exact text match and fixed reply per actor. In this example, a client sending the JSON string `"ping"` receives `"pong"` directly from the gateway. The actor's `onMessage` handler does not run, its idle timer is not reset, and a sleeping sandbox stays asleep. Other messages still run the handler normally. This is separate from WebSocket protocol ping/pong frames.

The SDK encodes application messages as JSON, so the example uses `JSON.stringify` to match the actual text on the wire. A raw WebSocket client must send the same bytes. Call `this.setWebSocketAutoResponse()` to clear the pair. The pair, metadata and tags survive actor hibernation while the gateway owns the room; gateway replacement disconnects clients. See [configuration](configuration.md#websockets) for limits and deployment details.
