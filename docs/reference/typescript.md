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

SQL and `@Persisted` fields commit together after successful methods or socket hooks. Fields occupy JSON values in the reserved `__terse_fields` table; names beginning with `__terse_` or `_litestream_` are reserved. Failed ordinary calls roll both back. Overlapping `@Reentrant` calls share state, and a failed call cannot roll back another call's changes.

Database access is available during actor invocations, after construction. The runtime owns transactions and database files; transaction control, attached databases, vacuuming, and storage-related pragmas are unavailable. SQLite on Node.js requires 22.19+. Deploy matching SDK and runtime versions.
