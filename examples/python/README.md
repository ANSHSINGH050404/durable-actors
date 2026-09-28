# Python chat actor

From the repository root, build the runtime with `cargo build --locked`, install Node dependencies with `pnpm install`, and build the shared CLI with `pnpm --dir sdk build`.

In this directory:

```sh
uv sync
DURABLE_ACTORS_BINARY="$(pwd)/../../target/debug/durable-actors" DURABLE_ACTORS_ENTRYPOINT=actors.py node ../../sdk/dist/cli.js dev
```

In a second terminal, from this directory:

```sh
node ../../sdk/dist/cli.js generate
uv run client.py
```

Actor methods, socket hooks, and clients use a synchronous API with ordinary `def`. The shared TypeScript CLI finds `.venv`, runs strict mypy, and generates Python clients. Import `actors` from `generated`, then use `actors.Chat.get("lobby")` with the SDK-managed connection pool. The handle accepts `actors.Chat.Message` and returns typed messages that persist across runtime restarts. `chat.connect(actors.Chat.Metadata(name="Ada"))` opens a typed WebSocket. Actor and method types use the same namespace paths as TypeScript, including `actors.Chat.Stub` and `actors.Chat.Methods.append.Result`.

Run `uv run watch.py` to subscribe to typed state updates and make an RPC call in the same process. The SDK merges updates and calls the callback in the background. Press Enter to close the subscription.

The example uses the repository SDK through `tool.uv.sources`. Remove that table when using the published package.
