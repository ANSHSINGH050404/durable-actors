# Python chat actor

Persistent messages, typed RPCs, and live state updates.

[Quickstart](../../sdk-python/README.md#quickstart) · [Reference](../../docs/reference/python-guide.md) · [Actor](actors.py) · [Client](client.py) · [Subscriber](watch.py)

## Run

From the repository root, build the runtime and shared CLI:

```sh
cargo build --locked
pnpm install
pnpm --dir sdk build
cd examples/python
uv sync
DURABLE_ACTORS_BINARY="$PWD/../../target/debug/durable-actors" DURABLE_ACTORS_ENTRYPOINT=actors.py node ../../sdk/dist/cli.js dev
```

In a second terminal, from `examples/python`:

```sh
node ../../sdk/dist/cli.js generate
uv run client.py
uv run watch.py
```

The client saves a message; the watcher prints live history until you press Enter. Restart the actor server to verify that messages persist.

## Call the actor

```python
from generated import actors

chat = actors.Chat.get("lobby")
print(chat.append(actors.Chat.Message(text="Hello from Python")))
```

This example uses the repository SDK through `tool.uv.sources`; remove that table to use the published package.
