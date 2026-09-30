# Durable Actors for Python

Durable actors and typed clients, backed by the Rust runtime.

[Quickstart](#quickstart) · [Reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/python.md) · [Runnable example](https://github.com/TerseAI/durable-actors/blob/main/examples/python/README.md)

## Quickstart

Requires Node.js 22.19+, pnpm, Python 3.11+, and uv.

```sh
pnpm dlx durable-actors init my-actors --template python
cd my-actors
pnpm install
uv sync
```

Define `src/actors.py`. Every instance field must use `persisted()` or `ephemeral()`. Wrap persisted fields with `emitted()` to broadcast saved changes.

```python
from durable_actors import Actor, emitted, persisted

class Counter(Actor):
    count: int = emitted(persisted(0))

    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
```

```sh
pnpm exec durable-actors dev
```

## Call an actor

In another terminal in the same directory, generate the client:

```sh
pnpm exec durable-actors generate
```

Save as `client.py` and run `uv run client.py`:

```python
from generated import actors

counter = actors.Counter.get("one")
print(counter.increment())
```

## Use the client in another application

Copy the entire generated package, including its helper modules, into the application. From that application's root, install the client runtime:

```sh
uv add durable-actors
```

This installs the runtime dependencies automatically. The `[codegen]` extras are only needed to generate or regenerate clients, not to run them.

## Subscribe to state

Callbacks receive complete typed snapshots on a background thread. Keep the process running while listening; the context manager closes the subscription.

```python
from generated import actors

counter = actors.Counter.get("one")
with counter.subscribe(lambda state: print(state.count), on_error=print):
    counter.increment()
    input("Press Enter to stop.\n")
```
