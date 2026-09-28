# Little Actors for Python

Define durable actors in Python and generate typed Python clients. Python 3.11+ is supported. Actor state, placement, routing, and WebSocket delivery use the same Rust runtime as the TypeScript SDK.

```sh
pnpm dlx durable-actors init my-actors --template python
cd my-actors
pnpm install
uv sync
pnpm exec durable-actors dev
```

The shared TypeScript CLI manages development, generation, and Python type checking. It selects the project's `.venv` (or the active environment); `DURABLE_ACTORS_PYTHON` overrides the interpreter. Install Node.js 22+, Python 3.11+, and uv. The CLI downloads the matching native runtime on macOS and Linux. When developing this repository, set `DURABLE_ACTORS_BINARY` to an absolute path to your `cargo build --locked` executable.

## Define actors

```python
from pydantic import BaseModel
from little_actors import Actor, emitted, ephemeral

class Message(BaseModel):
    text: str

class Chat(Actor):
    count: int = 0
    messages: list[Message] = emitted(default_factory=list)
    busy: bool = ephemeral(False)

    def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages
```

Public `def` methods become synchronous RPCs. Annotated fields persist by default. `emitted()` persists a field and broadcasts its saved changes; `ephemeral()` keeps a field temporary. Mutable defaults are copied for each actor, and both helpers accept `default_factory` for values constructed on activation. Use `ephemeral(default_factory=...)` for locks, caches, and service clients, and `ClassVar` for class constants. Classes extend `Actor` directly and use field defaults instead of constructors. Prefix helper methods with `_`. Synchronous handlers run on a worker thread, with calls serialized per actor. Socket hooks can also use ordinary `def`; `self.get_connections()` returns typed sockets.

Add `@reentrant` (imported from `little_actors`) to a `def` method to let other invocations enter before it finishes. Synchronous reentrant handlers overlap on worker threads; coordinate shared mutations and keep blocking I/O outside shared locks. Ordinary calls still serialize with each other. As in TypeScript, enabling reentrancy disables error rollback for the entire actor class. See the [execution semantics](../docs/reference/python.md#execution-and-failures) for details.

## Generate and use a client

With the actor server running, generate clients in your application:

```sh
pnpm add -D durable-actors
uv add 'little-actors[codegen]'
pnpm exec durable-actors generate --out-dir generated
```

You can also generate directly from a trusted source entrypoint:

```sh
pnpm exec durable-actors generate src/actors.py --out-dir generated
```

```python
from generated import actors

chat = actors.Chat.get("lobby")
messages: actors.Chat.Methods.append.Result = chat.append(actors.Chat.Message(text="hello"))
print(messages[0].text)
```

The CLI runs strict mypy on local actor definitions before generation and on the generated package afterward. `dev` checks definitions before startup and every reload; an invalid edit leaves the previous code running.

Generated RPC methods return typed values directly. The SDK creates a shared HTTP client when needed, reads configuration from the environment, and closes its connection pool at process exit. You only need the actor ID.

The generated package exposes `actors`, matching the TypeScript client namespace. Use `actors.Chat.get(id)` for a handle, `actors.Chat.Stub` for its type, and `actors.Chat.Methods.append.Args` / `.Result` for method types. Socket types live at `actors.Chat.Metadata`, `.Incoming`, `.Outgoing`, and `.State`; concrete models such as `actors.Chat.Message` are also available there. The package includes docstrings, independent Pydantic models, and `py.typed`. Consumers need only `little-actors`, not the actor project or the code generator. Regenerate after changing the actor contract, and include the generated package in your application's type checks.

Typing uses inline annotations and the [PEP 561](https://peps.python.org/pep-0561/) package marker. Both mypy and Pyright check the SDK and generated clients. Pydantic validates inputs, outputs, and persisted state at runtime. Python annotations remain ordinary annotations: `chat.append(42)` is rejected by a type checker and by runtime validation.

## Subscribe to state

Actors with emitted fields have a typed `subscribe` method:

```python
chat = actors.Chat.get("lobby")
subscription = chat.subscribe(lambda state: print(state.messages))
chat.append(actors.Chat.Message(text="hello"))
```

The SDK receives the initial state and applies later patches in the background. Each callback gets a complete typed snapshot of the emitted fields, and RPC calls continue normally. Call `subscription.close()` when finished. Callbacks run serially on a background thread; the subscription does not keep an otherwise finished process alive.

Pass `on_error=handler` to handle connection, validation, or callback failures. A failure stops the subscription; its exception is available as `subscription.error` and is logged if no handler is supplied. Actors with required connection metadata also require `metadata=...` when subscribing.

See the [Python reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/python.md) for supported types, sockets, reentrancy, deployment, and CLI options.

## Resource settings and backend helpers

Use `@sandbox(cpu=2, memory_mib=2048, idle_timeout_ms=60_000, regions=["canada"])` above an actor class to override deployment defaults. Import `sandbox` from `little_actors`.

Source classes also support `Chat.get("lobby")` with typed synchronous methods, including calls from other actors. Generated handles expose `broadcast(message)`; `actors.Chat.Authorization`, `actors.Chat.prepare_websocket(...)`, and `ActorProxy.handle(...)` issue typed browser access grants. `ActorSessionTransport` renews short-lived application credentials. The [Python reference](../docs/reference/python.md) covers these APIs and their docstrings.
