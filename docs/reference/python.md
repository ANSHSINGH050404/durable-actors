# Python reference

[Quickstart](../../sdk-python/README.md#quickstart) · [Runnable example](../../examples/python/README.md)

## Actor definitions

Extend `Actor` directly with typed `def` methods and field defaults; prefix helper methods with `_`. Annotated fields persist, `emitted()` also broadcasts changes, and `ephemeral()` keeps temporary values.

```python
from pydantic import BaseModel, Field
from durable_actors import Actor, emitted, ephemeral

class Message(BaseModel):
    text: str = Field(min_length=1)

class Chat(Actor):
    messages: list[Message] = emitted(default_factory=list)
    busy: bool = ephemeral(False)

    def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages
```

## Types and clients

Use concrete types at public boundaries: JSON primitives, typed collections, unions, models, dataclasses, dates, or UUIDs; `Any` and bare collections are rejected. Generated clients validate values and expose models and method types through the actor namespace.

```python
from generated import actors

chat: actors.Chat.Stub = actors.Chat.get("lobby")
messages: actors.Chat.Methods.append.Result = chat.append(actors.Chat.Message(text="hello"))
print(messages[0].text)
```

## State subscriptions

Callbacks receive complete typed snapshots of emitted fields on a background thread. Failures stop the subscription and reach `on_error`; use `with` to close it.

```python
with chat.subscribe(lambda state: print(state.messages), on_error=print):
    chat.append(actors.Chat.Message(text="hello"))
    input("Press Enter to stop.\n")
```

## Execution and failures

Calls serialize and roll back persisted state on failure by default. `@reentrant` allows overlapping threads and disables rollback for the entire class; protect shared mutations with an ephemeral lock.

```python
import time
from threading import Lock
from durable_actors import Actor, ephemeral, reentrant

class Counter(Actor):
    count: int = 0
    _lock: Lock = ephemeral(default_factory=Lock)

    def increment(self) -> int:
        with self._lock:
            self.count += 1
            return self.count

    @reentrant
    def wait(self, seconds: float) -> str:
        time.sleep(seconds)
        return self.id
```

An `ActorInvocationError` with code `outcome_unknown` means the call may have executed; replaying it can repeat side effects.

## Typed WebSockets

`Actor[Metadata, Incoming, Outgoing]` types the connection and its messages. This `Room` uses the `Message` model above.

```python
from durable_actors import ActorSocket

class Member(BaseModel):
    name: str

class Room(Actor[Member, Message, Message]):
    def on_message(self, socket: ActorSocket[Member, Message], message: Message) -> None:
        self.broadcast(Message(text=f"{socket.metadata.name}: {message.text}"))
```

Regenerate after adding `Room`, then connect with its required metadata. Connections also deliver `StateSnapshot` and `StateUpdate` events when an actor emits state.

```python
from generated import actors
from durable_actors import StateSnapshot, StateUpdate

room = actors.Room.get("lobby")
with room.connect(actors.Room.Metadata(name="Ada")) as connection:
    connection.send(actors.Room.Incoming(text="hello"))
    for event in connection:
        if isinstance(event, (StateSnapshot, StateUpdate)):
            print(event)
        else:
            print(event.text)
```

## CLI and configuration

The shared Node CLI uses the project's `.venv` and runs strict mypy on actor definitions and generated clients. Keep the CLI, Python SDK, and native runtime versions aligned.

```sh
pnpm add -D durable-actors
uv add 'durable-actors[codegen]'
pnpm exec durable-actors dev
```

Generate in another terminal, from the running server or a trusted source file:

```sh
pnpm exec durable-actors generate --language python
pnpm exec durable-actors generate src/actors.py --out-dir generated
```

Clients default to project `local` at `http://127.0.0.1:7100`; explicit `Client` options override environment settings. See [configuration](configuration.md) for server settings, and use `DURABLE_ACTORS_PYTHON` to select another interpreter.

```python
from durable_actors import Client

with Client(control_plane_url="http://127.0.0.1:7100", project_id="local") as client:
    chat = actors.Chat.get("lobby", client)
    print(chat.append(actors.Chat.Message(text="hello")))
```

## Source references and broadcasts

When actor source is available, call it directly without generation. Backend broadcasts send application messages without persisting them.

```python
from src.actors import Chat, Message, Room

print(Chat.get("lobby").append(Message(text="hello")))
Room.get("lobby").broadcast(Message(text="announcement"))
```

## Browser access

Authenticate the user and check room access before issuing a WebSocket grant. Treat its URL as a credential.

```python
from generated import actors, ActorProxy

access = actors.Room.Authorization(
    actor_id="lobby",
    metadata=actors.Room.Metadata(name="Ada"),
)
grant = ActorProxy.handle(access)
```

## Sandbox resources

Override deployment defaults per actor; see [configuration](configuration.md#per-actor-sandbox-overrides) for limits and placement rules.

```python
from durable_actors import Actor, sandbox

@sandbox(cpu=2, memory_mib=2048, regions=["canada"], idle_timeout_ms=60_000)
class CustomerAgent(Actor):
    count: int = 0

    def increment(self) -> int:
        self.count += 1
        return self.count
```

## Deployment

Register the project through the [HTTP API](openapi.md) with a Python `actorEntrypoint`, such as `src/actors.py`; dependencies come from `requirements.txt` or `pyproject.toml` and must match the runtime's SDK version. Include extra resources in `pyproject.toml` and load them with `importlib.resources`:

```toml
[tool.durable-actors]
include = ["templates/*.txt", "data/*.json"]
```

Use editor hover or `help()` for full API signatures, including `ActorSessionTransport` for renewable sessions and `ActorInvocationError` for failures.
