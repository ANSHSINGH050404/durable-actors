# Python SDK

The `little-actors` distribution provides actor authoring, a Python executor, and synchronous clients, driven by the shared TypeScript `durable-actors` CLI. Install `little-actors[codegen]` in development environments for generation and type checking. See the [quickstart](../../sdk-python/README.md) and [runnable example](../../examples/python).

## Actor definitions and typing

```python
from typing import Annotated, Literal
from pydantic import BaseModel, Field
from little_actors import Actor, emitted, ephemeral

class Message(BaseModel):
    role: Literal["user", "assistant"]
    text: Annotated[str, Field(min_length=1)]

class Chat(Actor):
    count: int = 0
    messages: list[Message] = emitted(default_factory=list)
    busy: bool = ephemeral(False)

    def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages
```

Annotated instance fields are persisted by default. `ephemeral()` excludes temporary values from persisted state and the public schema; use it for caches, locks, and service clients. `emitted()` persists a field and broadcasts its saved changes. `ClassVar` declarations are class constants and are not actor state.

Every RPC parameter, return value, and persisted field needs a concrete annotation. Supported schema types include JSON primitives, typed collections with string dictionary keys, fixed tuples, optional values, literals, unions (including discriminated unions), recursive models, Pydantic models, dataclasses, `typing_extensions.TypedDict`, dates, datetimes, and UUIDs. `JsonValue` explicitly describes arbitrary nested JSON. Bare collections, `Any`, and `object` are rejected at public boundaries. Ephemeral fields can hold Python objects such as API clients and locks.

Source annotations produce JSON Schema. Generated clients have ordinary synchronous methods and independent Pydantic models that describe the wire representation. Access those models through the actor namespace, such as `actors.Chat.Message`; actor implementation classes and source-only dependencies are not needed by consumers. Models whose names overlap namespace members such as `Stub` or `State` receive a `Model` suffix (repeated if needed to avoid another name collision). Schema constraints are generated into client models. When serialization changes a model shape, separate `Input` and `Output` models describe the two wire types; computed fields appear in outputs. Source-only Python validators and methods remain on the actor side, so the actor validates every call as well.

Public SDK classes and methods include docstrings for IDE hover and `help()`. Actor class and RPC method docstrings travel with the published contract and are preserved in generated clients, including generation from a running server. Pydantic model docstrings and `Field(description=...)` descriptions also appear in generated model documentation. Generated constructors, socket helpers, subscriptions, and state models document their SDK behavior. RPCs without author-supplied documentation receive a generic invocation description.

Inline annotations and a `py.typed` marker support mypy and Pyright without a custom checker plugin. Tests verify both valid calls and rejection of invalid calls, including generated model return types. Runtime validation is strict: a string is not coerced into an integer RPC argument.

Classes extend `Actor` directly. Public `def` methods are synchronous RPCs; `_` methods are helpers. Properties and static methods are not RPCs. `get_connections`, `broadcast`, `connect`, `prepare_websocket`, `subscribe`, and `get` are reserved. Lifecycle hooks are described below. Positional, positional-only, keyword-only, defaulted, and final variadic parameters are supported. The wire contract requires required parameters before optional parameters and variadic parameters last; `**kwargs` is unsupported.

Generated clients fill omitted middle arguments only when the contract provides a default. If a later argument is supplied and an earlier optional argument has no schema default (as in TypeScript contracts), the client raises `ValueError` before sending the RPC. Trailing optional arguments can always be omitted.

Each field must have a default or a `default_factory`. Defaults are copied per instance. Both `emitted()` and `ephemeral()` accept a value or a factory; use the standard `dataclasses.field(default_factory=...)` for a persisted field that does not emit changes. Factories run when an actor activates, rather than during schema extraction. Actor constructors are not supported. For example:

```python
from threading import Lock
lock: Lock = ephemeral(default_factory=Lock)
```

Persisted fields beginning with `_` stay out of the public socket state schema. They still exist in persisted storage and administrative state inspection. `emitted()` requires a public field and broadcasts its saved changes to connected clients. Undeclared instance fields are rejected when saving state.

## Execution and failures

Methods run serially by default. Use ordinary `def` for RPCs and socket hooks. Synchronous handlers run on a worker thread, so blocking I/O does not block executor communication. Actor methods, hooks, and clients use a synchronous API. Asyncio is an internal executor detail. A failed invocation restores persisted state to its previous snapshot. Ephemeral defaults are recreated when restoring or reactivating an actor. Use `self.id` within methods and hooks to read the current actor ID.

`@reentrant` supports ordinary `def` methods and socket hooks. A decorated invocation allows another call to enter before it finishes. Handlers run concurrently on worker threads, including while doing blocking I/O. Ordinary invocations still serialize with each other and prevent new calls from entering until completion, but already-running reentrant handlers may continue. Calling a decorated method directly through `self` inherits the outer invocation's policy.

```python
import time
from little_actors import Actor, reentrant

class Counter(Actor):
    count: int = 0

    def increment(self) -> int:
        self.count += 1
        return self.count

    @reentrant
    def wait(self, seconds: float) -> str:
        time.sleep(seconds)
        return self.id
```

Another client can call `increment()` while `wait()` is sleeping. The generated clients remain synchronous, so overlapping client calls need separate threads or processes.

Reentrant handlers share a live instance, and worker threads may overlap between arbitrary Python statements. Protect shared read-modify-write operations with an ephemeral `threading.Lock`, and keep blocking I/O outside the lock. This differs from TypeScript's single-threaded interleaving. Enabling reentrancy disables error rollback for the entire actor class: an exception must not erase another call's successful changes. Successful completions snapshot shared state, including changes visible from other in-flight invocations; completion sequences preserve commit ordering in Rust.

Actor code runs in a replaceable child process. Eviction terminates that process, including blocked handlers, while the supervisor retains the executor connection. The next call starts a fresh worker and restores saved state. Already-published socket effects are acknowledged before failed calls complete; terminated calls cannot publish additional effects or commit state. Completion sequence numbers continue across worker replacements. Workers also exit when their supervisor disappears.

The synchronous HTTP client caches direct actor routes, refreshes stale routes, and retries only a rejection known to precede execution. `ActorInvocationError` exposes `code` and `request_id`. A lost response raises `outcome_unknown`; automatically replaying it could repeat actor side effects.

## State subscriptions

Generated actors with emitted fields expose `subscribe(callback)`. The callback receives the complete current emitted state, including typed nested models:

```python
from generated import actors

chat = actors.Chat.get("lobby")
subscription = chat.subscribe(lambda state: print(state.messages))
chat.append(actors.Chat.Message(role="user", text="hello"))
```

The SDK delivers the initial snapshot and applies subsequent changes and removals before calling your callback. Omitted fields are preserved, changed fields are replaced, and removed optional fields return to their omitted/default state. Each callback receives a new model; mutating it does not affect later snapshots. Only emitted fields are included.

A subscription owns a WebSocket and receives events on a background thread, with callbacks executed serially on that thread. Keep the application running while listening. `subscription.close()` stops receiving, closes the socket, and waits for an active callback to finish; it can also be called from a callback. A subscription supports `with` for scoped cleanup. `subscription.closed` indicates that its receiver has finished.

Connection, validation, and callback failures stop the subscription and are available as `subscription.error`. Pass `on_error=handler` to receive the exception on the subscription thread; otherwise it is logged. Connection setup failures raise directly from `subscribe`. A normal remote close ends the subscription without an error. Reconnect explicitly by creating a new subscription.

Connection metadata is optional only when its declared type accepts `None`. For a typed `Member` model, use `chat.subscribe(callback, metadata=actors.Chat.Metadata(name="Ada"))`; the generator and type checker enforce the metadata type. Application messages are not delivered to state callbacks; use the lower-level connection API below for those.

## Typed WebSockets

Declare `Actor[Metadata, Incoming, Outgoing, Tag]` to type both ends of the socket and its allowed tags. `Metadata` and `Incoming` default to `JsonValue`, `Outgoing` defaults to `Incoming`, and `Tag` defaults to `str`. Use a `Literal` union for restricted tags; both the type checker and runtime validate them.

```python
from typing import Literal
from pydantic import BaseModel
from little_actors import Actor, ActorSocket

class Member(BaseModel):
    name: str

Tag = Literal["members", "admins"]

class Room(Actor[Member, Message, Message, Tag]):
    def on_connect(self, socket: ActorSocket[Member, Message, Tag]) -> None:
        socket.set_tags("members")

    def on_message(self, socket: ActorSocket[Member, Message, Tag], message: Message) -> None:
        self.broadcast(message)

    def on_disconnect(
        self, socket: ActorSocket[Member, Message, Tag], code: int,
        reason: str, was_clean: bool,
    ) -> None:
        pass
```

Import `ActorSocket` from `little_actors`. `self.get_connections()` returns typed sockets in synchronous handlers. Each socket has `id`, `metadata`, `tags`, and `state`, with `send`, `close`, `reject`, and `set_tags` operations. Assign `socket.metadata` to update it. `reject()` defaults to application close code 4003 and is valid only during connection. Socket handles are scoped to the active invocation.

```python
from generated import actors
from little_actors import StateSnapshot, StateUpdate

room = actors.Room.get("lobby")
with room.connect(actors.Room.Metadata(name="Ada")) as connection:
    connection.send(actors.Room.Incoming(role="user", text="hello"))
    for event in connection:
        if isinstance(event, StateSnapshot):
            print(event.state)
        elif isinstance(event, StateUpdate):
            print(event.changes.model_dump(exclude_unset=True), event.removed)
        else:
            print(event.text)
```

`connection.receive()` blocks until a message arrives; pass `timeout=5` to wait up to five seconds and raise `TimeoutError` if no message arrives. Use a `with` block to close the WebSocket connection, or call `connection.close()` explicitly.

`StateSnapshot` contains all emittable fields; `StateUpdate.changes` contains changed fields. Optional fields without a schema default use the typed `UNSET` sentinel, exported from `little_actors`. This preserves the distinction between omission and an explicit `None`. Use `model_fields_set`, `isinstance(value, Unset)`, or `model_dump(exclude_unset=True)` when processing patches. `prepare_websocket(metadata)` returns a short-lived grant when another process will connect. Reconnect explicitly with a new grant after expiry or connection loss.

Metadata is limited to 64 KiB and messages to 16 MiB. Application messages cannot use the reserved top-level types `state` or `state_update`.

## CLI and configuration

| Command | Purpose |
| --- | --- |
| `durable-actors init DIRECTORY --template python` | Create a typed counter project with Python and Node manifests. |
| `durable-actors dev` | Type-check and run actors; check and reload Python edits. |
| `durable-actors dev --no-watch` | Run without source watching. |
| `durable-actors generate [src/actors.py] --out-dir generated` | Generate from source, or the server when source is omitted, then type-check the client. |
| `durable-actors generate --language python` | Explicitly select Python clients for a published schema. |

Use the existing Node CLI (`pnpm exec durable-actors`). The Python package has no console command. Install `little-actors[codegen]` in your project's virtual environment. The CLI runs strict mypy before source compilation and reload, and after generation. Invalid edits leave the last working deployment running. The SDK and generated-client tests also validate Pyright compatibility.

The CLI selects `DURABLE_ACTORS_PYTHON`, then the active `VIRTUAL_ENV`, then the project's `.venv/bin/python`, then `python3`. Set `DURABLE_ACTORS_ENTRYPOINT=src/actors.py` in `.env`; the Python template creates this setting. `.env.local` and `.env` work just as they do for TypeScript projects. `DURABLE_ACTORS_PROJECT` overrides the project directory, and `DURABLE_ACTORS_DATA_DIR` the local state directory. `dev --port 0` selects a free port. `DURABLE_ACTORS_BINARY` selects a native executable instead of downloading it. Keep native runtime, TypeScript CLI, and Python SDK versions aligned.

Import `actors` from the generated package and obtain a typed handle with `actors.Chat.get("lobby")`. The factory makes no network request. `actors.Chat.Stub` is the handle type; `actors.Chat.Methods.append.Args` and `.Result` describe an RPC's ordered argument tuple and return value. Optional arguments use tuple unions and variadic arguments use unpacked tuple types. Supply keyword-only arguments by name when calling the method. Public persisted state is `actors.Chat.State`; subscription callbacks receive `actors.Chat.EmittedState`, containing only emitted fields. `actors.Chat.StatePatch` describes partial updates. The SDK creates one shared HTTP client when first needed and closes it at process exit. It reads environment configuration at that first construction.

For custom configuration or independent client lifetimes, pass an explicit `Client` as the second argument to `.get()` and close it with a `with` block or `.close()`. Client constructor arguments override environment variables.

Set `DURABLE_ACTORS_CONTROL_PLANE_URL`, `DURABLE_ACTORS_PROJECT_ID`, `DURABLE_ACTORS_SECRET`, and optionally `DURABLE_ACTORS_HOME_REGION`. Local defaults are `http://127.0.0.1:7100` and project `local`. Remote origins require a project ID. An injected `httpx.Client` lets applications supply transport and timeout policy; its lifecycle remains with the application. CLI commands load `.env.local` and `.env`, preserving exported environment overrides.

Generation from local source imports that source. Generation from a server consumes schemas and does not execute the actor implementation.

## Source-class references and backend broadcasts

When the actor source is available, call it directly without generation:

```python
from my_app.actors import Chat, Message

chat = Chat.get("lobby")
messages = chat.append(Message(role="user", text="hello"))
```

`get()` preserves synchronous method signatures and uses the shared client. It does not construct an actor or run field factories locally. Actor fields remain remote; access state through an RPC or generated subscription. References also support typed `connect(metadata)` and `broadcast(message)`. Filtered broadcasts belong in actor methods through `self.broadcast(...)`.

Actor methods may call other actors using these references. Calls follow the callee's normal serialization and reentrancy policy. A synchronous call back into an actor waiting for that call requires a reentrant entry point.

Generated handles support `actors.Chat.get("lobby").broadcast(message)` for sending application messages to connected clients. Broadcasts are not persisted, and uncertain deliveries are not automatically retried.

## Browser authorization and renewable sessions

After authenticating a user and checking their access, issue a browser WebSocket grant:

```python
from generated import actors, ActorAuthorization, ActorProxy

access: ActorAuthorization = actors.Room.Authorization(
    actor_id="lobby",
    metadata=actors.Room.Metadata(name="Ada"),
    home_region="canada",
)
grant = ActorProxy.handle(access)
# For a single actor, actors.Room.prepare_websocket(access) also works.
```

The authorization type binds the actor name to its metadata type. Pass an optional `Client` as the second argument to either helper for custom configuration. The default authorization lifetime is fifteen minutes; `authorization_lifetime_ms` accepts 1,000–86,400,000. `home_region` overrides the client's placement preference for socket setup. Treat the grant URL as a credential.

For clients using short-lived application sessions, pass an `ActorSessionTransport`:

```python
from generated import actors
from little_actors import ActorSession, ActorSessionTransport

def get_session() -> ActorSession:
    return authorize_with_your_backend()

with ActorSessionTransport(project_id="my-project", get_session=get_session) as session:
    chat = actors.Chat.get("lobby", session)
    print(chat.append(actors.Chat.Message(role="user", text="hello")))
```

`ActorSession` contains `project_id`, `control_plane_url`, `token`, and `expires_at_ms` (Unix milliseconds). Sessions require HTTPS outside localhost and about one minute of validity. `get_session` is synchronous; raise `ActorSessionRejectedError` when the backend denies or revokes access. The SDK renews sessions on a background thread, coalesces concurrent refreshes, stops renewal after a minute without activity, and keeps old transports alive until their in-flight requests finish. Explicitly close the session or use `with`.

Set `DURABLE_ACTORS_TELEMETRY=1` for JSON invocation timing on stderr, or pass a `telemetry` callback to `Client`. Events include the request ID, actor, method, outcome, and elapsed time, without arguments, results, or credentials.

## Sandbox resources

```python
from little_actors import Actor, sandbox

@sandbox(cpu=2, memory_mib=2048, regions=["canada"], idle_timeout_ms=60_000)
class CustomerAgent(Actor):
    count: int = 0

    def increment(self) -> int:
        self.count += 1
        return self.count
```

These are the same per-actor overrides as TypeScript's `@Sandbox`: CPU request and cap in cores (0.1–64, in 0.001 increments), memory request and cap in MiB (128–262144), allowed regions, and inactivity before eviction in milliseconds (1–86400000). Omitted values inherit deployment defaults. Regions must be nonempty and unique; their order is not a priority and existing actors retain their saved region. `SandboxRegion` and `SandboxOptions` are exported for typed configuration. See [configuration](configuration.md#per-actor-sandbox-overrides) for server defaults and placement rules.

## Deployment

The runtime image includes Python 3.13 and this SDK. Register a hosted deployment through the [HTTP API](openapi.md), using the source image's project directory and a Python `actorEntrypoint`, such as `actors.py`. The Modal builder installs dependencies, extracts the contract, and snapshots `actors.pyz` and its sibling `python/` dependency directory. Python hosts implement the existing executor protocol, persistence, residency, and socket effects. Spare sandboxes switch to a Python executor upon assignment; Python itself is not prewarmed in the shared Bun spare pool.

Hosted compilation has a ten-minute budget for dependency installation and packaging; the enclosing build request allows twelve minutes for provisioning and snapshotting. Build dependencies come from `requirements.txt` when present, otherwise from `[project].dependencies` in `pyproject.toml`. Pin dependencies for reproducible builds. The SDK and its runtime dependencies must match the image's installed versions. The builder applies these versions as pip constraints and removes duplicate copies from the deployment bundle; incompatible requirements fail during dependency resolution. The hosted Python version and platform must support any native dependencies; build them in the runtime image, rather than copying a macOS virtual environment into a Linux deployment.

Python source is packaged from the project, excluding hidden directories, `node_modules`, `venv`, `__pycache__`, `dist`, `target`, `generated`, and any directory containing `pyvenv.cfg`. Export actor classes from the entrypoint; an optional `__all__` controls exports. Explicitly include resources as needed:

```toml
[tool.little-actors]
include = ["templates/*.txt", "data/*.json"]
```

Use `importlib.resources` for packaged resources. Source and resources are limited to 32 MiB; dependency packages live outside the source archive. Each executor is permanently assigned to one actor identity. The generated clients in this SDK target Python.

## Releases

Python package versions are stamped and verified with the native and TypeScript versions by `scripts/release.mjs`. CI runs Python 3.11, 3.13, and 3.14, checks mypy and Pyright, and tests against a built Rust runtime. The release workflow publishes the tested wheel and source distribution through PyPI trusted publishing after native and image validation.

Before the first publication, the package owner must configure the `little-actors` PyPI project (or a pending publisher) for GitHub owner `TerseAI`, repository `durable-actors`, workflow `release.yml`, environment `pypi`. See [PyPI's trusted publishing setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
