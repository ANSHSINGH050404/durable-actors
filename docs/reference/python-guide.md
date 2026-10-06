# Python guide

## Before you start

```bash
pnpm install
pnpm --dir sdk build
cd examples/bank-python
uv sync
cp .env.example .env
```

## Defining an actor

Each bank account is an actor. An actor has fields and methods. `persisted()` saves a field after each successful method call.

To track a bank balance, define `src/actors.py`:

```python
from durable_actors import Actor, persisted


class BankAccount(Actor):
    _balance: int = persisted(0)

    def get_balance(self) -> int:
        return self._balance
```

## Generating a client

Start the local dev server from `examples/bank-python`. The first run downloads the runtime:

```bash
pnpm dev
```

Wait for `Ready`, then generate the client in a second terminal:

```bash
cd examples/bank-python
pnpm generate
```

This creates a `generated` package with typed stubs for your actors. Each actor instance is addressed by an ID. Create a `client.py` that gets the `demo` account and calls a method:

```python
from generated import actors

account = actors.BankAccount.get("demo")
print(account.get_balance())
```

Run it with `pnpm client`, which runs `uv run --env-file .env client.py` so the client connects to port 7112.

An actor starts when one of its methods is called, stays in memory while it is busy, and shuts down after sitting idle. Regenerate the client after changing an actor's methods.

## Ephemeral fields

Next, count the deposits made since the actor last woke up. `ephemeral()` keeps a field in memory only while the actor is awake. It resets when the actor shuts down.

```python
from durable_actors import Actor, ephemeral, persisted


class BankAccount(Actor):
    _balance: int = persisted(0)
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self._balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self._balance += amount
        self._deposits_since_wake += 1
        return self._balance
```

## WebSocket messages

Actors handle WebSocket connections directly. The three type parameters of `Actor` describe each connection. Payloads are JSON.

- **Metadata** identifies the client. In this example, it is the `user_id` passed when the client connects.
- **Incoming** messages are what a client can send. Here, `{"type": "ping"}`, which `on_message` answers with the balance.
- **Outgoing** messages are what the actor sends, through either `socket.send` or `broadcast`. Here, `{"type": "balance", "balance": ...}`.

The example also uses:

- **Tags.** `set_tags` labels one socket. `on_connect` tags each socket `"customer"`.
- **Automatic responses.** The gateway answers a raw text `ping` with `pong` without waking the actor. `on_connect` registers the pair. A JSON `{"type": "ping"}` still runs `on_message`.
- **Broadcasts.** `broadcast` sends to every connected socket. `deposit` limits it to sockets tagged `"customer"`, and `on_disconnect` uses `except_ids` to skip the socket that just left.
- **Individual sends.** `socket.send` sends to one socket. `on_connect` and `on_message` use it to send that client the current balance.

```python
from typing import Literal

from durable_actors import Actor, ActorSocket, ephemeral, persisted
from pydantic import BaseModel


class Metadata(BaseModel):
    user_id: str


class Incoming(BaseModel):
    type: Literal["ping"]


class Outgoing(BaseModel):
    type: Literal["balance"]
    balance: int


Socket = ActorSocket[Metadata, Outgoing]


class BankAccount(Actor[Metadata, Incoming, Outgoing]):
    _balance: int = persisted(0)
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self._balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self._balance += amount
        self._deposits_since_wake += 1
        self.broadcast(Outgoing(type="balance", balance=self._balance), tags=("customer",))
        return self._balance

    def on_connect(self, socket: Socket) -> None:
        socket.set_tags("customer")
        self.set_websocket_auto_response("ping", "pong")
        socket.send(Outgoing(type="balance", balance=self._balance))

    def on_message(self, socket: Socket, message: Incoming) -> None:
        socket.send(Outgoing(type="balance", balance=self._balance))

    def on_disconnect(self, socket: Socket, code: int, reason: str, was_clean: bool) -> None:
        self.broadcast(Outgoing(type="balance", balance=self._balance), except_ids=(socket.id,))
```

## Emitting state

`emitted()` publishes a public `persisted()` field to every connected client, so `_balance` becomes `balance`:

- When a client connects, it receives a `StateSnapshot` with the current emitted fields, here `message.state.balance`.
- After a successful call changes the field, clients receive a `StateUpdate` with the changes.

```python
from typing import Literal

from durable_actors import Actor, ActorSocket, emitted, ephemeral, persisted
from pydantic import BaseModel


class Metadata(BaseModel):
    user_id: str


class Incoming(BaseModel):
    type: Literal["ping"]


class Outgoing(BaseModel):
    type: Literal["balance"]
    balance: int


Socket = ActorSocket[Metadata, Outgoing]


class BankAccount(Actor[Metadata, Incoming, Outgoing]):
    balance: int = emitted(persisted(0))
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self.balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self.balance += amount
        self._deposits_since_wake += 1
        self.broadcast(Outgoing(type="balance", balance=self.balance), tags=("customer",))
        return self.balance

    def on_connect(self, socket: Socket) -> None:
        socket.set_tags("customer")
        self.set_websocket_auto_response("ping", "pong")
        socket.send(Outgoing(type="balance", balance=self.balance))

    def on_message(self, socket: Socket, message: Incoming) -> None:
        socket.send(Outgoing(type="balance", balance=self.balance))

    def on_disconnect(self, socket: Socket, code: int, reason: str, was_clean: bool) -> None:
        self.broadcast(Outgoing(type="balance", balance=self.balance), except_ids=(socket.id,))
```

A connecting client sees the snapshot, then updates as deposits land:

```python
from durable_actors import StateSnapshot, StateUpdate
from generated import actors

account = actors.BankAccount.get("demo")
with account.connect(actors.BankAccount.Metadata(user_id="ada")) as socket:
    for message in socket:
        if isinstance(message, StateSnapshot):
            print(message.state.balance)
        elif isinstance(message, StateUpdate):
            print(message.changes.balance)
        else:
            print(message)
```

## Interleaving calls

An actor runs one call at a time by default. Python methods run in a worker thread, and an `@interleave` method does not hold the actor while it runs: other calls can start at any point during it, not only while it waits on I/O. Protect state that overlapping calls share, for example with a `threading.Lock` in an `ephemeral()` field.

Add a second actor for wire transfers to `src/actors.py`:

```python
from durable_actors import Actor, emitted, interleave, persisted


class ClearingNetwork:
    def submit(self, reference: str) -> str:
        return f"receipt:{reference}"


clearing_network = ClearingNetwork()


class Wire(Actor):
    status: str = emitted(persisted("requested"))

    def get_status(self) -> str:
        return self.status

    @interleave
    def submit(self, reference: str) -> str:
        receipt = clearing_network.submit(reference)
        self.status = "submitted"
        return receipt
```

`get_status` can run while `submit` is still in progress.

Interleaving changes failure handling for the whole class. A failed call normally rolls back its field and SQLite changes. Once any method in a class uses `@interleave`, no call in that class rolls back, because overlapping calls may already have used the changes.

## Using SQLite

Each actor has its own SQLite database at `self.db`. `self.db.exec(sql, *bindings)` runs one statement and returns its rows as dictionaries. Here, `deposit` records each deposit in a ledger table:

```python
from durable_actors import Actor, emitted, persisted


class BankAccount(Actor):
    balance: int = emitted(persisted(0))

    def deposit(self, amount: int) -> int:
        self.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        self.balance += amount
        self.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        return self.balance
```

- SQL writes and `persisted()` fields commit together when the method or socket hook succeeds.
- Bind values with `?` placeholders. Values can be `str`, `int`, `float`, `bytes`, or `None`.
- `self.db` is available only while a method or socket hook runs.
- The runtime owns transactions and the database file, so `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `ATTACH`, `DETACH`, and `VACUUM` are rejected.
- The only allowed PRAGMAs are `table_info`, `table_xinfo`, `index_info`, `index_xinfo`, `index_list`, `foreign_key_list`, `foreign_key_check`, `integrity_check`, `quick_check`, and `user_version`.
- Names beginning with `__terse_` or `_litestream_` are reserved.

## Configuring actor resources

`@compute` overrides the deployment's default resources for one actor class:

```python
from durable_actors import Actor, compute, emitted, persisted


@compute(cpu=0.5, memory_mib=256, idle_timeout_ms=60_000)
class BankAccount(Actor):
    balance: int = emitted(persisted(0))
```
