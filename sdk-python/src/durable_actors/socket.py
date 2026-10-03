"""Actor-side socket handles and invocation-scoped socket effects."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from functools import partial
from threading import get_ident
from typing import Any, Generic, Literal, Protocol, TypeVar, cast

from pydantic import TypeAdapter
from typing_extensions import TypeVar as DefaultTypeVar

from .contract import Document, decode, encode
from .guards import is_document

Metadata = TypeVar("Metadata")
Outgoing = TypeVar("Outgoing")
Tag = DefaultTypeVar("Tag", bound=str, default=str)
SocketState = Literal["connecting", "open", "closed"]
T = TypeVar("T")


class Effects(Protocol):
    async def publish(self, effects: list[Document]) -> None: ...
    async def get_connections(
        self, tag: str | None = None, count_only: bool = False
    ) -> list[Document] | int: ...
    def admit(self) -> None: ...


class ActorSocket(Generic[Metadata, Outgoing, Tag]):
    """Actor-side handle to one typed WebSocket connection.

    Supplied to socket hooks or returned by Actor.get_connections(). Use the
    handle only during its current invocation. id identifies the connection;
    metadata and tags support connection selection and application context.
    """

    def __init__(
        self, connection: Document, scope: SocketScope, state: SocketState = "open"
    ) -> None:
        self.id: str = connection["id"]
        self._scope = scope
        self._metadata: Metadata = decode(scope.metadata, connection["metadata"])
        self._tags: tuple[Tag, ...] = cast(
            tuple[Tag, ...],
            tuple(scope.tag.validate_python(tag, strict=True) for tag in connection["tags"]),
        )
        self._state: SocketState = state

    @property
    def state(self) -> SocketState:
        """Current handle state: "connecting", "open", or "closed"."""
        return self._state

    @property
    def metadata(self) -> Metadata:
        """Typed connection metadata.

        Assign a replacement value to save changes. Mutating the returned value
        in place does not publish a metadata update. Encoded metadata is limited
        to 16 KiB.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, value: Metadata) -> None:
        """Validate and publish replacement connection metadata."""
        encoded = encode(self._scope.metadata, value)
        if len(json.dumps(encoded, separators=(",", ":")).encode()) > 16 * 1024:
            raise ValueError("socket metadata exceeds 16 KiB")
        self._scope.push(
            {
                "type": "set_metadata",
                "connection_id": self.id,
                "metadata": encoded,
            }
        )
        self._metadata = value

    @property
    def tags(self) -> tuple[Tag, ...]:
        """Current connection tags; replace them with set_tags()."""
        return self._tags

    def send(self, message: Outgoing) -> None:
        """Queue a value matching the actor's outgoing application-message type.

        Incoming WebSocket messages are limited to 32 MiB. The top-level message types
        "state" and "state_update" are reserved for emitted state.
        """
        if self._state == "closed":
            raise ValueError("cannot send on a closed socket")
        self._scope.push(
            {"type": "send", "connection_id": self.id, "message": self._scope.message(message)}
        )

    def close(self, code: int = 1000, reason: str = "") -> None:
        """Close this connection with code 1000 or an application code 3000-4999.

        The UTF-8 reason must fit within 123 bytes.
        """
        self._close("close", code, reason)

    def reject(self, code: int = 4003, reason: str = "connection rejected") -> None:
        """Reject a connecting socket from on_connect(); defaults to code 4003."""
        if self._state != "connecting":
            raise ValueError("only a connecting socket can be rejected")
        self._close("reject", code, reason)

    def set_tags(self, *tags: Tag) -> None:
        """Replace tags used by Actor.broadcast() selectors.

        Tags are deduplicated. At most 10 nonempty tags are allowed, with up to
        256 characters per tag.
        """
        checked = self._scope.validate_tags(tags)
        self._scope.push({"type": "set_tags", "connection_id": self.id, "tags": checked})
        self._tags = cast(tuple[Tag, ...], tuple(checked))

    def _close(self, kind: str, code: int, reason: str) -> None:
        if type(code) is not int or (code != 1000 and not 3000 <= code <= 4999):
            raise ValueError("close code must be 1000 or between 3000 and 4999")
        if len(reason.encode()) > 123:
            raise ValueError("close reason exceeds 123 bytes")
        if self._state != "closed":
            self._scope.push(
                {"type": kind, "connection_id": self.id, "code": code, "reason": reason}
            )
            self._state = "closed"


class SocketScope:
    def __init__(
        self,
        instance: object,
        actor_id: str,
        types: tuple[Any, Any, Any, Any],
        effects: Effects,
        connections: list[Document] | None,
        live: bool,
    ) -> None:
        self.loop = asyncio.get_running_loop()
        self.thread = get_ident()
        self.requests: set[asyncio.Task[Any]] = set()
        self.instance = instance
        self.actor_id = actor_id
        self.metadata, self.incoming, self.outgoing, self.tag = (
            TypeAdapter(hint) for hint in types
        )
        self.effects = effects
        self.live = live
        self.active = True
        self.pending: list[Document] = []
        self.output: asyncio.Task[None] | None = None
        self.failure: Exception | None = None
        self.sockets: dict[str, ActorSocket[Any, Any, Any]] = {}
        for connection in connections or []:
            self.socket(connection)

    def blocking(self, operation: Callable[[], Coroutine[Any, Any, T]]) -> T:
        if get_ident() == self.thread:
            raise RuntimeError("blocking actor operations must run on the handler thread")
        self.ensure_active()
        return asyncio.run_coroutine_threadsafe(self.exchange(operation), self.loop).result()

    async def exchange(self, operation: Callable[[], Coroutine[Any, Any, T]]) -> T:
        self.ensure_active()
        task = asyncio.current_task()
        assert task is not None
        self.requests.add(task)
        try:
            return await operation()
        finally:
            self.requests.discard(task)

    def cancel(self) -> None:
        self.active = False
        for request in self.requests:
            request.cancel()
        if self.output is not None:
            self.output.cancel()

    async def get_connections(self, tag: str | None = None) -> list[ActorSocket[Any, Any, Any]]:
        self.ensure_active()
        if tag is not None:
            validate_tags((tag,))
        connections = await self.effects.get_connections(tag=tag)
        if isinstance(connections, int):
            raise ValueError("expected socket list")
        sockets = [self.socket(connection) for connection in connections]
        ids = {socket.id for socket in sockets}
        return sockets + [
            socket
            for socket in self.sockets.values()
            if socket.state == "connecting"
            and socket.id not in ids
            and (tag is None or tag in socket.tags)
        ]

    async def get_connection_count(self) -> int:
        self.ensure_active()
        count = await self.effects.get_connections(count_only=True)
        if not isinstance(count, int):
            raise ValueError("expected socket count")
        return count + sum(socket.state == "connecting" for socket in self.sockets.values())

    def set_websocket_auto_response(
        self, request: str | None = None, response: str | None = None
    ) -> None:
        if (request is None) != (response is None):
            raise ValueError("automatic response requires both request and response")
        if any(value is not None and len(value) > 2048 for value in (request, response)):
            raise ValueError("automatic response exceeds 2048 characters")
        self.push({"type": "set_auto_response", "request": request, "response": response})

    def socket(
        self, connection: Document, state: SocketState = "open"
    ) -> ActorSocket[Any, Any, Any]:
        if state != "open" or connection["id"] not in self.sockets:
            self.sockets[connection["id"]] = ActorSocket(connection, self, state)
        return self.sockets[connection["id"]]

    def broadcast(
        self, message: Any, except_ids: tuple[str, ...], tags: tuple[str, ...], tag_match: str
    ) -> None:
        if tag_match not in ("all", "any"):
            raise ValueError("invalid broadcast selectors")
        self.push(
            {
                "type": "broadcast",
                "message": self.message(message),
                "except_connection_ids": list(except_ids),
                "tags": self.validate_tags(tags),
                "tag_match": tag_match,
            }
        )

    def validate_tags(self, tags: tuple[str, ...]) -> list[str]:
        return validate_tags(tuple(self.tag.validate_python(tag, strict=True) for tag in tags))

    def message(self, value: Any) -> Document:
        encoded = encode(self.outgoing, value)
        if is_document(encoded) and encoded.get("type") in {"state", "state_update"}:
            raise ValueError("state and state_update messages are reserved")
        data = json.dumps(encoded, separators=(",", ":"), allow_nan=False)
        return {"type": "text", "data": data}

    def push(self, effect: Document) -> None:
        if get_ident() != self.thread:
            self.blocking(partial(self.enqueue, effect))
            return
        self.ensure_active()
        if self.failure:
            raise self.failure
        self.pending.append(effect)
        if self.live and self.output is None:
            self.output = asyncio.create_task(self.drain())

    async def enqueue(self, effect: Document) -> None:
        self.push(effect)

    async def finish(self) -> list[Document]:
        if self.output is not None:
            await self.output
        if self.failure:
            raise self.failure
        return list(self.pending)

    async def drain(self) -> None:
        try:
            while self.pending:
                batch, self.pending = self.pending, []
                await self.effects.publish(batch)
        except Exception as error:
            self.failure = error
        finally:
            self.output = None

    def ensure_active(self) -> None:
        if not self.active:
            raise ValueError("socket scope is no longer active")


scope_context: ContextVar[SocketScope] = ContextVar("actor_socket_scope")


def current_scope(instance: object) -> SocketScope:
    scope = scope_context.get()
    scope.ensure_active()
    if scope.instance is not instance:
        raise ValueError("actor is outside its invocation")
    return scope


def validate_tags(tags: tuple[str, ...]) -> list[str]:
    unique = list(dict.fromkeys(tags))
    if len(unique) > 10 or any(not tag or len(tag) > 256 for tag in unique):
        raise ValueError("invalid socket tags")
    return unique
