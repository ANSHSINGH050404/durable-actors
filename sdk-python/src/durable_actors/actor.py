"""Define durable actors and control invocation concurrency."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Generic, Literal, Self, TypeVar

from typing_extensions import TypeVar as DefaultTypeVar

from .database import ActorDatabase, actor_database
from .json import JsonValue

if TYPE_CHECKING:
    from .client import ActorTransport
    from .connection import Connection
    from .socket import ActorSocket

Metadata = DefaultTypeVar("Metadata", default=JsonValue)
Incoming = DefaultTypeVar("Incoming", default=JsonValue)
Outgoing = DefaultTypeVar("Outgoing", default=Incoming)
Tag = DefaultTypeVar("Tag", bound=str, default=str)
F = TypeVar("F", bound=Callable[..., Any])


class Actor(Generic[Metadata, Incoming, Outgoing, Tag]):
    """Base class for durable actors with typed RPCs and WebSocket hooks.

    Public def methods become synchronous RPCs. Every field must declare persisted()
    or ephemeral(); emitted(persisted(...)) also broadcasts saved changes.
    Use field defaults or factories instead of a constructor.

    Generic parameters describe connection metadata, incoming application
    messages, outgoing application messages, and allowed connection tags. Outgoing
    defaults to Incoming, tags to str, and other parameters to JsonValue.
    """

    def __init__(self) -> None:
        """Initialize independent field defaults; the runtime restores persisted values afterward."""
        from .contract import describe_actor

        for field in describe_actor(type(self)).fields.values():
            value = (
                field.default_factory()
                if field.default_factory is not None
                else deepcopy(field.default)
            )
            setattr(self, field.name, value)

    @classmethod
    def get(cls, actor_id: str, transport: ActorTransport | None = None) -> Self:
        """Return a typed synchronous reference without activating the actor locally.

        RPC signatures match the source class. Fields and lifecycle hooks belong
        to the running actor; read state through RPCs or generated subscriptions.
        """
        from .reference import actor_reference

        return actor_reference(cls, actor_id, transport)

    def connect(self, metadata: Metadata) -> Connection[Incoming, Outgoing, JsonValue, JsonValue]:
        """Open a typed WebSocket through a source-class reference from get()."""
        raise RuntimeError("connect() requires an actor reference from get()")

    @property
    def id(self) -> str:
        """Identity of this actor, available during an active invocation or socket hook."""
        from .socket import current_scope

        return current_scope(self).actor_id

    @property
    def db(self) -> ActorDatabase:
        """Actor-local SQLite. Changes commit with persisted fields after a successful invocation."""
        return actor_database(self)

    def on_connect(self, socket: ActorSocket[Metadata, Outgoing, Tag]) -> None:
        """Handle a new WebSocket connection before it is accepted.

        Override with def to inspect metadata, set tags, send a welcome
        message, or reject with socket.reject(). The default accepts the connection.
        """
        pass

    def on_message(self, socket: ActorSocket[Metadata, Outgoing, Tag], message: Incoming) -> None:
        """Handle a validated incoming application message from a connected client.

        Override with def to update actor state or send typed replies
        through socket. The default ignores application messages.
        """
        pass

    def on_disconnect(
        self, socket: ActorSocket[Metadata, Outgoing, Tag], code: int, reason: str, was_clean: bool
    ) -> None:
        """Handle a closed connection; override with def for cleanup.

        Args:
            socket: Connection with its last known metadata and tags.
            code: WebSocket close status code.
            reason: WebSocket close reason.
            was_clean: Whether the connection completed a clean closing handshake.
        """
        pass

    def get_connections(self, tag: Tag | None = None) -> list[ActorSocket[Metadata, Outgoing, Tag]]:
        """Return connected sockets from a synchronous actor method or hook.

        Use the returned handles only during the current invocation.
        """
        from .socket import current_scope

        scope = current_scope(self)
        return scope.blocking(lambda: scope.get_connections(tag))

    def get_connection_count(self) -> int:
        """Return the active connection count without fetching the connection list."""
        from .socket import current_scope

        scope = current_scope(self)
        return scope.blocking(scope.get_connection_count)

    def set_websocket_auto_response(
        self, request: str | None = None, response: str | None = None
    ) -> None:
        """Match raw text frames without waking the actor. Omit both values to clear."""
        from .socket import current_scope

        current_scope(self).set_websocket_auto_response(request, response)

    def broadcast(
        self,
        message: Outgoing,
        *,
        except_ids: tuple[str, ...] = (),
        tags: tuple[Tag, ...] = (),
        tag_match: Literal["all", "any"] = "all",
    ) -> None:
        """Queue a typed application message for selected connections.

        Args:
            message: Value matching the actor's outgoing message type.
            except_ids: Connection IDs to exclude.
            tags: Restrict delivery to matching tags; empty selects all connections.
            tag_match: Use "all" to require every tag or "any" to require one.

        Must be called during an active actor invocation or socket hook.
        """
        from .socket import current_scope

        current_scope(self).broadcast(message, except_ids, tags, tag_match)


def interleave(method: F) -> F:
    """Allow other invocations to enter before this method finishes.

    Works with def RPCs and socket hooks. Handlers overlap on worker threads.
    Ordinary invocations still serialize with each other and block new entries.
    Nested method calls inherit the outer invocation's admission policy.

    Reentrant actors share a live instance. Coordinate shared mutable state
    across overlapping handlers. Failed invocations do not roll back state
    anywhere in the actor class, because that could erase another call's work.
    """
    setattr(method, "__actor_reentrant__", True)
    return method
