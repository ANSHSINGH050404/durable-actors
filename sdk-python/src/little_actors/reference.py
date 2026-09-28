"""Typed synchronous references using source actor definitions."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast

from pydantic import TypeAdapter

from .actor import Actor
from .client import ActorTransport, component, default_client
from .connection import Connection
from .contract import Method, decode, describe_actor, encode
from .json import JsonValue

A = TypeVar("A", bound=Actor[Any, Any, Any, Any])


def actor_reference(actor: type[A], actor_id: str, transport: ActorTransport | None) -> A:
    component(actor_id, 128)
    definition = describe_actor(actor)
    client = transport if transport is not None else default_client()
    metadata_adapter, incoming, outgoing = (
        TypeAdapter(hint) for hint in definition.socket_types[:3]
    )

    def connect(self: A, metadata: Any) -> Connection[Any, Any, JsonValue, JsonValue]:
        grant = client.prepare_websocket(
            actor.__name__, actor_id, encode(metadata_adapter, metadata)
        )
        return Connection[Any, Any, JsonValue, JsonValue].open(
            grant, incoming, outgoing, TypeAdapter(JsonValue), TypeAdapter(JsonValue)
        )

    def broadcast(
        self: A,
        message: Any,
        *,
        except_ids: tuple[str, ...] = (),
        tags: tuple[str, ...] = (),
        tag_match: str = "all",
    ) -> None:
        if except_ids or tags or tag_match != "all":
            raise ValueError(
                "source references broadcast to all connections; filtered broadcasts belong in actor methods"
            )
        client.broadcast(actor.__name__, actor_id, encode(outgoing, message))

    members: dict[str, Any] = {
        name: remote_method(actor, actor_id, client, name, method)
        for name, method in definition.methods.items()
    }
    members.update({name: property(unavailable) for name in definition.fields})
    members.update(
        {"connect": connect, "broadcast": broadcast, "id": property(lambda self: actor_id)}
    )
    reference = type(f"{actor.__name__}Reference", (actor,), members)
    return cast(A, object.__new__(reference))


def remote_method(
    actor: type[A], actor_id: str, client: ActorTransport, name: str, method: Method
) -> Callable[..., Any]:
    @wraps(getattr(actor, name))
    def invoke(self: A, *args: Any, **kwargs: Any) -> Any:
        bound = method.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        values: list[Any] = []
        for key, value in bound.arguments.items():
            encoded = encode(
                method.parameters[key],
                list(value)
                if method.signature.parameters[key].kind is inspect.Parameter.VAR_POSITIONAL
                else value,
            )
            if method.signature.parameters[key].kind is inspect.Parameter.VAR_POSITIONAL:
                values.extend(encoded)
            else:
                values.append(encoded)
        result = client.invoke(actor.__name__, actor_id, name, values)
        return decode(method.result, result)

    return invoke


def unavailable(self: object) -> Any:
    raise AttributeError("actor state is remote; read it through an RPC or generated subscription")
