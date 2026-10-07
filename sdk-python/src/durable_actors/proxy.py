"""Typed authorization data for granting browser WebSocket access."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, TypeAdapter

from .client import ActorTransport, SocketGrant, default_client

Metadata = TypeVar("Metadata")


class SocketAuthorization(BaseModel, Generic[Metadata]):
    """Actor access already approved by an application backend.

    Authenticate the user and authorize actor access before issuing a grant.
    Generated actors.Name.Authorization narrows the actor name and metadata.
    home_region optionally overrides placement.
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    actor_name: str
    actor_id: str
    metadata: Metadata
    home_region: str | None = None


def prepare_authorization(
    authorization: SocketAuthorization[Any], transport: ActorTransport | None = None
) -> SocketGrant:
    """Issue a grant for backend-approved actor access using the managed transport."""
    adapter: TypeAdapter[Any] = TypeAdapter(Any)
    metadata = adapter.dump_python(
        authorization.metadata, mode="json", by_alias=True, exclude_unset=True
    )
    return (transport if transport is not None else default_client()).prepare_websocket(
        authorization.actor_name,
        authorization.actor_id,
        metadata,
        home_region=authorization.home_region,
    )
