"""Typed authorization data for granting browser WebSocket access."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from .client import ActorTransport, SocketGrant, default_client

Metadata = TypeVar("Metadata")


class SocketAuthorization(BaseModel, Generic[Metadata]):
    """Actor access already approved by an application backend.

    Authenticate the user and authorize actor access before issuing a grant.
    Generated actors.Name.Authorization narrows the actor name and metadata.
    home_region optionally overrides placement; authorization_lifetime_ms
    defaults to fifteen minutes and accepts one second through one day.
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    actor_name: str
    actor_id: str
    metadata: Metadata
    home_region: str | None = None
    authorization_lifetime_ms: int = Field(default=900000, ge=1000, le=86400000)


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
        authorization_lifetime_ms=authorization.authorization_lifetime_ms,
    )
