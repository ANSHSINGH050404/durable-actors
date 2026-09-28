"""Per-actor sandbox resources and placement."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, TypeVar
from weakref import WeakKeyDictionary

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .actor import Actor
from .guards import is_actor

SandboxRegion = Literal[
    "canada",
    "north-america-east",
    "north-america-central",
    "north-america-south",
    "north-america-west",
    "europe-west",
    "asia-southeast",
]
A = TypeVar("A", bound=Actor[Any, Any, Any, Any])


class SandboxOptions(BaseModel):
    """Overrides for an actor's deployment defaults; omitted values are inherited.

    cpu is a request and cap in cores, memory_mib in MiB, and idle_timeout_ms
    in milliseconds. regions limits placement without expressing priority;
    existing actors retain their saved region.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, allow_inf_nan=False)
    cpu: float | None = Field(default=None, ge=0.1, le=64, multiple_of=0.001)
    memory_mib: int | None = Field(default=None, ge=128, le=262144, serialization_alias="memoryMiB")
    idle_timeout_ms: int | None = Field(
        default=None, ge=1, le=86400000, serialization_alias="idleTimeoutMs"
    )
    regions: list[SandboxRegion] | None = Field(default=None, min_length=1, max_length=7)

    @field_validator("regions")
    @classmethod
    def unique_regions(cls, value: list[SandboxRegion] | None) -> list[SandboxRegion] | None:
        if value is not None and len(value) != len(set(value)):
            raise ValueError("regions must be unique")
        return value


_options: WeakKeyDictionary[type[Actor[Any, Any, Any, Any]], SandboxOptions] = WeakKeyDictionary()


def sandbox(
    *,
    cpu: float | None = None,
    memory_mib: int | None = None,
    idle_timeout_ms: int | None = None,
    regions: list[SandboxRegion] | None = None,
) -> Callable[[type[A]], type[A]]:
    """Configure an actor class while preserving its type and method signatures.

    cpu accepts 0.1–64 cores in 0.001 increments; memory_mib accepts 128–262144;
    idle_timeout_ms accepts 1–86400000. regions must be nonempty and unique.
    Use once per actor class. Unspecified values inherit deployment defaults.
    """
    options = SandboxOptions(
        cpu=cpu, memory_mib=memory_mib, idle_timeout_ms=idle_timeout_ms, regions=regions
    )

    def decorate(actor: type[A]) -> type[A]:
        if not is_actor(actor):
            raise ValueError("sandbox requires an actor class")
        if actor in _options:
            raise ValueError("sandbox cannot be repeated")
        _options[actor] = options
        return actor

    return decorate


def sandbox_contract(actor: type[Actor[Any, Any, Any, Any]]) -> dict[str, Any]:
    options = _options.get(actor)
    return (
        {}
        if options is None
        else {"sandbox": options.model_dump(mode="json", by_alias=True, exclude_none=True)}
    )
