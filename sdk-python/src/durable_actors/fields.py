"""Declare actor field persistence and state broadcasting."""

from collections.abc import Callable
from dataclasses import MISSING, field
from typing import Any, Literal, TypeVar, cast, overload

from .guards import is_field

T = TypeVar("T")


@overload
def persisted(default: T) -> T:
    """Declare a field saved after successful invocations."""
    ...


@overload
def persisted(*, default_factory: Callable[[], T]) -> T:
    """Declare a field saved after successful invocations."""
    ...


def persisted(default: Any = MISSING, *, default_factory: Any = MISSING) -> Any:
    """Declare a field saved after successful invocations.

    Supply either a default value or default_factory, and annotate the field
    with its value type. Defaults are copied per actor; factories run on activation.

    Examples:
        count: int = persisted(0)
        messages: list[str] = persisted(default_factory=list)
    """
    return _persistence_field("persisted", default, default_factory)


@overload
def ephemeral(default: T) -> T:
    """Declare a temporary field excluded from persistence and emitted state."""
    ...


@overload
def ephemeral(*, default_factory: Callable[[], T]) -> T:
    """Declare a temporary field excluded from persistence and emitted state."""
    ...


def ephemeral(default: Any = MISSING, *, default_factory: Any = MISSING) -> Any:
    """Declare a temporary field excluded from persistence and emitted state.

    Supply either a default value or default_factory. Values are recreated when
    the actor is restored or activated. Use factories for locks and service clients.

    Examples:
        busy: bool = ephemeral(False)
        lock: Lock = ephemeral(default_factory=Lock)
    """
    return _persistence_field("ephemeral", default, default_factory)


def emitted(value: T) -> T:
    """Broadcast saved changes to a public persisted() field.

    Examples:
        count: int = emitted(persisted(0))
        messages: list[str] = emitted(persisted(default_factory=list))
    """
    if (
        not is_field(value)
        or value.metadata.get("durable_actors") != "persisted"
        or value.metadata.get("durable_actors_emitted")
    ):
        raise ValueError("emitted() requires a persisted() field and cannot be repeated")
    return cast(
        T,
        field(
            default=value.default,
            default_factory=value.default_factory,
            metadata={**value.metadata, "durable_actors_emitted": True},
        ),
    )


def _persistence_field(
    mode: Literal["persisted", "ephemeral"], default: Any, default_factory: Any
) -> Any:
    if is_field(default):
        raise ValueError("actor fields must declare exactly one of persisted() or ephemeral()")
    return field(
        default=default, default_factory=default_factory, metadata={"durable_actors": mode}
    )
