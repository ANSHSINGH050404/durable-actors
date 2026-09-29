"""Mark actor fields for state broadcasting or temporary storage."""

from collections.abc import Callable
from dataclasses import MISSING, field
from typing import Any, TypeVar, overload

T = TypeVar("T")


@overload
def emitted(default: T) -> T:
    """Declare a persisted field whose saved changes are broadcast to subscribers."""
    ...


@overload
def emitted(*, default_factory: Callable[[], T]) -> T:
    """Declare a persisted field whose saved changes are broadcast to subscribers."""
    ...


def emitted(default: Any = MISSING, *, default_factory: Any = MISSING) -> Any:
    """Declare a persisted field whose saved changes are broadcast to subscribers.

    Supply either a default value or default_factory, and annotate the field
    with its value type. Defaults are copied per actor; factories run on activation.

    Examples:
        count: int = emitted(0)
        messages: list[str] = emitted(default_factory=list)
    """
    return field(
        default=default, default_factory=default_factory, metadata={"durable_actors": "emitted"}
    )


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
    return field(
        default=default, default_factory=default_factory, metadata={"durable_actors": "ephemeral"}
    )
