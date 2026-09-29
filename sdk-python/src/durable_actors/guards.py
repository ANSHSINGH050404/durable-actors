from dataclasses import Field
from typing import Any, TypeGuard

from .actor import Actor


def is_document(value: object) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def is_list(value: object) -> TypeGuard[list[Any]]:
    return isinstance(value, list)


def is_actor(value: object) -> TypeGuard[type[Actor[Any, Any, Any, Any]]]:
    return isinstance(value, type) and value is not Actor and issubclass(value, Actor)


def is_field(value: object) -> TypeGuard[Field[Any]]:
    return isinstance(value, Field)
