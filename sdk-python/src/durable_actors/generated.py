"""Shared model and argument support for generated Python clients."""

from typing import Any

from pydantic import BaseModel, ConfigDict, TypeAdapter

from .contract import encode


class Unset:
    """Type of UNSET, representing an omitted argument or field rather than an explicit None."""

    pass


UNSET = Unset()
"""Sentinel for an omitted argument or field; distinct from an explicit None."""


class ClientModel(BaseModel):
    """Base for generated wire models, including fields that can be omitted with UNSET."""

    model_config = ConfigDict(arbitrary_types_allowed=True)


def is_unset(value: object) -> bool:
    return isinstance(value, Unset)


def argument(value: Any, adapter: TypeAdapter[Any]) -> Any:
    return value if isinstance(value, Unset) else encode(adapter, value)


def arguments(values: list[Any], defaults: list[Any]) -> list[Any]:
    while values and isinstance(values[-1], Unset):
        values.pop()
    for index, value in enumerate(values):
        if isinstance(value, Unset):
            if isinstance(defaults[index], Unset):
                raise ValueError(
                    f"cannot omit argument {index + 1} without a schema default before a supplied argument"
                )
            values[index] = defaults[index]
    return values
