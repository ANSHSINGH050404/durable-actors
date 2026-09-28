"""JsonValue describes JSON primitives, lists, and string-keyed dictionaries recursively."""

from typing import TYPE_CHECKING, TypeAlias

from typing_extensions import TypeAliasType

if TYPE_CHECKING:
    JsonValue: TypeAlias = (
        str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
    )
else:
    JsonValue = TypeAliasType(
        "JsonValue", str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
    )
