from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field
from typing_extensions import NotRequired, TypedDict

from durable_actors import Actor


class Leaf(BaseModel):
    kind: Literal["leaf"] = "leaf"
    value: Annotated[int, Field(ge=0)]


class Branch(BaseModel):
    kind: Literal["branch"] = "branch"
    children: list["Node"] = []


class Node(BaseModel):
    item: Annotated[Leaf | Branch, Field(discriminator="kind")]
    created_at: datetime
    identity: UUID
    note: str | None = None


class Trees(Actor):
    nodes: list[Node] = []

    def append(self, node: Node) -> list[Node]:
        self.nodes.append(node)
        return self.nodes

    def pair(self, value: tuple[int, str]) -> tuple[int, str]:
        return value


class Options(TypedDict):
    required: str
    optional: NotRequired[str]


class OptionActor(Actor):
    def echo(self, options: Options) -> Options:
        return options
