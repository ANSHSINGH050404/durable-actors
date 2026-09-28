from typing import Annotated, ClassVar, Literal

import pytest
from pydantic import BaseModel

from little_actors import Actor, emitted, ephemeral, reentrant
from little_actors.contract import describe_actor, public_contract


class Message(BaseModel):
    text: str
    role: Literal["user", "assistant"]


class Chat(Actor[Message, Message, Message]):
    messages: list[Message] = emitted(default_factory=list)
    busy: bool = ephemeral(False)

    def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages

    def clear(self) -> None:
        self.messages.clear()


def test_contract_describes_models_methods_and_public_state():
    contract = public_contract([Chat])
    actor = contract["actors"][0]
    assert contract["version"] == 1
    assert actor["actorName"] == "Chat"
    assert [method["name"] for method in actor["rpc"]["methods"]] == ["append", "clear"]
    assert actor["rpc"]["methods"][1]["result"] == {"kind": "void"}
    assert actor["socket"]["emittable"] == ["messages"]
    assert list(actor["socket"]["schema"]["definitions"]["State"]["properties"]) == ["messages"]
    assert "Message" in actor["rpc"]["schema"]["definitions"]


def test_mutable_defaults_are_per_actor():
    left, right = Chat(), Chat()
    left.messages.append(Message(text="hello", role="user"))
    assert right.messages == []


def test_missing_public_annotations_are_rejected():
    class Bad(Actor):
        def echo(self, value):
            return value

    with pytest.raises(ValueError, match="annotation"):
        describe_actor(Bad)


def test_fields_persist_by_default_and_class_variables_are_not_state():
    class Counter(Actor):
        count: int = 0
        history: list[int] = []
        _secret: str = "private"
        version: ClassVar[int] = 1

    definition = describe_actor(Counter)
    assert set(definition.fields) == {"count", "history", "_secret"}
    assert all(field.persisted for field in definition.fields.values())
    assert not any(field.emittable for field in definition.fields.values())
    state = public_contract([Counter])["actors"][0]["socket"]["schema"]["definitions"]["State"]
    assert set(state["properties"]) == {"count", "history"}
    left, right = Counter(), Counter()
    left.history.append(1)
    assert right.history == []


def test_reentrant_preserves_method_types_and_marks_runtime_metadata():
    class Counter(Actor):
        @reentrant
        def read(self) -> int:
            return 1

    assert describe_actor(Counter).reentrant_methods == {"read"}


def test_nested_untyped_values_are_rejected():
    from typing import Any

    class Bad(Actor):
        def echo(self, value: list[Any]) -> int:
            return 1

    with pytest.raises(ValueError, match="concrete|Any"):
        describe_actor(Bad)


def test_nested_untyped_structural_models_are_rejected():
    from dataclasses import make_dataclass
    from typing import Any

    from little_actors.contract import adapter_for

    for value in (make_dataclass("Untyped", [("payload", Any)]),):
        with pytest.raises(ValueError, match="concrete"):
            adapter_for(value)


def test_untyped_aliases_and_non_string_dictionary_keys_are_rejected():
    from typing import Any

    from typing_extensions import TypeAliasType

    from little_actors.contract import adapter_for

    for value in (TypeAliasType("Untyped", list[Any]), dict[int, str]):
        with pytest.raises(ValueError, match="concrete|string"):
            adapter_for(value)


def test_variadic_rpc_parameters_must_be_last():
    class Bad(Actor):
        def echo(self, *values: int, label: str = "") -> int:
            return len(values)

    with pytest.raises(ValueError, match="variadic"):
        describe_actor(Bad)


def test_field_factories_run_on_activation_and_create_fresh_values():
    from dataclasses import field
    from threading import Lock

    resources = []

    def create_lock():
        lock = Lock()
        resources.append(lock)
        return lock

    class Guarded(Actor):
        values: list[int] = field(default_factory=list)
        lock: object = ephemeral(default_factory=create_lock)

    public_contract([Guarded])
    assert resources == []
    left, right = Guarded(), Guarded()
    left.values.append(1)
    assert right.values == []
    assert resources == [left.lock, right.lock]
    assert left.lock is not right.lock
    assert hasattr(left.lock, "acquire")


def test_socket_model_names_may_match_contract_root_names():
    from jsonschema import Draft7Validator

    class Metadata(BaseModel):
        member: str

    class Incoming(BaseModel):
        text: str

    class Room(Actor[Metadata, Incoming, Incoming]):
        pass

    contract = public_contract([Room])["actors"][0]["socket"]["schema"]
    Draft7Validator({**contract, "$ref": "#/definitions/Metadata"}).validate({"member": "one"})
    Draft7Validator({**contract, "$ref": "#/definitions/Incoming"}).validate({"text": "hello"})


def test_nonfinite_json_and_unbound_type_variables_are_rejected():
    from typing import TypeVar

    from little_actors.contract import adapter_for, encode

    with pytest.raises(ValueError, match="concrete"):
        adapter_for(TypeVar("Unknown"))
    with pytest.raises(ValueError):
        encode(adapter_for(float), float("nan"))


def test_contract_uses_serialized_types_for_results():
    from pydantic import computed_field

    from little_actors.contract import adapter_for, schemas

    class Result(BaseModel):
        value: int

        @computed_field
        @property
        def doubled(self) -> int:
            return self.value * 2

    contract = schemas({"Result": adapter_for(Result)}, serialization={"Result"})
    result = contract["definitions"]["Result"]
    while "$ref" in result:
        result = contract["definitions"][result["$ref"].split("/")[-1]]
    assert result["properties"]["doubled"]["type"] == "integer"


def test_actor_fields_require_defaults():
    class Missing(Actor):
        count: int

    with pytest.raises(ValueError, match="require defaults"):
        describe_actor(Missing)


def test_persisted_field_defaults_honor_annotation_constraints():
    from pydantic import Field

    class Invalid(Actor):
        count: Annotated[int, Field(ge=0)] = -1

    with pytest.raises(ValueError, match="greater than or equal"):
        describe_actor(Invalid)


def test_emitted_fields_must_be_public():
    class Private(Actor):
        _secret: str = emitted("private")

    with pytest.raises(ValueError, match="must be public"):
        describe_actor(Private)


def test_field_helpers_preserve_static_value_types(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    source = Path(__file__).with_name("fixtures").joinpath("field_types.py").read_text()
    actors = tmp_path / "actors.py"
    actors.write_text(source)
    invalid = tmp_path / "invalid.py"
    invalid.write_text("""from actors import TypedActor
from little_actors import emitted, ephemeral

bad_default: int = emitted("bad")
bad_factory: int = ephemeral(default_factory=list)

def misuse(actor: TypedActor) -> None:
    actor.messages.append(1)
    actor.busy = "bad"
""")
    for checker, flags in (("mypy", ["--strict"]), ("pyright", [])):
        for target, expected_errors in ((actors, 0), (invalid, 4)):
            result = subprocess.run(
                [sys.executable, "-m", checker, *flags, str(target)],
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )
            assert result.returncode == bool(expected_errors), result.stdout + result.stderr
            if expected_errors:
                assert "4 errors" in result.stdout, result.stdout


def test_subscription_method_name_is_reserved():
    class Conflicting(Actor):
        def subscribe(self) -> None:
            pass

    with pytest.raises(ValueError, match="reserved"):
        describe_actor(Conflicting)
