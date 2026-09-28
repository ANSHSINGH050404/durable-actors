import ast
import asyncio
import importlib
import inspect
import json
import subprocess
import sys

from test_authoring import Chat

from little_actors.codegen import generate_client
from little_actors.contract import public_contract


class Transport:
    def broadcast(self, actor_name, actor_id, message):
        raise AssertionError("not used")

    def prepare_websocket(
        self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000, home_region=None
    ):
        raise AssertionError("not used")

    def invoke(self, actor_name, actor_id, method, args):
        assert (actor_name, actor_id, method) == ("Chat", "lobby", "append")
        return args


def test_generated_client_returns_typed_models_without_actor_source(tmp_path, monkeypatch):
    package = tmp_path / "generated"
    generate_client(json.loads(json.dumps(public_contract([Chat]))), package)
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("generated")
    models = module.actors.Chat
    message = models.Message(text="hello", role="user")
    result = module.actors.Chat.get("lobby", Transport()).append(message)
    assert isinstance(result[0], models.Message)
    assert result[0].text == "hello"
    assert (package / "py.typed").is_file()
    source = tmp_path / "usage.py"
    source.write_text("""from generated import actors
from little_actors.client import Client
def check(client: Client) -> None:
    chat: actors.Chat.Stub = actors.Chat.get("lobby")
    configured = actors.Chat.get("other", client)
    configured.append(actors.Chat.Message(text="hello", role="user"))
    result: actors.Chat.Methods.append.Result = chat.append(actors.Chat.Message(text="hello", role="user"))
    chat.append(42)
    connection = chat.connect(actors.Chat.Message(text="hello", role="user"))
    connection.send(42)
    subscription = chat.subscribe(lambda state: print(state.messages[0].text), metadata=actors.Chat.Message(text="hello", role="user"))
    subscription.close()
    chat.subscribe(lambda state: print(state.missing), metadata=actors.Chat.Message(text="hello", role="user"))
    chat.subscribe(lambda state: None)
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )
        assert result.returncode == 1, result.stdout + result.stderr
        assert "42" in result.stdout or "int" in result.stdout
        assert "4 errors" in result.stdout, result.stdout


def test_generated_rest_and_keyword_parameters_preserve_calling_convention(tmp_path, monkeypatch):
    from little_actors import Actor

    class Parameters(Actor):
        def total(self, initial: int, *values: int) -> int:
            return initial + sum(values)

        def label(self, *, value: str = "default") -> str:
            return value

    generate_client(public_contract([Parameters]), tmp_path / "parameters_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    generated = importlib.import_module("parameters_client")

    class Calls:
        def broadcast(self, actor_name, actor_id, message):
            raise AssertionError("not used")

        def prepare_websocket(
            self,
            actor_name,
            actor_id,
            metadata,
            *,
            authorization_lifetime_ms=900000,
            home_region=None,
        ):
            raise AssertionError("not used")

        def invoke(self, actor_name, actor_id, method, args):
            return sum(args) if method == "total" else (args[0] if args else "default")

    client = generated.actors.Parameters.get("one", Calls())
    assert client.total(1, 2, 3) == 6
    assert client.label(value="hello") == "hello"
    assert client.label() == "default"


def test_generated_names_cannot_shadow_client_runtime(tmp_path, monkeypatch):
    from little_actors import Actor

    class Connection(Actor):
        def call(self, json: str, TypeAdapter: int, argument: bool) -> str:
            return json

    generate_client(public_contract([Connection]), tmp_path / "names_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("names_client")

    class Calls:
        def broadcast(self, actor_name, actor_id, message):
            raise AssertionError("not used")

        def prepare_websocket(
            self,
            actor_name,
            actor_id,
            metadata,
            *,
            authorization_lifetime_ms=900000,
            home_region=None,
        ):
            raise AssertionError("not used")

        def invoke(self, actor_name, actor_id, method, args):
            return args[0]

    assert module.actors.Connection.get("one", Calls()).call("value", 42, True) == "value"


def test_generated_recursive_unions_dates_and_tuples(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4

    from fixtures.effects import Effects
    from fixtures.types import Trees

    from little_actors.runtime import ActorRuntime

    generate_client(public_contract([Trees]), tmp_path / "tree_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    remote = importlib.import_module("tree_client")
    models = remote.actors.Trees
    runtime = ActorRuntime(Trees, Effects())

    class Calls:
        def broadcast(self, actor_name, actor_id, message):
            raise AssertionError("not used")

        def prepare_websocket(
            self,
            actor_name,
            actor_id,
            metadata,
            *,
            authorization_lifetime_ms=900000,
            home_region=None,
        ):
            raise AssertionError("not used")

        def invoke(self, actor_name, actor_id, method, args):
            reply = asyncio.run(
                runtime.handle(
                    {
                        "type": "invoke",
                        "request_id": "one",
                        "actor": {
                            "project_id": "local",
                            "actor_name": actor_name,
                            "actor_id": actor_id,
                        },
                        "state": None,
                        "method": method,
                        "args": args,
                    }
                )
            )
            assert reply["type"] == "invoked", reply
            return reply["result"]

    now, identity = datetime.now(timezone.utc), uuid4()
    node = models.NodeInput(
        item=models.BranchInput(
            children=[
                models.NodeInput(item=models.Leaf(value=1), created_at=now, identity=identity)
            ]
        ),
        created_at=now,
        identity=identity,
    )
    client = remote.actors.Trees.get("one", Calls())
    result = client.append(node)
    assert result[0].item.children[0].item.value == 1
    assert result[0].created_at == now
    assert result[0].identity == identity
    assert client.pair((7, "seven")) == (7, "seven")
    source = tmp_path / "typed_tree.py"
    source.write_text("""from tree_client import actors
from little_actors import Client
def check(client: Client, node: actors.Trees.NodeInput) -> None:
    tree = actors.Trees.get("one", client)
    nodes: list[actors.Trees.NodeOutput] = tree.append(node)
    pair: tuple[int, str] = tree.pair((1, "one"))
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_generated_models_preserve_omitted_typed_dict_fields(tmp_path, monkeypatch):
    from fixtures.effects import Effects
    from fixtures.types import OptionActor

    from little_actors.runtime import ActorRuntime

    runtime = ActorRuntime(OptionActor, Effects())
    generate_client(public_contract([OptionActor]), tmp_path / "options_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    remote = importlib.import_module("options_client")
    models = remote.actors.OptionActor

    class Calls:
        def broadcast(self, actor_name, actor_id, message):
            raise AssertionError("not used")

        def prepare_websocket(
            self,
            actor_name,
            actor_id,
            metadata,
            *,
            authorization_lifetime_ms=900000,
            home_region=None,
        ):
            raise AssertionError("not used")

        def invoke(self, actor_name, actor_id, method, args):
            reply = asyncio.run(
                runtime.handle(
                    {
                        "type": "invoke",
                        "actor": {
                            "project_id": "local",
                            "actor_name": actor_name,
                            "actor_id": actor_id,
                        },
                        "state": None,
                        "method": method,
                        "args": args,
                    }
                )
            )
            assert reply["type"] == "invoked", reply
            return reply["result"]

    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        models.Options(required="present", optional=None)
    result = remote.actors.OptionActor.get("one", Calls()).echo(models.Options(required="present"))
    assert result.model_dump(exclude_unset=True) == {"required": "present"}


def test_generated_clients_only_fill_omitted_arguments_with_known_defaults(tmp_path, monkeypatch):
    import pytest

    from little_actors import Actor

    class Defaults(Actor):
        def greet(self, name: str = "friend", suffix: str = "!") -> list[str]:
            return [name, suffix]

        def nullable(self, name: str | None = None, suffix: str = "!") -> list[str | None]:
            return [name, suffix]

        def unknown(self, name: str = "friend", suffix: str = "!") -> list[str]:
            return [name, suffix]

    contract = public_contract([Defaults])
    definitions = contract["actors"][0]["rpc"]["schema"]["definitions"]
    for index in range(2):
        node = definitions[f"Method_unknown_Parameter_{index}"]
        del node["default"]
        del node["x-python-kind"]
    generate_client(contract, tmp_path / "defaults_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    generated = importlib.import_module("defaults_client")
    calls = []

    class Calls:
        def broadcast(self, actor_name, actor_id, message):
            raise AssertionError("not used")

        def prepare_websocket(
            self,
            actor_name,
            actor_id,
            metadata,
            *,
            authorization_lifetime_ms=900000,
            home_region=None,
        ):
            raise AssertionError("not used")

        def invoke(self, actor_name, actor_id, method, args):
            calls.append(args)
            return args

    client = generated.actors.Defaults.get("one", Calls())
    assert client.greet(suffix="?") == ["friend", "?"]
    assert client.nullable(suffix="?") == [None, "?"]
    assert client.unknown() == []
    assert client.unknown("Ada") == ["Ada"]
    assert client.unknown("Ada", "?") == ["Ada", "?"]
    calls.clear()
    with pytest.raises(ValueError, match="without a schema default"):
        client.unknown(suffix="?")
    assert calls == []


def test_generated_subscription_name_is_reserved(tmp_path):
    import pytest

    contract = public_contract([Chat])
    contract["actors"][0]["rpc"]["methods"][0]["name"] = "subscribe"
    with pytest.raises(ValueError, match="reserved"):
        generate_client(contract, tmp_path / "conflicting")


def test_generated_emitted_state_preserves_optional_contract_fields(tmp_path, monkeypatch):
    from little_actors import Unset

    contract = public_contract([Chat])
    contract["actors"][0]["socket"]["schema"]["definitions"]["State"]["required"] = []
    generate_client(contract, tmp_path / "optional_state_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    models = importlib.import_module("optional_state_client").actors.Chat
    state = models.EmittedState()
    assert isinstance(state.messages, Unset)
    assert state.model_dump(exclude_unset=True) == {}


def test_generated_docstrings_survive_contract_transport(tmp_path, monkeypatch):
    from fixtures.documented import Note, Notebook

    contract = json.loads(json.dumps(public_contract([Notebook])))
    generate_client(contract, tmp_path / "documented_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    remote = importlib.import_module("documented_client")
    models = remote.actors.Notebook

    assert inspect.getdoc(remote.actors.Notebook) == inspect.getdoc(Notebook)
    assert inspect.getdoc(remote.actors.Notebook.Stub.save) == inspect.getdoc(Notebook.save)
    assert inspect.getdoc(models.Note) == inspect.getdoc(Note)
    assert "actor_id" in inspect.getdoc(remote.actors.Notebook.get)
    assert "synchronously" in inspect.getdoc(remote.actors.Notebook.Stub.clear)
    assert "background thread" in inspect.getdoc(remote.actors.Notebook.Stub.subscribe)
    assert "metadata" in inspect.getdoc(remote.actors.Notebook.Stub.connect)
    assert "authorization_lifetime_ms" in inspect.getdoc(
        remote.actors.Notebook.Stub.prepare_websocket
    )
    assert "emitted" in inspect.getdoc(models.EmittedState)
    assert "omitted" in inspect.getdoc(models.StatePatch)

    source = ast.parse((tmp_path / "documented_client/_notebook_models.py").read_text())
    note = next(
        node for node in source.body if isinstance(node, ast.ClassDef) and node.name == "Note"
    )
    field_doc = next(node.value.value for node in note.body[2:] if isinstance(node, ast.Expr))
    assert inspect.cleandoc(field_doc) == "The note text, including whitespace."


def test_actor_namespaces_preserve_typed_imports_and_method_types(tmp_path, monkeypatch):
    from little_actors import Actor

    class Counter(Actor):
        count: int = 0

        def total(self, initial: int = 0, *values: int) -> int:
            return initial + sum(values)

        def clear(self) -> None:
            self.count = 0

    generate_client(public_contract([Chat, Counter]), tmp_path / "namespace_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    actors = importlib.import_module("namespace_client").actors
    assert isinstance(actors.Chat.get("lobby", Transport()), actors.Chat.Stub)
    assert actors.Counter.State(count=1).count == 1
    source = tmp_path / "typed_namespace.py"
    source.write_text("""from namespace_client import actors
from namespace_client.actors import Chat
from little_actors.client import ActorTransport

def check(transport: ActorTransport) -> None:
    chat: actors.Chat.Stub = Chat.get("lobby", transport)
    metadata: actors.Chat.Metadata = actors.Chat.Message(text="Ada", role="user")
    incoming: actors.Chat.Incoming = metadata
    outgoing: actors.Chat.Outgoing = incoming
    state: actors.Chat.State = actors.Chat.State(messages=[outgoing])
    args: actors.Chat.Methods.append.Args = (incoming,)
    result: actors.Chat.Methods.append.Result = chat.append(*args)
    empty: actors.Chat.Methods.clear.Args = ()
    cleared: actors.Chat.Methods.clear.Result = None
    chat.clear(*empty)
    counter = actors.Counter.get("one", transport)
    defaults: actors.Counter.Methods.total.Args = ()
    variadic: actors.Counter.Methods.total.Args = (1, 2, 3)
    count: actors.Counter.Methods.total.Result = counter.total(*variadic)
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    source.write_text(
        source.read_text()
        + '\ndef bad() -> None:\n    args: actors.Chat.Methods.append.Args = (42,)\n    result: actors.Chat.Methods.append.Result = 42\n    actors.Unknown.get("one")\n'
    )
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1, result.stdout + result.stderr
        assert "3 errors" in result.stdout, result.stdout


def test_actor_namespaces_keep_model_and_builtin_names_distinct(tmp_path, monkeypatch):
    import builtins

    from pydantic import BaseModel

    from little_actors import Actor

    class Stub(BaseModel):
        value: int

    class State(BaseModel):
        value: int

    class Names(Actor):
        payload: State = State(value=0)

        def echo(self, value: Stub) -> Stub:
            return value

    class str(Actor):
        def echo(self, value: builtins.str) -> builtins.str:
            return value

    generate_client(public_contract([str, Names]), tmp_path / "collision_client")
    monkeypatch.syspath_prepend(builtins.str(tmp_path))
    actors = importlib.import_module("collision_client").actors
    payload = actors.Names.StubModel(value=1)
    assert payload.value == 1
    assert actors.Names.State(payload=actors.Names.StateModel(value=2)).payload.value == 2
    source = tmp_path / "typed_collision.py"
    source.write_text("""from collision_client import actors

def check() -> None:
    value: str = actors.str.get("one").echo("hello")
    payload = actors.Names.StubModel(value=1)
    result: actors.Names.Methods.echo.Result = actors.Names.get("two").echo(payload)
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, builtins.str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_generated_null_types_work_as_fields_arguments_and_socket_metadata(tmp_path, monkeypatch):
    from pydantic import BaseModel

    from little_actors import Actor, SocketGrant

    class Empty(BaseModel):
        value: None

    class Nulls(Actor[None, None, None]):
        def echo(self, value: None = None) -> Empty:
            return Empty(value=value)

    class Calls:
        def invoke(self, actor_name, actor_id, method, args):
            assert (actor_name, actor_id, method) == ("Nulls", "one", "echo")
            assert args in ([], [None])
            return {"value": None}

        def prepare_websocket(
            self,
            actor_name,
            actor_id,
            metadata,
            *,
            authorization_lifetime_ms=900000,
            home_region=None,
        ):
            assert metadata is None
            return SocketGrant(
                websocket_url="wss://example.test/socket",
                home_region="canada",
                connect_by_ms=1,
                authorized_until_ms=2,
            )

        def broadcast(self, actor_name, actor_id, message):
            assert message is None

    generate_client(public_contract([Nulls]), tmp_path / "null_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    generated = importlib.import_module("null_client")
    client = generated.actors.Nulls.get("one", Calls())
    assert client.echo().value is None
    assert client.echo(None).value is None
    client.broadcast(None)
    assert client.prepare_websocket(None).home_region == "canada"
    authorization = generated.actors.Nulls.Authorization(actor_id="one", metadata=None)
    assert generated.ActorProxy.handle(authorization, Calls()).home_region == "canada"
    source = tmp_path / "usage.py"
    source.write_text("""from null_client import actors

def check() -> None:
    client = actors.Nulls.get("one")
    result: actors.Nulls.Empty = client.echo(None)
    client.connect(None)
    client.prepare_websocket(None)
    client.broadcast(None)
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
