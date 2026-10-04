import importlib
import json
import subprocess
import sys

import httpx
import pytest
from fixtures.sqlite import seed
from test_authoring import Chat

from durable_actors import ActorInvocationError, Client
from durable_actors.codegen import generate_client
from durable_actors.contract import public_contract

TARGET = {
    "route": "http://host.test",
    "token": "ticket",
    "ownerEpoch": 1,
    "expiresAtMs": 9999999999999,
}
GRANT = {
    "websocketUrl": "wss://host.test/socket",
    "homeRegion": "west",
    "connectByMs": 9999999999999,
    "authorizedUntilMs": 9999999999999,
}


def test_socket_authorization_and_backend_broadcast_use_placement_and_route():
    calls = []

    def handle(request):
        calls.append(
            (request.url.path, json.loads(request.content), request.headers.get("authorization"))
        )
        if request.url.path.endswith("/find-websocket"):
            return httpx.Response(200, json=GRANT)
        if request.url.path.endswith("/find"):
            return httpx.Response(200, json=TARGET)
        return httpx.Response(204)

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client(
            "http://control.test", project_id="test", home_region="west", http=http
        ) as client:
            client.prepare_websocket("Chat", "one", {"user": "Ada"})
            client.broadcast("Chat", "one", {"text": "hello"})
            client.broadcast("Chat", "one", {"text": "again"})
    assert calls[0][1]["homeRegion"] == "west"
    assert calls[1][0].endswith("/find")
    assert calls[1][1] == {"homeRegion": "west"}
    assert len(calls) == 4
    for _, body, credential in calls[2:]:
        assert credential == "Bearer ticket"
        assert body["ownerEpoch"] == 1
        effect = body["effects"][0]
        assert effect["type"] == "broadcast"
        assert json.loads(effect["message"]["data"])["text"] in {"hello", "again"}


def test_broadcast_with_unknown_outcome_is_not_replayed():
    calls = []

    def handle(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/find"):
            return httpx.Response(200, json=TARGET)
        raise httpx.ReadError("reply lost")

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            with pytest.raises(ActorInvocationError) as error:
                client.broadcast("Chat", "one", "hello")
    assert error.value.code == "outcome_unknown"
    assert len(calls) == 2


def test_generated_authorizations_are_typed_and_dispatch_to_the_correct_actor(
    tmp_path, monkeypatch
):
    generate_client(public_contract([Chat]), tmp_path / "parity_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    generated = importlib.import_module("parity_client")
    authorization = generated.actors.Chat.Authorization(
        actor_id="lobby",
        metadata=generated.actors.Chat.Message(text="Ada", role="user"),
        home_region="west",
    )
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json=GRANT)

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client(http=http) as client:
            grant = generated.ActorProxy.handle(authorization, client)
            assert grant.websocket_url == GRANT["websocketUrl"]
            generated.actors.Chat.prepare_websocket(authorization, client)
    assert len(calls) == 2
    assert all(request.url.path.endswith("/Chat/lobby/find-websocket") for request in calls)
    assert json.loads(calls[0].content) == {
        "metadata": {"text": "Ada", "role": "user"},
        "homeRegion": "west",
        "authorizationLifetimeMs": 900000,
    }
    source = tmp_path / "usage.py"
    source.write_text("""from parity_client import actors, ActorAuthorization, ActorProxy
from durable_actors import Client

def check(client: Client) -> None:
    authorization: ActorAuthorization = actors.Chat.Authorization(actor_id="one", metadata=actors.Chat.Message(text="Ada", role="user"))
    ActorProxy.handle(authorization, client)
    actors.Chat.prepare_websocket(authorization, client)
    actors.Chat.get("one", client).broadcast(actors.Chat.Message(text="hi", role="assistant"))
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
        + '\nactors.Chat.Authorization(actor_id="one", metadata=42)\nactors.Chat.get("one").broadcast(42)\n'
    )
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1 and "2 errors" in result.stdout, result.stdout + result.stderr


def test_client_telemetry_reports_outcomes_without_credentials_or_arguments():
    events = []
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"target": TARGET, "outcome": {"type": "completed", "result": 1}}
            )
        )
    ) as http:
        with Client(http=http, telemetry=events.append, api_key="secret") as client:
            assert client.invoke("Counter", "one", "increment", ["private"]) == 1
    assert len(events) == 1
    assert events[0]["outcome"] == "completed"
    assert events[0]["actor_name"] == "Counter"
    assert events[0]["completed_at_ms"] >= 0
    assert "secret" not in json.dumps(events) and "private" not in json.dumps(events)


def test_socket_tags_are_validated_and_fully_typed(tmp_path):
    source = tmp_path / "tagged.py"
    source.write_text("""from typing import Literal, assert_type
from durable_actors import Actor, ActorSocket

Tag = Literal["member", "admin"]
class Room(Actor[str, str, str, Tag]):
    def on_connect(self, socket: ActorSocket[str, str, Tag]) -> None:
        socket.set_tags("member")
        assert_type(socket.tags, tuple[Tag, ...])
        assert_type(socket.state, Literal["connecting", "open", "closed"])
        self.broadcast("hello", tags=("admin",), tag_match="any")

class Echo(Actor[None, str]):
    def on_message(self, socket: ActorSocket[None, str], message: str) -> None:
        socket.send(message)
        self.broadcast(message)
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
        + """
def bad(room: Room, socket: ActorSocket[str, str, Tag], echo: Echo) -> None:
    socket.set_tags("typo")
    room.broadcast("hi", tags=("typo",))
    room.broadcast("hi", tag_match="typo")
    echo.broadcast(42)
"""
    )
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1 and "4 errors" in result.stdout, result.stdout + result.stderr


async def test_socket_tag_contract_rejects_values_outside_the_declared_set():
    from typing import Literal

    from fixtures.effects import Effects
    from fixtures.sqlite import ActorRuntime

    from durable_actors import Actor, ActorSocket

    class Room(Actor[str, str, str, Literal["member", "admin"]]):
        def on_connect(self, socket: ActorSocket[str, str, Literal["member", "admin"]]) -> None:
            socket.set_tags("unknown")

    runtime = ActorRuntime(Room, Effects())
    reply = await runtime.handle(
        {
            "type": "websocket_event",
            "actor": {"project_id": "local", "actor_name": "Room", "actor_id": "one"},
            "sqlite": seed(),
            "event": {
                "type": "connect",
                "connection": {"id": "socket", "metadata": "Ada", "tags": []},
            },
            "connections": [],
        }
    )
    assert reply["type"] == "failed", reply
    assert "member" in reply["message"]


def test_source_class_references_preserve_signatures_and_use_the_transport(tmp_path):
    from durable_actors import Actor, ephemeral

    class Counter(Actor):
        cache: object = ephemeral(
            default_factory=lambda: pytest.fail("references must not activate actors")
        )

        def total(self, initial: int = 1, *values: int) -> int:
            return initial + sum(values)

        def label(self, *, prefix: str = "hello") -> str:
            return prefix

    def handle(request):
        body = json.loads(request.content)
        value = sum(body["args"]) if body["method"] == "total" else body["args"][0]
        outcome = {"type": "completed", "result": value}
        return httpx.Response(
            200,
            json={"target": TARGET, "outcome": outcome}
            if request.url.host != "host.test"
            else outcome,
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client(http=http) as transport:
            counter = Counter.get("one", transport)
            assert counter.total() == 1
            assert counter.total(2, 3, 4) == 9
            assert counter.label(prefix="Ada") == "Ada"
    source = tmp_path / "reference.py"
    source.write_text("""from durable_actors import Actor
class Counter(Actor):
    def increment(self, amount: int = 1) -> int:
        return amount

def call() -> int:
    return Counter.get("one").increment(2)

Counter.get("one").increment("bad")
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1 and "1 error" in result.stdout, result.stdout + result.stderr
