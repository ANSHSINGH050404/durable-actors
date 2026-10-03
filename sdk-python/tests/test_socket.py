import pytest
from fixtures.effects import Effects

from durable_actors import JsonValue
from durable_actors.socket import SocketScope


def scope():
    return SocketScope(
        object(), "one", (JsonValue, JsonValue, JsonValue, str), Effects(), [], False
    )


async def test_reject_defaults_to_a_supported_close_code():
    context = scope()
    socket = context.socket({"id": "one", "metadata": None, "tags": []}, "connecting")
    socket.reject()
    assert socket.state == "closed"
    assert context.pending[0]["code"] == 4003


async def test_actor_output_cannot_impersonate_state_events():
    with pytest.raises(ValueError, match="reserved"):
        scope().message({"type": "state", "state": {}})


async def test_metadata_limit_is_enforced_before_publishing():
    context = scope()
    socket = context.socket({"id": "one", "metadata": None, "tags": []})
    with pytest.raises(ValueError, match="16 KiB"):
        socket.metadata = "x" * 16383
    assert context.pending == []


async def test_tag_count_limit():
    context = scope()
    socket = context.socket({"id": "one", "metadata": None, "tags": []})
    socket.set_tags(*(str(i) for i in range(10)))
    with pytest.raises(ValueError):
        socket.set_tags(*(str(i) for i in range(11)))


async def test_count_and_tag_query_do_not_fetch_every_connection():
    class QueryEffects(Effects):
        def __init__(self):
            super().__init__()
            self.queries = []

        async def get_connections(self, tag=None, count_only=False):
            self.queries.append((tag, count_only))
            if count_only:
                return 32768
            assert tag == "blue"
            return [{"id": "one", "metadata": None, "tags": ["blue"]}]

    effects = QueryEffects()
    context = SocketScope(
        object(), "one", (JsonValue, JsonValue, JsonValue, str), effects, [], False
    )
    assert await context.get_connection_count() == 32768
    assert [socket.id for socket in await context.get_connections("blue")] == ["one"]
    assert effects.queries == [(None, True), ("blue", False)]


async def test_auto_response_configuration_and_clear():
    context = scope()
    context.set_websocket_auto_response("ping", "pong")
    context.set_websocket_auto_response()
    with pytest.raises(ValueError, match="2048"):
        context.set_websocket_auto_response("x" * 2049, "pong")
    assert context.pending == [
        {"type": "set_auto_response", "request": "ping", "response": "pong"},
        {"type": "set_auto_response", "request": None, "response": None},
    ]
