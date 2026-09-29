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
    with pytest.raises(ValueError, match="64 KiB"):
        socket.metadata = "x" * 65536
    assert context.pending == []
