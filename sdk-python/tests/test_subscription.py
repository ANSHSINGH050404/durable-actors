import json
from queue import Queue
from threading import Event, current_thread

import pytest
from pydantic import BaseModel, Field, TypeAdapter
from websockets.exceptions import ConnectionClosedOK
from websockets.frames import Close

from durable_actors.client import ActorProtocolError
from durable_actors.connection import Connection
from durable_actors.subscription import Subscription


class State(BaseModel):
    count: int
    entries: list[int] = Field(alias="items")
    label: str | None = None


class Patch(BaseModel):
    count: int | None = None
    entries: list[int] | None = Field(default=None, alias="items")
    label: str | None = None


class Wire:
    def __init__(self):
        self.messages = Queue()
        self.closed = Event()
        self.reading = Event()

    def recv(self, timeout=None):
        self.reading.set()
        value = self.messages.get(timeout=timeout)
        if isinstance(value, Exception):
            raise value
        return json.dumps(value)

    def send(self, message):
        raise AssertionError("state subscriptions do not send application messages")

    def close(self, code=1000, reason=""):
        if not self.closed.is_set():
            self.closed.set()
            self.messages.put(ConnectionClosedOK(Close(code, reason), Close(code, reason), True))


def connection(wire):
    return Connection(
        wire, TypeAdapter(str), TypeAdapter(str), TypeAdapter(State), TypeAdapter(Patch)
    )


def snapshot(version=1):
    return {
        "type": "state",
        "state": {"count": 1, "items": [1], "label": "present"},
        "version": version,
    }


def patch(changes, version, removed=()):
    return {
        "type": "state_update",
        "changes": changes,
        "removed": list(removed),
        "version": version,
    }


def test_subscription_delivers_isolated_complete_states_on_a_background_thread():
    wire = Wire()
    received = Queue()
    caller = current_thread()

    def changed(state):
        assert current_thread() is not caller
        received.put(state.model_copy(deep=True))
        state.entries.append(999)

    subscription = Subscription(connection(wire), changed, TypeAdapter(State))
    try:
        wire.messages.put(snapshot())
        wire.messages.put("application message")
        wire.messages.put(patch({"count": 2}, 3))
        wire.messages.put(patch({"count": 0}, 2))
        wire.messages.put(patch({"items": [2]}, 4, removed=["label"]))
        wire.messages.put(patch({"label": None}, 5))
        states = [received.get(timeout=2) for _ in range(4)]
        assert [(state.count, state.entries, state.label) for state in states] == [
            (1, [1], "present"),
            (2, [1], "present"),
            (2, [2], None),
            (2, [2], None),
        ]
        assert "label" not in states[2].model_fields_set
        assert "label" in states[3].model_fields_set
    finally:
        subscription.close()
    assert subscription.closed
    assert subscription.error is None
    assert wire.closed.is_set()


def test_close_unblocks_an_idle_receiver_and_is_idempotent():
    wire = Wire()
    subscription = Subscription(connection(wire), lambda state: None, TypeAdapter(State))
    assert wire.reading.wait(2)
    subscription.close()
    subscription.close()
    assert subscription.closed
    assert subscription.error is None


@pytest.mark.parametrize("failure", ["callback", "protocol", "connection"])
def test_subscription_reports_errors_and_closes_the_socket(failure):
    wire = Wire()
    errors = Queue()
    expected = RuntimeError("callback failed") if failure == "callback" else OSError("lost socket")

    def changed(state):
        if failure == "callback":
            raise expected

    subscription = Subscription(connection(wire), changed, TypeAdapter(State), on_error=errors.put)
    try:
        wire.messages.put(
            snapshot()
            if failure == "callback"
            else patch({"count": 2}, 1)
            if failure == "protocol"
            else expected
        )
        error = errors.get(timeout=2)
        if failure == "protocol":
            assert isinstance(error, ActorProtocolError)
        else:
            assert error is expected
        assert wire.closed.wait(2)
        assert subscription.error is error
    finally:
        subscription.close()
    assert subscription.closed


def test_callback_can_close_its_own_subscription():
    wire = Wire()
    done = Event()

    def changed(state):
        subscription.close()
        done.set()

    subscription = Subscription(connection(wire), changed, TypeAdapter(State))
    try:
        wire.messages.put(snapshot())
        assert done.wait(2)
    finally:
        subscription.close()
    assert subscription.closed
