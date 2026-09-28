import json

from pydantic import BaseModel, TypeAdapter
from websockets.exceptions import ConnectionClosedOK
from websockets.frames import Close

from little_actors.connection import Connection, StateSnapshot


class Message(BaseModel):
    text: str


class State(BaseModel):
    count: int


class Wire:
    def __init__(self):
        self.sent = []
        self.closed = False
        self.messages = [
            json.dumps({"type": "state", "state": {"count": 1}, "version": 1}),
            '{"text":"hello"}',
        ]

    def recv(self, timeout=None):
        if not self.messages:
            raise ConnectionClosedOK(Close(1000, ""), Close(1000, ""), True)
        return self.messages.pop(0)

    def send(self, value):
        self.sent.append(value)

    def close(self, code=1000, reason=""):
        self.closed = True


def test_socket_connection_validates_messages_and_separates_state():
    wire = Wire()
    with Connection(
        wire, TypeAdapter(Message), TypeAdapter(Message), TypeAdapter(State), TypeAdapter(State)
    ) as connection:
        connection.send(Message(text="sent"))
        snapshot, message = list(connection)
        assert isinstance(snapshot, StateSnapshot)
        assert snapshot.state.count == 1
        assert message.text == "hello"
    assert json.loads(wire.sent[0]) == {"text": "sent"}
    assert wire.closed
