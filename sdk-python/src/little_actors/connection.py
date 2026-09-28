"""Typed synchronous WebSockets and emitted-state events."""

from __future__ import annotations

import json
from types import TracebackType
from typing import Generic, Literal, Protocol, TypeVar

from pydantic import BaseModel, TypeAdapter
from websockets.exceptions import ConnectionClosedOK
from websockets.sync.client import connect

from .client import ActorProtocolError, SocketGrant
from .contract import encode
from .guards import is_document

Send = TypeVar("Send")
Receive = TypeVar("Receive")
State = TypeVar("State")
Patch = TypeVar("Patch")


class StateSnapshot(BaseModel, Generic[State]):
    """Complete emitted state received when a subscription connects.

    Attributes:
        type: Wire discriminator, "state".
        state: Typed values of emitted fields.
        version: Nonnegative state version used to order updates.
    """

    type: Literal["state"] = "state"
    state: State
    version: int


class StateUpdate(BaseModel, Generic[Patch]):
    """A patch to previously received emitted state.

    Attributes:
        type: Wire discriminator, "state_update".
        changes: Typed changed fields; omitted fields are unchanged.
        removed: Names of fields removed from the state.
        version: Nonnegative state version used to order updates.

    Use changes.model_dump(exclude_unset=True) to inspect only supplied fields.
    """

    type: Literal["state_update"] = "state_update"
    changes: Patch
    removed: list[str]
    version: int


class SocketWire(Protocol):
    def send(self, message: str) -> None: ...
    def recv(self, timeout: float | None = None) -> str | bytes: ...
    def close(self, code: int = 1000, reason: str = "") -> None: ...


class Connection(Generic[Send, Receive, State, Patch]):
    """Typed synchronous WebSocket returned by a generated actor's connect().

    Send application messages with send(). Read application messages and state
    events with receive() or iteration. Normal remote closure ends iteration.
    Close explicitly or use with. Generic parameters describe sent messages,
    received messages, emitted state, and state patches, respectively.
    """

    def __init__(
        self,
        wire: SocketWire,
        incoming: TypeAdapter[Send],
        outgoing: TypeAdapter[Receive],
        state: TypeAdapter[State],
        patch: TypeAdapter[Patch],
    ) -> None:
        self._wire = wire
        self._incoming = incoming
        self._outgoing = outgoing
        self._state = state
        self._patch = patch

    @classmethod
    def open(
        cls,
        grant: SocketGrant,
        incoming: TypeAdapter[Send],
        outgoing: TypeAdapter[Receive],
        state: TypeAdapter[State],
        patch: TypeAdapter[Patch],
    ) -> Connection[Send, Receive, State, Patch]:
        """Open an authorized socket using the supplied wire-type adapters.

        Generated clients supply these adapters automatically through connect().
        The grant must still be valid when the WebSocket handshake occurs.
        """
        wire = connect(grant.websocket_url, max_size=16 * 1024 * 1024)
        return cls(wire, incoming, outgoing, state, patch)

    def send(self, message: Send) -> None:
        """Validate and send one application message as JSON text."""
        data = encode(self._incoming, message)
        self._wire.send(json.dumps(data, separators=(",", ":"), allow_nan=False))

    def receive(
        self, timeout: float | None = None
    ) -> Receive | StateSnapshot[State] | StateUpdate[Patch]:
        """Block for an application message, state snapshot, or state update.

        Args:
            timeout: Maximum wait in seconds; None waits without a deadline.

        Raises:
            TimeoutError: No message arrived within the timeout.
            ActorProtocolError: A received message violates the actor protocol.

        Values are validated against the generated types. Remote socket closure
        raises the underlying WebSocket connection-closed exception.
        """
        data = self._wire.recv(timeout=timeout)
        if not isinstance(data, str):
            raise ActorProtocolError("actor sockets require JSON text")
        value = json.loads(data)
        if is_document(value) and value.get("type") in {"state", "state_update"}:
            if type(value.get("version")) is not int or value["version"] < 0:
                raise ActorProtocolError("invalid state version")
            if value["type"] == "state":
                state = self._state.validate_json(json.dumps(value["state"]), strict=True)
                return StateSnapshot[State](state=state, version=value["version"])
            patch = self._patch.validate_json(json.dumps(value["changes"]), strict=True)
            removed = TypeAdapter(list[str]).validate_python(value["removed"], strict=True)
            return StateUpdate[Patch](changes=patch, removed=removed, version=value["version"])
        return self._outgoing.validate_json(data, strict=True)

    def __iter__(self) -> Connection[Send, Receive, State, Patch]:
        return self

    def __next__(self) -> Receive | StateSnapshot[State] | StateUpdate[Patch]:
        try:
            return self.receive()
        except ConnectionClosedOK:
            raise StopIteration from None

    def close(self, code: int = 1000, reason: str = "") -> None:
        """Close the WebSocket with a status code and optional reason."""
        self._wire.close(code, reason)

    def __enter__(self) -> Connection[Send, Receive, State, Patch]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
