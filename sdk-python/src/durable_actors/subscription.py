"""Background subscriptions that merge emitted-state snapshots and patches."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from threading import Event, Thread, current_thread
from types import TracebackType
from typing import Any, Generic, Protocol, TypeVar

from pydantic import BaseModel, TypeAdapter
from websockets.exceptions import ConnectionClosedOK

from .client import ActorProtocolError
from .connection import StateSnapshot, StateUpdate

State = TypeVar("State", bound=BaseModel)


class StateStream(Protocol):
    def receive(self) -> object: ...
    def close(self) -> None: ...


class Subscription(Generic[State]):
    """Receive complete typed emitted state on a background thread.

    Create through a generated actor's subscribe(). The callback receives the
    initial snapshot and subsequent merged updates serially on the receiver
    thread. Each callback gets a fresh model. Application messages are ignored.

    Receiving, validation, or callback failures stop the subscription, populate
    error, and call on_error on the receiver thread, or are logged if no handler
    was supplied. A normal remote close stops without an error. Reconnect by
    creating a new subscription.

    Call close() or use with for cleanup. The receiver is a daemon thread and
    does not keep an otherwise finished process alive.
    """

    def __init__(
        self,
        connection: StateStream,
        callback: Callable[[State], None],
        state: TypeAdapter[State],
        *,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._connection = connection
        self._callback = callback
        self._state = _StateAccumulator(state)
        self._on_error = on_error
        self._error: Exception | None = None
        self._stopping = Event()
        self._done = Event()
        self._thread = Thread(target=self._run, name="durable-actors-subscription", daemon=True)
        self._thread.start()

    @property
    def closed(self) -> bool:
        """Whether the receiver has finished, including callback and connection cleanup."""
        return self._done.is_set()

    @property
    def error(self) -> Exception | None:
        """Failure that stopped the receiver, or None after normal closure."""
        return self._error

    def close(self) -> None:
        """Stop receiving, close the socket, and wait for the active callback to finish.

        Safe to call from the callback itself; that call does not wait on its own
        thread. In that case closed becomes true after the callback returns.
        """
        self._stopping.set()
        self._connection.close()
        if current_thread() is not self._thread:
            self._thread.join()

    def __enter__(self) -> Subscription[State]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                try:
                    event = self._connection.receive()
                except ConnectionClosedOK:
                    break
                state = self._state.apply(event)
                if state is not None and not self._stopping.is_set():
                    self._callback(state)
        except Exception as error:
            if not self._stopping.is_set():
                self._report(error)
        finally:
            try:
                self._connection.close()
            finally:
                self._done.set()

    def _report(self, error: Exception) -> None:
        self._error = error
        if self._on_error is None:
            logging.getLogger(__name__).exception("Actor state subscription stopped")
            return
        try:
            self._on_error(error)
        except Exception:
            logging.getLogger(__name__).exception("Actor subscription error handler failed")


class _StateAccumulator(Generic[State]):
    def __init__(self, adapter: TypeAdapter[State]) -> None:
        self._adapter = adapter
        self._value: dict[str, Any] | None = None
        self._version = -1

    def apply(self, event: object) -> State | None:
        if not isinstance(event, (StateSnapshot, StateUpdate)) or event.version <= self._version:
            return None
        value = event.model_dump(mode="json", by_alias=True, exclude_unset=True)
        if isinstance(event, StateSnapshot):
            self._value = value["state"]
        else:
            if self._value is None:
                raise ActorProtocolError("state update arrived before the initial snapshot")
            self._value.update(value["changes"])
            for name in value["removed"]:
                self._value.pop(name, None)
        state = self._adapter.validate_json(json.dumps(self._value), strict=True)
        self._version = event.version
        return state
