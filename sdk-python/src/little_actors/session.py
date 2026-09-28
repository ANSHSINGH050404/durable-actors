"""Synchronous actor transports with renewable application sessions."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import RLock, Timer
from types import TracebackType
from typing import Any, Protocol
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict

from .client import ActorTransport, Client, SocketGrant, component, validate_origin


class ActorSession(BaseModel):
    """Short-lived credentials returned by a trusted application backend.

    expires_at_ms is a Unix timestamp in milliseconds. Sessions must belong
    to the requested project and use HTTPS outside localhost.
    """

    model_config = ConfigDict(strict=True, frozen=True)
    project_id: str
    control_plane_url: str
    token: str
    expires_at_ms: int


class ActorSessionRejectedError(Exception):
    """The application backend explicitly denied or revoked actor access."""


class SessionClient(ActorTransport, Protocol):
    """An owned actor transport that can release its connections."""

    def close(self) -> None: ...


@dataclass
class _Lease:
    client: SessionClient
    expires_at: float
    users: int = 0


class ActorSessionTransport:
    """Obtain and renew credentials while reusing an actor transport.

    get_session is synchronous and should raise ActorSessionRejectedError on
    explicit denial. Renewal runs on a background thread and stops after a
    minute without activity. close() stops renewal and releases connections;
    active requests retain their transport until they finish. Supports with.
    """

    def __init__(
        self,
        *,
        project_id: str,
        get_session: Callable[[], ActorSession],
        create_transport: Callable[[ActorSession], SessionClient] | None = None,
        now: Callable[[], float] = time.time,
        schedule: Callable[[Callable[[], None], float], Callable[[], None]] | None = None,
    ) -> None:
        """Configure renewal; injected clock/scheduler durations use seconds.

        create_transport receives validated credentials and returns an owned
        transport. The default creates a Client with the session's credentials.
        """
        self._project_id = component(project_id, 64)
        self._get_session = get_session
        self._create_transport = create_transport or session_client
        self._now = now
        self._schedule = schedule or schedule_refresh
        self._lock = RLock()
        self._current: _Lease | None = None
        self._cancel: Callable[[], None] | None = None
        self._closed = False
        self._last_used = 0.0

    def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        """Invoke using a current session, refreshing before expiry when needed."""
        with self._acquire() as client:
            return client.invoke(actor_name, actor_id, method, args)

    def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        authorization_lifetime_ms: int = 900000,
        home_region: str | None = None,
    ) -> SocketGrant:
        """Issue a WebSocket grant with the current session's permissions."""
        with self._acquire() as client:
            return client.prepare_websocket(
                actor_name,
                actor_id,
                metadata,
                authorization_lifetime_ms=authorization_lifetime_ms,
                home_region=home_region,
            )

    def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        """Broadcast using the current session's permissions."""
        with self._acquire() as client:
            client.broadcast(actor_name, actor_id, message)

    def close(self) -> None:
        """Stop renewal and close owned transports once their active calls finish."""
        with self._lock:
            self._closed = True
            if self._cancel:
                self._cancel()
            self._replace(None)

    def __enter__(self) -> ActorSessionTransport:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @contextmanager
    def _acquire(self) -> Generator[SessionClient]:
        with self._lock:
            if self._closed:
                raise RuntimeError("session transport is closed")
            self._last_used = self._now()
            if self._current is None or self._current.expires_at <= self._now() + 5:
                self._refresh()
            lease = self._current
            assert lease is not None
            lease.users += 1
        try:
            yield lease.client
        finally:
            with self._lock:
                lease.users -= 1
                if lease is not self._current and not lease.users:
                    lease.client.close()

    def _refresh(self) -> None:
        if self._cancel:
            self._cancel()
        try:
            session = self._get_session()
        except ActorSessionRejectedError:
            self._replace(None)
            raise
        remaining = self._validate(session)
        if self._closed:
            raise RuntimeError("session transport is closed")
        self._replace(_Lease(self._create_transport(session), session.expires_at_ms / 1000))
        self._cancel = self._schedule(self._renew, remaining * random.uniform(0.75, 0.8))

    def _validate(self, session: ActorSession) -> float:
        origin = urlsplit(validate_origin(session.control_plane_url))
        if origin.scheme != "https" and origin.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("actor sessions require HTTPS outside localhost")
        remaining = session.expires_at_ms / 1000 - self._now()
        if (
            session.project_id != self._project_id
            or not session.token.strip()
            or not 5 < remaining <= 65
        ):
            raise ValueError("actor session is expired or invalid")
        return remaining

    def _replace(self, lease: _Lease | None) -> None:
        old, self._current = self._current, lease
        if old is not None and not old.users:
            old.client.close()

    def _renew(self) -> None:
        with self._lock:
            if self._closed or self._now() - self._last_used >= 60:
                return
            try:
                self._refresh()
            except Exception:
                if self._current is not None and self._current.expires_at > self._now() + 5:
                    self._cancel = self._schedule(self._renew, 5)


def session_client(session: ActorSession) -> Client:
    return Client(session.control_plane_url, project_id=session.project_id, api_key=session.token)


def schedule_refresh(callback: Callable[[], None], delay: float) -> Callable[[], None]:
    timer = Timer(delay, callback)
    timer.daemon = True
    timer.start()
    return timer.cancel
