"""Synchronous transport, connection grants, and invocation errors."""

from __future__ import annotations

import atexit
import json
import os
import re
import sys
import time
import uuid
from collections.abc import Callable
from threading import Lock
from types import TracebackType
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict

from .guards import is_document


class ActorInvocationError(Exception):
    """An RPC failure reported with its code and request identifier.

    Attributes:
        code: Failure category. "outcome_unknown" means execution may have
            happened even though its response was lost; retrying can repeat work.
        request_id: Identifier for correlating the invocation with runtime logs.

    str(error) returns the failure message.
    """

    def __init__(self, code: str, request_id: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id


class ActorProtocolError(Exception):
    """The runtime returned an invalid response or violated the actor wire protocol."""

    pass


class SocketGrant(BaseModel):
    """Authorization to open a WebSocket before its connection deadline.

    Attributes:
        websocket_url: Authorized ws:// or wss:// URL to connect to.
        home_region: Region hosting the actor.
        connect_by_ms: Latest connection time, as Unix epoch milliseconds.
        authorized_until_ms: Authorization expiry, as Unix epoch milliseconds.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=lambda name: {
            "websocket_url": "websocketUrl",
            "home_region": "homeRegion",
            "connect_by_ms": "connectByMs",
            "authorized_until_ms": "authorizedUntilMs",
        }[name],
    )
    websocket_url: str
    home_region: str
    connect_by_ms: int
    authorized_until_ms: int


class RpcTransport(Protocol):
    """Synchronous RPC boundary implemented by Client and custom transports."""

    def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any: ...


class ActorTransport(RpcTransport, Protocol):
    """Transport supporting synchronous RPCs, WebSocket authorization, and broadcasts."""

    def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        authorization_lifetime_ms: int = 900000,
        home_region: str | None = None,
    ) -> SocketGrant: ...

    def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None: ...


class Client:
    """Synchronous HTTP transport for actor RPCs, contracts, and socket grants.

    Generated clients use an SDK-managed shared instance by default. Construct
    a Client when configuration or lifetime must differ, then pass it to the
    generated actor client. Explicit clients support with and close().
    """

    def __init__(
        self,
        control_plane_url: str | None = None,
        *,
        project_id: str | None = None,
        api_key: str | None = None,
        home_region: str | None = None,
        http: httpx.Client | None = None,
        telemetry: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        """Configure a transport, using environment values for omitted options.

        Args:
            control_plane_url: Runtime origin. Defaults to
                DURABLE_ACTORS_CONTROL_PLANE_URL or http://127.0.0.1:7100.
            project_id: Project identity. Defaults to DURABLE_ACTORS_PROJECT_ID,
                or "local" for localhost origins. Required for other origins.
            api_key: Bearer credential. Defaults to DURABLE_ACTORS_SECRET,
                falling back to DURABLE_ACTORS_API_KEY.
            home_region: Optional placement preference, falling back to
                DURABLE_ACTORS_HOME_REGION.
            http: Optional caller-owned httpx.Client. When omitted, this transport
                creates and owns its HTTP client with a 180-second timeout.
            telemetry: Receives invocation timing and outcome, without payloads or
                credentials. Defaults to stderr when DURABLE_ACTORS_TELEMETRY=1.
        """
        self.origin = validate_origin(
            control_plane_url
            or os.environ.get("DURABLE_ACTORS_CONTROL_PLANE_URL", "http://127.0.0.1:7100")
        )
        local = urlsplit(self.origin).hostname in {"localhost", "127.0.0.1", "::1"}
        self.project_id = component(
            project_id or os.environ.get("DURABLE_ACTORS_PROJECT_ID") or ("local" if local else ""),
            64,
        )
        key = (
            api_key
            if api_key is not None
            else os.environ.get("DURABLE_ACTORS_SECRET", os.environ.get("DURABLE_ACTORS_API_KEY"))
        )
        if key is not None and not key.strip():
            raise ValueError("API key must not be empty")
        self.headers = {"authorization": f"Bearer {key.strip()}"} if key is not None else {}
        self.home_region = home_region or os.environ.get("DURABLE_ACTORS_HOME_REGION")
        if self.home_region is not None:
            component(self.home_region, 255)
        self._telemetry = telemetry or stderr_telemetry
        self._http = http if http is not None else httpx.Client(timeout=180, follow_redirects=False)
        self._owns_http = http is None
        self._closed = False
        self._targets: dict[tuple[str, str], dict[str, Any]] = {}

    def __enter__(self) -> Client:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close this transport and its owned HTTP client; injected HTTP clients stay open."""
        self._closed = True
        if self._owns_http:
            self._http.close()

    def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        """Invoke an actor method synchronously with JSON-compatible arguments.

        Args:
            actor_name: Exported actor class name.
            actor_id: Identity of the actor instance.
            method: Public RPC method name.
            args: Positional wire arguments in contract order.

        Returns:
            The JSON-compatible result. Generated clients decode its concrete type.

        Raises:
            ActorInvocationError: Execution failed or its outcome is unknown.
            ActorProtocolError: The server returned an invalid invocation outcome.

        Only failures known to precede execution are automatically retried.
        """
        request_id = str(uuid.uuid4())
        started = time.perf_counter()
        outcome = "failed"
        try:
            result = self._invoke(actor_name, actor_id, method, args, request_id)
            outcome = "completed"
            return result
        finally:
            self._telemetry(
                {
                    "type": "actor_invocation",
                    "request_id": request_id,
                    "actor_name": actor_name,
                    "actor_id": actor_id,
                    "method": method,
                    "outcome": outcome,
                    "completed_at_ms": (time.perf_counter() - started) * 1000,
                }
            )

    def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        authorization_lifetime_ms: int = 900000,
        home_region: str | None = None,
    ) -> SocketGrant:
        """Request a short-lived WebSocket grant without opening a connection.

        Args:
            actor_name: Exported actor class name.
            actor_id: Identity of the actor instance.
            metadata: JSON-compatible connection metadata, limited to 16 KiB.
            authorization_lifetime_ms: Duration from 1,000 to 86,400,000
                milliseconds; defaults to 15 minutes.
            home_region: Placement preference overriding the client default.

        Returns:
            The authorized URL, home region, and connection/authorization deadlines.
        """
        self._ensure_open()
        placement = home_region if home_region is not None else self.home_region
        if placement is not None:
            component(placement, 255)
        if (
            type(authorization_lifetime_ms) is not int
            or not 1000 <= authorization_lifetime_ms <= 86400000
        ):
            raise ValueError("authorization lifetime must be between one second and one day")
        if len(json.dumps(metadata, allow_nan=False).encode()) > 16384:
            raise ValueError("socket metadata exceeds 16 KiB")
        response = self._http.post(
            self.origin + self.actor_path(actor_name, actor_id) + "/find-websocket",
            headers=self.headers,
            json={
                "metadata": metadata,
                "authorizationLifetimeMs": authorization_lifetime_ms,
                **({"homeRegion": placement} if placement is not None else {}),
            },
            follow_redirects=False,
        )
        response.raise_for_status()
        grant = SocketGrant.model_validate(response.json(), strict=True)
        if urlsplit(grant.websocket_url).scheme not in {"ws", "wss"}:
            raise ActorProtocolError("invalid websocket URL")
        return grant

    def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        """Send an application message to all actor connections from a backend.

        Delivery is not persisted. An uncertain delivery raises outcome_unknown
        and is never replayed automatically.
        """
        self._ensure_open()
        path = self.actor_path(actor_name, actor_id)
        if is_document(message) and message.get("type") in {"state", "state_update"}:
            raise ValueError("state and state_update messages are reserved")
        data = json.dumps(message, separators=(",", ":"), allow_nan=False)
        request_id = str(uuid.uuid4())
        key = actor_name, actor_id
        target = self._target(key, path, request_id)
        payload: dict[str, Any] = {
            "ownerEpoch": target["ownerEpoch"],
            "effects": [
                {
                    "type": "broadcast",
                    "message": {"type": "text", "data": data},
                    "except_connection_ids": [],
                    "tags": [],
                }
            ],
        }
        try:
            response = self._http.post(
                target["route"] + path + "/socket-effects",
                headers={"authorization": f"Bearer {target['token']}", "x-request-id": request_id},
                json=payload,
                follow_redirects=False,
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            self._targets.pop(key, None)
            raise ActorInvocationError(
                "outcome_unknown", request_id, "broadcast delivery could not be confirmed"
            ) from error

    def get_contract(self) -> dict[str, Any]:
        """Fetch the published actor contract used to generate typed clients."""
        self._ensure_open()
        response = self._http.get(
            f"{self.origin}/v1/projects/{self.project_id}/deployment/contract",
            headers=self.headers,
            follow_redirects=False,
        )
        response.raise_for_status()
        value = document(response)
        if not re.fullmatch(
            r"sha256:[a-f0-9]{64}", value.get("contractHash", "")
        ) or not isinstance(value.get("contract"), dict):
            raise ActorProtocolError("invalid contract publication")
        return cast(dict[str, Any], value["contract"])

    def actor_path(self, actor_name: str, actor_id: str) -> str:
        """Return the project-scoped HTTP path after validating the actor name and ID."""
        return f"/v1/projects/{self.project_id}/actors/{component(actor_name, 255)}/{component(actor_id, 128)}"

    def _invoke(
        self, actor_name: str, actor_id: str, method: str, args: list[Any], request_id: str
    ) -> Any:
        if self._closed:
            raise RuntimeError("client is closed")
        path = self.actor_path(actor_name, actor_id)
        component(method, 255)
        json.dumps(args, allow_nan=False)
        key = actor_name, actor_id
        for attempt in range(2):
            target = self._targets.get(key)
            if target is not None and target["expiresAtMs"] <= time.time() * 1000:
                target = None
                self._targets.pop(key, None)
            try:
                reply = self._invoke_attempt(path, request_id, method, args, target, key)
            except httpx.TransportError as error:
                self._targets.pop(key, None)
                if target is not None and isinstance(error, httpx.ConnectError) and refused(error):
                    reply = {"type": "not_executed", "reason": "upstream_not_reached"}
                else:
                    raise ActorInvocationError(
                        "outcome_unknown",
                        request_id,
                        "invocation response was lost; execution may have occurred",
                    ) from error
            kind = reply.get("type")
            if kind == "completed" and "result" in reply:
                return reply["result"]
            if (
                kind == "failed"
                and isinstance(reply.get("code"), str)
                and isinstance(reply.get("message"), str)
            ):
                raise ActorInvocationError(reply["code"], request_id, reply["message"])
            if kind != "unauthenticated" and not (
                kind == "not_executed"
                and reply.get("reason")
                in {"stale_owner", "host_unavailable", "upstream_not_reached"}
            ):
                raise ActorProtocolError("invalid invocation outcome")
            self._targets.pop(key, None)
            if attempt:
                raise ActorInvocationError(
                    "unauthenticated" if kind == "unauthenticated" else "unavailable",
                    request_id,
                    "actor rejected invocation before execution",
                )
        raise AssertionError("unreachable")

    def _target(self, key: tuple[str, str], path: str, request_id: str) -> dict[str, Any]:
        target = self._targets.get(key)
        if target is not None and target["expiresAtMs"] > time.time() * 1000 + 5000:
            return target
        response = self._http.post(
            self.origin + path + "/find",
            headers={**self.headers, "x-request-id": request_id},
            json={"homeRegion": self.home_region} if self.home_region else {},
            follow_redirects=False,
        )
        response.raise_for_status()
        target = validate_target(document(response))
        self._targets[key] = target
        return target

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("client is closed")

    def _invoke_attempt(
        self,
        path: str,
        request_id: str,
        method: str,
        args: list[Any],
        target: dict[str, Any] | None,
        key: tuple[str, str],
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"requestId": request_id, "method": method, "args": args}
        headers = self.headers
        origin = self.origin
        if target is not None:
            body["ownerEpoch"] = target["ownerEpoch"]
            origin = target["route"]
            headers = {"authorization": f"Bearer {target['token']}"}
        elif self.home_region:
            body["homeRegion"] = self.home_region
        response = self._http.post(
            origin + path + "/invoke",
            headers={**headers, "x-request-id": request_id},
            json=body,
            follow_redirects=False,
        )
        if target is not None and response.status_code == 401:
            return {"type": "unauthenticated"}
        value = document(response)
        if not response.is_success:
            error = value.get("error", {})
            if response.status_code in {401, 403}:
                raise ActorInvocationError(
                    "unauthenticated", request_id, "application credential rejected"
                )
            if not isinstance(error, dict):
                raise ActorProtocolError("invalid invocation error")
            error = cast(dict[str, Any], error)
            if not isinstance(error.get("code"), str) or not isinstance(error.get("message"), str):
                raise ActorProtocolError(f"invalid HTTP {response.status_code} error")
            raise ActorInvocationError(error["code"], request_id, error["message"])
        if target is not None:
            return value
        target = validate_target(value.get("target"))
        reply = value.get("outcome")
        if not isinstance(reply, dict):
            raise ActorProtocolError("missing invocation outcome")
        reply = cast(dict[str, Any], reply)
        if reply.get("type") in {"completed", "failed"}:
            self._targets[key] = target
        return reply


_shared_client: Client | None = None
_shared_client_lock = Lock()


def default_client() -> Client:
    global _shared_client
    with _shared_client_lock:
        if _shared_client is None:
            _shared_client = Client()
            atexit.register(_shared_client.close)
        return _shared_client


def component(value: str, maximum: int) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value) or value in {".", ".."} or len(value) > maximum:
        raise ValueError("invalid actor identifier")
    return value


def validate_origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("expected an HTTP or HTTPS origin")
    return value.rstrip("/")


def validate_target(value: Any) -> dict[str, Any]:
    if not is_document(value):
        raise ActorProtocolError("invalid actor target")
    if not isinstance(value.get("token"), str) or not value["token"].strip():
        raise ActorProtocolError("invalid actor target")
    for key in ("ownerEpoch", "expiresAtMs"):
        if type(value.get(key)) is not int or not 0 < value[key] <= 2**53 - 1:
            raise ActorProtocolError("invalid actor target")
    validate_origin(value["route"])
    return value


def document(response: httpx.Response) -> dict[str, Any]:
    try:
        value = response.json()
    except ValueError as error:
        raise ActorProtocolError("response was not JSON") from error
    if not is_document(value):
        raise ActorProtocolError("response must be a JSON object")
    return value


def refused(error: BaseException) -> bool:
    import errno

    seen: set[int] = set()
    while id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, OSError) and error.errno == errno.ECONNREFUSED:
            return True
        cause = error.__cause__ or error.__context__
        if cause is None:
            return False
        error = cause
    return False


def stderr_telemetry(event: dict[str, Any]) -> None:
    if os.environ.get("DURABLE_ACTORS_TELEMETRY") == "1":
        print(json.dumps(event), file=sys.stderr)
