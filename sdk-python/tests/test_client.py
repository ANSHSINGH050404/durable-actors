import os
import subprocess
import sys

import httpx
import pytest

from durable_actors.client import ActorInvocationError, Client


def test_caches_route_and_retries_only_explicit_non_execution():
    calls = []
    target = {
        "route": "http://host.test",
        "token": "ticket",
        "ownerEpoch": 1,
        "expiresAtMs": 9999999999999,
    }

    def handle(request):
        calls.append(request.url.host)
        if request.url.host == "host.test":
            return httpx.Response(200, json={"type": "not_executed", "reason": "stale_owner"})
        return httpx.Response(
            200, json={"target": target, "outcome": {"type": "completed", "result": 3}}
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            assert client.invoke("Counter", "one", "increment", []) == 3
            assert client.invoke("Counter", "one", "increment", []) == 3
    assert calls == ["control.test", "host.test", "control.test"]


def test_lost_response_is_not_replayed():
    calls = []

    def handle(request):
        calls.append(request)
        raise httpx.ReadError("response lost")

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            with pytest.raises(ActorInvocationError) as failure:
                client.invoke("Counter", "one", "increment", [])
    assert failure.value.code == "outcome_unknown"
    assert len(calls) == 1


def test_default_client_shares_its_pool_and_closes_it_at_process_exit(tmp_path):
    closed = tmp_path / "closed"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import httpx
from durable_actors.client import default_client

class TrackedHttpClient(httpx.Client):
    def close(self):
        super().close()
        assert self.is_closed
        Path(sys.argv[1]).write_text("closed")

httpx.Client = TrackedHttpClient
with ThreadPoolExecutor(max_workers=8) as pool:
    clients = list(pool.map(lambda _: default_client(), range(32)))
assert all(client is clients[0] for client in clients)
""",
            str(closed),
        ],
        env={**os.environ, "DURABLE_ACTORS_CONTROL_PLANE_URL": "http://127.0.0.1:7100"},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert closed.read_text() == "closed"
