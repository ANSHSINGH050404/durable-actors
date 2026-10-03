import importlib
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from queue import Queue

import httpx
import pytest

from durable_actors.client import Client
from durable_actors.codegen import generate_client


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_python_actor_generated_client_and_durable_restart(tmp_path):
    (tmp_path / "actors.py").write_text("""from pydantic import BaseModel
from durable_actors import Actor, persisted, sandbox
import os
class Count(BaseModel):
    value: int
@sandbox(cpu=0.5, memory_mib=512, idle_timeout_ms=60000)
class Counter(Actor):
    'A durable counter.'
    count: int = persisted(0)
    def increment(self, amount: int = 1) -> Count:
        'Increment the count and return its new value.'
        self.count += amount
        return Count(value=self.count)
    def crash(self) -> None:
        os._exit(17)
""")
    port = free_port()
    for expected in (2, 4):
        with actor_server(tmp_path, "actors.py", port) as (client, contract):
            assert contract["actors"][0]["sandbox"] == {
                "cpu": 0.5,
                "memoryMiB": 512,
                "idleTimeoutMs": 60000,
            }
            generate_client(contract, tmp_path / "remote")
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from remote import actors; "
                    'assert actors.Counter.__doc__ == "A durable counter."; '
                    'assert actors.Counter.Stub.increment.__doc__ == "Increment the count and return its new value."; '
                    'print(actors.Counter.get("one").increment(2).value)',
                ],
                cwd=tmp_path,
                env={**os.environ, "DURABLE_ACTORS_CONTROL_PLANE_URL": client.origin},
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            assert int(result.stdout) == expected
            from durable_actors import ActorInvocationError

            with pytest.raises(ActorInvocationError) as error:
                client.invoke("Counter", "one", "crash", [])
            assert error.value.code == "actor_error"
            assert client.invoke("Counter", "one", "increment", [0]) == {"value": expected}


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_python_generated_socket_and_state_events(tmp_path, monkeypatch):
    from durable_actors import StateSnapshot, StateUpdate

    (tmp_path / "socket_actors.py").write_text("""from pydantic import BaseModel
from durable_actors import Actor, ActorSocket, emitted, interleave, persisted
class Payload(BaseModel):
    value: int
class Room(Actor[Payload, Payload, Payload]):
    count: int = emitted(persisted(0))
    def on_connect(self, socket: ActorSocket[Payload, Payload]) -> None:
        socket.set_tags("connected")
    @interleave
    def on_message(self, socket: ActorSocket[Payload, Payload], message: Payload) -> None:
        self.count += message.value
        socket.send(Payload(value=self.count + socket.metadata.value))
    def count_connections(self) -> int:
        return len(self.get_connections())
""")
    with actor_server(tmp_path, "socket_actors.py", free_port()) as (client, contract):
        generate_client(contract, tmp_path / "socket_client")
        monkeypatch.syspath_prepend(str(tmp_path))
        remote = importlib.import_module("socket_client")
        models = remote.actors.Room
        room = remote.actors.Room.get("lobby", client)
        with room.connect(models.Payload(value=2)) as connection:
            initial = connection.receive(timeout=5)
            assert isinstance(initial, StateSnapshot)
            assert initial.state.count == 0
            assert room.count_connections() == 1
            connection.send(models.Payload(value=3))
            message = connection.receive(timeout=5)
            assert isinstance(message, models.Payload)
            assert message.value == 5
            update = connection.receive(timeout=5)
            assert isinstance(update, StateUpdate)
            assert update.changes.count == 3
            room.broadcast(models.Payload(value=17))
            assert connection.receive(timeout=5).value == 17
            with pytest.raises(TimeoutError):
                connection.receive(timeout=0.01)
        source = importlib.import_module("socket_actors")
        reference = source.Room.get("lobby", client)
        with reference.connect(metadata=source.Payload(value=2)) as connection:
            snapshot = connection.receive(timeout=5)
            assert isinstance(snapshot, StateSnapshot)
            assert snapshot.state == {"count": 3}
            reference.broadcast(source.Payload(value=19))
            assert connection.receive(timeout=5).value == 19


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_generated_subscription_delivers_state_while_calling_rpcs(tmp_path, monkeypatch):
    (tmp_path / "actors.py").write_text("""from durable_actors import Actor, emitted, persisted
class Counter(Actor[None, None, None]):
    count: int = emitted(persisted(0))
    label: str = emitted(persisted("ready"))
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
    with actor_server(tmp_path, "actors.py", free_port()) as (client, contract):
        generate_client(contract, tmp_path / "subscription_client")
        monkeypatch.syspath_prepend(str(tmp_path))
        remote = importlib.import_module("subscription_client")
        counter = remote.actors.Counter.get("one", client)
        states = Queue()
        subscription = counter.subscribe(states.put)
        try:
            initial = states.get(timeout=5)
            assert (initial.count, initial.label) == (0, "ready")
            assert counter.increment() == 1
            updated = states.get(timeout=5)
            assert (updated.count, updated.label) == (1, "ready")
        finally:
            subscription.close()
        assert subscription.closed
        assert subscription.error is None


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_sync_reentrant_rpc_allows_another_rpc_to_release_its_wait(tmp_path, monkeypatch):
    (tmp_path / "actors.py").write_text("""from threading import Event
from durable_actors import Actor, ephemeral, interleave, persisted
class Waiting(Actor):
    count: int = persisted(0)
    entered: Event = ephemeral(default_factory=Event)
    release: Event = ephemeral(default_factory=Event)
    @interleave
    def hold(self) -> int:
        self.entered.set()
        if not self.release.wait(5):
            raise TimeoutError("waiting for finish")
        self.count += 10
        return self.count
    def ready(self) -> bool:
        return self.entered.is_set()
    def finish(self) -> int:
        self.count += 1
        result = self.count
        self.release.set()
        return result
    def read(self) -> int:
        return self.count
""")
    port = free_port()
    with actor_server(tmp_path, "actors.py", port) as (client, contract):
        generate_client(contract, tmp_path / "reentrant_client")
        monkeypatch.syspath_prepend(str(tmp_path))
        remote = importlib.import_module("reentrant_client")
        actor = remote.actors.Waiting.get("one", client)
        with ThreadPoolExecutor(max_workers=1) as callers:
            pending = callers.submit(actor.hold)
            deadline = time.monotonic() + 3
            while not actor.ready():
                assert time.monotonic() < deadline
                time.sleep(0.01)
            assert actor.finish() == 1
            assert pending.result(timeout=3) == 11
            assert actor.read() == 11
    with actor_server(tmp_path, "actors.py", port) as (client, _):
        assert remote.actors.Waiting.get("one", client).read() == 11


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_actor_calls_another_actor_through_a_source_reference(tmp_path):
    (tmp_path / "relay_actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
class Relay(Actor):
    def forward(self) -> int:
        return Counter.get("target").increment(3)
""")
    with actor_server(tmp_path, "relay_actors.py", free_port()) as (client, _):
        assert client.invoke("Relay", "one", "forward", []) == 3
        assert client.invoke("Relay", "one", "forward", []) == 6


@contextmanager
def actor_server(project, entrypoint, port):
    with (project / "runtime.log").open("w+") as log:
        process = subprocess.Popen(
            [
                os.environ["DURABLE_ACTORS_TEST_RUNTIME"],
                "dev",
                "--project",
                str(project),
                "--entrypoint",
                entrypoint,
                "--port",
                str(port),
            ],
            env={**os.environ, "DURABLE_ACTORS_PYTHON": sys.executable},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            with httpx.Client(timeout=5) as http:
                with Client(f"http://127.0.0.1:{port}", http=http) as client:
                    yield client, wait_for_contract(client, process, log)
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise


def wait_for_contract(client, process, log):
    deadline = time.monotonic() + 30
    while process.poll() is None and time.monotonic() < deadline:
        try:
            return client.get_contract()
        except (httpx.TransportError, httpx.HTTPStatusError):
            time.sleep(0.1)
    log.seek(0)
    pytest.fail("runtime did not become ready: " + log.read())


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
