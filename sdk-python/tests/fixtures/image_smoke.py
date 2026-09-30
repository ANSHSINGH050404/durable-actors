import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import httpx

from durable_actors import Client


def main() -> None:
    with tempfile.TemporaryDirectory() as directory, socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()
        Path(directory, "actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
        for expected in (1, 2):
            runtime = subprocess.Popen(
                [
                    "durable-actors",
                    "dev",
                    "--project",
                    directory,
                    "--entrypoint",
                    "actors.py",
                    "--port",
                    str(port),
                ],
                env={**os.environ, "DURABLE_ACTORS_PYTHON": "python3"},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                with (
                    httpx.Client(timeout=5) as http,
                    Client(f"http://127.0.0.1:{port}", http=http) as client,
                ):
                    deadline = time.monotonic() + 30
                    while True:
                        if runtime.poll() is not None:
                            raise RuntimeError(runtime.communicate())
                        if time.monotonic() >= deadline:
                            raise TimeoutError("actor runtime did not start")
                        try:
                            contract = client.get_contract()
                            assert contract["actors"][0]["actorName"] == "Counter"
                            break
                        except (httpx.TransportError, httpx.HTTPStatusError):
                            time.sleep(0.1)
                    assert client.invoke("Counter", "one", "increment", []) == expected
            finally:
                if runtime.poll() is None:
                    runtime.terminate()
                try:
                    runtime.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    runtime.kill()
                    runtime.communicate()
                    raise
    print("Python actor image: invocation and durable restart passed")


main()
