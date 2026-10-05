import asyncio
import json
import os
import sys
import tempfile
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from durable_actors.build import build_actor


def build(tmp_path, module, source):
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    (project / f"{module}.py").write_text(source)
    build_actor(project, f"{module}.py", tmp_path / "build")
    return str(tmp_path / "build/actors.pyz")


class ExecutorHost:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def send(self, message):
        self.writer.write((json.dumps(message) + "\n").encode())

    async def receive(self):
        line = await self.reader.readline()
        if not line:
            raise EOFError("executor disconnected")
        return json.loads(line)


@asynccontextmanager
async def warm_executor(entrypoint, environment):
    connected = asyncio.get_running_loop().create_future()

    async def accepted(reader, writer):
        connected.set_result(ExecutorHost(reader, writer))

    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        server = await asyncio.start_unix_server(accepted, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            "--generic",
            env={**os.environ, "DURABLE_ACTORS_EXECUTOR_SOCKET": socket},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(10):
                host = await connected
                assert await host.receive() == {"type": "warm", "protocol": 24}
                host.send({"type": "load", "entrypoint": entrypoint, "environment": environment})
                assert (await host.receive())["type"] == "attach"
                host.send({"type": "attached", "protocol": 24})
            yield host
        finally:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 5)
            server.close()
            if connected.done():
                connected.result().writer.transport.abort()
                with suppress(ConnectionError):
                    await asyncio.wait_for(connected.result().writer.wait_closed(), 1)
            await server.wait_closed()
