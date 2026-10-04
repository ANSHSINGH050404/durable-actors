import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fixtures.sqlite import commit, fields, seed

from durable_actors.build import build_actor


@pytest.mark.parametrize("generic", [False, True])
async def test_artifact_attaches_invokes_and_rehydrates_in_a_fresh_python_process(
    tmp_path, generic
):
    project = tmp_path / "project"
    project.mkdir()
    (project / "actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
""")
    contract = build_actor(project, "actors.py", tmp_path / "build")
    assert contract["actors"][0]["actorName"] == "Counter"
    actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
    state = seed()
    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        for expected in (2, 4):
            done = asyncio.get_running_loop().create_future()

            async def accepted(reader, writer):
                try:
                    if generic:
                        assert json.loads(await reader.readline()) == {
                            "type": "warm",
                            "protocol": 24,
                        }
                        writer.write(
                            (
                                json.dumps(
                                    {
                                        "type": "load",
                                        "entrypoint": str(tmp_path / "build/actors.pyz"),
                                    }
                                )
                                + "\n"
                            ).encode()
                        )
                        await writer.drain()
                    attached = json.loads(await reader.readline())
                    assert attached == {
                        "type": "attach",
                        "protocol": 24,
                        "actor_names": ["Counter"],
                    }
                    writer.write(b'{"type":"attached","protocol":24}\n')
                    writer.write(
                        (
                            json.dumps(
                                {
                                    "type": "command",
                                    "message_id": 1,
                                    "command": {
                                        "type": "invoke",
                                        "request_id": "r1",
                                        "actor": actor,
                                        "sqlite": state,
                                        "method": "increment",
                                        "args": [2],
                                    },
                                }
                            )
                            + "\n"
                        ).encode()
                    )
                    await writer.drain()
                    assert json.loads(await reader.readline()) == {
                        "type": "commit_sqlite",
                        "message_id": 1,
                    }
                    writer.write(
                        (
                            json.dumps(
                                {"type": "sqlite_committed", "message_id": 1, "txid": commit(state)}
                            )
                            + "\n"
                        ).encode()
                    )
                    await writer.drain()
                    reply = json.loads(await reader.readline())
                    assert reply["reply"]["result"] == expected
                    assert fields(state) == {"count": expected}
                    done.set_result({**state, **reply["reply"]["sqlite"]})
                except BaseException as error:
                    done.set_exception(error)
                finally:
                    writer.close()

            socket = str(Path(short) / "executor.sock")
            server = await asyncio.start_unix_server(accepted, socket)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "durable_actors.host",
                *(["--generic"] if generic else []),
                env={
                    **os.environ,
                    "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                    "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                state = await asyncio.wait_for(done, 10)
            finally:
                if process.returncode is None:
                    process.terminate()
                await process.communicate()
                server.close()
                await server.wait_closed()


@pytest.mark.parametrize("action", ["evict", "kill_host"])
async def test_worker_stops_blocked_handlers_on_eviction_and_host_death(tmp_path, action):
    project = tmp_path / "project"
    project.mkdir()
    (project / "eviction_actors.py").write_text("""import os
from threading import Event
from durable_actors import Actor, interleave, persisted
class Counter(Actor):
    count: int = persisted(0)
    @interleave
    def hold(self) -> None:
        self.broadcast(os.getpid())
        Event().wait()
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
    build_actor(project, "eviction_actors.py", tmp_path / "build")
    connected = asyncio.get_running_loop().create_future()

    async def accepted(reader, writer):
        connected.set_result((reader, writer))

    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        server = await asyncio.start_unix_server(accepted, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            env={
                **os.environ,
                "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        child_pid = None
        try:
            async with asyncio.timeout(5):
                reader, writer = await connected

                async def receive():
                    while True:
                        message = json.loads(await reader.readline())
                        if message["type"] != "commit_sqlite":
                            return message
                        send(
                            {
                                "type": "sqlite_committed",
                                "message_id": message["message_id"],
                                "txid": commit(invocation["sqlite"]),
                            }
                        )

                def send(message):
                    writer.write((json.dumps(message) + "\n").encode())

                assert (await receive())["type"] == "attach"
                send({"type": "attached", "protocol": 24})
                actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                invocation = {
                    "type": "invoke",
                    "request_id": "r1",
                    "actor": actor,
                    "sqlite": seed(),
                    "method": "hold",
                    "args": [],
                }
                send(
                    {
                        "type": "command",
                        "message_id": 0,
                        "command": {**invocation, "method": "increment"},
                    }
                )
                initial = await receive()
                assert initial["reply"]["sequence"] == 1
                send({"type": "command", "message_id": 1, "command": invocation})
                assert (await receive())["type"] == "ready_for_invocation"
                effect = await receive()
                assert effect["type"] == "socket_effects"
                child_pid = json.loads(effect["effects"][0]["message"]["data"])
                if action == "kill_host":
                    process.kill()
                    await process.wait()
                    for _ in range(100):
                        try:
                            os.kill(child_pid, 0)
                        except ProcessLookupError:
                            break
                        await asyncio.sleep(0.01)
                    else:
                        pytest.fail("actor worker survived its supervisor")
                    return
                send(
                    {
                        "type": "command",
                        "message_id": 2,
                        "command": {"type": "evict", "actor": actor},
                    }
                )
                send({"type": "socket_effects_published", "message_id": 1})
                replies = [await receive(), await receive()]
                outcomes = {reply["message_id"]: reply["reply"] for reply in replies}
                assert outcomes[1]["code"] == "actor_evicted"
                assert outcomes[2] == {"type": "evicted"}
                invocation["sqlite"] = seed({"count": 10})
                send(
                    {
                        "type": "command",
                        "message_id": 3,
                        "command": {
                            **invocation,
                            "method": "increment",
                            "sqlite": invocation["sqlite"],
                        },
                    }
                )
                restored = (await receive())["reply"]
                assert restored["result"] == 11
                assert restored["sequence"] == 2
        finally:
            if child_pid is not None:
                try:
                    os.kill(child_pid, 9)
                except ProcessLookupError:
                    pass
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 2)
            server.close()
            if connected.done():
                _, writer = connected.result()
                writer.transport.abort()
                await asyncio.wait_for(writer.wait_closed(), 1)
            await server.wait_closed()
