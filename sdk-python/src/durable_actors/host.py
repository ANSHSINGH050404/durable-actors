from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
import traceback
from contextlib import suppress
from contextvars import ContextVar
from pathlib import Path
from typing import Any, cast

from .build import load_artifact
from .client import component
from .contract import Document
from .executor_wire import Channel, serialize
from .runtime import ActorRuntime, failed
from .sqlite import SqliteStorage, valid_txid

message_id: ContextVar[int] = ContextVar("actor_message_id")


class Session(Channel):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        super().__init__(reader, writer)
        self.runtimes: dict[str, ActorRuntime] = {}
        self.assigned: Document | None = None
        self.pending: dict[tuple[str, int], asyncio.Future[Any]] = {}
        self.tasks: set[asyncio.Task[None]] = set()
        self.residency: asyncio.Task[None] | None = None

    async def run(self, entrypoint: str | None, generic: bool) -> None:
        try:
            if generic:
                await self.send({"type": "warm", "protocol": 24})
                assignment = await self.read()
                if (
                    assignment.get("type") != "load"
                    or not Path(assignment["entrypoint"]).is_absolute()
                ):
                    raise ValueError("invalid code assignment")
                entrypoint = assignment["entrypoint"]
            if not entrypoint:
                raise ValueError("DURABLE_ACTORS_ENTRYPOINT is required")
            path = Path(entrypoint)
            async with asyncio.timeout(60):
                while not path.is_file():
                    await asyncio.sleep(0.01)
            actors = load_artifact(path)
            self.runtimes = {
                actor.__name__: ActorRuntime(actor, self, SqliteStorage(self.commit_sqlite))
                for actor in actors
            }
            await self.send(
                {"type": "attach", "protocol": 24, "actor_names": sorted(self.runtimes)}
            )
            attached = await self.read()
            if attached.get("type") != "attached" or attached.get("protocol") != 24:
                raise ValueError("unsupported executor protocol")
            if attached.get("supports_residency"):
                self.residency = asyncio.create_task(self.report_residency())
            while True:
                message = await self.read()
                if message["type"] == "command":
                    if len(self.tasks) >= 512:
                        raise ValueError("executor command queue is full")
                    validate_command(message)
                    task = asyncio.create_task(self.execute(message))
                    self.tasks.add(task)
                    task.add_done_callback(self.completed)
                elif message["type"] in {
                    "socket_connections",
                    "socket_effects_published",
                    "sqlite_committed",
                }:
                    pending = self.pending.pop((message["type"], message["message_id"]))
                    if pending.cancelled():
                        continue
                    if message.get("error"):
                        pending.set_exception(RuntimeError(message["error"]))
                    else:
                        pending.set_result(
                            message.get("txid")
                            if message["type"] == "sqlite_committed"
                            else message.get("connections")
                        )
                else:
                    raise ValueError("unexpected executor message")
        finally:
            await self.close()

    async def execute(self, message: Document) -> None:
        token = message_id.set(message["message_id"])
        try:
            command = message["command"]
            actor = command["actor"]
            if self.assigned is not None and actor != self.assigned:
                reply = failed(
                    "actor_identity_mismatch", "sandbox is permanently assigned to another actor"
                )
            elif actor["actor_name"] not in self.runtimes:
                reply = failed("actor_name_not_found", "actor is not loaded")
            else:
                if command["type"] != "evict":
                    self.assigned = actor
                reply = await self.runtimes[actor["actor_name"]].handle(command)
            response = {"type": "reply", "message_id": message["message_id"], "reply": reply}
            await self.send(response)
        finally:
            message_id.reset(token)

    def completed(self, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.writer.close()

    async def commit_sqlite(self) -> int:
        txid = await self.exchange("commit_sqlite", "sqlite_committed")
        if not valid_txid(txid):
            raise ValueError("invalid host commit position")
        return int(txid)

    async def publish(self, effects: list[Document]) -> None:
        await self.exchange("socket_effects", "socket_effects_published", effects=effects)

    async def get_connections(
        self, tag: str | None = None, count_only: bool = False
    ) -> list[Document] | int:
        return cast(
            list[Document] | int,
            await self.exchange(
                "get_connections", "socket_connections", query={"tag": tag, "countOnly": count_only}
            ),
        )

    def admit(self) -> None:
        data = serialize({"type": "ready_for_invocation", "message_id": message_id.get()})
        self.writer.write(data)

    async def exchange(self, kind: str, response: str, **fields: Any) -> Any:
        key = response, message_id.get()
        if key in self.pending:
            raise ValueError("duplicate executor request")
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.pending[key] = future
        try:
            await self.send({"type": kind, "message_id": key[1], **fields})
            return await future
        finally:
            # Canceled exchanges still receive acknowledgments from the host.
            if not future.cancelled():
                self.pending.pop(key, None)

    async def report_residency(self) -> None:
        while True:
            actors = [
                runtime.identity
                for runtime in self.runtimes.values()
                if runtime.instance is not None
            ]
            await self.send({"type": "residency", "actors": actors})
            await asyncio.sleep(1)

    async def close(self) -> None:
        if self.residency:
            self.residency.cancel()
        for future in self.pending.values():
            if not future.done():
                future.cancel()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        self.writer.close()
        await self.writer.wait_closed()


def validate_command(message: Document) -> None:
    if type(message.get("message_id")) is not int or message["message_id"] < 0:
        raise ValueError("invalid executor message ID")
    command = message["command"]
    if command["type"] not in {"invoke", "hydrate", "evict", "websocket_event"}:
        raise ValueError("unsupported executor command")
    for field, size in (("project_id", 64), ("actor_name", 255), ("actor_id", 128)):
        component(command["actor"][field], size)
    if command["type"] == "invoke":
        component(command["method"], 255)
        if not isinstance(command["args"], list):
            raise ValueError("RPC arguments must be an array")


async def main() -> None:
    if "--worker" in sys.argv:
        channel = socket.socket(fileno=int(sys.argv[sys.argv.index("--worker") + 1]))
        reader, writer = await asyncio.open_connection(sock=channel, limit=sys.maxsize)
        status = 0
        try:
            await Session(reader, writer).run(os.environ.get("DURABLE_ACTORS_ENTRYPOINT"), False)
        except EOFError:
            pass
        except BaseException:
            traceback.print_exc()
            status = 1
        finally:
            # Only the child enters this branch; its supervisor has disconnected.
            os._exit(status)
    from .supervisor import Supervisor, Worker

    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    reader, writer = await asyncio.open_unix_connection(
        os.environ["DURABLE_ACTORS_EXECUTOR_SOCKET"], limit=sys.maxsize
    )
    with suppress(asyncio.CancelledError, EOFError):
        await Supervisor(reader, writer, Worker.start).run(
            os.environ.get("DURABLE_ACTORS_ENTRYPOINT"), "--generic" in sys.argv
        )


if __name__ == "__main__":
    asyncio.run(main())
