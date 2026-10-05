"""Keep executor communication alive while replaceable processes run actor code."""

from __future__ import annotations

import asyncio
import os
import socket
import sys
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field

from pydantic import TypeAdapter

from .contract import Document
from .executor_wire import Channel
from .runtime import failed

Environment = dict[str, str]
environment_adapter: TypeAdapter[Environment] = TypeAdapter(Environment)


@dataclass
class Pending:
    exchanges: set[str] = field(default_factory=set[str])
    reply: Document | None = None


class Supervisor(Channel):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        create_worker: Callable[[str, Environment], Awaitable[Worker]],
    ) -> None:
        super().__init__(reader, writer)
        self.create_worker = create_worker
        self.worker: Worker | None = None
        self.pump: asyncio.Task[None] | None = None
        self.pending: dict[int, Pending] = {}
        self.assigned: Document | None = None
        self.attached: Document = {}
        self.entrypoint = ""
        self.environment: Environment = {}
        self.actor_names: list[str] = []
        self.sequence = 0
        self.offset = 0

    async def run(self, entrypoint: str | None, generic: bool) -> None:
        try:
            if generic:
                await self.send({"type": "warm", "protocol": 24})
                load = await self.read()
                if load.get("type") != "load":
                    raise ValueError("expected actor code assignment")
                entrypoint = load.get("entrypoint")
                self.environment = environment_adapter.validate_python(
                    load.get("environment"), strict=True
                )
            if not entrypoint or not os.path.isabs(entrypoint):
                raise ValueError("an absolute actor entrypoint is required")
            self.entrypoint = entrypoint
            self.worker = await self.create_worker(entrypoint, self.environment)
            self.actor_names = self.worker.actor_names
            await self.send({"type": "attach", "protocol": 24, "actor_names": self.actor_names})
            self.attached = await self.read()
            if self.attached.get("type") != "attached" or self.attached.get("protocol") != 24:
                raise ValueError("unsupported executor protocol")
            await self.start_pump()
            while True:
                await self.accept(await self.read())
        finally:
            await self.stop_worker()
            self.writer.close()
            with suppress(ConnectionError):
                await self.writer.wait_closed()

    async def accept(self, message: Document) -> None:
        if message["type"] != "command":
            await self.acknowledge(message)
            return
        from .host import validate_command

        validate_command(message)
        command, identifier = message["command"], message["message_id"]
        actor = command["actor"]
        if self.assigned is not None and actor != self.assigned:
            await self.reply(
                identifier,
                failed(
                    "actor_identity_mismatch", "sandbox is permanently assigned to another actor"
                ),
            )
        elif actor["actor_name"] not in self.actor_names:
            await self.reply(identifier, failed("actor_name_not_found", "actor is not loaded"))
        elif command["type"] == "evict":
            await self.stop_worker()
            await self.fail_pending("actor_evicted", "actor was evicted by the Rust host")
            await self.reply(identifier, {"type": "evicted"})
        else:
            if len(self.pending) >= 512 or identifier in self.pending:
                raise ValueError("executor command queue is full or message ID is duplicated")
            self.assigned = actor
            if self.worker is None:
                self.worker = await self.create_worker(self.entrypoint, self.environment)
                if self.worker.actor_names != self.actor_names:
                    raise ValueError("worker actor definitions changed")
                self.offset = self.sequence
                await self.start_pump()
            self.pending[identifier] = Pending()
            await self.worker.send(message)

    async def acknowledge(self, message: Document) -> None:
        kind, identifier = message["type"], message["message_id"]
        pending = self.pending.get(identifier)
        if pending is None or kind not in pending.exchanges:
            raise ValueError("unexpected executor acknowledgment")
        pending.exchanges.remove(kind)
        if pending.reply is not None:
            await self.complete(identifier)
        elif self.worker is not None:
            await self.worker.send(message)

    async def start_pump(self) -> None:
        assert self.worker is not None
        await self.worker.send(self.attached)
        self.pump = asyncio.create_task(self.forward(self.worker))

    async def forward(self, worker: Worker) -> None:
        try:
            while True:
                message = await worker.read()
                if message["type"] == "residency":
                    await self.send(message)
                    continue
                identifier = message["message_id"]
                pending = self.pending[identifier]
                kind = message["type"]
                if kind == "reply":
                    reply = message["reply"]
                    if "sequence" in reply:
                        reply["sequence"] += self.offset
                        self.sequence = max(self.sequence, reply["sequence"])
                    pending.reply = reply
                    await self.complete(identifier)
                else:
                    if kind == "socket_effects":
                        pending.exchanges.add("socket_effects_published")
                    elif kind == "commit_sqlite":
                        pending.exchanges.add("sqlite_committed")
                    elif kind == "get_connections":
                        pending.exchanges.add("socket_connections")
                    elif kind != "ready_for_invocation":
                        raise ValueError("unexpected worker message")
                    await self.send(message)
        except (EOFError, ConnectionError, ValueError, KeyError) as error:
            await worker.close()
            if self.worker is worker:
                self.worker = None
                await self.fail_pending("actor_worker_failed", str(error))

    async def fail_pending(self, code: str, message: str) -> None:
        for identifier in list(self.pending):
            self.pending[identifier].reply = failed(code, message)
            await self.complete(identifier)
        if self.attached.get("supports_residency"):
            await self.send({"type": "residency", "actors": []})

    async def complete(self, identifier: int) -> None:
        pending = self.pending[identifier]
        if pending.reply is not None and not pending.exchanges:
            del self.pending[identifier]
            await self.reply(identifier, pending.reply)

    async def reply(self, identifier: int, value: Document) -> None:
        await self.send({"type": "reply", "message_id": identifier, "reply": value})

    async def stop_worker(self) -> None:
        if self.pump is not None:
            self.pump.cancel()
            await asyncio.gather(self.pump, return_exceptions=True)
            self.pump = None
        if self.worker is not None:
            await self.worker.close()
            self.worker = None


class Worker(Channel):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        process: asyncio.subprocess.Process,
    ) -> None:
        super().__init__(reader, writer)
        self.process = process
        self.actor_names: list[str] = []

    @classmethod
    async def start(cls, entrypoint: str, environment: Environment) -> Worker:
        parent, child = socket.socketpair()
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "durable_actors.host",
                "--worker",
                str(child.fileno()),
                env={**os.environ, **environment, "DURABLE_ACTORS_ENTRYPOINT": entrypoint},
                pass_fds=(child.fileno(),),
            )
        except BaseException:
            parent.close()
            raise
        finally:
            child.close()
        worker: Worker | None = None
        try:
            reader, writer = await asyncio.open_connection(sock=parent, limit=sys.maxsize)
            worker = cls(reader, writer, process)
            async with asyncio.timeout(60):
                attach = await worker.read()
            if attach.get("type") != "attach" or attach.get("protocol") != 24:
                raise ValueError("worker did not attach")
            worker.actor_names = attach["actor_names"]
            return worker
        except BaseException:
            if worker is not None:
                await worker.close()
            else:
                parent.close()
                with suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
            raise

    async def close(self) -> None:
        if self.process.returncode is None:
            with suppress(ProcessLookupError):
                self.process.kill()
        await self.process.wait()
        self.writer.close()
        with suppress(ConnectionError):
            await self.writer.wait_closed()
