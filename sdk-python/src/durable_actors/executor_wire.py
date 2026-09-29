"""Internal executor messages shared by the host and its worker."""

from __future__ import annotations

import asyncio
import json

from .contract import Document
from .guards import is_document

MAX_BYTES = 32 * 1024 * 1024


class Channel:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.write_lock = asyncio.Lock()

    async def send(self, message: Document) -> None:
        data = serialize(message)
        if len(data) > MAX_BYTES:
            raise ValueError("executor message exceeds 32 MiB")
        async with self.write_lock:
            self.writer.write(data)
            await self.writer.drain()

    async def read(self) -> Document:
        line = await self.reader.readline()
        if not line:
            raise EOFError("executor disconnected")
        if len(line) > MAX_BYTES:
            raise ValueError("executor message exceeds 32 MiB")
        value = json.loads(line)
        if not is_document(value):
            raise ValueError("executor messages must be objects")
        return value


def serialize(value: Document) -> bytes:
    return (json.dumps(value, separators=(",", ":"), allow_nan=False) + "\n").encode()
