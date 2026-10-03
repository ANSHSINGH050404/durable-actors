from __future__ import annotations

import json
import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import httpx

from .contract import Document


class SqliteCaptureError(RuntimeError):
    pass


class Storage(Protocol):
    def restore(self, state: Document | None) -> None: ...
    def fields(self) -> Document: ...
    def persist_fields(self, fields: Document) -> None: ...
    async def snapshot(self) -> Document: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


async def sync_litestream(state: Document) -> int:
    transport = httpx.AsyncHTTPTransport(uds=state["socket"])
    async with httpx.AsyncClient(transport=transport, timeout=35) as client:
        response = await client.post(
            "http://localhost/sync", json={"path": state["path"], "wait": True, "timeout": 30}
        )
        response.raise_for_status()
        result = response.json()
    txid = result.get("txid")
    if (
        result.get("path") != state["path"]
        or not valid_txid(txid)
        or not valid_txid(result.get("replicated_txid"))
        or result["replicated_txid"] < txid
    ):
        raise ValueError("invalid Litestream replication acknowledgement")
    return int(txid)


class SqliteStorage:
    def __init__(self, sync: Callable[[Document], Awaitable[int]] = sync_litestream) -> None:
        self.sync = sync
        self.connection: sqlite3.Connection | None = None
        self.seed: Document | None = None
        self.txid = 0
        self.version: tuple[int, int, int] = (0, 0, 0)
        self.failure: SqliteCaptureError | None = None

    def restore(self, state: Document | None) -> None:
        self.close()
        if (
            state is None
            or not valid_txid(state.get("txid"))
            or not all(isinstance(state.get(key), str) and state[key] for key in ("path", "socket"))
        ):
            raise ValueError("invalid actor SQLite recovery state")
        self.seed = state
        self.txid = state["txid"]
        self.version = self.change_token()

    def fields(self) -> Document:
        return {
            name: json.loads(value)
            for name, value in self.field_database().execute(
                "SELECT name, value FROM __terse_fields"
            )
        }

    def persist_fields(self, fields: Document) -> None:
        database = self.field_database()
        database.executemany(
            "INSERT INTO __terse_fields(name, value) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value WHERE value<>excluded.value",
            [
                (name, json.dumps(value, allow_nan=False, separators=(",", ":")))
                for name, value in fields.items()
            ],
        )
        database.execute(
            "DELETE FROM __terse_fields WHERE name NOT IN (SELECT value FROM json_each(?))",
            (json.dumps(list(fields)),),
        )

    async def snapshot(self) -> Document:
        try:
            database = self.open()
            version = self.change_token()
            database.commit()
            if version != self.version:
                assert self.seed is not None
                txid = await self.sync(self.seed)
                if not valid_txid(txid) or txid < self.txid:
                    raise ValueError("invalid Litestream transaction")
                self.txid = txid
                self.version = version
            return {"txid": self.txid}
        except Exception as error:
            self.failure = SqliteCaptureError("failed to replicate actor SQLite commit")
            raise self.failure from error

    def rollback(self) -> None:
        if self.connection is not None:
            self.connection.rollback()
            self.version = self.change_token()

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.seed = None
        self.failure = None

    def change_token(self) -> tuple[int, int, int]:
        database = self.open()
        return (
            database.total_changes,
            database.execute("PRAGMA schema_version").fetchone()[0],
            database.execute("PRAGMA user_version").fetchone()[0],
        )

    def field_database(self) -> sqlite3.Connection:
        database = self.open()
        if not database.in_transaction:
            # Acquire the writer lock before reads to avoid a busy read-to-write upgrade.
            database.execute("BEGIN IMMEDIATE")
        database.execute(
            "CREATE TABLE IF NOT EXISTS __terse_fields "
            "(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        return database

    def open(self) -> sqlite3.Connection:
        if self.failure is not None:
            raise self.failure
        if self.connection is None:
            if self.seed is None:
                raise ValueError("actor SQLite database has not been restored")
            self.connection = sqlite3.connect(self.seed["path"], isolation_level=None)
            for pragma in (
                "foreign_keys=ON",
                "journal_mode=WAL",
                "wal_autocheckpoint=0",
                "synchronous=FULL",
            ):
                self.connection.execute("PRAGMA " + pragma)
            if self.connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise ValueError("invalid actor SQLite database")
        return self.connection


def valid_txid(value: Any) -> bool:
    return type(value) is int and 0 < value <= 2**53 - 1
