import sqlite3

import pytest

from durable_actors.sqlite import SqliteCaptureError, SqliteStorage


@pytest.mark.asyncio
async def test_fields_commit_before_replication_and_rollback_together(tmp_path):
    path = tmp_path / "actor.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        "PRAGMA journal_mode=WAL; CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
    )
    calls = []

    async def sync(state):
        calls.append(state)
        assert connection.execute(
            "SELECT value FROM __terse_fields WHERE name='count'"
        ).fetchone() == ("7",)
        return 2

    storage = SqliteStorage(sync)
    try:
        storage.restore({"path": str(path), "socket": "/tmp/test.sock", "txid": 1})
        fields = {"count": 7, "nested": {"items": [None, {"done": True}]}}
        storage.persist_fields(fields)
        assert await storage.snapshot() == {"txid": 2}
        storage.persist_fields(fields)
        assert await storage.snapshot() == {"txid": 2}
        assert len(calls) == 1
        storage.persist_fields({"count": 99})
        storage.rollback()
        assert storage.fields() == fields
        assert await storage.snapshot() == {"txid": 2}
    finally:
        storage.close()
        connection.close()


@pytest.mark.asyncio
async def test_replication_failure_fences_later_writes(tmp_path):
    path = tmp_path / "actor.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT)")

    async def sync(state):
        raise OSError("daemon exited")

    storage = SqliteStorage(sync)
    try:
        storage.restore({"path": str(path), "socket": "/tmp/test.sock", "txid": 1})
        storage.persist_fields({"count": 1})
        with pytest.raises(SqliteCaptureError):
            await storage.snapshot()
        with pytest.raises(SqliteCaptureError):
            storage.persist_fields({"count": 2})
    finally:
        storage.close()
