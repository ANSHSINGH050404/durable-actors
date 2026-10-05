import asyncio
import sqlite3
from threading import Event, Thread
from time import sleep

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

    async def sync():
        calls.append(True)
        assert connection.execute(
            "SELECT value FROM __terse_fields WHERE name='count'"
        ).fetchone() == ("7",)
        return 2

    storage = SqliteStorage(sync)
    try:
        storage.restore({"path": str(path), "txid": 1})
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

    async def sync():
        raise OSError("host disconnected")

    storage = SqliteStorage(sync)
    try:
        storage.restore({"path": str(path), "txid": 1})
        storage.persist_fields({"count": 1})
        with pytest.raises(SqliteCaptureError):
            await storage.snapshot()
        with pytest.raises(SqliteCaptureError):
            storage.persist_fields({"count": 2})
    finally:
        storage.close()


@pytest.mark.asyncio
async def test_fields_wait_for_a_competing_sqlite_writer(tmp_path):
    path = tmp_path / "actor.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            "PRAGMA journal_mode=WAL; CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT)"
        )

    async def sync():
        return 2

    storage = SqliteStorage(sync)
    storage.restore({"path": str(path), "txid": 1})
    locked, release = Event(), Event()

    def hold_write_lock():
        with sqlite3.connect(path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            locked.set()
            release.wait(5)
            sleep(0.1)
            connection.commit()

    writer = Thread(target=hold_write_lock)
    writer.start()
    try:
        assert locked.wait(5)
        release.set()
        storage.persist_fields({"count": 1})
        assert await storage.snapshot() == {"txid": 2}
        assert storage.fields() == {"count": 1}
    finally:
        release.set()
        storage.close()
        writer.join()


@pytest.fixture
def storage(tmp_path):
    path = tmp_path / "actor.sqlite"
    path.touch()

    async def sync() -> int:
        return 2

    storage = SqliteStorage(sync)
    storage.restore({"path": str(path), "txid": 1})
    yield storage
    storage.close()


@pytest.mark.parametrize(
    ("sql", "bindings", "message"),
    [
        ("BEGIN IMMEDIATE", (), "own SQLite transactions"),
        ("SAVEPOINT nested", (), "own SQLite transactions"),
        ("RELEASE nested", (), "own SQLite transactions"),
        ("ROLLBACK", (), "own SQLite transactions"),
        ("ATTACH 'other.sqlite' AS other", (), "own SQLite transactions"),
        ("VACUUM", (), "own SQLite transactions"),
        ("CREATE TABLE __terse_shadow (value TEXT)", (), "names are reserved"),
        ("SELECT * FROM _litestream_seq", (), "names are reserved"),
        ("PRAGMA journal_mode = DELETE", (), "managed by the actor runtime"),
        ("SELECT 1; SELECT 2", (), "one SQL statement"),
        ("SELECT ?; SELECT 1", (1,), "one SQL statement"),
    ],
)
def test_exec_rejects_statements_the_runtime_owns(storage, sql, bindings, message):
    with pytest.raises(ValueError, match=message):
        storage.exec(sql, *bindings)


def test_exec_accepts_one_statement_with_trailing_semicolons_and_trigger_bodies(storage):
    storage.exec("CREATE TABLE events (name TEXT);")
    storage.exec("CREATE TABLE audit (name TEXT); -- created")
    storage.exec(
        "CREATE TRIGGER copy AFTER INSERT ON events BEGIN "
        "INSERT INTO audit VALUES (new.name); INSERT INTO audit VALUES ('after'); END"
    )
    storage.exec("INSERT INTO events VALUES (?)", "created")
    storage.exec("PRAGMA user_version = 3")
    assert storage.exec("SELECT name FROM audit ORDER BY rowid") == [
        {"name": "created"},
        {"name": "after"},
    ]
    assert storage.exec("PRAGMA user_version") == [{"user_version": 3}]


@pytest.mark.asyncio
async def test_snapshot_waits_for_the_database_lock_without_blocking_the_event_loop(storage):
    held, release = Event(), Event()
    timed_out = []

    def hold():
        with storage.lock:
            held.set()
            timed_out.append(not release.wait(3))

    holder = Thread(target=hold)
    holder.start()
    try:
        assert held.wait(5)
        snapshot = asyncio.create_task(storage.snapshot())
        await asyncio.sleep(0)
        release.set()
        assert await snapshot == {"txid": 1}
        assert timed_out == [False]
    finally:
        release.set()
        holder.join()


@pytest.mark.asyncio
async def test_invalid_sql_keeps_earlier_writes_and_releases_the_write_lock(tmp_path, storage):
    storage.exec("CREATE TABLE notes (body TEXT)")
    await storage.snapshot()
    other = sqlite3.connect(tmp_path / "actor.sqlite", timeout=0)
    try:
        with pytest.raises(sqlite3.OperationalError):
            storage.exec("SELECT FROM notes")
        other.execute("BEGIN IMMEDIATE")
        other.rollback()
        storage.exec("INSERT INTO notes VALUES ('kept')")
        with pytest.raises(sqlite3.OperationalError):
            storage.exec("INSERT INTO missing VALUES (1)")
        await storage.snapshot()
        assert other.execute("SELECT body FROM notes").fetchall() == [("kept",)]
    finally:
        other.close()
