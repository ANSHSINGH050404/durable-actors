import json
import sqlite3
import subprocess
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

import httpx

from durable_actors.runtime import ActorRuntime as Runtime
from durable_actors.sqlite import SqliteStorage


class SqliteFixture:
    def __init__(self):
        self.resources = ExitStack()
        self.directory = Path(self.resources.enter_context(tempfile.TemporaryDirectory(dir="/tmp")))
        self.socket = self.directory / "control.sock"
        config = self.directory / "config.json"
        config.write_text(
            json.dumps({"socket": {"enabled": True, "path": str(self.socket)}, "levels": []})
        )
        self.process = subprocess.Popen(
            ["litestream", "replicate", "-config", str(config)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.resources.callback(self.process.wait)
        self.resources.callback(self.process.terminate)
        self.client = self.resources.enter_context(
            httpx.Client(
                transport=httpx.HTTPTransport(uds=str(self.socket)),
                base_url="http://localhost",
                timeout=10,
            )
        )
        for _ in range(200):
            try:
                self.client.get("/info").raise_for_status()
                break
            except httpx.TransportError:
                time.sleep(0.02)
        else:
            self.close()
            raise RuntimeError("test Litestream did not start")
        self.count = 0

    def seed(self, fields=None):
        self.count += 1
        path = self.directory / f"{self.count}.sqlite"
        with sqlite3.connect(path) as database:
            database.execute("PRAGMA journal_mode=WAL")
            database.execute(
                "CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
            )
            database.executemany(
                "INSERT INTO __terse_fields VALUES (?, ?)",
                [(name, json.dumps(value)) for name, value in (fields or {}).items()],
            )
        self.client.post(
            "/register",
            json={
                "path": str(path),
                "replica_url": (self.directory / f"replica-{self.count}").as_uri(),
            },
        ).raise_for_status()
        reply = self.client.post("/sync", json={"path": str(path), "wait": True, "timeout": 10})
        reply.raise_for_status()
        return {"path": str(path), "txid": reply.json()["txid"]}

    def close(self):
        self.resources.close()


fixture = None


def seed(fields=None):
    global fixture
    if fixture is None:
        fixture = SqliteFixture()
    return fixture.seed(fields)


def close():
    global fixture
    if fixture is not None:
        fixture.close()
        fixture = None


def fields(state):
    with sqlite3.connect(state["path"]) as database:
        return {
            name: json.loads(value)
            for name, value in database.execute("SELECT name, value FROM __terse_fields")
        }


def commit(state):
    assert fixture is not None
    reply = fixture.client.post("/sync", json={"path": state["path"], "wait": True, "timeout": 10})
    reply.raise_for_status()
    return reply.json()["txid"]


class HostStorage(SqliteStorage):
    def __init__(self):
        super().__init__(self.capture)

    async def capture(self):
        return commit(self.seed)


class ActorRuntime(Runtime):
    def __init__(self, actor, effects):
        super().__init__(actor, effects, HostStorage())
