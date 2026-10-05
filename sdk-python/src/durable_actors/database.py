"""Invocation-scoped access to an actor's SQLite database."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Protocol
from weakref import WeakKeyDictionary, ref

SqliteValue = str | int | float | bytes | None


class SqlDatabase(Protocol):
    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]: ...


class _Admission:
    def __init__(self, actor: object) -> None:
        self.actor = actor
        self.active = True


_admission: ContextVar[_Admission | None] = ContextVar("actor_database_admission", default=None)
_databases: WeakKeyDictionary[object, ActorDatabase] = WeakKeyDictionary()


class ActorDatabase:
    """One actor's SQLite handle. Statements run only during its current invocation."""

    def __init__(self, actor: object, storage: SqlDatabase) -> None:
        # WeakKeyDictionary values must not strongly reference their keys.
        self._actor = ref(actor)
        self._storage = storage

    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        """Execute one statement with positional bindings and return its rows."""
        admission = _admission.get()
        if admission is None or admission.actor is not self._actor() or not admission.active:
            raise RuntimeError("actor database is unavailable outside its invocation")
        return self._storage.exec(sql, *bindings)


def bind_actor_database(actor: object, storage: SqlDatabase) -> None:
    _databases[actor] = ActorDatabase(actor, storage)


def actor_database(actor: object) -> ActorDatabase:
    database = _databases.get(actor)
    if database is None:
        raise RuntimeError("actor database is unavailable during construction")
    return database


@contextmanager
def actor_database_invocation(actor: object | None) -> Generator[None]:
    if actor is None:
        yield
        return
    admission = _Admission(actor)
    token: Token[_Admission | None] = _admission.set(admission)
    try:
        yield
    finally:
        admission.active = False
        _admission.reset(token)
