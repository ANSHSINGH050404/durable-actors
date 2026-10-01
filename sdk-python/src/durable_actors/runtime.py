from __future__ import annotations

import asyncio
import inspect
import json
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from .actor import Actor
from .contract import Document, Method, decode, describe_actor, encode
from .socket import Effects, SocketScope, scope_context
from .sqlite import SqliteCaptureError, SqliteStorage, Storage


class ActorRuntime:
    def __init__(
        self,
        actor: type[Actor[Any, Any, Any, Any]],
        effects: Effects,
        database: Storage | None = None,
    ) -> None:
        self.effects = effects
        self.database = database if database is not None else SqliteStorage()
        self.completion = asyncio.Lock()
        self.fatal: Document | None = None
        self.definition = describe_actor(actor)
        self.instance: Actor[Any, Any, Any, Any] | None = None
        self.identity: Document | None = None
        self.lifecycle = asyncio.Lock()
        self.pending: set[asyncio.Task[Document]] = set()
        self.serial = asyncio.Lock()
        self.sequence = 0
        self.last_state: Document = {}

    async def handle(self, command: Document) -> Document:
        async with self.lifecycle:
            if self.identity is not None and command["actor"] != self.identity:
                return failed("actor_identity_mismatch", "executor is assigned to another actor")
            if command["actor"]["actor_name"] != self.definition.actor.__name__:
                return failed("actor_name_not_found", "actor is not loaded")
            self.identity = command["actor"]
            if command["type"] == "evict":
                return await self.evict()
            task = asyncio.create_task(self.run(command))
            self.pending.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                raise
            return failed("actor_evicted", "actor was evicted by the Rust host")
        finally:
            self.pending.discard(task)

    async def evict(self) -> Document:
        tasks = tuple(self.pending)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.instance = None
        self.fatal = None
        self.database.close()
        return {"type": "evicted"}

    async def run(self, command: Document) -> Document:
        await self.serial.acquire()
        name = command.get("method") or "on_" + command.get("event", {}).get("type", "")
        released = False

        def admit() -> None:
            nonlocal released
            if name in self.definition.reentrant_methods:
                self.effects.admit()
                self.serial.release()
                released = True

        try:
            if self.fatal is not None:
                return self.fatal
            return await self.execute(command, admit)
        except Exception as error:
            self.instance = None
            failure = failed("invalid_actor_state", str(error))
            if self.definition.reentrant_methods:
                self.fatal = failure
            return failure
        finally:
            if not released:
                self.serial.release()

    async def execute(self, command: Document, admit: Any) -> Document:
        if self.instance is None:
            if command.get("resident_only"):
                return {"type": "state_required"}
            try:
                self.database.restore(command.get("sqlite"))
                self.restore(self.database.fields())
            except Exception as error:
                self.database.close()
                return failed("invalid_actor_state", str(error))
        if command["type"] == "hydrate":
            return {"type": "hydrated"}
        return await self.invoke(command, admit)

    async def invoke(self, command: Document, admit: Any) -> Document:
        instance = self.instance
        assert instance is not None
        socket_event = command["type"] == "websocket_event"
        name = "on_" + command["event"]["type"] if socket_event else command["method"]
        if not socket_event and name not in self.definition.methods:
            return failed("method_not_found", name)
        before = self.snapshot()
        connecting = socket_event and command["event"]["type"] == "connect"
        scope = SocketScope(
            instance,
            command["actor"]["actor_id"],
            self.definition.socket_types,
            self.effects,
            command.get("connections"),
            not connecting,
        )
        token = scope_context.set(scope)
        try:
            if socket_event:
                args = await socket_arguments(command["event"], scope)
                admit()
                if hasattr(instance, name):
                    await invoke_handler(scope, getattr(instance, name), *args)
                result = None
            else:
                method = self.definition.methods[name]
                arguments = bind_arguments(method, command["args"])
                admit()
                value = await invoke_handler(
                    scope, getattr(instance, name), *arguments.args, **arguments.kwargs
                )
                result = encode(method.result, value)
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise asyncio.CancelledError
            effects = await scope.finish()
            async with self.completion:
                if self.fatal is not None:
                    return self.fatal
                state = self.snapshot()
                previous = self.last_state if self.definition.reentrant_methods else before
                effects.extend(self.state_updates(previous, state, command))
                self.database.persist_fields(state)
                try:
                    sqlite = await self.database.snapshot()
                except SqliteCaptureError as error:
                    self.fatal = failed("actor_database_failed", str(error))
                    return self.fatal
                self.last_state = state
                reply: Document = {
                    "type": "websocket_handled" if socket_event else "invoked",
                    "sqlite": sqlite,
                }
                if not socket_event:
                    reply["result"] = result
                if socket_event or effects:
                    reply["effects"] = effects
                if self.definition.reentrant_methods:
                    self.sequence += 1
                    reply["sequence"] = self.sequence
                return reply
        except Exception as error:
            if not self.definition.reentrant_methods:
                self.database.rollback()
                self.restore(before)
            return failed(
                "actor_socket_failed" if socket_event else "actor_method_failed", str(error)
            )
        finally:
            scope.active = False
            try:
                output = scope.output
                if output is not None:
                    task = asyncio.current_task()
                    if task is not None and task.cancelling():
                        output.cancel()
                    await asyncio.gather(output, return_exceptions=True)
            finally:
                scope_context.reset(token)

    def state_updates(self, before: Document, after: Document, command: Document) -> list[Document]:
        fields = self.definition.fields
        changed = {
            name: value
            for name, value in after.items()
            if fields[name].emittable and before.get(name) != value
        }
        effects: list[Document] = []
        event = command.get("event", {})
        connecting = event.get("type") == "connect"
        if changed:
            effect: Document = {"type": "state_update", "changes": changed, "removed": []}
            if connecting:
                effect["except_connection_ids"] = [event["connection"]["id"]]
            effects.append(effect)
        if connecting:
            scope = scope_context.get()
            socket = scope.socket(event["connection"], "connecting")
            if socket.state != "closed" and any(field.emittable for field in fields.values()):
                effects.append(
                    {
                        "type": "state_snapshot",
                        "connection_id": socket.id,
                        "state": {
                            name: value for name, value in after.items() if fields[name].emittable
                        },
                    }
                )
        return effects

    def restore(self, state: Any) -> None:
        instance = self.definition.actor()
        if state is not None:
            if not isinstance(state, dict):
                raise ValueError("persisted state must be an object")
            for name, field in self.definition.fields.items():
                if field.persisted and name in state:
                    setattr(instance, name, decode(field.adapter, state[name]))
        self.instance = instance
        self.last_state = self.snapshot()

    def snapshot(self) -> Document:
        assert self.instance is not None
        unknown = set(vars(self.instance)) - self.definition.fields.keys()
        if unknown:
            raise ValueError(f"undeclared actor fields: {sorted(unknown)}")
        return {
            name: encode(field.adapter, getattr(self.instance, name))
            for name, field in self.definition.fields.items()
            if field.persisted
        }


async def invoke_handler(
    scope: SocketScope, handler: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    worker = asyncio.create_task(asyncio.to_thread(handler, *args, **kwargs))
    cancelled = False
    # Threads cannot be stopped: drain the handler before completing cancellation.
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancelled = True
            scope.cancel()
        except Exception:
            break
    if cancelled:
        if not worker.cancelled():
            worker.exception()
        raise asyncio.CancelledError
    return worker.result()


def bind_arguments(method: Method, values: list[Any]) -> inspect.BoundArguments:

    remaining = list(values)
    arguments: dict[str, Any] = {}
    for name, parameter in method.signature.parameters.items():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            arguments[name] = tuple(decode(method.parameters[name], remaining))
            remaining = []
        elif remaining:
            arguments[name] = decode(method.parameters[name], remaining.pop(0))
        elif parameter.default is inspect.Parameter.empty:
            raise ValueError(f"missing required argument: {name}")
    if remaining:
        raise ValueError("too many RPC arguments")
    bound = inspect.BoundArguments(method.signature, OrderedDict(arguments))
    bound.apply_defaults()
    return bound


def failed(code: str, message: str) -> Document:
    return {"type": "failed", "code": code, "message": message}


async def socket_arguments(event: Document, scope: SocketScope) -> list[Any]:
    kind = event["type"]
    if kind == "connect":
        return [scope.socket(event["connection"], "connecting")]
    if kind == "disconnect":
        return [
            scope.socket(event["connection"], "closed"),
            event["code"],
            event["reason"],
            event["was_clean"],
        ]
    socket = next(
        (item for item in await scope.get_connections() if item.id == event["connection_id"]), None
    )
    if socket is None or event["message"]["type"] != "text":
        raise ValueError("socket messages require an active connection and JSON text")
    return [socket, decode(scope.incoming, json.loads(event["message"]["data"]))]
