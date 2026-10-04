import asyncio
from threading import Event

import pytest
from fixtures.effects import Effects
from fixtures.sqlite import ActorRuntime, seed
from pydantic import BaseModel

from durable_actors import Actor, ephemeral, interleave, persisted


class Value(BaseModel):
    count: int


class Counter(Actor):
    value: Value = persisted(Value(count=0))
    calls: int = ephemeral(0)

    def increment(self, amount: int = 1) -> Value:
        self.value.count += amount
        self.calls += 1
        return self.value

    def fail(self) -> None:
        self.value.count = 99
        raise ValueError("failed")


ACTOR = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}


async def test_socket_handlers_load_connections_only_on_demand():
    from durable_actors import ActorSocket

    class LookupEffects(Effects):
        lookups = 0

        async def get_connections(self, tag=None, count_only=False):
            self.lookups += 1
            return await super().get_connections(tag, count_only)

    class Room(Actor[None, bool, int]):
        def on_message(self, socket: ActorSocket[None, int], enumerate: bool) -> None:
            socket.send(len(self.get_connections()) if enumerate else 0)

    effects = LookupEffects()
    effects.connections = [
        {"id": name, "metadata": None, "tags": []} for name in ["sender", "other"]
    ]
    runtime = ActorRuntime(Room, effects)
    for enumerate in [False, True]:
        reply = await runtime.handle(
            {
                "type": "websocket_event",
                "request_id": "lookup",
                "actor": {**ACTOR, "actor_name": "Room"},
                "sqlite": seed(),
                "connections": effects.connections[:1],
                "event": {
                    "type": "message",
                    "connection_id": "sender",
                    "message": {"type": "text", "data": "true" if enumerate else "false"},
                },
            }
        )
        assert reply["type"] == "websocket_handled"
        assert effects.published[-1]["message"]["data"] == ("2" if enumerate else "0")
        assert effects.lookups == int(enumerate)


def command(method="increment", args=None, **extra):
    return {
        "type": "invoke",
        "request_id": "r1",
        "actor": ACTOR,
        "method": method,
        "args": args or [],
        "sqlite": seed(),
        **extra,
    }


async def test_hydrates_validates_and_snapshots_typed_state():
    runtime = ActorRuntime(Counter, Effects())
    reply = await runtime.handle(command(args=[2], sqlite=seed({"value": {"count": 3}})))
    assert reply["result"] == {"count": 5}
    assert runtime.database.fields() == {"value": {"count": 5}}
    assert (await runtime.handle(command()))["result"] == {"count": 6}


async def test_failure_rolls_back_persisted_state():
    runtime = ActorRuntime(Counter, Effects())
    await runtime.handle(command())
    failure = await runtime.handle(command("fail"))
    assert failure["type"] == "failed"
    assert (await runtime.handle(command()))["result"] == {"count": 2}


async def test_residency_and_eviction_require_explicit_hydration():
    runtime = ActorRuntime(Counter, Effects())
    assert (await runtime.handle(command(resident_only=True)))["type"] == "state_required"
    await runtime.handle(command())
    assert (await runtime.handle({"type": "evict", "actor": ACTOR}))["type"] == "evicted"
    assert (await runtime.handle(command(resident_only=True)))["type"] == "state_required"


async def test_argument_validation_precedes_actor_execution():
    runtime = ActorRuntime(Counter, Effects())
    assert (await runtime.handle(command(args=["2"])))["type"] == "failed"
    assert (await runtime.handle(command()))["result"] == {"count": 1}


async def test_reentrant_failure_does_not_erase_overlapping_success():
    entered = asyncio.Event()
    resume = Event()
    loop = asyncio.get_running_loop()

    class Shared(Actor):
        count: int = persisted(0)

        @interleave
        def wait_and_fail(self) -> None:
            loop.call_soon_threadsafe(entered.set)
            if not resume.wait(3):
                raise TimeoutError("test did not release the handler")
            raise ValueError("failed")

        def increment(self) -> int:
            self.count += 1
            return self.count

    runtime = ActorRuntime(Shared, Effects())
    actor = {**ACTOR, "actor_name": "Shared"}
    pending = asyncio.create_task(runtime.handle(command("wait_and_fail", actor=actor)))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        reply = await asyncio.wait_for(runtime.handle(command(actor=actor)), 1)
        resume.set()
        assert (await pending)["type"] == "failed"
        assert reply["result"] == 1
        assert reply["sequence"] == 1
        assert (await runtime.handle(command(actor=actor)))["result"] == 2
    finally:
        resume.set()
        await asyncio.gather(pending, return_exceptions=True)


async def test_socket_messages_are_typed_and_emit_persisted_changes():
    from fixtures.effects import Effects

    from durable_actors import ActorSocket, emitted

    class Room(Actor[Value, Value, Value]):
        value: Value = emitted(persisted(Value(count=0)))

        def on_message(self, socket: ActorSocket[Value, Value], message: Value) -> None:
            self.value = message
            socket.send(Value(count=message.count + socket.metadata.count))

    effects = Effects()
    runtime = ActorRuntime(Room, effects)
    reply = await runtime.handle(
        {
            "type": "websocket_event",
            "request_id": "s1",
            "actor": {**ACTOR, "actor_name": "Room"},
            "sqlite": seed(),
            "connections": [{"id": "s1", "metadata": {"count": 2}, "tags": []}],
            "event": {
                "type": "message",
                "connection_id": "s1",
                "message": {"type": "text", "data": '{"count":3}'},
            },
        }
    )
    assert reply["type"] == "websocket_handled"
    assert effects.published[0]["message"]["data"] == '{"count":5}'
    assert reply["effects"][0]["changes"] == {"value": {"count": 3}}


@pytest.mark.parametrize("evict", [False, True])
async def test_sync_handlers_keep_order_and_drain_before_eviction(evict):
    entered = asyncio.Event()
    release = Event()
    loop = asyncio.get_running_loop()

    class Blocking(Actor):
        count: int = persisted(0)

        def hold(self) -> int:
            assert self.id == "one"
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("test did not release the handler")
            self.count += 1
            return self.count

        def increment(self) -> int:
            self.count += 1
            return self.count

    runtime = ActorRuntime(Blocking, Effects())
    actor = {**ACTOR, "actor_name": "Blocking"}
    active = asyncio.create_task(runtime.handle(command("hold", actor=actor)))
    tasks = [active]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert not active.done()
        running = next(iter(runtime.pending))
        queued = asyncio.create_task(runtime.handle(command(actor=actor)))
        tasks.append(queued)
        await asyncio.sleep(0)
        assert not queued.done()
        if evict:
            eviction = asyncio.create_task(runtime.handle({"type": "evict", "actor": actor}))
            tasks.append(eviction)
            await asyncio.sleep(0)
            running.cancel()
            await asyncio.sleep(0)
            assert not eviction.done()
        release.set()
        results = await asyncio.wait_for(asyncio.gather(*tasks), 1)
        if evict:
            assert [reply["code"] for reply in results[:2]] == ["actor_evicted"] * 2
            assert results[2] == {"type": "evicted"}
            assert (await runtime.handle(command(actor=actor, sqlite=seed({"count": 10}))))[
                "result"
            ] == 11
        else:
            assert [reply["result"] for reply in results] == [1, 2]
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_eviction_unblocks_sync_socket_queries():
    entered = asyncio.Event()

    class WaitingEffects(Effects):
        async def get_connections(self, tag=None, count_only=False):
            entered.set()
            await asyncio.Event().wait()

    class Room(Actor):
        def count_connections(self) -> int:
            return len(self.get_connections())

    runtime = ActorRuntime(Room, WaitingEffects())
    actor = {**ACTOR, "actor_name": "Room"}
    active = asyncio.create_task(runtime.handle(command("count_connections", actor=actor)))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert await asyncio.wait_for(runtime.handle({"type": "evict", "actor": actor}), 1) == {
            "type": "evicted"
        }
        assert (await active)["code"] == "actor_evicted"
    finally:
        active.cancel()
        await asyncio.gather(active, return_exceptions=True)


async def test_nested_sync_reentrant_calls_keep_the_ordinary_call_exclusive():
    entered = asyncio.Event()
    release = Event()
    loop = asyncio.get_running_loop()
    calls = []

    class Nested(Actor):
        def ordinary(self) -> None:
            calls.append("ordinary:start")
            self.hold()
            calls.append("ordinary:end")

        @interleave
        def hold(self) -> None:
            calls.append("hold")
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("test did not release the handler")

    effects = Effects()
    runtime = ActorRuntime(Nested, effects)
    actor = {**ACTOR, "actor_name": "Nested"}
    ordinary = asyncio.create_task(runtime.handle(command("ordinary", actor=actor)))
    tasks = [ordinary]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        direct = asyncio.create_task(runtime.handle(command("hold", actor=actor)))
        tasks.append(direct)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert effects.admissions == 0
        assert calls == ["ordinary:start", "hold"]
        release.set()
        replies = await asyncio.wait_for(asyncio.gather(*tasks), 1)
        assert all(reply["type"] == "invoked" for reply in replies)
        assert effects.admissions == 1
        assert calls == ["ordinary:start", "hold", "ordinary:end", "hold"]
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_eviction_drains_all_sync_reentrant_handlers_before_rehydrating():
    entered, finished = asyncio.Queue(), asyncio.Queue()
    releases = [Event(), Event()]
    loop = asyncio.get_running_loop()

    class Concurrent(Actor):
        count: int = persisted(0)

        @interleave
        def hold(self, index: int) -> None:
            loop.call_soon_threadsafe(entered.put_nowait, index)
            try:
                if not releases[index].wait(3):
                    raise TimeoutError("test did not release the handler")
                self.count = 99
                self.broadcast("too late")
            finally:
                loop.call_soon_threadsafe(finished.put_nowait, index)

        def increment(self) -> int:
            self.count += 1
            return self.count

    effects = Effects()
    runtime = ActorRuntime(Concurrent, effects)
    actor = {**ACTOR, "actor_name": "Concurrent"}
    tasks = [
        asyncio.create_task(runtime.handle(command("hold", args=[index], actor=actor)))
        for index in range(2)
    ]
    try:
        assert {await asyncio.wait_for(entered.get(), 1) for _ in range(2)} == {0, 1}
        eviction = asyncio.create_task(runtime.handle({"type": "evict", "actor": actor}))
        tasks.append(eviction)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        releases[0].set()
        assert await asyncio.wait_for(finished.get(), 1) == 0
        assert not eviction.done()
        releases[1].set()
        replies = await asyncio.wait_for(asyncio.gather(*tasks), 1)
        assert [reply["code"] for reply in replies[:2]] == ["actor_evicted"] * 2
        assert replies[2] == {"type": "evicted"}
        assert effects.published == []
        reply = await runtime.handle(command(actor=actor, sqlite=seed({"count": 10})))
        assert reply["result"] == 11
        assert reply["sequence"] == 1
    finally:
        for release in releases:
            release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
