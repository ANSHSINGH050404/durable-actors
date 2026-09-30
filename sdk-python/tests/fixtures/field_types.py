from threading import Lock
from typing import assert_type

from durable_actors import Actor, ActorSocket, emitted, ephemeral, persisted, reentrant


class TypedActor(Actor[str, str, str]):
    count: int = persisted(0)
    version: int = emitted(persisted(0))
    messages: list[str] = emitted(persisted(default_factory=list))
    busy: bool = ephemeral(False)
    lock: Lock = ephemeral(default_factory=Lock)

    @reentrant
    def append(self, message: str) -> int:
        with self.lock:
            self.messages.append(message)
            self.count += 1
        assert_type(self.count, int)
        assert_type(self.version, int)
        assert_type(self.messages, list[str])
        assert_type(self.busy, bool)
        assert_type(self.lock, Lock)
        return self.count

    def on_connect(self, socket: ActorSocket[str, str]) -> None:
        assert_type(self.id, str)
        assert_type(self.get_connections(), list[ActorSocket[str, str]])
        socket.send("hello")

    def on_message(self, socket: ActorSocket[str, str], message: str) -> None:
        self.append(message)

    def on_disconnect(
        self, socket: ActorSocket[str, str], code: int, reason: str, was_clean: bool
    ) -> None:
        pass
