from typing import Literal

from pydantic import BaseModel

from durable_actors import Actor, ActorSocket, compute, emitted, ephemeral, interleave, persisted


class Metadata(BaseModel):
    user_id: str


class Incoming(BaseModel):
    type: Literal["ping"]


class Outgoing(BaseModel):
    type: Literal["balance"]
    balance: int


Socket = ActorSocket[Metadata, Outgoing]


@compute(cpu=0.5, memory_mib=256, idle_timeout_ms=60_000)
class BankAccount(Actor[Metadata, Incoming, Outgoing]):
    balance: int = emitted(persisted(0))
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self.balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        self.balance += amount
        self._deposits_since_wake += 1
        self.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        self.broadcast(Outgoing(type="balance", balance=self.balance), tags=("customer",))
        return self.balance

    def on_connect(self, socket: Socket) -> None:
        socket.set_tags("customer")
        self.set_websocket_auto_response("ping", "pong")
        socket.send(Outgoing(type="balance", balance=self.balance))

    def on_message(self, socket: Socket, message: Incoming) -> None:
        socket.send(Outgoing(type="balance", balance=self.balance))

    def on_disconnect(self, socket: Socket, code: int, reason: str, was_clean: bool) -> None:
        self.broadcast(Outgoing(type="balance", balance=self.balance), except_ids=(socket.id,))


class ClearingNetwork:
    def submit(self, reference: str) -> str:
        return f"receipt:{reference}"


clearing_network = ClearingNetwork()


class Wire(Actor):
    status: str = emitted(persisted("requested"))

    def get_status(self) -> str:
        return self.status

    @interleave
    def submit(self, reference: str) -> str:
        receipt = clearing_network.submit(reference)
        self.status = "submitted"
        return receipt
