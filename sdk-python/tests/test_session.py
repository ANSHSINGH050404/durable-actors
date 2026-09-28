from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from little_actors.client import SocketGrant


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.scheduled = []

    def schedule(self, callback, delay):
        event = {"callback": callback, "delay": delay, "cancelled": False}
        self.scheduled.append(event)
        return lambda: event.update(cancelled=True)


class Transport:
    def __init__(self, session):
        self.token = session.token
        self.closed = False
        self.entered = Event()
        self.release = Event()

    def invoke(self, actor_name, actor_id, method, args):
        assert not self.closed
        if method == "hold":
            self.entered.set()
            assert self.release.wait(3)
            assert not self.closed
        return self.token

    def prepare_websocket(
        self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000, home_region=None
    ):
        return SocketGrant(
            websocket_url="wss://example.test/socket",
            home_region="west",
            connect_by_ms=1000,
            authorized_until_ms=2000,
        )

    def broadcast(self, actor_name, actor_id, message):
        assert not self.closed

    def close(self):
        self.closed = True


def make_session(clock, callback=None):
    from little_actors import ActorSession, ActorSessionTransport

    transports = []

    def get_session():
        if callback:
            callback()
        return ActorSession(
            project_id="project",
            control_plane_url="https://control.test",
            token=str(len(transports)),
            expires_at_ms=int(clock.now * 1000 + 60000),
        )

    def create(session):
        transport = Transport(session)
        transports.append(transport)
        return transport

    session = ActorSessionTransport(
        project_id="project",
        get_session=get_session,
        create_transport=create,
        now=lambda: clock.now,
        schedule=clock.schedule,
    )
    return session, transports


def test_sessions_share_refresh_renew_and_close_without_interrupting_active_calls():
    clock = Clock()
    session, transports = make_session(clock)
    with session, ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: session.invoke("Chat", "one", "read", []), range(20)))
        assert results == ["0"] * 20
        assert len(transports) == 1
        held = pool.submit(session.invoke, "Chat", "one", "hold", [])
        assert transports[0].entered.wait(1)
        clock.now += 46
        clock.scheduled[-1]["callback"]()
        assert len(transports) == 2
        assert session.invoke("Chat", "one", "read", []) == "1"
        assert not transports[0].closed
        transports[0].release.set()
        assert held.result(1) == "0"
        assert transports[0].closed
        assert session.prepare_websocket("Chat", "one", None).home_region == "west"
        session.broadcast("Chat", "one", "hello")
    assert all(transport.closed for transport in transports)
    assert clock.scheduled[-1]["cancelled"]
    with pytest.raises(RuntimeError, match="closed"):
        session.invoke("Chat", "one", "read", [])


def test_session_rejection_invalidates_cached_credentials_and_idle_sessions_stop_renewing():
    from little_actors import ActorSessionRejectedError

    clock = Clock()
    rejected = False

    def authorize():
        if rejected:
            raise ActorSessionRejectedError("access revoked")

    session, transports = make_session(clock, authorize)
    with session:
        session.invoke("Chat", "one", "read", [])
        rejected = True
        clock.now += 46
        clock.scheduled[-1]["callback"]()
        assert transports[0].closed
        with pytest.raises(ActorSessionRejectedError):
            session.invoke("Chat", "one", "read", [])
        rejected = False
        session.invoke("Chat", "one", "read", [])
        count = len(clock.scheduled)
        clock.now += 61
        clock.scheduled[-1]["callback"]()
        assert len(clock.scheduled) == count


@pytest.mark.parametrize(
    "change",
    [
        {"project_id": "wrong"},
        {"expires_at_ms": 1004000},
        {"expires_at_ms": 1200000},
        {"control_plane_url": "http://remote.test"},
        {"token": " "},
    ],
)
def test_invalid_sessions_never_construct_a_transport(change):
    from little_actors import ActorSession, ActorSessionTransport

    data = {
        "project_id": "project",
        "control_plane_url": "https://control.test",
        "token": "secret",
        "expires_at_ms": 1060000,
        **change,
    }
    with ActorSessionTransport(
        project_id="project",
        get_session=lambda: ActorSession(**data),
        now=lambda: 1000,
        create_transport=lambda _: pytest.fail("invalid session reached transport"),
    ) as session:
        with pytest.raises(ValueError):
            session.invoke("Chat", "one", "read", [])
