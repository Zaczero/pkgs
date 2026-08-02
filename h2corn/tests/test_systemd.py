"""`sd_notify` readiness.

Asserted against a real `AF_UNIX` datagram socket: the bytes `systemd` would
receive are the whole contract, and a mocked socket would only restate this
module's own code back at it.
"""

import os
import socket
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from h2corn import Config, _systemd

from tests._support import running_server

# The `sd_notify` protocol is a Linux/systemd contract: Windows has no
# `AF_UNIX` datagram transport for it and macOS has no abstract namespace, so
# these assert a contract that does not exist there.
pytestmark = pytest.mark.skipif(
    sys.platform != 'linux', reason='sd_notify is a Linux/systemd contract'
)


@pytest.fixture
def notify_socket(
    unix_socket_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[socket.socket]:
    # `unix_socket_dir`, not `tmp_path`: `sun_path` is capped near 104 bytes
    # and pytest's temp roots overflow it.
    path = unix_socket_dir / 'notify.sock'
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(str(path))
    sock.settimeout(2)
    monkeypatch.setenv('NOTIFY_SOCKET', str(path))
    try:
        yield sock
    finally:
        sock.close()


def test_ready_is_received_verbatim(notify_socket: socket.socket) -> None:
    assert _systemd.notify_ready() is True
    assert notify_socket.recv(64) == b'READY=1'


def test_stopping_is_received_verbatim(notify_socket: socket.socket) -> None:
    assert _systemd.notify_stopping() is True
    assert notify_socket.recv(64) == b'STOPPING=1'


def test_without_the_variable_nothing_is_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('NOTIFY_SOCKET', raising=False)
    # The overwhelmingly common case: not running under systemd at all.
    assert _systemd.notify_ready() is False
    assert _systemd.notify_stopping() is False


def test_an_abstract_namespace_address_is_translated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = f'\0h2corn-test-{os.getpid()}'
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(name)
    sock.settimeout(2)
    try:
        # systemd spells the abstract namespace with a leading '@'; Python
        # spells the same address with a leading NUL.
        monkeypatch.setenv('NOTIFY_SOCKET', '@' + name[1:])
        assert _systemd.notify_ready() is True
        assert sock.recv(64) == b'READY=1'
    finally:
        sock.close()


def test_a_relative_address_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    # systemd documents only an absolute path or the abstract namespace. A
    # relative one would resolve against a working directory the server may
    # have changed, so it names nothing reliable.
    monkeypatch.setenv('NOTIFY_SOCKET', 'notify.sock')
    assert _systemd.notify_ready() is False


@pytest.mark.asyncio
async def test_a_serving_server_announces_itself(notify_socket: socket.socket) -> None:
    """Readiness is reported when the server is actually accepting, not at exec.

    This is the whole point of `Type=notify`: without it `systemd` starts
    dependent units as soon as the process exists, against a socket nothing is
    listening on yet.
    """

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    async with running_server(app, Config(port=0, access_log=False, lifespan='off')):
        # Already sent by the time the server is serving, so this never blocks
        # on a notification that is still coming.
        assert notify_socket.recv(64) == b'READY=1'


def test_a_vanished_socket_does_not_raise(
    unix_socket_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Shutdown races here: systemd may already have torn its end down. A
    # status datagram that cannot be delivered must never take the server with
    # it.
    monkeypatch.setenv('NOTIFY_SOCKET', str(unix_socket_dir / 'absent.sock'))
    assert _systemd.notify_stopping() is False


def _fleet(config, ready_flags):
    """A supervisor whose worker readiness the test sets by hand."""
    from h2corn import _server, _supervisor
    from h2corn._cli import ImportSettings
    from h2corn._lib import prepare_tls

    supervisor = _supervisor._Supervisor(
        app=ImportSettings(target='example:app'),
        config=config,
        fds=(),
        identity=_server.ProcessIdentity(),
        prepared_tls=prepare_tls(config),
    )
    for index, ready in enumerate(ready_flags):
        read_fd, write_fd = os.pipe()
        worker = _supervisor._Worker(_StubProcess(index), read_fd, None)
        worker.ready = ready
        supervisor.workers[index] = worker
        os.close(write_fd)
    return supervisor


class _StubProcess:
    def __init__(self, sentinel: int) -> None:
        self._sentinel = sentinel

    @property
    def pid(self) -> int | None:
        return 1000 + self._sentinel

    @property
    def exitcode(self) -> int | None:
        return None

    @property
    def sentinel(self) -> int:
        return self._sentinel

    def start(self) -> None: ...
    def is_alive(self) -> bool:
        return True

    def join(self, timeout: float | None = None) -> None: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...
    def close(self) -> None: ...


def test_readiness_waits_for_every_worker(notify_socket: socket.socket) -> None:
    supervisor = _fleet(Config(workers=2), [True, False])
    supervisor.note_fleet_ready()
    assert not supervisor.notified_ready
    notify_socket.settimeout(0.2)
    with pytest.raises(TimeoutError):
        _ = notify_socket.recv(64)

    supervisor.workers[1].ready = True
    supervisor.note_fleet_ready()
    assert supervisor.notified_ready
    notify_socket.settimeout(2)
    assert notify_socket.recv(64) == b'READY=1'


def test_readiness_is_sent_once(notify_socket: socket.socket) -> None:
    supervisor = _fleet(Config(workers=1), [True])
    supervisor.note_fleet_ready()
    supervisor.note_fleet_ready()
    assert notify_socket.recv(64) == b'READY=1'
    notify_socket.settimeout(0.2)
    with pytest.raises(TimeoutError):
        _ = notify_socket.recv(64)


def test_a_retiring_worker_does_not_hold_readiness_back(
    notify_socket: socket.socket,
) -> None:
    """A worker on its way out is not serving capacity.

    Counting it would leave an already-serving fleet stuck in `activating`
    forever: it never reports ready, and its removal is a reconciliation rather
    than a READY byte.
    """
    from h2corn import _supervisor

    supervisor = _fleet(Config(workers=1), [True, False])
    supervisor.workers[1].retirement = _supervisor._WorkerRetirement('stop', None)
    supervisor.note_fleet_ready()

    assert supervisor.notified_ready
    assert notify_socket.recv(64) == b'READY=1'


def test_a_failed_notification_is_retried(
    unix_socket_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Latching on the attempt would suppress the retry a later pass makes."""
    monkeypatch.setenv('NOTIFY_SOCKET', str(unix_socket_dir / 'later.sock'))
    supervisor = _fleet(Config(workers=1), [True])
    supervisor.note_fleet_ready()
    assert not supervisor.notified_ready

    late = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    late.bind(str(unix_socket_dir / 'later.sock'))
    late.settimeout(2)
    try:
        supervisor.note_fleet_ready()
        assert supervisor.notified_ready
        assert late.recv(64) == b'READY=1'
    finally:
        late.close()
