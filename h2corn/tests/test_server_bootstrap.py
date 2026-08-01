import asyncio
import io
import os
import queue
import select
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from h2corn import Config


def _ipv6_loopback_is_bindable() -> bool:
    """Whether this kernel namespace has IPv6 enabled, not merely compiled."""
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(('::1', 0))
    except OSError:
        return False
    return True


def _gil_is_disabled() -> bool:
    is_gil_enabled = getattr(sys, '_is_gil_enabled', None)
    return callable(is_gil_enabled) and not is_gil_enabled()


def test_event_loop_factory_selection_is_explicit() -> None:
    import asyncio

    from h2corn._server import event_loop_factory

    assert event_loop_factory('asyncio') is asyncio.new_event_loop
    auto = event_loop_factory('auto')
    try:
        import uvloop
    except ImportError:
        assert auto is asyncio.new_event_loop
    else:
        assert auto is uvloop.new_event_loop
        assert event_loop_factory('uvloop') is uvloop.new_event_loop


def test_python_m_h2corn_runs_cli_without_target() -> None:
    result = subprocess.run(
        [sys.executable, '-Werror', '-m', 'h2corn', '--check-config'],
        check=False,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0
    assert 'RuntimeWarning' not in result.stderr


def test_python_m_h2corn_accepts_target_before_arguments() -> None:
    result = subprocess.run(
        [sys.executable, '-Werror', '-m', 'h2corn', 'example:app', '--check-config'],
        check=False,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0
    assert 'RuntimeWarning' not in result.stderr


def test_python_m_h2corn_server_runs_without_runpy_warning() -> None:
    result = subprocess.run(
        [sys.executable, '-Werror', '-m', 'h2corn._server', '--check-config'],
        check=False,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0
    assert 'RuntimeWarning' not in result.stderr


@pytest.fixture
def bind_listeners():
    """Build real bound listeners from a Config and close them at teardown.

    These tests exercise the actual socket setup against the running kernel
    rather than mocking the platform and asserting setsockopt call sequences —
    the listener's observable end state (non-blocking, port, options) is what
    matters and it stays meaningful on every OS.
    """
    from h2corn import _socket

    opened_leases: list[Any] = []

    def _bind(**config_kwargs: Any) -> tuple[Config, list[socket.socket]]:
        config = Config(**config_kwargs)
        leases = _socket._build_sockets(config)
        opened_leases.extend(leases)
        sockets: list[socket.socket] = []
        for lease in leases:
            assert lease.socket is not None
            sockets.append(lease.socket)
        return config, sockets

    yield _bind
    for lease in opened_leases:
        lease.release()


def test_listener_is_nonblocking(bind_listeners) -> None:
    # Whichever way it is made non-blocking (SOCK_NONBLOCK at creation on Linux,
    # setblocking(False) afterwards elsewhere), the listener must end up so.
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets
    assert all(not sock.getblocking() for sock in sockets)


def test_listener_sets_reuseaddr(bind_listeners) -> None:
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets[0].getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)


def test_build_sockets_keeps_config_stable_and_reports_allocated_port(
    bind_listeners,
) -> None:
    from h2corn._socket import bound_addresses

    config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    bound_port = sockets[0].getsockname()[1]
    assert bound_port > 0
    assert config.bind == ('127.0.0.1:0',)
    assert bound_addresses(sockets) == (f'127.0.0.1:{bound_port}',)


@pytest.mark.skipif(
    not _ipv6_loopback_is_bindable(),
    reason='IPv6 loopback is disabled in this kernel namespace',
)
def test_build_sockets_shares_kernel_port_across_zero_binds(bind_listeners) -> None:
    _config, sockets = bind_listeners(bind=('127.0.0.1:0', '[::1]:0'))
    assert len(sockets) == 2
    assert len({sock.getsockname()[1] for sock in sockets}) == 1


def test_build_sockets_rolls_back_listeners_on_partial_failure() -> None:
    from h2corn import _socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        free_port = probe.getsockname()[1]

    # The first listener binds the free port; the second targets a non-local
    # address (TEST-NET-1, RFC 5737) that no host can bind, so the build fails
    # partway and must roll back — leaving the first port bindable again.
    config = Config(bind=(f'127.0.0.1:{free_port}', f'192.0.2.1:{free_port}'))
    with pytest.raises(OSError):
        _socket._build_sockets(config)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as rebind:
        rebind.bind(('127.0.0.1', free_port))


@pytest.mark.skipif(sys.platform != 'linux', reason='TCP_DEFER_ACCEPT is Linux-only')
def test_listener_sets_tcp_defer_accept_on_linux(bind_listeners) -> None:
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets[0].getsockopt(socket.IPPROTO_TCP, socket.TCP_DEFER_ACCEPT) > 0


@pytest.mark.skipif(
    sys.platform != 'linux',
    reason='server-side TCP Fast Open via setsockopt is Linux-only',
)
def test_listener_enables_tcp_fastopen_on_linux(bind_listeners) -> None:
    # On a Linux listener TCP_FASTOPEN stores the accept-queue length we request.
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    # Query by the UAPI option number rather than the binding's attribute, so
    # the assertion holds on interpreters whose socket module omits the name.
    fastopen = getattr(socket, 'TCP_FASTOPEN', 23)
    assert sockets[0].getsockopt(socket.IPPROTO_TCP, fastopen) > 0


@pytest.mark.skipif(sys.platform == 'win32', reason='unix sockets are not supported')
def test_socket_path_cleanup_never_unlinks_a_replacement(
    unix_socket_dir: Path,
) -> None:
    """Cleanup must delete the inode it bound, not whatever holds the name.

    Generations overlap during a restart: the new one rebinds the path before
    the old one finishes cleaning up. Unlinking by name would take the live
    endpoint out from under it.
    """
    from h2corn import _socket

    socket_path = unix_socket_dir / 'generation.sock'
    config = Config(bind=(f'unix:{socket_path}',))

    old_listener = _socket._build_unix_listener(socket_path, config)
    # The next generation rebinds the same path, replacing the inode.
    new_listener = _socket._build_unix_listener(socket_path, config)
    try:
        old_listener.release()
        assert socket_path.is_socket(), 'the live socket must survive'
        entry = socket_path.lstat()
        assert new_listener.path is not None
        assert (entry.st_dev, entry.st_ino) == (
            new_listener.path.device,
            new_listener.path.inode,
        )

        new_listener.release()
        assert not socket_path.exists()
    finally:
        old_listener.release()
        new_listener.release()


def test_unix_socket_failure_after_bind_leaves_no_path_behind(
    monkeypatch: pytest.MonkeyPatch,
    unix_socket_dir: Path,
) -> None:
    """A path is owned from the moment it exists, not once it is returned."""
    from h2corn import _socket

    socket_path = unix_socket_dir / 'partial.sock'
    config = Config(bind=(f'unix:{socket_path}',))
    monkeypatch.setattr(
        _socket.socket.socket,
        'listen',
        lambda *_args: (_ for _ in ()).throw(OSError('listen failed')),
    )

    with pytest.raises(OSError, match='listen failed'):
        _socket._build_unix_listener(socket_path, config)

    assert not socket_path.exists()


def test_build_unix_socket_applies_owner_ids(
    monkeypatch: pytest.MonkeyPatch,
    unix_socket_dir: Path,
) -> None:
    from h2corn import _socket

    socket_path = unix_socket_dir / 'owned.sock'
    config = Config(bind=(f'unix:{socket_path}',))

    # chown(2) needs privileges we lack in CI, so record the request rather than
    # perform it; everything else — creating and binding the socket — is real.
    chowns = []
    monkeypatch.setattr(_socket.os, 'chown', lambda *args: chowns.append(args))

    listener = _socket._build_unix_listener(
        socket_path,
        config,
        owner_uid=1000,
        owner_gid=1001,
    )
    try:
        assert socket_path.is_socket()
        assert chowns == [(str(socket_path), 1000, 1001)]
        # Ownership is the inode that was bound, not the name.
        entry = socket_path.lstat()
        assert listener.path is not None
        assert (listener.path.device, listener.path.inode) == (
            entry.st_dev,
            entry.st_ino,
        )
    finally:
        listener.release()


def test_resolve_process_identity_uses_user_primary_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    monkeypatch.setattr(_server.sys, 'platform', 'linux')
    monkeypatch.setitem(
        sys.modules,
        'pwd',
        SimpleNamespace(
            getpwnam=lambda value: SimpleNamespace(
                pw_name=value,
                pw_uid=1000,
                pw_gid=1001,
            ),
            getpwuid=lambda value: SimpleNamespace(
                pw_name='www-data',
                pw_uid=value,
                pw_gid=1001,
            ),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        'grp',
        SimpleNamespace(
            getgrnam=lambda _value: SimpleNamespace(gr_gid=2001),
            getgrgid=lambda value: SimpleNamespace(gr_gid=value),
        ),
    )

    identity = _server.resolve_process_identity(Config(user='www-data'))

    assert identity == _server.ProcessIdentity(
        uid=1000,
        gid=1001,
        username='www-data',
    )


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX privilege drop')
def test_drop_process_privileges_sets_groups_before_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    calls = []
    monkeypatch.setattr(_server.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(_server.os, 'getegid', lambda: 0)
    monkeypatch.setattr(
        _server.os,
        'initgroups',
        lambda *args: calls.append(('initgroups', args)),
    )
    monkeypatch.setattr(
        _server.os,
        'setgid',
        lambda *args: calls.append(('setgid', args)),
    )
    monkeypatch.setattr(
        _server.os,
        'setuid',
        lambda *args: calls.append(('setuid', args)),
    )

    _server.drop_process_privileges(
        _server.ProcessIdentity(uid=1000, gid=1001, username='www-data')
    )

    assert calls == [
        ('initgroups', ('www-data', 1001)),
        ('setgid', (1001,)),
        ('setuid', (1000,)),
    ]


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_serve_import_target_defers_import_when_privilege_drop_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}
    imported = False

    def fake_import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    monkeypatch.setattr(_server.sys, 'platform', 'linux')
    monkeypatch.setattr(_server, 'import_target', fake_import_target)

    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda app, config, **_kwargs: captured.setdefault('supervisor', (app, config)),
    )

    import_settings = ImportSettings(target='example:app')
    config = Config(user='www-data')

    _server._serve_import_target(import_settings, config)

    assert imported is False
    assert captured['supervisor'] == (import_settings, config)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_worker_entry_imports_after_privilege_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server, _supervisor
    from h2corn._cli import ImportSettings

    order: list[str] = []
    captured = {}

    async def imported_app(_scope, _receive, _send):
        return None

    monkeypatch.setattr(
        _server,
        'drop_process_privileges',
        lambda _identity: order.append('drop'),
    )
    monkeypatch.setattr(
        _server,
        'import_target',
        lambda _import_settings: order.append('import') or imported_app,
    )

    class FakeServer:
        def __init__(self, app, config):
            captured['app'] = app
            self.app = app
            self.config = config
            self.releasing = False

        async def serve_worker_fds(self, *_args, **_kwargs):
            return None

        def shutdown(self):
            return None

        def _request_restart(self):
            return None

    monkeypatch.setattr(_server, 'Server', FakeServer)

    import_settings = ImportSettings(target='example:app')
    closed_fds: list[int] = []
    real_close = os.close

    def recording_close(fd: int) -> None:
        closed_fds.append(fd)
        real_close(fd)

    monkeypatch.setattr(_supervisor.os, 'close', recording_close)
    inherited_control_fd, inherited_control_peer = os.pipe()
    inherited_quiesce_peer, inherited_quiesce_fd = os.pipe()
    try:
        from h2corn._lib import prepare_tls

        _supervisor._worker_entry(
            import_settings,
            config=Config(),
            fds=(),
            identity=_server.ProcessIdentity(),
            prepared_tls=prepare_tls(Config()),
            expected_supervisor_pid=os.getppid(),
            inherited_supervisor_fds=(inherited_control_fd, inherited_quiesce_fd),
        )
        assert {inherited_control_fd, inherited_quiesce_fd} <= set(closed_fds)
    finally:
        real_close(inherited_control_peer)
        real_close(inherited_quiesce_peer)

    assert order == ['drop', 'import']
    assert captured['app'] is imported_app


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_worker_ready_is_emitted_only_by_serve_fds_ready_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server, _supervisor

    messages: list[bytes] = []
    ready_attempts = 0

    async def app(_scope, _receive, _send):
        return None

    class FakeServer:
        def __init__(self, app, config):
            self.app = app
            self.config = config
            self.releasing = False

        async def serve_worker_fds(self, *_args, **kwargs):
            assert _supervisor._CONTROL_READY not in messages
            assert kwargs['quiesce_fd'] == quiesce_read_fd
            os.close(quiesce_read_fd)
            kwargs['ready_trigger']()
            await asyncio.sleep(0.05)

        def shutdown(self):
            return None

        def _request_restart(self):
            return None

    control_read_fd, control_write_fd = os.pipe()
    quiesce_read_fd, quiesce_write_fd = os.pipe()
    config = Config(loop='asyncio', timeout_worker_healthcheck=0.03)
    monkeypatch.setattr(_server, 'Server', FakeServer)
    monkeypatch.setattr(_server, 'drop_process_privileges', lambda _identity: None)

    def _write(_fd: int, message: bytes) -> int:
        nonlocal ready_attempts
        if message == _supervisor._CONTROL_READY:
            ready_attempts += 1
            if ready_attempts == 1:
                raise BlockingIOError
        messages.append(message)
        return len(message)

    monkeypatch.setattr(_supervisor.os, 'write', _write)
    try:
        from h2corn._lib import prepare_tls

        _supervisor._worker_entry(
            app,
            config=config,
            fds=(),
            identity=_server.ProcessIdentity(),
            prepared_tls=prepare_tls(config),
            expected_supervisor_pid=os.getppid(),
            control_write_fd=control_write_fd,
            quiesce_read_fd=quiesce_read_fd,
        )
    finally:
        os.close(control_read_fd)
        os.close(quiesce_write_fd)

    assert ready_attempts >= 2
    assert _supervisor._CONTROL_READY in messages


def test_disabled_worker_healthcheck_never_creates_a_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    monkeypatch.setattr(_supervisor.time, 'monotonic', lambda: 10.0)
    disabled = _supervisor_state(Config(timeout_worker_healthcheck=0))
    enabled = _supervisor_state(Config(timeout_worker_healthcheck=3.5))
    process = _FakeWorkerProcess(7)
    disabled.workers[7] = _supervisor._Worker(process, read_fd, None)
    enabled.workers[7] = _supervisor._Worker(process, read_fd, None)
    try:
        os.write(write_fd, _supervisor._CONTROL_HEARTBEAT)
        disabled.drain_control_messages(7)
        assert disabled.workers[7].health_deadline is None

        os.write(write_fd, _supervisor._CONTROL_HEARTBEAT)
        enabled.drain_control_messages(7)
        assert enabled.workers[7].health_deadline == 13.5
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_worker_retirement_gives_request_cleanup_and_lifespan_separate_deadlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    now = 10.0
    monkeypatch.setattr(_supervisor.time, 'monotonic', lambda: now)
    supervisor = _supervisor_state(
        Config(timeout_graceful_shutdown=3.0, timeout_lifespan_shutdown=5.0)
    )
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    supervisor.workers[7] = _supervisor._Worker(_FakeWorkerProcess(7), read_fd, None)
    supervisor.begin_worker_retirement(7, restart=False)
    now = 11.0
    assert not supervisor.begin_worker_retirement(7, restart=False)
    # The native request wait consumes one grace interval; cancellation and
    # ASGI cleanup receive the second. Repeated retirement reasons cannot
    # silently move either boundary.
    assert supervisor.wait_timeout() == 5.0
    supervisor.kill_expired_retirements()
    assert supervisor.workers[7].retirement is not None

    now = 16.0
    os.write(write_fd, _supervisor._CONTROL_LIFESPAN)
    supervisor.drain_control_messages(7)
    # This acknowledgement, not the original retirement clock, starts the
    # primary lifespan deadline.
    assert supervisor.wait_timeout() == 5.0
    now = 20.99
    supervisor.kill_expired_retirements()
    assert supervisor.workers[7].retirement is not None
    now = 21.0
    supervisor.kill_expired_retirements()
    assert supervisor.workers[7].retirement is None
    os.close(read_fd)
    os.close(write_fd)


def test_worker_retirement_capacity_evicts_the_oldest_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    now = 10.0
    monkeypatch.setattr(_supervisor.time, 'monotonic', lambda: now)
    supervisor = _supervisor_state(Config(workers=1))
    supervisor.workers[7] = _supervisor._Worker(
        _FakeWorkerProcess(7),
        -1,
        None,
        expected_exit=True,
        retirement=_supervisor._WorkerRetirement('request cleanup', 70.0),
    )
    now = 11.0
    supervisor.workers[8] = _supervisor._Worker(
        _FakeWorkerProcess(8),
        -1,
        None,
        expected_exit=True,
        retirement=_supervisor._WorkerRetirement('request cleanup', 71.0),
    )

    supervisor.reconcile()

    assert supervisor.workers[7].forced_retirement_reap
    assert supervisor.workers[7].retirement is None
    assert supervisor.workers[8].retirement is not None


@pytest.mark.skipif(sys.platform == 'win32', reason='the supervisor is POSIX-only')
def test_supervisor_keeps_worker_alive_from_request_cleanup_through_lifespan(
    tmp_path: Path,
) -> None:
    """SIGTERM must traverse the worker's acknowledged lifecycle phases.

    The pipes are explicit cross-process synchronisation points.  In
    particular, the test releases request cleanup only after cancellation was
    observed; a supervisor that starts and spends its only deadline before
    native cancellation can never reach the lifespan marker.
    """
    from tests._support import find_free_port

    def wait_for_marker(fd: int, name: str) -> None:
        readable, _writable, _exceptional = select.select([fd], [], [], 5)
        assert readable, f'worker never reached {name}'
        assert os.read(fd, 1) == b'x'

    module = tmp_path / 'supervisor_lifecycle_app.py'
    module.write_text(
        """
import asyncio
import os


def marker(name):
    os.write(int(os.environ[f'H2CORN_MARK_{name}_FD']), b'x')


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                marker('STARTUP')
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                marker('LIFESPAN')
                await send({'type': 'lifespan.shutdown.complete'})
                return
    if scope['type'] == 'http':
        marker('REQUEST')
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            marker('CANCELLED')
            await asyncio.to_thread(os.read, int(os.environ['H2CORN_CLEANUP_FD']), 1)
            raise
"""
    )
    startup_read, startup_write = os.pipe()
    request_read, request_write = os.pipe()
    cancelled_read, cancelled_write = os.pipe()
    lifespan_read, lifespan_write = os.pipe()
    cleanup_read, cleanup_write = os.pipe()
    process: subprocess.Popen[bytes] | None = None
    request: socket.socket | None = None
    port = find_free_port()
    open_fds = {
        startup_read,
        startup_write,
        request_read,
        request_write,
        cancelled_read,
        cancelled_write,
        lifespan_read,
        lifespan_write,
        cleanup_read,
        cleanup_write,
    }
    try:
        environment = os.environ.copy()
        environment['PYTHONPATH'] = os.pathsep.join(
            filter(None, (str(tmp_path), environment.get('PYTHONPATH')))
        )
        environment.update({
            'H2CORN_MARK_STARTUP_FD': str(startup_write),
            'H2CORN_MARK_REQUEST_FD': str(request_write),
            'H2CORN_MARK_CANCELLED_FD': str(cancelled_write),
            'H2CORN_MARK_LIFESPAN_FD': str(lifespan_write),
            'H2CORN_CLEANUP_FD': str(cleanup_read),
        })
        command = (
            'from h2corn import Config, serve; '
            'from supervisor_lifecycle_app import app; '
            f"serve(app, Config(bind=('127.0.0.1:{port}',), access_log=False, "
            "lifespan='on', timeout_graceful_shutdown=1, "
            'timeout_lifespan_shutdown=5))'
        )
        process = subprocess.Popen(
            [sys.executable, '-c', command],
            env=environment,
            stderr=subprocess.PIPE,
            pass_fds=(
                startup_write,
                request_write,
                cancelled_write,
                lifespan_write,
                cleanup_read,
            ),
            start_new_session=True,
        )
        for fd in (
            startup_write,
            request_write,
            cancelled_write,
            lifespan_write,
            cleanup_read,
        ):
            os.close(fd)
            open_fds.remove(fd)

        wait_for_marker(startup_read, 'lifespan startup')
        deadline = time.monotonic() + 5
        while True:
            try:
                request = socket.create_connection(('127.0.0.1', port), timeout=0.2)
            except OSError:
                if time.monotonic() >= deadline:
                    pytest.fail('worker did not begin accepting after lifespan startup')
                continue
            break
        request.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
        wait_for_marker(request_read, 'request start')

        process.send_signal(signal.SIGTERM)
        wait_for_marker(cancelled_read, 'request cancellation')
        os.write(cleanup_write, b'x')
        wait_for_marker(lifespan_read, 'lifespan shutdown')
        exit_code = process.wait(timeout=5)
        assert process.stderr is not None
        assert exit_code == 0, process.stderr.read().decode()
    finally:
        if request is not None:
            request.close()
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        for fd in open_fds:
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.mark.parametrize(
    ('restart', 'message'),
    [(False, b'S'), (True, b'R')],
)
def test_worker_quiesce_message_transfers_kind_and_closes_writer(
    restart: bool,
    message: bytes,
) -> None:
    from h2corn import _supervisor

    read_fd, write_fd = os.pipe()
    try:
        assert _supervisor._send_worker_quiesce(write_fd, restart=restart) is None
        assert os.read(read_fd, 1) == message
        with pytest.raises(OSError):
            os.fstat(write_fd)
    finally:
        os.close(read_fd)


def test_worker_quiesce_reports_nothing_when_the_worker_already_left() -> None:
    """A worker that closed its read end is already in the asked-for state."""
    from h2corn import _supervisor

    read_fd, write_fd = os.pipe()
    os.close(read_fd)

    assert _supervisor._send_worker_quiesce(write_fd, restart=True) is None
    with pytest.raises(OSError):
        os.fstat(write_fd)


def test_worker_quiesce_reports_an_unexpected_failure_and_closes_the_channel() -> None:
    from h2corn import _supervisor

    read_fd, write_fd = os.pipe()
    try:
        # Writing to the *read* end fails with EBADF, which is not the ordinary
        # shutdown race and must still reach the operator.
        failure = _supervisor._send_worker_quiesce(read_fd, restart=True)
        assert isinstance(failure, OSError)
        assert not isinstance(failure, BrokenPipeError)
        with pytest.raises(OSError):
            os.fstat(read_fd)
    finally:
        os.close(write_fd)


def _supervisor_state(config: Config):
    """A `_Supervisor` whose worker state the test populates by hand."""
    from h2corn import _server, _supervisor
    from h2corn._cli import ImportSettings
    from h2corn._lib import prepare_tls

    return _supervisor._Supervisor(
        app=ImportSettings(target='example:app'),
        config=config,
        fds=(),
        identity=_server.ProcessIdentity(),
        prepared_tls=prepare_tls(config),
    )


class _FakeWorkerProcess:
    """Stands in for a worker process.

    Structural against `_supervisor._WorkerProcess`, so the members the
    supervisor actually uses are checked rather than assumed -- this double
    was missing `start` entirely and took no timeout on `join`.
    """

    def __init__(self, sentinel: int, *, alive: bool = False) -> None:
        self.sentinel = sentinel
        self.pid: int | None = sentinel + 1000
        self.exitcode: int | None = 0
        self._alive = alive
        self.closed = False
        self.started = False

    def start(self) -> None:
        self.started = True
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    def terminate(self) -> None:
        self._alive = False

    def kill(self) -> None:
        self._alive = False

    def join(self, timeout: float | None = None) -> None:
        del timeout

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    'failure',
    [
        'second-pipe',
        'process-construction',
        'process-start',
        'process-start-after-child',
        'parent-close',
        'sentinel-registration',
        'control-registration',
        'startup-log',
    ],
)
def test_supervisor_spawn_rolls_back_every_partial_registration_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """No failed spawn leaves a child or a supervisor-owned descriptor behind."""
    from h2corn import _log, _supervisor

    real_close = os.close
    real_pipe = _supervisor.nonblocking_pipe
    pipe_pairs: list[tuple[int, int]] = []

    class FakeProcess:
        def __init__(self) -> None:
            self.pid = 5000
            self._sentinel, self._sentinel_write = os.pipe()
            self.started = False
            self.closed = False

        @property
        def sentinel(self) -> int:
            return self._sentinel

        def start(self) -> None:
            if failure == 'process-start':
                raise OSError('planned failure')
            self.started = True
            if failure == 'process-start-after-child':
                raise OSError('planned failure')

        def is_alive(self) -> bool:
            return self.started

        def terminate(self) -> None:
            self.started = False

        def kill(self) -> None:
            self.started = False

        def join(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True
            for fd in (self._sentinel, self._sentinel_write):
                try:
                    real_close(fd)
                except OSError:
                    pass

    processes: list[FakeProcess] = []

    class Context:
        def Process(self, **_kwargs):  # noqa: N802
            if failure == 'process-construction':
                raise OSError('planned failure')
            process = FakeProcess()
            processes.append(process)
            return process

    pipe_calls = 0

    def make_pipe() -> tuple[int, int]:
        nonlocal pipe_calls
        pipe_calls += 1
        if failure == 'second-pipe' and pipe_calls == 2:
            raise OSError('planned failure')
        pair = real_pipe()
        pipe_pairs.append(pair)
        return pair

    monkeypatch.setattr(_supervisor, 'nonblocking_pipe', make_pipe)
    monkeypatch.setattr(
        _supervisor.multiprocessing,
        'get_context',
        lambda _name: Context(),
    )
    supervisor = _supervisor_state(Config())
    before = set(os.listdir('/proc/self/fd'))
    if failure == 'parent-close':
        failed_close = False

        def close_once(fd: int) -> None:
            nonlocal failed_close
            if not failed_close and fd == pipe_pairs[0][1]:
                failed_close = True
                raise OSError('planned failure')
            real_close(fd)

        monkeypatch.setattr(_supervisor.os, 'close', close_once)
    if failure in {'sentinel-registration', 'control-registration'}:
        real_register = supervisor.selector.register
        registrations = 0

        def register(*args, **kwargs):
            nonlocal registrations
            registrations += 1
            if failure == 'sentinel-registration' or registrations == 2:
                raise OSError('planned failure')
            return real_register(*args, **kwargs)

        monkeypatch.setattr(supervisor.selector, 'register', register)
    if failure == 'startup-log':
        monkeypatch.setattr(
            _log.Event,
            'log',
            lambda *_args, **_fields: (_ for _ in ()).throw(OSError('planned failure')),
        )
    try:
        with pytest.raises(OSError, match='planned failure'):
            supervisor.spawn_worker()
        assert supervisor.workers == {}
        assert all(process.closed for process in processes)
        assert set(os.listdir('/proc/self/fd')) == before
    finally:
        supervisor.selector.close()


def test_supervisor_reap_closes_each_owned_fd_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duplicate readiness from one dead child cannot close a reused fd."""
    from h2corn import _supervisor

    supervisor = _supervisor_state(Config())
    control_read, control_write = os.pipe()
    quiesce_read, quiesce_write = os.pipe()
    sentinel, sentinel_write = os.pipe()
    closed: list[int] = []
    real_close = os.close

    class Process:
        pid: int | None = 9876
        exitcode: int | None = 0
        _alive = False

        @property
        def sentinel(self) -> int:
            return sentinel

        def start(self) -> None:
            self._alive = True

        def is_alive(self) -> bool:
            return self._alive

        def terminate(self) -> None:
            self._alive = False

        def kill(self) -> None:
            self._alive = False

        def join(self, timeout: float | None = None) -> None:
            del timeout

        def close(self) -> None:
            real_close(sentinel)
            real_close(sentinel_write)

    def record_close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    supervisor.workers[sentinel] = _supervisor._Worker(
        Process(),
        control_read,
        quiesce_write,
        expected_exit=True,
    )
    monkeypatch.setattr(_supervisor.os, 'close', record_close)
    try:
        supervisor.handle_worker_event('worker-exit', sentinel)
        supervisor.handle_worker_event('worker-exit', sentinel)
        assert closed.count(control_read) == 1
        assert closed.count(quiesce_write) == 1
    finally:
        real_close(control_write)
        real_close(quiesce_read)
        supervisor.selector.close()


def test_supervisor_wait_never_exceeds_what_the_selector_accepts() -> None:
    """A far-off deadline must not become an `OverflowError` in the loop.

    `epoll_wait` takes signed 32-bit milliseconds, so a healthcheck deadline a
    few thousand years out cannot be handed over as-is. Waking early is free —
    the loop simply runs again.
    """
    import selectors

    from h2corn import _supervisor

    supervisor = _supervisor_state(Config(workers=1))
    supervisor.workers[3] = _supervisor._Worker(
        _FakeWorkerProcess(3), -1, None, health_deadline=time.monotonic() + 1e12
    )

    timeout = supervisor.wait_timeout()

    assert timeout is not None
    assert timeout == _supervisor._MAX_SELECT_TIMEOUT
    # And the value is one the selector will really take.
    selector = selectors.DefaultSelector()
    read_fd, write_fd = os.pipe()
    try:
        selector.register(read_fd, selectors.EVENT_READ)
        os.write(write_fd, b'x')
        assert selector.select(timeout)
    finally:
        selector.close()
        os.close(read_fd)
        os.close(write_fd)


def test_worker_replacement_capacity_allows_one_bounded_retiring_generation() -> None:
    from h2corn import _supervisor

    supervisor = _supervisor_state(Config(workers=4))

    def can_spawn(*, process_count: int, active_workers: int) -> bool:
        supervisor.workers = {
            sentinel: _supervisor._Worker(
                _FakeWorkerProcess(sentinel),
                -1,
                None,
                expected_exit=sentinel < process_count - active_workers,
            )
            for sentinel in range(process_count)
        }
        return supervisor.can_spawn_worker()

    target = 4
    assert can_spawn(process_count=target, active_workers=0)
    assert can_spawn(process_count=target * 2 - 1, active_workers=target - 1)
    assert not can_spawn(process_count=target * 2, active_workers=0)
    assert not can_spawn(process_count=target, active_workers=target)


def test_scale_down_during_reload_preserves_the_unready_replacement() -> None:
    # Insertion order mirrors two serving workers followed by their unready
    # rolling replacement. Reducing the target from two to one must retire the
    # other old worker, not consume the scale-down by killing the replacement.
    from h2corn import _supervisor

    supervisor = _supervisor_state(Config())
    supervisor.workers = {
        sentinel: _supervisor._Worker(_FakeWorkerProcess(sentinel), -1, None)
        for sentinel in (11, 12, 13)
    }
    supervisor.reload_cycle = _supervisor._ReloadCycle(target=11, replacement=13)

    assert supervisor.active_worker_capacity() == 2
    assert supervisor.scale_down_candidate() == 12


def test_health_retired_reload_replacement_cannot_retire_healthy_target() -> None:
    from h2corn import _supervisor

    supervisor = _supervisor_state(Config())
    supervisor.workers = {
        sentinel: _supervisor._Worker(_FakeWorkerProcess(sentinel), -1, None)
        for sentinel in (11, 13)
    }
    supervisor.reload_cycle = _supervisor._ReloadCycle(target=11, replacement=13)

    assert supervisor.is_viable_reload_replacement(13)
    supervisor.workers[13].expected_exit = True
    assert not supervisor.is_viable_reload_replacement(13)
    supervisor.workers[13].expected_exit = False
    assert not supervisor.is_viable_reload_replacement(12)


def test_failed_reload_replacement_spawn_leaves_no_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed spawn cannot leave a target without a real replacement."""
    from h2corn import _supervisor

    supervisor = _supervisor_state(Config())
    supervisor.workers = {11: _supervisor._Worker(_FakeWorkerProcess(11), -1, None)}
    supervisor.schedule_worker_retire(11)

    def fail_spawn(_self: object) -> int:
        raise OSError('planned replacement failure')

    monkeypatch.setattr(_supervisor._Supervisor, 'spawn_worker', fail_spawn)
    with pytest.raises(OSError, match='planned replacement failure'):
        supervisor.reconcile()

    assert supervisor.reload_cycle is None


def test_expected_retirement_does_not_count_as_a_worker_failure() -> None:
    from h2corn import _supervisor

    read_fd, write_fd = os.pipe()
    supervisor = _supervisor_state(Config())
    worker = _supervisor._Worker(
        _FakeWorkerProcess(11), read_fd, write_fd, expected_exit=True
    )
    supervisor.retire_worker(11, worker)

    assert not supervisor.failure_times


def test_pidfile_writes_and_cleans_up_regular_file(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'

    with _server._pidfile(Config(pid=pid_path)):
        assert pid_path.read_text() == f'{os.getpid()}\n'

    assert not pid_path.exists()


@pytest.mark.skipif(sys.platform == 'win32', reason='symlink semantics differ')
def test_pidfile_rejects_preexisting_symlink(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'
    victim = tmp_path / 'victim.txt'
    victim.write_text('SECRET\n')
    pid_path.symlink_to(victim)

    with pytest.raises(OSError), _server._pidfile(Config(pid=pid_path)):
        pass

    assert victim.read_text() == 'SECRET\n'
    assert pid_path.is_symlink()


@pytest.mark.skipif(
    sys.platform == 'win32',
    reason='an open pidfile cannot be replaced on Windows (it is locked)',
)
def test_pidfile_cleanup_keeps_replaced_path(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'

    with _server._pidfile(Config(pid=pid_path)):
        pid_path.unlink()
        pid_path.write_text('replacement\n')

    assert pid_path.read_text() == 'replacement\n'


def test_import_target_requires_module_app_form() -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    with pytest.raises(ValueError, match='module:app form'):
        _server.import_target(ImportSettings(target='demoapp'))


def test_import_target_names_the_target_when_module_import_fails() -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    with pytest.raises(
        RuntimeError,
        match=r"could not import module 'nosuchmod' from target 'nosuchmod:app'",
    ) as raised:
        _server.import_target(ImportSettings(target='nosuchmod:app'))

    assert isinstance(raised.value.__cause__, ModuleNotFoundError)


def test_tcp_listener_error_names_the_failed_bind() -> None:
    from h2corn import _socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(('127.0.0.1', 0))
        occupied.listen()
        host, port = occupied.getsockname()

        with pytest.raises(OSError, match=rf'could not bind {host}:{port}:'):
            _socket._build_tcp_listener(host, port, Config())


def test_import_target_requires_callable_attribute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    module_name = 'demoapp_not_callable'
    (tmp_path / f'{module_name}.py').write_text('app = 1\n')
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(
        TypeError,
        match=rf"import target '{module_name}:app' is not callable",
    ):
        _server.import_target(ImportSettings(target=f'{module_name}:app'))


def test_import_target_calls_factory_when_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    module_name = 'demoapp_factory'
    (tmp_path / f'{module_name}.py').write_text(
        """
def create_app():
    async def app(scope, receive, send):
        return None
    return app
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    app = _server.import_target(
        ImportSettings(target=f'{module_name}:create_app', factory=True)
    )

    assert callable(app)


def test_import_target_requires_factory_result_to_be_callable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    module_name = 'demoapp_factory_value'
    (tmp_path / f'{module_name}.py').write_text(
        """
def create_app():
    return 1
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(
        TypeError,
        match=rf"import target '{module_name}:create_app' factory returned a non-callable",
    ):
        _server.import_target(
            ImportSettings(target=f'{module_name}:create_app', factory=True)
        )


def test_import_target_uses_app_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    app_dir.mkdir()
    (app_dir / 'demoapp_in_app_dir.py').write_text(
        """
async def app(scope, receive, send):
    return None
"""
    )
    monkeypatch.chdir(tmp_path)

    app = _server.import_target(
        ImportSettings(target='demoapp_in_app_dir:app', app_dir=app_dir)
    )

    assert callable(app)


def test_import_target_moves_app_dir_to_sys_path_front(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    shadow_dir = tmp_path / 'shadow'
    app_dir.mkdir()
    shadow_dir.mkdir()
    (app_dir / 'demoapp_precedence.py').write_text(
        """
async def app(scope, receive, send):
    return None

app.source = 'app-dir'
"""
    )
    (shadow_dir / 'demoapp_precedence.py').write_text(
        """
async def app(scope, receive, send):
    return None

app.source = 'shadow'
"""
    )
    monkeypatch.setattr(sys, 'path', [str(shadow_dir), str(app_dir), *sys.path])
    monkeypatch.chdir(tmp_path)

    app = _server.import_target(
        ImportSettings(target='demoapp_precedence:app', app_dir=app_dir)
    )

    assert vars(app)['source'] == 'app-dir'


def test_import_target_loads_env_file_before_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    app_dir.mkdir()
    env_file = tmp_path / '.env'
    env_file.write_text('DEMO_APP_VALUE=loaded\n')
    (app_dir / 'demoapp_from_env.py').write_text(
        """
import os

async def app(scope, receive, send):
    return None

app.loaded = os.environ['DEMO_APP_VALUE']
"""
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('DEMO_APP_VALUE', raising=False)

    app = _server.import_target(
        ImportSettings(
            target='demoapp_from_env:app',
            app_dir=app_dir,
            env_file=env_file,
        )
    )

    assert vars(app)['loaded'] == 'loaded'


def test_import_target_env_file_does_not_override_existing_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    app_dir.mkdir()
    env_file = tmp_path / '.env'
    env_file.write_text('DEMO_APP_VALUE=loaded\n')
    (app_dir / 'demoapp_existing_env.py').write_text(
        """
import os

async def app(scope, receive, send):
    return None

app.loaded = os.environ['DEMO_APP_VALUE']
"""
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('DEMO_APP_VALUE', 'existing')

    app = _server.import_target(
        ImportSettings(
            target='demoapp_existing_env:app',
            app_dir=app_dir,
            env_file=env_file,
        )
    )

    assert vars(app)['loaded'] == 'existing'


@pytest.mark.parametrize(
    'family',
    [
        socket.AF_INET,
        pytest.param(
            getattr(socket, 'AF_UNIX', None),
            marks=pytest.mark.skipif(
                sys.platform == 'win32',
                reason='unix sockets are not supported on Windows',
            ),
        ),
    ],
)
def test_build_sockets_preserves_fd_listener_family(
    tmp_path: Path,
    family: int,
) -> None:
    from h2corn import _socket

    listener = socket.socket(family, socket.SOCK_STREAM)
    with listener:
        if family == socket.AF_UNIX:
            listener.bind(str(tmp_path / 'listener.sock'))
        else:
            listener.bind(('127.0.0.1', 0))
        listener.listen(1)

        leases = _socket._build_sockets(Config(bind=(f'fd://{listener.fileno()}',)))

        assert len(leases) == 1
        assert isinstance(leases[0], _socket._BorrowedListener)
        assert leases[0].socket is not None
        assert leases[0].socket.family == family
        # Borrowed listeners use a duplicate. A server can close its native
        # copy after serving without taking the embedding caller's fd with it.
        assert leases[0].socket.fileno() != listener.fileno()
        assert leases[0].socket.getsockname() == listener.getsockname()
        # Adoption borrows the descriptor; releasing the lease must not
        # close it out from under the `with` block that owns it.
        leases[0].release()


def test_adopting_a_descriptor_that_is_not_a_listener_is_refused() -> None:
    """
    An `fd://` bind names someone else's descriptor. A descriptor that
    cannot serve is rejected, and — since we never owned it — left open.
    """
    from h2corn import _socket

    connected = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with connected:
        with pytest.raises(ValueError, match='is not listening'):
            _socket._build_sockets(Config(bind=(f'fd://{connected.fileno()}',)))
        # Still ours, still usable: the refusal did not consume it.
        assert connected.fileno() >= 0
        connected.listen(1)


def test_binding_the_same_descriptor_twice_is_refused() -> None:
    """
    Two listeners built from one descriptor would both own it and both
    close it, so the pair is rejected where it is written.
    """
    with pytest.raises(ValueError, match='duplicate bind entry'):
        Config(bind=('fd://7', 'fd://7'))


@pytest.mark.skipif(
    sys.platform == 'win32', reason='the signal wakeup pipe is a POSIX mechanism'
)
def test_signal_wakeup_pipe_yields_nonblocking_fd() -> None:
    # However the pipe is created (os.pipe2 on Linux, os.pipe + set_blocking
    # elsewhere), it must hand back both ends as a tagged pipe and close them
    # again on exit.
    from h2corn import _socket

    with _socket.signal_wakeup_pipe() as pipe:
        assert isinstance(pipe, _socket._SignalWakeupPipe)
        assert pipe.read_fd >= 0
        assert pipe.write_fd >= 0
        assert os.get_blocking(pipe.read_fd) is False
        assert os.get_blocking(pipe.write_fd) is False
        read_fd = pipe.read_fd
        write_fd = pipe.write_fd
    with pytest.raises(OSError):
        os.fstat(read_fd)
    with pytest.raises(OSError):
        os.fstat(write_fd)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX control pipes')
def test_nonblocking_pipe_is_close_on_exec() -> None:
    from h2corn._socket import nonblocking_pipe

    read_fd, write_fd = nonblocking_pipe()
    try:
        assert not os.get_blocking(read_fd)
        assert not os.get_blocking(write_fd)
        assert not os.get_inheritable(read_fd)
        assert not os.get_inheritable(write_fd)
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_cli_trusted_proxy_flags_replace_base_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
forwarded_allow_ips = ["127.0.0.1", "::1"]
""".strip()
    )
    captured = {}

    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda app, config, **_kwargs: captured.setdefault('result', (app, config)),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--forwarded-allow-ips',
            '10.0.0.1, unix',
            'example:app',
        ],
    )

    _server.main()

    app, config = captured['result']
    assert app.target == 'example:app'
    assert app.factory is False
    assert config.forwarded_allow_ips == ('10.0.0.1', 'unix')


def test_cli_repeated_bind_replaces_base_bind_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('bind = ["127.0.0.1:9010"]')
    captured = {}

    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda _app, config, **_kwargs: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--bind',
            '127.0.0.1:9030',
            '--bind',
            'unix:/tmp/h2corn.sock',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].bind == ('127.0.0.1:9030', 'unix:/tmp/h2corn.sock')


def test_cli_arguments_override_env_and_toml_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
port = 9010
http1 = false
access_log = false
""".strip()
    )
    captured = {}

    monkeypatch.setenv('H2CORN_PORT', '9020')
    monkeypatch.setenv('H2CORN_HTTP1', 'false')
    monkeypatch.setenv('H2CORN_ACCESS_LOG', 'false')
    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda _app, config, **_kwargs: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--port',
            '9030',
            '--http1',
            '--access-log',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].bind == ('127.0.0.1:9030',)
    assert captured['config'].http1 is True
    assert captured['config'].access_log is True


def test_cli_legacy_env_port_overrides_toml_listener(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('port = 9010')
    captured = {}

    monkeypatch.setenv('H2CORN_PORT', '9020')
    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda _app, config, **_kwargs: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--config', str(config_path), 'example:app'],
    )

    _server.main()

    assert captured['config'].bind == ('127.0.0.1:9020',)


def test_cli_factory_flag_is_forwarded_to_import_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}

    # The CLI hands the *unimported* target to the supervisor, so each worker
    # imports it itself — that is what makes SIGHUP pick up new code.
    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda app, config, **_kwargs: captured.setdefault('supervised', (app, config)),
    )
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--factory', 'example:create_app'])

    _server.main()

    assert captured['supervised'][0] == ImportSettings(
        target='example:create_app',
        factory=True,
    )


def test_cli_app_dir_is_forwarded_to_import_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}

    # The CLI hands the *unimported* target to the supervisor, so each worker
    # imports it itself — that is what makes SIGHUP pick up new code.
    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda app, config, **_kwargs: captured.setdefault('supervised', (app, config)),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--app-dir', str(tmp_path / 'src'), 'example:app'],
    )

    _server.main()

    assert captured['supervised'][0] == ImportSettings(
        target='example:app',
        app_dir=(tmp_path / 'src').resolve(),
    )


def test_cli_env_file_is_forwarded_to_import_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}

    # The CLI hands the *unimported* target to the supervisor, so each worker
    # imports it itself — that is what makes SIGHUP pick up new code.
    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        'serve_with_supervisor',
        lambda app, config, **_kwargs: captured.setdefault('supervised', (app, config)),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--env-file', str(tmp_path / '.env'), 'example:app'],
    )

    _server.main()

    assert captured['supervised'][0] == ImportSettings(
        target='example:app',
        env_file=(tmp_path / '.env').resolve(),
    )


def test_cli_check_config_exits_before_import_and_serve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, 'import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--check-config', 'example:app'])

    _server.main()

    assert imported is False
    assert served is False


def test_cli_check_config_works_without_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, 'import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--check-config'])

    _server.main()

    assert imported is False
    assert served is False


def test_cli_print_config_exits_before_import_and_serve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False
    stdout = io.StringIO()

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, 'import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'stdout', stdout)
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--print-config', '--workers', '2', 'example:app'],
    )

    _server.main()

    assert imported is False
    assert served is False
    assert 'workers = 2' in stdout.getvalue()


def test_cli_print_config_works_without_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False
    stdout = io.StringIO()

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, 'import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'stdout', stdout)
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--print-config'])

    _server.main()

    assert imported is False
    assert served is False
    assert 'workers = 1' in stdout.getvalue()


def test_cli_print_config_round_trips_paths_and_unlimited_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    stdout = io.StringIO()
    pid_path = tmp_path / 'h2corn test.pid'

    monkeypatch.setattr(sys, 'stdout', stdout)
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--print-config',
            '--pid',
            os.fspath(pid_path),
            '--websocket-max-message-size',
            '0',
        ],
    )

    _server.main()

    printed = stdout.getvalue()
    parsed = tomllib.loads(printed)
    assert parsed['pid'] == os.fspath(pid_path)
    assert parsed['websocket_max_message_size'] == 0
    assert Config.from_mapping(parsed) == Config(
        pid=pid_path,
        websocket_max_message_size=0,
    )


@pytest.mark.asyncio
async def test_serve_warns_about_settings_only_a_supervisor_implements(
    recwarn: pytest.WarningsRecorder,
) -> None:
    """
    Worker retirement is something a supervisor does to a worker. An
    embedded server has no supervisor, so those settings do nothing — and
    saying so beats appearing to accept them.
    """
    from h2corn import Server

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})

    server = Server(app, Config(port=0, max_requests=100))
    serving = asyncio.create_task(server.serve())
    # Recorded rather than scoped to a `pytest.warns` block: the warning is
    # raised by the serve task, so exactly when it lands relative to any
    # block here is the scheduler's business.
    await server.wait_started()
    server.shutdown()
    await serving

    assert [
        str(warning.message)
        for warning in recwarn
        if 'max_requests describes' in str(warning.message)
    ]


@pytest.mark.asyncio
async def test_wait_started_resolves_when_the_listeners_are_live() -> None:
    from h2corn import Server

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})

    server = Server(app, Config(port=0))
    serving = asyncio.create_task(server.serve())
    await server.wait_started()
    try:
        # Readiness means connectable, not merely bound.
        host, _, port = server.addresses[0].rpartition(':')
        _reader, writer = await asyncio.open_connection(host, int(port))
        writer.close()
        await writer.wait_closed()
    finally:
        server.shutdown()
        await serving


@pytest.mark.asyncio
async def test_wait_started_can_be_awaited_before_serve_begins() -> None:
    """The waiter may reach the future first; both orders must work."""
    from h2corn import Server

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})

    server = Server(app, Config(port=0))
    waiting = asyncio.ensure_future(server.wait_started())
    await asyncio.sleep(0)
    serving = asyncio.create_task(server.serve())
    await asyncio.wait_for(waiting, timeout=10)
    assert server.addresses
    server.shutdown()
    await serving


@pytest.mark.asyncio
async def test_wait_started_raises_what_stopped_a_server_that_never_started() -> None:
    """Otherwise a bind failure is indistinguishable from a slow start."""
    from h2corn import Server

    async def app(scope, receive, send):
        return None

    # TEST-NET-1 (RFC 5737): never assignable, so the bind always fails.
    server = Server(app, Config(bind=('192.0.2.1:9',)))
    serving = asyncio.create_task(server.serve())
    with pytest.raises(OSError):
        await asyncio.wait_for(server.wait_started(), timeout=10)
    with pytest.raises(OSError):
        await serving


@pytest.mark.asyncio
async def test_wait_started_does_not_call_a_stopped_server_started() -> None:
    """Readiness describes the running server, not the one that ran."""
    from h2corn import Server

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})

    server = Server(app, Config(port=0))
    serving = asyncio.create_task(server.serve())
    await server.wait_started()
    server.shutdown()
    await serving

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(server.wait_started(), timeout=0.25)


@pytest.mark.asyncio
async def test_wait_started_refuses_to_wait_on_a_shutting_down_server() -> None:
    """
    Waiting on a draining server would mean waiting for the lifecycle after
    this one, which is not what anyone asking means.
    """
    from h2corn import Server

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})

    server = Server(app, Config(port=0))
    serving = asyncio.create_task(server.serve())
    await server.wait_started()
    server.shutdown()
    with pytest.raises(RuntimeError, match='shutting down'):
        await server.wait_started()
    await serving


@pytest.mark.asyncio
async def test_shutdown_during_claimed_starting_generation() -> None:
    """Shutdown cannot be overwritten by a later readiness publication.

    The lock deliberately releases after generation publication and holds the
    serving thread there. That gives shutdown the exact historical
    claim-to-begin slot without scheduler luck or a sleep.
    """
    from h2corn import Server

    claim_released = threading.Event()
    continue_claim = threading.Event()
    late_started = threading.Event()
    body_entered = threading.Event()
    release_body = threading.Event()

    async def app(scope, receive, send):
        raise AssertionError('the test replaces the embedded lifecycle')

    server = Server(app, Config(port=0, lifespan='off'))

    class ClaimWindowLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._first_exit = True
            self._shutdown_seen = False

        def __enter__(self):
            self._lock.acquire()
            return self

        def __exit__(self, *_args: object) -> None:
            state = server._readiness.value
            self._lock.release()
            if self._first_exit:
                self._first_exit = False
                claim_released.set()
                assert continue_claim.wait(2), 'test did not release claim window'
            elif state == 'stopping':
                self._shutdown_seen = True
            elif self._shutdown_seen and state == 'starting':
                # This branch is reachable only with the killing mutation:
                # STARTING was published after shutdown released the lock.
                late_started.set()
                assert continue_claim.wait(2), 'test did not release late STARTING'

    async def body(_generation) -> None:
        body_entered.set()
        await asyncio.to_thread(release_body.wait)

    server._state_lock = ClaimWindowLock()  # type: ignore[assignment]
    server._serve_embedded = body  # type: ignore[method-assign]
    failures: list[BaseException] = []

    def serve_in_thread() -> None:
        try:
            asyncio.run(server.serve())
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=serve_in_thread)
    thread.start()
    try:
        await asyncio.wait_for(asyncio.to_thread(claim_released.wait), timeout=2)
        server.shutdown()
        continue_claim.set()
        await asyncio.wait_for(asyncio.to_thread(body_entered.wait), timeout=2)
        with pytest.raises(RuntimeError, match='shutting down'):
            await server.wait_started()
        assert not late_started.is_set()
    finally:
        continue_claim.set()
        release_body.set()
        await asyncio.to_thread(thread.join, 2)
    assert not thread.is_alive()
    assert failures == []


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_shutdown_publishes_before_its_lifecycle_loop_can_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shutdown thread cannot retain a loop after the lifecycle releases it.

    The test pauses the old snapshot-then-publish shape *after* it releases
    the state lock. The owning loop is then allowed to return and close before
    publication resumes. The fixed shape publishes while still locked, so the
    publication event wins and no closed-loop call is possible.
    """
    from h2corn import Server

    body_started = threading.Event()
    release_body = threading.Event()
    release_shutdown = threading.Event()
    published = threading.Event()
    timeline: queue.SimpleQueue[str] = queue.SimpleQueue()
    shutdown_errors: list[BaseException] = []
    serve_errors: list[BaseException] = []

    async def app(_scope, _receive, _send):
        raise AssertionError('the test replaces the embedded lifecycle')

    server = Server(app, Config(port=0, lifespan='off'))

    class PublicationLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._armed = False
            self._shutdown_exits = 0

        def arm(self) -> None:
            self._armed = True

        def __enter__(self):
            self._lock.acquire()
            return self

        def __exit__(self, *_args: object) -> None:
            self._lock.release()
            if not self._armed:
                return
            # Shutdown first settles readiness, then snapshots its generation.
            self._shutdown_exits += 1
            if self._shutdown_exits != 2:
                return
            if published.is_set():
                timeline.put('published')
                return
            timeline.put('snapshot')
            assert release_shutdown.wait(2), 'test did not release shutdown'

    async def body(_generation) -> None:
        body_started.set()
        await asyncio.to_thread(release_body.wait)

    lock = PublicationLock()
    server._state_lock = lock  # type: ignore[assignment]
    server._serve_embedded = body  # type: ignore[method-assign]

    def serve_in_thread() -> None:
        try:
            asyncio.run(server.serve())
        except BaseException as exc:
            serve_errors.append(exc)

    def shutdown_in_thread() -> None:
        try:
            server.shutdown()
        except BaseException as exc:
            shutdown_errors.append(exc)

    serve_thread = threading.Thread(target=serve_in_thread)
    shutdown_thread: threading.Thread | None = None
    serve_thread.start()
    try:
        await asyncio.wait_for(asyncio.to_thread(body_started.wait), timeout=2)
        generation = server._generation
        assert generation is not None
        call_soon_threadsafe = generation.loop.call_soon_threadsafe

        def publish(callback, *args, **kwargs):
            published.set()
            return call_soon_threadsafe(callback, *args, **kwargs)

        monkeypatch.setattr(generation.loop, 'call_soon_threadsafe', publish)
        lock.arm()
        shutdown_thread = threading.Thread(target=shutdown_in_thread)
        shutdown_thread.start()
        outcome = await asyncio.wait_for(asyncio.to_thread(timeline.get), timeout=2)

        # The stale implementation reaches ``snapshot``. Let the body return
        # before allowing shutdown to continue, deterministically closing its
        # loop first; the recorded RuntimeError makes that mutation red.
        release_body.set()
        await asyncio.wait_for(asyncio.to_thread(serve_thread.join, 2), timeout=3)
        release_shutdown.set()
        await asyncio.wait_for(asyncio.to_thread(shutdown_thread.join, 2), timeout=3)
    finally:
        release_body.set()
        release_shutdown.set()
        if shutdown_thread is not None:
            await asyncio.to_thread(shutdown_thread.join, 2)
        await asyncio.to_thread(serve_thread.join, 2)

    assert not serve_thread.is_alive()
    assert shutdown_thread is not None and not shutdown_thread.is_alive()
    assert shutdown_errors == []
    assert serve_errors == []
    assert outcome == 'published'


@pytest.mark.asyncio
async def test_abandoned_readiness_waits_do_not_accumulate() -> None:
    """A caller that gives up must not leave a future behind for the server's
    lifetime — nor an exception nobody retrieves when the server later ends.
    """
    from h2corn import Server

    async def app(scope, receive, send):
        return None

    server = Server(app, Config(port=0))
    for _ in range(20):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(server.wait_started(), timeout=0.001)

    assert server._started_waiters == []


@pytest.mark.asyncio
async def test_wait_started_reports_a_configuration_serve_can_never_honour() -> None:
    """`workers` is rejected before any lifecycle begins, so a waiter that
    arrived first would otherwise wait for a server that can never start.
    """
    from h2corn import Server

    async def app(scope, receive, send):
        return None

    server = Server(app, Config(port=0, workers=4))
    waiting = asyncio.ensure_future(server.wait_started())
    await asyncio.sleep(0)
    with pytest.raises(NotImplementedError):
        await server.serve()
    with pytest.raises(NotImplementedError):
        await asyncio.wait_for(waiting, timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('config', 'error_type', 'message'),
    [
        (
            Config(port=0, pid=Path('server.pid'), user=os.getuid()),
            ValueError,
            'cannot combine pid',
        ),
        (Config(port=0, workers=2), NotImplementedError, 'only supports workers=1'),
        (Config(port=0, max_requests=1), UserWarning, 'nothing supervises'),
    ],
)
async def test_claimed_generation_preflight_settles_waiters(
    config: Config,
    error_type: type[BaseException],
    message: str,
) -> None:
    """Preflight runs after claim, so an existing waiter gets its real error."""
    from h2corn import Server

    waiter_registered = asyncio.Event()

    class Waiters(list[asyncio.Future[None]]):
        def append(self, waiter: asyncio.Future[None]) -> None:
            super().append(waiter)
            waiter_registered.set()

    async def app(scope, receive, send):
        raise AssertionError('preflight must fail before application startup')

    server = Server(app, config)
    server._started_waiters = Waiters()
    waiting = asyncio.create_task(server.wait_started())
    await asyncio.wait_for(waiter_registered.wait(), timeout=2)
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        serving = asyncio.create_task(server.serve())
        with pytest.raises(error_type, match=message) as serving_error:
            await asyncio.wait_for(serving, timeout=2)
    with pytest.raises(error_type, match=message) as waiting_error:
        await asyncio.wait_for(waiting, timeout=2)
    assert waiting_error.value is serving_error.value


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_duplicate_serve_does_not_settle_active_generation() -> None:
    """A failed claim must not answer waiters belonging to the first claim."""
    from h2corn import Server

    startup_entered = asyncio.Event()

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        startup_entered.set()
        await asyncio.Future()

    server = Server(app, Config(port=0, lifespan='on'))
    serving = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    waiter_registered = asyncio.Event()

    class Waiters(list[asyncio.Future[None]]):
        def append(self, waiter: asyncio.Future[None]) -> None:
            super().append(waiter)
            waiter_registered.set()

    server._started_waiters = Waiters()
    waiting = asyncio.create_task(server.wait_started())
    await asyncio.wait_for(waiter_registered.wait(), timeout=2)
    with pytest.raises(RuntimeError, match='already has an active'):
        await server.serve()
    assert not waiting.done()
    server.shutdown()
    with pytest.raises(RuntimeError, match='shutting down'):
        await asyncio.wait_for(waiting, timeout=2)
    await asyncio.wait_for(serving, timeout=2)


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
@pytest.mark.parametrize(
    'exit_kind', ['failed', 'raise', 'timeout', 'cancel', 'shutdown']
)
@pytest.mark.parametrize('listener_kind', ['tcp', 'unix'])
async def test_pre_native_exit_releases_created_listener(
    exit_kind: str,
    listener_kind: str,
    unix_socket_dir: Path,
) -> None:
    """Nothing before native adoption may consume a created listener lease."""
    from h2corn import Server

    if listener_kind == 'unix' and sys.platform == 'win32':
        pytest.skip('unix sockets are not supported')
    entered = asyncio.Event()
    socket_path = unix_socket_dir / f'{exit_kind}.sock'
    port: int | None = None
    if listener_kind == 'tcp':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        bind = (f'127.0.0.1:{port}',)
    else:
        bind = (f'unix:{socket_path}',)

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        entered.set()
        if exit_kind == 'failed':
            await send({'type': 'lifespan.startup.failed', 'message': 'planned'})
            return
        if exit_kind == 'raise':
            raise RuntimeError('planned startup failure')
        await asyncio.Future()

    server = Server(
        app,
        Config(
            bind=bind,
            lifespan='on',
            timeout_lifespan_startup=0.02 if exit_kind == 'timeout' else 60,
        ),
    )
    fd_baseline = len(os.listdir('/proc/self/fd')) if sys.platform == 'linux' else None
    serving = asyncio.create_task(server.serve())
    if exit_kind in {'cancel', 'shutdown'}:
        await asyncio.wait_for(entered.wait(), timeout=2)
        if exit_kind == 'cancel':
            serving.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(serving, timeout=2)
        else:
            server.shutdown()
            await asyncio.wait_for(serving, timeout=2)
    elif exit_kind == 'timeout':
        with pytest.raises(RuntimeError, match='lifespan startup timed out'):
            await asyncio.wait_for(serving, timeout=2)
    else:
        with pytest.raises(RuntimeError, match='lifespan startup'):
            await asyncio.wait_for(serving, timeout=2)

    assert server.addresses == ()
    if fd_baseline is not None:
        assert len(os.listdir('/proc/self/fd')) == fd_baseline
    if listener_kind == 'tcp':
        assert port is not None
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as rebound:
            rebound.bind(('127.0.0.1', port))
    else:
        assert not socket_path.exists()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as rebound:
            rebound.bind(str(socket_path))
        socket_path.unlink()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_pre_native_exit_preserves_borrowed_fd() -> None:
    """A failed embedded lifespan releases only h2corn's borrowed-fd duplicate."""
    from h2corn import Server

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)

        async def app(scope, receive, send):
            assert scope['type'] == 'lifespan'
            assert (await receive())['type'] == 'lifespan.startup'
            await send({'type': 'lifespan.startup.failed', 'message': 'planned'})

        server = Server(
            app,
            Config(bind=(f'fd://{listener.fileno()}',), lifespan='on'),
        )
        with pytest.raises(RuntimeError, match='planned'):
            await asyncio.wait_for(server.serve(), timeout=2)
        os.fstat(listener.fileno())
        assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)


def test_env_file_is_read_before_privileges_drop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    An env file holds a deployment's secrets and is routinely readable only
    by the starting user, so the supervisor resolves it before forking. This
    seals the file at the moment of the drop: a worker that still sees the
    value was handed it, not left to reopen a file it can no longer read.
    """
    from dataclasses import replace

    from h2corn import _server
    from h2corn._cli import ImportSettings

    env_file = tmp_path / 'secrets.env'
    env_file.write_text('H2CORN_DEMO_SECRET=from-file\n')
    (tmp_path / 'envdemo.py').write_text(
        'import os\n'
        'async def app(scope, receive, send):\n'
        '    return None\n'
        "app.loaded = os.environ.get('H2CORN_DEMO_SECRET')\n"
    )
    monkeypatch.delenv('H2CORN_DEMO_SECRET', raising=False)

    settings = ImportSettings(target='envdemo:app', app_dir=tmp_path, env_file=env_file)
    # What serve_with_supervisor does before it forks.
    _server.load_env_file(env_file)
    worker_settings = replace(settings, env_file=None)

    env_file.chmod(0o000)
    try:
        app = _server.import_target(worker_settings)
    finally:
        env_file.chmod(0o600)

    assert vars(app)['loaded'] == 'from-file'


def test_crash_loop_is_detected_after_a_worker_was_once_healthy() -> None:
    """
    The gate asks whether anything is serving now, not whether anything ever
    did. A lifetime latch meant one healthy worker at any point disabled
    crash-loop termination for good, so a deployment that came up and later
    broke respawned forever in silence.
    """
    from h2corn import _supervisor

    supervisor = _supervisor_state(Config(workers=1))
    # A worker reached READY, then went away.
    supervisor.workers[11] = _supervisor._Worker(
        _FakeWorkerProcess(11), -1, None, ready=True
    )
    supervisor.workers.pop(11)

    for _ in range(3):
        supervisor.record_worker_failure()

    assert supervisor.stopping
    assert supervisor.fatal_error == (
        'Stopped: 3 workers exited without ever becoming ready '
        '(last exit code unknown). The worker error is logged above.'
    )


def test_crash_loop_spares_a_fleet_that_still_has_a_healthy_worker() -> None:
    """One flapping worker must not take down workers that are serving."""
    supervisor = _supervisor_state(Config(workers=4))
    from h2corn import _supervisor

    supervisor.workers[21] = _supervisor._Worker(
        _FakeWorkerProcess(21), -1, None, ready=True
    )

    for _ in range(24):
        supervisor.record_worker_failure()

    assert not supervisor.stopping
    assert supervisor.fatal_error is None


def test_failed_wakeup_fd_installation_closes_its_pipe() -> None:
    """
    `set_wakeup_fd` is documented to raise off the main thread. The pipe was
    made before that call, so it belongs to us either way and must not be
    left behind for a caller that never received it.
    """
    import threading

    from h2corn import _socket

    leaked: list[int] = []

    def probe() -> None:
        before = len(os.listdir('/proc/self/fd'))
        for _ in range(5):
            with pytest.raises(ValueError), _socket.signal_wakeup_pipe():
                pass
        leaked.append(len(os.listdir('/proc/self/fd')) - before)

    thread = threading.Thread(target=probe)
    thread.start()
    thread.join()

    assert leaked == [0]


def test_interrupted_listener_acquisition_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
    unix_socket_dir: Path,
) -> None:
    """
    A KeyboardInterrupt between two binds is exactly when a half-built set of
    listeners — and any unix socket path already created — must be rolled
    back; catching only `Exception` left the path behind.
    """
    from h2corn import _socket

    first = unix_socket_dir / 'first.sock'
    second = unix_socket_dir / 'second.sock'
    real_build = _socket._build_unix_listener

    def build_then_interrupt(path, config, **kwargs):
        if os.fspath(path) == os.fspath(second):
            raise KeyboardInterrupt
        return real_build(path, config, **kwargs)

    monkeypatch.setattr(_socket, '_build_unix_listener', build_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        _socket._build_sockets(Config(bind=(f'unix:{first}', f'unix:{second}')))

    assert not first.exists(), 'the first socket path survived the interrupt'


@pytest.mark.parametrize('kind', ['tcp', 'unix', 'borrowed'])
def test_interrupted_listener_acquisition_releases_every_taken_listener(
    kind: str,
    unix_socket_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An interruption on a later bind releases every lease already taken."""
    from h2corn import _socket

    original_listener: socket.socket | None = None
    later_borrowed_fd: int | None = None
    rebound_port: int | None = None
    socket_path = unix_socket_dir / 'interrupted.sock'
    if kind == 'tcp':
        config = Config(bind=('127.0.0.1:0', '127.0.0.2:0'))
        builder_name = '_build_tcp_listener'
    elif kind == 'unix':
        config = Config(
            bind=(f'unix:{socket_path}', f'unix:{unix_socket_dir / "later.sock"}')
        )
        builder_name = '_build_unix_listener'
    else:
        original_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        original_listener.bind(('127.0.0.1', 0))
        original_listener.listen(1)
        later_borrowed_fd = os.dup(original_listener.fileno())
        config = Config(
            bind=(
                f'fd://{original_listener.fileno()}',
                f'fd://{later_borrowed_fd}',
            )
        )
        builder_name = '_adopt_listener'

    leases: list[Any] = []
    real_builder = getattr(_socket, builder_name)

    def build_once_then_interrupt(*args, **kwargs):
        nonlocal rebound_port
        if leases:
            raise KeyboardInterrupt
        lease = real_builder(*args, **kwargs)
        if kind == 'tcp':
            assert lease.socket is not None
            rebound_port = lease.socket.getsockname()[1]
        leases.append(lease)
        return lease

    monkeypatch.setattr(_socket, builder_name, build_once_then_interrupt)
    with pytest.raises(KeyboardInterrupt):
        _socket._build_sockets(config)

    assert len(leases) == 1
    assert leases[0].socket is None

    if kind == 'tcp':
        assert rebound_port is not None
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as rebound:
            rebound.bind(('127.0.0.1', rebound_port))
    elif kind == 'unix':
        assert not socket_path.exists()
    else:
        assert original_listener is not None
        try:
            os.fstat(original_listener.fileno())
            assert original_listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)
        finally:
            original_listener.close()
            assert later_borrowed_fd is not None
            os.close(later_borrowed_fd)


def test_multibind_interruption_rolls_back_acquired_listeners_in_reverse(
    unix_socket_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An interrupted third bind releases the two acquired listeners in reverse."""
    from h2corn import _socket

    labels: dict[int, str] = {}
    released: list[str] = []
    builds = 0
    real_build = _socket._build_unix_listener
    real_release = _socket._CreatedListener.release

    def build(*args, **kwargs):
        nonlocal builds
        if builds == 2:
            raise KeyboardInterrupt
        lease = real_build(*args, **kwargs)
        builds += 1
        labels[id(lease)] = f'listener-{builds}'
        return lease

    def release(lease):
        released.append(labels[id(lease)])
        real_release(lease)

    first = unix_socket_dir / 'first.sock'
    second = unix_socket_dir / 'second.sock'
    third = unix_socket_dir / 'third.sock'
    monkeypatch.setattr(_socket, '_build_unix_listener', build)
    monkeypatch.setattr(_socket._CreatedListener, 'release', release)
    with pytest.raises(KeyboardInterrupt):
        _socket._build_sockets(
            Config(bind=(f'unix:{first}', f'unix:{second}', f'unix:{third}'))
        )

    assert released == ['listener-2', 'listener-1']
    assert not first.exists()
    assert not second.exists()
    assert not third.exists()


def test_rollback_detaches_borrowed_descriptors_it_never_owned() -> None:
    """
    A descriptor that arrived through `fd://` belongs to whoever handed it
    over. Rolling back a partial build must detach it, not close it: closing
    destroys a caller's live listener over a failure elsewhere in the list,
    and frees the descriptor number for something else entirely.
    """
    from h2corn import _socket

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    not_listening = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with listener, not_listening:
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)

        with pytest.raises(ValueError, match='is not listening'):
            _socket._build_sockets(
                Config(
                    bind=(
                        f'fd://{listener.fileno()}',
                        f'fd://{not_listening.fileno()}',
                    )
                )
            )

        # Both are still the caller's, and the good one still accepts.
        os.fstat(listener.fileno())
        os.fstat(not_listening.fileno())
        assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX privilege drop')
def test_supervisor_prepares_tls_before_workers_with_mtls_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Supervisor privilege path loads certfile/keyfile/ca_certs once, prepares
    the acceptor, and hands the same prepared object to every worker.
    """
    import trustme
    from h2corn import _supervisor
    from h2corn._cli import ImportSettings

    ca = trustme.CA()
    server = ca.issue_cert('localhost')
    ca_cert = tmp_path / 'ca.crt'
    certfile = tmp_path / 'server.crt'
    keyfile = tmp_path / 'server.key'
    ca.cert_pem.write_to_path(str(ca_cert))
    server.cert_chain_pems[0].write_to_path(str(certfile))
    server.private_key_pem.write_to_path(str(keyfile))

    order: list[str] = []
    prepared_seen: list[object] = []

    from h2corn._lib import prepare_tls as real_prepare

    def tracking_prepare(config, tls_material=None):
        order.append('prepare')
        # Files still readable at prepare time.
        assert certfile.is_file() and keyfile.is_file() and ca_cert.is_file()
        assert tls_material is not None
        assert tls_material.client_ca is not None
        result = real_prepare(config, tls_material)
        prepared_seen.append(result)
        return result

    def fake_bound_sockets(config, socket_owner=None):
        import socket as sockmod
        from contextlib import contextmanager

        from h2corn._socket import _CreatedListener

        @contextmanager
        def _cm():
            s = sockmod.socket()
            s.bind(('127.0.0.1', 0))
            s.listen()
            lease = _CreatedListener(socket=s, path=None)
            try:
                yield (lease,)
            finally:
                lease.release()

        return _cm()

    class FakeSupervisor:
        def __init__(self, **kwargs):
            order.append('supervisor')
            prepared_seen.append(kwargs['prepared_tls'])
            self.fatal_error = None
            # Seal the files as if privileges dropped after prepare.
            certfile.chmod(0o000)
            keyfile.chmod(0o000)
            ca_cert.chmod(0o000)

        def run(self):
            order.append('run')

    monkeypatch.setattr(_supervisor, 'bound_sockets', fake_bound_sockets)
    monkeypatch.setattr(
        'h2corn._lib.prepare_tls',
        tracking_prepare,
    )
    # prepare_tls is imported inside serve_with_supervisor from ._lib
    from h2corn import _lib

    monkeypatch.setattr(_lib, 'prepare_tls', tracking_prepare)
    monkeypatch.setattr(_supervisor, '_Supervisor', FakeSupervisor)

    config = Config(
        workers=2,
        bind=('127.0.0.1:0',),
        certfile=certfile,
        keyfile=keyfile,
        ca_certs=ca_cert,
        cert_reqs='required',
    )
    try:
        _supervisor.serve_with_supervisor(
            ImportSettings(target='example:app'),
            config,
        )
    finally:
        certfile.chmod(0o644)
        keyfile.chmod(0o600)
        ca_cert.chmod(0o644)

    assert order == ['prepare', 'supervisor', 'run']
    assert len(prepared_seen) == 2
    assert prepared_seen[0] is prepared_seen[1]


def test_successful_build_then_exception_releases_created_keeps_borrowed(
    unix_socket_dir: Path,
) -> None:
    """Partial success then failure: created sockets/paths gone; borrowed live."""
    from h2corn import _socket

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        borrowed_fd = listener.fileno()
        uds = unix_socket_dir / 'created.sock'
        # Build succeeds fully first, then a later exception rolls nothing
        # back through _build_sockets — release is the caller's job via leases.
        leases = _socket._build_sockets(
            Config(bind=(f'fd://{borrowed_fd}', f'unix:{uds}'))
        )
        assert len(leases) == 2
        assert isinstance(leases[0], _socket._BorrowedListener)
        assert isinstance(leases[1], _socket._CreatedListener)
        assert uds.exists()
        for lease in reversed(leases):
            lease.release()
        # Borrowed: detached, still open and listening.
        os.fstat(borrowed_fd)
        assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)
        # Created: socket closed and path unlinked.
        assert not uds.exists()


def test_tls_rejection_of_borrowed_unix_fd_detaches(
    tmp_path: Path,
) -> None:
    """TLS-on-Unix refusal must detach the caller's fd, never close it."""
    from h2corn import _socket

    uds_path = tmp_path / 'tls-reject.sock'
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with listener:
        listener.bind(str(uds_path))
        listener.listen(1)
        fd = listener.fileno()
        with pytest.raises(OSError, match='TLS is supported only on TCP'):
            _socket._build_sockets(
                Config(
                    bind=(f'fd://{fd}',),
                    certfile=tmp_path / 'cert.pem',
                    keyfile=tmp_path / 'key.pem',
                )
            )
        os.fstat(fd)
        assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)


@pytest.mark.skipif(
    not hasattr(socket, 'SOCK_SEQPACKET') or sys.platform == 'win32',
    reason='SOCK_SEQPACKET not available',
)
def test_seqpacket_listener_is_rejected_and_remains_caller_owned() -> None:
    """Listening SOCK_SEQPACKET is not a stream socket and stays open."""
    from h2corn import _socket

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    with listener:
        # Some platforms need a path; bind an abstract name on Linux.
        listener.bind(f'\0h2corn-seqpacket-{os.getpid()}')
        listener.listen(1)
        fd = listener.fileno()
        with pytest.raises(ValueError, match='is not a stream socket'):
            _socket._build_sockets(Config(bind=(f'fd://{fd}',)))
        os.fstat(fd)


def test_transfer_consumes_socket_second_transfer_impossible() -> None:
    from h2corn import _socket

    leases = _socket._build_sockets(Config(bind=('127.0.0.1:0',)))
    try:
        lease = leases[0]
        fd = lease.transfer()
        assert lease.socket is None
        with pytest.raises(RuntimeError, match='already transferred'):
            lease.transfer()
        # Released after transfer only drops the path claim; fd stays open
        # until the native owner closes it.
        lease.release()
        os.fstat(fd)
        os.close(fd)
    finally:
        for remaining in leases:
            remaining.release()


def test_borrowed_release_detaches_never_closes() -> None:
    from h2corn import _socket

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        fd = listener.fileno()
        leases = _socket._build_sockets(Config(bind=(f'fd://{fd}',)))
        leases[0].release()
        os.fstat(fd)
        # A killing mutation would close() here and make fstat fail.


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_child_discard_fds_lists_explicit_provenance_set() -> None:
    """Workers close only the tagged discard set — never a broad scan."""
    from h2corn import _supervisor
    from h2corn._lib import prepare_tls
    from h2corn._server import ProcessIdentity
    from h2corn._socket import _SignalWakeupPipe

    async def _unused_app(*_args: object):
        return None

    supervisor = _supervisor._Supervisor(
        app=_unused_app,  # type: ignore[arg-type]
        config=Config(workers=2),
        fds=(),
        identity=ProcessIdentity(),
        prepared_tls=prepare_tls(Config()),
        pid_fd=42,
        parent_liveness_fd=43,
    )

    class _FakePopen:
        def __init__(self) -> None:
            self.finalizer = SimpleNamespace(_args=(100, 101))

    class _FakeWorker:
        def __init__(self) -> None:
            self.sentinel = 100
            self._popen = _FakePopen()

    supervisor.workers[100] = _supervisor._Worker(cast('Any', _FakeWorker()), 10, 11)
    supervisor.signal_wakeup = _SignalWakeupPipe(read_fd=20, write_fd=21)

    discard = supervisor._child_discard_fds(control_read_fd=30, quiesce_write_fd=31)
    assert set(discard) == {100, 101, 10, 11, 30, 31, 20, 21, 42, 43}


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_supervisor_uses_poll_selector() -> None:
    """PollSelector has no epoll/kqueue object to leak across fork."""
    import selectors

    from h2corn import _supervisor
    from h2corn._lib import prepare_tls
    from h2corn._server import ProcessIdentity

    async def _unused_app(*_args: object):
        return None

    supervisor = _supervisor._Supervisor(
        app=_unused_app,  # type: ignore[arg-type]
        config=Config(),
        fds=(),
        identity=ProcessIdentity(),
        prepared_tls=prepare_tls(Config()),
    )
    assert isinstance(supervisor.selector, selectors.PollSelector)


def test_pidfile_yields_open_fd(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'
    with _server._pidfile(Config(pid=pid_path)) as pid_fd:
        assert pid_fd is not None
        assert os.fstat(pid_fd).st_size > 0
    with _server._pidfile(Config()) as pid_fd:
        assert pid_fd is None


def test_take_reload_parent_liveness_fd_pops_env(monkeypatch) -> None:
    from h2corn import _reload

    monkeypatch.setenv(_reload._RELOAD_LIVENESS_ENV, '17')
    assert _reload.take_reload_parent_liveness_fd() == 17
    assert _reload._RELOAD_LIVENESS_ENV not in os.environ
    assert _reload.take_reload_parent_liveness_fd() is None


@pytest.mark.skipif(sys.platform != 'linux', reason='uses /proc fd inspection')
def test_four_worker_children_drop_supervisor_provenance_fds(
    tmp_path: Path,
) -> None:
    """Workers retain only stdio, listeners, control-write, and quiesce-read."""
    import signal

    worker_ready_dir = tmp_path / 'worker-ready'
    worker_ready_dir.mkdir()
    app = tmp_path / 'app.py'
    app.write_text(
        'import os\n'
        'from pathlib import Path\n'
        'async def app(scope, receive, send):\n'
        "    if scope['type'] == 'lifespan':\n"
        '        message = await receive()\n'
        "        if message['type'] == 'lifespan.startup':\n"
        f'            Path({str(worker_ready_dir)!r}, str(os.getpid())).touch()\n'
        "            await send({'type': 'lifespan.startup.complete'})\n"
        '            message = await receive()\n'
        "        if message['type'] == 'lifespan.shutdown':\n"
        "            await send({'type': 'lifespan.shutdown.complete'})\n"
        '        return\n'
        "    if scope['type'] == 'http':\n"
        "        await send({'type': 'http.response.start', 'status': 204, "
        "'headers': []})\n"
        "        await send({'type': 'http.response.body', 'body': b''})\n"
    )
    pidfile = tmp_path / 'server.pid'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    env = os.environ.copy()
    env['PYTHONPATH'] = (
        f'{tmp_path}:{env["PYTHONPATH"]}' if env.get('PYTHONPATH') else str(tmp_path)
    )
    process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'h2corn',
            'app:app',
            '--app-dir',
            str(tmp_path),
            '--host',
            '127.0.0.1',
            '--port',
            str(port),
            '--workers',
            '4',
            '--pid',
            str(pidfile),
            '--no-access-log',
            '--timeout-worker-healthcheck',
            '0',
            '--timeout-graceful-shutdown',
            '0.5',
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 10.0
        workers: list[int] = []
        worker_ready_pids: set[int] = set()
        while time.monotonic() < deadline:
            try:
                raw = Path(
                    f'/proc/{process.pid}/task/{process.pid}/children'
                ).read_text()
            except OSError:
                raw = ''
            workers = [int(item) for item in raw.split()]
            worker_ready_pids = {int(path.name) for path in worker_ready_dir.iterdir()}
            if (
                len(workers) == 4
                and set(workers) == worker_ready_pids
                and pidfile.exists()
            ):
                break
            time.sleep(0.05)
        assert len(workers) == 4, f'expected 4 workers, got {workers}'
        assert worker_ready_pids == set(workers)

        def fd_targets(pid: int) -> dict[int, str]:
            result: dict[int, str] = {}
            for entry in Path(f'/proc/{pid}/fd').iterdir():
                try:
                    result[int(entry.name)] = os.readlink(entry)
                except OSError:
                    pass
            return result

        parent_fds = fd_targets(process.pid)
        parent_pipes = {
            target for target in parent_fds.values() if target.startswith('pipe:')
        }
        parent_eventpoll = {
            target for target in parent_fds.values() if 'eventpoll' in target
        }
        # PollSelector must not leave an epoll object on the supervisor.
        # Workers create their own epoll for asyncio; that is expected.
        assert not parent_eventpoll

        shared_counts: list[int] = []
        for worker in workers:
            targets = fd_targets(worker)
            pidfile_fds = [
                fd for fd, target in targets.items() if target == str(pidfile)
            ]
            assert pidfile_fds == [], f'worker {worker} held pidfile: {pidfile_fds}'
            shared_with_parent = sum(
                1 for target in targets.values() if target in parent_pipes
            )
            shared_counts.append(shared_with_parent)
        # Multiprocessing keeps a few process-management pipes in every child.
        # What must not grow is *prior* worker sentinels and parent control
        # ends: later workers would share more parent pipe inodes than the
        # first if those were left open. Equality across the set is the kill.
        assert len(set(shared_counts)) == 1, (
            f'workers held unequal parent-pipe sets {dict(zip(workers, shared_counts, strict=True))}; '
            f'later workers leaking prior sentinels/control ends'
        )
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)
        if process.stderr is not None:
            process.stderr.close()
