import asyncio
import ctypes
import gc
import os
import re
import signal
import socket
import sys
import textwrap
import threading
import weakref
from pathlib import Path

import pytest
from h2corn import Config, Server

from tests._support import (
    assert_serve_reusable,
    find_free_port,
    h2_request,
    http1_request,
    running_server,
    wait_for_port,
    wait_for_server,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        sys.platform == 'win32',
        reason='POSIX worker supervisor (fork workers, signals, unix sockets)',
    ),
]


async def test_repeated_embedded_serve_releases_app_and_doorbell_fds() -> None:
    class App:
        async def __call__(self, scope, receive, send):
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})

    async def run_once() -> weakref.ReferenceType[App]:
        app = App()
        app_ref = weakref.ref(app)
        server = Server(
            app,
            Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
        )
        task = asyncio.create_task(server.serve())
        await wait_for_server(server, task)
        server.shutdown()
        await asyncio.wait_for(task, timeout=2)
        return app_ref

    # Warm the process-global Tokio runtime before measuring per-serve state.
    warm_ref = await run_once()
    gc.collect()
    assert warm_ref() is None

    fd_baseline = len(os.listdir('/proc/self/fd')) if sys.platform == 'linux' else None
    refs = [await run_once() for _ in range(6)]
    await asyncio.sleep(0)
    gc.collect()

    assert all(ref() is None for ref in refs)
    if fd_baseline is not None:
        assert len(os.listdir('/proc/self/fd')) == fd_baseline


async def test_same_server_can_serve_twice_with_fresh_shutdown_state() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    for _ in range(2):
        task = asyncio.create_task(server.serve())
        await wait_for_server(server, task)
        await asyncio.to_thread(server.shutdown)
        await asyncio.wait_for(task, timeout=2)
        assert server.addresses == ()


async def test_shutdown_during_lifespan_startup_is_not_lost() -> None:
    startup_entered = asyncio.Event()
    finish_startup = asyncio.Event()

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        startup_entered.set()
        await finish_startup.wait()
        await send({'type': 'lifespan.startup.complete'})
        assert (await receive())['type'] == 'lifespan.shutdown'
        await send({'type': 'lifespan.shutdown.complete'})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    task = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    assert server.addresses

    await asyncio.to_thread(server.shutdown)
    finish_startup.set()

    await asyncio.wait_for(task, timeout=2)
    assert server.addresses == ()


async def test_repeated_cancellation_waits_for_primary_lifespan_before_reuse() -> None:
    startup_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    generation = 0
    active_lifespans = 0
    max_active_lifespans = 0

    async def app(scope, receive, send):
        nonlocal active_lifespans, generation, max_active_lifespans
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        generation += 1
        active_lifespans += 1
        max_active_lifespans = max(max_active_lifespans, active_lifespans)
        try:
            if generation == 1:
                startup_entered.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    cleanup_started.set()
                    while not release_cleanup.is_set():
                        try:
                            await release_cleanup.wait()
                        except asyncio.CancelledError:
                            continue
                    cleanup_finished.set()
                    raise

            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
        finally:
            active_lifespans -= 1

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    first = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    first.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)
    first.cancel()
    await asyncio.sleep(0)
    assert not first.done(), 'repeated cancellation must retain lifespan ownership'
    with pytest.raises(RuntimeError, match='active serve'):
        await server.serve()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, timeout=2)

    assert cleanup_finished.is_set()
    assert active_lifespans == 0
    assert server.addresses == ()

    await assert_serve_reusable(server)
    assert generation == 2
    assert max_active_lifespans == 1


async def test_cancelling_serve_waits_for_native_graceful_drain_before_reuse() -> None:
    request_started = asyncio.Event()
    release_request = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan is disabled')
        request_started.set()
        await release_request.wait()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'drained'})

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=1,
        ),
    )
    first = asyncio.create_task(server.serve())
    await wait_for_server(server, first)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    first.cancel()
    await asyncio.sleep(0.05)
    assert not first.done(), 'native serving ownership must drain before reuse'

    release_request.set()
    status, _headers, body, _trailers = await asyncio.wait_for(request, timeout=2)
    assert (status, body) == (200, b'drained')
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, timeout=2)
    assert server.addresses == ()

    await assert_serve_reusable(server)


async def test_shutdown_global_deadline_cancels_never_finishing_http1_app() -> None:
    request_started = asyncio.Event()
    request_cancelled = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan is disabled')
        request_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            request_cancelled.set()

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=0.2,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    loop = asyncio.get_running_loop()
    started = loop.time()
    server.shutdown()
    await asyncio.wait_for(serving, timeout=1)

    assert loop.time() - started < 0.8
    assert request_cancelled.is_set()
    await asyncio.gather(request, return_exceptions=True)
    assert server.addresses == ()

    await assert_serve_reusable(server)


async def test_shutdown_waits_for_cancelled_request_cleanup_before_reuse() -> None:
    request_started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_done = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan is disabled')
        request_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release_cleanup.wait()
            cleanup_done.set()
            raise

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=0.05,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    server.shutdown()
    await asyncio.wait_for(cancellation_seen.wait(), timeout=2)
    assert not serving.done(), 'serve must retain cancelled Python task ownership'
    with pytest.raises(RuntimeError, match='already has an active serve'):
        await server.serve()

    release_cleanup.set()
    await asyncio.wait_for(serving, timeout=2)
    assert cleanup_done.is_set()
    await asyncio.gather(request, return_exceptions=True)

    await assert_serve_reusable(server)


async def test_repeated_cancellation_cannot_interrupt_native_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lib

    native_started = asyncio.Event()
    shutdown_received = asyncio.Event()
    release_drain = asyncio.Event()

    async def fake_serve_fds(
        _app,
        fds,
        _config,
        shutdown_trigger,
        *_args,
    ) -> None:
        native_started.set()
        try:
            assert await shutdown_trigger == 'stop'
            shutdown_received.set()
            await release_drain.wait()
        finally:
            for fd in fds:
                os.close(fd)

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)

    async def app(scope, receive, send):
        raise AssertionError('the fake native server does not dispatch requests')

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    task = asyncio.create_task(server.serve())
    await asyncio.wait_for(native_started.wait(), timeout=2)

    task.cancel()
    await asyncio.wait_for(shutdown_received.wait(), timeout=2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done(), 'repeated cancellation must remain shielded by native drain'

    release_drain.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)
    assert server.addresses == ()


async def test_native_cancelled_error_propagates_without_shutdown_spin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lib

    calls = 0

    async def fake_serve_fds(
        _app,
        fds,
        _config,
        shutdown_trigger,
        *_args,
    ) -> None:
        nonlocal calls
        calls += 1
        try:
            if calls == 1:
                raise asyncio.CancelledError
            assert await shutdown_trigger == 'stop'
        finally:
            for fd in fds:
                os.close(fd)

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)

    async def app(scope, receive, send):
        raise AssertionError('the fake native server does not dispatch requests')

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(server.serve(), timeout=2)
    assert calls == 1
    assert server.addresses == ()

    await assert_serve_reusable(server)
    assert calls == 2


async def test_synchronous_native_setup_failure_clears_state_before_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lib

    calls = 0
    published_addresses: list[tuple[str, ...]] = []

    def fake_serve_fds(
        _app,
        fds,
        _config,
        shutdown_trigger,
        *_args,
    ):
        nonlocal calls
        calls += 1
        published_addresses.append(server.addresses)
        if calls == 1:
            for fd in fds:
                os.close(fd)
            raise RuntimeError('synchronous native setup failed')

        async def run() -> None:
            try:
                assert await shutdown_trigger == 'stop'
            finally:
                for fd in fds:
                    os.close(fd)

        return run()

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)

    async def app(scope, receive, send):
        raise AssertionError('the fake native server does not dispatch requests')

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    with pytest.raises(RuntimeError, match='synchronous native setup failed'):
        await server.serve()
    assert published_addresses[0]
    assert server.addresses == ()

    await assert_serve_reusable(server)
    assert published_addresses[1]
    assert server.addresses == ()


async def test_concurrent_cross_thread_serve_has_exactly_one_winner() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    start = threading.Barrier(3)
    rejected = threading.Event()

    def run() -> str:
        start.wait()
        try:
            asyncio.run(server.serve())
        except RuntimeError:
            rejected.set()
            return 'rejected'
        return 'served'

    first = asyncio.create_task(asyncio.to_thread(run))
    second = asyncio.create_task(asyncio.to_thread(run))

    async def collect_outcomes() -> list[str]:
        return list(await asyncio.gather(first, second))

    outcomes = asyncio.create_task(collect_outcomes())
    await asyncio.to_thread(start.wait)
    try:
        await wait_for_server(server, outcomes, timeout=2)
        assert await asyncio.to_thread(rejected.wait, 2)
    finally:
        await asyncio.to_thread(server.shutdown)

    assert sorted(await asyncio.wait_for(outcomes, 2)) == [
        'rejected',
        'served',
    ]


async def test_server_rejects_concurrent_serve_calls() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    first = asyncio.create_task(server.serve())
    await wait_for_server(server, first)
    try:
        with pytest.raises(RuntimeError, match='active serve'):
            await server.serve()
    finally:
        server.shutdown()
        await asyncio.wait_for(first, timeout=2)


async def test_serve_fds_count_mismatch_closes_unadopted_handles() -> None:
    from h2corn._lib import serve_fds

    async def app(scope, receive, send):
        raise AssertionError('listener adoption must fail before app dispatch')

    async def attempt(raw_fds: list[int]) -> None:
        async def wait_for_shutdown() -> str:
            await asyncio.sleep(60)
            return 'stop'

        shutdown = asyncio.create_task(wait_for_shutdown())
        config = Config(
            bind=('127.0.0.1:1', '127.0.0.2:1'),
            access_log=False,
            lifespan='off',
        )
        quiesce_read_fd, quiesce_write_fd = os.pipe()
        try:
            with pytest.raises((OSError, RuntimeError)):
                await serve_fds(
                    app,
                    raw_fds,
                    config,
                    shutdown,
                    None,
                    None,
                    None,
                    quiesce_read_fd,
                )
        finally:
            os.close(quiesce_write_fd)
            shutdown.cancel()
            with pytest.raises(asyncio.CancelledError):
                await shutdown
        for fd in raw_fds:
            with pytest.raises(OSError):
                os.fstat(fd)
        with pytest.raises(OSError):
            os.fstat(quiesce_read_fd)

    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.setblocking(False)
    await attempt([listener.detach()])


@pytest.mark.parametrize(
    ('listener_fds', 'quiesce_fd'),
    [([-1], None), ([2**40], None), ([7, 7], None), ([7], 7)],
)
async def test_serve_fds_rejects_unsafe_descriptor_ownership(
    listener_fds: list[int],
    quiesce_fd: int | None,
) -> None:
    from h2corn._lib import serve_fds

    async def app(scope, receive, send):
        raise AssertionError('descriptor validation precedes app dispatch')

    shutdown = asyncio.get_running_loop().create_future()
    config = Config(
        bind=tuple(f'fd://{index}' for index in range(len(listener_fds))),
        lifespan='off',
    )
    with pytest.raises(ValueError):
        serve_fds(
            app,
            listener_fds,
            config,
            shutdown,
            None,
            None,
            None,
            quiesce_fd,
        )


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    async def _wait(timeout: float) -> bool:
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except TimeoutError:
            return process.returncode is not None
        return True

    def _signal(sig: int) -> None:
        if sys.platform != 'win32':
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                return
            except OSError:
                pass
            else:
                return
        process.send_signal(sig)

    if process.returncode is not None:
        await _wait(5)
        return

    _signal(signal.SIGTERM)
    if await _wait(5):
        return

    _signal(signal.SIGKILL)
    await _wait(5)


async def _wait_for_h2_success(
    *,
    port: int,
    body: bytes,
    timeout: float = 5.0,
) -> None:
    await _wait_for_h2_body(port=port, body=body, timeout=timeout)


async def _wait_for_h2_body(
    *,
    port: int,
    body: bytes,
    timeout: float = 5.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            status, response_body = await h2_request(port=port)
        except Exception:
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.05)
            continue
        if status == 200 and response_body == body:
            return
        if loop.time() >= deadline:
            raise AssertionError(
                f'timed out waiting for body {body!r}, got status={status} body={response_body!r}'
            )
        await asyncio.sleep(0.05)


async def _wait_for_h2_body_any(
    *, port: int, timeout: float = 5.0
) -> tuple[int, bytes]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            return await h2_request(port=port)
        except Exception:
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.05)


async def _wait_for_listening_port(
    process: asyncio.subprocess.Process,
    *,
    timeout: float = 5.0,
) -> int:
    assert process.stderr is not None
    stderr = process.stderr

    async def _read_port() -> int:
        while True:
            line = await stderr.readline()
            if not line:
                raise AssertionError('server exited before printing listening banner')
            match = re.search(rb'Listening on http://127\.0\.0\.1:(\d+)', line)
            if match is not None:
                return int(match.group(1))

    return await asyncio.wait_for(_read_port(), timeout=timeout)


async def _collect_lines(
    stream: asyncio.StreamReader | None,
    lines: list[bytes],
) -> None:
    if stream is None:
        return
    while line := await stream.readline():
        lines.append(line)


async def _spawn_server_process(
    *,
    tmp_path: Path,
    module_name: str,
    module_source: str,
    workers: int,
    port: int | None = None,
    extra_args: list[str] | None = None,
    stderr=None,
) -> tuple[asyncio.subprocess.Process, int]:
    port = find_free_port() if port is None else port
    module_path = tmp_path / f'{module_name}.py'
    module_path.write_text(textwrap.dedent(module_source).strip() + '\n')

    env = os.environ.copy()
    pythonpath = env.get('PYTHONPATH')
    env['PYTHONPATH'] = f'{tmp_path}:{pythonpath}' if pythonpath else str(tmp_path)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'h2corn._server',
        f'{module_name}:app',
        '--workers',
        str(workers),
        '--port',
        str(port),
        *(extra_args or []),
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL if stderr is None else stderr,
        start_new_session=sys.platform != 'win32',
    )
    return process, port


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux prctl parent-death signal')
async def test_parent_death_signal_allows_pid_one_supervisor(monkeypatch) -> None:
    from h2corn import _supervisor

    class FakeLibc:
        def prctl(self, *_args):
            return 0

    monkeypatch.setattr(
        ctypes,
        'CDLL',
        lambda *_args, **_kwargs: FakeLibc(),
    )
    monkeypatch.setattr(_supervisor.os, 'getppid', lambda: 1)

    def fail_exit(code: int):
        raise AssertionError(f'unexpected os._exit({code})')

    monkeypatch.setattr(_supervisor.os, '_exit', fail_exit)

    _supervisor._install_parent_death_signal(1)


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux prctl parent-death signal')
async def test_parent_death_signal_exits_when_expected_parent_died_before_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    class FakeLibc:
        def prctl(self, *_args):
            return 0

    exit_codes: list[int] = []

    def fake_exit(code: int):
        exit_codes.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(
        ctypes,
        'CDLL',
        lambda *_args, **_kwargs: FakeLibc(),
    )
    monkeypatch.setattr(_supervisor.os, 'getppid', lambda: 1)
    monkeypatch.setattr(_supervisor.os, '_exit', fake_exit)

    with pytest.raises(SystemExit):
        _supervisor._install_parent_death_signal(1234)

    assert exit_codes == [0]


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux prctl parent-death signal')
async def test_parent_death_signal_fails_closed_when_prctl_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    class FakeLibc:
        def prctl(self, *_args):
            return -1

    monkeypatch.setattr(
        ctypes,
        'CDLL',
        lambda *_args, **_kwargs: FakeLibc(),
    )
    monkeypatch.setattr(ctypes, 'get_errno', lambda: 1)
    monkeypatch.setattr(_supervisor.os, 'getppid', lambda: 1234)

    with pytest.raises(OSError, match='failed to install parent-death signal'):
        _supervisor._install_parent_death_signal(1234)


async def _wait_for_pid_change(
    *, port: int, previous_pid: bytes, timeout: float = 5.0
) -> bytes:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=1)
        except Exception:
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.05)
            continue
        assert status == 200
        if body != previous_pid:
            return body
        if loop.time() >= deadline:
            raise AssertionError(f'worker pid did not change from {previous_pid!r}')
        await asyncio.sleep(0.05)


async def _wait_for_worker_count(
    *, supervisor_pid: int, count: int, timeout: float = 5.0
) -> list[int]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        workers = _worker_pids(supervisor_pid)
        if len(workers) == count:
            return workers
        if loop.time() >= deadline:
            raise AssertionError(
                f'timed out waiting for {count} workers; found {workers}'
            )
        await asyncio.sleep(0.02)


async def _wait_for_path(path: Path, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not path.exists():
        if loop.time() >= deadline:
            raise AssertionError(f'timed out waiting for {path}')
        await asyncio.sleep(0.01)


async def test_unix_socket_serving(unix_socket_dir: Path) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'uds'})

    socket_path = unix_socket_dir / 'h2corn.sock'
    config = Config(bind=(f'unix:{socket_path}',))
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(uds=socket_path), timeout=5)

    assert status == 200
    assert body == b'uds'


async def test_unix_socket_cleanup_removes_owned_socket_path(
    unix_socket_dir: Path,
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    socket_path = unix_socket_dir / 'cleanup.sock'
    config = Config(bind=(f'unix:{socket_path}',))
    async with running_server(app, config):
        assert socket_path.exists()

    assert not socket_path.exists()


async def test_unix_socket_umask_limits_created_mode(
    unix_socket_dir: Path,
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    socket_path = unix_socket_dir / 'umask.sock'
    config = Config(bind=(f'unix:{socket_path}',), umask=0o077)
    async with running_server(app, config):
        assert socket_path.stat().st_mode & 0o077 == 0

    assert not socket_path.exists()


async def test_unix_socket_path_rejects_non_socket_files(tmp_path: Path) -> None:
    path = tmp_path / 'not-a-socket'
    path.write_text('occupied')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(app, Config(bind=(f'unix:{path}',)))

    with pytest.raises(OSError, match='not a socket'):
        await server.serve()

    assert path.read_text() == 'occupied'


async def test_multi_bind_reports_actual_server_port_per_listener() -> None:
    async def app(scope, receive, send):
        scope_port = scope['server'][1]
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': str(scope_port).encode()})

    # Two listeners on one host need distinct ports; multiple port-0 binds
    # deliberately share one ephemeral port (for 0.0.0.0 + [::] pairs), so
    # this is one of the few in-process cases that must pre-allocate.
    ports = (find_free_port(), find_free_port())
    config = Config(bind=tuple(f'127.0.0.1:{port}' for port in ports))
    async with running_server(app, config):
        for port in ports:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
            assert status == 200
            assert body == str(port).encode()


async def test_server_serve_writes_and_cleans_up_pid_file(tmp_path: Path) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'pid'})

    pid_path = tmp_path / 'h2corn.pid'
    config = Config(pid=pid_path, port=find_free_port())
    async with running_server(app, config):
        assert pid_path.read_text() == f'{os.getpid()}\n'

    assert not pid_path.exists()


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_serves_requests(tmp_path: Path, workers: int) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='supervisor_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'supervisor'})
        """,
        workers=workers,
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'supervisor'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_serves_requests_with_current_user_and_group(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='supervisor_identity_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'supervisor-identity'})
        """,
        workers=1,
        extra_args=['--user', str(os.getuid()), '--group', str(os.getgid())],
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'supervisor-identity'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_banner_reports_kernel_allocated_port(
    tmp_path: Path,
) -> None:
    process, configured_port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='banner_port_zero_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'banner-port-zero'})
        """,
        workers=1,
        port=0,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        bound_port = await _wait_for_listening_port(process)
        assert configured_port == 0
        assert bound_port != 0
        await wait_for_port(bound_port)
        status, body = await asyncio.wait_for(h2_request(port=bound_port), timeout=5)
        assert status == 200
        assert body == b'banner-port-zero'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_writes_and_cleans_up_pid_file(
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / 'h2corn.pid'
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='pidfile_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'pidfile'})
        """,
        workers=1,
        extra_args=['--pid', str(pid_path)],
    )

    try:
        await wait_for_port(port)
        assert pid_path.read_text() == f'{process.pid}\n'
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'pidfile'
    finally:
        await _terminate_process(process)

    assert not pid_path.exists()


async def test_worker_supervisor_shutdown_is_not_blocked_by_limit_connections(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='limit_connections_shutdown_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'limit-connections'})
        """,
        workers=1,
        extra_args=['--limit-connections', '1'],
    )

    reader = writer = None
    try:
        await wait_for_port(port)
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        await asyncio.sleep(0.1)
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=5)
        assert process.returncode is not None
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        elif reader is not None:
            reader.feed_eof()
        if process.returncode is None:
            await _terminate_process(process)


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_signal_controls_keep_serving_requests(
    tmp_path: Path,
    workers: int,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='signal_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'signal-control'})
        """,
        workers=workers,
    )

    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'signal-control')
        for sig in (signal.SIGTTOU, signal.SIGTTIN, signal.SIGTTOU, signal.SIGHUP):
            process.send_signal(sig)
            await _wait_for_h2_success(port=port, body=b'signal-control')
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_rolling_reload_waits_for_replacement_readiness(tmp_path: Path) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='readiness_gated_reload_app',
        module_source="""
        import asyncio
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                while True:
                    message = await receive()
                    if message['type'] == 'lifespan.startup':
                        await asyncio.sleep(0.8)
                        await send({'type': 'lifespan.startup.complete'})
                    elif message['type'] == 'lifespan.shutdown':
                        await send({'type': 'lifespan.shutdown.complete'})
                        return
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        """,
        workers=1,
    )

    try:
        old_pid = (await _wait_for_h2_body_any(port=port, timeout=5))[1]
        supervisor_fd_count = (
            len(os.listdir(f'/proc/{process.pid}/fd'))
            if sys.platform == 'linux'
            else None
        )
        process.send_signal(signal.SIGHUP)

        # The replacement deliberately remains in lifespan startup. Every
        # request must continue reaching the old worker throughout that gap.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.6
        while loop.time() < deadline:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=0.3)
            assert (status, body) == (200, old_pid)
            await asyncio.sleep(0.03)

        new_pid = await _wait_for_pid_change(
            port=port,
            previous_pid=old_pid,
            timeout=5,
        )
        assert new_pid != old_pid
        if supervisor_fd_count is not None:
            assert await _wait_for_worker_count(
                supervisor_pid=process.pid,
                count=1,
            ) == [int(new_pid)]
            assert len(os.listdir(f'/proc/{process.pid}/fd')) == supervisor_fd_count
        assert process.returncode is None
    finally:
        await _terminate_process(process)


@pytest.mark.skipif(sys.platform != 'linux', reason='worker count uses /proc')
async def test_scale_down_during_reload_keeps_unready_replacement(
    tmp_path: Path,
) -> None:
    delay_replacements = tmp_path / 'delay-replacements'
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='scale_down_during_reload_app',
        module_source=f"""
        import asyncio
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                while True:
                    message = await receive()
                    if message['type'] == 'lifespan.startup':
                        if os.path.exists({os.fspath(delay_replacements)!r}):
                            await asyncio.sleep(1)
                        await send({{'type': 'lifespan.startup.complete'}})
                    elif message['type'] == 'lifespan.shutdown':
                        await send({{'type': 'lifespan.shutdown.complete'}})
                        return
            await send({{'type': 'http.response.start', 'status': 200, 'headers': []}})
            await send({{'type': 'http.response.body', 'body': str(os.getpid()).encode()}})
        """,
        workers=2,
    )

    try:
        await _wait_for_h2_body_any(port=port, timeout=5)
        old_workers = set(
            await _wait_for_worker_count(
                supervisor_pid=process.pid,
                count=2,
            )
        )
        delay_replacements.write_text('delay\n')
        process.send_signal(signal.SIGHUP)

        # The third child is the replacement, held in lifespan startup for one
        # second. Scale-down must retire another old worker, never this child.
        await _wait_for_worker_count(supervisor_pid=process.pid, count=3)
        process.send_signal(signal.SIGTTOU)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.5
        while loop.time() < deadline:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=0.3)
            assert status == 200
            assert int(body) in old_workers

        [final_worker] = await _wait_for_worker_count(
            supervisor_pid=process.pid,
            count=1,
            timeout=6,
        )
        assert final_worker not in old_workers
        status, body = await _wait_for_h2_body_any(port=port, timeout=5)
        assert (status, int(body)) == (200, final_worker)
        assert process.returncode is None
    finally:
        await _terminate_process(process)


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_exits_on_worker_crash_loop(
    tmp_path: Path,
    workers: int,
) -> None:
    process, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='crashloop_app',
        module_source="""
        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.failed', 'message': 'boom'})
                    return
        """,
        workers=workers,
    )

    try:
        exit_code = await asyncio.wait_for(process.wait(), timeout=10)
    finally:
        await _terminate_process(process)

    assert exit_code != 0


async def test_worker_supervisor_exits_on_startup_watchdog_crash_loop(
    tmp_path: Path,
) -> None:
    stderr_lines: list[bytes] = []
    process, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='watchdog_crashloop_app',
        module_source="""
        import time

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                time.sleep(60)
        """,
        workers=1,
        extra_args=[
            '--timeout-worker-healthcheck',
            '0.15',
            '--timeout-graceful-shutdown',
            '0.15',
            '--timeout-lifespan-startup',
            '30',
        ],
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        exit_code = await asyncio.wait_for(process.wait(), timeout=10)
    finally:
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert exit_code != 0
    assert stderr.count('failed healthcheck and will be replaced') >= 3
    assert 'worker crash loop detected' in stderr


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_exits_on_unexpected_clean_worker_exit(
    tmp_path: Path,
    workers: int,
) -> None:
    stderr_lines: list[bytes] = []
    process, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='clean_exit_app',
        module_source="""
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                os._exit(0)
        """,
        workers=workers,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        exit_code = await asyncio.wait_for(process.wait(), timeout=10)
    finally:
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert exit_code != 0
    assert 'exited unexpectedly with code 0' in stderr
    assert 'worker crash loop detected' in stderr


async def test_worker_supervisor_recycles_workers_after_max_requests(
    tmp_path: Path,
) -> None:
    stderr_lines: list[bytes] = []
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='max_requests_app',
        module_source="""
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'http':
                await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        """,
        workers=1,
        extra_args=['--max-requests', '1', '--max-requests-jitter', '0'],
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        next_pid = await _wait_for_pid_change(port=port, previous_pid=body)
        assert next_pid != body
        assert process.returncode is None
    finally:
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert 'exited unexpectedly with code 0' not in stderr
    assert 'Stopped worker' in stderr


async def test_worker_supervisor_replaces_blocked_worker_before_grace_deadline(
    tmp_path: Path,
) -> None:
    blocked_path = tmp_path / 'blocked-worker'
    stderr_lines: list[bytes] = []
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='watchdog_overlap_app',
        module_source=f"""
        import os
        from pathlib import Path
        import time

        async def app(scope, receive, send):
            if scope['type'] != 'http':
                return
            if scope['path'] == '/block':
                Path({os.fspath(blocked_path)!r}).write_text(str(os.getpid()))
                time.sleep(60)
                await send({{'type': 'http.response.start', 'status': 200, 'headers': []}})
                await send({{'type': 'http.response.body', 'body': b'blocked'}})
                return
            await send({{'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]}})
            await send({{'type': 'http.response.body', 'body': str(os.getpid()).encode()}})
        """,
        workers=1,
        extra_args=[
            '--timeout-worker-healthcheck',
            '3',
            '--timeout-graceful-shutdown',
            '8',
        ],
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))
    blocking: asyncio.Task[tuple[int, bytes]] | None = None

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200

        blocking = asyncio.create_task(h2_request(port=port, path='/block'))
        await _wait_for_path(blocked_path)
        assert blocked_path.read_text() == body.decode()

        next_pid = await _wait_for_pid_change(port=port, previous_pid=body, timeout=10)
        assert next_pid != body
        overlapping_workers = set(_worker_pids(process.pid))
        assert overlapping_workers == {int(body), int(next_pid)}
        assert not _all_dead([int(body)]), (
            'replacement must serve before the blocked worker retirement deadline'
        )
        [only_worker] = await _wait_for_worker_count(
            supervisor_pid=process.pid,
            count=1,
            timeout=10,
        )
        assert only_worker == int(next_pid)
        assert _all_dead([int(body)])
        await asyncio.gather(blocking, return_exceptions=True)
        assert process.returncode is None
    finally:
        if blocking is not None:
            if not blocking.done():
                blocking.cancel()
            await asyncio.gather(blocking, return_exceptions=True)
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert (
        f'Worker [{body.decode()}] exceeded graceful shutdown timeout; killing'
        in stderr
    )
    assert f'Stopped worker [{body.decode()}]' in stderr
    assert 'exited unexpectedly' not in stderr


async def test_worker_supervisor_healthcheck_allows_async_lifespan_startup(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='slow_lifespan_app',
        module_source="""
        import asyncio

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                while True:
                    message = await receive()
                    if message['type'] == 'lifespan.startup':
                        await asyncio.sleep(0.6)
                        await send({'type': 'lifespan.startup.complete'})
                    elif message['type'] == 'lifespan.shutdown':
                        await send({'type': 'lifespan.shutdown.complete'})
                        return
            elif scope['type'] == 'http':
                await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                await send({'type': 'http.response.body', 'body': b'after-lifespan'})
        """,
        workers=1,
        extra_args=[
            '--timeout-worker-healthcheck',
            '0.2',
            '--timeout-lifespan-startup',
            '2',
        ],
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'after-lifespan'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_reload_restarts_server_after_python_source_change(
    tmp_path: Path,
) -> None:
    module_name = 'reload_app'
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name=module_name,
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'v1'})
        """,
        workers=1,
        extra_args=['--reload', '--app-dir', str(tmp_path)],
    )
    module_path = tmp_path / f'{module_name}.py'

    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'v1')

        module_path.write_text(
            textwrap.dedent(
                """
                async def app(scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': b'v2'})
                """
            ).strip()
            + '\n'
        )
        os.utime(module_path, None)

        await _wait_for_h2_body(port=port, body=b'v2', timeout=10)
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_reload_coalesces_bursty_writes_into_one_restart(
    tmp_path: Path,
) -> None:
    module_name = 'reload_coalesce_app'
    stderr_lines: list[bytes] = []
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name=module_name,
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'v1'})
        """,
        workers=1,
        extra_args=['--reload', '--app-dir', str(tmp_path)],
        stderr=asyncio.subprocess.PIPE,
    )
    module_path = tmp_path / f'{module_name}.py'
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'v1')

        module_path.write_text(
            textwrap.dedent(
                """
                async def app(scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': b'v2'})
                """
            ).strip()
            + '\n'
        )
        os.utime(module_path, None)
        await asyncio.sleep(0.02)
        module_path.write_text(
            textwrap.dedent(
                """
                async def app(scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': b'v3'})
                """
            ).strip()
            + '\n'
        )
        os.utime(module_path, None)

        await _wait_for_h2_body(port=port, body=b'v3', timeout=10)
        await asyncio.sleep(0.3)
        assert process.returncode is None
    finally:
        await _terminate_process(process)
        await stderr_task

    assert (
        sum(b'Reload change detected:' in line for line in stderr_lines)
        + sum(b'Reload changes detected:' in line for line in stderr_lines)
    ) == 1


async def test_reuse_port_allows_overlapping_server_generations(tmp_path: Path) -> None:
    """Two independent server processes share one port via SO_REUSEPORT and
    requests keep succeeding after the first generation drains away.
    """
    module_source = """
    import os

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
    """
    port = find_free_port()
    gen_a, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='reuse_port_app',
        module_source=module_source,
        workers=1,
        port=port,
        extra_args=['--reuse-port'],
    )
    try:
        status, pid_a = await _wait_for_h2_body_any(port=port)
        assert status == 200
        gen_b, _ = await _spawn_server_process(
            tmp_path=tmp_path,
            module_name='reuse_port_app',
            module_source=module_source,
            workers=1,
            port=port,
            extra_args=['--reuse-port'],
        )
        try:
            # Generation B is up once a different worker pid answers — only
            # then is draining A guaranteed not to empty the port.
            await _wait_for_pid_change(port=port, previous_pid=pid_a, timeout=10)
            await _terminate_process(gen_a)
            deadline = asyncio.get_running_loop().time() + 5
            served = 0
            while served < 5:
                try:
                    status, body = await asyncio.wait_for(
                        h2_request(port=port), timeout=5
                    )
                except OSError:
                    # A connection may still hash to A's just-closed socket
                    # for an instant; the kernel rebalances immediately.
                    if asyncio.get_running_loop().time() >= deadline:
                        raise
                    await asyncio.sleep(0.05)
                    continue
                assert status == 200
                assert body != pid_a
                served += 1
        finally:
            await _terminate_process(gen_b)
    finally:
        await _terminate_process(gen_a)


def _worker_pids(supervisor_pid: int) -> list[int]:
    """PIDs of the supervisor's forked worker children (Linux /proc scan)."""
    children = []
    for entry in os.listdir('/proc'):
        if not entry.isdigit():
            continue
        try:
            with open(f'/proc/{entry}/status') as status:
                ppid = next(
                    int(line.split()[1]) for line in status if line.startswith('PPid:')
                )
        except (OSError, StopIteration):
            continue
        if ppid == supervisor_pid:
            children.append(int(entry))
    return children


def _all_dead(pids: list[int]) -> bool:
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        except PermissionError:
            return False
        else:
            return False
    return True


@pytest.mark.skipif(
    sys.platform != 'linux',
    reason='orphan reaping relies on the /proc scan and PR_SET_PDEATHSIG',
)
@pytest.mark.parametrize('workers', [1, 2])
async def test_sigkilled_supervisor_leaves_no_orphan_workers(
    tmp_path: Path,
    workers: int,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='orphan_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'alive'})
        """,
        workers=workers,
    )
    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'alive')
        worker_pids = _worker_pids(process.pid)
        assert len(worker_pids) == workers

        # Hard-kill the supervisor (no graceful teardown): PR_SET_PDEATHSIG
        # must make the kernel reap every worker regardless.
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=5)

        deadline = asyncio.get_running_loop().time() + 5
        while not _all_dead(worker_pids):
            assert asyncio.get_running_loop().time() < deadline, (
                f'workers orphaned after supervisor SIGKILL: {worker_pids}'
            )
            await asyncio.sleep(0.05)
    finally:
        await _terminate_process(process)
        for pid in _worker_pids(process.pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
