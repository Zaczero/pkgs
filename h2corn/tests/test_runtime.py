import asyncio
import os
import re
import signal
import sys
import textwrap
from pathlib import Path

import pytest
from h2corn import Config, Server

from tests._support import (
    find_free_port,
    h2_request,
    running_server,
    wait_for_port,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(sys.platform == 'win32', reason='unix-only runtime test'),
]


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        await asyncio.wait_for(process.wait(), timeout=5)
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=5)


async def _wait_for_h2_success(
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
        assert status == 200
        assert response_body == body
        return


async def _wait_for_listening_port(
    process: asyncio.subprocess.Process,
    *,
    timeout: float = 5.0,
) -> int:
    assert process.stderr is not None

    async def _read_port() -> int:
        while True:
            line = await process.stderr.readline()
            if not line:
                raise AssertionError('server exited before printing listening banner')
            match = re.search(rb"Listening on http://127\.0\.0\.1:(\d+)", line)
            if match is not None:
                return int(match.group(1))

    return await asyncio.wait_for(_read_port(), timeout=timeout)


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
    )
    return process, port


async def _wait_for_pid_change(
    *, port: int, previous_pid: bytes, timeout: float = 5.0
) -> bytes:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            status, body = await h2_request(port=port)
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


async def test_unix_socket_serving(tmp_path: Path) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'uds'})

    config = Config(uds=tmp_path / 'h2corn.sock')
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(uds=config.uds), timeout=5)

    assert status == 200
    assert body == b'uds'


async def test_unix_socket_cleanup_removes_owned_socket_path(tmp_path: Path) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(uds=tmp_path / 'cleanup.sock')
    async with running_server(app, config):
        assert config.uds.exists()

    assert not config.uds.exists()


async def test_unix_socket_path_rejects_non_socket_files(tmp_path: Path) -> None:
    path = tmp_path / 'not-a-socket'
    path.write_text('occupied')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(app, Config(uds=path))

    with pytest.raises(OSError, match='not a socket'):
        await server.serve()

    assert path.read_text() == 'occupied'


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


async def test_worker_supervisor_recycles_workers_after_max_requests(
    tmp_path: Path,
) -> None:
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
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        next_pid = await _wait_for_pid_change(port=port, previous_pid=body)
        assert next_pid != body
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_replaces_worker_after_healthcheck_timeout(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='watchdog_app',
        module_source="""
        import os
        import time

        async def app(scope, receive, send):
            if scope['type'] != 'http':
                return
            if scope['path'] == '/block':
                time.sleep(1.0)
                await send({'type': 'http.response.start', 'status': 200, 'headers': []})
                await send({'type': 'http.response.body', 'body': b'blocked'})
                return
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        """,
        workers=1,
        extra_args=['--timeout-worker-healthcheck', '0.2'],
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200

        blocking = asyncio.create_task(
            asyncio.wait_for(h2_request(port=port, path='/block'), timeout=0.3)
        )
        try:
            await blocking
        except Exception:
            pass

        next_pid = await _wait_for_pid_change(port=port, previous_pid=body, timeout=10)
        assert next_pid != body
        assert process.returncode is None
    finally:
        await _terminate_process(process)
