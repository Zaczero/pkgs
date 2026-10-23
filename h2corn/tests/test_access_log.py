"""Access-log formats.

The text line is the human-readable default; `json` exists so a log shipper can
consume records without re-parsing them, which is why the numeric fields stay
numeric and the strings are escaped at the formatter.
"""

import asyncio
import io
import json
import os
import sys
from pathlib import Path

import pytest

from tests._support import HANG_BOUND, http1_request

pytestmark = pytest.mark.asyncio


def _json_records(err: str) -> list[dict]:
    """Every line of the stream, parsed.

    Nothing is skipped on purpose: a line that fails to parse is the defect
    this format exists to prevent.
    """
    return [json.loads(line) for line in err.splitlines()]


async def _serve_requests(
    log_path: Path,
    servers: tuple[tuple[str, bool, bytes], ...],
    *,
    app_source: str = """
async def app(scope, receive, send):
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': b'hello'})
""",
    expected_status: int = 200,
    expected_body: bytes = b'hello',
) -> str:
    # The process-wide batched sink must finish with the test that produced it.
    script = f"""
import asyncio
import sys

from h2corn import Config, Server

{app_source}

async def main():
    servers = []
    serving = []
    for log_format, access_log in {[(fmt, enabled) for fmt, enabled, _ in servers]!r}:
        server = Server(
            app,
            Config(
                port=0,
                access_log=access_log,
                log_format=log_format,
                lifespan='off',
            ),
        )
        servers.append(server)
        serving.append(asyncio.create_task(server.serve()))
        await server.wait_started()
    for server in servers:
        print(server.addresses[0], flush=True)
    await asyncio.to_thread(sys.stdin.buffer.read)
    for server in servers:
        server.shutdown()
    await asyncio.gather(*serving)

asyncio.run(main())
"""
    with log_path.open('wb') as stderr:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            '-c',
            script,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=stderr,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        try:
            ports = []
            for _ in servers:
                address = await asyncio.wait_for(
                    process.stdout.readline(),
                    timeout=HANG_BOUND,
                )
                assert address, (
                    'server exited before reporting its address: '
                    f'{log_path.read_text(encoding="utf-8")}'
                )
                port = int(address.decode().rsplit(':', 1)[1])
                ports.append(port)
            responses = await asyncio.gather(
                *(
                    http1_request(
                        port=port,
                        request=b'GET '
                        + target
                        + b' HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
                    )
                    for port, (_, _, target) in zip(ports, servers, strict=True)
                )
            )
            for status, _, body, _ in responses:
                assert status == expected_status
                assert body == expected_body
        finally:
            process.stdin.close()
            try:
                await process.stdin.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=HANG_BOUND)
            except TimeoutError:
                if process.returncode is None:
                    process.kill()
                await process.wait()
                raise

    assert process.returncode == 0, log_path.read_text(encoding='utf-8')
    return log_path.read_text(encoding='utf-8')


async def test_closed_log_pipe_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    from h2corn._log import Event

    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    with (
        io.TextIOWrapper(io.FileIO(write_fd, 'w'), write_through=True) as output,
        monkeypatch.context() as patch,
    ):
        patch.setattr(sys, 'stderr', output)
        Event.CLI_ERROR.log('Error: {detail}', detail='collector disconnected')


async def test_json_access_log_emits_one_parsable_record_per_request(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    stderr = await _serve_requests(
        tmp_path / 'server.stderr', (('json', True, b'/items?q=1'),)
    )

    records = _json_records(stderr)
    assert '/items?q=1' not in capfd.readouterr().err
    # The banner is encoded too, so the stream is entirely machine-readable.
    assert [record['event'] for record in records] == [
        'starting',
        'listening',
        'http1_enabled',
        'request',
    ]
    record = records[-1]

    assert record['level'] == 'info'
    assert record['pid'] > 0
    assert {r['pid'] for r in records} == {record['pid']}
    assert record['method'] == 'GET'
    assert record['target'] == '/items?q=1'
    assert record['protocol'] == 'HTTP/1.1'
    # Numbers stay numbers: a shipper must not have to parse "0.4ms" or "5b".
    assert record['status'] == 200
    assert isinstance(record['duration_ms'], float)
    assert record['rx_bytes'] == 0
    assert record['tx_bytes'] == 5
    # The client label carries no column padding -- that belongs to the text
    # line, and a forwarded host may legitimately end in a space.
    assert record['client'] == record['client'].strip()


async def test_json_access_log_escapes_a_hostile_target(
    tmp_path: Path,
) -> None:
    # A raw quote and backslash reach the log verbatim -- a request target is
    # not required to be percent-encoded on the wire. Unescaped, either one
    # ends the JSON string early and the record stops parsing.
    stderr = await _serve_requests(
        tmp_path / 'server.stderr', (('json', True, rb'/a"b\c'),)
    )

    requests = [r for r in _json_records(stderr) if r['event'] == 'request']
    assert len(requests) == 1, requests
    assert requests[0]['target'] == '/a"b\\c'


async def test_access_log_off_still_emits_diagnostics(
    tmp_path: Path,
) -> None:
    stderr = await _serve_requests(
        tmp_path / 'server.stderr',
        (('text', False, b'/quiet'),),
    )

    # No per-request record...
    assert '/quiet' not in stderr
    # ...but the stream is not silenced: diagnostics are a separate axis.
    assert 'Listening on' in stderr


@pytest.mark.parametrize(
    'formats',
    [('json', 'text'), ('text', 'json')],
    ids=['json_then_text', 'text_then_json'],
)
@pytest.mark.parametrize('text_access_log', [True, False])
async def test_embedded_servers_keep_their_request_log_format(
    tmp_path: Path,
    formats: tuple[str, str],
    text_access_log: bool,
) -> None:
    stderr = await _serve_requests(
        tmp_path / 'servers.stderr',
        tuple(
            (fmt, fmt == 'json' or text_access_log, f'/{fmt}-server'.encode())
            for fmt in formats
        ),
    )
    lines = stderr.splitlines()
    records = [json.loads(line) for line in lines if line.startswith('{')]
    requests = [record for record in records if record['event'] == 'request']
    assert len(requests) == 1, stderr
    assert requests[0]['target'] == '/json-server'
    assert requests[0]['status'] == 200
    text_requests = [line for line in lines if '/text-server' in line]
    assert len(text_requests) == int(text_access_log), stderr
    if text_access_log:
        assert 'GET /text-server HTTP/1.1' in text_requests[0]
        assert not text_requests[0].startswith('{')


@pytest.mark.parametrize('formats', [('json', 'text'), ('text', 'json')])
async def test_embedded_servers_keep_their_error_log_format(
    tmp_path: Path, formats: tuple[str, str]
) -> None:
    stderr = await _serve_requests(
        tmp_path / 'servers.stderr',
        tuple((fmt, False, b'/failure') for fmt in formats),
        app_source="""
async def app(scope, receive, send):
    raise RuntimeError('application failure canary')
""",
        expected_status=500,
        expected_body=b'',
    )
    records = [json.loads(line) for line in stderr.splitlines() if line.startswith('{')]
    failures = [record for record in records if record['event'] == 'failed']
    assert len(failures) == 1
    assert failures[0]['level'] == 'error'
    assert 'application failure canary' in failures[0]['detail']
    assert sum(line.startswith('request failed:') for line in stderr.splitlines()) == 1


@pytest.mark.parametrize(
    'exception',
    [
        "RuntimeError('outer failure')",
        "ExceptionGroup('group failure', [TypeError('member failure')])",
    ],
    ids=['chained', 'exception-group'],
)
async def test_json_application_error_preserves_exception_chain_and_notes(
    tmp_path: Path, exception: str
) -> None:
    stderr = await _serve_requests(
        tmp_path / 'server.stderr',
        (('json', False, b'/failure'),),
        app_source=f"""
async def app(scope, receive, send):
    try:
        raise ValueError('original cause')
    except ValueError as cause:
        error = {exception}
        error.add_note('diagnostic note')
        raise error from cause
""",
        expected_status=500,
        expected_body=b'',
    )
    failures = [r for r in _json_records(stderr) if r['event'] == 'failed']
    assert len(failures) == 1
    detail = failures[0]['detail']
    assert 'ValueError: original cause' in detail
    assert 'The above exception was the direct cause' in detail
    assert 'diagnostic note' in detail
    assert (
        'outer failure'
        if exception.startswith('RuntimeError')
        else 'TypeError: member failure'
    ) in detail


@pytest.mark.skipif(sys.platform == 'win32', reason='worker supervisor requires Unix')
@pytest.mark.parametrize(
    'fail_startup', [False, True], ids=['shutdown', 'startup-error']
)
async def test_programmatic_supervisor_keeps_json_diagnostics(
    fail_startup: bool,
) -> None:
    script = f"""
import os
import signal
from h2corn import Config, serve

async def app(scope, receive, send):
    assert scope['type'] == 'lifespan'
    assert (await receive())['type'] == 'lifespan.startup'
    if {fail_startup!r}:
        raise RuntimeError('worker startup canary')
    await send({{'type': 'lifespan.startup.complete'}})
    os.kill(os.getppid(), signal.SIGTERM)
    assert (await receive())['type'] == 'lifespan.shutdown'
    await send({{'type': 'lifespan.shutdown.complete'}})

serve(app, Config(port=0, lifespan='on', log_format='json', loop='asyncio'))
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-c',
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    prefix = bytearray()
    try:
        if fail_startup:
            assert process.stderr is not None
            async with asyncio.timeout(HANG_BOUND):
                while line := await process.stderr.readline():
                    prefix.extend(line)
                    if json.loads(line)['event'] == 'worker_failed':
                        process.terminate()
                        break
                else:
                    pytest.fail('worker exited without reporting its startup failure')
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=HANG_BOUND
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert not stdout
    records = _json_records((prefix + stderr).decode())
    events = [r['event'] for r in records]
    assert 'worker_started' in events
    assert 'supervisor_stopping' in events
    if not fail_startup:
        assert 'worker_stopped' in events
    started = next(r for r in records if r['event'] == 'worker_started')
    assert started['pid'] == process.pid
    assert started['worker_pid'] != process.pid
    failures = [r for r in records if r['event'] == 'worker_failed']
    assert bool(failures) is fail_startup
    if fail_startup:
        assert 'RuntimeError: worker startup canary' in failures[0]['detail']
        assert failures[0]['pid'] == next(
            r['worker_pid'] for r in records if r['event'] == 'worker_started'
        )
    else:
        assert process.returncode == 0


async def test_json_cli_startup_error_is_one_record(tmp_path: Path) -> None:
    certificate = tmp_path / 'missing-cert.pem'
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'h2corn',
        'unused:app',
        '--log-format',
        'json',
        '--certfile',
        str(certificate),
        '--keyfile',
        str(tmp_path / 'missing-key.pem'),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=HANG_BOUND
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 1
    assert not stdout
    records = _json_records(stderr.decode())
    assert len(records) == 1
    assert records[0]['event'] == 'cli_error'
    assert records[0]['level'] == 'error'
    assert str(certificate) in records[0]['detail']


async def test_json_websocket_close_has_its_own_event_and_code() -> None:
    script = """
import asyncio
from h2corn import Config, Server
from websockets.asyncio.client import connect

async def app(scope, receive, send):
    assert (await receive())['type'] == 'websocket.connect'
    await send({'type': 'websocket.accept'})
    message = await receive()
    await send({'type': 'websocket.send', 'text': message['text']})
    assert (await receive())['type'] == 'websocket.disconnect'

async def main():
    server = Server(app, Config(port=0, lifespan='off', log_format='json'))
    task = asyncio.create_task(server.serve())
    try:
        await server.wait_started()
        async with connect('ws://' + server.addresses[0] + '/echo') as ws:
            await ws.send('hello')
            assert await ws.recv() == 'hello'
    finally:
        server.shutdown()
        await task

asyncio.run(main())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-c',
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=HANG_BOUND
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 0, stderr.decode()
    assert not stdout
    records = _json_records(stderr.decode())
    handshakes = [r for r in records if r['event'] == 'request']
    sessions = [r for r in records if r['event'] == 'websocket_closed']
    assert len(handshakes) == 1
    assert handshakes[0]['status'] == 101
    assert 'close_code' not in handshakes[0]
    assert len(sessions) == 1
    assert sessions[0]['close_code'] == 1000
    assert 'status' not in sessions[0]
    assert sessions[0]['method'] == 'GET'
    assert sessions[0]['target'] == '/echo'
    assert sessions[0]['rx_bytes'] == 5
    assert sessions[0]['tx_bytes'] == 5
