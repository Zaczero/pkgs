"""Access-log formats.

The text line is the human-readable default; `json` exists so a log shipper can
consume records without re-parsing them, which is why the numeric fields stay
numeric and the strings are escaped at the formatter.
"""

import asyncio
import json
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
    return [json.loads(line) for line in err.splitlines() if line.strip()]


async def _serve_once(
    log_path: Path,
    log_format: str,
    target: bytes,
    *,
    access_log: bool = True,
) -> str:
    # The process-wide batched sink must finish with the test that produced it.
    script = f"""
import asyncio
import sys

from h2corn import Config, Server

async def app(scope, receive, send):
    await send({{'type': 'http.response.start', 'status': 200, 'headers': []}})
    await send({{'type': 'http.response.body', 'body': b'hello'}})

async def main():
    server = Server(
        app,
        Config(
            port=0,
            access_log={access_log!r},
            log_format={log_format!r},
            lifespan='off',
        ),
    )
    serving = asyncio.create_task(server.serve())
    await server.wait_started()
    print(server.addresses[0], flush=True)
    await asyncio.to_thread(sys.stdin.buffer.read)
    server.shutdown()
    await serving

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
            address = await asyncio.wait_for(
                process.stdout.readline(),
                timeout=HANG_BOUND,
            )
            assert address, (
                'server exited before reporting its address: '
                f'{log_path.read_text(encoding="utf-8")}'
            )
            port = int(address.decode().rsplit(':', 1)[1])
            await http1_request(
                port=port,
                request=b'GET '
                + target
                + b' HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
            )
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


async def test_json_access_log_emits_one_parsable_record_per_request(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    stderr = await _serve_once(tmp_path / 'server.stderr', 'json', b'/items?q=1')

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
    stderr = await _serve_once(tmp_path / 'server.stderr', 'json', rb'/a"b\c')

    requests = [r for r in _json_records(stderr) if r['event'] == 'request']
    assert len(requests) == 1, requests
    assert requests[0]['target'] == '/a"b\\c'


async def test_access_log_off_still_emits_diagnostics(
    tmp_path: Path,
) -> None:
    stderr = await _serve_once(
        tmp_path / 'server.stderr',
        'text',
        b'/quiet',
        access_log=False,
    )

    # No per-request record...
    assert '/quiet' not in stderr
    # ...but the stream is not silenced: diagnostics are a separate axis.
    assert 'Listening on' in stderr
