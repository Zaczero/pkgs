"""Access-log formats.

The text line is the human-readable default; `json` exists so a log shipper can
consume records without re-parsing them, which is why the numeric fields stay
numeric and the strings are escaped at the formatter.
"""

import asyncio
import json

import pytest
from h2corn import Config

from tests._support import http1_request, running_server, server_port

pytestmark = pytest.mark.asyncio


def _json_records(err: str) -> list[dict]:
    """Every line of the stream, parsed.

    Nothing is skipped on purpose: a line that fails to parse is the defect
    this format exists to prevent.
    """
    return [json.loads(line) for line in err.splitlines() if line.strip()]


async def _serve_once(log_format: str, target: bytes, *, access_log: bool = True) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'hello'})

    async with running_server(
        app,
        Config(port=0, access_log=access_log, log_format=log_format, lifespan='off'),
    ) as server:
        await http1_request(
            port=server_port(server),
            request=b'GET ' + target + b' HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
        )
    # The sink batches; give its writer a turn before reading the capture.
    await asyncio.sleep(0.1)


async def test_json_access_log_emits_one_parsable_record_per_request(
    capfd: pytest.CaptureFixture[str],
) -> None:
    await _serve_once('json', b'/items?q=1')

    records = _json_records(capfd.readouterr().err)
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
    capfd: pytest.CaptureFixture[str],
) -> None:
    # A raw quote and backslash reach the log verbatim -- a request target is
    # not required to be percent-encoded on the wire. Unescaped, either one
    # ends the JSON string early and the record stops parsing.
    await _serve_once('json', rb'/a"b\c')

    requests = [r for r in _json_records(capfd.readouterr().err) if r['event'] == 'request']
    assert len(requests) == 1, requests
    assert requests[0]['target'] == '/a"b\\c'


async def test_access_log_off_still_emits_diagnostics(
    capfd: pytest.CaptureFixture[str],
) -> None:
    await _serve_once('text', b'/quiet', access_log=False)

    err = capfd.readouterr().err
    # No per-request record...
    assert '/quiet' not in err
    # ...but the stream is not silenced: diagnostics are a separate axis.
    assert 'Listening on' in err
