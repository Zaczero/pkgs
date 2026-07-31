import asyncio
import importlib.metadata
import os
from contextlib import suppress
from pathlib import Path

import pytest
from h2corn import Config

from tests._support import (
    h2_request,
    http1_request,
    read_http1_response,
    read_http_request_body,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio


async def test_http1_request_round_trip() -> None:
    http_version = None

    async def app(scope, receive, send):
        nonlocal http_version
        assert scope['type'] == 'http'
        http_version = scope['http_version']
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'hello over http1'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert http_version == '1.1'
    assert status == 200
    assert headers[b'content-type'] == b'text/plain'
    assert body == b'hello over http1'
    assert trailers == []


async def test_http1_absolute_form_preserves_cleartext_scheme() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        body = (
            f'{scope["scheme"]}|{scope["path"]}|{scope["query_string"].decode()}'
        ).encode()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=b'GET https://example.com/absolute?x=1 HTTP/1.1\r\n\r\n',
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == b'18'
    assert body == b'http|/absolute|x=1'
    assert trailers == []


async def test_http1_absolute_form_fragment_is_rejected_before_uri_normalizes() -> None:
    """A fragment on the wire must not disappear before request validation."""
    seen = []

    async def app(scope, receive, send):
        seen.append(scope)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0, lifespan='off')
    async with running_server(app, config) as server:
        status, _, _, _ = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    b'GET http://example/p#frag HTTP/1.1\r\n'
                    b'Connection: close\r\n'
                    b'\r\n'
                ),
            ),
            timeout=5,
        )

    assert status == 400
    assert seen == [], 'the normalized /p target must never reach the application'


@pytest.mark.parametrize(
    ('target', 'raw_path', 'query_string'),
    [
        (b'/', b'/', b''),
        (b'/items%2Fall?q=1', b'/items%2Fall', b'q=1'),
    ],
)
async def test_http1_scope_raw_path_preserves_target_bytes(
    target: bytes,
    raw_path: bytes,
    query_string: bytes,
) -> None:
    state = {}

    async def app(scope, receive, send):
        state['raw_path'] = scope['raw_path']
        state['query_string'] = scope['query_string']
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, _, _, _ = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=b'GET %s HTTP/1.1\r\nHost: x\r\n\r\n' % target,
            ),
            timeout=5,
        )

    assert status == 200
    assert state == {'raw_path': raw_path, 'query_string': query_string}


async def test_http1_response_defaults_apply_to_normal_app_responses() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=0,
        date_header=True,
        response_headers=('x-extra: works',),
    )
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'ok'
    assert headers[b'x-extra'] == b'works'
    assert b'date' in headers
    assert trailers == []


@pytest.mark.parametrize(
    ('request_bytes', 'status', 'expected_scope'),
    [
        # RFC 9112 §3.2: every target form has a deliberately narrow home.
        (
            b'GET /origin?x=1 HTTP/1.1\r\nHost: example.com\r\n\r\n',
            200,
            ('GET', '/origin'),
        ),
        (b'OPTIONS * HTTP/1.1\r\nHost: example.com\r\n\r\n', 200, ('OPTIONS', '*')),
        (
            b'GET http://example.com/absolute HTTP/1.1\r\n\r\n',
            200,
            ('GET', '/absolute'),
        ),
        # A tunnel is not something an ASGI application can be handed.  It
        # must still be a syntactically complete host:port authority.
        (b'CONNECT example.com:443 HTTP/1.1\r\n\r\n', 501, None),
        (b'CONNECT example.com HTTP/1.1\r\n\r\n', 400, None),
        # Form/method mismatches, relative targets, fragments, missing Host,
        # and conflicting authorities are all rejected before dispatch.
        (b'CONNECT /tunnel HTTP/1.1\r\nHost: x\r\n\r\n', 400, None),
        (b'GET * HTTP/1.1\r\nHost: x\r\n\r\n', 400, None),
        (b'GET example.com:443 HTTP/1.1\r\nHost: x\r\n\r\n', 400, None),
        (b'GET p/q HTTP/1.1\r\nHost: x\r\n\r\n', 400, None),
        (b'GET http://example.com/p#fragment HTTP/1.1\r\n\r\n', 400, None),
        (b'GET / HTTP/1.1\r\n\r\n', 400, None),
        (b'GET / HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n', 400, None),
        (
            b'GET http://example.com/ HTTP/1.1\r\nHost: other.example\r\n\r\n',
            400,
            None,
        ),
    ],
)
async def test_request_target_and_host_grammar(
    request_bytes: bytes,
    status: int,
    expected_scope: tuple[str, str] | None,
) -> None:
    seen: list[tuple[str, str]] = []

    async def app(scope, receive, send):
        seen.append((scope['method'], scope['path']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(request_bytes)
            await writer.drain()
            response = await asyncio.wait_for(read_http1_response(reader), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert response[0] == status
    assert seen == ([] if expected_scope is None else [expected_scope])


@pytest.mark.parametrize('timeout_handshake', [5.0, 0.0])
async def test_http1_zero_handshake_timeout_means_no_limit(
    timeout_handshake: float,
) -> None:
    """`0` disables a timeout, as every other timeout option spells it.

    The native side used to pass the value to `timeout()` unconditionally, so
    zero expired on the first poll and rejected every connection — the exact
    opposite of what the option documents.
    """

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'served'})

    config = Config(port=0, timeout_handshake=timeout_handshake)
    async with running_server(app, config) as server:
        status, _, body, _ = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=b'GET / HTTP/1.1\r\nHost: x\r\n\r\n',
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'served'


@pytest.mark.parametrize(
    ('config_kwargs', 'expected'),
    [
        ({}, None),
        ({'server_header': 'on'}, b'h2corn'),
        (
            {'server_header': 'full'},
            f'h2corn/{importlib.metadata.version("h2corn")}'.encode(),
        ),
        # An explicitly configured header is a deliberate choice and beats the
        # generic mode; the application's own value beats both.
        (
            {'server_header': 'on', 'response_headers': ('server: acme-edge',)},
            b'acme-edge',
        ),
    ],
)
async def test_http1_server_header_modes(config_kwargs, expected: bytes | None) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, **config_kwargs)
    async with running_server(app, config) as server:
        _status, headers, _body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
            ),
            timeout=5,
        )

    assert headers.get(b'server') == expected


async def test_http1_server_header_yields_to_the_application() -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'server', b'set-by-app')],
        })
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, server_header='full')
    async with running_server(app, config) as server:
        _status, headers, _body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
            ),
            timeout=5,
        )

    assert headers.get(b'server') == b'set-by-app'


@pytest.mark.parametrize(
    ('body_limit', 'expected'),
    [
        # Within the limit, the body is genuinely wanted.
        (16, b'HTTP/1.1 100 Continue'),
        # Already destined for 413: inviting the upload only wastes it.
        (4, b'HTTP/1.1 413 Payload Too Large'),
    ],
)
async def test_http1_expect_continue_is_not_sent_for_a_doomed_request(
    body_limit: int, expected: bytes
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, max_request_body_size=body_limit)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n'
                b'Expect: 100-continue\r\nConnection: close\r\n\r\n'
            )
            await writer.drain()
            first = await asyncio.wait_for(reader.readline(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert first.strip() == expected


@pytest.mark.parametrize(
    ('trailer_section', 'accepted'),
    [
        (b'0\r\nA: 1\r\n\r\n', True),
        (b'0\r\n\r\n', True),
        # Trailers are header fields and are held to the same policy: draining
        # them as bare lines admitted any number of fields, and any text at all.
        (b'0\r\nA: 1\r\nB: 2\r\nC: 3\r\nD: 4\r\nE: 5\r\n\r\n', False),
        (b'0\r\nnot-a-header-line\r\n\r\n', False),
        # This is the actual field policy: request trailers cannot rewrite
        # framing, routing, authentication, or representation metadata.
        (b'0\r\nContent-Digest: sha-256=:abc=:\r\n\r\n', True),
        (b'0\r\nContent-Length: 0\r\n\r\n', False),
        (b'0\r\nTransfer-Encoding: chunked\r\n\r\n', False),
        (b'0\r\nAuthorization: Basic x\r\n\r\n', False),
        (b'0\r\nHost: replacement.example\r\n\r\n', False),
    ],
)
async def test_http1_chunked_trailers_obey_the_field_policy(
    trailer_section: bytes, accepted: bool
) -> None:
    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        while True:
            if not (await receive()).get('more_body'):
                break
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, limit_request_fields=4)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n'
                b'Connection: close\r\n\r\n' + trailer_section
            )
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert line.startswith(b'HTTP/1.1 200') is accepted


async def test_http1_keep_alive_reuses_connection() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                (
                    f'GET /one HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'
                    f'GET /two HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'
                ).encode()
            )
            await writer.drain()
            first = await asyncio.wait_for(read_http1_response(reader), timeout=5)
            second = await asyncio.wait_for(read_http1_response(reader), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert first[0] == 200
    assert first[2] == b'/one'
    assert second[0] == 200
    assert second[2] == b'/two'


async def test_http1_keep_alive_request_head_still_honors_timeout_request_header() -> (
    None
):
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    config = Config(
        port=0,
        timeout_keep_alive=1.0,
        timeout_request_header=0.05,
    )
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                f'GET /one HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode()
            )
            await writer.drain()
            first = await asyncio.wait_for(read_http1_response(reader), timeout=5)

            writer.write(b'GET /two HTTP/1.1\r\nHo')
            await writer.drain()
            await asyncio.sleep(0.2)
            closed = await asyncio.wait_for(reader.read(1), timeout=1)
        finally:
            writer.close()
            await writer.wait_closed()

    assert first[0] == 200
    assert first[2] == b'/one'
    assert closed == b''


async def test_http1_first_request_head_timeout_is_idle_not_total() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    config = Config(port=0, timeout_request_header=0.05)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            for part in (
                b'GET /slow HTTP/1.1\r\nHo',
                f'st: 127.0.0.1:{server_port(server)}\r\nX-De'.encode(),
                b'mo: works\r\n',
                b'\r\n',
            ):
                writer.write(part)
                await writer.drain()
                await asyncio.sleep(0.03)
            status, headers, body, trailers = await asyncio.wait_for(
                read_http1_response(reader),
                timeout=5,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert headers[b'content-length'] == b'5'
    assert body == b'/slow'
    assert trailers == []


async def test_http1_first_request_head_stall_honors_timeout_request_header() -> None:
    async def app(scope, receive, send):
        raise AssertionError(
            'stalled first request head should timeout before app dispatch'
        )

    config = Config(port=0, timeout_request_header=0.05)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(b'GET /slow HTTP/1.1\r\nHo')
            await writer.drain()
            await asyncio.sleep(0.2)
            closed = await asyncio.wait_for(reader.read(1), timeout=1)
        finally:
            writer.close()
            await writer.wait_closed()

    assert closed == b''


async def test_http1_request_head_can_arrive_in_small_segments() -> None:
    async def app(scope, receive, send):
        headers = dict(scope['headers'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.body',
            'body': b'|'.join((scope['path'].encode(), headers[b'x-demo'])),
        })

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            for part in (
                b'GET /slow HTTP/1.1\r\nHo',
                f'st: 127.0.0.1:{server_port(server)}\r\nX-De'.encode(),
                b'mo: works\r\n',
                b'\r\n',
            ):
                writer.write(part)
                await writer.drain()
                await asyncio.sleep(0.01)
            status, headers, body, trailers = await asyncio.wait_for(
                read_http1_response(reader),
                timeout=5,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert headers[b'content-length'] == b'11'
    assert body == b'/slow|works'
    assert trailers == []


async def test_http1_connection_recovers_after_client_closes_mid_header() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        _reader, writer = await asyncio.open_connection(
            '127.0.0.1', server_port(server)
        )
        writer.write(b'GET / HTTP/1.1\r\nHost: 127.0.0.1')
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.05)

        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == b'2'
    assert body == b'ok'
    assert trailers == []


async def test_http1_pathsend_and_trailers(tmp_path: Path) -> None:
    file_path = tmp_path / 'download.txt'
    payload = b'http1-pathsend'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
            'trailers': True,
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-finished', b'yes')],
        })

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'TE: trailers\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-type'] == b'text/plain'
    assert body == payload
    assert trailers == [(b'x-finished', b'yes')]


async def test_http1_pathsend_synthesizes_content_length_when_missing(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / 'payload-no-length.txt'
    payload = (b'http1-pathsend-no-length-' * 800)[:16000]
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'application/octet-stream')],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == str(len(payload)).encode()
    assert body == payload
    assert trailers == []


async def test_http1_pathsend_replaces_wrong_content_length(tmp_path: Path) -> None:
    file_path = tmp_path / 'wrong-length.txt'
    payload = b'http1-actual-file-bytes'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
                (b'content-length', b'99999'),
            ],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == str(len(payload)).encode()
    assert body == payload
    assert trailers == []


async def test_http1_pathsend_rejects_directory_with_403(tmp_path: Path) -> None:
    dir_path = tmp_path / 'not-a-file'
    dir_path.mkdir()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(dir_path)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, _headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 403
    assert body == b''
    assert trailers == []


async def test_http1_pathsend_follows_symlink_to_regular_file(tmp_path: Path) -> None:
    target = tmp_path / 'target.bin'
    payload = b'http1-symlink-payload'
    target.write_bytes(payload)
    link = tmp_path / 'link.bin'
    link.symlink_to(target)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'application/octet-stream')],
        })
        await send({'type': 'http.response.pathsend', 'path': str(link)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == str(len(payload)).encode()
    assert body == payload
    assert trailers == []


async def test_http1_compat_keeps_prior_knowledge_h2() -> None:
    http_version = None

    async def app(scope, receive, send):
        nonlocal http_version
        http_version = scope['http_version']
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'h2 still works'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert http_version == '2'
    assert status == 200
    assert body == b'h2 still works'


async def test_http1_accepts_registered_pri_method() -> None:
    method = None

    async def app(scope, receive, send):
        nonlocal method
        method = scope['method']
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'pri works'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'PRI / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert method == 'PRI'
    assert status == 200
    assert headers[b'content-length'] == b'9'
    assert body == b'pri works'
    assert trailers == []


async def test_http1_is_rejected_when_disabled() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0, http1=False)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode()
        )
        await writer.drain()
        try:
            header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
            payload = await asyncio.wait_for(
                reader.readexactly(int.from_bytes(header[:3], 'big')),
                timeout=5,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert header[3] == 0x07
    assert int.from_bytes(payload[4:8], 'big') == 0x01


async def test_http1_head_response_suppresses_app_body() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'hello'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'HEAD / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
                head_only=True,
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == b'5'
    assert body == b''
    assert trailers == []


async def test_http1_head_pathsend_keeps_empty_body(tmp_path: Path) -> None:
    file_path = tmp_path / 'head-http1-pathsend.txt'
    payload = b'head body should stay hidden in http1 too'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'HEAD / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
                head_only=True,
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == str(len(payload)).encode()
    assert body == b''
    assert trailers == []


async def test_http1_request_body_can_be_consumed_from_content_length() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'Content-Length: 7\r\n\r\npayload'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-type'] == b'text/plain'
    assert body == b'payload'
    assert trailers == []


async def test_http1_chunked_request_body_can_be_consumed() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'Transfer-Encoding: chunked\r\n\r\n'
                    '4\r\npayl\r\n'
                    '3\r\noad\r\n'
                    '0\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-type'] == b'text/plain'
    assert body == b'payload'
    assert trailers == []


async def test_http1_chunked_request_body_can_be_consumed_from_slow_small_writes() -> (
    None
):
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                (
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'Transfer-Encoding: chunked\r\n\r\n'
                ).encode()
            )
            await writer.drain()
            for part in (
                b'1\r\np\r\n',
                b'1\r\na\r\n',
                b'1\r\ny\r\n',
                b'1\r\nl\r\n',
                b'1\r\no\r\n',
                b'1\r\na\r\n',
                b'1\r\nd\r\n',
                b'0\r\n',
                b'\r\n',
            ):
                writer.write(part)
                await writer.drain()
                await asyncio.sleep(0.01)
            status, headers, body, trailers = await asyncio.wait_for(
                read_http1_response(reader),
                timeout=5,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert body == b'payload'
    assert trailers == []
    assert headers[b'content-length'] == b'7'


async def test_http1_chunked_request_extensions_and_trailers_are_accepted() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'Transfer-Encoding: chunked\r\n\r\n'
                    '3;foo=bar\r\nabc\r\n'
                    '4;bar=baz\r\ndefg\r\n'
                    '0\r\n'
                    'X-Ignored: yes\r\n'
                    '\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == b'7'
    assert body == b'abcdefg'
    assert trailers == []


async def test_http1_streaming_response_small_chunks_arrive_in_order() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        for chunk in (b'a', b'b', b'c'):
            await send({
                'type': 'http.response.body',
                'body': chunk,
                'more_body': True,
            })
            await asyncio.sleep(0.01)
        await send({'type': 'http.response.body', 'body': b'd'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'transfer-encoding'] == b'chunked'
    assert body == b'abcd'
    assert trailers == []


async def test_http1_streaming_chunk_reaches_the_client_before_the_app_continues() -> (
    None
):
    """A server-sent-events app must not have its event held in a write buffer.

    The ASGI spec requires a server to flush what `send()` was given before
    returning from it. Reading the whole response would pass even if the chunk
    sat buffered until the app finished, so this reads incrementally while the
    response is still open.
    """
    release = asyncio.Event()

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/event-stream')],
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: first\n\n',
            'more_body': True,
        })
        await asyncio.wait_for(release.wait(), timeout=10)
        await send({'type': 'http.response.body', 'body': b'', 'more_body': False})

    config = Config(port=0)
    async with running_server(app, config) as server:
        port = server_port(server)
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        try:
            writer.write(
                f'GET /events HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n'.encode()
            )
            await writer.drain()

            received = b''
            while b'data: first' not in received:
                # Fails by timing out if the chunk is only flushed later.
                received += await asyncio.wait_for(reader.read(4096), timeout=5)

            assert b'transfer-encoding: chunked' in received.lower()
        finally:
            release.set()
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=5)


@pytest.mark.parametrize(
    'request_lines',
    [
        ['Transfer-Encoding: gzip, chunked'],
        ['Transfer-Encoding: chunked, chunked'],
        ['Content-Length: 5', 'Transfer-Encoding: chunked'],
        ['Transfer-Encoding: chunked', 'Content-Length: 5'],
    ],
)
async def test_http1_rejects_unsupported_request_framing(
    request_lines: list[str],
) -> None:
    called = False

    async def app(scope, receive, send):
        nonlocal called
        if scope['path'] == '/strict-framing':
            called = True
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        request = '\r\n'.join([
            'POST /strict-framing HTTP/1.1',
            f'Host: 127.0.0.1:{server_port(server)}',
            *request_lines,
            '',
            '',
        ]).encode()
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(port=server_port(server), request=request),
            timeout=5,
        )

    assert called is False
    assert status == 400
    assert body == b''
    assert trailers == []
    assert headers[b'content-length'] == b'0'


@pytest.mark.parametrize(
    'field_line',
    [
        b'Content-Length: \x0c1',
        b'Content-Length: 1\x0c',
        b'Content-Length: 1\r',
        b'Transfer-Encoding: chunked\x0c',
    ],
)
async def test_http1_rejects_control_bytes_before_stripping_ows(
    field_line: bytes,
) -> None:
    """Illegal CTLs are rejected from the raw wire, not trimmed into framing."""
    called = False

    async def app(scope, receive, send):
        nonlocal called
        called = True
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    request = (
        b'POST /raw-controls HTTP/1.1\r\n'
        b'Host: example.test\r\n' + field_line + b'\r\n\r\nx'
    )
    async with running_server(app, Config(port=0, lifespan='off')) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(port=server_port(server), request=request), timeout=5
        )

    assert called is False
    assert (status, body) == (400, b'')


@pytest.mark.parametrize(
    'request_bytes',
    [
        b'GET /bare-request-line HTTP/1.1\nHost: example.test\r\n\r\n',
        b'GET /bare-header HTTP/1.1\r\nHost: example.test\n\r\n\r\n',
        (
            b'GET /mixed-delimiters HTTP/1.1\r\n'
            b'Host: example.test\r\n'
            b'X-Demo: first\n'
            b'X-Other: second\r\n\r\n'
        ),
        (b'GET /lone-cr HTTP/1.1\r\nHost: example.test\rX-Demo: value\r\n\r\n'),
    ],
    ids=['request-line', 'header-field', 'mixed-delimiters', 'lone-cr'],
)
async def test_http1_rejects_request_heads_without_crlf_line_terminators(
    request_bytes: bytes,
) -> None:
    """Strict CRLF avoids a request-head grammar differential with proxies."""
    called = False

    async def app(scope, receive, send):
        nonlocal called
        called = True
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(port=server_port(server), request=request_bytes), timeout=5
        )

    assert called is False
    assert (status, body) == (400, b'')


async def test_http1_rejects_a_second_cr_before_the_line_terminator() -> None:
    """A stray CR must not be stripped into a valid field value."""
    called = False

    async def app(scope, receive, send):
        nonlocal called
        called = True
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    request = (
        b'GET /double-cr HTTP/1.1\r\nHost: example.test\r\nX-Demo: value\r\r\n\r\n'
    )
    async with running_server(app, Config(port=0, lifespan='off')) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(request)
            await writer.drain()
            status, headers, body, _trailers = await asyncio.wait_for(
                read_http1_response(reader), timeout=5
            )
            closed = await asyncio.wait_for(reader.read(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert called is False
    assert (status, body, closed) == (400, b'', b'')
    assert headers[b'connection'] == b'close'


async def test_http1_rejects_control_bytes_in_raw_chunked_trailer() -> None:
    """Trailer validation must use raw bytes before removing OWS too."""
    seen_body = []

    async def app(scope, receive, send):
        seen_body.append(await receive())
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    request = (
        b'POST /raw-trailer-control HTTP/1.1\r\n'
        b'Host: example.test\r\n'
        b'Transfer-Encoding: chunked\r\n\r\n'
        b'1\r\nx\r\n0\r\nX-Trailer: \x0cvalue\r\n\r\n'
    )
    async with running_server(app, Config(port=0, lifespan='off')) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(request)
            await writer.drain()
            # A malformed trailer arrives after request dispatch, so the
            # transport closes rather than fabricating a second response.
            assert await asyncio.wait_for(reader.read(), timeout=5) == b''
        finally:
            writer.close()
            await writer.wait_closed()


async def test_http1_disconnect_on_aborted_upload() -> None:
    events = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
        try:
            while True:
                message = await receive()
                events.append(message['type'])
                if message['type'] == 'http.disconnect':
                    break
                if not message.get('more_body', False):
                    break
        except Exception as e:
            events.append(f'error: {e}')

    config = Config(port=0)
    async with running_server(app, config) as server:
        _reader, writer = await asyncio.open_connection(
            '127.0.0.1', server_port(server)
        )
        writer.write(
            f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\nContent-Length: 1000\r\n\r\npart1'.encode()
        )
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.1)

    assert events == ['http.request', 'http.disconnect']


async def test_http1_synchronous_app_failure_returns_500() -> None:
    def app(scope, receive, send):
        if scope['type'] == 'http':
            raise ValueError('Synchronous crash!')

        async def lifespan():
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return

        return lifespan()

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode()
        )
        await writer.drain()
        status, _headers, _body, _ = await asyncio.wait_for(
            read_http1_response(reader), timeout=5
        )
        writer.close()
        await writer.wait_closed()

    assert status == 500


async def test_http1_app_failure_does_not_wait_for_incomplete_upload() -> None:
    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
        raise RuntimeError('request failed before consuming the upload')

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\nContent-Length: 1000000\r\n\r\npart1'.encode()
        )
        await writer.drain()
        status, _headers, _body, _ = await asyncio.wait_for(
            read_http1_response(reader), timeout=5
        )
        assert status == 500
        assert await asyncio.wait_for(reader.read(), timeout=5) == b''
        writer.close()
        await writer.wait_closed()


async def test_http1_large_streaming_body_to_prevent_oom() -> None:
    events = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
        first_chunk = await receive()
        events.append(first_chunk['type'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\nContent-Length: 1000000\r\n\r\npart1'.encode()
        )
        await writer.drain()
        status, _headers, body, _ = await asyncio.wait_for(
            read_http1_response(reader), timeout=5
        )
        writer.close()
        await writer.wait_closed()

    assert status == 200
    assert body == b'ok'
    assert events == ['http.request']


async def test_http1_missing_host_header_returns_400() -> None:
    async def app(scope, receive, send):
        raise AssertionError('request should be rejected before the app runs')

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=b'GET / HTTP/1.1\r\nUser-Agent: test\r\n\r\n',
            ),
            timeout=5,
        )

    assert status == 400
    assert body == b''


async def test_http1_request_line_limit_returns_414() -> None:
    async def app(scope, receive, send):
        raise AssertionError('request line limit should reject before the app runs')

    config = Config(port=0, limit_request_line=16)
    async with running_server(app, config) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'GET /this-path-is-too-long HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 414
    assert body == b''


async def test_http1_header_field_count_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header field limit should reject before the app runs')

    config = Config(port=0, limit_request_fields=1)
    async with running_server(app, config) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\nX-One: 1\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_http1_header_field_size_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError(
            'header field size limit should reject before the app runs'
        )

    config = Config(port=0, limit_request_field_size=8)
    async with running_server(app, config) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\nX-Long: 123456789\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_http1_request_head_size_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError('request head limit should reject before the app runs')

    config = Config(port=0, limit_request_head_size=48)
    async with running_server(app, config) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'X-Long: 1234567890\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_http1_content_length_limit_returns_413() -> None:
    async def app(scope, receive, send):
        raise AssertionError(
            'request body size limit should reject before the app runs'
        )

    config = Config(port=0, max_request_body_size=4)
    async with running_server(app, config) as server:
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=server_port(server),
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\nContent-Length: 5\r\n\r\nhello'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 413
    assert body == b''


async def test_http1_body_limit_closes_connection_before_buffered_bytes_are_reparsed() -> (
    None
):
    seen = []

    async def app(scope, receive, send):
        seen.append((scope['method'], scope['path']))
        await read_http_request_body(receive)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    config = Config(port=0, max_request_body_size=4)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                (
                    f'POST /first HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n'
                    'Transfer-Encoding: chunked\r\n\r\n'
                    '2a\r\n'
                    f'GET /smuggled HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'
                    '0\r\n\r\n'
                ).encode()
            )
            await writer.drain()
            status, headers, body, _trailers = await asyncio.wait_for(
                read_http1_response(reader),
                timeout=5,
            )
            trailing = await asyncio.wait_for(reader.read(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 413
    assert headers[b'connection'] == b'close'
    assert body == b''
    assert trailing == b''
    assert seen == [('POST', '/first')]


async def test_upgrade_header_is_a_list_of_protocols() -> None:
    """
    RFC 9110 section 7.8 makes Upgrade a list in order of preference, so a
    client that also offers a protocol h2corn does not speak has still asked
    for the one it does.
    """

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert await receive() == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept'})
        await send({'type': 'websocket.close'})

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'GET /ws HTTP/1.1\r\n'
                b'Host: localhost\r\n'
                b'Connection: Upgrade\r\n'
                b'Upgrade: websocket, unknown/1.0\r\n'
                b'Sec-WebSocket-Version: 13\r\n'
                b'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n'
                b'\r\n'
            )
            await writer.drain()
            status, headers, _body, _trailers = await read_http1_response(reader)
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    assert status == 101
    assert headers[b'sec-websocket-accept'] == b's3pPLMBiTxaQ9kYGzzhZRbK+xOo='


async def test_head_with_trailers_writes_no_body_framing() -> None:
    """
    A response with no content has no trailer section to put trailers in.
    Waiting for them opened a chunked body, and HTTP/1 wrote a terminator
    and trailer lines into a response the client reads as bodyless — so the
    next response on a pipelined connection began mid-frame.
    """

    async def app(scope, receive, send):
        await send(
            {
                'type': 'http.response.start',
                'status': 200,
                'headers': [(b'trailer', b'x-finished')],
                'trailers': True,
            }
        )
        await send({'type': 'http.response.body', 'body': b'hello'})
        await send(
            {'type': 'http.response.trailers', 'headers': [(b'x-finished', b'yes')]}
        )

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            # HEAD then GET on one connection: any stray body framing from
            # the first shows up as corruption at the start of the second.
            writer.write(
                b'HEAD / HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n\r\n'
                b'GET / HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n'
                b'Connection: close\r\n\r\n'
            )
            await writer.drain()
            head_status, head_headers, head_body, head_trailers = (
                await read_http1_response(reader, head_only=True)
            )
            get_status, _get_headers, get_body, get_trailers = (
                await read_http1_response(reader)
            )
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    assert (head_status, head_body, head_trailers) == (200, b'', [])
    # The head still describes what a GET would return.
    assert head_headers.get(b'content-length') == b'5'
    assert b'transfer-encoding' not in head_headers
    # The GET that followed was read cleanly, so nothing leaked between them.
    assert (get_status, get_body) == (200, b'hello')
    assert get_trailers == [(b'x-finished', b'yes')]


@pytest.mark.parametrize(
    ('method', 'status', 'content_length'),
    [('HEAD', 200, b'8'), ('GET', 204, None), ('GET', 304, b'8')],
)
async def test_suppressed_pathsend_with_trailers_writes_no_body_framing(
    tmp_path: Path,
    method: str,
    status: int,
    content_length: bytes | None,
) -> None:
    file_path = tmp_path / 'head-pathsend.txt'
    payload = b'pathsend'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        if scope['path'] == '/second':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': payload})
            return
        await send(
            {
                'type': 'http.response.start',
                'status': status,
                'headers': [(b'trailer', b'x-finished')],
                'trailers': True,
            }
        )
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})
        await send(
            {'type': 'http.response.trailers', 'headers': [(b'x-finished', b'yes')]}
        )

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                method.encode()
                + b' / HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n\r\n'
                b'GET /second HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n'
                b'Connection: close\r\n\r\n'
            )
            await writer.drain()
            head_status, head_headers, head_body, head_trailers = (
                await read_http1_response(
                    reader, head_only=method == 'HEAD' or status == 304
                )
            )
            get_status, _get_headers, get_body, get_trailers = (
                await read_http1_response(reader)
            )
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    assert (head_status, head_body, head_trailers) == (status, b'', [])
    assert head_headers.get(b'content-length') == content_length
    assert b'transfer-encoding' not in head_headers
    assert (get_status, get_body) == (200, payload)
    assert get_trailers == []


async def test_rolling_pathsend_eof_closes_http1_connection(tmp_path: Path) -> None:
    file_path = tmp_path / 'truncated-pathsend.bin'
    file_path.write_bytes(b'x' * (900 * 1024))

    async def app(scope, _receive, send):
        if scope['path'] == '/second':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'SECOND-MARKER'})
            return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
            await writer.drain()
            head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=5)
            assert b'content-length: 921600\r\n' in head.lower()

            # The response has been admitted with the old length.  Truncate
            # only after its headers prove that admission happened.
            os.truncate(file_path, 0)
            writer.write(
                b'GET /second HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n'
            )
            await writer.drain()
            try:
                remainder = await asyncio.wait_for(reader.read(), timeout=5)
            except ConnectionResetError:
                remainder = b''
        finally:
            writer.close()

    assert b'HTTP/1.1 200 OK' not in remainder
    assert b'SECOND-MARKER' not in remainder


@pytest.mark.parametrize(
    ('status', 'content_length'),
    [(204, None), (304, b'0')],
)
async def test_http1_content_length_is_omitted_only_for_statuses_that_forbid_it(
    status: int,
    content_length: bytes | None,
) -> None:
    async def app(_scope, _receive, send):
        await send({
            'type': 'http.response.start',
            'status': status,
            'headers': [(b'content-length', b'7')],
        })
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        response_status, headers, body, trailers = await http1_request(
            port=server_port(server),
            request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
        )

    assert response_status == status
    assert headers.get(b'content-length') == content_length
    assert body == b''
    assert trailers == []


async def test_head_trailers_sent_after_the_response_completed_do_not_fail() -> None:
    """
    A HEAD response is complete once its headers are written, but the
    application declared trailers and is entitled to send them. Closing the
    stream at completion made that `send()` raise `SendAfterClose`, which
    killed the connection and took the next pipelined response with it.

    The race is made deterministic here: the application does not send its
    trailers until the test has already read the whole HEAD response, so the
    response is provably finished first.
    """
    head_response_read = asyncio.Event()
    trailer_send_error: list[str] = []

    async def app(scope, receive, send):
        await send(
            {
                'type': 'http.response.start',
                'status': 200,
                'headers': [(b'trailer', b'x-finished')],
                'trailers': True,
            }
        )
        await send({'type': 'http.response.body', 'body': b'hello'})
        if scope['method'] == 'HEAD':
            await head_response_read.wait()
        try:
            await send(
                {'type': 'http.response.trailers', 'headers': [(b'x-finished', b'yes')]}
            )
        except Exception as exc:
            trailer_send_error.append(f'{type(exc).__name__}: {exc}')

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(b'HEAD / HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n\r\n')
            await writer.drain()
            head_status, _headers, head_body, _trailers = await read_http1_response(
                reader, head_only=True
            )
            # The HEAD response is fully read, so the response has completed
            # before the application is released to send its trailers.
            head_response_read.set()

            writer.write(
                b'GET / HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n'
                b'Connection: close\r\n\r\n'
            )
            await writer.drain()
            get_status, _, get_body, get_trailers = await read_http1_response(reader)
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    assert (head_status, head_body) == (200, b'')
    assert trailer_send_error == [], 'declared trailers were refused after completion'
    # The connection survived, so the next request on it was answered.
    assert (get_status, get_body) == (200, b'hello')
    assert get_trailers == [(b'x-finished', b'yes')]
