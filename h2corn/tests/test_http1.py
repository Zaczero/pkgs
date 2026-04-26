import asyncio
from pathlib import Path

import pytest
from h2corn import Config

from tests._support import (
    find_free_port,
    h2_request,
    http1_request,
    read_http1_response,
    read_http_request_body,
    running_server,
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
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
            f'{scope["scheme"]}|{scope["path"]}|'
            f'{scope["query_string"].decode()}'
        ).encode()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=b'GET https://example.com/absolute?x=1 HTTP/1.1\r\n\r\n',
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'content-length'] == b'18'
    assert body == b'http|/absolute|x=1'
    assert trailers == []


async def test_http1_response_defaults_apply_to_normal_app_responses() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=find_free_port(),
        date_header=True,
        response_headers=('x-extra: works',),
    )
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'ok'
    assert headers[b'x-extra'] == b'works'
    assert b'date' in headers
    assert trailers == []


async def test_http1_keep_alive_reuses_connection() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        try:
            writer.write(
                (
                    f'GET /one HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'
                    f'GET /two HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'
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


async def test_http1_keep_alive_request_head_still_honors_timeout_request_header() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    config = Config(
        port=find_free_port(),
        timeout_keep_alive=1.0,
        timeout_request_header=0.05,
    )
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        try:
            writer.write(
                f'GET /one HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode()
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

    config = Config(port=find_free_port(), timeout_request_header=0.05)
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        try:
            for part in (
                b'GET /slow HTTP/1.1\r\nHo',
                f'st: 127.0.0.1:{config.port}\r\nX-De'.encode(),
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
        raise AssertionError('stalled first request head should timeout before app dispatch')

    config = Config(port=find_free_port(), timeout_request_header=0.05)
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        try:
            for part in (
                b'GET /slow HTTP/1.1\r\nHo',
                f'st: 127.0.0.1:{config.port}\r\nX-De'.encode(),
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        _reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(b'GET / HTTP/1.1\r\nHost: 127.0.0.1')
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.05)

        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n'
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'PRI / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
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

    config = Config(port=find_free_port(), http1=False)
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode()
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'HEAD / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'HEAD / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n'
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n'
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        try:
            writer.write(
                (
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n'
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n'
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode(),
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'transfer-encoding'] == b'chunked'
    assert body == b'abcd'
    assert trailers == []


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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        request = '\r\n'.join([
            'POST /strict-framing HTTP/1.1',
            f'Host: 127.0.0.1:{config.port}',
            *request_lines,
            '',
            '',
        ]).encode()
        status, headers, body, trailers = await asyncio.wait_for(
            http1_request(port=config.port, request=request),
            timeout=5,
        )

    assert called is False
    assert status == 400
    assert body == b''
    assert trailers == []
    assert headers[b'content-length'] == b'0'


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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        _reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\nContent-Length: 1000\r\n\r\npart1'.encode()
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode()
        )
        await writer.drain()
        status, _headers, _body, _ = await asyncio.wait_for(
            read_http1_response(reader), timeout=5
        )
        writer.close()
        await writer.wait_closed()

    assert status == 500


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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\nContent-Length: 1000000\r\n\r\npart1'.encode()
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=b'GET / HTTP/1.1\r\nUser-Agent: test\r\n\r\n',
            ),
            timeout=5,
        )

    assert status == 400
    assert body == b''


async def test_http1_request_line_limit_returns_414() -> None:
    async def app(scope, receive, send):
        raise AssertionError('request line limit should reject before the app runs')

    config = Config(port=find_free_port(), limit_request_line=16)
    async with running_server(app, config):
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'GET /this-path-is-too-long HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 414
    assert body == b''


async def test_http1_header_field_count_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header field limit should reject before the app runs')

    config = Config(port=find_free_port(), limit_request_fields=1)
    async with running_server(app, config):
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\nX-One: 1\r\n\r\n'
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

    config = Config(port=find_free_port(), limit_request_field_size=8)
    async with running_server(app, config):
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\nX-Long: 123456789\r\n\r\n'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_http1_request_head_size_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError('request head limit should reject before the app runs')

    config = Config(port=find_free_port(), limit_request_head_size=48)
    async with running_server(app, config):
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n'
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

    config = Config(port=find_free_port(), max_request_body_size=4)
    async with running_server(app, config):
        status, _headers, body, _trailers = await asyncio.wait_for(
            http1_request(
                port=config.port,
                request=(
                    f'POST / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\nContent-Length: 5\r\n\r\nhello'
                ).encode(),
            ),
            timeout=5,
        )

    assert status == 413
    assert body == b''
