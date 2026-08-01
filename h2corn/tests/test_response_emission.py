import asyncio
from contextlib import suppress

import h2.errors
import h2.events
import pytest
from h2corn import Config

from tests._support import (
    http1_request,
    open_h2_connection,
    read_http1_response,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio


async def _response(
    protocol: str,
    port: int,
    path: str = '/',
    *,
    method: str = 'GET',
    h1_close: bool = True,
    h1_bodyless: bool = False,
    h2_max_header_list_size: int | None = None,
) -> tuple[int, dict[bytes, bytes], bytes]:
    if protocol == 'h1':
        connection = b'Connection: close\r\n' if h1_close else b''
        status, headers, body, trailers = await http1_request(
            port=port,
            request=(
                f'{method} {path} HTTP/1.1\r\nHost: x\r\n'.encode()
                + connection
                + b'\r\n'
            ),
            head_only=h1_bodyless,
        )
        assert trailers == []
        return status, headers, body

    reader, writer, conn, authority = await open_h2_connection(
        port=port, max_header_list_size=h2_max_header_list_size
    )
    try:
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            stream_id,
            [
                (b':method', method.encode()),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', path.encode()),
            ],
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()
        status = None
        headers: dict[bytes, bytes] = {}
        body = bytearray()
        while True:
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            assert data, 'response stream closed before END_STREAM'
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    status = int(dict(event.headers)[b':status'])
                    headers = {
                        name: value for name, value in event.headers if name != b':status'
                    }
                elif isinstance(event, h2.events.DataReceived):
                    body.extend(event.data)
                    conn.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    assert event.stream_id == stream_id
                    assert status is not None
                    return status, headers, bytes(body)
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _send_h2_pair(
    *,
    port: int,
    first_path: bytes,
    second_path: bytes = b'/sibling',
    ping: bytes | None = None,
) -> tuple[dict[int, int], dict[int, bytes], set[int], set[bytes]]:
    reader, writer, conn, authority = await open_h2_connection(port=port)
    first = conn.get_next_available_stream_id()
    conn.send_headers(
        first,
        [
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', first_path),
        ],
        end_stream=True,
    )
    second = conn.get_next_available_stream_id()
    conn.send_headers(
        second,
        [
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', second_path),
        ],
        end_stream=True,
    )
    if ping is not None:
        conn.ping(ping)
    writer.write(conn.data_to_send())
    await writer.drain()

    statuses: dict[int, int] = {}
    bodies: dict[int, bytes] = {}
    chunks: dict[int, bytearray] = {first: bytearray(), second: bytearray()}
    resets: set[int] = set()
    ended: set[int] = set()
    ping_acks: set[bytes] = set()
    try:
        while (
            (first not in resets and first not in ended)
            or second not in ended
            or (ping is not None and ping not in ping_acks)
        ):
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            assert data, 'one failed response must not close an HTTP/2 connection'
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    statuses[event.stream_id] = int(dict(event.headers)[b':status'])
                elif isinstance(event, h2.events.DataReceived):
                    chunks[event.stream_id].extend(event.data)
                    conn.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    ended.add(event.stream_id)
                    bodies[event.stream_id] = bytes(chunks[event.stream_id])
                elif isinstance(event, h2.events.StreamReset):
                    resets.add(event.stream_id)
                elif isinstance(event, h2.events.ConnectionTerminated):
                    pytest.fail('one response error terminated the HTTP/2 connection')
                elif isinstance(event, h2.events.PingAckReceived):
                    ping_acks.add(event.ping_data)
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

    return statuses, bodies, resets, ping_acks


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_informational_start_is_rejected_and_caught_fallback_is_complete(
    protocol: str,
) -> None:
    caught = []

    async def app(scope, receive, send):
        try:
            await send({'type': 'http.response.start', 'status': 103, 'headers': []})
        except ValueError:
            caught.append(scope['path'])
            await send({'type': 'http.response.start', 'status': 418, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'fallback'})

    async with running_server(app, Config(port=0)) as server:
        status, _headers, body = await _response(protocol, server_port(server))

    assert caught == ['/']
    assert (status, body) == (418, b'fallback')


async def test_informational_start_does_not_desynchronise_http1_pipeline() -> None:
    seen = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        if scope['path'] == '/first':
            try:
                await send({'type': 'http.response.start', 'status': 103, 'headers': []})
            except ValueError:
                await send({'type': 'http.response.start', 'status': 418, 'headers': []})
                await send({'type': 'http.response.body', 'body': b'first'})
            return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'second'})

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'GET /first HTTP/1.1\r\nHost: x\r\n\r\n'
                b'GET /second HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n'
            )
            await writer.drain()
            first = await read_http1_response(reader)
            second = await read_http1_response(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    assert (first[0], first[2]) == (418, b'first')
    assert (second[0], second[2]) == (200, b'second')
    assert seen == ['/first', '/second']


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_streaming_declared_content_length_is_preserved_and_exact(
    protocol: str,
) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'Content-Type', b'text/plain'),
                (b'Content-Length', b'3'),
            ],
        })
        await send({'type': 'http.response.body', 'body': b'a', 'more_body': True})
        await send({'type': 'http.response.body', 'body': b'bc'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await _response(protocol, server_port(server))

    assert (status, body) == (200, b'abc')
    assert headers[b'content-type'] == b'text/plain'
    assert headers[b'content-length'] == b'3'
    if protocol == 'h1':
        assert b'transfer-encoding' not in headers
    else:
        assert all(name == name.lower() for name in headers)


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_application_transfer_encoding_is_absent_and_server_frames_response(
    protocol: str,
) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'transfer-encoding', b'gzip')],
        })
        await send({'type': 'http.response.body', 'body': b'a', 'more_body': True})
        await send({'type': 'http.response.body', 'body': b'bc'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await _response(protocol, server_port(server))

    assert (status, body) == (200, b'abc')
    assert b'transfer-encoding' not in headers or headers[b'transfer-encoding'] == b'chunked'
    if protocol == 'h1':
        assert headers[b'transfer-encoding'] == b'chunked'


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
@pytest.mark.parametrize(
    'content_lengths',
    [[b'0'], [b'1'], [b'1048576'], [b'0', b'1']],
)
async def test_205_replaces_every_application_content_length_with_zero(
    protocol: str, content_lengths: list[bytes]
) -> None:
    # 205 carries no content, but RFC 9112 section 6.3 does not make it
    # bodyless by status alone: without a length field the message is
    # delimited by connection close, which desynchronizes a keep-alive
    # connection. The transport owns the value and it is always zero.
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 205,
            'headers': [(b'content-length', value) for value in content_lengths],
        })
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await _response(protocol, server_port(server))

    assert (status, body) == (205, b'')
    assert headers[b'content-length'] == b'0'


async def test_205_does_not_desynchronize_a_pipelined_keep_alive_connection() -> None:
    async def app(scope, receive, send):
        status = 205 if scope['path'] == '/reset' else 200
        body = b'' if status == 205 else b'SECOND-MARKER'
        await send({
            'type': 'http.response.start',
            'status': status,
            'headers': [],
        })
        await send({'type': 'http.response.body', 'body': body})

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            b'GET /reset HTTP/1.1\r\nhost: localhost\r\n\r\n'
            b'GET /second HTTP/1.1\r\nhost: localhost\r\n\r\n'
        )
        await writer.drain()
        try:
            raw = await asyncio.wait_for(reader.readuntil(b'SECOND-MARKER'), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    first, _, second = raw.partition(b'\r\n\r\n')
    assert first.startswith(b'HTTP/1.1 205 ')
    assert b'content-length: 0' in first.lower()
    # Without framing on the 205, a client reads this entire response as the
    # 205's body and every later response on the connection is misparsed.
    assert second.startswith(b'HTTP/1.1 200 ')
    assert second.endswith(b'SECOND-MARKER')


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
@pytest.mark.parametrize(
    ('method', 'status'),
    [('GET', 204), ('GET', 205), ('GET', 304), ('HEAD', 200)],
)
async def test_bodyless_responses_never_emit_application_body(
    protocol: str, method: str, status: int
) -> None:
    payload = b'body that must not reach the peer'

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': status,
            'headers': [(b'content-length', str(len(payload)).encode())],
        })
        await send({'type': 'http.response.body', 'body': payload})

    async with running_server(app, Config(port=0)) as server:
        actual_status, _headers, body = await _response(
            protocol,
            server_port(server),
            method=method,
            h1_bodyless=method == 'HEAD' or status in {204, 205, 304},
        )

    assert (actual_status, body) == (status, b'')


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_response_accepts_a_one_megabyte_header_value(protocol: str) -> None:
    value = b'x' * (1 << 20)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'x-one-megabyte', value), (b'content-length', b'0')],
        })
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        if protocol == 'h1':
            reader, writer = await asyncio.open_connection(
                '127.0.0.1', server_port(server), limit=2 << 20
            )
            try:
                writer.write(b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n')
                await writer.drain()
                head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=5)
            finally:
                writer.close()
                await writer.wait_closed()
            assert b'x-one-megabyte: ' + value + b'\r\n' in head
        else:
            status, headers, body = await _response(
                protocol,
                server_port(server),
                h2_max_header_list_size=2 << 20,
            )
            assert (status, body) == (200, b'')
            assert headers[b'x-one-megabyte'] == value


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_streaming_response_drains_before_the_app_waits(protocol: str) -> None:
    release = asyncio.Event()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'first', 'more_body': True})
        await release.wait()
        await send({'type': 'http.response.body', 'body': b'second'})

    async with running_server(app, Config(port=0)) as server:
        port = server_port(server)
        if protocol == 'h1':
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            try:
                writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
                await writer.drain()
                head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=5)
                first = await asyncio.wait_for(reader.readexactly(10), timeout=5)
                release.set()
                tail = await asyncio.wait_for(reader.readexactly(16), timeout=5)
            finally:
                writer.close()
                await writer.wait_closed()

            assert b'transfer-encoding: chunked\r\n' in head.lower()
            assert first == b'5\r\nfirst\r\n'
            assert tail == b'6\r\nsecond\r\n0\r\n\r\n'
        else:
            reader, writer, conn, authority = await open_h2_connection(port=port)
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                stream_id,
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            status = None
            body = bytearray()
            try:
                while True:
                    data = await asyncio.wait_for(reader.read(65535), timeout=5)
                    assert data, 'response stream closed before END_STREAM'
                    for event in conn.receive_data(data):
                        if isinstance(event, h2.events.ResponseReceived):
                            status = int(dict(event.headers)[b':status'])
                        elif isinstance(event, h2.events.DataReceived):
                            body.extend(event.data)
                            conn.acknowledge_received_data(
                                event.flow_controlled_length, event.stream_id
                            )
                            if bytes(body) == b'first':
                                release.set()
                        elif isinstance(event, h2.events.StreamEnded):
                            assert event.stream_id == stream_id
                            break
                    else:
                        pending = conn.data_to_send()
                        if pending:
                            writer.write(pending)
                            await writer.drain()
                        continue
                    break
            finally:
                writer.close()
                await writer.wait_closed()

            assert status == 200
            assert body == b'firstsecond'


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_send_after_a_real_response_close_raises(protocol: str) -> None:
    attempt_late_send = asyncio.Event()
    late_send_finished = asyncio.Event()
    errors: list[str] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'complete'})
        await attempt_late_send.wait()
        try:
            await send({'type': 'http.response.body', 'body': b'must fail'})
        except OSError as error:
            errors.append(str(error))
        finally:
            late_send_finished.set()

    async with running_server(app, Config(port=0)) as server:
        port = server_port(server)
        if protocol == 'h1':
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            try:
                writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
                await writer.drain()
                status, _headers, body, trailers = await read_http1_response(reader)
                attempt_late_send.set()
                await asyncio.wait_for(late_send_finished.wait(), timeout=5)
            finally:
                writer.close()
                await writer.wait_closed()

            assert (status, body, trailers) == (200, b'complete', [])
        else:
            reader, writer, conn, authority = await open_h2_connection(port=port)
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                stream_id,
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            status = None
            body = bytearray()
            ended = False
            try:
                while not ended:
                    data = await asyncio.wait_for(reader.read(65535), timeout=5)
                    assert data, 'response stream closed before END_STREAM'
                    for event in conn.receive_data(data):
                        if isinstance(event, h2.events.ResponseReceived):
                            status = int(dict(event.headers)[b':status'])
                        elif isinstance(event, h2.events.DataReceived):
                            body.extend(event.data)
                            conn.acknowledge_received_data(
                                event.flow_controlled_length, event.stream_id
                            )
                        elif isinstance(event, h2.events.StreamEnded):
                            assert event.stream_id == stream_id
                            ended = True
                    pending = conn.data_to_send()
                    if pending:
                        writer.write(pending)
                        await writer.drain()
                attempt_late_send.set()
                await asyncio.wait_for(late_send_finished.wait(), timeout=5)
            finally:
                writer.close()
                await writer.wait_closed()

            assert (status, bytes(body)) == (200, b'complete')

    assert len(errors) == 1
    assert 'stream closed' in errors[0]


async def test_an_uncaught_late_send_is_not_reported_as_an_app_failure(
    capfd: pytest.CaptureFixture[str],
) -> None:
    """The server must not log the exception the server itself raised.

    A late send is deterministically after closure here -- the client has read
    the whole response before the app is released -- so `send()` really does
    raise. The application does not catch it, and ASGI still says this is not
    an application failure to report.
    """
    attempt_late_send = asyncio.Event()
    late_send_finished = asyncio.Event()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'complete'})
        await attempt_late_send.wait()
        try:
            # Uncaught on purpose: it propagates out of the app task.
            await send({'type': 'http.response.body', 'body': b'too late'})
        finally:
            late_send_finished.set()

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
            await writer.drain()
            status, _headers, body, _trailers = await read_http1_response(reader)
            attempt_late_send.set()
            await asyncio.wait_for(late_send_finished.wait(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert (status, body) == (200, b'complete')
    assert 'request failed:' not in capfd.readouterr().err


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_delayed_pathsend_is_not_lost(protocol: str, tmp_path) -> None:
    path = tmp_path / 'delayed-pathsend.txt'
    payload = b'delayed pathsend payload'
    path.write_bytes(payload)
    start_sent = asyncio.Event()
    release_pathsend = asyncio.Event()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        start_sent.set()
        await release_pathsend.wait()
        await send({'type': 'http.response.pathsend', 'path': str(path)})

    async with running_server(app, Config(port=0)) as server:
        port = server_port(server)
        if protocol == 'h1':
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            try:
                writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
                await writer.drain()
                await asyncio.wait_for(start_sent.wait(), timeout=5)
                release_pathsend.set()
                status, _headers, body, trailers = await read_http1_response(reader)
            finally:
                writer.close()
                await writer.wait_closed()

            assert (status, body, trailers) == (200, payload, [])
        else:
            reader, writer, conn, authority = await open_h2_connection(port=port)
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                stream_id,
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            await asyncio.wait_for(start_sent.wait(), timeout=5)
            release_pathsend.set()
            status = None
            body = bytearray()
            ended = False
            try:
                while not ended:
                    data = await asyncio.wait_for(reader.read(65535), timeout=5)
                    assert data, 'response stream closed before END_STREAM'
                    for event in conn.receive_data(data):
                        if isinstance(event, h2.events.ResponseReceived):
                            status = int(dict(event.headers)[b':status'])
                        elif isinstance(event, h2.events.DataReceived):
                            body.extend(event.data)
                            conn.acknowledge_received_data(
                                event.flow_controlled_length, event.stream_id
                            )
                        elif isinstance(event, h2.events.StreamEnded):
                            assert event.stream_id == stream_id
                            ended = True
                    pending = conn.data_to_send()
                    if pending:
                        writer.write(pending)
                        await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

            assert (status, bytes(body)) == (200, payload)


@pytest.mark.parametrize('protocol', ['h1', 'h2'])
async def test_invalid_response_header_can_be_caught_and_replaced(protocol: str) -> None:
    caught = []

    async def app(scope, receive, send):
        try:
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [(b'bad\r\nheader', b'value')],
            })
        except ValueError:
            caught.append(scope['path'])
            await send({'type': 'http.response.start', 'status': 418, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'replaced'})

    async with running_server(app, Config(port=0)) as server:
        status, _headers, body = await _response(protocol, server_port(server))

    assert caught == ['/']
    assert (status, body) == (418, b'replaced')


async def test_uncaught_invalid_response_header_is_complete_http1_500_and_closes() -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'bad\r\nheader', b'value')],
        })

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
            await writer.drain()
            status, _headers, body, trailers = await read_http1_response(reader)
            assert await asyncio.wait_for(reader.read(), timeout=5) == b''
        finally:
            writer.close()
            await writer.wait_closed()

    assert (status, body, trailers) == (500, b'', [])


async def test_uncaught_invalid_response_header_is_500_without_h2_sibling_reset() -> None:
    async def app(scope, receive, send):
        if scope['path'] == '/sibling':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'sibling survives'})
            return
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'bad\r\nheader', b'value')],
        })

    async with running_server(app, Config(port=0)) as server:
        statuses, bodies, resets, _ping_acks = await _send_h2_pair(
            port=server_port(server), first_path=b'/bad'
        )

    assert statuses == {1: 500, 3: 200}
    assert bodies == {1: b'', 3: b'sibling survives'}
    assert resets == set()


async def test_post_completion_event_is_ignored(capfd: pytest.CaptureFixture[str]) -> None:
    async def app(scope, receive, send):
        if scope['path'] == '/sibling':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'sibling survives'})
            return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'prefix', 'more_body': True})
        await send({'type': 'http.response.body', 'body': b'complete'})
        for _ in range(3):
            await send({'type': 'http.response.body', 'body': b'ignored'})

    ping = b'complete'
    async with running_server(app, Config(port=0)) as server:
        statuses, bodies, resets, ping_acks = await _send_h2_pair(
            port=server_port(server), first_path=b'/complete', ping=ping
        )

    assert statuses == {1: 200, 3: 200}
    assert bodies == {1: b'prefixcomplete', 3: b'sibling survives'}
    assert resets == set()
    assert ping_acks == {ping}
    assert 'request failed:' not in capfd.readouterr().err


@pytest.mark.parametrize('mismatch', ['short', 'long'])
async def test_streaming_content_length_mismatch_closes_http1(mismatch: str) -> None:
    seen = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-length', b'3')],
        })
        await send({'type': 'http.response.body', 'body': b'a', 'more_body': True})
        tail = b'b' if mismatch == 'short' else b'bcd'
        await send({'type': 'http.response.body', 'body': tail})

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'GET /bad HTTP/1.1\r\nHost: x\r\n\r\n'
                b'GET /sibling HTTP/1.1\r\nHost: x\r\n\r\n'
            )
            await writer.drain()
            head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=5)
            assert b'content-length: 3\r\n' in head.lower()
            remainder = await asyncio.wait_for(reader.read(), timeout=5)
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    assert remainder == b'a'
    assert seen == ['/bad']


@pytest.mark.parametrize('mismatch', ['short', 'long'])
async def test_streaming_content_length_mismatch_resets_only_h2_stream(
    mismatch: str,
) -> None:
    async def app(scope, receive, send):
        if scope['path'] == '/sibling':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'sibling survives'})
            return
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-length', b'3')],
        })
        await send({'type': 'http.response.body', 'body': b'a', 'more_body': True})
        tail = b'b' if mismatch == 'short' else b'bcd'
        await send({'type': 'http.response.body', 'body': tail})

    async with running_server(app, Config(port=0)) as server:
        statuses, bodies, resets, _ping_acks = await _send_h2_pair(
            port=server_port(server), first_path=b'/bad'
        )

    assert statuses[1] == 200
    assert statuses[3] == 200
    assert bodies[3] == b'sibling survives'
    assert resets == {1}


async def test_connection_close_finishes_first_http1_response_then_closes_pipeline() -> None:
    seen = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'connection', b'close')],
        })
        await send({'type': 'http.response.body', 'body': b'first'})

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'GET /first HTTP/1.1\r\nHost: x\r\n\r\n'
                b'GET /second HTTP/1.1\r\nHost: x\r\n\r\n'
            )
            await writer.drain()
            status, headers, body, trailers = await read_http1_response(reader)
            assert await asyncio.wait_for(reader.read(), timeout=5) == b''
        finally:
            writer.close()
            await writer.wait_closed()

    assert (status, body, trailers) == (200, b'first', [])
    assert headers[b'connection'] == b'close'
    assert seen == ['/first']


async def test_h2_dynamic_connection_option_rejects_before_csp_can_be_nominated() -> None:
    caught = []

    async def app(scope, receive, send):
        try:
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [
                    (b'connection', b'content-security-policy'),
                    (b'content-security-policy', b"default-src 'none'"),
                ],
            })
        except ValueError:
            caught.append(scope['path'])
            await send({'type': 'http.response.start', 'status': 418, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'rejected'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await _response('h2', server_port(server))

    assert caught == ['/']
    assert (status, body) == (418, b'rejected')
    assert b'content-security-policy' not in headers


async def test_http1_426_emits_paired_upgrade_advertisement_and_101_is_rejected() -> None:
    starts = []

    async def app(scope, receive, send):
        if scope['path'] == '/101':
            try:
                await send({'type': 'http.response.start', 'status': 101, 'headers': []})
            except ValueError:
                starts.append(101)
                await send({'type': 'http.response.start', 'status': 418, 'headers': []})
                await send({'type': 'http.response.body', 'body': b'no switch'})
            return
        await send({
            'type': 'http.response.start',
            'status': 426,
            'headers': [(b'connection', b'upgrade'), (b'upgrade', b'h2c')],
        })
        await send({'type': 'http.response.body', 'body': b'use h2c'})

    async with running_server(app, Config(port=0)) as server:
        status_426, headers_426, body_426 = await _response(
            'h1', server_port(server), h1_close=False
        )
        status_101, _headers_101, body_101 = await _response('h1', server_port(server), '/101')

    assert (status_426, body_426) == (426, b'use h2c')
    assert headers_426[b'connection'] == b'upgrade'
    assert headers_426[b'upgrade'] == b'h2c'
    assert starts == [101]
    assert (status_101, body_101) == (418, b'no switch')
