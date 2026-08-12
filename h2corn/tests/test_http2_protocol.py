import asyncio
import contextlib
import os
import sys
from pathlib import Path

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings
import pytest
from h2corn import Config, Server

import hpack
from tests._support import (
    find_free_port,
    h2_request,
    open_h2_connection,
    read_raw_h2_frames,
    running_server,
    server_port,
    wait_for_server,
)

pytestmark = pytest.mark.asyncio
SERVER_MAX_FRAME_SIZE = 64 * 1024


def _gil_is_disabled() -> bool:
    is_gil_enabled = getattr(sys, '_is_gil_enabled', None)
    return callable(is_gil_enabled) and not is_gil_enabled()


def _decode_h2_settings_payload(payload: bytes) -> dict[int, int]:
    if len(payload) % 6 != 0:
        raise ValueError('SETTINGS payload must be a sequence of 6-byte pairs')

    return {
        int.from_bytes(payload[offset : offset + 2], 'big'): int.from_bytes(
            payload[offset + 2 : offset + 6],
            'big',
        )
        for offset in range(0, len(payload), 6)
    }


def _encode_h2_frame(
    frame_type: int,
    payload: bytes = b'',
    *,
    flags: int = 0,
    stream_id: int = 0,
) -> bytes:
    return (
        len(payload).to_bytes(3, 'big')
        + bytes([frame_type, flags])
        + (stream_id & 0x7FFF_FFFF).to_bytes(4, 'big')
        + payload
    )


def _encode_h2_settings(
    settings: list[tuple[int, int]] | None = None,
    *,
    ack: bool = False,
) -> bytes:
    payload = (
        b''
        if settings is None
        else b''.join(
            setting_id.to_bytes(2, 'big') + value.to_bytes(4, 'big')
            for setting_id, value in settings
        )
    )
    return _encode_h2_frame(0x04, payload, flags=0x01 if ack else 0, stream_id=0)


async def _read_through_ping_ack(
    reader: asyncio.StreamReader,
    payload: bytes,
    *,
    timeout: float = 3.0,
) -> list[tuple[int, int, int, bytes]]:
    frames = []
    while True:
        header = await asyncio.wait_for(reader.readexactly(9), timeout=timeout)
        length = int.from_bytes(header[:3], 'big')
        frame_type = header[3]
        flags = header[4]
        stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
        frame_payload = await asyncio.wait_for(
            reader.readexactly(length), timeout=timeout
        )
        frames.append((frame_type, flags, stream_id, frame_payload))
        if frame_type == 0x06 and flags & 0x01 and frame_payload == payload:
            return frames


async def test_unknown_extension_frame_is_discarded_before_following_ping() -> None:
    async def app(_scope, _receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, h2_max_inbound_frame_size=SERVER_MAX_FRAME_SIZE)
    async with running_server(app, config) as server:
        reader, writer, _conn, _authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            payload = b'x' * SERVER_MAX_FRAME_SIZE
            unknown = _encode_h2_frame(0xF0, payload)
            ping = b'unknown!'
            writer.write(unknown[:17])
            await writer.drain()
            for offset in range(17, len(unknown), 4096):
                writer.write(unknown[offset : offset + 4096])
                await writer.drain()
            writer.write(_encode_h2_frame(0x06, ping))
            await writer.drain()

            frames = await _read_through_ping_ack(reader, ping)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x06 and flags & 0x01 and payload == ping
        for frame_type, flags, _stream_id, payload in frames
    )


async def _h2_expect_error(
    *,
    port: int,
    headers: list[tuple[bytes, bytes]],
    body: bytes = b'',
) -> tuple[str, int | None]:
    reader, writer, conn, _ = await open_h2_connection(port=port)
    try:
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, headers, end_stream=not body)
        if body:
            conn.send_data(stream_id, body, end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        while True:
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            if not data:
                return 'closed', None
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.StreamReset):
                    assert event.error_code is not None
                    return 'reset', int(event.error_code)
                if isinstance(event, h2.events.ConnectionTerminated):
                    assert event.error_code is not None
                    return 'goaway', int(event.error_code)
                if isinstance(event, h2.events.ResponseReceived):
                    return 'response', int(dict(event.headers)[b':status'])
                if isinstance(event, h2.events.DataReceived):
                    conn.acknowledge_received_data(
                        event.flow_controlled_length,
                        event.stream_id,
                    )
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _read_until_stream_end(
    reader: asyncio.StreamReader,
    *,
    stream_id: int = 1,
    timeout: float = 5.0,
) -> list[tuple[int, int, int, bytes]]:
    """Frames up to and including the end of `stream_id`.

    Terminal is END_STREAM on that stream, its RST_STREAM, or a connection
    GOAWAY -- every one a real event the server emits. Draining to an inactivity
    timeout instead pays that timeout on every green run, and proves less: a
    server that is merely slow looks exactly like one that is finished.

    A PING ACK is *not* a usable fence here. The HTTP/2 layer answers a PING
    before the application has run, so the ACK arrives ahead of the response it
    was supposed to be ordered behind.
    """
    frames: list[tuple[int, int, int, bytes]] = []
    while True:
        header = await asyncio.wait_for(reader.readexactly(9), timeout=timeout)
        length = int.from_bytes(header[:3], 'big')
        frame_type = header[3]
        flags = header[4]
        frame_stream = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
        payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
        frames.append((frame_type, flags, frame_stream, payload))
        if frame_type == 0x07:
            return frames
        if frame_stream == stream_id and (
            frame_type == 0x03 or (frame_type in (0x00, 0x01) and flags & 0x01)
        ):
            return frames


async def _raw_h2_request_frames(
    *,
    port: int,
    headers: list[tuple[bytes, bytes]],
    trailers: list[tuple[bytes, bytes]] | None = None,
) -> list[tuple[int, int, int, bytes]]:
    """Send raw HPACK so the server, rather than hyper-h2, owns validation."""
    reader, writer, _conn, _authority = await open_h2_connection(port=port)
    try:
        encoder = hpack.Encoder()
        head = encoder.encode(headers, huffman=False)
        flags = 0x04 | (0x01 if trailers is None else 0)
        payload = _encode_h2_frame(0x01, head, flags=flags, stream_id=1)
        if trailers is not None:
            payload += _encode_h2_frame(
                0x01,
                encoder.encode(trailers, huffman=False),
                flags=0x05,
                stream_id=1,
            )
        writer.write(payload)
        await writer.drain()
        return await _read_until_stream_end(reader)
    finally:
        writer.close()
        await writer.wait_closed()


def _h2_stream_protocol_error(frames: list[tuple[int, int, int, bytes]]) -> None:
    assert any(
        frame_type == 0x03
        and stream_id == 1
        and int.from_bytes(payload[:4], 'big')
        == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
        for frame_type, _flags, stream_id, payload in frames
    ), f'expected RST_STREAM(PROTOCOL_ERROR), saw {frames!r}'
    assert not any(
        frame_type == 0x01 and stream_id == 1
        for frame_type, _flags, stream_id, _payload in frames
    ), f'invalid request must not receive an HTTP response: {frames!r}'


def _h2_response_status(frames: list[tuple[int, int, int, bytes]]) -> int | None:
    decoder = hpack.Decoder()
    for frame_type, _flags, stream_id, payload in frames:
        if frame_type == 0x01 and stream_id == 1:
            headers = dict(decoder.decode(payload, raw=True))
            return int(headers[b':status'])
    return None


def _normal_h2_request(authority: bytes) -> list[tuple[bytes, bytes]]:
    return [
        (b':method', b'GET'),
        (b':scheme', b'http'),
        (b':authority', authority),
        (b':path', b'/'),
    ]


async def test_h2_request_target_and_host_grammar() -> None:
    seen = []

    async def app(scope, receive, send):
        seen.append((scope['method'], scope['path']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False, lifespan='off')
    async with running_server(app, config) as server:
        authority = f'127.0.0.1:{server_port(server)}'.encode()
        valid = await _raw_h2_request_frames(
            port=server_port(server), headers=_normal_h2_request(authority)
        )
        mismatched_host = await _raw_h2_request_frames(
            port=server_port(server),
            headers=[
                *_normal_h2_request(authority),
                (b'host', b'other.example'),
            ],
        )
        bare_connect = await _raw_h2_request_frames(
            port=server_port(server),
            headers=[(b':method', b'CONNECT'), (b':authority', b'example')],
        )

    assert _h2_response_status(valid) == 200
    assert seen == [('GET', '/')]
    _h2_stream_protocol_error(mismatched_host)
    _h2_stream_protocol_error(bare_connect)


@pytest.mark.parametrize(
    'headers',
    [
        [
            (b':method', b'GET'),
            (b':scheme', b'http!'),
            (b':authority', b'example.com'),
            (b':path', b'/'),
        ],
        [
            (b':method', b'GET'),
            (b':scheme', b' http'),
            (b':authority', b'example.com'),
            (b':path', b'/'),
        ],
        [
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', b'example.com'),
            (b':path', b'/'),
            (b'x-edge-ows', b'value '),
        ],
    ],
)
async def test_h2_scheme_and_edge_ows_are_rejected(
    headers: list[tuple[bytes, bytes]],
) -> None:
    async def app(scope, receive, send):
        raise AssertionError(f'invalid request reached app: {scope!r}')

    async with running_server(
        app, Config(port=0, access_log=False, lifespan='off')
    ) as server:
        frames = await _raw_h2_request_frames(port=server_port(server), headers=headers)

    _h2_stream_protocol_error(frames)


@pytest.mark.parametrize(
    'field',
    [
        (b'connection', b'close'),
        (b'proxy-connection', b'keep-alive'),
        (b'transfer-encoding', b'chunked'),
        (b'te', b'gzip'),
        (b'te', b'trailers, gzip'),
    ],
)
async def test_h2_connection_and_te_field_policy(
    field: tuple[bytes, bytes],
) -> None:
    async def app(scope, receive, send):
        raise AssertionError(f'forbidden H2 field reached app: {scope!r}')

    async with running_server(
        app, Config(port=0, access_log=False, lifespan='off')
    ) as server:
        frames = await _raw_h2_request_frames(
            port=server_port(server),
            headers=[
                (b':method', b'GET'),
                (b':scheme', b'http'),
                (b':authority', b'example.com'),
                (b':path', b'/'),
                field,
            ],
        )

    _h2_stream_protocol_error(frames)


@pytest.mark.parametrize(
    'trailer',
    [
        (b'content-length', b'0'),
        (b'host', b'replacement.example'),
        (b'authorization', b'Basic x'),
        (b'transfer-encoding', b'chunked'),
    ],
)
async def test_h2_request_trailers_obey_the_field_policy(
    trailer: tuple[bytes, bytes],
) -> None:
    async def app(scope, receive, send):
        # It may start before a trailing HEADERS block arrives, but the
        # forbidden trailer must end the stream rather than becoming a request
        # completion the application can consume.
        await receive()

    async with running_server(
        app, Config(port=0, access_log=False, lifespan='off')
    ) as server:
        frames = await _raw_h2_request_frames(
            port=server_port(server),
            headers=[
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', b'example.com'),
                (b':path', b'/'),
            ],
            trailers=[trailer],
        )

    _h2_stream_protocol_error(frames)


async def _start_blocked_request_server(
    *,
    status: int,
    body: bytes,
) -> tuple[
    Server,
    asyncio.Task[None],
    asyncio.Event,
    asyncio.StreamReader,
    asyncio.StreamWriter,
    h2.connection.H2Connection,
    int,
]:
    started = asyncio.Event()
    release = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            assert (await receive())['type'] == 'lifespan.startup'
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return
        started.set()
        await release.wait()
        await send({'type': 'http.response.start', 'status': status, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0, timeout_graceful_shutdown=2.0)
    server = Server(app, config)
    server_task = asyncio.create_task(server.serve())
    await wait_for_server(server, server_task)

    reader, writer, conn, authority = await open_h2_connection(port=server_port(server))
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
    await asyncio.wait_for(started.wait(), timeout=5)
    return server, server_task, release, reader, writer, conn, stream_id


async def test_h2_limit_concurrency_rejects_second_stream_with_503() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def app(scope, receive, send):
        if scope['path'] != '/slow':
            raise AssertionError(
                'concurrency rejection should happen before app dispatch'
            )
        started.set()
        await release.wait()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'slow'})

    config = Config(port=0, limit_concurrency=1)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            slow_stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                slow_stream_id,
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/slow'),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            await asyncio.wait_for(started.wait(), timeout=5)

            fast_stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                fast_stream_id,
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/fast'),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            fast_status = None
            fast_body = bytearray()
            fast_ended = False
            while not fast_ended:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.ResponseReceived):
                        if event.stream_id == fast_stream_id:
                            fast_status = int(dict(event.headers)[b':status'])
                    elif isinstance(event, h2.events.DataReceived):
                        conn.acknowledge_received_data(
                            event.flow_controlled_length,
                            event.stream_id,
                        )
                        if event.stream_id == fast_stream_id:
                            fast_body.extend(event.data)
                    elif isinstance(event, h2.events.StreamEnded):
                        fast_ended = event.stream_id == fast_stream_id
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            release.set()
            writer.close()
            await writer.wait_closed()

    assert fast_status == 503
    assert fast_body == b''


async def test_shutdown_drains_inflight_stream() -> None:
    (
        server,
        server_task,
        release,
        reader,
        writer,
        _conn,
        stream_id,
    ) = await _start_blocked_request_server(status=200, body=b'drained')

    server.shutdown()
    release.set()
    try:
        frames = await asyncio.wait_for(
            read_raw_h2_frames(reader, timeout=0.5, stop_at_goaway=False),
            timeout=5,
        )
    finally:
        writer.close()
        await writer.wait_closed()

    await asyncio.wait_for(server_task, timeout=5)
    status = None
    body = bytearray()
    trailers = []
    decoder = hpack.Decoder()
    for frame_type, _flags, frame_stream_id, payload in frames:
        if frame_stream_id != stream_id:
            continue
        if frame_type == 0x01:
            headers = decoder.decode(payload, raw=True)
            headers_map = dict(headers)
            if (raw_status := headers_map.get(b':status')) is not None:
                status = int(raw_status)
            else:
                trailers.extend(headers)
        elif frame_type == 0x00:
            body.extend(payload)
    assert status == 200
    assert bytes(body) == b'drained'
    assert trailers == []


async def test_shutdown_sends_goaway_before_releasing_inflight_stream() -> None:
    (
        server,
        server_task,
        release,
        reader,
        writer,
        conn,
        _,
    ) = await _start_blocked_request_server(status=204, body=b'')

    server.shutdown()
    try:
        while True:
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            if not data:
                raise AssertionError('connection closed before GOAWAY arrived')
            saw_goaway = False
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ConnectionTerminated):
                    assert event.error_code == 0
                    saw_goaway = True
                    continue
                if isinstance(
                    event, (h2.events.ResponseReceived, h2.events.DataReceived)
                ):
                    pytest.fail(
                        'response arrived before the blocked request was released'
                    )
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()
            if saw_goaway:
                break
    finally:
        release.set()
        writer.close()
        await writer.wait_closed()

    await asyncio.wait_for(server_task, timeout=5)


async def test_content_length_mismatch_is_rejected() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        kind, detail = await _h2_expect_error(
            port=server_port(server),
            headers=[
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', f'127.0.0.1:{server_port(server)}'.encode()),
                (b':path', b'/'),
                (b'content-length', b'0'),
            ],
            body=b'payload',
        )

    assert kind in {'reset', 'goaway', 'closed'}
    if kind == 'reset':
        assert detail == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)


async def test_short_content_length_cancels_app_suspended_away_from_receive() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def app(scope, receive, send):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    config = Config(port=0, lifespan='off', access_log=False)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            stream_id,
            [
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/'),
                (b'content-length', b'2'),
            ],
            end_stream=False,
        )
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.wait_for(started.wait(), timeout=5)

        conn.send_data(stream_id, b'x', end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        try:
            reset_code = None
            while reset_code is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data
                for event in conn.receive_data(data):
                    if (
                        isinstance(event, h2.events.StreamReset)
                        and event.stream_id == stream_id
                    ):
                        reset_code = int(event.error_code)
                        break
            assert reset_code == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
            await asyncio.wait_for(cancelled.wait(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()


async def test_incomplete_streaming_response_resets_stream() -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({
            'type': 'http.response.body',
            'body': b'partial',
            'more_body': True,
        })

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
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
        reset_code = None
        try:
            while reset_code is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                if not data:
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.ResponseReceived):
                        status = int(dict(event.headers)[b':status'])
                    elif isinstance(event, h2.events.DataReceived):
                        body.extend(event.data)
                        conn.acknowledge_received_data(
                            event.flow_controlled_length,
                            event.stream_id,
                        )
                    elif (
                        isinstance(event, h2.events.StreamReset)
                        and event.stream_id == stream_id
                    ):
                        reset_code = int(event.error_code)
                        break
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert bytes(body) in {b'', b'partial'}
    assert reset_code == int(h2.errors.ErrorCodes.INTERNAL_ERROR)


async def test_rolling_pathsend_eof_resets_only_that_stream(tmp_path: Path) -> None:
    file_path = tmp_path / 'truncated-pathsend.bin'
    file_path.write_bytes(b'x' * (900 * 1024))

    async def app(scope, _receive, send):
        if scope['path'] == '/second':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'second survives'})
            return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        first = conn.get_next_available_stream_id()
        conn.send_headers(
            first,
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

        first_headers_seen = False
        reset_code = None
        second_status = None
        second_body = bytearray()
        second = None
        try:
            while reset_code is None or second_body != b'second survives':
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data, 'rolling pathsend EOF must reset, not close HTTP/2'
                for event in conn.receive_data(data):
                    if (
                        isinstance(event, h2.events.ResponseReceived)
                        and event.stream_id == first
                        and not first_headers_seen
                    ):
                        first_headers_seen = True
                        # Headers prove the original fstat length was admitted;
                        # the small peer window keeps the rolling reader from
                        # consuming the whole file before this deterministic cut.
                        os.truncate(file_path, 0)
                        second = conn.get_next_available_stream_id()
                        conn.send_headers(
                            second,
                            [
                                (b':method', b'GET'),
                                (b':scheme', b'http'),
                                (b':authority', authority),
                                (b':path', b'/second'),
                            ],
                            end_stream=True,
                        )
                        conn.increment_flow_control_window(1 << 20)
                        conn.increment_flow_control_window(1 << 20, stream_id=first)
                    elif (
                        isinstance(event, h2.events.StreamReset)
                        and event.stream_id == first
                    ):
                        reset_code = int(event.error_code)
                    elif (
                        isinstance(event, h2.events.ResponseReceived)
                        and event.stream_id == second
                    ):
                        second_status = int(dict(event.headers)[b':status'])
                    elif isinstance(event, h2.events.DataReceived):
                        if event.stream_id == second:
                            second_body.extend(event.data)
                        conn.acknowledge_received_data(
                            event.flow_controlled_length, event.stream_id
                        )
                    elif isinstance(event, h2.events.ConnectionTerminated):
                        pytest.fail(
                            'rolling pathsend EOF terminated the HTTP/2 connection'
                        )
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    assert first_headers_seen
    assert reset_code == int(h2.errors.ErrorCodes.INTERNAL_ERROR)
    assert second_status == 200
    assert second_body == b'second survives'


async def test_stream_window_overrun_resets_only_that_stream() -> None:
    """A stream that overruns its own window is a stream error (RFC 9113 §5.2).

    Escalating it to GOAWAY took down every unrelated request multiplexed on
    the same connection.
    """

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            assert (await receive())['type'] == 'lifespan.startup'
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return
        # Never reads the body, so nothing replenishes the stream window.
        await asyncio.Future()

    config = Config(port=0, h2_initial_stream_window_size=65535)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        head = hpack.Encoder().encode([
            (b':method', b'POST'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/'),
        ])
        writer.write(_encode_h2_frame(0x01, head, flags=0x04, stream_id=1))
        # Ample connection credit, so only the stream window can be overrun.
        writer.write(_encode_h2_frame(0x08, (131070).to_bytes(4, 'big'), stream_id=0))
        await writer.drain()
        for _ in range(5):
            writer.write(_encode_h2_frame(0x00, b'x' * 16384, stream_id=1))
        await writer.drain()

        try:
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=True)
        finally:
            writer.close()
            await writer.wait_closed()

    flow_control_error = int(h2.errors.ErrorCodes.FLOW_CONTROL_ERROR)
    assert any(
        frame_type == 0x03
        and stream_id == 1
        and int.from_bytes(payload[:4], 'big') == flow_control_error
        for frame_type, _flags, stream_id, payload in frames
    ), 'the offending stream must be reset'
    assert not any(frame_type == 0x07 for frame_type, _f, _s, _p in frames), (
        'the connection must survive one stream overrunning its window'
    )


async def test_refused_field_block_ends_the_connection_not_just_the_stream() -> None:
    """HPACK's dynamic table is connection-wide.

    A block h2corn refuses to decode leaves its decoder behind the peer's
    encoder. Resetting only the stream let the *next* valid request fail with
    `COMPRESSION_ERROR`, with the application never seeing either request.
    """
    seen = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, h2_max_header_block_size=32)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        encoder = hpack.Encoder()
        oversized = encoder.encode([
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/first'),
            (b'x-demo', b'a' * 40),
        ])
        # Small only because it reuses the dynamic entry the refused block
        # inserted — the exact shape that used to desynchronise the decoder.
        follow_up = encoder.encode([
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/second'),
            (b'x-demo', b'a' * 40),
        ])
        assert len(oversized) > 32 and len(follow_up) < 32
        writer.write(_encode_h2_frame(0x01, oversized, flags=0x05, stream_id=1))
        writer.write(_encode_h2_frame(0x01, follow_up, flags=0x05, stream_id=3))
        await writer.drain()

        goaway_error = None
        reset_streams = []
        try:
            while True:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if header[3] == 0x03:
                    reset_streams.append(
                        int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                    )
                if header[3] == 0x07:
                    goaway_error = int.from_bytes(payload[4:8], 'big')
                    break
        finally:
            writer.close()
            await writer.wait_closed()

    # 0x9 is COMPRESSION_ERROR: the connection cannot continue once a field
    # block has gone undecoded. Resetting the stream and carrying on is the
    # regression — it reached the same GOAWAY, but only after a later, valid
    # request had already been corrupted by the drifted decoder.
    assert goaway_error == 0x9
    assert reset_streams == [], 'the refused block must not be answered stream-locally'
    assert seen == [], 'neither request may reach the application'


async def test_generic_connect_is_rejected_with_501() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        encoder = hpack.Encoder()
        headers = encoder.encode([
            (b':method', b'CONNECT'),
            (b':authority', authority),
        ])
        writer.write(_encode_h2_frame(0x01, headers, flags=0x05, stream_id=1))
        await writer.drain()

        try:
            while True:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                flags = header[4]
                stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x01 and stream_id == 1:
                    break
        finally:
            writer.close()
            await writer.wait_closed()

    assert frame_type == 0x01
    assert stream_id == 1
    assert flags & 0x01
    decoded_headers = dict(hpack.Decoder().decode(payload, raw=True))
    assert decoded_headers[b':status'] == b'501'


async def test_generic_connect_without_port_resets_stream_before_app_dispatch() -> None:
    """RFC 9113 CONNECT targets are host:port, not bare host names."""
    seen = []

    async def app(scope, receive, send):
        seen.append(scope)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0, access_log=False, lifespan='off')
    async with running_server(app, config) as server:
        reader, writer, _conn, _authority = await open_h2_connection(
            port=server_port(server)
        )
        headers = hpack.Encoder().encode([
            (b':method', b'CONNECT'),
            (b':authority', b'example'),
        ])
        writer.write(_encode_h2_frame(0x01, headers, flags=0x05, stream_id=1))
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=1, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x03
        and stream_id == 1
        and int.from_bytes(payload[:4], 'big')
        == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
        for frame_type, _flags, stream_id, payload in frames
    ), 'a bare CONNECT authority must receive RST_STREAM(PROTOCOL_ERROR)'
    assert not any(
        frame_type == 0x01 and stream_id == 1
        for frame_type, _flags, stream_id, _payload in frames
    ), 'the invalid CONNECT must not be translated to a 501 response'
    assert seen == [], 'the invalid CONNECT must not reach the application'


async def test_extended_connect_websocket_decodes_masked_h2_data_and_echoes() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive())['type'] == 'websocket.connect'
        await send({'type': 'websocket.accept'})
        message = await receive()
        assert message == {'type': 'websocket.receive', 'bytes': b'payload'}
        await send({'type': 'websocket.send', 'bytes': message['bytes']})
        await send({'type': 'websocket.close', 'code': 1000})

    config = Config(port=0, access_log=False, lifespan='off')
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        headers = hpack.Encoder().encode([
            (b':method', b'CONNECT'),
            (b':protocol', b'websocket'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/ws'),
            (b'sec-websocket-version', b'13'),
        ])
        mask = bytes.fromhex('37fa213d')
        payload = b'payload'
        websocket_frame = (
            bytes([0x82, 0x80 | len(payload)])
            + mask
            + bytes(byte ^ mask[index & 3] for index, byte in enumerate(payload))
        )
        writer.write(
            _encode_h2_frame(0x01, headers, flags=0x04, stream_id=1)
            + _encode_h2_frame(0x00, websocket_frame, stream_id=1)
        )
        await writer.drain()

        try:
            frames = await read_raw_h2_frames(reader, timeout=1.0, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    response_headers = next(
        payload
        for frame_type, _flags, stream_id, payload in frames
        if frame_type == 0x01 and stream_id == 1
    )
    decoded_headers = dict(hpack.Decoder().decode(response_headers, raw=True))
    assert decoded_headers[b':status'] == b'200'
    websocket_bytes = b''.join(
        payload
        for frame_type, _flags, stream_id, payload in frames
        if frame_type == 0x00 and stream_id == 1
    )
    assert websocket_bytes.startswith(b'\x82\x07payload')
    assert b'\x88\x02\x03\xe8' in websocket_bytes


async def test_max_concurrent_stream_limit_is_enforced() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'hello'})

    config = Config(port=0, max_concurrent_streams=1)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0})
        writer.write(conn.data_to_send())
        await writer.drain()

        first = conn.get_next_available_stream_id()
        conn.send_headers(
            first,
            [
                (b':method', b'GET'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/'),
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
                (b':path', b'/'),
            ],
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        reset_code = None
        try:
            while reset_code is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                if not data:
                    break
                for event in conn.receive_data(data):
                    if (
                        isinstance(event, h2.events.StreamReset)
                        and event.stream_id == second
                    ):
                        assert event.error_code is not None
                        reset_code = int(event.error_code)
                        break
                    if isinstance(event, h2.events.ConnectionTerminated):
                        assert event.error_code is not None
                        reset_code = int(event.error_code)
                        break
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    assert reset_code in {
        int(h2.errors.ErrorCodes.PROTOCOL_ERROR),
        int(h2.errors.ErrorCodes.REFUSED_STREAM),
    }


async def test_closed_stream_backlog_does_not_consume_concurrency_quota() -> None:
    release_apps = asyncio.Event()
    app_cancelled = asyncio.Event()
    second_started = asyncio.Event()
    third_started = asyncio.Event()

    async def app(scope, receive, send):
        if scope['path'] == '/first':
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})
            # Keep the request-input receiver alive without draining it so the
            # protocol-closed stream retains delivery backlog.
            try:
                await release_apps.wait()
            except asyncio.CancelledError:
                app_cancelled.set()
                raise
            return
        if scope['path'] == '/second':
            second_started.set()
        else:
            third_started.set()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'second'})
        try:
            await release_apps.wait()
        except asyncio.CancelledError:
            app_cancelled.set()
            raise

    config = Config(
        port=0,
        max_concurrent_streams=1,
        access_log=False,
        lifespan='off',
        timeout_keep_alive=0.05,
        timeout_graceful_shutdown=5,
    )
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        first = conn.get_next_available_stream_id()
        conn.send_headers(
            first,
            [
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/first'),
            ],
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        # Force response-close-first ordering before filling the app's bounded
        # request-input channel. More than its 32 entries is required to create
        # the retained backlog this regression is about.
        first_ended = False
        while not first_ended:
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            assert data
            first_ended = any(
                isinstance(event, h2.events.StreamEnded) and event.stream_id == first
                for event in conn.receive_data(data)
            )
        for index in range(40):
            conn.send_data(first, b'x', end_stream=index == 39)
        writer.write(conn.data_to_send())
        await writer.drain()

        second = conn.get_next_available_stream_id()
        conn.send_headers(
            second,
            [
                (b':method', b'GET'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/second'),
            ],
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        try:
            await asyncio.wait_for(second_started.wait(), timeout=5)
            second_ended = False
            while not second_ended:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data
                second_ended = any(
                    isinstance(event, h2.events.StreamEnded)
                    and event.stream_id == second
                    for event in conn.receive_data(data)
                )

            # One retained generation plus one just-closed pending app is the
            # explicit work budget at max_concurrent_streams=1. A third stream
            # is refused even though neither predecessor is protocol-active.
            third = conn.get_next_available_stream_id()
            conn.send_headers(
                third,
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/third'),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            reset_code = None
            while reset_code is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data
                for event in conn.receive_data(data):
                    if (
                        isinstance(event, h2.events.StreamReset)
                        and event.stream_id == third
                    ):
                        reset_code = event.error_code
            assert reset_code == int(h2.errors.ErrorCodes.REFUSED_STREAM)
            assert not third_started.is_set()

            server.shutdown()
            # Hold the drain open across two 0.05 s keep-alive periods so a
            # misfiring timer has a real window to cancel the apps, while the
            # 5 s graceful budget keeps the window far from its deadline.
            await asyncio.sleep(0.1)
        finally:
            release_apps.set()
            writer.close()
            await writer.wait_closed()

    # Released before the graceful deadline, both apps must have finished
    # uncancelled: keep-alive did not preempt server-timed drain work.
    assert not app_cancelled.is_set()


async def test_keep_alive_bounds_post_response_app_with_retained_body_backlog() -> None:
    app_cancelled = asyncio.Event()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})
        try:
            await asyncio.Future()
        finally:
            app_cancelled.set()

    config = Config(
        port=0,
        max_concurrent_streams=1,
        access_log=False,
        lifespan='off',
        timeout_keep_alive=0.1,
    )
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            stream_id,
            [
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/'),
            ],
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        response_ended = False
        while not response_ended:
            data = await asyncio.wait_for(reader.read(65535), timeout=2)
            assert data
            response_ended = any(
                isinstance(event, h2.events.StreamEnded)
                and event.stream_id == stream_id
                for event in conn.receive_data(data)
            )

        for index in range(40):
            conn.send_data(stream_id, b'x', end_stream=index == 39)
        writer.write(conn.data_to_send())
        await writer.drain()

        # The awaited cancellation alone proves keep-alive bounds the retained
        # app; probing "not yet cancelled" first would race the 0.1 s timer.
        await asyncio.wait_for(app_cancelled.wait(), timeout=2)

        writer.close()
        await writer.wait_closed()


async def test_client_must_send_settings_as_first_frame() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
            + _encode_h2_frame(0x06, b'\x00' * 8, stream_id=0)
        )
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    goaway = next(
        payload
        for frame_type, _flags, _stream_id, payload in frames
        if frame_type == 0x07
    )
    assert int.from_bytes(goaway[4:8], 'big') == int(
        h2.errors.ErrorCodes.PROTOCOL_ERROR
    )


async def test_server_settings_advertise_max_frame_size() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n' + _encode_h2_settings([]))
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    settings_payload = next(
        payload
        for frame_type, _flags, _stream_id, payload in frames
        if frame_type == 0x04 and payload
    )
    settings = _decode_h2_settings_payload(settings_payload)
    assert (
        settings[int(h2.settings.SettingCodes.MAX_FRAME_SIZE)] == SERVER_MAX_FRAME_SIZE
    )


async def test_server_settings_advertise_header_list_size() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(
        port=0,
        max_concurrent_streams=456,
        h2_max_header_list_size=123_456,
    )
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n' + _encode_h2_settings([]))
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    settings_payload = next(
        payload
        for frame_type, _flags, _stream_id, payload in frames
        if frame_type == 0x04 and payload
    )
    settings = _decode_h2_settings_payload(settings_payload)
    assert settings[int(h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS)] == 456
    assert settings[int(h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE)] == 123_456


async def test_invalid_ping_emits_goaway_after_valid_preface_and_settings() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
            + _encode_h2_settings([])
            + _encode_h2_frame(0x06, b'\x00' * 8, stream_id=1)
        )
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    goaway = next(
        payload
        for frame_type, _flags, _stream_id, payload in frames
        if frame_type == 0x07
    )
    assert int.from_bytes(goaway[4:8], 'big') == int(
        h2.errors.ErrorCodes.PROTOCOL_ERROR
    )


async def test_response_data_frames_respect_peer_max_frame_size() -> None:
    payload = b'x' * ((32 * 1024) + 4096)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        conn.update_settings({h2.settings.SettingCodes.MAX_FRAME_SIZE: 32 * 1024})
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
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    data_lengths = [
        len(frame_payload)
        for frame_type, _flags, frame_stream_id, frame_payload in frames
        if frame_type == 0x00 and frame_stream_id == stream_id
    ]
    assert data_lengths
    assert max(data_lengths) == 32 * 1024


async def test_response_data_frames_cap_at_server_target_when_peer_allows_more() -> (
    None
):
    payload = b'x' * (SERVER_MAX_FRAME_SIZE + 4096)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        conn.update_settings({
            h2.settings.SettingCodes.MAX_FRAME_SIZE: 1 << 20,
            h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 1 << 20,
        })
        conn.increment_flow_control_window(1 << 20)
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
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    data_lengths = [
        len(frame_payload)
        for frame_type, _flags, frame_stream_id, frame_payload in frames
        if frame_type == 0x00 and frame_stream_id == stream_id
    ]
    assert data_lengths
    assert max(data_lengths) == SERVER_MAX_FRAME_SIZE


async def test_streamed_multi_chunk_response_bytes_and_end_stream_placement() -> None:
    """Characterizes the vectored chunk emitter: a multi-chunk streamed body
    arrives byte-identical, every DATA frame respects the peer frame size,
    and END_STREAM lands exactly on the final DATA frame.
    """
    chunks = [b'a' * (20 * 1024), b'b' * (10 * 1024), b'', b'c' * 4]

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        for chunk in chunks[:-1]:
            await send({
                'type': 'http.response.body',
                'body': chunk,
                'more_body': True,
            })
        await send({'type': 'http.response.body', 'body': chunks[-1]})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        conn.update_settings({h2.settings.SettingCodes.MAX_FRAME_SIZE: 16 * 1024})
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
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    data_frames = [
        (flags, frame_payload)
        for frame_type, flags, frame_stream_id, frame_payload in frames
        if frame_type == 0x00 and frame_stream_id == stream_id
    ]
    assert data_frames
    assert b''.join(payload for _, payload in data_frames) == b''.join(chunks)
    assert all(len(payload) <= 16 * 1024 for _, payload in data_frames)
    end_flags = [bool(flags & 0x01) for flags, _ in data_frames]
    assert end_flags == [False] * (len(data_frames) - 1) + [True]


async def test_rapid_reset_flood_triggers_enhance_your_calm_goaway() -> None:
    """CVE-2023-44487 class guard: a client flooding HEADERS+RST_STREAM pairs
    is disconnected with GOAWAY ENHANCE_YOUR_CALM; a fresh well-behaved
    connection is unaffected.
    """

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        await receive()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        request_headers = [
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/'),
        ]
        try:
            for _ in range(400):
                stream_id = conn.get_next_available_stream_id()
                conn.send_headers(stream_id, request_headers, end_stream=True)
                conn.reset_stream(stream_id, error_code=0x8)  # CANCEL
                writer.write(conn.data_to_send())
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2.0)
        finally:
            writer.close()
            await writer.wait_closed()

        goaway_codes = [
            int.from_bytes(payload[4:8], 'big')
            for frame_type, _flags, _stream_id, payload in frames
            if frame_type == 0x07
        ]
        assert 0x0B in goaway_codes  # ENHANCE_YOUR_CALM

        # A fresh, well-behaved connection still gets served.
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )
        assert status == 200
        assert body == b'ok'


async def test_request_body_idle_timeout_only_resets_stalled_stream() -> None:
    async def app(scope, receive, send):
        if scope['path'] == '/slow':
            while True:
                message = await receive()
                assert message['type'] == 'http.request'
                if not message.get('more_body', False):
                    break
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})
            return

        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'fast'})

    config = Config(port=0, timeout_request_body_idle=0.1)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        slow_stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            slow_stream_id,
            [
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/slow'),
                (b'content-length', b'4'),
            ],
            end_stream=False,
        )
        conn.send_data(slow_stream_id, b'a', end_stream=False)
        writer.write(conn.data_to_send())
        await writer.drain()

        await asyncio.sleep(0.2)

        fast_stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            fast_stream_id,
            [
                (b':method', b'GET'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/fast'),
            ],
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        try:
            slow_reset = None
            fast_status = None
            fast_body = bytearray()

            while slow_reset is None or fast_status is None or fast_body != b'fast':
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.StreamReset):
                        if event.stream_id == slow_stream_id:
                            slow_reset = int(event.error_code)
                    elif isinstance(event, h2.events.ResponseReceived):
                        if event.stream_id == fast_stream_id:
                            fast_status = int(dict(event.headers)[b':status'])
                    elif isinstance(event, h2.events.DataReceived):
                        if event.stream_id == fast_stream_id:
                            fast_body.extend(event.data)
                        conn.acknowledge_received_data(
                            event.flow_controlled_length,
                            event.stream_id,
                        )
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    assert slow_reset == int(h2.errors.ErrorCodes.CANCEL)
    assert fast_status == 200
    assert fast_body == b'fast'


async def test_h2_header_block_size_limit_resets_stream() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header block limit should reject before the app runs')

    config = Config(port=0, h2_max_header_block_size=32)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        encoder = hpack.Encoder()
        block = encoder.encode([
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/'),
            (b'x-demo', b'abcdefghijklmnopqrstuvwxyz0123456789'),
        ])
        split_at = 16
        writer.write(
            _encode_h2_frame(0x01, block[:split_at], flags=0x01, stream_id=1)
            + _encode_h2_frame(0x09, block[split_at:], flags=0x04, stream_id=1)
        )
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x07
        and int.from_bytes(payload[4:8], 'big')
        == int(h2.errors.ErrorCodes.COMPRESSION_ERROR)
        for frame_type, _flags, _stream_id, payload in frames
    )
    assert not any(frame_type == 0x03 for frame_type, _f, _s, _p in frames)


async def test_h2_single_frame_header_block_size_limit_ends_the_connection() -> None:
    """A block the decoder never saw cannot be recovered from stream-locally.

    Resetting the stream and carrying on leaves HPACK's connection-wide table
    behind the peer's encoder, so a later valid request fails instead.
    """

    async def app(scope, receive, send):
        raise AssertionError('single-frame header block limit should reject early')

    config = Config(port=0, h2_max_header_block_size=32)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        block = hpack.Encoder().encode([
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/'),
            (b'x-demo', b'abcdefghijklmnopqrstuvwxyz0123456789'),
        ])
        writer.write(_encode_h2_frame(0x01, block, flags=0x05, stream_id=1))
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x07
        and int.from_bytes(payload[4:8], 'big')
        == int(h2.errors.ErrorCodes.COMPRESSION_ERROR)
        for frame_type, _flags, _stream_id, payload in frames
    )
    assert not any(frame_type == 0x03 for frame_type, _f, _s, _p in frames)


async def test_h2_header_field_limit_rejects_indexed_cookie_bomb() -> None:
    dispatched = False

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            assert (await receive())['type'] == 'lifespan.startup'
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return
        nonlocal dispatched
        dispatched = True
        raise AssertionError('header field limit should reject before scope build')

    config = Config(port=0, limit_request_fields=8)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        block = hpack.Encoder().encode(
            (
                (b':method', b'GET'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/'),
                *((b'cookie', b'a') for _ in range(9)),
            ),
            huffman=False,
        )
        writer.write(_encode_h2_frame(0x01, block, flags=0x05, stream_id=1))
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert not dispatched
    decoder = hpack.Decoder()
    assert any(
        frame_type == 0x01
        and stream_id == 1
        and dict(decoder.decode(payload, raw=True)).get(b':status') == b'431'
        for frame_type, _flags, stream_id, payload in frames
    )


async def test_h2_abandoned_header_fragment_ends_the_connection() -> None:
    """
    HPACK's dynamic table is connection-wide.  A field block that times out
    half-delivered is never fed to the decoder, but the peer's encoder already
    applied that block's insertions -- so the two tables are permanently out of
    step and every later block on the connection decodes against the wrong
    indices.  The connection must die with COMPRESSION_ERROR rather than
    survive as a connection whose next valid request fails mysteriously.

    One `hpack.Encoder` spans both streams here on purpose.  With a fresh
    encoder per block, no dynamic-table state crosses streams and the desync
    this guards against cannot occur at all.
    """

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'fast'})

    config = Config(port=0, timeout_request_header=0.05)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        encoder = hpack.Encoder()
        # Both blocks carry a header the static table cannot serve, so the
        # encoder inserts into its dynamic table for each of them.
        slow_block = encoder.encode([
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/slow'),
            (b'x-marker', b'slow-value'),
        ])
        # Encoded, not sent: it advances the peer encoder's dynamic table the
        # way a real client's next request would, which is what leaves the
        # server's decoder behind once the first block is abandoned.
        encoder.encode([
            (b':method', b'GET'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/fast'),
            (b'x-marker', b'fast-value'),
        ])
        writer.write(_encode_h2_frame(0x01, slow_block[:8], flags=0x01, stream_id=1))
        await writer.drain()

        goaway_error = None
        try:
            while goaway_error is None:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x07:
                    goaway_error = int.from_bytes(payload[4:8], 'big')
                elif frame_type == 0x03:
                    pytest.fail(
                        'the stalled stream was reset and the connection kept, '
                        'leaving the HPACK decoder behind the peer encoder'
                    )
            # The peer is told before the socket goes; a later block would
            # decode against a table the server never updated.
            assert not await asyncio.wait_for(reader.read(), timeout=5)
        finally:
            writer.close()
            await writer.wait_closed()

    assert goaway_error == int(h2.errors.ErrorCodes.COMPRESSION_ERROR)


async def test_h2_response_stall_timeout_resets_flow_control_blocked_stream() -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'x' * 1024})

    config = Config(
        port=0,
        h2_timeout_response_stall=0.05,
        timeout_keep_alive=10.0,
    )
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0})
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
        reset_code = None
        try:
            while reset_code is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                assert data
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.StreamReset):
                        reset_code = int(event.error_code)
        finally:
            writer.close()
            await writer.wait_closed()

    assert reset_code == int(h2.errors.ErrorCodes.CANCEL)


async def test_invalid_h2_preface_emits_goaway_protocol_error() -> None:
    async def app(scope, receive, send):
        raise AssertionError('invalid preface should fail before request dispatch')

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\rX')
        await writer.drain()
        try:
            header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
            assert header[3] == 0x07
            payload = await asyncio.wait_for(
                reader.readexactly(int.from_bytes(header[:3], 'big')),
                timeout=5,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert int.from_bytes(payload[4:8], 'big') == int(
        h2.errors.ErrorCodes.PROTOCOL_ERROR
    )


async def test_h2_inbound_frame_size_limit_ignores_larger_peer_setting() -> None:
    async def app(scope, receive, send):
        raise AssertionError(
            'oversized control frame should close before any request runs'
        )

    config = Config(port=0, h2_max_inbound_frame_size=16_384)
    async with running_server(app, config) as server:
        reader, writer, conn, _authority = await open_h2_connection(
            port=server_port(server)
        )
        await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
        conn.update_settings({h2.settings.SettingCodes.MAX_FRAME_SIZE: 32 * 1024})
        oversized_settings = [(0x01, 4096)] * 2731
        writer.write(conn.data_to_send() + _encode_h2_settings(oversized_settings))
        await writer.drain()
        try:
            frames = await read_raw_h2_frames(reader, timeout=5, stop_at_goaway=True)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x07
        and int.from_bytes(payload[4:8], 'big')
        == int(h2.errors.ErrorCodes.FRAME_SIZE_ERROR)
        for frame_type, _flags, _stream_id, payload in frames
    )


async def test_h2_padding_only_data_replenishes_flow_control_windows() -> None:
    async def app(scope, receive, send):
        await asyncio.sleep(2)

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)

        block = hpack.Encoder().encode([
            (b':method', b'POST'),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', b'/'),
        ])
        padded_data = _encode_h2_frame(
            0x00,
            bytes([255]) + (b'\0' * 255),
            flags=0x08,
            stream_id=1,
        )
        writer.write(
            _encode_h2_frame(0x01, block, flags=0x04, stream_id=1)
            + (padded_data * 32_769)
        )
        writer.write(_encode_h2_frame(0x06, b'padding!'))
        await writer.drain()
        try:
            # Fence on the PING rather than on silence: it is answered in
            # connection order, so its ACK proves the ~8 MB flood was consumed
            # and every WINDOW_UPDATE it earned is already in front of us. The
            # timeout is now only an error bound -- and the assertions below
            # stay real, because an ACK says nothing about window replenishment.
            frames = await _read_through_ping_ack(reader, b'padding!', timeout=15)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x08 and stream_id == 0
        for frame_type, _flags, stream_id, _payload in frames
    )
    assert any(
        frame_type == 0x08 and stream_id == 1
        for frame_type, _flags, stream_id, _payload in frames
    )


async def test_h2_receive_credit_waits_for_app_consumption_without_stalling_input() -> (
    None
):
    release = asyncio.Event()
    completed = asyncio.Event()
    received = 0

    async def app(scope, receive, send):
        nonlocal received
        await release.wait()
        while True:
            message = await receive()
            received += len(message.get('body', b''))
            if not message.get('more_body', False):
                break
        completed.set()
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, access_log=False, lifespan='off')
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
            block = hpack.Encoder().encode([
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/'),
            ])
            chunk = b'x' * 16_384
            first_ping = b'credit-1'
            writer.write(
                _encode_h2_settings(ack=True)
                + _encode_h2_frame(0x01, block, flags=0x04, stream_id=1)
                + b''.join(
                    _encode_h2_frame(0x00, chunk, stream_id=1) for _ in range(16)
                )
                + _encode_h2_frame(0x06, first_ping)
            )
            await writer.drain()
            first_frames = await _read_through_ping_ack(reader, first_ping)
            assert not any(frame_type == 0x08 for frame_type, *_ in first_frames)

            second_ping = b'credit-2'
            writer.write(
                b''.join(
                    _encode_h2_frame(
                        0x00,
                        chunk,
                        flags=0x01 if index == 15 else 0,
                        stream_id=1,
                    )
                    for index in range(16)
                )
                + _encode_h2_frame(0x06, second_ping)
            )
            await writer.drain()
            second_frames = await _read_through_ping_ack(reader, second_ping)
            assert not any(frame_type == 0x08 for frame_type, *_ in second_frames)

            release.set()
            await asyncio.wait_for(completed.wait(), timeout=3)
            assert received == 32 * len(chunk)
        finally:
            release.set()
            writer.close()
            await writer.wait_closed()


async def test_h2_backlogged_small_body_frames_are_coalesced_without_early_credit() -> (
    None
):
    release = asyncio.Event()
    completed = asyncio.Event()
    body_event_sizes = []

    async def app(scope, receive, send):
        await release.wait()
        while True:
            message = await receive()
            body = message.get('body', b'')
            if body:
                body_event_sizes.append(len(body))
            if not message.get('more_body', False):
                break
        completed.set()
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, access_log=False, lifespan='off')
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
            block = hpack.Encoder().encode([
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', authority),
                (b':path', b'/'),
            ])
            ping = b'bodybtch'
            writer.write(
                _encode_h2_settings(ack=True)
                + _encode_h2_frame(0x01, block, flags=0x04, stream_id=1)
                + b''.join(
                    _encode_h2_frame(
                        0x00,
                        bytes([index]),
                        flags=0x01 if index == 39 else 0,
                        stream_id=1,
                    )
                    for index in range(40)
                )
                + _encode_h2_frame(0x06, ping)
            )
            await writer.drain()

            frames = await _read_through_ping_ack(reader, ping)
            assert not any(frame_type == 0x08 for frame_type, *_ in frames)

            release.set()
            await asyncio.wait_for(completed.wait(), timeout=3)
            assert body_event_sizes == ([1] * 32) + [8]
        finally:
            release.set()
            writer.close()
            await writer.wait_closed()


async def test_h2_connection_close_response_header_is_stripped() -> None:
    """HTTP/2 cannot carry Connection; only HTTP/1 honours ``close``."""

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'connection', b'close')],
        })
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert (status, body) == (200, b'unreachable')


async def test_window_update_on_a_server_stream_id_is_a_protocol_error() -> None:
    """
    h2corn never initiates a stream, so every even id is idle forever and
    RFC 9113 section 5.1 makes a WINDOW_UPDATE for one a connection error.
    Treating them as closed instead let a peer make the writer allocate
    per-stream state for streams that could not exist.
    """

    async def app(scope, receive, send):
        raise AssertionError('no request is made')

    async with running_server(app, Config(port=0, access_log=False)) as server:
        reader, writer, _conn, _authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            # WINDOW_UPDATE, length 4, stream 2, increment 4096.
            writer.write(b'\x00\x00\x04\x08\x00\x00\x00\x00\x02\x00\x00\x10\x00')
            await writer.drain()
            frames = await read_raw_h2_frames(reader)
        finally:
            writer.close()

    goaway = [frame for frame in frames if frame[0] == 0x07]
    assert goaway, f'expected GOAWAY, saw {[frame[0] for frame in frames]}'
    assert int.from_bytes(goaway[-1][3][4:8], 'big') == 1  # PROTOCOL_ERROR


async def test_h2_priority_accepts_idle_self_dependency_without_reset() -> None:
    """RFC 9113 permits deprecated PRIORITY in every state, including idle."""

    async def app(scope, receive, send):
        raise AssertionError('PRIORITY must not create a request')

    async with running_server(app, Config(port=0, access_log=False)) as server:
        reader, writer, _conn, _authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
            ping = b'prio-idl'
            writer.write(
                _encode_h2_frame(
                    0x02,
                    (1).to_bytes(4, 'big') + b'\x00',
                    stream_id=1,
                )
                + _encode_h2_frame(0x06, ping)
            )
            await writer.drain()
            frames = await _read_through_ping_ack(reader, ping)
        finally:
            writer.close()
            await writer.wait_closed()

    assert not [frame for frame in frames if frame[0] in {0x03, 0x07}], frames


async def test_h2_malformed_priority_resets_only_its_stream() -> None:
    """A short PRIORITY is a stream FRAME_SIZE_ERROR, not connection GOAWAY."""
    stream_three_completed = asyncio.Event()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})
        if scope['path'] == '/three':
            stream_three_completed.set()

    async with running_server(app, Config(port=0, access_log=False)) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
            for stream_id, path in [(1, b'/one'), (3, b'/three')]:
                conn.send_headers(
                    stream_id,
                    [
                        (b':method', b'GET'),
                        (b':path', path),
                        (b':scheme', b'http'),
                        (b':authority', authority),
                    ],
                    end_stream=True,
                )
            writer.write(
                conn.data_to_send()
                + _encode_h2_frame(0x02, b'\x00\x00\x00\x00', stream_id=1)
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2.0, stop_at_goaway=False)
            await asyncio.wait_for(stream_three_completed.wait(), timeout=2.0)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x03
        and stream_id == 1
        and int.from_bytes(payload, 'big') == int(h2.errors.ErrorCodes.FRAME_SIZE_ERROR)
        for frame_type, _flags, stream_id, payload in frames
    ), frames
    assert not [frame for frame in frames if frame[0] == 0x07], frames


async def test_h2_malformed_priority_on_an_idle_stream_ends_the_connection() -> None:
    """
    RFC 9113 section 6.3 makes a wrong-length PRIORITY a stream error, but
    section 6.4 forbids sending RST_STREAM for a stream in the "idle" state and
    requires a peer that receives one to answer with a connection error.

    The stream-scoped answer is therefore only correct once the stream exists.
    On an idle stream -- any id the peer has not opened, including every
    even-numbered one -- h2corn used to emit RST_STREAM for a stream it had
    never seen, which a conforming client must tear the connection down over.
    """

    async def app(scope, receive, send):  # pragma: no cover - never reached
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0, access_log=False)) as server:
        reader, writer, conn, _authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
            # Stream 99 has never carried HEADERS, so it is idle.
            writer.write(
                conn.data_to_send()
                + _encode_h2_frame(0x02, b'\x00\x00\x00\x00', stream_id=99)
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2.0)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    assert not [frame for frame in frames if frame[0] == 0x03], (
        f'RST_STREAM must not be sent for an idle stream: {frames}'
    )
    assert [frame for frame in frames if frame[0] == 0x07], (
        f'an idle-stream frame-size violation ends the connection: {frames}'
    )


@pytest.mark.parametrize(
    ('frame_type', 'payload', 'name'),
    [
        (0x06, b'\x00' * 7, 'PING'),
        (0x03, b'\x00' * 3, 'RST_STREAM'),
        (0x08, b'\x00' * 3, 'WINDOW_UPDATE'),
        (0x04, b'\x00', 'SETTINGS'),
        (0x07, b'\x00' * 7, 'GOAWAY'),
    ],
)
async def test_h2_fixed_length_violation_on_a_live_stream_ends_the_connection(
    frame_type: int, payload: bytes, name: str
) -> None:
    """
    RFC 9113 scopes an invalid fixed length by frame type, not by stream.
    Sections 6.4, 6.5 and 6.7-6.9 make these connection errors even when the
    frame arrives carrying a live stream id; only PRIORITY (section 6.3) is a
    stream error, covered separately above.

    Answering with RST_STREAM instead left the connection open with its frame
    parser and the peer disagreeing about what had been consumed.
    """

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    async with running_server(app, Config(port=0, access_log=False)) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
            conn.send_headers(
                1,
                [
                    (b':method', b'GET'),
                    (b':path', b'/one'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                ],
                end_stream=True,
            )
            writer.write(
                conn.data_to_send() + _encode_h2_frame(frame_type, payload, stream_id=1)
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2.0, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    goaways = [frame for frame in frames if frame[0] == 0x07]
    assert goaways, f'a malformed {name} must end the connection: {frames}'
    assert int.from_bytes(goaways[0][3][4:8], 'big') == int(
        h2.errors.ErrorCodes.FRAME_SIZE_ERROR
    )
    # Exactly one: writing the error GOAWAY without going through the fatal
    # path left the loop running long enough to send a second GOAWAY(NO_ERROR)
    # contradicting it.
    assert len(goaways) == 1, f'one GOAWAY, not {len(goaways)}: {goaways}'


def _resident_kib(pid: int | None = None) -> int:
    status_path = '/proc/self/status' if pid is None else f'/proc/{pid}/status'
    with open(status_path) as status:
        for line in status:
            if line.startswith('VmRSS:'):
                return int(line.split()[1])
    raise AssertionError('VmRSS is not reported on this kernel')


@pytest.mark.skipif(sys.platform != 'linux', reason='reads /proc/self/status')
async def test_window_update_for_finished_streams_allocates_nothing() -> None:
    """
    An update for a stream that has ended may still be in flight and is
    ignored. Handing each to the writer created per-stream state for streams
    that were over, so one cheap request followed by a flood of updates for
    the ids below it retained megabytes: a few hundred KiB of frames bought
    tens of MiB. The server runs in this process, so its growth is ours.
    """
    # One request on a high id retires every lower one at a stroke.
    highest = 20_001
    updates = [(stream_id).to_bytes(4, 'big') for stream_id in range(1, highest, 2)]

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    async with running_server(app, Config(port=0, access_log=False)) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            conn.send_headers(
                highest,
                [
                    (b':method', b'GET'),
                    (b':path', b'/'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            while True:
                events = conn.receive_data(await reader.read(65536))
                if any(isinstance(event, h2.events.StreamEnded) for event in events):
                    break

            before = _resident_kib()
            writer.write(
                b''.join(
                    b'\x00\x00\x04\x08\x00' + stream + b'\x00\x00\x10\x00'
                    for stream in updates
                )
            )
            await writer.drain()
            writer.write(b'\x00\x00\x08\x06\x00\x00\x00\x00\x00' + b'h2corn!!')
            await writer.drain()
            # The PING is answered in connection order, so its ACK proves every
            # ignored update above was processed. Reading to it rather than
            # draining to a timeout also makes the answer itself the assertion:
            # a server that never ACKs raises here instead of passing quietly.
            frames = await _read_through_ping_ack(reader, b'h2corn!!', timeout=10)
            growth = _resident_kib() - before
        finally:
            writer.close()

    # The PING ack is the read's terminal condition above, so asserting it here
    # would be vacuous.
    assert not [frame for frame in frames if frame[0] == 0x07], 'unexpected GOAWAY'
    # Per-stream writer state ran to roughly 840 bytes, so the defect grew
    # this by several MiB. Ordinary buffer churn is far below the bound.
    assert growth < 4096, f'{len(updates)} ignored updates retained {growth} KiB'


@pytest.mark.skipif(
    sys.platform != 'linux' or _gil_is_disabled(),
    reason='requires a GIL build and /proc/self/status',
)
async def test_window_updates_after_local_half_close_retain_no_writer_state(
    tmp_path: Path,
) -> None:
    """A late update must not resurrect each sequential half-closed stream.

    Each request deliberately remains input-open while its header-only 204
    response closes the local side. The WINDOW_UPDATE then arrives in the
    exact legal half-closed state before empty DATA ends request input. A PING
    after every batch is a processing barrier: it rules out merely buffering
    the attack frames while claiming flat retention.
    """
    streams = 5_000
    # One request at a time is load-bearing: this leak evades concurrent-stream
    # accounting, so the test must not depend on any concurrent stream slot.
    batch = 1

    # Measure the server process, not this test process. The h2 client itself
    # keeps its closed-stream bookkeeping, which otherwise makes an RSS test
    # depend on interpreter allocator history rather than server retention.
    server_module = tmp_path / 'half_closed_measure_server.py'
    server_pid_path = tmp_path / 'half_closed_measure_server.pid'
    port = find_free_port()
    server_module.write_text(
        f"""
import asyncio
import os
from pathlib import Path

from h2corn import Config, Server

Path({os.fspath(server_pid_path)!r}).write_text(str(os.getpid()))

async def app(scope, receive, send):
    if scope['type'] != 'http':
        return
    await send({{'type': 'http.response.start', 'status': 204, 'headers': []}})
    await send({{'type': 'http.response.body', 'body': b''}})

if __name__ == '__main__':
    asyncio.run(Server(app, Config(port={port}, access_log=False, lifespan='off')).serve())
""".strip()
        + '\n'
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        os.fspath(server_module),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while not server_pid_path.exists():
            if loop.time() >= deadline:
                raise AssertionError('measurement server did not publish its PID')
            await asyncio.sleep(0.01)
        server_pid = int(server_pid_path.read_text())
        while True:
            try:
                probe_reader, probe_writer = await asyncio.open_connection(
                    '127.0.0.1', port
                )
            except OSError:
                if loop.time() >= deadline:
                    raise AssertionError('measurement server did not listen') from None
                await asyncio.sleep(0.01)
                continue
            probe_writer.close()
            await probe_writer.wait_closed()
            del probe_reader
            break

        async def exercise(send_updates: bool):
            reader, writer, conn, authority = await open_h2_connection(port=port)
            next_stream_id = 1
            try:
                await read_raw_h2_frames(reader, timeout=0.2, stop_at_goaway=False)
                for batch_start in range(0, streams, batch):
                    stream_ids = range(next_stream_id, next_stream_id + (batch * 2), 2)
                    for stream_id in stream_ids:
                        conn.send_headers(
                            stream_id,
                            [
                                (b':method', b'POST'),
                                (b':path', b'/half-close'),
                                (b':scheme', b'http'),
                                (b':authority', authority),
                            ],
                            end_stream=False,
                        )
                    writer.write(conn.data_to_send())
                    await writer.drain()

                    ended = set()
                    while len(ended) < batch:
                        data = await asyncio.wait_for(reader.read(1 << 16), timeout=3)
                        assert data, 'server closed before all header-only responses'
                        for event in conn.receive_data(data):
                            if isinstance(event, h2.events.StreamEnded):
                                ended.add(event.stream_id)
                        pending = conn.data_to_send()
                        if pending:
                            writer.write(pending)
                            await writer.drain()

                    ping = f'half{batch_start:04x}'.encode()
                    writer.write(
                        b''.join(
                            (
                                _encode_h2_frame(
                                    0x08,
                                    (4096).to_bytes(4, 'big'),
                                    stream_id=stream_id,
                                )
                                if send_updates
                                else b''
                            )
                            + _encode_h2_frame(0x00, flags=0x01, stream_id=stream_id)
                            for stream_id in stream_ids
                        )
                        + _encode_h2_frame(0x06, ping)
                    )
                    await writer.drain()
                    frames = await _read_through_ping_ack(reader, ping, timeout=3)
                    assert not [frame for frame in frames if frame[0] == 0x07], frames
                    next_stream_id += batch * 2
            except BaseException:
                writer.close()
                await writer.wait_closed()
                raise
            return reader, writer, conn

        # The reproduced writer resurrection retained about 0.49 KiB per
        # stream -- roughly 2.5 MiB here -- so this separates the defect by 2x
        # while staying a practical release gate.
        growth_budget_kib = 1280
        warmup_writers = []
        if sys.implementation.name != 'CPython':
            # The first structurally new passes compile JIT traces and grow
            # allocator arenas, and collection does not return them. Charging
            # that one-off cost to whichever pass runs first makes the
            # control/attack comparison meaningless -- successive no-update
            # passes measured roughly 25 MiB, then 9 MiB, then 1 MiB, none of
            # which is the defect. Warm until a no-update pass costs less than
            # the budget the attack itself must fit in, so the measurement
            # starts from a steady allocator. Hold every connection open, as
            # the control and attack ones are.
            for _ in range(4):
                warmup_before = _resident_kib(server_pid)
                _reader, warmup_writer, _conn = await exercise(False)
                warmup_writers.append(warmup_writer)
                if _resident_kib(server_pid) - warmup_before <= growth_budget_kib:
                    break

        before = _resident_kib(server_pid)
        control_reader, control_writer, control_conn = await exercise(False)
        after_control = _resident_kib(server_pid)
        attack_reader = attack_conn = None
        attack_writer = None
        try:
            attack_reader, attack_writer, attack_conn = await exercise(True)
            attack_growth = _resident_kib(server_pid) - after_control
        finally:
            control_writer.close()
            await control_writer.wait_closed()
            if attack_writer is not None:
                attack_writer.close()
                await attack_writer.wait_closed()
            for warmup_writer in warmup_writers:
                warmup_writer.close()
                await warmup_writer.wait_closed()
            # Keep both h2 clients alive through the measurement. Their equal
            # closed-stream bookkeeping is part of the matched control.
            del control_reader, control_conn, attack_reader, attack_conn
        control_growth = after_control - before
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                await process.wait()

    # Keep the identical no-update connection alive while exercising the
    # attack. That prevents allocator arenas from being returned between the
    # two runs, so `attack_growth` is the incremental cost of the late updates
    # rather than a noisy process-wide RSS movement.
    assert attack_growth <= growth_budget_kib, (
        f'half-closed updates added {attack_growth} KiB over {streams} streams '
        f'after a {control_growth} KiB matched control'
    )


@pytest.mark.skipif(sys.platform != 'linux', reason='reads /proc/self/status')
async def test_header_only_responses_retain_no_more_than_body_responses() -> None:
    """
    A response that ends with its HEADERS — empty, HEAD, 204, 304 — used to
    create writer state and never reach the flush pass that removes it, so it
    sat there for the life of the connection. Comparing against a response
    that does go through that pass needs no magic threshold: the two should
    cost the same, and the defect made the header-only one several times
    dearer.
    """
    batches, batch = 100, 100

    async def growth_over(body: bytes) -> int:
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': body})

        async with running_server(app, Config(port=0, access_log=False)) as server:
            reader, writer, conn, authority = await open_h2_connection(
                port=server_port(server)
            )
            next_stream = 1
            before = 0
            try:
                # One warm-up batch first, so arena growth is not counted.
                for round_index in range(batches + 1):
                    if round_index == 1:
                        before = _resident_kib()
                    ended = 0
                    for _ in range(batch):
                        conn.send_headers(
                            next_stream,
                            [
                                (b':method', b'GET'),
                                (b':path', b'/'),
                                (b':scheme', b'http'),
                                (b':authority', authority),
                            ],
                            end_stream=True,
                        )
                        next_stream += 2
                    writer.write(conn.data_to_send())
                    await writer.drain()
                    while ended < batch:
                        for event in conn.receive_data(await reader.read(1 << 20)):
                            if isinstance(event, h2.events.StreamEnded):
                                ended += 1
                return _resident_kib() - before
            finally:
                writer.close()

    # The body-bearing run goes first: it warms every allocator arena the
    # two share, so what the second run adds is attributable to it rather
    # than to whatever the process happened to do beforehand.
    with_body = await growth_over(b'x')
    header_only = await growth_over(b'')

    # Equal in principle; the defect made header-only several times dearer.
    # A body-bearing control can return allocator arenas and therefore report
    # negative RSS growth. It is not evidence that the header-only variant may
    # retain more; use zero as the floor before applying the measured noise
    # allowance.
    assert header_only <= max(with_body, 0) + 2048, (
        f'header-only responses retained {header_only} KiB against '
        f'{with_body} KiB for body-bearing ones'
    )


async def test_a_rejected_header_block_keeps_the_hpack_table_in_step() -> None:
    """
    The dynamic table is shared by every stream on the connection, so a block
    abandoned half-read leaves this decoder disagreeing with the peer's
    encoder for good: one stream answered 431 used to make the *next*,
    perfectly valid, stream fail with COMPRESSION_ERROR.
    """
    seen: list[str] = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False, limit_request_fields=8)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            # Over the field limit, and every field indexed so the peer's
            # encoder inserts them all into its dynamic table.
            conn.send_headers(
                1,
                [
                    (b':method', b'GET'),
                    (b':path', b'/rejected'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    *[(f'x-pad-{index}'.encode(), b'v') for index in range(24)],
                ],
                end_stream=True,
            )
            # A valid request on the same encoder context, which can only be
            # decoded if the table stayed in step through the rejection.
            conn.send_headers(
                3,
                [
                    (b':method', b'GET'),
                    (b':path', b'/valid'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                ],
                end_stream=True,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            statuses: dict[int, int] = {}
            terminated = False
            while len(statuses) < 2 and not terminated:
                for event in conn.receive_data(await reader.read(65536)):
                    if isinstance(event, h2.events.ResponseReceived):
                        statuses[event.stream_id] = int(dict(event.headers)[b':status'])
                    elif isinstance(event, h2.events.ConnectionTerminated):
                        terminated = True
        finally:
            writer.close()

    assert not terminated, 'the connection was torn down by the rejection'
    assert statuses[1] == 431
    assert statuses[3] == 200
    assert seen == ['/valid']


async def test_uppercase_header_resets_stream_without_breaking_hpack_table() -> None:
    """
    Uppercase names are stream PROTOCOL_ERROR, not compression errors.

    HPACK still inserts LiteralWithIndexing before the semantic reject, so a
    later stream can reuse subsequent dynamic entries without GOAWAY.
    """
    seen: list[str] = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            # Stream 1: insert uppercase X-Bad then a reusable x-shared entry.
            bad_block = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':path', b'/bad'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b'X-Bad', b'1'),
                    (b'x-shared', b'reused'),
                ],
                huffman=False,
            )
            # Stream 3: valid request reusing x-shared from the dynamic table.
            # Index only works if X-Bad was inserted (encoder and decoder agree).
            good_block = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':path', b'/good'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b'x-shared', b'reused'),
                ],
                huffman=False,
            )
            writer.write(
                _encode_h2_frame(0x01, bad_block, flags=0x05, stream_id=1)
                + _encode_h2_frame(0x01, good_block, flags=0x05, stream_id=3)
            )
            await writer.drain()

            reset_code: int | None = None
            status_good: int | None = None
            goaway = False
            # One decoder per connection, as a real client keeps.
            response_decoder = hpack.Decoder()
            while reset_code is None or status_good is None:
                data = await asyncio.wait_for(reader.read(65536), timeout=5)
                assert data, 'connection closed early'
                offset = 0
                while offset + 9 <= len(data):
                    length = int.from_bytes(data[offset : offset + 3], 'big')
                    frame_type = data[offset + 3]
                    flags = data[offset + 4]
                    stream_id = int.from_bytes(data[offset + 5 : offset + 9], 'big')
                    payload = data[offset + 9 : offset + 9 + length]
                    offset += 9 + length
                    if frame_type == 0x03 and stream_id == 1:
                        reset_code = int.from_bytes(payload[:4], 'big')
                    elif frame_type == 0x01 and stream_id == 3 and flags & 0x04:
                        headers = dict(response_decoder.decode(payload, raw=True))
                        status_good = int(headers[b':status'])
                    elif frame_type == 0x07:
                        goaway = True
        finally:
            writer.close()
            await writer.wait_closed()

    assert not goaway, 'uppercase field must not tear down the connection'
    assert reset_code == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
    assert status_good == 200
    assert seen == ['/good']


async def test_hpack_static_dynamic_and_never_indexed_match_python_reference() -> None:
    """Differential: server accepts blocks the Python HPACK encoder produces."""
    seen_auth: list[str] = []

    async def app(scope, receive, send):
        headers = {
            name.decode('latin1'): value.decode('latin1')
            for name, value in scope['headers']
        }
        if 'authorization' in headers:
            seen_auth.append(headers['authorization'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            # Static-indexed :method GET (index 2 → 0x82) mixed with never-indexed
            # authorization and a dynamic-indexed custom field.
            first = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':path', b'/one'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b'authorization', b'Bearer first'),
                    (b'x-dyn', b'v1'),
                ],
                huffman=False,
            )
            # Second block reuses x-dyn from the dynamic table (indexed).
            second = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':path', b'/two'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b'authorization', b'Bearer second'),
                    (b'x-dyn', b'v1'),
                ],
                huffman=False,
            )
            # Never-indexed must not collapse to a single-byte dynamic index.
            assert second[-1:] != bytes([0xBE]) or b'authorization' not in second
            writer.write(
                _encode_h2_frame(0x01, first, flags=0x05, stream_id=1)
                + _encode_h2_frame(0x01, second, flags=0x05, stream_id=3)
            )
            await writer.drain()

            statuses: dict[int, int] = {}
            # One decoder for the whole connection, as a real client keeps:
            # the server's response encoder indexes against a dynamic table
            # that persists across responses, so a fresh decoder per frame
            # cannot resolve an entry the previous response inserted.
            response_decoder = hpack.Decoder()
            while len(statuses) < 2:
                data = await asyncio.wait_for(reader.read(65536), timeout=5)
                assert data
                offset = 0
                while offset + 9 <= len(data):
                    length = int.from_bytes(data[offset : offset + 3], 'big')
                    frame_type = data[offset + 3]
                    flags = data[offset + 4]
                    stream_id = int.from_bytes(data[offset + 5 : offset + 9], 'big')
                    payload = data[offset + 9 : offset + 9 + length]
                    offset += 9 + length
                    if frame_type == 0x01 and flags & 0x04 and stream_id in {1, 3}:
                        headers = dict(response_decoder.decode(payload, raw=True))
                        statuses[stream_id] = int(headers[b':status'])
                    elif frame_type == 0x07:
                        raise AssertionError(
                            'unexpected GOAWAY during differential test'
                        )
        finally:
            writer.close()
            await writer.wait_closed()

    assert statuses == {1: 200, 3: 200}
    # Sorted: the two streams are dispatched concurrently and h2corn promises
    # no order between them. What matters is that the never-indexed value of
    # each block survived decoding intact.
    assert sorted(seen_auth) == ['Bearer first', 'Bearer second']


async def test_header_budget_then_bad_hpack_is_compression_error() -> None:
    """HTTP budget rejection must not suppress a later compression error."""

    async def app(scope, receive, send):
        raise AssertionError('should not run')

    config = Config(port=0, access_log=False, limit_request_fields=1)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            block = bytearray(
                encoder.encode(
                    [
                        (b':method', b'GET'),
                        (b':path', b'/'),
                        (b':scheme', b'http'),
                        (b':authority', authority),
                        (b'x0', b'1'),
                        (b'x1', b'1'),
                    ],
                    huffman=False,
                )
            )
            # Indexed representation with index 0 is never valid HPACK.
            block.append(0x80)
            writer.write(_encode_h2_frame(0x01, bytes(block), flags=0x05, stream_id=1))
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=0.5, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x07
        and int.from_bytes(payload[4:8], 'big')
        == int(h2.errors.ErrorCodes.COMPRESSION_ERROR)
        for frame_type, _flags, _stream_id, payload in frames
    )


async def test_fragmented_trailers_one_continuation_delivers_request_end() -> None:
    """Trailers split HEADERS + one CONTINUATION complete the request cleanly."""
    bodies: list[bytes] = []

    async def app(scope, receive, send):
        body = bytearray()
        more_body = True
        while more_body:
            event = await receive()
            assert event['type'] == 'http.request'
            body.extend(event.get('body', b''))
            more_body = event.get('more_body', False)
        bodies.append(bytes(body))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            head = encoder.encode(
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/trailers'),
                    (b'content-length', b'4'),
                ],
                huffman=False,
            )
            trailers = encoder.encode([(b'x-checksum', b'abcd')], huffman=False)
            assert len(trailers) >= 2
            split_at = max(1, len(trailers) // 2)
            writer.write(
                _encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + _encode_h2_frame(0x00, b'data', flags=0x00, stream_id=1)
                + _encode_h2_frame(0x01, trailers[:split_at], flags=0x01, stream_id=1)
                + _encode_h2_frame(0x09, trailers[split_at:], flags=0x04, stream_id=1)
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert bodies == [b'data']
    assert not any(frame_type == 0x03 for frame_type, *_ in frames)
    assert not any(frame_type == 0x07 for frame_type, *_ in frames)
    decoder = hpack.Decoder()
    assert any(
        frame_type == 0x01
        and stream_id == 1
        and dict(decoder.decode(payload, raw=True)).get(b':status') == b'200'
        for frame_type, _flags, stream_id, payload in frames
    )


async def test_fragmented_trailers_several_continuations_delivers_request_end() -> None:
    """Trailers spanning several CONTINUATION frames still end the request."""
    done = asyncio.Event()

    async def app(scope, receive, send):
        more_body = True
        while more_body:
            event = await receive()
            assert event['type'] == 'http.request'
            more_body = event.get('more_body', False)
        done.set()
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            head = encoder.encode(
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/multi'),
                ],
                huffman=False,
            )
            trailers = encoder.encode(
                [(b'x-a', b'1'), (b'x-b', b'2'), (b'x-c', b'3')],
                huffman=False,
            )
            assert len(trailers) >= 3
            third = len(trailers) // 3
            writer.write(
                _encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + _encode_h2_frame(0x00, b'z', flags=0x00, stream_id=1)
                + _encode_h2_frame(0x01, trailers[:third], flags=0x01, stream_id=1)
                + _encode_h2_frame(
                    0x09, trailers[third : 2 * third], flags=0x00, stream_id=1
                )
                + _encode_h2_frame(0x09, trailers[2 * third :], flags=0x04, stream_id=1)
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert done.is_set()
    assert not any(frame_type == 0x03 for frame_type, *_ in frames)
    assert not any(frame_type == 0x07 for frame_type, *_ in frames)


async def test_fragmented_trailers_without_end_stream_resets_stream_keeps_hpack() -> (
    None
):
    """Missing END_STREAM is a stream error only after the full block is decoded."""
    seen: list[str] = []

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        seen.append(scope['path'])
        # Do not block on body completion: a trailer PROTOCOL_ERROR finalizes
        # the stream; the HPACK property is the follow-up request.
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            head = encoder.encode(
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/first'),
                ],
                huffman=False,
            )
            trailers = encoder.encode(
                [(b'x-shared', b'reused-value')],
                huffman=False,
            )
            split_at = max(1, len(trailers) // 2)
            # END_STREAM is intentionally absent from the trailer HEADERS.
            writer.write(
                _encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + _encode_h2_frame(0x00, b'x', flags=0x00, stream_id=1)
                + _encode_h2_frame(0x01, trailers[:split_at], flags=0x00, stream_id=1)
                + _encode_h2_frame(0x09, trailers[split_at:], flags=0x04, stream_id=1)
            )
            await writer.drain()

            reset_code = None
            while reset_code is None:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x03 and stream_id == 1:
                    reset_code = int.from_bytes(payload[:4], 'big')
                assert frame_type != 0x07, (
                    'connection must survive a stream-local trailer error'
                )

            follow = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/second'),
                    (b'x-shared', b'reused-value'),
                ],
                huffman=False,
            )
            writer.write(_encode_h2_frame(0x01, follow, flags=0x05, stream_id=3))
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert reset_code == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
    assert '/second' in seen
    assert not any(frame_type == 0x07 for frame_type, *_ in frames)


async def test_trailer_budget_rejection_then_dynamic_index_reuse() -> None:
    """Trailer field-count reject still inserts; the next stream may index them."""
    seen: list[str] = []

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        seen.append(scope['path'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False, limit_request_fields=2)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            head = encoder.encode(
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/trailers'),
                ],
                huffman=False,
            )
            trailers = encoder.encode(
                [
                    (b'x-a', b'1'),
                    (b'x-b', b'2'),
                    (b'x-c', b'3'),
                    (b'x-shared', b'reused'),
                ],
                huffman=False,
            )
            writer.write(
                _encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + _encode_h2_frame(0x00, b'x', flags=0x00, stream_id=1)
                + _encode_h2_frame(0x01, trailers, flags=0x05, stream_id=1)
            )
            await writer.drain()

            reset_code = None
            while reset_code is None:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x03 and stream_id == 1:
                    reset_code = int.from_bytes(payload[:4], 'big')
                assert frame_type != 0x07

            follow = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/next'),
                    (b'x-shared', b'reused'),
                ],
                huffman=False,
            )
            writer.write(_encode_h2_frame(0x01, follow, flags=0x05, stream_id=3))
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=False)
        finally:
            writer.close()
            await writer.wait_closed()

    assert reset_code == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
    assert '/next' in seen
    assert not any(frame_type == 0x07 for frame_type, *_ in frames)


async def test_refused_new_stream_insertion_then_successful_indexed_request() -> None:
    """A concurrency-refused block is still decoded so later streams can index it."""
    release = asyncio.Event()
    seen: list[str] = []

    async def app(scope, receive, send):
        seen.append(scope['path'])
        if scope['path'] == '/hold':
            await release.wait()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False, max_concurrent_streams=1)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            hold = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/hold'),
                ],
                huffman=False,
            )
            refused = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/refused'),
                    (b'x-shared', b'from-refused'),
                ],
                huffman=False,
            )
            writer.write(
                _encode_h2_frame(0x01, hold, flags=0x05, stream_id=1)
                + _encode_h2_frame(0x01, refused, flags=0x05, stream_id=3)
            )
            await writer.drain()

            reset_code = None
            while reset_code is None:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x03 and stream_id == 3:
                    reset_code = int.from_bytes(payload[:4], 'big')
                assert frame_type != 0x07

            release.set()
            # Drain stream 1 response so the concurrency slot frees.
            while True:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                _payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x01 and stream_id == 1:
                    break

            follow = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/indexed'),
                    (b'x-shared', b'from-refused'),
                ],
                huffman=False,
            )
            writer.write(_encode_h2_frame(0x01, follow, flags=0x05, stream_id=5))
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=False)
        finally:
            release.set()
            writer.close()
            await writer.wait_closed()

    assert reset_code == int(h2.errors.ErrorCodes.REFUSED_STREAM)
    assert '/indexed' in seen
    assert '/refused' not in seen
    # Application dispatch of /indexed is the HPACK-alignment signal: a
    # drifted decoder would GOAWAY(COMPRESSION_ERROR) before the app ran.
    assert not any(frame_type == 0x07 for frame_type, *_ in frames)
    assert not any(
        frame_type == 0x03 and stream_id == 5
        for frame_type, _f, stream_id, _p in frames
    )


async def test_tracked_rejected_stream_insertion_then_successful_request() -> None:
    """HEADERS on a request-closed stream still feed HPACK before STREAM_CLOSED."""
    seen: list[str] = []
    first_done = asyncio.Event()
    first_release = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        seen.append(scope['path'])
        if scope['path'] == '/closed':
            event = await receive()
            assert event['type'] == 'http.request'
            assert not event.get('more_body', False)
            first_done.set()
            await first_release.wait()
            return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            head = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/closed'),
                ],
                huffman=False,
            )
            writer.write(_encode_h2_frame(0x01, head, flags=0x05, stream_id=1))
            await writer.drain()
            await asyncio.wait_for(first_done.wait(), timeout=5)

            late = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/late'),
                    (b'x-shared', b'from-tracked'),
                ],
                huffman=False,
            )
            writer.write(_encode_h2_frame(0x01, late, flags=0x05, stream_id=1))
            await writer.drain()

            reset_code = None
            while reset_code is None:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x03 and stream_id == 1:
                    reset_code = int.from_bytes(payload[:4], 'big')
                assert frame_type != 0x07

            follow = encoder.encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/after'),
                    (b'x-shared', b'from-tracked'),
                ],
                huffman=False,
            )
            writer.write(_encode_h2_frame(0x01, follow, flags=0x05, stream_id=3))
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=False)
        finally:
            first_release.set()
            writer.close()
            await writer.wait_closed()

    assert reset_code == int(h2.errors.ErrorCodes.STREAM_CLOSED)
    assert '/after' in seen
    assert not any(frame_type == 0x07 for frame_type, *_ in frames)


@pytest.mark.parametrize(
    (
        'interrupting_type',
        'interrupting_payload',
        'interrupting_flags',
        'interrupting_stream_id',
    ),
    [
        pytest.param(0x00, b'x', 0x01, 1, id='data'),
        pytest.param(
            0xF0, b'x' * SERVER_MAX_FRAME_SIZE, 0x00, 0, id='unknown-extension'
        ),
    ],
)
async def test_non_continuation_interrupts_header_block(
    interrupting_type: int,
    interrupting_payload: bytes,
    interrupting_flags: int,
    interrupting_stream_id: int,
) -> None:
    async def app(scope, receive, send):
        raise AssertionError('interrupted header block must not reach the app')

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            block = hpack.Encoder().encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                huffman=False,
            )
            writer.write(
                _encode_h2_frame(0x01, block[:8], flags=0x01, stream_id=1)
                + _encode_h2_frame(
                    interrupting_type,
                    interrupting_payload,
                    flags=interrupting_flags,
                    stream_id=interrupting_stream_id,
                )
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=True)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x07
        and int.from_bytes(payload[4:8], 'big')
        == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
        for frame_type, _flags, _stream_id, payload in frames
    )


async def test_wrong_stream_continuation_is_connection_protocol_error() -> None:
    async def app(scope, receive, send):
        raise AssertionError('mismatched CONTINUATION must not reach the app')

    config = Config(port=0, access_log=False)
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            block = hpack.Encoder().encode(
                [
                    (b':method', b'GET'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                huffman=False,
            )
            writer.write(
                _encode_h2_frame(0x01, block[:8], flags=0x01, stream_id=1)
                + _encode_h2_frame(0x09, block[8:], flags=0x04, stream_id=3)
            )
            await writer.drain()
            frames = await read_raw_h2_frames(reader, timeout=2, stop_at_goaway=True)
        finally:
            writer.close()
            await writer.wait_closed()

    assert any(
        frame_type == 0x07
        and int.from_bytes(payload[4:8], 'big')
        == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)
        for frame_type, _flags, _stream_id, payload in frames
    )


async def test_header_timeout_on_tracked_trailers_ends_the_connection() -> None:
    """A stalled trailer block cancels the owning application and ends the connection."""
    cancelled = asyncio.Event()
    second_started = asyncio.Event()
    # Gate a concurrent stream while the first is open so a double-counted
    # pending trailer would refuse stream 3 under max_concurrent_streams=1.
    first_body_seen = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        if scope['path'] == '/first':
            try:
                while True:
                    event = await receive()
                    if event.get('type') == 'http.disconnect':
                        cancelled.set()
                        return
                    if event['type'] == 'http.request':
                        if event.get('body'):
                            first_body_seen.set()
                        if not event.get('more_body', False):
                            break
            except Exception:
                cancelled.set()
                return
            try:
                await asyncio.sleep(30)
            except Exception:
                cancelled.set()
            return
        second_started.set()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=0,
        access_log=False,
        timeout_request_header=0.05,
        max_concurrent_streams=1,
    )
    async with running_server(app, config) as server:
        reader, writer, _conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            encoder = hpack.Encoder()
            head = encoder.encode(
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/first'),
                ],
                huffman=False,
            )
            trailers = encoder.encode([(b'x-trail', b'v')], huffman=False)
            writer.write(
                _encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + _encode_h2_frame(0x00, b'x', flags=0x00, stream_id=1)
            )
            await writer.drain()
            await asyncio.wait_for(first_body_seen.wait(), timeout=5)
            # Partial trailers keep the block open; timeout must finalize the app.
            writer.write(_encode_h2_frame(0x01, trailers[:1], flags=0x01, stream_id=1))
            await writer.drain()

            goaway_error = None
            while goaway_error is None:
                header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
                length = int.from_bytes(header[:3], 'big')
                frame_type = header[3]
                payload = await asyncio.wait_for(reader.readexactly(length), timeout=5)
                if frame_type == 0x07:
                    goaway_error = int.from_bytes(payload[4:8], 'big')

            assert await asyncio.wait_for(cancelled.wait(), timeout=2)
        finally:
            writer.close()
            await writer.wait_closed()

    # `trailers` was encoded on the same encoder as `head`, so the peer's
    # dynamic table already holds an entry the server's decoder never saw.
    # A trailer block is a field block like any other: abandoning it half
    # delivered desynchronizes the connection-wide compression context, so the
    # connection ends rather than the stream.
    assert goaway_error == int(h2.errors.ErrorCodes.COMPRESSION_ERROR)
