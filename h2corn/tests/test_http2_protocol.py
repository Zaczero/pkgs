import asyncio

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
    running_server,
    wait_for_port,
)

pytestmark = pytest.mark.asyncio
SERVER_MAX_FRAME_SIZE = 64 * 1024


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
                    return 'reset', int(event.error_code)
                if isinstance(event, h2.events.ConnectionTerminated):
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


async def _read_raw_h2_frames(
    reader: asyncio.StreamReader,
    *,
    timeout: float = 5.0,
    stop_at_goaway: bool = True,
) -> list[tuple[int, int, bytes]]:
    frames = []
    try:
        while True:
            header = await asyncio.wait_for(reader.readexactly(9), timeout=timeout)
            length = int.from_bytes(header[:3], 'big')
            frame_type = header[3]
            stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
            payload = await asyncio.wait_for(
                reader.readexactly(length), timeout=timeout
            )
            frames.append((frame_type, stream_id, payload))
            if stop_at_goaway and frame_type == 0x07:
                return frames
    except (asyncio.IncompleteReadError, TimeoutError):
        return frames


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

    config = Config(port=find_free_port(), timeout_graceful_shutdown=2.0)
    server = Server(app, config)
    server_task = asyncio.create_task(server.serve())
    await wait_for_port(config.port)

    reader, writer, conn, authority = await open_h2_connection(port=config.port)
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
            _read_raw_h2_frames(reader, timeout=0.5, stop_at_goaway=False),
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
    for frame_type, frame_stream_id, payload in frames:
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        kind, detail = await _h2_expect_error(
            port=config.port,
            headers=[
                (b':method', b'POST'),
                (b':scheme', b'http'),
                (b':authority', f'127.0.0.1:{config.port}'.encode()),
                (b':path', b'/'),
                (b'content-length', b'0'),
            ],
            body=b'payload',
        )

    assert kind in {'reset', 'goaway', 'closed'}
    if kind == 'reset':
        assert detail == int(h2.errors.ErrorCodes.PROTOCOL_ERROR)


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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
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


async def test_generic_connect_is_rejected_with_501() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'unreachable'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, _conn, authority = await open_h2_connection(port=config.port)
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
    decoded_headers = dict(hpack.Decoder().decode(payload))
    assert decoded_headers.get(b':status', decoded_headers.get(':status')) in {
        b'501',
        '501',
    }


async def test_max_concurrent_stream_limit_is_enforced() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'hello'})

    config = Config(port=find_free_port(), max_concurrent_streams=1)
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
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
                        reset_code = int(event.error_code)
                        break
                    if isinstance(event, h2.events.ConnectionTerminated):
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


async def test_client_must_send_settings_as_first_frame() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
            + _encode_h2_frame(0x06, b'\x00' * 8, stream_id=0)
        )
        await writer.drain()
        try:
            frames = await _read_raw_h2_frames(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    goaway = next(
        payload for frame_type, _stream_id, payload in frames if frame_type == 0x07
    )
    assert int.from_bytes(goaway[4:8], 'big') == int(
        h2.errors.ErrorCodes.PROTOCOL_ERROR
    )


async def test_server_settings_advertise_max_frame_size() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n' + _encode_h2_settings([]))
        await writer.drain()
        try:
            frames = await _read_raw_h2_frames(
                reader, timeout=0.2, stop_at_goaway=False
            )
        finally:
            writer.close()
            await writer.wait_closed()

    settings_payload = next(
        payload
        for frame_type, _stream_id, payload in frames
        if frame_type == 0x04 and payload
    )
    settings = _decode_h2_settings_payload(settings_payload)
    assert (
        settings[int(h2.settings.SettingCodes.MAX_FRAME_SIZE)] == SERVER_MAX_FRAME_SIZE
    )


async def test_invalid_ping_emits_goaway_after_valid_preface_and_settings() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
            + _encode_h2_settings([])
            + _encode_h2_frame(0x06, b'\x00' * 8, stream_id=1)
        )
        await writer.drain()
        try:
            frames = await _read_raw_h2_frames(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    goaway = next(
        payload for frame_type, _stream_id, payload in frames if frame_type == 0x07
    )
    assert int.from_bytes(goaway[4:8], 'big') == int(
        h2.errors.ErrorCodes.PROTOCOL_ERROR
    )


async def test_response_data_frames_respect_peer_max_frame_size() -> None:
    payload = b'x' * ((32 * 1024) + 4096)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
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
            frames = await _read_raw_h2_frames(
                reader, timeout=0.2, stop_at_goaway=False
            )
        finally:
            writer.close()
            await writer.wait_closed()

    data_lengths = [
        len(frame_payload)
        for frame_type, frame_stream_id, frame_payload in frames
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

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
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
            frames = await _read_raw_h2_frames(
                reader, timeout=0.2, stop_at_goaway=False
            )
        finally:
            writer.close()
            await writer.wait_closed()

    data_lengths = [
        len(frame_payload)
        for frame_type, frame_stream_id, frame_payload in frames
        if frame_type == 0x00 and frame_stream_id == stream_id
    ]
    assert data_lengths
    assert max(data_lengths) == SERVER_MAX_FRAME_SIZE


async def test_connection_specific_response_headers_are_passthrough_invalid() -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'connection', b'close')],
        })

    config = Config(port=find_free_port())
    async with running_server(app, config):
        with pytest.raises(
            h2.exceptions.ProtocolError, match='Connection-specific header field'
        ):
            await asyncio.wait_for(h2_request(port=config.port), timeout=5)
