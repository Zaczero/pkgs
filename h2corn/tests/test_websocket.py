import asyncio
import zlib

import h2.config
import h2.connection
import h2.events
import pytest
from fastapi import FastAPI, WebSocket
from h2corn import Config

from tests._support import (
    find_free_port,
    open_h2_connection,
    read_http1_response,
    running_server,
)

pytestmark = pytest.mark.asyncio


def _decode_ws_close_payload(payload: bytes) -> tuple[int, str]:
    if len(payload) < 2:
        raise ValueError('websocket close payload must include a code')
    return int.from_bytes(payload[:2], 'big'), payload[2:].decode()


def _encode_ws_client_frame(
    opcode: int,
    payload: bytes = b'',
    *,
    first_byte: int | None = None,
) -> bytes:
    mask = b'\x01\x02\x03\x04'
    first = first_byte if first_byte is not None else 0x80 | opcode
    length = len(payload)
    if length < 126:
        header = bytes([first, 0x80 | length])
    elif length < (1 << 16):
        header = bytes([first, 0x80 | 126]) + length.to_bytes(2, 'big')
    else:
        header = bytes([first, 0x80 | 127]) + length.to_bytes(8, 'big')
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return header + mask + masked


def _compress_permessage_deflate(payload: bytes) -> bytes:
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed = compressor.compress(payload)
    compressed += compressor.flush(zlib.Z_SYNC_FLUSH)
    return compressed[:-4]


def _decompress_permessage_deflate(payload: bytes) -> bytes:
    decompressor = zlib.decompressobj(wbits=-zlib.MAX_WBITS)
    return decompressor.decompress(payload + b'\x00\x00\xff\xff')


def _parse_ws_frames(data: bytes) -> tuple[list[tuple[int, bytes]], bytes]:
    frames = []
    cursor = 0
    while True:
        if len(data) - cursor < 2:
            return frames, data[cursor:]
        first = data[cursor]
        second = data[cursor + 1]
        cursor += 2
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            if len(data) - cursor < 2:
                return frames, data[cursor - 2 :]
            length = int.from_bytes(data[cursor : cursor + 2], 'big')
            cursor += 2
        elif length == 127:
            if len(data) - cursor < 8:
                return frames, data[cursor - 2 :]
            length = int.from_bytes(data[cursor : cursor + 8], 'big')
            cursor += 8
        mask = b''
        if masked:
            if len(data) - cursor < 4:
                return frames, data[cursor - 2 :]
            mask = data[cursor : cursor + 4]
            cursor += 4
        if len(data) - cursor < length:
            return frames, data[cursor - 2 :]
        payload = data[cursor : cursor + length]
        cursor += length
        if masked:
            payload = bytes(
                byte ^ mask[index % 4] for index, byte in enumerate(payload)
            )
        frames.append((opcode, payload))


def _parse_ws_frames_detailed(
    data: bytes,
) -> tuple[list[tuple[int, int, bytes]], bytes]:
    frames = []
    cursor = 0
    while True:
        if len(data) - cursor < 2:
            return frames, data[cursor:]
        first = data[cursor]
        second = data[cursor + 1]
        cursor += 2
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            if len(data) - cursor < 2:
                return frames, data[cursor - 2 :]
            length = int.from_bytes(data[cursor : cursor + 2], 'big')
            cursor += 2
        elif length == 127:
            if len(data) - cursor < 8:
                return frames, data[cursor - 2 :]
            length = int.from_bytes(data[cursor : cursor + 8], 'big')
            cursor += 8
        if masked:
            if len(data) - cursor < 4:
                return frames, data[cursor - 2 :]
            cursor += 4
        if len(data) - cursor < length:
            return frames, data[cursor - 2 :]
        payload = data[cursor : cursor + length]
        cursor += length
        frames.append((first, opcode, payload))


async def _http1_h2c_upgrade_request(
    *,
    port: int,
    path: str = '/',
) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    conn = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=True, header_encoding=None)
    )
    settings = conn.initiate_upgrade_connection().decode()
    writer.write(
        (
            f'GET {path} HTTP/1.1\r\n'
            f'Host: 127.0.0.1:{port}\r\n'
            'Connection: Upgrade, HTTP2-Settings\r\n'
            'Upgrade: h2c\r\n'
            f'HTTP2-Settings: {settings}\r\n'
            '\r\n'
        ).encode()
    )
    await writer.drain()
    status, _, _, _ = await read_http1_response(reader)
    assert status == 101

    writer.write(conn.data_to_send())
    await writer.drain()

    response_status = None
    response_body = bytearray()
    try:
        while True:
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            if not data:
                break
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    response_status = int(dict(event.headers)[b':status'])
                elif isinstance(event, h2.events.DataReceived):
                    response_body.extend(event.data)
                    conn.acknowledge_received_data(
                        event.flow_controlled_length,
                        event.stream_id,
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    pending = conn.data_to_send()
                    if pending:
                        writer.write(pending)
                        await writer.drain()
                    assert response_status is not None
                    return response_status, bytes(response_body)
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

    raise RuntimeError('h2c upgrade response stream ended unexpectedly')


async def _http1_websocket_handshake(
    *,
    port: int,
    path: str,
    method: str = 'GET',
    key: str | None = 'dGhlIHNhbXBsZSBub25jZQ==',
    version: str = '13',
    subprotocol: str | None = None,
    extensions: str | None = None,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[int, dict[bytes, bytes], bytes]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    key_header = '' if key is None else f'Sec-WebSocket-Key: {key}\r\n'
    subprotocol_header = (
        '' if subprotocol is None else f'Sec-WebSocket-Protocol: {subprotocol}\r\n'
    )
    extensions_header = (
        '' if extensions is None else f'Sec-WebSocket-Extensions: {extensions}\r\n'
    )
    extra_header_lines = ''.join(
        f'{name.decode("ascii")}: {value.decode("ascii")}\r\n'
        for name, value in extra_headers or ()
    )
    writer.write(
        (
            f'{method} {path} HTTP/1.1\r\n'
            f'Host: 127.0.0.1:{port}\r\n'
            'Connection: Upgrade\r\n'
            'Upgrade: websocket\r\n'
            f'Sec-WebSocket-Version: {version}\r\n'
            f'{subprotocol_header}'
            f'{extensions_header}'
            f'{key_header}'
            f'{extra_header_lines}'
            '\r\n'
        ).encode()
    )
    await writer.drain()
    try:
        status, headers, body, _ = await read_http1_response(reader)
        return status, headers, body
    finally:
        writer.close()
        await writer.wait_closed()


async def _http1_websocket_round_trip(
    *,
    port: int,
    path: str,
    text: str,
    subprotocol: str | None = None,
) -> tuple[dict[bytes, bytes], str]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    subprotocol_header = (
        '' if subprotocol is None else f'Sec-WebSocket-Protocol: {subprotocol}\r\n'
    )
    writer.write(
        (
            f'GET {path} HTTP/1.1\r\n'
            f'Host: 127.0.0.1:{port}\r\n'
            'Connection: Upgrade\r\n'
            'Upgrade: websocket\r\n'
            'Sec-WebSocket-Version: 13\r\n'
            f'{subprotocol_header}'
            'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n'
            '\r\n'
        ).encode()
    )
    await writer.drain()

    status, headers, _, _ = await read_http1_response(reader)
    assert status == 101

    writer.write(_encode_ws_client_frame(0x1, text.encode()))
    await writer.drain()

    ws_buffer = b''
    echoed = None
    try:
        while echoed is None:
            ws_buffer += await asyncio.wait_for(reader.read(65535), timeout=5)
            frames, ws_buffer = _parse_ws_frames(ws_buffer)
            for opcode, payload in frames:
                if opcode == 0x1:
                    echoed = payload.decode()
                    break
        writer.write(_encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')))
        await writer.drain()
        return headers, echoed
    finally:
        writer.close()
        await writer.wait_closed()


async def _http1_open_websocket_stream(
    *,
    port: int,
    path: str,
    subprotocol: str | None = None,
    extensions: str | None = None,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, dict[bytes, bytes]]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    subprotocol_header = (
        '' if subprotocol is None else f'Sec-WebSocket-Protocol: {subprotocol}\r\n'
    )
    extensions_header = (
        '' if extensions is None else f'Sec-WebSocket-Extensions: {extensions}\r\n'
    )
    writer.write(
        (
            f'GET {path} HTTP/1.1\r\n'
            f'Host: 127.0.0.1:{port}\r\n'
            'Connection: Upgrade\r\n'
            'Upgrade: websocket\r\n'
            'Sec-WebSocket-Version: 13\r\n'
            f'{subprotocol_header}'
            f'{extensions_header}'
            'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n'
            '\r\n'
        ).encode()
    )
    await writer.drain()

    status, headers, _, _ = await read_http1_response(reader)
    assert status == 101
    return reader, writer, headers


def _send_h2_websocket_headers(
    conn: h2.connection.H2Connection,
    writer: asyncio.StreamWriter,
    *,
    authority: bytes,
    path: str,
    version: str | None = '13',
    subprotocol: str | None = None,
    extensions: str | None = None,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> int:
    stream_id = conn.get_next_available_stream_id()
    headers = [
        (b':method', b'CONNECT'),
        (b':protocol', b'websocket'),
        (b':scheme', b'http'),
        (b':authority', authority),
        (b':path', path.encode()),
    ]
    if version is not None:
        headers.append((b'sec-websocket-version', version.encode()))
    if subprotocol is not None:
        headers.append((b'sec-websocket-protocol', subprotocol.encode()))
    if extensions is not None:
        headers.append((b'sec-websocket-extensions', extensions.encode()))
    if extra_headers is not None:
        headers.extend(extra_headers)

    conn.send_headers(stream_id, headers, end_stream=False)
    writer.write(conn.data_to_send())
    return stream_id


async def _h2_open_websocket_stream(
    *,
    port: int,
    path: str,
    subprotocol: str | None = None,
    extensions: str | None = None,
) -> tuple[
    asyncio.StreamReader,
    asyncio.StreamWriter,
    h2.connection.H2Connection,
    int,
    tuple[str, int | None, list[tuple[int, bytes]]] | None,
]:
    reader, writer, conn, authority = await open_h2_connection(port=port)
    stream_id = _send_h2_websocket_headers(
        conn,
        writer,
        authority=authority,
        path=path,
        subprotocol=subprotocol,
        extensions=extensions,
    )
    await writer.drain()

    initial_frames = []
    status = None
    ws_buffer = b''
    while status is None:
        data = await reader.read(65535)
        if not data:
            raise RuntimeError('websocket handshake connection closed')
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived):
                status = int(dict(event.headers)[b':status'])
            elif isinstance(event, h2.events.DataReceived):
                ws_buffer += event.data
                conn.acknowledge_received_data(event.flow_controlled_length, stream_id)
                parsed, ws_buffer = _parse_ws_frames(ws_buffer)
                initial_frames.extend(parsed)
            elif isinstance(event, h2.events.StreamEnded):
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
                assert status == 200
                return reader, writer, conn, stream_id, ('ended', None, initial_frames)
            elif isinstance(event, h2.events.StreamReset):
                return (
                    reader,
                    writer,
                    conn,
                    stream_id,
                    ('reset', int(event.error_code), initial_frames),
                )
            elif isinstance(event, h2.events.ConnectionTerminated):
                return (
                    reader,
                    writer,
                    conn,
                    stream_id,
                    ('goaway', int(event.error_code), initial_frames),
                )
        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()

    assert status == 200
    return reader, writer, conn, stream_id, None


async def _h2_websocket_handshake(
    *,
    port: int,
    path: str,
    version: str | None = '13',
    subprotocol: str | None = None,
    extensions: str | None = None,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[int, dict[bytes, bytes], bytes]:
    reader, writer, conn, authority = await open_h2_connection(port=port)
    stream_id = _send_h2_websocket_headers(
        conn,
        writer,
        authority=authority,
        path=path,
        version=version,
        subprotocol=subprotocol,
        extensions=extensions,
        extra_headers=extra_headers,
    )
    await writer.drain()

    status = None
    response_headers = {}
    body = bytearray()
    try:
        while True:
            data = await reader.read(65535)
            if not data:
                break
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    response_headers = dict(event.headers)
                    status = int(response_headers[b':status'])
                elif isinstance(event, h2.events.DataReceived):
                    body.extend(event.data)
                    conn.acknowledge_received_data(
                        event.flow_controlled_length, stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    pending = conn.data_to_send()
                    if pending:
                        writer.write(pending)
                        await writer.drain()
                    assert status is not None
                    return status, response_headers, bytes(body)
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

    raise RuntimeError('websocket handshake stream ended unexpectedly')


async def _h2_websocket_round_trip(
    *,
    port: int,
    path: str,
    text: str,
    subprotocol: str | None = None,
) -> tuple[int, str | None, str]:
    reader, writer, conn, authority = await open_h2_connection(port=port)
    stream_id = _send_h2_websocket_headers(
        conn,
        writer,
        authority=authority,
        path=path,
        subprotocol=subprotocol,
    )
    await writer.drain()

    status = None
    accepted_subprotocol = None
    while status is None:
        data = await reader.read(65535)
        if not data:
            raise RuntimeError('websocket handshake connection closed')
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived):
                header_map = dict(event.headers)
                status = int(header_map[b':status'])
                raw_subprotocol = header_map.get(b'sec-websocket-protocol')
                if raw_subprotocol is not None:
                    accepted_subprotocol = raw_subprotocol.decode()
        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()

    ws_buffer = b''
    echoed = None
    try:
        conn.send_data(
            stream_id, _encode_ws_client_frame(0x1, text.encode()), end_stream=False
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        while echoed is None:
            data = await reader.read(65535)
            if not data:
                raise RuntimeError('websocket closed before echo')
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.DataReceived):
                    ws_buffer += event.data
                    conn.acknowledge_received_data(
                        event.flow_controlled_length, stream_id
                    )
            frames, ws_buffer = _parse_ws_frames(ws_buffer)
            for opcode, payload in frames:
                if opcode == 0x1:
                    echoed = payload.decode()
                    break
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()

        conn.send_data(
            stream_id,
            _encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')),
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()
        return status, accepted_subprotocol, echoed
    finally:
        writer.close()
        await writer.wait_closed()


async def _read_next_ws_text(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    conn: h2.connection.H2Connection,
    stream_id: int,
    ws_buffer: bytes,
) -> tuple[str, bytes, str | None, int | None, list[tuple[int, bytes]]]:
    while True:
        data = await asyncio.wait_for(reader.read(65535), timeout=5)
        if not data:
            raise RuntimeError('websocket stream closed before the next text frame')
        stream_ended = False
        stream_reset = None
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.DataReceived):
                ws_buffer += event.data
                conn.acknowledge_received_data(event.flow_controlled_length, stream_id)
            elif isinstance(event, h2.events.StreamEnded):
                stream_ended = True
            elif isinstance(event, h2.events.StreamReset):
                stream_reset = event.error_code
        frames, ws_buffer = _parse_ws_frames(ws_buffer)
        for index, (opcode, payload) in enumerate(frames):
            if opcode == 0x1:
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
                terminal = None
                detail = None
                if stream_reset is not None:
                    terminal = 'reset'
                    detail = stream_reset
                elif stream_ended:
                    terminal = 'ended'
                return (
                    payload.decode(),
                    ws_buffer,
                    terminal,
                    detail,
                    frames[index + 1 :],
                )
        if stream_reset is not None:
            raise TypeError(
                f'websocket stream reset before the next text frame: {stream_reset}'
            )
        if stream_ended:
            raise TypeError('websocket stream ended before the next text frame')
        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()


async def _read_ws_server_result(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    conn: h2.connection.H2Connection,
    stream_id: int,
) -> tuple[str, int | None, list[tuple[int, bytes]]]:
    frames = []
    ws_buffer = b''

    while True:
        data = await asyncio.wait_for(reader.read(65535), timeout=5)
        if not data:
            return 'closed', None, frames
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.DataReceived):
                ws_buffer += event.data
                conn.acknowledge_received_data(event.flow_controlled_length, stream_id)
                parsed, ws_buffer = _parse_ws_frames(ws_buffer)
                frames.extend(parsed)
            elif isinstance(event, h2.events.StreamEnded):
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
                return 'ended', None, frames
            elif isinstance(event, h2.events.StreamReset):
                return 'reset', int(event.error_code), frames
            elif isinstance(event, h2.events.ConnectionTerminated):
                return 'goaway', int(event.error_code), frames

        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()


async def _read_raw_h2_frames(
    reader: asyncio.StreamReader,
    *,
    timeout: float = 5.0,
) -> list[tuple[int, int, bytes]]:
    frames = []
    try:
        while True:
            header = await asyncio.wait_for(reader.readexactly(9), timeout=timeout)
            length = int.from_bytes(header[:3], 'big')
            frame_type = header[3]
            stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
            payload = await asyncio.wait_for(
                reader.readexactly(length),
                timeout=timeout,
            )
            frames.append((frame_type, stream_id, payload))
    except (asyncio.IncompleteReadError, TimeoutError):
        return frames


async def _read_http1_ws_server_result(
    reader: asyncio.StreamReader,
) -> list[tuple[int, bytes]]:
    frames = []
    ws_buffer = b''

    while True:
        data = await asyncio.wait_for(reader.read(65535), timeout=5)
        if not data:
            return frames
        ws_buffer += data
        parsed, ws_buffer = _parse_ws_frames(ws_buffer)
        frames.extend(parsed)
        if any(opcode == 0x8 for opcode, _ in parsed):
            return frames


async def _assert_h2_websocket_close_code(
    app,
    *,
    client_frames: list[bytes] | None = None,
    expected_code: int,
    config: Config | None = None,
    extensions: str | None = None,
) -> None:
    config = config or Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
            extensions=extensions,
        )
        try:
            if client_frames:
                for frame in client_frames:
                    conn.send_data(stream_id, frame, end_stream=False)
                writer.write(conn.data_to_send())
                await writer.drain()
            terminal, detail, frames = initial or await _read_ws_server_result(
                reader,
                writer,
                conn,
                stream_id,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert terminal == 'ended'
    assert detail is None
    assert [opcode for opcode, _ in frames] == [0x8]
    assert _decode_ws_close_payload(frames[0][1])[0] == expected_code


async def _assert_http1_websocket_close_code(
    app,
    *,
    client_frames: list[bytes] | None = None,
    expected_code: int,
    config: Config | None = None,
    extensions: str | None = None,
) -> None:
    config = config or Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, _ = await _http1_open_websocket_stream(
            port=config.port,
            path='/ws',
            extensions=extensions,
        )
        try:
            if client_frames:
                for frame in client_frames:
                    writer.write(frame)
                await writer.drain()
            frames = await _read_http1_ws_server_result(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    close_frames = [(opcode, payload) for opcode, payload in frames if opcode == 0x8]
    assert [opcode for opcode, _ in close_frames] == [0x8]
    assert _decode_ws_close_payload(close_frames[0][1])[0] == expected_code


async def _assert_websocket_close_code(
    transport: str,
    app,
    *,
    client_frames: list[bytes] | None = None,
    expected_code: int,
    config: Config | None = None,
    extensions: str | None = None,
) -> None:
    if transport == 'h2':
        await _assert_h2_websocket_close_code(
            app,
            client_frames=client_frames,
            expected_code=expected_code,
            config=config,
            extensions=extensions,
        )
    else:
        await _assert_http1_websocket_close_code(
            app,
            client_frames=client_frames,
            expected_code=expected_code,
            config=config,
            extensions=extensions,
        )


def _build_websocket_denial_response_app():
    state = {'extensions': None, 'events': []}

    async def app(scope, receive, send):
        state['extensions'] = scope['extensions']
        state['events'].append(await receive())
        await send({
            'type': 'websocket.http.response.start',
            'status': 401,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({
            'type': 'websocket.http.response.body',
            'body': b'den',
            'more_body': True,
        })
        await send({'type': 'websocket.http.response.body', 'body': b'ied'})

    return app, state


def _build_websocket_unary_denial_response_app():
    state = {'extensions': None, 'events': []}

    async def app(scope, receive, send):
        state['extensions'] = scope['extensions']
        state['events'].append(await receive())
        await send({
            'type': 'websocket.http.response.start',
            'status': 401,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'websocket.http.response.body', 'body': b'denied'})

    return app, state


async def test_websocket_rfc8441_echo_round_trip() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept(subprotocol='chat')
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, subprotocol, echoed = await asyncio.wait_for(
            _h2_websocket_round_trip(
                port=config.port,
                path='/ws',
                text='hello',
                subprotocol='chat',
            ),
            timeout=5,
        )

    assert status == 200
    assert subprotocol == 'chat'
    assert echoed == 'echo:hello'


async def test_http1_websocket_upgrade_round_trip() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        headers, echoed = await asyncio.wait_for(
            _http1_websocket_round_trip(port=config.port, path='/ws', text='hello'),
            timeout=5,
        )

    assert headers[b'upgrade'] == b'websocket'
    assert echoed == 'echo:hello'


async def test_http1_websocket_idle_session_ignores_timeout_request_body_idle() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port(), timeout_request_body_idle=0.05)
    async with running_server(websocket_app, config):
        reader, writer, _ = await _http1_open_websocket_stream(
            port=config.port,
            path='/ws',
        )
        try:
            await asyncio.sleep(0.2)
            writer.write(_encode_ws_client_frame(0x1, b'hello'))
            await writer.drain()
            frames = await _read_http1_ws_server_result(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    assert frames[0] == (0x1, b'echo:hello')
    assert _decode_ws_close_payload(frames[1][1])[0] == 1000


async def test_h2_websocket_idle_session_ignores_timeout_request_body_idle() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port(), timeout_request_body_idle=0.05)
    async with running_server(websocket_app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
        )
        try:
            assert initial is None
            await asyncio.sleep(0.2)
            conn.send_data(
                stream_id, _encode_ws_client_frame(0x1, b'hello'), end_stream=False
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            terminal, detail, frames = await _read_ws_server_result(
                reader,
                writer,
                conn,
                stream_id,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert terminal == 'ended'
    assert detail is None
    assert frames[0] == (0x1, b'echo:hello')
    assert _decode_ws_close_payload(frames[1][1])[0] == 1000


@pytest.mark.parametrize(
    ('handshake', 'expected_status'),
    [
        (_h2_websocket_handshake, 200),
        (_http1_websocket_handshake, 101),
    ],
)
async def test_websocket_accepts_requested_subprotocol_across_transports(
    handshake,
    expected_status: int,
) -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept(subprotocol='superchat')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, _ = await asyncio.wait_for(
            handshake(
                port=config.port,
                path='/ws',
                subprotocol='chat, superchat',
            ),
            timeout=5,
        )

    assert status == expected_status
    assert headers[b'sec-websocket-protocol'] == b'superchat'


async def test_http1_websocket_scope_omits_empty_subprotocols() -> None:
    subprotocols = object()
    extensions = None
    events = []

    async def app(scope, receive, send):
        nonlocal extensions, subprotocols
        subprotocols = scope.get('subprotocols')
        extensions = scope['extensions']
        events.append(await receive())
        await send({'type': 'websocket.close'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(port=config.port, path='/ws'),
            timeout=5,
        )

    assert subprotocols is None
    assert extensions == {'websocket.http.response': {}}
    assert events == [{'type': 'websocket.connect'}]
    assert status == 403
    assert body == b''
    assert b'upgrade' not in headers


@pytest.mark.parametrize(
    ('handshake', 'expected_status'),
    [
        (_h2_websocket_handshake, 403),
        (_http1_websocket_handshake, 403),
    ],
)
async def test_websocket_proxy_headers_rewrite_scope_from_trusted_peer(
    handshake,
    expected_status: int,
) -> None:
    state = {}

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        state['scheme'] = scope['scheme']
        state['client'] = scope['client']
        state['server'] = scope['server']
        state['root_path'] = scope.get('root_path', '')
        assert await receive() == {'type': 'websocket.connect'}
        await send({'type': 'websocket.close'})

    config = Config(
        port=find_free_port(),
        root_path='/root',
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            handshake(
                port=config.port,
                path='/ws',
                extra_headers=[
                    (
                        b'forwarded',
                        b'for=203.0.113.10;proto=https;host=example.com:9443',
                    ),
                    (b'x-forwarded-prefix', b'/api'),
                ],
            ),
            timeout=5,
        )

    assert status == expected_status
    assert body == b''
    assert b'upgrade' not in headers
    assert state['scheme'] == 'wss'
    assert state['client'][0] == '203.0.113.10'
    assert isinstance(state['client'][1], int)
    assert state['server'] == ('example.com', 9443)
    assert state['root_path'] == '/api/root'


async def test_http1_websocket_invalid_version_is_rejected_with_426() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(port=config.port, path='/ws', version='12'),
            timeout=5,
        )

    assert status == 426
    assert headers[b'sec-websocket-version'] == b'13'
    assert body == b''


async def test_http1_websocket_missing_key_is_rejected_with_400() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(port=config.port, path='/ws', key=None),
            timeout=5,
        )

    assert status == 400
    assert body == b''
    assert b'upgrade' not in headers


async def test_http1_websocket_non_get_method_is_rejected_with_400() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(
                port=config.port,
                path='/ws',
                method='POST',
            ),
            timeout=5,
        )

    assert status == 400
    assert body == b''
    assert b'upgrade' not in headers


async def test_http1_h2c_upgrade_round_trip() -> None:
    async def app(scope, receive, send):
        assert scope['http_version'] == '2'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'upgraded'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            _http1_h2c_upgrade_request(port=config.port),
            timeout=5,
        )

    assert status == 200
    assert body == b'upgraded'


async def test_websocket_multiple_messages_round_trip_on_one_stream() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        for _ in range(2):
            message = await websocket.receive_text()
            await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
        )
        try:
            assert initial is None
            frames = []
            terminal = None
            detail = None
            ws_buffer = b''
            for message in ('one', 'two'):
                conn.send_data(
                    stream_id,
                    _encode_ws_client_frame(0x1, message.encode()),
                    end_stream=False,
                )
                writer.write(conn.data_to_send())
                await writer.drain()
                echoed, ws_buffer, terminal, detail, frames = await _read_next_ws_text(
                    reader,
                    writer,
                    conn,
                    stream_id,
                    ws_buffer,
                )
                assert echoed == f'echo:{message}'
            if terminal is None:
                terminal, detail, frames = await _read_ws_server_result(
                    reader,
                    writer,
                    conn,
                    stream_id,
                )
            else:
                frames = [
                    (opcode, payload) for opcode, payload in frames if opcode == 0x8
                ]
        finally:
            writer.close()
            await writer.wait_closed()

    assert terminal == 'ended'
    assert detail is None
    assert [opcode for opcode, _ in frames] == [0x8]
    assert _decode_ws_close_payload(frames[0][1]) == (1000, '')


async def test_websocket_fragmented_text_message_round_trip() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
        )
        try:
            assert initial is None
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x1, b'hel', first_byte=0x01),
                end_stream=False,
            )
            writer.write(conn.data_to_send())
            await writer.drain()
            await asyncio.sleep(0.01)

            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x0, b'lo'),
                end_stream=False,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            echoed, ws_buffer, terminal, detail, frames = await _read_next_ws_text(
                reader,
                writer,
                conn,
                stream_id,
                b'',
            )
            if terminal is None:
                terminal, detail, frames = await _read_ws_server_result(
                    reader,
                    writer,
                    conn,
                    stream_id,
                )
            else:
                frames = [
                    (opcode, payload) for opcode, payload in frames if opcode == 0x8
                ]
        finally:
            writer.close()
            await writer.wait_closed()

    assert echoed == 'echo:hello'
    assert ws_buffer == b''
    assert terminal == 'ended'
    assert detail is None
    assert [opcode for opcode, _ in frames] == [0x8]
    assert _decode_ws_close_payload(frames[0][1]) == (1000, '')


async def test_websocket_fragmented_text_message_with_interleaved_ping_round_trip() -> (
    None
):
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
        )
        try:
            assert initial is None
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x1, b'hel', first_byte=0x01),
                end_stream=False,
            )
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x9, b'hi'),
                end_stream=False,
            )
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x0, b'lo'),
                end_stream=False,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            pong_payload = None
            echoed = None
            close_frames = []
            ws_buffer = b''
            terminal = None
            detail = None
            while pong_payload is None or echoed is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                if not data:
                    raise RuntimeError('websocket closed before pong and echo arrived')
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.DataReceived):
                        ws_buffer += event.data
                        conn.acknowledge_received_data(
                            event.flow_controlled_length,
                            stream_id,
                        )
                    elif isinstance(event, h2.events.StreamEnded):
                        terminal = 'ended'
                    elif isinstance(event, h2.events.StreamReset):
                        terminal = 'reset'
                        detail = int(event.error_code)
                frames, ws_buffer = _parse_ws_frames(ws_buffer)
                for opcode, payload in frames:
                    if opcode == 0xA:
                        pong_payload = payload
                    elif opcode == 0x1:
                        echoed = payload.decode()
                    elif opcode == 0x8:
                        close_frames.append((opcode, payload))
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()

            if terminal is None:
                conn.send_data(
                    stream_id,
                    _encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')),
                    end_stream=True,
                )
                writer.write(conn.data_to_send())
                await writer.drain()
                terminal, detail, frames = await _read_ws_server_result(
                    reader,
                    writer,
                    conn,
                    stream_id,
                )
                close_frames.extend(
                    (opcode, payload) for opcode, payload in frames if opcode == 0x8
                )
        finally:
            writer.close()
            await writer.wait_closed()

    assert pong_payload == b'hi'
    assert echoed == 'echo:hello'
    assert ws_buffer == b''
    assert terminal in {'ended', 'closed'}
    assert detail is None
    assert [opcode for opcode, _ in close_frames] == [0x8]
    assert _decode_ws_close_payload(close_frames[0][1]) == (1000, '')


async def test_h2_websocket_single_frame_split_across_data_frames_round_trip() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    frame = _encode_ws_client_frame(0x1, b'hello')
    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
        )
        try:
            assert initial is None
            conn.send_data(stream_id, frame[:1], end_stream=False)
            conn.send_data(stream_id, frame[1:4], end_stream=False)
            conn.send_data(stream_id, frame[4:7], end_stream=False)
            conn.send_data(stream_id, frame[7:], end_stream=False)
            writer.write(conn.data_to_send())
            await writer.drain()

            echoed, ws_buffer, terminal, detail, frames = await _read_next_ws_text(
                reader,
                writer,
                conn,
                stream_id,
                b'',
            )
            if terminal is None:
                terminal, detail, frames = await _read_ws_server_result(
                    reader,
                    writer,
                    conn,
                    stream_id,
                )
            else:
                frames = [
                    (opcode, payload) for opcode, payload in frames if opcode == 0x8
                ]
        finally:
            writer.close()
            await writer.wait_closed()

    assert echoed == 'echo:hello'
    assert ws_buffer == b''
    assert terminal == 'ended'
    assert detail is None
    assert [opcode for opcode, _ in frames] == [0x8]
    assert _decode_ws_close_payload(frames[0][1]) == (1000, '')


@pytest.mark.parametrize(
    ('handshake', 'transport'),
    [
        (_h2_websocket_handshake, 'h2'),
        (_http1_websocket_handshake, 'http1'),
    ],
)
async def test_websocket_denial_response_extension_round_trip_across_transports(
    handshake,
    transport: str,
) -> None:
    app, state = _build_websocket_denial_response_app()

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            handshake(port=config.port, path='/ws'),
            timeout=5,
        )

    assert state['extensions'] == {'websocket.http.response': {}}
    assert state['events'] == [{'type': 'websocket.connect'}]
    assert status == 401
    assert headers[b'content-type'] == b'text/plain'
    assert body == b'denied', f'{transport} denial response body should match'


@pytest.mark.parametrize(
    ('handshake', 'transport'),
    [
        (_h2_websocket_handshake, 'h2'),
        (_http1_websocket_handshake, 'http1'),
    ],
)
async def test_websocket_unary_denial_response_is_fixed_length_across_transports(
    handshake,
    transport: str,
) -> None:
    app, state = _build_websocket_unary_denial_response_app()

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            handshake(port=config.port, path='/ws'),
            timeout=5,
        )

    assert state['extensions'] == {'websocket.http.response': {}}
    assert state['events'] == [{'type': 'websocket.connect'}]
    assert status == 401
    assert headers[b'content-type'] == b'text/plain'
    assert headers[b'content-length'] == b'6'
    assert body == b'denied'
    if transport == 'http1':
        assert b'transfer-encoding' not in headers


async def test_websocket_scope_omits_empty_subprotocols() -> None:
    subprotocols = object()
    extensions = None
    events = []

    async def app(scope, receive, send):
        nonlocal extensions, subprotocols
        subprotocols = scope.get('subprotocols')
        extensions = scope['extensions']
        events.append(await receive())
        await send({'type': 'websocket.close'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(port=config.port, path='/ws'),
            timeout=5,
        )

    assert subprotocols is None
    assert extensions == {'websocket.http.response': {}}
    assert events == [{'type': 'websocket.connect'}]
    assert status == 403
    assert body == b''
    assert b'sec-websocket-protocol' not in headers


async def test_http1_websocket_scope_exposes_requested_subprotocols() -> None:
    subprotocols = None
    events = []

    async def app(scope, receive, send):
        nonlocal subprotocols
        subprotocols = scope['subprotocols']
        events.append(await receive())
        await send({
            'type': 'websocket.accept',
            'subprotocol': 'superchat',
            'headers': [],
        })
        await send({'type': 'websocket.close'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(
                port=config.port,
                path='/ws',
                subprotocol='chat, superchat',
            ),
            timeout=5,
        )

    assert subprotocols == ['chat', 'superchat']
    assert events == [{'type': 'websocket.connect'}]
    assert status == 101
    assert headers[b'upgrade'] == b'websocket'
    assert headers[b'sec-websocket-protocol'] == b'superchat'
    assert body == b''


async def test_websocket_scope_exposes_requested_subprotocols() -> None:
    subprotocols = None
    events = []

    async def app(scope, receive, send):
        nonlocal subprotocols
        subprotocols = scope['subprotocols']
        events.append(await receive())
        await send({
            'type': 'websocket.accept',
            'subprotocol': 'superchat',
            'headers': [],
        })
        await send({'type': 'websocket.close'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(
                port=config.port,
                path='/ws',
                subprotocol='chat, superchat',
            ),
            timeout=5,
        )

    assert subprotocols == ['chat', 'superchat']
    assert events == [{'type': 'websocket.connect'}]
    assert status == 200
    assert headers[b'sec-websocket-protocol'] == b'superchat'
    assert body == b'\x88\x02\x03\xe8'


async def test_websocket_invalid_version_is_rejected_with_426() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(port=config.port, path='/ws', version='12'),
            timeout=5,
        )

    assert status == 426
    assert headers[b'sec-websocket-version'] == b'13'
    assert body == b''


async def test_websocket_rejects_unrequested_subprotocol() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept(subprotocol='other')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(
                port=config.port,
                path='/ws',
                subprotocol='chat',
            ),
            timeout=5,
        )

    assert status == 500
    assert b'sec-websocket-protocol' not in headers
    assert body == b''


async def test_websocket_rejects_extension_negotiation_headers() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept(
            headers=[(b'sec-websocket-extensions', b'permessage-deflate')]
        )
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(port=config.port, path='/ws'),
            timeout=5,
        )

    assert status == 500
    assert b'sec-websocket-extensions' not in headers
    assert body == b''


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_client_close_is_acknowledged_and_stream_ends(
    transport: str,
) -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept', 'headers': []})
        assert (await receive())['type'] == 'websocket.disconnect'

    await _assert_websocket_close_code(
        transport,
        app,
        client_frames=[_encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big'))],
        expected_code=1000,
    )


@pytest.mark.parametrize(
    ('transport', 'shutdown_method', 'expected_code'),
    [
        ('h2', 'shutdown', 1001),
        ('http1', 'shutdown', 1001),
        ('h2', 'restart', 1012),
        ('http1', 'restart', 1012),
    ],
)
async def test_websocket_graceful_server_shutdown_uses_expected_close_code(
    transport: str,
    shutdown_method: str,
    expected_code: int,
) -> None:
    disconnect_event = asyncio.Event()
    disconnects = []

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept', 'headers': []})
        disconnects.append(await receive())
        disconnect_event.set()

    config = Config(port=find_free_port(), timeout_graceful_shutdown=0.2)
    async with running_server(app, config) as server:
        if transport == 'h2':
            reader, writer, _conn, stream_id, initial = await _h2_open_websocket_stream(
                port=config.port,
                path='/ws',
            )
            assert initial is None
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=config.port,
                path='/ws',
            )
        getattr(server, shutdown_method)()
        await asyncio.wait_for(disconnect_event.wait(), timeout=5)
        try:
            if transport == 'h2':
                raw_frames = await _read_raw_h2_frames(reader)
                ws_buffer = b''.join(
                    payload
                    for frame_type, frame_stream_id, payload in raw_frames
                    if frame_type == 0x00 and frame_stream_id == stream_id
                )
                frames, remainder = _parse_ws_frames(ws_buffer)
                assert remainder == b''
            else:
                frames = await _read_http1_ws_server_result(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    assert disconnects == [{'type': 'websocket.disconnect', 'code': expected_code}]
    close_frames = [(opcode, payload) for opcode, payload in frames if opcode == 0x8]
    assert [opcode for opcode, _ in close_frames] == [0x8]
    assert _decode_ws_close_payload(close_frames[0][1])[0] == expected_code


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_send_after_disconnect_raises_oserror(
    transport: str,
) -> None:
    state = {}

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept', 'headers': []})
        state['disconnect'] = await receive()
        try:
            await send({'type': 'websocket.send', 'text': 'late'})
        except OSError:
            state['send_after_close'] = 'oserror'
        else:
            state['send_after_close'] = 'allowed'

    config = Config(port=find_free_port())
    async with running_server(app, config):
        if transport == 'h2':
            reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
                port=config.port,
                path='/ws',
            )
            assert initial is None
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')),
                end_stream=False,
            )
            writer.write(conn.data_to_send())
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=config.port,
                path='/ws',
            )
            writer.write(_encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')))
        await writer.drain()
        try:
            if transport == 'h2':
                await _read_ws_server_result(reader, writer, conn, stream_id)
            else:
                await _read_http1_ws_server_result(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    assert state == {
        'disconnect': {'type': 'websocket.disconnect', 'code': 1000},
        'send_after_close': 'oserror',
    }


async def test_websocket_rejects_rsv1_frames_without_extensions() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()

    await _assert_h2_websocket_close_code(
        websocket_app,
        client_frames=[_encode_ws_client_frame(0x1, b'hello', first_byte=0xC1)],
        expected_code=1002,
    )


async def test_websocket_rejects_new_data_frame_before_fragment_completion() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()

    await _assert_h2_websocket_close_code(
        websocket_app,
        client_frames=[
            _encode_ws_client_frame(0x1, b'hel', first_byte=0x01),
            _encode_ws_client_frame(0x1, b'lo'),
        ],
        expected_code=1002,
    )


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_invalid_outbound_close_reason_falls_back_to_1011(
    transport: str,
) -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept', 'headers': []})
        await send({'type': 'websocket.close', 'code': 1000, 'reason': 'x' * 124})

    await _assert_websocket_close_code(transport, app, expected_code=1011)


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_invalid_utf8_text_is_closed_with_1007(
    transport: str,
) -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()

    await _assert_websocket_close_code(
        transport,
        websocket_app,
        client_frames=[_encode_ws_client_frame(0x1, b'\xff')],
        expected_code=1007,
    )


async def test_http1_websocket_negotiates_permessage_deflate() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(
                port=config.port,
                path='/ws',
                extensions='permessage-deflate',
            ),
            timeout=5,
        )

    assert status == 101
    assert headers[b'sec-websocket-extensions'] == (
        b'permessage-deflate; server_no_context_takeover; client_no_context_takeover'
    )
    assert body == b''


async def test_h2_websocket_negotiates_permessage_deflate() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(
                port=config.port,
                path='/ws',
                extensions='permessage-deflate',
            ),
            timeout=5,
        )

    assert status == 200
    assert headers[b'sec-websocket-extensions'] == (
        b'permessage-deflate; server_no_context_takeover; client_no_context_takeover'
    )
    assert body == b'\x88\x02\x03\xe8'


@pytest.mark.parametrize(
    ('handshake', 'expected_status'),
    [
        (_h2_websocket_handshake, 200),
        (_http1_websocket_handshake, 101),
    ],
)
async def test_websocket_negotiates_permessage_deflate_across_transports(
    handshake,
    expected_status: int,
) -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        status, headers, _ = await asyncio.wait_for(
            handshake(
                port=config.port,
                path='/ws',
                extensions='permessage-deflate',
            ),
            timeout=5,
        )

    assert status == expected_status
    assert headers[b'sec-websocket-extensions'] == (
        b'permessage-deflate; server_no_context_takeover; client_no_context_takeover'
    )


async def test_h2_websocket_permessage_deflate_round_trip() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=find_free_port())
    async with running_server(websocket_app, config):
        reader, writer, conn, stream_id, initial = await _h2_open_websocket_stream(
            port=config.port,
            path='/ws',
            extensions='permessage-deflate',
        )
        try:
            assert initial is None
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(
                    0x1,
                    _compress_permessage_deflate(b'hello'),
                    first_byte=0xC1,
                ),
                end_stream=False,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            ws_buffer = b''
            echoed = None
            close_code = None
            while echoed is None or close_code is None:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                if not data:
                    raise RuntimeError('websocket closed before echo and close arrived')
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.DataReceived):
                        ws_buffer += event.data
                        conn.acknowledge_received_data(
                            event.flow_controlled_length,
                            stream_id,
                        )
                frames, ws_buffer = _parse_ws_frames_detailed(ws_buffer)
                for first, opcode, payload in frames:
                    if opcode == 0x1:
                        echoed = (
                            _decompress_permessage_deflate(payload).decode()
                            if first & 0x40
                            else payload.decode()
                        )
                    elif opcode == 0x8:
                        close_code = _decode_ws_close_payload(payload)[0]
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    assert echoed == 'echo:hello'
    assert close_code == 1000


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_message_size_limit_closes_with_1009(
    transport: str,
) -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()

    await _assert_websocket_close_code(
        transport,
        websocket_app,
        client_frames=[_encode_ws_client_frame(0x1, b'hello')],
        expected_code=1009,
        config=Config(port=find_free_port(), websocket_max_message_size=4),
    )


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_compressed_message_size_limit_closes_with_1009(
    transport: str,
) -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()

    await _assert_websocket_close_code(
        transport,
        websocket_app,
        client_frames=[
            _encode_ws_client_frame(
                0x1,
                _compress_permessage_deflate(b'hello'),
                first_byte=0xC1,
            )
        ],
        expected_code=1009,
        config=Config(port=find_free_port(), websocket_max_message_size=4),
        extensions='permessage-deflate',
    )
