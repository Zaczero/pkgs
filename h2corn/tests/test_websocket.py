import asyncio
import socket
import struct
import zlib
from contextlib import suppress
from typing import NamedTuple

import h2.config
import h2.connection
import h2.events
import pytest
from fastapi import FastAPI, WebSocket
from h2corn import Config, WebSocketScope

from tests._support import (
    h2_request,
    open_h2_connection,
    read_http1_response,
    read_raw_h2_frames,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio


class _H2WsHandshake(NamedTuple):
    """Client-side websocket state carried out of the h2 handshake helper.

    `terminal` is None while the stream is still open; `frames`/`buffer` hold
    any websocket frames (and residual partial-frame bytes) that arrived in
    the same socket flights as the handshake response and must seed the next
    reader instead of being dropped.
    """

    terminal: str | None
    detail: int | None
    frames: list[tuple[int, bytes]]
    buffer: bytes


async def _close_writer_after_expected_reset(writer: asyncio.StreamWriter) -> None:
    """Release a client transport after the tested server-side reset."""
    with suppress(ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
        writer.close()
    with suppress(ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
        await writer.wait_closed()


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


def _encode_ws_client_header_only(opcode: int, payload_len: int) -> bytes:
    """A complete client frame header (including mask), with no payload bytes.

    The wire decoder must reject invalid opcodes and oversized controls as
    soon as this much is available.  Do not manufacture the declared payload
    just to prove the admission point.
    """
    if payload_len < 126:
        length = bytes([0x80 | payload_len])
    elif payload_len < (1 << 16):
        length = b'\xfe' + payload_len.to_bytes(2, 'big')
    else:
        length = b'\xff' + payload_len.to_bytes(8, 'big')
    return bytes([0x80 | opcode]) + length + b'\x00\x00\x00\x00'


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
        # An incomplete frame must be handed back whole: rewinding a fixed two
        # bytes resumes inside a frame that carried an extended length or a
        # mask, and every later frame is then read out of a payload.
        start = cursor
        if len(data) - cursor < 2:
            return frames, data[start:]
        first = data[cursor]
        second = data[cursor + 1]
        cursor += 2
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            if len(data) - cursor < 2:
                return frames, data[start:]
            length = int.from_bytes(data[cursor : cursor + 2], 'big')
            cursor += 2
        elif length == 127:
            if len(data) - cursor < 8:
                return frames, data[start:]
            length = int.from_bytes(data[cursor : cursor + 8], 'big')
            cursor += 8
        mask = b''
        if masked:
            if len(data) - cursor < 4:
                return frames, data[start:]
            mask = data[cursor : cursor + 4]
            cursor += 4
        if len(data) - cursor < length:
            return frames, data[start:]
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
        # An incomplete frame must be handed back whole: rewinding a fixed two
        # bytes resumes inside a frame that carried an extended length or a
        # mask, and every later frame is then read out of a payload.
        start = cursor
        if len(data) - cursor < 2:
            return frames, data[start:]
        first = data[cursor]
        second = data[cursor + 1]
        cursor += 2
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            if len(data) - cursor < 2:
                return frames, data[start:]
            length = int.from_bytes(data[cursor : cursor + 2], 'big')
            cursor += 2
        elif length == 127:
            if len(data) - cursor < 8:
                return frames, data[start:]
            length = int.from_bytes(data[cursor : cursor + 8], 'big')
            cursor += 8
        if masked:
            if len(data) - cursor < 4:
                return frames, data[start:]
            cursor += 4
        if len(data) - cursor < length:
            return frames, data[start:]
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
    settings_header = conn.initiate_upgrade_connection()
    assert settings_header is not None
    settings = settings_header.decode()
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
    '_H2WsHandshake',
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
                handshake = _H2WsHandshake('ended', None, initial_frames, ws_buffer)
                return reader, writer, conn, stream_id, handshake
            elif isinstance(event, h2.events.StreamReset):
                handshake = _H2WsHandshake(
                    'reset', int(event.error_code), initial_frames, ws_buffer
                )
                return reader, writer, conn, stream_id, handshake
            elif isinstance(event, h2.events.ConnectionTerminated):
                assert event.error_code is not None
                handshake = _H2WsHandshake(
                    'goaway', int(event.error_code), initial_frames, ws_buffer
                )
                return reader, writer, conn, stream_id, handshake
        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()

    assert status == 200
    # The stream is still open, but frames may already have arrived in the
    # same socket flight as the response headers — carry them (and any
    # residual partial-frame bytes) forward so no frame is ever dropped at
    # the handshake handoff.
    return (
        reader,
        writer,
        conn,
        stream_id,
        _H2WsHandshake(None, None, initial_frames, ws_buffer),
    )


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
        await _close_writer_after_expected_reset(writer)

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
    handshake: _H2WsHandshake | None = None,
) -> tuple[str, int | None, list[tuple[int, bytes]]]:
    frames = list(handshake.frames) if handshake else []
    ws_buffer = handshake.buffer if handshake else b''
    if handshake is not None and handshake.terminal is not None:
        return handshake.terminal, handshake.detail, frames

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
                assert event.error_code is not None
                return 'goaway', int(event.error_code), frames

        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()


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
    config = config or Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
            extensions=extensions,
        )
        try:
            if client_frames:
                for frame in client_frames:
                    conn.send_data(stream_id, frame, end_stream=False)
                writer.write(conn.data_to_send())
                await writer.drain()
            terminal, detail, frames = await _read_ws_server_result(
                reader,
                writer,
                conn,
                stream_id,
                handshake,
            )
        finally:
            await _close_writer_after_expected_reset(writer)

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
    config = config or Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
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
            await _close_writer_after_expected_reset(writer)

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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, subprotocol, echoed = await asyncio.wait_for(
            _h2_websocket_round_trip(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        headers, echoed = await asyncio.wait_for(
            _http1_websocket_round_trip(
                port=server_port(server), path='/ws', text='hello'
            ),
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

    config = Config(port=0, timeout_request_body_idle=0.05)
    async with running_server(websocket_app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
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

    config = Config(port=0, timeout_request_body_idle=0.05)
    async with running_server(websocket_app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None
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
                handshake,
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, _ = await asyncio.wait_for(
            handshake(
                port=server_port(server),
                path='/ws',
                subprotocol='chat, superchat',
            ),
            timeout=5,
        )

    assert status == expected_status
    assert headers[b'sec-websocket-protocol'] == b'superchat'


async def test_http1_websocket_scope_exposes_required_empty_subprotocols() -> None:
    subprotocols = object()
    http_version = None
    extensions = None
    events = []

    async def app(scope, receive, send):
        nonlocal extensions, http_version, subprotocols
        http_version = scope['http_version']
        subprotocols = scope['subprotocols']
        extensions = scope['extensions']
        events.append(await receive())
        await send({'type': 'websocket.close'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(port=server_port(server), path='/ws'),
            timeout=5,
        )

    assert http_version == '1.1'
    assert subprotocols == []
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
        # Indexed, not `.get`: this request configures a non-empty root path,
        # so the key must be there. A defensive default would let the scope
        # stop carrying it -- or carry the wrong value -- unnoticed.
        state['root_path'] = scope['root_path']
        state['contract_keys'] = set(WebSocketScope.__required_keys__) - set(scope)
        assert await receive() == {'type': 'websocket.connect'}
        await send({'type': 'websocket.close'})

    config = Config(
        port=0,
        root_path='/root',
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            handshake(
                port=server_port(server),
                path='/ws',
                extra_headers=[
                    (
                        b'forwarded',
                        b'for=203.0.113.10;proto=https;host="example.com:9443"',
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
    assert state['contract_keys'] == set(), (
        f'websocket scope is missing required keys: {state["contract_keys"]}'
    )


async def test_http1_websocket_invalid_version_is_rejected_with_426() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(
                port=server_port(server), path='/ws', version='12'
            ),
            timeout=5,
        )

    assert status == 426
    assert headers[b'sec-websocket-version'] == b'13'
    assert body == b''


@pytest.mark.parametrize(
    'handshake', [_h2_websocket_handshake, _http1_websocket_handshake]
)
@pytest.mark.parametrize(
    ('version', 'duplicate'),
    [
        ('12', '13'),
        ('13', '12'),
        ('13', '13'),
    ],
)
async def test_websocket_duplicate_versions_are_rejected_before_app_dispatch(
    handshake,
    version: str,
    duplicate: str,
) -> None:
    dispatched = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        dispatched.append(scope)
        await receive()
        await send({'type': 'websocket.accept'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await handshake(
            port=server_port(server),
            path='/ws',
            version=version,
            extra_headers=[(b'sec-websocket-version', duplicate.encode())],
        )

    assert (status, body) == (400, b'')
    assert b'upgrade' not in headers
    assert dispatched == []


@pytest.mark.parametrize(
    ('handshake', 'expected_status'),
    [
        (_h2_websocket_handshake, 200),
        (_http1_websocket_handshake, 101),
    ],
)
async def test_websocket_single_supported_version_dispatches(
    handshake, expected_status: int
) -> None:
    dispatched = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        dispatched.append(scope)
        await receive()
        await send({'type': 'websocket.accept'})
        await send({'type': 'websocket.close'})

    async with running_server(app, Config(port=0)) as server:
        status, _headers, _body = await handshake(
            port=server_port(server), path='/ws', version='13'
        )

    assert status == expected_status
    assert len(dispatched) == 1


@pytest.mark.parametrize(
    ('key', 'duplicate'),
    [
        ('not-a-websocket-key', b'dGhlIHNhbXBsZSBub25jZQ=='),
        ('dGhlIHNhbXBsZSBub25jZQ==', b'not-a-websocket-key'),
        ('dGhlIHNhbXBsZSBub25jZQ==', b'dGhlIHNhbXBsZSBub25jZQ=='),
    ],
)
async def test_http1_websocket_invalid_or_duplicate_key_is_rejected_before_app_dispatch(
    key: str,
    duplicate: bytes,
) -> None:
    dispatched = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        dispatched.append(scope)
        await receive()
        await send({'type': 'websocket.accept'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await _http1_websocket_handshake(
            port=server_port(server),
            path='/ws',
            key=key,
            extra_headers=[(b'sec-websocket-key', duplicate)],
        )

    assert (status, body) == (400, b'')
    assert b'upgrade' not in headers
    assert dispatched == []


async def test_http1_websocket_missing_key_is_rejected_with_400() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(port=server_port(server), path='/ws', key=None),
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(
                port=server_port(server),
                path='/ws',
                method='POST',
            ),
            timeout=5,
        )

    assert status == 400
    assert body == b''
    assert b'upgrade' not in headers


@pytest.mark.parametrize(
    'headers',
    [
        [(b'content-length', b'1')],
        [(b'transfer-encoding', b'chunked')],
    ],
)
async def test_http1_websocket_handshake_rejects_a_request_body(headers) -> None:
    """Upgrade is a header-only transition, never an ASGI request body."""
    dispatched = []

    async def app(scope, receive, send):
        dispatched.append(scope)
        await send({'type': 'websocket.accept'})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        status, response_headers, body = await _http1_websocket_handshake(
            port=server_port(server), path='/ws', extra_headers=headers
        )

    assert status == 400
    assert body == b''
    assert b'upgrade' not in response_headers
    assert dispatched == []


@pytest.mark.parametrize(
    ('handshake', 'expected_status'),
    [
        (_h2_websocket_handshake, 400),
        (_http1_websocket_handshake, 400),
    ],
)
@pytest.mark.parametrize('subprotocol', ['bad token', 'chat/bad'])
async def test_websocket_subprotocols_must_be_tokens_across_transports(
    handshake,
    expected_status: int,
    subprotocol: str,
) -> None:
    """Malformed protocol names are rejected at handshake ingress, not exposed."""
    dispatched = []

    async def app(scope, receive, send):
        dispatched.append(scope)
        await send({'type': 'websocket.accept'})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        status, headers, body = await handshake(
            port=server_port(server), path='/ws', subprotocol=subprotocol
        )

    assert status == expected_status
    assert body == b''
    assert b'sec-websocket-protocol' not in headers
    assert dispatched == []


async def test_http1_h2c_upgrade_round_trip() -> None:
    async def app(scope, receive, send):
        assert scope['http_version'] == '2'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'upgraded'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            _http1_h2c_upgrade_request(port=server_port(server)),
            timeout=5,
        )

    assert status == 200
    assert body == b'upgraded'


def _full_inbound_queue_app(
    gate: asyncio.Event,
    finished: asyncio.Event,
    received: list,
):
    """Accept, pause, then drain every inbound message plus disconnect."""

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        assert (await receive())['type'] == 'websocket.connect'
        await send({'type': 'websocket.accept'})
        await gate.wait()
        try:
            while True:
                message = await receive()
                received.append(message)
                if message['type'] == 'websocket.disconnect':
                    return
        finally:
            finished.set()

    return app


def _parked_websocket_app(done: asyncio.Event):
    """Accept and park until cancelled or the process ends."""

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        await receive()
        await send({'type': 'websocket.accept'})
        try:
            await asyncio.Event().wait()
        finally:
            done.set()

    return app


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_inbound_backpressure_preserves_ordered_messages(
    transport: str,
) -> None:
    """A byte-full queue stalls decoding, not ordered data delivery.

    A peer Close or Ping queued behind unread application data is therefore
    delayed until the application drains. Server shutdown and locally
    scheduled ping timeouts stay independently selectable while this data
    plane is stalled; dedicated guards cover those paths below.
    """
    payloads = [bytes([index]) for index in range(6)]
    gate = asyncio.Event()
    finished = asyncio.Event()
    received: list = []
    app = _full_inbound_queue_app(gate, finished, received)
    client_data = b''.join(
        _encode_ws_client_frame(0x2, payload) for payload in payloads
    ) + _encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big'))

    config = Config(
        port=0,
        websocket_max_message_size=4,
        timeout_graceful_shutdown=5.0,
    )
    async with running_server(app, config) as server:
        if transport == 'h2':
            (
                reader,
                writer,
                conn,
                stream_id,
                handshake,
            ) = await _h2_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            try:
                assert handshake.terminal is None
                conn.send_data(stream_id, client_data, end_stream=False)
                writer.write(conn.data_to_send())
                await writer.drain()

                close_task = asyncio.create_task(
                    _read_ws_server_result(
                        reader,
                        writer,
                        conn,
                        stream_id,
                        handshake,
                    )
                )
                await asyncio.wait({close_task}, timeout=0.2)
                assert not close_task.done(), (
                    'peer Close passed unread application data before the '
                    'inbound queue was drained'
                )
                gate.set()
                await asyncio.wait_for(finished.wait(), timeout=2.0)
                terminal, detail, frames = await asyncio.wait_for(
                    close_task, timeout=2.0
                )
                assert terminal == 'ended', (terminal, detail, frames)
                close_frames = [payload for opcode, payload in frames if opcode == 0x8]
                assert [
                    _decode_ws_close_payload(payload)[0] for payload in close_frames
                ] == [1000]
            finally:
                writer.close()
                await writer.wait_closed()
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            try:
                writer.write(client_data)
                await writer.drain()

                close_task = asyncio.create_task(_read_http1_ws_server_result(reader))
                await asyncio.wait({close_task}, timeout=0.2)
                assert not close_task.done(), (
                    'peer Close passed unread application data before the '
                    'inbound queue was drained'
                )
                gate.set()
                await asyncio.wait_for(finished.wait(), timeout=2.0)
                frames = await asyncio.wait_for(close_task, timeout=2.0)
                close_frames = [payload for opcode, payload in frames if opcode == 0x8]
                assert [
                    _decode_ws_close_payload(payload)[0] for payload in close_frames
                ] == [1000]
            finally:
                writer.close()
                await writer.wait_closed()

    messages = [
        message['bytes']
        for message in received
        if message['type'] == 'websocket.receive'
    ]
    assert messages == payloads
    assert received[-1] == {'type': 'websocket.disconnect', 'code': 1000}


async def test_http1_websocket_peer_close_drains_full_inbound_queue() -> None:
    """Peer Close follows a byte-full inbound queue without dropping data.

    The Close deliberately waits behind the 33 messages while the application
    is paused. Once it resumes, every message arrives in order before the
    disconnect and the Close echo.
    """
    queued_messages = 32
    message_size = 512 * 1024
    payloads = [
        f'{index:02d}'.encode() + (b'x' * (message_size - 2))
        for index in range(queued_messages + 1)
    ]
    gate = asyncio.Event()
    finished = asyncio.Event()
    received: list = []
    app = _full_inbound_queue_app(gate, finished, received)

    config = Config(port=0, timeout_graceful_shutdown=5.0)
    async with running_server(app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            # The first 32 exactly exhaust 16 MiB. Message 33 is therefore
            # pending on byte capacity when the following Close arrives.
            for payload in payloads:
                writer.write(_encode_ws_client_frame(0x2, payload))
            writer.write(_encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')))
            await writer.drain()

            gate.set()
            await asyncio.wait_for(finished.wait(), timeout=2.0)
            frames = await asyncio.wait_for(
                _read_http1_ws_server_result(reader),
                timeout=2.0,
            )
            close_frames = [payload for opcode, payload in frames if opcode == 0x8]
            assert close_frames, frames
            assert _decode_ws_close_payload(close_frames[0])[0] == 1000
        finally:
            writer.close()
            await writer.wait_closed()

    texts = [
        message['bytes']
        for message in received
        if message['type'] == 'websocket.receive'
    ]
    assert texts == payloads
    assert received[-1]['type'] == 'websocket.disconnect'
    assert received[-1]['code'] == 1000


async def test_h2_websocket_peer_close_drains_full_inbound_queue() -> None:
    """RFC 8441 Close follows the full queue without dropping message 33."""
    queued_messages = 32
    inbound_budget = 2 * 1024 * 1024
    # One complete WebSocket message fits in one max-sized H2 DATA frame.
    message_size = (64 * 1024) - 16
    payloads = [
        f'{index:02d}'.encode() + (b'x' * (message_size - 2))
        for index in range(queued_messages + 1)
    ]
    gate = asyncio.Event()
    finished = asyncio.Event()
    received: list = []
    app = _full_inbound_queue_app(gate, finished, received)

    config = Config(
        port=0,
        websocket_max_message_size=inbound_budget,
        timeout_graceful_shutdown=5.0,
    )
    async with running_server(app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None

            async def send_ws_bytes(data: bytes) -> None:
                """Honor peer flow credit while preserving WS frame boundaries."""
                offset = 0
                while offset < len(data):
                    window = conn.local_flow_control_window(stream_id)
                    if window == 0:
                        inbound = await asyncio.wait_for(
                            reader.read(1 << 16), timeout=3
                        )
                        assert inbound, 'server closed while returning DATA credit'
                        conn.receive_data(inbound)
                        pending = conn.data_to_send()
                        if pending:
                            writer.write(pending)
                            await writer.drain()
                        continue
                    end = offset + min(
                        len(data) - offset, window, conn.max_outbound_frame_size
                    )
                    conn.send_data(stream_id, data[offset:end], end_stream=False)
                    writer.write(conn.data_to_send())
                    await writer.drain()
                    offset = end

            for payload in payloads:
                await send_ws_bytes(_encode_ws_client_frame(0x2, payload))
            await send_ws_bytes(_encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')))

            gate.set()
            await asyncio.wait_for(finished.wait(), timeout=2.0)
            terminal, detail, frames = await asyncio.wait_for(
                _read_ws_server_result(
                    reader,
                    writer,
                    conn,
                    stream_id,
                    handshake,
                ),
                timeout=2.0,
            )
            assert terminal == 'ended', (terminal, detail, frames)
            close_frames = [payload for opcode, payload in frames if opcode == 0x8]
            assert close_frames, frames
            assert _decode_ws_close_payload(close_frames[0])[0] == 1000
        finally:
            writer.close()
            await writer.wait_closed()

    messages = [
        message['bytes']
        for message in received
        if message['type'] == 'websocket.receive'
    ]
    assert messages == payloads
    assert received[-1]['type'] == 'websocket.disconnect'
    assert received[-1]['code'] == 1000


@pytest.mark.parametrize('queued_messages', [32, 33, 64])
async def test_http1_websocket_ping_timeout_with_full_inbound_queue(
    queued_messages: int,
) -> None:
    """A local ping timeout fires at every stalled inbound queue depth.

    At 33 and 64 messages, capacity acquisition is pending and transport reads
    are paused. The timeout must still win that selection rather than waiting
    for the application to resume.
    """
    done = asyncio.Event()
    app = _parked_websocket_app(done)
    interval, timeout = 0.05, 0.1

    config = Config(
        port=0,
        websocket_max_message_size=32,
        websocket_ping_interval=interval,
        websocket_ping_timeout=timeout,
        timeout_graceful_shutdown=5.0,
    )
    async with running_server(app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            for index in range(queued_messages):
                writer.write(_encode_ws_client_frame(0x1, bytes([index])))
            await writer.drain()

            with suppress(ConnectionResetError):
                closed = await asyncio.wait_for(
                    reader.read(),
                    timeout=(interval + timeout) * 8,
                )
                frames, _ = _parse_ws_frames(closed)
                assert any(opcode == 0x9 for opcode, _ in frames)
            await asyncio.wait_for(done.wait(), timeout=2.0)
        finally:
            await _close_writer_after_expected_reset(writer)


@pytest.mark.parametrize('queued_messages', [32, 33, 64])
async def test_h2_websocket_ping_timeout_with_full_inbound_queue(
    queued_messages: int,
) -> None:
    """RFC 8441 local ping timeouts win at every stalled queue depth.

    Peer-reset finish does not RST the H2 stream (same as other 1006 paths);
    the load-bearing signal is that the application is aborted while the
    inbound data plane is still full. At 33 and 64 messages this specifically
    proves the timeout is independent of the pending-capacity wait.
    """
    done = asyncio.Event()
    app = _parked_websocket_app(done)
    interval, timeout = 0.05, 0.1

    config = Config(
        port=0,
        websocket_max_message_size=32,
        websocket_ping_interval=interval,
        websocket_ping_timeout=timeout,
        timeout_graceful_shutdown=5.0,
    )
    async with running_server(app, config) as server:
        _reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None
            for index in range(queued_messages):
                conn.send_data(
                    stream_id,
                    _encode_ws_client_frame(0x1, bytes([index])),
                    end_stream=False,
                )
            writer.write(conn.data_to_send())
            await writer.drain()

            await asyncio.wait_for(done.wait(), timeout=(interval + timeout) * 8)
        finally:
            await _close_writer_after_expected_reset(writer)


@pytest.mark.parametrize('transport', ['h2', 'http1'])
@pytest.mark.parametrize('queued_messages', [32, 33, 64])
async def test_websocket_shutdown_reaches_a_fully_paused_inbound_queue(
    transport: str,
    queued_messages: int,
) -> None:
    """Server shutdown wins over a full or pending inbound data plane.

    With 33 or 64 messages the next decoded message is waiting for byte
    ownership and no transport read is selectable. Shutdown must still send
    its close and publish disconnect before the application is allowed to
    drain data.
    """
    gate = asyncio.Event()
    finished = asyncio.Event()
    received: list = []
    app = _full_inbound_queue_app(gate, finished, received)
    client_data = b''.join(
        _encode_ws_client_frame(0x2, bytes([index])) for index in range(queued_messages)
    )
    config = Config(
        port=0,
        websocket_max_message_size=32,
        timeout_graceful_shutdown=5.0,
    )

    async with running_server(app, config) as server:
        if transport == 'h2':
            (
                reader,
                writer,
                conn,
                stream_id,
                handshake,
            ) = await _h2_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            try:
                assert handshake.terminal is None
                conn.send_data(stream_id, client_data, end_stream=False)
                writer.write(conn.data_to_send())
                await writer.drain()

                server.shutdown()
                # A graceful H2 shutdown sends GOAWAY before the existing
                # stream's Close DATA. hyper-h2 closes its connection state at
                # GOAWAY, so inspect the raw continuation as the shutdown
                # close-code test does.
                raw_frames = await read_raw_h2_frames(
                    reader,
                    timeout=0.2,
                    stop_at_goaway=False,
                )
                ws_buffer = b''.join(
                    payload
                    for frame_type, _flags, frame_stream_id, payload in raw_frames
                    if frame_type == 0x00 and frame_stream_id == stream_id
                )
                frames, remainder = _parse_ws_frames(ws_buffer)
                assert remainder == b''
                close_frames = [payload for opcode, payload in frames if opcode == 0x8]
                assert [
                    _decode_ws_close_payload(payload)[0] for payload in close_frames
                ] == [1001]

                gate.set()
                await asyncio.wait_for(finished.wait(), timeout=2.0)
            finally:
                writer.close()
                await writer.wait_closed()
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            try:
                writer.write(client_data)
                await writer.drain()

                server.shutdown()
                frames = await asyncio.wait_for(
                    _read_http1_ws_server_result(reader),
                    timeout=2.0,
                )
                close_frames = [payload for opcode, payload in frames if opcode == 0x8]
                assert [
                    _decode_ws_close_payload(payload)[0] for payload in close_frames
                ] == [1001]

                gate.set()
                await asyncio.wait_for(finished.wait(), timeout=2.0)
            finally:
                writer.close()
                await writer.wait_closed()

    assert received[-1] == {'type': 'websocket.disconnect', 'code': 1001}


async def test_websocket_ping_interval_zero_emits_no_ping() -> None:
    """Interval zero is the off state; the configured timeout is irrelevant."""

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        await receive()
        await send({'type': 'websocket.accept'})
        await asyncio.Event().wait()

    config = Config(
        port=0,
        websocket_ping_interval=0.0,
        websocket_ping_timeout=0.05,
    )
    async with running_server(app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            try:
                data = await asyncio.wait_for(reader.read(65535), timeout=0.25)
            except TimeoutError:
                data = b''
            frames, _ = _parse_ws_frames(data)
            assert not any(opcode == 0x9 for opcode, _ in frames), frames
        finally:
            writer.close()
            await writer.wait_closed()


async def test_websocket_delivers_every_queued_message_when_the_app_returns() -> None:
    """A `send(...)`-then-return app must not lose what it queued.

    Once the application has returned there is no later batch, so the terminal
    drain cannot stop at the fairness quantum the steady-state loop uses: it
    bounds one batch at 64 KiB, and everything past that was dropped.
    """
    messages = 16
    payload = b'x' * (32 * 1024)

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        await receive()
        await send({'type': 'websocket.accept'})
        for _ in range(messages):
            await send({'type': 'websocket.send', 'bytes': payload})

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            frames = await _read_http1_ws_server_result(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    binary = [payload_bytes for opcode, payload_bytes in frames if opcode == 0x2]
    assert len(binary) == messages
    assert all(frame == payload for frame in binary)
    # And it ends with a proper close, not a bare EOF: the app returning and
    # its sender dropping are one event seen two ways, and only one of the two
    # used to send the close.
    assert [opcode for opcode, _ in frames if opcode == 0x8] == [0x8]


async def test_accepted_websocket_app_exception_still_finalizes_the_session() -> None:
    """An accepted app error must not turn the documented close into bare EOF."""

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        await receive()
        await send({'type': 'websocket.accept'})
        raise RuntimeError('intentional accepted-session failure')

    async with running_server(app, Config(port=0)) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            frames = await _read_http1_ws_server_result(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    close_frames = [payload for opcode, payload in frames if opcode == 0x8]
    assert len(close_frames) == 1, frames
    assert _decode_ws_close_payload(close_frames[0])[0] == 1000


async def test_websocket_ping_timeout_fires_when_pings_outpace_it() -> None:
    """A silent peer is dropped even when pings are more frequent than the timeout.

    `ping_interval` shorter than `ping_timeout` is the obvious reading of the
    two knobs, and it used to be the one configuration where the keepalive
    could never fire: each ping re-armed the deadline the previous one was
    being judged by, so a peer that never answered was never dropped.
    """
    interval, timeout = 0.1, 0.4

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        await receive()
        await send({'type': 'websocket.accept'})
        # Stay open; only the keepalive may end this session.
        await asyncio.Event().wait()

    config = Config(
        port=0,
        websocket_ping_interval=interval,
        websocket_ping_timeout=timeout,
    )
    async with running_server(app, config) as server:
        reader, writer, _ = await _http1_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            # This client never answers a ping. Allow the timeout plus an
            # interval of slack; the old behaviour never closed at all.
            closed = await asyncio.wait_for(
                reader.read(), timeout=(interval + timeout) * 6
            )
        finally:
            writer.close()
            await writer.wait_closed()

    # Pings arrive as 0x9 frames; the session ends without a graceful close.
    frames, _ = _parse_ws_frames(closed)
    assert any(opcode == 0x9 for opcode, _ in frames)


async def test_websocket_closes_normally_every_time_the_app_just_returns() -> None:
    """An app that returns without closing must always send a normal close.

    Its task completing and its send channel emptying are one event observed
    two ways, and the session used to end differently depending on which the
    `select!` saw first — so roughly half of these connections ended in a bare
    EOF logged as 1005. One connection cannot prove that; the repetition is
    what makes the race visible.
    """
    connections = 20

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        await receive()
        await send({'type': 'websocket.accept'})
        await send({'type': 'websocket.send', 'text': 'bye'})

    config = Config(port=0)
    closes = []
    async with running_server(app, config) as server:
        for _ in range(connections):
            reader, writer, _ = await _http1_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            try:
                frames = await _read_http1_ws_server_result(reader)
            finally:
                writer.close()
                await writer.wait_closed()
            closes.append([
                _decode_ws_close_payload(payload)
                for opcode, payload in frames
                if opcode == 0x8
            ])

    assert closes == [[(1000, '')]] * connections


async def test_websocket_multiple_messages_round_trip_on_one_stream() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        for _ in range(2):
            message = await websocket.receive_text()
            await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None
            frames = list(handshake.frames)
            terminal = None
            detail = None
            ws_buffer = handshake.buffer
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None
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
                handshake.buffer,
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None
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
            ws_buffer = handshake.buffer
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
    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
        )
        try:
            assert handshake.terminal is None
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
                handshake.buffer,
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            handshake(port=server_port(server), path='/ws'),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            handshake(port=server_port(server), path='/ws'),
            timeout=5,
        )

    assert state['extensions'] == {'websocket.http.response': {}}
    assert state['events'] == [{'type': 'websocket.connect'}]
    assert status == 401
    assert headers[b'content-type'] == b'text/plain'
    assert headers[b'content-length'] == b'6'
    assert b'date' in headers
    assert body == b'denied'
    if transport == 'http1':
        assert b'transfer-encoding' not in headers


@pytest.mark.parametrize(
    ('handshake', 'transport'),
    [
        (_h2_websocket_handshake, 'h2'),
        (_http1_websocket_handshake, 'http1'),
    ],
)
async def test_websocket_denial_rejects_informational_status_through_send(
    handshake,
    transport: str,
) -> None:
    caught = []

    async def app(scope, receive, send):
        assert (await receive()) == {'type': 'websocket.connect'}
        try:
            await send({
                'type': 'websocket.http.response.start',
                'status': 103,
                'headers': [],
            })
        except ValueError:
            caught.append(scope['http_version'])
            await send({
                'type': 'websocket.http.response.start',
                'status': 418,
                'headers': [],
            })
            await send({'type': 'websocket.http.response.body', 'body': b'fallback'})

    async with running_server(app, Config(port=0)) as server:
        status, _headers, body = await asyncio.wait_for(
            handshake(port=server_port(server), path='/ws'), timeout=5
        )

    assert caught == ['2' if transport == 'h2' else '1.1']
    assert (status, body) == (418, b'fallback')


@pytest.mark.parametrize(
    ('handshake', 'transport'),
    [
        (_h2_websocket_handshake, 'h2'),
        (_http1_websocket_handshake, 'http1'),
    ],
)
async def test_websocket_accept_strips_application_connection_fields(
    handshake,
    transport: str,
) -> None:
    application_fields = [
        (b'keep-alive', b'timeout=5'),
        (b'te', b'trailers'),
        (b'proxy-connection', b'keep-alive'),
        (b'connection', b'keep-alive'),
    ]

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        await receive()
        await send({'type': 'websocket.accept', 'headers': application_fields})
        await send({'type': 'websocket.close'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, _body = await asyncio.wait_for(
            handshake(port=server_port(server), path='/ws'), timeout=5
        )

    assert status == (200 if transport == 'h2' else 101)
    for name in (b'keep-alive', b'te', b'proxy-connection'):
        assert name not in headers
    if transport == 'h2':
        assert b'connection' not in headers
    else:
        # RFC 6455 owns this response field. The app's `keep-alive` request
        # cannot alter the server's required HTTP/1.1 upgrade syntax.
        assert headers[b'connection'].lower() == b'upgrade'


async def test_http1_websocket_accept_owns_transport_headers_once() -> None:
    application_fields = [
        (b'x-before', b'one'),
        (b'connection', b'upgrade'),
        (b'upgrade', b'not-websocket'),
        (b'sec-websocket-accept', b'bogus'),
        (b'x-after', b'two'),
    ]

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        await receive()
        await send({'type': 'websocket.accept', 'headers': application_fields})
        await send({'type': 'websocket.close'})

    async with running_server(app, Config(port=0)) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                b'GET /ws HTTP/1.1\r\n'
                b'Host: localhost\r\n'
                b'Connection: Upgrade\r\n'
                b'Upgrade: websocket\r\n'
                b'Sec-WebSocket-Version: 13\r\n'
                b'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n'
            )
            await writer.drain()
            response_head = await asyncio.wait_for(
                reader.readuntil(b'\r\n\r\n'), timeout=5
            )
        finally:
            writer.close()
            await writer.wait_closed()

    lines = response_head[:-4].split(b'\r\n')
    assert lines[0] == b'HTTP/1.1 101 Switching Protocols'
    fields = [line.split(b':', 1) for line in lines[1:]]
    assert [value.strip() for name, value in fields if name.lower() == b'upgrade'] == [
        b'websocket'
    ]
    assert [
        value.strip()
        for name, value in fields
        if name.lower() == b'sec-websocket-accept'
    ] == [b's3pPLMBiTxaQ9kYGzzhZRbK+xOo=']
    assert [
        (name.lower(), value.strip())
        for name, value in fields
        if name.lower() in {b'x-before', b'x-after'}
    ] == [(b'x-before', b'one'), (b'x-after', b'two')]


async def test_h2_websocket_accept_strips_http1_handshake_fields() -> None:
    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        await receive()
        await send({
            'type': 'websocket.accept',
            'headers': [
                (b'x-before', b'one'),
                (b'connection', b'upgrade'),
                (b'upgrade', b'not-websocket'),
                (b'sec-websocket-accept', b'bogus'),
                (b'x-after', b'two'),
            ],
        })
        await send({'type': 'websocket.close'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await _h2_websocket_handshake(
            port=server_port(server), path='/ws'
        )

    assert status == 200
    assert body == b'\x88\x02\x03\xe8'
    assert b'upgrade' not in headers
    assert b'sec-websocket-accept' not in headers
    assert headers[b'x-before'] == b'one'
    assert headers[b'x-after'] == b'two'


@pytest.mark.parametrize(
    ('handshake', 'transport'),
    [
        (_h2_websocket_handshake, 'h2'),
        (_http1_websocket_handshake, 'http1'),
    ],
)
async def test_websocket_denial_strips_application_connection_fields(
    handshake,
    transport: str,
) -> None:
    application_fields = [
        (b'keep-alive', b'timeout=5'),
        (b'te', b'trailers'),
        (b'proxy-connection', b'keep-alive'),
        (b'connection', b'keep-alive'),
    ]

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        await receive()
        await send({
            'type': 'websocket.http.response.start',
            'status': 401,
            'headers': application_fields,
        })
        await send({'type': 'websocket.http.response.body', 'body': b'denied'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await asyncio.wait_for(
            handshake(port=server_port(server), path='/ws'), timeout=5
        )

    assert (status, body) == (401, b'denied')
    for name in (b'keep-alive', b'te', b'proxy-connection'):
        assert name not in headers
    if transport == 'h2':
        assert b'connection' not in headers
    else:
        # Denials close the HTTP/1.1 upgrade connection; this is transport
        # policy, not the app's stripped `connection: keep-alive` field.
        assert headers[b'connection'].lower() == b'close'


async def test_h2_websocket_scope_exposes_required_empty_subprotocols() -> None:
    subprotocols = object()
    http_version = None
    extensions = None
    events = []

    async def app(scope, receive, send):
        nonlocal extensions, http_version, subprotocols
        http_version = scope['http_version']
        subprotocols = scope['subprotocols']
        extensions = scope['extensions']
        events.append(await receive())
        await send({'type': 'websocket.close'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(port=server_port(server), path='/ws'),
            timeout=5,
        )

    assert http_version == '2'
    assert subprotocols == []
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _http1_websocket_handshake(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(port=server_port(server), path='/ws', version='12'),
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

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, body = await asyncio.wait_for(
            _h2_websocket_handshake(
                port=server_port(server),
                path='/ws',
                subprotocol='chat',
            ),
            timeout=5,
        )

    assert status == 500
    assert b'sec-websocket-protocol' not in headers
    assert body == b''


@pytest.mark.parametrize(
    'handshake', [_h2_websocket_handshake, _http1_websocket_handshake]
)
@pytest.mark.parametrize(
    'name', [b'sec-websocket-protocol', b'sec-websocket-extensions']
)
async def test_websocket_rejects_application_negotiation_headers(
    handshake, name: bytes
) -> None:
    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            return
        await receive()
        await send({'type': 'websocket.accept', 'headers': [(name, b'value')]})
        await send({'type': 'websocket.close'})

    async with running_server(app, Config(port=0)) as server:
        status, headers, body = await asyncio.wait_for(
            handshake(port=server_port(server), path='/ws'),
            timeout=5,
        )

    assert status == 500
    assert name not in headers
    assert body == b''


async def test_failed_handshake_cancels_pending_app_before_releasing_admission() -> (
    None
):
    websocket_started = asyncio.Event()
    websocket_cancelled = asyncio.Event()
    http_saw_cancelled: list[bool] = []

    async def app(scope, receive, send):
        if scope['type'] == 'http':
            http_saw_cancelled.append(websocket_cancelled.is_set())
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'after-cancel'})
            return

        assert (await receive()) == {'type': 'websocket.connect'}
        websocket_started.set()
        try:
            await asyncio.Future()
        finally:
            websocket_cancelled.set()

    config = Config(
        port=0,
        access_log=False,
        lifespan='off',
        limit_concurrency=1,
        timeout_handshake=0.05,
    )
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        del reader
        writer.write(
            b'GET /ws HTTP/1.1\r\n'
            b'Host: localhost\r\n'
            b'Connection: Upgrade\r\n'
            b'Upgrade: websocket\r\n'
            b'Sec-WebSocket-Version: 13\r\n'
            b'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n'
        )
        await writer.drain()
        await asyncio.wait_for(websocket_started.wait(), timeout=2)

        # Force the handshake response write down a real transport-error path.
        raw_socket = writer.get_extra_info('socket')
        raw_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack('ii', 1, 0),
        )
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass

        for _ in range(200):
            status, body = await h2_request(port=server_port(server))
            if status == 200:
                break
            assert status == 503
            await asyncio.sleep(0.01)
        else:
            raise AssertionError(
                'request admission was not released after cancellation'
            )

        assert body == b'after-cancel'
        await asyncio.wait_for(websocket_cancelled.wait(), timeout=2)

    assert http_saw_cancelled == [True]


@pytest.mark.parametrize('transport', ['h2', 'http1'])
@pytest.mark.parametrize(
    ('peer_payload', 'expected_reply', 'expected_disconnect_code'),
    [
        ((1000).to_bytes(2, 'big'), (1000).to_bytes(2, 'big'), 1000),
        # RFC 6455 reserves 1005 for this exact ASGI observation, but it is
        # never encoded on the wire: an empty Close is answered with 88 00.
        (b'', b'', 1005),
    ],
)
async def test_websocket_client_close_is_acknowledged_and_stream_ends(
    transport: str,
    peer_payload: bytes,
    expected_reply: bytes,
    expected_disconnect_code: int,
) -> None:
    disconnects = []

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept', 'headers': []})
        disconnects.append(await receive())

    config = Config(port=0)
    async with running_server(app, config) as server:
        if transport == 'h2':
            (
                reader,
                writer,
                conn,
                stream_id,
                handshake,
            ) = await _h2_open_websocket_stream(port=server_port(server), path='/ws')
            try:
                conn.send_data(
                    stream_id,
                    _encode_ws_client_frame(0x8, peer_payload),
                    end_stream=False,
                )
                writer.write(conn.data_to_send())
                await writer.drain()
                terminal, detail, frames = await _read_ws_server_result(
                    reader, writer, conn, stream_id, handshake
                )
                assert (terminal, detail) == ('ended', None)
            finally:
                writer.close()
                await writer.wait_closed()
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=server_port(server), path='/ws'
            )
            try:
                writer.write(_encode_ws_client_frame(0x8, peer_payload))
                await writer.drain()
                frames = await _read_http1_ws_server_result(reader)
            finally:
                writer.close()
                await writer.wait_closed()

    close_frames = [payload for opcode, payload in frames if opcode == 0x8]
    assert close_frames == [expected_reply]
    assert disconnects == [
        {'type': 'websocket.disconnect', 'code': expected_disconnect_code}
    ]


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_websocket_rejects_peer_close_codes_outside_the_wire_range(
    transport: str,
) -> None:
    async def app(scope, receive, send):
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept'})
        await receive()

    await _assert_websocket_close_code(
        transport,
        app,
        client_frames=[_encode_ws_client_frame(0x8, (5000).to_bytes(2, 'big'))],
        expected_code=1002,
    )


@pytest.mark.parametrize(
    ('transport', 'restarting', 'expected_code'),
    [
        ('h2', False, 1001),
        ('http1', False, 1001),
        ('h2', True, 1012),
        ('http1', True, 1012),
    ],
)
async def test_websocket_graceful_server_shutdown_uses_expected_close_code(
    transport: str,
    restarting: bool,
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

    config = Config(port=0, timeout_graceful_shutdown=0.2)
    async with running_server(app, config) as server:
        stream_id = None
        if transport == 'h2':
            (
                reader,
                writer,
                _conn,
                stream_id,
                handshake,
            ) = await _h2_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            assert handshake.terminal is None
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
        if restarting:
            server._request_restart()
        else:
            server.shutdown()
        await asyncio.wait_for(disconnect_event.wait(), timeout=5)
        try:
            if transport == 'h2':
                raw_frames = await read_raw_h2_frames(reader, stop_at_goaway=False)
                ws_buffer = b''.join(
                    payload
                    for frame_type, _flags, frame_stream_id, payload in raw_frames
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        conn = None
        stream_id = None
        handshake = None
        if transport == 'h2':
            (
                reader,
                writer,
                conn,
                stream_id,
                handshake,
            ) = await _h2_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            assert handshake.terminal is None
            conn.send_data(
                stream_id,
                _encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')),
                end_stream=False,
            )
            writer.write(conn.data_to_send())
        else:
            reader, writer, _ = await _http1_open_websocket_stream(
                port=server_port(server),
                path='/ws',
            )
            writer.write(_encode_ws_client_frame(0x8, (1000).to_bytes(2, 'big')))
        await writer.drain()
        try:
            if transport == 'h2':
                assert conn is not None and stream_id is not None
                await _read_ws_server_result(reader, writer, conn, stream_id, handshake)
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


@pytest.mark.parametrize(
    ('handshake', 'expected_status'),
    [
        (_h2_websocket_handshake, 200),
        (_http1_websocket_handshake, 101),
    ],
)
@pytest.mark.parametrize(
    ('offer', 'negotiates'),
    [
        ('permessage-deflate', True),
        *[
            (f'permessage-deflate; client_max_window_bits={bits}', True)
            for bits in range(8, 16)
        ],
        ('permessage-deflate; client_max_window_bits=07', False),
        ('permessage-deflate; client_max_window_bits=16', False),
        ('permessage-deflate; server_max_window_bits', False),
        ('permessage-deflate; server_max_window_bits=15', False),
        ('permessage-deflate; unknown_parameter', False),
        (
            'permessage-deflate; client_max_window_bits=8; client_max_window_bits=9',
            False,
        ),
        ('permessage-deflate; client_no_context_takeover=yes', False),
        ('permessage-deflate; server_no_context_takeover=yes', False),
        ('permessage-deflate; =8, permessage-deflate', True),
        ('permessage-deflate; client_max_window_bits="\\1\\5"', True),
    ],
)
async def test_deflate_offer_matrix(
    handshake,
    expected_status: int,
    offer: str,
    negotiates: bool,
) -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        status, headers, _ = await asyncio.wait_for(
            handshake(
                port=server_port(server),
                path='/ws',
                extensions=offer,
            ),
            timeout=5,
        )

    assert status == expected_status
    expected_extension = (
        b'permessage-deflate; server_no_context_takeover; client_no_context_takeover'
    )
    if negotiates:
        assert headers[b'sec-websocket-extensions'] == expected_extension
    else:
        assert b'sec-websocket-extensions' not in headers


async def test_h2_websocket_permessage_deflate_round_trip() -> None:
    websocket_app = FastAPI()

    @websocket_app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f'echo:{message}')
        await websocket.close()

    config = Config(port=0)
    async with running_server(websocket_app, config) as server:
        reader, writer, conn, stream_id, handshake = await _h2_open_websocket_stream(
            port=server_port(server),
            path='/ws',
            extensions='permessage-deflate',
        )
        try:
            assert handshake.terminal is None
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

            ws_buffer = handshake.buffer
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
        config=Config(port=0, websocket_max_message_size=4),
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
        config=Config(port=0, websocket_max_message_size=4),
        extensions='permessage-deflate',
    )


@pytest.mark.parametrize('transport', ['h2', 'http1'])
async def test_oversized_control_frame_is_refused_from_its_header_alone(
    transport: str,
) -> None:
    """
    RFC 6455 section 5.5 caps a control frame at 125 bytes and forbids
    fragmenting it. Checking that only after the declared payload arrived
    meant a peer could announce a 64 MiB PING and be handed that much memory
    before the frame was ever ruled out. Sending the header and nothing else
    proves the refusal does not wait for a payload.
    """

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        assert (await receive())['type'] == 'websocket.connect'
        await send({'type': 'websocket.accept'})
        while True:
            message = await receive()
            if message['type'] == 'websocket.disconnect':
                return

    # PING, FIN, masked, 64-bit length of 64 MiB — header only.  A parser
    # that defers admission has nothing else it can consume here.
    await _assert_websocket_close_code(
        transport,
        app,
        client_frames=[_encode_ws_client_header_only(0x9, 64 * 1024 * 1024)],
        expected_code=1002,
    )


@pytest.mark.parametrize('transport', ['h2', 'http1'])
@pytest.mark.parametrize('opcode', [*range(3, 8), *range(11, 16)])
@pytest.mark.parametrize('payload_len', [0, 125, 126, (1 << 63) - 1])
async def test_reserved_opcode_rejected_from_header(
    transport: str,
    opcode: int,
    payload_len: int,
) -> None:
    """Every reserved opcode is terminal from its complete frame header."""

    async def app(scope, receive, send):
        assert (await receive()) == {'type': 'websocket.connect'}
        await send({'type': 'websocket.accept'})
        await receive()

    await _assert_websocket_close_code(
        transport,
        app,
        client_frames=[_encode_ws_client_header_only(opcode, payload_len)],
        expected_code=1002,
    )


async def test_h2_websocket_output_is_bounded_by_the_connection_byte_budget() -> None:
    """
    HTTP/2 WebSocket DATA shares the connection's outbound byte budget with
    ordinary responses.  Against a peer advertising a zero stream window the
    server can never write, so an application that keeps sending must be
    blocked by that budget -- otherwise every frame it hands over is retained
    in the per-stream pending queue and the peer chooses how much memory the
    server spends.

    The writer's command cap does not help: it bounds commands, not bytes, and
    a command's permit is released as soon as it reaches the pending queue.
    """
    # Comfortably larger than H2_OUTBOUND_RESPONSE_BYTE_CAPACITY (2 MiB).
    chunk = b'x' * (256 * 1024)
    sends_completed = 0
    all_sent = asyncio.Event()

    async def app(scope, receive, send):
        nonlocal sends_completed
        assert scope['type'] == 'websocket'
        await receive()
        await send({'type': 'websocket.accept'})
        for _ in range(64):
            await send({'type': 'websocket.send', 'bytes': chunk})
            sends_completed += 1
        all_sent.set()

    async with running_server(app, Config(port=0)) as server:
        _reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server), initial_window_size=0
        )
        _send_h2_websocket_headers(conn, writer, authority=authority, path='/flood')
        await writer.drain()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(all_sent.wait(), timeout=2)
        finally:
            writer.close()
            await writer.wait_closed()

    assert sends_completed > 0, 'the application never got to send anything'
    assert not all_sent.is_set(), (
        f'the application sent all 64 chunks ({sends_completed * len(chunk)} bytes) '
        'into a stream with a zero window: outbound WebSocket bytes are unbounded'
    )
    # The ceiling is the 2 MiB connection budget plus what the outbound ASGI
    # queue holds ahead of it -- that queue is bounded by event count
    # (WEBSOCKET_OUTBOUND_QUEUE_CAPACITY) rather than by bytes, so a large
    # message size raises this figure.  What must never happen is the
    # application running to completion, which is the unbounded case.
    assert sends_completed < 64, 'the send loop must block, not drain'


@pytest.mark.parametrize(
    ('subprotocol', 'expected'),
    [('other', ValueError), ('', ValueError)],
)
async def test_websocket_accept_subprotocol_error_is_raised_from_send(
    subprotocol: str, expected: type[BaseException]
) -> None:
    """
    A malformed `websocket.accept` value must raise out of `await send(...)`.

    Validating it later in the session let `send()` return successfully and the
    request fail afterwards, so an application could neither catch the error
    nor fall back to a subprotocol the client did offer.
    """
    raised: list[BaseException] = []
    accepted = False

    async def app(scope, receive, send):
        nonlocal accepted
        if scope['type'] != 'websocket':
            return
        await receive()
        try:
            await send({'type': 'websocket.accept', 'subprotocol': subprotocol})
        except BaseException as exc:
            raised.append(exc)
            # Recovering is the point: the client offered 'chat'.
            await send({'type': 'websocket.accept', 'subprotocol': 'chat'})
            accepted = True
            await send({'type': 'websocket.close', 'code': 1000})

    async with running_server(app, Config(port=0)) as server:
        status, headers, _body = await asyncio.wait_for(
            _h2_websocket_handshake(
                port=server_port(server), path='/ws', subprotocol='chat'
            ),
            timeout=5,
        )

    assert raised, 'send() returned successfully for a rejected subprotocol'
    assert isinstance(raised[0], expected), f'got {type(raised[0]).__name__}'
    assert accepted, 'the application could not recover and accept'
    assert status == 200
    assert headers.get(b'sec-websocket-protocol') == b'chat'


async def test_h2_websocket_denial_body_is_bounded_by_the_connection_byte_budget() -> (
    None
):
    """
    A denial response is driven straight from the handshake loop, so it never
    passes the ASGI admission that charges an ordinary response body. Against a
    zero stream window its payload would otherwise accumulate in the writer's
    per-stream queue exactly as accepted WebSocket data used to.
    """
    chunk = b'd' * (256 * 1024)
    sends_completed = 0
    all_sent = asyncio.Event()

    async def app(scope, receive, send):
        nonlocal sends_completed
        if scope['type'] != 'websocket':
            return
        await receive()
        await send({
            'type': 'websocket.http.response.start',
            'status': 403,
            'headers': [(b'content-type', b'text/plain')],
        })
        for _ in range(64):
            await send({
                'type': 'websocket.http.response.body',
                'body': chunk,
                'more_body': True,
            })
            sends_completed += 1
        all_sent.set()

    async with running_server(app, Config(port=0)) as server:
        _reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server), initial_window_size=0
        )
        _send_h2_websocket_headers(conn, writer, authority=authority, path='/denied')
        await writer.drain()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(all_sent.wait(), timeout=2)
        finally:
            writer.close()
            await writer.wait_closed()

    assert sends_completed > 0, 'the denial body never started'
    assert not all_sent.is_set(), (
        f'the application queued all 64 denial chunks '
        f'({sends_completed * len(chunk)} bytes) into a zero window'
    )
