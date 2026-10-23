"""Fragmented header blocks, trailers, and connection-wide HPACK state."""

import asyncio

import h2.errors
import pytest
from h2corn import Config

import hpack
from tests._support import (
    encode_h2_frame,
    open_h2_connection,
    read_raw_h2_frames,
    read_raw_h2_stream,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio
SERVER_MAX_FRAME_SIZE = 64 * 1024


@pytest.mark.parametrize(
    'fragments', [2, 3], ids=['one-continuation', 'several-continuations']
)
async def test_fragmented_trailers_deliver_request_end(fragments: int) -> None:
    """A complete fragmented trailer block ends the request and preserves its body."""
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
            trailers = encoder.encode(
                [(b'x-a', b'1'), (b'x-b', b'2'), (b'x-c', b'3')], huffman=False
            )
            chunks = [
                trailers[
                    index * len(trailers) // fragments : (index + 1)
                    * len(trailers)
                    // fragments
                ]
                for index in range(fragments)
            ]
            writer.write(
                encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + encode_h2_frame(0x00, b'data', flags=0x00, stream_id=1)
                + encode_h2_frame(0x01, chunks[0], flags=0x01, stream_id=1)
                + b''.join(
                    encode_h2_frame(
                        0x09,
                        chunk,
                        flags=0x04 if index == fragments - 1 else 0,
                        stream_id=1,
                    )
                    for index, chunk in enumerate(chunks[1:], start=1)
                )
            )
            await writer.drain()
            frames = await read_raw_h2_stream(reader, 1)
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
                encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + encode_h2_frame(0x00, b'x', flags=0x00, stream_id=1)
                + encode_h2_frame(0x01, trailers[:split_at], flags=0x00, stream_id=1)
                + encode_h2_frame(0x09, trailers[split_at:], flags=0x04, stream_id=1)
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
            writer.write(encode_h2_frame(0x01, follow, flags=0x05, stream_id=3))
            await writer.drain()
            frames = await read_raw_h2_stream(reader, 3)
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
                encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + encode_h2_frame(0x00, b'x', flags=0x00, stream_id=1)
                + encode_h2_frame(0x01, trailers, flags=0x05, stream_id=1)
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
            writer.write(encode_h2_frame(0x01, follow, flags=0x05, stream_id=3))
            await writer.drain()
            frames = await read_raw_h2_stream(reader, 3)
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
                encode_h2_frame(0x01, hold, flags=0x05, stream_id=1)
                + encode_h2_frame(0x01, refused, flags=0x05, stream_id=3)
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
            await read_raw_h2_stream(reader, 1)

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
            writer.write(encode_h2_frame(0x01, follow, flags=0x05, stream_id=5))
            await writer.drain()
            frames = await read_raw_h2_stream(reader, 5)
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
            writer.write(encode_h2_frame(0x01, head, flags=0x05, stream_id=1))
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
            writer.write(encode_h2_frame(0x01, late, flags=0x05, stream_id=1))
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
            writer.write(encode_h2_frame(0x01, follow, flags=0x05, stream_id=3))
            await writer.drain()
            frames = await read_raw_h2_stream(reader, 3)
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
                encode_h2_frame(0x01, block[:8], flags=0x01, stream_id=1)
                + encode_h2_frame(
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
                encode_h2_frame(0x01, block[:8], flags=0x01, stream_id=1)
                + encode_h2_frame(0x09, block[8:], flags=0x04, stream_id=3)
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
                encode_h2_frame(0x01, head, flags=0x04, stream_id=1)
                + encode_h2_frame(0x00, b'x', flags=0x00, stream_id=1)
            )
            await writer.drain()
            await asyncio.wait_for(first_body_seen.wait(), timeout=5)
            # Partial trailers keep the block open; timeout must finalize the app.
            writer.write(encode_h2_frame(0x01, trailers[:1], flags=0x01, stream_id=1))
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
