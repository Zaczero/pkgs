"""`http.response.zerocopysend`.

The application side cannot tell a real `sendfile` from a buffered fallback, and
it must not be able to — both are wire-equivalent by design. So the transfer
tests assert the bytes the client received rather than what the server was asked
to do, and cover both protocol versions wherever the two writers differ. The
rest are validation, ordering and lifetime tests, which assert the response the
application ends up with.

The descriptor is opened in the test body rather than inside the handler: it
only has to be alive across `send()`, and a blocking `open` in an async function
is the sort of thing this extension exists to avoid. The one exception is the
lifetime race below, where opening and closing inside the handler *is* the
property under test.
"""

import asyncio
import os
import sys
from pathlib import Path

import pytest
from h2corn import Config

from tests._support import (
    h2_request,
    h2_request_details,
    http1_request,
    open_h2_connection,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio

# Past the 1 MiB sendfile threshold, so these take the sendfile path where the
# transport supports it. The small fixtures elsewhere stay under it and exercise
# the rolling buffered read.
_LARGE = 3 * 1024 * 1024


def _payload_file(tmp_path: Path, data: bytes) -> Path:
    path = tmp_path / 'payload.bin'
    _ = path.write_bytes(data)
    return path


async def _http1(port: int, path: bytes = b'/') -> tuple[int, bytes]:
    status, _, body, _ = await http1_request(
        port=port,
        request=b'GET ' + path + b' HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
    )
    return status, body


def _serving(app):
    return running_server(app, Config(port=0, access_log=False, lifespan='off'))


async def test_single_segment_is_the_whole_body(tmp_path: Path) -> None:
    data = b'zero-copy payload'
    path = _payload_file(tmp_path, data)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.zerocopysend', 'file': handle})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            assert await _http1(port) == (200, data)
            assert await h2_request(port=port) == (200, data)


async def test_offset_and_count_select_a_range(tmp_path: Path) -> None:
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 3,
            'count': 4,
        })

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            assert await _http1(port) == (200, b'3456')
            assert await h2_request(port=port) == (200, b'3456')


async def test_absent_offset_starts_at_the_current_position(tmp_path: Path) -> None:
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.zerocopysend', 'file': handle})

    with path.open('rb') as handle:
        # The spec's default for a missing `offset`. Resolved at admission and
        # never moved, so both requests below see the same starting point.
        _ = handle.seek(6)
        async with _serving(app) as server:
            port = server_port(server)
            assert await _http1(port) == (200, b'6789')
            assert await h2_request(port=port) == (200, b'6789')


async def test_a_range_is_clamped_to_what_the_file_actually_holds(
    tmp_path: Path,
) -> None:
    """The framing length is the file's, never the application's claim.

    A `count` past the end, or an `offset` past it entirely, describes bytes
    that do not exist. Trusting either would declare a `Content-Length` the
    response can never satisfy, which on HTTP/1 desynchronizes the connection.
    """
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        overshoot = scope['path'] == '/overshoot'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        message = {
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 5 if overshoot else 99,
        }
        if overshoot:
            message['count'] = 1000
        await send(message)

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            status, headers, body, _ = await http1_request(
                port=port,
                request=(
                    b'GET /overshoot HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n'
                ),
            )
            assert (status, body) == (200, b'56789')
            assert headers[b'content-length'] == b'5'
            # Wholly past the end is an empty body, not an error: the range is
            # simply satisfied by nothing.
            assert await _http1(port, b'/past-eof') == (200, b'')


async def test_an_empty_range_is_a_complete_empty_body(tmp_path: Path) -> None:
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'count': 0,
        })

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            status, headers, body, _ = await http1_request(
                port=port,
                request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
            )
            assert (status, body) == (200, b'')
            assert headers[b'content-length'] == b'0'
            assert await h2_request(port=port) == (200, b'')


async def test_repeated_segments_and_body_keep_application_order(
    tmp_path: Path,
) -> None:
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.body',
            'body': b'<',
            'more_body': True,
        })
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 0,
            'count': 3,
            'more_body': True,
        })
        await send({
            'type': 'http.response.body',
            'body': b'|',
            'more_body': True,
        })
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 7,
            'count': 3,
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b'>'})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            # This is the ordering the single-slot body state could not express.
            assert await _http1(port) == (200, b'<012|789>')
            assert await h2_request(port=port) == (200, b'<012|789>')


async def test_a_declared_content_length_covers_segments_and_chunks(
    tmp_path: Path,
) -> None:
    """Framing accounting has to span both kinds of body item.

    A file segment contributes to the declared length exactly as a buffered
    chunk does. If it did not, a mixed body would either under-run its declared
    `Content-Length` -- hanging the client -- or over-run it.
    """
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-length', b'7')],
        })
        await send({
            'type': 'http.response.body',
            'body': b'ab',
            'more_body': True,
        })
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 2,
            'count': 3,
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b'yz'})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            status, headers, body, _ = await http1_request(
                port=port,
                request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
            )
            assert (status, body) == (200, b'ab234yz')
            assert headers[b'content-length'] == b'7'
            assert await h2_request(port=port) == (200, b'ab234yz')


async def test_large_segment_survives_the_sendfile_path(tmp_path: Path) -> None:
    data = os.urandom(_LARGE)
    path = _payload_file(tmp_path, data)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.zerocopysend', 'file': handle})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            status, body = await _http1(port)
            assert status == 200
            assert body == data
            status, body = await h2_request(port=port)
            assert status == 200
            assert body == data


async def test_chunks_queued_behind_a_large_segment_still_drain(
    tmp_path: Path,
) -> None:
    """A file at the head of a non-empty queue must not strand what follows.

    This is the shape the old single-slot body state could not hold at all, and
    the one most likely to stall: the segment is far larger than the HTTP/2
    connection window, so it is written across many flush turns while buffered
    chunks wait behind it. If the queue head were ever popped early, or the
    stream not rescheduled while a file still has bytes, the tail would never
    be written and the client would hang rather than fail.
    """
    data = os.urandom(_LARGE)
    path = _payload_file(tmp_path, data)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'more_body': True,
        })
        # Queued while the segment above is still being written out.
        for marker in (b'-one', b'-two', b'-three'):
            await send({
                'type': 'http.response.body',
                'body': marker,
                'more_body': True,
            })
        await send({'type': 'http.response.body', 'body': b'-end'})

    expected = data + b'-one-two-three-end'
    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            status, body = await _http1(port)
            assert status == 200
            assert body == expected
            status, body = await h2_request(port=port)
            assert status == 200
            assert body == expected


async def test_a_large_range_from_a_nonzero_offset_is_exact(tmp_path: Path) -> None:
    """The `sendfile` path must honour the start offset, not begin at zero.

    Every other range test here is small enough to take the buffered path, so
    an offset ignored by `sendfile` specifically would go unnoticed: the whole
    body would still arrive, just from the wrong place. This range is past the
    sendfile threshold and starts after a distinctive prefix, so beginning at
    zero produces the prefix instead of the payload.
    """
    prefix = b'P' * 4096
    payload = os.urandom(_LARGE)
    path = _payload_file(tmp_path, prefix + payload)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': len(prefix),
            'count': len(payload),
        })

    with path.open('rb') as handle:
        async with _serving(app) as server:
            port = server_port(server)
            status, body = await _http1(port)
            assert status == 200
            assert body == payload
            status, body = await h2_request(port=port)
            assert status == 200
            assert body == payload


async def test_descriptor_closed_immediately_after_send_still_transfers(
    tmp_path: Path,
) -> None:
    """The race the duplicate exists for.

    The specification leaves the descriptor with the application -- "ASGI
    servers are not responsible for closing descriptors" -- so a conforming app
    may close it the moment `send()` returns, while the transfer is still in
    flight. Without the `dup()` at ingress this reads from a closed or, worse, a
    recycled descriptor.
    """
    data = os.urandom(_LARGE)
    path = _payload_file(tmp_path, data)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        # ASYNC230/SIM115 are both correct in general and both wrong here: the
        # point of this test is a descriptor opened and closed *inside* the
        # handler, around the send, which is exactly the lifetime the extension
        # has to survive.
        handle = open(path, 'rb')  # noqa: ASYNC230, SIM115
        await send({'type': 'http.response.zerocopysend', 'file': handle})
        handle.close()
        # Churn descriptors so a stale number would be reused by something else
        # rather than merely being closed.
        spare = [open(os.devnull, 'rb') for _ in range(64)]  # noqa: ASYNC230, SIM115
        for entry in spare:
            entry.close()

    async with _serving(app) as server:
        port = server_port(server)
        status, body = await _http1(port)
        assert status == 200
        assert body == data
        status, body = await h2_request(port=port)
        assert status == 200
        assert body == data


async def test_a_flood_of_queued_segments_hits_backpressure(tmp_path: Path) -> None:
    """Queued segments are bounded by admission credit, not by the peer.

    Each queued segment holds a descriptor for as long as it waits, and the
    HTTP/2 writer's body queue has no length limit of its own — HTTP/1 writes
    each action as it goes and is bounded by the socket instead. A client that
    stops reading would otherwise let an application queue segments until the
    process ran out of descriptors, so admission charges each one a floor
    against the connection's outbound budget and the application's `send()`
    waits.
    """
    # 64 KiB each: the peer's initial connection window admits one, so the
    # rest can only queue -- which is what the credit bounds.
    segment_len = 64 * 1024
    path = _payload_file(tmp_path, b'x' * segment_len)
    admitted = 0
    blocked = asyncio.Event()

    async def app(scope, receive, send):
        nonlocal admitted
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        try:
            for _ in range(512):
                await asyncio.wait_for(
                    send({
                        'type': 'http.response.zerocopysend',
                        'file': handle,
                        'offset': 0,
                        'count': segment_len,
                        'more_body': True,
                    }),
                    timeout=1.0,
                )
                admitted += 1
        except TimeoutError:
            blocked.set()

    with path.open('rb') as handle:
        async with _serving(app) as server:
            _reader, writer, conn, authority = await open_h2_connection(
                port=server_port(server)
            )
            try:
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
                # Deliberately never read: the connection window closes and the
                # writer's queue is the only place segments can go.
                await asyncio.wait_for(blocked.wait(), timeout=30)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionResetError, BrokenPipeError):
                    pass

    # Bounded well below the 512 attempted: admission stopped long before the
    # descriptor table could be a concern. The exact figure is the connection
    # budget divided by the per-segment charge, plus whatever the peer's
    # initial window let through.
    assert blocked.is_set()
    assert 0 < admitted < 128, admitted


async def test_head_describes_the_length_without_a_body(tmp_path: Path) -> None:
    data = b'0123456789'
    path = _payload_file(tmp_path, data)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.zerocopysend', 'file': handle})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            status, headers, body, _ = await http1_request(
                port=server_port(server),
                request=b'HEAD / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
                head_only=True,
            )
            assert status == 200
            assert body == b''
            assert headers[b'content-length'] == str(len(data)).encode()


@pytest.mark.skipif(sys.platform != 'linux', reason='uses Linux procfs synthetic file')
async def test_a_file_whose_size_is_not_its_length_is_rejected() -> None:
    """`st_size` is a snapshot; "until its end" is an I/O condition.

    The synthetic filesystems disagree: `/proc/version` is a regular, seekable
    file reporting `st_size == 0` that reads its contents anyway. Trusting the
    metadata would answer an empty body and call it a complete response, which
    is a wrong answer rather than an error — so it is reported instead.

    Note that an explicit `count` is *also* clamped to `st_size`, so such a
    file cannot currently be served by this extension at all. Serving it would
    need EOF-terminated segments whose length is not known up front, and so a
    framing decision this does not yet make. Read it and use
    `http.response.body`.
    """
    seen: list[BaseException] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        try:
            await send({'type': 'http.response.zerocopysend', 'file': handle})
        except BaseException as exc:
            seen.append(exc)
        await send({'type': 'http.response.body', 'body': b'fallback'})

    with open('/proc/version', 'rb') as handle:  # noqa: ASYNC230
        async with _serving(app) as server:
            status, body = await _http1(server_port(server))

    # Rejected while parsing, so the response state is untouched and the
    # application can answer some other way.
    assert (status, body) == (200, b'fallback')
    assert len(seen) == 1, seen
    assert isinstance(seen[0], ValueError)
    assert 'usable size' in str(seen[0])


async def test_a_directory_is_rejected(tmp_path: Path) -> None:
    seen: list[BaseException] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})

        class _Dir:
            def fileno(self) -> int:
                return descriptor

        try:
            await send({'type': 'http.response.zerocopysend', 'file': _Dir()})
        except BaseException as exc:
            seen.append(exc)
        await send({'type': 'http.response.body', 'body': b'fallback'})

    descriptor = os.open(tmp_path, os.O_RDONLY)
    try:
        async with _serving(app) as server:
            assert await _http1(server_port(server)) == (200, b'fallback')
    finally:
        os.close(descriptor)

    assert len(seen) == 1, seen
    # sendfile needs a regular file, and a directory is the application's
    # mistake rather than a transport condition. It is rejected while parsing,
    # so the response state is untouched and the application still answers
    # normally -- contrast the before-start case below.
    assert isinstance(seen[0], ValueError)
    assert 'regular file' in str(seen[0])


async def test_a_write_only_descriptor_is_rejected_at_admission(
    tmp_path: Path,
) -> None:
    """Readability is checked before the response head is committed.

    An `O_WRONLY` descriptor fails only on the first read, which happens after
    the headers are on the wire — too late for the application to substitute
    anything. Rejecting it while parsing keeps that choice available.
    """
    path = tmp_path / 'out.bin'
    _ = path.write_bytes(b'0123456789')
    seen: list[BaseException] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        try:
            await send({'type': 'http.response.zerocopysend', 'file': handle})
        except BaseException as exc:
            seen.append(exc)
        await send({'type': 'http.response.body', 'body': b'fallback'})

    with path.open('wb') as handle:
        async with _serving(app) as server:
            assert await _http1(server_port(server)) == (200, b'fallback')

    assert len(seen) == 1, seen
    assert isinstance(seen[0], ValueError)
    assert 'open for reading' in str(seen[0])


@pytest.mark.parametrize(
    ('field', 'value'),
    [('offset', -1), ('count', -1), ('offset', 2**64), ('count', 2**64)],
)
async def test_an_out_of_range_offset_or_count_is_rejected(
    tmp_path: Path, field: str, value: int
) -> None:
    """Negative and oversized values are rejected, not wrapped.

    A negative `offset` reaching an unsigned range would become an enormous
    one, which then reads as an empty response rather than as the mistake it is.
    """
    path = _payload_file(tmp_path, b'0123456789')
    seen: list[BaseException] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        try:
            await send({
                'type': 'http.response.zerocopysend',
                'file': handle,
                field: value,
            })
        except BaseException as exc:
            seen.append(exc)
        await send({'type': 'http.response.body', 'body': b'fallback'})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            assert await _http1(server_port(server)) == (200, b'fallback')

    assert len(seen) == 1, seen
    assert isinstance(seen[0], TypeError), seen[0]


async def test_trailers_may_follow_a_file_segment(tmp_path: Path) -> None:
    """A segment is an ordinary body item, so trailers still close the response."""
    path = _payload_file(tmp_path, b'0123456789')

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'trailer', b'x-checksum')],
            'trailers': True,
        })
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 2,
            'count': 4,
        })
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-checksum', b'ok')],
        })

    with path.open('rb') as handle:
        async with _serving(app) as server:
            status, body, trailers = await h2_request_details(
                port=server_port(server),
                extra_headers=[(b'te', b'trailers')],
            )
            assert (status, body) == (200, b'2345')
            assert (b'x-checksum', b'ok') in trailers


async def test_send_after_terminal_file_segment_raises(tmp_path: Path) -> None:
    path = _payload_file(tmp_path, b'file segment')
    errors: list[BaseException] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.zerocopysend', 'file': handle})
        try:
            await send({'type': 'http.response.body', 'body': b'late'})
        except BaseException as exc:
            errors.append(exc)

    with path.open('rb') as handle:
        async with _serving(app) as server:
            assert await _http1(server_port(server)) == (200, b'file segment')

    assert len(errors) == 1
    assert type(errors[0]).__name__ == 'SendAfterCloseError'


async def test_before_response_start_is_an_application_error(tmp_path: Path) -> None:
    """A segment needs a response to belong to, and that is enforced.

    This is the same rule `http.response.early_hint` follows: a message that
    arrives before `http.response.start` is an ordering mistake the application
    can fix, so it fails the request rather than being quietly dropped.
    """
    path = _payload_file(tmp_path, b'x')

    async def app(scope, receive, send):
        await send({'type': 'http.response.zerocopysend', 'file': handle})
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    with path.open('rb') as handle:
        async with _serving(app) as server:
            status, body = await _http1(server_port(server))

    assert status == 500
    # The application's own 200 never reaches the wire.
    assert body == b''


async def test_extension_is_advertised_on_both_versions() -> None:
    advertised: list[bool] = []

    async def app(scope, receive, send):
        advertised.append('http.response.zerocopysend' in scope['extensions'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    async with _serving(app) as server:
        port = server_port(server)
        _ = await _http1(port)
        _ = await h2_request(port=port)

    assert advertised == [True, True]


async def test_the_applications_file_position_is_never_moved(tmp_path: Path) -> None:
    """A duplicate shares its file *description*, and so its position.

    Reads are positional for exactly this reason: an application that keeps
    using the handle after sending it must find it where it left it.
    """
    path = _payload_file(tmp_path, b'0123456789')
    positions: list[int] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.zerocopysend',
            'file': handle,
            'offset': 5,
            'count': 3,
        })

    with path.open('rb') as handle:
        _ = handle.seek(2)
        async with _serving(app) as server:
            port = server_port(server)
            # Observed after the client has the complete body, which is proof
            # the transfer finished -- a sleep would only be a guess about it.
            assert await _http1(port) == (200, b'567')
            positions.append(handle.tell())
            assert await h2_request(port=port) == (200, b'567')
            positions.append(handle.tell())

    assert positions == [2, 2]
