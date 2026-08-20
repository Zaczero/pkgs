"""`http.response.early_hint` (RFC 8297).

Advertised on HTTP/2 only: the RFC names interoperability and security risks
with HTTP/1 clients that mishandle an interim response, and a pipelined peer
that ignores one desynchronizes.
"""

import asyncio
import contextlib
import sys
from typing import Literal

import h2.connection
import h2.events
import pytest
from h2corn import Config

from tests._support import (
    http1_request,
    open_h2_connection,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio

LINKS = [b'</style.css>; rel=preload; as=style', b'</app.js>; rel=preload']

#: An interim event carries its whole header block; a final one carries only
#: the status, which is all these tests assert about it.
InterimEvent = tuple[Literal['interim'], list[tuple[bytes, bytes]]]
FinalEvent = tuple[Literal['final'], bytes]
ExchangeEvent = InterimEvent | FinalEvent


def _headers(event: ExchangeEvent) -> list[tuple[bytes, bytes]]:
    """The header block of an interim event, failing loudly on a final one."""
    kind, payload = event
    assert kind == 'interim', f'expected an interim event, got {kind}'
    assert not isinstance(payload, bytes)
    return payload


async def _h2_exchange(app, path: bytes = b'/'):
    """Drive one H2 request, returning every interim and final event in order."""
    async with running_server(app, Config(port=0, lifespan='off')) as server:
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
                (b':path', path),
            ],
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()

        events: list[ExchangeEvent] = []
        body = bytearray()
        ended = False
        try:
            while not ended:
                data = await asyncio.wait_for(reader.read(65535), timeout=5)
                if not data:
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.InformationalResponseReceived):
                        events.append(('interim', list(event.headers)))
                    elif isinstance(event, h2.events.ResponseReceived):
                        events.append(('final', dict(event.headers)[b':status']))
                    elif isinstance(event, h2.events.DataReceived):
                        body += event.data
                    elif isinstance(event, h2.events.StreamEnded):
                        ended = True
                writer.write(conn.data_to_send())
                await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
    return events, bytes(body)


async def test_early_hint_reaches_the_wire_before_the_final_response() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.early_hint', 'links': LINKS})
        await send({'type': 'http.response.body', 'body': b'done'})

    events, body = await _h2_exchange(app)

    assert [kind for kind, _ in events] == ['interim', 'final']
    interim = _headers(events[0])
    # Order and duplicates are preserved: a Link list is ordered by the app.
    assert interim == [
        (b':status', b'103'),
        (b'link', LINKS[0]),
        (b'link', LINKS[1]),
    ]
    assert events[1][1] == b'200'
    assert body == b'done'


async def test_multiple_hints_each_produce_their_own_block() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.early_hint', 'links': [LINKS[0]]})
        await send({'type': 'http.response.early_hint', 'links': [LINKS[1]]})
        await send({'type': 'http.response.body', 'body': b''})

    events, _ = await _h2_exchange(app)

    assert [kind for kind, _ in events] == ['interim', 'interim', 'final']
    assert _headers(events[0])[1] == (b'link', LINKS[0])
    assert _headers(events[1])[1] == (b'link', LINKS[1])


async def test_empty_links_and_a_generator_are_both_accepted() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.early_hint', 'links': []})
        await send({
            'type': 'http.response.early_hint',
            'links': (link for link in LINKS),
        })
        await send({'type': 'http.response.body', 'body': b''})

    events, _ = await _h2_exchange(app)

    assert [kind for kind, _ in events] == ['interim', 'interim', 'final']
    # An empty iterable is valid and produces a bare 103.
    assert _headers(events[0]) == [(b':status', b'103')]
    assert [value for name, value in _headers(events[1]) if name == b'link'] == LINKS


async def test_a_hint_after_the_body_has_started_is_ignored() -> None:
    started = asyncio.Event()

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'first', 'more_body': True})
        started.set()
        # The final HEADERS have already reached the peer; RFC 9113 8.1 forbids
        # another non-END_STREAM block, so this must be dropped rather than
        # corrupt the stream.
        await send({'type': 'http.response.early_hint', 'links': LINKS})
        await send({'type': 'http.response.body', 'body': b'second'})

    events, body = await _h2_exchange(app)

    assert [kind for kind, _ in events] == ['final']
    assert body == b'firstsecond'


async def test_the_extension_is_not_offered_on_http1() -> None:
    seen: list[list[str]] = []

    async def app(scope, receive, send):
        seen.append(sorted(scope['extensions']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        await http1_request(
            port=server_port(server),
            request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
        )

    expected = ['http.response.pathsend']
    if sys.platform != 'win32':
        expected.append('http.response.zerocopysend')
    assert seen == [expected]


async def test_a_hint_before_start_is_an_application_error() -> None:
    """ASGI places the hint after `http.response.start`, and that is enforced.

    The window is asymmetric on purpose: before start nothing is committed, so
    an error points at a fixable ordering mistake, whereas a hint after the
    final headers is ignored because the wire cannot carry another block. The
    rule costs nothing -- `http.response.start` writes no bytes, so a hint sent
    immediately after it still reaches the peer ahead of the response.
    """

    async def app(scope, receive, send):
        await send({'type': 'http.response.early_hint', 'links': LINKS})
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    events, _ = await _h2_exchange(app)

    assert [kind for kind, _ in events] == ['final']
    assert events[0][1] == b'500'


async def test_response_start_with_103_is_still_rejected() -> None:
    """The `exactly one final status` invariant is untouched by this feature."""

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 103, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    events, _ = await _h2_exchange(app)

    assert [kind for kind, _ in events] == ['final']
    assert events[0][1] == b'500'


async def test_a_hint_after_the_final_body_does_not_destroy_the_response() -> None:
    """A late hint is dropped, never fatal.

    Erroring here truncated an otherwise-complete response: HTTP/1 lost the
    terminating chunk and the trailers, HTTP/2 reset a stream whose 200 the
    client already had.
    """

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'trailer', b'x-t')],
            'trailers': True,
        })
        await send({'type': 'http.response.body', 'body': b'payload'})
        # The body is complete and the trailers are still pending.
        await send({'type': 'http.response.early_hint', 'links': LINKS})
        await send({'type': 'http.response.trailers', 'headers': [(b'x-t', b'1')]})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            b'GET / HTTP/1.1\r\nHost: x\r\nTE: trailers\r\nConnection: close\r\n\r\n'
        )
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=5)
        writer.close()
        await writer.wait_closed()

    body = raw.split(b'\r\n\r\n', 1)[1]
    # Terminating chunk and trailer section both present.
    assert body == b'7\r\npayload\r\n0\r\nx-t: 1\r\n\r\n'
