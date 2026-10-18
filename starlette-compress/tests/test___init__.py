from __future__ import annotations

import asyncio
import copy
import random
import sys
import sysconfig
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import TYPE_CHECKING, Any

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import (
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route
from starlette.testclient import TestClient

from starlette_compress import (
    CompressMiddleware,
    add_compress_type,
    remove_compress_type,
)
from starlette_compress._responder import CompressionResponder
from starlette_compress._utils import classify_start_message, parse_accept_encoding

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    TestClientFactory = Callable[[ASGIApp], TestClient]
else:
    TestClientFactory = Callable[..., TestClient]


def test_brotli_import_preserves_free_threaded_execution():
    if not sysconfig.get_config_var('Py_GIL_DISABLED'):
        pytest.skip('requires free-threaded CPython')

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        raise AssertionError('the app is not called during middleware construction')

    CompressMiddleware(app)

    assert sys._is_gil_enabled() is False  # type: ignore[attr-defined]


def test_compress_responses(test_client_factory: TestClientFactory):
    def homepage(request: Request) -> PlainTextResponse:
        return PlainTextResponse('x' * 4000)

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200

        try:
            assert response.text == 'x' * 4000
        except AssertionError:
            if encoding != 'zstd':
                raise

            assert _decompress_zstd(response.content) == b'x' * 4000

        assert response.headers['Content-Encoding'] == encoding
        assert int(response.headers['Content-Length']) < 4000
        assert response.headers['Vary'] == 'Accept-Encoding'


def test_compress_not_in_accept_encoding(test_client_factory: TestClientFactory):
    def homepage(request: Request) -> PlainTextResponse:
        return PlainTextResponse('x' * 4000)

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)
    response = client.get('/', headers={'accept-encoding': 'identity'})
    assert response.status_code == 200
    assert response.text == 'x' * 4000
    assert 'Content-Encoding' not in response.headers
    assert int(response.headers['Content-Length']) == 4000
    assert response.headers['Vary'] == 'Accept-Encoding'


def test_compress_ignored_for_small_responses(test_client_factory: TestClientFactory):
    def homepage(request: Request) -> PlainTextResponse:
        return PlainTextResponse('OK')

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200
        assert response.text == 'OK'
        assert 'Content-Encoding' not in response.headers
        assert int(response.headers['Content-Length']) == 2
        assert 'Vary' not in response.headers


@pytest.mark.parametrize(
    'chunk_size',
    [
        1,
        128 * 1024,  # 128KB
    ],
)
def test_compress_streaming_response(
    test_client_factory: TestClientFactory, chunk_size: int
):
    chunk_count = 70
    randomizer = random.Random(42)
    chunks = tuple(randomizer.randbytes(chunk_size) for _ in range(chunk_count))
    expected = b''.join(chunks)

    def homepage(request: Request) -> StreamingResponse:
        async def generator():
            for chunk in chunks:
                yield chunk

        return StreamingResponse(generator(), media_type='text/plain')

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200

        response_body = response.content
        if encoding == 'zstd' and response_body != expected:
            response_body = _decompress_zstd(response_body)
        assert response_body == expected

        assert response.headers['Content-Encoding'] == encoding
        assert 'Content-Length' not in response.headers
        assert response.headers['Vary'] == 'Accept-Encoding'


def test_compress_ignored_for_responses_with_encoding_set(
    test_client_factory: TestClientFactory,
):
    def homepage(request: Request) -> StreamingResponse:
        async def generator(content: bytes, count: int):
            for _ in range(count):
                yield content

        streaming = generator(content=b'x' * 400, count=10)
        return StreamingResponse(streaming, headers={'Content-Encoding': 'test'})

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': f'{encoding}, test'})
        assert response.status_code == 200
        assert response.text == 'x' * 4000
        assert response.headers['Content-Encoding'] == 'test'
        assert 'Content-Length' not in response.headers
        assert 'Vary' not in response.headers


def test_compress_not_ignored_for_identity_content_encoding(
    test_client_factory: TestClientFactory,
):
    def homepage(request: Request) -> PlainTextResponse:
        return PlainTextResponse(
            'x' * 4000, headers={'Content-Encoding': 'identity, identity'}
        )

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)
    response = client.get('/', headers={'accept-encoding': 'gzip'})
    assert response.status_code == 200
    assert response.text == 'x' * 4000
    assert response.headers['Content-Encoding'] == 'gzip'
    assert int(response.headers['Content-Length']) < 4000
    assert response.headers['Vary'] == 'Accept-Encoding'


@pytest.mark.parametrize(
    'remove_accept_encoding',
    [True, False],
)
def test_accept_encoding_removed_from_scope(
    test_client_factory: TestClientFactory,
    remove_accept_encoding: bool,
):
    def homepage(request: Request) -> PlainTextResponse:
        accept_encoding = request.headers.get('accept-encoding', '<missing>')
        return PlainTextResponse(headers={'x-request-accept-encoding': accept_encoding})

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[
            Middleware(
                CompressMiddleware, remove_accept_encoding=remove_accept_encoding
            )
        ],
    )

    client = test_client_factory(app)
    response = client.get('/', headers={'accept-encoding': 'gzip'})
    assert response.status_code == 200
    assert response.headers['x-request-accept-encoding'] == (
        '<missing>' if remove_accept_encoding else 'gzip'
    )


def test_compress_ignored_for_missing_accept_encoding(
    test_client_factory: TestClientFactory,
):
    def homepage(request: Request) -> PlainTextResponse:
        return PlainTextResponse('x' * 4000)

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)
    response = client.get('/', headers={'accept-encoding': ''})
    assert response.status_code == 200
    assert response.text == 'x' * 4000
    assert 'Content-Encoding' not in response.headers
    assert int(response.headers['Content-Length']) == 4000
    assert response.headers['Vary'] == 'Accept-Encoding'


def test_compress_ignored_for_missing_content_type(
    test_client_factory: TestClientFactory,
):
    def homepage(request: Request) -> Response:
        return Response('x' * 4000)

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200
        assert response.text == 'x' * 4000
        assert 'Content-Encoding' not in response.headers
        assert int(response.headers['Content-Length']) == 4000
        assert 'Vary' not in response.headers


def test_compress_registered_content_type(test_client_factory: TestClientFactory):
    def homepage(request: Request) -> Response:
        return Response('x' * 4000, media_type='test/test')

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200
        assert 'Content-Encoding' not in response.headers
        assert int(response.headers['Content-Length']) == 4000
        assert 'Vary' not in response.headers

    add_compress_type('test/test')

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200
        assert response.headers['Content-Encoding'] == encoding
        assert int(response.headers['Content-Length']) < 4000
        assert response.headers['Vary'] == 'Accept-Encoding'

    remove_compress_type('test/test')

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200
        assert 'Content-Encoding' not in response.headers
        assert int(response.headers['Content-Length']) == 4000
        assert 'Vary' not in response.headers


def test_parse_accept_encoding():
    assert parse_accept_encoding('') == frozenset()
    assert parse_accept_encoding('gzip, deflate') == {'gzip'}
    assert parse_accept_encoding('br;q=1.0,gzip;q=0.8, *;q=0.1') == {
        'br',
        'gzip',
        'zstd',
    }
    assert parse_accept_encoding('gzip;q=0, br;q=0.5') == {'br'}
    assert parse_accept_encoding('gzip;q=0, *;q=0.1') == {'br', 'zstd'}


def test_classify_start_message_reads_raw_headers():
    assert (
        classify_start_message(
            {
                'status': 200,
                'headers': [
                    (b'Content-Encoding', b'identity, identity'),
                    (b'Content-Type', b'text/html; charset=utf-8'),
                ],
            },
        )
        == 'buffered'
    )
    assert (
        classify_start_message(
            {
                'status': 200,
                'headers': [
                    (b'content-encoding', b'gzip'),
                    (b'content-type', b'text/html'),
                ],
            },
        )
        == 'skip'
    )
    assert (
        classify_start_message({
            'status': 200,
            'headers': [(b'content-type', b'image/png')],
        })
        == 'skip'
    )
    assert (
        classify_start_message(
            {
                'status': 200,
                'headers': [(b'content-type', b'text/event-stream')],
            },
        )
        == 'streaming'
    )


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd', 'identity'])
def test_pathsend_after_compressible_start_flushes_start_first(encoding: str):
    """`http.response.pathsend` must not orphan a buffered start message.

    Some ASGI servers (e.g. h2corn) implement the `http.response.pathsend`
    extension, which Starlette's `FileResponse` uses to skip the body chunk
    stream. The compress middleware buffers `http.response.start` for
    compressible content types while it waits for `http.response.body`. When
    pathsend follows instead, compression doesn't apply — but the buffered
    start must still be forwarded, otherwise the downstream server never
    sees the response start and produces a 500.
    """
    import asyncio

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/xml; charset=utf-8')],
        })
        await send({'type': 'http.response.pathsend', 'path': '/nonexistent'})

    middleware = CompressMiddleware(app)
    sent: list[Message] = []

    async def fake_send(message: Message) -> None:
        sent.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [(b'accept-encoding', encoding.encode())],
        'extensions': {'http.response.pathsend': {}},
    }
    asyncio.run(middleware(scope, fake_receive, fake_send))

    types = [m['type'] for m in sent]
    assert types == ['http.response.start', 'http.response.pathsend'], (
        f'expected start before pathsend, got {types}'
    )
    # No Content-Encoding should be set — pathsend bypasses compression.
    start_headers = dict(sent[0]['headers'])
    assert b'content-encoding' not in start_headers


# ---------------------------------------------------------------------------
# SSE / streaming-type acceptance gate (1.8.0)
# ---------------------------------------------------------------------------


def _make_incremental_decompressor(encoding: str):
    """Return a callable that consumes compressed bytes and returns newly available plaintext."""
    if encoding == 'gzip':
        import zlib

        obj = zlib.decompressobj(31)

        def feed(data: bytes) -> bytes:
            return obj.decompress(data)

        feed.eof = lambda: obj.eof  # type: ignore[attr-defined]
        return feed

    if encoding == 'br':
        import brotlicffi as brotli

        dec = brotli.Decompressor()

        def feed(data: bytes) -> bytes:
            out = dec.process(data)
            # Native brotli may return large events in 32 KiB pieces
            while True:
                more = dec.process(b'')
                if not more:
                    break
                out += more
            return out

        feed.is_finished = lambda: bool(dec.is_finished())  # type: ignore[attr-defined]
        return feed

    if encoding == 'zstd':
        if sys.version_info >= (3, 14):
            from compression import zstd

            # stdlib ZstdDecompressor is already a streaming decompressor
            obj = zstd.ZstdDecompressor()

            def feed(data: bytes) -> bytes:
                return obj.decompress(data)

            feed.eof = lambda: obj.eof  # type: ignore[attr-defined]
            return feed

        import zstandard as zstd_mod

        obj = zstd_mod.ZstdDecompressor().decompressobj()
        wire_parts: list[bytes] = []

        def feed(data: bytes) -> bytes:
            if data:
                wire_parts.append(data)
            return obj.decompress(data)

        def _legacy_frame_complete(expected: bytes) -> bool:
            # zstandard 0.15 decompressobj lacks .eof; prove frame completeness
            # with one-shot decompress of the full wire buffer.
            wire = b''.join(wire_parts)
            plain = zstd_mod.ZstdDecompressor().decompress(
                wire, max_output_size=len(expected)
            )
            return plain == expected

        feed.legacy_frame_complete = _legacy_frame_complete  # type: ignore[attr-defined]
        return feed

    raise ValueError(encoding)


def _decompress_zstd(data: bytes) -> bytes:
    """Decode a complete zstd frame, including frames without a size header."""
    return _make_incremental_decompressor('zstd')(data)


async def run_asgi(
    app: ASGIApp,
    *,
    accept_encoding: str = 'identity',
    pathsend: bool = False,
    zerocopysend: bool = False,
) -> list[Message]:
    """Drive an ASGI app (or middleware) once and capture outbound messages."""
    sent: list[Message] = []

    async def fake_send(message: Message) -> None:
        sent.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    headers = [(b'accept-encoding', accept_encoding.encode())]
    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': headers,
        'extensions': {},
    }
    if pathsend:
        scope['extensions']['http.response.pathsend'] = {}
    if zerocopysend:
        scope['extensions']['http.response.zerocopysend'] = {}

    await app(scope, fake_receive, fake_send)
    return sent


def _sse_headers(
    extra: list[tuple[bytes, bytes]] | None = None,
    content_type: bytes = b'text/event-stream',
) -> list[tuple[bytes, bytes]]:
    headers = [(b'content-type', content_type)]
    if extra:
        headers.extend(extra)
    return headers


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd'])
@pytest.mark.parametrize(
    'message_parts',
    [
        pytest.param([b'data: one\n\n', b'data: two\n\n'], id='separate-events'),
        pytest.param([b'data: one\n\ndata: two\n\n'], id='combined-events'),
        pytest.param([b'data: hel', b'lo\n\n'], id='split-event'),
        pytest.param([], id='empty-stream'),
    ],
)
@pytest.mark.parametrize('terminal_body', [b'', b'data: final\n\n'])
def test_sse_per_encoding_incremental_flush(
    encoding: str, message_parts: list[bytes], terminal_body: bytes
):
    """In-app after each body send: cumulative decoded plaintext equals events so far."""
    large = b'x' * (40 * 1024)
    events = [*message_parts, b'data: ' + large + b'\n\n'] if message_parts else []
    captured: list[Message] = []
    dec = _make_incremental_decompressor(encoding)
    plain = b''

    async def capture(message: Message) -> None:
        nonlocal plain
        captured.append(message)
        if message['type'] == 'http.response.body':
            plain += dec(message.get('body', b''))

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal plain
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        expected = b''
        for event in events:
            await send({
                'type': 'http.response.body',
                'body': event,
                'more_body': True,
            })
            expected += event
            # Assert inside the app, immediately after each awaited body send.
            assert plain == expected, (
                f'{encoding}: after event, plain={plain!r} expected={expected!r}'
            )
        await send({'type': 'http.response.body', 'body': terminal_body})
        assert plain == expected + terminal_body

    middleware = CompressMiddleware(app)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [(b'accept-encoding', encoding.encode())],
        'extensions': {},
    }
    asyncio.run(middleware(scope, fake_receive, capture))

    starts = [m for m in captured if m['type'] == 'http.response.start']
    bodies = [m for m in captured if m['type'] == 'http.response.body']
    assert len(starts) == 1
    terminals = [m for m in bodies if not m.get('more_body', False)]
    assert len(terminals) == 1

    start_headers = {k.lower(): v for k, v in starts[0]['headers']}
    assert start_headers[b'content-encoding'] == encoding.encode()
    assert b'content-length' not in start_headers
    expected_plain = b''.join(events) + terminal_body
    assert plain == expected_plain

    # Stream finalization: gzip/3.14-zstd .eof; brotli is_finished(); legacy zstd oneshot.
    if encoding == 'br':
        assert dec.is_finished()  # type: ignore[attr-defined]
    elif encoding == 'gzip' or (encoding == 'zstd' and sys.version_info >= (3, 14)):
        assert dec.eof()  # type: ignore[attr-defined]
    else:
        assert dec.legacy_frame_complete(expected_plain)  # type: ignore[attr-defined]


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd', 'identity'])
def test_sse_commit_at_start_ordering(encoding: str):
    """Start is committed before the first body (no pathsend in scope)."""
    captured: list[Message] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        # App has not supplied a body yet — start must already be downstream.
        assert [m['type'] for m in captured] == ['http.response.start']

        await send({
            'type': 'http.response.body',
            'body': b'data: one\n\n',
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app)

    async def capture(message: Message) -> None:
        captured.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [(b'accept-encoding', encoding.encode())],
        'extensions': {},
    }
    asyncio.run(middleware(scope, fake_receive, capture))

    starts = [m for m in captured if m['type'] == 'http.response.start']
    assert len(starts) == 1
    headers = {k.lower(): v for k, v in starts[0]['headers']}
    if encoding == 'identity':
        assert b'content-encoding' not in headers
        # Vary still set
        assert b'accept-encoding' in headers.get(b'vary', b'').lower()
    else:
        assert headers[b'content-encoding'] == encoding.encode()
        assert b'content-length' not in headers


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd', 'identity'])
@pytest.mark.parametrize('order', ['pathsend_first', 'body_first'])
def test_pathsend_event_stream(encoding: str, order: str):
    """pathsend-capable SSE: pathsend-first raw; body-first compresses (design)."""

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        if order == 'pathsend_first':
            await send({'type': 'http.response.pathsend', 'path': '/nonexistent'})
        else:
            # body-first on pathsend-capable scope commits compression (encoded)
            # or identity Vary passthrough — never mixes pathsend after body.
            await send({
                'type': 'http.response.body',
                'body': b'data: x\n\n',
                'more_body': False,
            })

    middleware = CompressMiddleware(app)
    sent = asyncio.run(run_asgi(middleware, accept_encoding=encoding, pathsend=True))

    if order == 'pathsend_first':
        types = [m['type'] for m in sent]
        assert types == ['http.response.start', 'http.response.pathsend']
        start_headers = {k.lower(): v for k, v in sent[0]['headers']}
        assert b'content-encoding' not in start_headers
    else:
        # body-first: compresses (encoded) or identity-passthrough with Vary
        starts = [m for m in sent if m['type'] == 'http.response.start']
        assert len(starts) == 1
        start_headers = {k.lower(): v for k, v in starts[0]['headers']}
        if encoding == 'identity':
            assert b'content-encoding' not in start_headers
        else:
            assert start_headers.get(b'content-encoding') == encoding.encode()
            # terminal-first body under pathsend → one-shot with Content-Length
            assert b'content-length' in start_headers


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd'])
def test_sse_oneshot_under_pathsend_capable_scope(encoding: str):
    """Blessing: terminal-first body on pathsend-capable scope → one-shot + CL."""
    payload = b'data: oneshot-pathsend\n\n'

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        await send({
            'type': 'http.response.body',
            'body': payload,
            'more_body': False,
        })

    middleware = CompressMiddleware(app)
    sent = asyncio.run(run_asgi(middleware, accept_encoding=encoding, pathsend=True))
    starts = [m for m in sent if m['type'] == 'http.response.start']
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    assert len(starts) == 1
    assert len(bodies) == 1
    assert bodies[0].get('more_body', False) is False
    headers = {k.lower(): v for k, v in starts[0]['headers']}
    assert headers[b'content-encoding'] == encoding.encode()
    assert b'content-length' in headers
    assert headers[b'content-length'] == str(len(bodies[0]['body'])).encode()

    if encoding == 'gzip':
        import gzip

        assert gzip.decompress(bodies[0]['body']) == payload
    elif encoding == 'br':
        import brotlicffi as brotli

        assert brotli.decompress(bodies[0]['body']) == payload
    elif sys.version_info >= (3, 14):
        from compression import zstd

        assert zstd.decompress(bodies[0]['body']) == payload
    else:
        import zstandard as zstd_mod

        assert zstd_mod.ZstdDecompressor().decompress(bodies[0]['body']) == payload


def test_zerocopysend_stripped_on_encoded_path():
    """Encoded responder: inner app must not see http.response.zerocopysend."""
    seen_extensions: list[dict[str, Any]] = []

    async def app(scope, receive, send):
        seen_extensions.append(dict(scope.get('extensions', {})))
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: zc\n\n',
            'more_body': False,
        })

    middleware = CompressMiddleware(app)
    original_ext = {
        'http.response.zerocopysend': {},
        'http.response.pathsend': {},
    }
    sent: list[Message] = []

    async def fake_send(message: Message) -> None:
        sent.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [(b'accept-encoding', b'gzip')],
        'extensions': dict(original_ext),
    }
    asyncio.run(middleware(scope, fake_receive, fake_send))

    assert seen_extensions, 'app was not invoked'
    assert 'http.response.zerocopysend' not in seen_extensions[0]
    # pathsend remains; only zerocopy is stripped
    assert 'http.response.pathsend' in seen_extensions[0]
    # original scope not mutated
    assert 'http.response.zerocopysend' in scope['extensions']
    # conforming app falls back to bodies and gets compressed
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert headers[b'content-encoding'] == b'gzip'


def test_zerocopysend_visible_on_identity_path():
    """Identity responder keeps zerocopysend advertised to the app."""
    seen_extensions: list[dict[str, Any]] = []

    async def app(scope, receive, send):
        seen_extensions.append(dict(scope.get('extensions', {})))
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: zc\n\n',
            'more_body': False,
        })

    middleware = CompressMiddleware(app)
    sent = asyncio.run(
        run_asgi(middleware, accept_encoding='identity', zerocopysend=True)
    )
    assert seen_extensions
    assert 'http.response.zerocopysend' in seen_extensions[0]
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert b'content-encoding' not in headers


def test_sse_empty_continuations_compressed():
    """Compressed path emits nothing for empty mid-stream bodies (incl. first empty)."""

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        await send({
            'type': 'http.response.body',
            'body': b'',
            'more_body': True,
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: hi\n\n',
            'more_body': True,
        })
        await send({
            'type': 'http.response.body',
            'body': b'',
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app)
    sent = asyncio.run(run_asgi(middleware, accept_encoding='gzip'))
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    # No mid-stream empty body=b'' with more_body=True on compressed path
    for m in bodies:
        if m.get('more_body', False):
            assert m.get('body', b'') != b'', m
    # exactly one terminal
    terminals = [m for m in bodies if not m.get('more_body', False)]
    assert len(terminals) == 1

    dec = _make_incremental_decompressor('gzip')
    plain = b''.join(dec(m.get('body', b'')) for m in bodies)
    assert plain == b'data: hi\n\n'


def test_sse_empty_continuations_identity():
    """Identity preserves empty application messages; final empty still terminates."""

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers(),
        })
        await send({
            'type': 'http.response.body',
            'body': b'',
            'more_body': True,
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: hi\n\n',
            'more_body': True,
        })
        await send({
            'type': 'http.response.body',
            'body': b'',
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app)
    sent = asyncio.run(run_asgi(middleware, accept_encoding='identity'))
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    assert [m.get('body', b'') for m in bodies] == [
        b'',
        b'data: hi\n\n',
        b'',
        b'',
    ]
    assert bodies[-1].get('more_body', False) is False


def test_registry_lifecycle_streaming():
    ctype = 'application/x-test-sse-lifecycle'

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', ctype.encode())],
        })
        await send({'type': 'http.response.body', 'body': b'hello'})

    middleware = CompressMiddleware(app, minimum_size=500)

    def response_is_compressed():
        sent = asyncio.run(run_asgi(middleware, accept_encoding='gzip'))
        headers = dict(sent[0]['headers'])
        wire = b''.join(m.get('body', b'') for m in sent[1:])
        if b'content-encoding' in headers:
            import gzip

            assert headers[b'content-encoding'] == b'gzip'
            assert gzip.decompress(wire) == b'hello'
            return True
        assert wire == b'hello'
        return False

    assert not response_is_compressed()
    try:
        add_compress_type(ctype.upper())
        assert not response_is_compressed()
        add_compress_type(ctype, streaming=True)
        assert response_is_compressed()
        add_compress_type(ctype)
        assert response_is_compressed()
        add_compress_type(ctype, streaming=True)
        assert response_is_compressed()
        remove_compress_type(ctype.upper())
        assert not response_is_compressed()
        add_compress_type(ctype)
        assert not response_is_compressed()
    finally:
        remove_compress_type(ctype)


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd'])
def test_ordinary_stream_below_threshold_roundtrip(encoding):
    """Ordinary streams below minimum_size still produce a complete encoded body."""
    chunks = [b'hello world ' * 10 for _ in range(5)]

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        for c in chunks:
            await send({
                'type': 'http.response.body',
                'body': c,
                'more_body': True,
            })
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app, minimum_size=1000)
    sent = asyncio.run(run_asgi(middleware, accept_encoding=encoding))
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    assert dict(sent[0]['headers'])[b'content-encoding'] == encoding.encode()
    assert sum(not m.get('more_body', False) for m in bodies) == 1

    dec = _make_incremental_decompressor(encoding)
    plain = b''.join(dec(m.get('body', b'')) for m in bodies)
    assert plain == b''.join(chunks)


@pytest.mark.skipif(sys.version_info >= (3, 14), reason='legacy zstd only pre-3.14')
def test_legacy_zstd_large_message_bounded_and_pledged():
    """One ~10MB message yields multiple bounded wire pieces; one-shot decodable."""
    import zstandard as zstd_mod

    size = 10 * 1024 * 1024
    # Incompressible payload so the chunker emits multiple ~128KiB pieces
    random.seed(0)
    payload = random.randbytes(size)

    # Use streaming (more_body) path for chunker bounds — one-shot uses compress()
    async def app_stream(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'content-length', str(size).encode()),
            ],
        })
        await send({
            'type': 'http.response.body',
            'body': payload,
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app_stream)
    sent = asyncio.run(run_asgi(middleware, accept_encoding='zstd'))
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    pieces = [m.get('body', b'') for m in bodies if m.get('body')]
    assert len(pieces) >= 2, f'expected multiple bounded pieces, got {len(pieces)}'
    # pieces should be bounded (~128KiB chunker default)
    assert all(len(p) <= 256 * 1024 for p in pieces), [len(p) for p in pieces]

    wire = b''.join(m.get('body', b'') for m in bodies)
    # content-size pledge: one-shot decompress works
    plain = zstd_mod.ZstdDecompressor().decompress(wire)
    assert plain == payload


@pytest.mark.parametrize('pathsend', [False, True])
def test_zstd_sse_mismatched_content_length_roundtrip(pathsend):
    """C2 regression: SSE with mismatched Content-Length must not raise ZstdError."""
    events = [b'data: one\n\n', b'data: two\n\n', b'data: three\n\n']
    # Deliberately wrong Content-Length (would pledge wrong size to chunker pre-fix)
    wrong_cl = b'999999'

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers([(b'content-length', wrong_cl)]),
        })
        for event in events:
            await send({
                'type': 'http.response.body',
                'body': event,
                'more_body': True,
            })
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app)
    sent = asyncio.run(run_asgi(middleware, accept_encoding='zstd', pathsend=pathsend))
    starts = [m for m in sent if m['type'] == 'http.response.start']
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    assert len(starts) == 1
    headers = {k.lower(): v for k, v in starts[0]['headers']}
    assert headers[b'content-encoding'] == b'zstd'
    assert b'content-length' not in headers

    wire = b''.join(m.get('body', b'') for m in bodies)
    expected = b''.join(events)
    plain = _decompress_zstd(wire)
    assert plain == expected
    assert sum(not m.get('more_body', False) for m in bodies) == 1


@pytest.mark.parametrize(
    'status,extra_headers',
    [
        (100, []),
        (204, []),
        (205, []),
        (304, []),
        (200, [(b'content-range', b'bytes 0-10/100')]),
    ],
)
def test_status_and_range_bypasses(status: int, extra_headers: list):
    """Bypass statuses forward the start message EXACTLY (no Vary / CE mutation)."""
    start_headers_list = _sse_headers(extra_headers)
    start_msg: Message = {
        'type': 'http.response.start',
        'status': status,
        'headers': start_headers_list,
    }
    start_snapshot = copy.deepcopy(start_msg)

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send(start_msg)
        await send({'type': 'http.response.body', 'body': b''})

    middleware = CompressMiddleware(app)
    sent = asyncio.run(run_asgi(middleware, accept_encoding='gzip'))
    assert len([m for m in sent if m['type'] == 'http.response.start']) == 1
    # Complete message equality (same keys + values), not merely status/headers.
    assert sent[0] == start_snapshot
    start_headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert b'content-encoding' not in start_headers
    # No stray Vary mutation on bypass
    assert b'vary' not in start_headers
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    assert len(bodies) == 1 and bodies[0].get('body', b'') == b''


def test_sse_headers_vary_merge_and_precompressed():
    async def app_vary(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers([(b'vary', b'Origin')]),
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: x\n\n',
            'more_body': False,
        })

    middleware = CompressMiddleware(app_vary)
    sent = asyncio.run(run_asgi(middleware, accept_encoding='gzip'))
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert headers[b'content-encoding'] == b'gzip'
    assert b'content-length' not in headers
    vary = headers[b'vary'].decode()
    assert 'Origin' in vary
    assert 'Accept-Encoding' in vary

    async def app_pre(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers([(b'content-encoding', b'gzip')]),
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: x\n\n',
            'more_body': False,
        })

    sent = asyncio.run(run_asgi(CompressMiddleware(app_pre), accept_encoding='gzip'))
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert headers[b'content-encoding'] == b'gzip'
    # original body not re-compressed (same length content)
    bodies = [m for m in sent if m['type'] == 'http.response.body']
    assert bodies[0]['body'] == b'data: x\n\n'


def test_sse_content_encoding_identity_still_compressible():
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers([(b'content-encoding', b'identity')]),
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: x\n\n',
            'more_body': False,
        })

    sent = asyncio.run(run_asgi(CompressMiddleware(app), accept_encoding='gzip'))
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert headers[b'content-encoding'] == b'gzip'


def test_identity_sse_preserves_content_length():
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': _sse_headers([(b'content-length', b'11')]),
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: x\n\n',
            'more_body': False,
        })

    sent = asyncio.run(run_asgi(CompressMiddleware(app), accept_encoding='identity'))
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert b'content-encoding' not in headers
    assert headers[b'content-length'] == b'11'


def test_trailers_after_body_no_duplicate_start():
    """Protocol-valid trailers: extension advertised + trailers:True on start."""

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'trailer', b'x-checksum'),
            ],
            'trailers': True,
        })
        await send({
            'type': 'http.response.body',
            'body': b'x' * 1000,
            'more_body': False,
        })
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-checksum', b'abc')],
        })

    middleware = CompressMiddleware(app)
    sent: list[Message] = []

    async def fake_send(message: Message) -> None:
        sent.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [(b'accept-encoding', b'gzip')],
        'extensions': {'http.response.trailers': {}},
    }
    asyncio.run(middleware(scope, fake_receive, fake_send))
    starts = [m for m in sent if m['type'] == 'http.response.start']
    assert len(starts) == 1
    assert starts[0].get('trailers') is True
    assert any(m['type'] == 'http.response.trailers' for m in sent)
    # Compressed oneshot body still present
    assert any(
        m['type'] == 'http.response.body' and m.get('body', b'') != b'x' * 1000
        for m in sent
    )


@pytest.mark.parametrize('accept_encoding', ['identity', 'gzip'])
def test_repeated_start_raises_runtime_error_and_preserves_response(
    accept_encoding,
):
    errors: list[RuntimeError] = []
    original_start: Message = {
        'type': 'http.response.start',
        'status': 201,
        'headers': [(b'content-type', b'text/plain'), (b'x-original', b'retained')],
    }

    async def app(scope, receive, send):
        await send(original_start)
        with pytest.raises(
            RuntimeError,
            match=r'http\.response\.start has already been received',
        ) as caught:
            await send({
                'type': 'http.response.start',
                'status': 500,
                'headers': [(b'content-type', b'text/plain')],
            })
        errors.append(caught.value)
        await send({'type': 'http.response.body', 'body': b'original'})

    middleware = CompressMiddleware(
        app, zstd=False, brotli=False, gzip=accept_encoding == 'gzip'
    )
    sent = asyncio.run(run_asgi(middleware, accept_encoding=accept_encoding))

    starts = [message for message in sent if message['type'] == 'http.response.start']
    bodies = [message for message in sent if message['type'] == 'http.response.body']
    assert len(errors) == 1
    assert len(starts) == 1
    assert starts[0]['status'] == 201
    assert starts[0]['headers'] == original_start['headers']
    assert bodies == [{'type': 'http.response.body', 'body': b'original'}]


@pytest.mark.parametrize(
    ('setting', 'value', 'error_type', 'message'),
    [
        pytest.param(
            'brotli_quality',
            -1,
            ValueError,
            'brotli_quality must be between 0 and 11, got -1',
            id='brotli-too-low',
        ),
        pytest.param(
            'brotli_quality',
            12,
            ValueError,
            'brotli_quality must be between 0 and 11, got 12',
            id='brotli-too-high',
        ),
        pytest.param(
            'brotli_quality',
            1.5,
            TypeError,
            'brotli_quality must be an integer, got float',
            id='brotli-wrong-type',
        ),
        pytest.param(
            'gzip_level',
            -1,
            ValueError,
            'gzip_level must be between 0 and 9, got -1',
            id='gzip-too-low',
        ),
        pytest.param(
            'gzip_level',
            10,
            ValueError,
            'gzip_level must be between 0 and 9, got 10',
            id='gzip-too-high',
        ),
        pytest.param(
            'gzip_level',
            '4',
            TypeError,
            'gzip_level must be an integer, got str',
            id='gzip-wrong-type',
        ),
    ],
)
def test_enabled_codec_levels_are_validated_at_setup(
    setting, value, error_type, message
):
    async def app(scope, receive, send):
        return None

    options = {'zstd': False, 'brotli': False, 'gzip': False, setting: value}
    options['brotli' if setting == 'brotli_quality' else 'gzip'] = True

    with pytest.raises(error_type) as caught:
        CompressMiddleware(app, **options)

    assert str(caught.value) == message


@pytest.mark.parametrize(
    ('setting', 'value', 'encoding'),
    [
        pytest.param('brotli_quality', 0, 'br', id='brotli-fastest'),
        pytest.param('brotli_quality', 11, 'br', id='brotli-best'),
        pytest.param('gzip_level', 0, 'gzip', id='gzip-fastest'),
        pytest.param('gzip_level', 9, 'gzip', id='gzip-best'),
    ],
)
def test_enabled_codec_level_boundaries_reach_the_provider(setting, value, encoding):
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'x' * 1000})

    options = {'zstd': False, 'brotli': False, 'gzip': False, setting: value}
    options['brotli' if setting == 'brotli_quality' else 'gzip'] = True
    sent = asyncio.run(
        run_asgi(
            CompressMiddleware(app, minimum_size=1, **options),
            accept_encoding=encoding,
        )
    )

    headers = {name.lower(): value for name, value in sent[0]['headers']}
    assert headers[b'content-encoding'] == encoding.encode()
    assert any(message['type'] == 'http.response.body' for message in sent)


def test_disabled_codec_levels_are_not_validated():
    async def app(scope, receive, send):
        return None

    middleware = CompressMiddleware(
        app,
        zstd=False,
        brotli=False,
        brotli_quality=12,
        gzip=False,
        gzip_level=-1,
    )

    assert middleware.app is app


def test_case_insensitive_event_stream_content_type():
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'Text/Event-Stream')],
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: x\n\n',
            'more_body': False,
        })

    sent = asyncio.run(run_asgi(CompressMiddleware(app), accept_encoding='gzip'))
    headers = {k.lower(): v for k, v in sent[0]['headers']}
    assert headers[b'content-encoding'] == b'gzip'
    # commit-at-start: Content-Length removed
    assert b'content-length' not in headers


@pytest.mark.parametrize('encoding', ['gzip', 'br', 'zstd'])
def test_sse_testclient_roundtrip(
    test_client_factory: TestClientFactory, encoding: str
):
    async def gen():
        yield b'data: one\n\n'
        yield b'data: two\n\n'

    def homepage(request: Request) -> StreamingResponse:
        return StreamingResponse(gen(), media_type='text/event-stream')

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )
    client = test_client_factory(app)
    response = client.get('/', headers={'accept-encoding': encoding})
    assert response.status_code == 200
    assert response.headers['Content-Encoding'] == encoding
    assert 'Content-Length' not in response.headers

    # httpx/TestClient may return raw zstd when its decoder is unavailable.
    try:
        assert response.content == b'data: one\n\ndata: two\n\n'
    except AssertionError:
        if encoding != 'zstd':
            raise

        assert _decompress_zstd(response.content) == b'data: one\n\ndata: two\n\n'


def test_mixed_case_managed_headers_oneshot():
    """Mixed-case CL/CE/Vary must normalize so mutations hit; no stale duplicates."""
    body = b'x' * 1000

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'Content-Type', b'text/plain'),
                (b'X-MiXeD', b'keep-me'),
                (b'Content-Length', str(len(body)).encode()),
                (b'Content-Encoding', b'identity'),
                (b'Vary', b'Origin'),
            ],
        })
        await send({'type': 'http.response.body', 'body': body})

    sent = asyncio.run(run_asgi(CompressMiddleware(app), accept_encoding='gzip'))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    raw = start['headers']
    # Exactly one of each managed name (case-insensitive), all lowercase wire names.
    names_lower = [n.lower() for n, _ in raw]
    assert names_lower.count(b'content-length') == 1
    assert names_lower.count(b'content-encoding') == 1
    assert names_lower.count(b'vary') == 1
    headers = {n.lower(): v for n, v in raw}
    assert headers[b'content-encoding'] == b'gzip'
    assert headers[b'content-length'] != str(len(body)).encode()
    assert int(headers[b'content-length']) < len(body)
    # Exact wire-order Vary (kills value-reversal mutant).
    assert headers[b'vary'] == b'Origin, Accept-Encoding'
    # Unmanaged mixed-case names keep exact spelling (kills lowercase-everything).
    assert (b'Content-Type', b'text/plain') in raw
    assert (b'X-MiXeD', b'keep-me') in raw
    # No mixed-case remnants for managed headers
    for name, _ in raw:
        if name.lower() in (b'content-length', b'content-encoding', b'vary'):
            assert name == name.lower()


def test_mixed_case_streaming_content_length_removal():
    """Streaming commit must delete mixed-case Content-Length (not leave a stale field)."""

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'Content-Type', b'text/event-stream'),
                (b'X-MiXeD', b'keep-me'),
                (b'Content-Length', b'9999'),
            ],
        })
        await send({
            'type': 'http.response.body',
            'body': b'data: one\n\n',
            'more_body': True,
        })
        await send({'type': 'http.response.body', 'body': b''})

    sent = asyncio.run(run_asgi(CompressMiddleware(app), accept_encoding='gzip'))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    names_lower = [n.lower() for n, _ in start['headers']]
    assert b'content-length' not in names_lower
    headers = {n.lower(): v for n, v in start['headers']}
    assert headers[b'content-encoding'] == b'gzip'
    assert (b'Content-Type', b'text/event-stream') in start['headers']
    assert (b'X-MiXeD', b'keep-me') in start['headers']


def test_mixed_case_repeated_vary_coalesced():
    """Repeated differently-cased Vary fields coalesce in wire order with Accept-Encoding."""

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'Vary', b'Origin'),
                (b'vArY', b'Cookie'),
            ],
        })
        await send({'type': 'http.response.body', 'body': b'x' * 1000})

    sent = asyncio.run(run_asgi(CompressMiddleware(app), accept_encoding='gzip'))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    vary_fields = [v for n, v in start['headers'] if n.lower() == b'vary']
    assert len(vary_fields) == 1
    # Exact wire order: Origin, Cookie, Accept-Encoding (kills value-reversal).
    assert vary_fields[0] == b'Origin, Cookie, Accept-Encoding'
    # Single field only — no leftover mixed-case duplicates
    assert sum(1 for n, _ in start['headers'] if n.lower() == b'vary') == 1


def test_mixed_case_skip_small_pathsend_byte_identical():
    """Skip / small / pathsend-first with mixed-case managed headers forward byte-for-byte."""
    mixed = [
        (b'Content-Type', b'text/plain'),
        (b'Content-Length', b'2'),
        (b'X-MiXeD', b'keep'),
        (b'Vary', b'Origin'),
    ]

    async def small_app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': list(mixed),
        })
        await send({'type': 'http.response.body', 'body': b'OK'})

    sent = asyncio.run(run_asgi(CompressMiddleware(small_app), accept_encoding='gzip'))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    assert start['headers'] == mixed

    skip_headers = [
        (b'Content-Type', b'image/png'),
        (b'Content-Length', b'4'),
        (b'X-MiXeD', b'keep'),
        (b'Vary', b'Origin'),
    ]

    async def skip_app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': list(skip_headers),
        })
        await send({'type': 'http.response.body', 'body': b'data'})

    sent = asyncio.run(run_asgi(CompressMiddleware(skip_app), accept_encoding='gzip'))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    assert start['headers'] == skip_headers

    pathsend_headers = [
        (b'Content-Type', b'text/plain'),
        (b'Content-Length', b'9999'),
        (b'X-MiXeD', b'keep'),
        (b'Vary', b'Origin'),
    ]

    async def pathsend_app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': list(pathsend_headers),
        })
        await send({'type': 'http.response.pathsend', 'path': '/nonexistent'})

    sent = asyncio.run(
        run_asgi(
            CompressMiddleware(pathsend_app),
            accept_encoding='gzip',
            pathsend=True,
        )
    )
    start = next(m for m in sent if m['type'] == 'http.response.start')
    assert start['headers'] == pathsend_headers


def test_failing_oneshot_pathsend_fallback_preserves_start():
    """Raising oneshot must not mutate start; app can fall back to pathsend cleanly."""
    body = b'x' * 1000
    start_headers = [
        (b'content-type', b'text/plain'),
        (b'content-length', str(len(body)).encode()),
        (b'x-app', b'v1'),
    ]
    start_message: Message = {
        'type': 'http.response.start',
        'status': 200,
        'headers': list(start_headers),
    }
    start_snapshot = copy.deepcopy(start_message)
    forwarded: list[Message] = []

    def oneshot(data: bytes) -> bytes:
        raise RuntimeError('oneshot boom')

    def create_encoder(content_length: int):
        raise AssertionError('create_encoder must not be called on terminal body')

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send(start_message)
        try:
            await send({'type': 'http.response.body', 'body': body})
        except RuntimeError:
            # Fall back to pathsend with the same start dict the app already sent.
            await send({'type': 'http.response.pathsend', 'path': '/fallback'})

    responder = CompressionResponder(app, 500, 'fake', oneshot, create_encoder)

    async def fake_send(message: Message) -> None:
        forwarded.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [],
        'extensions': {'http.response.pathsend': {}},
    }
    asyncio.run(responder(scope, fake_receive, fake_send))

    assert [m['type'] for m in forwarded] == [
        'http.response.start',
        'http.response.pathsend',
    ]
    start = forwarded[0]
    names = {n.lower() for n, _ in start['headers']}
    assert b'content-encoding' not in names
    # No Accept-Encoding Vary injection from a failed commit.
    vary_vals = [v for n, v in start['headers'] if n.lower() == b'vary']
    assert not any(b'Accept-Encoding' in v for v in vary_vals)
    assert start['headers'] == start_headers
    assert start_message == start_snapshot
    assert start_message['headers'] == start_headers


def test_failing_encoder_factory_pathsend_fallback_preserves_start():
    """Raising create_encoder must not mutate start; pathsend fallback stays raw."""
    chunk = b'x' * 1000
    start_headers = [
        (b'content-type', b'text/plain'),
        (b'content-length', b'999999'),
        (b'Vary', b'Origin'),
        (b'X-MiXeD', b'keep'),
    ]
    start_message: Message = {
        'type': 'http.response.start',
        'status': 200,
        'headers': list(start_headers),
    }
    start_snapshot = copy.deepcopy(start_message)
    forwarded: list[Message] = []

    def oneshot(data: bytes) -> bytes:
        raise AssertionError('oneshot must not be called on more_body stream-begin')

    def create_encoder(content_length: int):
        raise RuntimeError(f'factory boom cl={content_length}')

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send(start_message)
        try:
            await send({
                'type': 'http.response.body',
                'body': chunk,
                'more_body': True,
            })
        except RuntimeError:
            await send({'type': 'http.response.pathsend', 'path': '/fallback'})

    responder = CompressionResponder(app, 500, 'fake', oneshot, create_encoder)

    async def fake_send(message: Message) -> None:
        forwarded.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [],
        'extensions': {'http.response.pathsend': {}},
    }
    asyncio.run(responder(scope, fake_receive, fake_send))

    assert [m['type'] for m in forwarded] == [
        'http.response.start',
        'http.response.pathsend',
    ]
    start = forwarded[0]
    names = {n.lower() for n, _ in start['headers']}
    assert b'content-encoding' not in names
    # Original mixed-case Vary left alone; no Accept-Encoding injection.
    assert start['headers'] == start_headers
    assert start_message == start_snapshot


@pytest.mark.parametrize('headers_shape', ['generator', 'tuple'])
def test_scope_headers_materialized_to_list(headers_shape: str):
    """Raw ASGI: generator/tuple scope headers become a list with every original field."""
    original = [
        (b'host', b'example.com'),
        (b'accept-encoding', b'gzip'),
        (b'x-custom', b'value'),
        (b'cookie', b'a=1'),
    ]
    seen_inside: dict[str, Any] = {}

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        headers = scope['headers']
        seen_inside['type'] = type(headers)
        seen_inside['headers'] = list(headers)
        body = b'x' * 1000
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'content-length', str(len(body)).encode()),
            ],
        })
        await send({'type': 'http.response.body', 'body': body})

    if headers_shape == 'generator':
        request_headers: Any = (pair for pair in original)
    else:
        request_headers = tuple(original)

    sent: list[Message] = []

    async def fake_send(message: Message) -> None:
        sent.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': request_headers,
        'extensions': {},
    }
    asyncio.run(CompressMiddleware(app)(scope, fake_receive, fake_send))

    assert seen_inside['type'] is list
    assert seen_inside['headers'] == original
    start = next(m for m in sent if m['type'] == 'http.response.start')
    headers = {n.lower(): v for n, v in start['headers']}
    assert headers[b'content-encoding'] == b'gzip'


def test_dispatch_multi_ae_second_field_and_latin1_and_remove_all():
    """Compact AE dispatch matrix: multi-field, latin-1 non-UTF8, remove-all."""
    # (a) Multiple Accept-Encoding fields; only the SECOND contains a supported coding.
    seen_ae_a: list[bytes] = []

    async def app_a(scope: Scope, receive: Receive, send: Send) -> None:
        for name, value in scope['headers']:
            if name == b'accept-encoding':
                seen_ae_a.append(value)
        body = b'x' * 1000
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'content-length', str(len(body)).encode()),
            ],
        })
        await send({'type': 'http.response.body', 'body': body})

    sent: list[Message] = []

    async def fake_send(message: Message) -> None:
        sent.append(message)

    async def fake_receive() -> Message:
        return {'type': 'http.disconnect'}

    scope_a: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [
            (b'accept-encoding', b'deflate, identity'),
            (b'accept-encoding', b'gzip'),
            (b'host', b'example.com'),
        ],
        'extensions': {},
    }
    asyncio.run(CompressMiddleware(app_a)(scope_a, fake_receive, fake_send))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    assert {n.lower(): v for n, v in start['headers']}[b'content-encoding'] == b'gzip'

    # (b) AE value with a latin-1 non-UTF8 byte alongside 'gzip' still negotiates.
    sent.clear()
    latin1_ae = b'gzip, x-\xff-token'

    async def app_b(scope: Scope, receive: Receive, send: Send) -> None:
        body = b'x' * 1000
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'content-length', str(len(body)).encode()),
            ],
        })
        await send({'type': 'http.response.body', 'body': body})

    scope_b: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [(b'accept-encoding', latin1_ae)],
        'extensions': {},
    }
    asyncio.run(CompressMiddleware(app_b)(scope_b, fake_receive, fake_send))
    start = next(m for m in sent if m['type'] == 'http.response.start')
    assert {n.lower(): v for n, v in start['headers']}[b'content-encoding'] == b'gzip'

    # (c) remove_accept_encoding=True with multiple AE fields → app sees ZERO.
    sent.clear()
    seen_ae_c: list[bytes] = []

    async def app_c(scope: Scope, receive: Receive, send: Send) -> None:
        for name, value in scope['headers']:
            if name == b'accept-encoding':
                seen_ae_c.append(value)
        body = b'x' * 1000
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'content-length', str(len(body)).encode()),
            ],
        })
        await send({'type': 'http.response.body', 'body': body})

    scope_c: Scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/',
        'headers': [
            (b'accept-encoding', b'identity'),
            (b'accept-encoding', b'gzip'),
            (b'host', b'example.com'),
        ],
        'extensions': {},
    }
    asyncio.run(
        CompressMiddleware(app_c, remove_accept_encoding=True)(
            scope_c, fake_receive, fake_send
        )
    )
    assert seen_ae_c == []
    start = next(m for m in sent if m['type'] == 'http.response.start')
    assert {n.lower(): v for n, v in start['headers']}[b'content-encoding'] == b'gzip'


@pytest.mark.parametrize(
    'accept_encoding,expected_encoding',
    [
        ('zstd;q=0.1, gzip;q=1', 'zstd'),
        ('*;q=1, zstd;q=0, br;q=0', 'gzip'),
        ('zstd;q=invalid, br;q=0.2, gzip;q=1', 'br'),
        ('zstd;q=0, br;q=0, gzip;q=0, identity;q=0', 'identity'),
    ],
)
def test_negotiation_uses_positive_quality_then_server_priority(
    accept_encoding, expected_encoding
):
    payload = b'ordinary response\n' * 100

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    sent = asyncio.run(
        run_asgi(CompressMiddleware(app), accept_encoding=accept_encoding)
    )
    headers = dict(sent[0]['headers'])
    assert headers.get(b'content-encoding', b'identity') == expected_encoding.encode()
    wire = b''.join(m.get('body', b'') for m in sent[1:])
    if expected_encoding == 'identity':
        assert wire == payload
    else:
        assert _make_incremental_decompressor(expected_encoding)(wire) == payload
    assert sent[-1].get('more_body', False) is False


def test_shared_middleware_zstd_oneshot_across_loop_threads():
    ready = Barrier(2, timeout=10)
    payloads = {
        '/one': bytes(range(256)) * 1024,
        '/two': b'other response\n' * 20_000,
    }

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        ready.wait()
        await send({
            'type': 'http.response.body',
            'body': payloads[scope['path']],
        })

    middleware = CompressMiddleware(app)

    def request(path):
        async def run():
            sent = []

            async def receive():
                return {'type': 'http.disconnect'}

            async def send(message):
                sent.append(message)

            for _ in range(8):
                sent.clear()
                await middleware(
                    {
                        'type': 'http',
                        'method': 'GET',
                        'path': path,
                        'headers': [(b'accept-encoding', b'zstd')],
                    },
                    receive,
                    send,
                )
                assert dict(sent[0]['headers'])[b'content-encoding'] == b'zstd'
                assert _decompress_zstd(sent[1]['body']) == payloads[path]
                assert sent[1].get('more_body', False) is False

        asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(request, path) for path in payloads]
        for future in futures:
            future.result(timeout=20)
