import random
import sys
from typing import Callable

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import (
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route
from starlette.testclient import TestClient
from starlette.types import ASGIApp

from starlette_compress import (
    CompressMiddleware,
    add_compress_type,
    remove_compress_type,
)
from starlette_compress._utils import is_start_message_satisfied, parse_accept_encoding

TestClientFactory = Callable[[ASGIApp], TestClient]


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
            # TODO: remove after new zstd support in httpx
            if encoding != 'zstd' or sys.version_info < (3, 14):
                raise
            from compression import zstd

            assert zstd.decompress(response.content) == b'x' * 4000

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
    random.seed(42)
    chunk_count = 70

    def homepage(request: Request) -> StreamingResponse:
        async def generator(count: int):
            for _ in range(count):
                # enough entropy is required for successful chunks
                yield random.getrandbits(8 * chunk_size).to_bytes(chunk_size, 'big')

        streaming = generator(chunk_count)
        return StreamingResponse(streaming, media_type='text/plain')

    app = Starlette(
        routes=[Route('/', endpoint=homepage)],
        middleware=[Middleware(CompressMiddleware)],
    )

    client = test_client_factory(app)

    for encoding in ('gzip', 'br', 'zstd'):
        response = client.get('/', headers={'accept-encoding': encoding})
        assert response.status_code == 200

        try:
            assert len(response.content) == chunk_count * chunk_size
        except AssertionError:
            # TODO: remove after new zstd support in httpx
            if encoding != 'zstd' or sys.version_info < (3, 14):
                raise
            from compression import zstd

            assert len(zstd.decompress(response.content)) == chunk_count * chunk_size

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


def test_is_start_message_satisfied_reads_raw_headers():
    assert is_start_message_satisfied(
        {
            'headers': [
                (b'Content-Encoding', b'identity, identity'),
                (b'Content-Type', b'text/html; charset=utf-8'),
            ],
        },
    )
    assert not is_start_message_satisfied(
        {'headers': [(b'content-encoding', b'gzip'), (b'content-type', b'text/html')]},
    )
    assert not is_start_message_satisfied({
        'headers': [(b'content-type', b'image/png')]
    })


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
        await send({'type': 'http.response.pathsend', 'path': '/tmp/whatever'})

    middleware = CompressMiddleware(app)
    sent: list[dict] = []

    async def fake_send(message: dict) -> None:
        sent.append(message)

    async def fake_receive() -> dict:
        return {'type': 'http.disconnect'}

    scope = {
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
