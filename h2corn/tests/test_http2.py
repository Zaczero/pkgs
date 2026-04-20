import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import h2.events
import pytest
from fastapi import FastAPI
from h2corn import Config, Server
from starlette.requests import Request
from starlette.responses import FileResponse, PlainTextResponse

from tests._support import (
    find_free_port,
    h2_request,
    h2_request_details,
    open_h2_connection,
    read_h2_response,
    read_http_request_body,
    running_server,
)

pytestmark = pytest.mark.asyncio


async def h2_request_with_headers(
    *,
    host: str = '127.0.0.1',
    port: int | None = None,
    method: str = 'GET',
    path: str = '/',
) -> tuple[int, list[tuple[bytes, bytes]], bytes]:
    reader, writer, conn, authority = await open_h2_connection(host=host, port=port)
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
        response_headers: list[tuple[bytes, bytes]] = []
        response_body = bytearray()

        while True:
            data = await asyncio.wait_for(reader.read(65535), timeout=5)
            if not data:
                break
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    status = int(dict(event.headers)[b':status'])
                    response_headers = [
                        (name, value)
                        for (name, value) in event.headers
                        if name != b':status'
                    ]
                elif isinstance(event, h2.events.DataReceived):
                    response_body.extend(event.data)
                    conn.acknowledge_received_data(
                        event.flow_controlled_length,
                        stream_id,
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    pending = conn.data_to_send()
                    if pending:
                        writer.write(pending)
                        await writer.drain()
                    assert status is not None
                    return status, response_headers, bytes(response_body)
            pending = conn.data_to_send()
            if pending:
                writer.write(pending)
                await writer.drain()

        raise RuntimeError('response stream ended unexpectedly')
    finally:
        writer.close()
        await writer.wait_closed()


async def test_http2_response_defaults_apply_to_normal_app_responses() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=find_free_port(),
        date_header=True,
        response_headers=('x-extra: works',),
    )
    async with running_server(app, config):
        status, response_headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=config.port),
            timeout=5,
        )

    headers = dict(response_headers)
    assert status == 200
    assert body == b'ok'
    assert headers[b'x-extra'] == b'works'
    assert b'date' in headers


async def test_h2_request_round_trip() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'hello from h2corn'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == b'hello from h2corn'


async def test_empty_body_request_omits_empty_state_and_terminal_receive_event() -> (
    None
):
    received = []
    has_state = None

    async def app(scope, receive, send):
        nonlocal has_state
        assert scope['type'] == 'http'
        has_state = 'state' in scope
        first = await receive()
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})
        second = await receive()
        received.extend((first, second))

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert has_state is False
    assert received == [
        {'type': 'http.request'},
        {'type': 'http.disconnect'},
    ]
    assert status == 204
    assert body == b''


async def test_fastapi_request_state_defaults_without_scope_state() -> None:
    fastapi_app = FastAPI()

    @fastapi_app.get('/state')
    async def state_endpoint(request: Request) -> PlainTextResponse:
        request.state.message = 'ready'
        return PlainTextResponse(request.state.message)

    config = Config(port=find_free_port())
    async with running_server(fastapi_app, config):
        status, body = await asyncio.wait_for(
            h2_request(port=config.port, path='/state'),
            timeout=5,
        )

    assert status == 200
    assert body == b'ready'


async def test_http_scope_advertises_pathsend_extension() -> None:
    extensions = None
    client = None

    async def app(scope, receive, send):
        nonlocal client, extensions
        extensions = scope['extensions']
        client = scope['client']
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert extensions == {'http.response.pathsend': {}}
    assert client is not None
    assert status == 204
    assert body == b''


async def test_http_scope_advertises_trailer_extension_when_request_accepts_it() -> (
    None
):
    extensions = None

    async def app(scope, receive, send):
        nonlocal extensions
        extensions = scope['extensions']
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[(b'te', b'trailers')],
            ),
            timeout=5,
        )

    assert extensions == {
        'http.response.pathsend': {},
        'http.response.trailers': {},
    }
    assert status == 204
    assert body == b''


async def test_fastapi_lifespan_state_is_visible_to_requests() -> None:
    seen = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.message = 'ready'
        seen.append('startup')
        yield
        seen.append('shutdown')

    fastapi_app = FastAPI(lifespan=lifespan)

    @fastapi_app.get('/message')
    async def message(request: Request) -> PlainTextResponse:
        return PlainTextResponse(request.app.state.message)

    config = Config(port=find_free_port())
    async with running_server(fastapi_app, config):
        status, body = await asyncio.wait_for(
            h2_request(port=config.port, path='/message'),
            timeout=5,
        )

    assert status == 200
    assert body == b'ready'
    assert seen == ['startup', 'shutdown']


async def test_fastapi_request_headers_work_with_tuple_backed_scope_headers() -> None:
    fastapi_app = FastAPI()

    @fastapi_app.get('/header')
    async def header(request: Request) -> PlainTextResponse:
        return PlainTextResponse(request.headers['x-demo'])

    config = Config(port=find_free_port())
    async with running_server(fastapi_app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                path='/header',
                extra_headers=[(b'x-demo', b'works')],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'works'


async def test_scope_headers_support_repeated_iteration_and_synthesized_host() -> None:
    async def app(scope, receive, send):
        first_pass = list(scope['headers'])
        second_pass = list(scope['headers'])
        assert first_pass == second_pass
        headers = dict(second_pass)
        payload = b'|'.join((headers[b'host'], headers[b'x-demo']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                path='/headers',
                extra_headers=[(b'x-demo', b'works')],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'127.0.0.1:{config.port}|works'.encode()


async def test_lifespan_startup_failure_is_reported() -> None:
    class FailingLifespan:
        async def __aenter__(self) -> None:
            raise RuntimeError('boom')

        async def __aexit__(self, *_):
            return False

    def lifespan(app: FastAPI) -> FailingLifespan:
        return FailingLifespan()

    fastapi_app = FastAPI(lifespan=lifespan)
    server = Server(fastapi_app, Config(port=find_free_port()))

    with pytest.raises(RuntimeError, match='boom'):
        await server.serve()


async def test_h2_header_list_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header list limit should reject before the app runs')

    config = Config(port=find_free_port(), h2_max_header_list_size=8)
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[(b'x-demo', b'0123456789')],
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_h2_header_list_limit_counts_rfc_overhead() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header list limit should reject before the app runs')

    config = Config(port=find_free_port(), h2_max_header_list_size=90)
    extra_headers = [(f'x{i}'.encode(), b'1') for i in range(10)]
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=extra_headers,
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_h2_content_length_limit_returns_413() -> None:
    async def app(scope, receive, send):
        raise AssertionError(
            'request body size limit should reject before the app runs'
        )

    config = Config(port=find_free_port(), max_request_body_size=4)
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                method='POST',
                body=b'hello',
                extra_headers=[(b'content-length', b'5')],
            ),
            timeout=5,
        )

    assert status == 413
    assert body == b''


async def test_request_body_can_be_consumed() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(port=config.port, method='POST', body=b'payload'),
            timeout=5,
        )

    assert status == 200
    assert body == b'payload'


async def test_request_body_can_be_consumed_across_multiple_data_frames() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
        try:
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                stream_id,
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                end_stream=False,
            )
            conn.send_data(stream_id, b'pay', end_stream=False)
            conn.send_data(stream_id, b'load', end_stream=True)
            writer.write(conn.data_to_send())
            await writer.drain()
            status, body, trailers = await read_h2_response(
                reader,
                writer,
                conn,
                stream_id,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert body == b'payload'
    assert trailers == []


async def test_request_body_can_be_consumed_across_delayed_small_data_frames() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
        try:
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                stream_id,
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                end_stream=False,
            )
            writer.write(conn.data_to_send())
            await writer.drain()

            for index, chunk in enumerate((b'p', b'a', b'y', b'l', b'o', b'a', b'd')):
                conn.send_data(
                    stream_id,
                    chunk,
                    end_stream=index == 6,
                )
                writer.write(conn.data_to_send())
                await writer.drain()
                await asyncio.sleep(0.01)

            status, body, trailers = await read_h2_response(
                reader,
                writer,
                conn,
                stream_id,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert body == b'payload'
    assert trailers == []


async def test_request_body_can_be_consumed_with_empty_h2_data_frames() -> None:
    async def app(scope, receive, send):
        body = await read_http_request_body(receive)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        reader, writer, conn, authority = await open_h2_connection(port=config.port)
        try:
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(
                stream_id,
                [
                    (b':method', b'POST'),
                    (b':scheme', b'http'),
                    (b':authority', authority),
                    (b':path', b'/'),
                ],
                end_stream=False,
            )
            conn.send_data(stream_id, b'', end_stream=False)
            conn.send_data(stream_id, b'pay', end_stream=False)
            conn.send_data(stream_id, b'', end_stream=False)
            conn.send_data(stream_id, b'load', end_stream=True)
            writer.write(conn.data_to_send())
            await writer.drain()
            status, body, trailers = await read_h2_response(
                reader,
                writer,
                conn,
                stream_id,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert body == b'payload'
    assert trailers == []


async def test_h2_streaming_response_small_chunks_arrive_in_order() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        for chunk in (b'a', b'b', b'c'):
            await send({
                'type': 'http.response.body',
                'body': chunk,
                'more_body': True,
            })
            await asyncio.sleep(0.01)
        await send({'type': 'http.response.body', 'body': b'd'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == b'abcd'


async def test_response_trailers_are_sent_when_request_accepts_them() -> None:
    extensions = None

    async def app(scope, receive, send):
        nonlocal extensions
        extensions = scope['extensions']
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'trailer', b'x-checksum, x-finished'),
            ],
            'trailers': True,
        })
        await send({'type': 'http.response.body', 'body': b'payload'})
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-checksum', b'ok')],
            'more_trailers': True,
        })
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-finished', b'yes')],
        })

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body, trailers = await asyncio.wait_for(
            h2_request_details(
                port=config.port,
                extra_headers=[(b'te', b'trailers')],
            ),
            timeout=5,
        )

    assert extensions == {
        'http.response.pathsend': {},
        'http.response.trailers': {},
    }
    assert status == 200
    assert body == b'payload'
    assert trailers == [(b'x-checksum', b'ok'), (b'x-finished', b'yes')]


async def test_response_trailers_require_request_te_trailers() -> None:
    extensions = None

    async def app(scope, receive, send):
        nonlocal extensions
        extensions = scope['extensions']
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
            'trailers': True,
        })

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert extensions == {'http.response.pathsend': {}}
    assert status == 500
    assert body == b''


async def test_http_response_pathsend_streams_file(tmp_path: Path) -> None:
    file_path = tmp_path / 'payload.bin'
    payload = (b'pathsend-' * 5000)[:40000]
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
                (b'content-length', str(len(payload)).encode()),
            ],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == payload


async def test_http_response_pathsend_synthesizes_content_length_when_missing(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / 'payload-no-length.bin'
    payload = (b'pathsend-no-length-' * 2000)[:24000]
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
            ],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=config.port),
            timeout=5,
        )

    assert status == 200
    assert body == payload
    assert dict(headers)[b'content-length'] == str(len(payload)).encode()


async def test_http_response_pathsend_streams_large_file(tmp_path: Path) -> None:
    file_path = tmp_path / 'large-payload.bin'
    payload = (b'large-pathsend-' * 22000)[:300000]
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
                (b'content-length', str(len(payload)).encode()),
            ],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == payload


async def test_h2_head_pathsend_synthesizes_content_length_and_keeps_empty_body(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / 'head-h2-pathsend.txt'
    payload = b'head body should stay hidden in http2 too'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=config.port, method='HEAD'),
            timeout=5,
        )

    assert status == 200
    assert body == b''
    assert dict(headers)[b'content-length'] == str(len(payload)).encode()


async def test_http_response_pathsend_can_be_followed_by_trailers(
    tmp_path: Path,
) -> None:
    extensions = None
    file_path = tmp_path / 'trailers.bin'
    payload = b'pathsend-with-trailers'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        nonlocal extensions
        extensions = scope['extensions']
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
                (b'content-length', str(len(payload)).encode()),
                (b'trailer', b'x-finished'),
            ],
            'trailers': True,
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-finished', b'yes')],
        })

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body, trailers = await asyncio.wait_for(
            h2_request_details(
                port=config.port,
                extra_headers=[(b'te', b'trailers')],
            ),
            timeout=5,
        )

    assert extensions == {
        'http.response.pathsend': {},
        'http.response.trailers': {},
    }
    assert status == 200
    assert body == payload
    assert trailers == [(b'x-finished', b'yes')]


async def test_starlette_file_response_uses_pathsend(tmp_path: Path) -> None:
    file_path = tmp_path / 'hello.txt'
    payload = b'hello from file response'
    file_path.write_bytes(payload)

    fastapi_app = FastAPI()

    @fastapi_app.get('/download')
    async def download() -> FileResponse:
        return FileResponse(file_path)

    config = Config(port=find_free_port())
    async with running_server(fastapi_app, config):
        status, body = await asyncio.wait_for(
            h2_request(port=config.port, path='/download'),
            timeout=5,
        )

    assert status == 200
    assert body == payload


async def test_starlette_head_file_response_keeps_empty_body(tmp_path: Path) -> None:
    file_path = tmp_path / 'head.txt'
    file_path.write_bytes(b'head body should stay hidden')

    fastapi_app = FastAPI()

    @fastapi_app.head('/download')
    async def head_download() -> FileResponse:
        return FileResponse(file_path)

    config = Config(port=find_free_port())
    async with running_server(fastapi_app, config):
        status, body = await asyncio.wait_for(
            h2_request(port=config.port, path='/download', method='HEAD'),
            timeout=5,
        )

    assert status == 200
    assert body == b''


async def test_h2_head_response_suppresses_app_body() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'hello'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(port=config.port, method='HEAD'),
            timeout=5,
        )

    assert status == 200
    assert body == b''


async def test_h2_no_body_unary_request_can_complete_inline() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'inline'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == b'inline'


async def test_h2_no_body_request_falls_back_after_initial_await() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await asyncio.sleep(0)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'await-before-start'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == b'await-before-start'


async def test_h2_no_body_request_preserves_buffered_start_when_falling_back() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await asyncio.sleep(0)
        await send({'type': 'http.response.body', 'body': b'await-after-start'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(port=config.port), timeout=5)

    assert status == 200
    assert body == b'await-after-start'


async def test_h2_disconnect_on_aborted_upload() -> None:
    events = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
        try:
            while True:
                message = await receive()
                events.append(message['type'])
                if message['type'] == 'http.disconnect':
                    break
                if not message.get('more_body', False):
                    break
        except Exception as e:
            events.append(f'error: {e}')

    config = Config(port=find_free_port())
    async with running_server(app, config):
        _reader, writer, conn, auth = await open_h2_connection(port=config.port)
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            stream_id,
            [
                (b':method', b'POST'),
                (b':path', b'/'),
                (b':scheme', b'http'),
                (b':authority', auth),
                (b'content-length', b'1000'),
            ],
            end_stream=False,
        )
        conn.send_data(stream_id, b'part1', end_stream=False)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.1)

    assert events == ['http.request', 'http.disconnect']


async def test_h2_synchronous_app_failure_returns_500() -> None:
    def app(scope, receive, send):
        if scope['type'] == 'http':
            raise ValueError('Synchronous crash!')

        async def lifespan():
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return

        return lifespan()

    config = Config(port=find_free_port())
    async with running_server(app, config):
        status, _body = await asyncio.wait_for(
            h2_request(port=config.port, method='GET'),
            timeout=5,
        )

    assert status == 500
