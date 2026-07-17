"""ASGI semantics battery for the pump/duck-future architecture.

Pins the asyncio-facing contract: request tasks are real ``asyncio.Task``s
(``current_task`` identity, cancellation, contextvars), duck futures survive
``wait_for``/``gather``/anyio cancellation scopes without losing body events,
and concurrent streams resolved in one pump batch never trip CPython's
``_enter_task`` re-entrancy guard.
"""

import asyncio
import contextvars

import anyio
import pytest
from h2corn import Config

from tests._support import (
    h2_request,
    open_h2_connection,
    read_h2_response,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio

_VAR: contextvars.ContextVar[str] = contextvars.ContextVar(
    'h2corn_test', default='unset'
)


def _config() -> Config:
    return Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')


async def _respond(send, body: bytes) -> None:
    await send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [(b'content-type', b'text/plain')],
    })
    await send({'type': 'http.response.body', 'body': body})


async def _streaming_exchange(port: int, *, body_parts: list[bytes], pause: float):
    """Send headers first, then body parts after ``pause`` — forcing the app's
    receive() onto the suspended duck-future path.
    """
    reader, writer, conn, authority = await open_h2_connection(port=port)
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
        )
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(pause)
        for i, part in enumerate(body_parts):
            conn.send_data(stream_id, part, end_stream=i == len(body_parts) - 1)
            writer.write(conn.data_to_send())
            await writer.drain()
            await asyncio.sleep(pause)
        return await read_h2_response(reader, writer, conn, stream_id)
    finally:
        writer.close()
        await writer.wait_closed()


async def test_current_task_identity_and_name_across_suspension() -> None:
    seen: dict[str, object] = {}

    async def app(scope, receive, send):
        task_before = asyncio.current_task()
        assert task_before is not None
        message = await receive()  # suspends: body arrives later
        seen['same'] = asyncio.current_task() is task_before
        seen['name'] = task_before.get_name()
        seen['body'] = message.get('body', b'')
        await _respond(send, b'ok')

    config = _config()
    async with running_server(app, config) as server:
        status, body, _ = await asyncio.wait_for(
            _streaming_exchange(
                server_port(server), body_parts=[b'payload'], pause=0.05
            ),
            timeout=5,
        )
    assert status == 200 and body == b'ok'
    assert seen['same'] is True
    assert seen['name'] == 'h2corn.request'
    assert seen['body'] == b'payload'


async def test_contextvars_survive_duck_future_suspension() -> None:
    seen: dict[str, str] = {}

    async def app(scope, receive, send):
        _VAR.set('inside-request')
        await receive()
        seen['after_await'] = _VAR.get()
        await _respond(send, b'ok')

    config = _config()
    async with running_server(app, config) as server:
        status, _, _ = await asyncio.wait_for(
            _streaming_exchange(server_port(server), body_parts=[b'x'], pause=0.05),
            timeout=5,
        )
    assert status == 200
    assert seen['after_await'] == 'inside-request'


async def test_wait_for_receive_timeout_does_not_lose_body() -> None:
    seen: dict[str, bytes] = {}

    async def app(scope, receive, send):
        # First receive() times out and cancels the duck future; the body
        # arriving afterwards must be observed by the next receive().
        try:
            await asyncio.wait_for(receive(), timeout=0.05)
        except TimeoutError:
            pass
        message = await receive()
        seen['body'] = message.get('body', b'')
        await _respond(send, b'ok')

    config = _config()
    async with running_server(app, config) as server:
        status, _, _ = await asyncio.wait_for(
            _streaming_exchange(
                server_port(server), body_parts=[b'late-body'], pause=0.2
            ),
            timeout=5,
        )
    assert status == 200
    assert seen['body'] == b'late-body'


async def test_gather_with_receive_and_timer() -> None:
    seen: dict[str, object] = {}

    async def app(scope, receive, send):
        message, _ = await asyncio.gather(receive(), asyncio.sleep(0.01))
        seen['body'] = message.get('body', b'')
        await _respond(send, b'ok')

    config = _config()
    async with running_server(app, config) as server:
        status, _, _ = await asyncio.wait_for(
            _streaming_exchange(
                server_port(server), body_parts=[b'gathered'], pause=0.05
            ),
            timeout=5,
        )
    assert status == 200
    assert seen['body'] == b'gathered'


async def test_anyio_cancellation_scope_and_task_group() -> None:
    seen: dict[str, object] = {}

    async def app(scope, receive, send):
        with anyio.move_on_after(0.05) as scope_:
            await receive()
        seen['timed_out'] = scope_.cancelled_caught
        message = None
        async with anyio.create_task_group() as tg:
            tg.start_soon(anyio.sleep, 0.01)
            message = await receive()
        assert message is not None
        seen['body'] = message.get('body', b'')
        await _respond(send, b'ok')

    config = _config()
    async with running_server(app, config) as server:
        status, _, _ = await asyncio.wait_for(
            _streaming_exchange(
                server_port(server), body_parts=[b'anyio-body'], pause=0.2
            ),
            timeout=5,
        )
    assert status == 200
    assert seen['timed_out'] is True
    assert seen['body'] == b'anyio-body'


async def test_concurrent_streams_resolve_without_reentrancy_errors() -> None:
    """Multiple suspended app tasks resolved in the same pump batch must not
    trip the `_enter_task` guard (would surface as 500s/RuntimeErrors).
    """

    async def app(scope, receive, send):
        message = await receive()
        await _respond(send, message.get('body', b''))

    config = _config()
    async with running_server(app, config) as server:
        results = await asyncio.wait_for(
            asyncio.gather(*[
                _streaming_exchange(
                    server_port(server), body_parts=[f'req-{i}'.encode()], pause=0.05
                )
                for i in range(8)
            ]),
            timeout=10,
        )
    for i, (status, body, _) in enumerate(results):
        assert status == 200
        assert body == f'req-{i}'.encode()


async def test_app_timer_and_sleep_interleave_with_pump() -> None:
    """The pump must not starve loop timers: asyncio.sleep inside the app
    completes while other requests flow.
    """

    async def app(scope, receive, send):
        await asyncio.sleep(0.02)
        await _respond(send, b'slept')

    config = _config()
    async with running_server(app, config) as server:
        bodies = await asyncio.wait_for(
            asyncio.gather(*[h2_request(port=server_port(server)) for _ in range(16)]),
            timeout=10,
        )
    assert all(status == 200 and body == b'slept' for status, body in bodies)


async def test_scope_metadata_mutation_is_request_local() -> None:
    requests = 0

    async def app(scope, receive, send):
        nonlocal requests
        if requests == 0:
            scope['asgi']['version'] = 'application-mutated'
            scope['extensions']['application.private'] = {}
            scope['extensions']['http.response.pathsend']['application.private'] = True
        else:
            assert scope['asgi']['version'] == '3.0'
            assert 'application.private' not in scope['extensions']
            assert (
                'application.private'
                not in scope['extensions']['http.response.pathsend']
            )
        requests += 1
        await _respond(send, b'ok')

    async with running_server(app, _config()) as server:
        first = await h2_request(port=server_port(server))
        second = await h2_request(port=server_port(server))

    assert first == (200, b'ok')
    assert second == (200, b'ok')
    assert requests == 2


async def test_peer_disconnect_cancels_abandoned_application_task() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def app(scope, receive, send):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with running_server(app, _config()) as server:
        _reader, writer, conn, authority = await open_h2_connection(
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
        await asyncio.wait_for(started.wait(), timeout=5)
        writer.close()
        await writer.wait_closed()
        await asyncio.wait_for(cancelled.wait(), timeout=5)


async def test_app_exception_after_cancelled_receive_yields_500() -> None:
    async def app(scope, receive, send):
        try:
            await asyncio.wait_for(receive(), timeout=0.05)
        except TimeoutError:
            pass
        raise RuntimeError('boom after cancellation')

    config = _config()
    async with running_server(app, config) as server:
        status, _, _ = await asyncio.wait_for(
            _streaming_exchange(server_port(server), body_parts=[b'x'], pause=0.2),
            timeout=5,
        )
    assert status == 500
