"""Request callbacks must not retain an otherwise unreachable event loop."""

import asyncio
import gc
import threading
import weakref
from typing import Any, cast

import pytest
from h2corn import Config

from tests._support import (
    gil_is_disabled,
    http1_request,
    running_server,
    server_port,
)


@pytest.mark.parametrize('protocol', ['http', 'websocket'])
@pytest.mark.parametrize('callback', ['receive', 'send'])
def test_retained_request_callback_does_not_keep_closed_loop_alive(protocol, callback):
    async def serve_once():
        loop = asyncio.get_running_loop()
        loop_ref = weakref.ref(loop)
        loop_with_callback = cast('Any', loop)

        async def app(scope, receive, send):
            loop_with_callback.retained_callback = (
                receive if callback == 'receive' else send
            )
            if scope['type'] == 'websocket':
                assert (await receive())['type'] == 'websocket.connect'
                await send({'type': 'websocket.accept'})
                await send({'type': 'websocket.close', 'code': 1000})
            else:
                await send({
                    'type': 'http.response.start',
                    'status': 204,
                    'headers': [],
                })
                await send({'type': 'http.response.body', 'body': b''})

        async with running_server(
            app, Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')
        ) as server:
            if protocol == 'http':
                status, _, _, _ = await http1_request(
                    port=server_port(server),
                    request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
                )
                assert status == 204
            else:
                reader, writer = await asyncio.open_connection(
                    '127.0.0.1', server_port(server)
                )
                try:
                    writer.write(
                        b'GET / HTTP/1.1\r\nHost: localhost\r\n'
                        b'Upgrade: websocket\r\nConnection: Upgrade\r\n'
                        b'Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\n'
                        b'Sec-WebSocket-Version: 13\r\n\r\n'
                    )
                    await writer.drain()
                    assert (
                        await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 5)
                    ).startswith(b'HTTP/1.1 101 ')
                finally:
                    writer.close()
                    await writer.wait_closed()
        if protocol == 'websocket' and callback == 'receive':
            event = await asyncio.wait_for(loop_with_callback.retained_callback(), 2)
            assert event['type'] == 'websocket.disconnect'
        if callback == 'send':
            message = (
                {'type': 'http.response.body'}
                if protocol == 'http'
                else {'type': 'websocket.send', 'bytes': b'late'}
            )
            with pytest.raises(
                OSError, match='ASGI send called after the stream closed'
            ):
                await loop_with_callback.retained_callback(message)
        return loop_ref

    with asyncio.Runner() as runner:
        loop_ref = runner.run(serve_once())
    del runner
    gc.collect()
    assert loop_ref() is None


def test_retained_http_callbacks_report_disconnect_after_shutdown():
    async def serve_once():
        callbacks = {}

        async def app(scope, receive, send):
            callbacks['receive'] = receive
            callbacks['send'] = send
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})

        async with running_server(
            app, Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')
        ) as server:
            status, _, _, _ = await http1_request(
                port=server_port(server),
                request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
            )
            assert status == 204

        assert (await asyncio.wait_for(callbacks['receive'](), 2))[
            'type'
        ] == 'http.request'
        assert (await asyncio.wait_for(callbacks['receive'](), 2))[
            'type'
        ] == 'http.disconnect'
        with pytest.raises(OSError, match='ASGI send called after the stream closed'):
            await callbacks['send']({'type': 'http.response.body'})

    asyncio.run(serve_once())


@pytest.mark.skipif(not gil_is_disabled(), reason='requires free-threaded Python')
def test_retained_callbacks_do_not_keep_secondary_loops_alive():
    async def serve_once():
        loops = {}
        lock = threading.Lock()

        async def app(scope, receive, send):
            loop = asyncio.get_running_loop()
            cast('Any', loop).retained_callbacks = (receive, send)
            with lock:
                loops[id(loop)] = weakref.ref(loop)
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})

        async with running_server(
            app,
            Config(
                bind=('127.0.0.1:0',),
                access_log=False,
                lifespan='off',
                loop_threads=4,
            ),
        ) as server:
            request = b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'
            responses = await asyncio.gather(
                *(
                    http1_request(port=server_port(server), request=request)
                    for _ in range(32)
                )
            )
            assert all(response[0] == 204 for response in responses)
        assert len(loops) == 4
        return tuple(loops.values())

    with asyncio.Runner() as runner:
        loop_refs = runner.run(serve_once())
    del runner
    gc.collect()
    assert all(ref() is None for ref in loop_refs)
