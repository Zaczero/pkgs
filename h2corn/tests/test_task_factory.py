import asyncio
import sys

import pytest
from h2corn import Config

from tests._support import http1_request, running_server, server_port


@pytest.mark.asyncio
async def test_request_task_uses_factory_installed_after_server_start() -> None:
    factory_kwargs: list[dict[str, object]] = []

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    def task_factory(loop, coroutine, **kwargs):
        code = getattr(coroutine, 'cr_code', None)
        if code is not None and code.co_name == 'app':
            factory_kwargs.append(kwargs.copy())
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    config = Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')
    async with running_server(app, config) as server:
        loop = asyncio.get_running_loop()
        previous_factory = loop.get_task_factory()
        loop.set_task_factory(task_factory)
        try:
            port = server_port(server)
            status, _headers, body, _trailers = await http1_request(
                port=port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n'.encode(),
            )
        finally:
            loop.set_task_factory(previous_factory)

    assert status == 200
    assert body == b'ok'
    assert len(factory_kwargs) == 1
    if type(loop).create_task is asyncio.BaseEventLoop.create_task:
        if sys.version_info >= (3, 14):
            assert factory_kwargs[0]['eager_start'] is True
        else:
            assert 'eager_start' not in factory_kwargs[0]


def test_request_task_uses_loop_subclass_create_task() -> None:
    request_tasks: list[str] = []

    class RecordingLoop(asyncio.SelectorEventLoop):
        def create_task(self, coro, **kwargs):
            code = getattr(coro, 'cr_code', None)
            if code is not None and code.co_name == 'app':
                request_tasks.append(code.co_name)
            return super().create_task(coro, **kwargs)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    async def exchange() -> tuple[int, bytes]:
        config = Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')
        async with running_server(app, config) as server:
            port = server_port(server)
            status, _headers, body, _trailers = await http1_request(
                port=port,
                request=f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n'.encode(),
            )
            return status, body

    loop = RecordingLoop()
    try:
        status, body = loop.run_until_complete(exchange())
    finally:
        loop.close()

    assert status == 200
    assert body == b'ok'
    assert request_tasks == ['app']
