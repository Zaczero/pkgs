import asyncio
import socket
import threading
import weakref
from typing import cast

import pytest
from h2corn import ApplicationFactory, Config, Server

from tests._support import (
    HANG_BOUND,
    gil_is_disabled,
    server_port,
    stop_serving,
    wait_for_server,
)

pytestmark = pytest.mark.asyncio


async def _request(server: Server) -> bytes:
    reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
    try:
        writer.write(b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
        await writer.drain()
        return await reader.read()
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.parametrize('lifespan', ['on', 'off'])
async def test_factory_pairs_each_app_with_its_request_loop_and_state(lifespan) -> None:
    lock = threading.Lock()
    constructed = []
    requested = []
    closed = []
    app_refs = []
    released_loops = []
    effective_loops = 3 if gil_is_disabled() else 1

    def released(owner_loop):
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        with lock:
            released_loops.append((owner_loop, running_loop))

    def create_app():
        owner_loop = asyncio.get_running_loop()
        token = object()
        with lock:
            constructed.append((owner_loop, token))

        async def app(scope, receive, send):
            assert asyncio.get_running_loop() is owner_loop
            if scope['type'] == 'lifespan':
                assert (await receive())['type'] == 'lifespan.startup'
                scope['state']['owner'] = token
                await send({'type': 'lifespan.startup.complete'})
                assert (await receive())['type'] == 'lifespan.shutdown'
                with lock:
                    closed.append(token)
                await send({'type': 'lifespan.shutdown.complete'})
            else:
                if lifespan == 'on':
                    assert scope['state']['owner'] is token
                else:
                    assert 'state' not in scope
                with lock:
                    requested.append(token)
                await send({
                    'type': 'http.response.start',
                    'status': 204,
                    'headers': [],
                })
                await send({'type': 'http.response.body'})

        with lock:
            app_refs.append(weakref.ref(app))
        weakref.finalize(app, released, owner_loop)
        return app

    server = Server(
        create_app,
        Config(port=0, loop_threads=3, lifespan=lifespan, access_log=False),
        factory=True,
    )
    serving = asyncio.create_task(server.serve())
    try:
        await wait_for_server(server, serving)
        for _ in range(2 * effective_loops):
            assert (await _request(server)).startswith(b'HTTP/1.1 204')
    finally:
        await stop_serving(server, serving)

    assert server.app is create_app
    assert len(constructed) == effective_loops
    assert len({loop for loop, _ in constructed}) == effective_loops
    assert set(requested) == {token for _, token in constructed}
    assert set(closed) == (set(requested) if lifespan == 'on' else set())
    assert all(reference() is None for reference in app_refs)
    assert len(released_loops) == effective_loops
    assert all(owner is running for owner, running in released_loops)


async def test_factory_is_reconstructed_for_each_serve_generation() -> None:
    instances = []

    def create_app():
        instances.append(object())

        async def app(_scope, _receive, send):
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body'})

        return app

    server = Server(
        create_app, Config(port=0, lifespan='off', access_log=False), factory=True
    )
    for _ in range(2):
        serving = asyncio.create_task(server.serve())
        try:
            await wait_for_server(server, serving)
            assert (await _request(server)).startswith(b'HTTP/1.1 204')
        finally:
            await stop_serving(server, serving)
    assert len(instances) == 2
    assert instances[0] is not instances[1]


@pytest.mark.parametrize('result', ['non-callable', 'awaitable', 'exception'])
async def test_factory_failures_are_fatal_under_auto_lifespan(result) -> None:
    async def async_factory():
        return None

    def create_app():
        asyncio.get_running_loop()
        if result == 'exception':
            raise ValueError('constructor failed')
        return async_factory() if result == 'awaitable' else 7

    server = Server(
        cast('ApplicationFactory', create_app),
        Config(port=0, lifespan='auto'),
        factory=True,
    )
    error = ValueError if result == 'exception' else TypeError
    match = 'constructor failed' if result == 'exception' else 'application factory'
    with pytest.raises(error, match=match):
        await server.serve()
    assert server.addresses == ()
    assert not server.releasing


async def test_factory_shutdown_preserves_tasks_on_borrowed_primary_loop() -> None:
    waiting = asyncio.Event()
    caller_task = asyncio.create_task(waiting.wait())

    def create_app():
        async def app(_scope, _receive, send):
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body'})

        return app

    server = Server(create_app, Config(port=0, lifespan='off'), factory=True)
    serving = asyncio.create_task(server.serve())
    try:
        await wait_for_server(server, serving)
        await stop_serving(server, serving)
        assert not caller_task.done()
    finally:
        waiting.set()
        await caller_task
        await stop_serving(server, serving)


async def test_startup_failure_is_published_before_primary_cleanup_settles() -> None:
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    def create_app():
        async def app(_scope, receive, send):
            assert (await receive())['type'] == 'lifespan.startup'
            await send({
                'type': 'lifespan.startup.failed',
                'message': 'acquisition failed',
            })
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cleanup_started.set()
                await release_cleanup.wait()
                raise

        return app

    server = Server(
        create_app,
        Config(port=0, lifespan='on', timeout_lifespan_startup=0, access_log=False),
        factory=True,
    )
    ready = asyncio.create_task(server.wait_started())
    serving = asyncio.create_task(server.serve())
    try:
        await asyncio.wait_for(cleanup_started.wait(), HANG_BOUND)
        returned_before_cleanup = serving.done()
        readiness_before_cleanup = ready.done()
    finally:
        release_cleanup.set()
        with pytest.raises(RuntimeError, match='acquisition failed'):
            await serving
        with pytest.raises(RuntimeError, match='acquisition failed'):
            await ready
        generation = server._generation
        if generation is not None:
            with pytest.raises(RuntimeError, match='acquisition failed'):
                await asyncio.wait_for(asyncio.shield(generation.released), HANG_BOUND)
    assert returned_before_cleanup
    assert readiness_before_cleanup


@pytest.mark.skipif(
    not gil_is_disabled(), reason='requires actual GIL-disabled loop shards'
)
async def test_failed_factory_startup_retains_peer_cleanup_without_readiness() -> None:
    main_loop = asyncio.get_running_loop()
    lock = threading.Lock()
    secondary_started = threading.Event()
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    primary_closed = threading.Event()
    loops = []
    ports = []
    count = 0

    def create_app():
        nonlocal count
        loop = asyncio.get_running_loop()
        with lock:
            ordinal = count
            count += 1
            loops.append(loop)
            if ordinal == 0:
                ports.append(server_port(server))

        async def app(scope, receive, send):
            assert scope['type'] == 'lifespan'
            assert (await receive())['type'] == 'lifespan.startup'
            if ordinal == 1:
                secondary_started.set()
                try:
                    await asyncio.Future()
                finally:
                    cleanup_started.set()
                    await asyncio.to_thread(release_cleanup.wait, HANG_BOUND)
                return
            if ordinal == 2:
                assert await asyncio.to_thread(secondary_started.wait, HANG_BOUND)
                await send({
                    'type': 'lifespan.startup.failed',
                    'message': 'peer failed',
                })
                return
            if ordinal == 0:
                assert loop is main_loop
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            primary_closed.set()
            await send({'type': 'lifespan.shutdown.complete'})

        return app

    server = Server(
        create_app,
        Config(
            port=0,
            loop_threads=3,
            lifespan='on',
            timeout_lifespan_startup=0,
            timeout_lifespan_shutdown=0,
            access_log=False,
        ),
        factory=True,
    )
    serving = asyncio.create_task(server.serve())
    ready = asyncio.create_task(server.wait_started())
    next_ready = None
    try:
        with pytest.raises(RuntimeError, match='peer failed'):
            await asyncio.wait_for(asyncio.shield(serving), HANG_BOUND)
        with pytest.raises(RuntimeError, match='peer failed'):
            await ready
        assert await asyncio.to_thread(cleanup_started.wait, HANG_BOUND)
        assert server.releasing
        assert server.addresses == ()
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', ports[0]))
        assert not primary_closed.is_set()
        assert not any(loop.is_closed() for loop in loops)
        with pytest.raises(RuntimeError, match='still releasing'):
            await server.serve()
        next_ready = asyncio.create_task(server.wait_started())
        await asyncio.sleep(0)
    finally:
        release_cleanup.set()
        generation = server._generation
        if generation is not None:
            with pytest.raises(RuntimeError, match='peer failed'):
                await asyncio.wait_for(asyncio.shield(generation.released), HANG_BOUND)
        if not ready.done():
            ready.cancel()
    assert primary_closed.is_set()
    assert all(loop.is_closed() for loop in loops if loop is not main_loop)
    assert not server.releasing
    assert next_ready is not None
    assert not next_ready.done()
    serving_again = asyncio.create_task(server.serve())
    try:
        await asyncio.wait_for(asyncio.shield(next_ready), HANG_BOUND)
    finally:
        await stop_serving(server, serving_again)


@pytest.mark.skipif(
    not gil_is_disabled(), reason='requires actual GIL-disabled loop shards'
)
async def test_private_loop_settles_async_generators_and_default_executor() -> None:
    main_loop = asyncio.get_running_loop()
    generator_closed = threading.Event()
    executor_finished = threading.Event()
    release_executor = threading.Event()
    generators = []
    private_loops = []

    async def resource():
        try:
            yield None
        finally:
            generator_closed.set()

    def worker():
        release_executor.wait(HANG_BOUND)
        executor_finished.set()

    def create_app():
        owner_loop = asyncio.get_running_loop()

        async def app(_scope, receive, send):
            assert (await receive())['type'] == 'lifespan.startup'
            if owner_loop is not main_loop:
                private_loops.append(owner_loop)
                generator = resource()
                await anext(generator)
                generators.append(generator)
                owner_loop.run_in_executor(None, worker)
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})

        return app

    server = Server(
        create_app,
        Config(port=0, loop_threads=2, lifespan='on', access_log=False),
        factory=True,
    )
    serving = asyncio.create_task(server.serve())
    try:
        await wait_for_server(server, serving)
        server.shutdown()
        assert await asyncio.to_thread(generator_closed.wait, HANG_BOUND)
        assert not executor_finished.is_set()
        assert not serving.done()
    finally:
        release_executor.set()
        await stop_serving(server, serving)
    assert executor_finished.is_set()
    assert len(private_loops) == 1
    assert private_loops[0].is_closed()
