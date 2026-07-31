import asyncio
import os
import socket
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import h2.events
import h2.settings
import pytest
from fastapi import FastAPI
from h2corn import Config, Server
from starlette.requests import Request
from starlette.responses import FileResponse, PlainTextResponse

from tests._support import (
    h2_request,
    h2_request_details,
    open_h2_connection,
    read_h2_response,
    read_http_request_body,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio

H2_OUTBOUND_RESPONSE_BYTE_CAPACITY = 2 * 1024 * 1024


def _gil_is_disabled() -> bool:
    is_gil_enabled = getattr(sys, '_is_gil_enabled', None)
    return callable(is_gil_enabled) and not is_gil_enabled()


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
        port=0,
        date_header=True,
        response_headers=('x-extra: works',),
    )
    async with running_server(app, config) as server:
        status, response_headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=server_port(server)),
            timeout=5,
        )

    headers = dict(response_headers)
    assert status == 200
    assert body == b'ok'
    assert headers[b'x-extra'] == b'works'
    assert b'date' in headers


@pytest.mark.parametrize(
    ('status', 'content_length'),
    [(204, None), (304, b'7'), (205, b'0')],
)
async def test_http2_content_length_is_omitted_only_for_statuses_that_forbid_it(
    status: int,
    content_length: bytes | None,
) -> None:
    async def app(_scope, _receive, send):
        await send({
            'type': 'http.response.start',
            'status': status,
            'headers': [(b'content-length', b'7')],
        })
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        response_status, response_headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=server_port(server)), timeout=5
        )

    assert response_status == status
    assert dict(response_headers).get(b'content-length') == content_length
    assert body == b''


async def test_h2_request_round_trip() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'hello from h2corn'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert status == 200
    assert body == b'hello from h2corn'


async def test_h2_response_body_byte_budget_waits_for_flow_control_progress() -> None:
    body_admitted = asyncio.Queue()

    async def app(_scope, _receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        for index in range(H2_OUTBOUND_RESPONSE_BYTE_CAPACITY // (64 * 1024) + 1):
            await send({
                'type': 'http.response.body',
                'body': bytes([index]) * (64 * 1024),
                'more_body': True,
            })
            body_admitted.put_nowait(index)
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0, lifespan='off')) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
        try:
            stream_id = conn.get_next_available_stream_id()
            conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0})
            # The HTTP/2 default connection window is 65,535 bytes. Make it
            # exactly one conventional body chunk so the following stream
            # grant writes 64 KiB and leaves both send windows at zero.
            conn.increment_flow_control_window(1)
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

            for expected in range(H2_OUTBOUND_RESPONSE_BYTE_CAPACITY // (64 * 1024)):
                assert (
                    await asyncio.wait_for(body_admitted.get(), timeout=5) == expected
                )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(body_admitted.get(), timeout=0.1)

            conn.increment_flow_control_window(64 * 1024, stream_id=stream_id)
            writer.write(conn.data_to_send())
            await writer.drain()
            assert await asyncio.wait_for(body_admitted.get(), timeout=5) == (
                H2_OUTBOUND_RESPONSE_BYTE_CAPACITY // (64 * 1024)
            )

            status, body, _trailers = await read_h2_response(
                reader,
                writer,
                conn,
                stream_id,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    assert status == 200
    assert len(body) == H2_OUTBOUND_RESPONSE_BYTE_CAPACITY + 64 * 1024


async def test_empty_body_request_gets_empty_state_and_terminal_receive_event() -> (
    None
):
    received = []
    state = None

    async def app(scope, receive, send):
        nonlocal state
        assert scope['type'] == 'http'
        # Present even though this app's lifespan stored nothing: the key says
        # the server supports lifespan state, and an app reading it directly
        # would otherwise get a `KeyError` decided by unrelated startup code.
        state = scope.get('state', 'missing')
        first = await receive()
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})
        second = await receive()
        received.extend((first, second))

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert state == {}
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

    config = Config(port=0)
    async with running_server(fastapi_app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), path='/state'),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(fastapi_app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), path='/message'),
            timeout=5,
        )

    assert status == 200
    assert body == b'ready'
    assert seen == ['startup', 'shutdown']


@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_each_loop_has_transactional_lifespan_and_isolated_state() -> None:
    main_loop_id = id(asyncio.get_running_loop())
    lock = threading.Lock()
    startups: list[int] = []
    shutdowns: list[int] = []
    loop_modules: list[str] = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            loop_id = id(asyncio.get_running_loop())
            scope['state']['loop_id'] = loop_id
            with lock:
                startups.append(loop_id)
                loop_modules.append(type(asyncio.get_running_loop()).__module__)
            assert (await receive())['type'] == 'lifespan.startup'
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            with lock:
                shutdowns.append(loop_id)
            return

        body = str(scope['state']['loop_id']).encode()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(port=0, loop_threads=4, lifespan='on', access_log=False)
    async with running_server(app, config) as server:
        bodies = []
        for _ in range(8):
            status, body = await h2_request(port=server_port(server))
            assert status == 200
            bodies.append(int(body))

        assert startups[0] == main_loop_id
        assert len(set(startups)) == 4
        assert set(bodies) == set(startups)
        main_family = type(asyncio.get_running_loop()).__module__.split('.', 1)[0]
        assert {module.split('.', 1)[0] for module in loop_modules} == {main_family}

    assert set(shutdowns) == set(startups)


@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_secondary_lifespan_startups_run_concurrently() -> None:
    main_loop_id = id(asyncio.get_running_loop())
    lock = threading.Lock()
    secondary_count = 0
    secondary_start_barrier = threading.Barrier(3, timeout=1)

    async def app(scope, receive, send):
        nonlocal secondary_count
        if scope['type'] == 'lifespan':
            assert (await receive())['type'] == 'lifespan.startup'
            if id(asyncio.get_running_loop()) != main_loop_id:
                with lock:
                    secondary_count += 1
                try:
                    secondary_start_barrier.wait()
                except threading.BrokenBarrierError as exc:
                    raise AssertionError(
                        'secondary lifespan startup was serialized'
                    ) from exc
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return

        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, loop_threads=4, lifespan='on', access_log=False)
    async with running_server(app, config) as server:
        status, _ = await h2_request(port=server_port(server))
        assert status == 204

    assert secondary_count == 3


@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_secondary_lifespan_failure_rolls_back_started_loops() -> None:
    main_loop_id = id(asyncio.get_running_loop())
    lock = threading.Lock()
    startups: list[int] = []
    shutdowns: list[int] = []
    # One secondary fails only after every other secondary has published
    # startup completion, so rollback must reach every retained owner.
    success_barrier = threading.Barrier(3, timeout=2)

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        loop_id = id(asyncio.get_running_loop())
        with lock:
            startups.append(loop_id)
            startup_ordinal = len(startups)
        assert (await receive())['type'] == 'lifespan.startup'
        if loop_id != main_loop_id and startup_ordinal == 4:
            try:
                success_barrier.wait()
            except threading.BrokenBarrierError as exc:
                raise AssertionError('successful secondaries did not complete') from exc
            await send({'type': 'lifespan.startup.failed', 'message': 'secondary'})
            return
        await send({'type': 'lifespan.startup.complete'})
        if loop_id != main_loop_id:
            try:
                success_barrier.wait()
            except threading.BrokenBarrierError as exc:
                raise AssertionError('secondary startups lost a peer') from exc
        assert (await receive())['type'] == 'lifespan.shutdown'
        await send({'type': 'lifespan.shutdown.complete'})
        with lock:
            shutdowns.append(loop_id)

    server = Server(
        app,
        Config(port=0, loop_threads=4, lifespan='on', access_log=False),
    )
    with pytest.raises(RuntimeError, match='lifespan startup failed: secondary'):
        await server.serve()

    assert startups[0] == main_loop_id
    assert len(set(startups)) == 4
    # Every retained successful lifespan — primary plus secondaries that
    # crossed startup completion — receives exactly one shutdown.
    assert set(shutdowns) == set(startups[:-1])
    assert startups[-1] not in shutdowns
    assert len(shutdowns) == len(set(shutdowns))


@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_secondary_lifespan_failure_awaits_cancelled_startup_cleanup() -> None:
    main_loop_id = id(asyncio.get_running_loop())
    lock = threading.Lock()
    secondary_count = 4
    secondary_start_barrier = threading.Barrier(secondary_count, timeout=2)
    rollback_barrier = threading.Barrier(2, timeout=2)
    cancelled_cleanup_done = threading.Event()
    secondary_loop_ids: list[int] = []
    secondary_shutdowns: list[int] = []
    # Every secondary startup is awaited under this bound (no abort_all). A
    # peer that ignores cancellation until the bound is spent forces the wall
    # clock to at least this long before rollback can finish.
    startup_timeout = 0.5

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        loop_id = id(asyncio.get_running_loop())
        assert (await receive())['type'] == 'lifespan.startup'

        if loop_id == main_loop_id:
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return

        with lock:
            secondary_loop_ids.append(loop_id)
        try:
            secondary_start_barrier.wait()
        except threading.BrokenBarrierError as exc:
            raise AssertionError('secondary startups did not overlap') from exc

        # Roles by stable loop-id order so hang vs fail is not a race on
        # arrival: highest id fails, next hangs until startup cancel/timeout.
        with lock:
            ranked = sorted(set(secondary_loop_ids))
            role = ranked.index(loop_id)

        if role == secondary_count - 1:
            # Let the successful runners publish completion first so they
            # deterministically exercise transactional rollback.
            await asyncio.sleep(0.05)
            await send({'type': 'lifespan.startup.failed', 'message': 'secondary'})
            return
        if role == secondary_count - 2:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                # Cleanup is deliberately asynchronous. Successful-runner
                # shutdown must wait for it (cancelled_cleanup_done) before
                # completing; the hang itself is only released by the startup
                # bound or by discard after a peer failure's rollback path.
                await asyncio.sleep(0.05)
                cancelled_cleanup_done.set()
                raise

        await send({'type': 'lifespan.startup.complete'})
        assert (await receive())['type'] == 'lifespan.shutdown'
        assert cancelled_cleanup_done.is_set()
        try:
            rollback_barrier.wait()
        except threading.BrokenBarrierError as exc:
            raise AssertionError('successful rollbacks were serialized') from exc
        await send({'type': 'lifespan.shutdown.complete'})
        with lock:
            secondary_shutdowns.append(loop_id)

    server = Server(
        app,
        Config(
            port=0,
            loop_threads=5,
            lifespan='on',
            timeout_lifespan_startup=startup_timeout,
            access_log=False,
        ),
    )
    loop = asyncio.get_running_loop()
    started = loop.time()
    # Lowest-index error wins: depending on shard index order the hang may
    # surface as startup timeout and the explicit failure as secondary — both
    # are real startup failures under the await-all-bounded-outcomes contract.
    with pytest.raises(
        RuntimeError,
        match=r'lifespan startup (failed: secondary|timed out)',
    ):
        await server.serve()

    elapsed = loop.time() - started
    # Floor: the hanging peer is only released when its startup bound fires.
    # Ceiling: that bound, plus the deliberate 50ms cleanup sleeps and the
    # 2-party rollback barrier (ready as soon as cleanup finishes — not its
    # full timeout), with a little free-thread scheduling headroom.
    assert elapsed >= startup_timeout * 0.5
    assert elapsed < startup_timeout + 0.05 + 0.05 + 0.5
    assert cancelled_cleanup_done.is_set()
    assert len(secondary_shutdowns) == 2
    assert len(set(secondary_shutdowns)) == 2


@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_secondary_startup_retains_its_loop_until_cancelled_task_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lifespan

    main_loop_id = id(asyncio.get_running_loop())
    secondary_cleanup_started = threading.Event()
    release_secondary_cleanup = threading.Event()
    stop_entered = threading.Event()
    stop_completed = threading.Event()
    release_stop = threading.Event()
    secondary_loops: list[asyncio.AbstractEventLoop] = []
    original_stop_lifespan_runner = _lifespan.stop_lifespan_runner

    async def observe_stop_lifespan_runner(runner, *, shutdown_timeout):
        stop_entered.set()
        try:
            return await original_stop_lifespan_runner(
                runner, shutdown_timeout=shutdown_timeout
            )
        finally:
            stop_completed.set()
            await asyncio.to_thread(release_stop.wait)

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        if id(asyncio.get_running_loop()) == main_loop_id:
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return

        secondary_loops.append(asyncio.get_running_loop())
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            secondary_cleanup_started.set()
            while not release_secondary_cleanup.is_set():
                try:
                    await asyncio.to_thread(release_secondary_cleanup.wait)
                except asyncio.CancelledError:
                    task = asyncio.current_task()
                    assert task is not None
                    task.uncancel()
            raise

    monkeypatch.setattr(
        _lifespan, 'stop_lifespan_runner', observe_stop_lifespan_runner
    )
    server = Server(
        app,
        Config(
            port=0,
            loop_threads=2,
            lifespan='on',
            timeout_lifespan_startup=0.05,
            access_log=False,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await asyncio.wait_for(asyncio.to_thread(secondary_cleanup_started.wait), timeout=2)
    await asyncio.wait_for(asyncio.to_thread(stop_entered.wait), timeout=2)
    try:
        await asyncio.wait_for(asyncio.to_thread(stop_completed.wait), timeout=0.2)
    except TimeoutError:
        stop_completed_early = False
    else:
        stop_completed_early = True
    loops_remain_open = secondary_loops and all(
        not loop.is_closed() for loop in secondary_loops
    )
    release_secondary_cleanup.set()
    release_stop.set()
    with pytest.raises(RuntimeError, match='lifespan startup timed out'):
        await asyncio.wait_for(serving, timeout=2)
    assert not stop_completed_early, 'the retained task still owns its secondary loop'
    assert loops_remain_open
    assert all(loop.is_closed() for loop in secondary_loops)


@pytest.mark.skipif(
    not _gil_is_disabled(),
    reason='requires a free-threaded interpreter with the GIL disabled',
)
async def test_uvloop_secondary_factory_mismatch_fails_transactionally(
    monkeypatch,
) -> None:
    if type(asyncio.get_running_loop()).__module__ != 'uvloop':
        pytest.skip('requires the uvloop test-loop variant')

    import uvloop

    monkeypatch.setattr(uvloop, 'new_event_loop', asyncio.SelectorEventLoop)

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        await send({'type': 'lifespan.startup.complete'})
        assert (await receive())['type'] == 'lifespan.shutdown'
        await send({'type': 'lifespan.shutdown.complete'})

    server = Server(
        app,
        Config(port=0, loop_threads=2, lifespan='on', access_log=False),
    )
    with pytest.raises(
        RuntimeError,
        match='Uvloop secondary-loop factory returned Asyncio loop',
    ):
        await server.serve()

    assert server.addresses == ()


async def test_fastapi_request_headers_work_with_tuple_backed_scope_headers() -> None:
    fastapi_app = FastAPI()

    @fastapi_app.get('/header')
    async def header(request: Request) -> PlainTextResponse:
        return PlainTextResponse(request.headers['x-demo'])

    config = Config(port=0)
    async with running_server(fastapi_app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        port = server_port(server)
        status, body = await asyncio.wait_for(
            h2_request(
                port=port,
                path='/headers',
                extra_headers=[(b'x-demo', b'works')],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'127.0.0.1:{port}|works'.encode()


async def test_lifespan_startup_failure_is_reported() -> None:
    class FailingLifespan:
        async def __aenter__(self) -> None:
            raise RuntimeError('boom')

        async def __aexit__(self, *_):
            return False

    def lifespan(app: FastAPI) -> FailingLifespan:
        return FailingLifespan()

    fastapi_app = FastAPI(lifespan=lifespan)
    server = Server(fastapi_app, Config(port=0))

    with pytest.raises(RuntimeError, match='boom'):
        await server.serve()


async def test_h2_header_list_limit_returns_431() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header list limit should reject before the app runs')

    config = Config(port=0, h2_max_header_list_size=8)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                extra_headers=[(b'x-demo', b'0123456789')],
            ),
            timeout=5,
        )

    assert status == 431
    assert body == b''


async def test_h2_header_list_limit_counts_rfc_overhead() -> None:
    async def app(scope, receive, send):
        raise AssertionError('header list limit should reject before the app runs')

    config = Config(port=0, h2_max_header_list_size=90)
    extra_headers = [(f'x{i}'.encode(), b'1') for i in range(10)]
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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

    config = Config(port=0, max_request_body_size=4)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), method='POST', body=b'payload'),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
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
    chunk_seen = asyncio.Queue()

    async def app(scope, receive, send):
        body = bytearray()
        while True:
            message = await receive()
            chunk = message.get('body', b'')
            body.extend(chunk)
            if chunk:
                chunk_seen.put_nowait(len(chunk))
            if not message.get('more_body', False):
                break
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': bytes(body)})

    config = Config(port=0, lifespan='off')
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
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
                assert await asyncio.wait_for(chunk_seen.get(), timeout=1) == 1

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        reader, writer, conn, authority = await open_h2_connection(
            port=server_port(server)
        )
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body, trailers = await asyncio.wait_for(
            h2_request_details(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=server_port(server)),
            timeout=5,
        )

    assert status == 200
    assert body == payload
    assert dict(headers)[b'content-length'] == str(len(payload)).encode()


async def test_http_response_pathsend_replaces_wrong_content_length(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / 'wrong-length.bin'
    payload = b'h2-actual-file-bytes'
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
                (b'content-length', b'99999'),
            ],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=server_port(server)),
            timeout=5,
        )

    assert status == 200
    assert body == payload
    assert dict(headers)[b'content-length'] == str(len(payload)).encode()


async def test_http_response_pathsend_rejects_non_regular_files(
    tmp_path: Path,
) -> None:
    dir_path = tmp_path / 'dir'
    dir_path.mkdir()
    fifo_path = tmp_path / 'fifo'
    os.mkfifo(fifo_path)
    sock_path = tmp_path / 'sock'
    # Bind then leave the socket path in place for pathsend to open.
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(sock_path))
    device_path = Path('/dev/null')

    async def app(scope, receive, send):
        path = scope['path'].lstrip('/')
        target = {
            'dir': dir_path,
            'fifo': fifo_path,
            'sock': sock_path,
            'dev': device_path,
        }[path]
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(target)})

    config = Config(port=0)
    try:
        async with running_server(app, config) as server:
            port = server_port(server)
            for name in ('dir', 'fifo', 'sock', 'dev'):
                status, body = await asyncio.wait_for(
                    h2_request(port=port, path=f'/{name}'),
                    timeout=5,
                )
                assert status == 403, name
                assert body == b''
    finally:
        listener.close()


def _native_thread_count() -> int:
    return len(os.listdir(f'/proc/{os.getpid()}/task'))


def _threads_waiting_on_pipe() -> int:
    """Count threads blocked in the classic FIFO open/read wait paths."""
    blocked = 0
    task_root = Path(f'/proc/{os.getpid()}/task')
    for tid_dir in task_root.iterdir():
        try:
            wchan = (tid_dir / 'wchan').read_text().strip()
        except OSError:
            continue
        if wchan in {
            'pipe_wait',
            'pipe_read',
            'wait_for_partner',
            'unix_stream_data_wait',
            'fifo_open',
        }:
            blocked += 1
    return blocked


async def test_http_response_pathsend_fifo_16_returns_to_baseline(
    tmp_path: Path,
) -> None:
    """16 concurrent pathsend opens of a FIFO with no peer must 403 promptly
    without leaving blocking-pool threads stuck in the open wait.
    """
    fifo_path = tmp_path / 'flood.fifo'
    os.mkfifo(fifo_path)
    regular_path = tmp_path / 'ok.bin'
    regular_path.write_bytes(b'post-flood-ok')

    async def app(scope, receive, send):
        target = regular_path if scope['path'] == '/ok' else fifo_path
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.pathsend', 'path': str(target)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        port = server_port(server)
        # Warm the process so baseline includes runtime/worker threads.
        warm_status, _ = await asyncio.wait_for(h2_request(port=port, path='/'), timeout=5)
        assert warm_status == 403
        await asyncio.sleep(0.1)
        baseline = _native_thread_count()
        assert _threads_waiting_on_pipe() == 0

        results = await asyncio.wait_for(
            asyncio.gather(*[h2_request(port=port) for _ in range(16)]),
            timeout=5,
        )
        assert all(status == 403 and body == b'' for status, body in results)

        # Stuck opens hold a pool thread for the process lifetime. Idle
        # blocking-pool retention is expected; the load-bearing checks are
        # that no waiter remains and the pool still serves a real file.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if _threads_waiting_on_pipe() == 0:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(
                f'blocking pool threads remain waiting on the FIFO: '
                f'pipe_waiters={_threads_waiting_on_pipe()} '
                f'threads={_native_thread_count()} baseline={baseline}'
            )

        status, body = await asyncio.wait_for(
            h2_request(port=port, path='/ok'),
            timeout=5,
        )
        assert status == 200
        assert body == b'post-flood-ok'
        assert _threads_waiting_on_pipe() == 0
        # Thread count must not remain inflated by 16 stuck openers.
        assert _native_thread_count() < baseline + 16


async def test_http_response_pathsend_follows_symlink_to_regular_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / 'target.bin'
    payload = b'h2-symlink-payload'
    target.write_bytes(payload)
    link = tmp_path / 'link.bin'
    link.symlink_to(target)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'application/octet-stream')],
        })
        await send({'type': 'http.response.pathsend', 'path': str(link)})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=server_port(server)),
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert status == 200
    assert body == payload


async def test_http_response_pathsend_sendfile_tier_delivers_byte_identical(
    tmp_path: Path,
) -> None:
    """Files ≥ 1 MiB take the zero-copy sendfile tier; bytes must arrive
    identical through it.
    """
    file_path = tmp_path / 'sendfile-payload.bin'
    payload = (b'sendfile-tier-' * 120000)[: 1536 * 1024]
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=10
        )

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, headers, body = await asyncio.wait_for(
            h2_request_with_headers(port=server_port(server), method='HEAD'),
            timeout=5,
        )

    assert status == 200
    assert body == b''
    assert dict(headers)[b'content-length'] == str(len(payload)).encode()


@pytest.mark.parametrize(
    ('method', 'response_status'), [('HEAD', 200), ('GET', 204), ('GET', 304)]
)
async def test_h2_suppressed_pathsend_discards_declared_trailers(
    tmp_path: Path,
    method: str,
    response_status: int,
) -> None:
    file_path = tmp_path / 'head-h2-pathsend-trailers.txt'
    file_path.write_bytes(b'hidden')

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': response_status,
            'headers': [(b'trailer', b'x-finished')],
            'trailers': True,
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-finished', b'yes')],
        })

    async with running_server(app, Config(port=0)) as server:
        status, body, trailers = await asyncio.wait_for(
            h2_request_details(
                port=server_port(server),
                method=method,
                extra_headers=[(b'te', b'trailers')],
            ),
            timeout=5,
        )

    assert status == response_status
    assert body == b''
    assert trailers == []


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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body, trailers = await asyncio.wait_for(
            h2_request_details(
                port=server_port(server),
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

    config = Config(port=0)
    async with running_server(fastapi_app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), path='/download'),
            timeout=5,
        )

    assert status == 200
    assert body == payload


async def test_starlette_file_response_accepts_relative_pathsend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_path = tmp_path / 'relative-file-response.txt'
    payload = b'hello from relative file response'
    file_path.write_bytes(payload)
    monkeypatch.chdir(tmp_path)
    relative_path = Path(file_path.name)

    fastapi_app = FastAPI()

    @fastapi_app.get('/download')
    async def download() -> FileResponse:
        return FileResponse(relative_path)

    config = Config(port=0)
    async with running_server(fastapi_app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), path='/download'),
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

    config = Config(port=0)
    async with running_server(fastapi_app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), path='/download', method='HEAD'),
            timeout=5,
        )

    assert status == 200
    assert body == b''


async def test_h2_head_response_suppresses_app_body() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'hello'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), method='HEAD'),
            timeout=5,
        )

    assert status == 200
    assert body == b''


async def test_h2_no_body_unary_request_can_complete_inline() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'inline'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert status == 200
    assert body == b'inline'


async def test_h2_no_body_request_falls_back_after_initial_await() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await asyncio.sleep(0)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'await-before-start'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

    assert status == 200
    assert body == b'await-before-start'


async def test_h2_no_body_request_preserves_buffered_start_when_falling_back() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await asyncio.sleep(0)
        await send({'type': 'http.response.body', 'body': b'await-after-start'})

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server)), timeout=5
        )

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

    config = Config(port=0)
    async with running_server(app, config) as server:
        _reader, writer, conn, auth = await open_h2_connection(port=server_port(server))
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

    config = Config(port=0)
    async with running_server(app, config) as server:
        status, _body = await asyncio.wait_for(
            h2_request(port=server_port(server), method='GET'),
            timeout=5,
        )

    assert status == 500
