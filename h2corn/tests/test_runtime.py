import asyncio
import ctypes
import gc
import os
import re
import resource
import signal
import socket
import sys
import textwrap
import threading
import weakref
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import TypedDict

import h2.exceptions
import pytest
from h2corn import Config, Server
from h2corn import _server as server_module

from tests._support import (
    assert_serve_reusable,
    find_free_port,
    h2_request,
    http1_request,
    open_fd_count,
    running_server,
    wait_for_port,
    wait_for_server,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        sys.platform == 'win32',
        reason='POSIX worker supervisor (fork workers, signals, unix sockets)',
    ),
]


class _PrivilegeIdentity(TypedDict, total=False):
    user: int
    group: int


async def test_repeated_embedded_serve_releases_app_and_doorbell_fds() -> None:
    class App:
        async def __call__(self, scope, receive, send):
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})

    async def run_once() -> weakref.ReferenceType[App]:
        app = App()
        app_ref = weakref.ref(app)
        server = Server(
            app,
            Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
        )
        task = asyncio.create_task(server.serve())
        await wait_for_server(server, task)
        server.shutdown()
        await asyncio.wait_for(task, timeout=2)
        return app_ref

    serves = 6

    # Warm the process-global Tokio runtime before measuring per-serve state.
    warm_ref = await run_once()
    gc.collect()
    assert warm_ref() is None

    if sys.implementation.name != 'CPython':
        # A completed serve's `_RustFuture` transitively owns the shard
        # doorbell's eventfd. Without reference counting it survives until the
        # next tracing collection, so a warm-up descriptor can still be open
        # when the baseline is taken and be closed during the measured batch --
        # which reads as a *negative* leak. Burn a full batch first so both
        # sides of the comparison measure the same steady state.
        for _ in range(serves):
            await run_once()
        await asyncio.sleep(0)
        gc.collect()

    fd_baseline = open_fd_count()
    refs = [await run_once() for _ in range(serves)]
    await asyncio.sleep(0)
    gc.collect()

    assert all(ref() is None for ref in refs)
    fd_count = open_fd_count()
    if sys.implementation.name == 'CPython':
        assert fd_count == fd_baseline
    else:
        # Without reference counting the doorbell eventfds are released by
        # whichever collection happens to reach them, so the count wobbles
        # by a descriptor or two between samples rather than settling. It
        # does not grow: measured across five consecutive batches it read
        # 12, 14, 12, 11, 12.
        #
        # The property is that batches do not accumulate, so the bound is
        # one descriptor per serve -- a real leak adds exactly `serves` and
        # still fails, while the sampling wobble does not.
        assert fd_count - fd_baseline < serves


async def test_same_server_can_serve_twice_with_fresh_shutdown_state() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    for _ in range(2):
        task = asyncio.create_task(server.serve())
        await wait_for_server(server, task)
        await asyncio.to_thread(server.shutdown)
        await asyncio.wait_for(task, timeout=2)
        assert server.addresses == ()


async def test_shutdown_during_lifespan_startup_is_not_lost() -> None:
    startup_entered = asyncio.Event()
    finish_startup = asyncio.Event()

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        startup_entered.set()
        await finish_startup.wait()
        await send({'type': 'lifespan.startup.complete'})
        assert (await receive())['type'] == 'lifespan.shutdown'
        await send({'type': 'lifespan.shutdown.complete'})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    task = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    assert server.addresses
    generation = server._generation
    assert generation is not None

    await asyncio.to_thread(server.shutdown)
    finish_startup.set()

    await asyncio.wait_for(task, timeout=2)
    await asyncio.wait_for(asyncio.shield(generation.released), timeout=2)
    assert server.addresses == ()


async def test_cancelled_startup_releases_public_caller_before_lifespan_cleanup() -> (
    None
):
    startup_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    generation = 0
    active_lifespans = 0
    max_active_lifespans = 0

    async def app(scope, receive, send):
        nonlocal active_lifespans, generation, max_active_lifespans
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        generation += 1
        active_lifespans += 1
        max_active_lifespans = max(max_active_lifespans, active_lifespans)
        try:
            if generation == 1:
                startup_entered.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    cleanup_started.set()
                    while not release_cleanup.is_set():
                        try:
                            await release_cleanup.wait()
                        except asyncio.CancelledError:
                            continue
                    cleanup_finished.set()
                    raise

            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
        finally:
            active_lifespans -= 1

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    first = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    first.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, timeout=2)

    retained_generation = server._generation
    assert retained_generation is not None
    assert server.releasing is True
    assert not retained_generation.released.done()
    with pytest.raises(RuntimeError, match='still releasing a previous serve'):
        await server.serve()

    release_cleanup.set()
    await asyncio.wait_for(asyncio.shield(retained_generation.released), timeout=2)

    assert cleanup_finished.is_set()
    assert active_lifespans == 0
    assert server.addresses == ()

    await assert_serve_reusable(server)
    assert generation == 2
    assert max_active_lifespans == 1


async def test_startup_timeout_returns_before_retained_lifespan_cleanup() -> None:
    startup_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    lifespan_runs = 0

    async def app(scope, receive, _send):
        nonlocal lifespan_runs
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        lifespan_runs += 1
        if lifespan_runs > 1:
            await _send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await _send({'type': 'lifespan.shutdown.complete'})
            return
        startup_entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleanup_started.set()
            await release_cleanup.wait()
            raise

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='on',
            timeout_lifespan_startup=0.2,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)

    with pytest.raises(RuntimeError, match='lifespan startup timed out'):
        await asyncio.wait_for(asyncio.shield(serving), timeout=0.05)

    generation = server._generation
    assert generation is not None
    assert server.releasing is True
    assert not generation.released.done()

    release_cleanup.set()
    with pytest.raises(RuntimeError, match='lifespan startup timed out'):
        await asyncio.wait_for(asyncio.shield(generation.released), timeout=2)
    assert server.releasing is False
    await assert_serve_reusable(server)


async def test_shutdown_timeout_returns_before_retained_lifespan_cleanup() -> None:
    shutdown_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    lifespan_runs = 0

    async def app(scope, receive, send):
        nonlocal lifespan_runs
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
        lifespan_runs += 1
        await send({'type': 'lifespan.startup.complete'})
        assert (await receive())['type'] == 'lifespan.shutdown'
        if lifespan_runs > 1:
            await send({'type': 'lifespan.shutdown.complete'})
            return
        shutdown_entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleanup_started.set()
            await release_cleanup.wait()
            raise

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='on',
            timeout_lifespan_shutdown=0.2,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    server.shutdown()
    await asyncio.wait_for(shutdown_entered.wait(), timeout=2)
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)

    with pytest.raises(RuntimeError, match='lifespan shutdown timed out'):
        await asyncio.wait_for(asyncio.shield(serving), timeout=0.05)

    generation = server._generation
    assert generation is not None
    assert server.releasing is True
    assert not generation.released.done()

    release_cleanup.set()
    with pytest.raises(RuntimeError, match='lifespan shutdown timed out'):
        await asyncio.wait_for(asyncio.shield(generation.released), timeout=2)
    assert server.releasing is False
    await assert_serve_reusable(server)


@pytest.mark.parametrize('identity', [{'user': 1}, {'group': 1}])
async def test_embedded_serve_rejects_pidfile_across_privilege_drop(
    tmp_path: Path,
    identity: _PrivilegeIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def app(_scope, _receive, _send):
        raise AssertionError('invalid embedded configuration must not start the app')

    pid = tmp_path / 'h2corn.pid'
    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), pid=pid, access_log=False, **identity),
    )

    def reject_privilege_drop(_identity) -> None:
        raise AssertionError('invalid embedded configuration reached privilege drop')

    monkeypatch.setattr(server_module, 'drop_process_privileges', reject_privilege_drop)

    with pytest.raises(
        ValueError,
        match=r'use h2corn\.serve\(\) so the privileged supervisor owns the pidfile',
    ):
        await server.serve()
    assert not pid.exists()


async def test_cancelling_serve_waits_for_native_graceful_drain_before_reuse() -> None:
    request_started = asyncio.Event()
    release_request = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan is disabled')
        request_started.set()
        await release_request.wait()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'drained'})

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=1,
        ),
    )
    first = asyncio.create_task(server.serve())
    await wait_for_server(server, first)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    first.cancel()
    await asyncio.sleep(0.05)
    assert not first.done(), 'native serving ownership must drain before reuse'

    release_request.set()
    status, _headers, body, _trailers = await asyncio.wait_for(request, timeout=2)
    assert (status, body) == (200, b'drained')
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, timeout=2)
    assert server.addresses == ()

    await assert_serve_reusable(server)


async def test_shutdown_global_deadline_cancels_never_finishing_http1_app() -> None:
    request_started = asyncio.Event()
    request_cancelled = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan is disabled')
        request_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            request_cancelled.set()

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=0.2,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    loop = asyncio.get_running_loop()
    started = loop.time()
    server.shutdown()
    await asyncio.wait_for(serving, timeout=1)

    assert loop.time() - started < 0.8
    assert request_cancelled.is_set()
    await asyncio.gather(request, return_exceptions=True)
    assert server.addresses == ()

    await assert_serve_reusable(server)


async def test_shutdown_waits_for_cancelled_request_cleanup_before_reuse() -> None:
    request_started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_done = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan is disabled')
        request_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release_cleanup.wait()
            cleanup_done.set()
            raise

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=0.05,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    server.shutdown()
    await asyncio.wait_for(cancellation_seen.wait(), timeout=2)
    with pytest.raises(RuntimeError, match='already has an active serve'):
        await server.serve()

    # `timeout_graceful_shutdown` is a real bound: serve() stops waiting for
    # cleanup that outlives it rather than hanging on the application — and
    # says so, because an application that ignores cancellation is a bug the
    # operator needs to see.
    with pytest.raises(RuntimeError, match='requests still running'):
        await asyncio.wait_for(serving, timeout=2)
    assert not cleanup_done.is_set(), 'the app is still cleaning up'
    assert server.releasing is True

    # The generation still owns the app, its shards and its lifespan, so
    # reuse stays blocked until it has really let go.
    with pytest.raises(RuntimeError, match='still releasing a previous serve'):
        await server.serve()

    # Reuse unblocks by itself once the straggler settles; wait for generation
    # release rather than polling private drain state.
    generation = server._generation
    assert generation is not None
    release_cleanup.set()
    await asyncio.wait_for(cleanup_done.wait(), timeout=5)
    await asyncio.wait_for(asyncio.shield(generation.released), timeout=5)
    await asyncio.gather(request, return_exceptions=True)
    assert server.releasing is False

    await assert_serve_reusable(server)


async def test_lifespan_shutdown_runs_only_after_request_releases_the_app() -> None:
    """Lifespan shutdown closes what startup opened; a request may still use it.

    When cleanup outlives the graceful deadline, `serve()` stops waiting and
    `releasing` is true — but lifespan shutdown must not run until the request
    has actually released the application, and it must run exactly once after.
    """
    order: list[str] = []
    request_started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.shutdown':
                    order.append('lifespan-shutdown')
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
                await send({'type': 'lifespan.startup.complete'})
            return
        request_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release_cleanup.wait()
            order.append('request-cleanup-done')
            raise

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='on',
            timeout_graceful_shutdown=0.05,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    port = int(server.addresses[0].rsplit(':', 1)[1])
    request = asyncio.create_task(
        http1_request(
            port=port,
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=2)

    server.shutdown()
    await asyncio.wait_for(cancellation_seen.wait(), timeout=2)
    with pytest.raises(RuntimeError, match='requests still running'):
        await asyncio.wait_for(serving, timeout=2)

    assert server.releasing is True
    assert order == [], 'nothing may have shut down while the request runs'
    with pytest.raises(RuntimeError, match='still releasing a previous serve'):
        await server.serve()

    generation = server._generation
    assert generation is not None
    release_cleanup.set()
    await asyncio.wait_for(asyncio.shield(generation.released), timeout=5)
    await asyncio.gather(request, return_exceptions=True)
    assert order == ['request-cleanup-done', 'lifespan-shutdown']
    assert server.releasing is False
    await assert_serve_reusable(server)


async def test_generation_holds_through_lifespan_shutdown_so_two_cannot_overlap() -> (
    None
):
    """Ownership stays until lifespan finishes, not when native drain alone ends.

    After acceptance stops and the native server has released listeners, lifespan
    shutdown may still own application resources. Clearing `_generation` at that
    native boundary would let a second `serve()` start a second lifespan while
    the first is still shutting down.
    """
    active_lifespans = 0
    max_active_lifespans = 0
    shutdown_entered = asyncio.Event()
    release_shutdown = asyncio.Event()

    async def app(scope, receive, send):
        nonlocal active_lifespans, max_active_lifespans
        if scope['type'] != 'lifespan':
            await send({'type': 'http.response.start', 'status': 204, 'headers': []})
            await send({'type': 'http.response.body', 'body': b''})
            return
        active_lifespans += 1
        max_active_lifespans = max(max_active_lifespans, active_lifespans)
        try:
            assert (await receive())['type'] == 'lifespan.startup'
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            shutdown_entered.set()
            await release_shutdown.wait()
            await send({'type': 'lifespan.shutdown.complete'})
        finally:
            active_lifespans -= 1

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    first = asyncio.create_task(server.serve())
    await wait_for_server(server, first)
    server.shutdown()
    await asyncio.wait_for(shutdown_entered.wait(), timeout=2)

    assert server._generation is not None
    assert not first.done()
    assert active_lifespans == 1
    with pytest.raises(RuntimeError, match='active serve'):
        await server.serve()
    assert max_active_lifespans == 1
    assert active_lifespans == 1

    release_shutdown.set()
    await asyncio.wait_for(first, timeout=2)
    assert active_lifespans == 0
    assert server._generation is None

    await assert_serve_reusable(server)
    assert max_active_lifespans == 1


async def test_repeated_cancellation_cannot_interrupt_native_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lib

    native_started = asyncio.Event()
    shutdown_received = asyncio.Event()
    release_drain = asyncio.Event()

    def fake_serve_fds(
        _app,
        fds,
        _config,
        shutdown_trigger,
        _retire_trigger,
        _lifespan_handoff,
        ready_trigger,
        *_args,
        **_kwargs,
    ):
        async def run() -> None:
            ready_trigger()
            native_started.set()
            assert await shutdown_trigger == 'stop'
            shutdown_received.set()
            await release_drain.wait()

        return run()

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)

    async def app(scope, receive, send):
        raise AssertionError('the fake native server does not dispatch requests')

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    task = asyncio.create_task(server.serve())
    await asyncio.wait_for(native_started.wait(), timeout=2)

    task.cancel()
    await asyncio.wait_for(shutdown_received.wait(), timeout=2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done(), 'repeated cancellation must remain shielded by native drain'

    release_drain.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)
    assert server.addresses == ()


async def test_native_cancelled_error_propagates_without_shutdown_spin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lib

    calls = 0

    def fake_serve_fds(
        _app,
        fds,
        _config,
        shutdown_trigger,
        _retire_trigger,
        _lifespan_handoff,
        ready_trigger,
        *_args,
        **_kwargs,
    ):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise asyncio.CancelledError

        async def run() -> None:
            ready_trigger()
            assert await shutdown_trigger == 'stop'

        return run()

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)

    async def app(scope, receive, send):
        raise AssertionError('the fake native server does not dispatch requests')

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(server.serve(), timeout=2)
    assert calls == 1
    assert server.addresses == ()

    await assert_serve_reusable(server)
    assert calls == 2


async def test_synchronous_native_setup_failure_clears_state_before_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _lib

    calls = 0
    published_addresses: list[tuple[str, ...]] = []

    def fake_serve_fds(
        _app,
        fds,
        _config,
        shutdown_trigger,
        _retire_trigger,
        _lifespan_handoff,
        ready_trigger,
        *_args,
        **_kwargs,
    ):
        nonlocal calls
        calls += 1
        published_addresses.append(server.addresses)
        if calls == 1:
            raise RuntimeError('synchronous native setup failed')

        async def run() -> None:
            ready_trigger()
            assert await shutdown_trigger == 'stop'

        return run()

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)

    async def app(scope, receive, send):
        raise AssertionError('the fake native server does not dispatch requests')

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    with pytest.raises(RuntimeError, match='synchronous native setup failed'):
        await server.serve()
    assert published_addresses[0]
    assert server.addresses == ()

    await assert_serve_reusable(server)
    assert published_addresses[1]
    assert server.addresses == ()


async def test_concurrent_cross_thread_serve_has_exactly_one_winner() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    start = threading.Barrier(3)
    rejected = threading.Event()

    def run() -> str:
        start.wait()
        try:
            asyncio.run(server.serve())
        except RuntimeError:
            rejected.set()
            return 'rejected'
        return 'served'

    first = asyncio.create_task(asyncio.to_thread(run))
    second = asyncio.create_task(asyncio.to_thread(run))

    async def collect_outcomes() -> list[str]:
        return list(await asyncio.gather(first, second))

    outcomes = asyncio.create_task(collect_outcomes())
    await asyncio.to_thread(start.wait)
    try:
        await wait_for_server(server, outcomes, timeout=2)
        assert await asyncio.to_thread(rejected.wait, 2)
    finally:
        await asyncio.to_thread(server.shutdown)

    assert sorted(await asyncio.wait_for(outcomes, 2)) == [
        'rejected',
        'served',
    ]


async def test_server_rejects_concurrent_serve_calls() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    first = asyncio.create_task(server.serve())
    await wait_for_server(server, first)
    try:
        with pytest.raises(RuntimeError, match='active serve'):
            await server.serve()
    finally:
        server.shutdown()
        await asyncio.wait_for(first, timeout=2)


@pytest.mark.parametrize('with_quiesce', [True, False])
async def test_worker_fds_rejected_generation_releases_transferred_descriptors(
    with_quiesce: bool,
) -> None:
    listener_peer, listener_endpoint = socket.socketpair()
    listener_fd = listener_endpoint.detach()
    quiesce_peer, quiesce_endpoint = socket.socketpair()
    quiesce_read = quiesce_endpoint.detach()
    async def app(*_args: object) -> None:
        pass

    from h2corn._lib import prepare_tls

    config = Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')
    server = Server(app, config)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def active(_generation) -> None:
        entered.set()
        await release.wait()

    generation = server._claim_generation(active)
    await entered.wait()
    try:
        with pytest.raises(RuntimeError, match='active serve'):
            await server.serve_worker_fds(
                [listener_fd],
                quiesce_fd=quiesce_read if with_quiesce else None,
                prepared_tls=prepare_tls(config),
            )
        listener_peer.settimeout(1)
        assert listener_peer.recv(1) == b''
        if with_quiesce:
            quiesce_peer.settimeout(1)
            assert quiesce_peer.recv(1) == b''
        else:
            os.close(quiesce_read)
    finally:
        release.set()
        await asyncio.wait_for(generation.released, timeout=2)
        listener_peer.close()
        quiesce_peer.close()


async def test_worker_fds_success_releases_transferred_descriptors(monkeypatch) -> None:
    from h2corn import _lib
    from h2corn._lib import prepare_tls

    listener_peer, listener_endpoint = socket.socketpair()
    quiesce_peer, quiesce_endpoint = socket.socketpair()
    listener_fd = listener_endpoint.detach()
    quiesce_fd = quiesce_endpoint.detach()
    started = asyncio.Event()

    async def fake_serve_fds(
        _app,
        _fds,
        _config,
        shutdown,
        _retire_trigger,
        _lifespan_handoff,
        mark_started,
        _quiesce_fd,
        *,
        prepared_tls,
    ):
        assert prepared_tls is not None
        mark_started()
        started.set()
        await shutdown

    monkeypatch.setattr(_lib, 'serve_fds', fake_serve_fds)
    async def app(*_args: object) -> None:
        pass

    config = Config(lifespan='off')
    server = Server(app, config)
    try:
        serving = asyncio.create_task(
            server.serve_worker_fds(
                [listener_fd], quiesce_fd=quiesce_fd, prepared_tls=prepare_tls(config)
            )
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        server.shutdown()
        await asyncio.wait_for(serving, timeout=2)
        listener_peer.settimeout(1)
        quiesce_peer.settimeout(1)
        assert listener_peer.recv(1) == b''
        assert quiesce_peer.recv(1) == b''
    finally:
        listener_peer.close()
        quiesce_peer.close()


async def test_worker_startup_failure_releases_owned_resources_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = RuntimeError('worker startup sentinel')
    releases: list[str] = []
    quiesce_closes: list[int] = []

    class Lease:
        def __init__(self, name: str) -> None:
            self.name = name

        def release(self) -> None:
            releases.append(self.name)

    listeners = [Lease('first'), Lease('second')]
    quiesce_fd = 73

    monkeypatch.setattr(
        server_module, 'lease_owned_fds', lambda *_args, **_kwargs: listeners
    )
    monkeypatch.setattr(
        server_module.os,
        'close',
        quiesce_closes.append,
    )

    async def fail_startup(*_args, **_kwargs) -> None:
        raise sentinel

    async def app(*_args):
        raise AssertionError('startup runner is replaced')

    from h2corn._lib import prepare_tls

    config = Config(lifespan='on')
    server = Server(app, config)
    monkeypatch.setattr(server, '_run_primary_startup', fail_startup)

    with pytest.raises(RuntimeError) as error:
        await server.serve_worker_fds(
            [11, 12], quiesce_fd=quiesce_fd, prepared_tls=prepare_tls(config)
        )

    assert error.value is sentinel
    assert releases == ['second', 'first']
    assert quiesce_closes == [quiesce_fd]


async def test_lease_owned_fds_closes_partial_construction(monkeypatch) -> None:
    from h2corn import _socket

    first_peer, first_endpoint = socket.socketpair()
    second_peer, second_endpoint = socket.socketpair()
    first_fd = first_endpoint.detach()
    second_fd = second_endpoint.detach()
    original = _socket._InheritedListener
    calls = 0

    def fail_after_first(*, fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError('lease construction failed')
        return original(fd=fd)

    monkeypatch.setattr(_socket, '_InheritedListener', fail_after_first)
    try:
        with pytest.raises(RuntimeError, match='lease construction failed'):
            _socket.lease_owned_fds([first_fd, second_fd])
        first_peer.settimeout(1)
        second_peer.settimeout(1)
        assert first_peer.recv(1) == b''
        assert second_peer.recv(1) == b''
    finally:
        first_peer.close()
        second_peer.close()


async def test_lease_owned_fds_rolls_back_an_iterable_that_raises() -> None:
    from h2corn import _socket

    peers: list[socket.socket] = []
    fds: list[int] = []
    for _ in range(2):
        peer, endpoint = socket.socketpair()
        peers.append(peer)
        fds.append(endpoint.detach())

    def values():
        yield from fds
        raise RuntimeError('iteration failed')

    try:
        with pytest.raises(RuntimeError, match='iteration failed'):
            _socket.lease_owned_fds(values())
        for peer in peers:
            peer.settimeout(1)
            assert peer.recv(1) == b''
    finally:
        for peer in peers:
            peer.close()


async def test_lease_owned_fds_claims_quiesce_before_iterable_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    listener_peer, listener_endpoint = socket.socketpair()
    quiesce_peer, quiesce_endpoint = socket.socketpair()
    listener_fd = listener_endpoint.detach()
    quiesce_fd = quiesce_endpoint.detach()
    real_close = os.close
    closed: list[int] = []

    def record_close(value: int) -> None:
        if value == quiesce_fd:
            closed.append(value)
        real_close(value)

    monkeypatch.setattr(_socket.os, 'close', record_close)

    def values():
        yield listener_fd
        raise RuntimeError('iteration sentinel')

    try:
        with pytest.raises(RuntimeError, match='iteration sentinel'):
            _socket.lease_owned_fds(values(), quiesce_fd=quiesce_fd)
        listener_peer.settimeout(1)
        quiesce_peer.settimeout(1)
        assert listener_peer.recv(1) == b''
        assert quiesce_peer.recv(1) == b''
        assert closed == [quiesce_fd]
    finally:
        listener_peer.close()
        quiesce_peer.close()
        with suppress(OSError):
            real_close(listener_fd)
        with suppress(OSError):
            real_close(quiesce_fd)


async def test_lease_owned_fds_claims_quiesce_before_invalid_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    listener_peer, listener_endpoint = socket.socketpair()
    quiesce_peer, quiesce_endpoint = socket.socketpair()
    listener_fd = listener_endpoint.detach()
    quiesce_fd = quiesce_endpoint.detach()
    real_close = os.close
    closed: list[int] = []

    def record_close(value: int) -> None:
        if value == quiesce_fd:
            closed.append(value)
        real_close(value)

    monkeypatch.setattr(_socket.os, 'close', record_close)

    def values() -> Iterator[int]:
        yield listener_fd
        yield []  # type: ignore[return-value]

    try:
        with pytest.raises(TypeError, match='listener fds must be integers'):
            _socket.lease_owned_fds(values(), quiesce_fd=quiesce_fd)
        listener_peer.settimeout(1)
        quiesce_peer.settimeout(1)
        assert listener_peer.recv(1) == b''
        assert quiesce_peer.recv(1) == b''
        assert closed == [quiesce_fd]
    finally:
        listener_peer.close()
        quiesce_peer.close()
        with suppress(OSError):
            real_close(listener_fd)
        with suppress(OSError):
            real_close(quiesce_fd)


async def test_lease_owned_fds_preserves_type_error_and_closes_prior_fd() -> None:
    from h2corn import _socket

    peer, endpoint = socket.socketpair()
    fd = endpoint.detach()

    def values() -> Iterator[int]:
        yield fd
        yield []  # type: ignore[return-value]

    try:
        with pytest.raises(TypeError, match='listener fds must be integers'):
            _socket.lease_owned_fds(values())
        peer.settimeout(1)
        assert peer.recv(1) == b''
    finally:
        peer.close()
        with suppress(OSError):
            os.close(fd)


async def test_lease_owned_fds_rejects_bool_quiesce_and_closes_normalized_fd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    closed: list[int] = []
    monkeypatch.setattr(_socket.os, 'close', closed.append)

    with pytest.raises(TypeError, match=re.escape('quiesce fd must be an integer')):
        _socket.lease_owned_fds([], quiesce_fd=True)

    assert closed == [1]


async def test_lease_owned_fds_rejects_bool_listener_and_closes_normalized_fd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    closed: list[int] = []
    monkeypatch.setattr(_socket.os, 'close', closed.append)

    with pytest.raises(TypeError, match=re.escape('listener fds must be integers')):
        _socket.lease_owned_fds([False])

    assert closed == [0]


async def test_lease_owned_fds_bool_listener_aliases_prior_fd_without_double_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    closed: list[int] = []
    monkeypatch.setattr(_socket.os, 'close', closed.append)

    with pytest.raises(TypeError, match=re.escape('listener fds must be integers')):
        _socket.lease_owned_fds([0, False])

    assert closed == [0]


async def test_lease_owned_fds_bool_listener_aliases_quiesce_without_double_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    closed: list[int] = []
    monkeypatch.setattr(_socket.os, 'close', closed.append)

    with pytest.raises(TypeError, match=re.escape('listener fds must be integers')):
        _socket.lease_owned_fds([True], quiesce_fd=1)

    assert closed == [1]


async def test_lease_owned_fds_closes_prior_listener_before_bool_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    peer, endpoint = socket.socketpair()
    listener_fd = endpoint.detach()
    real_close = os.close
    closed: list[int] = []

    def record_close(value: int) -> None:
        if value == listener_fd:
            closed.append(value)
            real_close(value)
        elif value == 0:
            closed.append(value)
        else:
            real_close(value)

    monkeypatch.setattr(_socket.os, 'close', record_close)
    try:
        with pytest.raises(TypeError, match=re.escape('listener fds must be integers')):
            _socket.lease_owned_fds([listener_fd, False])
        peer.settimeout(1)
        assert peer.recv(1) == b''
        assert closed == [listener_fd, 0]
    finally:
        peer.close()
        with suppress(OSError):
            real_close(listener_fd)


async def test_lease_owned_fds_rejects_duplicate_listener_without_double_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    peer, endpoint = socket.socketpair()
    fd = endpoint.detach()
    real_close = os.close
    closed: list[int] = []

    def record_close(value: int) -> None:
        if value == fd:
            closed.append(value)
        real_close(value)

    monkeypatch.setattr(_socket.os, 'close', record_close)
    try:
        with pytest.raises(ValueError, match='unique'):
            _socket.lease_owned_fds([fd, fd])
        peer.settimeout(1)
        assert peer.recv(1) == b''
        assert closed == [fd]
    finally:
        peer.close()
        with suppress(OSError):
            real_close(fd)


async def test_lease_owned_fds_rejects_listener_quiesce_duplicate_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    peer, endpoint = socket.socketpair()
    fd = endpoint.detach()
    real_close = os.close
    closed: list[int] = []

    def record_close(value: int) -> None:
        if value == fd:
            closed.append(value)
        real_close(value)

    monkeypatch.setattr(_socket.os, 'close', record_close)
    try:
        with pytest.raises(ValueError, match='unique'):
            _socket.lease_owned_fds([fd], quiesce_fd=fd)
        peer.settimeout(1)
        assert peer.recv(1) == b''
        assert closed == [fd]
    finally:
        peer.close()
        with suppress(OSError):
            real_close(fd)


async def test_worker_fd_construction_failure_closes_listeners_and_quiesce(
    monkeypatch,
) -> None:
    from h2corn import _socket

    listener_peers = []
    listeners = []
    for _ in range(2):
        peer, endpoint = socket.socketpair()
        listener_peers.append(peer)
        listeners.append(endpoint.detach())
    quiesce_peer, quiesce_fd = socket.socketpair()
    quiesce_peer.settimeout(1)
    original = _socket._InheritedListener
    calls = 0

    def fail_after_first(*, fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError('lease construction failed')
        return original(fd=fd)

    monkeypatch.setattr(_socket, '_InheritedListener', fail_after_first)
    async def app(*_args: object) -> None:
        pass

    from h2corn._lib import prepare_tls

    config = Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off')
    server = Server(app, config)
    try:
        with pytest.raises(RuntimeError, match='lease construction failed'):
            await server.serve_worker_fds(
                listeners,
                quiesce_fd=quiesce_fd.detach(),
                prepared_tls=prepare_tls(config),
            )
        for peer in listener_peers:
            peer.settimeout(1)
            assert peer.recv(1) == b''
        assert quiesce_peer.recv(1) == b''
    finally:
        for peer in listener_peers:
            peer.close()
        quiesce_peer.close()


async def test_serve_fds_count_mismatch_leaves_caller_handles_open() -> None:
    from h2corn._lib import prepare_tls, serve_fds

    async def app(scope, receive, send):
        raise AssertionError('listener adoption must fail before app dispatch')

    async def attempt(raw_fds: list[int]) -> None:
        async def wait_for_shutdown() -> str:
            await asyncio.sleep(60)
            return 'stop'

        shutdown = asyncio.create_task(wait_for_shutdown())
        config = Config(
            bind=('127.0.0.1:1', '127.0.0.2:1'),
            access_log=False,
            lifespan='off',
        )
        quiesce_read_fd, quiesce_write_fd = os.pipe()
        try:
            with pytest.raises((OSError, RuntimeError)):
                await serve_fds(
                    app,
                    raw_fds,
                    config,
                    shutdown,
                    None,
                    None,
                    None,
                    quiesce_read_fd,
                    prepared_tls=prepare_tls(config),
                )
        finally:
            os.close(quiesce_write_fd)
            shutdown.cancel()
            with pytest.raises(asyncio.CancelledError):
                await shutdown
        for fd in raw_fds:
            os.fstat(fd)
        os.fstat(quiesce_read_fd)
        for fd in raw_fds:
            os.close(fd)
        os.close(quiesce_read_fd)

    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.setblocking(False)
    await attempt([listener.detach()])


@pytest.mark.parametrize(
    ('listener_fds', 'quiesce_fd'),
    [
        ([-1], None),
        pytest.param(
            [2**40],
            None,
            marks=pytest.mark.skipif(
                sys.platform == 'win32' and ctypes.sizeof(ctypes.c_void_p) == 8,
                reason='2**40 is within the 64-bit Windows handle range',
            ),
        ),
        ([7, 7], None),
        ([7], 7),
    ],
)
async def test_serve_fds_rejects_unsafe_descriptor_ownership(
    listener_fds: list[int],
    quiesce_fd: int | None,
) -> None:
    from h2corn._lib import prepare_tls, serve_fds

    async def app(scope, receive, send):
        raise AssertionError('descriptor validation precedes app dispatch')

    shutdown = asyncio.get_running_loop().create_future()
    config = Config(
        bind=tuple(f'fd://{index}' for index in range(len(listener_fds))),
        lifespan='off',
    )
    with pytest.raises(ValueError):
        serve_fds(
            app,
            listener_fds,
            config,
            shutdown,
            None,
            None,
            None,
            quiesce_fd,
            prepared_tls=prepare_tls(config),
        )


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows ingress regression')
async def test_serve_fds_negative_handle_is_value_error() -> None:
    from h2corn._lib import prepare_tls, serve_fds

    async def app(*_args: object) -> None:
        pass

    config = Config(bind=('fd://0',), lifespan='off')
    with pytest.raises(ValueError):
        serve_fds(
            app,
            [-1],
            config,
            asyncio.get_running_loop().create_future(),
            prepared_tls=prepare_tls(config),
        )


@pytest.mark.parametrize('closed_source', ['listener', 'quiesce'])
async def test_serve_fds_closed_positive_sources_raise_oserror(
    closed_source: str,
) -> None:
    from h2corn._lib import prepare_tls, serve_fds

    async def app(*_args: object) -> None:
        pass

    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.setblocking(False)
    listener_fd = listener.detach()
    read_fd, write_fd = os.pipe()
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit == resource.RLIM_INFINITY or soft_limit > 2**31 - 1:
        os.close(listener_fd)
        os.close(read_fd)
        os.close(write_fd)
        pytest.skip('RLIMIT_NOFILE is infinite or outside the native fd range')
    invalid_fd = soft_limit
    if closed_source == 'quiesce':
        os.close(read_fd)
        quiesce_fd = invalid_fd
    else:
        with suppress(OSError):
            os.close(write_fd)
        os.close(read_fd)
        quiesce_fd = None
    config = Config(bind=('fd://0',), lifespan='off')
    try:
        with pytest.raises(OSError):
            serve_fds(
                app,
                [invalid_fd] if closed_source == 'listener' else [listener_fd],
                config,
                asyncio.get_running_loop().create_future(),
                quiesce_fd=quiesce_fd,
                prepared_tls=prepare_tls(config),
            )
        if closed_source == 'quiesce':
            os.fstat(listener_fd)
    finally:
        os.close(listener_fd)
        with suppress(OSError):
            os.close(read_fd)
        with suppress(OSError):
            os.close(write_fd)


async def test_serve_fds_returns_awaitable_and_duplicates_sources() -> None:
    from h2corn._lib import prepare_tls, serve_fds

    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.setblocking(False)
    address = listener.getsockname()
    listener_fd = listener.detach()
    quiesce_read, quiesce_write = os.pipe()
    shutdown = asyncio.get_running_loop().create_future()
    config = Config(bind=('fd://0',), lifespan='off')

    async def app(scope, receive, send):
        if scope['type'] == 'http':
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'fd-duplicate'})

    try:
        result = serve_fds(
            app,
            [listener_fd],
            config,
            shutdown,
            quiesce_fd=quiesce_read,
            prepared_tls=prepare_tls(config),
        )
        os.fstat(listener_fd)
        os.fstat(quiesce_read)
        assert hasattr(result, '__await__')
        os.close(listener_fd)
        os.close(quiesce_read)
        await wait_for_port(address[1])
        status, _, body, _ = await http1_request(
            port=address[1],
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
        )
        assert status == 200
        assert body == b'fd-duplicate'
        os.write(quiesce_write, b'S')
        await asyncio.wait_for(result, timeout=2)
    finally:
        os.close(quiesce_write)


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    async def _wait(timeout: float) -> bool:
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except TimeoutError:
            return process.returncode is not None
        return True

    def _signal(sig: int) -> None:
        if sys.platform != 'win32':
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                return
            except OSError:
                pass
            else:
                return
        process.send_signal(sig)

    if process.returncode is not None:
        await _wait(5)
        return

    _signal(signal.SIGTERM)
    if await _wait(5):
        return

    _signal(signal.SIGKILL)
    await _wait(5)


async def _wait_for_h2_success(
    *,
    port: int,
    body: bytes,
    timeout: float = 5.0,
) -> None:
    await _wait_for_h2_body(port=port, body=body, timeout=timeout)


async def _wait_for_h2_body(
    *,
    port: int,
    body: bytes,
    timeout: float = 5.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            status, response_body = await h2_request(port=port)
        except Exception:
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.05)
            continue
        if status == 200 and response_body == body:
            return
        if loop.time() >= deadline:
            raise AssertionError(
                f'timed out waiting for body {body!r}, got status={status} body={response_body!r}'
            )
        await asyncio.sleep(0.05)


async def _wait_for_h2_body_any(
    *, port: int, timeout: float = 5.0
) -> tuple[int, bytes]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            return await h2_request(port=port)
        except Exception:
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.05)


async def _wait_for_listening_port(
    process: asyncio.subprocess.Process,
    *,
    timeout: float = 5.0,
) -> int:
    assert process.stderr is not None
    stderr = process.stderr

    async def _read_port() -> int:
        while True:
            line = await stderr.readline()
            if not line:
                raise AssertionError('server exited before printing listening banner')
            match = re.search(rb'Listening on http://127\.0\.0\.1:(\d+)', line)
            if match is not None:
                return int(match.group(1))

    return await asyncio.wait_for(_read_port(), timeout=timeout)


async def _collect_lines(
    stream: asyncio.StreamReader | None,
    lines: list[bytes],
) -> None:
    if stream is None:
        return
    while line := await stream.readline():
        lines.append(line)


async def _spawn_server_process(
    *,
    tmp_path: Path,
    module_name: str,
    module_source: str,
    workers: int,
    port: int | None = None,
    extra_args: list[str] | None = None,
    stderr=None,
) -> tuple[asyncio.subprocess.Process, int]:
    port = find_free_port() if port is None else port
    module_path = tmp_path / f'{module_name}.py'
    module_path.write_text(textwrap.dedent(module_source).strip() + '\n')

    env = os.environ.copy()
    pythonpath = env.get('PYTHONPATH')
    env['PYTHONPATH'] = f'{tmp_path}:{pythonpath}' if pythonpath else str(tmp_path)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'h2corn._server',
        f'{module_name}:app',
        '--workers',
        str(workers),
        '--port',
        str(port),
        *(extra_args or []),
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL if stderr is None else stderr,
        start_new_session=sys.platform != 'win32',
    )
    return process, port


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux prctl parent-death signal')
async def test_parent_death_signal_allows_pid_one_supervisor(monkeypatch) -> None:
    from h2corn import _supervisor

    class FakeLibc:
        def prctl(self, *_args):
            return 0

    monkeypatch.setattr(
        ctypes,
        'CDLL',
        lambda *_args, **_kwargs: FakeLibc(),
    )
    monkeypatch.setattr(_supervisor.os, 'getppid', lambda: 1)

    def fail_exit(code: int):
        raise AssertionError(f'unexpected os._exit({code})')

    monkeypatch.setattr(_supervisor.os, '_exit', fail_exit)

    _supervisor._install_parent_death_signal(1)


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux prctl parent-death signal')
async def test_parent_death_signal_exits_when_expected_parent_died_before_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    class FakeLibc:
        def prctl(self, *_args):
            return 0

    exit_codes: list[int] = []

    def fake_exit(code: int):
        exit_codes.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(
        ctypes,
        'CDLL',
        lambda *_args, **_kwargs: FakeLibc(),
    )
    monkeypatch.setattr(_supervisor.os, 'getppid', lambda: 1)
    monkeypatch.setattr(_supervisor.os, '_exit', fake_exit)

    with pytest.raises(SystemExit):
        _supervisor._install_parent_death_signal(1234)

    assert exit_codes == [0]


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux prctl parent-death signal')
async def test_parent_death_signal_fails_closed_when_prctl_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _supervisor

    class FakeLibc:
        def prctl(self, *_args):
            return -1

    monkeypatch.setattr(
        ctypes,
        'CDLL',
        lambda *_args, **_kwargs: FakeLibc(),
    )
    monkeypatch.setattr(ctypes, 'get_errno', lambda: 1)
    monkeypatch.setattr(_supervisor.os, 'getppid', lambda: 1234)

    with pytest.raises(OSError, match='failed to install parent-death signal'):
        _supervisor._install_parent_death_signal(1234)


async def _wait_for_pid_change(
    *, port: int, previous_pid: bytes, timeout: float = 5.0
) -> bytes:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=1)
        except Exception:
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.05)
            continue
        assert status == 200
        if body != previous_pid:
            return body
        if loop.time() >= deadline:
            raise AssertionError(f'worker pid did not change from {previous_pid!r}')
        await asyncio.sleep(0.05)


async def _wait_for_worker_count(
    *, supervisor_pid: int, count: int, timeout: float = 5.0
) -> list[int]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        workers = _worker_pids(supervisor_pid)
        if len(workers) == count:
            return workers
        if loop.time() >= deadline:
            raise AssertionError(
                f'timed out waiting for {count} workers; found {workers}'
            )
        await asyncio.sleep(0.02)


async def _wait_for_path(path: Path, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not path.exists():
        if loop.time() >= deadline:
            raise AssertionError(f'timed out waiting for {path}')
        await asyncio.sleep(0.01)


async def test_unix_socket_serving(unix_socket_dir: Path) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'uds'})

    socket_path = unix_socket_dir / 'h2corn.sock'
    config = Config(bind=(f'unix:{socket_path}',))
    async with running_server(app, config):
        status, body = await asyncio.wait_for(h2_request(uds=socket_path), timeout=5)

    assert status == 200
    assert body == b'uds'


async def test_access_log_regular_file_sink_flushes_every_line_on_graceful_shutdown(
    tmp_path: Path,
) -> None:
    """When stderr is a regular file the batched sink must retain every line.

    A burst larger than one sink buffer (32 KiB) forces at least one full
    batch plus a trailing partial batch; graceful shutdown has to drain both
    or the last requests vanish from the log.
    """
    # ~64-byte access-log lines; 800 requests ≈ 50 KiB > INITIAL_CAPACITY (32 KiB).
    request_count = 800
    log_path = tmp_path / 'access.log'
    marker = '/access-log-marker'

    module_source = """
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})
    """

    with log_path.open('wb') as log_file:
        process, port = await _spawn_server_process(
            tmp_path=tmp_path,
            module_name='access_log_burst_app',
            module_source=module_source,
            workers=1,
            extra_args=['--access-log', '--lifespan', 'off'],
            stderr=log_file,
        )
        try:
            await wait_for_port(port, timeout=10)
            # Concurrent burst so the sink writer is still busy when later
            # lines arrive and coalesce into additional batches.
            results = await asyncio.gather(*[
                h2_request(port=port, path=marker) for _ in range(request_count)
            ])
            assert all(status == 204 and body == b'' for status, body in results)
            # Let every completed request emit into the sink before SIGTERM.
            await asyncio.sleep(0.1)
        finally:
            await _terminate_process(process)

    # Re-open after the process closes stderr so the file view is complete.
    text = log_path.read_text(errors='replace')
    assert len(text) > 32 * 1024, (
        f'burst must exceed one sink batch; log is only {len(text)} bytes'
    )
    # Access-log lines look like: client "GET /access-log-marker HTTP/2" 204 ...
    # Status may be plain or ANSI-styled; match the marker path and 204 digits.
    line_re = re.compile(re.escape(marker) + r'.*\b204\b')
    logged = [line for line in text.splitlines() if line_re.search(line)]
    assert len(logged) == request_count, (
        f'expected {request_count} access-log lines, found {len(logged)}; '
        f'file has {len(text)} bytes and {text.count(chr(10))} newlines'
    )
    # The last batch is the whole point: the final request must be present,
    # not only early lines that flushed while the sink was still draining.
    assert marker in logged[-1]


async def test_unix_socket_cleanup_removes_owned_socket_path(
    unix_socket_dir: Path,
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    socket_path = unix_socket_dir / 'cleanup.sock'
    config = Config(bind=(f'unix:{socket_path}',))
    async with running_server(app, config):
        assert socket_path.exists()

    assert not socket_path.exists()


async def test_unix_socket_umask_limits_created_mode(
    unix_socket_dir: Path,
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    socket_path = unix_socket_dir / 'umask.sock'
    config = Config(bind=(f'unix:{socket_path}',), umask=0o077)
    async with running_server(app, config):
        assert socket_path.stat().st_mode & 0o077 == 0

    assert not socket_path.exists()


async def test_unix_socket_path_rejects_non_socket_files(tmp_path: Path) -> None:
    path = tmp_path / 'not-a-socket'
    path.write_text('occupied')

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(app, Config(bind=(f'unix:{path}',)))

    with pytest.raises(OSError, match='not a socket'):
        await server.serve()

    assert path.read_text() == 'occupied'


async def test_multi_bind_reports_actual_server_port_per_listener() -> None:
    async def app(scope, receive, send):
        scope_port = scope['server'][1]
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': str(scope_port).encode()})

    # Two listeners on one host need distinct ports; multiple port-0 binds
    # deliberately share one ephemeral port (for 0.0.0.0 + [::] pairs), so
    # this is one of the few in-process cases that must pre-allocate.
    ports = (find_free_port(), find_free_port())
    config = Config(bind=tuple(f'127.0.0.1:{port}' for port in ports))
    async with running_server(app, config):
        for port in ports:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
            assert status == 200
            assert body == str(port).encode()


async def test_server_serve_writes_and_cleans_up_pid_file(tmp_path: Path) -> None:
    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': b'pid'})

    pid_path = tmp_path / 'h2corn.pid'
    config = Config(pid=pid_path, port=find_free_port())
    async with running_server(app, config):
        assert pid_path.read_text() == f'{os.getpid()}\n'

    assert not pid_path.exists()


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_serves_requests(tmp_path: Path, workers: int) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='supervisor_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'supervisor'})
        """,
        workers=workers,
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'supervisor'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_serves_requests_with_current_user_and_group(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='supervisor_identity_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'supervisor-identity'})
        """,
        workers=1,
        extra_args=['--user', str(os.getuid()), '--group', str(os.getgid())],
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'supervisor-identity'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_banner_reports_kernel_allocated_port(
    tmp_path: Path,
) -> None:
    process, configured_port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='banner_port_zero_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'banner-port-zero'})
        """,
        workers=1,
        port=0,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        bound_port = await _wait_for_listening_port(process)
        assert configured_port == 0
        assert bound_port != 0
        await wait_for_port(bound_port)
        status, body = await asyncio.wait_for(h2_request(port=bound_port), timeout=5)
        assert status == 200
        assert body == b'banner-port-zero'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_worker_supervisor_writes_and_cleans_up_pid_file(
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / 'h2corn.pid'
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='pidfile_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'pidfile'})
        """,
        workers=1,
        extra_args=['--pid', str(pid_path)],
    )

    try:
        await wait_for_port(port)
        assert pid_path.read_text() == f'{process.pid}\n'
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'pidfile'
    finally:
        await _terminate_process(process)

    assert not pid_path.exists()


async def test_worker_supervisor_shutdown_is_not_blocked_by_limit_connections(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='limit_connections_shutdown_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'limit-connections'})
        """,
        workers=1,
        extra_args=['--limit-connections', '1'],
    )

    reader = writer = None
    try:
        await wait_for_port(port)
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        await asyncio.sleep(0.1)
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=5)
        assert process.returncode is not None
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        elif reader is not None:
            reader.feed_eof()
        if process.returncode is None:
            await _terminate_process(process)


@pytest.mark.skipif(sys.platform != 'linux', reason='worker-set inspection uses /proc')
async def test_supervisor_signal_worker_transitions(tmp_path: Path) -> None:
    """Signals are observable worker-set changes, not merely live requests."""
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='signal_app',
        module_source="""
        import os

        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        """,
        workers=2,
    )

    try:
        await wait_for_port(port)
        initial = set(
            await _wait_for_worker_count(
                supervisor_pid=process.pid,
                count=2,
            )
        )
        process.send_signal(signal.SIGTTOU)
        [after_down] = await _wait_for_worker_count(
            supervisor_pid=process.pid,
            count=1,
        )
        assert after_down in initial
        process.send_signal(signal.SIGTTOU)
        # The second scale-down is a deliberate no-op, so the set remains
        # singleton while the server still handles an actual request.
        assert await _wait_for_worker_count(
            supervisor_pid=process.pid,
            count=1,
        ) == [after_down]
        assert (await h2_request(port=port))[0] == 200

        process.send_signal(signal.SIGTTIN)
        after_up = set(
            await _wait_for_worker_count(
                supervisor_pid=process.pid,
                count=2,
            )
        )
        assert after_down in after_up
        assert after_up - {after_down}

        process.send_signal(signal.SIGHUP)
        deadline = asyncio.get_running_loop().time() + 10
        while True:
            reloaded = set(_worker_pids(process.pid))
            if len(reloaded) == 2 and reloaded.isdisjoint(after_up):
                break
            assert asyncio.get_running_loop().time() < deadline, (
                f'SIGHUP did not replace {after_up}; current workers are {reloaded}'
            )
            await asyncio.sleep(0.02)
        assert (await h2_request(port=port))[0] == 200
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_max_requests_jitter_applied_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a positive retirement budget draws jitter, and exactly once."""
    from h2corn import _server, _supervisor
    from h2corn._lib import prepare_tls

    class FakeWorker:
        next_pid = 10_000

        def __init__(self) -> None:
            self.pid = FakeWorker.next_pid
            FakeWorker.next_pid += 1
            self._sentinel, self._sentinel_write = os.pipe()

        @property
        def sentinel(self) -> int:
            return self._sentinel

        def start(self) -> None:
            pass

        def is_alive(self) -> bool:
            return True

        def terminate(self) -> None:
            pass

        def kill(self) -> None:
            pass

        def join(self, _timeout: float | None = None) -> None:
            pass

        def close(self) -> None:
            for fd in (self._sentinel, self._sentinel_write):
                with suppress(OSError):
                    os.close(fd)

    captured_configs: list[Config] = []

    class FakeContext:
        def Process(self, *, kwargs, **_ignored):  # noqa: N802
            captured_configs.append(kwargs['config'])
            return FakeWorker()

    from h2corn import _log

    monkeypatch.setattr(
        _supervisor.multiprocessing, 'get_context', lambda _name: FakeContext()
    )
    monkeypatch.setattr(_log.Event, 'log', lambda *_args, **_fields: None)

    async def app(*_args: object) -> None:
        pass

    def spawn_with(config: Config, randint) -> int:
        monkeypatch.setattr(_supervisor.random, 'randint', randint)
        supervisor = _supervisor._Supervisor(
            app=app,
            config=config,
            fds=(),
            identity=_server.ProcessIdentity(),
            prepared_tls=prepare_tls(config),
        )
        try:
            return supervisor.spawn_worker()
        finally:
            for worker in supervisor.workers.values():
                with suppress(OSError):
                    os.close(worker.control_read_fd)
                if worker.quiesce_write_fd is not None:
                    with suppress(OSError):
                        os.close(worker.quiesce_write_fd)
                worker.process.close()
            supervisor.selector.close()

    draws: list[tuple[int, int]] = []
    assert (
        spawn_with(
            Config(workers=1, max_requests=11, max_requests_jitter=7),
            lambda low, high: draws.append((low, high)) or 7,
        )
        != -1
    )
    assert captured_configs[-1].max_requests == 18
    assert draws == [(0, 7)]

    def rng_must_not_run(*_args: object) -> int:
        raise AssertionError('zero budget or zero jitter must not draw randomness')

    assert spawn_with(Config(workers=1, max_requests=0), rng_must_not_run) != -1
    assert captured_configs[-1].max_requests == 0
    # A nonzero jitter without a budget is rejected at configuration ingress,
    # so no worker path can draw randomness for that illegal state.
    with pytest.raises(ValueError, match='jitter requires max_requests'):
        Config(workers=1, max_requests=0, max_requests_jitter=7)
    assert (
        spawn_with(
            Config(workers=1, max_requests=11, max_requests_jitter=0), rng_must_not_run
        )
        != -1
    )
    assert captured_configs[-1].max_requests == 11


async def test_rolling_reload_waits_for_replacement_readiness(tmp_path: Path) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='readiness_gated_reload_app',
        module_source="""
        import asyncio
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                while True:
                    message = await receive()
                    if message['type'] == 'lifespan.startup':
                        await asyncio.sleep(0.8)
                        await send({'type': 'lifespan.startup.complete'})
                    elif message['type'] == 'lifespan.shutdown':
                        await send({'type': 'lifespan.shutdown.complete'})
                        return
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        """,
        workers=1,
    )

    try:
        old_pid = (await _wait_for_h2_body_any(port=port, timeout=5))[1]
        supervisor_fd_count = (
            len(os.listdir(f'/proc/{process.pid}/fd'))
            if sys.platform == 'linux'
            else None
        )
        process.send_signal(signal.SIGHUP)

        # The replacement deliberately remains in lifespan startup. Every
        # request must continue reaching the old worker throughout that gap.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.6
        while loop.time() < deadline:
            status, body = await asyncio.wait_for(h2_request(port=port), timeout=0.3)
            assert (status, body) == (200, old_pid)
            await asyncio.sleep(0.03)

        new_pid = await _wait_for_pid_change(
            port=port,
            previous_pid=old_pid,
            timeout=5,
        )
        assert new_pid != old_pid
        if supervisor_fd_count is not None:
            assert await _wait_for_worker_count(
                supervisor_pid=process.pid,
                count=1,
            ) == [int(new_pid)]
            assert len(os.listdir(f'/proc/{process.pid}/fd')) == supervisor_fd_count
        assert process.returncode is None
    finally:
        await _terminate_process(process)


@pytest.mark.skipif(sys.platform != 'linux', reason='worker count uses /proc')
async def test_scale_down_during_reload_keeps_unready_replacement(
    tmp_path: Path,
) -> None:
    delay_replacements = tmp_path / 'delay-replacements'
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='scale_down_during_reload_app',
        module_source=f"""
        import asyncio
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                while True:
                    message = await receive()
                    if message['type'] == 'lifespan.startup':
                        if os.path.exists({os.fspath(delay_replacements)!r}):
                            await asyncio.sleep(1)
                        await send({{'type': 'lifespan.startup.complete'}})
                    elif message['type'] == 'lifespan.shutdown':
                        await send({{'type': 'lifespan.shutdown.complete'}})
                        return
            await send({{'type': 'http.response.start', 'status': 200, 'headers': []}})
            await send({{'type': 'http.response.body', 'body': str(os.getpid()).encode()}})
        """,
        workers=2,
    )

    try:
        await _wait_for_h2_body_any(port=port, timeout=5)
        old_workers = set(
            await _wait_for_worker_count(
                supervisor_pid=process.pid,
                count=2,
            )
        )
        delay_replacements.write_text('delay\n')
        process.send_signal(signal.SIGHUP)

        # The third child is the replacement, held in lifespan startup for one
        # second. Scale-down must retire another old worker, never this child.
        await _wait_for_worker_count(supervisor_pid=process.pid, count=3)
        process.send_signal(signal.SIGTTOU)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.5
        served = 0
        while loop.time() < deadline:
            try:
                status, body = await asyncio.wait_for(
                    h2_request(port=port), timeout=0.3
                )
            except (h2.exceptions.ProtocolError, OSError, TimeoutError):
                # A connection opened into the scale-down window can receive
                # the retiring worker's GOAWAY before its request is answered
                # (the h2 client then refuses to send on a closed connection),
                # or be refused outright. That is what a graceful retirement
                # looks like from outside, and a real client retries. The claim
                # under test is about *which* worker answers, so only answered
                # requests count — and at least one must be.
                continue
            served += 1
            assert status == 200
            assert int(body) in old_workers
        assert served, 'no request was answered during scale-down'

        [final_worker] = await _wait_for_worker_count(
            supervisor_pid=process.pid,
            count=1,
            timeout=6,
        )
        assert final_worker not in old_workers
        status, body = await _wait_for_h2_body_any(port=port, timeout=5)
        assert (status, int(body)) == (200, final_worker)
        assert process.returncode is None
    finally:
        await _terminate_process(process)


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_exits_on_worker_crash_loop(
    tmp_path: Path,
    workers: int,
) -> None:
    process, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='crashloop_app',
        module_source="""
        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.failed', 'message': 'boom'})
                    return
        """,
        workers=workers,
    )

    try:
        exit_code = await asyncio.wait_for(process.wait(), timeout=10)
    finally:
        await _terminate_process(process)

    assert exit_code != 0


async def test_worker_supervisor_exits_on_startup_watchdog_crash_loop(
    tmp_path: Path,
) -> None:
    stderr_lines: list[bytes] = []
    process, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='watchdog_crashloop_app',
        module_source="""
        import time

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                time.sleep(60)
        """,
        workers=1,
        extra_args=[
            '--timeout-worker-healthcheck',
            '0.15',
            '--timeout-graceful-shutdown',
            '0.15',
            '--timeout-lifespan-startup',
            '30',
        ],
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        exit_code = await asyncio.wait_for(process.wait(), timeout=10)
    finally:
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert exit_code != 0
    assert stderr.count('failed healthcheck and will be replaced') >= 3
    assert 'Stopped: 3 workers exited without ever becoming ready' in stderr
    assert 'The worker error is logged above.' in stderr


@pytest.mark.parametrize('workers', [1, 2])
async def test_worker_supervisor_exits_on_unexpected_clean_worker_exit(
    tmp_path: Path,
    workers: int,
) -> None:
    stderr_lines: list[bytes] = []
    process, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='clean_exit_app',
        module_source="""
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                os._exit(0)
        """,
        workers=workers,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        exit_code = await asyncio.wait_for(process.wait(), timeout=10)
    finally:
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert exit_code != 0
    assert 'exited unexpectedly with code 0' in stderr
    assert 'Stopped: 3 workers exited without ever becoming ready' in stderr
    assert 'last exit code 0' in stderr


async def test_worker_supervisor_recycles_workers_after_max_requests(
    tmp_path: Path,
) -> None:
    stderr_lines: list[bytes] = []
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='max_requests_app',
        module_source="""
        import os

        async def app(scope, receive, send):
            if scope['type'] == 'http':
                await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        """,
        workers=1,
        extra_args=['--max-requests', '1', '--max-requests-jitter', '0'],
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        next_pid = await _wait_for_pid_change(port=port, previous_pid=body)
        assert next_pid != body
        assert process.returncode is None
    finally:
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert 'exited unexpectedly with code 0' not in stderr
    assert 'Stopped worker' in stderr


@pytest.mark.skipif(sys.platform != 'linux', reason='inspects Linux procfs children')
async def test_worker_supervisor_replaces_blocked_worker_after_request_cleanup_deadline(
    tmp_path: Path,
) -> None:
    """A wedged worker gets request cleanup before the supervisor kills it."""
    blocked_path = tmp_path / 'blocked-worker'
    stderr_lines: list[bytes] = []
    healthcheck_failed = asyncio.Event()
    # The assertions below are proportional to this, not absolute: the worker
    # must be alive at `graceful_timeout + 0.5` and gone after the second grace
    # at `2 * graceful_timeout`, leaving `graceful_timeout - 0.5` of margin. At
    # 8.0 this test alone was 19 s and set the floor for the whole parallel
    # suite. Do not drop below ~3.0 -- the margin shrinks with it.
    graceful_timeout = 4.0

    async def collect_stderr(stream: asyncio.StreamReader | None) -> None:
        if stream is None:
            return
        while line := await stream.readline():
            stderr_lines.append(line)
            if b'failed healthcheck and will be replaced' in line:
                healthcheck_failed.set()

    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='watchdog_overlap_app',
        module_source=f"""
        import os
        from pathlib import Path
        import time

        async def app(scope, receive, send):
            if scope['type'] != 'http':
                return
            if scope['path'] == '/block':
                Path({os.fspath(blocked_path)!r}).write_text(str(os.getpid()))
                time.sleep(60)
                await send({{'type': 'http.response.start', 'status': 200, 'headers': []}})
                await send({{'type': 'http.response.body', 'body': b'blocked'}})
                return
            await send({{'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]}})
            await send({{'type': 'http.response.body', 'body': str(os.getpid()).encode()}})
        """,
        workers=1,
        extra_args=[
            '--timeout-worker-healthcheck',
            '3',
            '--timeout-graceful-shutdown',
            str(graceful_timeout),
        ],
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(collect_stderr(process.stderr))
    blocking: asyncio.Task[tuple[int, bytes]] | None = None

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200

        blocking = asyncio.create_task(h2_request(port=port, path='/block'))
        await _wait_for_path(blocked_path)
        assert blocked_path.read_text() == body.decode()

        # This is the boundary at which the supervisor opens ``request
        # cleanup``. Native request draining owns the first configured grace;
        # Python cancellation and ASGI cleanup own the second.
        await asyncio.wait_for(healthcheck_failed.wait(), timeout=5)
        next_pid = await _wait_for_pid_change(port=port, previous_pid=body, timeout=10)
        assert next_pid != body
        overlapping_workers = set(_worker_pids(process.pid))
        assert overlapping_workers == {int(body), int(next_pid)}
        assert not _all_dead([int(body)]), (
            'replacement must serve while the blocked worker is in request cleanup'
        )

        # A one-grace deadline kills here, precisely when Python has only
        # begun cancellation. The blocked app cannot acknowledge native drain,
        # so it must still be alive until the second grace expires.
        await asyncio.sleep(graceful_timeout + 0.5)
        assert not _all_dead([int(body)]), (
            'worker died before its request-cleanup grace elapsed'
        )
        [only_worker] = await _wait_for_worker_count(
            supervisor_pid=process.pid,
            count=1,
            timeout=graceful_timeout + 5,
        )
        assert only_worker == int(next_pid)
        assert _all_dead([int(body)])
        await asyncio.gather(blocking, return_exceptions=True)
        assert process.returncode is None
    finally:
        if blocking is not None:
            if not blocking.done():
                blocking.cancel()
            await asyncio.gather(blocking, return_exceptions=True)
        await _terminate_process(process)
        await asyncio.wait_for(stderr_task, timeout=5)

    stderr = b''.join(stderr_lines).decode()
    assert (
        f'Worker [{body.decode()}] exceeded request cleanup timeout; killing' in stderr
    )
    assert f'Stopped worker [{body.decode()}]' in stderr
    assert 'exited unexpectedly' not in stderr


async def test_worker_supervisor_healthcheck_allows_async_lifespan_startup(
    tmp_path: Path,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='slow_lifespan_app',
        module_source="""
        import asyncio

        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                while True:
                    message = await receive()
                    if message['type'] == 'lifespan.startup':
                        await asyncio.sleep(0.6)
                        await send({'type': 'lifespan.startup.complete'})
                    elif message['type'] == 'lifespan.shutdown':
                        await send({'type': 'lifespan.shutdown.complete'})
                        return
            elif scope['type'] == 'http':
                await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                await send({'type': 'http.response.body', 'body': b'after-lifespan'})
        """,
        workers=1,
        extra_args=[
            '--timeout-worker-healthcheck',
            '0.2',
            '--timeout-lifespan-startup',
            '2',
        ],
    )

    try:
        await wait_for_port(port)
        status, body = await asyncio.wait_for(h2_request(port=port), timeout=5)
        assert status == 200
        assert body == b'after-lifespan'
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_reload_restarts_server_after_python_source_change(
    tmp_path: Path,
) -> None:
    module_name = 'reload_app'
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name=module_name,
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'v1'})
        """,
        workers=1,
        extra_args=['--reload', '--app-dir', str(tmp_path)],
    )
    module_path = tmp_path / f'{module_name}.py'

    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'v1')

        module_path.write_text(
            textwrap.dedent(
                """
                async def app(scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': b'v2'})
                """
            ).strip()
            + '\n'
        )
        os.utime(module_path, None)

        await _wait_for_h2_body(port=port, body=b'v2', timeout=10)
        assert process.returncode is None
    finally:
        await _terminate_process(process)


async def test_reload_coalesces_bursty_writes_into_one_restart(
    tmp_path: Path,
) -> None:
    module_name = 'reload_coalesce_app'
    stderr_lines: list[bytes] = []
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name=module_name,
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'v1'})
        """,
        workers=1,
        extra_args=['--reload', '--app-dir', str(tmp_path)],
        stderr=asyncio.subprocess.PIPE,
    )
    module_path = tmp_path / f'{module_name}.py'
    stderr_task = asyncio.create_task(_collect_lines(process.stderr, stderr_lines))

    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'v1')

        module_path.write_text(
            textwrap.dedent(
                """
                async def app(scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': b'v2'})
                """
            ).strip()
            + '\n'
        )
        os.utime(module_path, None)
        await asyncio.sleep(0.02)
        module_path.write_text(
            textwrap.dedent(
                """
                async def app(scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': b'v3'})
                """
            ).strip()
            + '\n'
        )
        os.utime(module_path, None)

        await _wait_for_h2_body(port=port, body=b'v3', timeout=10)
        await asyncio.sleep(0.3)
        assert process.returncode is None
    finally:
        await _terminate_process(process)
        await stderr_task

    assert (
        sum(b'Reload change detected:' in line for line in stderr_lines)
        + sum(b'Reload changes detected:' in line for line in stderr_lines)
    ) == 1


async def test_reuse_port_allows_overlapping_server_generations(tmp_path: Path) -> None:
    """Two independent server processes share one port via SO_REUSEPORT and
    requests keep succeeding after the first generation drains away.
    """
    module_source = """
    import os

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
    """
    port = find_free_port()
    gen_a, _ = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='reuse_port_app',
        module_source=module_source,
        workers=1,
        port=port,
        extra_args=['--reuse-port'],
    )
    try:
        status, pid_a = await _wait_for_h2_body_any(port=port)
        assert status == 200
        gen_b, _ = await _spawn_server_process(
            tmp_path=tmp_path,
            module_name='reuse_port_app',
            module_source=module_source,
            workers=1,
            port=port,
            extra_args=['--reuse-port'],
        )
        try:
            # Generation B is up once a different worker pid answers — only
            # then is draining A guaranteed not to empty the port.
            await _wait_for_pid_change(port=port, previous_pid=pid_a, timeout=10)
            await _terminate_process(gen_a)
            deadline = asyncio.get_running_loop().time() + 5
            served = 0
            while served < 5:
                try:
                    status, body = await asyncio.wait_for(
                        h2_request(port=port), timeout=5
                    )
                except OSError:
                    # A connection may still hash to A's just-closed socket
                    # for an instant; the kernel rebalances immediately.
                    if asyncio.get_running_loop().time() >= deadline:
                        raise
                    await asyncio.sleep(0.05)
                    continue
                assert status == 200
                assert body != pid_a
                served += 1
        finally:
            await _terminate_process(gen_b)
    finally:
        await _terminate_process(gen_a)


def _worker_pids(supervisor_pid: int) -> list[int]:
    """PIDs of the supervisor's forked worker children (Linux /proc scan)."""
    children = []
    for entry in os.listdir('/proc'):
        if not entry.isdigit():
            continue
        try:
            with open(f'/proc/{entry}/status') as status:
                ppid = next(
                    int(line.split()[1]) for line in status if line.startswith('PPid:')
                )
        except (OSError, StopIteration):
            continue
        if ppid == supervisor_pid:
            children.append(int(entry))
    return children


def _all_dead(pids: list[int]) -> bool:
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        except PermissionError:
            return False
        else:
            return False
    return True


@pytest.mark.skipif(
    sys.platform != 'linux',
    reason='orphan reaping relies on the /proc scan and PR_SET_PDEATHSIG',
)
@pytest.mark.parametrize('workers', [1, 2])
async def test_sigkilled_supervisor_leaves_no_orphan_workers(
    tmp_path: Path,
    workers: int,
) -> None:
    process, port = await _spawn_server_process(
        tmp_path=tmp_path,
        module_name='orphan_app',
        module_source="""
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'alive'})
        """,
        workers=workers,
    )
    try:
        await wait_for_port(port)
        await _wait_for_h2_success(port=port, body=b'alive')
        worker_pids = _worker_pids(process.pid)
        assert len(worker_pids) == workers

        # Hard-kill the supervisor (no graceful teardown): PR_SET_PDEATHSIG
        # must make the kernel reap every worker regardless.
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=5)

        deadline = asyncio.get_running_loop().time() + 5
        while not _all_dead(worker_pids):
            assert asyncio.get_running_loop().time() < deadline, (
                f'workers orphaned after supervisor SIGKILL: {worker_pids}'
            )
            await asyncio.sleep(0.05)
    finally:
        await _terminate_process(process)
        for pid in _worker_pids(process.pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
