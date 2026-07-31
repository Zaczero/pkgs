import asyncio
from typing import Any

import pytest
from h2corn import Config, Server
from h2corn._config import LifespanMode
from h2corn._lifespan import LifespanRunner, await_with_timeout, cancel_task

from tests._support import wait_for_server

pytestmark = pytest.mark.asyncio


async def _run_lifespan(
    app,
    *,
    mode: LifespanMode = 'auto',
    startup_timeout: float | None = None,
    shutdown_timeout: float | None = None,
    after_startup=None,
) -> None:
    """Drive one primary lifespan the way Server does, without binding."""
    if mode == 'off':
        if after_startup is not None:
            await after_startup(None)
        return
    runner = LifespanRunner(app)
    try:
        await await_with_timeout(
            runner.startup(required=mode == 'on'),
            startup_timeout,
            'lifespan startup timed out',
        )
    except BaseException:
        await runner.discard_task(startup_timeout)
        raise
    try:
        if after_startup is not None:
            await after_startup(runner)
    finally:
        try:
            await await_with_timeout(
                runner.shutdown(),
                shutdown_timeout,
                'lifespan shutdown timed out',
            )
        except BaseException:
            await runner.discard_task(shutdown_timeout)
            raise


async def test_lifespan_startup_failure_is_reported() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        assert scope['asgi'] == {'version': '3.0', 'spec_version': '2.0'}
        message = await receive()
        assert message['type'] == 'lifespan.startup'
        await send({'type': 'lifespan.startup.failed', 'message': 'boom'})

    async def after(_runner):
        raise AssertionError('serve body should not run after startup failure')

    with pytest.raises(RuntimeError, match='lifespan startup failed: boom'):
        await _run_lifespan(app, after_startup=after)


async def test_lifespan_failure_without_a_message_has_no_dangling_colon() -> None:
    async def app(_scope, receive, send):
        await receive()
        await send({'type': 'lifespan.startup.failed'})

    with pytest.raises(RuntimeError, match=r'^lifespan startup failed$'):
        await _run_lifespan(app)


async def test_lifespan_missing_protocol_is_treated_as_optional() -> None:
    served = False

    async def app(scope, _receive, _send):
        assert scope['type'] == 'lifespan'

    async def after(_runner):
        nonlocal served
        served = True

    await _run_lifespan(app, after_startup=after)
    assert served is True


async def test_lifespan_can_be_disabled() -> None:
    served = False

    async def app(scope, _receive, _send):
        raise AssertionError('lifespan scope should not be used when disabled')

    async def after(runner):
        nonlocal served
        served = True
        assert runner is None

    await _run_lifespan(app, mode='off', after_startup=after)
    assert served is True


async def test_lifespan_on_requires_support() -> None:
    async def app(scope, _receive, _send):
        assert scope['type'] == 'lifespan'

    async def after(_runner):
        raise AssertionError('serve body should not run without required lifespan')

    with pytest.raises(
        RuntimeError,
        match='lifespan startup is required but the app does not support it',
    ):
        await _run_lifespan(app, mode='on', after_startup=after)


@pytest.mark.parametrize('mode', ['auto', 'on'])
async def test_lifespan_falls_back_when_the_app_never_reads_the_scope(
    mode: LifespanMode,
) -> None:
    served = False

    async def app(scope, _receive, _send):
        assert scope['type'] == 'http', 'this app only speaks HTTP'

    async def after(_runner):
        nonlocal served
        served = True

    if mode == 'on':
        with pytest.raises(RuntimeError, match='does not support it') as raised:
            await _run_lifespan(app, mode=mode, after_startup=after)
        assert isinstance(raised.value.__cause__, AssertionError)
        assert served is False
    else:
        await _run_lifespan(app, mode=mode, after_startup=after)
        assert served is True


@pytest.mark.parametrize(
    'error', [AssertionError('boom'), KeyError('boom'), RuntimeError('boom')]
)
async def test_lifespan_auto_falls_back_on_any_exception(error: Exception) -> None:
    served = False

    async def app(_scope, receive, _send):
        await receive()
        raise error

    async def after(_runner):
        nonlocal served
        served = True

    await _run_lifespan(app, mode='auto', after_startup=after)
    assert served is True


@pytest.mark.parametrize(
    'error', [AssertionError('boom'), KeyError('boom'), RuntimeError('boom')]
)
async def test_lifespan_on_reports_the_diagnosis_and_keeps_the_cause(
    error: Exception,
) -> None:
    served = False

    async def app(_scope, receive, _send):
        await receive()
        raise error

    async def after(_runner):
        nonlocal served
        served = True

    with pytest.raises(RuntimeError, match='does not support it') as raised:
        await _run_lifespan(app, mode='on', after_startup=after)
    assert isinstance(raised.value.__cause__, type(error))
    assert served is False


async def test_lifespan_startup_failed_message_is_loud_in_auto_mode() -> None:
    served = False

    async def app(_scope, receive, send):
        await receive()
        await send({'type': 'lifespan.startup.failed', 'message': 'db is down'})

    async def after(_runner):
        nonlocal served
        served = True

    with pytest.raises(RuntimeError, match='db is down'):
        await _run_lifespan(app, mode='auto', after_startup=after)
    assert served is False


async def test_lifespan_startup_timeout_is_reported() -> None:
    async def app(scope, receive, _send):
        assert scope['type'] == 'lifespan'
        message = await receive()
        assert message['type'] == 'lifespan.startup'
        await asyncio.sleep(0.05)

    async def after(_runner):
        raise AssertionError('serve body should not run after startup timeout')

    with pytest.raises(RuntimeError, match='lifespan startup timed out'):
        await _run_lifespan(app, after_startup=after, startup_timeout=0.01)


async def test_repeated_cancellation_waits_for_lifespan_child_cleanup() -> None:
    startup_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def app(scope, receive, _send):
        assert scope['type'] == 'lifespan'
        assert (await receive())['type'] == 'lifespan.startup'
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

    serving = asyncio.create_task(_run_lifespan(app, mode='on'))
    await asyncio.wait_for(startup_entered.wait(), timeout=1)
    serving.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)

    serving.cancel()
    await asyncio.sleep(0)
    assert not serving.done(), 'repeated cancellation must not abandon the child task'

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(serving, timeout=1)
    assert cleanup_finished.is_set()


async def test_lifespan_shutdown_timeout_is_reported() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        startup = await receive()
        assert startup['type'] == 'lifespan.startup'
        await send({'type': 'lifespan.startup.complete'})
        shutdown = await receive()
        assert shutdown['type'] == 'lifespan.shutdown'
        await asyncio.sleep(0.05)

    async def after(_runner):
        return None

    with pytest.raises(RuntimeError, match='lifespan shutdown timed out'):
        await _run_lifespan(app, after_startup=after, shutdown_timeout=0.01)


async def test_lifespan_shutdown_failure_cleans_up_app_task() -> None:
    app_task: asyncio.Task[Any] | None = None

    async def app(scope, receive, send):
        nonlocal app_task
        app_task = asyncio.current_task()
        assert scope['type'] == 'lifespan'
        startup = await receive()
        assert startup['type'] == 'lifespan.startup'
        await send({'type': 'lifespan.startup.complete'})
        shutdown = await receive()
        assert shutdown['type'] == 'lifespan.shutdown'
        await send({'type': 'lifespan.shutdown.failed', 'message': 'boom'})
        await asyncio.sleep(10)

    async def after(_runner):
        return None

    with pytest.raises(RuntimeError, match='lifespan shutdown failed: boom'):
        await _run_lifespan(app, after_startup=after)

    assert app_task is not None
    assert app_task.cancelled()


async def test_lifespan_app_timeout_error_is_not_rewritten_as_server_timeout() -> None:
    async def app(scope, receive, _send):
        assert scope['type'] == 'lifespan'
        startup = await receive()
        assert startup['type'] == 'lifespan.startup'
        raise TimeoutError('custom-timeout')

    async def after(_runner):
        return None

    with pytest.raises(RuntimeError, match='does not support it') as raised:
        await _run_lifespan(app, mode='on', after_startup=after, startup_timeout=1.0)
    assert isinstance(raised.value.__cause__, TimeoutError)
    assert str(raised.value.__cause__) == 'custom-timeout'


async def test_startup_timeout_is_bounded_by_an_app_that_ignores_cancellation() -> None:
    released = asyncio.Event()

    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        await receive()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await released.wait()
            raise

    async def after(_runner):
        raise AssertionError('serve must not run: startup never completed')

    try:
        with pytest.raises(RuntimeError, match='lifespan startup timed out'):
            await asyncio.wait_for(
                _run_lifespan(
                    app, mode='on', after_startup=after, startup_timeout=0.05
                ),
                timeout=5,
            )
    finally:
        released.set()
        await asyncio.sleep(0)


async def test_discard_task_retains_task_until_it_settles() -> None:
    """discard_task returns False and keeps ownership when the task outlives timeout."""
    release = asyncio.Event()

    async def resistant() -> None:
        while not release.is_set():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                # Swallow cancellation until the test releases ownership.
                task = asyncio.current_task()
                if task is not None:
                    task.uncancel()

    runner = LifespanRunner(lambda *_: None)  # type: ignore[arg-type]
    runner._task = asyncio.get_running_loop().create_task(resistant())
    runner._active = True
    # Let the task enter its sleep so cancel cannot finish it before it runs.
    await asyncio.sleep(0)
    assert not runner._task.done()
    settled = await runner.discard_task(timeout=0.01)
    assert settled is False
    assert runner._task is not None
    assert not runner._task.done()
    release.set()
    # Wake the sleeping task so it can observe the release.
    runner._task.cancel()
    await asyncio.wait_for(asyncio.shield(runner._task), timeout=1)
    assert await runner.discard_task() is True
    assert runner._task is None


async def test_cancel_task_zero_timeout_still_waits_for_cooperative_cleanup() -> None:
    cancelling = asyncio.Event()
    release = asyncio.Event()

    async def cooperative() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelling.set()
            await release.wait()
            raise

    task = asyncio.create_task(cooperative())
    await asyncio.sleep(0)
    cleanup = asyncio.create_task(cancel_task(task, timeout=0))
    await asyncio.wait_for(cancelling.wait(), timeout=1)
    assert not cleanup.done()
    release.set()
    await asyncio.wait_for(cleanup, timeout=1)
    assert task.cancelled()


async def test_cancel_task_waits_once_for_the_actual_remaining_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timeout is one deadline wait, not 5 ms polling slices."""
    from h2corn import _lifespan

    class Clock:
        now = 100.0

        def time(self) -> float:
            return self.now

    clock = Clock()
    observed_timeouts: list[float] = []
    release = asyncio.Event()

    async def resistant() -> None:
        while not release.is_set():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                task = asyncio.current_task()
                assert task is not None
                task.uncancel()

    async def fake_wait(_tasks, timeout: float):
        observed_timeouts.append(timeout)
        await asyncio.sleep(0)
        clock.now += timeout
        return set(), set()

    task = asyncio.create_task(resistant())
    await asyncio.sleep(0)
    monkeypatch.setattr(_lifespan.asyncio, 'get_running_loop', lambda: clock)
    monkeypatch.setattr(_lifespan.asyncio, 'wait', fake_wait)
    try:
        await cancel_task(task, timeout=7.0)
    finally:
        release.set()
        task.cancel()
        await asyncio.wait_for(task, timeout=1)

    assert observed_timeouts == [7.0]


async def test_server_shutdown_during_blocked_startup_never_serves() -> None:
    startup_entered = asyncio.Event()
    finish_startup = asyncio.Event()
    saw_http = False

    async def app(scope, receive, send):
        nonlocal saw_http
        if scope['type'] == 'lifespan':
            assert (await receive())['type'] == 'lifespan.startup'
            startup_entered.set()
            await finish_startup.wait()
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return
        saw_http = True

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    task = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    assert server.addresses
    server.shutdown()
    finish_startup.set()
    await asyncio.wait_for(task, timeout=2)
    assert saw_http is False
    assert server.addresses == ()


async def test_wait_started_stays_pending_while_lifespan_startup_holds() -> None:
    startup_entered = asyncio.Event()
    finish_startup = asyncio.Event()

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            assert (await receive())['type'] == 'lifespan.startup'
            startup_entered.set()
            await finish_startup.wait()
            await send({'type': 'lifespan.startup.complete'})
            assert (await receive())['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='on'),
    )
    serving = asyncio.create_task(server.serve())
    await asyncio.wait_for(startup_entered.wait(), timeout=2)
    assert server.addresses, 'listeners bind before lifespan startup finishes'
    waiting = asyncio.create_task(server.wait_started())
    await asyncio.sleep(0)
    assert not waiting.done(), 'readiness must wait for lifespan + native acceptance'
    finish_startup.set()
    await asyncio.wait_for(waiting, timeout=2)
    server.shutdown()
    await asyncio.wait_for(serving, timeout=2)


async def test_zero_graceful_timeout_on_empty_server_completes() -> None:
    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            raise AssertionError('lifespan off')
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    server = Server(
        app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=0,
        ),
    )
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    server.shutdown()
    await asyncio.wait_for(serving, timeout=2)
    assert server.addresses == ()
    assert server.releasing is False
