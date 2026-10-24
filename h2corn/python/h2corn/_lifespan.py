from __future__ import annotations

import asyncio
import inspect
from typing import cast

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from typing import Any

    from h2corn._types import (
        Application,
        ApplicationFactory,
        FrameworkASGIApp,
        FrameworkMessage,
        State,
    )


async def cancel_task(
    task: asyncio.Future[Any] | None,
    *,
    timeout: float | None = None,
) -> None:
    """Cancel `task` and wait for it to settle.

    `timeout` bounds that wait. Without one this waits as long as the task
    takes, which is right when the task is known to be cooperative — but an
    application that catches `CancelledError` and never returns is not, and
    a configured lifespan timeout that then waited forever was no timeout at
    all. On expiry the task is left running and detached: it still owns
    whatever the application opened, and taking that away underneath it
    would be worse than the process ending with it open.
    """
    if task is None:
        return
    if not task.done() and (
        not isinstance(task, asyncio.Task) or not task.cancelling()
    ):
        task.cancel()

    # Cancellation of this cleanup's caller is distinct from cancellation of
    # the child. Keep the exact child ownership until it has settled; otherwise
    # a repeated cancellation can make a Server reusable while its previous
    # lifespan task is still running.
    deferred_cancellation: asyncio.CancelledError | None = None
    expires_at = (
        None
        if timeout is None or timeout <= 0
        else asyncio.get_running_loop().time() + timeout
    )
    while not task.done():
        if expires_at is not None and asyncio.get_running_loop().time() >= expires_at:
            if deferred_cancellation is not None:
                raise deferred_cancellation
            return
        try:
            if expires_at is None:
                await asyncio.shield(task)
            else:
                await asyncio.wait(
                    [task],
                    timeout=expires_at - asyncio.get_running_loop().time(),
                )
        except asyncio.CancelledError as exc:
            if task.done():
                break
            if deferred_cancellation is None:
                deferred_cancellation = exc

    try:
        task.result()
    except asyncio.CancelledError:
        pass
    if deferred_cancellation is not None:
        raise deferred_cancellation


async def await_with_timeout(
    awaitable: Awaitable[None],
    timeout_seconds: float | None,
    message: str,
):
    if timeout_seconds is None or timeout_seconds <= 0:
        await awaitable
        return
    timeout = asyncio.timeout(timeout_seconds)
    try:
        async with timeout:
            await awaitable
    except TimeoutError as exc:
        if timeout.expired():
            raise RuntimeError(message) from exc
        raise


class LifespanRunner:
    def __init__(self, app: Application):
        self._app = cast('FrameworkASGIApp', app)
        self.state: State = {}
        self._recv: asyncio.Queue[FrameworkMessage] = asyncio.Queue()
        self._send: asyncio.Queue[FrameworkMessage] = asyncio.Queue()
        self._task: asyncio.Future[None] | None = None
        self._active = False

    async def discard_task(self, timeout: float | None = None) -> bool:
        """Abandon the lifespan task, waiting at most `timeout` for it.

        Returns `True` when the task has settled (or there was none). Returns
        `False` and retains `_task` when the bound expires while the task is
        still running: the task still owns whatever the application opened, so
        callers that guard reuse must keep waiting until it actually finishes.
        """
        task = self._task
        if task is None:
            return True
        self._active = False
        try:
            await cancel_task(task, timeout=timeout)
        except (Exception, asyncio.CancelledError):
            pass
        if not task.done():
            return False
        self._task = None
        return True

    async def wait_retained_task(self) -> None:
        """Wait for a task still held after `discard_task` returned `False`."""
        task = self._task
        if task is None:
            return
        try:
            await asyncio.shield(task)
        except (Exception, asyncio.CancelledError):
            pass
        finally:
            if task.done():
                self._task = None

    async def wait_failed_startup(self) -> None:
        if not self._active:
            await self.wait_retained_task()

    async def _exchange(self, message: FrameworkMessage):
        assert self._task is not None
        event_task = asyncio.create_task(self._send.get())
        try:
            await self._recv.put(message)
            waiters: set[asyncio.Future[Any]] = {self._task, event_task}
            done, _pending = await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            await cancel_task(event_task)

        return event_task.result() if event_task in done else None

    async def startup(self, *, required: bool):
        scope = {
            'type': 'lifespan',
            'asgi': {'version': '3.0', 'spec_version': '2.0'},
            'state': self.state,
        }
        startup_seen = False

        async def receive():
            nonlocal startup_seen
            startup_seen = True
            return await self._recv.get()

        async def send(message: FrameworkMessage):
            if message.get('type') == 'lifespan.startup.complete':
                self._active = True
            await self._send.put(message)

        self._task = asyncio.ensure_future(self._app(scope, receive, send))
        message = await self._exchange({'type': 'lifespan.startup'})
        if message is not None:
            match message.get('type'):
                case 'lifespan.startup.complete':
                    return
                case 'lifespan.startup.failed':
                    detail = message.get('message', '')
                    raise RuntimeError(
                        'lifespan startup failed'
                        if not detail
                        else f'lifespan startup failed: {detail}'
                    )
                case _ if not startup_seen:
                    await self.discard_task()
                    return
                case message_type:
                    raise RuntimeError(
                        f'unexpected lifespan startup event: {message_type}'
                    )

        assert self._task is not None
        if self._task.cancelled():
            self._task = None
            if startup_seen:
                raise RuntimeError('lifespan startup was cancelled')
            return
        exc = self._task.exception()
        self._task = None
        # In `auto` mode any exception means "this application does not do
        # lifespan", and the server carries on without it. That is the spec's
        # fallback, and it costs nothing in practice because a framework that
        # *does* implement lifespan reports a real startup failure through
        # `lifespan.startup.failed` — Starlette sends exactly that before
        # re-raising, so a genuine failure is already loud above and never
        # reaches here. Neither the exception's type nor whether the
        # application happened to call `receive()` distinguishes the two cases:
        # an HTTP-only application that calls `receive()` unconditionally hits
        # both. `on` mode fails instead, because there the operator asked for
        # lifespan and wants to know why it did not run.
        if required:
            # Chained, not re-raised bare: the application's exception alone
            # reads like the server failed (an app raising `TimeoutError` looks
            # exactly like the startup timeout), while the diagnosis alone
            # hides where it came from.
            raise RuntimeError(
                'lifespan startup is required but the app does not support it'
            ) from exc
        return

    async def shutdown(self):
        if not self._active or self._task is None:
            return
        # A cancelled startup exchange may leave its completion queued.
        # It must not be mistaken for the response to shutdown.
        if not self._send.empty():
            self._send.get_nowait()
        message = await self._exchange({'type': 'lifespan.shutdown'})
        if message is None:
            if self._task.cancelled():
                self._task = None
                self._active = False
                raise RuntimeError('lifespan shutdown was cancelled')
            exc = self._task.exception()
            self._task = None
            self._active = False
            if exc is not None:
                raise RuntimeError('lifespan shutdown crashed') from exc
            return
        match message.get('type'):
            case 'lifespan.shutdown.complete':
                pass
            case 'lifespan.shutdown.failed':
                detail = message.get('message', '')
                raise RuntimeError(
                    'lifespan shutdown failed'
                    if not detail
                    else f'lifespan shutdown failed: {detail}'
                )
            case message_type:
                raise RuntimeError(
                    f'unexpected lifespan shutdown event: {message_type}'
                )

        task = self._task
        assert task is not None
        try:
            await task
        finally:
            self._task = None
            self._active = False


class PreparedApplication:
    """Own one application's construction and lifespan on its request loop."""

    def __init__(
        self,
        source: Application | ApplicationFactory,
        factory: bool,
        mode: str,
    ) -> None:
        self.source = source
        self.factory = factory
        self.mode = mode
        self.app: Application | None = None
        self._runner: LifespanRunner | None = None
        self._startup_task: asyncio.Task[Any] | None = None

    @property
    def state(self) -> State | None:
        return None if self._runner is None else self._runner.state

    async def startup(self, *, required: bool) -> None:
        self._startup_task = asyncio.current_task()
        app = cast('ApplicationFactory', self.source)() if self.factory else self.source
        if inspect.isawaitable(app):
            if inspect.iscoroutine(app):
                app.close()
            raise TypeError(
                'application factory must return an ASGI callable, not an awaitable'
            )
        if not callable(app):
            raise TypeError('application factory returned a non-callable')
        self.app = cast('Application', app)
        if self.mode != 'off':
            self._runner = LifespanRunner(self.app)
            await self._runner.startup(required=required)
        self._startup_task = None

    async def discard_task(self, timeout: float | None = None) -> bool:
        startup_pending = self._startup_task is not None
        if not await self._cancel_startup(timeout):
            return False
        if self._runner is None:
            return True
        # Publishing startup completion commits resources even when the
        # startup waiter is cancelled before it can observe the message.
        if startup_pending:
            try:
                await await_with_timeout(
                    self._runner.shutdown(),
                    timeout,
                    'lifespan startup rollback timed out',
                )
            except (Exception, asyncio.CancelledError):
                pass
        return await self._runner.discard_task(timeout)

    async def _cancel_startup(self, timeout: float | None = None) -> bool:
        startup = self._startup_task
        if startup is not None and startup is not asyncio.current_task():
            try:
                await cancel_task(startup, timeout=timeout)
            except (Exception, asyncio.CancelledError):
                pass
            if not startup.done():
                return False
            self._startup_task = None
        return True

    async def settle_startup(self) -> PreparedApplication:
        await self._cancel_startup()
        if self._runner is not None:
            await self._runner.wait_failed_startup()
        return self

    async def wait_retained_task(self) -> None:
        startup = self._startup_task
        if startup is not None and startup is not asyncio.current_task():
            try:
                await asyncio.shield(startup)
            except (Exception, asyncio.CancelledError):
                pass
            finally:
                if startup.done():
                    self._startup_task = None
        if self._runner is not None:
            await self._runner.wait_retained_task()

    async def shutdown(self) -> None:
        await self._cancel_startup()
        if self._runner is not None:
            await self._runner.shutdown()


async def start_prepared_application(
    runner: PreparedApplication,
    *,
    required: bool,
    startup_timeout: float | None,
) -> PreparedApplication:
    """Start one secondary-loop runner and return its owning object."""
    try:
        await await_with_timeout(
            runner.startup(required=required),
            startup_timeout,
            'lifespan startup timed out',
        )
    except BaseException:
        # Bound discard by the same startup budget: an app that ignores
        # cancellation must not turn a timed-out startup into an unbounded wait.
        await runner.discard_task(startup_timeout)
        raise
    return runner


async def stop_prepared_application(
    runner: PreparedApplication,
    *,
    shutdown_timeout: float | None,
) -> None:
    """Stop one secondary-loop runner, always cleaning up its app task."""
    try:
        await await_with_timeout(
            runner.shutdown(),
            shutdown_timeout,
            'lifespan shutdown timed out',
        )
    except BaseException:
        await runner.discard_task(shutdown_timeout)
        await runner.wait_retained_task()
        raise
    await runner.wait_retained_task()
