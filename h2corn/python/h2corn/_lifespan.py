from __future__ import annotations

import asyncio
from typing import cast

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from typing import Any

    from ._types import (
        Application,
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
    if not task.done():
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
                await asyncio.wait([task], timeout=0.005)
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

    async def _exchange(self, message: FrameworkMessage):
        assert self._task is not None
        event_task = asyncio.create_task(self._send.get())
        try:
            await self._recv.put(message)
            done, _pending = await asyncio.wait(
                {self._task, event_task},
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
            await self._send.put(message)

        self._task = asyncio.ensure_future(self._app(scope, receive, send))
        message = await self._exchange({'type': 'lifespan.startup'})
        if message is not None:
            match message.get('type'):
                case 'lifespan.startup.complete':
                    self._active = True
                    return
                case 'lifespan.startup.failed':
                    await self.discard_task()
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
                    await self.discard_task()
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
                await self.discard_task()
                detail = message.get('message', '')
                raise RuntimeError(
                    'lifespan shutdown failed'
                    if not detail
                    else f'lifespan shutdown failed: {detail}'
                )
            case message_type:
                await self.discard_task()
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


async def start_lifespan_runner(
    runner: LifespanRunner,
    *,
    required: bool,
    startup_timeout: float | None,
) -> LifespanRunner:
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


async def stop_lifespan_runner(
    runner: LifespanRunner,
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
