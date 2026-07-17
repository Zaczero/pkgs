from __future__ import annotations

import asyncio
from typing import cast

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, MutableMapping
    from typing import Any

    from ._config import LifespanMode
    from ._lib import LifespanHandoff
    from ._types import (
        Application,
        FrameworkASGIApp,
        FrameworkMessage,
        FrameworkReceive,
        FrameworkSend,
        State,
    )


async def cancel_task(task: asyncio.Future[Any] | None) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()

    # Cancellation of this cleanup's caller is distinct from cancellation of
    # the child. Keep the exact child ownership until it has settled; otherwise
    # a repeated cancellation can make a Server reusable while its previous
    # lifespan task is still running.
    deferred_cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
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


async def serve_with_lifespan(
    app: Application,
    serve: Callable[[Application, LifespanHandoff | None], Awaitable[None]],
    *,
    mode: LifespanMode = 'auto',
    startup_timeout: float | None = None,
    shutdown_timeout: float | None = None,
):
    if mode == 'off':
        await serve(app, None)
        return

    lifespan = LifespanRunner(app)
    try:
        await _await_with_timeout(
            lifespan.startup(required=mode == 'on'),
            startup_timeout,
            'lifespan startup timed out',
        )
    except BaseException:
        await lifespan.discard_task()
        raise
    try:
        # Rust consumes the exact typed handoff and calls the original app
        # directly. The wrapper remains the portable state-copying fallback.
        from ._lib import LifespanHandoff

        handoff = LifespanHandoff(
            app,
            lifespan.state,
            mode == 'on',
            startup_timeout,
            shutdown_timeout,
        )
        await serve(lifespan.app, handoff)
    finally:
        try:
            await _await_with_timeout(
                lifespan.shutdown(),
                shutdown_timeout,
                'lifespan shutdown timed out',
            )
        except BaseException:
            await lifespan.discard_task()
            raise


async def _await_with_timeout(
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

        async def scope_state_app(
            scope: MutableMapping[str, Any],
            receive: FrameworkReceive,
            send: FrameworkSend,
        ) -> None:
            if scope['type'] in ('http', 'websocket') and self.state:
                scope['state'] = self.state.copy()
            await self._app(scope, receive, send)

        self.app: FrameworkASGIApp = scope_state_app

    async def discard_task(self) -> None:
        task = self._task
        self._task = None
        self._active = False
        if task is None:
            return
        try:
            await cancel_task(task)
        except (Exception, asyncio.CancelledError):
            pass

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
                    raise RuntimeError(
                        f'lifespan startup failed: {message.get("message", "")}'
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
        if (
            startup_seen
            and exc is not None
            and not isinstance(exc, (AssertionError, KeyError, TypeError))
        ):
            raise exc
        if required:
            raise RuntimeError(
                'lifespan startup is required but the app does not support it'
            )
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
                raise RuntimeError(
                    f'lifespan shutdown failed: {message.get("message", "")}'
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
        await _await_with_timeout(
            runner.startup(required=required),
            startup_timeout,
            'lifespan startup timed out',
        )
    except BaseException:
        await runner.discard_task()
        raise
    return runner


async def stop_lifespan_runner(
    runner: LifespanRunner,
    *,
    shutdown_timeout: float | None,
) -> None:
    """Stop one secondary-loop runner, always cleaning up its app task."""
    try:
        await _await_with_timeout(
            runner.shutdown(),
            shutdown_timeout,
            'lifespan shutdown timed out',
        )
    except BaseException:
        await runner.discard_task()
        raise
