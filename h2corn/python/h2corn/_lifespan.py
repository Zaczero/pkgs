from __future__ import annotations

import asyncio

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from ._types import ASGIApp, Message, State


async def _cancel_task(task: asyncio.Task[Any] | None):
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _serve_with_lifespan(
    app: ASGIApp,
    serve: Callable[[ASGIApp], Awaitable[None]],
    *,
    mode: str = 'auto',
    startup_timeout: float | None = None,
    shutdown_timeout: float | None = None,
):
    if mode == 'off':
        await serve(app)
        return

    lifespan = _LifespanRunner(app)
    try:
        await _await_with_timeout(
            lifespan.startup(required=mode == 'on'),
            startup_timeout,
            'lifespan startup timed out',
        )
    except Exception:
        await lifespan._discard_task()
        raise
    try:
        await serve(lifespan.app)
    finally:
        try:
            await _await_with_timeout(
                lifespan.shutdown(),
                shutdown_timeout,
                'lifespan shutdown timed out',
            )
        except Exception:
            await lifespan._discard_task()
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


class _LifespanRunner:
    def __init__(self, app: ASGIApp):
        self._app = app
        self._state: State = {}
        self._recv: asyncio.Queue[Message] = asyncio.Queue()
        self._send: asyncio.Queue[Message] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._active = False

        async def _scope_state_app(scope, receive, send):
            if scope['type'] in ('http', 'websocket') and self._state:
                scope['state'] = self._state.copy()
            await app(scope, receive, send)

        self.app = _scope_state_app

    async def _discard_task(self):
        task = self._task
        self._task = None
        self._active = False
        if task is None:
            return
        try:
            await _cancel_task(task)
        except (Exception, asyncio.CancelledError):
            pass

    async def _exchange(self, message: Message):
        assert self._task is not None
        event_task = asyncio.create_task(self._send.get())
        try:
            await self._recv.put(message)
            done, _pending = await asyncio.wait(
                {self._task, event_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            await _cancel_task(event_task)

        return event_task.result() if event_task in done else None

    async def startup(self, *, required: bool):
        scope = {
            'type': 'lifespan',
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
            'state': self._state,
        }
        startup_seen = False

        async def receive():
            nonlocal startup_seen
            startup_seen = True
            return await self._recv.get()

        async def send(message: Message):
            await self._send.put(message)

        self._task = asyncio.create_task(self._app(scope, receive, send))
        message = await self._exchange({'type': 'lifespan.startup'})
        if message is not None:
            match message.get('type'):
                case 'lifespan.startup.complete':
                    self._active = True
                    return
                case 'lifespan.startup.failed':
                    await self._discard_task()
                    raise RuntimeError(
                        f"lifespan startup failed: {message.get('message', '')}"
                    )
                case _ if not startup_seen:
                    await self._discard_task()
                    return
                case message_type:
                    await self._discard_task()
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
            raise RuntimeError('lifespan startup is required but the app does not support it')
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
                await self._discard_task()
                raise RuntimeError(
                    f"lifespan shutdown failed: {message.get('message', '')}"
                )
            case message_type:
                await self._discard_task()
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
