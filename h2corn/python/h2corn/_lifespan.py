from __future__ import annotations

import asyncio

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from ._types import ASGIApp, Message, State


async def _cancel_task(task: asyncio.Task[Any] | None) -> None:
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
    startup_timeout: float | None = None,
    shutdown_timeout: float | None = None,
) -> None:
    lifespan = _LifespanRunner(app)
    await _await_with_timeout(
        lifespan.startup(),
        startup_timeout,
        'lifespan startup timed out',
    )
    try:
        await serve(lifespan.app)
    finally:
        await _await_with_timeout(
            lifespan.shutdown(),
            shutdown_timeout,
            'lifespan shutdown timed out',
        )


async def _await_with_timeout(
    awaitable: Awaitable[None],
    timeout_seconds: float | None,
    message: str,
) -> None:
    if timeout_seconds is None or timeout_seconds <= 0:
        await awaitable
        return
    try:
        await asyncio.wait_for(awaitable, timeout_seconds)
    except TimeoutError as exc:
        raise RuntimeError(message) from exc


class _ScopeStateApp:
    def __init__(self, app: ASGIApp, state: State) -> None:
        self._app = app
        self._state = state

    async def __call__(self, scope, receive, send):
        if scope['type'] in {'http', 'websocket'} and self._state:
            scope['state'] = self._state.copy()
        await self._app(scope, receive, send)


class _LifespanRunner:
    def __init__(self, app: ASGIApp) -> None:
        self._app = app
        self._state: State = {}
        self._recv: asyncio.Queue[Message] = asyncio.Queue()
        self._send: asyncio.Queue[Message] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._active = False
        self.app = _ScopeStateApp(app, self._state)

    async def _discard_task(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        try:
            await _cancel_task(task)
        except (Exception, asyncio.CancelledError):
            pass

    async def _exchange(self, message: Message) -> Message | None:
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

        if event_task in done:
            return event_task.result()
        return None

    async def startup(self) -> None:
        scope = {
            'type': 'lifespan',
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
            'state': self._state,
        }
        startup_seen = False

        async def receive() -> Message:
            nonlocal startup_seen
            startup_seen = True
            return await self._recv.get()

        async def send(message: Message) -> None:
            await self._send.put(message)

        self._task = asyncio.create_task(self._app(scope, receive, send))
        message = await self._exchange({'type': 'lifespan.startup'})
        if message is not None:
            message_type = message.get('type')
            if message_type == 'lifespan.startup.complete':
                self._active = True
                return
            if message_type == 'lifespan.startup.failed':
                details = message.get('message', '')
                await self._discard_task()
                raise RuntimeError(f'lifespan startup failed: {details}')
            if not startup_seen:
                await self._discard_task()
                return
            await self._discard_task()
            raise RuntimeError(f'unexpected lifespan startup event: {message_type}')

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
        return

    async def shutdown(self) -> None:
        if not self._active or self._task is None:
            return
        message = await self._exchange({'type': 'lifespan.shutdown'})
        if message is not None:
            message_type = message.get('type')
            if message_type == 'lifespan.shutdown.failed':
                details = message.get('message', '')
                raise RuntimeError(f'lifespan shutdown failed: {details}')
            if message_type != 'lifespan.shutdown.complete':
                raise RuntimeError(
                    f'unexpected lifespan shutdown event: {message_type}'
                )
        elif self._task.cancelled():
            self._task = None
            raise RuntimeError('lifespan shutdown was cancelled')
        else:
            exc = self._task.exception()
            self._task = None
            if exc is not None:
                raise RuntimeError('lifespan shutdown crashed') from exc
            return

        await self._task
