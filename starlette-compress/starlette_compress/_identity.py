from __future__ import annotations

from starlette.datastructures import MutableHeaders

from starlette_compress._utils import classify_start_message

TYPE_CHECKING = False
if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

_INIT = 0
_PENDING = 1
_COMMITTED = 2
_PASSTHROUGH = 3


class IdentityResponder:
    __slots__ = (
        'app',
        'minimum_size',
    )

    def __init__(self, app: ASGIApp, minimum_size: int) -> None:
        self.app = app
        self.minimum_size = minimum_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        state = _INIT
        start_message: Message | None = None
        streaming = False
        pathsend_capable = 'http.response.pathsend' in scope.get('extensions', {})

        async def wrapper(message: Message) -> None:
            nonlocal state, start_message, streaming

            message_type: str = message['type']

            if message_type == 'http.response.start':
                if state != _INIT:
                    raise AssertionError(
                        'Unexpected repeated http.response.start message'
                    )

                verdict = classify_start_message(message)
                if verdict == 'skip':
                    state = _PASSTHROUGH
                    await send(message)
                    return

                streaming = verdict == 'streaming'

                # Streaming types without pathsend: send start immediately so
                # clients see headers before the first body; pass messages through.
                if streaming and not pathsend_capable:
                    headers = MutableHeaders(raw=message['headers'])
                    headers.add_vary_header('Accept-Encoding')
                    # Content-Length is preserved for identity
                    state = _COMMITTED
                    await send(message)
                    return

                start_message = message
                state = _PENDING
                return

            if state in (_PASSTHROUGH, _COMMITTED):
                # Committed identity: pure passthrough, including empty bodies
                await send(message)
                return

            if state == _PENDING:
                assert start_message is not None

                if message_type != 'http.response.body':
                    pending = start_message
                    start_message = None
                    state = _PASSTHROUGH
                    await send(pending)
                    await send(message)
                    return

                body: bytes = message.get('body', b'')
                more_body: bool = message.get('more_body', False)

                if not streaming and not more_body and len(body) < self.minimum_size:
                    pending = start_message
                    start_message = None
                    state = _PASSTHROUGH
                    await send(pending)
                    await send(message)
                    return

                headers = MutableHeaders(raw=start_message['headers'])
                headers.add_vary_header('Accept-Encoding')
                pending = start_message
                start_message = None
                state = _COMMITTED
                await send(pending)
                await send(message)
                return

            await send(message)

        await self.app(scope, receive, wrapper)
