from __future__ import annotations

from starlette.datastructures import MutableHeaders

from starlette_compress._utils import classify_start_message

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from typing import Protocol

    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    class StreamEncoder(Protocol):
        def feed(self, data: bytes, flush: bool) -> Iterable[bytes]: ...
        def finish(self) -> Iterable[bytes]: ...

    OneshotCompress = Callable[[bytes], bytes]
    EncoderFactory = Callable[[int], StreamEncoder]

_INIT = 0
_PENDING = 1
_COMMITTED = 2
_PASSTHROUGH = 3
_FINISHED = 4


def _content_length_from_headers(headers: MutableHeaders) -> int:
    try:
        return int(headers.get('Content-Length', -1))
    except ValueError:
        return -1


async def _send_compressed(
    encoder: StreamEncoder,
    body: bytes,
    more_body: bool,
    streaming: bool,
    send: Send,
) -> None:
    """Feed one app body message into the encoder and emit wire messages.

    Empty non-final bodies skip codec work. Empty mid-stream codec output is
    never forwarded as ``body=b''`` with ``more_body=True``. Exactly one
    terminal body message is always sent when ``more_body`` is false.
    """
    if more_body:
        if not body:
            return
        for chunk in encoder.feed(body, flush=streaming):
            if chunk:
                await send({
                    'type': 'http.response.body',
                    'body': chunk,
                    'more_body': True,
                })
        return

    if body:
        for chunk in encoder.feed(body, flush=False):
            if chunk:
                await send({
                    'type': 'http.response.body',
                    'body': chunk,
                    'more_body': True,
                })

    final_chunks = [chunk for chunk in encoder.finish() if chunk]
    if not final_chunks:
        await send({'type': 'http.response.body', 'body': b''})
        return
    for chunk in final_chunks[:-1]:
        await send({
            'type': 'http.response.body',
            'body': chunk,
            'more_body': True,
        })
    await send({
        'type': 'http.response.body',
        'body': final_chunks[-1],
    })


class CompressionResponder:
    """Shared ASGI control loop for encoded compression responders."""

    __slots__ = (
        'app',
        'create_encoder',
        'encoding',
        'minimum_size',
        'oneshot',
    )

    def __init__(
        self,
        app: ASGIApp,
        minimum_size: int,
        encoding: str,
        oneshot: OneshotCompress,
        create_encoder: EncoderFactory,
    ) -> None:
        self.app = app
        self.minimum_size = minimum_size
        self.encoding = encoding
        self.oneshot = oneshot
        self.create_encoder = create_encoder

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        state = _INIT
        start_message: Message | None = None
        encoder: StreamEncoder | None = None
        streaming = False
        extensions = scope.get('extensions', {})
        pathsend_capable = 'http.response.pathsend' in extensions

        # Encoded path: never advertise zerocopysend — after Content-Encoding
        # is committed, a raw file would corrupt the response. Shallow-copy
        # only; do not mutate the caller's scope.
        if 'http.response.zerocopysend' in extensions:
            scope = {
                **scope,
                'extensions': {
                    k: v
                    for k, v in extensions.items()
                    if k != 'http.response.zerocopysend'
                },
            }

        async def wrapper(message: Message) -> None:
            nonlocal state, start_message, encoder, streaming

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

                # Streaming types without pathsend: commit at start so clients
                # (e.g. EventSource) see headers before the first body event.
                if streaming and not pathsend_capable:
                    headers = MutableHeaders(raw=message['headers'])
                    # Streaming: never pledge Content-Length to the encoder
                    # (SSE body size may not match the header).
                    encoder = self.create_encoder(-1)
                    headers['Content-Encoding'] = self.encoding
                    headers.add_vary_header('Accept-Encoding')
                    del headers['Content-Length']
                    state = _COMMITTED
                    await send(message)
                    return

                start_message = message
                state = _PENDING
                return

            if state in (_PASSTHROUGH, _FINISHED):
                await send(message)
                return

            if state == _PENDING:
                assert start_message is not None

                if message_type != 'http.response.body':
                    # pathsend / trailers / unknown: forward original start raw
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
                headers['Content-Encoding'] = self.encoding
                headers.add_vary_header('Accept-Encoding')

                if not more_body:
                    compressed_body = self.oneshot(body)
                    headers['Content-Length'] = str(len(compressed_body))
                    message['body'] = compressed_body
                    pending = start_message
                    start_message = None
                    state = _FINISHED
                    await send(pending)
                    await send(message)
                    return

                # Stream-begin: only buffered (non-streaming) paths may pledge
                # Content-Length; streaming always uses -1.
                if streaming:
                    content_length = -1
                else:
                    content_length = _content_length_from_headers(headers)
                del headers['Content-Length']
                encoder = self.create_encoder(content_length)
                pending = start_message
                start_message = None
                state = _COMMITTED
                await send(pending)
                await _send_compressed(
                    encoder, body, more_body, streaming, send
                )
                return

            if state == _COMMITTED:
                assert encoder is not None

                if message_type != 'http.response.body':
                    # Trailers / extensions after start — do not re-send start
                    await send(message)
                    return

                body = message.get('body', b'')
                more_body = message.get('more_body', False)
                await _send_compressed(
                    encoder, body, more_body, streaming, send
                )
                if not more_body:
                    state = _FINISHED
                return

            # Pre-start extension messages (e.g. http.response.debug) before
            # http.response.start — forward unchanged.
            await send(message)

        await self.app(scope, receive, wrapper)
