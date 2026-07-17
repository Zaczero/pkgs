from __future__ import annotations

from platform import python_implementation

from starlette_compress._responder import CompressionResponder

TYPE_CHECKING = False

if python_implementation() == 'CPython' and not TYPE_CHECKING:
    try:
        import brotli
    except ModuleNotFoundError:
        import brotlicffi as brotli
else:
    try:
        import brotlicffi as brotli
    except ModuleNotFoundError:
        import brotli

if TYPE_CHECKING:
    from collections.abc import Iterable

    from starlette.types import ASGIApp, Receive, Scope, Send


class _BrotliStreamEncoder:
    __slots__ = ('_compressor',)

    def __init__(self, quality: int) -> None:
        self._compressor = brotli.Compressor(quality=quality)

    def feed(self, data: bytes, flush: bool) -> Iterable[bytes]:
        out = self._compressor.process(data)
        if flush:
            out += self._compressor.flush()
        if out:
            yield out

    def finish(self) -> Iterable[bytes]:
        out = self._compressor.finish()
        if out:
            yield out


class BrotliResponder:
    __slots__ = ('_responder',)

    def __init__(self, app: ASGIApp, minimum_size: int, quality: int) -> None:
        def oneshot(body: bytes) -> bytes:
            return brotli.compress(body, quality=quality)

        def create_encoder(content_length: int) -> _BrotliStreamEncoder:
            return _BrotliStreamEncoder(quality)

        self._responder = CompressionResponder(
            app, minimum_size, 'br', oneshot, create_encoder
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._responder(scope, receive, send)
