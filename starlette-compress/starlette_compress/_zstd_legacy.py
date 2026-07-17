from __future__ import annotations

from zstandard import ZstdCompressor  # type: ignore

from starlette_compress._responder import CompressionResponder

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Iterable

    from starlette.types import ASGIApp, Receive, Scope, Send
    from zstandard import ZstdCompressionChunker  # type: ignore


class _ZstdLegacyStreamEncoder:
    """Streaming encoder backed by zstandard's chunker (bounded output).

    The size pledge applies only when a known Content-Length was supplied
    (buffered streams); streaming always passes ``-1``.

    Per-message flush uses ``chunker.flush()`` with no argument. Never pass
    module-level ``zstandard.FLUSH_BLOCK`` (it equals COMPRESSOBJ_FLUSH_FINISH
    and ends the frame).
    """

    __slots__ = ('_chunker',)

    def __init__(self, chunker: ZstdCompressionChunker) -> None:
        self._chunker = chunker

    def feed(self, data: bytes, flush: bool) -> Iterable[bytes]:
        yield from self._chunker.compress(data)
        if flush:
            yield from self._chunker.flush()

    def finish(self) -> Iterable[bytes]:
        yield from self._chunker.finish()


class ZstdResponder:
    __slots__ = ('_responder',)

    def __init__(self, app: ASGIApp, minimum_size: int, level: int) -> None:
        compressor = ZstdCompressor(level=level)

        def oneshot(body: bytes) -> bytes:
            return compressor.compress(body)

        def create_encoder(content_length: int) -> _ZstdLegacyStreamEncoder:
            chunker = ZstdCompressor(level=level).chunker(content_length)
            return _ZstdLegacyStreamEncoder(chunker)

        self._responder = CompressionResponder(
            app, minimum_size, 'zstd', oneshot, create_encoder
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._responder(scope, receive, send)
