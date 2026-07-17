from __future__ import annotations

import gzip
import zlib

from starlette_compress._responder import CompressionResponder

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Iterable

    from starlette.types import ASGIApp, Receive, Scope, Send


class _GzipStreamEncoder:
    __slots__ = ('_obj',)

    def __init__(self, level: int) -> None:
        self._obj = zlib.compressobj(level, zlib.DEFLATED, 31)

    def feed(self, data: bytes, flush: bool) -> Iterable[bytes]:
        out = self._obj.compress(data)
        if flush:
            out += self._obj.flush(zlib.Z_SYNC_FLUSH)
        if out:
            yield out

    def finish(self) -> Iterable[bytes]:
        out = self._obj.flush(zlib.Z_FINISH)
        if out:
            yield out


class GZipResponder:
    __slots__ = ('_responder',)

    def __init__(self, app: ASGIApp, minimum_size: int, level: int) -> None:
        def oneshot(body: bytes) -> bytes:
            return gzip.compress(body, compresslevel=level)

        def create_encoder(content_length: int) -> _GzipStreamEncoder:
            return _GzipStreamEncoder(level)

        self._responder = CompressionResponder(
            app, minimum_size, 'gzip', oneshot, create_encoder
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._responder(scope, receive, send)
