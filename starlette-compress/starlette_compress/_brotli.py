from __future__ import annotations

import sysconfig

from starlette_compress._responder import CompressionResponder

TYPE_CHECKING = False

_FREE_THREADED = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))

# The current PyPI brotli release lacks free-threaded module support.
if _FREE_THREADED:
    import brotlicffi as brotli
else:
    import brotli

if TYPE_CHECKING:
    from collections.abc import Iterable

    from starlette.types import ASGIApp


class _BrotliStreamEncoder:
    __slots__ = ('_compressor',)

    def __init__(self, quality: int) -> None:
        self._compressor = brotli.Compressor(quality=quality)

    def feed(self, data: bytes, flush: bool) -> Iterable[bytes]:
        out = self._compressor.process(data)
        if _FREE_THREADED:
            # Drain CFFI's fixed-size output buffer before the next input call.
            chunks = [out] if out else []
            while True:
                out = self._compressor.process(b'')
                if not out:
                    break
                chunks.append(out)
            if flush:
                out = self._compressor.flush()
                if out:
                    chunks.append(out)
            return tuple(chunks)
        if flush:
            out += self._compressor.flush()
        return (out,) if out else ()

    def finish(self) -> Iterable[bytes]:
        out = self._compressor.finish()
        return (out,) if out else ()


class BrotliResponder(CompressionResponder):
    __slots__ = ()

    def __init__(self, app: ASGIApp, minimum_size: int, quality: int) -> None:
        def oneshot(body: bytes) -> bytes:
            return brotli.compress(body, quality=quality)

        def create_encoder(content_length: int) -> _BrotliStreamEncoder:
            return _BrotliStreamEncoder(quality)

        super().__init__(app, minimum_size, 'br', oneshot, create_encoder)
