# starlette-compress

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/starlette-compress)](https://pypi.org/p/starlette-compress)

**starlette-compress** is a fast and simple middleware for compressing responses in [Starlette](https://www.starlette.io). It supports more compression algorithms than Starlette's built-in GZipMiddleware, and has more sensible defaults.

- Python 3.9+ support
- Python 3.14 `compression.zstd` support
- Compatible with `asyncio` and `trio` backends
- ZStd, Brotli, and GZip compression
- Sensible default configuration
- [Zero-Clause BSD](https://choosealicense.com/licenses/0bsd/) — public domain dedication
- [Semantic Versioning](https://semver.org) compliance

## Installation

```sh
pip install starlette-compress
```

## Basic Usage

### Starlette

```py
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette_compress import CompressMiddleware

middleware = [
    Middleware(CompressMiddleware)
]

app = Starlette(routes=..., middleware=middleware)
```

### FastAPI

You can use starlette-compress with [FastAPI](https://fastapi.tiangolo.com) too:

```py
from fastapi import FastAPI
from starlette_compress import CompressMiddleware

app = FastAPI()
app.add_middleware(CompressMiddleware)
```

## Advanced Usage

### Changing Minimum Response Size

Control the minimum size of the response to compress. By default, responses must be at least 500 bytes to be compressed. Streaming-registered types (e.g. `text/event-stream`) bypass this threshold and are always compressed when accepted.

```py
# Starlette
middleware = [
    Middleware(CompressMiddleware, minimum_size=1000)
]

# FastAPI
app.add_middleware(CompressMiddleware, minimum_size=1000)
```

### Tuning Compression Levels

Adjust the compression levels for each algorithm. Higher levels mean smaller files but slower compression. Default level is 4 for all algorithms.

```py
# Starlette
middleware = [
    Middleware(CompressMiddleware, zstd_level=6, brotli_quality=6, gzip_level=6)
]

# FastAPI
app.add_middleware(CompressMiddleware, zstd_level=6, brotli_quality=6, gzip_level=6)
```

### Supporting Custom Content-Types

Manage the supported content-types. Unknown response types are not compressed. [Check here](https://github.com/Zaczero/pkgs/blob/main/starlette-compress/starlette_compress/_utils.py) for the default configuration.

```py
from starlette_compress import add_compress_type, remove_compress_type

add_compress_type("application/my-custom-type")
remove_compress_type("application/json")
```

### Server-Sent Events (SSE)

`text/event-stream` responses are compressed by default. The middleware flushes the compressor after each non-empty ASGI body message (it does not parse SSE frames — two events in one message share one flush; empty continuation messages are skipped). Streaming types bypass `minimum_size`. The middleware forwards `http.response.start` immediately so clients such as `EventSource` can open before the first event — though ASGI servers may still buffer that start until the first body. On pathsend-capable scopes, start is deferred to the first content message (body or pathsend) so a file send is never advertised as compressed.

To opt out:

```py
from starlette_compress import remove_compress_type

remove_compress_type("text/event-stream")
```

NDJSON / JSONL are **not registered by default** (they are neither compressed nor treated as streaming until added). Opt in when you need compression and/or low-latency line delivery:

```py
from starlette_compress import add_compress_type

add_compress_type("application/x-ndjson", streaming=True)
add_compress_type("application/jsonl", streaming=True)
```

Flush guarantees that compressed bytes are available from the codec after each non-empty message; it does not control proxy or CDN buffering (this package does not set `X-Accel-Buffering` or similar).

### Dropping Accept-Encoding Header

By default, starlette-compress leaves the request `Accept-Encoding` header intact. Set
`remove_accept_encoding=True` to remove it before calling downstream middleware/application.
