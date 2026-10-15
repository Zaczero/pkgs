# starlette-compress

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/starlette-compress)](https://pypi.org/p/starlette-compress)

**starlette-compress** compresses ASGI HTTP responses with Zstandard, Brotli, or
Gzip. Use it with [Starlette](https://www.starlette.io), FastAPI, or another
ASGI application on Python 3.10+.

- Python 3.10+ support
- Python 3.14 `compression.zstd` support
- Compatible with `asyncio` and `trio` backends
- ZStd, Brotli, and GZip compression
- [Zero-Clause BSD license](https://choosealicense.com/licenses/0bsd/)
- [Semantic Versioning](https://semver.org) compliance

## Installation

```sh
pip install starlette-compress
```

## Basic Usage

### Starlette

Install the HTTP test client for this self-contained example:

```sh
pip install httpx2
```

The script makes an in-process request and inspects compressed bytes before
client-side automatic decoding:

```py
import gzip

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient
from starlette_compress import CompressMiddleware

payload = "Hello from a compressed response!\n" * 100

async def homepage(request):
    return PlainTextResponse(payload)

app = Starlette(
    routes=[Route("/", homepage)],
    middleware=[Middleware(CompressMiddleware)],
)
with TestClient(app) as client:
    with client.stream("GET", "/", headers={"Accept-Encoding": "gzip"}) as response:
        wire = b"".join(response.iter_raw())
        assert response.headers["content-encoding"] == "gzip"
        assert int(response.headers["content-length"]) == len(wire)
        assert gzip.decompress(wire).decode() == payload
        print(response.status_code, response.headers["content-encoding"])
```

This prints `200 gzip`. To serve the application, use your ASGI server's
application-loading instructions. TLS termination and proxy buffering belong
to that deployment.

### FastAPI

Install `fastapi` to use this complete application definition:

```py
from fastapi import FastAPI
from starlette_compress import CompressMiddleware

app = FastAPI()
app.add_middleware(CompressMiddleware)

@app.get("/")
async def homepage():
    return {"message": "Hello!" * 200}
```

## Advanced Usage

### Changing Minimum Response Size

`minimum_size` applies to an ordinary one-shot body: a terminal first body
message below the threshold is forwarded unchanged. Once an ordinary response
sends `more_body=True`, compression begins without buffering to reach the
threshold, even if the entire response is small. Streaming-registered types
(e.g. `text/event-stream`) bypass the threshold for one-shot bodies too.
See the [constructor reference](starlette_compress/__init__.py) for defaults.

```py
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette_compress import CompressMiddleware

# Starlette
middleware = [
    Middleware(CompressMiddleware, minimum_size=1000)
]
app = Starlette(middleware=middleware)
```

### Tuning Compression Levels

Adjust compression levels to trade compression work for output size. Supported
levels and defaults are in the [constructor reference](starlette_compress/__init__.py).

```py
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette_compress import CompressMiddleware

middleware = [
    Middleware(CompressMiddleware, zstd_level=6, brotli_quality=6, gzip_level=6)
]
app = Starlette(middleware=middleware)
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

## Negotiation and Response Policy

Enabled algorithms are selected in server priority order: Zstandard, Brotli,
then Gzip. A positive `q` makes a coding eligible; relative quality values do
not reorder that priority. For example, `zstd;q=0.1, gzip;q=1` selects Zstandard.
Non-positive or unparseable `q` values exclude a coding; `*` enables supported
codings except explicit exclusions. Multiple `Accept-Encoding` fields are
combined. No header, no eligible enabled coding, or `identity` alone uses the
identity path. This middleware falls back to identity rather than returning
406 when no enabled coding is eligible.

Only registered media types are eligible (parameters and case are ignored).
Missing or unknown types, informational responses, status 204/205/304,
`Content-Range`, and an existing non-identity `Content-Encoding` bypass
compression. A delayed-start response whose first body is terminal gets its
encoded `Content-Length`; streaming paths remove it. Eligible body responses
merge `Accept-Encoding` into `Vary`, including the identity path, except
ordinary bodies below threshold. Raw pathsend forwarding preserves the
original headers.

Ordinary streams have no per-message flush guarantee. Streaming-registered
types flush after each non-empty continuing ASGI body message and finish on
the terminal message; omitted `more_body` means false. Configure the
process-wide registry before serving requests. Registration is
case-insensitive and idempotent; a plain re-add retains streaming membership,
and removal clears both memberships.

`http.response.pathsend` is forwarded uncompressed when it is the first
content message. The identity path preserves zero-copy capability; an encoded
path removes `http.response.zerocopysend` from the downstream scope so raw
file bytes cannot be appended to an encoded response.

Middleware instances can be shared across event-loop threads. Legacy
Zstandard operations own their compressor; Python 3.14+ uses
`compression.zstd`. CPython uses `brotli`, and other Python implementations
use `brotlicffi`. Codec configuration and encoding errors propagate to the
application/server; a failed one-shot compression does not commit compressed
headers.

## Reference and Support

The [public middleware constructor](starlette_compress/__init__.py) owns
call forms, defaults, and level ranges. The public
[`add_compress_type` and `remove_compress_type` reference](starlette_compress/_utils.py)
owns registry operations and lists default media types.
Report problems in the [issue tracker](https://github.com/Zaczero/pkgs/issues),
including interpreter, backend, response type, and a minimal application.
