# starlette-compress

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/starlette-compress)](https://pypi.org/p/starlette-compress)

**starlette-compress** compresses ASGI HTTP responses with Zstandard, Brotli, or
Gzip. Use it with [Starlette](https://www.starlette.io), FastAPI, or another
ASGI application on Python 3.11+.

Works with asyncio and Trio, including streaming responses and server-sent events.

## Installation

```sh
pip install starlette-compress
```

<a id="basic-usage"></a>

## Add the middleware

### Starlette

```py
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette_compress import CompressMiddleware

async def homepage(request):
    return PlainTextResponse("Hello!" * 200)

app = Starlette(
    routes=[Route("/", homepage)],
    middleware=[Middleware(CompressMiddleware)],
)
```

Serve `app` with your ASGI server. Responses are compressed according to the
client's `Accept-Encoding` header.

### FastAPI

```py
from fastapi import FastAPI
from starlette_compress import CompressMiddleware

app = FastAPI()
app.add_middleware(CompressMiddleware)

@app.get("/")
async def homepage():
    return {"message": "Hello!" * 200}
```

<a id="advanced-usage"></a>

## Configure compression

### Changing minimum response size

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

middleware = [
    Middleware(CompressMiddleware, minimum_size=1000)
]
app = Starlette(middleware=middleware)
```

### Tuning compression levels

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

### Supporting custom content types

Unknown content types are not compressed. Add or remove types before serving
requests; see the [default media types](starlette_compress/_utils.py).

```py
from starlette_compress import add_compress_type, remove_compress_type

add_compress_type("application/my-custom-type")
remove_compress_type("application/json")
```

### Server-sent events (SSE)

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

### Dropping Accept-Encoding header

By default, starlette-compress leaves the request `Accept-Encoding` header intact. Set
`remove_accept_encoding=True` to remove it before calling downstream middleware/application.

<a id="negotiation-and-response-policy"></a>

## Response behavior

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

Middleware instances can be shared across event-loop threads. Codec errors
propagate to the application/server.

## Reference and Support

The [middleware reference](starlette_compress/__init__.py) lists
defaults and compression-level ranges. The
[`add_compress_type` and `remove_compress_type` reference](starlette_compress/_utils.py)
describes registry operations and default media types.

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
