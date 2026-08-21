---
description: The ASGI surface h2corn presents to an application — advertised extensions, sending files, lifespan state, and what send() raises.
---

# ASGI surface

`h2corn` implements [ASGI 3](https://asgi.readthedocs.io/en/latest/specs/main.html).
Server-specific behavior is its advertised extensions, file-send paths, and
the exceptions raised by invalid `await send(...)` messages.

## Extensions

Extensions are advertised per connection under `scope["extensions"]`, and several are offered only
where they can be delivered:

| Extension | Scope | Offered when |
| --------- | ----- | ------------ |
| `http.response.pathsend` | HTTP | Always |
| `http.response.zerocopysend` | HTTP | Unix only |
| `http.response.trailers` | HTTP | The client sent `TE: trailers` |
| `http.response.early_hint` | HTTP | The request is HTTP/2 |
| `websocket.http.response` | WebSocket | Always |
| `tls` | HTTP and WebSocket | `h2corn` terminated the TLS itself |

The `tls` entry carries the negotiated connection parameters, including any client certificate —
see [Direct TLS](deployment/tls.md#reading-the-connection-from-an-application).

## Sending a file

Two extensions send a file without routing its bytes through Python. Use `pathsend` when the server
should open the file, and `zerocopysend` when you already hold a descriptor, or want to send a
*range* of one — which `pathsend` cannot express.

`pathsend` is terminal: it is the whole response body, and mixing it with `http.response.body`
raises. `zerocopysend` is not — it may be sent repeatedly and interleaved with
`http.response.body`:

```python title="partial application fragment"
async def app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    with open("/srv/media/clip.mp4", "rb") as handle:
        await send({
            "type": "http.response.zerocopysend",
            "file": handle,
            "offset": 1024,
            "count": 4096,
        })
```

The descriptor stays yours. `h2corn` duplicates it and closes only its own copy, so you may close
yours as soon as `send()` returns, which is what the ASGI specification asks of an application. Your
file position is never moved.

The range is sized from the descriptor's reported size, so a file whose size is not its length —
anything under `/proc` or `/sys` — is rejected rather than served as an empty body. Read those and
send them with `http.response.body`.

Zero-copy is best-effort, and the buffered path is byte-for-byte identical on
the wire. TLS, Unix sockets, non-Linux platforms, and small ranges use the
buffered path. On HTTP/1, a segment that is not the entire body — one mixed with
`http.response.body` or followed by trailers — is also streamed through a
rolling read rather than `sendfile`.

## Lifespan state

The lifespan scope has no `extensions` key. `scope["state"]` is the namespace
the application fills during startup:

```python title="partial lifespan fragment"
async def lifespan(scope, receive, send):
    message = await receive()          # lifespan.startup
    scope["state"]["pool"] = await open_pool()
    await send({"type": "lifespan.startup.complete"})
```

Each request scope then receives a **shallow copy** of that namespace as `scope["state"]`. The
values are shared — a connection pool stored at startup is the same pool every request sees — but
the mapping is not: a key a request adds to its own `scope["state"]` is invisible to the lifespan
namespace and to every other request.

## What `send()` raises

`h2corn` validates each outbound message at `await send(message)`.

| Exception | Raised for |
| --------- | ---------- |
| `TypeError` | A field of the wrong Python type. |
| `ValueError` | A malformed value: a missing required field, an invalid response header or trailer field, a non-final or non-three-digit status, an invalid WebSocket close value, an empty accepted subprotocol, or a `websocket.send` setting neither or both payload fields. |
| `RuntimeError` | An invalid message sequence — a body before `http.response.start`, `http.response.pathsend` mixed with a body, trailers at the wrong point, an unexpected WebSocket event. Also a handshake that timed out, a compression failure, or an application that ended before completing its response. |
| `OSError` | `send()` called after the stream has closed. |

An exception raised by the application itself propagates unchanged.

`ValueError` also covers a `websocket.accept` that names a subprotocol the client did not offer, or
that carries a header the handshake owns. Both are raised from the `send()` call that supplied them,
so an application can catch one and accept a subprotocol the client *did* offer instead.

Catch `ValueError` around `send()` when an application means to replace a malformed outbound message
with a valid one. Uvicorn raises `RuntimeError` for several of these, so a handler ported from it may
be catching the wrong type.
