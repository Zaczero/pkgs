---
description: Precisely typed ASGI scope, message and application aliases exported by h2corn.
---

# Types

::: h2corn.ASGIApp
    options:
      show_signature: false

::: h2corn.FrameworkASGIApp
    options:
      show_signature: false

::: h2corn.Application
    options:
      show_signature: false

`Application` is the accepted union of the precise [`ASGIApp`][h2corn.ASGIApp]
and the broader [`FrameworkASGIApp`][h2corn.FrameworkASGIApp] boundary.

[`ASGIApp`][h2corn.ASGIApp] uses h2corn's precise, discriminated scope and
message unions. [`FrameworkASGIApp`][h2corn.FrameworkASGIApp] matches the
broader mutable-mapping annotations used by FastAPI, Starlette, Django, and
similar frameworks; both are accepted anywhere h2corn accepts an application.

::: h2corn.Scope
    options:
      show_signature: false

::: h2corn.ReceiveMessage
    options:
      show_signature: false

::: h2corn.SendMessage
    options:
      show_signature: false

::: h2corn.Message
    options:
      show_signature: false

::: h2corn.Receive
    options:
      show_signature: false

::: h2corn.Send
    options:
      show_signature: false

::: h2corn.ASGIVersions
    options:
      show_signature: false

::: h2corn.HTTPASGIVersions
    options:
      show_signature: false

::: h2corn.LifespanASGIVersions
    options:
      show_signature: false

::: h2corn.HTTPScope
    options:
      show_signature: false

::: h2corn.WebSocketScope
    options:
      show_signature: false

::: h2corn.LifespanScope
    options:
      show_signature: false

::: h2corn.Headers
    options:
      show_signature: false

::: h2corn.ScopeHeaders
    options:
      show_signature: false

::: h2corn.State
    options:
      show_signature: false

::: h2corn.ExtensionParameters
    options:
      show_signature: false

::: h2corn.Extensions
    options:
      show_signature: false

::: h2corn.HTTPExtensions
    options:
      show_signature: false

::: h2corn.WebSocketExtensions
    options:
      show_signature: false

::: h2corn.TLSExtension
    options:
      show_signature: false

`HTTPExtensions` and `WebSocketExtensions` show the capabilities for the
current connection. `http.response.zerocopysend` is Unix-only,
`http.response.trailers` requires the peer's `TE: trailers`, and
`http.response.early_hint` is HTTP/2-only. `tls` is present only when h2corn
terminated TLS. Extension parameter mappings are shared server metadata and
must be treated as read-only; the outer `scope["extensions"]` mapping remains
the per-scope place for an application's namespaced key.

## Extension mapping fields

ASGI extension keys use dotted names and map to their parameter types.

### `HTTPExtensions["http.response.pathsend"]` { #h2corn.HTTPExtensions.http.response.pathsend }

Advertised on HTTP scopes. The application may send
`http.response.pathsend` when the server should open the named file as the
terminal response body.

### `HTTPExtensions["http.response.zerocopysend"]` { #h2corn.HTTPExtensions.http.response.zerocopysend }

Advertised on Unix HTTP listeners. The application may send a file descriptor
or range without routing its bytes through Python; see [Sending a file](../asgi.md#sending-a-file).

### `HTTPExtensions["http.response.trailers"]` { #h2corn.HTTPExtensions.http.response.trailers }

Advertised when the HTTP client sent `TE: trailers`; the application may then
send HTTP response trailers after the body.

### `HTTPExtensions["http.response.early_hint"]` { #h2corn.HTTPExtensions.http.response.early_hint }

Advertised for HTTP/2 requests. The application may send RFC 8297 `103 Early
Hints` before the final response.

### `HTTPExtensions["tls"]` { #h2corn.HTTPExtensions.tls }

Present only when h2corn terminated TLS on the connection. Its value is the
[`TLSExtension`][h2corn.TLSExtension] mapping; a TLS-terminating proxy does not
produce this key.

### `WebSocketExtensions["websocket.http.response"]` { #h2corn.WebSocketExtensions.websocket.http.response }

Advertised on every WebSocket scope. Its value is the empty
`ExtensionParameters` mapping for the HTTP response extension.

### `WebSocketExtensions["tls"]` { #h2corn.WebSocketExtensions.tls }

Present only when h2corn terminated TLS on the WebSocket connection, with the
same [`TLSExtension`][h2corn.TLSExtension] value and proxy boundary as HTTP.

## Receive events

::: h2corn.HTTPRequest
    options:
      show_signature: false

::: h2corn.HTTPDisconnect
    options:
      show_signature: false

::: h2corn.WebSocketConnect
    options:
      show_signature: false

::: h2corn.WebSocketReceiveBytes
    options:
      show_signature: false

::: h2corn.WebSocketReceiveText
    options:
      show_signature: false

::: h2corn.WebSocketDisconnect
    options:
      show_signature: false

::: h2corn.LifespanStartup
    options:
      show_signature: false

::: h2corn.LifespanShutdown
    options:
      show_signature: false

## HTTP response events

::: h2corn.HTTPResponseStart
    options:
      show_signature: false

::: h2corn.HTTPResponseBody
    options:
      show_signature: false

::: h2corn.HTTPResponseTrailers
    options:
      show_signature: false

::: h2corn.HTTPResponsePathsend
    options:
      show_signature: false

::: h2corn.HTTPResponseZeroCopySend
    options:
      show_signature: false

::: h2corn.HTTPResponseEarlyHint
    options:
      show_signature: false

## WebSocket response events

::: h2corn.WebSocketAccept
    options:
      show_signature: false

::: h2corn.WebSocketSendBytes
    options:
      show_signature: false

::: h2corn.WebSocketSendText
    options:
      show_signature: false

::: h2corn.WebSocketClose
    options:
      show_signature: false

::: h2corn.WebSocketHTTPResponseStart
    options:
      show_signature: false

::: h2corn.WebSocketHTTPResponseBody
    options:
      show_signature: false

## Lifespan response events

::: h2corn.LifespanStartupComplete
    options:
      show_signature: false

::: h2corn.LifespanStartupFailed
    options:
      show_signature: false

::: h2corn.LifespanShutdownComplete
    options:
      show_signature: false

::: h2corn.LifespanShutdownFailed
    options:
      show_signature: false

```python title="partial type-checking fragment"
from h2corn import ASGIApp, Receive, ReceiveMessage, Scope, Send


def request_body(message: ReceiveMessage) -> bytes:
    if message["type"] == "http.request":
        return message.get("body", b"")
    return b""


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    ...


typed_app: ASGIApp = app
```

The wire contracts follow the ASGI
[HTTP and WebSocket](https://asgi.readthedocs.io/en/latest/specs/www.html) and
[lifespan](https://asgi.readthedocs.io/en/latest/specs/lifespan.html)
specifications.
`h2corn` accepts callables matching either `ASGIApp` or `FrameworkASGIApp`,
including
[FastAPI](https://fastapi.tiangolo.com/),
[Starlette](https://www.starlette.io/),
[Django](https://docs.djangoproject.com/en/stable/howto/deployment/asgi/) (`asgi.application`),
[Litestar](https://litestar.dev/), and
[Quart](https://quart.palletsprojects.com/).
