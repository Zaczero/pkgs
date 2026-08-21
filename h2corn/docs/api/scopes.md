---
description: ASGI version, header, state, and extension types shared by h2corn scopes.
---

# Scopes and extensions

These types describe values shared by the HTTP, WebSocket, and lifespan scope
contracts. Extension mappings are server metadata and should be treated as
read-only; the outer `scope["extensions"]` mapping remains the per-scope place
for an application's namespaced key.

::: h2corn.ASGIVersions
    options:
      show_signature: false

::: h2corn.HTTPASGIVersions
    options:
      show_signature: false

::: h2corn.LifespanASGIVersions
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

::: h2corn.Extensions
    options:
      show_signature: false

::: h2corn.HTTPExtensions
    options:
      show_signature: false

::: h2corn.WebSocketExtensions
    options:
      show_signature: false

::: h2corn.ExtensionParameters
    options:
      show_signature: false

::: h2corn.TLSExtension
    options:
      show_signature: false

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
`TLSExtension` mapping; a TLS-terminating proxy does not
produce this key.

### `WebSocketExtensions["websocket.http.response"]` { #h2corn.WebSocketExtensions.websocket.http.response }

Advertised on every WebSocket scope. Its value is the empty
`ExtensionParameters` mapping for the HTTP response extension.

### `WebSocketExtensions["tls"]` { #h2corn.WebSocketExtensions.tls }

Present only when h2corn terminated TLS on the WebSocket connection, with the
same `TLSExtension` value and proxy boundary as HTTP.
