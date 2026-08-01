# FAQ

## Should I expose `h2corn` directly to the internet?

Either works. The common topology runs `h2corn` behind a reverse
proxy that handles TLS and public-edge hardening — see
[Behind a proxy](deployment/proxy.md). When that isn't a fit, `h2corn`
can terminate TLS itself with [Direct TLS](deployment/tls.md). Browsers
don't speak cleartext `h2c`, so the edge has to advertise HTTPS one way
or the other.

## Why prefer `h2c` upstream instead of HTTP/1.1?

It keeps the proxy → app hop on a modern protocol instead of
translating requests back down to HTTP/1.1 before they reach the
application server, removing a protocol-conversion boundary where
HTTP/1.1 framing ambiguity and connection-reuse issues can reappear.
PortSwigger's [request smuggling](https://portswigger.net/web-security/request-smuggling)
and [HTTP/2 downgrading](https://portswigger.net/web-security/request-smuggling/advanced/http2-downgrading)
material is a useful reference.

## Why not HTTP/3?

HTTP/3's gains — connection migration, head-of-line resilience, faster
handshakes — mostly matter at the public edge, where network
conditions vary and clients churn. On a short, trusted internal
connection between a reverse proxy and an application server, the
benefits shrink while the cost (UDP, QUIC stack, broader attack
surface) does not. `h2c` is simpler, more widely supported, and a
better fit for that hop.

## Why is HTTP/1.1 even an option?

Browsers do not speak cleartext `h2c`. Without TLS in front, a browser
cannot talk directly to an `h2c`-only server, so HTTP/1.1 is kept
available for **local development and testing**. In production,
disable it with `--no-http1`.

## Is HTTP/1.0 supported?

No. `h2corn` speaks HTTP/1.1 and HTTP/2; an `HTTP/1.0` request-line is
answered with `400 Bad Request`.

HTTP/1.0 is a second framing regime — responses delimited by connection
close, no chunked transfer coding, no keep-alive by default, no
`100-continue` — and carrying it through the whole response path costs
more than the remaining HTTP/1.0 clients are worth. If something in front
of `h2corn` still speaks it, terminate it at the proxy.

## Does this work on Windows?

Yes, but the full Unix-style worker supervisor does not. On Windows,
`h2corn` always runs in single-worker, in-process mode. Linux and macOS
get the multi-worker supervisor with signals, rolling reload, and live
scaling.

## Can I use it with Django?

Yes — point `h2corn` at the Django ASGI application:

```bash
h2corn myproject.asgi:application --workers 4 --no-http1
```

Django channels and any other ASGI 3 framework work the same way.

## What can an ASGI `send()` call raise?

`h2corn` validates each outbound ASGI message at `await send(message)`. A
field of the wrong Python type raises `TypeError`. A malformed application
value raises `ValueError`: this includes a missing required field, an invalid
response header or trailer field, a non-final or non-three-digit response
status, an invalid WebSocket close value, an empty accepted subprotocol, or a
`websocket.send` message that sets neither or both payload fields.

`ValueError` also covers a `websocket.accept` that names a subprotocol the
client did not offer, or that carries a header the handshake owns — both are
raised from the `send()` call that supplied them, so an application can catch
one and accept a subprotocol the client did offer instead.

`RuntimeError` reports an invalid message sequence, such as sending a response
body before `http.response.start`, mixing `http.response.pathsend` with a
response body, sending trailers at the wrong point, or sending an unexpected
WebSocket event. It also covers conditions that are nobody's message in
particular: a handshake that timed out, a compression failure, an application
that ended before completing its response. Calling `send()` after the stream
has closed raises `OSError`. An exception raised by the application itself
propagates unchanged.

Every error variant is mapped to one of these four types explicitly; there is
no fall-through, so a type here is a decision rather than a default.

This differs from Uvicorn: it reports several malformed values as
`RuntimeError`. Catch `ValueError` around `send()` when an application intends
to replace a malformed outbound message with a valid one.

## Does `h2corn` support gRPC?

`h2corn` is an ASGI server, not a gRPC server. It speaks HTTP/2 framing
correctly, but there is no built-in gRPC dispatcher; if your application
exposes gRPC endpoints via an ASGI-compatible bridge, those will work
like any other ASGI handler.

## Where do I report bugs or request features?

In the project's [GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).
For paid help with deployment, upgrades, or performance work, see the
[Support](support.md) page.
