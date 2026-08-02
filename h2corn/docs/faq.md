---
description: Short answers about h2c, HTTP/1.1, HTTP/3, Windows, Django, gRPC, and where to report bugs.
---

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

## Which ASGI extensions are supported?

`pathsend`, `zerocopysend`, `trailers`, `early_hint`, `websocket.http.response`
and `tls`, each offered where it can be delivered — see the
[ASGI surface](asgi.md#extensions) for the conditions and for sending files.

## What can an ASGI `send()` call raise?

`TypeError` for a field of the wrong type, `ValueError` for a malformed value,
`RuntimeError` for an invalid message sequence, and `OSError` after the stream
has closed. The [full mapping](asgi.md#what-send-raises) lists what falls under
each — it differs from Uvicorn, which reports several malformed values as
`RuntimeError`.

## Does `h2corn` support gRPC?

`h2corn` is an ASGI server, not a gRPC server. It speaks HTTP/2 framing
correctly, but there is no built-in gRPC dispatcher; if your application
exposes gRPC endpoints via an ASGI-compatible bridge, those will work
like any other ASGI handler.

## Where do I report bugs or request features?

In the project's [GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).
For paid help with deployment, upgrades, or performance work, see the
[Support](support.md) page.
