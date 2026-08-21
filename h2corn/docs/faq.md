---
description: Short answers about h2c, HTTP/1.1, HTTP/3, Windows, Django, gRPC, and where to report bugs.
---

# FAQ

## Should I expose `h2corn` directly to the internet?

Direct public exposure requires h2corn to terminate TLS. The deployment then
supplies certificate renewal, ALPN, firewall policy, request limits, abuse
controls, and edge observability. A reverse proxy keeps those controls at the
edge; see [Behind a proxy](deployment/proxy.md#behind-a-reverse-proxy). Direct exposure uses
[Direct TLS](deployment/tls.md#direct-tls).

## Why prefer `h2c` upstream instead of HTTP/1.1?

It keeps the proxy → app hop on HTTP/2 instead of translating requests
back down to HTTP/1.1 before they reach the application server, removing
a protocol-conversion boundary where
HTTP/1.1 framing ambiguity and connection-reuse issues can reappear.
TLS, peer isolation, and application validation remain required.
PortSwigger documents [request smuggling](https://portswigger.net/web-security/request-smuggling)
and [HTTP/2 downgrading](https://portswigger.net/web-security/request-smuggling/advanced/http2-downgrading)
risks.

## Why not HTTP/3?

HTTP/3's gains — connection migration, head-of-line resilience, faster
handshakes — mostly matter at the public edge, where network
conditions vary and clients churn. On a short, trusted internal
connection between a reverse proxy and an application server, the
benefits shrink while the cost (UDP, QUIC stack, broader attack
surface) does not, and `h2c` has wider proxy support.

## Why is HTTP/1.1 even an option?

Browsers do not speak cleartext `h2c`. Without TLS in front, a browser
cannot talk directly to an `h2c`-only server, so HTTP/1.1 is kept
available for **local development and testing**. In production, HTTP/1.1 is
disabled with [`--no-http1`](configuration.md#option-http1) when every upstream client speaks h2c or HTTP/2; it
remains enabled when a proxy uses an HTTP/1.1 WebSocket upgrade route, as
described in [Behind a reverse proxy](deployment/proxy.md#behind-a-reverse-proxy).

## Is HTTP/1.0 supported?

No. `h2corn` speaks HTTP/1.1 and HTTP/2; an `HTTP/1.0` request-line is
answered with `400 Bad Request`.

HTTP/1.0 is a second framing regime: responses are delimited by connection
close, with no chunked transfer coding, no keep-alive by default, and no
`100-continue`. HTTP/1.0 clients in front of `h2corn` require termination at
the proxy.

## Does this work on Windows?

Yes, but the full Unix-style worker supervisor does not. On Windows,
`h2corn` always runs in single-worker, in-process mode. Linux and macOS
get the multi-worker supervisor with signals, rolling reload, and live
scaling.

## Can I use it with Django?

Yes — point `h2corn` at the Django ASGI application. Replace
`myproject.asgi:application` with your project's import target:

```bash
h2corn myproject.asgi:application --workers 4 --no-http1
```

Django Channels and other ASGI frameworks can use the same ASGI boundary;
validate their lifespan, WebSocket, and middleware behavior before deployment.

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

`h2corn` is an ASGI server, not a gRPC server. It has no built-in gRPC
dispatcher, and no gRPC-to-ASGI bridge is verified by this project. A
bridge may be usable if it exposes a conforming ASGI application, but treat
that combination as unverified: test HTTP/2 trailers, streaming, deadlines,
metadata, and cancellation with the specific bridge and framework before
deploying it.

## Where do I report bugs or request features?

In the project's [GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).
For paid help with deployment, upgrades, or performance work, see the
[Support](support.md#support) page.
