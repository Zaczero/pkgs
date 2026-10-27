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

It keeps HTTP/2 between the proxy and application, avoiding a downgrade to
HTTP/1.1 where framing ambiguities can introduce
[request smuggling](https://portswigger.net/web-security/request-smuggling/advanced/http2-downgrading)
risks. The public edge still needs TLS, the cleartext `h2c` hop needs peer
isolation, and the application remains responsible for request validation.

## Why not HTTP/3?

HTTP/3's gains — connection migration, head-of-line resilience, faster
handshakes — mostly matter at the public edge, where network
conditions vary and clients churn. On a short, trusted internal
connection between a reverse proxy and an application server, the
benefits shrink while the cost (UDP, QUIC stack, broader attack
surface) does not, and `h2c` has wider proxy support.

<a id="why-is-http11-even-an-option"></a>

## Does h2corn support HTTP/1.1?

Yes. HTTP/1.1 is enabled by default on the same listener as HTTP/2, including
for WebSocket upgrades. It also lets browsers connect directly without TLS;
browsers do not speak cleartext `h2c`.

Prefer HTTP/2 where your clients and proxy support it. You can disable HTTP/1.1
with [`--no-http1`](configuration.md#option-http1) when every route uses HTTP/2.
WebSocket routes then need RFC 8441 extended CONNECT support from the client
and proxy. See [Behind a reverse proxy](deployment/proxy.md#behind-a-reverse-proxy).

## Is HTTP/1.0 supported?

No. `h2corn` speaks HTTP/1.1 and HTTP/2; an `HTTP/1.0` request-line is
answered with `400 Bad Request`.

Use a proxy that translates HTTP/1.0 clients to HTTP/1.1 or HTTP/2 upstream.

## Which HTTP/1.1 requests require a `Host` field?

Every HTTP/1.1 request must include exactly one `Host` field, including
absolute-form requests and `CONNECT`. An absolute-form target whose authority
does not match `Host` receives `400 Bad Request`. A valid `CONNECT` request
with `Host` receives `501 Not Implemented` because h2corn does not provide
HTTP/1.1 tunnels.

## Does this work on Windows?

Yes, but the full Unix-style worker supervisor does not. On Windows,
`h2corn` always runs in single-worker, in-process mode. Linux and macOS
get the multi-worker supervisor with signals, rolling reload, and live
scaling.

## Can I use it with Django?

Pass your Django ASGI import target:

```bash
uv run h2corn myproject.asgi:application --workers 4
```

Django Channels and other ASGI frameworks use the same integration boundary.
For WebSockets, choose HTTP/1.1 or an HTTP/2 path that supports RFC 8441
extended CONNECT.

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

Yes. h2corn hosts ASGI applications that serve Connect, native gRPC, and
gRPC-Web. Enable native gRPC admission with the [`grpc` option](configuration.md#option-grpc);
Connect and gRPC-Web use the ordinary HTTP application path. Your RPC framework
provides service routing and protocol behavior. See the
[gRPC and Connect guide](grpc.md).

## Where do I report bugs or request features?

In the project's [GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).
For paid help with deployment, upgrades, or performance work, see the
[Support](support.md#support) page.
