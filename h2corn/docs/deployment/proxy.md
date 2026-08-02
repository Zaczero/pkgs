---
description: Run h2corn behind nginx, Caddy or HAProxy with an h2c upstream, and get the forwarding headers right.
---

# Behind a reverse proxy

The most common production topology runs `h2corn` behind a reverse proxy
that handles browser-facing TLS:

```text
browser/client  →  reverse proxy (TLS edge)  →  h2corn (h2c)
```

The proxy takes care of ALPN negotiation, TLS termination, and
public-edge hardening. `h2corn` runs the application side of the
connection on `h2c` — cleartext HTTP/2 over TCP or a Unix socket inside the
trust boundary, established by
[prior knowledge](https://www.rfc-editor.org/rfc/rfc9113.html#section-3.3)
rather than an `Upgrade` handshake.

Keeping this hop on HTTP/2 avoids the HTTP/1.1 *downgrade* surface that
[request smuggling](https://portswigger.net/web-security/request-smuggling)
and [HTTP/2 downgrading](https://portswigger.net/web-security/request-smuggling/advanced/http2-downgrading)
research repeatedly targets. Where a proxy can speak `h2c` upstream, that beats
an HTTP/1.1 fallback.

When a separate proxy isn't a good fit for your environment, `h2corn`
can terminate TLS itself instead — see [Direct TLS](tls.md).

## Proxy headers and PROXY protocol

`h2corn` accepts two kinds of trust hop metadata, both opt-in and
gated by `--forwarded-allow-ips`:

- **`--proxy-headers`** trusts standard `Forwarded` and `X-Forwarded-*`
  headers from peers in `--forwarded-allow-ips`. These carry request
  metadata such as scheme, host, and the original client address.
- **`--proxy-protocol v1|v2`** parses HAProxy's
  [PROXY protocol](https://github.com/haproxy/haproxy/blob/master/doc/proxy-protocol.txt)
  on inbound connections. It carries transport-level peer information
  on the connection itself, useful when you want the original source IP
  for connection-level metrics or per-IP limits.

In most deployments, proxy headers alone are enough. Add PROXY protocol
when the upstream is explicitly configured to send it.

`h2corn` reads a forwarding header from the right, skipping hops that match
`--forwarded-allow-ips` and taking the first address outside that set. Values a
client prepends sit to the left of the ones your proxy adds, so they lose. A
proxy that *appends* rather than replaces — nginx's `proxy_add_x_forwarded_for`,
HAProxy's `option forwardfor`, Caddy's default — is the shape this expects.

Either spelling works, and `Forwarded` ([RFC 7239](https://www.rfc-editor.org/rfc/rfc7239.html))
takes precedence where both arrive. The configurations below set `X-Forwarded-*`
because that is what nginx and Caddy emit natively; HAProxy can send `Forwarded`
instead with `option forwarded`.

What that cannot do is judge a header your proxy never writes. If your proxy
sets `X-Forwarded-For` and `X-Forwarded-Proto` but nothing else, a client's own
`X-Forwarded-Prefix` becomes the application's `root_path`, its
`X-Forwarded-Host` becomes `scope["server"]`, and its `Forwarded` header
outranks all of the `X-Forwarded-*` ones. **Delete the forwarding headers your
proxy does not itself set** — `proxy_set_header <name> ""` in nginx,
`del-header` in HAProxy, `header_up -` in Caddy. All three configurations below
do this.

Without `--proxy-headers` none of them is trusted at all, which is the right
setting when nothing upstream writes them.

## nginx

[nginx](https://nginx.org/) speaks HTTP/2 to an upstream from **1.29.4**
onwards, where the changelog records
"*the ngx_http_proxy_module supports HTTP/2*". Set
[`proxy_http_version 2`](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_http_version)
and keep the upstream URL on `http://`, which makes that hop cleartext `h2c`.
It needs `ngx_http_v2_module`, which distribution builds normally include.
This module has carried
[security advisories](https://nginx.org/en/security_advisories.html) since it
shipped, so run a currently supported release and check that list against it.

Upstream multiplexing is not part of it yet. NGINX describes each upstream
connection as handling "*one request at a time rather than interleaving multiple
requests on a single connection*", and the wire agreed when this was measured:
ten overlapping requests arrived at `h2corn` on ten separate connections, while
sequential ones reused one keepalive connection. The win on this hop is HPACK and
unambiguous framing, not fewer connections. The browser-facing hop multiplexes
normally.

!!! warning "WebSockets need their own HTTP/1.1 location"
    nginx performs the HTTP/1.1 `Upgrade` handshake and does not translate it
    into the [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441) extended
    `CONNECT` that HTTP/2 requires (see [WebSockets](../websockets.md)), so a
    location on `proxy_http_version 2` cannot carry one. The configuration below
    routes `/ws` over HTTP/1.1 and everything else over `h2c`, so **do not pass
    `--no-http1`** with nginx if you serve WebSockets. For WebSockets over a
    pure HTTP/2 upstream, use [HAProxy](#haproxy).

```nginx title="nginx.conf"
--8<-- "nginx.conf"
```

Pair it with:

```bash
h2corn hello:app \
  --bind 127.0.0.1:8000 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1,::1
```

## Caddy

[Caddy](https://caddyserver.com/) speaks `h2c` upstream natively with
its
[`reverse_proxy`](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy)
directive.

!!! warning "Caddy cannot carry WebSockets over `h2c`"
    Caddy forwards the client's `Upgrade` header unchanged rather than
    translating it into the
    [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441) extended `CONNECT`
    that HTTP/2 requires, and its own HTTP/2 transport then refuses it — `502`,
    logged as `http2: invalid Upgrade request header`. This happens whether or
    not HTTP/1 is enabled upstream.

    The configuration below routes upgrades over HTTP/1.1 and everything else
    over `h2c`, so **do not pass `--no-http1`** with Caddy if you serve
    WebSockets. For WebSockets on a pure HTTP/2 upstream, use
    [HAProxy](#haproxy).

```nginx title="Caddyfile"
--8<-- "Caddyfile"
```

Pair it with:

```bash
h2corn hello:app \
  --bind 127.0.0.1:8000 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1,::1,unix
```

## HAProxy

[HAProxy](https://www.haproxy.com/) speaks HTTP/2 upstream with
`proto h2` and can layer PROXY protocol v2 on the same connection — see
the [HAProxy HTTP guide](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/protocol-support/http/)
for the full directive set.

**This is the recommended topology if you serve WebSockets.** HAProxy
translates a browser's HTTP/1.1 `Upgrade` handshake into the RFC 8441 extended
`CONNECT` that `h2corn` accepts over `h2c`, so the WebSocket rides the same
HTTP/2 upstream as everything else and `--no-http1` can stay on.

```text title="haproxy.cfg"
--8<-- "haproxy.cfg"
```

Pair it with:

```bash
h2corn hello:app \
  --bind 127.0.0.1:8000 \
  --proxy-protocol v2 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1,::1,unix \
  --no-http1
```

## Other proxies

`h2corn` works with any reverse proxy that speaks `h2c` upstream. The three
above are the ones with recipes here; Traefik and Envoy also support it. If
you're evaluating an alternative that cannot, pick one that can rather than
falling back to HTTP/1.1 between the proxy and the application.
