# Behind a reverse proxy

The most common production topology runs `h2corn` behind a reverse proxy
that handles browser-facing TLS:

```text
browser/client  →  reverse proxy (TLS edge)  →  h2corn (h2c)
```

The proxy takes care of ALPN negotiation, TLS termination, and
public-edge hardening. `h2corn` runs the application side of the
connection on
[`h2c`](https://datatracker.ietf.org/doc/html/rfc9113) — cleartext
HTTP/2 over TCP or a Unix socket inside the trust boundary.

When a separate proxy isn't a good fit for your environment, `h2corn`
can terminate TLS itself instead — see [Direct TLS](tls.md).

!!! note "Why h2c upstream?"
    Keeping the proxy → app hop on HTTP/2 avoids the HTTP/1.1 *downgrade*
    surface that
    [request smuggling](https://portswigger.net/web-security/request-smuggling)
    and [HTTP/2 downgrading](https://portswigger.net/web-security/request-smuggling/advanced/http2-downgrading)
    research repeatedly targets. If your proxy can speak `h2c` upstream,
    prefer that over an HTTP/1.1 fallback.

## Proxy headers and PROXY protocol

`h2corn` accepts two kinds of trust hop metadata, both opt-in and
gated by `--forwarded-allow-ips`:

- **`--proxy-headers`** trusts standard `Forwarded` and `X-Forwarded-*`
  headers from peers in `--forwarded-allow-ips`. These carry request
  metadata such as scheme, host, and the original client address.
- **`--proxy-protocol v1|v2`** parses HAProxy's
  [PROXY protocol](https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt)
  on inbound connections. It carries transport-level peer information
  on the connection itself, useful when you want the original source IP
  for connection-level metrics or per-IP limits.

In most deployments, proxy headers alone are enough. Add PROXY protocol
when the upstream is explicitly configured to send it.

!!! danger "The proxy must overwrite every header it vouches for"
    `--forwarded-allow-ips` says *who* may state a client's provenance, not
    *which* headers are genuine. A client can send `X-Forwarded-For` and
    `X-Forwarded-Proto` of its own, and a proxy that merely appends to them —
    or passes them through untouched, which is the default for most — hands
    them to `h2corn` from a trusted address. The application then sees the
    attacker's chosen client IP, scheme, host, and root path.

    So the proxy has to *replace* the headers it sets and *delete* the ones it
    does not: HAProxy's `http-request set-header`/`del-header`, Caddy's
    `header_up`. The example configurations below do this; if you adapt them,
    keep that part. Without `--proxy-headers` no forwarding header is trusted
    at all, which is the right setting when nothing upstream sets them.

## nginx

[nginx](https://nginx.org/) speaks HTTP/2 to an upstream from **1.29.4**
onwards, where the changelog records
"*the ngx_http_proxy_module supports HTTP/2*". Set
[`proxy_http_version 2`](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_http_version)
and keep the upstream URL on `http://`, which makes that hop cleartext `h2c`.
It needs `ngx_http_v2_module`, which distribution builds normally include.

!!! danger "Run 1.30.1+ or 1.31.0+, not merely 1.29.4+"
    [CVE-2026-42926](https://nginx.org/en/security_advisories.html) is an
    HTTP/2 request injection in `ngx_http_proxy_module` — the module this
    recipe turns on — affecting **1.29.4 through 1.30.0**. Those are exactly
    the releases that first carried the feature, so "new enough to have it" is
    not the same as "safe to deploy it". Use **1.30.1 or later** on the stable
    branch, or **1.31.0 or later** on mainline.

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

!!! warning "WebSockets need their own HTTP/1.1 location"
    nginx still performs the HTTP/1.1 `Upgrade` handshake and does not
    translate it into the [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)
    extended `CONNECT` that HTTP/2 requires, so a location forced to
    `proxy_http_version 2` cannot carry them. The configuration above routes
    `/ws` over HTTP/1.1 and everything else over `h2c`, which means **do not
    pass `--no-http1`** with nginx if you serve WebSockets. For WebSockets over
    a pure HTTP/2 upstream, use [HAProxy](#haproxy) below.

!!! warning "nginx does not multiplex to the upstream yet"
    NGINX describes the current implementation as one where "*each upstream
    connection handles one request at a time rather than interleaving multiple
    requests on a single connection*", and that matches what the wire shows:
    ten overlapping requests through nginx 1.30.4 arrive at `h2corn` on ten
    separate connections, while sequential ones reuse a single keepalive
    connection.

    So the win on this hop is HPACK header compression and the absence of
    HTTP/1.1 framing ambiguity — **not** the connection-count reduction that
    end-to-end HTTP/2 implies elsewhere. NGINX says multiplexing is planned.
    None of this affects the browser-facing hop, where nginx multiplexes
    normally.

## Caddy

[Caddy](https://caddyserver.com/) speaks `h2c` upstream natively with
its
[`reverse_proxy`](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy)
directive.

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

!!! warning "Caddy cannot carry WebSockets over `h2c` — use HAProxy for those"
    Caddy forwards the client's `Upgrade` header to the upstream unchanged
    rather than translating it into the
    [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441) extended
    `CONNECT` that HTTP/2 requires, and its own HTTP/2 transport then refuses
    it — the handshake fails with `502` and Caddy logs
    `http2: invalid Upgrade request header`. This is independent of `h2corn`:
    it happens whether or not HTTP/1 is enabled upstream.

    The configuration above works around it by sending upgrade requests over
    HTTP/1.1 and everything else over `h2c`, which means **do not pass
    `--no-http1`** with Caddy if you serve WebSockets.

    If you want WebSockets *and* a pure HTTP/2 upstream, use
    [HAProxy](#haproxy) below: it does the translation, so `--no-http1` works
    and every hop stays HTTP/2.

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
