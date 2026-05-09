# Behind a reverse proxy

The most common production topology runs `h2corn` behind a reverse proxy
that handles browser-facing TLS:

```text
browser/client  →  reverse proxy (TLS edge)  →  h2corn (h2c)
```

The proxy handles ALPN negotiation, TLS termination, and public-edge
hardening. `h2corn` then runs the application side of the connection on
[`h2c`](https://datatracker.ietf.org/doc/html/rfc9113) — cleartext
HTTP/2 over TCP or a Unix socket inside the trust boundary.

If a separate proxy isn't a good fit for your environment, `h2corn` can
also terminate TLS itself — see [Direct TLS](tls.md).

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
  --forwarded-allow-ips 127.0.0.1,::1,unix \
  --no-http1
```

## HAProxy

[HAProxy](https://www.haproxy.com/) speaks HTTP/2 upstream with
`proto h2` and can add PROXY protocol v2 on the same connection.

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

Reference: [HAProxy HTTP guide](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/protocol-support/http/).

## Other proxies

`h2corn` works with any reverse proxy that speaks `h2c` upstream. Caddy
and HAProxy currently do this cleanly. If you are evaluating an
alternative that cannot, prefer one that can over falling back to
HTTP/1.1 between the proxy and the application.
