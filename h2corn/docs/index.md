---
description: A drop-in ASGI server for FastAPI, Starlette, Django and Litestar, with end-to-end HTTP/2 and a production worker supervisor.
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

# Blazing-fast Python ASGI

<p class="hero-stats"><strong>70–95% lower latency</strong> at p50–p99</p>

Switch the server, not your app. h2corn runs FastAPI, Starlette, Django,
Litestar and any ASGI 3 app, with end-to-end HTTP/2 and a production
worker supervisor.

[Get started :material-arrow-right:](quickstart.md){ .md-button .md-button--primary }
[Why h2corn](#why-h2corn){ .md-button }

</div>

<div class="grid cards" markdown>

-   :material-flash:{ .lg .middle } **Fast by default**

    ---

    HTTP framing, TLS, and stream multiplexing run natively. Requests
    only cross into Python when there is real handler work to do.

    [:octicons-arrow-right-24: Benchmarks](benchmarks.md)

-   :material-shield-lock:{ .lg .middle } **Secure deployments**

    ---

    HTTP/2 end-to-end keeps the proxy → app hop off HTTP/1.1, removing
    the downgrade surface that request-smuggling research targets.

    [:octicons-arrow-right-24: Behind a proxy](deployment/proxy.md)

-   :material-cog-sync:{ .lg .middle } **Operator-friendly**

    ---

    A multi-worker supervisor built for long-running deployments —
    rolling reload, live scaling, recycling, heartbeats.

    [:octicons-arrow-right-24: Operations](deployment/operations.md)

</div>

## Why h2corn

<div class="rationale" markdown>

A browser reaches your edge over HTTP/3 or HTTP/2. What happens on the next hop
is almost always HTTP/1.1 — the proxy translates the request down before the
application server ever sees it. That translation is what
[HTTP/2 downgrading](https://portswigger.net/web-security/request-smuggling/advanced/http2-downgrading)
research targets, and it is the hop most deployments never think about.

h2corn removes it. The proxy speaks `h2c` straight into the application, with
[WebSockets](websockets.md) on that same connection
([RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)) instead of a
hijacked HTTP/1.1 socket. The edge keeps doing what edges are good at; the last
hop stops being the weak one.

Speed comes with it. In most Python services the handler is quick and the server
around it is what keeps p99 high, so moving connections, TLS and HTTP into Rust
leaves the event loop doing one thing — running your code. Across the twenty
comparable scenarios of the benchmark suite, the median one comes out at **84%
lower p50 and 82% lower p99** than the fastest of `uvicorn`, `hypercorn` and
`gunicorn` in that same scenario; four workers serve a small plaintext GET at
**~242k RPS, p99 0.8 ms**, about **5×** the nearest alternative on the identical
Starlette app and over **20×** on HTTP/2.
[See benchmarks](benchmarks.md).

</div>
