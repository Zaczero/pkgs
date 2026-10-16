---
description: The fastest Python ASGI server. Built in Rust, with HTTP/2 end to end.
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

<a id="http2-asgi-server-for-python"></a>

# The fastest Python ASGI server

Rust handles the wire. Python handles your application.
HTTP/2, WebSockets, TLS, and a multi-worker supervisor in one ASGI server.

**86–97% lower p50–p99 latency** in our
[HTTP/1.1 benchmark](benchmarks.md).

[Get started :material-arrow-right:](quickstart.md){ .md-button .md-button--primary }
[Why h2corn](#why-h2corn){ .md-button }

</div>

## Fast from p50 to p99

One Starlette application, four servers, and four fresh starts per server.

![HTTP/1.1 GET latency: h2corn measured 0.995 ms at p50 and 1.671 ms at p99, compared with 9.458–30.003 ms and 12.253–38.665 ms for the other servers.](assets/benchmarks/latency-loopback.svg#only-light)
![HTTP/1.1 GET latency: h2corn measured 0.995 ms at p50 and 1.671 ms at p99, compared with 9.458–30.003 ms and 12.253–38.665 ms for the other servers.](assets/benchmarks/latency-dark-loopback.svg#only-dark)

Measured on a Ryzen 9 5950X with CPython 3.14.8.
[Open the chart at full size](assets/benchmarks/latency-loopback.svg) or
[see the data, configuration, and all 27 workloads](benchmarks.md).

<div class="grid cards" markdown>

-   :material-flash:{ .lg .middle } **Native protocol handling**

    ---

    HTTP framing, TLS, and stream multiplexing run in Rust. ASGI request
    handling remains in Python.

    [:octicons-arrow-right-24: ASGI support](asgi.md)

-   :material-shield-lock:{ .lg .middle } **HTTP/2 end to end**

    ---

    Keep HTTP/2 between your reverse proxy and application over a private
    `h2c` connection, or terminate TLS directly in h2corn.

    [:octicons-arrow-right-24: Behind a proxy](deployment/proxy.md)

-   :material-cog-sync:{ .lg .middle } **Operator-friendly**

    ---

    A multi-worker supervisor built for long-running deployments —
    rolling reload, live scaling, recycling, heartbeats.

    [:octicons-arrow-right-24: Operations](deployment/operations.md)

</div>

## Why h2corn

<div class="rationale" markdown>

h2corn puts HTTP/2 at the application boundary, with native protocol handling
and ordinary Python ASGI callables. Use HTTP/1.1 for local browser development
and WebSocket upgrade routes, or RFC 8441 for WebSockets over HTTP/2.

Start with the [quickstart](quickstart.md), look up
[configuration](configuration.md) or the [Python API](api/index.md), and see
[support](support.md) for questions or deployment help.

</div>
