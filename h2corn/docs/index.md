---
description: HTTP/2 ASGI for Python. Built in Rust, with HTTP/2 end to end.
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

<a id="http2-asgi-server-for-python"></a>
<a id="the-fastest-python-asgi-server"></a>

# HTTP/2 ASGI for Python

Rust handles the wire. Python handles your application.
HTTP/2, WebSockets, TLS, and a multi-worker supervisor in one ASGI server.

[Get started :material-arrow-right:](quickstart.md){ .md-button .md-button--primary }
[User guide](guide.md){ .md-button }

</div>

<div class="benchmark-highlight" markdown>

<a id="fast-from-p50-to-p99"></a>

## Benchmarks

**10–31× higher throughput** in the one-worker HTTP/1.1 GET benchmark.
h2corn, Uvicorn, Hypercorn, and Gunicorn serve the same Starlette application
on standard asyncio, with pure-Python HTTP/1.1 parsers for Uvicorn and Gunicorn.

![One-worker HTTP/1.1 text-response comparison: request rate, latency percentiles, and sampled deployment memory for four servers. Exact data and paired comparisons are on the benchmark page.](assets/benchmarks/benchmark_http_1_get_1_worker-loopback.svg#only-light)
![One-worker HTTP/1.1 text-response comparison: request rate, latency percentiles, and sampled deployment memory for four servers. Exact data and paired comparisons are on the benchmark page.](assets/benchmarks/benchmark_http_1_get_1_worker-loopback-dark.svg#only-dark)

[See the measured environment, exact data, configuration, and workloads](benchmarks.md).

</div>

<span id="why-h2corn"></span>

<div class="grid cards" markdown>

-   :material-flash:{ .lg .middle } **Native protocol handling**

    ---

    HTTP framing, TLS, and stream multiplexing run in Rust. Connect, native
    gRPC, and gRPC-Web applications use the same ASGI boundary.

    [:octicons-arrow-right-24: ASGI support](asgi.md) ·
    [gRPC and Connect](grpc.md)

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

For settings and Python integration, see [Configuration](configuration.md)
and the [API reference](api/index.md). [Support](support.md) covers questions
and deployment help.
