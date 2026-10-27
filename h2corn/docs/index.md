---
description: High-performance ASGI for Python, built in Rust for HTTP/2, HTTP/1.1, WebSockets, and gRPC.
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

<a id="http2-asgi-server-for-python"></a>
<a id="the-fastest-python-asgi-server"></a>

# The fastest ASGI server for Python

Built in Rust. Ready for HTTP/2, HTTP/1.1, WebSockets, and gRPC.

Up to **30× the throughput** of the compared Uvicorn, Hypercorn, and Gunicorn
configurations in a one-worker GET test. [Explore the benchmarks](benchmarks.md).

[Get started :material-arrow-right:](quickstart.md){ .md-button .md-button--primary }
[User guide](guide.md){ .md-button }

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

    Use a private `h2c` connection between your proxy and application, or
    terminate TLS directly in h2corn. HTTP/1.1 is available on the same
    listener when you need it.

    [:octicons-arrow-right-24: Behind a proxy](deployment/proxy.md)

-   :material-cog-sync:{ .lg .middle } **Operator-friendly**

    ---

    A multi-worker supervisor built for long-running deployments —
    rolling reload, live scaling, recycling, heartbeats.

    [:octicons-arrow-right-24: Operations](deployment/operations.md)

</div>

<div class="benchmark-highlight" markdown>

<a id="fast-from-p50-to-p99"></a>

## Benchmarks

In a one-worker HTTP/1.1 GET benchmark, h2corn delivered **9.7–30× the
throughput** of the compared Uvicorn, Hypercorn, and Gunicorn configurations.
All four served the same Starlette application using standard asyncio.

<picture>
  <source media="(max-width: 600px)" srcset="assets/benchmarks/benchmark_http_1_get_1_worker-loopback-narrow.svg">
  <img src="assets/benchmarks/benchmark_http_1_get_1_worker-loopback.svg#only-light" alt="One-worker HTTP/1.1 text-response comparison: request rate, latency percentiles, and sampled deployment memory for four servers. Exact data and paired comparisons are on the benchmark page.">
</picture>
<picture>
  <source media="(max-width: 600px)" srcset="assets/benchmarks/benchmark_http_1_get_1_worker-loopback-narrow-dark.svg">
  <img src="assets/benchmarks/benchmark_http_1_get_1_worker-loopback-dark.svg#only-dark" alt="One-worker HTTP/1.1 text-response comparison: request rate, latency percentiles, and sampled deployment memory for four servers. Exact data and paired comparisons are on the benchmark page.">
</picture>

[See the measured environment, exact data, configuration, and workloads](benchmarks.md).

</div>

For settings and Python integration, see [Configuration](configuration.md)
and the [API reference](api/index.md). [Support](support.md) covers questions
and deployment help.
