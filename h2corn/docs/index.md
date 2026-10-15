---
description: An HTTP/2 ASGI server for Python applications, with proxy, TLS, WebSocket, and worker configuration.
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

# HTTP/2 ASGI server for Python

Run an ASGI application with HTTP/1.1, HTTP/2, or direct TLS. For a reverse
proxy deployment, h2corn can accept ordinary requests over `h2c`; the proxy and
WebSocket route determine which protocols each hop uses.

[Get started :material-arrow-right:](quickstart.md){ .md-button .md-button--primary }
[Why h2corn](#why-h2corn){ .md-button }

</div>

<div class="grid cards" markdown>

-   :material-flash:{ .lg .middle } **Native protocol handling**

    ---

    HTTP framing, TLS, and stream multiplexing run in Rust. ASGI request
    handling remains in Python.

    [:octicons-arrow-right-24: Benchmarks](benchmarks.md)

-   :material-shield-lock:{ .lg .middle } **Choose each protocol hop**

    ---

    A proxy configured for `h2c` can use HTTP/2 between the proxy and h2corn.
    That cleartext hop still requires network isolation; HTTP/2 alone does not
    secure an application.

    [:octicons-arrow-right-24: Behind a proxy](deployment/proxy.md)

-   :material-cog-sync:{ .lg .middle } **Operator-friendly**

    ---

    A multi-worker supervisor built for long-running deployments —
    rolling reload, live scaling, recycling, heartbeats.

    [:octicons-arrow-right-24: Operations](deployment/operations.md)

</div>

## Why h2corn

<div class="rationale" markdown>

Configure a reverse proxy to use `h2c` when you want HTTP/2 between the proxy
and h2corn. The hop is cleartext, so keep it within a private network boundary.
For WebSockets, the nginx and Caddy examples use a separate HTTP/1.1 upgrade
route; HTTP/2-only WebSockets require the client and proxy path to support RFC
8441 extended CONNECT.

The benchmark page describes its workloads and limits.
[Read the benchmark method](benchmarks.md).

</div>
