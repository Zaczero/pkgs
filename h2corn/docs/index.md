---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

# Blazing-fast Python ASGI

<p class="hero-stats"><strong>70–95%</strong> lower latency at p50–p99</p>

A drop-in ASGI server for FastAPI, Starlette, Django, Litestar, and any
ASGI 3 app. Same `module:app` start line, same `--workers`, plus
end-to-end HTTP/2 and a production-grade worker supervisor.

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

    Multi-worker supervisor with graceful shutdown, rolling reload, live
    signal-driven scaling, jittered recycling, and worker heartbeats.

    [:octicons-arrow-right-24: Operations](deployment/operations.md)

</div>

## Why h2corn

<div class="rationale" markdown>

### Lower latency

In most Python services the handler itself is quick — it's the server
around it that keeps p99 high. h2corn moves that overhead out of
Python entirely: connections, TLS, and HTTP are handled in Rust, and
the event loop is left free to do one thing — run your code. Across
the benchmark suite that comes out to **70–95% lower latency at
p50–p99** than `uvicorn`, `hypercorn`, or `gunicorn`.

### Higher throughput

The same shift shows up as capacity. Each worker absorbs several
times the load of a pure-Python server, so traffic that used to need
a dozen pods fits in a couple. Four workers serve a small JSON GET at
**~228k RPS, p99 1.0 ms** — about **10×** the nearest alternative
serving the identical Starlette app.
[See benchmarks](benchmarks.md).

### Modern protocols

HTTP/2 stays end-to-end, including WebSockets. Keeping the proxy → app
hop on `h2c` removes the HTTP/1.1 downgrade where framing ambiguity
and connection-reuse issues reappear — the surface that
[request-smuggling research](https://portswigger.net/web-security/request-smuggling)
repeatedly targets. WebSockets ride the same connection
([RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)) instead of
hijacking a separate HTTP/1.1 socket.

### Production ready

Everything a long-running deployment expects, in the box. The
supervisor opens listeners once and inherits them into workers;
`SIGHUP` performs a rolling reload, `SIGTTIN`/`SIGTTOU` scales the pool
live, worker recycling staggers memory growth with jitter, and
per-worker heartbeats replace anything wedged.

</div>
