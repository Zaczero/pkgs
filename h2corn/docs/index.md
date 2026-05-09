---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="h2corn" class="hero-logo">

# Blazing-fast Python ASGI

<p class="hero-stats"><strong>60–95%</strong> lower latency at p50–p99</p>

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

-   :material-puzzle:{ .lg .middle } **Drop-in for ASGI**

    ---

    FastAPI, Starlette, Django, Litestar, Quart — anything ASGI 3.
    Migration from `uvicorn`, `hypercorn`, or `gunicorn` is a one-line
    swap.

    [:octicons-arrow-right-24: Quickstart](quickstart.md)

</div>

## Why h2corn

**Performance where it counts.** The HTTP hot path — accept loop, framing,
TLS, multiplexing — runs in Rust on top of [Hyper](https://hyper.rs/)
and [Tokio](https://tokio.rs/). Python only sees a request once there is
handler work to do, so server overhead stops dominating the budget.
[See benchmarks](benchmarks.md).

**HTTP/2 end-to-end.** Keeping the proxy → app hop on HTTP/2 (`h2c`)
removes the HTTP/1.1 downgrade where framing ambiguity and
connection-reuse issues reappear — the surface that
[request-smuggling research](https://portswigger.net/web-security/request-smuggling)
repeatedly targets. WebSockets ride the same connection
([RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)) instead of
hijacking a separate HTTP/1.1 socket.

**Production-ready supervisor.** Listeners are opened once and inherited
into workers. `SIGHUP` does a rolling reload, `SIGTTIN`/`SIGTTOU` scales
the pool live, worker recycling staggers memory growth, and per-worker
heartbeats replace anything wedged.

## A 60-second start

=== "uv"

    ```bash
    uv add h2corn
    ```

=== "pip"

    ```bash
    pip install h2corn
    ```

```python title="hello.py"
--8<-- "hello.py"
```

```bash
h2corn hello:app
```

```text
h2corn v1.4.0 • HTTP/2 ASGI
Listening on http://127.0.0.1:8000
HTTP/1 compatibility is enabled; disable with --no-http1

Started worker [12345]
127.0.0.1:54321 "GET / HTTP/1.1" 200 0.4ms tx=25b
```

[Continue to the quickstart :material-arrow-right:](quickstart.md){ .md-button }
