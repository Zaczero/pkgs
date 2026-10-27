<p align="center">
  <img src="docs/assets/logo.svg" alt="h2corn" width="180">
</p>

<h1 align="center">h2corn</h1>

<p align="center">
  <strong>The fastest ASGI server for Python.</strong>
</p>

<p align="center">
  Built in Rust. Ready for HTTP/2, HTTP/1.1, WebSockets, and gRPC.
</p>

<p align="center">
  Up to 30× the throughput of the compared Uvicorn, Hypercorn, and Gunicorn
  configurations in a one-worker GET test. <a href="https://h2corn.monicz.dev/benchmarks/">Explore the benchmarks</a>.
</p>

<p align="center">
  <a href="https://pypi.org/p/h2corn"><img src="https://shields.monicz.dev/pypi/pyversions/h2corn" alt="PyPI - Python Version"></a>
  <a href="https://liberapay.com/Zaczero/"><img src="https://shields.monicz.dev/liberapay/patrons/Zaczero?logo=liberapay&amp;label=Patrons" alt="Liberapay Patrons"></a>
  <a href="https://github.com/sponsors/Zaczero"><img src="https://shields.monicz.dev/github/sponsors/Zaczero?logo=github&amp;label=Sponsors&amp;color=%23db61a2" alt="GitHub Sponsors"></a>
</p>

<p align="center">
  <a href="https://h2corn.monicz.dev/">Documentation</a> ·
  <a href="https://h2corn.monicz.dev/quickstart/">Quickstart</a> ·
  <a href="https://h2corn.monicz.dev/grpc/">gRPC and Connect</a> ·
  <a href="https://h2corn.monicz.dev/configuration/">Configuration</a> ·
  <a href="https://h2corn.monicz.dev/benchmarks/">Benchmarks</a>
</p>

---

- **HTTP/2 end to end** — use `h2c` between your proxy and application.
  HTTP/1.1 is available on the same listener when you need it.
- **WebSockets** — HTTP/1.1 upgrades and HTTP/2 extended CONNECT (RFC 8441).
- **Direct TLS** — TLS 1.2 and 1.3, including mutual TLS with client-certificate details exposed to the application.
- **Operator controls** — worker management, reload, recycling, health checks, and bounded graceful shutdown.

## Install

h2corn supports CPython 3.11 and newer. See
[Python compatibility and wheels](docs/compatibility.md) for supported standard
and free-threaded builds.

```bash
pip install h2corn
```

<a id="a-60-second-start"></a>

## Quickstart

Run an existing ASGI application with `h2corn module:app`, or try this
[FastAPI](https://fastapi.tiangolo.com/) application (`pip install fastapi`):

```python
# hello.py
from fastapi import FastAPI

app = FastAPI()


@app.get('/')
async def index():
    return {'message': 'hello from h2corn'}
```

```bash
h2corn hello:app
```

Open <http://127.0.0.1:8000/>. Add `--reload` for development.

For production, use a TLS-terminating reverse proxy with a private `h2c`
upstream, or [terminate TLS in h2corn](https://h2corn.monicz.dev/deployment/tls/).
HTTP/1.1 is enabled by default for compatibility. You can disable it with
`--no-http1` when every route supports HTTP/2, including WebSocket routes,
which require RFC 8441 support. See the
[proxy recipes](https://h2corn.monicz.dev/deployment/proxy/) for routing and
forwarded-header configuration.

## Is h2corn right for you?

h2corn serves ASGI applications on Python 3.11+, including free-threaded
CPython. Linux and macOS support the multi-worker supervisor; Windows runs a
single worker. It can host Connect, native gRPC, and gRPC-Web through ASGI
applications; your RPC framework owns service routing and RPC semantics. See
the [gRPC and Connect guide](https://h2corn.monicz.dev/grpc/).

Moving an existing deployment? See the
[migration guide](https://h2corn.monicz.dev/migrating/) for commands and changed defaults.

## Benchmarks

**9.7–30× the throughput** of the compared Uvicorn, Hypercorn, and Gunicorn
configurations in the one-worker HTTP/1.1 GET benchmark. All serve the same
Starlette application using standard asyncio.

<picture>
  <source media="(max-width: 600px) and (prefers-color-scheme: dark)" srcset="bench/results/plots/benchmark_http_1_get_1_worker-loopback-narrow-dark.svg">
  <source media="(max-width: 600px)" srcset="bench/results/plots/benchmark_http_1_get_1_worker-loopback-narrow.svg">
  <source media="(prefers-color-scheme: dark)" srcset="bench/results/plots/benchmark_http_1_get_1_worker-loopback-dark.svg">
  <img src="bench/results/plots/benchmark_http_1_get_1_worker-loopback.svg" alt="One-worker HTTP/1.1 text-response comparison: request rate, latency percentiles, and sampled deployment memory for h2corn, Uvicorn, Hypercorn, and Gunicorn.">
</picture>

See [the measured environment, exact data, server configuration, and workloads](https://h2corn.monicz.dev/benchmarks/),
including HTTP/2, streaming, WebSockets, and gRPC.

## Support

Bug reports, feature requests, and questions go in the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).

For deployment review, migration help, performance audits, or prioritized
work, commercial support is available through
[monicz.dev](https://monicz.dev). See the
[Support page](https://h2corn.monicz.dev/support/) for details.

Security disclosures use GitHub's
[private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new).
