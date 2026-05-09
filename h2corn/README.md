<p align="center">
  <img src="docs/assets/logo.svg" alt="h2corn" width="180">
</p>

<h1 align="center">h2corn</h1>

<p align="center">
  <strong>High-performance HTTP/2 ASGI server</strong> for FastAPI, Starlette, and similar applications.<br>
  <a href="https://h2corn.monicz.dev/">Documentation</a> ·
  <a href="https://h2corn.monicz.dev/quickstart/">Quickstart</a> ·
  <a href="https://h2corn.monicz.dev/configuration/">Configuration</a> ·
  <a href="https://h2corn.monicz.dev/benchmarks/">Benchmarks</a>
</p>

---

`h2corn` keeps application traffic on **HTTP/2 end-to-end** instead of
downgrading to HTTP/1.1 inside your trust boundary. It is built around `h2c`
behind a trusted reverse proxy, with optional direct TLS for TCP listeners.

- **Better security** for the proxy → application connection (no HTTP/1.1 downgrade)
- **Higher throughput** and lower latency from a Rust core on Tokio + Hyper
- **Compatible** with any ASGI 3 application — FastAPI, Starlette, Django, Litestar
- **Direct TLS** with Rustls and modern defaults
- **RFC 8441 WebSockets** over HTTP/2
- **Operator-friendly**: multi-worker supervisor with graceful shutdown, rolling reload, live scaling, worker recycling, and health checks

## Install

```bash
uv add h2corn         # or: pip install h2corn
```

## A 60-second start

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

For production, put `h2corn` behind a reverse proxy that speaks `h2c`
upstream (Caddy or HAProxy), then disable HTTP/1.1 with `--no-http1`.
The full deployment recipes live in
[the docs](https://h2corn.monicz.dev/deployment/proxy/).

## Benchmarks

In local runs comparing `h2corn`, `uvicorn`, `hypercorn`, and `gunicorn`
across baseline GETs, Unix sockets, static files, streaming, and
WebSockets, `h2corn` leads on every scenario tested:

![HTTP/1 GET, 4 workers. h2corn ~90k RPS p99 2.3ms.](bench/results/plots/benchmark_http_1_get_4_workers.svg)

Full plots and methodology: [Benchmarks](https://h2corn.monicz.dev/benchmarks/).

## Support

Bug reports, feature requests, and questions go in the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).

For deployment review, migration help, performance audits, or prioritized
work, commercial support is available through
[monicz.dev](https://monicz.dev). See the
[Support page](https://h2corn.monicz.dev/support/) for details.

Security disclosures: use GitHub's
[private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new).

## License

MIT.
