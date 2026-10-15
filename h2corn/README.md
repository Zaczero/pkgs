<p align="center">
  <img src="docs/assets/logo.svg" alt="h2corn" width="180">
</p>

<h1 align="center">h2corn</h1>

<p align="center">
  <strong>HTTP/2 ASGI server for Python</strong>, written in Rust.<br>
  Serve ASGI applications with HTTP/1.1, HTTP/2, or direct TLS.
</p>

<p align="center">
  <a href="https://pypi.org/p/h2corn"><img src="https://shields.monicz.dev/pypi/pyversions/h2corn" alt="PyPI - Python Version"></a>
  <a href="https://liberapay.com/Zaczero/"><img src="https://shields.monicz.dev/liberapay/patrons/Zaczero?logo=liberapay&amp;label=Patrons" alt="Liberapay Patrons"></a>
  <a href="https://github.com/sponsors/Zaczero"><img src="https://shields.monicz.dev/github/sponsors/Zaczero?logo=github&amp;label=Sponsors&amp;color=%23db61a2" alt="GitHub Sponsors"></a>
</p>

<p align="center">
  <a href="https://h2corn.monicz.dev/">Documentation</a> ·
  <a href="https://h2corn.monicz.dev/quickstart/">Quickstart</a> ·
  <a href="https://h2corn.monicz.dev/configuration/">Configuration</a> ·
  <a href="https://h2corn.monicz.dev/benchmarks/">Benchmarks</a>
</p>

---

- **HTTP/2 proxy hop** — a proxy configured for `h2c` can send ordinary requests to h2corn over cleartext HTTP/2. The private hop still needs network isolation.
- **WebSockets** — HTTP/2 extended CONNECT is supported when the client and proxy use RFC 8441. The nginx and Caddy examples keep a separate HTTP/1.1 route for clients using that upgrade handshake.
- **Direct TLS** — TLS 1.2 and 1.3, including mutual TLS with client-certificate details exposed to the application.
- **Operator controls** — worker management, reload, recycling, health checks, and bounded graceful shutdown.

## Install

h2corn supports Python 3.11 and newer.

```bash
uv init h2corn-demo
cd h2corn-demo
uv add h2corn fastapi
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
uv run h2corn hello:app
```

The command runs in the uv-managed project environment. With pip, install
h2corn and FastAPI into an activated virtual environment, then run the same
command without the `uv run` prefix.

For production, put `h2corn` behind a reverse proxy that speaks `h2c` upstream.
Keep HTTP/1.1 enabled when a proxy route uses HTTP/1.1 WebSocket upgrades; use
`--no-http1` only when all routes use HTTP/2 and the WebSocket client/proxy path
supports RFC 8441 extended CONNECT. Deployment recipes for nginx, HAProxy, and
Caddy live in
[the docs](https://h2corn.monicz.dev/deployment/proxy/).

## Is h2corn right for you?

h2corn is a good choice when:

- your proxy supports `h2c` and you want HTTP/2 between the proxy and server
- your client and proxy support RFC 8441 for WebSockets over HTTP/2
- you run long-lived deployments that need rolling reload, live scaling, worker
  recycling, and a bounded shutdown budget
- you need a Rust implementation of an ASGI server

h2corn is not the right choice when:

- you need gRPC, which h2corn does not serve
- you want a pure-Python server

## Benchmarks

The [benchmark page](https://h2corn.monicz.dev/benchmarks/) describes the
workloads and measurement method. It currently publishes no comparison figures
because matching aggregate records are not included.

## Support

Bug reports, feature requests, and questions go in the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).

For deployment review, migration help, performance audits, or prioritized
work, commercial support is available through
[monicz.dev](https://monicz.dev). See the
[Support page](https://h2corn.monicz.dev/support/) for details.

Security disclosures use GitHub's
[private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new).
