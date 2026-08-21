---
description: Install h2corn, run your first ASGI app, and understand the flags a production start line needs.
---

# Quickstart

This tutorial runs a FastAPI application under h2corn, proves both the HTTP/1.1
and cleartext `h2c` paths against it, and ends with the flags a production start
line needs.

## Install

=== "uv"

    ```bash
    uv add h2corn fastapi
    ```

=== "pip"

    ```bash
    pip install h2corn fastapi
    ```

`h2corn` requires **Python 3.11+**. Wheels are published for Linux,
macOS, and Windows, including ABI-matched free-threaded CPython (`cp3XXt`)
builds. The multi-worker supervisor is Unix-only — see
[Operations](deployment/operations.md).

## Run your first server

Save a tiny FastAPI app as `hello.py`:

```python title="hello.py"
--8<-- "hello.py"
```

Then start `h2corn`, pointing it at the module and the ASGI app
object inside it:

```bash
h2corn hello:app
```

Visit <http://127.0.0.1:8000/> in your browser. The response is
`{"message": "hello from h2corn"}`.

For a repeatable terminal check, leave that process running and use a second
terminal:

```bash
curl --http1.1 --fail http://127.0.0.1:8000/
```

The request succeeds because the default `http1` setting is enabled.

!!! note "Why does the browser use HTTP/1.1?"
    Browsers do not speak cleartext `h2c` with prior knowledge. The
    development server therefore keeps HTTP/1.1 enabled for local browser
    testing. A cleartext browser success is HTTP/1.1, not HTTP/2. In
    production, HTTPS is exposed through a [reverse proxy](deployment/proxy.md)
    or h2corn's own [Direct TLS](deployment/tls.md). `--no-http1` applies only
    when the client or proxy is configured for HTTP/2.

## Prove h2c with prior knowledge

An HTTP/2 client must opt into cleartext prior knowledge. Use a curl build that
lists HTTP/2 in `curl --version`:

```bash
curl --version | grep -q 'HTTP2'
curl --http2-prior-knowledge --fail http://127.0.0.1:8000/
```

`--http2-prior-knowledge` sends the HTTP/2 connection preface straight to
h2corn with no `Upgrade` handshake, on the same listener the HTTP/1.1 request
used.

## Hot reload

While iterating locally, add `--reload` so `h2corn` restarts whenever
your source files change:

```bash
h2corn hello:app --reload
```

The watcher follows `*.py` by default; tune it with `--reload-include`
and `--reload-exclude`. Reload is intended for development only and
cannot be combined with multiple workers.

## Application factories

Some applications expose their ASGI object through a factory function
rather than a module-level attribute. `--factory` makes `h2corn` call the
target:

```python title="factory.py"
--8<-- "factory.py"
```

```bash
h2corn factory:create_app --factory
```

## Deploy

In production, `h2corn` typically sits behind a reverse proxy that
terminates browser-facing TLS. The application server itself runs on a
local listener with several workers. A production start line can use:

```bash
h2corn hello:app \
  --bind 127.0.0.1:8000 \
  --workers 4 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1,::1,unix \
  --no-http1
```

Production flags:

- **`--bind 127.0.0.1:8000`** listens on loopback only so the proxy can
  reach it locally. Use `0.0.0.0:port` for a public-facing TCP listener.
- **`--workers 4`** runs four worker processes; size it to the core count and
  the application's concurrency.
- **`--proxy-headers`** interprets `X-Forwarded-For` and
  `X-Forwarded-Proto` from peers listed in `--forwarded-allow-ips`. Use
  `--forwarded-fields` to select additional X-Forwarded fields or the RFC
  7239 `Forwarded` dialect.
- **`--no-http1`** rejects HTTP/1.1 connections outright, so once the upstream
  is configured to speak `h2c`, an accidental fallback fails immediately
  instead of serving at the lower protocol.

For larger command lines, use a
[TOML config file](deployment/operations.md#toml-config-files) with the same
keys.

## Migrate from another ASGI server

ASGI entrypoint mapping does not include options, protocol limits, lifespan,
WebSockets, forwarded identity, or shutdown behavior; those differ between
servers. `myapp:app` is the application import target.

### From Uvicorn

```bash
# Uvicorn
uvicorn myapp:app --host 127.0.0.1 --port 8000 --workers 4

# h2corn
h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

Use `--loop asyncio` or `--loop uvloop` to choose the callback loop.
`--reload` is a development-only watcher and is limited to one worker.

### From Hypercorn

```bash
# Hypercorn
hypercorn myapp:app --bind 127.0.0.1:8000 --workers 4

# h2corn
h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

Both serve HTTP/2. Port the remaining settings through the
[Configuration reference](configuration.md), then test the protocol path you
deploy.

### From Gunicorn

```bash
# Gunicorn with its ASGI worker
gunicorn myapp:app -k uvicorn.workers.UvicornWorker --workers 4 \
  --bind 127.0.0.1:8000

# h2corn
h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

h2corn does not load Gunicorn worker classes or Gunicorn configuration. Its
worker lifecycle, signals, and resource limits are documented in
[Operations](deployment/operations.md).

## Framework boundary

h2corn accepts ASGI 3 callables. Django (`asgi:application`), Litestar, Quart,
and similar frameworks work when their exported callable meets the ASGI
contract; validate lifespan, WebSockets, and middleware behavior.

## Next steps

- [Behind a proxy](deployment/proxy.md) — full Caddy and HAProxy
  recipes with `h2c` upstream.
- [Direct TLS](deployment/tls.md) — terminate TLS in `h2corn` itself
  for single-server deployments.
- [Operations](deployment/operations.md) — multi-worker supervisor,
  signals, rolling reload, live scaling, and recycling.
- [Configuration](configuration.md) — every option, in CLI, environment,
  and TOML form.
