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
[Operations](deployment/operations.md#worker-pool).

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

With no address given, h2corn binds [`127.0.0.1:8000`](configuration.md#option-bind) —
reachable from this machine and nowhere else. Visit <http://127.0.0.1:8000/> in
your browser. The response is `{"message": "hello from h2corn"}`.

For a repeatable terminal check, leave that process running and use a second
terminal:

```bash
curl --http1.1 --fail http://127.0.0.1:8000/
```

The request succeeds because the default [`http1`](configuration.md#option-http1)
setting is enabled.

!!! note "Why does the browser use HTTP/1.1?"
    Browsers do not speak cleartext `h2c` with prior knowledge. The
    development server therefore keeps HTTP/1.1 enabled for local browser
    testing. A cleartext browser success is HTTP/1.1, not HTTP/2. In
    production, HTTPS is exposed through a [reverse proxy](deployment/proxy.md#behind-a-reverse-proxy)
    or h2corn's own [Direct TLS](deployment/tls.md#direct-tls). [`--no-http1`](configuration.md#option-http1) applies only
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

While iterating locally, add [`--reload`](configuration.md#command-reload) so `h2corn` restarts whenever
your source files change:

```bash
h2corn hello:app --reload
```

The watcher follows `*.py` by default; tune it with
[`--reload-include`](configuration.md#command-reload_include) and
[`--reload-exclude`](configuration.md#command-reload_exclude). Reload is intended for development only and
cannot be combined with multiple workers.

## Application factories

Some applications expose their ASGI object through a factory function
rather than a module-level attribute. [`--factory`](configuration.md#command-factory)
makes `h2corn` call the target:

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

- **[`--bind`](configuration.md#option-bind) `127.0.0.1:8000`** listens on loopback only so the proxy can
  reach it locally. Use `0.0.0.0:port` for a public-facing TCP listener.
- **[`--workers`](configuration.md#option-workers) `4`** runs four worker processes; size it to the core count and
  the application's concurrency.
- **[`--proxy-headers`](configuration.md#option-proxy_headers)** interprets `X-Forwarded-For` and
  `X-Forwarded-Proto` from peers listed in
  [`--forwarded-allow-ips`](configuration.md#option-forwarded_allow_ips). Use
  [`--forwarded-fields`](configuration.md#option-forwarded_fields) to select additional X-Forwarded fields or the RFC
  7239 `Forwarded` dialect.
- **[`--no-http1`](configuration.md#option-http1)** rejects HTTP/1.1 connections outright, so once the upstream
  is configured to speak `h2c`, an accidental fallback fails immediately
  instead of serving at the lower protocol.

Swap the start line for [`--check-config`](configuration.md#command-check_config)
to validate the resolved configuration and exit without importing the target or
binding a listener, and [`--print-config`](configuration.md#command-print_config)
to print what a start would actually use once environment, file, and CLI values
have been merged.

For larger command lines, use a
[TOML config file](deployment/operations.md#toml-config-files) with the same
keys.

## Migrate from another ASGI server

ASGI entrypoint mapping does not include options, protocol limits, lifespan,
WebSockets, forwarded identity, or shutdown behavior; those differ between
servers. `myapp:app` is the application import target.

These are the defaults most likely to change behavior after the swap, read from
uvicorn 0.52.4, gunicorn 26.1.0 and hypercorn 0.18.0:

| What differs | Elsewhere | h2corn |
|---|---|---|
| Forwarded-header trust | uvicorn interprets `X-Forwarded-For` and `X-Forwarded-Proto` with no opt-in | off until [`--proxy-headers`](configuration.md#option-proxy_headers), then only from peers in [`--forwarded-allow-ips`](configuration.md#option-forwarded_allow_ips) |
| HTTPS marker spelling | gunicorn also reads `X-Forwarded-Ssl` and `X-Forwarded-Protocol` | neither is recognized; the proxy must send `X-Forwarded-Proto` |
| Idle keep-alive | uvicorn 5 s, gunicorn 2 s, hypercorn 5 s | [`--timeout-keep-alive`](configuration.md#option-timeout_keep_alive) 120 s |
| Graceful stop budget | gunicorn 30 s, hypercorn 3 s, uvicorn unbounded | [`--timeout-graceful-shutdown`](configuration.md#option-timeout_graceful_shutdown) 30 s |

### From Uvicorn

```bash
# Uvicorn
uvicorn myapp:app --host 127.0.0.1 --port 8000 --workers 4

# h2corn
h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

Use [`--loop`](configuration.md#option-loop) `asyncio` or `uvloop` to choose the callback loop.
[`--reload`](configuration.md#command-reload) is a development-only watcher and is limited to one worker.

### From Hypercorn

```bash
# Hypercorn
hypercorn myapp:app --bind 127.0.0.1:8000 --workers 4

# h2corn
h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

Both serve HTTP/2. Port the remaining settings through the
[Configuration reference](configuration.md#option-index), then test the protocol path you
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

- [Behind a proxy](deployment/proxy.md#behind-a-reverse-proxy) — full Caddy and HAProxy
  recipes with `h2c` upstream.
- [Direct TLS](deployment/tls.md#direct-tls) — terminate TLS in `h2corn` itself
  for single-server deployments.
- [Operations](deployment/operations.md#signals) — multi-worker supervisor,
  signals, rolling reload, live scaling, and recycling.
- [Configuration](configuration.md#option-index) — every option, in CLI, environment,
  and TOML form.
