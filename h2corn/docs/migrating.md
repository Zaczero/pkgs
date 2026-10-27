---
description: Move an ASGI application from Uvicorn, Hypercorn, or Gunicorn to h2corn, including commands, proxy trust, and timeout differences.
---

# Migrating to h2corn

h2corn uses the same `module:app` import target as other ASGI servers.
The server options, proxy trust, timeouts, and worker lifecycle need their own
configuration. The examples below use `myapp:app` as the application target
and bind to loopback.

## From Uvicorn

```bash
uvicorn myapp:app --host 127.0.0.1 --port 8000 --workers 4

h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

Use [`--loop`](configuration.md#option-loop) to choose `asyncio` or `uvloop`.
[`--reload`](configuration.md#command-reload) is a development-only watcher
and is limited to one worker.

## From Hypercorn

```bash
hypercorn myapp:app --bind 127.0.0.1:8000 --workers 4

h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

Both serve HTTP/1.1 and HTTP/2. Map the remaining settings through the
[configuration reference](configuration.md#option-index), including the
protocols used between your clients, proxy, and application.

## From Gunicorn

```bash
gunicorn myapp:app -k uvicorn.workers.UvicornWorker --workers 4 \
  --bind 127.0.0.1:8000

h2corn myapp:app --bind 127.0.0.1:8000 --workers 4
```

h2corn does not load Gunicorn worker classes or configuration files.
[Operations](deployment/operations.md) covers its worker lifecycle, signals,
and resource limits.

## Defaults that affect migration

The comparisons below use the defaults in
[Uvicorn 0.54.0](https://github.com/encode/uvicorn/blob/0.54.0/uvicorn/config.py),
[Gunicorn 26.2.0](https://github.com/benoitc/gunicorn/blob/26.2.0/gunicorn/config.py),
and [Hypercorn 0.18.0](https://github.com/pgjones/hypercorn/blob/0.18.0/src/hypercorn/config.py).

| Setting | Other servers | h2corn |
| --- | --- | --- |
| Forwarded-header trust | Uvicorn enables `X-Forwarded-For` and `X-Forwarded-Proto` handling for trusted peers; the default peer is `127.0.0.1`. | Off until [`--proxy-headers`](configuration.md#option-proxy_headers), then only from peers in [`--forwarded-allow-ips`](configuration.md#option-forwarded_allow_ips). |
| HTTPS marker spelling | Gunicorn also reads `X-Forwarded-Ssl` and `X-Forwarded-Protocol`. | The proxy must send `X-Forwarded-Proto`. |
| Idle keep-alive | Uvicorn: 5 s; Gunicorn: 2 s; Hypercorn: 5 s. | [`--timeout-keep-alive`](configuration.md#option-timeout_keep_alive): 120 s. |
| Graceful stop budget | Gunicorn: 30 s; Hypercorn: 3 s; Uvicorn: unbounded. | [`--timeout-graceful-shutdown`](configuration.md#option-timeout_graceful_shutdown): 30 s. |

For production, configure the [proxy trust and WebSocket route](deployment/proxy.md)
or [direct TLS](deployment/tls.md), and account for the complete
[shutdown budget](deployment/operations.md#shutdown-budget).

## Framework compatibility {#framework-boundary}

h2corn accepts ASGI 3 callables. Django (`asgi:application`), Litestar, Quart,
and similar frameworks work when their exported callable meets that contract.
Application lifespan, WebSockets, and middleware remain part of the integration;
see [ASGI support](asgi.md) for extensions and server-specific behavior.
