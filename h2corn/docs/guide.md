---
description: Deploy an ASGI application, manage workers and shutdown, use WebSockets, or embed h2corn in an existing event loop.
---

# User guide

## Deploy an application

- [Behind a reverse proxy](deployment/proxy.md) — nginx, Caddy, and HAProxy
  recipes, forwarded identity, and WebSocket routing.
- [Direct TLS](deployment/tls.md) — certificates, client authentication, and
  HTTPS without a separate proxy.
- [Migrating to h2corn](migrating.md) — commands and behavior changes from
  Uvicorn, Hypercorn, and Gunicorn.

## Run and monitor

- [Operations](deployment/operations.md) — worker pools, configuration,
  readiness, rolling reload, shutdown, and logs.
- [Troubleshooting](troubleshooting.md) — connection errors, startup failures,
  proxy identity, TLS, and reload.
- [Configuration reference](configuration.md) — CLI flags, environment
  variables, TOML settings, and defaults.

## Integrate with Python

- [Embedding](embedding.md) — run a server in your event loop, await readiness,
  and control shutdown.
- [WebSockets](websockets.md) — application code and HTTP/1.1 or HTTP/2
  transport requirements.
- [ASGI support](asgi.md) — file sending, lifespan state, extensions, and
  errors from `send()`.
- [Python API](api/index.md) — `Config`, `Server`, `serve`, and typed ASGI
  messages.
