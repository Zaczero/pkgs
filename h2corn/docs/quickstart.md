# Quickstart

This page gets you from zero to a running `h2corn` server in under a minute.

## Install

=== "uv"

    ```bash
    uv add h2corn
    ```

=== "pip"

    ```bash
    pip install h2corn
    ```

`h2corn` requires **Python 3.11+**. Wheels are published for Linux,
macOS and Windows (the multi-worker supervisor is Unix-only — see
[Operations](deployment/operations.md)).

## A minimal application

```python title="hello.py"
--8<-- "hello.py"
```

Start it:

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

That's it — visit <http://127.0.0.1:8000/> in your browser.

!!! info "Why does the response say HTTP/1.1?"
    Browsers do not speak cleartext `h2c`, so the development server keeps
    HTTP/1.1 enabled for local testing. In production, the edge advertises
    HTTPS — either through a [reverse proxy](deployment/proxy.md) or
    `h2corn`'s own [Direct TLS](deployment/tls.md) — and you turn HTTP/1.1
    off with `--no-http1`.

## Application factories

Pass `--factory` when your target is a zero-argument callable that returns
the ASGI app:

```python title="factory.py"
--8<-- "factory.py"
```

```bash
h2corn factory:create_app --factory
```

## Hot reload during development

```bash
h2corn hello:app --reload
```

The watcher follows `*.py` by default; tune it with `--reload-include` and
`--reload-exclude`. Reload is intended for development only and is mutually
exclusive with multiple workers.

## Production-style command

A typical shape behind a reverse proxy:

```bash
h2corn hello:app \
  --bind 127.0.0.1:8000 \
  --workers 4 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1,::1,unix \
  --no-http1
```

`--no-http1` is a fail-closed hardening flag: once the upstream is
configured to speak `h2c`, an accidental fallback fails immediately
instead of quietly serving traffic on the older protocol.

Next steps:

- [Behind a proxy](deployment/proxy.md) — full Caddy and HAProxy recipes.
- [Direct TLS](deployment/tls.md) — terminate TLS in `h2corn` itself.
- [Configuration](configuration.md) — the complete option list.
