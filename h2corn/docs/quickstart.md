---
description: Run an ASGI application with h2corn, enable reload, and try HTTP/2.
---

# Quickstart

Run an existing ASGI app with `h2corn module:app`, or start with the FastAPI
example below.

## Install

=== "uv"

    ```bash
    uv add h2corn fastapi
    ```

=== "pip"

    ```bash
    pip install h2corn fastapi
    ```

`h2corn` supports **Python 3.11 or newer**. Wheels are published for Linux,
macOS, and Windows, including ABI-matched free-threaded CPython 3.14t builds.
The multi-worker supervisor is Unix-only — see
[Operations](deployment/operations.md#worker-pool).

With uv, prefix the commands below with `uv run`.

## Run your first server

Save as `hello.py`:

```python title="hello.py"
--8<-- "hello.py"
```

Start the development server:

```bash
h2corn hello:app
```

With no address given, h2corn binds [`127.0.0.1:8000`](configuration.md#option-bind) —
reachable from this machine and nowhere else. Visit <http://127.0.0.1:8000/> in
your browser. The response is `{"message": "hello from h2corn"}`.

Browsers use HTTP/1.1 here: they require TLS for HTTP/2.

## Try HTTP/2 {#prove-h2c-with-prior-knowledge}

An HTTP/2 client must opt into cleartext prior knowledge. Use a curl build that
lists HTTP/2 in `curl --version`:

```bash
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

<span id="deploy"></span>
<span id="migrate-from-another-asgi-server"></span>
<span id="from-uvicorn"></span>
<span id="from-hypercorn"></span>
<span id="from-gunicorn"></span>
<span id="framework-boundary"></span>

## Next steps

- [Behind a proxy](deployment/proxy.md#behind-a-reverse-proxy) — nginx, Caddy,
  and HAProxy recipes with TLS at the edge and a private `h2c` upstream.
- [Direct TLS](deployment/tls.md#direct-tls) — terminate TLS in `h2corn` itself
  for single-server deployments.
- [Operations](deployment/operations.md) — multi-worker supervisor,
  signals, rolling reload, live scaling, and recycling.
- [Migrating](migrating.md) — Uvicorn, Hypercorn, and Gunicorn commands and
  behavior differences.
- [Configuration](configuration.md#option-index) — every option, in CLI, environment,
  and TOML form.
