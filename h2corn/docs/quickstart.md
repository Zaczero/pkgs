# Quickstart

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
macOS, and Windows; the multi-worker supervisor is Unix-only — see
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

You'll see something like this in your terminal:

```text
h2corn v1.4.0 • HTTP/2 ASGI
Listening on http://127.0.0.1:8000
HTTP/1 compatibility is enabled; disable with --no-http1

Started worker [12345]
127.0.0.1:54321 "GET / HTTP/1.1" 200 0.4ms tx=25b
```

Visit <http://127.0.0.1:8000/> in your browser. You should see
`{"message": "hello from h2corn"}` come back.

!!! info "Why does the response say HTTP/1.1?"
    Browsers do not speak cleartext `h2c`, so the development server
    keeps HTTP/1.1 enabled for local testing. In production, the edge
    advertises HTTPS — either through a
    [reverse proxy](deployment/proxy.md) or `h2corn`'s own
    [Direct TLS](deployment/tls.md) — and you turn HTTP/1.1 off with
    `--no-http1`.

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
rather than a module-level attribute. Pass `--factory` and `h2corn`
will call it for you:

```python title="factory.py"
--8<-- "factory.py"
```

```bash
h2corn factory:create_app --factory
```

## Deploy

In production, `h2corn` typically sits behind a reverse proxy that
terminates browser-facing TLS. The application server itself runs on a
local listener with several workers, looking something like this:

```bash
h2corn hello:app \
  --bind 127.0.0.1:8000 \
  --workers 4 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1,::1,unix \
  --no-http1
```

Each flag above makes a deliberate choice worth understanding before
you ship it:

- **`--bind 127.0.0.1:8000`** listens on loopback only so the proxy can
  reach it locally. Use `0.0.0.0:port` if you want a public-facing TCP
  listener instead.
- **`--workers 4`** is a reasonable starting point on a 4-core box.
  Size it to your workload and revisit after measuring.
- **`--proxy-headers`** trusts standard `Forwarded` and `X-Forwarded-*`
  headers, but only when they come from the peers listed in
  `--forwarded-allow-ips`. Set that list to wherever your proxy
  actually connects from.
- **`--no-http1`** is a fail-closed hardening flag. Once the upstream
  is configured to speak `h2c`, an accidental fallback fails
  immediately instead of quietly serving traffic on the older protocol.

Once you have more than a handful of flags, prefer a
[TOML config file](deployment/operations.md#toml-config-files) — same
keys, single source of truth, easier to review in a pull request.

## Next steps

- [Behind a proxy](deployment/proxy.md) — full Caddy and HAProxy
  recipes with `h2c` upstream.
- [Direct TLS](deployment/tls.md) — terminate TLS in `h2corn` itself
  for single-server deployments.
- [Operations](deployment/operations.md) — multi-worker supervisor,
  signals, rolling reload, live scaling, and recycling.
- [Configuration](configuration.md) — every option, in CLI, environment,
  and TOML form.
