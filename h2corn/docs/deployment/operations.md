# Operations

`h2corn` runs in one of two shapes:

- The **CLI supervisor** (`h2corn module:app` or [`serve()`][h2corn.serve])
  spawns and supervises one or more worker processes. This is the
  production deployment mode and the focus of this page.
- The **embedded server** ([`Server`][h2corn.Server]) runs a single
  worker inside your own event loop — see [Embedding](../embedding.md).

The supervisor is **POSIX-only**. On Windows, `serve()` automatically
falls back to single-worker, in-process mode.

## Worker pool

```bash
h2corn hello:app --workers 4
```

The supervisor opens listeners once in the parent process and inherits
the file descriptors into each worker. Workers accept connections
directly on a [Tokio](https://tokio.rs/) runtime — no shared
user-space accept queue.

## Shared ports across processes

```bash
h2corn hello:app --reuse-port
```

`--reuse-port` sets `SO_REUSEPORT` on the TCP listeners so another server
process can bind the same port: start a new generation during a
zero-downtime deploy and stop the old one, or run several independently
managed processes behind one port. Workers of a single server always share
its listener, so the kernel's shared accept queue lets any idle worker pick
up a connection. TCP listeners only.

## Event loop

`--loop` selects the Python event-loop implementation:

```bash
h2corn hello:app --loop auto      # default
```

| Value     | Behavior                                                              |
| --------- | -------------------------------------------------------------------- |
| `auto`    | Use `uvloop` if it is installed, otherwise the stdlib asyncio loop.  |
| `asyncio` | Always the standard-library asyncio loop.                            |
| `uvloop`  | Always `uvloop`; errors at startup if it is not installed.           |

`uvloop` is an optional dependency — install it with the extra:

```bash
pip install h2corn[uvloop]
```

Unlike a pure-Python server, h2corn runs its accept loop, framing, and
socket I/O in Rust; the Python loop schedules the application's callbacks.
Choose between asyncio and uvloop from measurements of the real application.

## Free-threaded Python

On a free-threaded (no-GIL) CPython build, one worker can run the
application on several event loops in parallel:

```bash
h2corn hello:app --loop-threads 4
```

PyPI publishes distinct `cp3XXt` wheels for supported free-threaded CPython
releases. Importing h2corn does not silently re-enable the GIL.

Requests are balanced across the loops round-robin, and each request
runs entirely on one loop. Each loop runs a separate ASGI lifespan cycle
and receives its own lifespan state dictionary. Applications using loop-bound
resources should create them during lifespan startup; startup and shutdown
side effects therefore run once per loop and must be safe to repeat.

On a regular (GIL)
build `loop_threads` above 1 is a no-op — the GIL would serialize the
loops anyway, so a single loop is used. Combine with `--runtime-threads`
to scale the I/O side accordingly. Secondary loops require the built-in
`asyncio` or `uvloop` factory; h2corn rejects multiple loop threads when
embedded in an unknown custom loop rather than silently changing topology.

`--loop-threads` composes with `--workers`: each forked worker runs its
own set of loop threads, so `-w 4 --loop-threads 4` is 16 event loops.
Loop threads share the process heap but require the application and its
dependencies to support concurrent execution on free-threaded Python. Workers
provide stronger isolation and independent application state. Choose the mix
from application behavior and measured throughput, latency, and memory; keep
`workers × loop_threads` at or below the machine's core count unless a
workload-specific measurement supports oversubscription.

## Signals

The supervisor responds to four standard signals:

| Signal              | Effect                                                                |
| ------------------- | --------------------------------------------------------------------- |
| `SIGINT` / `SIGTERM`| Graceful shutdown. In-flight requests are given up to `--timeout-graceful-shutdown` seconds to finish. |
| `SIGHUP`            | Rolling reload — workers are restarted one at a time.                 |
| `SIGTTIN`           | Scale up by one worker.                                               |
| `SIGTTOU`           | Scale down by one worker.                                             |

Live scaling makes it easy to size the pool without restarting:

```bash
# Add two workers
kill -SIGTTIN $(cat /var/run/h2corn.pid)
kill -SIGTTIN $(cat /var/run/h2corn.pid)

# Drop one worker
kill -SIGTTOU $(cat /var/run/h2corn.pid)
```

Use `--pid /var/run/h2corn.pid` so deployment tooling can find the
supervisor reliably.

## Worker recycling

Retire workers after a request budget to stagger memory growth and
other long-tail process state:

```bash
h2corn hello:app \
  --workers 4 \
  --max-requests 50000 \
  --max-requests-jitter 5000
```

The supervisor adds up to `--max-requests-jitter` extra requests to each
worker's budget, so retirements are spread out over time rather than
firing at the same instant on every worker.

## Health checks

Each worker emits a periodic heartbeat to the supervisor. If the
supervisor does not see a heartbeat within
`--timeout-worker-healthcheck` seconds (default 30), the worker is
replaced. This protects against a worker getting wedged in a busy loop
or a blocking syscall that never returns to the event loop.

Set `--timeout-worker-healthcheck 0` to disable.

## Crash backoff

Workers that crash on startup are restarted with exponential backoff.
A sustained crash loop will eventually stop the supervisor instead of
respawning forever, so a misconfigured deployment fails loudly rather
than burning resources.

## TOML config files

For anything more than a handful of flags, prefer a TOML file:

```toml title="h2corn.toml"
--8<-- "h2corn.toml"
```

```bash
h2corn hello:app --config h2corn.toml
# or
H2CORN_CONFIG=h2corn.toml h2corn hello:app
```

CLI flags still win over TOML values, so a deploy can override one
setting without rewriting the file.

## Process identity

Drop privileges after binding to a low port:

```bash
sudo h2corn hello:app \
  --bind 0.0.0.0:443 \
  --certfile /etc/ssl/example/fullchain.pem \
  --keyfile /etc/ssl/example/privkey.pem \
  --user www-data \
  --group www-data
```

The supervisor binds the listeners as root, then resolves
`--user`/`--group` and switches identity in the workers before the
ASGI app is imported. Unix sockets created by the supervisor inherit
the same ownership, with permissions controlled by `--uds-permissions`.

## Observability

`h2corn` does not bundle a metrics endpoint, structured-log emitter,
or trace exporter. ASGI is the right place to add those — anything you
plug in works the same regardless of which ASGI server is in front,
and server upgrades stay independent from instrumentation changes.

Common drop-in middleware:

| Need                       | Library                                                                                              |
| -------------------------- | ---------------------------------------------------------------------------------------------------- |
| Prometheus `/metrics`      | [`prometheus-fastapi-instrumentator`](https://github.com/trallnag/prometheus-fastapi-instrumentator) (FastAPI) or [`starlette-prometheus`](https://github.com/perdy/starlette-prometheus) (any Starlette/ASGI app) |
| OpenTelemetry traces       | [`opentelemetry-instrumentation-asgi`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/asgi/asgi.html) |
| Structured / JSON access logs | [`asgi-correlation-id`](https://github.com/snok/asgi-correlation-id) + [`structlog`](https://www.structlog.org/) wired into your app's logging config |
| Liveness / readiness       | A plain ASGI route — e.g. FastAPI `@app.get('/healthz')` — exposed to the orchestrator              |

Sketch with FastAPI + Prometheus:

```python title="app.py"
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
Instrumentator().instrument(app).expose(app)
```

```bash
h2corn app:app --workers 4 --no-http1
# /metrics is now scraped by Prometheus on the same listener
```

## Full option reference

Every option above — and several more not covered here — is documented
with its CLI flag, environment variable, TOML key, and default in the
[Configuration reference](../configuration.md).
