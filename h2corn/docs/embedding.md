---
description: Run h2corn inside your own event loop with the Server class — startup, shutdown, readiness, and reuse.
---

# Embedding

The CLI (`h2corn module:app`) and the [`serve()`][h2corn.serve] function
cover the common case: spawn the server as a top-level process. When you
need finer control — running inside an existing event loop, supervising
the server from your own code, or driving it from tests — reach for the
[`Server`][h2corn.Server] class instead. Full signatures are in the
[API reference](api/index.md).

## Inside an asyncio app

```python title="embedded.py"
--8<-- "embedded.py"
```

[`Server.serve()`][h2corn.Server.serve] is an async function that runs
until the server is asked to shut down. It is single-worker by design;
when you need multiple workers, fall back to [`serve()`][h2corn.serve],
which goes through the same multi-process supervisor as the CLI.

`Server.serve()` owns an in-process lifecycle, so it rejects a configuration
that combines `pid` with `user` or `group`: the pidfile and privilege change
need one supervisor to own their ordering. On Unix, use
[`h2corn.serve()`][h2corn.serve] or the CLI for that topology; their
supervisor owns the pidfile and manages worker privilege changes.

## Programmatic shutdown

Call [`shutdown()`][h2corn.Server.shutdown] from any thread or
coroutine to begin a graceful stop. In-flight requests get up to
`Config.timeout_graceful_shutdown` seconds to complete, and their
cleanup gets the same budget again once they are cancelled.

`serve()` stops waiting when that budget is spent, whatever the
application does — one that catches `CancelledError` and never finishes
cannot hold it open. An **explicit** `shutdown()` that hits the deadline
raises `RuntimeError` rather than returning as if the stop had succeeded.

Requests that outlive the budget still hold the server: lifespan shutdown
waits for them, [`releasing`][h2corn.Server.releasing] stays true, and a
`serve()` call made in that window raises
`RuntimeError: this Server is still releasing a previous serve() call`.

```python
import asyncio
from h2corn import Config, Server
from hello import app


async def main():
    server = Server(app, Config(bind=('127.0.0.1:8000',)))

    async def stop_after(delay: float):
        await asyncio.sleep(delay)
        server.shutdown()

    await asyncio.gather(server.serve(), stop_after(5.0))


asyncio.run(main())
```

## Lifecycle

One `serve()` call at a time, and a `Server` can be reused once that call has
let go of everything it held.

- **Sequential reuse works.** After `shutdown()`, cancellation, or a startup
  failure — and after any straggling requests and lifespan shutdown have
  finished — the same `Server` can `serve()` again with fresh state.
- **Concurrent calls are rejected.** A second `serve()` while the first is
  still running, or still winding down, raises
  `RuntimeError: this Server already has an active serve() call` or
  `RuntimeError: this Server is still releasing a previous serve() call`.
  [`releasing`][h2corn.Server.releasing] tells you which situation you are in:
  it stays true after `serve()` returns until reuse is safe.
- **Cancellation drains gracefully.** Cancelling the task running `serve()`
  does not abort in-flight work — it starts the same bounded drain as
  `shutdown()`. The difference is what surfaces: cancellation always re-raises
  `CancelledError` once the drain finishes or its budget runs out, never the
  over-budget `RuntimeError`.
- **The public surface is five names:** `serve`, `shutdown`, `wait_started`,
  `addresses`, and `releasing`.

## Knowing when the server is up

`serve()` runs until the server stops, so it is normally a task — and
whoever started it usually needs to know the port is open before doing
anything else. [`Server.wait_started()`][h2corn.Server.wait_started]
answers exactly that:

```python
serving = asyncio.create_task(server.serve())
await server.wait_started()
# Requests sent from here are accepted.
```

It resolves once the server is serving, not when the listeners open — those
can be bound while lifespan startup is still running, and `wait_started()`
stays pending through that. A failed bind, a failed lifespan startup, or a
shutdown before readiness raises instead, to everyone waiting at the time.
Only success is remembered: start waiting after a failed attempt and you are
asking about the next one.

Readiness is a fact about the process rather than about one event loop, so a
`Server` driven from another thread's loop can still be awaited from yours.

## Binding to any free port

Bind port `0` and read the kernel-assigned address back from
[`Server.addresses`][h2corn.Server.addresses] — ideal for test harnesses
and service discovery:

```python
server = Server(app, Config(bind=('127.0.0.1:0',)))
serving = asyncio.create_task(server.serve())
# Resolves when the listeners are live, and raises whatever stopped the
# server if it never gets that far.
await server.wait_started()
print(server.addresses)  # ('127.0.0.1:54123',)
```

When several TCP listeners all bind port `0` (for example `0.0.0.0:0`
plus `[::]:0`), they deliberately share one kernel-assigned port.

## Which entrypoint to use

| You want…                                            | Use                                  |
| ---------------------------------------------------- | ------------------------------------ |
| The standard CLI experience, multi-worker            | `h2corn module:app`                  |
| The same behavior from Python                        | [`h2corn.serve(app, config)`][h2corn.serve] |
| A single worker inside your own event loop           | [`h2corn.Server(app, config).serve()`][h2corn.Server] |
| To embed in a test harness with programmatic stop    | [`Server`][h2corn.Server] + `shutdown()` |
