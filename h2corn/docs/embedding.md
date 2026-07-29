# Embedding

The CLI (`h2corn module:app`) and the [`serve()`][h2corn.serve] function
cover the common case: spawn the server as a top-level process. When you
need finer control — running inside an existing event loop, supervising
the server from your own code, or driving it from tests — reach for the
[`Server`][h2corn.Server] class instead.

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
raises `RuntimeError` naming what is still running rather than returning
as if the stop had succeeded. An over-budget drain does **not** skip
lifespan shutdown: that phase is deferred until the straggling requests
finally release. Until then
[`releasing`][h2corn.Server.releasing] is true and the same `Server`
cannot `serve()` again. A call made in that window raises
`RuntimeError: this Server is still releasing a previous serve() call`.
Fix the application if you see the deadline error.

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

A `Server` owns one serve generation at a time. That generation remains
the sole owner of listeners, native drain, primary lifespan, secondary
lifespans and the reuse guard until every one has actually released. A
public return or timeout is not ownership release.

- **Sequential reuse is supported.** Once a generation has fully
  released — after `shutdown()`, cancellation, or a startup failure, and
  after any late request cleanup and lifespan shutdown — the same
  `Server` instance can `serve()` again with fresh shutdown state.
- **Concurrent calls are rejected.** A second `serve()` while a
  generation is active (including while [`releasing`][h2corn.Server.releasing]
  is true after the public caller returned) raises
  `RuntimeError: this Server already has an active serve() call` or
  `RuntimeError: this Server is still releasing a previous serve() call`.
- **Cancellation drains gracefully.** Cancelling the task running
  `serve()` does not abort in-flight work: it triggers the same bounded
  graceful drain as `shutdown()` (native acceptance stops, cooperative
  tasks get up to `Config.timeout_graceful_shutdown` seconds). Unlike
  explicit shutdown, cancellation always re-raises `CancelledError`
  after the drain completes or its budget runs out — it does not surface
  the unreleased-drain `RuntimeError`. Lifespan shutdown still follows
  the native drain (deferred until requests release if the budget ran
  out first).
- **Public surface.** Embedders use `serve`, `shutdown`, `wait_started`,
  `addresses`, and `releasing` — nothing else.

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

It resolves only after lifespan startup and native acceptance — listeners
may already be bound while startup is still running, and
`wait_started()` stays pending until the server is actually serving.
A failure (failed bind, failed lifespan startup, shutdown before ready)
is delivered to every waiter registered for **that** lifecycle; only a
successful start is remembered for later callers. Someone who begins
waiting after a failed generation is asking about the next one, not
inheriting the previous answer. Readiness is a fact about the process,
not about one event loop: a `Server` driven from another thread's loop
can still be awaited from yours.

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
