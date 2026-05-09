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

## Programmatic shutdown

Call [`shutdown()`][h2corn.Server.shutdown] from any thread or
coroutine to begin a graceful stop. In-flight requests get up to
`Config.timeout_graceful_shutdown` seconds to complete.

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

## Which entrypoint to use

| You want…                                            | Use                                  |
| ---------------------------------------------------- | ------------------------------------ |
| The standard CLI experience, multi-worker            | `h2corn module:app`                  |
| The same behavior from Python                        | [`h2corn.serve(app, config)`][h2corn.serve] |
| A single worker inside your own event loop           | [`h2corn.Server(app, config).serve()`][h2corn.Server] |
| To embed in a test harness with programmatic stop    | [`Server`][h2corn.Server] + `shutdown()` |
