from __future__ import annotations

import asyncio
import os
import sys
from typing import Literal

from ._cli import run_cli
from ._config import Config
from ._lifespan import _cancel_task, _serve_with_lifespan
from ._socket import _bound_socket

TYPE_CHECKING = False

if TYPE_CHECKING:
    from ._types import ASGIApp


class Server:
    def __init__(self, app: ASGIApp, config: Config | None = None) -> None:
        self.app = app
        self.config = Config() if config is None else config
        self._shutdown_future: asyncio.Future[str] | None = None

    def shutdown(self, kind: Literal['stop', 'restart'] = 'stop') -> None:
        """Signal the server to initiate a graceful shutdown."""
        if self._shutdown_future is not None and not self._shutdown_future.done():
            self._shutdown_future.set_result(kind)

    def restart(self) -> None:
        """Signal the server to initiate a graceful restart-style shutdown."""
        self.shutdown('restart')

    async def _serve_fd(
        self,
        app: ASGIApp,
        fd: int,
        retire_trigger=None,
    ) -> None:
        from ._lib import serve_fd

        if self._shutdown_future is None:
            self._shutdown_future = asyncio.get_running_loop().create_future()

        async def _await_shutdown() -> str:
            return await self._shutdown_future

        shutdown_task = asyncio.create_task(_await_shutdown())
        try:
            await serve_fd(app, fd, self.config, shutdown_task, retire_trigger)
        finally:
            await _cancel_task(shutdown_task)

    async def serve(self) -> None:
        """
        Serve the ASGI application asynchronously.

        Raises `NotImplementedError` if `Config.workers` is not 1.
        """
        if self.config.workers != 1:
            raise NotImplementedError(
                'Server.serve() is the in-process API and only supports workers=1'
            )

        with _bound_socket(self.config) as sock:
            from ._lib import emit_banner

            emit_banner(self.config)

            async def _serve_app(app: ASGIApp) -> None:
                fd = sock.detach()
                await self._serve_fd(app, fd)

            await _serve_with_lifespan(
                self.app,
                _serve_app,
                startup_timeout=self.config.timeout_lifespan_startup,
                shutdown_timeout=self.config.timeout_lifespan_shutdown,
            )


def serve(app: ASGIApp, config: Config | None = None) -> None:
    """
    Primary entrypoint to start the server.

    On Unix-like systems, if the configuration specifies multiple workers,
    this launches a multiprocessing supervisor that supports zero-downtime
    reloads and dynamic scaling.

    Available signals for dynamic control (Unix only):
    - `SIGHUP`: Reload workers.
    - `SIGTTIN` / `SIGTTOU`: Scale workers up / down.
    - `SIGINT` / `SIGTERM`: Graceful shutdown.

    On Windows, this always falls back to running a single-worker in-process server.
    """
    config = Config() if config is None else config
    if sys.platform != 'win32':
        from ._supervisor import _serve_supervisor

        _serve_supervisor(app, config)
        return

    asyncio.run(Server(app, config).serve())


def _import_target(target: str) -> ASGIApp:
    import importlib

    module_name, _, attr = target.partition(':')
    if not module_name or not attr:
        raise ValueError('target must be module:app')

    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())

    module = importlib.import_module(module_name)
    app = getattr(module, attr)
    if not callable(app):
        raise TypeError(f'{target} is not callable')
    return app


def main() -> None:
    run_cli(serve, _import_target)


if __name__ == '__main__':
    main()
