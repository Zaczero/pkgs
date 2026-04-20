from __future__ import annotations

import asyncio
import os
import re
import sys
from typing import Literal

from ._cli import ImportSettings, run_cli
from ._config import Config
from ._lifespan import _cancel_task, _serve_with_lifespan
from ._socket import _bound_sockets

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from ._types import ASGIApp


_ENV_KEY_PATTERN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')


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

    async def _serve_fds(
        self,
        app: ASGIApp,
        fds: list[int],
        retire_trigger: Callable[[], None] | None = None,
    ):
        from ._lib import serve_fds

        if self._shutdown_future is None:
            self._shutdown_future = asyncio.get_running_loop().create_future()

        async def _await_shutdown():
            return await self._shutdown_future

        shutdown_task = asyncio.create_task(_await_shutdown())
        try:
            await serve_fds(app, fds, self.config, shutdown_task, retire_trigger)
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

        with _bound_sockets(self.config) as socks:
            from ._lib import emit_banner

            emit_banner(self.config)

            async def _serve_app(app: ASGIApp):
                await self._serve_fds(app, [sock.detach() for sock in socks])

            await _serve_with_lifespan(
                self.app,
                _serve_app,
                mode=self.config.lifespan,
                startup_timeout=self.config.timeout_lifespan_startup,
                shutdown_timeout=self.config.timeout_lifespan_shutdown,
            )


def serve(app: ASGIApp, config: Config | None = None) -> None:
    """
    Primary entrypoint to start the server.

    On Unix-like systems, this runs through the multiprocessing supervisor,
    even for `workers=1`, so signal handling, inherited listeners, and worker
    lifecycle stay consistent with the CLI entrypoint.

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


def _load_env_file(path: Path):
    with path.open(encoding='utf-8') as handle:
        for number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('export '):
                line = line[7:].lstrip()

            key, sep, value = line.partition('=')
            key = key.strip()
            if not sep or not _ENV_KEY_PATTERN.fullmatch(key):
                raise ValueError(f'invalid env file entry at {path}:{number}')

            value = value.strip()
            if value[:1] in {'"', "'"}:
                if value[-1:] != value[:1]:
                    raise ValueError(f'invalid quoted env value at {path}:{number}')
                try:
                    import ast

                    parsed = ast.literal_eval(value)
                except (SyntaxError, ValueError) as exc:
                    raise ValueError(
                        f'invalid quoted env value at {path}:{number}'
                    ) from exc
                if not isinstance(parsed, str):
                    raise ValueError(f'invalid quoted env value at {path}:{number}')
                value = parsed
            elif ' #' in value:
                value = value.split(' #', 1)[0].rstrip()

            os.environ.setdefault(key, value)


def _import_target(import_settings: ImportSettings):
    import importlib

    target = import_settings.target
    module_name, _, attr = target.partition(':')
    if not module_name or not attr:
        raise ValueError('target must be in module:app form')

    if import_settings.env_file is not None:
        _load_env_file(import_settings.env_file)

    import_dir = (
        os.getcwd()
        if import_settings.app_dir is None
        else os.fspath(import_settings.app_dir)
    )
    if import_dir not in sys.path:
        sys.path.insert(0, import_dir)

    module = importlib.import_module(module_name)
    target_obj = getattr(module, attr)
    if not callable(target_obj):
        raise TypeError(f'import target {target!r} is not callable')
    if not import_settings.factory:
        return target_obj

    app = target_obj()
    if not callable(app):
        raise TypeError(f'import target {target!r} factory returned a non-callable')
    return app


def main() -> None:
    run_cli(serve, _import_target)


if __name__ == '__main__':
    main()
