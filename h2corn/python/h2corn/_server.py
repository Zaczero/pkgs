from __future__ import annotations

import asyncio
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class _ProcessIdentity:
    uid: int | None = None
    gid: int | None = None
    username: str | None = None


@contextmanager
def _pidfile(config: Config):
    path = config.pid
    if path is None:
        yield
        return

    pid_text = f'{os.getpid()}\n'.encode()

    fd = os.open(
        path,
        (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, 'O_CLOEXEC', 0)
            | getattr(os, 'O_NOFOLLOW', 0)
        ),
        0o666,
    )

    def unlink_pidfile():
        try:
            current = os.lstat(path)
            if os.path.samestat(os.fstat(fd), current):
                path.unlink()
        except OSError:
            pass

    try:
        _ = os.write(fd, pid_text)
        yield
    finally:
        try:
            unlink_pidfile()
        finally:
            os.close(fd)


@contextmanager
def _process_umask(config: Config):
    if config.umask is None:
        yield
        return

    previous = os.umask(config.umask)
    try:
        yield
    finally:
        os.umask(previous)


def _resolve_process_identity(config: Config):
    if sys.platform == 'win32':
        if config.user is None and config.group is None:
            return _ProcessIdentity()
        raise NotImplementedError('user and group are supported only on Unix')

    import grp
    import pwd

    uid = gid = None
    username = None
    primary_gid = None
    user = config.user
    group = config.group

    if isinstance(user, int):
        uid = user
        try:
            passwd = pwd.getpwuid(uid)
        except KeyError:
            passwd = None
        else:
            username = passwd.pw_name
            primary_gid = passwd.pw_gid
    elif isinstance(user, str):
        try:
            passwd = pwd.getpwnam(user)
        except KeyError as exc:
            raise ValueError(f'unknown user: {user!r}') from exc
        uid = passwd.pw_uid
        username = passwd.pw_name
        primary_gid = passwd.pw_gid

    if isinstance(group, int):
        gid = group
    elif isinstance(group, str):
        try:
            gid = grp.getgrnam(group).gr_gid
        except KeyError as exc:
            raise ValueError(f'unknown group: {group!r}') from exc
    elif primary_gid is not None:
        gid = primary_gid

    if config.user is not None and uid is not None and gid is None:
        raise ValueError(
            'group is required when user does not resolve to a primary group'
        )

    return _ProcessIdentity(uid=uid, gid=gid, username=username)


def _drop_process_privileges(identity: _ProcessIdentity):
    if identity.uid is None and identity.gid is None:
        return

    if identity.uid is not None and identity.username is not None and os.geteuid() == 0:
        os.initgroups(
            identity.username,
            os.getegid() if identity.gid is None else identity.gid,
        )
    elif identity.gid is not None and hasattr(os, 'setgroups') and os.geteuid() == 0:
        os.setgroups([identity.gid])

    if identity.gid is not None and os.getegid() != identity.gid:
        os.setgid(identity.gid)
    if identity.uid is not None and os.geteuid() != identity.uid:
        os.setuid(identity.uid)


class Server:
    """
    In-process, single-worker ASGI server.

    Use this when you want to embed `h2corn` inside an existing event loop —
    for example, inside a test harness, a custom CLI, or an application that
    manages its own process model. For ordinary deployments, prefer
    [`serve`][h2corn.serve], which goes through the multi-worker supervisor
    and matches the `h2corn` CLI.

    The configured `bind` listeners are opened, lifespan startup runs, then
    the server processes requests until [`shutdown()`][h2corn.Server.shutdown]
    is called or the surrounding task is cancelled.

    Example:

        import asyncio
        from h2corn import Config, Server

        async def main():
            server = Server(app, Config(bind=('127.0.0.1:8000',)))
            await server.serve()

        asyncio.run(main())
    """

    def __init__(self, app: ASGIApp, config: Config | None = None) -> None:
        self.app = app
        self.config = Config() if config is None else config
        self._shutdown_future: asyncio.Future[str] | None = None

    def shutdown(self, kind: Literal['stop', 'restart'] = 'stop') -> None:
        """
        Initiate a graceful shutdown of an in-flight `serve()` call.

        Safe to call from any thread or coroutine. The currently in-flight
        requests are given up to `Config.timeout_graceful_shutdown` seconds
        to complete before the server returns.
        """
        if self._shutdown_future is not None and not self._shutdown_future.done():
            self._shutdown_future.set_result(kind)

    def restart(self) -> None:
        """
        Equivalent to `shutdown('restart')`.

        Used by the supervisor to distinguish a graceful reload from a
        terminal stop. Outside the supervisor, this behaves the same as
        `shutdown()`.
        """
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
        Run the server until shutdown.

        Binds the configured listeners, runs ASGI lifespan startup, processes
        requests, then runs lifespan shutdown when the loop exits.

        Raises `NotImplementedError` if `Config.workers` is not 1; multi-worker
        deployments must go through [`serve`][h2corn.serve].
        """
        if self.config.workers != 1:
            raise NotImplementedError(
                'Server.serve() is the in-process API and only supports workers=1'
            )

        identity = _resolve_process_identity(self.config)
        with (
            _process_umask(self.config),
            _bound_sockets(
                self.config,
                socket_owner=(identity.uid, identity.gid),
            ) as socks,
        ):
            _drop_process_privileges(identity)
            with _pidfile(self.config):
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
    Start the server. This is the primary programmatic entrypoint and matches
    the behavior of the `h2corn` CLI.

    On Unix, the call goes through the multi-worker supervisor, even for
    `workers=1`, so signal handling, inherited listeners, and worker lifecycle
    stay identical to the CLI. On Windows, a single in-process worker is used.

    Blocks until the server is asked to shut down via one of:

    | Signal              | Effect                                          |
    | ------------------- | ----------------------------------------------- |
    | `SIGINT` / `SIGTERM`| Graceful shutdown                               |
    | `SIGHUP`            | Rolling worker reload                           |
    | `SIGTTIN`           | Scale workers up by one                         |
    | `SIGTTOU`           | Scale workers down by one                       |

    Args:
        app: An ASGI 3 application callable.
        config: Server configuration. Defaults to `Config()` (one worker,
            bound to `127.0.0.1:8000`).

    Example:

        from h2corn import Config, serve
        from myapp import app

        serve(app, Config(bind=('127.0.0.1:8000',), workers=4))
    """
    config = Config() if config is None else config
    if sys.platform != 'win32':
        from ._supervisor import _serve_supervisor

        with _process_umask(config), _pidfile(config):
            _serve_supervisor(app, config)
        return

    asyncio.run(Server(app, config).serve())


def _serve_cli_target(import_settings: ImportSettings, config: Config) -> None:
    if sys.platform != 'win32' and (
        config.user is not None or config.group is not None
    ):
        from ._supervisor import _serve_supervisor

        with _process_umask(config), _pidfile(config):
            _serve_supervisor(import_settings, config)
        return

    serve(_import_target(import_settings), config)


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
    sys.path[:] = [entry for entry in sys.path if entry != import_dir]
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
    run_cli(_serve_cli_target)


if __name__ == '__main__':
    main()
