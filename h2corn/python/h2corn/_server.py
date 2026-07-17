from __future__ import annotations

import asyncio
import os
import re
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Literal, cast

from ._cli import ImportSettings, run_cli
from ._config import Config
from ._lifespan import cancel_task, serve_with_lifespan
from ._socket import bound_addresses, bound_sockets, nonblocking_pipe

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from ._lib import LifespanHandoff
    from ._types import Application, ASGIApp


_ENV_KEY_PATTERN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')


def event_loop_factory(loop: str) -> Callable[[], asyncio.AbstractEventLoop]:
    """Return the explicit worker-loop constructor for ``loop``."""
    import importlib.util

    if loop == 'asyncio' or importlib.util.find_spec('uvloop') is None:
        if loop == 'uvloop':
            raise RuntimeError(
                "loop='uvloop' requires the uvloop package — install the "
                "'h2corn[uvloop]' extra"
            )
        return asyncio.new_event_loop
    import uvloop

    return uvloop.new_event_loop


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
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

    try:
        _ = os.write(fd, pid_text)
        yield
    finally:
        try:
            is_ours = os.path.samestat(os.fstat(fd), os.lstat(path))
        except OSError:
            is_ours = False
        os.close(fd)
        if is_ours:
            try:
                path.unlink()
            except OSError:
                pass


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


def resolve_process_identity(config: Config) -> ProcessIdentity:
    if sys.platform == 'win32':
        if config.user is None and config.group is None:
            return ProcessIdentity()
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

    return ProcessIdentity(uid=uid, gid=gid, username=username)


def drop_process_privileges(identity: ProcessIdentity) -> None:
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

    app: Application
    config: Config

    def __init__(self, app: Application, config: Config | None = None) -> None:
        self.app = app
        self.config = Config() if config is None else config
        self._state_lock = threading.Lock()
        self._shutdown_future: asyncio.Future[str] | None = None
        self._quiesce_write_fd: int | None = None
        self._serve_loop: asyncio.AbstractEventLoop | None = None
        self._pending_shutdown: Literal['stop', 'restart'] | None = None
        self._serving = False
        self._addresses: tuple[str, ...] = ()

    @property
    def addresses(self) -> tuple[str, ...]:
        """
        Resolved listener addresses, in `Config.bind` string form.

        Empty until [`serve()`][h2corn.Server.serve] has bound the listeners.
        Unlike `Config.bind`, these carry the port the kernel actually
        assigned — bind to port `0` and read the address back from here:

            server = Server(app, Config(bind=('127.0.0.1:0',)))
            task = asyncio.create_task(server.serve())
            while not server.addresses:
                await asyncio.sleep(0)
            host, _, port = server.addresses[0].rpartition(':')
        """
        with self._state_lock:
            return self._addresses

    def shutdown(self, kind: Literal['stop', 'restart'] = 'stop') -> None:
        """
        Initiate a graceful shutdown of an in-flight `serve()` call.

        Safe to call from any thread or coroutine. Native acceptance stops
        immediately and cooperative tasks receive up to
        `Config.timeout_graceful_shutdown` seconds to finish. Arbitrary
        synchronous Python code cannot be preempted in-process; use supervised
        workers when a hard kill bound is required.
        """
        with self._state_lock:
            future = self._shutdown_future
            loop = self._serve_loop
            if future is None or loop is None:
                # Lifespan startup runs before the Rust server creates its
                # shutdown future. Retain the first request so a caller can
                # stop a Server as soon as its lifecycle has begun.
                if self._serving and self._pending_shutdown is None:
                    self._pending_shutdown = kind
                return

            quiesce_write_fd = self._quiesce_write_fd
            self._quiesce_write_fd = None
            if quiesce_write_fd is not None:
                try:
                    os.write(quiesce_write_fd, b'R' if kind == 'restart' else b'S')
                except OSError:
                    # Closing the writer becomes a fail-closed native stop.
                    pass
                finally:
                    os.close(quiesce_write_fd)

            def _resolve() -> None:
                if not future.done():
                    future.set_result(kind)

            # Keep the lifecycle state locked through publication to prevent
            # cleanup from closing the loop between the snapshot and enqueue.
            loop.call_soon_threadsafe(_resolve)

    def restart(self) -> None:
        """
        Equivalent to `shutdown('restart')`.

        Used by the supervisor to distinguish a graceful reload from a
        terminal stop. Outside the supervisor, this behaves the same as
        `shutdown()`.
        """
        self.shutdown('restart')

    async def serve_inherited_fds(
        self,
        app: Application,
        fds: list[int],
        retire_trigger: Callable[[], None] | None = None,
        lifespan_handoff: LifespanHandoff | None = None,
        ready_trigger: Callable[[], None] | None = None,
        quiesce_fd: int | None = None,
    ) -> None:
        """Transfer listener ownership into one native serve lifecycle.

        This is the supervisor/worker entry seam: forked workers hand their
        inherited listener fds (and control triggers) straight to the native
        server, bypassing the socket binding that [`serve()`]
        [h2corn.Server.serve] performs. Ownership of `fds` transfers
        unconditionally. Most embedders want `serve()` instead.
        """
        from ._lib import serve_fds

        loop = asyncio.get_running_loop()
        shutdown_future = loop.create_future()
        internal_quiesce_write_fd: int | None = None
        if quiesce_fd is None and sys.platform != 'win32':
            quiesce_fd, internal_quiesce_write_fd = nonblocking_pipe()
        with self._state_lock:
            if self._shutdown_future is not None:
                if internal_quiesce_write_fd is not None:
                    assert quiesce_fd is not None
                    os.close(quiesce_fd)
                    os.close(internal_quiesce_write_fd)
                raise RuntimeError('this Server already has an active serve() call')
            self._serve_loop = loop
            self._shutdown_future = shutdown_future
            self._quiesce_write_fd = internal_quiesce_write_fd
            pending_shutdown = self._pending_shutdown
            self._pending_shutdown = None

        if pending_shutdown is not None:
            self.shutdown(pending_shutdown)

        async def _await_shutdown() -> Literal['stop', 'restart']:
            return await shutdown_future

        shutdown_task = asyncio.create_task(_await_shutdown())
        cancellation: asyncio.CancelledError | None = None
        try:
            native_serve = asyncio.ensure_future(
                serve_fds(
                    app,
                    fds,
                    self.config,
                    shutdown_task,
                    retire_trigger,
                    lifespan_handoff,
                    ready_trigger,
                    quiesce_fd,
                )
            )
            while True:
                shielded = asyncio.shield(native_serve)
                try:
                    # Rust remains the sole owner until its bounded graceful
                    # drain and loop-thread teardown actually complete.
                    await shielded
                    break
                except asyncio.CancelledError as exc:
                    if native_serve.cancelled():
                        # asyncio.shield is cancelled both when its caller is
                        # cancelled and when the inner future is cancelled.
                        # Inspect the owned future to distinguish the latter;
                        # its cancellation is a native failure, not a graceful
                        # shutdown request to absorb.
                        await native_serve
                    if cancellation is None:
                        cancellation = exc
                        self.shutdown()
                    # A second cancel must not expose a half-drained Server.
                    continue
            if cancellation is not None:
                raise cancellation
        finally:
            try:
                await cancel_task(shutdown_task)
            finally:
                with self._state_lock:
                    quiesce_write_fd = self._quiesce_write_fd
                    self._quiesce_write_fd = None
                    if self._shutdown_future is shutdown_future:
                        self._shutdown_future = None
                        self._serve_loop = None
                if quiesce_write_fd is not None:
                    os.close(quiesce_write_fd)

    async def serve(self) -> None:
        """
        Run the server until shutdown.

        Binds the configured listeners, runs ASGI lifespan startup, processes
        requests, then runs lifespan shutdown when the loop exits.

        One lifecycle at a time, reusable afterwards: once a `serve()` call
        has fully finished, the same `Server` can `serve()` again with fresh
        shutdown state, while a second concurrent call raises `RuntimeError`.
        Cancelling the `serve()` task does not abort in-flight work — it
        triggers the same bounded graceful drain as
        [`shutdown()`][h2corn.Server.shutdown] and re-raises the cancellation
        only after the native server has fully drained and released its
        listeners.

        Raises `NotImplementedError` if `Config.workers` is not 1; multi-worker
        deployments must go through [`serve`][h2corn.serve].
        """
        if self.config.workers != 1:
            raise NotImplementedError(
                'Server.serve() is the in-process API and only supports workers=1'
            )

        with self._state_lock:
            if self._serving:
                raise RuntimeError('this Server already has an active serve() call')
            self._serving = True
            self._pending_shutdown = None
        try:
            await self._serve_once()
        finally:
            with self._state_lock:
                self._serving = False
                self._pending_shutdown = None

    async def _serve_once(self) -> None:
        """Run one reusable server lifecycle after concurrency is guarded."""
        identity = resolve_process_identity(self.config)
        with (
            _process_umask(self.config),
            bound_sockets(
                self.config,
                socket_owner=(identity.uid, identity.gid),
            ) as socks,
        ):
            with self._state_lock:
                self._addresses = bound_addresses(socks)
            try:
                drop_process_privileges(identity)
                with _pidfile(self.config):
                    from ._lib import emit_banner

                    # replace() must clear the synced host/port convenience pair:
                    # bind plus host/port together fail validation.
                    emit_banner(
                        replace(self.config, bind=self.addresses, host=None, port=None)
                    )

                    async def _serve_app(
                        app: Application,
                        lifespan_handoff: LifespanHandoff | None,
                    ) -> None:
                        await self.serve_inherited_fds(
                            app,
                            [sock.detach() for sock in socks],
                            lifespan_handoff=lifespan_handoff,
                        )

                    await serve_with_lifespan(
                        self.app,
                        _serve_app,
                        mode=self.config.lifespan,
                        startup_timeout=self.config.timeout_lifespan_startup,
                        shutdown_timeout=self.config.timeout_lifespan_shutdown,
                    )
            finally:
                with self._state_lock:
                    self._addresses = ()


def serve(app: Application, config: Config | None = None) -> None:
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
        from ._supervisor import serve_with_supervisor

        with _process_umask(config), _pidfile(config):
            serve_with_supervisor(app, config)
        return

    with asyncio.Runner(loop_factory=event_loop_factory(config.loop)) as runner:
        runner.run(Server(app, config).serve())


def _serve_import_target(import_settings: ImportSettings, config: Config) -> None:
    if sys.platform != 'win32' and (
        config.user is not None or config.group is not None
    ):
        from ._supervisor import serve_with_supervisor

        with _process_umask(config), _pidfile(config):
            serve_with_supervisor(import_settings, config)
        return

    serve(import_target(import_settings), config)


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


def import_target(import_settings: ImportSettings) -> ASGIApp:
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
        return cast('ASGIApp', target_obj)

    app = target_obj()
    if not callable(app):
        raise TypeError(f'import target {target!r} factory returned a non-callable')
    return cast('ASGIApp', app)


def main() -> None:
    run_cli(_serve_import_target)


if __name__ == '__main__':
    main()
