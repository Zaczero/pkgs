from __future__ import annotations

import asyncio
import os
import re
import sys
import threading
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import cast

from h2corn._cli import ImportSettings, run_cli
from h2corn._config import Config
from h2corn._lifespan import LifespanRunner, await_with_timeout, cancel_task
from h2corn._socket import (
    ListenerLease,
    bound_addresses,
    bound_sockets,
    lease_owned_fds,
    nonblocking_pipe,
)
from h2corn._systemd import notify_ready, notify_stopping

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence
    from pathlib import Path

    from h2corn._lib import _LifespanHandoff, _PreparedTls
    from h2corn._types import Application, ASGIApp


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


@dataclass(frozen=True, slots=True)
class TlsMaterial:
    """
    PEM bytes for the configured TLS identity.

    A private key is normally readable only by the user the server starts
    as, so it is a privileged resource in exactly the way a port below 1024
    is: read it while the process can, and carry the bytes across the
    `setuid` that follows.
    """

    certificate: bytes
    private_key: bytes
    client_ca: bytes | None


def _read_pem(path: os.PathLike[str] | str, setting: str) -> bytes:
    try:
        with open(path, 'rb') as handle:
            return handle.read()
    except OSError as exc:
        raise ValueError(f'{setting}: {exc.strerror}: {path}') from exc


def load_tls_material(config: Config) -> TlsMaterial | None:
    """Read the configured TLS files, or return `None` when TLS is off."""
    if config.certfile is None:
        return None
    # Config validation pairs certfile with keyfile, and ca_certs with
    # cert_reqs, so only the presence of a certificate is checked here.
    assert config.keyfile is not None
    return TlsMaterial(
        certificate=_read_pem(config.certfile, 'certfile'),
        private_key=_read_pem(config.keyfile, 'keyfile'),
        client_ca=(
            None if config.ca_certs is None else _read_pem(config.ca_certs, 'ca_certs')
        ),
    )


@contextmanager
def _pidfile(config: Config):
    """Open the pidfile and yield its descriptor (or `None` when unset).

    The open fd is part of the supervisor's child-discard set: workers must
    not inherit a write handle on the live pidfile.
    """
    path = config.pid
    if path is None:
        yield None
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
        yield fd
    finally:
        try:
            is_ours = os.path.samestat(os.fstat(fd), os.lstat(path))
        except OSError:
            is_ours = False
        os.close(fd)
        if is_ours:
            try:
                path.unlink()
            except OSError as exc:
                # Most often a server that dropped privileges into a
                # directory it may no longer write. Saying so beats leaving
                # an operator to find a stale pidfile and wonder.
                warnings.warn(
                    f'could not remove the pidfile {path}: {exc.strerror}',
                    stacklevel=2,
                )


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


def _resolve_on_owning_loop(
    waiter: asyncio.Future[None],
    error: BaseException | None,
) -> None:
    def settle() -> None:
        if waiter.done():
            return
        if error is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(error)

    loop = waiter.get_loop()
    if loop.is_closed():
        return
    try:
        loop.call_soon_threadsafe(settle)
    except RuntimeError:
        # The loop closed between the check and the call, which means the
        # task waiting on it is gone too. There is no one left to tell.
        pass


class _Readiness(Enum):
    """Whether this server is accepting connections, and if not, why not.

    The four states are what `wait_started()` has to distinguish: a server
    that has not begun, one on its way up, one accepting, and one on its way
    down. A server both serving and shutting down is not representable.
    """

    IDLE = 'idle'
    STARTING = 'starting'
    SERVING = 'serving'
    STOPPING = 'stopping'


class _ShutdownKind(Enum):
    STOP = 'stop'
    RESTART = 'restart'


@dataclass(slots=True)
class _ServeGeneration:
    """One exclusive serve lifecycle.

    The generation remains the sole owner of listeners, native drain, primary
    lifespan, secondary lifespans and the reuse guard until every one has
    actually released. A public return or timeout is not ownership release:
    `returned` may settle at the graceful deadline while `released` still
    owns the application.
    """

    loop: asyncio.AbstractEventLoop
    shutdown: asyncio.Future[_ShutdownKind]
    returned: asyncio.Future[None]
    released: asyncio.Task[None]
    quiesce_write_fd: int | None = None


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

    `Config` describes a whole deployment, so some of its settings belong to
    the supervisor that runs several workers rather than to one server.
    `workers` is refused outright — asking for eight and getting one is not
    a detail — while `max_requests`, `max_requests_jitter` and
    `timeout_worker_healthcheck` only say when a supervisor would replace a
    worker, so [`serve()`][h2corn.Server.serve] warns and carries on.

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
        self._generation: _ServeGeneration | None = None
        self._addresses: tuple[str, ...] = ()
        self._readiness = _Readiness.IDLE
        self._started_waiters: list[asyncio.Future[None]] = []

    def _settle_started(self, state: _Readiness, error: BaseException | None) -> None:
        """Answer everyone currently waiting to hear that this server started.

        Readiness is a fact about the process, not about one event loop: a
        `Server` may be driven from another thread's loop entirely, so each
        waiter is resolved on the loop it is waiting in.

        Only success is remembered. A failure belongs to the lifecycle it
        ended, so someone who starts waiting afterwards is asking about the
        next one and waits for it, rather than inheriting an answer to a
        question they did not ask.
        """
        with self._state_lock:
            if self._readiness is state and state is _Readiness.SERVING:
                return
            self._readiness = state
            waiters = self._started_waiters
            self._started_waiters = []
        if state is _Readiness.SERVING:
            # Same fact, reported outward. Under the supervisor this is a no-op
            # in the worker: `_worker_entry` drops `NOTIFY_SOCKET` so only the
            # supervisor, which knows when the whole fleet is serving, answers
            # for the unit.
            notify_ready()
        for waiter in waiters:
            _resolve_on_owning_loop(waiter, error)

    def _forget_started_waiter(self, waiter: asyncio.Future[None]) -> None:
        with self._state_lock:
            if waiter in self._started_waiters:
                self._started_waiters.remove(waiter)

    async def wait_started(self) -> None:
        """
        Wait until this server is accepting connections.

        Resolves once the listeners are live, so a caller that starts
        `serve()` as a task can await this instead of watching
        [`addresses`][h2corn.Server.addresses] for one to appear.

        Raises rather than waiting when waiting cannot end well: whatever
        stopped the server if it stops without ever serving, and
        `RuntimeError` if it is already shutting down — a server on its way
        down is not one that is about to come up.

        Example:

            serving = asyncio.create_task(server.serve())
            await server.wait_started()
            # the port is open here
        """
        loop = asyncio.get_running_loop()
        with self._state_lock:
            if self._readiness is _Readiness.SERVING:
                return
            if self._readiness is _Readiness.STOPPING:
                # Waiting would mean waiting for the lifecycle after this one,
                # which is not what anyone means by "wait until it starts".
                raise RuntimeError('the server is shutting down')
            waiter = loop.create_future()
            self._started_waiters.append(waiter)
        # This waiter belongs to this call alone, so cancelling it affects
        # nobody else — and awaiting it directly is what lets a caller who
        # gives up take its future off the list on the way out.
        waiter.add_done_callback(self._forget_started_waiter)
        await waiter

    @property
    def releasing(self) -> bool:
        """
        Whether a previous `serve()` is still releasing its requests.

        `serve()` stops waiting once the graceful budget is spent, but requests
        that ignored cancellation keep the application and its loop shards
        alive. While this is true the same `Server` cannot `serve()` again.
        Lifespan shutdown runs only after those requests finally release.
        """
        with self._state_lock:
            generation = self._generation
            if generation is None:
                return False
            return generation.returned.done() and not generation.released.done()

    @property
    def addresses(self) -> tuple[str, ...]:
        """
        Resolved listener addresses, in `Config.bind` string form.

        Empty until [`serve()`][h2corn.Server.serve] has bound the listeners.
        Unlike `Config.bind`, these carry the port the kernel actually
        assigned — bind to port `0` and read the address back from here:

            server = Server(app, Config(bind=('127.0.0.1:0',)))
            serving = asyncio.create_task(server.serve())
            await server.wait_started()
            host, _, port = server.addresses[0].rpartition(':')
        """
        with self._state_lock:
            return self._addresses

    def shutdown(self) -> None:
        """
        Initiate a graceful shutdown of an in-flight `serve()` call.

        Safe to call from any thread or coroutine. Native acceptance stops
        immediately and cooperative tasks receive up to
        `Config.timeout_graceful_shutdown` seconds to finish. Arbitrary
        synchronous Python code cannot be preempted in-process; use supervised
        workers when a hard kill bound is required.
        """
        self._request_shutdown(_ShutdownKind.STOP)

    def _request_restart(self) -> None:
        """Ask a supervisor-owned worker to finish this generation for restart."""
        self._request_shutdown(_ShutdownKind.RESTART)

    def _request_shutdown(self, kind: _ShutdownKind) -> None:
        """Request stop or restart on the active generation, if any.

        The generation is published before binding and lifespan startup, so an
        immediate request is retained rather than dropped on the floor.
        """
        # Acceptance stops here, so this server is no longer one that
        # `wait_started()` can report as started, and anyone still waiting to
        # hear that it did is told what actually happened instead.
        self._settle_started(
            _Readiness.STOPPING, RuntimeError('the server is shutting down')
        )
        # Only a real stop. A RESTART is a supervisor-internal generation
        # change: the unit is not shutting down, and saying so would have
        # systemd publish a state the service is not in. Under the supervisor
        # this is a no-op anyway, since workers have no `NOTIFY_SOCKET`.
        if kind is _ShutdownKind.STOP:
            notify_stopping()
        with self._state_lock:
            generation = self._generation
            if generation is None:
                return
            future = generation.shutdown
            loop = generation.loop
            quiesce_write_fd = generation.quiesce_write_fd
            generation.quiesce_write_fd = None

            def _resolve() -> None:
                if not future.done():
                    future.set_result(kind)

            # `_clear_generation()` takes this lock before its owning
            # lifecycle can be forgotten and its loop closed. Publishing while
            # the generation is still locked makes a snapshot of that loop
            # and a closed loop mutually exclusive.
            loop.call_soon_threadsafe(_resolve)

        if quiesce_write_fd is not None:
            try:
                os.write(
                    quiesce_write_fd,
                    b'R' if kind is _ShutdownKind.RESTART else b'S',
                )
            except OSError:
                # Closing the writer becomes a fail-closed native stop.
                pass
            finally:
                os.close(quiesce_write_fd)

    def _finish_returned(
        self,
        generation: _ServeGeneration,
        error: BaseException | None,
    ) -> None:
        """Settle the public caller boundary for one generation once."""
        if generation.returned.done():
            return
        if error is None:
            generation.returned.set_result(None)
        else:
            generation.returned.set_exception(error)

    def _clear_generation(
        self, generation: _ServeGeneration, task: asyncio.Task[None]
    ) -> None:
        """Drop the reuse guard only after this generation has fully released."""
        with self._state_lock:
            if self._generation is generation:
                self._generation = None
        # Consume any lifecycle exception once the public boundary has already
        # observed it (or timed out); otherwise asyncio warns on abandon.
        if not task.cancelled():
            task.exception()

    async def _await_generation(self, generation: _ServeGeneration) -> None:
        """Wait on the public boundary, absorbing cancellation into shutdown."""
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(generation.returned)
                break
            except asyncio.CancelledError as exc:
                if generation.returned.done():
                    break
                if cancellation is None:
                    cancellation = exc
                    self._request_shutdown(_ShutdownKind.STOP)
                continue
            except BaseException:
                if cancellation is not None:
                    raise cancellation from None
                raise
        if cancellation is not None:
            raise cancellation
        await generation.returned

    def _claim_generation(
        self,
        body: Callable[[_ServeGeneration], Awaitable[None]],
    ) -> _ServeGeneration:
        """Publish a generation before any binding or lifespan work begins."""
        loop = asyncio.get_running_loop()
        with self._state_lock:
            existing = self._generation
            if existing is not None:
                if existing.returned.done() and not existing.released.done():
                    raise RuntimeError(
                        'this Server is still releasing a previous serve() call'
                    )
                raise RuntimeError('this Server already has an active serve() call')

            shutdown = loop.create_future()
            returned = loop.create_future()
            holder: list[_ServeGeneration] = []

            async def _lifecycle() -> None:
                generation = holder[0]
                error: BaseException | None = None
                try:
                    await body(generation)
                except BaseException as exc:
                    error = exc
                    raise
                finally:
                    self._finish_returned(generation, error)
                    self._settle_started(
                        _Readiness.IDLE,
                        RuntimeError('the server stopped before it started serving')
                        if error is None
                        else error,
                    )

            released = loop.create_task(_lifecycle())
            generation = _ServeGeneration(
                loop=loop,
                shutdown=shutdown,
                returned=returned,
                released=released,
            )
            holder.append(generation)
            released.add_done_callback(
                lambda task, generation=generation: self._clear_generation(
                    generation, task
                )
            )
            self._generation = generation
            # Publication is one transaction: a cross-thread shutdown must
            # observe STARTING with the generation it is stopping, never a
            # later write that resurrects readiness after STOPPING.
            self._readiness = _Readiness.STARTING
        return generation

    async def serve(self) -> None:
        """
        Run the server until shutdown.

        Binds the configured listeners, runs ASGI lifespan startup, processes
        requests, then runs lifespan shutdown after the native drain has fully
        released the application.

        One lifecycle at a time, reusable afterwards: once a `serve()` call
        has fully finished, the same `Server` can `serve()` again with fresh
        shutdown state, while a second concurrent call raises `RuntimeError`.
        Cancelling the `serve()` task does not abort in-flight work — it
        triggers the same bounded graceful drain as
        [`shutdown()`][h2corn.Server.shutdown] and re-raises the cancellation
        once that drain finishes or its budget runs out.

        The budget is real: an application that catches `CancelledError` and
        never finishes cannot hold `serve()` open. `serve()` raises
        `RuntimeError` instead, naming the problem. Lifespan shutdown still
        runs after those requests finally release; until then the same
        `Server` cannot `serve()` again.

        Raises `NotImplementedError` if `Config.workers` is not 1; multi-worker
        deployments must go through [`serve`][h2corn.serve].
        """
        generation = self._claim_generation(self._serve_embedded)
        await self._await_generation(generation)

    async def serve_worker_fds(
        self,
        fds: Iterable[int],
        *,
        retire_trigger: Callable[[], None] | None = None,
        ready_trigger: Callable[[], None] | None = None,
        drain_complete_trigger: Callable[[], None] | None = None,
        quiesce_fd: int | None = None,
        prepared_tls: _PreparedTls,
    ) -> None:
        """Run one worker lifecycle from already-owned listener fds.

        Supervisor workers hand inherited listeners and control triggers here
        instead of rebinding. Ownership of `fds` transfers unconditionally.
        """
        listeners = lease_owned_fds(fds, quiesce_fd=quiesce_fd)
        released = False

        def _release_owned() -> BaseException | None:
            nonlocal released
            if released:
                return None
            released = True
            cleanup_error: BaseException | None = None
            for listener in reversed(listeners):
                try:
                    listener.release()
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
            if quiesce_fd is not None:
                try:
                    os.close(quiesce_fd)
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
            if cleanup_error is not None:
                return cleanup_error
            return None

        async def _body(generation: _ServeGeneration) -> None:
            try:
                await self._serve_with_primary_lifespan(
                    generation,
                    listeners,
                    retire_trigger=retire_trigger,
                    ready_trigger=ready_trigger,
                    drain_complete_trigger=drain_complete_trigger,
                    quiesce_fd=quiesce_fd,
                    release_pre_native_listeners=_release_owned,
                    prepared_tls=prepared_tls,
                )
            except BaseException:
                _release_owned()
                raise
            else:
                cleanup_error = _release_owned()
                if cleanup_error is not None:
                    raise cleanup_error

        try:
            generation = self._claim_generation(_body)
        except BaseException:
            _release_owned()
            raise
        await self._await_generation(generation)

    def _warn_about_supervisor_only_settings(self) -> None:
        """Name the settings this entry point cannot act on.

        These are honoured for a worker the supervisor drives (it owns the
        process model they describe), so the warning belongs to `serve()`,
        which is the entry point that has no supervisor behind it.
        """
        default = Config()
        ignored = [
            name
            for name in (
                'max_requests',
                'max_requests_jitter',
                'timeout_worker_healthcheck',
            )
            if getattr(self.config, name) != getattr(default, name)
        ]
        if ignored:
            names = ', '.join(ignored)
            verb = 'describes' if len(ignored) == 1 else 'describe'
            warnings.warn(
                f'{names} {verb} when a supervisor replaces a worker, and '
                'nothing supervises Server.serve(): it will run until you '
                'shut it down. Use h2corn.serve() to get that.',
                stacklevel=3,
            )

    async def _serve_embedded(self, generation: _ServeGeneration) -> None:
        """Bind listeners and run one embedded lifecycle for `serve()`."""
        if self.config.pid is not None and (
            self.config.user is not None or self.config.group is not None
        ):
            raise ValueError(
                'Server.serve() cannot combine pid with user or group; '
                'use h2corn.serve() so the privileged supervisor owns the pidfile'
            )
        if self.config.workers != 1:
            raise NotImplementedError(
                'Server.serve() is the in-process API and only supports workers=1'
            )
        self._warn_about_supervisor_only_settings()
        identity = resolve_process_identity(self.config)
        # Everything that needs the starting user's privileges is acquired
        # before `drop_process_privileges` below: the listening sockets and
        # the TLS key, which is routinely root-owned and mode 0600.
        # Read PEM → prepare acceptor → banner, all before the drop so workers
        # never reopen certificate paths.
        from h2corn._lib import emit_banner, prepare_tls

        tls_material = load_tls_material(self.config)
        prepared_tls = prepare_tls(self.config, tls_material)
        with (
            _process_umask(self.config),
            bound_sockets(
                self.config,
                socket_owner=(identity.uid, identity.gid),
            ) as leases,
        ):
            with self._state_lock:
                self._addresses = bound_addresses(
                    tuple(lease.socket for lease in leases if lease.socket is not None)
                )
            try:
                # The pidfile is written before the drop for the same reason
                # the listeners are bound before it: its directory is often
                # writable only by the starting user.
                with _pidfile(self.config):
                    emit_banner(
                        replace(self.config, bind=self.addresses),
                        prepared_tls,
                    )
                    drop_process_privileges(identity)
                    await self._serve_with_primary_lifespan(
                        generation,
                        leases,
                        prepared_tls=prepared_tls,
                    )
            finally:
                with self._state_lock:
                    self._addresses = ()

    async def _serve_with_primary_lifespan(
        self,
        generation: _ServeGeneration,
        listeners: Sequence[ListenerLease],
        *,
        retire_trigger: Callable[[], None] | None = None,
        ready_trigger: Callable[[], None] | None = None,
        drain_complete_trigger: Callable[[], None] | None = None,
        quiesce_fd: int | None = None,
        release_pre_native_listeners: Callable[[], BaseException | None] | None = None,
        prepared_tls: _PreparedTls,
    ) -> None:
        """Run primary lifespan around the native server for one generation."""
        mode = self.config.lifespan
        startup_timeout = self.config.timeout_lifespan_startup
        shutdown_timeout = self.config.timeout_lifespan_shutdown
        if mode == 'off':
            await self._native_serve(
                generation,
                listeners,
                lifespan_handoff=None,
                retire_trigger=retire_trigger,
                ready_trigger=ready_trigger,
                quiesce_fd=quiesce_fd,
                prepared_tls=prepared_tls,
            )
            return

        if release_pre_native_listeners is None:

            def _release_bound_listeners() -> BaseException | None:
                # Lifespan is the last fallible step before native ownership. A
                # public startup error must not leave the bound descriptors (or a
                # created Unix path) live while retained lifespan cleanup runs.
                cleanup_error: BaseException | None = None
                for listener in reversed(listeners):
                    try:
                        listener.release()
                    except BaseException as exc:
                        if cleanup_error is None:
                            cleanup_error = exc
                with self._state_lock:
                    self._addresses = ()
                return cleanup_error

            release_pre_native_listeners = _release_bound_listeners

        lifespan = LifespanRunner(self.app)
        try:
            await self._run_primary_startup(
                generation,
                lifespan,
                mode,
                startup_timeout,
                release_pre_native_listeners,
            )
        except _StartupAborted as exc:
            # Shutdown was requested before startup finished; the public
            # boundary settles without error and cancellation is re-raised
            # by the caller if that is how stop was asked for.
            if exc.cleanup_error is not None:
                raise exc.cleanup_error from None
            return
        except BaseException as exc:
            # Startup is the primary failure; cleanup must not replace it.
            release_pre_native_listeners()
            await self._retain_until_lifespan_settles(
                generation, lifespan, startup_timeout, exc
            )
            raise

        try:
            from h2corn._lib import _LifespanHandoff

            handoff = _LifespanHandoff(
                self.app,
                lifespan.state,
                mode == 'on',
                startup_timeout,
                shutdown_timeout,
            )
            await self._native_serve(
                generation,
                listeners,
                lifespan_handoff=handoff,
                retire_trigger=retire_trigger,
                ready_trigger=ready_trigger,
                quiesce_fd=quiesce_fd,
                prepared_tls=prepared_tls,
            )
        finally:
            # Native drain is complete: every request has released the
            # application, so lifespan shutdown can close what startup opened.
            # A supervised worker reports this exact boundary before starting
            # lifespan shutdown, so the supervisor can give that separate
            # phase its own hard-stop deadline.
            if drain_complete_trigger is not None:
                drain_complete_trigger()
            try:
                await await_with_timeout(
                    lifespan.shutdown(),
                    shutdown_timeout,
                    'lifespan shutdown timed out',
                )
            except BaseException as exc:
                await self._retain_until_lifespan_settles(
                    generation, lifespan, shutdown_timeout, exc
                )
                raise

    async def _run_primary_startup(
        self,
        generation: _ServeGeneration,
        lifespan: LifespanRunner,
        mode: str,
        startup_timeout: float | None,
        release_pre_native_listeners: Callable[[], BaseException | None],
    ) -> None:
        """Run primary lifespan startup, aborting if shutdown is requested first."""
        startup = asyncio.ensure_future(
            await_with_timeout(
                lifespan.startup(required=mode == 'on'),
                startup_timeout,
                'lifespan startup timed out',
            )
        )

        async def _wait_shutdown() -> None:
            # Shield so cancelling this waiter cannot cancel the generation's
            # shutdown future (asyncio cancels a Future when its last waiter
            # is cancelled).
            await asyncio.shield(generation.shutdown)

        stop = asyncio.create_task(_wait_shutdown())
        try:
            done, _pending = await asyncio.wait(
                {startup, stop},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if startup in done:
                await startup
                return
            # Shutdown won the race: abandon startup so a cooperative app
            # observes cancellation, and retain ownership if it does not.
            try:
                cleanup_error = release_pre_native_listeners()
            except BaseException as exc:
                cleanup_error = exc
            if cleanup_error is None:
                self._finish_returned(generation, None)
            startup.cancel()
            try:
                await startup
            except (Exception, asyncio.CancelledError):
                pass
            settled = await lifespan.discard_task(startup_timeout)
            if not settled:
                await lifespan.wait_retained_task()
            if cleanup_error is not None:
                self._finish_returned(generation, cleanup_error)
            raise _StartupAborted(cleanup_error)
        finally:
            if not stop.done():
                stop.cancel()
                try:
                    await stop
                except (Exception, asyncio.CancelledError):
                    pass
            if not startup.done():
                startup.cancel()
                try:
                    await startup
                except (Exception, asyncio.CancelledError):
                    pass

    async def _retain_until_lifespan_settles(
        self,
        generation: _ServeGeneration,
        lifespan: LifespanRunner,
        timeout: float | None,
        error: BaseException,
    ) -> None:
        """Publish the public error, then wait out a cancellation-resistant task."""
        self._finish_returned(generation, error)
        settled = await lifespan.discard_task(timeout)
        if not settled:
            await lifespan.wait_retained_task()

    async def _native_serve(
        self,
        generation: _ServeGeneration,
        listeners: Sequence[ListenerLease],
        *,
        lifespan_handoff: _LifespanHandoff | None,
        retire_trigger: Callable[[], None] | None,
        ready_trigger: Callable[[], None] | None,
        quiesce_fd: int | None,
        prepared_tls: _PreparedTls,
    ) -> None:
        """Drive one native serve lifecycle owned by `generation`."""
        from h2corn._lib import serve_fds

        internal_quiesce_write_fd: int | None = None
        if quiesce_fd is None and sys.platform != 'win32':
            quiesce_fd, internal_quiesce_write_fd = nonblocking_pipe()
        with self._state_lock:
            if self._generation is not generation:
                if internal_quiesce_write_fd is not None:
                    assert quiesce_fd is not None
                    os.close(quiesce_fd)
                    os.close(internal_quiesce_write_fd)
                raise RuntimeError('this Server already has an active serve() call')
            generation.quiesce_write_fd = internal_quiesce_write_fd

        def _mark_started() -> None:
            # Readiness is SERVING only after lifespan startup (already done
            # when this runs) and native acceptance.
            self._settle_started(_Readiness.SERVING, None)
            if ready_trigger is not None:
                ready_trigger()

        if generation.shutdown.done():
            # A request published before native construction still stops
            # acceptance immediately once the native side exists.
            self._request_shutdown(generation.shutdown.result())

        async def _await_shutdown() -> str:
            # Shield: cancel_task(shutdown_task) must not cancel the
            # generation future that _request_shutdown publishes into.
            kind = await asyncio.shield(generation.shutdown)
            return kind.value

        shutdown_task = asyncio.create_task(_await_shutdown())
        # The drain's budget is an instant, not a task: it starts when shutdown
        # is requested, and whether it has passed is a question for the clock.
        drain_expires_at: float | None = None
        try:
            # Native duplication is synchronous. Keep the leases alive while
            # it runs, then the caller may close them without affecting native.
            native_result = serve_fds(
                self.app,
                [listener.raw_handle() for listener in listeners],
                self.config,
                shutdown_task,
                retire_trigger,
                lifespan_handoff,
                _mark_started,
                quiesce_fd,
                prepared_tls=prepared_tls,
            )
            native_serve = asyncio.ensure_future(native_result)
            while True:
                shielded = asyncio.shield(native_serve)
                try:
                    if drain_expires_at is None:
                        if generation.shutdown.done():
                            if native_serve.done():
                                await shielded
                                break
                            # The native side spends up to
                            # `timeout_graceful_shutdown` letting requests
                            # finish and only then cancels them, so cleanup
                            # gets the same budget again. A zero budget still
                            # waits for an empty drain to finish; only a
                            # positive budget can settle the public boundary
                            # early while ownership continues.
                            budget = 2 * self.config.timeout_graceful_shutdown
                            if budget <= 0:
                                await shielded
                                break
                            drain_expires_at = generation.loop.time() + budget
                            continue
                        await asyncio.wait(
                            (shielded, shutdown_task),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    else:
                        remaining = drain_expires_at - generation.loop.time()
                        if remaining > 0:
                            try:
                                await asyncio.wait_for(shielded, remaining)
                            except TimeoutError:
                                pass
                        if native_serve.done():
                            await asyncio.shield(native_serve)
                            break
                        # Budget spent with the native server still releasing.
                        # Public callers may leave; this generation keeps
                        # ownership until drain and lifespan shutdown finish.
                        self._finish_returned(
                            generation,
                            RuntimeError(
                                'shutdown finished with requests still running: they '
                                'ignored cancellation for longer than '
                                f'{2 * self.config.timeout_graceful_shutdown:g}s '
                                '(twice timeout_graceful_shutdown). Lifespan '
                                'shutdown will run after those requests release'
                            ),
                        )
                        await asyncio.shield(native_serve)
                        return
                    if native_serve.done():
                        await shielded
                        break
                    continue
                except asyncio.CancelledError:
                    if native_serve.cancelled():
                        # Native cancellation is a real failure, not a graceful
                        # request to absorb into shutdown.
                        await native_serve
                    # The released task is not cancelled by the public caller;
                    # keep draining under the generation's ownership.
                    if not generation.shutdown.done():
                        self._request_shutdown(_ShutdownKind.STOP)
                    continue
        finally:
            try:
                await cancel_task(shutdown_task)
            finally:
                with self._state_lock:
                    if self._generation is generation:
                        quiesce_write_fd = generation.quiesce_write_fd
                        generation.quiesce_write_fd = None
                    else:
                        quiesce_write_fd = None
                if quiesce_write_fd is not None:
                    os.close(quiesce_write_fd)
                # A caller-supplied quiesce source is borrowed by serve_fds.
                # Its owner is serve_worker_fds (or the internal pipe branch
                # below), not this native lifecycle.
                if internal_quiesce_write_fd is not None:
                    assert quiesce_fd is not None
                    os.close(quiesce_fd)


class _StartupAborted(BaseException):
    """Internal: primary lifespan startup stopped because shutdown was requested."""

    def __init__(self, cleanup_error: BaseException | None = None) -> None:
        self.cleanup_error = cleanup_error


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
        from h2corn._supervisor import serve_with_supervisor

        with _process_umask(config), _pidfile(config) as pid_fd:
            serve_with_supervisor(app, config, pid_fd=pid_fd)
        return

    with asyncio.Runner(loop_factory=event_loop_factory(config.loop)) as runner:
        runner.run(Server(app, config).serve())


def _serve_import_target(import_settings: ImportSettings, config: Config) -> None:
    """Serve a `module:attr` target, importing it inside each worker.

    The import is deferred rather than done here and inherited through `fork`,
    so a replacement worker picks up the current code — which is what makes
    `SIGHUP` reload the application rather than restart workers around the
    module the parent imported at startup. It also stops workers sharing
    whatever the import opened.
    """
    if sys.platform != 'win32':
        from h2corn._supervisor import serve_with_supervisor

        with _process_umask(config), _pidfile(config) as pid_fd:
            serve_with_supervisor(import_settings, config, pid_fd=pid_fd)
        return

    serve(import_target(import_settings), config)


def load_env_file(path: Path) -> None:
    """Apply an env file to this process, without overriding what is set."""
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
        load_env_file(import_settings.env_file)

    import_dir = (
        os.getcwd()
        if import_settings.app_dir is None
        else os.fspath(import_settings.app_dir)
    )
    sys.path[:] = [entry for entry in sys.path if entry != import_dir]
    sys.path.insert(0, import_dir)

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise RuntimeError(
            f'could not import module {module_name!r} from target {target!r}'
        ) from exc
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
