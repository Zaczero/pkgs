from __future__ import annotations

import asyncio
import multiprocessing
import os
import random
import selectors
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field, replace
from multiprocessing.process import BaseProcess

from ._cli import ImportSettings
from ._lifespan import cancel_task, serve_with_lifespan
from ._socket import (
    bound_addresses,
    bound_sockets,
    drain_fd,
    nonblocking_pipe,
    signal_wakeup_pipe,
    swap_signal_handlers,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Collection
    from typing import Any

    from ._config import Config
    from ._lib import LifespanHandoff
    from ._server import ProcessIdentity
    from ._types import Application

_WORKER_FAILURE_WINDOW = 5.0
_WORKER_FAILURE_BACKOFF_INITIAL = 0.1
_WORKER_FAILURE_BACKOFF_MAX = 1.0
_CONTROL_HEARTBEAT = b'H'
_CONTROL_RETIRE = b'R'
_CONTROL_READY = b'Y'
_QUIESCE_RESTART = b'R'
_QUIESCE_STOP = b'S'
_RESTART_SIGNAL = signal.SIGUSR1


@dataclass(slots=True)
class _ReloadCycle:
    replacement: int | None = None
    target: int | None = None


@dataclass(slots=True)
class _WorkerRetirements:
    graceful_timeout: float
    deadlines: dict[int, float] = field(default_factory=dict[int, float])

    def begin(self, sentinel: int) -> None:
        # A repeated reason to retire the same worker must not extend its hard
        # ownership deadline.
        self.deadlines.setdefault(
            sentinel,
            time.monotonic() + self.graceful_timeout,
        )

    def finish(self, sentinel: int) -> None:
        self.deadlines.pop(sentinel, None)

    def pop_oldest(self) -> int | None:
        if not self.deadlines:
            return None
        sentinel = next(iter(self.deadlines))
        del self.deadlines[sentinel]
        return sentinel

    def pop_expired(self, now: float) -> tuple[int, ...]:
        expired = tuple(
            sentinel for sentinel, deadline in self.deadlines.items() if deadline <= now
        )
        for sentinel in expired:
            del self.deadlines[sentinel]
        return expired

    def next_timeout(self, now: float) -> float | None:
        if not self.deadlines:
            return None
        return max(0.0, min(self.deadlines.values()) - now)


def _log_line(message: str):
    sys.stderr.write(f'{message}\n')
    sys.stderr.flush()


def _terminate_workers(
    workers: Collection[BaseProcess],
    *,
    graceful_timeout: float,
):
    deadline = time.monotonic() + graceful_timeout
    for worker in workers:
        if worker.is_alive():
            worker.terminate()
    for worker in workers:
        remaining = max(0.0, deadline - time.monotonic())
        worker.join(remaining)
    for worker in workers:
        if worker.is_alive():
            worker.kill()
            worker.join()


def _restart_worker(worker: BaseProcess):
    if not worker.is_alive() or worker.pid is None:
        return
    try:
        os.kill(worker.pid, _RESTART_SIGNAL)
    except OSError:
        worker.terminate()


def _send_worker_quiesce(fd: int, *, restart: bool) -> OSError | None:
    try:
        os.write(fd, _QUIESCE_RESTART if restart else _QUIESCE_STOP)
    except OSError as exc:
        return exc
    finally:
        # EOF is the fail-closed fallback understood by the native receiver,
        # and ownership is transferred at most once by the caller's pop().
        os.close(fd)
    return None


def _renew_worker_healthcheck(
    deadlines: dict[int, float],
    sentinel: int,
    timeout_seconds: float,
) -> None:
    if timeout_seconds > 0:
        deadlines[sentinel] = time.monotonic() + timeout_seconds


def _clone_config(config: Config, /, **overrides: Any) -> Config:
    return replace(config, host=None, port=None, **overrides)


def _install_parent_death_signal(expected_supervisor_pid: int) -> None:
    """Bind this worker's lifetime to the supervisor's (Linux only).

    Ask the kernel to `SIGKILL` the worker the moment its supervisor dies for
    any reason — graceful exit, crash, `SIGKILL`, or the OOM killer — so a
    hard-killed supervisor can never leave orphaned workers behind. Must run
    *after* any privilege drop: `setuid`/`setgid` clear `PDEATHSIG`.
    """
    if sys.platform != 'linux':
        return
    import ctypes

    pr_set_pdeathsig = 1
    libc = ctypes.CDLL(None, use_errno=True)
    installed = libc.prctl(pr_set_pdeathsig, signal.SIGKILL, 0, 0, 0) == 0
    # Close the fork→prctl race against the parent identity captured before
    # Process.start(). If the supervisor died before this child installed
    # PDEATHSIG, it has already been reparented and must exit explicitly. PID 1
    # remains valid when it was the expected supervisor from the outset.
    if os.getppid() != expected_supervisor_pid:
        os._exit(0)
    if not installed:
        error_number = ctypes.get_errno()
        raise OSError(error_number, 'failed to install parent-death signal')


def _worker_entry(
    app: Application | ImportSettings,
    config: Config,
    fds: tuple[int, ...],
    identity: ProcessIdentity,
    expected_supervisor_pid: int,
    inherited_supervisor_fds: tuple[int, ...] = (),
    control_write_fd: int | None = None,
    quiesce_read_fd: int | None = None,
):
    from ._server import (
        Server,
        drop_process_privileges,
        event_loop_factory,
        import_target,
    )

    # `fork` copies every supervisor-side control end into the new worker. None
    # is useful there, and retaining them prevents EOF while growing each
    # successive worker's descriptor table quadratically.
    for inherited_fd in inherited_supervisor_fds:
        try:
            os.close(inherited_fd)
        except OSError:
            pass

    drop_process_privileges(identity)
    _install_parent_death_signal(expected_supervisor_pid)
    loop_factory = event_loop_factory(config.loop)
    active_app = import_target(app) if isinstance(app, ImportSettings) else app
    server = Server(active_app, _clone_config(config, workers=1))
    ready = False

    def _send_control(message: bytes):
        if control_write_fd is None:
            return
        try:
            os.write(control_write_fd, message)
        except OSError:
            pass

    async def _heartbeat_loop(interval: float):
        while True:
            # Readiness is level-triggered once achieved. If an earlier write
            # hit EAGAIN behind queued heartbeats, a later tick republishes it.
            _send_control(_CONTROL_READY if ready else _CONTROL_HEARTBEAT)
            await asyncio.sleep(interval)

    def _mark_ready() -> None:
        nonlocal ready
        ready = True
        _send_control(_CONTROL_READY)

    async def _serve_app(
        app: Application,
        lifespan_handoff: LifespanHandoff | None,
    ) -> None:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, server.shutdown)
        loop.add_signal_handler(signal.SIGTERM, server.shutdown)
        if _RESTART_SIGNAL not in {signal.SIGINT, signal.SIGTERM}:
            loop.add_signal_handler(_RESTART_SIGNAL, server.restart)
        await server.serve_inherited_fds(
            app,
            list(fds),
            (lambda: _send_control(_CONTROL_RETIRE))
            if config.max_requests > 0
            else None,
            lifespan_handoff,
            ready_trigger=_mark_ready,
            quiesce_fd=quiesce_read_fd,
        )

    async def _run_worker():
        heartbeat_task = (
            asyncio.create_task(_heartbeat_loop(config.timeout_worker_healthcheck / 3))
            if config.timeout_worker_healthcheck > 0
            else None
        )
        try:
            await serve_with_lifespan(
                active_app,
                _serve_app,
                mode=config.lifespan,
                startup_timeout=config.timeout_lifespan_startup,
                shutdown_timeout=config.timeout_lifespan_shutdown,
            )
        finally:
            await cancel_task(heartbeat_task)
            if control_write_fd is not None:
                os.close(control_write_fd)

    with asyncio.Runner(loop_factory=loop_factory) as runner:
        runner.run(_run_worker())


@dataclass(slots=True)
class _Supervisor:
    """Single owner of the supervisor's mutable worker-lifecycle state.

    The invariants live here by name: `expected_exits` and `reload_scheduled`
    are always subsets of `workers`' sentinels, every spawned worker has
    exactly one entry in `worker_controls`/`worker_quiesce_writes` (fd
    ownership transfers at most once, by `pop`), and `reload_cycle` tracks at
    most one in-flight rolling replacement.
    """

    app: Application | ImportSettings
    config: Config
    fds: tuple[int, ...]
    identity: ProcessIdentity
    supervisor_pid: int = field(default_factory=os.getpid)
    selector: selectors.BaseSelector = field(default_factory=selectors.DefaultSelector)
    workers: dict[int, BaseProcess] = field(default_factory=dict[int, BaseProcess])
    worker_controls: dict[int, int] = field(default_factory=dict[int, int])
    worker_quiesce_writes: dict[int, int] = field(default_factory=dict[int, int])
    control_workers: dict[int, int] = field(default_factory=dict[int, int])
    heartbeat_deadlines: dict[int, float] = field(default_factory=dict[int, float])
    expected_exits: set[int] = field(default_factory=set[int])
    reload_scheduled: set[int] = field(default_factory=set[int])
    reload_queue: deque[int] = field(default_factory=deque[int])
    reload_cycle: _ReloadCycle = field(default_factory=_ReloadCycle)
    forced_retirement_reaps: set[int] = field(default_factory=set[int])
    failure_times: deque[float] = field(default_factory=deque[float])
    failure_backoff: float = _WORKER_FAILURE_BACKOFF_INITIAL
    respawn_at: float | None = None
    any_worker_became_healthy: bool = False
    stopping: bool = False
    reload_requested: bool = False
    fatal_error: RuntimeError | None = None
    target_workers: int = field(init=False)
    retirements: _WorkerRetirements = field(init=False)

    def __post_init__(self) -> None:
        self.target_workers = self.config.workers
        self.retirements = _WorkerRetirements(self.config.timeout_graceful_shutdown)

    def active_workers(self) -> int:
        return len(self.workers) - len(self.expected_exits)

    def active_worker_capacity(self) -> int:
        replacement = self.reload_cycle.replacement
        replacement_is_starting = (
            replacement is not None
            and replacement in self.workers
            and replacement not in self.expected_exits
        )
        return self.target_workers + int(replacement_is_starting)

    def can_spawn_worker(self) -> bool:
        # Keep one bounded retiring generation alongside one serving
        # generation. This replaces unhealthy capacity immediately without
        # allowing repeated replacement failures to grow the process set
        # without bound.
        return (
            self.active_workers() < self.target_workers
            and len(self.workers) < self.target_workers * 2
        )

    def scale_down_candidate(self) -> int | None:
        for sentinel in reversed(self.workers):
            if (
                sentinel not in self.expected_exits
                and sentinel != self.reload_cycle.replacement
            ):
                return sentinel
        return None

    def is_viable_reload_replacement(self, sentinel: int) -> bool:
        return (
            sentinel == self.reload_cycle.replacement
            and sentinel not in self.expected_exits
        )

    def spawn_worker(self) -> int:
        # Readiness is a core supervisor signal, not an optional healthcheck
        # feature: rolling replacement must never retire the serving worker
        # before its successor has completed lifespan and adopted listeners.
        control_read_fd, control_write_fd = nonblocking_pipe()
        try:
            quiesce_read_fd, quiesce_write_fd = nonblocking_pipe()
        except BaseException:
            os.close(control_read_fd)
            os.close(control_write_fd)
            raise
        worker_max_requests = self.config.max_requests
        if worker_max_requests > 0 and self.config.max_requests_jitter > 0:
            worker_max_requests += random.randint(0, self.config.max_requests_jitter)
        worker_config = _clone_config(self.config, max_requests=worker_max_requests)
        inherited_supervisor_fds = (
            *self.worker_controls.values(),
            *self.worker_quiesce_writes.values(),
            control_read_fd,
            quiesce_write_fd,
        )
        worker: BaseProcess | None = None
        try:
            worker = multiprocessing.get_context('fork').Process(
                target=_worker_entry,
                args=(
                    self.app,
                    worker_config,
                    self.fds,
                    self.identity,
                    self.supervisor_pid,
                    inherited_supervisor_fds,
                    control_write_fd,
                    quiesce_read_fd,
                ),
            )
            worker.start()
        except BaseException:
            os.close(control_read_fd)
            os.close(control_write_fd)
            os.close(quiesce_read_fd)
            os.close(quiesce_write_fd)
            if worker is not None:
                worker.close()
            raise
        assert worker.pid is not None
        os.close(control_write_fd)
        os.close(quiesce_read_fd)
        sentinel = worker.sentinel
        assert isinstance(sentinel, int)
        _log_line(f'Started worker [{worker.pid}]')
        self.workers[sentinel] = worker
        self.selector.register(sentinel, selectors.EVENT_READ)
        self.worker_controls[sentinel] = control_read_fd
        self.worker_quiesce_writes[sentinel] = quiesce_write_fd
        self.control_workers[control_read_fd] = sentinel
        self.selector.register(control_read_fd, selectors.EVENT_READ)
        _renew_worker_healthcheck(
            self.heartbeat_deadlines,
            sentinel,
            self.config.timeout_worker_healthcheck,
        )
        return sentinel

    def record_worker_failure(self) -> None:
        if self.stopping:
            return
        now = time.monotonic()
        self.failure_times.append(now)
        while (
            self.failure_times and now - self.failure_times[0] > _WORKER_FAILURE_WINDOW
        ):
            self.failure_times.popleft()
        self.respawn_at = now + self.failure_backoff
        self.failure_backoff = min(
            self.failure_backoff * 2,
            _WORKER_FAILURE_BACKOFF_MAX,
        )
        if (
            len(self.failure_times) >= max(3, self.target_workers * 3)
            and not self.any_worker_became_healthy
        ):
            self.fatal_error = RuntimeError('worker crash loop detected')
            self.stopping = True

    def retire_worker(self, worker: BaseProcess, *, expected: bool) -> None:
        sentinel = worker.sentinel
        self.expected_exits.discard(sentinel)
        self.retirements.finish(sentinel)
        self.forced_retirement_reaps.discard(sentinel)
        self.reload_scheduled.discard(sentinel)
        self.heartbeat_deadlines.pop(sentinel, None)
        try:
            self.selector.unregister(sentinel)
        except KeyError:
            pass
        control_fd = self.worker_controls.pop(sentinel, None)
        if control_fd is not None:
            self.control_workers.pop(control_fd, None)
            try:
                self.selector.unregister(control_fd)
            except KeyError:
                pass
            os.close(control_fd)
        quiesce_write_fd = self.worker_quiesce_writes.pop(sentinel, None)
        if quiesce_write_fd is not None:
            os.close(quiesce_write_fd)
        if expected:
            _log_line(f'Stopped worker [{worker.pid}]')
        else:
            _log_line(
                f'Worker [{worker.pid}] exited unexpectedly with code {worker.exitcode}'
            )
            self.record_worker_failure()
        worker.close()

    def schedule_worker_retire(self, sentinel: int) -> None:
        if sentinel in self.expected_exits or sentinel in self.reload_scheduled:
            return
        self.reload_scheduled.add(sentinel)
        self.reload_queue.append(sentinel)

    def next_reload_target(self) -> int | None:
        while self.reload_queue:
            sentinel = self.reload_queue[0]
            if sentinel in self.workers and sentinel not in self.expected_exits:
                return sentinel
            self.reload_queue.popleft()
            self.reload_scheduled.discard(sentinel)
        return None

    def request_reload_retire(self, sentinel: int) -> None:
        if self.reload_queue and self.reload_queue[0] == sentinel:
            self.reload_queue.popleft()
        else:
            try:
                self.reload_queue.remove(sentinel)
            except ValueError:
                pass
        self.reload_scheduled.discard(sentinel)
        self.begin_worker_retirement(sentinel, restart=True)

    def quiesce_worker(self, sentinel: int, *, restart: bool) -> None:
        quiesce_write_fd = self.worker_quiesce_writes.pop(sentinel, None)
        if quiesce_write_fd is None:
            return
        if exc := _send_worker_quiesce(quiesce_write_fd, restart=restart):
            worker = self.workers.get(sentinel)
            worker_pid = worker.pid if worker is not None else 'unknown'
            # Closing the write end is itself a fail-closed stop request:
            # native retirement treats EOF as ordinary stop.
            _log_line(
                f'Worker [{worker_pid}] quiesce signal failed ({exc}); closing channel'
            )

    def begin_worker_retirement(self, sentinel: int, *, restart: bool) -> bool:
        if sentinel in self.expected_exits:
            return False
        worker = self.workers.get(sentinel)
        if worker is None:
            return False
        self.expected_exits.add(sentinel)
        self.heartbeat_deadlines.pop(sentinel, None)
        self.retirements.begin(sentinel)
        self.quiesce_worker(sentinel, restart=restart)
        if restart:
            _restart_worker(worker)
        elif worker.is_alive():
            worker.terminate()
        return True

    def force_kill_retirement(self, sentinel: int, message: str) -> None:
        worker = self.workers.get(sentinel)
        if worker is None:
            return
        self.forced_retirement_reaps.add(sentinel)
        if worker.is_alive():
            _log_line(message)
            worker.kill()

    def kill_expired_retirements(self) -> None:
        for sentinel in self.retirements.pop_expired(time.monotonic()):
            worker = self.workers.get(sentinel)
            if worker is None:
                continue
            self.force_kill_retirement(
                sentinel,
                f'Worker [{worker.pid}] exceeded graceful shutdown timeout; killing',
            )

    def drain_control_messages(self, control_fd: int) -> None:
        sentinel = self.control_workers.get(control_fd)
        if sentinel is None:
            return
        while True:
            try:
                data = os.read(control_fd, 1024)
            except BlockingIOError:
                return
            if not data:
                return
            if _CONTROL_HEARTBEAT[0] in data or _CONTROL_READY[0] in data:
                _renew_worker_healthcheck(
                    self.heartbeat_deadlines,
                    sentinel,
                    self.config.timeout_worker_healthcheck,
                )
            if _CONTROL_READY[0] in data:
                self.any_worker_became_healthy = True
                if self.is_viable_reload_replacement(sentinel):
                    target = self.reload_cycle.target
                    self.reload_cycle.replacement = None
                    self.reload_cycle.target = None
                    if target is not None:
                        self.request_reload_retire(target)
            if _CONTROL_RETIRE[0] in data:
                self.schedule_worker_retire(sentinel)

    def check_worker_healthchecks(self) -> None:
        if self.config.timeout_worker_healthcheck <= 0:
            return
        now = time.monotonic()
        for sentinel, deadline in tuple(self.heartbeat_deadlines.items()):
            if deadline > now:
                continue
            worker = self.workers.get(sentinel)
            if worker is None:
                self.heartbeat_deadlines.pop(sentinel, None)
                continue
            _log_line(f'Worker [{worker.pid}] failed healthcheck and will be replaced')
            self.heartbeat_deadlines.pop(sentinel, None)
            if self.begin_worker_retirement(sentinel, restart=False):
                # A watchdog replacement is intentional teardown after an
                # actual worker failure. Count the failure here because
                # the later expected process exit must not count it twice.
                self.record_worker_failure()

    def request_scale_down(self) -> bool:
        sentinel = self.scale_down_candidate()
        if sentinel is None:
            return False
        return self.begin_worker_retirement(sentinel, restart=False)

    def reconcile(self) -> None:
        if self.reload_requested:
            self.reload_requested = False
            self.failure_times.clear()
            self.failure_backoff = _WORKER_FAILURE_BACKOFF_INITIAL
            self.respawn_at = None
            for sentinel in self.workers:
                self.schedule_worker_retire(sentinel)
        while self.active_workers() > self.active_worker_capacity():
            if not self.request_scale_down():
                break
        if self.stopping:
            return
        if self.respawn_at is not None and time.monotonic() < self.respawn_at:
            return
        self.respawn_at = None
        target = self.next_reload_target()
        if (
            target is not None
            and self.reload_cycle.replacement is None
            and not self.expected_exits
            and self.active_workers() <= self.target_workers
        ):
            self.reload_cycle.target = target
            self.reload_cycle.replacement = self.spawn_worker()
            return
        if (
            self.active_workers() < self.target_workers
            and len(self.workers) >= self.target_workers * 2
        ):
            # Capacity is exhausted by retiring workers. Evict exactly one
            # oldest retirement, then wait for its selector event before
            # admitting another replacement attempt. This bounds overlap
            # without allowing a wedged old generation to block recovery.
            if not self.forced_retirement_reaps:
                oldest = self.retirements.pop_oldest()
                if oldest is not None:
                    worker = self.workers.get(oldest)
                    if worker is not None:
                        self.force_kill_retirement(
                            oldest,
                            f'Worker [{worker.pid}] blocked replacement capacity; killing',
                        )
            return
        while self.can_spawn_worker():
            self.spawn_worker()

    def wait_timeout(self) -> float | None:
        if (
            self.reload_queue
            and self.reload_cycle.replacement is None
            and not self.expected_exits
            and self.active_workers() <= self.target_workers
        ):
            return 0.0
        timeout_seconds: list[float] = []
        if (
            not self.stopping
            and self.respawn_at is not None
            and self.active_workers() < self.target_workers
        ):
            timeout_seconds.append(max(0.0, self.respawn_at - time.monotonic()))
        if self.heartbeat_deadlines:
            timeout_seconds.append(
                max(0.0, min(self.heartbeat_deadlines.values()) - time.monotonic())
            )
        retirement_timeout = self.retirements.next_timeout(time.monotonic())
        if retirement_timeout is not None:
            timeout_seconds.append(retirement_timeout)
        return min(timeout_seconds, default=None)

    def handle_stop(self, *_: object) -> None:
        self.stopping = True

    def handle_reload(self, *_: object) -> None:
        self.reload_requested = True

    def handle_scale_up(self, *_: object) -> None:
        self.target_workers += 1

    def handle_scale_down(self, *_: object) -> None:
        if self.target_workers > 1:
            self.target_workers -= 1

    def run(self) -> None:
        with (
            signal_wakeup_pipe() as wakeup_fd,
            swap_signal_handlers({
                signal.SIGINT: self.handle_stop,
                signal.SIGTERM: self.handle_stop,
                signal.SIGHUP: self.handle_reload,
                signal.SIGTTIN: self.handle_scale_up,
                signal.SIGTTOU: self.handle_scale_down,
            }),
        ):
            self.selector.register(wakeup_fd, selectors.EVENT_READ)
            try:
                self.reconcile()
                while not self.stopping:
                    ready = self.selector.select(self.wait_timeout())
                    for key, _ in ready:
                        fileobj = key.fileobj
                        if not isinstance(fileobj, int):
                            continue
                        if fileobj == wakeup_fd:
                            drain_fd(wakeup_fd)
                            continue
                        if fileobj in self.control_workers:
                            self.drain_control_messages(fileobj)
                            continue
                        worker = self.workers.pop(fileobj, None)
                        if worker is None:
                            try:
                                self.selector.unregister(fileobj)
                            except KeyError:
                                pass
                            continue
                        if fileobj == self.reload_cycle.replacement:
                            self.reload_cycle.replacement = None
                            self.reload_cycle.target = None
                        worker.join()
                        self.retire_worker(
                            worker,
                            expected=fileobj in self.expected_exits
                            or fileobj in self.reload_scheduled,
                        )
                    self.check_worker_healthchecks()
                    self.kill_expired_retirements()
                    self.reconcile()
            finally:
                self.stopping = True
                _log_line('Shutting down supervisor')
                try:
                    self.selector.unregister(wakeup_fd)
                except KeyError:
                    pass
                for sentinel in tuple(self.workers):
                    try:
                        self.selector.unregister(sentinel)
                    except KeyError:
                        pass
                self.selector.close()

                for sentinel in tuple(self.workers):
                    self.quiesce_worker(sentinel, restart=False)
                _terminate_workers(
                    list(self.workers.values()),
                    graceful_timeout=self.config.timeout_graceful_shutdown,
                )

                for sentinel in list(self.workers):
                    worker = self.workers.pop(sentinel)
                    self.retire_worker(worker, expected=True)

                self.expected_exits.clear()


def serve_with_supervisor(app: Application | ImportSettings, config: Config) -> None:
    if sys.platform == 'win32':
        raise NotImplementedError('worker supervisor mode is not supported on Windows')

    from ._server import resolve_process_identity

    identity = resolve_process_identity(config)
    with bound_sockets(config, socket_owner=(identity.uid, identity.gid)) as socks:
        from ._lib import emit_banner

        # Banner shows the RESOLVED addresses (meaningful when binding port 0).
        emit_banner(replace(config, bind=bound_addresses(socks), host=None, port=None))
        supervisor = _Supervisor(
            app=app,
            config=config,
            fds=tuple(sock.fileno() for sock in socks),
            identity=identity,
        )
        supervisor.run()

        if supervisor.fatal_error is not None:
            raise supervisor.fatal_error
