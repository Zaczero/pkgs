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
from typing import Any, cast

from ._cli import ImportSettings
from ._lifespan import cancel_task
from ._reload import take_reload_parent_liveness_fd
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
    from typing import Protocol

    class _WorkerProcess(Protocol):
        """What the supervisor needs of a worker process.

        Narrower than `BaseProcess` on purpose: it names the dependency
        exactly, and it lets a test double stand in structurally instead of
        subclassing a process implementation it would have to keep in step.
        """

        @property
        def pid(self) -> int | None: ...
        @property
        def exitcode(self) -> int | None: ...
        @property
        def sentinel(self) -> int: ...
        def start(self) -> None: ...
        def is_alive(self) -> bool: ...
        def join(self, timeout: float | None = None) -> None: ...
        def terminate(self) -> None: ...
        def kill(self) -> None: ...
        def close(self) -> None: ...

    from ._config import Config
    from ._lib import _PreparedTls
    from ._server import ProcessIdentity
    from ._types import Application

_WORKER_FAILURE_WINDOW = 5.0
_WORKER_FAILURE_BACKOFF_INITIAL = 0.1
_WORKER_FAILURE_BACKOFF_MAX = 1.0
_CONTROL_HEARTBEAT = b'H'
_CONTROL_RETIRE = b'R'
_CONTROL_READY = b'Y'
_CONTROL_LIFESPAN = b'L'
_QUIESCE_RESTART = b'R'
_QUIESCE_STOP = b'S'
_RESTART_SIGNAL = signal.SIGUSR1
# `epoll_wait` takes its timeout as a signed 32-bit count of milliseconds.
_MAX_SELECT_TIMEOUT = (2**31 - 1) / 1000


@dataclass(slots=True)
class _ReloadCycle:
    target: int
    replacement: int


@dataclass(slots=True)
class _WorkerRetirement:
    """One worker's current supervisor-owned hard-stop phase."""

    phase: str
    deadline: float | None


@dataclass(slots=True)
class _Worker:
    """Everything the supervisor owns for one child process.

    A worker exists in this map from the instant the child starts until every
    selector registration, owned pipe end, health deadline, and retirement
    phase has been released. No second registry can describe a different
    lifecycle for the same sentinel.
    """

    process: _WorkerProcess
    control_read_fd: int
    quiesce_write_fd: int | None
    ready: bool = False
    health_deadline: float | None = None
    expected_exit: bool = False
    reload_scheduled: bool = False
    retirement: _WorkerRetirement | None = None
    forced_retirement_reap: bool = False


def _log_line(message: str):
    sys.stderr.write(f'{message}\n')
    sys.stderr.flush()


def _restart_worker(worker: _WorkerProcess):
    if not worker.is_alive() or worker.pid is None:
        return
    try:
        os.kill(worker.pid, _RESTART_SIGNAL)
    except OSError:
        worker.terminate()


def _send_worker_quiesce(fd: int, *, restart: bool) -> OSError | None:
    """Ask a worker to quiesce; return only a genuinely unexpected failure.

    `EPIPE` means the worker already closed its read end — it is on its way out
    already, which is the state the signal was asking for. Reporting that as a
    failure sends an operator chasing a normal shutdown race.
    """
    try:
        os.write(fd, _QUIESCE_RESTART if restart else _QUIESCE_STOP)
    except BrokenPipeError:
        return None
    except OSError as exc:
        return exc
    finally:
        # EOF is the fail-closed fallback understood by the native receiver,
        # and ownership is transferred at most once by the caller's pop().
        os.close(fd)
    return None


def _clone_config(config: Config, /, **overrides: Any) -> Config:
    return replace(config, **overrides)


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
    *,
    config: Config,
    fds: tuple[int, ...],
    identity: ProcessIdentity,
    prepared_tls: _PreparedTls,
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

    # Clear the parent's signal-wakeup routing before closing its pipe ends:
    # a signal after a bare close would write into a recycled descriptor.
    try:
        signal.set_wakeup_fd(-1)
    except ValueError:
        # Documented to raise off the main thread; workers are forked from
        # the main thread, but a non-main entry must not leave the rest of
        # the discard set behind.
        pass

    # Explicit discard only: every prior worker sentinel, parent control and
    # quiesce end, the opposite ends of *this* worker's pipes, the signal
    # wakeup pair, the pidfile, and any reload-parent liveness read end.
    # Unrelated application descriptors are left alone.
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
    native_drain_complete = False

    def _send_control(message: bytes):
        if control_write_fd is None:
            return
        try:
            os.write(control_write_fd, message)
        except OSError:
            pass

    async def _heartbeat_loop(interval: float):
        while True:
            # Readiness and native-drain completion are level-triggered. If an
            # earlier write hit EAGAIN behind queued heartbeats, a later tick
            # republishes the current lifecycle phase rather than letting a
            # full pipe turn graceful cleanup into an arbitrary hard kill.
            if native_drain_complete:
                _send_control(_CONTROL_LIFESPAN)
            else:
                _send_control(_CONTROL_READY if ready else _CONTROL_HEARTBEAT)
            await asyncio.sleep(interval)

    def _mark_ready() -> None:
        nonlocal ready
        ready = True
        _send_control(_CONTROL_READY)

    def _mark_native_drain_complete() -> None:
        nonlocal native_drain_complete
        native_drain_complete = True
        _send_control(_CONTROL_LIFESPAN)

    async def _run_worker():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, server.shutdown)
        loop.add_signal_handler(signal.SIGTERM, server.shutdown)
        if _RESTART_SIGNAL not in {signal.SIGINT, signal.SIGTERM}:
            loop.add_signal_handler(
                _RESTART_SIGNAL,
                # Deliberately private on `Server`: an embedder driving its own
                # process has no supervisor to request a restart from. This is
                # the one caller, and it is the supervisor that owns the signal.
                server._request_restart,  # pyright: ignore[reportPrivateUsage]
            )
        heartbeat_task = (
            asyncio.create_task(_heartbeat_loop(config.timeout_worker_healthcheck / 3))
            if config.timeout_worker_healthcheck > 0
            else None
        )
        try:
            await server.serve_worker_fds(
                list(fds),
                retire_trigger=(
                    (lambda: _send_control(_CONTROL_RETIRE))
                    if config.max_requests > 0
                    else None
                ),
                ready_trigger=_mark_ready,
                drain_complete_trigger=_mark_native_drain_complete,
                quiesce_fd=quiesce_read_fd,
                prepared_tls=prepared_tls,
            )
        finally:
            await cancel_task(heartbeat_task)
            if control_write_fd is not None:
                os.close(control_write_fd)

    with asyncio.Runner(loop_factory=loop_factory) as runner:
        runner.run(_run_worker())


def _posix_worker_selector() -> selectors.BaseSelector:
    # poll(2) needs no persistent kernel object, so nothing extra is copied
    # into a forked worker. DefaultSelector on Linux is epoll, which would.
    return selectors.PollSelector()


def _worker_process_fds(worker: _WorkerProcess) -> tuple[int, ...]:
    """Parent-side process-management fds for one worker (sentinel pair).

    `Process.sentinel` is only the wait end. The fork Popen keeps a matching
    write end open in the parent; both must be discarded in later children or
    each successive worker inherits one more dead handle.
    """
    popen: object | None = getattr(worker, '_popen', None)
    finalizer: object | None = (
        getattr(popen, 'finalizer', None) if popen is not None else None
    )
    raw_args: object | None = (
        getattr(finalizer, '_args', None) if finalizer is not None else None
    )
    if isinstance(raw_args, tuple):
        return tuple(
            item for item in cast('tuple[object, ...]', raw_args) if type(item) is int
        )
    sentinel = worker.sentinel
    if type(sentinel) is int:
        return (sentinel,)
    return ()


@dataclass(slots=True)
class _Supervisor:
    """Single owner of the supervisor's mutable worker-lifecycle state.

    Each `workers[sentinel]` record owns the process, control/readiness state,
    deadline, retirement phase, and its one-shot quiesce writer. `reload_cycle`
    tracks at most one in-flight rolling replacement.
    """

    app: Application | ImportSettings
    config: Config
    fds: tuple[int, ...]
    identity: ProcessIdentity
    prepared_tls: _PreparedTls
    pid_fd: int | None = None
    parent_liveness_fd: int | None = None
    supervisor_pid: int = field(default_factory=os.getpid)
    selector: selectors.BaseSelector = field(default_factory=_posix_worker_selector)
    # Tagged as the pair from signal_wakeup_pipe(); typed loosely so the
    # private dataclass need not be re-exported across the package.
    signal_wakeup: Any | None = None
    workers: dict[int, _Worker] = field(default_factory=dict[int, _Worker])
    reload_queue: deque[int] = field(default_factory=deque[int])
    reload_cycle: _ReloadCycle | None = None
    failure_times: deque[float] = field(default_factory=deque[float])
    failure_backoff: float = _WORKER_FAILURE_BACKOFF_INITIAL
    respawn_at: float | None = None
    stopping: bool = False
    reload_requested: bool = False
    fatal_error: str | None = None
    last_failure_exit_code: int | None = None
    target_workers: int = field(init=False)

    def __post_init__(self) -> None:
        self.target_workers = self.config.workers

    def _child_discard_fds(
        self,
        *,
        control_read_fd: int,
        quiesce_write_fd: int,
    ) -> tuple[int, ...]:
        """Descriptors a freshly forked worker must close and not retain."""
        discard: list[int] = [
            *(
                fd
                for worker in self.workers.values()
                for fd in _worker_process_fds(worker.process)
            ),
            *(worker.control_read_fd for worker in self.workers.values()),
            *(
                worker.quiesce_write_fd
                for worker in self.workers.values()
                if worker.quiesce_write_fd is not None
            ),
            control_read_fd,
            quiesce_write_fd,
        ]
        if self.signal_wakeup is not None:
            discard.append(self.signal_wakeup.read_fd)
            discard.append(self.signal_wakeup.write_fd)
        if self.pid_fd is not None:
            discard.append(self.pid_fd)
        if self.parent_liveness_fd is not None:
            discard.append(self.parent_liveness_fd)
        return tuple(discard)

    def active_workers(self) -> int:
        return sum(not worker.expected_exit for worker in self.workers.values())

    def active_worker_capacity(self) -> int:
        replacement = (
            None if self.reload_cycle is None else self.reload_cycle.replacement
        )
        replacement_is_starting = (
            replacement is not None
            and replacement in self.workers
            and not self.workers[replacement].expected_exit
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
        replacement = (
            None if self.reload_cycle is None else self.reload_cycle.replacement
        )
        for sentinel in reversed(self.workers):
            worker = self.workers[sentinel]
            if not worker.expected_exit and sentinel != replacement:
                return sentinel
        return None

    def is_viable_reload_replacement(self, sentinel: int) -> bool:
        return (
            self.reload_cycle is not None
            and sentinel == self.reload_cycle.replacement
            and not self.workers[sentinel].expected_exit
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
        inherited_supervisor_fds = self._child_discard_fds(
            control_read_fd=control_read_fd,
            quiesce_write_fd=quiesce_write_fd,
        )
        process: _WorkerProcess | None = None
        try:
            process = multiprocessing.get_context('fork').Process(
                target=_worker_entry,
                args=(self.app,),
                kwargs={
                    'config': worker_config,
                    'fds': self.fds,
                    'identity': self.identity,
                    'prepared_tls': self.prepared_tls,
                    'expected_supervisor_pid': self.supervisor_pid,
                    'inherited_supervisor_fds': inherited_supervisor_fds,
                    'control_write_fd': control_write_fd,
                    'quiesce_read_fd': quiesce_read_fd,
                },
            )
            process.start()
        except BaseException:
            for fd in (
                control_read_fd,
                control_write_fd,
                quiesce_read_fd,
                quiesce_write_fd,
            ):
                try:
                    os.close(fd)
                except OSError:
                    pass
            if process is not None:
                if process.is_alive():
                    process.terminate()
                process.join()
                if process.is_alive():
                    process.kill()
                    process.join()
                process.close()
            raise
        assert process.pid is not None
        sentinel = process.sentinel
        assert isinstance(sentinel, int)
        worker = _Worker(process, control_read_fd, quiesce_write_fd)
        self.workers[sentinel] = worker
        parent_fds = {
            control_read_fd,
            control_write_fd,
            quiesce_read_fd,
            quiesce_write_fd,
        }
        try:
            os.close(control_write_fd)
            parent_fds.remove(control_write_fd)
            os.close(quiesce_read_fd)
            parent_fds.remove(quiesce_read_fd)
            self.selector.register(
                sentinel,
                selectors.EVENT_READ,
                ('worker-exit', sentinel),
            )
            self.selector.register(
                control_read_fd,
                selectors.EVENT_READ,
                ('worker-control', sentinel),
            )
            _log_line(f'Started worker [{process.pid}]')
        except BaseException:
            self.workers.pop(sentinel, None)
            for fd in (sentinel, control_read_fd):
                try:
                    self.selector.unregister(fd)
                except KeyError:
                    pass
            for fd in parent_fds:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if process.is_alive():
                process.terminate()
            process.join()
            if process.is_alive():
                process.kill()
                process.join()
            process.close()
            raise
        if self.config.timeout_worker_healthcheck > 0:
            worker.health_deadline = (
                time.monotonic() + self.config.timeout_worker_healthcheck
            )
        return sentinel

    def record_worker_failure(self, exit_code: int | None = None) -> None:
        if self.stopping:
            return
        if exit_code is not None:
            self.last_failure_exit_code = exit_code
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
        # Gate on whether anything is serving *now*, not on whether anything
        # ever did. A lifetime latch meant one healthy worker at any point in
        # the past disabled this for good, so a deployment that came up and
        # later broke — a bad config push, a dependency outage — respawned
        # forever in silence. A fleet with one flapping worker still has
        # healthy ones here and is left alone.
        if len(self.failure_times) >= 3 and not any(
            worker.ready for worker in self.workers.values()
        ):
            last_exit_code = (
                'unknown'
                if self.last_failure_exit_code is None
                else str(self.last_failure_exit_code)
            )
            self.fatal_error = (
                f'Stopped: {len(self.failure_times)} workers exited without ever becoming ready '
                f'(last exit code {last_exit_code}). The worker error is logged above.'
            )
            self.stopping = True

    def retire_worker(self, sentinel: int, worker: _Worker) -> None:
        expected = worker.expected_exit or worker.reload_scheduled
        try:
            self.selector.unregister(sentinel)
        except KeyError:
            pass
        try:
            self.selector.unregister(worker.control_read_fd)
        except KeyError:
            pass
        os.close(worker.control_read_fd)
        if worker.quiesce_write_fd is not None:
            os.close(worker.quiesce_write_fd)
            worker.quiesce_write_fd = None
        if expected:
            _log_line(f'Stopped worker [{worker.process.pid}]')
        else:
            _log_line(
                f'Worker [{worker.process.pid}] exited unexpectedly with code '
                f'{worker.process.exitcode}'
            )
            self.record_worker_failure(worker.process.exitcode)
        worker.process.close()

    def schedule_worker_retire(self, sentinel: int) -> None:
        worker = self.workers.get(sentinel)
        if worker is None or worker.expected_exit or worker.reload_scheduled:
            return
        worker.reload_scheduled = True
        self.reload_queue.append(sentinel)

    def next_reload_target(self) -> int | None:
        while self.reload_queue:
            sentinel = self.reload_queue[0]
            worker = self.workers.get(sentinel)
            if worker is not None and not worker.expected_exit:
                return sentinel
            self.reload_queue.popleft()
            if worker is not None:
                worker.reload_scheduled = False
        return None

    def request_reload_retire(self, sentinel: int) -> None:
        if self.reload_queue and self.reload_queue[0] == sentinel:
            self.reload_queue.popleft()
        else:
            try:
                self.reload_queue.remove(sentinel)
            except ValueError:
                pass
        worker = self.workers.get(sentinel)
        if worker is not None:
            worker.reload_scheduled = False
        self.begin_worker_retirement(sentinel, restart=True)

    def quiesce_worker(self, sentinel: int, *, restart: bool) -> None:
        worker = self.workers.get(sentinel)
        if worker is None:
            return
        quiesce_write_fd = worker.quiesce_write_fd
        worker.quiesce_write_fd = None
        if quiesce_write_fd is None:
            return
        if exc := _send_worker_quiesce(quiesce_write_fd, restart=restart):
            worker_pid = worker.process.pid
            # Closing the write end is itself a fail-closed stop request:
            # native retirement treats EOF as ordinary stop.
            _log_line(
                f'Worker [{worker_pid}] quiesce signal failed ({exc}); closing channel'
            )

    def begin_worker_retirement(self, sentinel: int, *, restart: bool) -> bool:
        worker = self.workers.get(sentinel)
        if worker is None or worker.expected_exit:
            return False
        worker.expected_exit = True
        worker.health_deadline = None
        if worker.retirement is None:
            worker.retirement = _WorkerRetirement(
                'request cleanup',
                time.monotonic() + 2 * self.config.timeout_graceful_shutdown,
            )
        self.quiesce_worker(sentinel, restart=restart)
        if restart:
            _restart_worker(worker.process)
        elif worker.process.is_alive():
            worker.process.terminate()
        return True

    def force_kill_retirement(self, sentinel: int, message: str) -> None:
        worker = self.workers.get(sentinel)
        if worker is None:
            return
        worker.forced_retirement_reap = True
        if worker.process.is_alive():
            _log_line(message)
            worker.process.kill()

    def kill_expired_retirements(self) -> None:
        now = time.monotonic()
        for sentinel, worker in tuple(self.workers.items()):
            retirement = worker.retirement
            if (
                retirement is None
                or retirement.deadline is None
                or retirement.deadline > now
            ):
                continue
            worker.retirement = None
            self.force_kill_retirement(
                sentinel,
                f'Worker [{worker.process.pid}] exceeded {retirement.phase} timeout; killing',
            )

    def drain_control_messages(self, sentinel: int) -> None:
        worker = self.workers.get(sentinel)
        if worker is None:
            return
        while True:
            try:
                data = os.read(worker.control_read_fd, 1024)
            except BlockingIOError:
                return
            if not data:
                return
            if (
                _CONTROL_HEARTBEAT[0] in data or _CONTROL_READY[0] in data
            ) and self.config.timeout_worker_healthcheck > 0:
                worker.health_deadline = (
                    time.monotonic() + self.config.timeout_worker_healthcheck
                )
            if _CONTROL_READY[0] in data:
                worker.ready = True
                reload_cycle = self.reload_cycle
                if reload_cycle is not None and self.is_viable_reload_replacement(
                    sentinel
                ):
                    target = reload_cycle.target
                    self.reload_cycle = None
                    self.request_reload_retire(target)
            if _CONTROL_RETIRE[0] in data:
                self.schedule_worker_retire(sentinel)
            if _CONTROL_LIFESPAN[0] in data:
                retirement = worker.retirement
                if retirement is not None and retirement.phase != 'lifespan shutdown':
                    retirement.phase = 'lifespan shutdown'
                    retirement.deadline = (
                        None
                        if self.config.timeout_lifespan_shutdown <= 0
                        else time.monotonic() + self.config.timeout_lifespan_shutdown
                    )

    def handle_worker_event(self, kind: str, sentinel: int) -> None:
        """Consume a control or sentinel event for a worker we own."""
        if kind == 'worker-control':
            self.drain_control_messages(sentinel)
            return
        worker = self.workers.pop(sentinel, None)
        if worker is None:
            try:
                self.selector.unregister(sentinel)
            except KeyError:
                pass
            return
        if self.reload_cycle is not None and sentinel == self.reload_cycle.replacement:
            self.reload_cycle = None
        worker.process.join()
        self.retire_worker(sentinel, worker)

    def wait_for_retired_workers(self, wakeup_fd: int) -> None:
        """Reap final retirements without giving their phase budgets to startup.

        This is the terminal SIGTERM/SIGINT path.  It still uses the same
        control pipe and acknowledgement as rolling retirements; closing the
        selector and joining each worker for one shared grace used to bypass
        that lifecycle entirely.
        """
        for sentinel in tuple(self.workers):
            self.begin_worker_retirement(sentinel, restart=False)
        while self.workers:
            for key, _ in self.selector.select(self.wait_timeout()):
                fileobj = key.fileobj
                if not isinstance(fileobj, int):
                    continue
                if fileobj == wakeup_fd:
                    drain_fd(wakeup_fd)
                elif key.data is not None:
                    kind, sentinel = cast('tuple[str, int]', key.data)
                    self.handle_worker_event(kind, sentinel)
            self.kill_expired_retirements()

    def check_worker_healthchecks(self) -> None:
        if self.config.timeout_worker_healthcheck <= 0:
            return
        now = time.monotonic()
        for sentinel, worker in tuple(self.workers.items()):
            deadline = worker.health_deadline
            if deadline is None or deadline > now:
                continue
            _log_line(
                f'Worker [{worker.process.pid}] failed healthcheck and will be replaced'
            )
            worker.health_deadline = None
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
            and self.reload_cycle is None
            and not any(worker.expected_exit for worker in self.workers.values())
            and self.active_workers() <= self.target_workers
        ):
            replacement = self.spawn_worker()
            self.reload_cycle = _ReloadCycle(target, replacement)
            return
        if (
            self.active_workers() < self.target_workers
            and len(self.workers) >= self.target_workers * 2
        ):
            # Capacity is exhausted by retiring workers. Evict exactly one
            # oldest retirement, then wait for its selector event before
            # admitting another replacement attempt. This bounds overlap
            # without allowing a wedged old generation to block recovery.
            oldest = next(
                (
                    (sentinel, worker)
                    for sentinel, worker in self.workers.items()
                    if worker.retirement is not None
                    and not worker.forced_retirement_reap
                ),
                None,
            )
            if oldest is not None:
                sentinel, worker = oldest
                worker.retirement = None
                self.force_kill_retirement(
                    sentinel,
                    f'Worker [{worker.process.pid}] blocked replacement capacity; killing',
                )
            return
        while self.can_spawn_worker():
            self.spawn_worker()

    def wait_timeout(self) -> float | None:
        if (
            self.reload_queue
            and self.reload_cycle is None
            and not any(worker.expected_exit for worker in self.workers.values())
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
        now = time.monotonic()
        deadlines = [
            deadline
            for worker in self.workers.values()
            for deadline in (
                worker.health_deadline,
                None if worker.retirement is None else worker.retirement.deadline,
            )
            if deadline is not None
        ]
        if deadlines:
            timeout_seconds.append(max(0.0, min(deadlines) - now))
        timeout = min(timeout_seconds, default=None)
        if timeout is None:
            return None
        # A deadline further out than the selector's wait can express is not an
        # error — waking early just re-runs this loop — but handing it over
        # raises `OverflowError` from inside the supervisor.
        return min(timeout, _MAX_SELECT_TIMEOUT)

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
            signal_wakeup_pipe() as wakeup,
            swap_signal_handlers({
                signal.SIGINT: self.handle_stop,
                signal.SIGTERM: self.handle_stop,
                signal.SIGHUP: self.handle_reload,
                signal.SIGTTIN: self.handle_scale_up,
                signal.SIGTTOU: self.handle_scale_down,
            }),
        ):
            self.signal_wakeup = wakeup
            self.selector.register(wakeup.read_fd, selectors.EVENT_READ)
            if self.parent_liveness_fd is not None:
                self.selector.register(self.parent_liveness_fd, selectors.EVENT_READ)
            try:
                self.reconcile()
                while not self.stopping:
                    ready = self.selector.select(self.wait_timeout())
                    for key, _ in ready:
                        fileobj = key.fileobj
                        if not isinstance(fileobj, int):
                            continue
                        if fileobj == wakeup.read_fd:
                            drain_fd(wakeup.read_fd)
                            continue
                        if (
                            self.parent_liveness_fd is not None
                            and fileobj == self.parent_liveness_fd
                        ):
                            # Watcher SIGKILL (or any exit) closes the write
                            # end; EOF here is terminal for the whole family.
                            try:
                                data = os.read(self.parent_liveness_fd, 1)
                            except BlockingIOError:
                                continue
                            if not data:
                                self.stopping = True
                            continue
                        kind, sentinel = cast('tuple[str, int]', key.data)
                        self.handle_worker_event(kind, sentinel)
                    self.check_worker_healthchecks()
                    self.kill_expired_retirements()
                    self.reconcile()
            finally:
                self.stopping = True
                try:
                    _log_line('Shutting down supervisor')
                except OSError:
                    pass
                self.wait_for_retired_workers(wakeup.read_fd)
                try:
                    self.selector.unregister(wakeup.read_fd)
                except KeyError:
                    pass
                if self.parent_liveness_fd is not None:
                    try:
                        self.selector.unregister(self.parent_liveness_fd)
                    except KeyError:
                        pass
                self.selector.close()

                self.signal_wakeup = None


def serve_with_supervisor(
    app: Application | ImportSettings,
    config: Config,
    *,
    pid_fd: int | None = None,
) -> None:
    if sys.platform == 'win32':
        raise NotImplementedError('worker supervisor mode is not supported on Windows')

    from ._server import load_env_file, load_tls_material, resolve_process_identity

    # Strip before any application import: workers inherit the cleaned env.
    parent_liveness_fd = take_reload_parent_liveness_fd()

    identity = resolve_process_identity(config)
    # An env file holds a deployment's secrets and is routinely readable only
    # by the starting user, so it is read here rather than in a worker that
    # has already dropped to an unprivileged identity. Applying it to this
    # process's environment is what every forked worker inherits, and
    # clearing the setting is what stops a worker reopening the file.
    if isinstance(app, ImportSettings) and app.env_file is not None:
        load_env_file(app.env_file)
        app = replace(app, env_file=None)
    # Read PEM and build the acceptor once, while the supervisor still holds
    # the starting user's privileges: every forked worker reuses the same
    # prepared state and none of them reopen a key they may no longer read.
    from ._lib import emit_banner, prepare_tls

    prepared_tls = prepare_tls(config, load_tls_material(config))
    with bound_sockets(config, socket_owner=(identity.uid, identity.gid)) as leases:
        sockets = tuple(sock for lease in leases if (sock := lease.socket) is not None)
        if len(sockets) != len(leases):
            raise RuntimeError('listener already transferred before serve')
        # Banner shows the RESOLVED addresses (meaningful when binding port 0).
        emit_banner(
            replace(config, bind=bound_addresses(sockets)),
            prepared_tls,
        )
        listener_fds = tuple(sock.fileno() for sock in sockets)
        supervisor = _Supervisor(
            app=app,
            config=config,
            fds=listener_fds,
            identity=identity,
            prepared_tls=prepared_tls,
            pid_fd=pid_fd,
            parent_liveness_fd=parent_liveness_fd,
        )
        try:
            supervisor.run()
        finally:
            if parent_liveness_fd is not None:
                try:
                    os.close(parent_liveness_fd)
                except OSError:
                    pass

        if supervisor.fatal_error is not None:
            _log_line(supervisor.fatal_error)
            raise SystemExit(1)
