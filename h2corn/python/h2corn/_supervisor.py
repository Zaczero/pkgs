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
class _WorkerRetirements:
    """Hard-stop deadlines advanced by the worker's lifecycle acknowledgement.

    The native server owns the first graceful request wait, then Python task
    cancellation/cleanup gets the same configured interval.  Only once native
    ownership has drained can primary lifespan shutdown start, and the worker
    tells the supervisor exactly when that happened.  Lifespan therefore gets
    its own configured deadline rather than sharing a wall-clock guess started
    before request cancellation even began.
    """

    graceful_timeout: float
    lifespan_timeout: float
    retirements: dict[int, _WorkerRetirement] = field(
        default_factory=dict[int, _WorkerRetirement]
    )

    def begin(self, sentinel: int) -> None:
        # A repeated reason to retire the same worker must not extend its hard
        # ownership deadline or rewind an already acknowledged lifecycle.
        self.retirements.setdefault(
            sentinel,
            _WorkerRetirement(
                'request cleanup',
                time.monotonic() + 2 * self.graceful_timeout,
            ),
        )

    def begin_lifespan_shutdown(self, sentinel: int) -> bool:
        """Advance a retiring worker only after native request ownership drains."""
        retirement = self.retirements.get(sentinel)
        if retirement is None or retirement.phase == 'lifespan shutdown':
            return False
        retirement.phase = 'lifespan shutdown'
        retirement.deadline = (
            None
            if self.lifespan_timeout <= 0
            else time.monotonic() + self.lifespan_timeout
        )
        return True

    def finish(self, sentinel: int) -> None:
        self.retirements.pop(sentinel, None)

    def pop_oldest(self) -> int | None:
        if not self.retirements:
            return None
        sentinel = next(iter(self.retirements))
        del self.retirements[sentinel]
        return sentinel

    def pop_expired(self, now: float) -> tuple[tuple[int, str], ...]:
        expired = tuple(
            (sentinel, retirement.phase)
            for sentinel, retirement in self.retirements.items()
            if retirement.deadline is not None and retirement.deadline <= now
        )
        for sentinel, _phase in expired:
            del self.retirements[sentinel]
        return expired

    def next_timeout(self, now: float) -> float | None:
        deadlines = [
            retirement.deadline
            for retirement in self.retirements.values()
            if retirement.deadline is not None
        ]
        if not deadlines:
            return None
        return max(0.0, min(deadlines) - now)


def _log_line(message: str):
    sys.stderr.write(f'{message}\n')
    sys.stderr.flush()


def _restart_worker(worker: BaseProcess):
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
    from typing import cast

    import h2corn._server as _server_mod

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
                # Package-private: supervisor is the sole restart signal owner.
                lambda: server._request_shutdown(  # pyright: ignore[reportPrivateUsage]
                    cast('Any', _server_mod)._ShutdownKind.RESTART
                ),
            )
        heartbeat_task = (
            asyncio.create_task(_heartbeat_loop(config.timeout_worker_healthcheck / 3))
            if config.timeout_worker_healthcheck > 0
            else None
        )
        try:
            # Package-private worker entry: Server owns the generation lifecycle.
            await server._serve_worker_fds(  # pyright: ignore[reportPrivateUsage]
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


def _worker_process_fds(worker: BaseProcess) -> tuple[int, ...]:
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
    prepared_tls: _PreparedTls
    pid_fd: int | None = None
    parent_liveness_fd: int | None = None
    supervisor_pid: int = field(default_factory=os.getpid)
    selector: selectors.BaseSelector = field(default_factory=_posix_worker_selector)
    # Tagged as the pair from signal_wakeup_pipe(); typed loosely so the
    # private dataclass need not be re-exported across the package.
    signal_wakeup: Any | None = None
    workers: dict[int, BaseProcess] = field(default_factory=dict[int, BaseProcess])
    worker_controls: dict[int, int] = field(default_factory=dict[int, int])
    worker_quiesce_writes: dict[int, int] = field(default_factory=dict[int, int])
    control_workers: dict[int, int] = field(default_factory=dict[int, int])
    heartbeat_deadlines: dict[int, float] = field(default_factory=dict[int, float])
    expected_exits: set[int] = field(default_factory=set[int])
    reload_scheduled: set[int] = field(default_factory=set[int])
    reload_queue: deque[int] = field(default_factory=deque[int])
    reload_cycle: _ReloadCycle | None = None
    forced_retirement_reaps: set[int] = field(default_factory=set[int])
    failure_times: deque[float] = field(default_factory=deque[float])
    failure_backoff: float = _WORKER_FAILURE_BACKOFF_INITIAL
    respawn_at: float | None = None
    ready_workers: set[int] = field(default_factory=set[int])
    stopping: bool = False
    reload_requested: bool = False
    fatal_error: str | None = None
    last_failure_exit_code: int | None = None
    target_workers: int = field(init=False)
    retirements: _WorkerRetirements = field(init=False)

    def __post_init__(self) -> None:
        self.target_workers = self.config.workers
        self.retirements = _WorkerRetirements(
            self.config.timeout_graceful_shutdown,
            self.config.timeout_lifespan_shutdown,
        )

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
                for fd in _worker_process_fds(worker)
            ),
            *self.worker_controls.values(),
            *self.worker_quiesce_writes.values(),
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
        return len(self.workers) - len(self.expected_exits)

    def active_worker_capacity(self) -> int:
        replacement = (
            None if self.reload_cycle is None else self.reload_cycle.replacement
        )
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
        replacement = (
            None if self.reload_cycle is None else self.reload_cycle.replacement
        )
        for sentinel in reversed(self.workers):
            if sentinel not in self.expected_exits and sentinel != replacement:
                return sentinel
        return None

    def is_viable_reload_replacement(self, sentinel: int) -> bool:
        return (
            self.reload_cycle is not None
            and sentinel == self.reload_cycle.replacement
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
        inherited_supervisor_fds = self._child_discard_fds(
            control_read_fd=control_read_fd,
            quiesce_write_fd=quiesce_write_fd,
        )
        worker: BaseProcess | None = None
        try:
            worker = multiprocessing.get_context('fork').Process(
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
        # Take ownership of the started process before anything that can
        # fail: a worker this supervisor does not know about is one it can
        # neither quiesce nor reap, and it goes on serving after the
        # supervisor gives up. Logging in particular is fallible — a stderr
        # sink can be closed or full — and used to run first.
        self.workers[sentinel] = worker
        self.worker_controls[sentinel] = control_read_fd
        self.worker_quiesce_writes[sentinel] = quiesce_write_fd
        self.control_workers[control_read_fd] = sentinel
        self.selector.register(sentinel, selectors.EVENT_READ)
        self.selector.register(control_read_fd, selectors.EVENT_READ)
        _log_line(f'Started worker [{worker.pid}]')
        _renew_worker_healthcheck(
            self.heartbeat_deadlines,
            sentinel,
            self.config.timeout_worker_healthcheck,
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
        if (
            len(self.failure_times) >= 3
            and not self.ready_workers
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

    def retire_worker(self, worker: BaseProcess) -> None:
        sentinel = worker.sentinel
        expected = sentinel in self.expected_exits or sentinel in self.reload_scheduled
        self.ready_workers.discard(sentinel)
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
            self.record_worker_failure(worker.exitcode)
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
        for sentinel, phase in self.retirements.pop_expired(time.monotonic()):
            worker = self.workers.get(sentinel)
            if worker is None:
                continue
            self.force_kill_retirement(
                sentinel,
                f'Worker [{worker.pid}] exceeded {phase} timeout; killing',
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
                self.ready_workers.add(sentinel)
                reload_cycle = self.reload_cycle
                if reload_cycle is not None and self.is_viable_reload_replacement(sentinel):
                    target = reload_cycle.target
                    self.reload_cycle = None
                    self.request_reload_retire(target)
            if _CONTROL_RETIRE[0] in data:
                self.schedule_worker_retire(sentinel)
            if _CONTROL_LIFESPAN[0] in data:
                self.retirements.begin_lifespan_shutdown(sentinel)

    def handle_worker_event(self, fileobj: int) -> None:
        """Consume a control or sentinel event for a worker we own."""
        if fileobj in self.control_workers:
            self.drain_control_messages(fileobj)
            return
        worker = self.workers.pop(fileobj, None)
        if worker is None:
            try:
                self.selector.unregister(fileobj)
            except KeyError:
                pass
            return
        if self.reload_cycle is not None and fileobj == self.reload_cycle.replacement:
            self.reload_cycle = None
        worker.join()
        self.retire_worker(worker)

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
                else:
                    self.handle_worker_event(fileobj)
            self.kill_expired_retirements()

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
            and self.reload_cycle is None
            and not self.expected_exits
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
            and self.reload_cycle is None
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
                        self.handle_worker_event(fileobj)
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

                self.expected_exits.clear()
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
            replace(config, bind=bound_addresses(sockets), host=None, port=None),
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
