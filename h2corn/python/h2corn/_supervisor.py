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
from dataclasses import replace

from ._socket import (
    _bound_sockets,
    _drain_fd,
    _nonblocking_pipe,
    _signal_wakeup_pipe,
    _swap_signal_handlers,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Collection
    from multiprocessing.process import BaseProcess

    from ._config import Config
    from ._types import ASGIApp

_WORKER_FAILURE_WINDOW = 5.0
_WORKER_FAILURE_BACKOFF_INITIAL = 0.1
_WORKER_FAILURE_BACKOFF_MAX = 1.0
_CONTROL_HEARTBEAT = b'H'
_CONTROL_RETIRE = b'R'
_RESTART_SIGNAL = signal.SIGUSR1


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


def _clone_config(config: Config, /, **overrides):
    cloned = replace(config, host=None, port=None, **overrides)
    object.__setattr__(cloned, '_bind_fd_is_unix', config._bind_fd_is_unix)
    return cloned


def _worker_entry(app: ASGIApp, config: Config, fds: tuple[int, ...]):
    from ._server import Server

    server = Server(app, _clone_config(config, workers=1))
    control_write_fd = config._control_write_fd

    def _send_control(message: bytes):
        if control_write_fd is None:
            return
        try:
            os.write(control_write_fd, message)
        except OSError:
            pass

    async def _heartbeat_loop(interval: float):
        while True:
            _send_control(_CONTROL_HEARTBEAT)
            await asyncio.sleep(interval)

    async def _serve_app(app: ASGIApp):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, server.shutdown)
        loop.add_signal_handler(signal.SIGTERM, server.shutdown)
        if _RESTART_SIGNAL not in {signal.SIGINT, signal.SIGTERM}:
            loop.add_signal_handler(_RESTART_SIGNAL, server.restart)
        heartbeat_task = (
            asyncio.create_task(_heartbeat_loop(config.timeout_worker_healthcheck / 3))
            if config.timeout_worker_healthcheck > 0
            else None
        )
        try:
            await server._serve_fds(
                app,
                list(fds),
                (lambda: _send_control(_CONTROL_RETIRE))
                if config.max_requests > 0
                else None,
            )
        finally:
            from ._lifespan import _cancel_task

            await _cancel_task(heartbeat_task)
            if control_write_fd is not None:
                os.close(control_write_fd)

    from ._lifespan import _serve_with_lifespan

    asyncio.run(
        _serve_with_lifespan(
            server.app,
            _serve_app,
            startup_timeout=config.timeout_lifespan_startup,
            shutdown_timeout=config.timeout_lifespan_shutdown,
        )
    )


def _serve_supervisor(app: ASGIApp, config: Config):
    if sys.platform == 'win32':
        raise NotImplementedError('worker supervisor mode is not supported on Windows')

    with _bound_sockets(config) as socks:
        from ._lib import emit_banner

        emit_banner(config)
        fds = tuple(sock.fileno() for sock in socks)
        workers: dict[int, BaseProcess] = {}
        worker_controls: dict[int, int] = {}
        control_workers: dict[int, int] = {}
        heartbeat_deadlines: dict[int, float] = {}
        any_worker_became_healthy = False
        target_workers = config.workers
        stopping = False
        reload_requested = False
        shrink_requested = 0
        fatal_error: RuntimeError | None = None
        ctx = multiprocessing.get_context('fork')
        expected_exits: set[int] = set()
        failure_times: deque[float] = deque()
        failure_backoff = _WORKER_FAILURE_BACKOFF_INITIAL
        respawn_at: float | None = None
        reload_scheduled: set[int] = set()
        reload_queue: deque[int] = deque()
        selector = selectors.DefaultSelector()

        def _active_workers():
            return len(workers) - len(expected_exits)

        def _spawn_worker():
            control_read_fd = None
            if config.max_requests > 0 or config.timeout_worker_healthcheck > 0:
                control_read_fd, control_write_fd = _nonblocking_pipe()
            else:
                control_write_fd = None
            worker_max_requests = config.max_requests
            if worker_max_requests > 0 and config.max_requests_jitter > 0:
                worker_max_requests += random.randint(0, config.max_requests_jitter)
            worker_config = _clone_config(config, max_requests=worker_max_requests)
            if control_write_fd is not None:
                object.__setattr__(worker_config, '_control_write_fd', control_write_fd)
            worker = ctx.Process(target=_worker_entry, args=(app, worker_config, fds))
            worker.start()
            assert worker.pid is not None
            if control_write_fd is not None:
                os.close(control_write_fd)
            sentinel = worker.sentinel
            assert isinstance(sentinel, int)
            _log_line(f'Started worker [{worker.pid}]')
            workers[sentinel] = worker
            selector.register(sentinel, selectors.EVENT_READ)
            if control_read_fd is not None:
                worker_controls[sentinel] = control_read_fd
                control_workers[control_read_fd] = sentinel
                selector.register(control_read_fd, selectors.EVENT_READ)
                if config.timeout_worker_healthcheck > 0:
                    heartbeat_deadlines[sentinel] = (
                        time.monotonic() + config.timeout_worker_healthcheck
                    )

        def _record_worker_failure(exitcode: int | None):
            nonlocal fatal_error, stopping, failure_backoff, respawn_at
            if stopping or exitcode in {None, 0, -signal.SIGINT, -signal.SIGTERM}:
                return
            now = time.monotonic()
            failure_times.append(now)
            while failure_times and now - failure_times[0] > _WORKER_FAILURE_WINDOW:
                failure_times.popleft()
            respawn_at = now + failure_backoff
            failure_backoff = min(failure_backoff * 2, _WORKER_FAILURE_BACKOFF_MAX)
            if (
                len(failure_times) >= max(3, target_workers * 3)
                and not any_worker_became_healthy
            ):
                fatal_error = RuntimeError('worker crash loop detected')
                stopping = True

        def _retire_worker(worker: BaseProcess, *, expected: bool):
            sentinel = worker.sentinel
            expected_exits.discard(sentinel)
            reload_scheduled.discard(sentinel)
            heartbeat_deadlines.pop(sentinel, None)
            try:
                selector.unregister(sentinel)
            except KeyError:
                pass
            control_fd = worker_controls.pop(sentinel, None)
            if control_fd is not None:
                control_workers.pop(control_fd, None)
                try:
                    selector.unregister(control_fd)
                except KeyError:
                    pass
                os.close(control_fd)
            if expected:
                _log_line(f'Stopped worker [{worker.pid}]')
            else:
                _log_line(
                    f'Worker [{worker.pid}] exited unexpectedly with code {worker.exitcode}'
                )
                _record_worker_failure(worker.exitcode)
            worker.close()

        def _schedule_worker_retire(sentinel: int):
            if sentinel in expected_exits or sentinel in reload_scheduled:
                return
            reload_scheduled.add(sentinel)
            reload_queue.append(sentinel)

        def _drain_control_messages(control_fd: int):
            nonlocal any_worker_became_healthy
            sentinel = control_workers.get(control_fd)
            if sentinel is None:
                return
            while True:
                try:
                    data = os.read(control_fd, 1024)
                except BlockingIOError:
                    return
                if not data:
                    return
                if (
                    config.timeout_worker_healthcheck > 0
                    and _CONTROL_HEARTBEAT[0] in data
                ):
                    heartbeat_deadlines[sentinel] = (
                        time.monotonic() + config.timeout_worker_healthcheck
                    )
                    any_worker_became_healthy = True
                if _CONTROL_RETIRE[0] in data:
                    _schedule_worker_retire(sentinel)

        def _check_worker_healthchecks():
            if config.timeout_worker_healthcheck <= 0:
                return
            now = time.monotonic()
            for sentinel, deadline in tuple(heartbeat_deadlines.items()):
                if deadline > now:
                    continue
                worker = workers.get(sentinel)
                if worker is None:
                    heartbeat_deadlines.pop(sentinel, None)
                    continue
                _log_line(
                    f'Worker [{worker.pid}] failed healthcheck and will be replaced'
                )
                heartbeat_deadlines.pop(sentinel, None)
                expected_exits.add(sentinel)
                if worker.is_alive():
                    worker.terminate()

        def _request_scale_down():
            nonlocal shrink_requested
            for sentinel in reversed(workers):
                if sentinel in expected_exits:
                    continue
                worker = workers[sentinel]
                expected_exits.add(sentinel)
                if worker.is_alive():
                    worker.terminate()
                shrink_requested -= 1
                return
            shrink_requested = 0

        def _request_reload_retire():
            while reload_queue:
                sentinel = reload_queue.popleft()
                reload_scheduled.discard(sentinel)
                if sentinel in expected_exits:
                    continue
                worker = workers.get(sentinel)
                if worker is None:
                    continue
                expected_exits.add(sentinel)
                _restart_worker(worker)
                return True
            return False

        def _reconcile():
            nonlocal failure_backoff, reload_requested, shrink_requested, respawn_at
            if reload_requested:
                reload_requested = False
                failure_times.clear()
                failure_backoff = _WORKER_FAILURE_BACKOFF_INITIAL
                respawn_at = None
                for sentinel in workers:
                    _schedule_worker_retire(sentinel)
            while shrink_requested > 0:
                _request_scale_down()
                if shrink_requested > 0 and not workers:
                    break
            if stopping:
                return
            if respawn_at is not None and time.monotonic() < respawn_at:
                return
            respawn_at = None
            if reload_queue:
                if _active_workers() <= target_workers:
                    _spawn_worker()
                    return
                if not expected_exits and _request_reload_retire():
                    return
            while _active_workers() < target_workers:
                _spawn_worker()

        def _wait_timeout():
            if reload_queue and (
                _active_workers() <= target_workers or not expected_exits
            ):
                return 0.0
            timeout_seconds = []
            if (
                not stopping
                and respawn_at is not None
                and _active_workers() < target_workers
            ):
                timeout_seconds.append(max(0.0, respawn_at - time.monotonic()))
            if heartbeat_deadlines:
                timeout_seconds.append(
                    max(0.0, min(heartbeat_deadlines.values()) - time.monotonic())
                )
            return min(timeout_seconds, default=None)

        def _handle_stop(*_):
            nonlocal stopping
            stopping = True

        def _handle_reload(*_):
            nonlocal reload_requested
            reload_requested = True

        def _handle_scale_up(*_):
            nonlocal target_workers
            target_workers += 1

        def _handle_scale_down(*_):
            nonlocal target_workers, shrink_requested
            if target_workers > 1:
                target_workers -= 1
                shrink_requested += 1

        with (
            _signal_wakeup_pipe() as wakeup_fd,
            _swap_signal_handlers({
                signal.SIGINT: _handle_stop,
                signal.SIGTERM: _handle_stop,
                signal.SIGHUP: _handle_reload,
                signal.SIGTTIN: _handle_scale_up,
                signal.SIGTTOU: _handle_scale_down,
            }),
        ):
            selector.register(wakeup_fd, selectors.EVENT_READ)
            try:
                _reconcile()
                while not stopping:
                    ready = selector.select(_wait_timeout())
                    for key, _ in ready:
                        fileobj = key.fileobj
                        if not isinstance(fileobj, int):
                            continue
                        if fileobj == wakeup_fd:
                            _drain_fd(wakeup_fd)
                            continue
                        if fileobj in control_workers:
                            _drain_control_messages(fileobj)
                            continue
                        worker = workers.pop(fileobj, None)
                        if worker is None:
                            try:
                                selector.unregister(fileobj)
                            except KeyError:
                                pass
                            continue
                        worker.join()
                        _retire_worker(worker, expected=fileobj in expected_exits)
                    _check_worker_healthchecks()
                    _reconcile()
            finally:
                stopping = True
                _log_line('Shutting down supervisor')
                try:
                    selector.unregister(wakeup_fd)
                except KeyError:
                    pass
                for sentinel in tuple(workers):
                    try:
                        selector.unregister(sentinel)
                    except KeyError:
                        pass
                selector.close()

                _terminate_workers(
                    list(workers.values()),
                    graceful_timeout=config.timeout_graceful_shutdown,
                )

                for sentinel in list(workers):
                    worker = workers.pop(sentinel)
                    _retire_worker(worker, expected=True)

                expected_exits.clear()

        if fatal_error is not None:
            raise fatal_error
