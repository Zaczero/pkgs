from __future__ import annotations

import ctypes
import errno
import os
import selectors
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path
from typing import cast

from ._config import FdBindSpec, parse_bind_spec
from ._socket import (
    drain_fd,
    nonblocking_pipe,
    signal_wakeup_pipe,
    swap_signal_handlers,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Protocol, Self

    from ._cli import ImportSettings
    from ._config import Config

    class _Kqueue(Protocol):
        def fileno(self) -> int: ...
        def control(
            self,
            changelist: Sequence[object] | None,
            max_events: int,
            timeout: float | None,
        ) -> list[object]: ...
        def close(self) -> None: ...

    class _KqueueModule(Protocol):
        KQ_FILTER_VNODE: int
        KQ_EV_ADD: int
        KQ_EV_ENABLE: int
        KQ_EV_CLEAR: int
        KQ_NOTE_WRITE: int
        KQ_NOTE_EXTEND: int
        KQ_NOTE_ATTRIB: int
        KQ_NOTE_LINK: int
        KQ_NOTE_RENAME: int
        KQ_NOTE_DELETE: int

        def kqueue(self) -> _Kqueue: ...
        def kevent(self, ident: int, **kwargs: int) -> object: ...

    class _Notifier(Protocol):
        def fileno(self) -> int: ...
        def consume(self) -> bool: ...
        def rebuild(self) -> None: ...
        def close(self) -> None: ...

    class _QuietPeriodSelector(Protocol):
        """The subset of selector behavior the coalescing loop needs."""

        def select(self, timeout: float) -> list[tuple[selectors.SelectorKey, int]]: ...

    class _QuietPeriodNotifier(Protocol):
        """The one notifier action the coalescing loop can perform."""

        def consume(self) -> bool: ...

    class _ReloadProcess(Protocol):
        """A child process group leader; its stdio representation is irrelevant."""

        pid: int

        def poll(self) -> int | None: ...
        def terminate(self) -> None: ...
        def kill(self) -> None: ...
        def wait(self, timeout: float | None = None) -> int: ...


_RELOAD_IGNORE_DIRS = frozenset({
    '__pycache__',
    '__pypackages__',
    'build',
    'dist',
    'node_modules',
    'target',
    'venv',
})
_RELOAD_COALESCE_DELAY = 0.2
# Private: only the reload watcher sets this; the child supervisor pops it
# before any application import and watches the read end for EOF.
_RELOAD_LIVENESS_ENV = '_H2CORN_RELOAD_LIVENESS_FD'
_INOTIFY_EVENT = struct.Struct('iIII')
_INOTIFY_DIR_REBUILD_MASK = (
    0x0000_0040  # IN_MOVED_FROM
    | 0x0000_0080  # IN_MOVED_TO
    | 0x0000_0100  # IN_CREATE
    | 0x0000_0200  # IN_DELETE
)
_INOTIFY_SELF_REBUILD_MASK = (
    0x0000_0400  # IN_DELETE_SELF
    | 0x0000_0800  # IN_MOVE_SELF
    | 0x0000_8000  # IN_IGNORED
)
_INOTIFY_ISDIR = 0x4000_0000
_INOTIFY_MASK = (
    0x0000_0002  # IN_MODIFY
    | 0x0000_0004  # IN_ATTRIB
    | 0x0000_0008  # IN_CLOSE_WRITE
    | 0x0000_0040  # IN_MOVED_FROM
    | 0x0000_0080  # IN_MOVED_TO
    | 0x0000_0100  # IN_CREATE
    | 0x0000_0200  # IN_DELETE
    | 0x0000_0400  # IN_DELETE_SELF
    | 0x0000_0800  # IN_MOVE_SELF
)


def _inotify_bindings() -> tuple[
    Callable[[int], int],
    Callable[[int, bytes, int], int],
    Callable[[int, int], int],
]:
    """Bind ``inotify_init1``/``inotify_add_watch``/``inotify_rm_watch``.

    The one place ctypes meets the type checker: only `_InotifyNotifier` calls
    this, and `_create_notifier` constructs that class exclusively on Linux.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    init1 = libc.inotify_init1
    init1.argtypes = [ctypes.c_int]
    init1.restype = ctypes.c_int
    add_watch = libc.inotify_add_watch
    add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
    add_watch.restype = ctypes.c_int
    rm_watch = libc.inotify_rm_watch
    rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
    rm_watch.restype = ctypes.c_int
    return init1, add_watch, rm_watch


def _log_line(message: str):
    sys.stderr.write(f'{message}\n')
    sys.stderr.flush()


def _display_reload_path(path: Path):
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return os.fspath(path)


def _changed_paths(
    previous: Mapping[Path, int],
    current: Mapping[Path, int],
):
    return tuple(
        path
        for path in sorted(set(previous) | set(current))
        if previous.get(path) != current.get(path)
    )


def _reload_change_message(changed_paths: tuple[Path, ...]):
    path = _display_reload_path(changed_paths[0])
    if len(changed_paths) == 1:
        return f'Reload change detected: {path}; restarting'
    return (
        f'Reload changes detected: {path} (+{len(changed_paths) - 1} more); restarting'
    )


def _watch_dirs(
    import_settings: ImportSettings,
    reload_dirs: tuple[Path, ...],
):
    roots = reload_dirs or (
        Path.cwd() if import_settings.app_dir is None else import_settings.app_dir,
    )
    return tuple(dict.fromkeys(root.resolve() for root in roots))


def _matches_patterns(candidates: tuple[str, ...], patterns: tuple[str, ...]):
    return bool(patterns) and any(
        fnmatchcase(candidate, pattern)
        for pattern in patterns
        for candidate in candidates
    )


def _match_dir_pattern(path: Path, root: Path, patterns: tuple[str, ...]):
    relative = path.relative_to(root)
    return _matches_patterns(
        (path.name, relative.as_posix(), *relative.parts),
        patterns,
    )


def _is_excluded_dir(path: Path, root: Path, exclude_patterns: tuple[str, ...]):
    return path.name in _RELOAD_IGNORE_DIRS or _match_dir_pattern(
        path,
        root,
        exclude_patterns,
    )


def _should_watch_file(
    path: Path,
    root: Path,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
):
    relative = path.relative_to(root).as_posix()
    candidates = (path.name, relative)
    return _matches_patterns(candidates, include_patterns) and not _matches_patterns(
        candidates,
        exclude_patterns,
    )


def _prune_walk_dirs(
    dirnames: list[str],
    current_path: Path,
    root: Path,
    exclude_patterns: tuple[str, ...],
):
    dirnames[:] = [
        dirname
        for dirname in dirnames
        if not _is_excluded_dir(
            current_path / dirname,
            root,
            exclude_patterns,
        )
    ]


def _walk_watch_dirs(roots: tuple[Path, ...], exclude_patterns: tuple[str, ...]):
    for root in roots:
        if not root.is_dir():
            continue
        for current_root, dirnames, _ in os.walk(root):
            current_path = Path(current_root)
            _prune_walk_dirs(dirnames, current_path, root, exclude_patterns)
            yield current_path


def _walk_watch_files(
    roots: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
):
    for root in roots:
        if root.is_file():
            if _should_watch_file(
                root, root.parent, include_patterns, exclude_patterns
            ):
                yield root
            continue
        if not root.is_dir():
            continue
        for current_root, dirnames, filenames in os.walk(root):
            current_path = Path(current_root)
            _prune_walk_dirs(dirnames, current_path, root, exclude_patterns)
            for filename in filenames:
                path = current_path / filename
                if _should_watch_file(path, root, include_patterns, exclude_patterns):
                    yield path


def _watch_file_snapshot(
    watch_dirs: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
):
    snapshot: dict[Path, int] = {}
    for path in _walk_watch_files(watch_dirs, include_patterns, exclude_patterns):
        try:
            snapshot[path] = path.stat().st_mtime_ns
        except OSError:
            continue
    return snapshot


def _inotify_needs_rebuild(mask: int):
    return bool(
        mask & _INOTIFY_SELF_REBUILD_MASK
        or (mask & _INOTIFY_ISDIR and mask & _INOTIFY_DIR_REBUILD_MASK)
    )


class _InotifyNotifier:
    def __init__(self, roots: tuple[Path, ...], exclude_patterns: tuple[str, ...]):
        self._roots = roots
        self._exclude_patterns = exclude_patterns
        inotify_init1, self._inotify_add_watch, self._inotify_rm_watch = (
            _inotify_bindings()
        )
        fd = inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if fd < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        self._fd = fd
        self._watches: dict[int, Path] = {}
        try:
            self._sync_watches()
        except BaseException:
            os.close(self._fd)
            self._fd = -1
            raise

    def _sync_watches(self):
        active: dict[int, Path] = {}
        for path in _walk_watch_dirs(self._roots, self._exclude_patterns):
            wd = self._inotify_add_watch(
                self._fd,
                os.fsencode(path),
                _INOTIFY_MASK,
            )
            if wd < 0:
                error = ctypes.get_errno()
                if error in {errno.ENOENT, errno.ENOTDIR}:
                    continue
                raise OSError(error, os.strerror(error), os.fspath(path))
            active[wd] = path
        for stale_wd in self._watches.keys() - active.keys():
            self._inotify_rm_watch(self._fd, stale_wd)
        self._watches = active

    def fileno(self) -> int:
        return self._fd

    def consume(self):
        event_size = _INOTIFY_EVENT.size
        unpack_event = _INOTIFY_EVENT.unpack_from
        needs_rebuild = False
        while True:
            try:
                chunk = os.read(self._fd, 64 * 1024)
            except BlockingIOError:
                return needs_rebuild
            if not chunk:
                return needs_rebuild
            offset = 0
            chunk_len = len(chunk)
            while offset + event_size <= chunk_len:
                _, mask, _, name_len = unpack_event(chunk, offset)
                needs_rebuild |= _inotify_needs_rebuild(mask)
                offset += event_size + name_len

    def rebuild(self):
        self._sync_watches()

    def close(self):
        if self._fd != -1:
            os.close(self._fd)
            self._fd = -1
            self._watches.clear()


class _KqueueNotifier:
    def __init__(
        self,
        roots: tuple[Path, ...],
        include_patterns: tuple[str, ...],
        exclude_patterns: tuple[str, ...],
    ):
        import select

        self._roots = roots
        self._include_patterns = include_patterns
        self._exclude_patterns = exclude_patterns
        self._select = cast('_KqueueModule', select)
        self._kqueue: _Kqueue = self._select.kqueue()
        self._fds: list[int] = []
        self._rebuild()

    def _close_fds(self):
        for fd in self._fds:
            os.close(fd)
        self._fds.clear()

    def _rebuild(self):
        self._close_fds()
        changelist: list[object] = []
        for path in _walk_watch_dirs(self._roots, self._exclude_patterns):
            try:
                fd = os.open(path, os.O_EVTONLY)
            except OSError:
                continue
            self._fds.append(fd)
            changelist.append(
                self._select.kevent(
                    fd,
                    filter=self._select.KQ_FILTER_VNODE,
                    flags=(
                        self._select.KQ_EV_ADD
                        | self._select.KQ_EV_ENABLE
                        | self._select.KQ_EV_CLEAR
                    ),
                    fflags=(
                        self._select.KQ_NOTE_WRITE
                        | self._select.KQ_NOTE_EXTEND
                        | self._select.KQ_NOTE_ATTRIB
                        | self._select.KQ_NOTE_LINK
                        | self._select.KQ_NOTE_RENAME
                        | self._select.KQ_NOTE_DELETE
                    ),
                )
            )
        for path in _walk_watch_files(
            self._roots,
            self._include_patterns,
            self._exclude_patterns,
        ):
            try:
                fd = os.open(path, os.O_EVTONLY)
            except OSError:
                continue
            self._fds.append(fd)
            changelist.append(
                self._select.kevent(
                    fd,
                    filter=self._select.KQ_FILTER_VNODE,
                    flags=(
                        self._select.KQ_EV_ADD
                        | self._select.KQ_EV_ENABLE
                        | self._select.KQ_EV_CLEAR
                    ),
                    fflags=(
                        self._select.KQ_NOTE_WRITE
                        | self._select.KQ_NOTE_EXTEND
                        | self._select.KQ_NOTE_ATTRIB
                        | self._select.KQ_NOTE_LINK
                        | self._select.KQ_NOTE_RENAME
                        | self._select.KQ_NOTE_DELETE
                    ),
                )
            )
        if changelist:
            self._kqueue.control(changelist, 0, 0)

    def fileno(self) -> int:
        return self._kqueue.fileno()

    def consume(self):
        needs_rebuild = False
        while self._kqueue.control(None, 128, 0):
            needs_rebuild = True
        return needs_rebuild

    def rebuild(self):
        self._rebuild()

    def close(self):
        self._close_fds()
        self._kqueue.close()


def _create_notifier(
    watch_dirs: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
) -> _Notifier:
    if sys.platform == 'linux':
        return _InotifyNotifier(watch_dirs, exclude_patterns)
    if sys.platform == 'darwin':
        return _KqueueNotifier(watch_dirs, include_patterns, exclude_patterns)
    raise NotImplementedError('reload is currently supported only on Linux and macOS')


def _child_argv(argv: Sequence[str] | None) -> list[str]:
    child_args: list[str] = []
    args = iter(sys.argv[1:] if argv is None else argv)
    for arg in args:
        if arg == '--reload':
            continue
        if arg in {'--reload-dir', '--reload-include', '--reload-exclude'}:
            next(args, None)
            continue
        if arg.startswith(('--reload-dir=', '--reload-include=', '--reload-exclude=')):
            continue
        child_args.append(arg)
    return child_args


def _wait_for_reload_quiet_period(
    selector: _QuietPeriodSelector,
    notifier: _QuietPeriodNotifier,
    wakeup_fd: int,
) -> tuple[bool, bool]:
    needs_rebuild = False
    deadline = time.monotonic() + _RELOAD_COALESCE_DELAY
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, needs_rebuild
        ready = selector.select(remaining)
        if not ready:
            return False, needs_rebuild
        for key, _ in ready:
            fileobj = key.fileobj
            if not isinstance(fileobj, int):
                continue
            if fileobj == wakeup_fd:
                drain_fd(wakeup_fd)
                return True, needs_rebuild
            needs_rebuild |= notifier.consume()
            deadline = time.monotonic() + _RELOAD_COALESCE_DELAY


def take_reload_parent_liveness_fd() -> int | None:
    """Pop the watcher-liveness read fd from the environment, if present."""
    raw = os.environ.pop(_RELOAD_LIVENESS_ENV, None)
    if raw is None:
        return None
    return int(raw)


def _configured_fd_bind_fds(config: Config) -> tuple[int, ...]:
    """Listener fds named by `fd://` binds — re-passed on every reload spawn."""
    fds: list[int] = []
    for bind in config.bind:
        match parse_bind_spec(bind):
            case FdBindSpec(fd):
                fds.append(fd)
            case _:
                pass
    return tuple(fds)


@dataclass(slots=True)
class _ReloadChild:
    """One reload generation: process group, pycache dir, liveness write end."""

    process: _ReloadProcess
    pycache_dir: Path
    liveness_write_fd: int | None

    @classmethod
    def spawn(
        cls,
        args: list[str],
        env: Mapping[str, str] | None,
        *,
        pass_fds: tuple[int, ...],
    ) -> Self:
        # One rollback transaction: pycache dir → liveness pipe → env → child.
        # Any failure after a step undoes every earlier step.
        pycache_dir: Path | None = None
        liveness_read_fd: int | None = None
        liveness_write_fd: int | None = None
        process: _ReloadProcess | None = None
        try:
            pycache_dir = Path(tempfile.mkdtemp(prefix='h2corn-reload-pyc-'))
            liveness_read_fd, liveness_write_fd = nonblocking_pipe()
            child_env = os.environ.copy() if env is None else dict(env)
            child_env['PYTHONPYCACHEPREFIX'] = os.fspath(pycache_dir)
            child_env[_RELOAD_LIVENESS_ENV] = str(liveness_read_fd)
            # Its own process group: the child is a supervisor with workers of
            # its own, so signalling the child alone reached the middle of the
            # tree and left the leaves for something else to notice.
            command = [sys.executable, '-m', 'h2corn', *args]
            inherit = (liveness_read_fd, *pass_fds)
            if sys.platform == 'win32':
                process = subprocess.Popen(command, env=child_env, pass_fds=inherit)
            else:
                process = subprocess.Popen(
                    command,
                    env=child_env,
                    process_group=0,
                    pass_fds=inherit,
                )
            # Watcher keeps the write end; the child alone holds the read end.
            os.close(liveness_read_fd)
            liveness_read_fd = None
            return cls(process, pycache_dir, liveness_write_fd)
        except BaseException:
            if process is not None:
                try:
                    if process.poll() is None:
                        if sys.platform == 'win32':
                            process.kill()
                        else:
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                process.kill()
                        process.wait()
                except OSError:
                    pass
            if liveness_read_fd is not None:
                try:
                    os.close(liveness_read_fd)
                except OSError:
                    pass
            if liveness_write_fd is not None:
                try:
                    os.close(liveness_write_fd)
                except OSError:
                    pass
            if pycache_dir is not None:
                shutil.rmtree(pycache_dir, ignore_errors=True)
            raise

    def stop(self, graceful_timeout: float) -> None:
        def signal_tree(number: int) -> None:
            if sys.platform == 'win32':
                self.process.terminate() if number == signal.SIGTERM else self.process.kill()
                return
            try:
                os.killpg(self.process.pid, number)
            except ProcessLookupError:
                # The whole group is already gone, which is the goal.
                pass

        try:
            if sys.platform == 'win32':
                if self.process.poll() is None:
                    signal_tree(signal.SIGTERM)
                    try:
                        self.process.wait(timeout=graceful_timeout)
                    except subprocess.TimeoutExpired:
                        signal_tree(signal.SIGKILL)
                        self.process.wait()
            else:
                def group_exists() -> bool:
                    try:
                        os.killpg(self.process.pid, 0)
                    except ProcessLookupError:
                        return False
                    return True

                # Let the child supervisor orchestrate its normal shutdown.
                # Its process group remains the completion boundary below.
                self.process.terminate()
                deadline = time.monotonic() + max(0.0, graceful_timeout)
                while group_exists():
                    # Reap a leader that already exited without mistaking its
                    # descendants for completion of the process group.
                    self.process.poll()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        # The leader can exit on SIGTERM before descendants do;
                        # their shared process group is the ownership boundary.
                        signal_tree(signal.SIGKILL)
                        break
                    time.sleep(min(remaining, 0.01))
                while group_exists():
                    signal_tree(signal.SIGKILL)
                    time.sleep(0.001)
        finally:
            write_fd = self.liveness_write_fd
            self.liveness_write_fd = None
            if write_fd is not None:
                try:
                    os.close(write_fd)
                except OSError:
                    pass
            shutil.rmtree(self.pycache_dir, ignore_errors=True)


def serve_with_reload(
    import_settings: ImportSettings,
    config: Config,
    *,
    reload_dirs: tuple[Path, ...],
    reload_includes: tuple[str, ...],
    reload_excludes: tuple[str, ...],
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    watch_dirs = _watch_dirs(import_settings, reload_dirs)
    child_args = _child_argv(argv)
    pass_fds = _configured_fd_bind_fds(config)
    stopping = False

    def _handle_stop(*_):
        nonlocal stopping
        stopping = True

    # Notifier and selector exist before any spawn and always close — a
    # logging failure during teardown must not skip their cleanup, and a
    # spawn failure must not leave them behind.
    notifier = _create_notifier(
        watch_dirs,
        reload_includes,
        reload_excludes,
    )
    selector = selectors.DefaultSelector()
    child: _ReloadChild | None = None
    try:
        with (
            signal_wakeup_pipe() as wakeup,
            swap_signal_handlers({
                signal.SIGINT: _handle_stop,
                signal.SIGTERM: _handle_stop,
            }),
        ):
            selector.register(wakeup.read_fd, selectors.EVENT_READ)
            selector.register(notifier.fileno(), selectors.EVENT_READ)
            # Taken once the watches exist, so an edit made while they were being
            # registered still shows up as a difference.
            snapshot = _watch_file_snapshot(
                watch_dirs,
                reload_includes,
                reload_excludes,
            )
            child = _ReloadChild.spawn(child_args, env, pass_fds=pass_fds)
            try:
                _log_line('Reload enabled')
            except OSError:
                pass
            while not stopping:
                for key, _ in selector.select():
                    fileobj = key.fileobj
                    if not isinstance(fileobj, int):
                        continue
                    if fileobj == wakeup.read_fd:
                        drain_fd(wakeup.read_fd)
                        continue
                    needs_rebuild = notifier.consume()
                    stop_requested, pending_rebuild = _wait_for_reload_quiet_period(
                        selector,
                        notifier,
                        wakeup.read_fd,
                    )
                    needs_rebuild |= pending_rebuild
                    if stop_requested:
                        stopping = True
                        break
                    if needs_rebuild:
                        notifier.rebuild()
                    next_snapshot = _watch_file_snapshot(
                        watch_dirs,
                        reload_includes,
                        reload_excludes,
                    )
                    changed_paths = _changed_paths(snapshot, next_snapshot)
                    if not changed_paths:
                        continue
                    snapshot = next_snapshot
                    try:
                        _log_line(_reload_change_message(changed_paths))
                    except OSError:
                        pass
                    child.stop(config.timeout_graceful_shutdown)
                    child = _ReloadChild.spawn(child_args, env, pass_fds=pass_fds)
    finally:
        try:
            selector.close()
        except OSError:
            pass
        try:
            notifier.close()
        except OSError:
            pass
        if child is not None:
            try:
                child.stop(config.timeout_graceful_shutdown)
            except OSError:
                pass
