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
from fnmatch import fnmatchcase
from pathlib import Path

from ._socket import _drain_fd, _signal_wakeup_pipe, _swap_signal_handlers

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from ._cli import ImportSettings
    from ._config import Config

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
_inotify_init1 = _inotify_add_watch = _inotify_rm_watch = None

if sys.platform == 'linux':
    _libc = ctypes.CDLL(None, use_errno=True)
    _inotify_init1 = _libc.inotify_init1
    _inotify_init1.argtypes = [ctypes.c_int]
    _inotify_init1.restype = ctypes.c_int
    _inotify_add_watch = _libc.inotify_add_watch
    _inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
    _inotify_add_watch.restype = ctypes.c_int
    _inotify_rm_watch = _libc.inotify_rm_watch
    _inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
    _inotify_rm_watch.restype = ctypes.c_int


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
        fd = _inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if fd < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        self._fd = fd
        self._watches: dict[int, Path] = {}
        self._sync_watches()

    def _sync_watches(self):
        active: dict[int, Path] = {}
        for path in _walk_watch_dirs(self._roots, self._exclude_patterns):
            wd = _inotify_add_watch(self._fd, os.fsencode(path), _INOTIFY_MASK)
            if wd < 0:
                error = ctypes.get_errno()
                if error in {errno.ENOENT, errno.ENOTDIR}:
                    continue
                raise OSError(error, os.strerror(error), os.fspath(path))
            active[wd] = path
        for stale_wd in self._watches.keys() - active.keys():
            _inotify_rm_watch(self._fd, stale_wd)
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
        self._select = select
        self._kqueue = select.kqueue()
        self._fds: list[int] = []
        self._rebuild()

    def _close_fds(self):
        for fd in self._fds:
            os.close(fd)
        self._fds.clear()

    def _rebuild(self):
        self._close_fds()
        changelist = []
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
):
    if sys.platform == 'linux':
        return _InotifyNotifier(watch_dirs, exclude_patterns)
    if sys.platform == 'darwin':
        return _KqueueNotifier(watch_dirs, include_patterns, exclude_patterns)
    raise NotImplementedError('reload is currently supported only on Linux and macOS')


def _child_argv(argv: Sequence[str] | None):
    child_args = []
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
    selector: selectors.BaseSelector,
    notifier,
    wakeup_fd: int,
):
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
                _drain_fd(wakeup_fd)
                return True, needs_rebuild
            needs_rebuild |= notifier.consume()
            deadline = time.monotonic() + _RELOAD_COALESCE_DELAY


def _spawn_reload_child(args: list[str], env: Mapping[str, str] | None):
    child_env = os.environ.copy() if env is None else dict(env)
    pycache_dir = Path(tempfile.mkdtemp(prefix='h2corn-reload-pyc-'))
    child_env['PYTHONPYCACHEPREFIX'] = os.fspath(pycache_dir)
    return (
        subprocess.Popen([sys.executable, '-m', 'h2corn', *args], env=child_env),
        pycache_dir,
    )


def _stop_reload_child(
    process: subprocess.Popen[bytes],
    graceful_timeout: float,
    pycache_dir: Path,
):
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=graceful_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    finally:
        shutil.rmtree(pycache_dir, ignore_errors=True)


def _serve_with_reload(
    import_settings: ImportSettings,
    config: Config,
    *,
    reload_dirs: tuple[Path, ...],
    reload_includes: tuple[str, ...],
    reload_excludes: tuple[str, ...],
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
):
    watch_dirs = _watch_dirs(import_settings, reload_dirs)
    child_args = _child_argv(argv)
    snapshot = _watch_file_snapshot(
        watch_dirs,
        reload_includes,
        reload_excludes,
    )
    child, pycache_dir = _spawn_reload_child(child_args, env)
    stopping = False

    def _handle_stop(*_):
        nonlocal stopping
        stopping = True

    _log_line('Reload enabled')

    notifier = _create_notifier(
        watch_dirs,
        reload_includes,
        reload_excludes,
    )
    selector = selectors.DefaultSelector()
    with (
        _signal_wakeup_pipe() as wakeup_fd,
        _swap_signal_handlers({
            signal.SIGINT: _handle_stop,
            signal.SIGTERM: _handle_stop,
        }),
    ):
        selector.register(wakeup_fd, selectors.EVENT_READ)
        selector.register(notifier.fileno(), selectors.EVENT_READ)
        try:
            while not stopping:
                for key, _ in selector.select():
                    fileobj = key.fileobj
                    if not isinstance(fileobj, int):
                        continue
                    if fileobj == wakeup_fd:
                        _drain_fd(wakeup_fd)
                        continue
                    needs_rebuild = notifier.consume()
                    stop_requested, pending_rebuild = _wait_for_reload_quiet_period(
                        selector,
                        notifier,
                        wakeup_fd,
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
                    _log_line(_reload_change_message(changed_paths))
                    _stop_reload_child(
                        child,
                        config.timeout_graceful_shutdown,
                        pycache_dir,
                    )
                    child, pycache_dir = _spawn_reload_child(child_args, env)
        finally:
            selector.close()
            notifier.close()
            _stop_reload_child(child, config.timeout_graceful_shutdown, pycache_dir)
