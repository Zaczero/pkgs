from __future__ import annotations

import ctypes
import errno
import os
import selectors
import shutil
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
_RELOAD_STOP_POLL_INTERVAL = 0.05
_INOTIFY_EVENT = struct.Struct('iIII')
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
_libc = ctypes.CDLL(None, use_errno=True)
_inotify_init1 = _libc.inotify_init1
_inotify_init1.argtypes = [ctypes.c_int]
_inotify_init1.restype = ctypes.c_int
_inotify_add_watch = _libc.inotify_add_watch
_inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
_inotify_add_watch.restype = ctypes.c_int


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
    return f'Reload changes detected: {path} (+{len(changed_paths) - 1} more); restarting'


def _watch_dirs(
    import_settings: ImportSettings,
    reload_dirs: tuple[Path, ...],
):
    roots = reload_dirs or (
        Path.cwd() if import_settings.app_dir is None else import_settings.app_dir,
    )
    return tuple(dict.fromkeys(root.resolve() for root in roots))


def _match_file_pattern(path: Path, root: Path, patterns: tuple[str, ...]):
    if not patterns:
        return False
    relative = path.relative_to(root)
    candidates = [path.name, relative.as_posix()]
    return any(
        fnmatchcase(candidate, pattern)
        for pattern in patterns
        for candidate in candidates
        if candidate
    )


def _match_dir_pattern(path: Path, root: Path, patterns: tuple[str, ...]):
    if not patterns:
        return False
    relative = path.relative_to(root)
    candidates = [path.name, relative.as_posix(), *relative.parts]
    return any(
        fnmatchcase(candidate, pattern)
        for pattern in patterns
        for candidate in candidates
        if candidate
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
    return _match_file_pattern(
        path, root, include_patterns
    ) and not _match_file_pattern(
        path,
        root,
        exclude_patterns,
    )


def _walk_watch_dirs(roots: tuple[Path, ...], exclude_patterns: tuple[str, ...]):
    for root in roots:
        if not root.exists():
            continue
        if not root.is_dir():
            continue
        for current_root, dirnames, _ in os.walk(root):
            current_path = Path(current_root)
            dirnames[:] = [
                dirname
                for dirname in dirnames
                if not _is_excluded_dir(
                    current_path / dirname,
                    root,
                    exclude_patterns,
                )
            ]
            yield current_path


def _walk_watch_paths(
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
        if not root.exists():
            continue
        for current_root, dirnames, filenames in os.walk(root):
            current_path = Path(current_root)
            dirnames[:] = [
                dirname
                for dirname in dirnames
                if not _is_excluded_dir(
                    current_path / dirname,
                    root,
                    exclude_patterns,
                )
            ]
            yield current_path
            for filename in filenames:
                path = current_path / filename
                if _should_watch_file(path, root, include_patterns, exclude_patterns):
                    yield path


def _python_file_snapshot(
    watch_dirs: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
):
    snapshot: dict[Path, int] = {}
    for path in _walk_watch_paths(watch_dirs, include_patterns, exclude_patterns):
        if path.is_dir():
            continue
        try:
            snapshot[path] = path.stat().st_mtime_ns
        except OSError:
            continue
    return snapshot


class _InotifyNotifier:
    def __init__(
        self, roots: tuple[Path, ...], exclude_patterns: tuple[str, ...]
    ) -> None:
        self._roots = roots
        self._exclude_patterns = exclude_patterns
        self._fd = -1
        self._rebuild()

    def _rebuild(self):
        if self._fd != -1:
            os.close(self._fd)
        fd = _inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if fd < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        self._fd = fd
        for path in _walk_watch_dirs(self._roots, self._exclude_patterns):
            if _inotify_add_watch(fd, os.fsencode(path), _INOTIFY_MASK) < 0:
                error = ctypes.get_errno()
                if error not in {errno.ENOENT, errno.ENOTDIR}:
                    raise OSError(error, os.strerror(error), os.fspath(path))

    def fileno(self) -> int:
        return self._fd

    def consume(self):
        while True:
            try:
                chunk = os.read(self._fd, 64 * 1024)
            except BlockingIOError:
                break
            if not chunk:
                break
            offset = 0
            while offset + _INOTIFY_EVENT.size <= len(chunk):
                _, _, _, name_len = _INOTIFY_EVENT.unpack_from(chunk, offset)
                offset += _INOTIFY_EVENT.size + name_len
        self._rebuild()

    def close(self):
        if self._fd != -1:
            os.close(self._fd)
            self._fd = -1


class _KqueueNotifier:
    def __init__(
        self,
        roots: tuple[Path, ...],
        include_patterns: tuple[str, ...],
        exclude_patterns: tuple[str, ...],
    ) -> None:
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
        for path in _walk_watch_paths(
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
        while self._kqueue.control(None, 128, 0):
            pass
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
    return [arg for arg in (sys.argv[1:] if argv is None else argv) if arg != '--reload']


def _spawn_reload_child(args: list[str], env: Mapping[str, str] | None):
    child_env = os.environ.copy() if env is None else dict(env)
    pycache_dir = Path(tempfile.mkdtemp(prefix='h2corn-reload-pyc-'))
    child_env['PYTHONPYCACHEPREFIX'] = os.fspath(pycache_dir)
    return (
        subprocess.Popen(
            [sys.executable, '-m', 'h2corn._server', *args], env=child_env
        ),
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
            deadline = time.monotonic() + graceful_timeout
            while process.poll() is None and time.monotonic() < deadline:
                time.sleep(_RELOAD_STOP_POLL_INTERVAL)
            if process.poll() is None:
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
    import signal

    watch_dirs = _watch_dirs(import_settings, reload_dirs)
    child_args = _child_argv(argv)
    snapshot = _python_file_snapshot(
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
    with _signal_wakeup_pipe() as wakeup_fd, _swap_signal_handlers({
        signal.SIGINT: _handle_stop,
        signal.SIGTERM: _handle_stop,
    }):
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
                    notifier.consume()
                    next_snapshot = _python_file_snapshot(
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
