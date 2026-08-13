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
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path
from typing import cast

from h2corn._config import FdBindSpec, parse_bind_spec
from h2corn._log import Event
from h2corn._socket import (
    drain_fd,
    nonblocking_pipe,
    signal_wakeup_pipe,
    swap_signal_handlers,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    from typing import Protocol, Self

    from h2corn._cli import ImportSettings
    from h2corn._config import Config

    class _KEvent(Protocol):
        """What this module reads off a `select.kevent`.

        `select.kqueue` exists only on the BSDs, so a checker configured for
        every platform cannot see the real type. Declaring the one attribute
        used here keeps the watcher typed without pretending to model it.
        """

        ident: int

    class _Kqueue(Protocol):
        def fileno(self) -> int: ...
        def control(
            self,
            changelist: Sequence[object] | None,
            max_events: int,
            timeout: float | None,
        ) -> list[_KEvent]: ...
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

    class _WatchSync(Protocol):
        """The two operations snapshot reconciliation drives."""

        def rescan(self, directories: set[Path]) -> None: ...
        def rebuild(self) -> None: ...

    class _Notifier(_WatchSync, Protocol):
        def fileno(self) -> int: ...
        def consume(self) -> _ReloadEvents: ...
        def close(self) -> None: ...

    class _QuietPeriodSelector(Protocol):
        """The subset of selector behavior the coalescing loop needs."""

        def select(self, timeout: float) -> list[tuple[selectors.SelectorKey, int]]: ...

    class _QuietPeriodNotifier(Protocol):
        """The one notifier action the coalescing loop can perform."""

        def consume(self) -> _ReloadEvents: ...

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
_INOTIFY_Q_OVERFLOW = 0x0000_4000
_INOTIFY_IGNORED = 0x0000_8000
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


@dataclass(slots=True)
class _ReloadEvents:
    """Filesystem facts preserved from one or more notifier reads.

    Paths are enough to restart for ordinary file changes. A directory change
    is deliberately different: only that subtree needs a snapshot refresh and
    watch synchronisation. Queue overflow loses provenance, so it is the one
    condition that rebuilds every watch and conservatively restarts.
    """

    paths: set[Path] = field(default_factory=set[Path])
    rescan_directories: set[Path] = field(default_factory=set[Path])
    full_resync: bool = False

    def merge(self, other: _ReloadEvents) -> None:
        self.paths.update(other.paths)
        self.rescan_directories.update(other.rescan_directories)
        self.full_resync |= other.full_resync


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


def _reload_overflow_message():
    return 'Reload event queue overflowed; resynchronizing and restarting'


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


def _walk_watch_dirs_from(
    root: Path,
    start: Path,
    exclude_patterns: tuple[str, ...],
) -> Iterator[Path]:
    if not start.is_dir() or (
        start != root and _is_excluded_dir(start, root, exclude_patterns)
    ):
        return
    for current_root, dirnames, _ in os.walk(start):
        current_path = Path(current_root)
        _prune_walk_dirs(dirnames, current_path, root, exclude_patterns)
        yield current_path


def _walk_watch_files(
    roots: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
) -> Iterator[Path]:
    for root in roots:
        yield from _walk_watch_files_from(
            root,
            root,
            include_patterns,
            exclude_patterns,
        )


def _walk_watch_files_from(
    root: Path,
    start: Path,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
) -> Iterator[Path]:
    if start.is_file():
        if _should_watch_file(start, root.parent, include_patterns, exclude_patterns):
            yield start
        return
    if not start.is_dir() or (
        start != root and _is_excluded_dir(start, root, exclude_patterns)
    ):
        return
    for current_root, dirnames, filenames in os.walk(start):
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


def _watch_root_for_path(path: Path, roots: tuple[Path, ...]) -> Path | None:
    for root in roots:
        if path == root or path.is_relative_to(root):
            return root
    return None


def _minimal_subtrees(paths: set[Path]) -> tuple[Path, ...]:
    """Drop nested paths: their ancestor's rescan already covers them."""
    selected: list[Path] = []
    for path in sorted(paths, key=lambda item: (len(item.parts), os.fspath(item))):
        if not any(path.is_relative_to(ancestor) for ancestor in selected):
            selected.append(path)
    return tuple(selected)


def _rescan_file_snapshot(
    snapshot: dict[Path, int],
    directories: set[Path],
    roots: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
) -> tuple[Path, ...]:
    """Refresh only changed subtrees and return their semantic delta."""
    changed: set[Path] = set()
    for directory in _minimal_subtrees(directories):
        root = _watch_root_for_path(directory, roots)
        if root is None:
            continue
        previous = {
            path: mtime
            for path, mtime in snapshot.items()
            if path == directory or path.is_relative_to(directory)
        }
        for path in previous:
            del snapshot[path]
        current: dict[Path, int] = {}
        for path in _walk_watch_files_from(
            root,
            directory,
            include_patterns,
            exclude_patterns,
        ):
            try:
                current[path] = path.stat().st_mtime_ns
            except OSError:
                continue
        snapshot.update(current)
        changed.update(_changed_paths(previous, current))
    return tuple(sorted(changed))


def _refresh_direct_paths(snapshot: dict[Path, int], paths: set[Path]) -> None:
    for path in paths:
        try:
            snapshot[path] = path.stat().st_mtime_ns
        except OSError:
            snapshot.pop(path, None)


def _apply_reload_events(
    notifier: _WatchSync,
    events: _ReloadEvents,
    snapshot: dict[Path, int],
    roots: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
) -> tuple[Path, ...]:
    """Synchronize watcher and snapshot state, walking only lost identity."""
    if events.full_resync:
        notifier.rebuild()
        next_snapshot = _watch_file_snapshot(
            roots,
            include_patterns,
            exclude_patterns,
        )
        changed_paths = _changed_paths(snapshot, next_snapshot)
        snapshot.clear()
        snapshot.update(next_snapshot)
        return changed_paths

    notifier.rescan(events.rescan_directories)
    rescanned_paths = _rescan_file_snapshot(
        snapshot,
        events.rescan_directories,
        roots,
        include_patterns,
        exclude_patterns,
    )
    _refresh_direct_paths(snapshot, events.paths)
    return tuple(sorted(set(rescanned_paths) | events.paths))


def _inotify_needs_rebuild(mask: int):
    return bool(
        mask & _INOTIFY_SELF_REBUILD_MASK
        or (mask & _INOTIFY_ISDIR and mask & _INOTIFY_DIR_REBUILD_MASK)
    )


class _InotifyNotifier:
    def __init__(
        self,
        roots: tuple[Path, ...],
        include_patterns: tuple[str, ...],
        exclude_patterns: tuple[str, ...],
    ):
        self._roots = roots
        self._include_patterns = include_patterns
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
        self._ignored_watches: set[int] = set()
        try:
            self._sync_watches()
        except BaseException:
            os.close(self._fd)
            self._fd = -1
            raise

    def _sync_watches(self):
        self._remove_watches(set(self._watches))
        for root in self._roots:
            self._add_subtree(root, root)

    def _remove_watches(self, watch_descriptors: set[int]) -> None:
        for wd in watch_descriptors:
            self._watches.pop(wd, None)
            self._ignored_watches.add(wd)
            try:
                self._inotify_rm_watch(self._fd, wd)
            except OSError:
                pass

    def _add_subtree(self, root: Path, directory: Path) -> None:
        for path in _walk_watch_dirs_from(root, directory, self._exclude_patterns):
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
            self._watches[wd] = path

    def fileno(self) -> int:
        return self._fd

    def consume(self) -> _ReloadEvents:
        event_size = _INOTIFY_EVENT.size
        unpack_event = _INOTIFY_EVENT.unpack_from
        events = _ReloadEvents()
        while True:
            try:
                chunk = os.read(self._fd, 64 * 1024)
            except BlockingIOError:
                return events
            if not chunk:
                return events
            offset = 0
            chunk_len = len(chunk)
            while offset + event_size <= chunk_len:
                wd, mask, _cookie, name_len = unpack_event(chunk, offset)
                name_start = offset + event_size
                raw_name = chunk[name_start : name_start + name_len].rstrip(b'\0')
                offset += event_size + name_len
                if mask & _INOTIFY_Q_OVERFLOW:
                    events.full_resync = True
                    continue
                directory = self._watches.get(wd)
                if directory is None:
                    if mask & _INOTIFY_IGNORED and wd in self._ignored_watches:
                        self._ignored_watches.discard(wd)
                        continue
                    # An unknown descriptor means a deleted/reused watch raced
                    # this read. Its identity is no longer trustworthy.
                    events.full_resync = True
                    continue
                path = directory / os.fsdecode(raw_name) if raw_name else directory
                if mask & _INOTIFY_ISDIR:
                    root = _watch_root_for_path(path, self._roots)
                    if (
                        _inotify_needs_rebuild(mask)
                        and root is not None
                        and not _is_excluded_dir(path, root, self._exclude_patterns)
                    ):
                        events.rescan_directories.add(
                            directory.parent
                            if mask & _INOTIFY_SELF_REBUILD_MASK
                            else path
                        )
                    if mask & _INOTIFY_IGNORED:
                        self._watches.pop(wd, None)
                    continue
                root = _watch_root_for_path(path, self._roots)
                if root is not None and _should_watch_file(
                    path,
                    root,
                    self._include_patterns,
                    self._exclude_patterns,
                ):
                    events.paths.add(path)
                if mask & _INOTIFY_SELF_REBUILD_MASK:
                    events.rescan_directories.add(directory.parent)
                    if mask & _INOTIFY_IGNORED:
                        self._watches.pop(wd, None)

    def rebuild(self):
        self._sync_watches()

    def rescan(self, directories: set[Path]) -> None:
        for directory in _minimal_subtrees(directories):
            root = _watch_root_for_path(directory, self._roots)
            if root is None:
                self.rebuild()
                return
            stale = {
                wd
                for wd, watched in self._watches.items()
                if watched == directory or watched.is_relative_to(directory)
            }
            self._remove_watches(stale)
            self._add_subtree(root, directory)

    def close(self):
        if self._fd != -1:
            os.close(self._fd)
            self._fd = -1
            self._watches.clear()
            self._ignored_watches.clear()


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
        self._paths: dict[int, Path] = {}
        self._directory_fds: set[int] = set()
        try:
            self._rebuild()
        except BaseException:
            # The kqueue and any watch descriptors opened so far are already
            # ours. A caller that never received the object cannot close them.
            self.close()
            raise

    def _close_fds(self):
        for fd in self._fds:
            os.close(fd)
        self._fds.clear()
        self._paths.clear()
        self._directory_fds.clear()

    def _remove_subtree(self, directory: Path) -> None:
        stale = [
            fd
            for fd, path in self._paths.items()
            if path == directory or path.is_relative_to(directory)
        ]
        for fd in stale:
            os.close(fd)
            self._fds.remove(fd)
            self._paths.pop(fd, None)
            self._directory_fds.discard(fd)

    def _add_watch(
        self, path: Path, *, directory: bool, changelist: list[object]
    ) -> None:
        try:
            fd = os.open(path, os.O_EVTONLY)
        except OSError:
            return
        self._fds.append(fd)
        self._paths[fd] = path
        if directory:
            self._directory_fds.add(fd)
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

    def _add_subtree(self, root: Path, directory: Path) -> None:
        changelist: list[object] = []
        for path in _walk_watch_dirs_from(root, directory, self._exclude_patterns):
            self._add_watch(path, directory=True, changelist=changelist)
        for path in _walk_watch_files_from(
            root,
            directory,
            self._include_patterns,
            self._exclude_patterns,
        ):
            self._add_watch(path, directory=False, changelist=changelist)
        if changelist:
            self._kqueue.control(changelist, 0, 0)

    def _rebuild(self):
        self._close_fds()
        for root in self._roots:
            self._add_subtree(root, root)

    def fileno(self) -> int:
        return self._kqueue.fileno()

    def consume(self) -> _ReloadEvents:
        events = _ReloadEvents()
        while received := self._kqueue.control(None, 128, 0):
            for event in received:
                fd = event.ident
                path = self._paths.get(fd)
                if path is None:
                    events.full_resync = True
                elif fd in self._directory_fds:
                    events.rescan_directories.add(path)
                else:
                    events.paths.add(path)
        return events

    def rebuild(self):
        self._rebuild()

    def rescan(self, directories: set[Path]) -> None:
        for directory in _minimal_subtrees(directories):
            root = _watch_root_for_path(directory, self._roots)
            if root is None:
                self.rebuild()
                return
            self._remove_subtree(directory)
            self._add_subtree(root, directory)

    def close(self):
        self._close_fds()
        self._kqueue.close()


def _create_notifier(
    watch_dirs: tuple[Path, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
) -> _Notifier:
    if sys.platform == 'linux':
        return _InotifyNotifier(watch_dirs, include_patterns, exclude_patterns)
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
) -> tuple[bool, _ReloadEvents]:
    events = _ReloadEvents()
    deadline = time.monotonic() + _RELOAD_COALESCE_DELAY
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, events
        ready = selector.select(remaining)
        if not ready:
            return False, events
        for key, _ in ready:
            fileobj = key.fileobj
            if not isinstance(fileobj, int):
                continue
            if fileobj == wakeup_fd:
                drain_fd(wakeup_fd)
                return True, events
            events.merge(notifier.consume())
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
            except PermissionError:
                # Darwin can reject a signal while an exited group leader has
                # not been reaped yet. The outer group-existence loop retries.
                self.process.poll()

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
                    except PermissionError:
                        return True
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
    try:
        selector = selectors.DefaultSelector()
    except BaseException:
        # The notifier owns descriptors from the moment it is constructed, so
        # acquiring the selector after it must not be able to strand them.
        notifier.close()
        raise
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
                Event.RELOAD_ENABLED.log('Reload enabled')
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
                    events = notifier.consume()
                    stop_requested, pending_events = _wait_for_reload_quiet_period(
                        selector,
                        notifier,
                        wakeup.read_fd,
                    )
                    events.merge(pending_events)
                    if stop_requested:
                        stopping = True
                        break
                    changed_paths = _apply_reload_events(
                        notifier,
                        events,
                        snapshot,
                        watch_dirs,
                        reload_includes,
                        reload_excludes,
                    )
                    if not changed_paths and not events.full_resync:
                        continue
                    try:
                        Event.RELOAD_TRIGGERED.log(
                            '{message}',
                            message=_reload_overflow_message()
                            if events.full_resync and not changed_paths
                            else _reload_change_message(changed_paths),
                            paths=[str(path) for path in changed_paths],
                            full_resync=events.full_resync,
                        )
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
