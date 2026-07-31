import errno
import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import cast

import h2corn._reload as reload_module
import pytest
from h2corn import Config
from h2corn._cli import ImportSettings
from h2corn._reload import (
    _INOTIFY_EVENT,
    _INOTIFY_ISDIR,
    _changed_paths,
    _child_argv,
    _InotifyNotifier,
    _reload_change_message,
    _watch_dirs,
    _watch_file_snapshot,
)

_linux_only = pytest.mark.skipif(
    sys.platform != 'linux', reason='inotify is Linux-only'
)


def test_reload_snapshot_respects_include_patterns(tmp_path: Path) -> None:
    python_file = tmp_path / 'app.py'
    other_file = tmp_path / 'messages.mo'
    python_file.write_text('value = 1\n')
    other_file.write_text('catalog')

    snapshot = _watch_file_snapshot((tmp_path,), ('*.py',), ())

    assert python_file in snapshot
    assert other_file not in snapshot


def test_reload_snapshot_ignores_hidden_files_by_default(tmp_path: Path) -> None:
    visible = tmp_path / 'app.py'
    hidden = tmp_path / '.app.py'
    visible.write_text('value = 1\n')
    hidden.write_text('value = 2\n')

    snapshot = _watch_file_snapshot(
        (tmp_path,), ('*.py',), ('.*', '.py[cod]', '.sw.*', '~*')
    )

    assert visible in snapshot
    assert hidden not in snapshot


def test_reload_snapshot_matches_relative_path_include_patterns(tmp_path: Path) -> None:
    locale_dir = tmp_path / 'locale' / 'en'
    locale_dir.mkdir(parents=True)
    catalog = locale_dir / 'messages.mo'
    catalog.write_text('catalog')

    snapshot = _watch_file_snapshot((tmp_path,), ('locale/**/*.mo',), ())

    assert catalog in snapshot


def test_reload_snapshot_respects_exclude_patterns(tmp_path: Path) -> None:
    included = tmp_path / 'app.py'
    excluded_dir = tmp_path / 'tests'
    excluded_dir.mkdir()
    excluded = excluded_dir / 'test_app.py'
    included.write_text('value = 1\n')
    excluded.write_text('value = 2\n')

    snapshot = _watch_file_snapshot((tmp_path,), ('*.py',), ('tests',))

    assert included in snapshot
    assert excluded not in snapshot


def test_reload_snapshot_ignores_dunder_pypackages_dir_by_default(
    tmp_path: Path,
) -> None:
    package_dir = tmp_path / '__pypackages__'
    package_dir.mkdir()
    generated = package_dir / 'generated.py'
    generated.write_text('value = 1\n')

    snapshot = _watch_file_snapshot(
        (tmp_path,), ('*.py',), ('.*', '.py[cod]', '.sw.*', '~*')
    )

    assert generated not in snapshot


def test_reload_overflow_rebuilds_every_watch_and_refreshes_the_full_snapshot(
    tmp_path: Path,
) -> None:
    """Queue overflow discards event identity, so only full resync is sound."""
    app = tmp_path / 'app.py'
    app.write_text('first')
    snapshot = _watch_file_snapshot((tmp_path,), ('*.py',), ())
    added = tmp_path / 'added.py'
    added.write_text('second')

    class Notifier:
        rebuilds = 0
        rescans = 0

        def rebuild(self) -> None:
            self.rebuilds += 1

        def rescan(self, directories: set[Path]) -> None:
            del directories
            self.rescans += 1

    notifier = Notifier()

    changed = reload_module._apply_reload_events(
        notifier,
        reload_module._ReloadEvents(full_resync=True),
        snapshot,
        (tmp_path,),
        ('*.py',),
        (),
    )

    assert changed == (added,)
    assert snapshot == _watch_file_snapshot((tmp_path,), ('*.py',), ())
    assert (notifier.rebuilds, notifier.rescans) == (1, 0)


def test_reload_direct_event_restarts_even_when_the_snapshot_timestamp_is_unchanged(
    tmp_path: Path,
) -> None:
    app = tmp_path / 'app.py'
    app.write_text('before')
    snapshot = _watch_file_snapshot((tmp_path,), ('*.py',), ())
    original_mtime = app.stat().st_mtime_ns
    app.write_text('after!')
    os.utime(app, ns=(original_mtime, original_mtime))

    class Notifier:
        def rebuild(self) -> None:
            raise AssertionError('direct file event must not rebuild watches')

        def rescan(self, directories: set[Path]) -> None:
            assert not directories

    changed = reload_module._apply_reload_events(
        Notifier(),
        reload_module._ReloadEvents(paths={app}),
        snapshot,
        (tmp_path,),
        ('*.py',),
        (),
    )

    assert changed == (app,)


def test_reload_dirs_override_default_watch_root(tmp_path: Path) -> None:
    app_dir = tmp_path / 'app'
    watched = tmp_path / 'watched'
    app_dir.mkdir()
    watched.mkdir()

    watch_dirs = _watch_dirs(
        ImportSettings(target='example:app', app_dir=app_dir),
        (watched,),
    )

    assert watch_dirs == (watched.resolve(),)


def test_child_argv_strips_reload_parent_flags() -> None:
    child_argv = _child_argv([
        '--reload',
        '--reload-dir',
        'src',
        '--reload-include=*.mo',
        '--reload-exclude',
        'tests',
        '--workers',
        '1',
        'example:app',
    ])

    assert child_argv == ['--workers', '1', 'example:app']


def test_reload_spawns_nothing_when_the_notifier_cannot_be_created(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A watcher that cannot start must not leave a server running.

    The child used to be spawned first, so a notifier failure — which is
    deterministic on platforms without one — orphaned both the serving child
    and its pycache directory.
    """
    spawned = []

    def _spawn(*args, **kwargs):
        spawned.append((args, kwargs))
        raise AssertionError('nothing may be spawned before the watcher exists')

    monkeypatch.setattr(reload_module._ReloadChild, 'spawn', _spawn)
    monkeypatch.setattr(
        reload_module,
        '_create_notifier',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError('no watcher here')),
    )

    with pytest.raises(OSError, match='no watcher here'):
        reload_module.serve_with_reload(
            ImportSettings(target='example:app'),
            Config(),
            reload_dirs=(tmp_path,),
            reload_includes=('*.py',),
            reload_excludes=(),
        )

    assert spawned == []


def test_spawn_reload_child_uses_package_module_entrypoint(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured = {}
    pycache_dir = tmp_path / 'pycache'

    class FakeProcess:
        pass

    def popen(args, env, **kwargs):
        captured['args'] = args
        captured['env'] = env
        captured['pass_fds'] = kwargs.get('pass_fds')
        return FakeProcess()

    monkeypatch.setattr(
        reload_module.tempfile,
        'mkdtemp',
        lambda **_kwargs: pycache_dir,
    )
    monkeypatch.setattr(reload_module.subprocess, 'Popen', popen)

    child = reload_module._ReloadChild.spawn(
        ['example:app', '--port', '8000'],
        {'H2CORN_TEST': '1'},
        pass_fds=(),
    )
    try:
        assert isinstance(child.process, FakeProcess)
        assert child.pycache_dir == pycache_dir
        assert child.liveness_write_fd is not None
        assert captured['args'] == [
            reload_module.sys.executable,
            '-m',
            'h2corn',
            'example:app',
            '--port',
            '8000',
        ]
        assert captured['env']['H2CORN_TEST'] == '1'
        assert captured['env']['PYTHONPYCACHEPREFIX'] == str(pycache_dir)
        assert reload_module._RELOAD_LIVENESS_ENV in captured['env']
        assert captured['pass_fds'] is not None
        assert (
            int(captured['env'][reload_module._RELOAD_LIVENESS_ENV])
            in captured['pass_fds']
        )
    finally:
        if child.liveness_write_fd is not None:
            os.close(child.liveness_write_fd)
            child.liveness_write_fd = None
        if pycache_dir.exists():
            pycache_dir.rmdir()


def test_changed_paths_detects_modified_added_and_removed_files(tmp_path: Path) -> None:
    modified = tmp_path / 'modified.py'
    removed = tmp_path / 'removed.py'
    added = tmp_path / 'added.py'

    previous = {
        modified: 1,
        removed: 2,
    }
    current = {
        modified: 3,
        added: 4,
    }

    assert _changed_paths(previous, current) == (added, modified, removed)


def test_reload_change_message_includes_changed_path(
    tmp_path: Path, monkeypatch
) -> None:
    changed = tmp_path / 'app.py'
    monkeypatch.chdir(tmp_path)

    message = _reload_change_message((changed,))

    assert message == 'Reload change detected: app.py; restarting'


def test_reload_change_message_summarizes_many_paths(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    changed = tuple(tmp_path / f'file{i}.py' for i in range(5))

    message = _reload_change_message(changed)

    assert message == 'Reload changes detected: file0.py (+4 more); restarting'


def test_reload_events_extend_quiet_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every event restarts coalescing; a stop wakeup returns immediately."""
    now = 0.0
    timeouts: list[float] = []

    class Notifier:
        def __init__(self) -> None:
            self.events = iter((
                reload_module._ReloadEvents(paths={Path('one.py')}),
                reload_module._ReloadEvents(rescan_directories={Path('package')}),
            ))

        def consume(self):
            return next(self.events)

    notifier = Notifier()
    notifier_key = selectors.SelectorKey(9, 9, selectors.EVENT_READ, None)

    class Selector:
        calls = 0

        def select(self, timeout: float):
            nonlocal now
            timeouts.append(timeout)
            match self.calls:
                case 0:
                    now = 0.19
                    result = [(notifier_key, selectors.EVENT_READ)]
                case 1:
                    now = 0.38
                    result = [(notifier_key, selectors.EVENT_READ)]
                case _:
                    now += timeout
                    result = []
            self.calls += 1
            return result

    monkeypatch.setattr(reload_module.time, 'monotonic', lambda: now)
    stop, events = reload_module._wait_for_reload_quiet_period(
        Selector(), notifier, wakeup_fd=7
    )
    assert stop is False
    assert events.paths == {Path('one.py')}
    assert events.rescan_directories == {Path('package')}
    assert timeouts == pytest.approx([0.2, 0.2, 0.2])

    drained: list[int] = []
    monkeypatch.setattr(reload_module, 'drain_fd', drained.append)
    wakeup_key = selectors.SelectorKey(7, 7, selectors.EVENT_READ, None)

    class WakeupSelector:
        def select(self, timeout: float):
            return [(wakeup_key, selectors.EVENT_READ)]

    stop, events = reload_module._wait_for_reload_quiet_period(
        WakeupSelector(), notifier, wakeup_fd=7
    )
    assert (stop, events.paths, events.rescan_directories, drained) == (
        True,
        set(),
        set(),
        [7],
    )


def test_darwin_kqueue_rebuilds_directory_watch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Rebuild replaces every vnode fd and includes newly discovered paths."""
    watched = tmp_path / 'watched'
    watched.mkdir()
    (watched / 'initial.py').write_text('initial')
    opened: list[Path] = []
    closed: list[int] = []
    registered: list[list[object]] = []
    next_fd = 100

    class Kqueue:
        def __init__(self) -> None:
            self.queued: list[list[object]] = []

        def fileno(self) -> int:
            return 42

        def control(self, changelist, _max_events, _timeout):
            if changelist is not None:
                registered.append(list(changelist))
                return []
            return self.queued.pop(0) if self.queued else []

        def close(self) -> None:
            pass

    class Select:
        KQ_FILTER_VNODE = -4
        KQ_EV_ADD = 1
        KQ_EV_ENABLE = 2
        KQ_EV_CLEAR = 4
        KQ_NOTE_WRITE = 8
        KQ_NOTE_EXTEND = 16
        KQ_NOTE_ATTRIB = 32
        KQ_NOTE_LINK = 64
        KQ_NOTE_RENAME = 128
        KQ_NOTE_DELETE = 256

        def kqueue(self):
            return Kqueue()

        def kevent(self, ident: int, **kwargs: int) -> object:
            return (ident, kwargs)

    def fake_open(path, _flags: int) -> int:
        nonlocal next_fd
        opened.append(Path(path))
        fd = next_fd
        next_fd += 1
        return fd

    monkeypatch.setitem(sys.modules, 'select', Select())
    monkeypatch.setattr(reload_module.os, 'open', fake_open)
    monkeypatch.setattr(reload_module.os, 'close', closed.append)
    monkeypatch.setattr(reload_module.os, 'O_EVTONLY', 0, raising=False)
    notifier = reload_module._KqueueNotifier((watched,), ('*.py',), ())
    first_fds = tuple(notifier._fds)

    class Event:
        def __init__(self, ident: int) -> None:
            self.ident = ident

    initial_file_fd = next(
        fd for fd, path in notifier._paths.items() if path == watched / 'initial.py'
    )
    initial_dir_fd = next(fd for fd, path in notifier._paths.items() if path == watched)
    installed = cast('Kqueue', notifier._kqueue)
    installed.queued.append([Event(initial_file_fd), Event(initial_dir_fd)])
    events = notifier.consume()
    assert events.paths == {watched / 'initial.py'}
    assert events.rescan_directories == {watched}

    (watched / 'new-directory').mkdir()
    (watched / 'new-directory' / 'new.py').write_text('new')
    notifier.rebuild()
    second_fds = tuple(notifier._fds)
    notifier.close()

    assert set(first_fds).issubset(closed)
    assert set(second_fds).issubset(closed)
    assert watched / 'new-directory' in opened
    assert watched / 'new-directory' / 'new.py' in opened
    assert len(registered) == 2


def _inotify_notifier(root: Path, *, exclude_patterns: tuple[str, ...] = ()):
    notifier = object.__new__(_InotifyNotifier)
    notifier._fd = 1
    notifier._roots = (root,)
    notifier._include_patterns = ('*.py',)
    notifier._exclude_patterns = exclude_patterns
    notifier._watches = {1: root}
    notifier._ignored_watches = set()
    return notifier


def _packed_inotify_event(wd: int, mask: int, cookie: int, name: str = '') -> bytes:
    encoded_name = name.encode() + (b'\0' if name else b'')
    return _INOTIFY_EVENT.pack(wd, mask, cookie, len(encoded_name)) + encoded_name


def test_inotify_preserves_packed_file_events_even_when_mtime_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first = tmp_path / 'first.py'
    second = tmp_path / 'second.py'
    ignored = tmp_path / 'notes.txt'
    first.write_text('before')
    second.write_text('before')
    ignored.write_text('before')
    unchanged_mtime = first.stat().st_mtime_ns
    first.write_text('after!')
    os.utime(first, ns=(unchanged_mtime, unchanged_mtime))
    notifier = _inotify_notifier(tmp_path)

    chunks = iter([
        b''.join((
            _packed_inotify_event(1, 0x0000_0008, 0, 'first.py'),
            _packed_inotify_event(1, 0x0000_0008, 0, 'second.py'),
            _packed_inotify_event(1, 0x0000_0008, 0, 'notes.txt'),
        )),
        BlockingIOError(),
    ])

    def fake_read(_fd: int, _size: int):
        chunk = next(chunks)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    monkeypatch.setattr(reload_module.os, 'read', fake_read)

    events = notifier.consume()
    assert events.paths == {first, second}
    assert events.rescan_directories == set()
    assert events.full_resync is False


@_linux_only
def test_inotify_rebuild_preserves_fileno(tmp_path: Path) -> None:
    notifier = _InotifyNotifier((tmp_path,), ('*.py',), ())
    try:
        original_fd = notifier.fileno()
        (tmp_path / 'sub').mkdir()
        notifier.rebuild()
        assert notifier.fileno() == original_fd
    finally:
        notifier.close()


@_linux_only
def test_inotify_rebuild_keeps_events_on_originally_registered_fd(
    tmp_path: Path,
) -> None:
    notifier = _InotifyNotifier((tmp_path,), ('*.py',), ())
    try:
        registered_fd = notifier.fileno()
        sel = selectors.DefaultSelector()
        try:
            sel.register(registered_fd, selectors.EVENT_READ)
            (tmp_path / 'sub').mkdir()
            assert sel.select(timeout=1.0), 'parent watch must deliver subdir create'
            notifier.consume()
            notifier.rebuild()
            (tmp_path / 'sub' / 'newfile.txt').write_text('hello')
            assert sel.select(timeout=1.0), (
                'rebuild() must preserve the fd seen by the selector'
            )
        finally:
            sel.close()
    finally:
        notifier.close()


def test_inotify_rename_cookie_and_directory_events_rescan_only_the_subtree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    nested = tmp_path / 'nested'
    nested.mkdir()
    old = tmp_path / 'old.py'
    old.write_text('old')
    renamed = tmp_path / 'renamed.py'
    old.rename(renamed)
    created = nested / 'created.py'
    created.write_text('created')
    notifier = _inotify_notifier(tmp_path)

    chunks = iter([
        b''.join((
            _packed_inotify_event(1, 0x0000_0040, 73, 'old.py'),
            _packed_inotify_event(1, 0x0000_0080, 73, 'renamed.py'),
            _packed_inotify_event(
                1,
                _INOTIFY_ISDIR | 0x0000_0100,
                0,
                'nested',
            ),
        )),
        BlockingIOError(),
    ])

    def fake_read(_fd: int, _size: int):
        chunk = next(chunks)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    monkeypatch.setattr(reload_module.os, 'read', fake_read)

    events = notifier.consume()
    assert events.paths == {tmp_path / 'old.py', renamed}
    assert events.rescan_directories == {nested}
    snapshot: dict[Path, int] = {}
    assert reload_module._rescan_file_snapshot(
        snapshot,
        events.rescan_directories,
        (tmp_path,),
        ('*.py',),
        (),
    ) == (created,)


def test_inotify_ignores_excluded_topology_and_forces_full_resync_on_overflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    notifier = _inotify_notifier(tmp_path, exclude_patterns=('generated',))
    chunks = iter([
        b''.join((
            _packed_inotify_event(
                1,
                _INOTIFY_ISDIR | 0x0000_0100,
                0,
                'generated',
            ),
            _packed_inotify_event(-1, 0x0000_4000, 0),
        )),
        BlockingIOError(),
    ])

    def fake_read(_fd: int, _size: int):
        chunk = next(chunks)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    monkeypatch.setattr(reload_module.os, 'read', fake_read)

    events = notifier.consume()
    assert events.paths == set()
    assert events.rescan_directories == set()
    assert events.full_resync is True


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX process groups')
def test_reload_child_stop_waits_for_and_kills_the_whole_process_group(
    tmp_path: Path,
) -> None:
    """A dead group leader must not let a SIGTERM-resistant leaf escape."""
    from h2corn import _reload

    process = subprocess.Popen(
        [
            sys.executable,
            '-c',
            (
                'import signal, subprocess, sys, time; '
                'child = subprocess.Popen(['
                'sys.executable, "-c", '
                '"import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
                'time.sleep(60)"'
                ']); '
                'print(child.pid, flush=True); '
                'time.sleep(60)'
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
        process_group=0,
    )
    assert process.stdout is not None
    descendant_pid = int(process.stdout.readline())
    pycache_dir = tmp_path / 'pycache'
    pycache_dir.mkdir()
    child = _reload._ReloadChild(process, pycache_dir, None)
    try:
        assert os.getpgid(child.process.pid) == child.process.pid
        child.stop(0.05)
        assert child.process.poll() is not None
        deadline = time.monotonic() + 1.0
        while True:
            try:
                os.kill(descendant_pid, 0)
            except ProcessLookupError:
                break
            assert time.monotonic() < deadline, 'SIGKILL must reach the descendant'
            time.sleep(0.01)
    finally:
        try:
            os.killpg(child.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if child.process.poll() is None:
            child.process.wait()
        if process.stdout is not None:
            process.stdout.close()

    assert not child.pycache_dir.exists()


def test_reload_child_spawn_failure_rolls_back_resources(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A failed Popen leaves no tempdir, liveness pipe ends, or child behind."""
    pycache_dir = tmp_path / 'pycache'
    monkeypatch.setattr(
        reload_module.tempfile,
        'mkdtemp',
        lambda **_kwargs: str(pycache_dir),
    )

    def boom(*_args, **_kwargs):
        raise OSError('spawn refused')

    monkeypatch.setattr(reload_module.subprocess, 'Popen', boom)

    before = set(os.listdir('/proc/self/fd'))
    with pytest.raises(OSError, match='spawn refused'):
        reload_module._ReloadChild.spawn(['example:app'], None, pass_fds=())
    after = set(os.listdir('/proc/self/fd'))

    assert not pycache_dir.exists()
    assert before == after


def test_reload_child_passes_configured_fd_binds(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """fd:// binds are re-passed on every spawn so reload keeps the listener."""
    captured: dict[str, object] = {}
    pycache_dir = tmp_path / 'pycache'

    class FakeProcess:
        pass

    def popen(_args, env, **kwargs):
        captured['pass_fds'] = kwargs.get('pass_fds')
        captured['env'] = env
        return FakeProcess()

    monkeypatch.setattr(
        reload_module.tempfile,
        'mkdtemp',
        lambda **_kwargs: str(pycache_dir),
    )
    monkeypatch.setattr(reload_module.subprocess, 'Popen', popen)

    child = reload_module._ReloadChild.spawn(
        ['example:app'],
        None,
        pass_fds=(7, 9),
    )
    try:
        pass_fds = captured['pass_fds']
        env = captured['env']
        assert isinstance(pass_fds, tuple)
        assert isinstance(env, dict)
        # pass_fds always includes the liveness read end first.
        assert pass_fds[1:] == (7, 9)
        assert int(env[reload_module._RELOAD_LIVENESS_ENV]) == pass_fds[0]
    finally:
        if child.liveness_write_fd is not None:
            os.close(child.liveness_write_fd)
            child.liveness_write_fd = None
        if pycache_dir.exists():
            pycache_dir.rmdir()


def test_configured_fd_bind_fds_extracts_fd_specs() -> None:
    assert reload_module._configured_fd_bind_fds(
        Config(bind=('127.0.0.1:0', 'fd://3', 'unix:/tmp/x.sock', 'fd://5'))
    ) == (3, 5)


@_linux_only
def test_inotify_init_failure_after_fd_open_closes_fd(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """If initial watches fail, the inotify fd is closed rather than leaked."""

    def fail_sync(self):
        raise OSError(errno.EACCES, 'permission denied')

    monkeypatch.setattr(
        reload_module._InotifyNotifier,
        '_sync_watches',
        fail_sync,
    )
    before = set(os.listdir('/proc/self/fd'))
    with pytest.raises(OSError, match='permission denied'):
        _InotifyNotifier((tmp_path,), ('*.py',), ())
    after = set(os.listdir('/proc/self/fd'))
    assert before == after


def _proc_children(pid: int) -> list[int]:
    try:
        raw = Path(f'/proc/{pid}/task/{pid}/children').read_text()
    except OSError:
        return []
    return [int(item) for item in raw.split()]


def _wait_http(port: int, body: bytes, timeout: float = 8.0) -> bytes:
    import socket
    import time

    deadline = time.monotonic() + timeout
    last: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5) as sock:
                sock.sendall(
                    b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'
                )
                chunks: list[bytes] = []
                while chunk := sock.recv(65536):
                    chunks.append(chunk)
                response = b''.join(chunks)
                if b'200 OK' in response and body in response:
                    return response
        except OSError as exc:
            last = exc
        time.sleep(0.05)
    raise RuntimeError(f'server did not become ready: {last}')


@_linux_only
def test_reload_with_fd_bind_serves_across_generations(
    tmp_path: Path,
) -> None:
    """`--reload --bind fd://N` keeps the listener across at least two generations."""
    import signal
    import socket
    import subprocess
    import time

    app = tmp_path / 'app.py'
    app.write_text(
        'async def app(scope, receive, send):\n'
        "    if scope['type'] == 'http':\n"
        "        await send({'type': 'http.response.start', 'status': 200, "
        "'headers': [(b'content-length', b'7')]})\n"
        "        await send({'type': 'http.response.body', 'body': b'reload1'})\n"
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.set_inheritable(True)
    port = listener.getsockname()[1]
    fd = listener.fileno()
    env = os.environ.copy()
    env['PYTHONPATH'] = (
        f'{tmp_path}:{env["PYTHONPATH"]}' if env.get('PYTHONPATH') else str(tmp_path)
    )
    watcher = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'h2corn',
            'app:app',
            '--reload',
            '--app-dir',
            str(tmp_path),
            '--bind',
            f'fd://{fd}',
            '--no-access-log',
            '--timeout-graceful-shutdown',
            '1',
        ],
        env=env,
        pass_fds=(fd,),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    listener.close()
    try:
        _wait_http(port, b'reload1')
        first_children = _proc_children(watcher.pid)
        assert first_children, 'reload watcher must have spawned a child'
        original_mtime = app.stat().st_mtime_ns
        app.write_text(
            'async def app(scope, receive, send):\n'
            "    if scope['type'] == 'http':\n"
            "        await send({'type': 'http.response.start', 'status': 200, "
            "'headers': [(b'content-length', b'7')]})\n"
            "        await send({'type': 'http.response.body', 'body': b'reload2'})\n"
        )
        # This is still a real write event, but its timestamp is restored to
        # prove reload does not need an O(tree) snapshot comparison to notice it.
        os.utime(app, ns=(original_mtime, original_mtime))
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                _wait_http(port, b'reload2', timeout=0.5)
                break
            except RuntimeError:
                time.sleep(0.1)
        else:
            raise AssertionError('second generation never served reload2')
        second_children = _proc_children(watcher.pid)
        assert second_children, 'reload must keep a child after the second generation'
    finally:
        if watcher.poll() is None:
            os.killpg(watcher.pid, signal.SIGTERM)
            try:
                watcher.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(watcher.pid, signal.SIGKILL)
                watcher.wait(timeout=3)
        if watcher.stderr is not None:
            watcher.stderr.close()


@_linux_only
def test_sigkill_watcher_stops_child_on_liveness_eof(tmp_path: Path) -> None:
    """SIGKILL of the watcher closes the liveness write end and stops the family."""
    import signal
    import socket
    import subprocess
    import time

    app = tmp_path / 'app.py'
    app.write_text(
        'async def app(scope, receive, send):\n'
        "    if scope['type'] == 'http':\n"
        "        await send({'type': 'http.response.start', 'status': 200, "
        "'headers': [(b'content-length', b'4')]})\n"
        "        await send({'type': 'http.response.body', 'body': b'live'})\n"
    )
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    env = os.environ.copy()
    env['PYTHONPATH'] = (
        f'{tmp_path}:{env["PYTHONPATH"]}' if env.get('PYTHONPATH') else str(tmp_path)
    )
    watcher = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'h2corn',
            'app:app',
            '--reload',
            '--app-dir',
            str(tmp_path),
            '--host',
            '127.0.0.1',
            '--port',
            str(port),
            '--no-access-log',
            '--timeout-graceful-shutdown',
            '0.5',
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _wait_http(port, b'live')
        children = _proc_children(watcher.pid)
        assert len(children) == 1
        child_pid = children[0]
        os.kill(watcher.pid, signal.SIGKILL)
        watcher.wait(timeout=3)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                f'child supervisor {child_pid} survived watcher SIGKILL'
            )
    finally:
        if watcher.poll() is None:
            try:
                os.killpg(watcher.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            watcher.wait(timeout=3)
        if watcher.stderr is not None:
            watcher.stderr.close()
