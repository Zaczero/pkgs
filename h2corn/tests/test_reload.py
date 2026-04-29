from pathlib import Path

import h2corn._reload as reload_module
from h2corn._cli import ImportSettings
from h2corn._reload import (
    _INOTIFY_DIR_REBUILD_MASK,
    _INOTIFY_EVENT,
    _INOTIFY_ISDIR,
    _changed_paths,
    _child_argv,
    _InotifyNotifier,
    _reload_change_message,
    _watch_dirs,
    _watch_file_snapshot,
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

    snapshot = _watch_file_snapshot((tmp_path,), ('*.py',), ('.*', '.py[cod]', '.sw.*', '~*'))

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


def test_reload_snapshot_ignores_dunder_pypackages_dir_by_default(tmp_path: Path) -> None:
    package_dir = tmp_path / '__pypackages__'
    package_dir.mkdir()
    generated = package_dir / 'generated.py'
    generated.write_text('value = 1\n')

    snapshot = _watch_file_snapshot((tmp_path,), ('*.py',), ('.*', '.py[cod]', '.sw.*', '~*'))

    assert generated not in snapshot


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
    child_argv = _child_argv(
        [
            '--reload',
            '--reload-dir',
            'src',
            '--reload-include=*.mo',
            '--reload-exclude',
            'tests',
            '--workers',
            '1',
            'example:app',
        ]
    )

    assert child_argv == ['--workers', '1', 'example:app']


def test_spawn_reload_child_uses_package_module_entrypoint(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured = {}
    pycache_dir = tmp_path / 'pycache'

    class FakeProcess:
        pass

    def popen(args, env):
        captured['args'] = args
        captured['env'] = env
        return FakeProcess()

    monkeypatch.setattr(
        reload_module.tempfile,
        'mkdtemp',
        lambda **_kwargs: pycache_dir,
    )
    monkeypatch.setattr(reload_module.subprocess, 'Popen', popen)

    process, returned_pycache_dir = reload_module._spawn_reload_child(
        ['example:app', '--port', '8000'],
        {'H2CORN_TEST': '1'},
    )

    assert isinstance(process, FakeProcess)
    assert returned_pycache_dir == pycache_dir
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


def test_reload_change_message_includes_changed_path(tmp_path: Path, monkeypatch) -> None:
    changed = tmp_path / 'app.py'
    monkeypatch.chdir(tmp_path)

    message = _reload_change_message((changed,))

    assert message == 'Reload change detected: app.py; restarting'


def test_reload_change_message_summarizes_many_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    changed = tuple(tmp_path / f'file{i}.py' for i in range(5))

    message = _reload_change_message(changed)

    assert message == 'Reload changes detected: file0.py (+4 more); restarting'


def test_inotify_consume_skips_rebuild_for_file_events(monkeypatch) -> None:
    notifier = object.__new__(_InotifyNotifier)
    notifier._fd = 1

    chunks = iter([
        _INOTIFY_EVENT.pack(1, 0x0000_0008, 0, 0),
        BlockingIOError(),
    ])

    def fake_read(_fd: int, _size: int):
        chunk = next(chunks)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    monkeypatch.setattr(reload_module.os, 'read', fake_read)

    assert notifier.consume() is False


def test_inotify_consume_rebuilds_for_directory_topology_events(monkeypatch) -> None:
    notifier = object.__new__(_InotifyNotifier)
    notifier._fd = 1

    chunks = iter([
        _INOTIFY_EVENT.pack(1, _INOTIFY_ISDIR | _INOTIFY_DIR_REBUILD_MASK, 0, 0),
        BlockingIOError(),
    ])

    def fake_read(_fd: int, _size: int):
        chunk = next(chunks)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    monkeypatch.setattr(reload_module.os, 'read', fake_read)

    assert notifier.consume() is True
