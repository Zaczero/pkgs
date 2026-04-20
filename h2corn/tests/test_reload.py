from pathlib import Path

from h2corn._cli import ImportSettings
from h2corn._reload import (
    _changed_paths,
    _python_file_snapshot,
    _reload_change_message,
    _watch_dirs,
)


def test_reload_snapshot_respects_include_patterns(tmp_path: Path) -> None:
    python_file = tmp_path / 'app.py'
    other_file = tmp_path / 'messages.mo'
    python_file.write_text('value = 1\n')
    other_file.write_text('catalog')

    snapshot = _python_file_snapshot((tmp_path,), ('*.py',), ())

    assert python_file in snapshot
    assert other_file not in snapshot


def test_reload_snapshot_ignores_hidden_files_by_default(tmp_path: Path) -> None:
    visible = tmp_path / 'app.py'
    hidden = tmp_path / '.app.py'
    visible.write_text('value = 1\n')
    hidden.write_text('value = 2\n')

    snapshot = _python_file_snapshot((tmp_path,), ('*.py',), ('.*', '.py[cod]', '.sw.*', '~*'))

    assert visible in snapshot
    assert hidden not in snapshot


def test_reload_snapshot_matches_relative_path_include_patterns(tmp_path: Path) -> None:
    locale_dir = tmp_path / 'locale' / 'en'
    locale_dir.mkdir(parents=True)
    catalog = locale_dir / 'messages.mo'
    catalog.write_text('catalog')

    snapshot = _python_file_snapshot((tmp_path,), ('locale/**/*.mo',), ())

    assert catalog in snapshot


def test_reload_snapshot_respects_exclude_patterns(tmp_path: Path) -> None:
    included = tmp_path / 'app.py'
    excluded_dir = tmp_path / 'tests'
    excluded_dir.mkdir()
    excluded = excluded_dir / 'test_app.py'
    included.write_text('value = 1\n')
    excluded.write_text('value = 2\n')

    snapshot = _python_file_snapshot((tmp_path,), ('*.py',), ('tests',))

    assert included in snapshot
    assert excluded not in snapshot


def test_reload_snapshot_ignores_dunder_pypackages_dir_by_default(tmp_path: Path) -> None:
    package_dir = tmp_path / '__pypackages__'
    package_dir.mkdir()
    generated = package_dir / 'generated.py'
    generated.write_text('value = 1\n')

    snapshot = _python_file_snapshot((tmp_path,), ('*.py',), ('.*', '.py[cod]', '.sw.*', '~*'))

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
