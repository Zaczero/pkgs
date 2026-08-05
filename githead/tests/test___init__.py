from pathlib import Path

import pytest

from githead import githead


@pytest.fixture
def git_path(tmp_path: Path) -> Path:
    path = tmp_path / 'git'
    (path / 'refs').mkdir(parents=True)
    (path / 'refs/foo').write_text('bar\n')
    return path


@pytest.mark.parametrize('type_', [Path, str])
def test_direct(type_, git_path: Path):
    (git_path / 'HEAD').write_text('bca663418428d603eea8243d08a5ded19eb19a34\n')
    assert githead(type_(git_path)) == 'bca663418428d603eea8243d08a5ded19eb19a34'


def test_reference(git_path: Path):
    (git_path / 'HEAD').write_text('ref: refs/foo\n')
    assert githead(git_path) == 'bar'


def test_dir_not_found():
    with pytest.raises(FileNotFoundError):
        githead(Path('tests/git-not-found'))


def test_reference_not_found(git_path: Path):
    (git_path / 'HEAD').write_text('ref: refs/not-found\n')
    with pytest.raises(FileNotFoundError):
        githead(git_path)


def test_reference_outside_git(git_path: Path):
    (git_path / 'HEAD').write_text('ref: ../test___init__.py\n')
    with pytest.raises(ValueError, match='HEAD references outside of'):
        githead(git_path)
