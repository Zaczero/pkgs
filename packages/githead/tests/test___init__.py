from pathlib import Path

import pytest
from githash import githash

GIT_PATH = Path('tests/git')
HEAD_PATH = GIT_PATH.joinpath('HEAD')


@pytest.mark.parametrize('type_', [Path, str])
def test_direct(type_):
    HEAD_PATH.write_text('bca663418428d603eea8243d08a5ded19eb19a34\n')
    assert githash(type_(GIT_PATH)) == 'bca663418428d603eea8243d08a5ded19eb19a34'


def test_reference():
    HEAD_PATH.write_text('ref: refs/foo\n')
    assert githash(GIT_PATH) == 'bar'


def test_dir_not_found():
    with pytest.raises(FileNotFoundError):
        githash(Path('tests/git-not-found'))


def test_reference_not_found():
    HEAD_PATH.write_text('ref: refs/not-found\n')
    with pytest.raises(FileNotFoundError):
        githash(GIT_PATH)


def test_reference_outside_git():
    HEAD_PATH.write_text('ref: ../test___init__.py\n')
    with pytest.raises(ValueError, match='HEAD references outside of .git directory'):
        githash(GIT_PATH)
