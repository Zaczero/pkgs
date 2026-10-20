"""Fixture wiring: importable fake runtime + a StubConfig factory."""

from importlib import import_module

import pytest

from tests._support import FIXTURES


@pytest.fixture
def libcst_provider() -> None:
    import_module('libcst')


@pytest.fixture
def mypy_provider() -> None:
    import_module('mypy')


@pytest.fixture
def rust_parser() -> None:
    import_module('tree_sitter')
    import_module('tree_sitter_rust')


@pytest.fixture
def pristine_stub() -> str:
    return (FIXTURES / 'fakepkg' / '_lib.pyi').read_text()


@pytest.fixture
def pristine_src() -> str:
    return (FIXTURES / 'src' / 'lib.rs').read_text()


@pytest.fixture(autouse=True)
def _isolated_mypy_cache(tmp_path_factory: pytest.TempPathFactory, monkeypatch) -> None:
    """Keep the validity gate's persistent cache out of the user's home."""
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path_factory.getbasetemp() / 'cache'))
