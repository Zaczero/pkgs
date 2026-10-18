"""Fixture wiring: importable fake runtime + a StubConfig factory."""

import pytest

from tests._support import FIXTURES


@pytest.fixture
def libcst_provider() -> None:
    pytest.importorskip('libcst', reason='stub generation requires LibCST')


@pytest.fixture
def mypy_provider() -> None:
    pytest.importorskip('mypy', reason='type-checking requires mypy')


@pytest.fixture
def rust_parser() -> None:
    pytest.importorskip('tree_sitter', reason='Rust source checks require tree-sitter')
    pytest.importorskip(
        'tree_sitter_rust', reason='Rust source checks require the Rust grammar'
    )


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
