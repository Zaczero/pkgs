"""Fixture wiring: importable fake runtime + a StubConfig factory."""

import pytest

from tests._support import FIXTURES


@pytest.fixture
def pristine_stub() -> str:
    return (FIXTURES / 'fakepkg' / '_lib.pyi').read_text()


@pytest.fixture
def pristine_src() -> str:
    return (FIXTURES / 'src' / 'lib.rs').read_text()
