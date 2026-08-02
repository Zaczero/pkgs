"""Fixture wiring: importable fake runtime + a StubConfig factory."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pyo3stubs import StubConfig, TokenConfig

FIXTURES = Path(__file__).parent / 'fixtures'

if str(FIXTURES) not in sys.path:
    sys.path.insert(0, str(FIXTURES))


def make_config(tmp_path: Path, **overrides: object) -> StubConfig:
    """A config over the fixture package.

    ``stub_text`` / ``src_text`` overrides materialize a mutated copy under
    ``tmp_path``, so a seeded violation never touches the pristine fixtures.
    """
    stub_path = FIXTURES / 'fakepkg' / '_lib.pyi'
    src_root = FIXTURES / 'src'
    stub_text = overrides.pop('stub_text', None)
    src_text = overrides.pop('src_text', None)
    if stub_text is not None:
        package = tmp_path / 'fakepkg'
        package.mkdir(exist_ok=True)
        # mypy stub-package resolution via MYPYPATH needs the package marker.
        (package / '__init__.pyi').write_text('')
        stub_path = package / '_lib.pyi'
        stub_path.write_text(str(stub_text))
    if src_text is not None:
        src_root = tmp_path / 'src'
        src_root.mkdir(exist_ok=True)
        (src_root / 'lib.rs').write_text(str(src_text))
    defaults: dict[str, object] = {
        'module': 'fakepkg._lib',
        'stub_path': stub_path,
        'src_root': src_root,
        'leak_allowlist': {'Orphan': 'fixture: deliberately unregistered'},
        'uninspectable_allowlist': {
            'hidden_callable': 'fixture: __signature__ raises',
        },
        'tokens': TokenConfig(
            types_module='fakepkg._types',
            vocabulary_export='_token_vocabulary',
        ),
    }
    defaults.update(overrides)
    return StubConfig(**defaults)  # type: ignore[arg-type]


def seeded(source: str, anchor: str, replacement: str, *, count: int = 1) -> str:
    """``source`` with ``anchor`` replaced, proving the mutation landed.

    Ten of thirteen mutating tests once omitted this guard, so a fixture edit
    that drifted an anchor silently turned the test into a no-op that still
    passed. Asserting the occurrence count makes that class unrepresentable
    rather than merely absent.
    """
    found = source.count(anchor)
    assert found == count, (
        f'fixture drifted: expected {count} occurrence(s) of {anchor!r}, found {found}'
    )
    mutated = source.replace(anchor, replacement)
    assert mutated != source, f'mutation is a no-op: {anchor!r} -> {replacement!r}'
    return mutated


@pytest.fixture
def pristine_stub() -> str:
    return (FIXTURES / 'fakepkg' / '_lib.pyi').read_text()


@pytest.fixture
def pristine_src() -> str:
    return (FIXTURES / 'src' / 'lib.rs').read_text()
