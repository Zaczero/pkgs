"""mypy-backed oracle gates over the fixture package (slower: real mypy runs)."""

from __future__ import annotations

import pytest
from conftest import FIXTURES, make_config
from pyo3stubs.oracle import collect_stubtest_errors, collect_validity_errors


@pytest.fixture(autouse=True)
def _fixture_pythonpath(monkeypatch):
    # stubtest runs in a subprocess; the fixture package must be importable
    # there, not just on this process's sys.path.
    monkeypatch.setenv('PYTHONPATH', str(FIXTURES))


def test_validity_clean(tmp_path):
    assert collect_validity_errors(make_config(tmp_path)) == []


def test_validity_flags_bare_impl_after_overload(tmp_path, pristine_stub):
    mutated = pristine_stub.replace(
        '@overload\ndef overloaded(value: str) -> str:',
        'def overloaded(value: str) -> str:',
    )
    assert mutated != pristine_stub
    errors = collect_validity_errors(make_config(tmp_path, stub_text=mutated))
    assert errors, 'bare implementation def in a stub must fail mypy validity'


def test_stubtest_clean():
    cfg = make_config(FIXTURES)  # tmp unused: pristine paths only
    assert collect_stubtest_errors(cfg) == []


def test_stubtest_flags_wrong_default(tmp_path, pristine_stub):
    mutated = pristine_stub.replace('flag: bool = True', 'flag: bool = False')
    assert mutated != pristine_stub
    errors = collect_stubtest_errors(make_config(tmp_path, stub_text=mutated))
    assert any('flag' in e and 'default' in e for e in errors)
