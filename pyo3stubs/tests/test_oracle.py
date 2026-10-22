"""External checker failures remain failures even without useful diagnostics."""

from __future__ import annotations

import json
import subprocess

import pytest

from pyo3stubs.gates import Status, gate
from tests._support import make_config


@pytest.mark.parametrize('name', ['validity', 'stubtest', 'pyright'])
def test_silent_checker_failure_cannot_pass(tmp_path, monkeypatch, name):
    output = (
        json.dumps({'generalDiagnostics': [], 'summary': {'filesAnalyzed': 1}})
        if name == 'pyright'
        else ''
    )
    monkeypatch.setattr(
        subprocess,
        'run',
        lambda *args, **_kwargs: subprocess.CompletedProcess(args[0], 2, output, ''),
    )

    result = gate(name).run(make_config(tmp_path))

    assert result.status is Status.VIOLATIONS
    assert len(result.errors) == 1
    assert 'status 2' in result.errors[0]


def test_stubtest_counts_diagnostics_with_their_expected_and_actual_values(
    tmp_path, monkeypatch
):
    output = (
        'error: fakepkg._lib.first is inconsistent\n'
        'Stub: def (value: int)\n'
        'Runtime: def (value, extra)\n\n'
        'error: fakepkg._lib.second is inconsistent\n'
        'Stub: def (value: str)\n'
        'Runtime: def (value=Ellipsis)\n\n'
        'Found 2 errors (checked 1 module)\n'
    )
    monkeypatch.setattr(
        subprocess,
        'run',
        lambda *args, **_kwargs: subprocess.CompletedProcess(args[0], 1, output, ''),
    )

    result = gate('stubtest').run(make_config(tmp_path))

    assert result.status is Status.VIOLATIONS
    assert len(result.errors) == 2
    assert 'Stub: def (value: int)' in result.errors[0]
    assert 'Runtime: def (value, extra)' in result.errors[0]
    assert 'Runtime: def (value=Ellipsis)' in result.errors[1]
