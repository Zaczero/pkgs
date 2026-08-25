"""Validation-only ownership for every retained focused benchmark case."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_CASES = sorted((_ROOT / 'benches' / 'cases').glob('case_*.py'))


@pytest.mark.parametrize('case_path', _CASES, ids=lambda path: path.stem)
def test_focused_case_validation_is_not_measurement(case_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(case_path), '--validate'],
        cwd=_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload == {
        'artifact_type': 'gometry-focused-case-validation',
        'case_id': case_path.stem,
        'schema_version': 1,
        'timed': False,
        'useful_output': payload['useful_output'],
    }
    assert payload['useful_output']['count'] > 0
    assert payload['useful_output']['unit']
