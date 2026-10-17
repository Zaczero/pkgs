"""Validation-only ownership for every retained focused benchmark case."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_CASES = sorted((_ROOT / 'benches' / 'cases').glob('case_*.py'))
sys.path.insert(0, str(_ROOT / 'tools'))
import benchmark_runtime  # ruff: ignore[module-import-not-at-top-of-file]
from quality_runtime import (  # ruff: ignore[module-import-not-at-top-of-file]
    quality_receipt_path,
)


@pytest.mark.parametrize('case_path', _CASES, ids=lambda path: path.stem)
def test_focused_case_validation_is_not_measurement(
    case_path: Path, quality_artifact_root: Path
) -> None:
    subject = benchmark_runtime.load_subject(
        quality_receipt_path(quality_artifact_root)
    )
    environment = benchmark_runtime.environment_for_subject(
        subject,
        benchmark_runtime.package_root_for_subject(subject),
        invocation_id='validation',
        control=False,
    )
    result = subprocess.run(
        [sys.executable, str(case_path), '--validate'],
        cwd=_ROOT,
        env=environment,
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
