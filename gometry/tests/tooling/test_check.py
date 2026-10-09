from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from tests._support import load_tool

GOMETRY_ROOT = Path(__file__).resolve().parents[2]

quality_check = load_tool('quality_check', GOMETRY_ROOT / 'tools/check.py')
runtime_artifact = sys.modules['runtime_artifact']


def test_no_fix_keeps_source_checks_running_when_runtime_admission_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--no-fix'])
    admissions: list[Path] = []

    def stale(_root: Path) -> None:
        admissions.append(_root)
        raise runtime_artifact.QualityPreparationRequired('runtime is stale')

    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: pytest.fail('--no-fix must not load the quality-preparation owner'),
    )
    monkeypatch.setattr(
        runtime_artifact,
        'inspect_runtime_inputs',
        stale,
    )
    calls: list[tuple[tuple[str, ...], bool]] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        calls.append((arguments, require_runtime))
        return True

    monkeypatch.setattr(quality_check, 'run', run)
    environment_before = os.environ.copy()

    status = quality_check.main()

    output = capsys.readouterr().err
    expected_source_checks = [
        (arguments, False)
        for arguments, runtime_required in quality_check.CHECKS
        if not runtime_required
    ]
    expected_skips = [
        ' '.join(arguments)
        for arguments, runtime_required in quality_check.CHECKS
        if runtime_required
    ]
    assert status == 1
    assert admissions == [GOMETRY_ROOT]
    assert calls == expected_source_checks
    assert output.count('quality runtime prerequisite failed:') == 1
    assert 'quality runtime prerequisite failed: runtime is stale' in output
    assert 'Skipped runtime-dependent quality checks:' in output
    assert all(f'  {label}' in output for label in expected_skips)
    assert 'TOTAL QUALITY FAILURES: 1' in output
    assert f'TOTAL QUALITY SKIPPED: {len(expected_skips)}' in output
    assert os.environ == environment_before


def test_no_fix_inert_admission_enables_runtime_child_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--no-fix'])
    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: pytest.fail('--no-fix must not load the quality-preparation owner'),
    )
    package_root = tmp_path / 'packages' / 'identified-runtime'
    admission = type(
        'Admission',
        (),
        {'receipt': {'package_root': str(package_root)}},
    )()
    admissions: list[Path] = []

    def admit(root: Path):
        admissions.append(root)
        return admission

    monkeypatch.setattr(runtime_artifact, 'inspect_runtime_inputs', admit)
    calls: list[tuple[tuple[str, ...], bool, dict[str, str] | None]] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        calls.append((arguments, require_runtime, environment))
        return True

    monkeypatch.setattr(quality_check, 'run', run)

    status = quality_check.main()

    assert status == 0
    assert admissions == [GOMETRY_ROOT]
    assert len(calls) == len(quality_check.CHECKS)
    runtime_calls = [call for call in calls if call[1]]
    assert len(runtime_calls) == sum(
        runtime_required for _, runtime_required in quality_check.CHECKS
    )
    assert all(
        call[2] is not None
        and call[2]['PYTHONPATH'].split(os.pathsep)[0] == str(package_root)
        for call in runtime_calls
    )
