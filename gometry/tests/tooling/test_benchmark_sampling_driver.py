"""Release sampling decisions exercised through the benchmark driver."""

# ruff: file-ignore[module-import-not-at-top-of-file] - Select checkout tooling before importing it.
from __future__ import annotations

import dataclasses
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
_BENCHES = GOMETRY_ROOT / 'benches'
for _path in (
    _BENCHES / 'drivers',
    _BENCHES / 'python',
    _BENCHES / 'support',
    GOMETRY_ROOT / 'tools',
):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import bench as bench_driver
from _bench_registry import RELEASE_OPERATIONS
from benchmark_evidence import compact_artifact
from benchmark_observation import make_observation


def _driver_run(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    elapsed_seconds: float,
    total_timeout: int = 2_400,
    failed_block: int | None = None,
    operations: tuple[Any, ...] | None = None,
) -> Path:
    selected_operations = operations or (RELEASE_OPERATIONS[0],)
    operations_by_name = {
        operation.gometry: operation for operation in selected_operations
    }
    subject = {
        'kind': 'wheel',
        'path': str(tmp_path / 'subject.whl'),
        'receipt_sha256': 'a' * 64,
        'source': {
            'source_inputs_sha256': 'b' * 64,
            'native_source_inputs_sha256': 'c' * 64,
        },
    }
    admission_digest = hashlib.sha256(
        json.dumps({'kind': 'gometry-benchmark-admission'}).encode()
    ).hexdigest()

    def prepare_admission(
        admitted_subject: dict[str, Any], _package_root: Path, path: Path
    ) -> None:
        admission = {'kind': 'gometry-benchmark-admission'}
        encoded = json.dumps(admission).encode()
        path.write_bytes(encoded)
        admitted_subject['run_admission'] = {
            'path': str(path),
            'sha256': admission_digest,
            'seconds': 0.01,
        }

    monkeypatch.setattr(
        bench_driver.benchmark_runtime, 'load_subject', lambda _path=None: subject
    )
    monkeypatch.setattr(
        bench_driver.benchmark_runtime,
        'package_root_for_subject',
        lambda _subject, _explicit=None: tmp_path,
    )
    monkeypatch.setattr(
        bench_driver.benchmark_runtime,
        'prepare_run_admission',
        prepare_admission,
    )
    monkeypatch.setattr(
        bench_driver.benchmark_runtime,
        'complete_run_admission',
        lambda _subject: {'completion': 0.01},
    )
    monkeypatch.setattr(
        bench_driver,
        'collect',
        lambda **_: {
            'metadata': {'cpu_frequency_state': {}},
            'hard_failures': [],
            'warnings': [],
        },
    )
    monkeypatch.setattr(
        bench_driver,
        'select_benchmark_cpu',
        lambda _cpu: (0, 'test-selected-cpu'),
    )
    monkeypatch.setattr(
        bench_driver,
        'collect_contention',
        lambda: {'metadata': {}, 'warnings': []},
    )
    profiles = dict(bench_driver.PROFILES)
    profiles['release'] = dataclasses.replace(
        profiles['release'], total_timeout=total_timeout
    )
    monkeypatch.setattr(bench_driver, 'PROFILES', profiles)

    def write_artifact(path: Path, artifact: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf-8') as stream:
            json.dump(
                compact_artifact(artifact, admission_digest),
                stream,
                sort_keys=True,
            )
            stream.write('\n')

    def fake_run(command: list[str], **_: Any) -> dict[str, Any]:
        output = Path(command[command.index('--output') + 1])
        if any('bench_oracle.py' in part for part in command):
            operation_names = command[command.index('--operations') + 1].split(',')
            write_artifact(
                output,
                {
                    'schema_version': 1,
                    'artifact_type': 'gometry-public-oracle',
                    'operations': operation_names,
                    'identities': [
                        f'public:{operation_name}:oracle'
                        for operation_name in operation_names
                    ],
                    'verified': len(operation_names),
                    'state_scope': 'external-fresh-process',
                    'phase_seconds': {'fixture_and_oracle': 0.02},
                },
            )
            returncode = 0
        else:
            block_id = command[command.index('--block-id') + 1]
            block_number = int(block_id.split('-block-', 1)[1].split('-', 1)[0])
            operation = operations_by_name[command[command.index('--gometry') + 1]]
            returncode = int(failed_block == block_number)
            if returncode == 0:
                lead = command[command.index('--lead') + 1]
                ratio = (
                    1.5 if outcome == 'resolved' else 0.8 if block_number % 2 else 1.2
                )
                useful_work = {'count': 1, 'unit': 'case invocation'}
                observation_a = make_observation(
                    metric='duration',
                    raw_values=[1.0],
                    useful_work=useful_work,
                    oracle_identity=f'public:{operation.gometry}:oracle',
                    phase='warm',
                    warmup_iterations=2,
                    calibration_iterations=1,
                )
                observation_b = make_observation(
                    metric='duration',
                    raw_values=[ratio],
                    useful_work=useful_work,
                    oracle_identity=f'public:{operation.gometry}:oracle',
                    phase='warm',
                    warmup_iterations=2,
                    calibration_iterations=1,
                )
                write_artifact(
                    output,
                    {
                        'schema_version': 1,
                        'artifact_type': 'gometry-paired-block',
                        'block_id': block_id,
                        'pair_id': command[command.index('--pair-id') + 1],
                        'operation': {
                            'gometry': operation.gometry,
                            'competitor': operation.competitor,
                        },
                        'lead_order': ['A', 'B'] if lead == 'ab' else ['B', 'A'],
                        'arms': {
                            'A': {'observation': observation_a},
                            'B': {'observation': observation_b},
                        },
                        'phase_seconds': {
                            'fixture_and_competitor_import': 0.003,
                            'measurement': 0.007,
                            'admission': 0.002,
                            'identity': 0.001,
                            'completion': 0.002,
                        },
                    },
                )
        return {
            'command': command,
            'returncode': returncode,
            'timeout': False,
            'elapsed_seconds': elapsed_seconds,
            'stdout_tail': '',
            'stderr_tail': 'injected worker failure' if returncode else '',
        }

    monkeypatch.setattr(bench_driver, '_run', fake_run)
    output_dir = tmp_path / 'runs'
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'bench.py',
            '--profile',
            'release',
            '--filter',
            ','.join(operation.gometry for operation in selected_operations),
            '--output-dir',
            str(output_dir),
        ],
    )
    bench_driver.main()
    return next(output_dir.glob('*/run.json'))


def test_driver_stops_a_resolved_comparison_at_the_first_look(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = _driver_run(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        outcome='resolved',
        elapsed_seconds=0.01,
    )
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    operation = RELEASE_OPERATIONS[0].gometry
    decision = manifest['sampling']['decisions'][operation]

    assert len(decision['block_ids']) == 6
    assert decision['collection_status'] == 'complete'
    assert decision['inference']['direction'] == 'above'
    assert decision['inference']['reason'] == 'direction-resolved'
    assert all(command.get('phase_seconds') for command in manifest['commands'])


def test_driver_extends_an_unresolved_comparison_to_the_second_look(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = _driver_run(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        outcome='unresolved',
        elapsed_seconds=0.01,
        operations=RELEASE_OPERATIONS[:2],
    )
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    paired_commands = [
        command
        for command in manifest['commands']
        if command.get('kind') == 'paired-block'
    ]

    assert len(manifest['public_operations']) == 2
    assert len(paired_commands) == 24
    assert all(
        int(command['block_id'].split('-block-', 1)[1].split('-', 1)[0]) <= 6
        for command in paired_commands[:12]
    )
    assert all(
        int(command['block_id'].split('-block-', 1)[1].split('-', 1)[0]) >= 7
        for command in paired_commands[12:]
    )
    for operation in RELEASE_OPERATIONS[:2]:
        decision = manifest['sampling']['decisions'][operation.gometry]
        assert len(decision['block_ids']) == 12
        assert decision['lead_orders'] == ['ab', 'ba'] * 6
        assert decision['collection_status'] == 'complete'
        assert decision['inference']['reason'] == 'maximum-look'
        assert decision['inference']['direction'] == 'unresolved'
    assert manifest['commands_complete'] is True


def test_driver_completes_budget_limited_comparison_as_inconclusive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = _driver_run(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        outcome='unresolved',
        elapsed_seconds=10.0,
        total_timeout=85,
    )
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    operation = RELEASE_OPERATIONS[0].gometry
    decision = manifest['sampling']['decisions'][operation]

    assert len(decision['block_ids']) == 6
    assert decision['collection_status'] == 'budget-limited'
    assert decision['inference']['next_look'] == 12
    assert manifest['commands_complete'] is True


def test_driver_does_not_complete_a_failed_first_look(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(SystemExit) as error:
        _driver_run(
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            outcome='unresolved',
            elapsed_seconds=0.01,
            failed_block=3,
        )
    assert error.value.code == 1
    manifest_path = next((tmp_path / 'runs').glob('*/run.json'))
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    operation = RELEASE_OPERATIONS[0].gometry

    assert manifest['oracle_ok'] is True
    assert manifest['commands_complete'] is False
    assert manifest['sampling']['decisions'][operation]['collection_status'] == (
        'incomplete'
    )
    assert len(manifest['sampling']['actual_plan'][operation]['block_ids']) == 6
