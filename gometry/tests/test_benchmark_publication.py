"""Publication transaction rejection, atomicity, and deterministic output."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_BENCHES = _ROOT / 'benches'
for _path in (_BENCHES / 'support', _BENCHES / 'drivers'):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import bench as bench_driver  # noqa: E402
from _bench_registry import RELEASE_OPERATIONS  # noqa: E402
from benchmark_observation import make_observation  # noqa: E402


def _block_payload(operation, index: int) -> dict[str, object]:
    lead = 'ab' if index % 2 == 0 else 'ba'
    order = ['A', 'B'] if lead == 'ab' else ['B', 'A']
    observation_a = make_observation(
        metric='duration',
        raw_values=[1.0],
        useful_work={'count': 1, 'unit': 'equivalent-case-invocation'},
        oracle_identity=f'public:{operation.gometry}:oracle-v1',
        warmup_iterations=2,
        calibration_iterations=5,
    )
    observation_b = make_observation(
        metric='duration',
        raw_values=[2.0],
        useful_work={'count': 1, 'unit': 'equivalent-case-invocation'},
        oracle_identity=f'public:{operation.gometry}:oracle-v1',
        warmup_iterations=2,
        calibration_iterations=5,
    )
    provenance = {
        'source_inputs_sha256': 'source-digest',
        'extension': {'sha256': 'extension-digest'},
        'packages': dict.fromkeys(
            (
                'gometry', 'numpy', 'shapely', 'pyproj', 'h3', 'geopandas',
                'pyarrow', 'mercantile', 'pygeohash',
            ),
            '1',
        ),
    }
    return {
        'schema_version': 1,
        'artifact_type': 'gometry-paired-block',
        'block_id': f'competitors-pair-{RELEASE_OPERATIONS.index(operation) + 1:02d}-block-{index + 1:02d}-{lead}',
        'pair_id': f'competitors-pair-{RELEASE_OPERATIONS.index(operation) + 1:02d}',
        'operation': {
            'gometry': operation.gometry,
            'competitor': operation.competitor,
            'canonical_symbols': list(operation.canonical_symbols),
        },
        'lead_order': order,
        'state_scope': 'shared-process-prepared-fixture',
        'timestamps_monotonic_ns': {'block_started': 1, 'block_ended': 20},
        'sampling': {
            'warmup_iterations': 2,
            'repetitions': 5,
            'calibration_strategy': 'fixed-repetitions',
        },
        'arms': {
            'A': {
                'row': operation.gometry,
                'started_monotonic_ns': 2 if lead == 'ab' else 12,
                'ended_monotonic_ns': 3 if lead == 'ab' else 13,
                'provenance_before': provenance,
                'provenance_after': provenance,
                'observation': observation_a,
            },
            'B': {
                'row': operation.competitor,
                'started_monotonic_ns': 12 if lead == 'ab' else 2,
                'ended_monotonic_ns': 13 if lead == 'ab' else 3,
                'provenance_before': provenance,
                'provenance_after': provenance,
                'observation': observation_b,
            },
        },
    }


def _result(tmp_path: Path) -> dict[str, object]:
    artifacts: list[str] = []
    for operation in RELEASE_OPERATIONS:
        for index in range(6):
            artifact = tmp_path / f'block-{RELEASE_OPERATIONS.index(operation):02d}-{index:02d}.json'
            artifact.write_text(json.dumps(_block_payload(operation, index)), encoding='utf-8')
            artifacts.append(str(artifact))
    metadata = {
        'git_commit': 'deadbeef',
        'source_inputs_sha256': 'source-digest',
        'python_executable': '/python',
        'python': '3.14',
        'python_version': '3.14 test build',
        'python_build': ['build', 'date'],
        'python_implementation': 'CPython',
        'python_debug': False,
        'python_cache_tag': 'cpython-314',
        'extension': {
            'path': '/checkout/target/release/gometry.so',
            'sha256': 'extension-digest',
            'size_bytes': 10,
            'debug_build': False,
            'release_build': True,
            'debug_assertion_marker': 'vendor base-cell partition changed',
            'debug_assertion_marker_count': 0,
            'profile_evidence_method': 'extension_bytes_marker_scan',
            'profile_evidence': 'debug_assertion_marker_absent',
            'tied_to_checkout': True,
        },
        'uv_lock': {'path': '/checkout/uv.lock', 'sha256': 'lock-digest', 'size_bytes': 10},
        'packages': {
            package: {'version': '1'}
            for package in (
                'gometry', 'numpy', 'shapely', 'pyproj', 'h3',
                'geopandas', 'pyarrow', 'mercantile', 'pygeohash',
            )
        },
        'cpu_model': 'test',
        'cpu_governor': 'performance',
        'cpu_affinity': [1],
        'isolated_cpus': '1',
        'selected_cpu': 1,
        'selected_cpu_source': 'explicit-kernel-isolated',
        'selected_cpu_governor': 'performance',
        'cpu_frequency_state': {},
        'geos_version': '3.13.0',
        'proj_version': '9.5.0',
    }
    operations = [
        {
            'domain': operation.domain,
            'label': operation.label,
            'workload': operation.workload,
            'suite': operation.suite,
            'canonical_symbols': list(operation.canonical_symbols),
            'gometry': operation.gometry,
            'competitor': operation.competitor,
            'competitor_label': operation.competitor_label,
            'footnotes': list(operation.footnotes),
        }
        for operation in RELEASE_OPERATIONS
    ]
    return {
        'profile': 'release',
        'plan_only': False,
        'full_manifest': True,
        'filter': None,
        'oracle_ok': True,
        'commands_complete': True,
        'publishable': True,
        'row_counts': {'competitors': 74},
        'public_operations': operations,
        'source_artifacts': artifacts,
        'sampling': {
            'worker': 'bench_paired.py',
            'measured_blocks': 6,
            'lead_orders': ['ab', 'ba', 'ab', 'ba', 'ab', 'ba'],
            'warmup_iterations': 2,
            'repetitions': 5,
        },
        'doctor': {'metadata': metadata, 'hard_failures': [], 'warnings': ['load advisory']},
    }


def _run_manifest(tmp_path: Path, result: dict[str, object] | None = None) -> Path:
    result = _result(tmp_path) if result is None else result
    run_directory = tmp_path / 'run-1-release'
    raw_directory = run_directory / 'raw'
    raw_directory.mkdir(parents=True)
    source_artifacts = [Path(str(path)) for path in result['source_artifacts']]
    copied: list[Path] = []
    for source in source_artifacts:
        artifact = raw_directory / source.name
        shutil.copy2(source, artifact)
        copied.append(artifact)
    result.update({
        'manifest_type': bench_driver._RUN_MANIFEST_TYPE,
        'schema_version': bench_driver._MANIFEST_SCHEMA_VERSION,
        'run_id': 'run-1-release',
        'artifacts': [f'raw/{path.name}' for path in copied],
        'raw_artifacts': [{
            'path': f'raw/{artifact.name}',
            'sha256': bench_driver._file_sha256(artifact),
            'size_bytes': artifact.stat().st_size,
        } for artifact in copied],
    })
    manifest = run_directory / 'run.json'
    manifest.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return manifest


def _analysis(tmp_path: Path) -> tuple[Path, Path]:
    run_manifest = _run_manifest(tmp_path)
    analysis_manifest = bench_driver.analyze_run(
        run_manifest, output_dir=tmp_path / 'analyses'
    )
    return run_manifest, analysis_manifest


def test_release_measurement_cannot_publish_or_mutate_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    current = tmp_path / 'current'
    current.mkdir()
    (current / 'sentinel').write_bytes(b'keep')
    before = {
        path.relative_to(current).as_posix(): path.read_bytes()
        for path in current.rglob('*')
        if path.is_file()
    }

    def fake_plans(*, output_dir: Path, **_: object) -> tuple[list[dict[str, object]], dict[str, list[Path]]]:
        artifact = output_dir / 'release.json'
        artifact.write_text(json.dumps({'benchmarks': []}), encoding='utf-8')
        plan = {
            'kind': 'paired-block',
            'suite': 'competitors',
            'label': 'competitors-release',
            'rows': (),
            'environment': {},
            'command': ['fake-benchmark'],
            'output': str(artifact),
        }
        return [plan], {'competitors': [artifact]}

    monkeypatch.setattr(bench_driver, '_validate_oracle_builders', lambda _: None)
    monkeypatch.setattr(bench_driver, '_benchmark_plans', fake_plans)
    monkeypatch.setattr(bench_driver, '_resource_plans', lambda **_: [])
    monkeypatch.setattr(
        bench_driver,
        'select_benchmark_cpu',
        lambda _cpu: (0, 'explicit-kernel-isolated'),
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
        'collect_contention',
        lambda: {'metadata': {}, 'warnings': []},
    )
    monkeypatch.setattr(
        bench_driver,
        '_run',
        lambda command, **_: {
            'command': command,
            'returncode': 0,
            'timeout': False,
            'elapsed_seconds': 0.0,
            'stdout_tail': '',
            'stderr_tail': '',
        },
    )

    def fail_if_called(*_: object, **__: object) -> None:
        raise AssertionError('measurement invoked publication')

    monkeypatch.setattr(bench_driver, 'publish_current_run', fail_if_called)
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'bench.py',
            '--profile',
            'release',
            '--output-dir',
            str(tmp_path / 'measurements'),
        ],
    )
    bench_driver.main()

    after = {
        path.relative_to(current).as_posix(): path.read_bytes()
        for path in current.rglob('*')
        if path.is_file()
    }
    assert after == before
    manifests = sorted((tmp_path / 'measurements').glob('*/run.json'))
    assert len(manifests) == 1
    assert (manifests[0].parent / 'raw/release.json').is_file()
    assert not (manifests[0].parent / 'run.md').exists()


def test_publication_rejects_filtered_run_without_touching_current(tmp_path: Path) -> None:
    result = _result(tmp_path)
    result['filter'] = 'gometry.points/10k_numpy_xy'
    run_manifest = _run_manifest(tmp_path, result)
    current = tmp_path / 'current'
    current.mkdir()
    sentinel = current / 'sentinel'
    sentinel.write_text('keep', encoding='utf-8')
    with pytest.raises(SystemExit, match='filtered'):
        bench_driver.analyze_run(run_manifest, output_dir=tmp_path / 'stage')
    assert sentinel.read_text(encoding='utf-8') == 'keep'


@pytest.mark.parametrize(
    ('changes', 'message'),
    [
        ({'profile': 'smoke'}, 'only a release profile'),
        ({'plan_only': True}, 'filtered or plan-only'),
        ({'commands_complete': False}, 'run is incomplete'),
    ],
)
def test_non_release_and_incomplete_runs_are_unpublishable(
    tmp_path: Path, changes: dict[str, object], message: str
) -> None:
    result = _result(tmp_path)
    result.update(changes)
    result['publishable'] = False
    run_manifest = _run_manifest(tmp_path, result)
    with pytest.raises(SystemExit, match=message):
        bench_driver.analyze_run(run_manifest, output_dir=tmp_path / 'stage')


def test_publication_rejects_missing_artifact_and_metadata(tmp_path: Path) -> None:
    result = _result(tmp_path)
    run_manifest = _run_manifest(tmp_path, result)
    next(run_manifest.parent.joinpath('raw').glob('block-*.json')).unlink()
    with pytest.raises(SystemExit, match='missing raw artifact'):
        bench_driver.analyze_run(run_manifest, output_dir=tmp_path / 'stage')
    incomplete_root = tmp_path / 'incomplete'
    incomplete_root.mkdir()
    incomplete = _result(incomplete_root)
    del incomplete['doctor']['metadata']['uv_lock']  # type: ignore[index]
    incomplete_manifest = _run_manifest(incomplete_root, incomplete)
    with pytest.raises(SystemExit, match='missing benchmark identity metadata'):
        bench_driver.analyze_run(incomplete_manifest, output_dir=tmp_path / 'stage2')


def test_publication_accepts_absent_or_untracked_lock_identity(tmp_path: Path) -> None:
    result = _result(tmp_path)
    result['doctor']['metadata']['uv_lock'] = {  # type: ignore[index]
        'path': None,
        'sha256': None,
        'size_bytes': None,
    }
    run_manifest = _run_manifest(tmp_path, result)
    analysis_manifest = bench_driver.analyze_run(
        run_manifest, output_dir=tmp_path / 'analyses'
    )
    current = tmp_path / 'current'
    bench_driver.publish_current_run(
        analysis_manifest, output_dir=tmp_path / 'stage', current_dir=current
    )
    identity = json.loads((current / 'identity.json').read_text(encoding='utf-8'))
    assert identity['uv_lock']['sha256'] is None


def test_publication_is_complete_and_deterministic(tmp_path: Path) -> None:
    _run_manifest_path, analysis_manifest = _analysis(tmp_path)
    current = tmp_path / 'current'
    bench_driver.publish_current_run(
        analysis_manifest, output_dir=tmp_path / 'stage', current_dir=current
    )
    first = {
        path.relative_to(current).as_posix(): path.read_bytes()
        for path in current.rglob('*')
        if path.is_file()
    }
    assert {'identity.json', 'summary.json', 'summary.md'} <= set(first)
    assert len([path for path in first if path.startswith('raw/')]) == 222
    summary_md = (current / 'summary.md').read_text(encoding='utf-8')
    assert '## Run environment' in summary_md
    assert 'CPU `test` on core `1`' in summary_md
    assert 'Python `3.14 test build`' in summary_md
    assert 'gometry `1`' in summary_md
    assert 'GEOS 3.13.0; PROJ 9.5.0' in summary_md
    assert '<table>' not in summary_md
    summary = json.loads((current / 'summary.json').read_text(encoding='utf-8'))
    assert len(summary['operations']) == 37
    assert all(operation['blocks'] for operation in summary['operations'])
    bench_driver.publish_current_run(
        analysis_manifest, output_dir=tmp_path / 'stage-again', current_dir=current
    )
    second = {
        path.relative_to(current).as_posix(): path.read_bytes()
        for path in current.rglob('*')
        if path.is_file()
    }
    assert second == first


def test_cli_requires_explicit_analysis_then_publish_manifests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_manifest = _run_manifest(tmp_path)
    analysis_root = tmp_path / 'cli-analyses'
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'bench.py',
            '--analyze',
            str(run_manifest),
            '--output-dir',
            str(analysis_root),
        ],
    )
    bench_driver.main()
    analysis_manifest = analysis_root / 'run-1-release-analysis/analysis.json'
    assert analysis_manifest.is_file()

    current = tmp_path / 'current'
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'bench.py',
            '--publish',
            str(analysis_manifest),
            '--output-dir',
            str(tmp_path / 'stage'),
            '--current-dir',
            str(current),
        ],
    )
    bench_driver.main()
    assert (current / 'summary.json').is_file()


def test_publication_rejects_changed_raw_artifact(tmp_path: Path) -> None:
    _run_manifest_path, analysis_manifest = _analysis(tmp_path)
    source = Path(
        json.loads(
            analysis_manifest.read_text(encoding='utf-8')
        )['source_run_manifest']
    )
    artifact = next(source.parent.joinpath('raw').glob('block-*.json'))
    artifact.write_bytes(artifact.read_bytes() + b'\n')
    with pytest.raises(SystemExit, match='digest mismatch'):
        bench_driver.publish_current_run(
            analysis_manifest,
            output_dir=tmp_path / 'stage',
            current_dir=tmp_path / 'current',
        )
    assert not (tmp_path / 'current').exists()


def test_publication_rejects_changed_analysis_summary(tmp_path: Path) -> None:
    _run_manifest_path, analysis_manifest = _analysis(tmp_path)
    summary = analysis_manifest.parent / 'summary.json'
    summary.write_text(summary.read_text(encoding='utf-8') + ' ', encoding='utf-8')
    with pytest.raises(SystemExit, match='analysis summary identity is stale'):
        bench_driver.publish_current_run(
            analysis_manifest,
            output_dir=tmp_path / 'stage',
            current_dir=tmp_path / 'current',
        )


def test_publication_rolls_back_when_current_replacement_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _run_manifest_path, analysis_manifest = _analysis(tmp_path)
    current = tmp_path / 'current'
    current.mkdir()
    (current / 'sentinel').write_text('keep', encoding='utf-8')
    original_rename = Path.rename

    def fail_stage_rename(path: Path, target: Path | str) -> Path:
        if path.name.startswith('.publication-') and Path(target) == current:
            raise OSError('injected replacement failure')
        return original_rename(path, target)

    monkeypatch.setattr(Path, 'rename', fail_stage_rename)
    with pytest.raises(OSError, match='injected replacement failure'):
        bench_driver.publish_current_run(
            analysis_manifest,
            output_dir=tmp_path / 'stage',
            current_dir=current,
        )
    assert (current / 'sentinel').read_text(encoding='utf-8') == 'keep'
    assert not (tmp_path / '.current.previous').exists()
