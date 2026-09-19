"""Publication transaction rejection, atomicity, and deterministic output."""

from __future__ import annotations

import hashlib
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
from _bench_registry import ALL_PUBLIC_OPERATIONS, scaling_record  # noqa: E402
from benchmark_observation import make_observation  # noqa: E402

from tests._bench_support import (  # noqa: E402
    HASH,
    file_identity,
    package_identity,
    producer,
)


def _block_payload(operation, index: int, lock_path: str = 'lock') -> dict[str, object]:
    lead = 'ab' if index % 2 == 0 else 'ba'
    order = ['A', 'B'] if lead == 'ab' else ['B', 'A']
    scaling = scaling_record(operation)
    useful_work = {
        'count': max(1, int(scaling['point'])) if scaling else 1,
        'unit': scaling['useful_work_unit']
        if scaling
        else 'equivalent-case-invocation',
    }
    observation_a = make_observation(
        metric='duration',
        raw_values=[1.0],
        oracle_identity=f'public:{operation.gometry}:oracle',
        phase=operation.phase,
        useful_work=useful_work,
        warmup_iterations=2,
        calibration_iterations=5,
    )
    observation_b = make_observation(
        metric='duration',
        raw_values=[2.0],
        oracle_identity=f'public:{operation.gometry}:oracle',
        phase=operation.phase,
        useful_work=useful_work,
        warmup_iterations=2,
        calibration_iterations=5,
    )
    provenance_a = producer(operation.gometry, 'A', lock_path)
    provenance_b = producer(operation.competitor, 'B', lock_path)
    observation_a['producer_identity'] = provenance_a
    observation_b['producer_identity'] = provenance_b
    return {
        'schema_version': 1,
        'artifact_type': 'gometry-paired-block',
        'profile': 'release',
        'run_id': 'run-1-release',
        'block_id': f'competitors-pair-{ALL_PUBLIC_OPERATIONS.index(operation) + 1:02d}-block-{index + 1:02d}-{lead}',
        'pair_id': f'competitors-pair-{ALL_PUBLIC_OPERATIONS.index(operation) + 1:02d}',
        'operation': {
            'gometry': operation.gometry,
            'competitor': operation.competitor,
            'canonical_symbols': list(operation.canonical_symbols),
            'phase': operation.phase,
            'scaling': scaling,
        },
        'lead_order': order,
        'state_scope': 'shared-process-prepared-fixture',
        'timestamps_monotonic_ns': {'block_started': 1, 'block_ended': 20},
        'sampling': {
            'warmup_iterations': 2,
            'repetitions': 5,
            'calibration_strategy': 'fixed-repetitions',
            'phase': operation.phase,
            'scaling': scaling,
        },
        'arms': {
            'A': {
                'row': operation.gometry,
                'started_monotonic_ns': 2 if lead == 'ab' else 12,
                'ended_monotonic_ns': 3 if lead == 'ab' else 13,
                'provenance_before': provenance_a,
                'provenance_after': provenance_a,
                'observation': observation_a,
            },
            'B': {
                'row': operation.competitor,
                'started_monotonic_ns': 12 if lead == 'ab' else 2,
                'ended_monotonic_ns': 13 if lead == 'ab' else 3,
                'provenance_before': provenance_b,
                'provenance_after': provenance_b,
                'observation': observation_b,
            },
        },
    }


def _result(tmp_path: Path) -> dict[str, object]:
    lock = tmp_path / 'lock'
    lock.write_bytes(b'x')
    artifacts: list[str] = []
    lock_path = str(tmp_path / 'lock')
    for operation in ALL_PUBLIC_OPERATIONS:
        for index in range(6):
            operation_index = ALL_PUBLIC_OPERATIONS.index(operation)
            artifact = tmp_path / f'block-{operation_index:02d}-{index:02d}.json'
            artifact.write_text(
                json.dumps(_block_payload(operation, index, lock_path)),
                encoding='utf-8',
            )
            artifacts.append(str(artifact))
    oracle = tmp_path / 'oracle.json'
    oracle.write_text(
        json.dumps({
            'schema_version': 1,
            'artifact_type': 'gometry-public-oracle',
            'operations': [operation.gometry for operation in ALL_PUBLIC_OPERATIONS],
            'identities': [
                f'public:{operation.gometry}:oracle'
                for operation in ALL_PUBLIC_OPERATIONS
            ],
            'verified': len(ALL_PUBLIC_OPERATIONS),
            'state_scope': 'external-fresh-process',
        }),
        encoding='utf-8',
    )
    artifacts.insert(0, str(oracle))
    metadata = {
        'git_commit': 'deadbeef',
        'source_inputs_sha256': HASH,
        'python_executable': '/python',
        'python': '3.14',
        'python_version': '3.14 test build',
        'python_build': ['build', 'date'],
        'python_implementation': 'CPython',
        'python_debug': False,
        'python_cache_tag': 'cpython-314',
        'extension': {
            'path': '/checkout/target/release/gometry.so',
            'sha256': HASH,
            'size_bytes': 1,
            'build_info': {
                'schema_version': 1,
                'source_inputs_format': 'sha256-path-nul-content-nul',
                'source_inputs_sha256': HASH,
                'profile_family': 'release',
                'opt_level': '3',
                'debug': False,
                'debug_assertions': False,
                'target': 'test-target',
                'target_features': 'sse2',
            },
        },
        'uv_lock': {'path': str(lock), 'sha256': HASH, 'size_bytes': 1},
        'packages': {
            package: package_identity(package)
            for package in (
                'gometry',
                'numpy',
                'shapely',
                'pyproj',
                'h3',
                'geopandas',
                'pyarrow',
                'mercantile',
                'pygeohash',
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
        'cpu_topology': {'available': False, 'cpus': {}, 'allowed_cpus': [1]},
        'cache_topology': [],
        'numa_topology': {'available': False, 'nodes': {}},
        'allowed_memory_nodes': None,
        'memory_policy': None,
        'memory_placement': {'available': False},
        'cgroup_v2': {'available': False},
        'loaded_native_libraries': [],
        'native_library_identity': {
            'available': True,
            'source': '/proc/self/maps',
            'reason': None,
        },
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
            'footnotes': list(getattr(operation, 'footnotes', ())),
            'phase': operation.phase,
            'scaling': scaling_record(operation),
        }
        for operation in ALL_PUBLIC_OPERATIONS
    ]
    return {
        'profile': 'release',
        'plan_only': False,
        'full_manifest': True,
        'filter': None,
        'oracle_ok': True,
        'commands_complete': True,
        'publishable': True,
        'row_counts': {'competitors': 106},
        'public_operations': operations,
        'source_artifacts': artifacts,
        'oracle_artifact': 'raw/oracle.json',
        'sampling': {
            'worker': 'bench_paired.py',
            'measured_blocks': 6,
            'lead_orders': ['ab', 'ba', 'ab', 'ba', 'ab', 'ba'],
            'warmup_iterations': 2,
            'repetitions': 5,
            'phase': 'warm',
            'oracle_state_scope': 'external-fresh-process',
        },
        'doctor': {
            'metadata': metadata,
            'hard_failures': [],
            'warnings': ['load advisory'],
        },
    }


def _run_manifest(tmp_path: Path, result: dict[str, object] | None = None) -> Path:
    result = _result(tmp_path) if result is None else result
    run_directory = tmp_path / 'run'
    raw_directory = run_directory / 'raw'
    raw_directory.mkdir(parents=True)
    artifacts = []
    for source in result.pop('source_artifacts'):
        artifact = raw_directory / Path(str(source)).name
        shutil.copy2(source, artifact)
        artifacts.append(f'raw/{artifact.name}')
    result.update({
        'manifest_type': bench_driver._RUN_MANIFEST_TYPE,
        'schema_version': bench_driver._MANIFEST_SCHEMA_VERSION,
        'artifacts': artifacts,
    })
    manifest = run_directory / 'run.json'
    manifest.write_text(
        json.dumps(result, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    return manifest


def _mutate_first_block(run_manifest: Path, mutate) -> None:
    for raw in sorted(run_manifest.parent.joinpath('raw').glob('block-*.json')):
        payload = json.loads(raw.read_text(encoding='utf-8'))
        mutate(payload)
        raw.write_text(json.dumps(payload), encoding='utf-8')
        return
    raise AssertionError('fixture has no paired block artifact')


def _publish(run_manifest: Path, tmp_path: Path) -> Path:
    return bench_driver.publish_run(run_manifest, current_dir=tmp_path / 'current')


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

    def fake_plans(
        *, output_dir: Path, **_: object
    ) -> tuple[list[dict[str, object]], dict[str, list[Path]]]:
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
            'metadata': {
                'cpu_frequency_state': {},
                'uv_lock': {
                    'path': str(tmp_path / 'lock'),
                    'sha256': hashlib.sha256(b'x').hexdigest(),
                    'size_bytes': 1,
                },
            },
            'hard_failures': [],
            'warnings': [],
        },
    )
    monkeypatch.setattr(
        bench_driver,
        'collect_contention',
        lambda: {'metadata': {}, 'warnings': []},
    )

    def fake_run(command: list[str], **_: object) -> dict[str, object]:
        if '--output' in command:
            Path(command[command.index('--output') + 1]).write_text(
                '{}', encoding='utf-8'
            )
        return {
            'command': command,
            'returncode': 0,
            'timeout': False,
            'elapsed_seconds': 0.0,
            'stdout_tail': '',
            'stderr_tail': '',
        }

    monkeypatch.setattr(bench_driver, '_run', fake_run)

    def fail_if_called(*_: object, **__: object) -> None:
        raise AssertionError('measurement invoked publication')

    monkeypatch.setattr(bench_driver, 'publish_run', fail_if_called)
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
    (tmp_path / 'lock').write_bytes(b'x')
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


def test_publication_rejects_filtered_run_without_touching_current(
    tmp_path: Path,
) -> None:
    result = _result(tmp_path)
    result['filter'] = 'gometry.points/10k_numpy_xy'
    run_manifest = _run_manifest(tmp_path, result)
    current = tmp_path / 'current'
    current.mkdir()
    sentinel = current / 'sentinel'
    sentinel.write_text('keep', encoding='utf-8')
    with pytest.raises(SystemExit, match='filtered'):
        _publish(run_manifest, tmp_path)
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
        _publish(run_manifest, tmp_path)


def test_publication_rejects_missing_artifact_and_metadata(tmp_path: Path) -> None:
    result = _result(tmp_path)
    run_manifest = _run_manifest(tmp_path, result)
    next(run_manifest.parent.joinpath('raw').glob('block-*.json')).unlink()
    with pytest.raises(SystemExit, match='references a missing artifact'):
        _publish(run_manifest, tmp_path)
    incomplete_root = tmp_path / 'incomplete'
    incomplete_root.mkdir()
    incomplete = _result(incomplete_root)
    del incomplete['doctor']['metadata']['uv_lock']  # type: ignore[index]
    incomplete_manifest = _run_manifest(incomplete_root, incomplete)
    with pytest.raises(SystemExit, match='missing benchmark identity metadata'):
        _publish(incomplete_manifest, tmp_path)


@pytest.mark.parametrize(
    ('mutation', 'message'),
    [
        (
            lambda payload: payload['operation'].__setitem__(
                'competitor', 'shapely.swapped'
            ),
            'extra or swapped',
        ),
        (
            lambda payload: payload.__setitem__(
                'block_id', 'competitors-pair-01-block-01-ba'
            ),
            'extra or swapped',
        ),
        (
            lambda payload: payload.__setitem__('lead_order', ['B', 'A']),
            'extra or swapped',
        ),
    ],
    ids=('operation', 'block', 'lead'),
)
def test_publication_rejects_exact_matrix_mutations(
    tmp_path: Path, mutation, message: str
) -> None:
    run_manifest = _run_manifest(tmp_path)
    _mutate_first_block(run_manifest, mutation)
    with pytest.raises(SystemExit, match=message):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_comparator_module_mutation(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        identity = payload['arms']['B']['provenance_before']
        identity['implementation']['module']['sha256'] = '1' * 64
        payload['arms']['B']['provenance_after'] = identity
        payload['arms']['B']['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='module identity mismatch'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_native_library_mutation(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        identity = payload['arms']['A']['provenance_before']
        identity['implementation']['native_libraries'][0]['sha256'] = '1' * 64
        payload['arms']['A']['provenance_after'] = identity
        payload['arms']['A']['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='native-library identity mismatch'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_comparator_native_library_mutation(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        identity = payload['arms']['B']['provenance_before']
        identity['implementation']['native_libraries'] = [
            {
                **file_identity('/checkout/libgeos_c.so'),
                'sha256': '1' * 64,
            }
        ]
        identity['implementation']['native_library_status'] = 'identified'
        payload['arms']['B']['provenance_after'] = identity
        payload['arms']['B']['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='native-library identity mismatch'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_dependency_lock_mutation(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        for side in ('A', 'B'):
            identity = payload['arms'][side]['provenance_before']
            identity['uv_lock']['sha256'] = '1' * 64
            payload['arms'][side]['provenance_after'] = identity
            payload['arms'][side]['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='lock identity mismatch'):
        _publish(run_manifest, tmp_path)


@pytest.mark.parametrize(
    ('mutation', 'message'),
    [
        ('missing', 'required geos engine is not attributed'),
        ('unrelated', 'native-library ownership mismatch'),
        ('hash', 'native-library identity mismatch'),
    ],
)
def test_publication_rejects_geos_attribution_mutations(
    tmp_path: Path, mutation: str, message: str
) -> None:
    run_manifest = _run_manifest(tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        identity = payload['arms']['B']['provenance_before']
        if mutation == 'missing':
            identity['implementation']['native_libraries'] = []
            identity['implementation']['native_library_status'] = 'not-applicable'
        elif mutation == 'unrelated':
            identity['implementation']['native_libraries'] = [
                file_identity('/checkout/libpython.so')
            ]
        else:
            identity['implementation']['native_libraries'][0]['sha256'] = '1' * 64
        payload['arms']['B']['provenance_after'] = identity
        payload['arms']['B']['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match=message):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_effective_build_fact_mutation(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        for side in ('A', 'B'):
            identity = payload['arms'][side]['provenance_before']
            identity['extension']['build_info']['opt_level'] = '0'
            payload['arms'][side]['provenance_after'] = identity
            payload['arms'][side]['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='effective build identity'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_extra_paired_artifact(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)
    result = json.loads(run_manifest.read_text(encoding='utf-8'))
    source = next(run_manifest.parent.joinpath('raw').glob('block-*.json'))
    extra = run_manifest.parent / 'raw' / 'extra-paired.json'
    shutil.copy2(source, extra)
    result['artifacts'].append(f'raw/{extra.name}')
    run_manifest.write_text(json.dumps(result), encoding='utf-8')
    with pytest.raises(SystemExit, match='duplicate paired matrix identity'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_mismatched_external_oracle_artifact(
    tmp_path: Path,
) -> None:
    run_manifest = _run_manifest(tmp_path)
    oracle = run_manifest.parent / 'raw' / 'oracle.json'
    payload = json.loads(oracle.read_text(encoding='utf-8'))
    payload['verified'] -= 1
    oracle.write_text(json.dumps(payload), encoding='utf-8')
    with pytest.raises(SystemExit, match='oracle artifact is incomplete'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_absent_or_untracked_lock_identity(tmp_path: Path) -> None:
    result = _result(tmp_path)
    result['doctor']['metadata']['uv_lock'] = {  # type: ignore[index]
        'path': None,
        'sha256': None,
        'size_bytes': None,
    }
    run_manifest = _run_manifest(tmp_path, result)
    with pytest.raises(SystemExit, match='requires a lock file identity'):
        _publish(run_manifest, tmp_path)


def test_publication_ignores_packages_no_block_loaded(tmp_path: Path) -> None:
    result = _result(tmp_path)
    result['doctor']['metadata']['packages']['unused'] = package_identity('unused')  # type: ignore[index]
    _publish(_run_manifest(tmp_path, result), tmp_path)


def test_publication_writes_only_the_deterministic_summary(tmp_path: Path) -> None:
    run_manifest = _run_manifest(tmp_path)
    current = tmp_path / 'current'
    current.mkdir()
    (current / 'summary.md').write_text('previous table', encoding='utf-8')

    published = _publish(run_manifest, tmp_path)
    first = published.read_bytes()
    assert published == current / 'summary.md'
    assert sorted(path.name for path in current.iterdir()) == ['summary.md']
    summary_md = first.decode('utf-8')
    assert '## Run environment' in summary_md
    assert 'Measured on test with Linux' in summary_md
    assert 'CPython 3.14, at commit `deadbeef`.' in summary_md
    assert 'Versions:' in summary_md and 'GEOS 3.13.0, PROJ 9.5.0.' in summary_md
    assert 'core' not in summary_md.split('###')[0]
    assert '<table>' not in summary_md
    assert '#### union_all: n-ary overlay input count' in summary_md

    assert _publish(run_manifest, tmp_path).read_bytes() == first


def test_cli_publishes_one_run_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_manifest = _run_manifest(tmp_path)
    current = tmp_path / 'current'
    monkeypatch.setattr(
        sys,
        'argv',
        ['bench.py', '--publish', str(run_manifest), '--current-dir', str(current)],
    )
    bench_driver.main()
    assert (current / 'summary.md').is_file()
