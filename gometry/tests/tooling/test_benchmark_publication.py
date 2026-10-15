"""Publication transaction rejection, atomicity, and deterministic output."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_BENCHES = _ROOT / 'benches'
for _path in (_BENCHES / 'support', _BENCHES / 'drivers'):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import bench as bench_driver  # noqa: E402
import benchmark_runtime  # noqa: E402
from _bench_registry import ALL_PUBLIC_OPERATIONS, scaling_record  # noqa: E402
from benchmark_observation import make_observation  # noqa: E402

from tests.tooling._bench_support import (  # noqa: E402
    HASH,
    file_identity,
    package_identity,
    producer,
)

_CURRENT_COMMIT = 'a' * 40


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
    provenance_a = producer(
        operation.gometry,
        'A',
        lock_path,
        source_commit=_CURRENT_COMMIT,
        harness_commit=_CURRENT_COMMIT,
    )
    provenance_b = producer(
        operation.competitor, 'B', lock_path, harness_commit=_CURRENT_COMMIT
    )
    observation_a['producer_identity'] = provenance_a
    observation_b['producer_identity'] = provenance_b
    block_id = (
        f'competitors-pair-{ALL_PUBLIC_OPERATIONS.index(operation) + 1:02d}-'
        f'block-{index + 1:02d}-{lead}'
    )
    process_pid = 2000 + ALL_PUBLIC_OPERATIONS.index(operation) * 10 + index
    invocation_id = f'{block_id}-invocation'
    case_processes = {}
    for side, row, provenance, observation in (
        ('A', operation.gometry, provenance_a, observation_a),
        ('B', operation.competitor, provenance_b, observation_b),
    ):
        case_process = {
            'pid': process_pid,
            'invocation_id': invocation_id,
            'row': row,
            'identity_sha256': benchmark_runtime.identity_sha256(provenance),
        }
        observation['case_process'] = case_process
        case_processes[side] = case_process
    return {
        'schema_version': 1,
        'artifact_type': 'gometry-paired-block',
        'profile': 'release',
        'run_id': 'run-1-release',
        'block_id': block_id,
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
        'process': {
            'pid': process_pid,
            'invocation_id': invocation_id,
            'platform': 'test',
            'executable': '/python',
        },
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
                'case_process': case_processes['A'],
                'observation': observation_a,
            },
            'B': {
                'row': operation.competitor,
                'started_monotonic_ns': 12 if lead == 'ab' else 2,
                'ended_monotonic_ns': 13 if lead == 'ab' else 3,
                'provenance_before': provenance_b,
                'provenance_after': provenance_b,
                'case_process': case_processes['B'],
                'observation': observation_b,
            },
        },
    }


def _build_result(tmp_path: Path) -> dict[str, object]:
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
        'harness_commit': _CURRENT_COMMIT,
        'source_inputs_sha256': HASH,
        'native_source_inputs_sha256': HASH,
        'python_executable': '/python',
        'python_prefix': '/venv',
        'python': '3.14',
        'python_version': '3.14 test build',
        'python_build': ['build', 'date'],
        'python_implementation': 'CPython',
        'python_debug': False,
        'python_cache_tag': 'cpython-314',
        'python_ext_suffix': '.cpython-314.so',
        'python_gil_disabled': False,
        'python_gil_enabled': True,
        'extension': {
            'path': '/checkout/site-packages/gometry/_lib.cpython-314.so',
            'sha256': HASH,
            'size_bytes': 1,
            'build_info': {
                'schema_version': 1,
                'source_inputs_format': 'sha256-path-nul-content-nul',
                'source_inputs_sha256': HASH,
                'native_source_inputs_sha256': HASH,
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
        'row_counts': {
            'competitors': sum(
                len(operation.rows) for operation in ALL_PUBLIC_OPERATIONS
            )
        },
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


@pytest.fixture(scope='module')
def publication_baseline(tmp_path_factory: pytest.TempPathFactory) -> str:
    root = tmp_path_factory.mktemp('publication-baseline')
    return json.dumps(_build_result(root))


def _result(publication_baseline: str) -> dict[str, object]:
    return json.loads(publication_baseline)


def _synthetic_subject() -> dict[str, object]:
    identity = producer('gometry.op/fixture', 'A')
    binding = identity['runtime_binding']
    return {
        'kind': 'release',
        'path': binding['path'],
        'receipt_sha256': binding['sha256'],
        'source': {
            'commit': _CURRENT_COMMIT,
            'version': '1',
            'source_inputs_sha256': identity['source_inputs_sha256'],
            'native_source_inputs_sha256': HASH,
            'python_members': binding['python_members'],
        },
        'wheels': [
            {
                'filename': binding['wheel']['filename'],
                'sha256': binding['wheel']['archive']['sha256'],
                'extension_member': binding['wheel']['extension_member'],
                'extension_sha256': binding['wheel']['extension_sha256'],
            }
        ],
    }


@pytest.fixture(autouse=True)
def prevalidated_receipt_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    subject = _synthetic_subject()
    monkeypatch.setattr(
        bench_driver, '_current_checkout_commit', lambda: _CURRENT_COMMIT
    )

    def read_subject(path: Path) -> dict[str, object]:
        if str(path.resolve()) != subject['path']:
            raise AssertionError(f'unexpected synthetic receipt path: {path}')
        return subject

    monkeypatch.setattr(
        bench_driver.artifact_release,
        'validate_benchmark_receipt',
        read_subject,
    )
    return subject


def _run_manifest(
    tmp_path: Path,
    publication_baseline: str,
    result: dict[str, object] | None = None,
) -> Path:
    result = _result(publication_baseline) if result is None else result
    run_directory = tmp_path / 'run'
    raw_directory = run_directory / 'raw'
    raw_directory.mkdir(parents=True)
    artifacts = []
    for source in result.pop('source_artifacts'):
        artifact = raw_directory / Path(str(source)).name
        os.link(source, artifact)
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


def _replace_artifact(path: Path, content: str) -> None:
    temporary = path.with_name(f'.{path.name}.tmp')
    temporary.write_text(content, encoding='utf-8')
    temporary.replace(path)


def _mutate_first_block(run_manifest: Path, mutate) -> None:
    for raw in sorted(run_manifest.parent.joinpath('raw').glob('block-*.json')):
        payload = json.loads(raw.read_text(encoding='utf-8'))
        mutate(payload)
        for side in ('A', 'B'):
            identity = payload['arms'][side]['provenance_before']
            identity['runtime_binding']['extension'] = identity['extension']
            case_process = payload['arms'][side]['case_process']
            case_process['identity_sha256'] = benchmark_runtime.identity_sha256(
                identity
            )
            payload['arms'][side]['observation']['case_process'] = case_process
        _replace_artifact(raw, json.dumps(payload))
        return
    raise AssertionError('fixture has no paired block artifact')


def _publish(run_manifest: Path, tmp_path: Path) -> Path:
    return bench_driver.publish_run(run_manifest, current_dir=tmp_path / 'current')


def test_release_measurement_cannot_publish_or_mutate_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prevalidated_receipt_boundary: dict[str, object],
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
    monkeypatch.setattr(
        bench_driver.benchmark_runtime,
        'load_subject',
        lambda _path=None: prevalidated_receipt_boundary,
    )
    monkeypatch.setattr(
        bench_driver.benchmark_runtime,
        'package_root_for_subject',
        lambda _subject, _explicit=None: Path('/checkout/site-packages'),
    )
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
    tmp_path: Path, publication_baseline: str
) -> None:
    result = _result(publication_baseline)
    result['filter'] = 'gometry.points/10k_numpy_xy'
    run_manifest = _run_manifest(tmp_path, publication_baseline, result)
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
    tmp_path: Path,
    publication_baseline: str,
    changes: dict[str, object],
    message: str,
) -> None:
    result = _result(publication_baseline)
    result.update(changes)
    result['publishable'] = False
    run_manifest = _run_manifest(tmp_path, publication_baseline, result)
    with pytest.raises(SystemExit, match=message):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_missing_artifact_and_metadata(
    tmp_path: Path, publication_baseline: str
) -> None:
    result = _result(publication_baseline)
    run_manifest = _run_manifest(tmp_path, publication_baseline, result)
    next(run_manifest.parent.joinpath('raw').glob('block-*.json')).unlink()
    with pytest.raises(SystemExit, match='references a missing artifact'):
        _publish(run_manifest, tmp_path)
    incomplete_root = tmp_path / 'incomplete'
    incomplete_root.mkdir()
    incomplete = _result(publication_baseline)
    del incomplete['doctor']['metadata']['uv_lock']  # type: ignore[index]
    incomplete_manifest = _run_manifest(
        incomplete_root, publication_baseline, incomplete
    )
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
    tmp_path: Path, publication_baseline: str, mutation, message: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)
    _mutate_first_block(run_manifest, mutation)
    with pytest.raises(SystemExit, match=message):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_comparator_module_mutation(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

    def mutate(payload: dict[str, object]) -> None:
        identity = payload['arms']['B']['provenance_before']
        identity['implementation']['module']['sha256'] = '1' * 64
        payload['arms']['B']['provenance_after'] = identity
        payload['arms']['B']['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='module identity mismatch'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_native_library_mutation(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

    def mutate(payload: dict[str, object]) -> None:
        identity = payload['arms']['A']['provenance_before']
        identity['implementation']['native_libraries'][0]['sha256'] = '1' * 64
        payload['arms']['A']['provenance_after'] = identity
        payload['arms']['A']['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(
        SystemExit, match='source receipt extension differs from arm native identity'
    ):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_comparator_native_library_mutation(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

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


def test_publication_rejects_dependency_lock_mutation(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

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
    tmp_path: Path,
    publication_baseline: str,
    mutation: str,
    message: str,
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

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


def test_publication_rejects_effective_build_fact_mutation(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

    def mutate(payload: dict[str, object]) -> None:
        for side in ('A', 'B'):
            identity = payload['arms'][side]['provenance_before']
            identity['extension']['build_info']['opt_level'] = '0'
            payload['arms'][side]['provenance_after'] = identity
            payload['arms'][side]['observation']['producer_identity'] = identity

    _mutate_first_block(run_manifest, mutate)
    with pytest.raises(SystemExit, match='effective build identity'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_extra_paired_artifact(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)
    result = json.loads(run_manifest.read_text(encoding='utf-8'))
    source = next(run_manifest.parent.joinpath('raw').glob('block-*.json'))
    extra = run_manifest.parent / 'raw' / 'extra-paired.json'
    shutil.copy2(source, extra)
    result['artifacts'].append(f'raw/{extra.name}')
    run_manifest.write_text(json.dumps(result), encoding='utf-8')
    with pytest.raises(SystemExit, match='duplicate paired matrix identity'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_mismatched_external_oracle_artifact(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)
    oracle = run_manifest.parent / 'raw' / 'oracle.json'
    payload = json.loads(oracle.read_text(encoding='utf-8'))
    payload['verified'] -= 1
    _replace_artifact(oracle, json.dumps(payload))
    with pytest.raises(SystemExit, match='oracle artifact is incomplete'):
        _publish(run_manifest, tmp_path)


def test_publication_rejects_stale_harness_commit(
    tmp_path: Path, publication_baseline: str
) -> None:
    result = _result(publication_baseline)
    result['doctor']['metadata']['harness_commit'] = 'b' * 40  # type: ignore[index]

    with pytest.raises(SystemExit, match='harness is not at the current checkout'):
        _publish(_run_manifest(tmp_path, publication_baseline, result), tmp_path)

    assert not (tmp_path / 'current' / 'summary.md').exists()


@pytest.mark.parametrize(
    ('producer_commit', 'clear_attribution'),
    [
        pytest.param('b' * 40, False, id='older-source'),
        pytest.param(None, True, id='unattributed-source'),
    ],
)
def test_publication_rejects_noncurrent_gometry_producer(
    tmp_path: Path,
    publication_baseline: str,
    producer_commit: str | None,
    clear_attribution: bool,
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)

    def set_producer_commit(payload: dict[str, object]) -> None:
        arms = payload['arms']
        assert isinstance(arms, dict)
        for arm in arms.values():
            assert isinstance(arm, dict)
            identity = arm['provenance_before']
            assert isinstance(identity, dict)
            implementation = identity['implementation']
            assert isinstance(implementation, dict)
            if implementation['runtime'] == 'gometry':
                implementation['source_commit'] = producer_commit
                if clear_attribution:
                    implementation['source_receipt'] = None
                    implementation['source_wheel'] = None
            arm['provenance_after'] = identity
            observation = arm['observation']
            assert isinstance(observation, dict)
            observation['producer_identity'] = identity

    _mutate_first_block(run_manifest, set_producer_commit)

    with pytest.raises(
        SystemExit, match='producer is not at the current checkout commit'
    ):
        _publish(run_manifest, tmp_path)

    assert not (tmp_path / 'current' / 'summary.md').exists()


def test_publication_rejects_absent_or_untracked_lock_identity(
    tmp_path: Path, publication_baseline: str
) -> None:
    result = _result(publication_baseline)
    result['doctor']['metadata']['uv_lock'] = {  # type: ignore[index]
        'path': None,
        'sha256': None,
        'size_bytes': None,
    }
    run_manifest = _run_manifest(tmp_path, publication_baseline, result)
    with pytest.raises(SystemExit, match='requires a lock file identity'):
        _publish(run_manifest, tmp_path)


def test_publication_ignores_packages_no_block_loaded(
    tmp_path: Path, publication_baseline: str
) -> None:
    result = _result(publication_baseline)
    result['doctor']['metadata']['packages']['unused'] = package_identity('unused')  # type: ignore[index]
    _publish(_run_manifest(tmp_path, publication_baseline, result), tmp_path)


def test_publication_writes_only_the_deterministic_summary(
    tmp_path: Path, publication_baseline: str
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)
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
    assert (
        f'CPython 3.14. Benchmark harness commit: `{_CURRENT_COMMIT[:8]}`.'
        in summary_md
    )
    assert 'Versions:' in summary_md and 'GEOS 3.13.0, PROJ 9.5.0.' in summary_md
    assert 'core' not in summary_md.split('###')[0]
    assert '<table>' not in summary_md
    assert '#### union_all: n-ary overlay input count' in summary_md

    assert _publish(run_manifest, tmp_path).read_bytes() == first


def test_cli_publishes_one_run_manifest(
    tmp_path: Path,
    publication_baseline: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_manifest = _run_manifest(tmp_path, publication_baseline)
    current = tmp_path / 'current'
    monkeypatch.setattr(
        sys,
        'argv',
        ['bench.py', '--publish', str(run_manifest), '--current-dir', str(current)],
    )
    bench_driver.main()
    assert (current / 'summary.md').is_file()
