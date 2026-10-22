"""Release matrix invariants for the public benchmark evidence surface."""

from __future__ import annotations

import dataclasses
import importlib
import json
import os
import subprocess
import sys
import venv
from pathlib import Path

import pytest

_BENCHES = Path(__file__).resolve().parents[2] / 'benches'
GOMETRY_ROOT = _BENCHES.parent
_SUPPORT = _BENCHES / 'support'
_PYTHON = _BENCHES / 'python'
for _path in (_SUPPORT, _PYTHON):
    path_s = str(_path)
    if path_s not in sys.path:
        sys.path.insert(0, path_s)
_DRIVERS = _BENCHES / 'drivers'
if str(_DRIVERS) not in sys.path:
    sys.path.insert(0, str(_DRIVERS))

_bench_pairs = importlib.import_module('_bench_pairs')
_bench_registry = importlib.import_module('_bench_registry')
_bench_driver = importlib.import_module('bench')

DOMAIN_ORDER = _bench_registry.DOMAIN_ORDER
RELEASE = _bench_registry.RELEASE
RELEASE_OPERATIONS = _bench_registry.RELEASE_OPERATIONS
PUBLIC_API_SYMBOLS = _bench_registry.public_api_symbols()
SMOKE = _bench_registry.SMOKE
PAIR_WORKER = _bench_registry.PAIR_WORKER
expand_filter_to_pairs = _bench_registry.expand_filter_to_pairs
operation_for_row = _bench_registry.operation_for_row
validate_canonical_symbols = _bench_registry.validate_canonical_symbols
find_competitor = _bench_pairs.find_competitor
pair_units = _bench_pairs.pair_units


EXPECTED_PAIR_WORKER = 'bench_paired.py'


def test_release_operations_pair_comparators_in_every_domain() -> None:
    assert RELEASE_OPERATIONS
    assert all(op.paired and op.competitor for op in RELEASE_OPERATIONS)
    assert {op.domain for op in RELEASE_OPERATIONS} == set(DOMAIN_ORDER)
    assert all('Real-world' not in op.domain for op in RELEASE_OPERATIONS)


def test_release_operations_name_canonical_public_endpoints() -> None:
    validate_canonical_symbols()
    assert all(op.canonical_symbols for op in RELEASE_OPERATIONS)
    assert {
        'GeometryArray.area',
        'SpatialIndex.join',
        'SpatialIndex.candidates',
        'Groups.to_pairs',
    } <= PUBLIC_API_SYMBOLS
    assert all(
        symbol in PUBLIC_API_SYMBOLS
        for op in RELEASE_OPERATIONS
        for symbol in op.canonical_symbols
    )


def test_public_inventory_includes_type_only_exports() -> None:
    assert {
        'Cell',
        'Extremes',
        'Features',
        'PolygonizeResult',
    } <= PUBLIC_API_SYMBOLS
    assert {
        'Cell.children_count',
        'H3Cell.children_count',
        'Features.geometries',
        'Features.properties',
        'Features.ids',
        'Extremes.west',
        'Extremes.north',
        'PolygonizeResult.polygons',
    } <= PUBLIC_API_SYMBOLS
    assert 'GeometryArray._replace_at' not in PUBLIC_API_SYMBOLS


def test_manifest_serialization_retains_canonical_endpoints() -> None:
    records = _bench_driver._serialize_public_operations(
        RELEASE_OPERATIONS,
        {'competitors': RELEASE.rows('competitors')},
    )
    assert len(records) == len(RELEASE_OPERATIONS)
    assert records[0]['canonical_symbols'] == ['from_wkb']
    assert all(record['canonical_symbols'] for record in records)
    endpoint = next(
        record
        for record in records
        if record['gometry'] == 'gometry.union_all/1024_service_areas'
    )
    assert endpoint['scaling']['point'] == 1_024


def test_scaling_curves_are_sparse_ordered_and_single_dimension() -> None:
    _bench_registry.validate_scaling_operations()
    for curve in _bench_registry.SCALING_CURVES:
        assert len(curve.points) == 3
        assert [point.point for point in curve.points] == sorted(
            point.point for point in curve.points
        )
    # A reused endpoint is an existing release row, not a derived one.
    assert len(_bench_registry.SCALING_OPERATIONS) == sum(
        3 - (curve.reuse is not None) for curve in _bench_registry.SCALING_CURVES
    )
    assert len(_bench_registry.ALL_PUBLIC_OPERATIONS) == len(RELEASE_OPERATIONS) + len(
        _bench_registry.SCALING_OPERATIONS
    )


def test_scaling_curves_reuse_canonical_endpoint_rows() -> None:
    overlay = next(
        operation
        for operation in RELEASE_OPERATIONS
        if operation.gometry == 'gometry.union_all/1024_service_areas'
    )
    grid = next(
        operation
        for operation in RELEASE_OPERATIONS
        if operation.gometry == 'gometry.tile_cover.bbox/BR_z10_15340_tiles'
    )
    assert overlay.scaling is not None and overlay.scaling.point == 1_024
    assert grid.scaling is not None and grid.scaling.point == 15_340
    assert all('scaling=overlay-input-count' not in row for row in overlay.rows)
    assert all('scaling=grid-cover-output-count' not in row for row in grid.rows)


def test_invalid_canonical_endpoint_rejects_historically_valid_row() -> None:
    operation = RELEASE_OPERATIONS[0]
    mutated = dataclasses.replace(
        operation,
        canonical_symbols=('from_wkb.removed',),
    )
    with pytest.raises(ValueError, match='non-public API symbol'):
        validate_canonical_symbols((mutated,))


def test_release_uses_one_typed_paired_worker() -> None:
    assert PAIR_WORKER == EXPECTED_PAIR_WORKER


def test_release_paired_worker_imports_before_a_release_run() -> None:
    script = EXPECTED_PAIR_WORKER
    importlib.import_module(Path(script).stem)


def test_benchmark_entrypoints_prefer_the_workspace_benchroom_source() -> None:
    scripts = (
        _DRIVERS / 'bench.py',
        _PYTHON / EXPECTED_PAIR_WORKER,
        _PYTHON / 'bench_oracle.py',
        _DRIVERS / 'bench_ab.py',
        _BENCHES / 'support' / 'summarize_bench.py',
        GOMETRY_ROOT / 'tools' / 'docs' / 'benchmark_charts.py',
    )
    environment = os.environ.copy()
    environment.pop('PYTHONPATH', None)
    probe = (
        'import runpy, sys; from pathlib import Path; script = Path(sys.argv[1]); '
        'sys.path.insert(0, str(script.parent)); '
        'runpy.run_path(script, run_name="checkout_import_test"); '
        'import benchroom; print(Path(benchroom.__file__).resolve())'
    )
    expected = (
        GOMETRY_ROOT.parent / 'benchroom' / 'benchroom' / '__init__.py'
    ).resolve()

    for script in scripts:
        result = subprocess.run(
            [sys.executable, '-c', probe, str(script)],
            cwd=GOMETRY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert Path(result.stdout.strip()) == expected


def test_all_child_commands_use_the_selected_interpreter_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environment = tmp_path / 'isolated'
    venv.EnvBuilder(with_pip=False).create(environment)
    interpreter = str(environment / 'bin' / 'python')
    monkeypatch.setattr(sys, 'executable', interpreter)
    for script in (
        'benches/python/bench_oracle.py',
        'benches/python/bench_paired.py',
        'benches/cases/case_resource_tail.py',
    ):
        path = tmp_path / script
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            'import json, sys\n'
            'print(json.dumps({"executable": sys.executable, "prefix": sys.prefix}))\n'
        )
    cpu = min(os.sched_getaffinity(0))
    binding = {'path': str(tmp_path / 'wheel.whl'), 'receipt_sha256': '0' * 64}
    commands = [
        _bench_driver._oracle_command({'competitors': RELEASE_OPERATIONS[0].rows}),
        _bench_driver._paired_command(
            gometry=RELEASE_OPERATIONS[0].gometry,
            pair_id='pair',
            block_id='block',
            lead='ab',
            output=tmp_path / 'paired.json',
            profile=SMOKE,
            cpu=cpu,
            receipt_binding=binding,
        ),
        _bench_driver._resource_plans(
            output_dir=tmp_path,
            timestamp='run',
            cpu=cpu,
            receipt_binding=binding,
            package_root=tmp_path,
        )[0]['command'],
    ]
    observed = [
        json.loads(
            subprocess.run(
                command,
                cwd=tmp_path,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout
        )
        for command in commands
    ]
    assert observed == [{'executable': interpreter, 'prefix': str(environment)}] * 3


def test_all_public_rows_are_unique_and_fully_paired() -> None:
    names = [name for op in _bench_registry.ALL_PUBLIC_OPERATIONS for name in op.rows]
    assert len(names) == len(set(names))
    assert len({op.competitor for op in RELEASE_OPERATIONS}) == len(RELEASE_OPERATIONS)
    assert sorted(RELEASE.rows('competitors')) == sorted(names)
    assert _bench_registry.SUITES == ('competitors',)
    assert all(
        len(unit) == 2
        for unit in pair_units(RELEASE.rows('competitors'), suite='competitors')
    )


def test_revised_operations_and_labels() -> None:
    rows = {row for op in RELEASE_OPERATIONS for row in op.rows}
    assert not any('binary_view' in row for row in rows)
    assert not any('s2_cover' in row for row in rows)
    assert any('from_wkt' in row for row in rows)
    assert any('bounds/10k_projected_buildings' in row for row in rows)
    assert 'gometry.intersection/10k_realistic_linework_categories' in rows
    assert any('point_on_surface' in row for row in rows)
    intersects = next(op for op in RELEASE_OPERATIONS if 'intersects' in op.gometry)
    assert '12k' in intersects.workload and '12k' in intersects.gometry
    prepared = next(
        op for op in RELEASE_OPERATIONS if 'contains/coordinate' in op.gometry
    )
    assert prepared.competitor.startswith('shapely.contains_xy.prepared')
    destination = next(op for op in RELEASE_OPERATIONS if 'destination' in op.gometry)
    assert 'Shapely' in destination.competitor and 'GeoPandas' in destination.competitor
    geohash = next(op for op in RELEASE_OPERATIONS if 'geohash' in op.gometry)
    assert 'batched' not in geohash.footnotes
    country_io = next(op for op in RELEASE_OPERATIONS if 'from_geojson' in op.gometry)
    assert country_io.domain == 'Array construction & I/O'
    assert all(
        next(op for op in RELEASE_OPERATIONS if token in op.gometry).domain
        == 'CRS & geodesy'
        for token in ('chart_edge_area', 'chart_edge_length')
    )


def test_public_pairing_via_manifest() -> None:
    available = set(RELEASE.rows('competitors'))
    for op in RELEASE_OPERATIONS:
        assert find_competitor(op.gometry, available) == op.competitor
        assert operation_for_row(op.gometry) is op
        assert operation_for_row(op.competitor) is op


def test_filter_selects_both_pair_members() -> None:
    op = RELEASE_OPERATIONS[0]
    assert expand_filter_to_pairs({op.gometry}) == set(op.rows)
    assert expand_filter_to_pairs({op.competitor}) == set(op.rows)


def test_smoke_shares_operation_tuple() -> None:
    assert SMOKE.operations == RELEASE.operations
    assert SMOKE.scaling_operations == ()
    assert SMOKE.rows('competitors') == tuple(
        row for op in RELEASE_OPERATIONS for row in op.rows
    )
    assert SMOKE.sampling_policy is None
    assert SMOKE.initial_paired_blocks == 1
    assert SMOKE.paired_sampling_args == ('--warmups', '1', '--repetitions', '1')


def test_release_uses_the_shared_six_then_twelve_block_policy() -> None:
    assert RELEASE.sampling_policy is not None
    assert RELEASE.sampling_policy.looks == (6, 12)
    assert RELEASE.initial_paired_blocks == 6
    assert RELEASE.maximum_paired_blocks == 12
    assert RELEASE.paired_sampling_args == ('--warmups', '2', '--repetitions', '5')


def test_release_plans_the_first_six_then_only_the_second_six(
    tmp_path: Path,
) -> None:
    operation = RELEASE_OPERATIONS[0]
    common = {
        'profile_name': 'release',
        'profile': RELEASE,
        'selected': {'competitors': operation.rows},
        'output_dir': tmp_path,
        'timestamp': 'run',
        'cpu': 0,
        'receipt_binding': {'path': '/receipt', 'receipt_sha256': '0' * 64},
        'package_root': tmp_path,
    }
    first, _ = _bench_driver._benchmark_plans(
        **common,
        block_numbers={operation.gometry: tuple(range(1, 7))},
    )
    extension, _ = _bench_driver._benchmark_plans(
        **common,
        block_numbers={operation.gometry: tuple(range(7, 13))},
    )
    assert [plan['block_id'] for plan in first] == [
        f'competitors-pair-01-block-{number:02d}-{"ab" if number % 2 else "ba"}'
        for number in range(1, 7)
    ]
    assert [plan['block_id'] for plan in extension] == [
        f'competitors-pair-01-block-{number:02d}-{"ab" if number % 2 else "ba"}'
        for number in range(7, 13)
    ]


def test_extension_admission_protects_required_work_and_finish_reserve() -> None:
    admitted, rejected = _bench_driver._admit_extensions(
        [
            {'operation': 'op-a', 'estimated_seconds': 20.0},
            {'operation': 'op-b', 'estimated_seconds': 30.0},
        ],
        remaining_seconds=89.0,
        required_seconds=40.0,
        reserve_seconds=10.0,
    )
    assert admitted == {'op-a': 20.0}
    assert rejected == {'op-b': 'budget-limited'}


def test_plan_only_records_the_allowed_maximum_without_claiming_measurements(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _bench_driver.benchmark_runtime,
        'load_subject',
        lambda _path=None: {
            'path': str(tmp_path / 'receipt.json'),
            'receipt_sha256': '0' * 64,
            'source': {},
        },
    )
    monkeypatch.setattr(
        _bench_driver.benchmark_runtime,
        'package_root_for_subject',
        lambda _subject, _explicit=None: tmp_path,
    )
    monkeypatch.setattr(_bench_driver, '_validate_oracle_builders', lambda _: None)
    monkeypatch.setattr(
        _bench_driver,
        'select_benchmark_cpu',
        lambda _cpu: (0, 'explicit-kernel-isolated'),
    )
    monkeypatch.setattr(
        _bench_driver,
        'collect',
        lambda **_: {
            'metadata': {'cpu_frequency_state': {}},
            'hard_failures': [],
            'warnings': [],
        },
    )
    monkeypatch.setattr(
        _bench_driver,
        '_run',
        lambda *_args, **_kwargs: pytest.fail('plan-only must not run commands'),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'bench.py',
            '--profile',
            'release',
            '--plan-only',
            '--filter',
            RELEASE_OPERATIONS[0].gometry,
            '--output-dir',
            str(tmp_path / 'planned'),
        ],
    )

    _bench_driver.main()

    manifest_path = next((tmp_path / 'planned').glob('*/run.json'))
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    operation = RELEASE_OPERATIONS[0].gometry
    assert manifest['sampling']['mode'] == 'plan-only'
    assert len(manifest['sampling']['maximum_plan'][operation]['block_ids']) == 12
    assert 'actual_plan' not in manifest['sampling']
    assert manifest['commands'] == []
    assert manifest['commands_complete'] is False
    assert manifest['sampling_plan_artifact'] == 'sampling-plan.json'


def test_domain_order_is_contiguous() -> None:
    seen: list[str] = []
    for op in RELEASE_OPERATIONS:
        if not seen or seen[-1] != op.domain:
            seen.append(op.domain)
    assert tuple(seen) == DOMAIN_ORDER
