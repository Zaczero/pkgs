"""Release matrix invariants for the public benchmark evidence surface."""

from __future__ import annotations

import dataclasses
import importlib
import sys
from pathlib import Path

import pytest

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
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


def test_release_operation_counts_and_five_domains() -> None:
    assert len(RELEASE_OPERATIONS) == 38
    assert sum(len(op.rows) for op in RELEASE_OPERATIONS) == 76
    assert all(op.paired and op.competitor for op in RELEASE_OPERATIONS)
    assert [
        sum(op.domain == domain for op in RELEASE_OPERATIONS) for domain in DOMAIN_ORDER
    ] == [7, 16, 6, 4, 5]
    assert len(DOMAIN_ORDER) == 5
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
    assert len(records) == 38
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
    assert any('clip_by_rect' in row for row in rows)
    assert any('point_on_surface' in row for row in rows)
    intersects = next(op for op in RELEASE_OPERATIONS if 'intersects' in op.gometry)
    assert '12k' in intersects.workload and '12k' in intersects.gometry
    prepared = next(op for op in RELEASE_OPERATIONS if 'contains_xy' in op.gometry)
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
        for token in ('geodesic_area', 'geodesic_length')
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
    assert SMOKE.paired_blocks == 1
    assert SMOKE.paired_sampling_args == ('--warmups', '1', '--repetitions', '1')


def test_release_declares_exact_balanced_block_matrix() -> None:
    assert RELEASE.paired_blocks == 6
    assert RELEASE.paired_sampling_args == ('--warmups', '2', '--repetitions', '5')


def test_domain_order_is_contiguous() -> None:
    seen: list[str] = []
    for op in RELEASE_OPERATIONS:
        if not seen or seen[-1] != op.domain:
            seen.append(op.domain)
    assert tuple(seen) == DOMAIN_ORDER
