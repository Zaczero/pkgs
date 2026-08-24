"""Release matrix invariants for the public benchmark evidence surface."""

from __future__ import annotations

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

_bench_pairs = importlib.import_module('_bench_pairs')
_bench_registry = importlib.import_module('_bench_registry')

DOMAIN_ORDER = _bench_registry.DOMAIN_ORDER
RELEASE = _bench_registry.RELEASE
RELEASE_OPERATIONS = _bench_registry.RELEASE_OPERATIONS
SMOKE = _bench_registry.SMOKE
SCRIPTS = _bench_registry.SCRIPTS
expand_filter_to_pairs = _bench_registry.expand_filter_to_pairs
operation_for_row = _bench_registry.operation_for_row
find_competitor = _bench_pairs.find_competitor
pair_units = _bench_pairs.pair_units


EXPECTED_RELEASE_SCRIPTS = {
    'competitors': 'bench_competitors.py',
}


def test_direct_worker_rejects_explicitly_empty_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = importlib.import_module('_bench_config')
    monkeypatch.setenv('GOMETRY_BENCH_FILTER', ' , ')
    with pytest.raises(SystemExit, match='must select at least one benchmark'):
        config.selected_benchmarks('competitors')


def test_release_operation_counts_and_five_domains() -> None:
    assert len(RELEASE_OPERATIONS) == 37
    assert sum(len(op.rows) for op in RELEASE_OPERATIONS) == 74
    assert all(op.paired and op.competitor for op in RELEASE_OPERATIONS)
    assert [
        sum(op.domain == domain for op in RELEASE_OPERATIONS)
        for domain in DOMAIN_ORDER
    ] == [7, 15, 6, 4, 5]
    assert len(DOMAIN_ORDER) == 5
    assert all('Real-world' not in op.domain for op in RELEASE_OPERATIONS)


def test_release_script_inventory_is_complete() -> None:
    assert SCRIPTS == EXPECTED_RELEASE_SCRIPTS


@pytest.mark.parametrize('script', EXPECTED_RELEASE_SCRIPTS.values(), ids=EXPECTED_RELEASE_SCRIPTS)
def test_release_scripts_import_before_a_release_run(script: str) -> None:
    importlib.import_module(Path(script).stem)


def test_all_public_rows_are_unique_and_fully_paired() -> None:
    names = [name for op in RELEASE_OPERATIONS for name in op.rows]
    assert len(names) == len(set(names)) == 74
    assert len({op.competitor for op in RELEASE_OPERATIONS}) == 37
    assert len(RELEASE.rows('competitors')) == 74
    assert _bench_registry.SUITES == ('competitors',)
    assert all(len(unit) == 2 for unit in pair_units(RELEASE.rows('competitors'), suite='competitors'))


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
        next(op for op in RELEASE_OPERATIONS if token in op.gometry).domain == 'CRS & geodesy'
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
    assert SMOKE.rows('competitors') == RELEASE.rows('competitors')
    assert SMOKE.sampling_args == ('--debug-single-value',)
    assert SMOKE.paired_sampling_args == ('--debug-single-value',)


def test_domain_order_is_contiguous() -> None:
    seen: list[str] = []
    for op in RELEASE_OPERATIONS:
        if not seen or seen[-1] != op.domain:
            seen.append(op.domain)
    assert tuple(seen) == DOMAIN_ORDER
