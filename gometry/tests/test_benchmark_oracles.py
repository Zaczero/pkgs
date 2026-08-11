"""Equivalence helpers for the untimed cross-library oracle (Lane 1)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import numpy as np
import pytest

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
_SUPPORT = _BENCHES / 'support'
_PYTHON = _BENCHES / 'python'
for _path in (_SUPPORT, _PYTHON):
    path_s = str(_path)
    if path_s not in sys.path:
        sys.path.insert(0, path_s)

_bench_oracles = importlib.import_module('_bench_oracles')
_bench_registry = importlib.import_module('_bench_registry')
# Register the 32 public builders (side-effect import).
importlib.import_module('_bench_public_cases')
_bench_oracle = importlib.import_module('bench_oracle')

PUBLIC_CASE_BUILDERS = _bench_oracles.PUBLIC_CASE_BUILDERS
OracleContext = _bench_oracles.OracleContext
OracleMismatch = _bench_oracles.OracleMismatch
exact_coordinates = _bench_oracles.exact_coordinates
exact_mask = _bench_oracles.exact_mask
geometry_equivalent = _bench_oracles.geometry_equivalent
metric_allclose = _bench_oracles.metric_allclose
non_unique_points_contract = _bench_oracles.non_unique_points_contract
normalized_index_pairs = _bench_oracles.normalized_index_pairs
normalized_tile_set = _bench_oracles.normalized_tile_set
normalized_uint64_set = _bench_oracles.normalized_uint64_set
rowwise_geometry_exact = _bench_oracles.rowwise_geometry_exact
RELEASE_OPERATIONS = _bench_registry.RELEASE_OPERATIONS
validate_builders = _bench_oracle.validate_builders


def _ctx(
    kind: str = 'test',
    unit: str | None = None,
    *,
    index: int = 0,
) -> OracleContext:
    return OracleContext(
        operation=RELEASE_OPERATIONS[index],
        kind=kind,
        unit=unit,
    )


def test_exact_mask_pass() -> None:
    a = np.array([True, False, True], dtype=bool)
    b = np.array([True, False, True], dtype=bool)
    exact_mask(a, b, _ctx('exact_mask'))


def test_exact_mask_fail_value() -> None:
    a = np.array([True, False], dtype=bool)
    b = np.array([True, True], dtype=bool)
    with pytest.raises(OracleMismatch, match='exact_mask'):
        exact_mask(a, b, _ctx('exact_mask'))


def test_exact_mask_fail_dtype() -> None:
    a = np.array([1, 0], dtype=np.int8)
    b = np.array([True, False], dtype=bool)
    with pytest.raises(OracleMismatch, match='boolean'):
        exact_mask(a, b, _ctx('exact_mask'))


def test_exact_coordinates_pass() -> None:
    a = np.array([[1.0, 2.0], [3.0, 4.0]])
    b = a + 1e-12
    exact_coordinates(a, b, _ctx('exact_coordinates', unit='m'))


def test_exact_coordinates_requires_unit() -> None:
    a = np.array([1.0, 2.0])
    with pytest.raises(OracleMismatch, match='unit'):
        exact_coordinates(a, a, _ctx('exact_coordinates', unit=None))


def test_exact_coordinates_fail() -> None:
    a = np.array([1.0, 2.0])
    b = np.array([1.0, 99.0])
    with pytest.raises(OracleMismatch, match='exact_coordinates'):
        exact_coordinates(a, b, _ctx('exact_coordinates', unit='m'))


def test_metric_allclose_pass() -> None:
    a = np.array([100.0, 200.5])
    b = np.array([100.0, 200.5 + 1e-10])
    metric_allclose(a, b, _ctx('metric', unit='m'))


def test_metric_allclose_refuses_empty_unit() -> None:
    a = np.array([1.0])
    with pytest.raises(OracleMismatch, match='unit'):
        metric_allclose(a, a, _ctx('metric', unit=''))
    with pytest.raises(OracleMismatch, match='unit'):
        metric_allclose(a, a, _ctx('metric', unit=None))


def test_metric_allclose_square_degree_vs_square_metre_fails() -> None:
    """Load-bearing: planar square-degree area must not pass as m2.

    This is the bug class that shipped when gometry geodesic m2 was paired
    with Shapely planar square degrees without a unit-aware oracle.
    """
    # Rough Brazil-scale: ~1.5e14 m2 vs a planar degree2 figure ~80-100
    square_metres = np.array([155_374_338_084_110.78])
    square_degrees = np.array([85.5])  # planar lon/lat "area"
    with pytest.raises(OracleMismatch, match='metric_allclose'):
        metric_allclose(
            square_metres,
            square_degrees,
            _ctx('metric', unit='m2_ellipsoidal'),
        )


def test_metric_allclose_close_m2_pass() -> None:
    a = np.array([1_000_000.0])
    b = np.array([1_000_000.0 + 1e-7])
    metric_allclose(a, b, _ctx('metric', unit='m2'))


def test_normalized_uint64_set_pass() -> None:
    a = np.array([3, 1, 2], dtype=np.uint64)
    b = np.array([2, 3, 1], dtype=np.uint64)
    normalized_uint64_set(a, b, _ctx('uint64'))


def test_normalized_uint64_set_fail_content() -> None:
    a = np.array([1, 2, 3], dtype=np.uint64)
    b = np.array([1, 2, 4], dtype=np.uint64)
    with pytest.raises(OracleMismatch, match='normalized_uint64_set'):
        normalized_uint64_set(a, b, _ctx('uint64'))


def test_normalized_uint64_set_fail_duplicates() -> None:
    a = np.array([1, 1, 2], dtype=np.uint64)
    b = np.array([1, 2], dtype=np.uint64)
    with pytest.raises(OracleMismatch, match='duplicate'):
        normalized_uint64_set(a, b, _ctx('uint64'))


def test_normalized_tile_set_pass() -> None:
    a = [(10, 2, 3), (10, 1, 1)]
    b = [(10, 1, 1), (10, 2, 3)]
    normalized_tile_set(a, b, _ctx('tiles'))


def test_normalized_tile_set_fail() -> None:
    a = [(10, 1, 1)]
    b = [(10, 1, 2)]
    with pytest.raises(OracleMismatch, match='normalized_tile_set'):
        normalized_tile_set(a, b, _ctx('tiles'))


def test_normalized_index_pairs_pass() -> None:
    left = (np.array([1, 0, 2]), np.array([10, 5, 7]))
    right = (np.array([2, 1, 0]), np.array([7, 10, 5]))
    normalized_index_pairs(left, right, _ctx('pairs'))


def test_normalized_index_pairs_fail() -> None:
    left = (np.array([0]), np.array([1]))
    right = (np.array([0]), np.array([2]))
    with pytest.raises(OracleMismatch, match='normalized_index_pairs'):
        normalized_index_pairs(left, right, _ctx('pairs'))


def test_rowwise_geometry_exact_pass() -> None:
    import gometry as gm

    left = gm.GeometryArray([gm.Point(1.0, 2.0), gm.Point(3.0, 4.0)])
    right = gm.GeometryArray([gm.Point(1.0, 2.0), gm.Point(3.0, 4.0)])
    rowwise_geometry_exact(left, right, _ctx('rowwise'))


def test_rowwise_geometry_exact_fail_coords() -> None:
    import gometry as gm

    left = gm.GeometryArray([gm.Point(1.0, 2.0)])
    right = gm.GeometryArray([gm.Point(1.0, 9.0)])
    with pytest.raises(OracleMismatch, match='rowwise_geometry_exact'):
        rowwise_geometry_exact(left, right, _ctx('rowwise'))


def test_geometry_equivalent_equals_pass() -> None:
    import gometry as gm

    a = gm.box(0, 0, 1, 1)
    b = gm.box(0, 0, 1, 1)
    geometry_equivalent(a, b, _ctx('geom_eq', unit='symdiff'))


def test_geometry_equivalent_fail_disjoint() -> None:
    import gometry as gm

    a = gm.box(0, 0, 1, 1)
    b = gm.box(10, 10, 11, 11)
    with pytest.raises(OracleMismatch, match='geometry_equivalent'):
        geometry_equivalent(a, b, _ctx('geom_eq', unit='symdiff'))


def test_non_unique_points_contract_pass() -> None:
    import gometry as gm

    sources = [gm.box(0, 0, 2, 2), gm.box(5, 5, 8, 8)]
    points = [gm.Point(1.0, 1.0), gm.Point(6.0, 6.0)]
    non_unique_points_contract(
        points,
        points,
        _ctx('pts'),
        sources=sources,
    )


def test_non_unique_points_contract_fail_not_point() -> None:
    import gometry as gm

    with pytest.raises(OracleMismatch, match='nonempty Point'):
        non_unique_points_contract(
            [gm.box(0, 0, 1, 1)],
            [gm.Point(0.5, 0.5)],
            _ctx('pts'),
        )


def test_non_unique_points_contract_fail_not_covered() -> None:
    import gometry as gm

    sources = [gm.box(0, 0, 1, 1)]
    points = [gm.Point(50.0, 50.0)]
    with pytest.raises(OracleMismatch, match='not covered'):
        non_unique_points_contract(
            points,
            points,
            _ctx('pts'),
            sources=sources,
        )


def test_public_case_builders_is_mapping() -> None:
    assert isinstance(PUBLIC_CASE_BUILDERS, dict)
    public = {op.gometry for op in RELEASE_OPERATIONS}
    assert set(PUBLIC_CASE_BUILDERS).issubset(public)


def test_public_case_builders_exact_bijection() -> None:
    """Every RELEASE op has exactly one builder; no orphans (fail-closed)."""
    public = {op.gometry for op in RELEASE_OPERATIONS}
    builders = set(PUBLIC_CASE_BUILDERS)
    assert builders == public, (
        f'missing={sorted(public - builders)} orphans={sorted(builders - public)}'
    )


def test_validate_builders_rejects_missing_selected() -> None:
    """Removing any selected builder must fail before verification/timing."""
    ops = list(RELEASE_OPERATIONS)
    incomplete = dict(PUBLIC_CASE_BUILDERS)
    removed = ops[0].gometry
    del incomplete[removed]
    with pytest.raises(SystemExit, match='missing builders'):
        validate_builders(ops, incomplete)


def test_validate_builders_rejects_orphan() -> None:
    bloated = dict(PUBLIC_CASE_BUILDERS)
    bloated['gometry.not_a_real_op'] = lambda: None  # type: ignore[assignment]
    with pytest.raises(SystemExit, match='outside RELEASE_OPERATIONS'):
        validate_builders(list(RELEASE_OPERATIONS), bloated)


def test_oracle_mismatch_message_is_rich() -> None:
    ctx = _ctx('kind_token', unit='m2')
    with pytest.raises(OracleMismatch) as ei:
        metric_allclose(
            np.array([1.0]),
            np.array([2.0]),
            ctx,
        )
    msg = str(ei.value)
    assert 'gometry=' in msg
    assert 'competitor=' in msg
    assert 'kind=' in msg
    assert 'unit=' in msg
    assert 'm2' in msg
