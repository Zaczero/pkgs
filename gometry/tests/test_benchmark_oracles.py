"""Test oracle mechanics and registry completeness without release workloads.

The deliberate ``benches/python/bench_oracle.py --all`` command owns
production-scale workload equivalence; ordinary pytest covers its registry,
dispatch, and failure contracts with local cases.
"""

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
# Register the complete public builder map (side-effect import).
importlib.import_module('_bench_public_cases')
importlib.import_module('_bench_scaling_cases')
_bench_oracle = importlib.import_module('bench_oracle')

PUBLIC_CASE_BUILDERS = _bench_oracles.PUBLIC_CASE_BUILDERS
PUBLIC_SCALING_CASE_BUILDERS = _bench_oracles.PUBLIC_SCALING_CASE_BUILDERS
OracleContext = _bench_oracles.OracleContext
OracleMismatch = _bench_oracles.OracleMismatch
exact_coordinates = _bench_oracles.exact_coordinates
exact_mask = _bench_oracles.exact_mask
areal_result_equivalent = _bench_oracles.areal_result_equivalent
linework_result_equivalent = _bench_oracles.linework_result_equivalent
metric_allclose = _bench_oracles.metric_allclose
non_unique_points_contract = _bench_oracles.non_unique_points_contract
normalized_index_pairs = _bench_oracles.normalized_index_pairs
normalized_tile_set = _bench_oracles.normalized_tile_set
normalized_uint64_set = _bench_oracles.normalized_uint64_set
parsed_geometry_exact = _bench_oracles.parsed_geometry_exact
RELEASE_OPERATIONS = _bench_registry.RELEASE_OPERATIONS
SCALING_OPERATIONS = _bench_registry.SCALING_OPERATIONS
ALL_PUBLIC_OPERATIONS = _bench_registry.ALL_PUBLIC_OPERATIONS
validate_builders = _bench_oracle.validate_builders
run_oracle = _bench_oracle.run_oracle


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


def test_parsed_geometry_exact_passes_recursive_structure() -> None:
    import gometry as gm
    import shapely

    wkt = (
        'GEOMETRYCOLLECTION (POINT (1 2), '
        'POLYGON ((0 0, 4 0, 4 4, 0 0), (1 1, 2 1, 1 2, 1 1)))'
    )
    parsed_geometry_exact(
        [gm.from_wkt(wkt)],
        [shapely.from_wkt(wkt)],
        _ctx('parsed'),
    )


def test_parsed_geometry_exact_rejects_coordinate_bits() -> None:
    import gometry as gm
    import shapely

    with pytest.raises(OracleMismatch, match='structure mismatch'):
        parsed_geometry_exact(
            [gm.Point(1.0, 2.0)],
            [shapely.Point(1.0, 9.0)],
            _ctx('parsed'),
        )


@pytest.mark.parametrize(
    ('left_wkt', 'right_wkt'),
    [
        (
            'GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1))',
            'GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), POINT (0 0))',
        ),
        (
            'GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1))',
            'GEOMETRYCOLLECTION (POINT (0 0))',
        ),
        (
            'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))',
            'MULTILINESTRING ((2 2, 3 3), (0 0, 1 1))',
        ),
        (
            'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))',
            'MULTILINESTRING ((0 0, 1 1))',
        ),
        (
            'POLYGON ((0 0, 10 0, 10 10, 0 0), '
            '(1 1, 2 1, 1 2, 1 1), (3 3, 4 3, 3 4, 3 3))',
            'POLYGON ((0 0, 10 0, 10 10, 0 0), '
            '(3 3, 4 3, 3 4, 3 3), (1 1, 2 1, 1 2, 1 1))',
        ),
        (
            'POLYGON ((0 0, 10 0, 10 10, 0 0), (1 1, 2 1, 1 2, 1 1))',
            'POLYGON ((0 0, 10 0, 10 10, 0 0))',
        ),
        ('POINT Z (1 2 3)', 'POINT (1 2)'),
        ('POINT M (1 2 3)', 'POINT (1 2)'),
    ],
    ids=(
        'collection-order',
        'collection-boundary',
        'part-order',
        'part-boundary',
        'ring-order',
        'ring-boundary',
        'lost-z',
        'lost-m',
    ),
)
def test_parsed_geometry_exact_rejects_structure_changes(
    left_wkt: str, right_wkt: str
) -> None:
    import gometry as gm
    import shapely

    with pytest.raises(OracleMismatch, match='structure mismatch'):
        parsed_geometry_exact(
            [gm.from_wkt(left_wkt)],
            [shapely.from_wkt(right_wkt)],
            _ctx('parsed'),
        )


def test_parsed_geometry_exact_rejects_missing_to_empty() -> None:
    import shapely

    with pytest.raises(OracleMismatch, match='missingness'):
        parsed_geometry_exact(
            [None],
            [shapely.Point()],
            _ctx('parsed'),
        )


def test_areal_result_equivalent_accepts_equal_and_numerical_boundary() -> None:
    import gometry as gm
    import shapely

    areal_result_equivalent(
        [gm.box(0, 0, 1, 1)],
        [shapely.box(0, 0, 1, 1)],
        _ctx('areal'),
    )
    areal_result_equivalent(
        [gm.box(0, 0, 1, 1)],
        [shapely.box(1e-5, 0, 1.00001, 1)],
        _ctx('areal'),
    )


def test_areal_result_equivalent_rejects_distinct_points() -> None:
    import gometry as gm
    import shapely

    with pytest.raises(OracleMismatch, match=r'(lower-dimensional|kind mismatch)'):
        areal_result_equivalent(
            [gm.Point(0, 0)],
            [shapely.Point(100, 100)],
            _ctx('areal'),
        )


def test_areal_result_equivalent_rejects_wrong_line_even_at_zero_area() -> None:
    import gometry as gm
    import shapely

    with pytest.raises(OracleMismatch, match='lower-dimensional'):
        areal_result_equivalent(
            [gm.LineString([(0, 0), (1, 0)])],
            [shapely.LineString([(100, 100), (101, 100)])],
            _ctx('areal'),
        )


def test_areal_result_equivalent_rejects_wrong_empty_kind() -> None:
    import gometry as gm
    import shapely

    with pytest.raises(OracleMismatch, match='kind mismatch'):
        areal_result_equivalent(
            [gm.from_wkt('POLYGON EMPTY')],
            [shapely.from_wkt('LINESTRING EMPTY')],
            _ctx('areal'),
        )


@pytest.mark.parametrize(
    'right',
    [
        pytest.param(
            'POINT (100 100)',
            id='distinct-point',
        ),
        pytest.param(
            'LINESTRING (100 100, 101 101)',
            id='far-line',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 0)), POINT (100 100))',
            id='polygon-with-external-point',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 0)), '
            'LINESTRING (100 100, 101 100))',
            id='polygon-with-external-line',
        ),
    ],
)
def test_areal_result_equivalent_rejects_non_areal_support(right: str) -> None:
    import gometry as gm
    import shapely

    with pytest.raises(OracleMismatch, match=r'(lower-dimensional|kind mismatch)'):
        areal_result_equivalent(
            [gm.box(0, 0, 1, 1)],
            [shapely.from_wkt(right)],
            _ctx('areal'),
        )


def test_repair_areal_result_allows_exact_collapsed_support_only() -> None:
    import gometry as gm
    import shapely

    areal_result_equivalent(
        [gm.LineString([(0, 0), (1, 0)])],
        [shapely.LineString([(0, 0), (1, 0)])],
        _ctx('repair'),
        allow_collapsed=True,
    )
    areal_result_equivalent(
        [gm.Point(0, 0)],
        [shapely.MultiPoint([(0, 0)])],
        _ctx('repair'),
        allow_collapsed=True,
    )
    with pytest.raises(OracleMismatch, match='lower-dimensional residual'):
        areal_result_equivalent(
            [gm.LineString([(0, 0), (1, 0)])],
            [shapely.LineString([(0, 1e-4), (1, 1e-4)])],
            _ctx('repair'),
            allow_collapsed=True,
        )


def test_linework_result_equivalent_uses_hausdorff_and_length() -> None:
    import gometry as gm
    import shapely

    line = gm.LineString([(0, 0), (1, 0)])
    linework_result_equivalent(
        [line],
        [shapely.LineString([(0, 5e-10), (1, 5e-10)])],
        _ctx('linework'),
    )
    with pytest.raises(OracleMismatch, match='Hausdorff'):
        linework_result_equivalent(
            [line],
            [shapely.LineString([(0, 10), (1, 10)])],
            _ctx('linework'),
        )
    with pytest.raises(OracleMismatch, match='component structure'):
        linework_result_equivalent(
            [gm.MultiLineString([[(0, 0), (1, 0)], [(2, 0), (3, 0)]])],
            [shapely.MultiLineString([[(0, 0), (1, 0)]])],
            _ctx('linework'),
        )


def test_linework_result_equivalent_preserves_empty_kind_and_missing_rules() -> None:
    import gometry as gm
    import shapely

    linework_result_equivalent(
        [gm.from_wkt('LINESTRING EMPTY')],
        [shapely.from_wkt('GEOMETRYCOLLECTION EMPTY')],
        _ctx('linework'),
    )
    with pytest.raises(OracleMismatch, match='missingness'):
        linework_result_equivalent(
            [None],
            [shapely.from_wkt('LINESTRING EMPTY')],
            _ctx('linework'),
        )


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


def test_public_scaling_builders_exact_bijection() -> None:
    public = {op.gometry for op in _bench_registry.SCALING_OPERATIONS}
    assert set(PUBLIC_SCALING_CASE_BUILDERS) == public
    _bench_registry.validate_scaling_operations()


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


def test_public_oracle_registries_cover_every_operation_once() -> None:
    release_identities = tuple(operation.gometry for operation in RELEASE_OPERATIONS)
    scaling_identities = tuple(operation.gometry for operation in SCALING_OPERATIONS)
    identities = (*release_identities, *scaling_identities)
    assert identities == tuple(operation.gometry for operation in ALL_PUBLIC_OPERATIONS)
    assert len(identities) == len(set(identities))
    assert set(PUBLIC_CASE_BUILDERS) == set(release_identities)
    assert set(PUBLIC_SCALING_CASE_BUILDERS) == set(scaling_identities)


def test_run_oracle_dispatches_local_release_and_scaling_cases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_operation = RELEASE_OPERATIONS[0]
    scaling_operation = SCALING_OPERATIONS[0]
    observed: list[str] = []

    class LocalCase:
        kind = 'local'

        def __init__(self, label: str, operation: object) -> None:
            self.label = label
            self.operation = operation

        def verify(self, context: OracleContext) -> None:
            assert context.operation is self.operation
            observed.append(self.label)

    def release_builder() -> LocalCase:
        return LocalCase('release', release_operation)

    def scaling_builder() -> LocalCase:
        return LocalCase('scaling', scaling_operation)

    monkeypatch.setattr(
        _bench_oracle,
        'PUBLIC_CASE_BUILDERS',
        {release_operation.gometry: release_builder},
    )
    monkeypatch.setattr(
        _bench_oracle,
        'PUBLIC_SCALING_CASE_BUILDERS',
        {scaling_operation.gometry: scaling_builder},
    )

    assert run_oracle([release_operation, scaling_operation]) == 0
    assert observed == ['release', 'scaling']


def test_public_oracle_returns_nonzero_on_mismatch() -> None:
    operation = RELEASE_OPERATIONS[0]

    class FailingCase:
        kind = 'test'

        def verify(self, context: OracleContext) -> None:
            raise OracleMismatch('intentional mismatch')

    assert (
        run_oracle(
            [operation],
            builders={operation.gometry: FailingCase},
        )
        == 1
    )


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
