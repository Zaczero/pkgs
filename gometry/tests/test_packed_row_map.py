"""Packed Lines/Polygons row_map correctness — permuted take oracle parity."""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import TYPE_CHECKING

import gometry as gm
import numpy as np
import pytest

from tests._support import bools, canon, floats

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class RowMapKind:
    name: str
    extension: str
    permuted_take_20: Callable[[], tuple[gm.GeometryArray, gm.GeometryArray]]
    total_bounds_case: Callable[
        [], tuple[gm.GeometryArray, gm.GeometryArray, tuple[float, ...]]
    ]
    scattered: Callable[[], gm.GeometryArray]


def _line_permuted_take_20() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.5), (2.0, 0.0)], crs=3857),
        gm.LineString([(10.0, 10.0), (11.0, 11.0)], crs=3857),
        gm.LineString([(20.0, 20.0), (21.0, 1.0), (22.0, 20.0), (23.0, 1.0)], crs=3857),
    ])
    permuted = lines[[2, 0]]
    oracle = gm.GeometryArray([lines[2], lines[0]])
    return (permuted, oracle)


def _polygon_permuted_take_20() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    polys = gm.GeometryArray([
        gm.box(0.0, 0.0, 1.0, 1.0, crs=3857),
        gm.box(10.0, 10.0, 11.0, 11.0, crs=3857),
        gm.from_wkt('POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))', crs=3857),
    ])
    permuted = polys[[2, 0]]
    oracle = gm.GeometryArray([polys[2], polys[0]])
    return (permuted, oracle)


def test_packed_polygon_empty_rows_compare_exactly_after_gather() -> None:
    empty = gm.from_wkt('POLYGON EMPTY')
    nonempty = gm.box(0.0, 0.0, 1.0, 1.0)
    source = gm.GeometryArray([empty, nonempty])
    gathered = source[[0, 0]]
    same = source[[0, 0]]
    different = source[[0, 1]]

    assert bools(gm.equals_exact(gathered, same, 0.0)) == [True, True]
    assert bools(gm.equals_exact(gathered, different, 0.0)) == [True, False]


def _line_total_bounds_case() -> tuple[
    gm.GeometryArray, gm.GeometryArray, tuple[float, ...]
]:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=3857),
        gm.LineString([(100.0, 100.0), (101.0, 101.0)], crs=3857),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)], crs=3857),
    ])
    selected = lines[[0, 2]]
    oracle = gm.GeometryArray([lines[0], lines[2]])
    return (selected, oracle, (0.0, 0.0, 3.0, 3.0))


def _polygon_total_bounds_case() -> tuple[
    gm.GeometryArray, gm.GeometryArray, tuple[float, ...]
]:
    polys = gm.GeometryArray([
        gm.box(0.0, 0.0, 1.0, 1.0, crs=3857),
        gm.box(100.0, 100.0, 101.0, 101.0, crs=3857),
        gm.box(2.0, 2.0, 3.0, 3.0, crs=3857),
    ])
    selected = polys[[0, 2]]
    oracle = gm.GeometryArray([polys[0], polys[2]])
    return (selected, oracle, (0.0, 0.0, 3.0, 3.0))


def _scattered_lines() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.LineString([(5.0, 5.0), (5.1, 5.1)], crs=3857),
        gm.LineString([(0.0, 0.0), (0.1, 0.1)], crs=3857),
        gm.LineString([(5.0, 0.0), (5.1, 0.1)], crs=3857),
        gm.LineString([(0.0, 5.0), (0.1, 5.1)], crs=3857),
    ])


def _scattered_polygons() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.box(5.0, 5.0, 5.1, 5.1, crs=3857),
        gm.box(0.0, 0.0, 0.1, 0.1, crs=3857),
        gm.box(5.0, 0.0, 5.1, 0.1, crs=3857),
        gm.box(0.0, 5.0, 0.1, 5.1, crs=3857),
    ])


ROW_MAP_KINDS = [
    RowMapKind(
        'line',
        'geoarrow.linestring',
        _line_permuted_take_20,
        _line_total_bounds_case,
        _scattered_lines,
    ),
    RowMapKind(
        'polygon',
        'geoarrow.polygon',
        _polygon_permuted_take_20,
        _polygon_total_bounds_case,
        _scattered_polygons,
    ),
]


def _mixed_sort_oracle(arr: gm.GeometryArray, *, hilbert: bool) -> gm.GeometryArray:
    mixed = gm.from_wkt([g.to_wkt() for g in arr], crs=arr.crs)
    if hilbert:
        return mixed.sort_by_spatial_key(curve='hilbert')
    return mixed.sort_by_spatial_key(curve='morton')


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_simplify_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.simplify(0.1, preserve_topology=False)) == canon(
        oracle.simplify(0.1, preserve_topology=False)
    )


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_reverse_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.reverse()) == canon(oracle.reverse())


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_to_arrow_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    permuted_arrow = permuted.to_arrow()
    oracle_arrow = oracle.to_arrow()
    assert permuted_arrow.type.extension_name == kind.extension
    assert permuted_arrow == oracle_arrow


def test_repeated_packed_line_rows_can_exceed_source_vertex_count() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.5), (2.0, 0.0)], crs=3857),
        gm.LineString([(10.0, 10.0), (11.0, 11.0)], crs=3857),
    ])
    repeated = lines[[0, 0, 0]]
    oracle = gm.GeometryArray([lines[0], lines[0], lines[0]])

    assert repeated.to_arrow() == oracle.to_arrow()
    assert canon(repeated.translate(1.0, -1.0)) == canon(oracle.translate(1.0, -1.0))


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_concat_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.concat(permuted)) == canon(oracle.concat(oracle))


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_concat_selected_with_dense_matches_oracle(
    kind: RowMapKind,
) -> None:
    permuted, oracle = kind.permuted_take_20()
    dense, dense_oracle = kind.permuted_take_20()
    dense = gm.GeometryArray(list(dense_oracle))
    assert canon(permuted.concat(dense)) == canon(oracle.concat(dense))
    assert canon(dense.concat(permuted)) == canon(dense.concat(oracle))
    assert canon(permuted.concat(dense, permuted)) == canon(
        oracle.concat(dense, oracle)
    )


def test_packed_duplicate_line_rows_concat_dense_and_missing() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.5), (2.0, 0.0)]),
        gm.LineString([(10.0, 10.0), (11.0, 11.0)]),
        None,
    ])
    repeated = lines[[0, 0, 2]]
    dense = gm.GeometryArray([
        gm.LineString([(20.0, 20.0), (21.0, 21.0)]),
        gm.LineString([(30.0, 0.0), (31.0, 1.0)]),
    ])
    oracle = gm.GeometryArray([lines[0], lines[0], None, dense[0], dense[1]])
    joined = repeated.concat(dense)
    assert joined.to_wkt() == oracle.to_wkt()
    assert joined.is_missing.tolist() == oracle.is_missing.tolist()


def test_packed_zm_selected_concat_preserves_ordinates() -> None:
    points = gm.points(
        [0.0, 1.0, 2.0],
        [3.0, 4.0, 5.0],
        z=[6.0, 7.0, 8.0],
        m=[9.0, 10.0, 11.0],
    )
    selected = points[[2, 0, 2]]
    dense = gm.points([100.0], [200.0], z=[300.0], m=[400.0])
    joined = selected.concat(dense)
    assert joined.coords.x.tolist() == [2.0, 0.0, 2.0, 100.0]
    assert joined.coords.y.tolist() == [5.0, 3.0, 5.0, 200.0]
    assert joined.coords.z.tolist() == [8.0, 6.0, 8.0, 300.0]
    assert joined.coords.m.tolist() == [11.0, 9.0, 11.0, 400.0]


def test_packed_selected_reverse_matches_oracle() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.5), (2.0, 0.0)]),
        gm.LineString([(10.0, 10.0), (11.0, 11.0)]),
        gm.LineString([(20.0, 1.0), (21.0, 2.0), (22.0, 3.0)]),
    ])
    selected = lines[[2, 0, 2]]
    oracle = gm.GeometryArray([lines[2], lines[0], lines[2]])
    assert canon(selected.reverse()) == canon(oracle.reverse())


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_to_crs_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    target = 'EPSG:4326'
    assert canon(permuted.to_crs(target)) == canon(oracle.to_crs(target))


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_length_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert floats(permuted.length) == floats(oracle.length)


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_coordinate_buffer_matches_logical_order(
    kind: RowMapKind,
) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert permuted.coords.x.tolist() == oracle.coords.x.tolist()
    assert list(permuted.coords.x) == oracle.coords.x.tolist()
    assert np.asarray(permuted.coords.x).tolist() == oracle.coords.x.tolist()


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_total_bounds_excludes_unselected_rows(kind: RowMapKind) -> None:
    selected, oracle, expected = kind.total_bounds_case()
    assert selected.total_bounds == oracle.total_bounds == expected


_EXTERNAL_HOLE_BOUNDS = (0.0, 0.0, 11.0, 11.0)


def _external_hole_polygon() -> gm.Polygon:
    shell = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
    external_hole = [
        (10.0, 10.0),
        (11.0, 10.0),
        (11.0, 11.0),
        (10.0, 11.0),
        (10.0, 10.0),
    ]
    return gm.Polygon(shell, [external_hole], crs=3857)


def _external_hole_array(*, degenerate_sibling: bool = False) -> gm.GeometryArray:
    rows: list[gm.Polygon] = [_external_hole_polygon()]
    if degenerate_sibling:
        rows.append(gm.Polygon([(0.0, 0.0)] * 4, crs=3857))
    return gm.GeometryArray(rows, crs=3857)


def test_packed_polygon_scalar_bounds_include_external_hole() -> None:
    assert _external_hole_polygon().bounds == _EXTERNAL_HOLE_BOUNDS


def test_packed_polygon_per_row_bounds_include_external_hole() -> None:
    np.testing.assert_array_equal(
        _external_hole_array().bounds,
        np.array([_EXTERNAL_HOLE_BOUNDS]),
    )


def test_packed_polygon_bounds_keep_empty_and_missing_rows() -> None:
    values = gm.GeometryArray(
        [
            _external_hole_polygon(),
            gm.Polygon([], crs=3857),
            None,
        ],
        crs=3857,
    )
    np.testing.assert_array_equal(
        values.bounds,
        np.array([
            _EXTERNAL_HOLE_BOUNDS,
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan, np.nan],
        ]),
    )


def test_packed_polygon_cold_total_bounds_include_external_hole() -> None:
    assert _external_hole_array().total_bounds == _EXTERNAL_HOLE_BOUNDS


def test_packed_polygon_warm_total_bounds_include_external_hole() -> None:
    values = _external_hole_array()
    _ = values.bounds
    assert values.total_bounds == _EXTERNAL_HOLE_BOUNDS


def test_packed_polygon_duplicate_selection_total_bounds_include_external_hole() -> (
    None
):
    values = _external_hole_array()
    assert values[[0, 0]].total_bounds == _EXTERNAL_HOLE_BOUNDS


def test_packed_polygon_envelope_fallback_bounds_include_external_hole() -> None:
    values = _external_hole_array(degenerate_sibling=True)
    assert values.envelope()[0].bounds == _EXTERNAL_HOLE_BOUNDS


@pytest.mark.parametrize('curve', ['hilbert', 'morton'])
def test_packed_polygon_default_curve_frame_includes_external_hole(curve: str) -> None:
    values = _external_hole_array()
    assert (
        values.spatial_key(curve=curve).tolist()
        == values.spatial_key(
            curve=curve,
            bounds=_EXTERNAL_HOLE_BOUNDS,
        ).tolist()
    )


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_centroid_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.centroid()) == canon(oracle.centroid())


def test_packed_lines_row_map_point_on_surface_matches_oracle() -> None:
    kind = ROW_MAP_KINDS[0]
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.point_on_surface()) == canon(oracle.point_on_surface())


def test_packed_polygons_row_map_bounds_matches_oracle() -> None:
    kind = ROW_MAP_KINDS[1]
    permuted, oracle = kind.permuted_take_20()
    assert floats(permuted.bounds) == floats(oracle.bounds)


def test_packed_polygons_row_map_area_matches_oracle() -> None:
    kind = ROW_MAP_KINDS[1]
    permuted, oracle = kind.permuted_take_20()
    assert floats(permuted.area) == floats(oracle.area)


def test_packed_lines_row_map_equals_exact_matches_oracle() -> None:
    kind = ROW_MAP_KINDS[0]
    permuted, oracle = kind.permuted_take_20()
    assert bools(gm.equals_exact(permuted, oracle)) == [True, True]
    assert bools(gm.equals_exact(oracle, permuted)) == [True, True]


@pytest.mark.parametrize(
    ('kind', 'method', 'args', 'kwargs'),
    [
        (k, method, args, kwargs)
        for k in ROW_MAP_KINDS
        for method, args, kwargs in [
            ('translate', (3.0, -2.0), {}),
            ('affine_transform', ([1.0, 0.25, -0.5, 1.0, 2.0, 4.0]), {}),
            ('snap_to_grid', (0.5), {}),
            ('set_z', (7.0), {}),
            ('set_m', (9.0), {}),
            ('rotate', (15.0), {}),
            ('scale', (2.0, 0.5), {}),
        ]
    ],
)
def test_packed_row_map_column_maps_match_oracle(
    kind: RowMapKind, method: str, args: tuple[object, ...], kwargs: dict[str, object]
) -> None:
    permuted, oracle = kind.permuted_take_20()
    call_args = args if isinstance(args, tuple) else (args,)
    result = getattr(permuted, method)(*call_args, **kwargs)
    expected = getattr(oracle, method)(*call_args, **kwargs)
    assert len(result) == len(expected) == 2
    assert canon(result) == canon(expected)


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_to_crs_preserves_row_map(kind: RowMapKind) -> None:
    permuted, _ = kind.permuted_take_20()
    transformed = permuted.to_crs('EPSG:4326')
    assert gm.equals(transformed[0], permuted[0].to_crs('EPSG:4326'))
    assert gm.equals(transformed[1], permuted[1].to_crs('EPSG:4326'))


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_pickle_round_trip(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    restored = pickle.loads(pickle.dumps(permuted))
    assert restored.to_arrow() == oracle.to_arrow()
    assert canon(restored.to_crs('EPSG:4326')) == canon(permuted.to_crs('EPSG:4326'))


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
@pytest.mark.parametrize('hilbert', [True, False])
def test_packed_row_map_sort_by_curve_preserves_extension(
    kind: RowMapKind, hilbert: bool
) -> None:
    arr = kind.scattered()
    sorted_arr = (
        arr.sort_by_spatial_key(curve='hilbert')
        if hilbert
        else arr.sort_by_spatial_key(curve='morton')
    )
    assert sorted_arr.to_arrow().type.extension_name == kind.extension


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
@pytest.mark.parametrize('hilbert', [True, False])
def test_packed_row_map_sort_by_curve_matches_mixed_oracle(
    kind: RowMapKind, hilbert: bool
) -> None:
    arr = kind.scattered()
    sorted_arr = (
        arr.sort_by_spatial_key(curve='hilbert')
        if hilbert
        else arr.sort_by_spatial_key(curve='morton')
    )
    oracle = _mixed_sort_oracle(arr, hilbert=hilbert)
    assert canon(sorted_arr) == canon(oracle)


@pytest.mark.parametrize(
    ('rows', 'bounds'),
    [
        (
            [gm.Point(0, 0), gm.Point(100, 100), gm.Point(10, 10)],
            (0, 0, 10, 10),
        ),
        (
            [
                gm.LineString([(0, 0), (1, 1)]),
                gm.LineString([(100, 100), (101, 101)]),
                gm.LineString([(10, 10), (11, 11)]),
            ],
            (0, 0, 11, 11),
        ),
        (
            [
                gm.box(0, 0, 1, 1),
                gm.box(100, 100, 101, 101),
                gm.box(10, 10, 11, 11),
            ],
            (0, 0, 11, 11),
        ),
        (
            [
                gm.Point(0, 0),
                gm.LineString([(100, 100), (101, 101)]),
                gm.Point(10, 10),
            ],
            (0, 0, 10, 10),
        ),
    ],
    ids=['points', 'lines', 'polygons', 'mixed'],
)
def test_nullable_spatial_keys_ignore_hidden_payload(
    rows: list[gm.Geometry], bounds: tuple[int, int, int, int]
) -> None:
    masked = gm.GeometryArray(rows)._with_missing([False, True, False])
    for selected in (masked, masked[::-1], masked[[2, 1, 0]]):
        assert (
            selected.spatial_key().tolist()
            == selected.spatial_key(bounds=bounds).tolist()
        )


def _point_gather_array() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    points = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs='EPSG:4326'),
        gm.Point(1.0, 0.0, crs='EPSG:4326'),
        gm.Point(2.0, 0.0, crs='EPSG:4326'),
        gm.Point(3.0, 0.0, crs='EPSG:4326'),
    ])
    gathered = points[[3, 1]]
    oracle = gm.GeometryArray([points[3], points[1]])
    return (gathered, oracle)


def test_packed_points_row_map_geodesic_distance_matches_oracle() -> None:
    gathered, oracle = _point_gather_array()
    target = gm.Point(0.5, 0.1, crs='EPSG:4326')
    assert floats(gm.distance(gathered, target)) == floats(gm.distance(oracle, target))


def test_packed_points_row_map_geodesic_dwithin_matches_oracle() -> None:
    gathered, oracle = _point_gather_array()
    target = gm.Point(0.5, 0.1, crs='EPSG:4326')
    assert bools(gm.dwithin(gathered, target, 2000000.0)) == bools(
        gm.dwithin(oracle, target, 2000000.0)
    )


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_segmentize_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.segmentize(0.5)) == canon(oracle.segmentize(0.5))


@pytest.mark.parametrize('kind', ROW_MAP_KINDS, ids=lambda k: k.name)
def test_packed_row_map_densify_matches_oracle(kind: RowMapKind) -> None:
    permuted, oracle = kind.permuted_take_20()
    assert canon(permuted.segmentize(fraction=0.25)) == canon(
        oracle.segmentize(fraction=0.25)
    )


@pytest.mark.parametrize(
    ('method', 'args', 'kwargs'),
    [('set_z', (7.0), {}), ('quantize', (3), {}), ('to_crs', ('EPSG:4326'), {})],
)
def test_packed_row_map_compact_output_after_column_maps(
    method: str, args: tuple[object, ...], kwargs: dict[str, object]
) -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.5), (2.0, 0.0)], crs=3857),
        gm.LineString([(10.0, 10.0), (11.0, 11.0)], crs=3857),
        gm.LineString([(20.0, 20.0), (21.0, 1.0), (22.0, 20.0), (23.0, 1.0)], crs=3857),
    ])
    permuted = lines[[2, 0]]
    call_args = args if isinstance(args, tuple) else (args,)
    if method == 'to_crs':
        result = permuted.to_crs(*call_args, **kwargs)
    else:
        result = getattr(permuted, method)(*call_args, **kwargs)
    assert result.to_arrow().type.extension_name == 'geoarrow.linestring'
    assert gm.equals(
        result[0], permuted[0] if method != 'to_crs' else permuted[0].to_crs(*call_args)
    )
    assert gm.equals(
        result[1], permuted[1] if method != 'to_crs' else permuted[1].to_crs(*call_args)
    )


def test_masked_z_and_empty_lines_scatter_through_to_crs() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[10.0, 11.0], crs=4326),
        None,
        gm.from_wkt('LINESTRING EMPTY', crs=4326),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)], z=[12.0, 13.0], crs=4326),
    ])
    scattered = lines[[3, 1, 2, 0]]
    expected = gm.GeometryArray([lines[3], None, lines[2], lines[0]]).to_crs(3857)
    transformed = scattered.to_crs(3857)
    assert transformed.is_missing.tolist() == [False, True, False, False]
    assert transformed[1] is None
    assert transformed[2].to_wkt() == 'LINESTRING EMPTY'
    assert transformed[0].has_z and transformed[3].has_z
    assert transformed.to_wkt() == expected.to_wkt()
    assert transformed.crs == expected.crs


def test_masked_packed_points_to_crs_preserves_rows_axes_and_order() -> None:
    values = gm.GeometryArray(
        [
            gm.Point(-73.0, 41.0, z=10.0, m=7.0),
            None,
            gm.Point(-72.5, 41.25, z=20.0, m=8.0),
        ],
        crs=4326,
    )

    selected = values[[2, 1, 0]]
    transformed = selected.to_crs(3857)
    assert transformed.is_missing.tolist() == [False, True, False]
    assert transformed[1] is None
    assert transformed.crs.to_epsg() == 3857
    for row in (0, 2):
        expected = selected[row].to_crs(3857)
        actual = transformed[row]
        np.testing.assert_allclose([actual.x, actual.y], [expected.x, expected.y])
        assert actual.z == expected.z
        assert actual.m == expected.m
