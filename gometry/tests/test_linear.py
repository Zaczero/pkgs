"""Linear referencing — interpolate/locate/substring, nearest points, splitting
lines by points, and empty-linework error contracts.
"""

import math
import sys
from typing import cast

import gometry as gm
import numpy as np
import pytest

pyproj = pytest.importorskip(
    'pyproj', reason='pyproj==3.7.2 oracle wheel is unavailable for this interpreter'
)
Geod = pyproj.Geod


def test_line_interpolate_and_locate_point_are_rust_backed_and_preserve_axes() -> None:
    line = gm.LineString(
        [(0, 0), (3, 4), (6, 4)], z=[0, 5, 8], m=[10, 15, 18], crs=3857
    )
    multilines = gm.MultiLineString([[(0, 0), (3, 4)], [(3, 4), (6, 4)]], crs=3857)
    target = gm.Point(4, 5, crs=3857)
    lonlat_line = gm.LineString([(0, 0), (1, 0)], crs=4326)
    interpolated = line.line_interpolate(6)
    normalized = cast('gm.Geometry', line.line_interpolate(0.75, normalized=True))
    substring = line.line_substring(2, 6)
    normalized_substring = line.line_substring(0.25, 0.75, normalized=True)
    point_substring = line.line_substring(6, 6)
    array = cast(
        'gm.GeometryArray', gm.GeometryArray([line, multilines]).line_interpolate(6)
    )
    substring_array = cast(
        'gm.GeometryArray', gm.GeometryArray([line, multilines]).line_substring(4, 6)
    )
    assert interpolated.to_wkt() == 'POINT ZM (4 4 6 16)'
    assert gm.equals_exact(normalized, interpolated)
    assert gm.equals_exact(line.line_interpolate(-2), interpolated)
    assert substring.geometry_type == 'LineString'
    assert [
        axis for point in substring.coords.to_nested() for axis in point
    ] == pytest.approx([1.2, 1.6, 2, 12, 3, 4, 5, 15, 4, 4, 6, 16])
    with pytest.raises(gm.GeometryError, match='use reverse'):
        line.line_substring(6, 2)
    with pytest.raises(gm.GeometryError, match='use reverse'):
        line.line_substring(16.0, 12.0, basis='m')
    assert gm.equals_exact(normalized_substring, substring)
    assert gm.equals_exact(line.line_substring(-6, -2), substring)
    assert gm.equals_exact(point_substring, interpolated)
    assert line.line_locate(target) == pytest.approx(6)
    assert line.line_locate(target, normalized=True) == pytest.approx(0.75)
    assert line.line_locate(target) == pytest.approx(6)
    assert gm.equals_exact(array[0], interpolated)
    assert array[1].to_wkt() == 'POINT (4 4)'
    assert [
        axis for point in substring_array[0].coords.to_nested() for axis in point
    ] == pytest.approx([2.4, 3.2, 4, 14, 3, 4, 5, 15, 4, 4, 6, 16])
    assert [
        axis for point in substring_array[1].coords.to_nested() for axis in point
    ] == pytest.approx([2.4, 3.2, 3, 4, 4, 4])
    assert gm.GeometryArray([line, multilines]).line_locate(target) == pytest.approx([
        6,
        6,
    ])
    assert [
        line.line_locate(gm.Point(1, 1, crs=3857)),
        line.line_locate(gm.Point(4, 4, crs=3857)),
    ] == pytest.approx([1.4, 6])
    scalar_many = line.line_locate(gm.points([1, 4], [1, 4], crs=3857))
    assert scalar_many == pytest.approx([1.4, 6])
    assert gm.GeometryArray([
        line,
        gm.LineString([(0, 0), (0, 4)], crs=3857),
    ]).line_locate(gm.points([1, 0], [1, 3], crs=3857)) == pytest.approx([1.4, 3])
    assert gm.GeometryArray([
        line,
        gm.LineString([(0, 0), (0, 4)], crs=3857),
    ]).line_locate(gm.points([1, 0], [1, 3], crs=3857)) == pytest.approx([1.4, 3])
    geographic_length = lonlat_line.length
    assert geographic_length == pytest.approx(111319.49079327357)
    assert (
        lonlat_line.line_interpolate(0.5, normalized=True).to_wkt() == 'POINT (0.5 0)'
    )
    one_metre_east = 1.0 / geographic_length
    assert lonlat_line.line_interpolate(1).x == pytest.approx(one_metre_east)
    assert (
        gm.GeometryArray([lonlat_line]).line_interpolate(1)[0].to_wkt()
        == lonlat_line.line_interpolate(1).to_wkt()
    )
    assert lonlat_line.line_substring(0, 1).length == pytest.approx(1.0)
    half_length = lonlat_line.line_locate(gm.Point(0.5, 0, crs=4326))
    assert half_length == pytest.approx(geographic_length / 2)
    assert gm.GeometryArray([lonlat_line]).line_locate(
        gm.Point(0.5, 0, crs=4326)
    ) == pytest.approx([geographic_length / 2])
    assert lonlat_line.line_substring(0, half_length).length == pytest.approx(
        half_length
    )
    with pytest.raises(ValueError, match='same length'):
        gm.GeometryArray([line, line]).line_locate(gm.points([1], [1]))
    with pytest.raises(ValueError, match='same length'):
        gm.GeometryArray([line, line]).line_locate(gm.points([1], [1]))
    with pytest.raises(TypeError, match='Point'):
        line.line_locate(gm.box(0, 0, 1, 1, crs=3857))
    with pytest.raises(TypeError, match='LineString or MultiLineString'):
        gm.Point(0, 0).line_interpolate(1)
    with pytest.raises(ValueError, match='start must be finite'):
        line.line_substring(math.nan, 1)


def test_nearest_points_are_rust_backed_and_preserve_crs() -> None:
    line = gm.LineString([(0, 0), (4, 0)], z=[0, 8], m=[10, 18], crs=4326)
    point = gm.Point(1, 3, crs=4326)
    left, right = gm.nearest_points(line, point, unit='planar')
    assert left.to_wkt() == 'POINT (1 0)'
    assert right.to_wkt() == 'POINT (1 3)'
    assert left.crs == 'EPSG:4326'
    assert right.crs == 'EPSG:4326'
    geo_left, geo_right = gm.nearest_points(line, point)
    assert geo_left.crs == 'EPSG:4326'
    assert geo_right.crs == 'EPSG:4326'
    assert geo_left.y == 0.0
    assert 0.0 <= geo_left.x <= 4.0
    geod = Geod(ellps='WGS84')
    reference = geod.inv(point.x, point.y, 1.0, 0.0)[2]
    attained = geod.inv(point.x, point.y, geo_left.x, geo_left.y)[2]
    assert attained <= reference + 4e-11 * 6_378_137.0
    # The returned point must attain the reported metric minimum.
    assert gm.distance(
        gm.Point(geo_left.x, geo_left.y, crs=4326), point
    ) == pytest.approx(gm.distance(line, point), abs=1e-9)
    oblique = gm.LineString([(0, 10), (4, 12)], crs=4326)
    off_oblique = gm.Point(1, 13, crs=4326)
    bowed, _ = gm.nearest_points(oblique, off_oblique)
    flat, _ = gm.nearest_points(oblique, off_oblique, unit='planar')
    assert (flat.x, flat.y) == pytest.approx((2.0, 11.0), abs=1e-12)
    assert bowed.x != flat.x
    assert gm.distance(bowed, off_oblique) + 1.0 < gm.distance(flat, off_oblique)
    with pytest.raises(
        ValueError, match='nearest_points requires matching CRS metadata'
    ):
        gm.nearest_points(line, gm.Point(1, 3, crs=3857))
    left, right = gm.nearest_points(
        gm.LineString([(0, 0), (2, 2)]), gm.LineString([(0, 2), (2, 0)])
    )
    assert left.to_wkt() == 'POINT (1 1)'
    assert right.to_wkt() == 'POINT (1 1)'
    pair_left, pair_right = gm.nearest_points(
        gm.GeometryArray([gm.Point(0, 2), gm.Point(3, 0)]),
        gm.LineString([(0, 0), (2, 0)]),
    )
    assert isinstance(pair_left, gm.GeometryArray)
    assert isinstance(pair_right, gm.GeometryArray)
    assert pair_left.to_wkt() == ['POINT (0 2)', 'POINT (3 0)']
    assert pair_right.to_wkt() == ['POINT (0 0)', 'POINT (2 0)']
    assert [
        (left.to_wkt(), right.to_wkt())
        for left, right in zip(pair_left, pair_right, strict=True)
    ] == [('POINT (0 2)', 'POINT (0 0)'), ('POINT (3 0)', 'POINT (2 0)')]
    assert pair_left[1:].to_wkt() == ['POINT (3 0)']
    top_left, top_right = gm.nearest_points(
        gm.Point(0, 2),
        gm.GeometryArray([
            gm.LineString([(0, 0), (2, 0)]),
            gm.LineString([(10, 0), (12, 0)]),
        ]),
    )
    pairwise = gm.nearest_points(
        gm.GeometryArray([gm.Point(0, 2), gm.Point(11, 3)]),
        gm.GeometryArray([
            gm.LineString([(0, 0), (2, 0)]),
            gm.LineString([(10, 0), (12, 0)]),
        ]),
    )
    assert top_left.to_wkt() == ['POINT (0 2)', 'POINT (0 2)']
    assert top_right.to_wkt() == ['POINT (0 0)', 'POINT (10 0)']
    assert [
        (left.to_wkt(), right.to_wkt()) for left, right in zip(*pairwise, strict=True)
    ] == [('POINT (0 2)', 'POINT (0 0)'), ('POINT (11 3)', 'POINT (11 0)')]
    with pytest.raises(ValueError, match='same length'):
        gm.nearest_points(
            gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]),
            gm.GeometryArray([gm.Point(0, 0)]),
        )
    with pytest.raises(ValueError, match='same length'):
        gm.nearest_points(
            gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]),
            gm.GeometryArray([gm.Point(0, 0)]),
        )
    with pytest.raises(
        ValueError, match='nearest_points requires matching CRS metadata'
    ):
        gm.nearest_points(gm.Point(0, 0, crs=4326), gm.Point(0, 0, crs=3857))


def test_split_lines_by_points_is_rust_backed_and_preserves_crs_and_measures() -> None:
    line = gm.LineString([(0, 0), (4, 0)], z=[0, 8], m=[10, 18], crs=4326)
    pieces = gm.split(line, gm.MultiPoint([(1, 0), (3, 0)], crs=4326))
    top_level = gm.split(line, gm.Point(2, 0, crs=4326))
    multiline = gm.MultiLineString([[(0, 0), (2, 0)], [(0, 1), (2, 1)]], crs=3857)
    multiline_pieces = gm.split(multiline, gm.MultiPoint([(1, 0), (1, 1)], crs=3857))
    array_pieces = gm.split(gm.GeometryArray([line]), gm.Point(2, 0, crs=4326))
    scalar_array_splitter_pieces = gm.split(
        line, gm.GeometryArray([gm.Point(1, 0, crs=4326), gm.Point(3, 0, crs=4326)])
    )
    pairwise_array_pieces = gm.split(
        gm.GeometryArray([line, gm.LineString([(0, 0), (6, 0)], crs=4326)]),
        gm.GeometryArray([gm.Point(2, 0, crs=4326), gm.Point(3, 0, crs=4326)]),
    )
    mixed_axis_splitters = gm.GeometryArray([
        gm.Point(1, 0, crs=4326),
        gm.from_wkt('MULTIPOINT Z ((3 0 7))', crs=4326),
    ])
    mixed_axis_splitter_pieces = gm.split(line, mixed_axis_splitters)
    assert [piece.to_wkt() for piece in pieces] == [
        'LINESTRING ZM (0 0 0 10, 1 0 2 12)',
        'LINESTRING ZM (1 0 2 12, 3 0 6 16)',
        'LINESTRING ZM (3 0 6 16, 4 0 8 18)',
    ]
    assert pieces.crs == 'EPSG:4326'
    assert all(piece.crs == 'EPSG:4326' for piece in pieces)
    assert [piece.to_wkt() for piece in top_level] == [
        'LINESTRING ZM (0 0 0 10, 2 0 4 14)',
        'LINESTRING ZM (2 0 4 14, 4 0 8 18)',
    ]
    assert [piece.to_wkt() for piece in multiline_pieces] == [
        'LINESTRING (0 0, 1 0)',
        'LINESTRING (1 0, 2 0)',
        'LINESTRING (0 1, 1 1)',
        'LINESTRING (1 1, 2 1)',
    ]
    assert all(piece.crs == 'EPSG:3857' for piece in multiline_pieces)
    assert array_pieces.offsets.tolist() == [0, 2]
    assert [piece.to_wkt() for piece in array_pieces[0]] == [
        piece.to_wkt() for piece in top_level
    ]
    assert [piece.to_wkt() for piece in scalar_array_splitter_pieces] == [
        piece.to_wkt() for piece in pieces
    ]
    assert pairwise_array_pieces.offsets.tolist() == [0, 2, 4]
    assert [piece.to_wkt() for piece in pairwise_array_pieces[0]] == [
        'LINESTRING ZM (0 0 0 10, 2 0 4 14)',
        'LINESTRING ZM (2 0 4 14, 4 0 8 18)',
    ]
    assert [piece.to_wkt() for piece in pairwise_array_pieces[1]] == [
        'LINESTRING (0 0, 3 0)',
        'LINESTRING (3 0, 6 0)',
    ]
    assert [piece.to_wkt() for piece in mixed_axis_splitter_pieces] == [
        piece.to_wkt() for piece in pieces
    ]
    assert [piece.to_wkt() for piece in gm.split(line, gm.Point(5, 0, crs=4326))] == [
        line.to_wkt()
    ]
    assert [piece.to_wkt() for piece in gm.split(line, gm.Point(0, 0, crs=4326))] == [
        line.to_wkt()
    ]
    with pytest.raises(TypeError, match='LineString or MultiLineString'):
        gm.split(gm.Point(0, 0), gm.Point(0, 0))
    with pytest.raises(TypeError, match='Point or MultiPoint'):
        gm.split(line, gm.box(0, 0, 1, 1, crs=4326))
    with pytest.raises(ValueError, match='same length'):
        gm.split(gm.GeometryArray([line, line]), gm.GeometryArray([gm.Point(1, 0)]))
    with pytest.raises(
        gm.CRSMismatchError, match='split requires matching CRS metadata'
    ):
        gm.split(line, gm.Point(2, 0, crs=3857))


def test_split_parts_preserves_coordinate_epoch() -> None:
    line = gm.LineString([(0, 0), (4, 0)], crs=4326).set_epoch(2020.0)
    splitter = gm.Point(2, 0, crs=4326).set_epoch(2020.0)
    pieces = gm.split(line, splitter)
    assert pieces.epoch == 2020.0
    assert all(piece.epoch == 2020.0 for piece in pieces)


def test_split_tolerance_governs_membership_and_dedup() -> None:
    line = gm.LineString([(0, 0), (4, 0)], crs=4326)
    # Exact topological membership by default: a point 5e-10 off the line
    # does not split it.
    default_pieces = gm.split(line, gm.Point(2, 5e-10, crs=4326))
    assert [piece.to_wkt() for piece in default_pieces] == [line.to_wkt()]
    # The same off-line point splits when tolerance covers its offset.
    toleranced = gm.split(line, gm.Point(2, 5e-10, crs=4326), tolerance=1e-9)
    assert [piece.to_wkt() for piece in toleranced] == [
        'LINESTRING (0 0, 2 0)',
        'LINESTRING (2 0, 4 0)',
    ]
    # Two on-line cuts 5e-13 apart are distinct at the default (identity dedup),
    # yielding three pieces rather than collapsing to two.
    two_cuts = gm.split(line, gm.MultiPoint([(2, 0), (2.0000000000005, 0)], crs=4326))
    assert len(two_cuts) == 3
    # A negative tolerance is rejected at the boundary.
    with pytest.raises(
        ValueError, match='tolerance must be a non-negative finite number'
    ):
        gm.split(line, gm.Point(2, 0, crs=4326), tolerance=-1.0)


EMPTY_LINEWORK = (
    'LINESTRING EMPTY',
    'MULTILINESTRING EMPTY',
    'MULTILINESTRING (EMPTY)',
)


@pytest.mark.parametrize('wkt', EMPTY_LINEWORK)
def test_empty_linework_answers_every_linear_reference(wkt: str) -> None:
    empty = gm.from_wkt(wkt)
    assert empty.line_interpolate(0.5, normalized=True).to_wkt() == 'POINT EMPTY'
    assert empty.line_interpolate(3.0, basis='m').to_wkt() == 'POINT EMPTY'
    assert empty.line_interpolate([0.1, 0.5]).to_wkt() == ['POINT EMPTY'] * 2
    assert empty.line_interpolate(count=3).to_wkt() == ['POINT EMPTY'] * 3
    assert math.isnan(empty.line_locate(gm.Point(1, 0)))
    assert math.isnan(empty.line_locate(gm.Point(1, 0), basis='m'))
    assert empty.line_substring(0.1, 0.9).to_wkt() == 'LINESTRING EMPTY'
    assert empty.line_substring(1, 2, basis='m').to_wkt() == 'LINESTRING EMPTY'
    measured = empty.interpolate_m(0.0, 1.0)
    assert measured.is_empty
    assert measured.has_m


def test_empty_linework_rows_match_the_scalar_answers() -> None:
    line = gm.LineString([(0, 0), (4, 0)])
    rows = gm.GeometryArray([line, gm.from_wkt('LINESTRING EMPTY'), None])
    assert rows.line_interpolate(0.5, normalized=True).to_wkt() == [
        'POINT (2 0)',
        'POINT EMPTY',
        None,
    ]
    samples = rows.line_interpolate(count=2)
    assert [group.to_wkt() for group in samples] == [
        ['POINT (0 0)', 'POINT (4 0)'],
        ['POINT EMPTY', 'POINT EMPTY'],
        [],
    ]
    assert rows.interpolate_m(0.0, 1.0).to_wkt() == [
        'LINESTRING M (0 0 0, 4 0 1)',
        'LINESTRING M EMPTY',
        None,
    ]


def test_measure_defects_raise_for_a_scalar_and_degrade_an_array_row() -> None:
    unmeasured = gm.LineString([(0, 0), (4, 0)])
    measured = gm.LineString([(0, 0), (4, 0)], m=[0, 10])
    with pytest.raises(gm.InvalidGeometryError, match='M ordinate'):
        unmeasured.line_locate(gm.Point(1, 0), basis='m')
    located = gm.GeometryArray([measured, unmeasured]).line_locate(
        gm.Point(1, 0), basis='m'
    )
    assert located[0] == pytest.approx(2.5)
    assert math.isnan(located[1])


def test_an_empty_query_point_locates_nowhere() -> None:
    line = gm.LineString([(0, 0), (4, 0)])
    measured = gm.LineString([(0, 0), (4, 0)], m=[0, 10])
    empty = gm.from_wkt('POINT EMPTY')
    assert math.isnan(line.line_locate(empty))
    assert math.isnan(measured.line_locate(empty, basis='m'))
    assert math.isnan(gm.GeometryArray([line]).line_locate(empty)[0])
    geographic = gm.LineString([(0, 0), (1, 0)], crs=4326)
    assert math.isnan(geographic.line_locate(gm.from_wkt('POINT EMPTY', crs=4326)))
    with pytest.raises(gm.GeometryTypeError, match='LineString'):
        gm.Polygon([(0, 0), (1, 0), (1, 1)]).line_locate(empty)


def test_query_rows_with_an_empty_point_locate_row_by_row() -> None:
    line = gm.LineString([(0, 0), (4, 0)])
    measured = gm.LineString([(0, 0), (4, 0)], m=[0, 10])
    queries = gm.GeometryArray([
        gm.Point(1, 0),
        gm.from_wkt('POINT EMPTY'),
        gm.Point(2, 0),
        None,
    ])
    expected = [1.0, math.nan, 2.0, math.nan]
    np.testing.assert_array_equal(line.line_locate(queries), expected)
    np.testing.assert_array_equal(
        gm.GeometryArray([line] * 4).line_locate(queries), expected
    )
    np.testing.assert_array_equal(
        gm.GeometryArray([measured] * 4).line_locate(queries, basis='m'),
        [2.5, math.nan, 5.0, math.nan],
    )
    with pytest.raises(gm.GeometryTypeError, match='line_locate requires a Point'):
        gm.GeometryArray([line]).line_locate(
            gm.GeometryArray([gm.Polygon([(0, 0), (1, 0), (1, 1)])])
        )


def test_scalar_line_locate_rejects_a_query_in_another_crs() -> None:
    line = gm.LineString([(0, 0), (1, 0)], crs=4326)
    with pytest.raises(gm.CRSMismatchError):
        line.line_locate(gm.Point(0.5, 0, crs=3857))


def test_measured_lrs_and_distance_3d_have_free_and_array_forms() -> None:
    line = gm.from_wkt('LINESTRING M (0 0 0, 10 0 10)', crs=4326)
    probe = gm.Point(4, 1, crs=4326)
    lines = gm.GeometryArray([line, line], crs=4326)
    assert line.line_locate(probe, basis='m') == pytest.approx(4.0, abs=2e-4)
    assert lines.line_locate(probe, basis='m') == pytest.approx([4.0, 4.0], abs=2e-4)
    scalar_many = line.line_locate(
        gm.points([4.0, 8.0], [1.0, 1.0], crs=4326), basis='m'
    )
    assert scalar_many == pytest.approx([4.0, 8.0], abs=2e-4)
    interpolated = cast('gm.Point', line.line_interpolate(5.0, basis='m'))
    assert (interpolated.x, interpolated.y) == (5.0, 0.0)
    array_interpolated = cast(
        'gm.GeometryArray', lines.line_interpolate(5.0, basis='m')
    )
    assert len(array_interpolated) == 2
    assert array_interpolated.crs == 'EPSG:4326'
    sub = cast('gm.LineString', line.line_substring(2.0, 4.0, basis='m'))
    assert sub.to_wkt() == 'LINESTRING M (2 0 2, 4 0 4)'
    subs = cast('gm.GeometryArray', lines.line_substring(2.0, 4.0, basis='m'))
    assert [piece.to_wkt() for piece in subs] == [sub.to_wkt()] * 2
    a = gm.from_wkt('POINT Z (0 0 0)')
    b = gm.from_wkt('POINT Z (1 1 1)')
    expected = 3**0.5
    assert gm.distance_3d(a, b) == pytest.approx(expected)
    pair = gm.GeometryArray([a, a])
    assert gm.distance_3d(pair, b) == pytest.approx([expected, expected])
    assert gm.distance_3d(pair, gm.GeometryArray([b, b])) == pytest.approx(
        [expected] * 2
    )
    with pytest.raises(ValueError, match='matching CRS'):
        gm.distance_3d(gm.from_wkt('POINT Z (0 0 0)', crs=4326), b)


def test_scalar_m_interpolation_accepts_a_station_column() -> None:
    line = gm.from_wkt('LINESTRING ZM (0 0 10 0, 10 0 20 100)')

    points = line.line_interpolate([25.0, 75.0], basis='m')

    assert isinstance(points, gm.GeometryArray)
    assert points.to_wkt() == [
        'POINT ZM (2.5 0 12.5 25)',
        'POINT ZM (7.5 0 17.5 75)',
    ]
    assert line.line_interpolate(np.array([25.0, 75.0]), basis='m').to_wkt() == (
        points.to_wkt()
    )

    non_monotonic = gm.from_wkt('LINESTRING M (0 0 0, 1 0 2, 2 0 1)')
    with pytest.raises(gm.InvalidGeometryError, match='monotonic'):
        non_monotonic.line_interpolate([0.5, 1.5], basis='m')


def test_distance_3d_generalized_over_linework() -> None:
    """distance_3d is the minimum 3D Euclidean distance over segment linework."""
    assert gm.distance_3d(gm.Point(0, 0, z=0), gm.Point(1, 2, z=2)) == 3.0
    assert (
        gm.distance_3d(
            gm.from_wkt('POINT Z (0 0 0)'),
            gm.from_wkt('LINESTRING Z (10 0 0, 10 10 0)'),
        )
        == 10.0
    )
    assert gm.distance_3d(
        gm.from_wkt('LINESTRING Z (0 0 0, 1 0 0)'),
        gm.from_wkt('LINESTRING Z (0 1 1, 0 1 -1)'),
    ) == pytest.approx(1.0)
    assert (
        gm.distance_3d(
            gm.from_wkt('POINT Z (5 -5 0)'),
            gm.from_wkt('POLYGON Z ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'),
        )
        == 5.0
    )
    assert (
        gm.distance_3d(
            gm.from_wkt('LINESTRING Z (0 0 0, 2 0 0)'),
            gm.from_wkt('LINESTRING Z (1 -1 0, 1 1 0)'),
        )
        == 0.0
    )
    with pytest.raises(ValueError, match='requires a Z ordinate'):
        gm.distance_3d(
            gm.from_wkt('LINESTRING (0 0, 1 1)'),
            gm.from_wkt('LINESTRING Z (0 0 0, 1 1 1)'),
        )


def test_distance_3d_indexed_xyzm_line_uses_z_and_ignores_m() -> None:
    line = gm.LineString(
        [(float(index), 0.0) for index in range(65)],
        z=[0.0] * 65,
        m=[float(index) * 1e12 for index in range(65)],
    )
    probe = gm.Point(32.0, 3.0, z=4.0, m=-1e150)

    assert gm.distance_3d(probe, line) == 5.0


def test_distance_3d_array_degrades_missing_z_rows() -> None:
    """Array rows without Z on every vertex degrade to nan; scalar still raises."""
    mixed = gm.GeometryArray([
        gm.from_wkt('POINT Z (0 0 0)'),
        gm.from_wkt('POINT (0 0)'),
    ])
    target = gm.from_wkt('POINT Z (1 1 1)')
    assert gm.distance_3d(mixed, target)[0] == pytest.approx(3**0.5)
    assert math.isnan(gm.distance_3d(mixed, target)[1])
    with pytest.raises(ValueError, match='requires a Z ordinate'):
        gm.distance_3d(mixed[1], target)


def test_distance_3d_scalar_broadcast_uses_prepared_fixed_operand() -> None:
    """Scalar-fixed array.distance_3d matches the row-wise scalar oracle."""
    long_line = gm.LineString([(float(i), 0.0, float(i) * 0.1) for i in range(2000)])
    probes = gm.GeometryArray([
        gm.Point(0.0, 5.0, z=0.0),
        gm.Point(100.0, 5.0, z=10.0),
        gm.Point(500.0, -3.0, z=50.0),
    ])
    expected = [gm.distance_3d(probe, long_line) for probe in probes]
    assert gm.distance_3d(probes, long_line) == pytest.approx(expected)


def test_zero_length_line_linear_referencing_degenerates_cleanly() -> None:
    degenerate = gm.LineString([(1, 1), (1, 1)])
    assert gm.equals(degenerate.line_interpolate(5.0), gm.Point(1, 1))
    assert gm.equals(degenerate.line_substring(0.0, 1.0), gm.Point(1, 1))
    assert degenerate.line_locate(gm.Point(9, 9)) == 0.0


def test_line_interpolate_keeps_subnormal_length_and_reports_length_overflow() -> None:
    tiny = math.ulp(0.0)
    subnormal = gm.LineString([(0.0, 0.0), (tiny, 0.0)])
    assert subnormal.line_interpolate(tiny).x == tiny

    maximum = sys.float_info.max
    with pytest.raises(gm.GeometryError, match='non-finite segment length'):
        gm.LineString([(-maximum, 0.0), (maximum, 0.0)]).line_interpolate(0.0)

    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.0)]),
        gm.LineString([(-maximum, 0.0), (maximum, 0.0)]),
    ])
    with pytest.raises(gm.GeometryError, match='non-finite segment length') as error:
        lines.line_interpolate(0.0)
    assert error.value.__notes__ == ['while processing array element 1']

    with pytest.raises(gm.GeometryError, match='total length must be finite'):
        gm.LineString([
            (0.0, 0.0),
            (1.0e308, 0.0),
            (0.0, 0.0),
            (1.0e308, 0.0),
        ]).line_interpolate(0.0)


def test_measured_lrs_rejects_non_monotone_m_and_handles_plateaus() -> None:
    non_monotone = gm.from_wkt('LINESTRING M (0 0 0, 1 0 2, 2 0 1)')
    with pytest.raises(ValueError, match='monotonic'):
        non_monotone.line_interpolate(1.5, basis='m')
    with pytest.raises(ValueError, match='monotonic'):
        non_monotone.line_locate(gm.Point(1, 0), basis='m')
    plateau = gm.from_wkt('LINESTRING M (0 0 0, 1 0 0, 2 0 2)')
    assert gm.equals(plateau.line_interpolate(0.0, basis='m'), gm.Point(0, 0))


def test_measured_lrs_keeps_zero_xy_vertices_multipart_windows_and_first_wins() -> None:
    zero_xy = gm.from_wkt('LINESTRING ZM (0 0 1 0, 0 0 2 1, 10 0 3 2)')
    assert zero_xy.line_interpolate(1.0, basis='m').to_wkt() == 'POINT ZM (0 0 2 1)'
    assert zero_xy.line_substring(1.0, 1.0, basis='m').to_wkt() == 'POINT ZM (0 0 2 1)'
    assert zero_xy.line_locate(gm.Point(0, 0), basis='m') == 0.0

    multipart = gm.from_wkt('MULTILINESTRING M ((0 0 0, 10 0 10), (20 0 20, 30 0 30))')
    assert multipart.line_interpolate(15.0, basis='m').to_wkt() == 'POINT M (15 0 15)'
    bad_boundary = gm.from_wkt(
        'MULTILINESTRING M ((0 0 0, 10 0 10), (20 0 9, 30 0 30))'
    )
    with pytest.raises(ValueError, match='monotonic'):
        bad_boundary.line_interpolate(10.0, basis='m')

    overlapping = gm.from_wkt('LINESTRING M (0 0 0, 10 0 10, 0 0 20)')
    assert overlapping.line_locate(gm.Point(5, 1), basis='m') == pytest.approx(5.0)


def test_measured_lrs_handles_opposite_sign_extreme_m_with_z_and_arrays() -> None:
    maximum = sys.float_info.max
    line = gm.from_wkt(
        f'LINESTRING ZM (0 0 10 {-maximum}, 2 0 20 {maximum})',
        crs=4326,
    )

    midpoint = cast('gm.Point', line.line_interpolate(0.0, basis='m'))
    assert (midpoint.x, midpoint.y, midpoint.z, midpoint.m) == (1.0, 0.0, 15.0, 0.0)
    substring = cast('gm.LineString', line.line_substring(-maximum, 0.0, basis='m'))
    assert list(substring.coords)[1] == (1.0, 0.0, 15.0, 0.0)
    assert next(iter(substring.coords)) == (0.0, 0.0, 10.0, -maximum)

    rows = gm.GeometryArray([line], crs=4326)
    array_midpoint = rows.line_interpolate(0.0, basis='m')[0]
    array_substring = rows.line_substring(-maximum, 0.0, basis='m')[0]
    assert (array_midpoint.x, array_midpoint.y, array_midpoint.z, array_midpoint.m) == (
        1.0,
        0.0,
        15.0,
        0.0,
    )
    assert list(array_substring.coords)[1] == (1.0, 0.0, 15.0, 0.0)


def test_linear_referencing_is_crs_aware_with_a_planar_escape() -> None:
    line = gm.LineString([(0, 0), (1, 0)], crs=4326)
    assert line.line_interpolate(50000).x == pytest.approx(0.4492, abs=0.001)
    assert line.line_interpolate(0.5, unit='planar').x == pytest.approx(0.5)
    here = gm.Point(0.5, 0, crs=4326)
    assert line.line_locate(here) == pytest.approx(55660, rel=0.001)
    assert line.line_locate(here, unit='planar') == pytest.approx(0.5)
    half = line.line_substring(0.0, 0.5, unit='planar')
    assert list(half.coords)[-1][0] == pytest.approx(0.5)
    assert line.line_interpolate(
        0.5, normalized=True, unit='meters'
    ).x == pytest.approx(0.5)
    normalized_half = line.line_substring(0.0, 0.5, normalized=True, unit='planar')
    assert list(normalized_half.coords)[-1][0] == pytest.approx(0.5)
    assert line.line_locate(here, normalized=True, unit='meters') == pytest.approx(0.5)
    assert [
        point.x for point in line.line_interpolate(count=3, unit='meters')
    ] == pytest.approx([0.0, 0.5, 1.0])


def test_normalized_lrs_selects_planar_metric_across_packed_and_gathered_rows() -> None:
    line = gm.LineString([(0, 0), (40, 60)], crs=4326)
    short = gm.LineString([(0, 0), (8, 12)], crs=4326)
    query = gm.Point(20, 30, crs=4326)

    midpoint = line.line_interpolate(0.5, normalized=True, unit='planar')
    assert (midpoint.x, midpoint.y) == pytest.approx((20, 30))
    substring = line.line_substring(0.25, 0.75, normalized=True, unit='planar')
    assert [tuple(coords[:2]) for coords in substring.coords] == [
        (10.0, 15.0),
        (30.0, 45.0),
    ]
    assert line.line_locate(query, normalized=True, unit='planar') == pytest.approx(0.5)
    assert [
        (point.x, point.y) for point in line.line_interpolate(count=3, unit='planar')
    ] == [(0.0, 0.0), (20.0, 30.0), (40.0, 60.0)]

    packed = gm.GeometryArray([line, short])
    packed_points = packed.line_interpolate(0.5, normalized=True, unit='planar')
    assert [(point.x, point.y) for point in packed_points] == [
        (20.0, 30.0),
        (4.0, 6.0),
    ]
    packed_substrings = packed.line_substring(
        0.25, 0.75, normalized=True, unit='planar'
    )
    assert [
        [tuple(coords[:2]) for coords in route.coords] for route in packed_substrings
    ] == [
        [(10.0, 15.0), (30.0, 45.0)],
        [(2.0, 3.0), (6.0, 9.0)],
    ]
    assert packed.line_locate(query, normalized=True, unit='planar') == pytest.approx([
        0.5,
        1.0,
    ])
    packed_samples = packed.line_interpolate(count=3, unit='planar')
    assert [[(point.x, point.y) for point in route] for route in packed_samples] == [
        [(0.0, 0.0), (20.0, 30.0), (40.0, 60.0)],
        [(0.0, 0.0), (4.0, 6.0), (8.0, 12.0)],
    ]

    nullable = gm.GeometryArray([line, None, short], crs=4326)
    gathered = nullable[[2, 1, 0, 2]]
    gathered_points = gathered.line_interpolate(0.5, normalized=True, unit='planar')
    assert [
        None if point is None else (point.x, point.y) for point in gathered_points
    ] == [(4.0, 6.0), None, (20.0, 30.0), (4.0, 6.0)]
    gathered_substrings = gathered.line_substring(
        0.25, 0.75, normalized=True, unit='planar'
    )
    assert [
        None if route is None else [tuple(coords[:2]) for coords in route.coords]
        for route in gathered_substrings
    ] == [
        [(2.0, 3.0), (6.0, 9.0)],
        None,
        [(10.0, 15.0), (30.0, 45.0)],
        [(2.0, 3.0), (6.0, 9.0)],
    ]
    gathered_locations = gathered.line_locate(query, normalized=True, unit='planar')
    assert np.isnan(gathered_locations[1])
    assert gathered_locations[[0, 2, 3]] == pytest.approx([1.0, 0.5, 1.0])
    gathered_samples = gathered.line_interpolate(count=3, unit='planar')
    assert [[(point.x, point.y) for point in route] for route in gathered_samples] == [
        [(0.0, 0.0), (4.0, 6.0), (8.0, 12.0)],
        [],
        [(0.0, 0.0), (20.0, 30.0), (40.0, 60.0)],
        [(0.0, 0.0), (4.0, 6.0), (8.0, 12.0)],
    ]

    with pytest.raises(ValueError, match='requires a CRS'):
        gm.LineString([(0, 0), (1, 1)]).line_interpolate(
            0.5, normalized=True, unit='meters'
        )
