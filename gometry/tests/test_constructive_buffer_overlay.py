"""Constructive and overlay operations — buffer/simplify/offset, cleanup,
orientation, normalize, clip, polygonize, summaries, and Z interpolation.
"""

import math
from typing import cast

import gometry as gm
import numpy as np
import pytest

shapely = pytest.importorskip(
    'shapely', reason='shapely is an optional oracle dependency'
)


def test_planar_buffer_simplify_and_polygon_overlay_are_rust_backed() -> None:
    left = gm.box(0, 0, 2, 2, crs=3857)
    right = gm.box(1, 1, 3, 3, crs=3857)
    line = gm.LineString([(0, 0), (1, 0.01), (2, 0)], crs=3857)
    multiline = gm.MultiLineString([[(0, 0), (1, 0)], [(2, 0), (1, 0)]], crs=3857)
    buffered = gm.Point(0, 0, crs=3857).buffer(1)
    flat_line_buffer = gm.LineString([(0, 0), (2, 0)]).buffer(1, cap_style='flat')
    square_line_buffer = gm.LineString([(0, 0), (2, 0)]).buffer(1, cap_style='square')
    bevel_corner_buffer = gm.LineString([(0, 0), (2, 0), (2, 2)]).buffer(
        0.5, join_style='bevel', quadrant_segments=4
    )
    miter_corner_buffer = gm.LineString([(0, 0), (2, 0), (2, 2)]).buffer(
        0.5, join_style='miter', quadrant_segments=4
    )
    offset_curve = gm.LineString(
        [(0, 0), (2, 0), (2, 2)], z=[0, 2, 4], crs=3857
    ).offset_curve(1)
    reverse_offset_curve = gm.LineString(
        [(0, 0), (2, 0), (2, 2)], crs=3857
    ).offset_curve(-1, join_style='miter')
    round_offset_curve = gm.LineString([(0, 0), (2, 0), (2, 2)], crs=3857).offset_curve(
        -1, quadrant_segments=2
    )
    simplified = line.simplify(0.1, method='dp')
    snapped = gm.snap(
        gm.LineString(
            [(0, 0), (0.9, 0.1), (2, 0)], z=[0, 2, 4], m=[10, 12, 14], crs=3857
        ),
        gm.Point(1, 0, crs=3857),
        0.25,
    )
    huge_snap = gm.snap(gm.LineString([(0, 0), (2e200, 0)]), gm.Point(1e200, 0), 6e199)
    shared = gm.shared_paths(
        gm.LineString([(0, 0), (1, 0), (2, 0)], crs=3857),
        gm.LineString([(1, 0), (0, 0), (2, 0)], crs=3857),
    )
    merged = multiline.line_merge()
    intersection = gm.intersection(left, right)
    unioned = gm.union(left, right)
    diff = gm.difference(left, right)
    xor = gm.symmetric_difference(left, right)
    all_union = gm.union_all([left, right])
    buffered_top_level = cast('gm.Geometry', gm.Point(0, 0).buffer(1))
    assert buffered.geometry_type == 'Polygon'
    assert buffered.crs == gm.CRS('EPSG:3857')
    assert buffered.area == pytest.approx(math.pi, rel=0.04)
    assert buffered_top_level.area == pytest.approx(buffered.area)
    geographic_buffer = cast('gm.Geometry', gm.Point(0, 0, crs=4326).buffer(100.0))
    assert geographic_buffer.geometry_type in ('Polygon', 'MultiPolygon')
    assert flat_line_buffer.area == pytest.approx(4)
    assert square_line_buffer.area == pytest.approx(8)
    assert bevel_corner_buffer.area < miter_corner_buffer.area
    assert offset_curve.to_wkt() == 'LINESTRING Z (0 1 0, 1 1 2, 1 2 4)'
    assert offset_curve.crs == gm.CRS('EPSG:3857')
    assert (
        cast('gm.Geometry', reverse_offset_curve).to_wkt()
        == 'LINESTRING (0 -1, 3 -1, 3 2)'
    )
    assert (
        cast('gm.Geometry', round_offset_curve).to_wkt()
        == 'LINESTRING (0 -1, 2 -1, 2.7071067811865475 -0.7071067811865476, 3 0, 3 2)'
    )
    assert (
        gm.LineString([(0, 0), (2, 0)]).offset_curve(1).to_wkt()
        == 'LINESTRING (0 1, 2 1)'
    )
    assert gm.LineString([(0, 0), (2, 0)], crs=3857).offset_curve(1).crs == gm.CRS(
        'EPSG:3857'
    )
    geographic_offset = gm.LineString([(0, 0), (2, 0)], crs=4326).offset_curve(1)
    assert geographic_offset.crs == gm.CRS('EPSG:4326')
    assert geographic_offset.geometry_type == 'LineString'
    assert simplified.to_wkt() == 'LINESTRING (0 0, 2 0)'
    assert snapped.to_wkt() == 'LINESTRING ZM (0 0 0 10, 1 0 2 12, 2 0 4 14)'
    assert snapped.crs == gm.CRS('EPSG:3857')
    assert shared.geometry_type == 'GeometryCollection'
    assert shared.crs == gm.CRS('EPSG:3857')
    assert [part.to_wkt() for part in shared.parts] == [
        'MULTILINESTRING ((1 0, 2 0))',
        'MULTILINESTRING ((0 0, 1 0))',
    ]
    assert merged.to_wkt() == 'LINESTRING (0 0, 1 0, 2 0)'
    assert merged.crs == gm.CRS('EPSG:3857')
    assert intersection.area == 1
    assert unioned.area == 7
    assert all_union.area == 7
    assert diff.area == 3
    assert xor.area == 6
    overlay_left = gm.GeometryArray([left, gm.box(10, 10, 12, 12, crs=3857)])
    overlay_right = gm.GeometryArray([right, gm.box(11, 11, 13, 13, crs=3857)])
    overlay_intersections = cast(
        'gm.GeometryArray', gm.intersection(overlay_left, overlay_right)
    )
    overlay_unions = cast('gm.GeometryArray', gm.union(overlay_left, overlay_right))
    overlay_differences = cast(
        'gm.GeometryArray', gm.difference(overlay_left, overlay_right)
    )
    overlay_xors = cast(
        'gm.GeometryArray', gm.symmetric_difference(overlay_left, overlay_right)
    )
    scalar_left_intersections = cast(
        'gm.GeometryArray', gm.intersection(left, overlay_right)
    )
    assert overlay_intersections.crs == gm.CRS('EPSG:3857')
    np.testing.assert_allclose(
        [item.area for item in overlay_intersections], [1.0, 1.0]
    )
    np.testing.assert_allclose([item.area for item in overlay_unions], [7.0, 7.0])
    np.testing.assert_allclose([item.area for item in overlay_differences], [3.0, 3.0])
    np.testing.assert_allclose([item.area for item in overlay_xors], [6.0, 6.0])
    np.testing.assert_allclose(
        [item.area for item in scalar_left_intersections], [1.0, 0.0]
    )
    assert [item.area for item in gm.intersection(overlay_left, overlay_right)] == [
        1.0,
        1.0,
    ]
    assert [item.area for item in gm.union(overlay_left, overlay_right)] == [7.0, 7.0]
    np.testing.assert_allclose(
        [item.area for item in gm.difference(overlay_left, right)], [3.0, 4.0]
    )
    assert [
        item.area for item in gm.symmetric_difference(overlay_left, overlay_right)
    ] == [6.0, 6.0]
    with pytest.raises(ValueError, match='same length'):
        gm.intersection(overlay_left, gm.GeometryArray([right]))
    with pytest.raises(
        gm.CRSMismatchError, match='intersection requires matching CRS metadata'
    ):
        gm.intersection(left, gm.box(1, 1, 3, 3, crs=4326))
    with pytest.raises(
        gm.CRSMismatchError, match='union requires matching CRS metadata'
    ):
        gm.union(
            overlay_left,
            gm.GeometryArray([
                gm.box(1, 1, 3, 3, crs=4326),
                gm.box(11, 11, 13, 13, crs=4326),
            ]),
        )
    with pytest.raises(
        gm.CRSMismatchError, match='GeometryArray requires one shared CRS'
    ):
        gm.union_all([left, gm.box(1, 1, 3, 3, crs=4326)])
    with pytest.raises(
        gm.CRSMismatchError, match='GeometryArray requires one shared CRS'
    ):
        gm.union_all([gm.box(0, 0, 1, 1), left])
    assert gm.union_all([gm.box(0, 0, 1, 1, crs=3857), left]).crs == gm.CRS('EPSG:3857')
    simplified_array = cast(
        'gm.GeometryArray', gm.GeometryArray([line]).simplify(0.1, method='dp')
    )
    assert simplified_array[0].to_wkt() == 'LINESTRING (0 0, 2 0)'
    snapped_array = cast(
        'gm.GeometryArray',
        gm.snap(gm.GeometryArray([line]), gm.Point(1, 0, crs=3857), 0.1),
    )
    assert snapped_array[0].to_wkt() == 'LINESTRING (0 0, 1 0, 2 0)'

    assert huge_snap.coords.to_nested() == [(0.0, 0.0), (1e200, 0.0), (2e200, 0.0)]
    assert (
        cast('gm.Geometry', gm.snap(line, gm.Point(1, 0, crs=3857), 0.1)).to_wkt()
        == 'LINESTRING (0 0, 1 0, 2 0)'
    )
    pairwise_snapped = cast(
        'gm.GeometryArray',
        gm.snap(
            gm.GeometryArray([
                gm.LineString([(0, 0), (0.9, 0), (2, 0)]),
                gm.LineString([(0, 0), (1.9, 0), (3, 0)]),
            ]),
            gm.GeometryArray([gm.Point(1, 0), gm.Point(2, 0)]),
            0.2,
        ),
    )
    scalar_snapped = cast(
        'gm.GeometryArray',
        gm.snap(
            line,
            gm.GeometryArray([gm.Point(1, 0, crs=3857), gm.Point(2, 0, crs=3857)]),
            0.1,
        ),
    )
    assert [item.to_wkt() for item in pairwise_snapped] == [
        'LINESTRING (0 0, 1 0, 2 0)',
        'LINESTRING (0 0, 2 0, 3 0)',
    ]
    assert [item.to_wkt() for item in scalar_snapped] == [
        'LINESTRING (0 0, 1 0, 2 0)',
        'LINESTRING (0 0, 1 0.01, 2 0)',
    ]
    with pytest.raises(ValueError, match='same length'):
        gm.snap(gm.GeometryArray([line, line]), gm.GeometryArray([gm.Point(1, 0)]), 0.1)
    with pytest.raises(
        gm.CRSMismatchError, match='snap requires matching CRS metadata'
    ):
        gm.snap(line, gm.Point(1, 0, crs=4326), 0.1)
    shared_array = cast(
        'gm.GeometryArray',
        gm.shared_paths(
            gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]),
            gm.LineString([(0, 0), (1, 0)]),
        ),
    )
    assert [part.to_wkt() for part in shared_array[0].parts] == [
        'MULTILINESTRING ((0 0, 1 0))',
        'MULTILINESTRING EMPTY',
    ]
    pairwise_shared = cast(
        'gm.GeometryArray',
        gm.shared_paths(
            gm.GeometryArray([
                gm.LineString([(0, 0), (2, 0)]),
                gm.LineString([(10, 0), (12, 0)]),
            ]),
            gm.GeometryArray([
                gm.LineString([(0, 0), (1, 0)]),
                gm.LineString([(12, 0), (11, 0)]),
            ]),
        ),
    )
    assert [part.to_wkt() for part in pairwise_shared[0].parts] == [
        'MULTILINESTRING ((0 0, 1 0))',
        'MULTILINESTRING EMPTY',
    ]
    assert [part.to_wkt() for part in pairwise_shared[1].parts] == [
        'MULTILINESTRING EMPTY',
        'MULTILINESTRING ((11 0, 12 0))',
    ]
    with pytest.raises(ValueError, match='same length'):
        gm.shared_paths(gm.GeometryArray([line, line]), gm.GeometryArray([line]))
    with pytest.raises(
        gm.CRSMismatchError, match='shared_paths requires matching CRS metadata'
    ):
        gm.shared_paths(line, gm.LineString([(0, 0), (1, 0)], crs=4326))
    buffered_array = cast(
        'gm.GeometryArray', gm.GeometryArray([gm.Point(0, 0)]).buffer(1)
    )
    assert buffered_array[0].area == pytest.approx(buffered.area)
    geographic_array_buffer = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).buffer(100.0)
    assert geographic_array_buffer[0].geometry_type in ('Polygon', 'MultiPolygon')
    styled_array = cast(
        'gm.GeometryArray',
        gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]).buffer(1, cap_style='flat'),
    )
    assert styled_array[0].area == pytest.approx(flat_line_buffer.area)
    offset_array = cast(
        'gm.GeometryArray',
        gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]).offset_curve(1),
    )
    assert offset_array[0].to_wkt() == 'LINESTRING (0 1, 2 1)'
    geographic_offset_array = gm.GeometryArray([
        gm.LineString([(0, 0), (2, 0)], crs=4326)
    ]).offset_curve(1)
    assert geographic_offset_array.crs == gm.CRS('EPSG:4326')
    assert geographic_offset_array[0].geometry_type == 'LineString'
    offset_array_from_function = cast(
        'gm.GeometryArray',
        gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]).offset_curve(1),
    )
    assert offset_array_from_function[0].to_wkt() == offset_array[0].to_wkt()
    merged_array = cast('gm.GeometryArray', gm.GeometryArray([multiline]).line_merge())
    assert merged_array[0].to_wkt() == merged.to_wkt()
    point_union = gm.union(gm.Point(0, 0, crs=3857), right)
    assert point_union.geometry_type == 'GeometryCollection'
    assert point_union.crs == gm.CRS('EPSG:3857')
    with pytest.raises(TypeError, match='LineString or MultiLineString'):
        left.line_merge()
    with pytest.raises(ValueError, match='unknown buffer cap_style'):
        gm.Point(0, 0).buffer(1, cap_style='bad')
    with pytest.raises(ValueError, match='unknown buffer join_style'):
        gm.Point(0, 0).buffer(1, join_style='bad')
    with pytest.raises(ValueError, match='quadrant_segments'):
        gm.Point(0, 0).buffer(1, quadrant_segments=0)
    with pytest.raises(TypeError, match='LineString or MultiLineString'):
        gm.Point(0, 0).offset_curve(1)
    with pytest.raises(ValueError, match='distance must be finite'):
        gm.LineString([(0, 0), (1, 0)]).offset_curve(math.inf)
    with pytest.raises(TypeError, match='LineString or MultiLineString'):
        gm.shared_paths(left, line)
    with pytest.raises(ValueError, match='non-negative finite'):
        gm.snap(line, gm.Point(1, 0, crs=3857), -1)
    overflow_offset = gm.LineString([(1.5e308, 0.0), (1.5e308, 1.0)]).offset_curve(
        -1.5e308
    )
    assert overflow_offset.is_empty


def test_multipart_buffer_normalizes_areal_union_before_erosion() -> None:
    left = gm.box(0, 0, 2, 4)
    right = gm.box(2, 0, 4, 4)
    multipart = gm.MultiPolygon([left, right])
    union = gm.union(left, right)

    eroded = multipart.buffer(-0.5)
    expected = union.buffer(-0.5)

    assert eroded.area == pytest.approx(9.0)
    assert gm.equals(eroded, expected)
    assert gm.equals(multipart.buffer(0.0), union)

    nested = gm.GeometryCollection([left, gm.GeometryCollection([right])])
    assert gm.equals(nested.buffer(-0.5), expected)


def test_multipoint_buffer_repairs_neighbor_float_collision() -> None:
    result = gm.from_wkt('MULTIPOINT((3 2),(4 1))').buffer(1.0)

    assert not result.is_empty
    assert result.is_valid
    assert shapely.from_wkb(result.to_wkb()).is_valid


def test_union_of_point_buffers_repairs_neighbor_float_collision() -> None:
    result = gm.union(
        gm.Point(3, 2).buffer(1.0),
        gm.Point(4, 1).buffer(1.0),
    )

    assert not result.is_empty
    assert result.is_valid
    assert shapely.from_wkb(result.to_wkb()).is_valid


def test_array_disjoint_overlay_handles_mixed_ordinate_axes() -> None:
    z_line = gm.LineString([(0, 0), (1, 0)], z=[1, 2])
    xy_line = gm.LineString([(10, 0), (11, 0)])

    forward = cast('gm.GeometryArray', gm.union(gm.GeometryArray([z_line]), xy_line))
    reverse = cast('gm.GeometryArray', gm.union(xy_line, gm.GeometryArray([z_line])))

    assert forward[0].coordinate_axes == 'XY'
    assert reverse[0].coordinate_axes == 'XY'
    assert sorted(forward[0].coords.to_nested()) == sorted(
        reverse[0].coords.to_nested()
    )


# A polygon whose envelope misses the line, one whose envelope overlaps it
# without touching it (a C around the line's end), and no polygon at all.
@pytest.mark.parametrize(
    'polygon',
    [
        gm.box(1, 1, 3, 4),
        gm.Polygon([
            (4, 1),
            (6, 1),
            (6, -1),
            (4, -1),
            (4, -0.5),
            (5.5, -0.5),
            (5.5, 0.5),
            (4, 0.5),
        ]),
        None,
    ],
)
def test_overlay_keeps_line_vertices_whatever_else_shares_the_arrangement(
    polygon: gm.Geometry | None,
) -> None:
    line = gm.LineString([(0, 0), (2, 0), (5, 0)])
    operands = [line] if polygon is None else [line, polygon]
    for result in (
        gm.union_all(gm.GeometryArray(operands)),
        gm.symmetric_difference_all(gm.GeometryArray(operands)),
        gm.difference(line, polygon or gm.GeometryCollection([])),
    ):
        assert 'LINESTRING (0 0, 2 0, 5 0)' in str(result)


def test_buffer_rejects_nonfinite_derived_coordinates_without_empty_fallback() -> None:
    polygon = gm.Polygon([
        (-1e308, -1e308),
        (1e308, -1e308),
        (1e308, 1e308),
        (-1e308, 1e308),
        (-1e308, -1e308),
    ])
    with pytest.raises(gm.InvalidGeometryError, match='non-finite'):
        polygon.buffer(1e308)
    with pytest.raises(gm.InvalidGeometryError, match='non-finite'):
        gm.GeometryArray([polygon]).buffer(1e308)
    assert polygon.buffer(0).to_wkt() == polygon.to_wkt()

    line = gm.LineString([(1e308, 0.0), (1e308, 1.0)])
    with pytest.raises(gm.InvalidGeometryError, match='non-finite'):
        line.buffer(1e308, side='right')
    with pytest.raises(gm.InvalidGeometryError, match='non-finite'):
        gm.GeometryArray([line]).buffer(1e308, side='right')

    ordinary = gm.Point(1e150, -1e150).buffer(1e150)
    assert not ordinary.is_empty
    assert all(np.isfinite(value) for point in ordinary.coords for value in point[:2])


def test_buffer_keeps_a_finite_extreme_edge_after_raw_delta_overflow() -> None:
    """The removed raw-delta guard must not reject a representable buffer.

    Mutation: restore that guard and the overflowing ``end - start`` below
    rejects before this production-path assertion.
    """
    start = -1.5e308
    end = 1.5e308
    radius = 1e300
    assert not math.isfinite(end - start)

    line = gm.LineString([(start, 0.0), (end, 0.0)])
    buffered = line.buffer(radius)
    sided = line.buffer(radius, side='left')
    offset = line.offset_curve(radius)
    for result in (buffered, sided, offset):
        assert not result.is_empty
        assert all(np.isfinite(value) for point in result.coords for value in point[:2])

    # Measure the boundary in a frame whose origin is the source's left
    # endpoint. The fallback residual is the independent reference for the
    # old overflowing subtraction, not a finite-only assertion.
    _, exponent = math.frexp(max(abs(start), abs(end), radius))
    scale = math.ldexp(1.0, -exponent)

    def residual(value: float, origin: float) -> float:
        difference = value - origin
        return (
            difference * scale
            if math.isfinite(difference)
            else value * scale - origin * scale
        )

    source_span = residual(end, start)
    scaled_radius = radius * scale
    for point in buffered.coords:
        x = residual(point[0], start)
        y = point[1] * scale
        distance = math.hypot(x - min(max(x, 0.0), source_span), y)
        assert distance <= scaled_radius + 32 * np.finfo(float).eps


def test_buffer_accepts_tiny_mixed_scale_and_oriented_edges() -> None:
    cases = [
        ([(-1.5e308, 0.0), (1.5e308, 0.0)], 1e300),
        ([(1.5e308, -1.5e308), (1.5e308, 1.5e308)], 1e300),
        ([(-1.5e308, -1.5e308), (1.5e308, 1.5e308)], 1e300),
        ([(0.0, 0.0), (5e-324, 0.0)], 5e-324),
        ([(1e308, 1e308), (1e308, 1.0000000000000002e308)], 1e292),
    ]
    for coordinates, radius in cases:
        line = gm.LineString(coordinates)
        for cap_style in ('round', 'flat', 'square'):
            result = line.buffer(radius, cap_style=cap_style, quadrant_segments=4)
            assert not result.is_empty
            assert all(
                np.isfinite(value) for point in result.coords for value in point[:2]
            )

    joined = gm.LineString([
        (-1.5e308, 0.0),
        (0.0, 1e300),
        (1.5e308, 0.0),
    ])
    for join_style in ('round', 'bevel', 'miter'):
        result = joined.buffer(1e299, join_style=join_style, quadrant_segments=4)
        assert not result.is_empty
        assert all(np.isfinite(value) for point in result.coords for value in point[:2])


def test_buffer_keeps_extreme_polygon_shell_and_hole_finite() -> None:
    shell = [
        (-1.5e308, -1.5e308),
        (1.5e308, -1.5e308),
        (1.5e308, 1.5e308),
        (-1.5e308, 1.5e308),
        (-1.5e308, -1.5e308),
    ]
    hole = [
        (-1e301, -1e301),
        (1e301, -1e301),
        (1e301, 1e301),
        (-1e301, 1e301),
        (-1e301, -1e301),
    ]
    polygon = gm.Polygon(shell, holes=[hole])
    for distance in (1e300, -1e300):
        result = polygon.buffer(distance)
        assert not result.is_empty
        assert all(np.isfinite(value) for point in result.coords for value in point[:2])


def test_snap_dedups_only_identical_projected_positions() -> None:
    # Two reference points 5e-13 apart on the unit segment both lie exactly on
    # it; even at tolerance 0 they insert as distinct vertices rather than
    # collapsing under a fixed parametric epsilon.
    line = gm.LineString([(0, 0), (1, 0)], crs=3857)
    reference = gm.MultiPoint([(0.5, 0), (0.5000000000005, 0)], crs=3857)
    snapped = cast('gm.Geometry', gm.snap(line, reference, 0.0))
    assert snapped.num_coordinates == 4
    coords = np.asarray(snapped.coords, dtype=np.float64)
    assert coords[1][0] == 0.5
    assert coords[2][0] == 0.5000000000005


def test_snap_dense_targets_snap_each_source_vertex_then_deduplicate() -> None:
    source = [(0.1 * index, 0.0) for index in range(64)]
    targets = [(source[index][0] + 0.1, 0.1) for index in range(0, 64, 16)]
    reference = gm.MultiPoint(targets)
    groups = (
        ((0, 1, 2, 3), 0),
        ((15, 16, 17, 18, 19), 1),
        ((31, 32, 33, 34, 35), 2),
        ((47, 48, 49, 50, 51), 3),
    )
    for indices, target_index in groups:
        for index in indices:
            point = gm.snap(gm.Point(*source[index]), reference, 0.25)
            assert tuple(point.coords[0]) == targets[target_index], index

    line = gm.LineString(source)
    snapped = cast('gm.Geometry', gm.snap(line, reference, 0.25))
    expected = (
        targets[0],
        *source[4:15],
        targets[1],
        *source[20:31],
        targets[2],
        *source[36:47],
        targets[3],
        *source[52:64],
    )
    assert snapped.num_coordinates == 49
    assert tuple(map(tuple, snapped.coords)) == expected

    rows = gm.snap(gm.GeometryArray([line]), reference, 0.25)
    assert rows[0].num_coordinates == 49
    assert tuple(map(tuple, rows[0].coords)) == expected


def test_snap_keeps_two_endpoints_when_line_vertices_collapse_to_one_xy() -> None:
    line = gm.LineString(
        [(0.0, 0.0), (0.1, 0.0), (0.2, 0.0)],
        z=[10.0, 11.0, 12.0],
        m=[100.0, 101.0, 102.0],
    )
    snapped = gm.snap(line, gm.Point(0.1, 0.1), 0.25)
    assert snapped.num_coordinates == 2
    assert tuple(map(tuple, snapped.coords)) == (
        (0.1, 0.1, 10.0, 100.0),
        (0.1, 0.1, 12.0, 102.0),
    )


def test_snap_includes_a_vertex_at_exactly_the_tolerance() -> None:
    point = gm.Point(0.0, 0.0)
    reference = gm.Point(3.0, 4.0)
    assert tuple(gm.snap(point, reference, 5.0).coords[0]) == (3.0, 4.0)
    assert tuple(gm.snap(point, reference, math.nextafter(5.0, 0.0)).coords[0]) == (
        0.0,
        0.0,
    )


def test_snap_moved_vertices_keep_their_source_zm() -> None:
    source = gm.from_wkt('LINESTRING ZM (0 0 11 101, 10 0 22 202)')
    reference = gm.from_wkt('POINT ZM (0.5 0 900 9000)')
    snapped = gm.snap(source, reference, 0.6)
    assert snapped.has_z and snapped.has_m
    assert tuple(map(tuple, snapped.coords)) == (
        (0.5, 0.0, 11.0, 101.0),
        (10.0, 0.0, 22.0, 202.0),
    )


def test_snap_inserts_segment_vertices_in_order_with_interpolated_zm() -> None:
    source = gm.from_wkt('LINESTRING ZM (0 0 10 100, 10 0 20 200)')
    reference = gm.from_wkt(
        'MULTIPOINT ZM ((7 0.1 700 7000), (5 0.1 500 5000), '
        '(5 -0.1 501 5001), (3 0.1 300 3000))'
    )
    snapped = gm.snap(source, reference, 0.2)
    assert tuple(map(tuple, snapped.coords)) == (
        (0.0, 0.0, 10.0, 100.0),
        (3.0, 0.1, 13.0, 130.0),
        (5.0, -0.1, 15.0, 150.0),
        (5.0, 0.1, 15.0, 150.0),
        (7.0, 0.1, 17.0, 170.0),
        (10.0, 0.0, 20.0, 200.0),
    )


def test_snap_does_not_insert_targets_within_tolerance_of_segment_endpoints() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 0.0)])
    reference = gm.MultiPoint([(0.1, 0.1), (0.0, 0.01)])
    snapped = gm.snap(line, reference, 0.2)
    assert tuple(map(tuple, snapped.coords)) == ((0.0, 0.01), (10.0, 0.0))


def test_binary_array_dispatch_preserves_operand_order_and_missing_masks() -> None:
    scalar = gm.box(1, 1, 4, 4)
    values = gm.GeometryArray([gm.box(0, 0, 2, 2), None, gm.box(3, 3, 5, 5)])

    scalar_right = cast('gm.GeometryArray', gm.difference(values, scalar))
    scalar_left = cast('gm.GeometryArray', gm.difference(scalar, values))
    assert scalar_right.is_missing.tolist() == [False, True, False]
    assert scalar_left.is_missing.tolist() == [False, True, False]
    np.testing.assert_allclose([scalar_right[0].area, scalar_right[2].area], [3.0, 3.0])
    np.testing.assert_allclose([scalar_left[0].area, scalar_left[2].area], [8.0, 8.0])
    assert scalar_right[1] is None and scalar_left[1] is None

    left = gm.GeometryArray([gm.box(0, 0, 2, 2), None, gm.box(0, 0, 2, 2)])
    right = gm.GeometryArray([gm.box(1, 1, 4, 4), gm.box(0, 0, 1, 1), None])
    left_minus_right = cast('gm.GeometryArray', gm.difference(left, right))
    right_minus_left = cast('gm.GeometryArray', gm.difference(right, left))
    assert left_minus_right.is_missing.tolist() == [False, True, True]
    assert right_minus_left.is_missing.tolist() == [False, True, True]
    assert left_minus_right[0].area == pytest.approx(3.0)
    assert right_minus_left[0].area == pytest.approx(8.0)


def test_intersection_all_and_symmetric_difference_all_reductions() -> None:
    import functools
    import warnings

    boxes = [gm.box(0, 0, 4, 4), gm.box(1, 1, 5, 5), gm.box(2, 0, 6, 3)]
    s_boxes = [
        shapely.box(0, 0, 4, 4),
        shapely.box(1, 1, 5, 5),
        shapely.box(2, 0, 6, 3),
    ]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        oracle_intersection = shapely.intersection_all(s_boxes).area
        oracle_symdiff = shapely.symmetric_difference_all(s_boxes).area
    inter = gm.intersection_all(boxes)
    manual = functools.reduce(gm.intersection, boxes)
    assert inter.area == pytest.approx(manual.area)
    assert inter.area == pytest.approx(oracle_intersection)
    xor = gm.symmetric_difference_all(boxes)
    assert xor.area == pytest.approx(oracle_symdiff)
    assert (
        gm.symmetric_difference_all([
            gm.box(0, 0, 2, 2),
            gm.box(0, 0, 2, 2),
            gm.box(1, 1, 3, 3),
        ]).to_wkt()
        == 'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'
    )
    assert gm.intersection_all([gm.box(0, 0, 1, 1)]).area == 1
    assert (
        gm.intersection_all([
            gm.box(0, 0, 4, 4),
            gm.from_wkt('LINESTRING(1 1,3 3)'),
        ]).to_wkt()
        == 'LINESTRING (1 1, 3 3)'
    )
    assert (
        gm.intersection_all([gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6)]).to_wkt()
        == 'POLYGON EMPTY'
    )
    with pytest.raises(
        gm.InvalidGeometryError, match='intersection_all requires at least one geometry'
    ):
        gm.intersection_all([])
    with pytest.raises(gm.InvalidGeometryError, match='requires at least one geometry'):
        gm.symmetric_difference_all([])
    with pytest.raises(gm.CRSMismatchError):
        gm.intersection_all([
            gm.box(0, 0, 4, 4, crs=3857),
            gm.box(1, 1, 5, 5, crs=4326),
        ])
    with pytest.raises(gm.CRSMismatchError):
        gm.intersection_all([gm.box(0, 0, 4, 4, crs=3857), gm.box(1, 1, 5, 5)])
    assert gm.intersection_all([
        gm.box(0, 0, 4, 4, crs=3857),
        gm.box(1, 1, 5, 5, crs=3857),
    ]).crs == gm.CRS('EPSG:3857')


def test_cleanup_and_segmentize_are_rust_backed() -> None:
    line = gm.LineString([(0, 0), (0, 0), (3, 0)], z=[0, 0, 6], m=[1, 1, 4], crs=4326)
    polygon = gm.Polygon([(0, 0), (2, 0), (2, 0), (2, 2), (0, 0)], crs=4326)
    array = gm.GeometryArray([line, polygon])
    cleaned = line.remove_repeated_points()
    cleaned_array = cast('gm.GeometryArray', array.remove_repeated_points())
    tolerant = gm.LineString([(0, 0), (0.05, 0), (1, 0)]).remove_repeated_points(
        tolerance=0.1
    )
    collapsed = gm.LineString([(0, 0), (0, 0)]).remove_repeated_points()
    snapped = gm.snap(gm.LineString([(0, 0), (0.1, 0)]), gm.Point(0, 0), 1.0)
    # `line` is EPSG:4326, where `segmentize` measures along the ellipsoid;
    # this assertion is about Z/M carry through the subdivision, so it asks for
    # coordinate-space subdivision explicitly.
    segmented = line.remove_repeated_points().segmentize(1.0, unit='planar')
    segmented_array = cast(
        'gm.GeometryArray',
        gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]).segmentize(0.5),
    )
    assert cleaned.to_wkt() == 'LINESTRING ZM (0 0 0 1, 3 0 6 4)'
    assert cleaned.crs == gm.CRS('EPSG:4326')
    assert cleaned_array[0].to_wkt() == cleaned.to_wkt()
    assert cleaned_array[1].to_wkt() == 'POLYGON ((0 0, 2 0, 2 2, 0 0))'
    assert tolerant.to_wkt() == 'LINESTRING (0 0, 1 0)'
    assert collapsed.geometry_type == 'LineString'
    assert collapsed.coords.to_nested() == [(0.0, 0.0), (0.0, 0.0)]
    assert snapped.coords.to_nested() == [(0.0, 0.0), (0.0, 0.0)]
    huge_tolerance = gm.LineString([(0, 0), (1e200, 0), (2e200, 0)])
    huge_cleaned = huge_tolerance.remove_repeated_points(tolerance=1.5e200)
    assert huge_cleaned.coords.to_nested() == [(0.0, 0.0), (2e200, 0.0)]
    assert segmented.to_wkt() == 'LINESTRING ZM (0 0 0 1, 1 0 2 2, 2 0 4 3, 3 0 6 4)'
    assert segmented_array[0].to_wkt() == 'LINESTRING (0 0, 0.5 0, 1 0, 1.5 0, 2 0)'
    with pytest.raises(ValueError, match='positive finite'):
        line.segmentize(0)


def test_densify_is_rust_backed() -> None:
    line = gm.LineString([(0, 0), (3, 0)], z=[0, 6], m=[1, 4], crs=4326)
    densified = line.segmentize(fraction=0.5)
    densified_array = cast(
        'gm.GeometryArray',
        gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]).segmentize(fraction=0.5),
    )
    assert densified.to_wkt() == 'LINESTRING ZM (0 0 0 1, 1.5 0 3 2.5, 3 0 6 4)'
    assert densified_array[0].to_wkt() == 'LINESTRING (0 0, 1 0, 2 0)'
    with pytest.raises(
        gm.GeometryError, match=r'fraction must be in \(0, 1\], got 1.5'
    ):
        line.segmentize(fraction=1.5)


def test_convex_buffer_fast_path_matches_the_general_engine() -> None:
    square = gm.box(0, 0, 1, 1)
    buffered = square.buffer(0.5, quadrant_segments=2)
    assert buffered.is_valid
    assert gm.covers(buffered, square)
    corners = 0.5 * 8 * 0.5**2 * math.sin(math.pi / 4)
    assert buffered.area == pytest.approx(1 + 4 * 0.5 + corners, rel=1e-12)
    notched = gm.Polygon([
        (0, 0),
        (0.45, 0),
        (0.5, 0.2),
        (0.55, 0),
        (1, 0),
        (1, 1),
        (0, 1),
    ])
    buffered_notched = notched.buffer(0.3)
    assert buffered_notched.is_valid
    assert gm.covers(buffered_notched, notched)
    assert square.buffer(-0.2).area == pytest.approx(0.36, rel=1e-06)
    assert square.buffer(0.5, join_style='miter').is_valid


def test_simplify_preserve_topology_default() -> None:
    tricky = gm.from_wkt('POLYGON ((0 0, 10 0, 10 1, 5 0.4, 0 1, 0 0))')
    raw = tricky.simplify(2.0, method='dp', preserve_topology=False)
    assert raw.to_wkt() == 'POLYGON EMPTY'
    safe = tricky.simplify(2.0, method='dp')
    assert safe.is_valid and (not safe.is_empty)
    assert isinstance(safe, gm.Polygon)
    folded = gm.from_wkt(
        'POLYGON ((5.574 4.898, 4.654 7.662, 4.27 7.637, 1.18 8.806, 4.281 6.734, 2.816 4.456, 1.643 4.123, 5.928 1.518, 6.857 0.603, 5.574 4.898))'
    )
    assert not folded.simplify(1.5, method='dp', preserve_topology=False).is_valid
    assert folded.simplify(1.5, method='dp').is_valid
    original = set(tricky.coords)
    assert set(safe.coords) <= original
    wiggly = gm.LineString([(0, 0), (1, 0.1), (2, -0.1), (3, 0)])
    assert (
        wiggly.simplify(0.5, method='dp').to_wkt()
        == wiggly.simplify(0.5, method='dp', preserve_topology=False).to_wkt()
    )
    zigzag = gm.LineString([(0, 0), (10, 0), (10, 2), (1, 0.5), (0, 3)])
    assert zigzag.is_simple
    assert zigzag.simplify(2.0, method='dp').is_simple
    batch = gm.GeometryArray([tricky, gm.box(0, 0, 1, 1)]).simplify(2.0, method='dp')
    assert all(g.is_valid for g in batch)


def test_snap_to_grid_repair_guarantees_validity() -> None:
    poly = gm.from_wkt('POLYGON ((0 0, 4.1 0, 0.1 3, 4 3, 0 0))')
    snapped = poly.snap_to_grid(1)
    assert not snapped.is_valid
    fixed = poly.snap_to_grid(1, repair=True)
    assert fixed.is_valid
    assert isinstance(fixed, gm.MultiPolygon)
    values = [*fixed.coords.x, *fixed.coords.y]
    assert all(v is not None and v == round(v) for v in values)
    square = gm.box(0.2, 0.2, 3.8, 2.7)
    assert (
        square.snap_to_grid(1, repair=True).to_wkt() == square.snap_to_grid(1).to_wkt()
    )
    batch = gm.GeometryArray([poly, square]).snap_to_grid(1, repair=True)
    assert all(g.is_valid for g in batch)


def test_repair_strips_antennas_and_splits_pinches() -> None:
    spike = gm.from_wkt('POLYGON ((5 1, 2 2, 1 4, 5 1, 3 2, 5 1))')
    repaired = spike.repair()
    assert repaired.is_valid
    assert repaired.normalize().to_wkt() == 'POLYGON ((1 4, 2 2, 5 1, 1 4))'
    pinched = gm.from_wkt('POLYGON ((1 3, 2 1, 2 2, 3 3, 2 1, 4 4, 1 3))')
    repaired = pinched.repair()
    assert repaired.is_valid
    assert repaired.to_wkt() == 'POLYGON ((1 3, 2 1, 4 4, 1 3), (2 1, 2 2, 3 3, 2 1))'


def test_repair_nodes_crossings_canonically() -> None:
    bowtie = gm.from_wkt(
        'POLYGON ((1.2967700716400383 1.1716548052334819, 4.978224177552314 2.35131753761224, 4.182307256371944 2.3817660434966745, 3.19534070272081 0.7530821201176197, 1.2967700716400383 1.1716548052334819))'
    )
    repaired = bowtie.repair()
    assert repaired.is_valid
    coords = [
        (3.4686264044857955, 2.6965698344637157),
        (4.341082584482902, 2.7040938616555006),
        (2.108913376298025, 4.36479492736843),
        (0.6907302900966644, 3.7485047782239267),
        (3.0079284117261635, 1.1004012882351981),
        (1.628715629487102, 3.7673454968197815),
    ]
    rounded_crossing = gm.Polygon(coords)
    assert rounded_crossing.is_valid
    assert not rounded_crossing.snap_to_grid(1).is_valid
    repaired_crossing = rounded_crossing.snap_to_grid(1, repair=True)
    assert repaired_crossing.is_valid
    assert isinstance(repaired_crossing, gm.MultiPolygon)


def test_minimum_clearance_line_realizes_the_clearance() -> None:
    square = gm.box(0, 0, 3, 2, crs=3857)
    line = square.minimum_clearance_line()
    assert isinstance(line, gm.LineString)
    assert line.crs == gm.CRS('EPSG:3857')
    assert line.length == pytest.approx(square.minimum_clearance())
    geo = gm.box(13.0, 52.0, 13.1, 52.2, crs=4326)
    geo_line = geo.minimum_clearance_line()
    assert geo_line.crs == gm.CRS('EPSG:4326')
    geo_coords = np.asarray(geo_line.coords, dtype=np.float64)
    geo_witness_distance = gm.distance(
        gm.Point(*geo_coords[0][:2], crs=4326),
        gm.Point(*geo_coords[-1][:2], crs=4326),
    )
    assert geo_witness_distance == pytest.approx(geo.minimum_clearance(), rel=0.001)
    assert geo_line.length >= geo_witness_distance - 1e-9
    assert gm.Point(1, 1).minimum_clearance_line().is_empty
    batch = gm.GeometryArray([gm.box(0, 0, 3, 2), gm.box(0, 0, 1, 5)])
    np.testing.assert_allclose(
        [g.length for g in batch.minimum_clearance_line()], [2.0, 1.0]
    )


def test_geographic_minimum_clearance_line_preserves_input_vertex_bits() -> None:
    vertices = [
        (13.123456789012344, 52.123456789012344),
        (13.223456789012343, 52.123456789012344),
        (13.223456789012343, 52.52345678901234),
        (13.123456789012344, 52.52345678901234),
    ]
    poly = gm.Polygon(vertices, crs=4326)
    line = poly.minimum_clearance_line()
    endpoints = [(coord[0], coord[1]) for coord in line.coords]
    assert all(endpoint in vertices for endpoint in endpoints)
    witness_distance = gm.distance(
        gm.Point(*endpoints[0], crs=4326),
        gm.Point(*endpoints[-1], crs=4326),
    )
    assert witness_distance == pytest.approx(poly.minimum_clearance(), rel=0.001)
    assert line.length >= witness_distance - 1e-9


def test_geographic_minimum_clearance_line_vertex_segment_matches_scalar() -> None:
    linework = gm.LineString(
        [
            (13.0, 52.0),
            (13.3, 52.0),
            (13.2, 52.0009),
        ],
        crs=4326,
    )
    witness = linework.minimum_clearance_line()
    endpoints = [(coord[0], coord[1]) for coord in witness.coords]
    assert (13.2, 52.0009) in endpoints
    witness_distance = gm.distance(
        gm.Point(*endpoints[0], crs=4326),
        gm.Point(*endpoints[-1], crs=4326),
    )
    assert witness_distance == pytest.approx(linework.minimum_clearance())
    assert witness.length >= witness_distance - 1e-9


def test_unique_points_dedups_in_first_occurrence_order() -> None:
    line = gm.LineString([(0, 0), (1, 1), (0, 0), (2, 2)], crs=4326)
    unique = line.unique_points()
    assert isinstance(unique, gm.MultiPoint)
    assert unique.to_wkt() == 'MULTIPOINT ((0 0), (1 1), (2 2))'
    assert unique.crs == gm.CRS('EPSG:4326')
    measured = gm.LineString([(0, 0), (0, 0), (1, 0)], z=[1, 2, 3])
    assert len(cast('gm.MultiPoint', measured.unique_points()).parts) == 3
    batch = cast('gm.GeometryArray', gm.GeometryArray([line, line]).unique_points())
    assert [g.to_wkt() for g in batch] == [unique.to_wkt()] * 2
    assert gm.from_wkt('LINESTRING EMPTY').unique_points().is_empty


def test_unique_points_preserves_heterogeneous_point_identity_and_order() -> None:
    source = gm.GeometryCollection([
        gm.Point(0, 0),
        gm.Point(0, 0, z=10),
        gm.Point(0, 0, m=20),
        gm.Point(0, 0, z=30, m=40),
        gm.Point(0, 0),
    ])

    result = source.unique_points()

    assert result.geometry_type == 'GeometryCollection'
    assert result.coordinate_axes == 'XYZM'
    assert [part.coordinate_axes for part in result.parts] == [
        'XY',
        'XYZ',
        'XYM',
        'XYZM',
    ]
    assert [list(part.coords) for part in result.parts] == [
        [(0.0, 0.0)],
        [(0.0, 0.0, 10.0)],
        [(0.0, 0.0, 20.0)],
        [(0.0, 0.0, 30.0, 40.0)],
    ]
    assert result == gm.from_wkb(result.to_wkb())


def test_unique_points_keeps_homogeneous_singleton_and_empty_outputs() -> None:
    singleton = gm.GeometryCollection([gm.Point(1, 2, z=3)])
    assert singleton.unique_points().geometry_type == 'MultiPoint'
    assert singleton.unique_points().coordinate_axes == 'XYZ'

    empty = gm.from_wkt('POINT Z EMPTY').unique_points()
    assert empty.geometry_type == 'MultiPoint'
    assert empty.coordinate_axes == 'XYZ'
    assert empty.is_empty

    sparse = gm.from_wkt('MULTIPOINT Z ((0 0 1), EMPTY, (1 1 2))').unique_points()
    assert sparse.geometry_type == 'MultiPoint'
    assert sparse.coordinate_axes == 'XYZ'
    assert list(sparse.coords) == [(0.0, 0.0, 1.0), (1.0, 1.0, 2.0)]


def test_unique_points_does_not_promote_axes_from_empty_collection_members() -> None:
    source = gm.GeometryCollection([
        gm.from_wkt('POINT Z EMPTY'),
        gm.Point(2, 3),
        gm.from_wkt('POINT M EMPTY'),
    ])

    result = source.unique_points()

    assert result.geometry_type == 'MultiPoint'
    assert result.coordinate_axes == 'XY'
    assert list(result.coords) == [(2.0, 3.0)]

    all_empty = gm.GeometryCollection([
        gm.from_wkt('POINT Z EMPTY'),
        gm.from_wkt('POINT M EMPTY'),
    ]).unique_points()
    assert all_empty.geometry_type == 'MultiPoint'
    assert all_empty.coordinate_axes == 'XYZM'
    assert all_empty.is_empty


def test_unique_points_scalar_and_array_preserve_per_row_result_shapes() -> None:
    mixed = gm.GeometryCollection([
        gm.Point(0, 0),
        gm.Point(1, 1, z=10),
        gm.Point(2, 2, m=20),
        gm.Point(3, 3, z=30, m=40),
    ])
    expected_mixed = gm.GeometryCollection([
        gm.Point(0, 0),
        gm.Point(1, 1, z=10),
        gm.Point(2, 2, m=20),
        gm.Point(3, 3, z=30, m=40),
    ])
    homogeneous = gm.MultiPoint([(0, 0), (1, 1)])
    empty = gm.from_wkt('POINT Z EMPTY')

    result = gm.GeometryArray([mixed, homogeneous, None, empty]).unique_points()

    assert [None if part is None else part.geometry_type for part in result] == [
        'GeometryCollection',
        'MultiPoint',
        None,
        'MultiPoint',
    ]
    assert result[0] == expected_mixed
    assert result[0] == gm.from_wkb(result[0].to_wkb())
    assert result[1].coordinate_axes == 'XY'
    assert result[2] is None
    assert result[3].coordinate_axes == 'XYZ'


def test_reverse_and_orient_polygons_are_rust_backed() -> None:
    line = gm.LineString(
        [(0, 0), (1, 0), (2, 0)], z=[0, 2, 4], m=[10, 12, 14], crs=4326
    )
    polygon = gm.Polygon(
        [(0, 0), (0, 1), (1, 1), (0, 0)],
        holes=[[(0.2, 0.2), (0.5, 0.2), (0.2, 0.5), (0.2, 0.2)]],
        crs=4326,
    )
    collection = gm.GeometryCollection([line, polygon], crs=4326)
    assert line.reverse().to_wkt() == 'LINESTRING ZM (2 0 4 14, 1 0 2 12, 0 0 0 10)'
    assert line.reverse().crs == gm.CRS('EPSG:4326')
    assert gm.Point(1, 2).reverse().to_wkt() == 'POINT (1 2)'
    assert (
        polygon.reverse().to_wkt()
        == 'POLYGON ((0 0, 1 1, 0 1, 0 0), (0.2 0.2, 0.2 0.5, 0.5 0.2, 0.2 0.2))'
    )
    assert polygon.orient_polygons().to_wkt() == polygon.reverse().to_wkt()
    assert polygon.orient_polygons(ccw=False).to_wkt() == polygon.to_wkt()
    assert (
        collection.orient_polygons().to_wkt()
        == 'GEOMETRYCOLLECTION (LINESTRING ZM (0 0 0 10, 1 0 2 12, 2 0 4 14), POLYGON ((0 0, 1 1, 0 1, 0 0), (0.2 0.2, 0.2 0.5, 0.5 0.2, 0.2 0.2)))'
    )
    array = gm.GeometryArray([line, polygon])
    assert (
        cast('gm.GeometryArray', array.reverse())[0].to_wkt() == line.reverse().to_wkt()
    )
    assert array.orient_polygons()[1].to_wkt() == polygon.reverse().to_wkt()


def test_normalize_is_rust_backed() -> None:
    line = gm.LineString([(1, 0), (0, 0)], z=[2, 4], m=[3, 5], crs=4326)
    polygon = gm.Polygon(
        [(0, 0), (1, 1), (0, 1), (0, 0)],
        holes=[[(0.2, 0.2), (0.2, 0.5), (0.5, 0.2), (0.2, 0.2)]],
        crs=4326,
    )
    collection = gm.GeometryCollection([gm.Point(2, 2, crs=4326), line, polygon])
    assert line.normalize().to_wkt() == 'LINESTRING ZM (0 0 4 5, 1 0 2 3)'
    assert line.normalize().crs == gm.CRS('EPSG:4326')
    assert (
        polygon.normalize().to_wkt()
        == 'POLYGON ((0 0, 1 1, 0 1, 0 0), (0.2 0.2, 0.2 0.5, 0.5 0.2, 0.2 0.2))'
    )
    assert (
        collection.normalize().to_wkt()
        == 'GEOMETRYCOLLECTION (POINT (2 2), LINESTRING ZM (0 0 4 5, 1 0 2 3), POLYGON ((0 0, 1 1, 0 1, 0 0), (0.2 0.2, 0.2 0.5, 0.5 0.2, 0.2 0.2)))'
    )
    assert (
        gm.MultiPoint([(1, 0), (2, 0)]).normalize().to_wkt()
        == 'MULTIPOINT ((1 0), (2 0))'
    )
    assert gm.GeometryArray([line]).normalize()[0].to_wkt() == line.normalize().to_wkt()
    assert gm.equals_exact(
        line.normalize(),
        gm.LineString([(0, 0), (1, 0)], z=[4, 2], m=[5, 3], crs=4326).normalize(),
    )
    np.testing.assert_array_equal(
        gm.equals_exact(
            line.normalize(), gm.GeometryArray([line.reverse()]).normalize()
        ),
        [True],
    )


def test_clip_by_rect_clips_scalar_array_and_preserves_line_axes() -> None:
    polygon = gm.box(-1, -1, 3, 3, crs=3857)
    line = gm.LineString(
        [(-2, 0), (0, 0), (2, 0)], z=[0, 2, 4], m=[10, 12, 14], crs=3857
    )
    inside = gm.Point(0, 0, crs=3857)
    outside = gm.Point(4, 4, crs=3857)
    clipped_polygon = polygon.clip_by_rect(0, 0, 2, 2)
    clipped_line = cast('gm.Geometry', line.clip_by_rect(-1, -1, 1, 1))
    clipped_array = cast(
        'gm.GeometryArray',
        gm.GeometryArray([polygon, outside]).clip_by_rect(0, 0, 2, 2),
    )
    assert clipped_polygon.area == pytest.approx(4)
    assert clipped_polygon.crs == gm.CRS('EPSG:3857')
    assert clipped_line.to_wkt() == 'LINESTRING ZM (-1 0 1 11, 0 0 2 12, 1 0 3 13)'
    assert inside.clip_by_rect(0, 0, 2, 2).to_wkt() == 'POINT (0 0)'
    assert outside.clip_by_rect(0, 0, 2, 2).to_wkt() == 'POINT EMPTY'
    assert clipped_array[0].area == pytest.approx(4)
    assert clipped_array[1].to_wkt() == 'POINT EMPTY'
    with pytest.raises(ValueError, match='clip rectangle bounds'):
        polygon.clip_by_rect(2, 0, 1, 1)


def test_clip_by_rect_wkb_matches_box_intersection_in_the_same_frame() -> None:
    rect = (-10.0, -10.0, 30.0, 50.0)
    subject = gm.box(-20, -15, 40, 55, crs=4326)
    rectangle = gm.box(*rect, crs=4326)
    clipped = subject.clip_by_rect(*rect)
    expected = gm.intersection(subject, rectangle)
    assert clipped.to_wkb() == expected.to_wkb()

    array = gm.GeometryArray([subject, gm.box(-100, -80, -90, -70, crs=4326)])
    clipped_array = array.clip_by_rect(*rect)
    expected_array = gm.intersection(array, rectangle)
    assert [item.to_wkb() for item in clipped_array] == [
        item.to_wkb() for item in expected_array
    ]

    seam_subject = gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)],
        crs=4326,
    )
    seam_rect = (175.0, -5.0, -175.0, 5.0)
    seam_box = gm.box(*seam_rect, crs=4326, wrap='split')
    assert (
        seam_subject.clip_by_rect(*seam_rect).to_wkb()
        == gm.intersection(seam_subject, seam_box).to_wkb()
    )

    contained = gm.Polygon(
        [(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)],
        crs=4326,
    )
    contained_bounds = (-10.0, -10.0, 10.0, 10.0)
    geographic_box = gm.box(*contained_bounds, crs=4326)
    assert contained.clip_by_rect(*contained_bounds).to_wkb() == contained.to_wkb()
    assert gm.intersection(contained, geographic_box).to_wkb() == contained.to_wkb()


def test_clip_by_rect_contained_polygonal_wkb_matches_intersection() -> None:
    bounds = (-1.0, -1.0, 5.0, 5.0)
    subjects = [
        gm.from_wkt('POLYGON ((0 0, 2 0, 4 0, 4 4, 0 4, 0 0))'),
        gm.from_wkt('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'),
        gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'),
        gm.from_wkt(
            'MULTIPOLYGON (((0 0, 2 0, 2 1, 2 2, 0 2, 0 0)), '
            '((2 1, 3 0, 4 1, 3 2, 2 1)))'
        ),
    ]
    rectangle = gm.box(*bounds)

    for subject in subjects:
        expected_wkb = subject.to_wkb()
        assert subject.clip_by_rect(*bounds).to_wkb() == expected_wkb
        assert gm.intersection(subject, rectangle).to_wkb() == expected_wkb

    geometries = gm.GeometryArray(subjects)
    clipped = geometries.clip_by_rect(*bounds)
    expected = gm.intersection(geometries, rectangle)
    source_wkb = [geometry.to_wkb() for geometry in geometries]
    assert [geometry.to_wkb() for geometry in clipped] == source_wkb
    assert [geometry.to_wkb() for geometry in expected] == source_wkb


def test_clip_by_rect_array_wkb_matches_box_intersection_on_mixed_dimensions() -> None:
    bounds = (1.0, 1.0, 4.0, 4.0)
    geometries = gm.GeometryArray([
        gm.from_wkt('LINESTRING (5 3, 4 1)'),
        gm.from_wkt('MULTILINESTRING ((5 3, 4 1), (1 1, 4 1))'),
        gm.from_wkt('POLYGON ((4 2, 6 2, 6 3, 4 3, 4 2))'),
        gm.from_wkt(
            'MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))'
        ),
    ])
    rectangle = gm.box(*bounds)
    clipped = geometries.clip_by_rect(*bounds)
    expected = gm.intersection(geometries, rectangle)
    assert [item.to_wkb() for item in clipped] == [item.to_wkb() for item in expected]


def test_clip_by_rect_line_array_keeps_categories_and_multipart_fallback() -> None:
    crossing = gm.LineString([(-1, 0.5), (0.5, 0.6), (2, 0.5)])
    inside = gm.LineString([(0.1, 0.5), (0.5, 0.6), (0.9, 0.5)])
    outside = gm.LineString([(2, 0.5), (2.5, 0.6), (3, 0.5)])
    multipart = gm.LineString([
        (-1, 0.5),
        (0.5, 0.5),
        (2, 0.5),
        (2.5, 0.7),
        (0.5, 0.8),
        (-1, 0.8),
    ])

    result = gm.GeometryArray([crossing, inside, outside]).clip_by_rect(0, 0, 1, 1)

    assert len(result) == 3
    assert not result[0].is_empty
    assert result[0].to_wkt() != crossing.to_wkt()
    assert result[0].to_wkt() == crossing.clip_by_rect(0, 0, 1, 1).to_wkt()
    assert all(0 <= x <= 1 and 0 <= y <= 1 for x, y, *_ in result[0].coords)
    assert not result[1].is_empty
    assert result[1].to_wkt() == inside.to_wkt()
    assert result[2].to_wkt() == 'LINESTRING EMPTY'

    multipart_result = gm.GeometryArray([inside, multipart]).clip_by_rect(0, 0, 1, 1)
    assert multipart_result[0].to_wkt() == inside.to_wkt()
    assert multipart_result[1].geometry_type == 'MultiLineString'
    assert multipart_result[1].to_wkt() == multipart.clip_by_rect(0, 0, 1, 1).to_wkt()


def test_clip_by_rect_line_array_preserves_axes_masks_selection_and_frame() -> None:
    zm = gm.GeometryArray([
        gm.LineString([(-1, 0.5, 0, 10), (2, 0.5, 3, 13)])
    ]).clip_by_rect(0, 0, 1, 1)
    assert zm[0].to_wkt() == 'LINESTRING ZM (0 0.5 1 11, 1 0.5 2 12)'

    rows = gm.GeometryArray([
        gm.LineString([(-1, 0.5), (2, 0.5)]),
        gm.LineString([(0.1, 0.5), (0.9, 0.5)]),
        gm.LineString([(2, 0.5), (3, 0.5)]),
    ])
    masked = rows._with_missing([False, True, False]).clip_by_rect(0, 0, 1, 1)
    assert masked.is_missing.tolist() == [False, True, False]
    assert masked[0].to_wkt() == 'LINESTRING (0 0.5, 1 0.5)'
    assert masked[1] is None
    assert masked[2].to_wkt() == 'LINESTRING EMPTY'

    selected = rows[[2, 0]].clip_by_rect(0, 0, 1, 1)
    assert [item.to_wkt() for item in selected] == [
        'LINESTRING EMPTY',
        'LINESTRING (0 0.5, 1 0.5)',
    ]

    empty_row = gm.GeometryArray([gm.LineString([]), rows[0]]).clip_by_rect(0, 0, 1, 1)
    assert [item.to_wkt() for item in empty_row] == [
        'LINESTRING EMPTY',
        'LINESTRING (0 0.5, 1 0.5)',
    ]

    framed = gm.GeometryArray([rows[0]], crs=4326, epoch=2020.0).clip_by_rect(
        0, 0, 1, 1
    )
    assert framed.crs == gm.CRS('EPSG:4326')
    assert framed.epoch == 2020.0


def test_clip_by_rect_packed_missing_and_gathered_rows_preserve_mask_and_frame() -> (
    None
):
    all_missing = gm.GeometryArray([None, None], crs=4326, epoch=2020.0)
    clipped_missing = all_missing.clip_by_rect(-1, -1, 1, 1)
    assert clipped_missing.is_missing.tolist() == [True, True]
    assert clipped_missing.crs == gm.CRS('EPSG:4326')
    assert clipped_missing.epoch == 2020.0

    # A geographic rectangle may cross the longitude seam.
    crossing = all_missing.clip_by_rect(170, -10, -170, 10)
    assert crossing.is_missing.tolist() == [True, True]

    mixed = gm.GeometryArray(
        [
            None,
            gm.Point(0.5, 0.5, crs=4326),
            None,
        ],
        crs=4326,
        epoch=2020.0,
    )
    clipped_mixed = mixed.clip_by_rect(-1, -1, 1, 1)
    assert clipped_mixed.is_missing.tolist() == [True, False, True]
    assert clipped_mixed[1].to_wkt(drop_epoch=True) == 'POINT (0.5 0.5)'
    assert clipped_mixed.crs == gm.CRS('EPSG:4326')
    assert clipped_mixed.epoch == 2020.0

    mixed_storage = gm.GeometryArray(
        [
            gm.Point(0.5, 0.5, crs=4326),
            gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
            None,
        ],
        crs=4326,
        epoch=2020.0,
    )
    gathered_missing = mixed_storage[[2, 2]].clip_by_rect(-1, -1, 1, 1)
    assert gathered_missing.is_missing.tolist() == [True, True]
    assert gathered_missing.crs == gm.CRS('EPSG:4326')
    assert gathered_missing.epoch == 2020.0

    empty = gm.GeometryArray([], crs=4326, epoch=2020.0).clip_by_rect(-1, -1, 1, 1)
    assert len(empty) == 0
    assert empty.crs == gm.CRS('EPSG:4326')
    assert empty.epoch == 2020.0


def test_clip_by_rect_validates_bounds_when_arrays_have_no_present_rows() -> None:
    missing = gm.GeometryArray([None, None])
    empty = gm.GeometryArray([])
    for rows in (missing, empty):
        with pytest.raises(gm.GeometryError, match='clip rectangle bounds'):
            rows.clip_by_rect(2, 0, 1, 1)

    geographic_missing = gm.GeometryArray([None], crs=4326)
    with pytest.raises(gm.GeometryError, match='geographic rectangle bounds'):
        geographic_missing.clip_by_rect(-1, 1, 1, -1)


def test_clip_by_rect_line_array_reentry_at_same_boundary_matches_scalar() -> None:
    coords = [(0.5, 0.5), (0.0, 0.5), (-1.0, 1.5), (0.0, 0.5), (0.75, 0.5)]
    for line in (gm.LineString(coords), gm.LineString(list(reversed(coords)))):
        scalar = line.clip_by_rect(0, 0, 1, 1)
        packed = gm.GeometryArray([line]).clip_by_rect(0, 0, 1, 1)[0]
        assert packed.geometry_type == scalar.geometry_type == 'LineString'
        assert packed.to_wkt() == scalar.to_wkt()


def test_clip_by_rect_empty_matches_intersection_type() -> None:
    rect = (0.0, 0.0, 2.0, 2.0)
    box = gm.box(*rect)
    expected = {
        'MULTIPOINT(9 9,8 8)': 'POINT EMPTY',
        'LINESTRING(5 5,6 6)': 'LINESTRING EMPTY',
        'LINESTRING(-5 5,15 5)': 'LINESTRING EMPTY',
        'POLYGON((9 9,10 9,10 10,9 10,9 9))': 'POLYGON EMPTY',
        'GEOMETRYCOLLECTION(POINT(9 9),LINESTRING(8 8,9 9))': 'LINESTRING EMPTY',
    }
    for wkt, empty in expected.items():
        geom = gm.from_wkt(wkt)
        clipped = geom.clip_by_rect(*rect)
        intersection = gm.intersection(geom, box)
        assert clipped.to_wkt() == empty
        assert intersection.to_wkt() == empty
        assert clipped.to_wkb() == intersection.to_wkb()


@pytest.mark.parametrize(
    ('source', 'bounds'),
    [
        pytest.param(
            'POLYGON ((10 10,14 10,14 14,10 14,10 10),(9 9,15 9,15 15,9 15,9 9))',
            (9.5, 9.5, 13, 14.5),
            id='winding-shell-covered-by-hole',
        ),
        pytest.param(
            'POLYGON ((10 10,11 10,14 10,12 10,10 10),(9 9,15 9,15 11,9 11,9 9))',
            (9.5, 9.5, 13, 10.5),
            id='collapsed-shell-covered-by-hole',
        ),
    ],
)
def test_rectangle_intersection_excludes_shell_covered_by_hole(
    source: str, bounds: tuple[float, float, float, float]
) -> None:
    subject = gm.from_wkt(source)
    rectangle = gm.box(*bounds)

    assert gm.relate(subject, rectangle) == 'FFFFFF212'
    assert gm.intersection(subject, rectangle).is_empty
    assert gm.intersection(rectangle, subject).is_empty
    assert subject.clip_by_rect(*bounds).is_empty


def test_rectangle_shell_ownership_survives_array_nulls_and_gathers() -> None:
    winding = gm.from_wkt(
        'POLYGON ((10 10,14 10,14 14,10 14,10 10),(9 9,15 9,15 15,9 15,9 9))'
    )
    collapsed = gm.from_wkt(
        'POLYGON ((10 10,11 10,14 10,12 10,10 10),(9 9,15 9,15 11,9 11,9 9))'
    )
    ordinary = gm.box(10, 10, 12, 12)
    bounds = (9.5, 9.5, 13, 14.5)
    rectangle = gm.box(*bounds)
    subjects = gm.GeometryArray([winding, collapsed, ordinary])
    rectangles = gm.GeometryArray([rectangle, rectangle, rectangle])

    forward = gm.intersection(subjects, rectangles)
    reverse = gm.intersection(rectangles, subjects)
    assert [geometry.is_empty for geometry in forward] == [True, True, False]
    assert [geometry.is_empty for geometry in reverse] == [True, True, False]

    nullable = subjects._with_missing([False, True, False])
    nullable_result = gm.intersection(nullable, rectangles)
    assert nullable_result.is_missing.tolist() == [False, True, False]
    assert nullable_result[0].is_empty
    assert not nullable_result[2].is_empty

    gathered = nullable[[2, 0, 1]]
    gathered_rectangles = rectangles[[2, 0, 1]]
    gathered_result = gm.intersection(gathered_rectangles, gathered)
    assert gathered_result.is_missing.tolist() == [False, False, True]
    assert not gathered_result[0].is_empty
    assert gathered_result[1].is_empty

    clipped = subjects.clip_by_rect(*bounds)
    assert [geometry.is_empty for geometry in clipped] == [True, True, False]
    nullable_clipped = nullable.clip_by_rect(*bounds)
    assert nullable_clipped.is_missing.tolist() == [False, True, False]
    assert nullable_clipped[0].is_empty
    gathered_clipped = gathered.clip_by_rect(*bounds)
    assert gathered_clipped.is_missing.tolist() == [False, False, True]
    assert gathered_clipped[1].is_empty


def test_union_all_removes_collapsed_shell_points_covered_by_their_own_holes() -> None:
    inner = gm.from_wkt('POLYGON ((1 1,1 1,1 1,1 1),(0 0,2 0,2 2,0 2,0 0))')
    outer = gm.from_wkt('POLYGON ((3 3,3 3,3 3,3 3),(-2 -2,4 -2,4 4,-2 4,-2 -2))')
    cases = [
        (
            'collection-inner-first',
            gm.GeometryCollection([inner, outer]),
        ),
        (
            'collection-outer-first',
            gm.GeometryCollection([outer, inner]),
        ),
        (
            'multipolygon-inner-first',
            gm.MultiPolygon([inner, outer]),
        ),
        (
            'multipolygon-outer-first',
            gm.MultiPolygon([outer, inner]),
        ),
    ]

    assert gm.union_all([inner]).to_wkt() == 'POLYGON EMPTY'
    assert gm.union_all([outer]).to_wkt() == 'POLYGON EMPTY'
    for name, geometry in cases:
        result = gm.union_all([geometry])
        assert result.to_wkt() == 'POLYGON EMPTY', name
        assert gm.relate(result, gm.Point(3, 3)) == 'FFFFFF0F2', name


def test_union_all_preserves_incident_curve_and_unrelated_explicit_point() -> None:
    inner = 'POLYGON ((3 3,3 3,3 3,3 3),(2 2,4 2,4 4,2 4,2 2))'
    outer = 'POLYGON ((3 3,3 3,3 3,3 3),(-2 -2,8 -2,8 8,-2 8,-2 -2))'
    cases = [
        (
            'inner-first',
            gm.GeometryCollection([
                gm.from_wkt(inner),
                gm.from_wkt(outer),
                gm.LineString([(2, 3), (4, 3)]),
                gm.Point(10, 10),
            ]),
        ),
        (
            'outer-first',
            gm.GeometryCollection([
                gm.from_wkt(outer),
                gm.from_wkt(inner),
                gm.LineString([(2, 3), (4, 3)]),
                gm.Point(10, 10),
            ]),
        ),
    ]

    for name, geometry in cases:
        result = gm.union_all([geometry])
        assert result.to_wkt() == (
            'GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (2 3, 3 3, 4 3))'
        ), name
        assert gm.relate(result, gm.Point(10, 10)) == '0F1FF0FF2', name
        assert gm.relate(result, gm.Point(3, 3)) == '0F1FF0FF2', name


def test_rectangle_clips_positive_collapsed_shell_support() -> None:
    collapsed = gm.from_wkt('POLYGON ((0 0,2 0,4 0,0 0))')

    clipped = collapsed.clip_by_rect(1, -1, 3, 1)

    assert clipped.to_wkt() == 'LINESTRING (1 0, 3 0)'


def test_shared_paths_tiny_collinear_segments_stay_same_direction() -> None:
    # Regression: the shared-direction dot product underflowed to 0.0 for
    # ~1e-162 collinear segments (products below f64's subnormal floor),
    # misclassifying two IDENTICAL segments as opposite-direction. Every scale
    # must keep the shared run in the forward (same-direction) component and
    # leave the backward (opposite) component empty.
    for length in (1.0, 1e-160, 1e-162, 1e-170):
        line = gm.LineString([(0.0, 0.0), (length, 0.0)])
        forward, backward = gm.shared_paths(line, line).parts
        assert not forward.is_empty, length
        assert backward.is_empty, length


def test_shared_paths_jointly_nodes_partial_overlaps_into_maximal_runs() -> None:
    left = gm.LineString([(0, 0), (2, 0), (4, 0)])

    same, opposite = gm.shared_paths(left, gm.LineString([(1, 0), (3, 0)])).parts
    assert same.to_wkt() == 'MULTILINESTRING ((1 0, 2 0, 3 0))'
    assert opposite.is_empty

    same, opposite = gm.shared_paths(left, gm.LineString([(3, 0), (1, 0)])).parts
    assert same.is_empty
    assert opposite.to_wkt() == 'MULTILINESTRING ((1 0, 2 0, 3 0))'


def test_overlay_operators_provenance_shared_edge():
    left = gm.box(0, 0, 2, 2)
    right = gm.box(2, 0, 4, 2)
    assert gm.equals(left | right, gm.box(0, 0, 4, 2))
    assert (left & right).area == 0.0
    a = gm.box(0, 0, 2, 2)
    b = gm.box(1, 1, 3, 3)
    assert gm.equals(a & b, gm.box(1, 1, 2, 2))
