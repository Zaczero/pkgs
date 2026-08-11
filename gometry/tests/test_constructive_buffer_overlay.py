"""Constructive and overlay operations — buffer/simplify/offset, cleanup,
orientation, normalize, clip, polygonize, summaries, and Z interpolation.
"""

import math
from typing import cast

import gometry as gm
import numpy as np
import pytest


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
    assert buffered.crs == 'EPSG:3857'
    assert buffered.area == pytest.approx(math.pi, rel=0.04)
    assert buffered_top_level.area == pytest.approx(buffered.area)
    geographic_buffer = cast('gm.Geometry', gm.Point(0, 0, crs=4326).buffer(100.0))
    assert geographic_buffer.geometry_type in ('Polygon', 'MultiPolygon')
    assert flat_line_buffer.area == pytest.approx(4)
    assert square_line_buffer.area == pytest.approx(8)
    assert bevel_corner_buffer.area < miter_corner_buffer.area
    assert offset_curve.to_wkt() == 'LINESTRING Z (0 1 0, 1 1 2, 1 2 4)'
    assert offset_curve.crs == 'EPSG:3857'
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
    assert gm.LineString([(0, 0), (2, 0)], crs=3857).offset_curve(1).crs == 'EPSG:3857'
    geographic_offset = gm.LineString([(0, 0), (2, 0)], crs=4326).offset_curve(1)
    assert geographic_offset.crs == 'EPSG:4326'
    assert geographic_offset.geometry_type == 'LineString'
    assert simplified.to_wkt() == 'LINESTRING (0 0, 2 0)'
    assert snapped.to_wkt() == 'LINESTRING ZM (0 0 0 10, 1 0 2 12, 2 0 4 14)'
    assert snapped.crs == 'EPSG:3857'
    assert shared.geometry_type == 'GeometryCollection'
    assert shared.crs == 'EPSG:3857'
    assert [part.to_wkt() for part in gm.parts(shared)] == [
        'MULTILINESTRING ((1 0, 2 0))',
        'MULTILINESTRING ((0 0, 1 0))',
    ]
    assert merged.to_wkt() == 'LINESTRING (0 0, 1 0, 2 0)'
    assert merged.crs == 'EPSG:3857'
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
    assert overlay_intersections.crs == 'EPSG:3857'
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
    assert gm.union_all([gm.box(0, 0, 1, 1, crs=3857), left]).crs == 'EPSG:3857'
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
    assert [part.to_wkt() for part in gm.parts(shared_array[0])] == [
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
    assert [part.to_wkt() for part in gm.parts(pairwise_shared[0])] == [
        'MULTILINESTRING ((0 0, 1 0))',
        'MULTILINESTRING EMPTY',
    ]
    assert [part.to_wkt() for part in gm.parts(pairwise_shared[1])] == [
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
    assert geographic_offset_array.crs == 'EPSG:4326'
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
    assert point_union.crs == 'EPSG:3857'
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


def test_snap_dedups_only_identical_projected_positions() -> None:
    # Two reference points 5e-13 apart on the unit segment both lie exactly on
    # it; even at tolerance 0 they insert as distinct vertices rather than
    # collapsing under a fixed parametric epsilon.
    line = gm.LineString([(0, 0), (1, 0)], crs=3857)
    reference = gm.MultiPoint([(0.5, 0), (0.5000000000005, 0)], crs=3857)
    snapped = cast('gm.Geometry', gm.snap(line, reference, 0.0))
    assert snapped.num_coordinates == 4
    coords = gm.get_coordinates(snapped)
    assert coords[1][0] == 0.5
    assert coords[2][0] == 0.5000000000005


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

    import shapely

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
    assert (
        gm.intersection_all([
            gm.box(0, 0, 4, 4, crs=3857),
            gm.box(1, 1, 5, 5, crs=3857),
        ]).crs
        == 'EPSG:3857'
    )


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
    assert cleaned.crs == 'EPSG:4326'
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
    assert line.crs == 'EPSG:3857'
    assert line.length == pytest.approx(square.minimum_clearance())
    geo = gm.box(13.0, 52.0, 13.1, 52.2, crs=4326)
    geo_line = geo.minimum_clearance_line()
    assert geo_line.crs == 'EPSG:4326'
    assert geo_line.length == pytest.approx(geo.minimum_clearance(), rel=0.001)
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
    assert line.length == pytest.approx(poly.minimum_clearance(), rel=0.001)


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
    assert witness.length == pytest.approx(linework.minimum_clearance())


def test_unique_points_dedups_in_first_occurrence_order() -> None:
    line = gm.LineString([(0, 0), (1, 1), (0, 0), (2, 2)], crs=4326)
    unique = line.unique_points()
    assert isinstance(unique, gm.MultiPoint)
    assert unique.to_wkt() == 'MULTIPOINT ((0 0), (1 1), (2 2))'
    assert unique.crs == 'EPSG:4326'
    measured = gm.LineString([(0, 0), (0, 0), (1, 0)], z=[1, 2, 3])
    assert len(cast('gm.MultiPoint', measured.unique_points()).parts) == 3
    batch = cast('gm.GeometryArray', gm.GeometryArray([line, line]).unique_points())
    assert [g.to_wkt() for g in batch] == [unique.to_wkt()] * 2
    assert gm.from_wkt('LINESTRING EMPTY').unique_points().is_empty


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
    assert line.reverse().crs == 'EPSG:4326'
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
    assert line.normalize().crs == 'EPSG:4326'
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
    assert clipped_polygon.crs == 'EPSG:3857'
    assert clipped_line.to_wkt() == 'LINESTRING ZM (-1 0 1 11, 0 0 2 12, 1 0 3 13)'
    assert inside.clip_by_rect(0, 0, 2, 2).to_wkt() == 'POINT (0 0)'
    assert outside.clip_by_rect(0, 0, 2, 2).to_wkt() == 'POINT EMPTY'
    assert clipped_array[0].area == pytest.approx(4)
    assert clipped_array[1].to_wkt() == 'POINT EMPTY'
    with pytest.raises(ValueError, match='clip rectangle bounds'):
        polygon.clip_by_rect(2, 0, 1, 1)


def test_clip_by_rect_empty_is_subject_typed() -> None:
    rect = (0.0, 0.0, 2.0, 2.0)
    box = gm.box(*rect)
    expected = {
        'MULTIPOINT(9 9,8 8)': 'POINT EMPTY',
        'LINESTRING(5 5,6 6)': 'LINESTRING EMPTY',
        'LINESTRING(-5 5,15 5)': 'LINESTRING EMPTY',
        'POLYGON((9 9,10 9,10 10,9 10,9 9))': 'POLYGON EMPTY',
        'GEOMETRYCOLLECTION(POINT(9 9),LINESTRING(8 8,9 9))': 'GEOMETRYCOLLECTION EMPTY',
    }
    for wkt, empty in expected.items():
        geom = gm.from_wkt(wkt)
        assert geom.clip_by_rect(*rect).to_wkt() == empty
        if not wkt.startswith('GEOMETRY'):
            assert gm.intersection(geom, box).to_wkt() == empty


def test_shared_paths_tiny_collinear_segments_stay_same_direction() -> None:
    # Regression: the shared-direction dot product underflowed to 0.0 for
    # ~1e-162 collinear segments (products below f64's subnormal floor),
    # misclassifying two IDENTICAL segments as opposite-direction. Every scale
    # must keep the shared run in the forward (same-direction) component and
    # leave the backward (opposite) component empty.
    for length in (1.0, 1e-160, 1e-162, 1e-170):
        line = gm.LineString([(0.0, 0.0), (length, 0.0)])
        forward, backward = gm.parts(gm.shared_paths(line, line))
        assert not forward.is_empty, length
        assert backward.is_empty, length
