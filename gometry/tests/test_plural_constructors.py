"""Plural geometry constructors — columnar broadcast and sequence builders."""

import inspect

import gometry as gm
import pytest


def test_points_scalar_broadcast() -> None:
    pts = gm.points(0, [1, 2, 3])
    assert len(pts) == 3
    assert pts[0].x == 0.0 and pts[0].y == 1.0
    assert pts[1].x == 0.0 and pts[1].y == 2.0
    assert pts[2].x == 0.0 and pts[2].y == 3.0
    shared_z = gm.points([1, 2, 3], [4, 5, 6], z=10)
    assert [p.z for p in shared_z] == [10.0, 10.0, 10.0]


def test_points_all_scalar_raises() -> None:
    with pytest.raises(gm.GeometryError, match='Point'):
        gm.points(0, 1)


def test_points_param_names_match_point() -> None:
    point_params = set(inspect.signature(gm.Point).parameters) - {'self'}
    points_params = set(inspect.signature(gm.points).parameters) - {'self'}
    assert {'x', 'y', 'z', 'm', 'crs', 'epoch'} <= point_params
    assert {'x', 'y', 'z', 'm', 'crs', 'epoch'} <= points_params


def test_boxes_columnar_broadcast() -> None:
    boxes = gm.boxes(0, 0, [1, 2], [1, 2])
    assert len(boxes) == 2
    assert (boxes[0]).to_wkt() == 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    assert (boxes[1]).to_wkt() == 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'


def test_boxes_param_names_match_box() -> None:
    box_params = set(inspect.signature(gm.box).parameters) - {'self'}
    boxes_params = set(inspect.signature(gm.boxes).parameters) - {'self'}
    assert box_params == boxes_params


def test_line_strings_round_trip() -> None:
    data = [[(0, 0), (1, 1)], [(2, 2), (3, 3)]]
    lines = gm.line_strings(data, crs=4326)
    assert len(lines) == 2
    assert (lines[0]).to_wkt() == 'LINESTRING (0 0, 1 1)'
    assert (lines[1]).to_wkt() == 'LINESTRING (2 2, 3 3)'
    assert lines.crs == 'EPSG:4326'


def test_polygons_round_trip() -> None:
    shell = [(0, 0), (1, 0), (1, 1), (0, 1)]
    polys = gm.polygons([shell], crs=4326)
    assert len(polys) == 1
    assert gm.equals(polys[0], gm.Polygon(shell, crs=4326))


def test_polygons_one_shot_iterator_keeps_shell_and_hole() -> None:
    """P21: one-shot ring iterators must not drop the shell during classification."""
    shell = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
    hole = [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]
    from_list = gm.polygons([[shell, hole]])[0]
    from_iter = gm.polygons([iter([shell, hole])])[0]
    assert from_list.area == 99.0
    assert from_iter.area == 99.0
    assert gm.equals(from_list, from_iter)
    # Bare-shell list form still works (positive path).
    bare = gm.polygons([shell])[0]
    assert bare.area == 100.0


def test_multipolygon_one_shot_iterator_keeps_shell_and_hole() -> None:
    """P21: MultiPolygon raw members share polygon_from_coordinates_item."""
    shell = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
    hole = [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]
    from_list = gm.MultiPolygon([[shell, hole]])
    from_iter = gm.MultiPolygon([iter([shell, hole])])
    assert from_list.area == 99.0
    assert from_iter.area == 99.0
    assert gm.equals(from_list, from_iter)


def test_multi_points_round_trip() -> None:
    data = [[(0, 0), (1, 1)], [(2, 2)]]
    multipoints = gm.multi_points(data, crs=4326)
    assert len(multipoints) == 2
    assert gm.equals(multipoints[0], gm.MultiPoint(data[0], crs=4326))
    assert gm.equals(multipoints[1], gm.MultiPoint(data[1], crs=4326))


def test_multi_line_strings_round_trip() -> None:
    data = [[[(0, 0), (1, 1)]], [[(2, 2), (3, 3)], [(4, 4), (5, 5)]]]
    multilines = gm.multi_line_strings(data, crs=4326)
    assert len(multilines) == 2
    assert gm.equals(multilines[0], gm.MultiLineString(data[0], crs=4326))
    assert gm.equals(multilines[1], gm.MultiLineString(data[1], crs=4326))


def test_multi_polygons_round_trip() -> None:
    left = [[(0, 0), (1, 0), (1, 1)]]
    right = [[(2, 2), (3, 2), (3, 3)]]
    multipolys = gm.multi_polygons([[left, right]], crs=4326)
    assert len(multipolys) == 1
    assert gm.equals(multipolys[0], gm.MultiPolygon([left, right], crs=4326))


def test_plural_coordinate_factories_reject_built_geometries() -> None:
    members = [gm.LineString([(0, 0), (1, 1)]), gm.LineString([(2, 2), (3, 3)])]
    shell = [(0, 0), (1, 0), (1, 1), (0, 1)]
    left = [[(0, 0), (1, 0), (1, 1)]]
    right = [[(2, 2), (3, 2), (3, 3)]]
    cases = [
        (gm.line_strings, members),
        (gm.polygons, [gm.Polygon(shell)]),
        (gm.multi_points, [gm.MultiPoint([(0, 0)])]),
        (gm.multi_line_strings, [gm.MultiLineString([[(0, 0), (1, 1)]])]),
        (gm.multi_polygons, [gm.MultiPolygon([left, right])]),
    ]
    for factory, values in cases:
        with pytest.raises(TypeError, match='raw coordinate inputs only'):
            factory(values)


def test_scalar_multipart_constructors_reconcile_member_frames() -> None:
    points = [gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326)]
    lines = [gm.LineString([(0, 0), (1, 1)], crs=4326)]
    polygons = [gm.Polygon([(0, 0), (1, 0), (1, 1)], crs=4326)]
    assert gm.MultiPoint(points).crs == 'EPSG:4326'
    assert gm.MultiLineString(lines).crs == 'EPSG:4326'
    assert gm.MultiPolygon(polygons).crs == 'EPSG:4326'
    for constructor, values in (
        (gm.MultiPoint, points),
        (gm.MultiLineString, lines),
        (gm.MultiPolygon, polygons),
    ):
        with pytest.raises(gm.CRSMismatchError):
            constructor(values, crs=3857)


def test_polygon_xy_column_form() -> None:
    poly = gm.Polygon(x=[0, 1, 1, 0], y=[0, 0, 1, 1])
    assert (poly).to_wkt() == 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    with pytest.raises(gm.InvalidGeometryError):
        gm.Polygon(shell=[(0, 0), (1, 0), (1, 1)], x=[0, 1])


def test_from_arrow_accepts_epoch() -> None:
    pytest.importorskip('pyarrow')
    values = gm.points([0, 1], [2, 3], crs=4326)
    table = (values).to_arrow()
    arr = gm.from_arrow(table, epoch=2020.5)
    assert isinstance(arr, gm.GeometryArray)
    assert arr.epoch == pytest.approx(2020.5)
