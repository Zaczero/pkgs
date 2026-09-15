"""Array self-intersection Groups: crossing points, missing/empty offsets."""

from __future__ import annotations

import gometry as gm


def test_self_intersections_crossing_line_point_coords() -> None:
    rows = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (2.0, 0.0), (1.0, 1.0), (1.0, -1.0)])
    ])
    groups = rows.self_intersections()
    assert [(point.x, point.y) for point in groups[0]] == [(1.0, 0.0)]
    assert list(groups.offsets) == [0, 1]


def test_self_intersections_missing_and_empty_row_offsets() -> None:
    crossing = gm.LineString([(0.0, 0.0), (2.0, 0.0), (1.0, 1.0), (1.0, -1.0)])
    empty = gm.from_wkt('LINESTRING EMPTY')
    groups = gm.GeometryArray([crossing, None, empty]).self_intersections()
    assert [(point.x, point.y) for point in groups[0]] == [(1.0, 0.0)]
    assert len(groups[1]) == 0
    assert len(groups[2]) == 0
    assert list(groups.offsets) == [0, 1, 1, 1]
