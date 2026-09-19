"""Array self-intersection Groups: crossing points, missing/empty offsets."""

from __future__ import annotations

import math
import sys

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


def test_self_intersections_report_one_point_for_concurrent_lines() -> None:
    # Each line runs from a point to its exact negation, so all eight pass
    # through the origin exactly and meet in that one point.
    lines = []
    for index in range(8):
        angle = index * math.pi / 8.0
        x, y = math.cos(angle), math.sin(angle)
        lines.append([(x, y), (-x, -y)])
    points = gm.MultiLineString(lines).self_intersections()
    assert [(point.x, point.y) for point in points] == [(0.0, 0.0)]


def test_self_intersections_round_a_crossing_to_nearest_even() -> None:
    # The crossing's exact y is 1.5 + 2^-52 + 2^-53, halfway between two
    # doubles; nearest-even rounding takes the upper one.
    x = 1.0 + sys.float_info.epsilon
    lines = gm.MultiLineString([[(0.0, 0.0), (2.0, 3.0)], [(x, 0.0), (x, 3.0)]])
    points = lines.self_intersections()
    assert [(point.x, point.y) for point in points] == [
        (x, 1.5 + 2.0 * sys.float_info.epsilon)
    ]
