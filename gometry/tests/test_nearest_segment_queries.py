"""Traversal-independent nearest-segment witness ties."""

import gometry as gm


def test_indexed_nearest_exact_tie_uses_lowest_source_segment() -> None:
    target = gm.MultiLineString([
        gm.LineString([(-1, 1), (1, 1)]),
        gm.LineString([(-1 + 3 * i, -3 + 4 * i) for i in range(81)]),
    ])
    _, foot = gm.nearest_points(gm.Point(0, 0), target)
    assert foot.to_wkb() == gm.Point(0, 1).to_wkb()
