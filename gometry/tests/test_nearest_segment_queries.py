"""Stable source ordinals and per-probe clearance admission."""

import gometry as gm
import pytest


@pytest.mark.parametrize('extra', [0, 58], ids=['small', 'indexed'])
def test_clearance_equal_probes_retain_first_improving_probe(extra: int) -> None:
    shape = gm.GeometryCollection([
        gm.MultiPoint([(0, 1), (10, 1)]),
        gm.LineString([(9, 0), (11, 0)]),
        gm.LineString([(-1, 0), (1, 0)]),
        *(gm.Point(1000 + 10 * i, 1000) for i in range(extra)),
    ])
    assert shape.minimum_clearance() == 1.0
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([
            (0, 1),
            (0, 0),
        ]).to_wkb()
    )


def test_indexed_nearest_exact_tie_uses_lowest_source_segment() -> None:
    target = gm.MultiLineString([
        gm.LineString([(-1, 1), (1, 1)]),
        gm.LineString([(-1 + 3 * i, -3 + 4 * i) for i in range(81)]),
    ])
    _, foot = gm.nearest_points(gm.Point(0, 0), target)
    assert foot.to_wkb() == gm.Point(0, 1).to_wkb()


def test_indexed_clearance_vertex_witness_retains_first_source_ordinate() -> None:
    shape = gm.MultiPoint(
        [(1, 0), (0, 0), (0, 0), *((1000 + 10 * i, 1000) for i in range(61))],
        z=[20, 10, 100, *([1] * 61)],
    )
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([(1, 0), (0, 0)], z=[20, 10]).to_wkb()
    )
