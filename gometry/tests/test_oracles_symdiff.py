"""symmetric_difference ring-reassembly fallback — shapely oracle parity."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import gometry as gm
import pytest

shapely = pytest.importorskip('shapely')
shapely_from_wkt = shapely.from_wkt

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class SymdiffCase:
    name: str
    left: Callable[[], gm.Geometry]
    right: Callable[[], gm.Geometry]


def _touching_at_point() -> tuple[gm.Geometry, gm.Geometry]:
    return (gm.box(0, 0, 1, 1), gm.box(1, 1, 2, 2))


def _crossing_two_corners() -> tuple[gm.Geometry, gm.Geometry]:
    return (gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3))


def _crossing_three_corners() -> tuple[gm.Geometry, gm.Geometry]:
    return (gm.box(0, 0, 3, 3), gm.box(2, 0, 5, 2))


def _crossing_four_corners() -> tuple[gm.Geometry, gm.Geometry]:
    return (gm.box(0, 0, 4, 4), gm.box(1, 1, 3, 3))


def _nested_contains() -> tuple[gm.Geometry, gm.Geometry]:
    return (gm.box(0, 0, 10, 10), gm.box(2, 2, 8, 8))


def _both_holed() -> tuple[gm.Geometry, gm.Geometry]:
    outer_a = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
    hole_a = [(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]
    outer_b = [(5, 5), (15, 5), (15, 15), (5, 15), (5, 5)]
    hole_b = [(8, 8), (12, 8), (12, 12), (8, 12), (8, 8)]
    return (gm.Polygon(outer_a, holes=[hole_a]), gm.Polygon(outer_b, holes=[hole_b]))


SYMDIFF_CASES = [
    SymdiffCase(
        'touching_at_point',
        lambda: _touching_at_point()[0],
        lambda: _touching_at_point()[1],
    ),
    SymdiffCase(
        'crossing_two_corners',
        lambda: _crossing_two_corners()[0],
        lambda: _crossing_two_corners()[1],
    ),
    SymdiffCase(
        'crossing_three_corners',
        lambda: _crossing_three_corners()[0],
        lambda: _crossing_three_corners()[1],
    ),
    SymdiffCase(
        'crossing_four_corners',
        lambda: _crossing_four_corners()[0],
        lambda: _crossing_four_corners()[1],
    ),
    SymdiffCase(
        'nested_contains', lambda: _nested_contains()[0], lambda: _nested_contains()[1]
    ),
    SymdiffCase('both_holed', lambda: _both_holed()[0], lambda: _both_holed()[1]),
]


def _assert_symdiff_oracle(left: gm.Geometry, right: gm.Geometry) -> gm.Geometry:
    got = gm.symmetric_difference(left, right)
    want = shapely.symmetric_difference(
        shapely_from_wkt(left.to_wkt()), shapely_from_wkt(right.to_wkt())
    )
    assert got.is_empty == want.is_empty
    if not got.is_empty:
        assert shapely.equals(shapely_from_wkt(got.to_wkt()), want)
        assert got.area == pytest.approx(want.area, rel=1e-09, abs=1e-09)
    return got


@pytest.mark.parametrize('case', SYMDIFF_CASES, ids=lambda c: c.name)
def test_symmetric_difference_matches_shapely_oracle(case: SymdiffCase) -> None:
    left, right = (case.left(), case.right())
    forward = _assert_symdiff_oracle(left, right)
    # Commutativity: reverse call equals the forward oracle result.
    assert gm.equals(forward, gm.symmetric_difference(right, left))
