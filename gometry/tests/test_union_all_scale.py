"""R23-A extreme finite-coordinate overlay regressions."""

from itertools import permutations

import gometry as gm


def _fixture(extreme: float):
    p1 = gm.Polygon(
        zip(
            [-extreme, 0.5, 0.5, -extreme, -extreme],
            [-extreme, -extreme, 0.5, 0.5, -extreme],
            strict=True,
        )
    )
    p2 = gm.Polygon(
        zip(
            [-extreme, 0.5, extreme, -extreme, -extreme],
            [0.5, 0.5, extreme, extreme, 0.5],
            strict=True,
        )
    )
    p3 = gm.Polygon(
        zip(
            [0.5, extreme, extreme, 0.5, 0.5],
            [-extreme, -extreme, extreme, 0.5, -extreme],
            strict=True,
        )
    )
    return (p1, p2, p3), gm.box(-extreme, -extreme, extreme, extreme)


def test_union_all_opposite_finite_extremes_is_order_independent() -> None:
    polygons, clip = _fixture(1e308)
    results = []
    for order in permutations(range(3)):
        result = gm.GeometryArray([polygons[index] for index in order]).union_all()
        assert not result.is_empty
        assert result.bounds == clip.bounds
        assert gm.covers(result, clip)
        results.append(result)

    first = results[0]
    assert all(gm.equals(first, result) for result in results[1:])
