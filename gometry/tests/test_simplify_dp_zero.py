"""Douglas-Peucker tolerance 0 keeps vertices off the closed chord."""

from __future__ import annotations

import gometry as gm
import pytest


@pytest.mark.parametrize('preserve_topology', [False, True])
@pytest.mark.parametrize(
    ('tolerance', 'expected'),
    [
        (0.70710679, [(0.0, 0.0), (268435457.0, 268435457.0)]),
        (
            0.7,
            [
                (0.0, 0.0),
                (134217728.0, 134217729.0),
                (268435457.0, 268435457.0),
            ],
        ),
    ],
)
def test_dp_uses_exact_distance_and_lowest_index(
    preserve_topology: bool,
    tolerance: float,
    expected: list[tuple[float, float]],
) -> None:
    """All three interior distances are exactly 1/sqrt(2)."""
    line = gm.LineString([
        (0.0, 0.0),
        (134217728.0, 134217729.0),
        (134217732.0, 134217733.0),
        (134217736.0, 134217737.0),
        (268435457.0, 268435457.0),
    ])
    result = line.simplify(tolerance, method='dp', preserve_topology=preserve_topology)
    assert list(result.coords) == expected


def test_dp_zero_l_bend_keeps_the_corner() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
    out = line.simplify(0.0, method='dp')
    assert out.to_wkt() == 'LINESTRING (0 0, 1 1, 2 0)'


def test_dp_zero_collinear_interior_drops() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)])
    out = line.simplify(0.0, method='dp')
    assert out.to_wkt() == 'LINESTRING (0 0, 2 0)'


def test_dp_zero_backtrack_keeps_outside_endpoint() -> None:
    line = gm.LineString([(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)])
    out = line.simplify(0.0, method='dp')
    assert out.to_wkt() == 'LINESTRING (0 0, 2 0, 1 0)'


def test_dp_zero_farthest_drops_avoidable_collinear() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (2.0, 1.0)])
    out = line.simplify(0.0, method='dp')
    assert out.to_wkt() == 'LINESTRING (0 0, 2 0, 2 1)'


def test_dp_zero_xyzm_keeps_corner_ordinates() -> None:
    line = gm.LineString(
        [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
        z=[1.0, 7.0, 3.0],
        m=[10.0, 70.0, 30.0],
    )
    out = line.simplify(0.0, method='dp')
    assert list(out.coords) == [
        (0.0, 0.0, 1.0, 10.0),
        (1.0, 1.0, 7.0, 70.0),
        (2.0, 0.0, 3.0, 30.0),
    ]


def test_dp_zero_collection_and_missing_rows() -> None:
    bent = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
    collinear = gm.LineString([(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)])
    collection = gm.GeometryCollection([bent, collinear])
    out = collection.simplify(0.0, method='dp')
    assert [g.to_wkt() for g in out.parts] == [
        'LINESTRING (0 0, 1 1, 2 0)',
        'LINESTRING (0 0, 2 0)',
    ]
    array = gm.GeometryArray([bent, None, collinear])
    simplified = array.simplify(0.0, method='dp', preserve_topology=False)
    assert simplified.is_missing.tolist() == [False, True, False]
    assert simplified[0].to_wkt() == 'LINESTRING (0 0, 1 1, 2 0)'
    assert simplified[2].to_wkt() == 'LINESTRING (0 0, 2 0)'
