"""Exact local-Delaunay checks on the public triangulation surfaces."""

from __future__ import annotations

from fractions import Fraction
from typing import cast

import gometry as gm
import pytest

_Point = tuple[Fraction, Fraction]


def _orientation(a: _Point, b: _Point, c: _Point) -> Fraction:
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _incircle(a: _Point, b: _Point, c: _Point, d: _Point) -> Fraction:
    ax, ay = a[0] - d[0], a[1] - d[1]
    bx, by = b[0] - d[0], b[1] - d[1]
    cx, cy = c[0] - d[0], c[1] - d[1]
    return (
        (ax * ax + ay * ay) * (bx * cy - by * cx)
        + (bx * bx + by * by) * (cx * ay - cy * ax)
        + (cx * cx + cy * cy) * (ax * by - ay * bx)
    )


def _assert_local_delaunay(triangles: gm.GeometryArray[gm.Polygon]) -> None:
    edges: dict[tuple[_Point, _Point], list[tuple[_Point, _Point, _Point]]] = {}
    for triangle in triangles:
        coordinates = cast('gm.Polygon', triangle).exterior.coords[:3]
        a, b, c = [(Fraction(x), Fraction(y)) for x, y, *_ in coordinates]
        assert _orientation(a, b, c) > 0
        for left, right, opposite in ((a, b, c), (b, c, a), (c, a, b)):
            first, second = sorted((left, right))
            edges.setdefault((first, second), []).append((left, right, opposite))
    for edge, incident in edges.items():
        assert len(incident) <= 2, edge
        if len(incident) != 2:
            continue
        a, b, c = incident[0]
        d = incident[1][2]
        if _orientation(c, d, a) * _orientation(c, d, b) < 0:
            assert _incircle(a, b, c, d) <= 0, (
                'illegal unconstrained interior edge',
                tuple(tuple(map(float, point)) for point in (a, b, c, d)),
            )


@pytest.mark.parametrize('surface', ['scalar', 'array', 'delaunay'])
def test_constraint_recovery_legalizes_the_seed_35_polygon(surface: str) -> None:
    polygon = gm.Polygon([
        (0.3202050387506045, 0.0017946837865403596),
        (0.7069116514223635, 0.004556853775252638),
        (0.6765942594663125, 0.19026836690202795),
        (0.3046756322359895, 0.21209210052463723),
        (0.40387029152576776, 0.6614338559267148),
        (-0.21620944117359106, 0.3505949742172562),
        (-0.3680648294398593, 0.45700094248086115),
        (-0.5108837855200573, 0.15440392822048296),
        (-0.8678008548086853, 0.23157876213638195),
        (0.17903490825765114, -0.4476126697984749),
        (0.37597103802434656, -0.9005513877926329),
        (0.8018321096270082, -0.5495842501105047),
        (0.3781119585764764, -0.24557365724621946),
        (0.3253632896780207, -0.1407806405341791),
        (0.7461555712126661, -0.25900317237246323),
        (0.39802280370727594, -0.052868985310945894),
    ])
    assert polygon.is_valid
    if surface == 'array':
        triangles = gm.GeometryArray([polygon]).triangulate(method='constrained')[0]
    else:
        method = 'delaunay' if surface == 'delaunay' else 'constrained'
        triangles = polygon.triangulate(method=method)

    _assert_local_delaunay(triangles)
