"""CDT mesh-refinement controls on ``triangulate(method='constrained')``."""

from __future__ import annotations

import math
from typing import cast

import gometry as gm
import pytest
from gometry import GeometryError


def _sliver_polygon() -> gm.Polygon:
    """Quadrilateral with a very acute corner — sliver-prone for CDT."""
    return gm.Polygon([(0, 0), (20, 0), (20, 0.2), (0.1, 0.05)])


def _triangle_coords(triangle: gm.Polygon) -> list[tuple[float, float]]:
    return [(x, y) for x, y, *_ in triangle.exterior.coords[:3]]


def _interior_angles_deg(triangle: gm.Polygon) -> list[float]:
    coords = _triangle_coords(triangle)

    def angle_at(vertex: int) -> float:
        prev_pt = coords[vertex - 1]
        curr = coords[vertex]
        next_pt = coords[(vertex + 1) % 3]
        v1 = (prev_pt[0] - curr[0], prev_pt[1] - curr[1])
        v2 = (next_pt[0] - curr[0], next_pt[1] - curr[1])
        dot = v1[0] * v2[0] + v1[1] * v2[1]
        len1 = math.hypot(*v1)
        len2 = math.hypot(*v2)
        cos_angle = max(-1.0, min(1.0, dot / (len1 * len2)))
        return math.degrees(math.acos(cos_angle))

    return [angle_at(i) for i in range(3)]


def _min_interior_angle_deg(triangles: gm.GeometryArray[gm.Polygon]) -> float:
    return min(
        min(_interior_angles_deg(cast('gm.Polygon', triangle)))
        for triangle in triangles
    )


def _z_polygon() -> gm.Polygon:
    return gm.Polygon(
        [(0, 0, 1), (10, 0, 2), (10, 10, 3), (0, 10, 4)],
        holes=[[(3, 3, 5), (7, 3, 6), (7, 7, 7), (3, 7, 8)]],
    )


@pytest.fixture
def sliver() -> gm.Polygon:
    return _sliver_polygon()


def test_refine_raises_minimum_angle_and_adds_triangles(sliver: gm.Polygon) -> None:
    base = sliver.triangulate(method='constrained')
    refined = sliver.triangulate(method='constrained', min_angle=25.0)
    assert len(refined) > len(base)
    assert _min_interior_angle_deg(refined) > _min_interior_angle_deg(base)


def test_max_area_caps_triangle_areas(sliver: gm.Polygon) -> None:
    max_area = 2.0
    refined = sliver.triangulate(method='constrained', max_area=max_area)
    eps = max_area * 1e-09 + 1e-12
    for triangle in refined:
        assert triangle.area <= max_area + eps


@pytest.mark.parametrize(
    ('kwargs', 'pattern'),
    [
        ({'min_angle': 0}, 'min_angle must be a positive finite number'),
        ({'min_angle': -5}, 'min_angle must be a positive finite number'),
        ({'min_angle': 31}, 'min_angle must be at most 30 degrees'),
        ({'min_angle': float('nan')}, 'min_angle must be finite'),
        ({'max_area': 0}, 'max_area must be a positive finite number'),
        ({'max_area': -1}, 'max_area must be a positive finite number'),
        ({'max_area': float('inf')}, 'max_area must be finite'),
    ],
)
def test_param_validation(
    sliver: gm.Polygon, kwargs: dict[str, bool | float], pattern: str
) -> None:
    with pytest.raises(GeometryError, match=pattern):
        sliver.triangulate(method='constrained', **kwargs)


@pytest.mark.parametrize('kwargs', [{'min_angle': 25.0}, {'max_area': 1.0}])
def test_quality_constraint_implies_refine(
    sliver: gm.Polygon, kwargs: dict[str, float]
) -> None:
    base = sliver.triangulate(method='constrained')
    implied = sliver.triangulate(method='constrained', **kwargs)
    assert len(implied) > len(base)


def test_refine_drops_zm_preserves_without(sliver: gm.Polygon) -> None:
    zpoly = _z_polygon()
    preserved = zpoly.triangulate(method='constrained')
    assert preserved
    assert all(t.coordinate_axes == 'XYZ' for t in preserved)
    refined = zpoly.triangulate(method='constrained', max_area=2.0)
    assert refined
    assert all(t.coordinate_axes == 'XY' for t in refined)
    assert all(
        t.coordinate_axes == 'XY' for t in sliver.triangulate(method='constrained')
    )


def test_array_refinement_drops_zm_while_plain_mesh_preserves_it() -> None:
    zpoly = _z_polygon()
    array = gm.GeometryArray([zpoly])
    assert all(
        t.coordinate_axes == 'XYZ' for t in array.triangulate(method='constrained')[0]
    )
    assert all(
        t.coordinate_axes == 'XY'
        for t in array.triangulate(method='constrained', max_area=2.0)[0]
    )
    rows = gm.GeometryArray([gm.box(0, 0, 4, 4), gm.box(0, 0, 4, 4)])
    varied = rows.triangulate(method='constrained', max_area=[4.0, 1.0])
    assert len(varied[1]) > len(varied[0])


def test_free_and_array_surfaces_match(sliver: gm.Polygon) -> None:
    kwargs = {'min_angle': 25.0}
    method = sliver.triangulate(method='constrained', **kwargs)
    free = sliver.triangulate(method='constrained', **kwargs)
    # Array is per-row Groups; row 0 is this single geometry's triangles.
    array = gm.GeometryArray([sliver]).triangulate(method='constrained', **kwargs)[0]
    assert len(method) == len(free) == len(array)
    assert [t.to_wkt() for t in method] == [t.to_wkt() for t in free]
    assert [t.to_wkt() for t in method] == [t.to_wkt() for t in array]
