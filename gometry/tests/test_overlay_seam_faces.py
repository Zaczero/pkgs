"""Thin antimeridian faces keep their source-side attribution."""

from __future__ import annotations

import gometry as gm
import pytest


def _thin_seam_polygon(west: float, east: float, height: float) -> gm.Polygon:
    return gm.Polygon(
        [
            (west, 0.0),
            (east, 0.0),
            (east, height),
            (west, height),
            (west, 0.0),
        ],
        crs=4326,
    )


@pytest.mark.parametrize(
    ('west', 'east', 'height'),
    [
        pytest.param(179.0, -179.0, 1e-8, id='two-degree-below-probe-step'),
        pytest.param(179.0, -179.0, 1e-9, id='two-degree-well-below-probe-step'),
        pytest.param(170.0, -170.0, 1e-6, id='twenty-degree-at-probe-step'),
        pytest.param(170.0, -170.0, 1e-7, id='twenty-degree-at-half-step'),
        pytest.param(170.0, -170.0, 1e-8, id='twenty-degree-below-probe-step'),
    ],
)
def test_thin_seam_faces_keep_source_support_below_probe_step(
    west: float, east: float, height: float
) -> None:
    polygon = _thin_seam_polygon(west, east, height)
    union = gm.union_all([polygon])

    assert union.bounds == polygon.bounds
    for longitude in ((west + 180.0) / 2.0, (east - 180.0) / 2.0):
        assert gm.contains(union, gm.Point(longitude, height / 2.0, crs=4326))
    assert not gm.contains(union, gm.Point(0.0, height / 2.0, crs=4326))


def test_thin_seam_union_keeps_exclusions_and_disjoint_area() -> None:
    thin = _thin_seam_polygon(170.0, -170.0, 1e-6)
    far = gm.box(0.0, 40.0, 1.0, 41.0, crs=4326)
    union = gm.union_all([thin, far])

    assert gm.contains(union, gm.Point(175.0, 0.5e-6, crs=4326))
    assert gm.contains(union, gm.Point(-175.0, 0.5e-6, crs=4326))
    assert not gm.contains(union, gm.Point(0.5, 0.5, crs=4326))

    holed = gm.Polygon(
        [
            (170.0, -1.0),
            (-170.0, -1.0),
            (-170.0, 1.0),
            (170.0, 1.0),
            (170.0, -1.0),
        ],
        holes=[
            [
                (175.0, -0.5),
                (-175.0, -0.5),
                (-175.0, 0.5),
                (175.0, 0.5),
                (175.0, -0.5),
            ]
        ],
        crs=4326,
    )
    holed_union = gm.union_all([holed])
    assert gm.contains(holed_union, gm.Point(179.0, 0.75, crs=4326))
    assert not gm.contains(holed_union, gm.Point(179.0, 0.0, crs=4326))

    degenerate = gm.Polygon(
        [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
        crs=4326,
    )
    assert gm.union_all([degenerate]).area == 0.0

    ordinary = gm.Polygon(
        [(0.0, 0.0), (2.0, 0.0), (2.0, 1e-8), (0.0, 1e-8), (0.0, 0.0)],
        crs=4326,
    )
    assert gm.GeometryCollection([ordinary], crs=4326).area == pytest.approx(
        ordinary.area
    )


# OpenStreetMap Argentina and Uruguay borders near the Rio de la Plata, clipped
# to a 0.03-degree box. Two border vertices lie one ULP apart, so the dissolve
# has a sliver face containing no representable point.
_ARGENTINA_CLIP = (
    'MULTIPOLYGON (((-58.10222180941994 -33.010937543229446, -58.084289 -33.0009615, '
    '-58.085477253434156 -32.99, -58.089648910878246 -32.99, '
    '-58.0878711 -32.9965279, -58.10222180941994 -33.010937543229446)), '
    '((-58.11 -33.00166081596568, -58.10769305699579 -32.99, -58.11 -32.99, '
    '-58.11 -33.00166081596568)))'
)
_URUGUAY_CLIP = (
    'POLYGON ((-58.11 -33.02, -58.08 -33.02, -58.08 -32.99, '
    '-58.085477253434156 -32.99, -58.084289 -33.0009615, '
    '-58.10256757394891 -33.011129892448544, -58.0878711 -32.9965279, '
    '-58.089648910878246 -32.99, -58.10765630407339 -32.99, '
    '-58.11 -33.001904214459806, -58.11 -33.02))'
)


def test_dissolve_keeps_sliver_faces_without_representable_interior() -> None:
    argentina = gm.from_wkt(_ARGENTINA_CLIP).set_crs(4326)
    uruguay = gm.from_wkt(_URUGUAY_CLIP).set_crs(4326)
    # A disjoint seam member needs membership probes for faces it does not bound.
    seam = _thin_seam_polygon(179.0, -179.0, 1.0)

    dissolved = gm.GeometryCollection([seam, argentina, uruguay], crs=4326).area

    assert dissolved == pytest.approx(
        gm.union_all([argentina, uruguay]).area + gm.union_all([seam]).area,
        rel=1e-12,
    )
