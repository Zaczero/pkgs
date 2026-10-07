"""Thin antimeridian faces keep their source-side attribution."""

from __future__ import annotations

import json
import math
from pathlib import Path

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


def _meridian_arc_length(start_latitude: float, end_latitude: float) -> float:
    """Independent composite-Simpson integral of the authored ellipsoid."""
    semi_major = 7_000_000.0
    flattening = 1.0 / 150.0
    eccentricity_squared = flattening * (2.0 - flattening)
    start = math.radians(start_latitude)
    end = math.radians(end_latitude)
    intervals = 4096
    step = (end - start) / intervals

    def meridian_radius(latitude: float) -> float:
        denominator = 1.0 - eccentricity_squared * math.sin(latitude) ** 2
        return semi_major * (1.0 - eccentricity_squared) / denominator**1.5

    weighted = [
        (1 if index in (0, intervals) else 4 if index % 2 else 2)
        * meridian_radius(start + index * step)
        for index in range(intervals + 1)
    ]
    return abs(step * math.fsum(weighted) / 3.0)


def test_q_intersection_carries_zm_by_actual_non_wgs_meridian_arclength() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(
        'LINESTRING ZM (179 -10 10 100, 179 20 70 700)',
        crs=crs,
    )
    seam_crossing = gm.from_wkt('LINESTRING (170 0, -170 0)', crs=crs)

    result = gm.intersection(source, seam_crossing)

    fraction = _meridian_arc_length(-10.0, 0.0) / _meridian_arc_length(-10.0, 20.0)
    assert abs(fraction - 1.0 / 3.0) > 1.0e-5
    assert result.coordinate_axes == 'XYZM'
    assert (result.x, result.y) == pytest.approx((179.0, 0.0), rel=0.0, abs=1.0e-12)
    assert result.z == pytest.approx(10.0 + 60.0 * fraction, rel=0.0, abs=2.0e-9)
    assert result.m == pytest.approx(100.0 + 600.0 * fraction, rel=0.0, abs=2.0e-8)
    assert abs(result.z - 30.0) > 1.0e-4
    assert abs(result.m - 300.0) > 1.0e-3


@pytest.mark.parametrize(
    ('seam_latitude', 'expected'),
    [
        pytest.param(-10.0, (179.0, -10.0, 10.0, 100.0), id='source-t0'),
        pytest.param(20.0, (179.0, 20.0, 70.0, 700.0), id='source-t1'),
    ],
)
def test_q_intersection_keeps_the_authentic_source_endpoint_record(
    seam_latitude: float,
    expected: tuple[float, float, float, float],
) -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(
        'LINESTRING ZM (179 -10 10 100, 179 20 70 700)',
        crs=crs,
    )
    seam_crossing = gm.from_wkt(
        f'LINESTRING (170 {seam_latitude}, -170 {seam_latitude})',
        crs=crs,
    )

    result = gm.intersection(source, seam_crossing)

    assert result.coordinate_axes == 'XYZM'
    assert (result.x, result.y, result.z, result.m) == expected


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


def _assert_geographic_overlay_area_identity(
    left: gm.Geometry, right: gm.Geometry
) -> gm.Geometry:
    intersection = gm.intersection(left, right)
    union = gm.union(left, right)
    symmetric_difference = gm.symmetric_difference(left, right)

    assert intersection.is_valid
    assert union.is_valid
    assert symmetric_difference.is_valid
    assert intersection.length > 0.0
    # Areas are geodesic square metres; allow 1e-12 relative and 1e-3 absolute
    # floating-point roundoff in the overlay identity.
    assert intersection.area + symmetric_difference.area == pytest.approx(
        union.area, rel=1e-12, abs=1e-3
    )
    return intersection


def test_minimized_country_shared_border_intersection_keeps_its_source_interval() -> (
    None
):
    russia = gm.Polygon(
        [
            (-179.5551997, 70.0996777),
            (179.8176414, 69.7948444),
            (-179.8683419985996, 69.94636166206753),
            (-179.5551997, 70.0996777),
        ],
        crs=4326,
    )
    norway = gm.Polygon(
        [
            (179.8176414, 69.7948444),
            (-179.5551997, 70.0996777),
            (-179.86921630140046, 69.94816043793249),
            (179.8176414, 69.7948444),
        ],
        crs=4326,
    )

    intersection = _assert_geographic_overlay_area_identity(russia, norway)

    assert intersection.area == pytest.approx(0.0, abs=1e-3)
    assert gm.covers(intersection, gm.Point(179.8176414, 69.7948444, crs=4326))


def test_synthetic_seam_shared_edge_intersection_is_valid() -> None:
    left = gm.Polygon(
        [(179.0, 0.0), (-179.0, 0.0), (-179.8, -1.0), (179.0, 0.0)],
        crs=4326,
    )
    right = gm.Polygon(
        [(-179.0, 0.0), (179.0, 0.0), (179.8, 1.0), (-179.0, 0.0)],
        crs=4326,
    )

    intersection = _assert_geographic_overlay_area_identity(left, right)

    assert intersection.area == pytest.approx(0.0, abs=1e-3)
    assert gm.covers(intersection, gm.Point(179.0, 0.0, crs=4326))


def test_russia_norway_intersection_emits_the_shared_country_border() -> None:
    fixture = Path(__file__).resolve().parents[1] / 'fixtures/osm_countries_0_1.geojson'
    source = json.loads(fixture.read_text(encoding='utf-8'))
    wanted = {'RU', 'NO'}
    features = {
        feature['properties']['tags'].get('ISO3166-1:alpha2'): feature
        for feature in source['features']
        if feature['properties']['tags'].get('ISO3166-1:alpha2') in wanted
    }
    countries = dict(
        zip(
            ('RU', 'NO'),
            gm.from_geojson(
                json.dumps({
                    'type': 'FeatureCollection',
                    'features': [features['RU'], features['NO']],
                }),
                crs=4326,
            ),
            strict=True,
        )
    )

    intersection = _assert_geographic_overlay_area_identity(
        countries['RU'], countries['NO']
    )

    assert intersection.area == pytest.approx(0.0, abs=1e-3)
    assert gm.covers(intersection, gm.Point(31.4448003, 70.0996777, crs=4326))
