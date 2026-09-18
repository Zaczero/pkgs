"""Geographic dissolves keep member membership around the antimeridian."""

from __future__ import annotations

import json
from pathlib import Path

import gometry as gm
import pytest

_COUNTRIES = (
    Path(__file__).resolve().parents[1] / 'fixtures' / 'osm_countries_0_1.geojson'
)


@pytest.fixture(scope='module')
def countries() -> dict[str, gm.Geometry]:
    text = _COUNTRIES.read_text(encoding='utf-8')
    codes = [
        feature['properties']['tags'].get('ISO3166-1:alpha2')
        for feature in json.loads(text)['features']
    ]
    return dict(zip(codes, gm.from_geojson(text, crs=4326), strict=True))


def test_union_keeps_holes_of_a_member_bordering_an_antimeridian_member(
    countries: dict[str, gm.Geometry],
) -> None:
    # China's holes share no boundary with its shell; Russia crosses the
    # antimeridian and borders China, so the two unwrap as one chart component.
    russia, china = countries['RU'], countries['CN']

    union = gm.union_all([russia, china])

    assert union.area == pytest.approx(russia.area + china.area, rel=1e-12)


def test_collection_area_counts_overlapping_members_once(
    countries: dict[str, gm.Geometry],
) -> None:
    members = list(countries.values())

    collection = gm.GeometryCollection(members, crs=4326).area

    dissolved = gm.union_all(members)
    assert collection == pytest.approx(
        sum(part.area for part in gm.parts(dissolved)), rel=1e-12
    )
    assert collection < sum(member.area for member in members)


def test_union_with_a_member_touching_the_seam_stays_valid() -> None:
    crossing = gm.Polygon(
        [(170.0, 0.0), (-170.0, 0.0), (-170.0, 10.0), (170.0, 10.0), (170.0, 0.0)],
        crs=4326,
    )
    touching = gm.Polygon(
        [(178.0, 20.0), (180.0, 20.0), (180.0, 21.0), (178.0, 21.0), (178.0, 20.0)],
        crs=4326,
    )

    union = gm.union_all([crossing, touching])

    assert union.is_valid
    assert gm.contains(union, gm.Point(179.0, 20.5, crs=4326))
    assert not gm.contains(union, gm.Point(-179.0, 20.5, crs=4326))
