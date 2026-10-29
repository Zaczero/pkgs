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
    codes = ('RU', 'CN')
    wanted = set(codes)
    features = {}
    for feature in json.loads(text)['features']:
        code = feature['properties']['tags'].get('ISO3166-1:alpha2')
        if code in wanted:
            features[code] = feature
    selected = json.dumps({
        'type': 'FeatureCollection',
        'features': [features[code] for code in codes],
    })
    collection = gm.from_geojson(selected, crs=4326)
    assert isinstance(collection, gm.Features)
    return dict(zip(codes, collection.geometries, strict=True))


def test_union_keeps_holes_of_a_member_bordering_an_antimeridian_member(
    countries: dict[str, gm.Geometry],
) -> None:
    # China's holes share no boundary with its shell; Russia crosses the
    # antimeridian and borders China, so the two unwrap as one chart component.
    russia, china = countries['RU'], countries['CN']

    union = gm.union_all([russia, china])

    assert union.area == pytest.approx(russia.area + china.area, rel=1e-12)


def test_collection_area_counts_overlapping_members_once() -> None:
    first = gm.box(0, 0, 2, 2, crs=4326)
    second = gm.box(1, 0, 3, 2, crs=4326)

    collection_area = gm.GeometryCollection([first, second], crs=4326).area
    dissolved = gm.union_all([first, second])

    assert collection_area == pytest.approx(dissolved.area, rel=1e-12)
    assert collection_area < first.area + second.area


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


def test_dissolve_stops_after_expected_plus_one() -> None:
    """R02: dissolve keys stop after expected+1 (unbounded iterators bound)."""
    import itertools

    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    geoms, keys = arr.dissolve(by=[0, 0])
    assert keys == [0]
    assert len(geoms) == 1

    with pytest.raises(gm.GeometryError, match='one key per geometry'):
        arr.dissolve(by=itertools.repeat(0))

    with pytest.raises(gm.GeometryError, match='one key per geometry'):
        arr.dissolve(by=['a'])
