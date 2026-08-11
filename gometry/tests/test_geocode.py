"""Flat point-code APIs: plus codes (Open Location Code) and OSM shortlinks.

Oracle values are hardcoded from Google's canonical OLC test CSVs
(``test_data/{encoding,decoding,shortCodeTests,validityTests}.csv``); the
kernel was swept against all of them at port time (decoding/short/validity
100%; encoding matches the reference implementation exactly — the CSV's
float-edge rows are defined by its integer columns, which the reference
itself fails from degrees).
"""

import math

import gometry as gm
import numpy as np
import pytest


def test_pluscode_encodes_canonical_vectors() -> None:
    # encoding.csv vectors (lat, lon, length -> code).
    vectors = [
        (20.375, 2.775, 6, '7FG49Q00+'),
        (20.3700625, 2.7821875, 10, '7FG49QCJ+2V'),
        (20.3701125, 2.782234375, 11, '7FG49QCJ+2VX'),
        (20.3701135, 2.78223535156, 13, '7FG49QCJ+2VXGJ'),
        (47.0000625, 8.0000625, 10, '8FVC2222+22'),
        (-41.2730625, 174.7859375, 10, '4VCPPQGP+Q9'),
        (0.5, -179.5, 4, '62G20000+'),
        (-90, -180, 4, '22220000+'),
        (90, 1, 4, 'CFX30000+'),
        (37.539669125, -122.375069724, 15, '849VGJQF+VX7QR3J'),
    ]
    for lat, lon, length, expected in vectors:
        assert gm.pluscode_encode(lon, lat, length=length) == expected, expected
    # Point / array inputs are CRS-aware; bare pairs are lon, lat.
    assert gm.pluscode_encode(gm.Point(8.628, 47.366, crs=4326)) == '8FVC9J8H+C6'
    assert gm.pluscode_encode(
        gm.GeometryArray([gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326)])
    ) == ['6FG22222+22', '6FH32222+22']
    # Validation: even short lengths only, 2..=15 (the reference silently
    # clamps oversized lengths; gometry validates instead).
    with pytest.raises(gm.GeometryError, match='must be even'):
        gm.pluscode_encode(0, 0, length=5)
    with pytest.raises(gm.GeometryError, match='between 2 and 15'):
        gm.pluscode_encode(0, 0, length=16)


def test_pluscode_rejects_out_of_range_bare_coordinates() -> None:
    # Bare lon/lat are domain-validated (no silent OLC clip/wrap): out-of-range
    # finite coordinates raise InvalidGeometryError rather than mint a code for
    # a different location. Non-finite and geometry inputs stay rejected.
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(181.0, 20.0)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(-181.0, 20.0)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(8.0, 91.0)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(8.0, -91.0)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(float('nan'), 0.0)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(0.0, float('inf'))
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(gm.Point(181.0, 91.0, crs=4326))
    # In-domain still encodes.
    assert isinstance(gm.pluscode_encode(-179.0, 20.0), str)


def test_pluscode_polygon_short_codes_and_validity() -> None:
    # decoding.csv vectors: code -> (lng_lo, lat_lo, lng_hi, lat_hi).
    area = gm.pluscode_polygon('8FVC2222+22')
    assert area.crs == 'OGC:CRS84'
    assert area.bounds == (8.0, 47.0, 8.000125, 47.000125)
    assert gm.pluscode_polygon('62G20000+').bounds == (-180.0, 0.0, -179.0, 1.0)
    assert gm.pluscode_polygon('8FVC2222+22GG').bounds == (
        8.000078125,
        47.00006,
        8.0000859375,
        47.000065,
    )
    # shortCodeTests.csv vectors (lon, lat references).
    assert gm.pluscode_shorten('9C3W9QCJ+2VX', -1.217765625, 51.3701125) == '+2VX'
    assert gm.pluscode_shorten('9C3W9QCJ+2VX', -1.232865625, 51.3701125) == '9QCJ+2VX'
    assert gm.pluscode_recover('9QCJ+2VX', -1.217765625, 51.3852125) == '9C3W9QCJ+2VX'
    # Recovery wraps cell edges (R-row); a full code passes through
    # normalized.
    assert gm.pluscode_recover('2222+22', 0.0, 89.6) == 'CFX22222+22'
    assert gm.pluscode_recover('8fvc2222+22', 0, 0) == '8FVC2222+22'
    assert (
        gm.pluscode_shorten('9C3W9QCJ+2VX', reference=-1.217765625, lat=51.3701125)
        == '+2VX'
    )
    assert (
        gm.pluscode_recover('9QCJ+2VX', reference=-1.217765625, lat=51.3852125)
        == '9C3W9QCJ+2VX'
    )
    bulk = gm.pluscode_polygon(['8FVC2222+22', '62G20000+'])
    assert isinstance(bulk, gm.GeometryArray)
    assert bulk.crs == 'OGC:CRS84'
    assert bulk.bounds.tolist() == [
        [8.0, 47.0, 8.000125, 47.000125],
        [-180.0, 0.0, -179.0, 1.0],
    ]
    assert gm.pluscode_shorten(
        ['9C3W9QCJ+2VX', '9C3W9QCJ+2VX'],
        [-1.217765625, -1.232865625],
        51.3701125,
    ) == ['+2VX', '9QCJ+2VX']
    assert gm.pluscode_recover(
        ['9QCJ+2VX', '8fvc2222+22'],
        [-1.217765625, 0.0],
        [51.3852125, 0.0],
    ) == ['9C3W9QCJ+2VX', '8FVC2222+22']
    # validityTests.csv: non-full codes raise ParseError from area/shorten.
    for bad in ['8FWC2345+G', '8FWC2_45+G6', '8FWC2η45+G6', 'WC2345+G6g', '2345+G6']:
        with pytest.raises(gm.ParseError, match='full plus code'):
            gm.pluscode_polygon(bad)
    with pytest.raises(gm.GeometryError, match='padded'):
        gm.pluscode_shorten('7FG49Q00+', 2.775, 20.375)
    with pytest.raises(gm.ParseError, match='short plus code'):
        gm.pluscode_recover('not-a-code', 0, 0)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_shorten('9C3W9QCJ+2VX', -181.0, 51.37)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_recover('9QCJ+2VX', -1.2, 91.0)


def test_osm_shortlink_round_trips() -> None:
    code = gm.osm_shortlink_encode(13.365, 52.5077, zoom=17)
    assert code == '0MbEUxVoG-'
    lon, lat, zoom = gm.osm_shortlink_location(code)
    assert (round(lon, 4), round(lat, 4), zoom) == (
        13.365,
        52.5077,
        17,
    )
    # The legacy '@' spelling decodes like '~'; '-' pads partial zooms.
    tilde = gm.osm_shortlink_encode(-1.2, 51.75, zoom=16)
    assert gm.osm_shortlink_location(tilde.replace('~', '@')) == (
        gm.osm_shortlink_location(tilde)
    )
    for zoom in [0, 3, 8, 16, 22]:
        trip_lon, trip_lat, trip_zoom = gm.osm_shortlink_location(
            gm.osm_shortlink_encode(-73.99, 40.73, zoom=zoom)
        )
        assert trip_zoom == zoom
        # One quadtile at this zoom bounds the round-trip error.
        cell = 360.0 / 2 ** (zoom + 8)
        assert trip_lon == pytest.approx(-73.99, abs=cell)
        assert trip_lat == pytest.approx(40.73, abs=cell)
    # Point inputs and gates.
    assert gm.osm_shortlink_encode(gm.Point(13.365, 52.5077, crs=4326)) == (
        gm.osm_shortlink_encode(13.365, 52.5077)
    )
    codes = [
        gm.osm_shortlink_encode(13.365, 52.5077, zoom=17),
        gm.osm_shortlink_encode(-73.99, 40.73, zoom=8),
    ]
    lons, lats, zooms = gm.osm_shortlink_location(codes)
    assert isinstance(lons, np.ndarray)
    assert isinstance(lats, np.ndarray)
    assert isinstance(zooms, np.ndarray)
    np.testing.assert_array_equal(zooms, [17, 8])
    np.testing.assert_allclose(lons, [13.365, -73.99], atol=0.01)
    np.testing.assert_allclose(lats, [52.5077, 40.73], atol=0.01)
    with pytest.raises(gm.GeometryError, match='between 0 and 22'):
        gm.osm_shortlink_encode(0, 0, zoom=23)
    with pytest.raises(gm.ParseError, match='invalid OSM shortlink'):
        gm.osm_shortlink_location('ab!cd')
    # Non-ASCII must be rejected — never truncated to a low-byte alphabet digit.
    with pytest.raises(gm.ParseError, match='invalid OSM shortlink character'):
        gm.osm_shortlink_location('ŁŁŁŁ')
    with pytest.raises(gm.ParseError, match='empty'):
        gm.osm_shortlink_location('---')
    # A code too short to name a real zoom level is rejected, not returned
    # as upstream Ruby's nonsense negative zoom.
    with pytest.raises(gm.ParseError, match='too short'):
        gm.osm_shortlink_location('A')
    assert isinstance(zoom, int)


def test_osm_shortlink_normalizes_admitted_pole_neighborhoods() -> None:
    for pole, away_from_equator in ((90.0, math.inf), (-90.0, -math.inf)):
        hemisphere = math.copysign(1.0, pole)
        for latitude in (
            pole,
            math.nextafter(pole, 0.0),
            math.nextafter(pole, away_from_equator),
            hemisphere * 89.999_999_999_9,
            hemisphere * 89.999_999,
            hemisphere * 89.99,
        ):
            code = gm.osm_shortlink_encode(gm.Point(10.0, latitude, crs=4326))
            assert math.copysign(1.0, gm.osm_shortlink_location(code)[1]) == hemisphere
    north = gm.osm_shortlink_encode(0.0, 90.0)
    south = gm.osm_shortlink_encode(0.0, -90.0)
    assert north != south
    assert gm.osm_shortlink_encode(0.0, math.nextafter(90.0, math.inf)) == north
    assert gm.osm_shortlink_encode(0.0, 90.000_000_000_000_5) == north
    assert gm.osm_shortlink_encode([0.0, 0.0], [90.0, 90.000_000_000_000_5]) == [
        north,
        north,
    ]
    pole = gm.Point(0.0, 90.0, crs=4326)
    assert gm.osm_shortlink_encode(pole) == north
    assert gm.osm_shortlink_encode(pole.to_crs(3995)) == north


def test_geocode_encoders_broadcast_coordinate_lanes_and_propagate_missing() -> None:
    lons = [8.628, 13.365]
    lats = [47.366, 52.5077]
    assert gm.pluscode_encode(lons, lats) == [
        gm.pluscode_encode(lon, lat) for lon, lat in zip(lons, lats, strict=True)
    ]
    assert gm.osm_shortlink_encode(lons, lats, zoom=17) == [
        gm.osm_shortlink_encode(lon, lat, zoom=17)
        for lon, lat in zip(lons, lats, strict=True)
    ]

    points = gm.GeometryArray([gm.Point(8.628, 47.366, crs=4326), None])
    assert gm.pluscode_encode(points) == ['8FVC9J8H+C6', None]
    assert gm.osm_shortlink_encode(points) == [
        gm.osm_shortlink_encode(points[0]),
        None,
    ]

    projected = gm.GeometryArray([
        gm.Point(8.628, 47.366, crs=4326).to_crs(3857),
        None,
    ])
    assert gm.pluscode_encode(projected) == ['8FVC9J8H+C6', None]

    polygons = gm.pluscode_polygon(code for code in ['8FVC2222+22', '62G20000+'])
    assert all(isinstance(polygon, gm.Polygon) for polygon in polygons)

    with pytest.raises(gm.GeometryTypeError, match='Point'):
        gm.pluscode_encode(gm.box(0, 0, 1, 1, crs=4326))
    with pytest.raises(gm.GeometryTypeError, match='Point'):
        gm.osm_shortlink_encode(gm.box(0, 0, 1, 1, crs=4326))
