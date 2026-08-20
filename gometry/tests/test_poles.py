"""Pole-encircling geographic polygons — membership and sensible geodesic area."""

import math

import gometry as gm

from tests._support import bools


def _pole_hexagons() -> tuple[gm.Polygon, gm.Polygon]:
    ring = [(0, 80), (60, 80), (120, 80), (180, 80), (-120, 80), (-60, 80)]
    hex_north = gm.Polygon(ring, crs=4326)
    hex_south = gm.Polygon([(x, -abs(y)) for x, y in ring], crs=4326)
    return (hex_north, hex_south)


def test_pole_encircling_hexagon_contains_and_covers() -> None:
    hex_north, hex_south = _pole_hexagons()
    north_pole = gm.Point(0, 90, crs=4326)
    south_pole = gm.Point(0, -90, crs=4326)
    assert gm.contains(hex_north, north_pole)
    assert not gm.contains(hex_north, south_pole)
    assert gm.covers(hex_north, north_pole)
    assert gm.intersects(hex_north, north_pole)
    assert gm.within(north_pole, hex_north)
    assert gm.contains(hex_south, south_pole)
    assert not gm.contains(hex_south, north_pole)
    assert gm.covers(hex_south, south_pole)
    assert gm.intersects(hex_south, south_pole)
    assert gm.within(south_pole, hex_south)


def test_pole_encircling_hexagon_array_predicates() -> None:
    hex_north, hex_south = _pole_hexagons()
    rows = gm.GeometryArray([hex_north, hex_south])
    north_pole = gm.Point(0, 90, crs=4326)
    south_pole = gm.Point(0, -90, crs=4326)
    assert bools(gm.contains(rows, gm.GeometryArray([north_pole, north_pole]))) == [
        True,
        False,
    ]
    assert bools(gm.contains(rows, south_pole)) == [False, True]
    assert bools(gm.covers(rows, gm.GeometryArray([north_pole, south_pole]))) == [
        True,
        True,
    ]
    assert bools(gm.intersects(rows, gm.GeometryArray([north_pole, south_pole]))) == [
        True,
        True,
    ]


def test_pole_boundary_predicates_match_scalar_and_cell_polygon_batches() -> None:
    """A planar bounds proxy may not reject an exact geographic pole."""
    for latitude in (90.0, -90.0):
        pole = gm.Point(0.0, latitude, crs=4326)
        near = 80.0 if latitude > 0.0 else -80.0
        cap = gm.Polygon(
            [(-10.0, near), (10.0, near), (10.0, latitude), (-10.0, latitude)],
            crs=4326,
        )
        expected = {
            'intersects': True,
            'covers': True,
            'touches': True,
            'disjoint': False,
        }
        for name, scalar in expected.items():
            predicate = getattr(gm, name)
            assert bool(predicate(cap, pole)) is scalar
            assert bools(predicate(gm.GeometryArray([cap], crs=4326), pole)) == [scalar]

        for cell in (
            gm.H3Cell(0.0, latitude, resolution=2),
            gm.S2Cell(0.0, latitude, level=2),
        ):
            polygons = gm.CellArray([cell], type=type(cell)).polygon
            for name in expected:
                predicate = getattr(gm, name)
                assert bools(predicate(polygons, pole)) == [
                    bool(predicate(cell.polygon, pole))
                ]


def test_pairwise_pole_longitude_neighbours_bypass_planar_bounds_gates() -> None:
    """Every spelling of a physical pole must reach geographic membership."""
    for latitude in (90.0, -90.0):
        near = 80.0 if latitude > 0.0 else -80.0
        cap = gm.Polygon(
            [(-10.0, near), (10.0, near), (10.0, latitude), (-10.0, latitude)],
            crs=4326,
        )
        caps = gm.GeometryArray([cap], crs=4326)
        # Each center lies beyond the cap's planar envelope. Sweep every
        # stored-double neighbour in both directions: all spell the same pole.
        for center in (-179.0, -170.0, 170.0, 179.0):
            for longitude in (
                math.nextafter(center, -math.inf),
                center,
                math.nextafter(center, math.inf),
            ):
                pole = gm.Point(longitude, latitude, crs=4326)
                poles = gm.GeometryArray([pole], crs=4326)
                for name in ('contains', 'covers', 'intersects', 'touches', 'disjoint'):
                    predicate = getattr(gm, name)
                    assert bools(predicate(caps, poles)) == [bool(predicate(cap, pole))]
                for name in (
                    'within',
                    'covered_by',
                    'intersects',
                    'touches',
                    'disjoint',
                ):
                    predicate = getattr(gm, name)
                    assert bools(predicate(poles, caps)) == [bool(predicate(pole, cap))]
                prepared = pole.prepare()
                for name in ('intersects', 'covers', 'disjoint'):
                    expected = bool(getattr(gm, name)(pole, cap))
                    assert bools(getattr(gm, name)(prepared, caps)) == [expected]


def test_scalar_polar_cap_packed_and_prepared_predicates_use_pole_topology() -> None:
    """Packed point short-circuits may not reject a nonzero-longitude pole."""
    for latitude in (90.0, -90.0):
        near = 80.0 if latitude > 0.0 else -80.0
        cap = gm.Polygon(
            [(-10.0, near), (10.0, near), (10.0, latitude), (-10.0, latitude)],
            crs=4326,
        )
        prepared = cap.prepare()
        for pole_latitude in (
            math.nextafter(latitude, 0.0),
            latitude,
            math.nextafter(latitude, math.copysign(math.inf, latitude)),
        ):
            for longitude in (0.0, -170.0, 170.0):
                pole = gm.Point(longitude, pole_latitude, crs=4326)
                packed = gm.GeometryArray([pole], crs=4326)
                for name in ('covers', 'intersects', 'touches', 'disjoint'):
                    expected = bool(getattr(gm, name)(cap, pole))
                    assert bools(getattr(gm, name)(cap, packed)) == [expected]
                    assert bools(getattr(gm, name)(prepared, packed)) == [expected]


def test_pole_longitude_neighbours_reach_every_spatial_index_route() -> None:
    """Index envelopes may narrow candidates, never reject a pole spelling."""
    for latitude in (90.0, -90.0):
        near = 80.0 if latitude > 0.0 else -80.0
        cap = gm.Polygon(
            [(-10.0, near), (10.0, near), (10.0, latitude), (-10.0, latitude)],
            crs=4326,
        )
        cap_index = gm.SpatialIndex([cap])
        for pole_latitude in (
            math.nextafter(latitude, 0.0),
            latitude,
            math.nextafter(latitude, math.copysign(math.inf, latitude)),
        ):
            for center in (-179.0, -170.0, 170.0, 179.0):
                for longitude in (
                    math.nextafter(center, -math.inf),
                    center,
                    math.nextafter(center, math.inf),
                ):
                    pole = gm.Point(longitude, pole_latitude, crs=4326)
                    poles = gm.GeometryArray([pole], crs=4326)
                    expected = bool(gm.intersects(pole, cap))
                    expected_ids = [0] if expected else []
                    if expected:
                        assert cap_index.candidates(pole).tolist() == [0]
                    assert (
                        cap_index.query(pole, predicate='intersects').tolist()
                        == expected_ids
                    )
                    assert cap_index.query(poles, predicate='intersects').to_list() == [
                        expected_ids
                    ]
                    assert tuple(
                        values.tolist()
                        for values in gm.join(
                            poles,
                            gm.GeometryArray([cap], crs=4326),
                            predicate='intersects',
                        )
                    ) == (([0], [0]) if expected else ([], []))
                    assert tuple(
                        values.tolist()
                        for values in gm.SpatialIndex([pole, cap]).self_join(
                            predicate='intersects'
                        )
                    ) == (([0], [1]) if expected else ([], []))
                    # The point must also be widened when it is an indexed packed
                    # row, and when it enters through the mutable overflow path.
                    assert (
                        gm
                        .SpatialIndex(poles)
                        .query(cap, predicate='intersects')
                        .tolist()
                        == expected_ids
                    )
                    mutable = gm.SpatialIndex([cap])
                    assert mutable.insert(pole) == 1
                    assert tuple(
                        values.tolist()
                        for values in mutable.self_join(predicate='intersects')
                    ) == (([0], [1]) if expected else ([], []))


def _pole_reaching_index_shapes(
    latitude: float, longitude: float
) -> dict[str, gm.Geometry]:
    near = 80.0 if latitude > 0.0 else -80.0
    line = [(longitude - 10.0, latitude), (longitude + 10.0, latitude)]
    ring = [
        (longitude - 10.0, near),
        (longitude + 10.0, near),
        (longitude + 10.0, latitude),
        (longitude - 10.0, latitude),
    ]
    return {
        'point': gm.Point(longitude, latitude, crs=4326),
        'line': gm.LineString(line, crs=4326),
        'multi_point': gm.MultiPoint(line, crs=4326),
        'multi_line': gm.MultiLineString([line], crs=4326),
        'polygon': gm.Polygon(ring, crs=4326),
        'multi_polygon': gm.MultiPolygon([ring], crs=4326),
        'collection': gm.GeometryCollection(
            [
                gm.Point(longitude, latitude, crs=4326),
                gm.LineString(line, crs=4326),
                gm.Polygon(ring, crs=4326),
            ],
            crs=4326,
        ),
    }


def test_pole_normalization_closes_every_index_and_join_geometry_lane() -> None:
    """Every pole-carrying shape widens before an R-tree can reject it."""
    for latitude in (90.0, -90.0):
        near = 80.0 if latitude > 0.0 else -80.0
        cap = gm.Polygon(
            [(-10.0, near), (10.0, near), (10.0, latitude), (-10.0, latitude)],
            crs=4326,
        )
        cap_rows = gm.GeometryArray([cap], crs=4326)
        cap_index = gm.SpatialIndex([cap])
        for pole_latitude in (
            math.nextafter(latitude, 0.0),
            latitude,
            math.nextafter(latitude, math.copysign(math.inf, latitude)),
        ):
            for longitude in (0.0, 170.0):
                for shape in _pole_reaching_index_shapes(
                    pole_latitude, longitude
                ).values():
                    rows = gm.GeometryArray([shape], crs=4326)
                    expected = bool(gm.intersects(shape, cap))
                    expected_ids = [0] if expected else []
                    if expected:
                        assert cap_index.candidates(shape).tolist() == [0]
                    assert (
                        cap_index.query(shape, predicate='intersects').tolist()
                        == expected_ids
                    )
                    assert cap_index.query(rows, predicate='intersects').to_list() == [
                        expected_ids
                    ]
                    assert (
                        gm
                        .SpatialIndex(rows)
                        .query(cap, predicate='intersects')
                        .tolist()
                        == expected_ids
                    )
                    assert tuple(
                        values.tolist()
                        for values in gm.join(rows, cap_rows, predicate='intersects')
                    ) == (([0], [0]) if expected else ([], []))


def test_pole_encircling_hexagon_area_is_nontrivial() -> None:
    hex_north, _ = _pole_hexagons()
    area = hex_north.area
    assert area > 0.0
    assert math.isfinite(area)
    assert area < 0.5 * 510000000000000.0
