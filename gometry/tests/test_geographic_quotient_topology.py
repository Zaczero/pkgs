"""Public geographic quotient topology regression paths."""

import csv
import hashlib
import json
import math
from fractions import Fraction
from itertools import pairwise
from pathlib import Path
from typing import Any

import gometry as gm
import numpy as np
import pytest

from tests._support import WEST_RADIAN_SOUTH_GRAD_CRS, ids, pair_rows


def seam_box() -> gm.Polygon:
    return gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)], crs=4326
    )


def _north_polar_cap() -> gm.Polygon:
    return gm.Polygon(
        [(-180, 80), (-90, 80), (0, 80), (90, 80), (180, 80), (-180, 80)],
        crs=4326,
    )


_T2_NOTCHED_SHELL = [
    (-180, 5),
    (-90, 5),
    (-10, 5),
    (-10, -10),
    (0, -10),
    (10, -10),
    (10, 5),
    (90, 5),
    (180, 5),
    (180, 90),
    (90, 90),
    (0, 90),
    (-90, 90),
    (-180, 90),
    (-180, 5),
]


def _t2_chart_area(geometry: gm.Geometry) -> float:
    polygons = (
        [geometry] if geometry.geometry_type == 'Polygon' else list(geometry.parts)
    )

    def ring_area(coordinates: Any) -> float:
        points = [(float(point[0]), float(point[1])) for point in coordinates]
        return abs(
            sum(
                start[0] * end[1] - end[0] * start[1] for start, end in pairwise(points)
            )
            / 2.0
        )

    return sum(
        ring_area(polygon.exterior.coords)
        - sum(ring_area(hole.coords) for hole in polygon.interiors)
        for polygon in polygons
    )


RingPoints = tuple[tuple[float, float], ...]
BoxRings = tuple[RingPoints, ...]

_COLLINEAR_BOX_CASES: tuple[tuple[str, BoxRings], ...] = (
    (
        'north-pole-short',
        (((-60.0, 80.0), (60.0, 80.0), (60.0, 90.0), (-60.0, 90.0), (-60.0, 80.0)),),
    ),
    (
        'north-pole-wide',
        (
            (
                (-120.0, 70.0),
                (0.0, 70.0),
                (120.0, 70.0),
                (120.0, 90.0),
                (0.0, 90.0),
                (-120.0, 90.0),
                (-120.0, 70.0),
            ),
        ),
    ),
    (
        'north-pole-full-longitude',
        (
            (
                (-180.0, 70.0),
                (-60.0, 70.0),
                (60.0, 70.0),
                (180.0, 70.0),
                (180.0, 90.0),
                (60.0, 90.0),
                (-60.0, 90.0),
                (-180.0, 90.0),
                (-180.0, 70.0),
            ),
        ),
    ),
    (
        'south-pole-short',
        (
            (
                (-60.0, -90.0),
                (60.0, -90.0),
                (60.0, -80.0),
                (-60.0, -80.0),
                (-60.0, -90.0),
            ),
        ),
    ),
    (
        'south-pole-wide',
        (
            (
                (-120.0, -90.0),
                (0.0, -90.0),
                (120.0, -90.0),
                (120.0, -70.0),
                (0.0, -70.0),
                (-120.0, -70.0),
                (-120.0, -90.0),
            ),
        ),
    ),
    (
        'south-pole-full-longitude',
        (
            (
                (-180.0, -90.0),
                (-60.0, -90.0),
                (60.0, -90.0),
                (180.0, -90.0),
                (180.0, -70.0),
                (60.0, -70.0),
                (-60.0, -70.0),
                (-180.0, -70.0),
                (-180.0, -90.0),
            ),
        ),
    ),
    (
        'full-longitude-band',
        (
            (
                (-180.0, 70.0),
                (-60.0, 70.0),
                (60.0, 70.0),
                (180.0, 70.0),
                (180.0, 80.0),
                (60.0, 80.0),
                (-60.0, 80.0),
                (-180.0, 80.0),
                (-180.0, 70.0),
            ),
        ),
    ),
    (
        'seam-crossing',
        (
            (
                (170.0, -10.0),
                (180.0, -10.0),
                (180.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ),
            (
                (-180.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (-180.0, 10.0),
                (-180.0, -10.0),
            ),
        ),
    ),
    (
        'ordinary',
        (((-20.0, 10.0), (20.0, 10.0), (20.0, 20.0), (-20.0, 20.0), (-20.0, 10.0)),),
    ),
)


def _polygonal_box(rings: BoxRings) -> gm.Geometry:
    polygons = [gm.Polygon(ring, crs=4326) for ring in rings]
    if len(polygons) == 1:
        return polygons[0]
    return gm.MultiPolygon(polygons, crs=4326)


def _densify_box_edges(
    geometry: gm.Geometry, *, tile_domain: bool = False
) -> gm.Geometry:
    polygons = (
        [geometry] if geometry.geometry_type == 'Polygon' else list(geometry.parts)
    )
    rings = []
    for polygon in polygons:
        coordinates = np.asarray(polygon.coords, dtype=np.float64).tolist()
        dense = []
        for start, end in pairwise(coordinates):
            dense.append((float(start[0]), float(start[1])))
            start_is_pole = abs(float(start[1])) == 90.0
            end_is_pole = abs(float(end[1])) == 90.0
            subdivisions = 2 if tile_domain and (start_is_pole != end_is_pole) else 3
            for step in range(1, subdivisions):
                fraction = step / subdivisions
                dense.append((
                    float(start[0] + (end[0] - start[0]) * fraction),
                    float(start[1] + (end[1] - start[1]) * fraction),
                ))
        dense.append((float(coordinates[-1][0]), float(coordinates[-1][1])))
        rings.append(tuple(dense))
    return _polygonal_box(tuple(rings))


def _exact_predicate_signature(
    left: gm.Geometry, right: gm.Geometry
) -> tuple[bool, bool, bool, bool, bool, str]:
    return (
        gm.intersects(left, right),
        gm.covers(left, right),
        gm.within(left, right),
        gm.contains(left, right),
        gm.touches(left, right),
        gm.relate(left, right),
    )


def test_short_north_polar_box_is_disjoint_from_antimeridian_tile() -> None:
    polar_box = _polygonal_box((
        (
            (-60.0, 80.0),
            (60.0, 80.0),
            (60.0, 90.0),
            (-60.0, 90.0),
            (-60.0, 80.0),
        ),
    ))
    tile = _polygonal_box((
        (
            (-180.0, 82.67628497834903),
            (-157.5, 82.67628497834903),
            (-157.5, 85.0511287798066),
            (-180.0, 85.0511287798066),
            (-180.0, 82.67628497834903),
        ),
    ))
    assert gm.relate(polar_box, tile) == 'FF2FF1212'


@pytest.mark.parametrize(
    ('name', 'rings'),
    _COLLINEAR_BOX_CASES,
    ids=[name for name, _ in _COLLINEAR_BOX_CASES],
)
def test_geographic_predicates_ignore_collinear_box_subdivision(
    name: str,
    rings: BoxRings,
) -> None:
    minimal = _polygonal_box(rings)
    densified = _densify_box_edges(minimal)

    points = [
        gm.Point(longitude, latitude, crs=4326)
        for longitude, latitude in [
            (-180.0, -90.0),
            (-180.0, 85.0),
            (-60.0, -85.0),
            (-60.0, 80.0),
            (0.0, -90.0),
            (0.0, 0.0),
            (0.0, 85.0),
            (0.0, 90.0),
            (60.0, -80.0),
            (60.0, 85.0),
            (180.0, 0.0),
            (180.0, 90.0),
        ]
    ]
    longitude_windows = [(-180.0, -157.5), (-60.0, 60.0), (157.5, 180.0)]
    latitude_windows = [(-90.0, -80.0), (-10.0, 10.0), (70.0, 80.0), (80.0, 90.0)]
    rectangles = [
        _polygonal_box((
            (
                (-180.0, 82.67628497834903),
                (-157.5, 82.67628497834903),
                (-157.5, 85.0511287798066),
                (-180.0, 85.0511287798066),
                (-180.0, 82.67628497834903),
            ),
        ))
    ]
    rectangles.extend(
        _polygonal_box((
            (
                (west, south),
                (east, south),
                (east, north),
                (west, north),
                (west, south),
            ),
        ))
        for west, east in longitude_windows
        for south, north in latitude_windows
    )

    for probe_index, probe in enumerate([*rectangles, *points]):
        for reverse in (False, True):
            if reverse:
                left, right = probe, minimal
                dense_left, dense_right = probe, densified
            else:
                left, right = minimal, probe
                dense_left, dense_right = densified, probe
            assert _exact_predicate_signature(
                left, right
            ) == _exact_predicate_signature(dense_left, dense_right), (
                f'{name}: probe {probe_index}, reverse={reverse}'
            )


@pytest.mark.parametrize(
    ('name', 'rings'),
    _COLLINEAR_BOX_CASES,
    ids=[name for name, _ in _COLLINEAR_BOX_CASES],
)
def test_geographic_grid_covers_ignore_collinear_box_subdivision(
    name: str,
    rings: BoxRings,
) -> None:
    minimal = _polygonal_box(rings)
    densified = _densify_box_edges(minimal, tile_domain=True)

    def cover_tokens(geometry: gm.Geometry) -> tuple[tuple[str, ...], ...]:
        return (
            tuple(
                sorted(cell.token for cell in gm.h3_cover(geometry, 0, max_cells=None))
            ),
            tuple(
                sorted(
                    cell.token
                    for cell in gm.s2_cover(geometry, level=2, max_cells=None)
                )
            ),
            tuple(
                sorted(
                    cell.token for cell in gm.geohash_cover(geometry, 1, max_cells=None)
                )
            ),
            tuple(
                sorted(
                    cell.token for cell in gm.tile_cover(geometry, 3, max_cells=None)
                )
            ),
        )

    assert cover_tokens(minimal) == cover_tokens(densified), name


@pytest.mark.parametrize(
    ('name', 'rings'),
    _COLLINEAR_BOX_CASES,
    ids=[name for name, _ in _COLLINEAR_BOX_CASES],
)
def test_geographic_measures_ignore_collinear_box_subdivision(
    name: str,
    rings: BoxRings,
) -> None:
    minimal = _polygonal_box(rings)
    densified = _densify_box_edges(minimal)
    assert minimal.area == pytest.approx(densified.area, rel=1e-13, abs=1e-7), name
    assert minimal.length == pytest.approx(densified.length, rel=1e-13, abs=1e-7), name

    values = gm.GeometryArray([minimal, densified], crs=4326)
    assert values.area[0] == pytest.approx(values.area[1], rel=1e-13, abs=1e-7), name
    assert values.length[0] == pytest.approx(values.length[1], rel=1e-13, abs=1e-7), (
        name
    )


def _country_source_edges(
    geojson: dict[str, Any],
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    edges = []
    for feature in geojson['features']:
        geometry = feature['geometry']
        if geometry['type'] == 'Polygon':
            polygons = [geometry['coordinates']]
        elif geometry['type'] == 'MultiPolygon':
            polygons = geometry['coordinates']
        else:
            raise AssertionError(f'unexpected country geometry: {geometry["type"]}')
        for polygon in polygons:
            for ring in polygon:
                for start, end in pairwise(ring):
                    edges.append((
                        (float(start[0]), float(start[1])),
                        (float(end[0]), float(end[1])),
                    ))
    return edges


def _is_nearest_even_source_crossing(
    edges: list[tuple[tuple[float, float], tuple[float, float]]],
    longitude: float,
    latitude: float,
) -> bool:
    period = Fraction(360)
    x_low = Fraction(math.nextafter(longitude, -math.inf))
    x_high = Fraction(math.nextafter(longitude, math.inf))
    y_low = Fraction(math.nextafter(latitude, -math.inf))
    y_high = Fraction(math.nextafter(latitude, math.inf))
    nearby = []
    for edge_index, (start, end) in enumerate(edges):
        start_x = Fraction(start[0])
        end_x = Fraction(end[0])
        start_y = Fraction(start[1])
        end_y = Fraction(end[1])
        for turns in (-1, 0, 1):
            shift = turns * period
            first = (start_x + shift, start_y)
            second = (end_x + shift, end_y)
            if (
                max(first[0], second[0]) >= x_low
                and min(first[0], second[0]) <= x_high
                and max(first[1], second[1]) >= y_low
                and min(first[1], second[1]) <= y_high
            ):
                nearby.append((edge_index, first, second))

    def cross(
        first: tuple[Fraction, Fraction], second: tuple[Fraction, Fraction]
    ) -> Fraction:
        return first[0] * second[1] - first[1] * second[0]

    for index, (left_edge, start, end) in enumerate(nearby):
        direction = (end[0] - start[0], end[1] - start[1])
        for right_edge, other_start, other_end in nearby[index + 1 :]:
            if left_edge == right_edge:
                continue
            other_direction = (
                other_end[0] - other_start[0],
                other_end[1] - other_start[1],
            )
            denominator = cross(direction, other_direction)
            if denominator == 0:
                continue
            offset = (other_start[0] - start[0], other_start[1] - start[1])
            left_parameter = cross(offset, other_direction) / denominator
            right_parameter = cross(offset, direction) / denominator
            if not (0 < left_parameter < 1 and 0 < right_parameter < 1):
                continue
            exact_x = start[0] + left_parameter * direction[0]
            exact_y = start[1] + left_parameter * direction[1]
            exact_x -= period * ((exact_x + 180) // period)
            if float(exact_x) == longitude and float(exact_y) == latitude:
                return True
    return False


def _indexed_exact_covers(
    index: gm.SpatialIndex,
    geometries: list[gm.Geometry],
    points: list[gm.Geometry],
) -> list[bool]:
    candidates = index.candidates(gm.GeometryArray(points))
    pair_geometries = []
    pair_points = []
    for point, rows in zip(points, candidates, strict=True):
        for row in rows:
            pair_geometries.append(geometries[int(row)])
            pair_points.append(point)
    pair_results = (
        gm.covers(
            gm.GeometryArray(pair_geometries), gm.GeometryArray(pair_points)
        ).tolist()
        if pair_geometries
        else []
    )
    memberships = []
    offset = 0
    for rows in candidates:
        end = offset + len(rows)
        memberships.append(any(pair_results[offset:end]))
        offset = end
    return memberships


def test_quotient_seam_membership_reaches_every_topology_consumer() -> None:
    source = seam_box()
    point = gm.Point(180, 0, crs=4326)
    outside = gm.Point(0, 0, crs=4326)

    assert gm.intersects(source, point)
    assert gm.intersects(point, source)
    assert gm.intersects(gm.GeometryArray([source, source]), point).tolist() == [
        True,
        True,
    ]
    assert gm.intersects(source, gm.GeometryArray([point, outside])).tolist() == [
        True,
        False,
    ]
    assert gm.relate_pattern(source, point, 'T*****FF*')

    index = gm.SpatialIndex([source])
    assert ids(index.query(point, predicate='intersects')) == [0]
    assert [
        row.tolist()
        for row in index.query(
            gm.GeometryArray([point, outside]), predicate='intersects'
        )
    ] == [
        [0],
        [],
    ]
    assert pair_rows(index.join(gm.GeometryArray([point]), predicate='intersects')) == [
        (0, 0)
    ]
    assert pair_rows(
        gm.join(gm.GeometryArray([point]), [source], predicate='intersects')
    ) == [(0, 0)]
    assert pair_rows(
        gm.SpatialIndex([source, point]).self_join(predicate='intersects')
    ) == [(0, 1)]

    assert gm.intersection(source, point).to_wkt() == point.to_wkt()
    assert len(gm.h3_cover(source, resolution=2, cell_rule='center')) > 0
    assert gm.intersects(source, gm.box(179, -1, 180, 1, crs=4326))


def test_subnormal_seam_crossing_keeps_exact_predicate_and_nary_contacts() -> None:
    """S2's lifted edge reaches the seam at t=1/4 and exact latitude δ/4."""
    point = gm.Point(180, 0, crs=4326)
    delta = math.ulp(0.0)
    cases = (
        (
            gm.LineString([(170, 0), (-160, delta)], crs=4326),
            False,
            'FF1FF00F2',
        ),
        (
            gm.LineString([(170, 0), (-150, delta)], crs=4326),
            False,
            'FF1FF00F2',
        ),
        (
            gm.LineString([(170, 0), (-160, 0)], crs=4326),
            True,
            '0F1FF0FF2',
        ),
    )
    for line, intersects, matrix in cases:
        assert gm.intersects(line, point) is intersects
        assert gm.contains(line, point) is intersects
        assert gm.within(point, line) is intersects
        assert gm.relate(line, point) == matrix
        result = gm.intersection_all([line, point])
        assert result.is_empty is (not intersects)
        if intersects:
            assert result.geometry_type == 'Point'
            assert gm.equals(result, point)


def test_ordinary_geographic_polygons_relate_like_their_planar_twins() -> None:
    """Away from the seam and the poles a chart-linear polygon pair is its planar pair.

    The full nine-byte matrix is asserted, both ways, because a wrong boundary
    row once passed every named predicate this file checks.
    """
    from shapely import from_wkt, relate

    hexagon = gm.Polygon(
        [(math.cos(t), math.sin(t)) for t in (i * math.pi / 3 for i in range(6))]
        + [(1.0, 0.0)],
        crs=4326,
    )
    whole = gm.box(-10, -10, 10, 10, crs=4326)
    partial = gm.box(-10, -10, 0.25, 10, crs=4326)
    apart = gm.box(5, 5, 10, 10, crs=4326)
    for left, right in ((hexagon, whole), (hexagon, partial), (hexagon, apart)):
        expected = relate(from_wkt(left.to_wkt()), from_wkt(right.to_wkt()))
        assert gm.relate(left, right) == expected
        assert gm.relate(right, left) == relate(
            from_wkt(right.to_wkt()), from_wkt(left.to_wkt())
        )
    assert gm.relate(hexagon, whole) == '2FF1FF212'
    assert gm.relate(hexagon, partial) == '212101212'
    assert gm.relate(hexagon, hexagon) == '2FFF1FFF2'
    boundary_vertex = gm.Point(math.cos(math.pi / 3), math.sin(math.pi / 3), crs=4326)
    assert gm.relate(hexagon, boundary_vertex) == 'FF20F1FF2'
    assert gm.intersects(hexagon, gm.GeometryArray([whole, apart])).tolist() == [
        True,
        False,
    ]
    assert gm.intersects(gm.GeometryArray([whole, apart]), hexagon).tolist() == [
        True,
        False,
    ]

    index = gm.SpatialIndex([hexagon])
    assert ids(index.query(partial, predicate='intersects')) == [0]
    assert ids(index.query(partial, predicate='within')) == []
    assert ids(index.query(whole, predicate='contains')) == [0]
    assert pair_rows(
        gm.join(gm.GeometryArray([partial, apart]), [hexagon], predicate='intersects')
    ) == [(0, 0)]


def test_country_union_rounding_residuals_are_nearest_even_crossings() -> None:
    fixture = Path(__file__).parents[1] / 'fixtures/osm_countries_0_1.geojson'
    fixture_bytes = fixture.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == (
        'ca8c0a9019e2ab11c880ce85ecf7dd73d9b2a4b15e3265d41daf1af2bbfcfe66'
    )
    source_data = json.loads(fixture_bytes)
    country_array = gm.from_geojson(fixture_bytes.decode('utf-8'), crs=4326)
    countries = list(country_array)
    assert len(countries) == 217
    validity = [country.is_valid for country in countries]
    assert sum(validity) == 217
    assert country_array.is_valid.tolist() == validity
    assert country_array.is_simple.tolist() == [
        country.is_simple for country in countries
    ]
    countries_by_code = {
        feature['properties']['tags']['ISO3166-1:alpha2']: (country, validity[index])
        for index, (feature, country) in enumerate(
            zip(source_data['features'], countries, strict=True)
        )
    }
    periodic_country_bounds = {
        'RU': (19.4041722, 41.1850968, -168.976944, 82.0546231),
        'US': (144.4137024, -14.760345, -64.3562671, 71.5729219),
        'FJ': (174.4230867, -21.9420593, -178.011665, -12.2865087),
    }
    for code in ('RU', 'US', 'FJ'):
        country, is_valid = countries_by_code[code]
        assert country.geometry_type == 'MultiPolygon'
        assert country.bounds == pytest.approx(periodic_country_bounds[code])
        assert is_valid

    source_index = gm.SpatialIndex(countries)
    union = gm.union_all(countries)
    union_parts = [union] if union.geometry_type == 'Polygon' else list(union.parts)
    assert union.geometry_type in ('Polygon', 'MultiPolygon')
    with (
        Path(__file__).with_name('fixtures') / 'country_union_membership_candidates.csv'
    ).open(newline='') as source:
        candidates = [
            (
                int(row['component']),
                float(row['longitude']),
                float(row['latitude']),
            )
            for row in csv.DictReader(source)
        ]
    assert len(candidates) == 193

    # Candidate 240 is the expected construction-rounding residual. The
    # First-release contract ("Rounded Noding / First-release contract") keeps
    # checked nearest-f64 emission: exact membership belongs to Q, not to the
    # rounded reconstruction of its source geometries.
    candidate_240 = (240, 35.021769720844745, 32.14166086431683)
    assert candidate_240 in candidates

    candidate_points = [
        gm.Point(longitude, latitude, crs=4326) for _, longitude, latitude in candidates
    ]
    # The index supplies only a bounding-box prefilter; exact public `covers`
    # decides source membership for every candidate row.
    source_membership = _indexed_exact_covers(source_index, countries, candidate_points)
    # Intersecting one MultiPoint checks all candidates against the dissolved
    # output in one exact Q build instead of rebuilding Q once per point.
    union_intersection = gm.intersection(
        union,
        gm.MultiPoint(
            [(longitude, latitude) for _, longitude, latitude in candidates],
            crs=4326,
        ),
    )
    if union_intersection.is_empty:
        union_covered_points = set()
    elif union_intersection.geometry_type == 'Point':
        union_covered_points = {(union_intersection.x, union_intersection.y)}
    elif union_intersection.geometry_type == 'MultiPoint':
        union_covered_points = {
            (point.x, point.y) for point in union_intersection.parts
        }
    else:
        raise AssertionError(
            f'unexpected point intersection: {union_intersection.geometry_type}'
        )
    residuals = []
    for (component, longitude, latitude), source_covered in zip(
        candidates, source_membership, strict=True
    ):
        union_covers = (longitude, latitude) in union_covered_points
        if source_covered:
            assert union_covers, (
                component,
                longitude,
                latitude,
                'union false negative',
            )
        elif union_covers:
            residuals.append((component, longitude, latitude))

    assert candidate_240 in residuals
    residual_coordinates = {
        (longitude, latitude) for _, longitude, latitude in residuals
    }
    emitted_vertices = set()
    for polygon in union_parts:
        for ring in (polygon.exterior, *polygon.interiors):
            for coordinate in ring.coords:
                vertex = (float(coordinate[0]), float(coordinate[1]))
                if vertex in residual_coordinates:
                    emitted_vertices.add(vertex)

    source_edges = _country_source_edges(source_data)
    for component, longitude, latitude in residuals:
        point = (longitude, latitude)
        assert point in emitted_vertices, (component, point, 'not an emitted vertex')
        assert _is_nearest_even_source_crossing(source_edges, longitude, latitude), (
            component,
            point,
            'not a nearest-even constructed crossing',
        )
        neighbor_longitudes = (
            math.nextafter(longitude, -math.inf),
            longitude,
            math.nextafter(longitude, math.inf),
        )
        latitudes = (
            math.nextafter(latitude, -math.inf),
            latitude,
            math.nextafter(latitude, math.inf),
        )
        neighbors = [
            gm.Point(neighbor_x, neighbor_y, crs=4326)
            for neighbor_x in neighbor_longitudes
            for neighbor_y in latitudes
        ]
        assert any(_indexed_exact_covers(source_index, countries, neighbors)), (
            component,
            point,
            'no source-covered point within one ULP per ordinate',
        )


def test_geographic_overlay_uses_pole_equivalence_for_binary_and_aggregates() -> None:
    """A pole has one location even when its source vertices spell it differently."""
    left = gm.Polygon([(0, 90), (10, 80), (-10, 80), (0, 90)], crs=4326)
    right = gm.Polygon([(100, 90), (90, 80), (110, 80), (100, 90)], crs=4326)
    pole = gm.Point(-120, 90, crs=4326)

    # Predicates already use the quotient. Overlay must agree for both its
    # binary dispatcher and shared all-input assembly.
    assert gm.intersects(left, right)
    for result in (
        gm.intersection(left, right),
        gm.intersection_all([left, right]),
        gm.intersection_all(gm.GeometryArray([left, right])),
    ):
        assert result.geometry_type == 'Point'
        assert gm.equals(result, pole)


def test_geographic_overlay_keeps_seam_and_polar_area_membership() -> None:
    """The quotient graph augments, rather than replaces, the seam chart."""
    seam_left = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 20), (170, 20), (170, 0)], crs=4326
    )
    seam_right = gm.Polygon(
        [(175, 10), (-175, 10), (-175, 30), (175, 30), (175, 10)], crs=4326
    )
    cap = gm.Polygon(
        [(-180, 80), (-90, 80), (0, 80), (90, 80), (180, 80), (-180, 80)],
        crs=4326,
    )
    polar_hole = gm.Polygon(
        [(-180, 70), (-90, 70), (0, 70), (90, 70), (180, 70), (-180, 70)],
        holes=[[(-180, 80), (-90, 80), (0, 80), (90, 80), (180, 80), (-180, 80)]],
        crs=4326,
    )
    polar_triangle = gm.Polygon([(120, 90), (110, 85), (130, 85), (120, 90)], crs=4326)
    full_band = gm.box(-180, -10, 180, 10, crs=4326)
    band_window = gm.box(20, -5, 40, 5, crs=4326)
    full_band_index = gm.SpatialIndex([full_band])

    for longitude in (-180, 180):
        seam_point = gm.Point(longitude, 0, crs=4326)
        assert gm.covers(full_band, seam_point)
        assert gm.intersects(full_band, seam_point)
        assert ids(full_band_index.query(seam_point, predicate='intersects')) == [0]

    seam_intersection = gm.intersection_all([seam_left, seam_right])
    assert gm.covers(seam_intersection, gm.Point(180, 15, crs=4326))
    assert gm.equals(gm.intersection(cap, polar_triangle), polar_triangle)
    assert gm.intersection(polar_hole, polar_triangle).is_empty
    assert gm.equals(gm.intersection(full_band, band_window), band_window)


@pytest.mark.parametrize('ccw', [True, False])
def test_canonical_full_band_union_keeps_its_chart_region(ccw: bool) -> None:
    band = gm.box(-180.0, -80.0, 180.0, 80.0, ccw=ccw, crs=4326)
    result = gm.union_all([band])
    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert result.area == pytest.approx(band.area, rel=2e-12)
    for longitude in (-179.0, 0.0, 179.0):
        assert gm.covers(result, gm.Point(longitude, 0.0, crs=4326))
        assert not gm.covers(result, gm.Point(longitude, 85.0, crs=4326))


def test_oriented_mixed_unit_full_band_overlay_keeps_its_source_boundary() -> None:
    band = gm.box(
        -math.pi,
        -10.0 * 10.0 / 9.0,
        math.pi,
        10.0 * 10.0 / 9.0,
        crs=WEST_RADIAN_SOUTH_GRAD_CRS,
    )
    window = gm.box(
        -math.radians(40.0),
        -5.0 * 10.0 / 9.0,
        -math.radians(20.0),
        5.0 * 10.0 / 9.0,
        crs=WEST_RADIAN_SOUTH_GRAD_CRS,
    )
    result = gm.intersection(band, window)
    interior = gm.Point(-math.radians(30.0), 0.0, crs=WEST_RADIAN_SOUTH_GRAD_CRS)
    exterior = gm.Point(
        -math.radians(30.0), -20.0 * 10.0 / 9.0, crs=WEST_RADIAN_SOUTH_GRAD_CRS
    )

    assert gm.equals(result, window)
    assert gm.covers(band, interior)
    assert gm.covers(result, interior)
    assert not gm.covers(band, exterior)


def test_canceled_parallel_levels_do_not_fill_the_intervening_band() -> None:
    polygon = gm.Polygon(
        [
            (0, 10),
            (360, 10),
            (360, 15),
            (0, 15),
            (0, 20),
            (360, 20),
            (360, 0),
            (0, 0),
            (0, 10),
        ],
        crs=4326,
    )
    probes = gm.GeometryArray([
        gm.Point(90, 5, crs=4326),
        gm.Point(90, 12, crs=4326),
        gm.Point(90, 17, crs=4326),
    ])

    assert gm.covers(polygon, gm.Point(90, 5, crs=4326))
    assert not gm.covers(polygon, gm.Point(90, 12, crs=4326))
    assert gm.covers(polygon, gm.Point(90, 17, crs=4326))
    assert gm.covers(polygon, probes).tolist() == [True, False, True]


def test_geographic_bounds_use_primitive_periodic_support_across_storage_paths() -> (
    None
):
    def width(bounds: tuple[float, float, float, float]) -> float:
        return (bounds[2] - bounds[0]) % 360.0

    cloud = gm.MultiPoint([(0, 0), (120, 0), (-120, 0), (180, 0)], crs=4326)
    assert width(cloud.bounds) == pytest.approx(240.0)
    assert gm.Point(180, 3, crs=4326).bounds == (-180.0, 3.0, -180.0, 3.0)
    assert gm.Point(-180, 3, crs=4326).bounds == (-180.0, 3.0, -180.0, 3.0)

    packed = gm.points(
        [0.0, 120.0, -120.0, 180.0, 999.0],
        [0.0, 0.0, 0.0, 0.0, 999.0],
        crs=4326,
    )
    nullable = packed._with_missing([False, False, False, False, True])
    for selected in (
        nullable,
        nullable[:],
        nullable[[2, 0, 3, 1, 4]],
    ):
        assert width(selected.total_bounds) == pytest.approx(240.0)

    mixed = gm.GeometryArray(
        [
            gm.MultiPoint([(0, 0), (120, 0), (-120, 0)], crs=4326),
            gm.Point(180, 0, crs=4326),
        ],
        crs=4326,
    )
    assert width(mixed.total_bounds) == pytest.approx(240.0)


def test_packed_geographic_polygon_bounds_keep_periodic_support_on_windows_and_gathers() -> (
    None
):
    seam = gm.Polygon(
        [(170.0, 0.0), (-170.0, 0.0), (-170.0, 10.0), (170.0, 10.0)],
        crs=4326,
    )
    ordinary = gm.box(10.0, 10.0, 20.0, 20.0, crs=4326)
    packed = gm.GeometryArray([seam, ordinary, seam], crs=4326)
    expected = (170.0, 0.0, -170.0, 10.0)
    selections = (
        packed[:1],
        packed[[2, 0]],
        packed._with_missing([False, True, False]),
    )

    for selected in selections:
        assert selected.total_bounds == expected
        for index, missing in enumerate(selected.is_missing):
            if missing:
                assert all(math.isnan(float(value)) for value in selected.bounds[index])
            else:
                assert tuple(selected.bounds[index]) == expected


def test_south_full_turn_ring_emits_its_polar_shell() -> None:
    cap = gm.Polygon([(0, -80), (90, -80), (180, -80), (-90, -80), (0, -80)], crs=4326)

    result = gm.union_all([cap])

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(0, -85, crs=4326))
    assert not gm.contains(result, gm.Point(0, -70, crs=4326))


def test_explicit_period_edges_keep_q_on_the_lifted_chart_side() -> None:
    bump = gm.Polygon([(0, 60), (360, 60), (315, 65), (0, 60)], crs=4326)
    hop_closure = gm.Polygon(
        [
            (45, 60),
            (135, 60),
            (225, 60),
            (315, 60),
            (405, 60),
            (405, 90),
            (45, 90),
            (45, 60),
        ],
        crs=4326,
    )
    oblique_closure = gm.Polygon(
        [
            (0, 60),
            (90, 60),
            (180, 60),
            (270, 60),
            (360, 60),
            (450, 90),
            (360, 90),
            (270, 90),
            (180, 90),
            (90, 90),
            (0, 60),
        ],
        crs=4326,
    )

    for point in [(0, 80), (150, 80), (150, 62), (310, 62), (305, 63)]:
        assert gm.contains(bump, gm.Point(*point, crs=4326)), point
    assert not gm.contains(bump, gm.Point(150, 50, crs=4326))

    for shape in [hop_closure, oblique_closure]:
        assert gm.contains(shape, gm.Point(0, 75, crs=4326))
        assert gm.contains(shape, gm.Point(200, 75, crs=4326))
        assert not gm.contains(shape, gm.Point(0, 50, crs=4326))
        assert not gm.contains(shape, gm.Point(0, -50, crs=4326))


def test_intersection_emits_a_new_shell_at_the_north_pole() -> None:
    cap = gm.Polygon([(0, 80), (90, 80), (180, 80), (-90, 80), (0, 80)], crs=4326)
    wedge = gm.Polygon([(120, 90), (110, 75), (130, 75), (120, 90)], crs=4326)

    result = gm.intersection(cap, wedge)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(120, 85, crs=4326))
    assert not gm.contains(result, gm.Point(120, 78, crs=4326))
    assert not gm.equals(result, cap)
    assert not gm.equals(result, wedge)


def test_difference_emits_a_new_shell_across_the_seam() -> None:
    source = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 20), (170, 20), (170, 0)], crs=4326
    )
    cutter = gm.Polygon(
        [(175, 10), (-175, 10), (-175, 30), (175, 30), (175, 10)], crs=4326
    )

    result = gm.difference(source, cutter)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(172, 15, crs=4326))
    assert gm.contains(result, gm.Point(180, 5, crs=4326))
    assert not gm.contains(result, gm.Point(180, 15, crs=4326))
    assert not gm.equals(result, source)
    assert not gm.equals(result, cutter)


def test_t2_notched_intersection_pairs_winding_cycles_without_a_pole() -> None:
    source = gm.Polygon(_T2_NOTCHED_SHELL, crs=4326)
    band = gm.box(-180, -30, 180, 30, crs=4326)

    assert source.is_valid
    result = gm.intersection(source, band)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert _t2_chart_area(result) == 9300.0
    assert gm.contains(result, gm.Point(0, -5, crs=4326))
    assert gm.contains(result, gm.Point(0, 20, crs=4326))
    for point in (
        (50, 0),
        (0, 60),
        (0, 90),
        (0, -90),
    ):
        assert not gm.contains(result, gm.Point(*point, crs=4326)), point
    for point in ((50, 5), (50, 30)):
        assert gm.covers(result, gm.Point(*point, crs=4326)), point
    assert gm.covers(result, gm.Point(180, 20, crs=4326))
    assert gm.covers(result, gm.Point(-180, 20, crs=4326))


@pytest.mark.parametrize('reverse_winding_hole', [False, True])
def test_t2_bounded_exclusion_and_finite_hole_share_the_selected_strip(
    reverse_winding_hole: bool,
) -> None:
    winding_hole = [(0, 30), (90, 30), (180, 30), (-90, 30), (0, 30)]
    if reverse_winding_hole:
        winding_hole.reverse()
    shell = gm.Polygon(_T2_NOTCHED_SHELL, crs=4326)
    polar_exclusion = gm.Polygon(winding_hole, crs=4326)
    finite_exclusion = gm.Polygon(
        [(20, 10), (30, 10), (30, 15), (20, 15), (20, 10)],
        crs=4326,
    )
    band = gm.box(-180, -30, 180, 30, crs=4326)

    assert shell.is_valid
    assert polar_exclusion.is_valid
    assert finite_exclusion.is_valid
    strip = gm.difference(shell, polar_exclusion)
    assert strip.is_valid
    without_finite_hole = gm.difference(strip, finite_exclusion)
    assert without_finite_hole.is_valid
    result = gm.intersection(without_finite_hole, band)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert _t2_chart_area(result) == 9250.0
    assert gm.contains(result, gm.Point(0, -5, crs=4326))
    assert gm.contains(result, gm.Point(35, 12, crs=4326))
    assert not gm.contains(result, gm.Point(25, 12, crs=4326))
    assert gm.covers(result, gm.Point(20, 12, crs=4326))
    assert not gm.contains(result, gm.Point(0, 60, crs=4326))
    split = result.split_antimeridian()
    assert split.is_valid
    assert not gm.contains(split, gm.Point(25, 12, crs=4326))
    reused = gm.intersection(split, band)
    assert reused.is_valid
    assert not gm.contains(reused, gm.Point(25, 12, crs=4326))
    assert gm.contains(reused, gm.Point(35, 12, crs=4326))


def _t2_north_closed_cap(
    boundary: list[tuple[int, int]],
) -> gm.Polygon:
    return gm.Polygon(
        [
            *boundary,
            (180, 90),
            (90, 90),
            (0, 90),
            (-90, 90),
            (-180, 90),
            boundary[0],
        ],
        crs=4326,
    )


def test_t2_two_ambiguous_selected_cycles_form_one_pole_free_support() -> None:
    lower = _t2_north_closed_cap([
        (-180, -20),
        (-90, 10),
        (0, -20),
        (90, 10),
        (180, -20),
    ])
    upper = _t2_north_closed_cap([
        (-180, -15),
        (-90, 15),
        (0, -15),
        (90, 15),
        (180, -15),
    ])

    assert lower.is_valid
    assert upper.is_valid
    result = gm.difference(lower, upper)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert _t2_chart_area(result) == 1800.0
    for point in ((0, -17), (90, 12), (-90, 12), (180, -17), (-180, -17)):
        assert gm.contains(result, gm.Point(*point, crs=4326)), point
    for point in ((0, 0), (0, -30), (0, 90), (0, -90)):
        assert not gm.contains(result, gm.Point(*point, crs=4326)), point
    split = result.split_antimeridian()
    assert split.is_valid
    assert gm.contains(split, gm.Point(0, -17, crs=4326))
    assert not gm.contains(split, gm.Point(0, 0, crs=4326))
    reused = gm.intersection(split, gm.box(-180, -30, 180, 30, crs=4326))
    assert reused.is_valid
    assert gm.contains(reused, gm.Point(0, -17, crs=4326))
    assert not gm.contains(reused, gm.Point(0, 0, crs=4326))


@pytest.mark.parametrize(
    ('pole_latitude', 'inside_latitude', 'outside_latitude'),
    [(-5, -85, 85), (5, 85, -85)],
    ids=['south', 'north'],
)
@pytest.mark.parametrize('reverse_boundary', [False, True])
def test_t2_generated_equator_crossing_cap_closes_only_its_selected_pole(
    pole_latitude: int,
    inside_latitude: int,
    outside_latitude: int,
    reverse_boundary: bool,
) -> None:
    boundary = [
        (-180, pole_latitude),
        (-90, pole_latitude),
        (0, pole_latitude),
        (90, pole_latitude),
        (180, pole_latitude),
        (-180, pole_latitude),
    ]
    if reverse_boundary:
        boundary.reverse()
    hole_latitude = 50 if pole_latitude > 0 else -50
    finite_hole = [
        (20, hole_latitude - 5),
        (30, hole_latitude - 5),
        (30, hole_latitude + 5),
        (20, hole_latitude + 5),
        (20, hole_latitude - 5),
    ]
    cap = gm.Polygon(boundary, holes=[finite_hole], crs=4326)
    connector = gm.box(-10, -20, 10, 10, crs=4326)

    assert cap.is_valid
    result = gm.union(cap, connector)
    split = result.split_antimeridian()
    reimported = gm.from_wkb(result.to_wkb(include_srid=True))
    reused = gm.union(split, connector)

    for geometry in (result, split, reimported, reused):
        assert geometry.geometry_type in ('Polygon', 'MultiPolygon')
        assert geometry.is_valid
        assert geometry.validate().valid
        assert gm.contains(geometry, gm.Point(0, inside_latitude, crs=4326))
        assert gm.contains(geometry, gm.Point(0, 0, crs=4326))
        assert not gm.contains(geometry, gm.Point(50, 0, crs=4326))
        assert not gm.contains(geometry, gm.Point(0, outside_latitude, crs=4326))
        assert not gm.contains(geometry, gm.Point(25, hole_latitude, crs=4326))
        assert gm.contains(geometry, gm.Point(35, hole_latitude, crs=4326))
        assert gm.covers(
            geometry,
            gm.Point(-180, -10 if pole_latitude < 0 else 10, crs=4326),
        )
        assert gm.covers(
            geometry,
            gm.Point(180, -10 if pole_latitude < 0 else 10, crs=4326),
        )
        assert geometry.area == pytest.approx(result.area)
        assert geometry.length == pytest.approx(result.length)
    assert split.split_antimeridian().to_wkt() == split.to_wkt()


def _t2_produced_strip() -> gm.Geometry:
    source = gm.Polygon(_T2_NOTCHED_SHELL, crs=4326)
    band = gm.box(-180, -30, 180, 30, crs=4326)
    return gm.intersection(source, band)


def test_t2_reimported_strip_preserves_support_and_metrics() -> None:
    strip = _t2_produced_strip()
    reimported = gm.from_wkb(strip.to_wkb(include_srid=True))
    split = strip.split_antimeridian()

    for geometry in (strip, reimported, split):
        assert geometry.is_valid
        assert geometry.validate().valid
        assert gm.contains(geometry, gm.Point(0, -5, crs=4326))
        assert gm.contains(geometry, gm.Point(0, 20, crs=4326))
        assert not gm.contains(geometry, gm.Point(50, 0, crs=4326))
        assert not gm.contains(geometry, gm.Point(0, 90, crs=4326))
        assert not gm.contains(geometry, gm.Point(0, -90, crs=4326))
        assert gm.covers(geometry, gm.Point(180, 20, crs=4326))
        assert gm.covers(geometry, gm.Point(-180, 20, crs=4326))
        assert geometry.area == pytest.approx(strip.area)
        assert geometry.length == pytest.approx(strip.length)
        assert gm.distance(geometry, strip) == 0.0
        assert gm.hausdorff_distance(geometry.boundary(), strip.boundary()) == 0.0


def test_t2_reused_strip_enters_exact_spatial_index_refinement() -> None:
    strip = _t2_produced_strip()
    reimported = gm.from_wkb(strip.to_wkb(include_srid=True))
    split = strip.split_antimeridian()
    near = gm.Point(50, 0, crs=4326)

    assert 0.0 < gm.distance(strip, near) < 700_000.0
    for geometry in (strip, reimported, split):
        index = gm.SpatialIndex([geometry])
        witness = gm.Point(0, 20, crs=4326)
        assert ids(index.query(witness, predicate='intersects')) == [0]
        assert ids(index.nearest(near)) == [0]
        assert ids(index.query(near, predicate='dwithin', distance=700_000.0)) == [0]
        assert ids(index.query(near, predicate='dwithin', distance=100_000.0)) == []


def test_t2_split_strip_remains_clippable_and_overlayable() -> None:
    strip = _t2_produced_strip()
    split = strip.split_antimeridian()
    band = gm.box(-180, -30, 180, 30, crs=4326)

    assert gm.equals(split.clip_by_rect(-180, -30, 180, 30), strip)

    second_intersection = gm.intersection(split, band)
    assert second_intersection.is_valid
    assert gm.contains(second_intersection, gm.Point(0, -5, crs=4326))
    assert not gm.contains(second_intersection, gm.Point(0, 90, crs=4326))
    inside = gm.box(-1, 18, 1, 22, crs=4326)
    outside = gm.box(40, -2, 60, 2, crs=4326)
    assert gm.contains(gm.intersection(split, inside), gm.Point(0, 20, crs=4326))
    assert not gm.contains(gm.intersection(split, outside), gm.Point(50, 0, crs=4326))
    assert gm.contains(gm.union(split, outside), gm.Point(50, 0, crs=4326))
    assert not gm.contains(gm.difference(split, inside), gm.Point(0, 20, crs=4326))


def test_union_merges_seam_adjacent_source_rings_into_one_shell() -> None:
    east = gm.Polygon([(170, 0), (180, 0), (180, 10), (170, 10), (170, 0)], crs=4326)
    west = gm.Polygon(
        [(-180, 0), (-170, 0), (-170, 10), (-180, 10), (-180, 0)], crs=4326
    )

    result = gm.union_all([east, west])

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(175, 5, crs=4326))
    assert gm.contains(result, gm.Point(-175, 5, crs=4326))
    assert not gm.contains(result, gm.Point(0, 5, crs=4326))
    assert not gm.equals(result, east)
    assert not gm.equals(result, west)


def test_union_keeps_disconnected_seam_boxes_as_multipolygon() -> None:
    east = gm.box(170, 0, 175, 5, crs=4326)
    west = gm.box(-175, 0, -170, 5, crs=4326)

    result = gm.union_all([east, west])

    assert result.geometry_type == 'MultiPolygon'
    assert len(result.parts) == 2
    assert gm.contains(result, gm.Point(172.5, 2.5, crs=4326))
    assert gm.contains(result, gm.Point(-172.5, 2.5, crs=4326))
    assert not gm.contains(result, gm.Point(180, 2.5, crs=4326))


def test_geographic_array_candidates_keep_periodic_envelope_selectivity() -> None:
    source = seam_box()
    distant = gm.box(-10, -1, 10, 1, crs=4326)
    index = gm.SpatialIndex([source, distant])
    queries = gm.GeometryArray([source])

    assert [row.tolist() for row in index.candidates(queries)] == [[0]]


def test_quotient_topology_ignores_z_and_m_ordinates() -> None:
    xy = seam_box()
    xyzm = gm.from_wkt(
        'POLYGON ZM ((170 -10 1 2, -170 -10 3 4, -170 10 5 6, '
        '170 10 7 8, 170 -10 1 2))',
        crs=4326,
    )
    point = gm.Point(180, 0, crs=4326)
    assert gm.relate(xy, point) == gm.relate(xyzm, point)


def test_epsg_4807_pole_index_candidate_reaches_exact_refinement() -> None:
    pole = math.nextafter(100.0, math.inf)
    north = gm.Point(0, pole, crs=4807)
    alternate_spelling = gm.Point(200, pole, crs=4807)
    index = gm.SpatialIndex([north])
    assert ids(index.candidates(alternate_spelling)) == [0]
    assert ids(index.query(alternate_spelling, predicate='intersects')) == [0]

    outside_pole = math.nextafter(math.nextafter(100.0, math.inf), math.inf)
    with pytest.raises(gm.InvalidGeometryError, match='latitude'):
        gm.SpatialIndex([gm.Point(0, outside_pole, crs=4807)])


def test_full_turn_self_relation_keeps_the_closed_area_boundary() -> None:
    polygon = _north_polar_cap()

    assert gm.equals(polygon, polygon)
    assert gm.relate(polygon, polygon) == '2FFF1FFF2'
    result = gm.intersection(polygon, polygon)
    assert result.geometry_type == 'Polygon'
    assert gm.equals(result, polygon)
    assert result.area > 0.0
    assert result.area == polygon.area


@pytest.mark.parametrize(
    'representation',
    ['split', 'geojson'],
    ids=['antimeridian-split', 'geojson-round-trip'],
)
def test_full_turn_polar_cap_contains_pole_after_representation(
    representation: str,
) -> None:
    cap = _north_polar_cap()
    geometry = (
        cap.split_antimeridian()
        if representation == 'split'
        else gm.from_geojson(cap.to_geojson())
    )

    assert gm.contains(geometry, gm.Point(0, 90, crs=geometry.crs))


def test_seam_only_ring_has_line_support_not_global_area() -> None:
    polygon = gm.Polygon(
        [(-180, -85), (180, -85), (180, 85), (-180, 85), (-180, -85)],
        crs=4326,
    )
    seam_line = gm.LineString([(-180, -85), (-180, 85)], crs=4326)
    center = gm.Point(0, 0, crs=4326)
    near_seam = gm.Point(179, 0, crs=4326)

    assert polygon.area == 0.0
    assert not gm.contains(polygon, center)
    assert not gm.contains(polygon, near_seam)
    assert not gm.covers(polygon, center)
    assert not gm.covers(polygon, near_seam)
    assert gm.covers(polygon, seam_line)
    assert gm.intersects(polygon, seam_line)
    assert gm.covers(polygon, gm.Point(180, 0, crs=4326))
