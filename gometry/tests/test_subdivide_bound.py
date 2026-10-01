"""Subdivision keeps every emitted part within its public coordinate bound."""

from __future__ import annotations

import json
import math
import random
from fractions import Fraction
from itertools import pairwise
from pathlib import Path

import gometry as gm
import pytest
import shapely

_COUNTRIES = (
    Path(__file__).resolve().parents[1] / 'fixtures' / 'osm_countries_0_1.geojson'
)


def _polygon_members(geometry: gm.Geometry) -> list[gm.Geometry]:
    return [geometry] if geometry.geometry_type == 'Polygon' else list(geometry.parts)


def _geometry_rings(geometry: gm.Geometry) -> list[list[tuple[float, ...]]]:
    return [
        list(ring.coords)
        for polygon in _polygon_members(geometry)
        for ring in [polygon.exterior, *polygon.interiors]
    ]


def _shapely_polygon_hole_counts(geometry) -> tuple[int, int]:
    polygons = [geometry] if geometry.geom_type == 'Polygon' else list(geometry.geoms)
    return len(polygons), sum(len(polygon.interiors) for polygon in polygons)


def _exact_ring_area(ring) -> Fraction:
    twice_area = sum(
        (
            Fraction(float(start[0])) * Fraction(float(end[1]))
            - Fraction(float(end[0])) * Fraction(float(start[1]))
        )
        for start, end in pairwise(ring.coords)
    )
    return abs(twice_area) / 2


def _exact_planar_area(geometry: gm.Geometry) -> Fraction:
    # Keep floating area-evaluation error out of the geometric rounding bound.
    area = Fraction()
    for polygon in _polygon_members(geometry):
        rings = [polygon.exterior, *polygon.interiors]
        area += _exact_ring_area(rings[0])
        area -= sum((_exact_ring_area(ring) for ring in rings[1:]), Fraction())
    return area


def _rounding_area_bound(
    source: gm.Geometry,
    parts: gm.GeometryArray,
    union: shapely.Geometry,
    label: str,
) -> float:
    source_rings = _geometry_rings(source)
    source_vertices = {
        (float(coordinate[0]), float(coordinate[1]))
        for ring in source_rings
        for coordinate in ring
    }
    cut_x = {float(part.bounds[index]) for part in parts for index in (0, 2)}
    cut_y = {float(part.bounds[index]) for part in parts for index in (1, 3)}
    union_geometry = shapely.from_wkb(union.wkb)
    union_rings = [
        list(ring.coords)
        for polygon in (
            [union_geometry]
            if union_geometry.geom_type == 'Polygon'
            else list(union_geometry.geoms)
        )
        for ring in [polygon.exterior, *polygon.interiors]
    ]
    for ring in union_rings:
        for coordinate in ring[:-1]:
            vertex = (float(coordinate[0]), float(coordinate[1]))
            assert (
                vertex in source_vertices or vertex[0] in cut_x or vertex[1] in cut_y
            ), f'{label}: constructed union vertex {vertex} is off every cut line'

    area_bound = 0.0
    for part in parts:
        for ring in _geometry_rings(part):
            vertices = [(float(point[0]), float(point[1])) for point in ring[:-1]]
            for index, (x, y) in enumerate(vertices):
                if (x, y) in source_vertices:
                    continue
                assert x in cut_x or y in cut_y, (
                    f'{label}: constructed part vertex {(x, y)} is off every cut line'
                )
                previous = vertices[(index - 1) % len(vertices)]
                following = vertices[(index + 1) % len(vertices)]
                x_ulp, y_ulp = math.ulp(x), math.ulp(y)
                incident_edge_lengths = math.hypot(
                    x - previous[0], y - previous[1]
                ) + math.hypot(following[0] - x, following[1] - y)
                area_bound += (
                    math.sqrt(2.0) * 0.5 * max(x_ulp, y_ulp) * incident_edge_lengths
                )
    return area_bound


def _assert_subdivision_contract(
    geometry: gm.Geometry,
    max_vertices: int,
    label: str,
) -> None:
    parts = geometry.subdivide(max_vertices=max_vertices)

    assert len(parts) > 0, label
    assert all(part.num_coordinates <= max_vertices for part in parts), label
    assert all(part.is_valid for part in parts), label

    spatial_index = gm.SpatialIndex(parts)
    for left_index, left in enumerate(parts):
        for right_index in spatial_index.query(left):
            if right_index <= left_index:
                continue
            assert gm.relate_pattern(left, parts[right_index], 'F********'), (
                f'{label}: parts {left_index} and {right_index} overlap in their interiors'
            )

    source = shapely.from_wkb(geometry.to_wkb())
    # Keep the set oracle independent of gometry's exact overlay implementation.
    union = shapely.union_all([shapely.from_wkb(part.to_wkb()) for part in parts])
    assert _shapely_polygon_hole_counts(source) == _shapely_polygon_hole_counts(
        union
    ), label

    area_bound = _rounding_area_bound(geometry, parts, union, label)
    assert union.symmetric_difference(source).area <= area_bound, (
        f'{label}: symmetric-difference area exceeds the constructed-node rounding bound'
    )
    exact_part_area = sum((_exact_planar_area(part) for part in parts), Fraction())
    exact_source_area = _exact_planar_area(geometry)
    assert abs(exact_part_area - exact_source_area) <= area_bound, (
        f'{label}: exact planar area sum exceeds the constructed-node rounding bound'
    )


@pytest.fixture(scope='module')
def countries() -> list[tuple[str, gm.Geometry]]:
    features = json.loads(_COUNTRIES.read_text(encoding='utf-8'))['features']
    collection = json.dumps({'type': 'FeatureCollection', 'features': features})
    geometries = gm.from_geojson(collection, crs=4326)
    return [
        (
            feature['properties']['tags'].get('ISO3166-1:alpha2', f'feature-{index}'),
            geometry,
        )
        for index, (feature, geometry) in enumerate(
            zip(features, geometries, strict=True)
        )
    ]


@pytest.mark.parametrize('max_vertices', [8, 16, 64])
def test_country_subdivisions_obey_the_bound_and_preserve_the_partition(
    countries: list[tuple[str, gm.Geometry]],
    max_vertices: int,
) -> None:
    for code, geometry in countries:
        _assert_subdivision_contract(
            geometry, max_vertices, f'{code}, max_vertices={max_vertices}'
        )


def _star_polygon_with_hole(rng: random.Random, vertex_count: int) -> gm.Geometry:
    phase = rng.random() * math.tau
    shell = []
    for index in range(vertex_count):
        angle = phase + math.tau * index / vertex_count
        radius = 10.0 * (0.78 + 0.22 * rng.random())
        shell.append((radius * math.cos(angle), radius * math.sin(angle)))
    shell.append(shell[0])

    hole_count = 7
    hole_phase = rng.random() * math.tau
    hole = [
        (
            0.25 + 0.8 * math.cos(hole_phase + math.tau * index / hole_count),
            -0.15 + 0.8 * math.sin(hole_phase + math.tau * index / hole_count),
        )
        for index in range(hole_count)
    ]
    hole.append(hole[0])
    return gm.Polygon(shell, holes=[hole])


@pytest.mark.parametrize('max_vertices', [8, 16, 64])
def test_random_star_polygons_with_holes_obey_the_bound(
    max_vertices: int,
) -> None:
    rng = random.Random(255)
    for vertex_count in (31, 47, 63):
        geometry = _star_polygon_with_hole(rng, vertex_count)
        assert geometry.is_valid
        _assert_subdivision_contract(
            geometry,
            max_vertices,
            f'star-{vertex_count}, max_vertices={max_vertices}',
        )


def test_subdivision_revisits_over_budget_clipped_multipart_children() -> None:
    geometry = gm.from_wkt('POLYGON((0 0,20 0,20 20,0 20,0 0),(0 0,5 2,2 5,0 0))')
    _assert_subdivision_contract(geometry, 8, 'shell-touching hole')


def test_subdivision_separates_multipolygon_members_that_fit() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((10 10,20 10,20 20,10 20,10 10)))'
    )
    parts = geometry.subdivide(max_vertices=8)

    assert [part.geometry_type for part in parts] == ['Polygon', 'Polygon']
    _assert_subdivision_contract(geometry, 8, 'touching MultiPolygon')
