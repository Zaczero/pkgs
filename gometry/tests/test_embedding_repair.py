"""Public regressions for certified overlay coordinate embeddings."""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
from copy import deepcopy
from fractions import Fraction
from pathlib import Path
from typing import Any

import gometry as gm
import pytest

shapely = pytest.importorskip(
    'shapely', reason='shapely is an optional oracle dependency'
)

_COUNTRIES = (
    Path(__file__).resolve().parents[1] / 'fixtures' / 'osm_countries_0_1.geojson'
)
_COUNTRY_CODES = ('AR', 'UY')


def _country_features() -> dict[str, dict[str, Any]]:
    source = json.loads(_COUNTRIES.read_text(encoding='utf-8'))
    wanted = set(_COUNTRY_CODES)
    return {
        feature['properties']['tags']['ISO3166-1:alpha2']: feature
        for feature in source['features']
        if feature['properties']['tags'].get('ISO3166-1:alpha2') in wanted
    }


def _countries(crs: int | None) -> tuple[gm.Geometry, gm.Geometry]:
    features = _country_features()
    argentina = gm.from_geojson(features['AR']['geometry'], crs=crs)
    uruguay = gm.from_geojson(features['UY']['geometry'], crs=crs)
    return argentina, uruguay


def _permute_country_rings(
    feature: dict[str, Any], *, offset: int, reverse: bool
) -> dict[str, Any]:
    geometry = deepcopy(feature['geometry'])
    if geometry['type'] == 'Polygon':
        polygons = [geometry['coordinates']]
    else:
        polygons = geometry['coordinates']
    for polygon in polygons:
        for ring_index, ring in enumerate(polygon):
            open_ring = ring[:-1]
            shift = (offset + ring_index) % len(open_ring)
            rotated = open_ring[shift:] + open_ring[:shift]
            if reverse:
                rotated.reverse()
            polygon[ring_index] = [*rotated, rotated[0]]
    return geometry


def test_ar_uy_overlay_results_are_valid_in_planar_and_geographic_domains() -> None:
    for crs in (None, 4326):
        argentina, uruguay = _countries(crs)
        operations = (
            gm.union,
            gm.intersection,
            gm.difference,
            gm.symmetric_difference,
        )
        for operation in operations:
            for left, right in ((argentina, uruguay), (uruguay, argentina)):
                result = operation(left, right)
                assert not result.is_empty, (crs, operation.__name__)
                assert result.is_valid, (crs, operation.__name__)
                assert shapely.from_wkb(result.to_wkb()).is_valid, (
                    crs,
                    operation.__name__,
                )


def test_ar_uy_symmetric_difference_wkb_is_invariant_to_operand_and_ring_order() -> (
    None
):
    features = _country_features()
    for crs in (None, 4326):
        argentina, uruguay = _countries(crs)
        expected = gm.symmetric_difference(argentina, uruguay).to_wkb()
        permuted_argentina = gm.from_geojson(
            _permute_country_rings(features['AR'], offset=1, reverse=True),
            crs=crs,
        )
        permuted_uruguay = gm.from_geojson(
            _permute_country_rings(features['UY'], offset=2, reverse=True),
            crs=crs,
        )
        for left, right in (
            (argentina, uruguay),
            (uruguay, argentina),
            (permuted_argentina, permuted_uruguay),
            (permuted_uruguay, permuted_argentina),
        ):
            result = gm.symmetric_difference(left, right)
            assert result.to_wkb() == expected


def _ar_uy_symmetric_difference_digest() -> str:
    argentina, uruguay = _countries(None)
    return hashlib.sha256(
        gm.symmetric_difference(argentina, uruguay).to_wkb()
    ).hexdigest()


def test_ar_uy_symmetric_difference_wkb_is_stable_across_hash_seed_processes() -> None:
    module_path = str(Path(__file__).resolve())
    code = (
        'import runpy; '
        f'module = runpy.run_path({module_path!r}); '
        "print(module['_ar_uy_symmetric_difference_digest']())"
    )
    digests = []
    for seed in ('11', '29'):
        environment = os.environ.copy()
        environment['PYTHONHASHSEED'] = seed
        process = subprocess.run(
            [sys.executable, '-c', code],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
            env=environment,
        )
        digests.append(process.stdout.strip())
    assert digests[0] == digests[1]


def test_moved_crossing_node_interpolates_exact_zm_ordinates() -> None:
    ulp = 2.0**-52

    def lattice(i: int, j: int) -> tuple[float, float]:
        return 1.0 + i * ulp, 1.0 + j * ulp

    sources = gm.GeometryCollection([
        gm.LineString(
            [lattice(-1, -1), lattice(1, 1)],
            z=[-12, 12],
            m=[1, -1],
        ),
        gm.LineString(
            [lattice(0, 1), lattice(1, -1)],
            z=[7, -2],
            m=[2, -5],
        ),
        gm.LineString(
            [lattice(0, 0), lattice(2, 1)],
            z=[0, 17],
            m=[0, -4],
        ),
    ])
    noded = sources.node()

    assert noded.coordinate_axes == 'XYZM'
    source_vertices = {
        lattice(-1, -1),
        lattice(1, 1),
        lattice(0, 1),
        lattice(1, -1),
        lattice(0, 0),
        lattice(2, 1),
    }
    neighboring_floats = {
        math.nextafter(1.0, -math.inf),
        1.0,
        math.nextafter(1.0, math.inf),
    }
    moved = {
        (coordinate[2], coordinate[3])
        for line in noded.parts
        for coordinate in line.coords
        if (coordinate[0], coordinate[1])
        in {(x, y) for x in neighboring_floats for y in neighboring_floats}
        and (coordinate[0], coordinate[1]) not in source_vertices
        and (coordinate[0], coordinate[1]) != (1.0, 1.0)
    }
    assert moved == {
        (4.0, float(Fraction(-1, 3))),
        (float(Fraction(17, 5)), float(Fraction(-4, 5))),
    }


def test_moved_xym_crossings_preserve_exact_m_ordinates() -> None:
    ulp = 2.0**-52

    def lattice(i: int, j: int) -> tuple[float, float]:
        return 1.0 + i * ulp, 1.0 + j * ulp

    sources = gm.GeometryCollection([
        gm.LineString([lattice(-1, -1), lattice(1, 1)], m=[1, -1]),
        gm.LineString([lattice(0, 1), lattice(1, -1)], m=[2, -5]),
        gm.LineString([lattice(0, 0), lattice(2, 1)], m=[0, -4]),
    ])
    noded = sources.node()

    assert noded.coordinate_axes == 'XYM'
    source_vertices = {
        lattice(-1, -1),
        lattice(1, 1),
        lattice(0, 1),
        lattice(1, -1),
        lattice(0, 0),
        lattice(2, 1),
    }
    neighboring_floats = {
        math.nextafter(1.0, -math.inf),
        1.0,
        math.nextafter(1.0, math.inf),
    }
    moved = {
        coordinate[2]
        for line in noded.parts
        for coordinate in line.coords
        if (coordinate[0], coordinate[1])
        in {(x, y) for x in neighboring_floats for y in neighboring_floats}
        and (coordinate[0], coordinate[1]) not in source_vertices
        and (coordinate[0], coordinate[1]) != (1.0, 1.0)
    }
    assert moved == {
        float(Fraction(-1, 3)),
        float(Fraction(-4, 5)),
    }


def test_difference_keeps_a_nearby_point_separate_and_pins_exact_axes() -> None:
    smallest = math.ulp(0.0)
    line = gm.LineString([(-1.0, 0.0), (7.0, smallest)])
    point = gm.Point(1.0, 0.0)
    assert not gm.intersects(line, point)
    source = gm.GeometryCollection([line, point])
    outside = gm.MultiPolygon([
        gm.box(-2.0, -1.0, 0.0, 1.0),
        gm.box(2.0, -1.0, 8.0, 1.0),
    ])

    result = gm.difference(source, outside)

    assert result.geometry_type == 'GeometryCollection'
    parts = list(result.parts)
    points = [part for part in parts if part.geometry_type == 'Point']
    lines = [part for part in parts if part.geometry_type == 'LineString']
    assert len(points) == 1
    assert gm.equals(points[0], point)
    assert len(lines) == 1
    coordinates = list(lines[0].coords)
    assert len(coordinates) == 2
    assert {coordinate[0] for coordinate in coordinates} == {0.0, 2.0}
    assert not gm.intersects(lines[0], point)
    assert lines[0].length > 1.9
