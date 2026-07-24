"""Derive point and line layers from the OSM countries fixture at runtime."""

from __future__ import annotations

from pathlib import Path

import gometry as gm

FIXTURE = Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'


def load_country_parts() -> gm.GeometryArray:
    return gm.from_geojson(FIXTURE.read_text(encoding='utf-8'))


def _polygon_rings(geometry: gm.Geometry) -> list[gm.Geometry]:
    if geometry.geometry_type == 'Polygon':
        return [geometry.exterior, *geometry.interiors]
    if geometry.geometry_type == 'MultiPolygon':
        rings: list[gm.Geometry] = []
        for part in gm.parts(geometry):
            rings.append(part.exterior)
            rings.extend(part.interiors)
        return rings
    return []


def country_boundary_vertices(
    parts: gm.GeometryArray | None = None,
) -> gm.GeometryArray:
    """All boundary ring vertices as a points layer."""
    if parts is None:
        parts = load_country_parts()
    xs: list[float] = []
    ys: list[float] = []
    for geometry in parts:
        for ring in _polygon_rings(geometry):
            xs.extend(ring.coords.x)
            ys.extend(ring.coords.y)
    return gm.points(xs, ys, crs=4326)


def country_boundary_lines(
    parts: gm.GeometryArray | None = None,
) -> gm.GeometryArray:
    """Each country exterior ring as a LineString."""
    if parts is None:
        parts = load_country_parts()
    lines: list[gm.Geometry] = []
    for geometry in parts:
        if geometry.geometry_type == 'Polygon':
            lines.append(geometry.exterior)
        elif geometry.geometry_type == 'MultiPolygon':
            lines.extend(part.exterior for part in gm.parts(geometry))
    return gm.GeometryArray(lines, crs=4326)
