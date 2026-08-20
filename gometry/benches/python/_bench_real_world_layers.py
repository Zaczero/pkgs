"""Derive point and line layers from the OSM countries fixture at runtime."""

from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import gometry as gm

FIXTURE = Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'

# Frozen Brazil bounds from the fixture (spec §2)
BRAZIL_BOUNDS: tuple[float, float, float, float] = (
    -73.9830625,
    -33.8694284,
    -28.6364123,
    5.2458691,
)

_CACHE: dict[str, Any] = {}


def _cached(key: str, factory: Any) -> Any:
    if key not in _CACHE:
        _CACHE[key] = factory()
    return _CACHE[key]


def clear_real_world_cache() -> None:
    _CACHE.clear()
    load_country_geojson_text.cache_clear()
    load_country_features.cache_clear()
    load_country_parts.cache_clear()


@lru_cache(maxsize=1)
def load_country_geojson_text() -> str:
    return FIXTURE.read_text(encoding='utf-8')


@lru_cache(maxsize=1)
def load_country_features() -> list[dict[str, Any]]:
    data = json.loads(load_country_geojson_text())
    return list(data['features'])


@lru_cache(maxsize=1)
def load_country_parts() -> gm.GeometryArray:
    # Explicit EPSG:4326: fixtures and competitors tag WGS84 as 4326; the
    # public from_geojson default is OGC:CRS84 (RFC 7946 / GeoParquet).
    return gm.from_geojson(load_country_geojson_text(), crs=4326)


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


def country_exteriors_multilinestring(
    parts: gm.GeometryArray | None = None,
) -> gm.MultiLineString:
    """All exteriors as one MultiLineString (geodesic length metric)."""
    lines = country_boundary_lines(parts)
    # Build MultiLineString from coordinate sequences
    chains = [list(zip(line.coords.x, line.coords.y, strict=True)) for line in lines]
    return gm.MultiLineString(chains, crs=4326)


def country_collection(
    parts: gm.GeometryArray | None = None,
) -> gm.GeometryCollection:
    if parts is None:
        parts = load_country_parts()
    return gm.GeometryCollection(list(parts), crs=4326)


def brazil_geometry() -> gm.Geometry:
    """Brazil selected by ISO3166-1:alpha2 == 'BR' (never by row index)."""

    def build():
        features = load_country_features()
        parts = load_country_parts()
        for i, feat in enumerate(features):
            props = feat.get('properties') or {}
            tags = props.get('tags') or {}
            if isinstance(tags, dict) and tags.get('ISO3166-1:alpha2') == 'BR':
                geom = parts[i]
                # pin frozen bounds
                b = geom.bounds
                assert b is not None
                expected = BRAZIL_BOUNDS
                if any(abs(a - e) > 1e-7 for a, e in zip(b, expected, strict=True)):
                    raise RuntimeError(
                        f'Brazil bounds drifted: got {b}, expected {expected}'
                    )
                return geom
        raise RuntimeError('Brazil (ISO3166-1:alpha2=BR) not found in country fixture')

    return _cached('brazil_geometry', build)


def brazil_bbox_polygon() -> gm.Geometry:
    """Exact Brazil bounds as a box (tile_cover.bbox fixture)."""

    def build():
        w, s, e, n = BRAZIL_BOUNDS
        return gm.box(w, s, e, n, crs=4326)

    return _cached('brazil_bbox', build)


def _representative_lonlat(feat: dict[str, Any]) -> tuple[float, float]:
    props = feat.get('properties') or {}
    rep = props.get('representative_point')
    if isinstance(rep, dict) and rep.get('type') == 'Point':
        coords = rep['coordinates']
        return float(coords[0]), float(coords[1])
    raise RuntimeError('feature missing representative_point')


def country_pois_10k() -> gm.GeometryArray:
    """46 points/country + 1 extra for first 18; sunflower disk ≤40% clearance."""

    def build():
        features = load_country_features()
        parts = load_country_parts()
        assert len(features) == 217
        # 46 per country = 46*217 = 9982; one extra to the first 18 → 10000.
        assert 46 * 217 + 18 == 10_000

        xs: list[float] = []
        ys: list[float] = []
        golden = math.pi * (3.0 - math.sqrt(5.0))

        for i, (feat, geom) in enumerate(zip(features, parts, strict=True)):
            n_pts = 46 + (1 if i < 18 else 0)
            lon0, lat0 = _representative_lonlat(feat)
            rep = gm.Point(lon0, lat0, crs=4326)
            # boundary clearance outside timing (this is fixture build)
            clearance = float(gm.distance(rep, geom.boundary(), unit='meters'))
            # disk radius in degrees ≈ metres / (111320 * cos(lat)) for lon,
            # and metres / 110540 for lat — use isotropic local metres→deg
            # Place in a planar lon/lat disk with radius corresponding to 40% of
            # clearance (metres) converted at the representative latitude.
            max_m = 0.4 * max(clearance, 1.0)
            m_per_deg_lat = 110_540.0
            m_per_deg_lon = 111_320.0 * max(0.2, math.cos(math.radians(lat0)))
            # sunflower in unit disk then scale
            for k in range(n_pts):
                # radius fraction: sqrt((k+0.5)/n)
                r_frac = math.sqrt((k + 0.5) / n_pts)
                theta = k * golden
                east_m = max_m * r_frac * math.cos(theta)
                north_m = max_m * r_frac * math.sin(theta)
                lon = lon0 + east_m / m_per_deg_lon
                lat = lat0 + north_m / m_per_deg_lat
                # clamp lat
                lat = max(-90.0, min(90.0, lat))
                if lon > 180.0:
                    lon -= 360.0
                elif lon < -180.0:
                    lon += 360.0
                xs.append(lon)
                ys.append(lat)
        assert len(xs) == 10_000
        return gm.points(xs, ys, crs=4326)

    return _cached('country_pois_10k', build)


def country_pois_10k_shapely():
    import shapely

    def build():
        pois = country_pois_10k()
        return shapely.points(np.asarray(pois.coords.x), np.asarray(pois.coords.y))

    return _cached('country_pois_10k_sh', build)


def country_parts_shapely():
    import shapely

    def build():
        text = load_country_geojson_text()
        coll = shapely.from_geojson(text)
        return shapely.get_parts(coll)

    return _cached('country_parts_sh', build)


def country_collection_shapely():
    import shapely

    def build():
        parts = country_parts_shapely()
        return shapely.GeometryCollection(list(parts))

    return _cached('country_collection_sh', build)


def country_exteriors_shapely():
    import shapely

    def build():
        parts = country_parts_shapely()
        lines = []
        for g in parts:
            if g.geom_type == 'Polygon':
                lines.append(shapely.LineString(g.exterior.coords))
            elif g.geom_type == 'MultiPolygon':
                lines.extend(shapely.LineString(p.exterior.coords) for p in g.geoms)
        return shapely.MultiLineString(lines)

    return _cached('country_exteriors_sh', build)
