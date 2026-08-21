"""Lazy synthetic fixtures for the public RELEASE benchmark set (spec §2).

Heavy 10k/100k constructions are built on first access only — never at import.
"""

from __future__ import annotations

import math
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    import gometry as gm

_CRS_UTM = 32634
_N10K = 10_000

# ---------------------------------------------------------------------------
# Lazy cache (module-level; builders drop references between oracle ops)
# ---------------------------------------------------------------------------

_CACHE: dict[str, Any] = {}


def _cached(key: str, factory: Any) -> Any:
    if key not in _CACHE:
        _CACHE[key] = factory()
    return _CACHE[key]


def clear_public_fixture_cache() -> None:
    """Drop memoized fixtures (oracle memory bounding between ops)."""
    _CACHE.clear()
    points_xy_numpy.cache_clear()
    # lru_cache helpers below


# ---------------------------------------------------------------------------
# Low-level numeric helpers
# ---------------------------------------------------------------------------


def _halton(index: int, base: int) -> float:
    """1-based Halton sequence in (0, 1)."""
    result = 0.0
    f = 1.0 / base
    i = index
    while i > 0:
        result += f * (i % base)
        i //= base
        f /= base
    return result


def _radial_polygon_xy(
    cx: float,
    cy: float,
    n_verts: int,
    radius: float,
    phase: float,
    *,
    wobble: float = 0.15,
) -> list[tuple[float, float]]:
    """Irregular radial footprint (open ring; caller closes)."""
    pts: list[tuple[float, float]] = []
    for j in range(n_verts):
        t = phase + 2.0 * math.pi * j / n_verts
        r = radius * (
            1.0 + wobble * math.sin(3.0 * t + phase) + 0.08 * math.cos(5.0 * t)
        )
        pts.append((cx + r * math.cos(t), cy + r * math.sin(t)))
    return pts


# ---------------------------------------------------------------------------
# POINTS / roads / buildings / mixed
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def points_xy_numpy() -> tuple[np.ndarray, np.ndarray]:
    """Contiguous float64 X/Y for points/10k (CRS-free construction)."""
    i = np.arange(_N10K, dtype=np.float64)
    x = 500_000.0 + 150.0 * (i % 100) + 7.0 * np.sin(0.17 * i)
    y = 5_200_000.0 + 150.0 * (i // 100) + 7.0 * np.cos(0.19 * i)
    return np.ascontiguousarray(x), np.ascontiguousarray(y)


def points_10k_gometry():
    import gometry as gm

    def build():
        x, y = points_xy_numpy()
        return gm.points(x, y, crs=_CRS_UTM)

    return _cached('points_10k_gm', build)


def points_10k_shapely():
    import shapely

    def build():
        x, y = points_xy_numpy()
        return shapely.points(x, y)

    return _cached('points_10k_sh', build)


def roads_10k_gometry():
    """Exactly 100,000 vertices across 10k LineStrings."""
    import gometry as gm

    def build():
        geoms: list[gm.Geometry] = []
        total = 0
        for i in range(_N10K):
            n = 8 + i % 5
            x0 = 500_000.0 + 80.0 * (i % 100)
            y0 = 5_200_000.0 + 80.0 * (i // 100)
            coords = [
                (
                    x0 + 12.0 * j,
                    y0 + 1.5 * j + 7.0 * math.sin(0.45 * j + 0.071 * i),
                )
                for j in range(n)
            ]
            geoms.append(gm.LineString(coords))
            total += n
        assert total == 100_000, total
        return gm.GeometryArray(geoms, crs=_CRS_UTM)

    return _cached('roads_10k_gm', build)


def roads_10k_shapely():
    import shapely

    def build():
        geoms = []
        total = 0
        for i in range(_N10K):
            n = 8 + i % 5
            x0 = 500_000.0 + 80.0 * (i % 100)
            y0 = 5_200_000.0 + 80.0 * (i // 100)
            coords = [
                (
                    x0 + 12.0 * j,
                    y0 + 1.5 * j + 7.0 * math.sin(0.45 * j + 0.071 * i),
                )
                for j in range(n)
            ]
            geoms.append(shapely.LineString(coords))
            total += n
        assert total == 100_000, total
        return np.asarray(geoms, dtype=object)

    return _cached('roads_10k_sh', build)


def buildings_10k_gometry():
    """100x100 nonoverlapping irregular radial footprints; 20% holed."""
    import gometry as gm

    def build():
        geoms: list[gm.Geometry] = []
        spacing = 50.0
        for i in range(_N10K):
            col = i % 100
            row = i // 100
            cx = 500_000.0 + spacing * col
            cy = 5_200_000.0 + spacing * row
            n_verts = 4 + (i % 9)  # 4..12
            radius = 8.0 + (i % 11)  # 8..18
            phase = 0.17 * i
            exterior = _radial_polygon_xy(cx, cy, n_verts, radius, phase)
            holes: list[list[tuple[float, float]]] | None = None
            if i % 5 == 0:
                # reversed 0.25-scale hole (CW if exterior is CCW)
                hole = _radial_polygon_xy(
                    cx, cy, max(4, n_verts // 2), radius * 0.25, phase + 0.3
                )
                hole.reverse()
                holes = [hole]
            geoms.append(gm.Polygon(exterior, holes))
        arr = gm.GeometryArray(geoms, crs=_CRS_UTM)
        return arr

    return _cached('buildings_10k_gm', build)


def buildings_10k_shapely():
    import shapely

    def build():
        geoms = []
        spacing = 50.0
        for i in range(_N10K):
            col = i % 100
            row = i // 100
            cx = 500_000.0 + spacing * col
            cy = 5_200_000.0 + spacing * row
            n_verts = 4 + (i % 9)
            radius = 8.0 + (i % 11)
            phase = 0.17 * i
            exterior = _radial_polygon_xy(cx, cy, n_verts, radius, phase)
            holes = None
            if i % 5 == 0:
                hole = _radial_polygon_xy(
                    cx, cy, max(4, n_verts // 2), radius * 0.25, phase + 0.3
                )
                hole.reverse()
                holes = [hole]
            geoms.append(shapely.Polygon(exterior, holes))
        return np.asarray(geoms, dtype=object)

    return _cached('buildings_10k_sh', build)


def mixed_10k_gometry():
    """4k Point + 3k LineString + 2k Polygon + 1k two-part MultiPolygon."""
    import gometry as gm

    def build():
        geoms: list[gm.Geometry] = []
        # Deterministic interleave by kind schedule
        counts = {'Point': 0, 'LineString': 0, 'Polygon': 0, 'MultiPolygon': 0}
        targets = {
            'Point': 4000,
            'LineString': 3000,
            'Polygon': 2000,
            'MultiPolygon': 1000,
        }
        # Cycle kinds until filled
        kind_order = ['Point', 'LineString', 'Polygon', 'MultiPolygon']
        i = 0
        while sum(counts.values()) < _N10K:
            kind = kind_order[i % 4]
            if counts[kind] >= targets[kind]:
                i += 1
                # find next available
                if all(counts[k] >= targets[k] for k in kind_order):
                    break
                continue
            k = counts[kind]
            x0 = 500_000.0 + 20.0 * (k % 200)
            y0 = 5_200_000.0 + 20.0 * (k // 200)
            if kind == 'Point':
                geoms.append(gm.Point(x0, y0))
            elif kind == 'LineString':
                geoms.append(
                    gm.LineString([
                        (x0, y0),
                        (x0 + 15.0, y0 + 3.0 * math.sin(0.1 * k)),
                        (x0 + 30.0, y0 + 1.0),
                    ])
                )
            elif kind == 'Polygon':
                geoms.append(
                    gm.Polygon(
                        _radial_polygon_xy(x0, y0, 6 + k % 5, 10.0 + k % 7, 0.05 * k)
                    )
                )
            else:
                p1 = gm.Polygon(_radial_polygon_xy(x0, y0, 5, 8.0, 0.1 * k))
                p2 = gm.Polygon(
                    _radial_polygon_xy(x0 + 25.0, y0 + 25.0, 5, 8.0, 0.1 * k + 1.0)
                )
                geoms.append(gm.MultiPolygon([p1, p2]))
            counts[kind] += 1
            i += 1
        assert len(geoms) == _N10K
        assert counts == targets
        return gm.GeometryArray(geoms, crs=_CRS_UTM)

    return _cached('mixed_10k_gm', build)


def mixed_10k_shapely():
    """Shapely mirror of MIXED_10K with SRID 32634 set (fair EWKB competitor)."""
    import shapely

    def build():
        gm_arr = mixed_10k_gometry()
        # from_wkb of plain WKB drops SRID; re-tag so to_wkb(include_srid=True,
        # flavor='extended') emits real EWKB matching gometry's column.
        geoms = shapely.from_wkb(gm_arr.to_wkb())
        return shapely.set_srid(geoms, _CRS_UTM)

    return _cached('mixed_10k_sh', build)


def mixed_ewkb_10k() -> np.ndarray:
    """One immutable object ndarray of little-endian EWKB with SRID 32634."""

    def build():
        arr = mixed_10k_gometry()
        blobs = arr.to_wkb(include_srid=True)
        out = np.empty(len(blobs), dtype=object)
        for i, b in enumerate(blobs):
            out[i] = bytes(b) if not isinstance(b, bytes) else b
        out.flags.writeable = False
        return out

    return _cached('mixed_ewkb_10k', build)


def arrow_binary_view_mixed_ewkb_10k():
    """10k mixed EWKB values in a GeoArrow WKB ``binary_view`` array (no nulls).

    Storage is ``pa.binary_view()`` with ``geoarrow.wkb`` extension metadata and
    EPSG:32634 — the public BinaryView from_arrow release fixture.
    """

    def build():
        import pyarrow as pa

        from gometry._arrow import GEOARROW_WKB, _extension_type_from_storage

        ewkb = mixed_ewkb_10k()
        storage = pa.array(ewkb.tolist(), type=pa.binary_view())
        return pa.ExtensionArray.from_storage(
            _extension_type_from_storage(
                pa, GEOARROW_WKB, storage.type, 'EPSG:32634', None
            ),
            storage,
        )

    return _cached('arrow_binary_view_mixed_ewkb_10k', build)


def arrow_mixed_100k():
    """100k GeoArrow WKB provider via GeoSeries.to_arrow (10% missing)."""

    def build():
        import geopandas as gpd
        import shapely

        n = 100_000
        # 10k missing, 45k Point, 25k LineString, 15k Polygon, 5k MultiPolygon
        geoms: list[Any] = [None] * n
        # Place non-missing in a deterministic pattern
        # First fill kinds into slots that are not every 10th
        kind_slots = [i for i in range(n) if i % 10 != 0]  # 90k non-missing
        # remaining 10k every-tenth are missing — but we need exactly 10k missing
        # every 10th of 100k = 10k missing. Perfect.
        assert len(kind_slots) == 90_000
        # Assign kinds to kind_slots: 45k / 25k / 15k / 5k
        targets = [
            ('Point', 45_000),
            ('LineString', 25_000),
            ('Polygon', 15_000),
            ('MultiPolygon', 5_000),
        ]
        pos = 0
        for kind, count in targets:
            for k in range(count):
                idx = kind_slots[pos]
                pos += 1
                x0 = 500_000.0 + 0.5 * (k % 1000)
                y0 = 5_200_000.0 + 0.5 * (k // 1000)
                if kind == 'Point':
                    geoms[idx] = shapely.Point(x0, y0)
                elif kind == 'LineString':
                    geoms[idx] = shapely.LineString([
                        (x0, y0),
                        (x0 + 10.0, y0 + 1.0),
                        (x0 + 20.0, y0),
                    ])
                elif kind == 'Polygon':
                    geoms[idx] = shapely.Polygon([
                        (x0, y0),
                        (x0 + 8.0, y0),
                        (x0 + 8.0, y0 + 8.0),
                        (x0, y0 + 8.0),
                    ])
                else:
                    geoms[idx] = shapely.MultiPolygon([
                        shapely.Polygon([
                            (x0, y0),
                            (x0 + 5.0, y0),
                            (x0 + 5.0, y0 + 5.0),
                            (x0, y0 + 5.0),
                        ]),
                        shapely.Polygon([
                            (x0 + 10.0, y0 + 10.0),
                            (x0 + 15.0, y0 + 10.0),
                            (x0 + 15.0, y0 + 15.0),
                            (x0 + 10.0, y0 + 15.0),
                        ]),
                    ])
        assert pos == 90_000
        gs = gpd.GeoSeries(geoms, crs=_CRS_UTM)
        return gs.to_arrow(geometry_encoding='WKB')

    return _cached('arrow_mixed_100k', build)


# ---------------------------------------------------------------------------
# Prepared polygon + probes
# ---------------------------------------------------------------------------


def prepared_polygon_and_probes():
    """Holed 1,316-coordinate polygon + 100k probes over a 12 km square."""

    def build():
        import gometry as gm
        import shapely

        cx, cy = 500_000.0, 5_200_000.0
        # Exterior: 1024 open verts → 1025 with closure
        n_ext = 1024
        ext: list[tuple[float, float]] = []
        for j in range(n_ext):
            t = 2.0 * math.pi * j / n_ext
            # R0 tuned so Halton probes over the 12 km square hit ~53.76%
            r = (
                5_081.0
                + 380.0 * math.sin(3.0 * t)
                + 210.0 * math.cos(5.0 * t)
                + 90.0 * math.sin(7.0 * t + 0.4)
            )
            ext.append((cx + r * math.cos(t), cy + r * math.sin(t)))
        holes: list[list[tuple[float, float]]] = []
        # Three 96-point holes (open) at different centers
        hole_specs = [
            (cx - 1_200.0, cy - 800.0, 650.0, 0.0),
            (cx + 1_400.0, cy + 600.0, 720.0, 0.7),
            (cx + 200.0, cy + 1_500.0, 580.0, 1.3),
        ]
        for hx, hy, hr, phase in hole_specs:
            hole: list[tuple[float, float]] = []
            for j in range(96):
                t = phase + 2.0 * math.pi * j / 96
                r = hr * (1.0 + 0.12 * math.sin(2.0 * t))
                # CW hole (reversed angle)
                hole.append((hx + r * math.cos(-t), hy + r * math.sin(-t)))
            holes.append(hole)
        gm_poly = gm.Polygon(ext, holes, crs=_CRS_UTM)
        # coordinate count including closures
        n_coords = len(gm_poly.exterior.coords.x)
        for h in gm_poly.interiors:
            n_coords += len(h.coords.x)
        # Probe with 100k low-discrepancy points over a 12 km square
        half = 6_000.0
        n_probe = 100_000
        xs = np.empty(n_probe, dtype=np.float64)
        ys = np.empty(n_probe, dtype=np.float64)
        for i in range(n_probe):
            # Halton 2/3 low-discrepancy in unit square
            u = _halton(i + 1, 2)
            v = _halton(i + 1, 3)
            xs[i] = cx - half + 12_000.0 * u
            ys[i] = cy - half + 12_000.0 * v
        sh_poly = shapely.Polygon(ext, holes)
        shapely.prepare(sh_poly)
        gm_prep = gm_poly.prepare()
        return {
            'gm_poly': gm_poly,
            'gm_prep': gm_prep,
            'sh_poly': sh_poly,
            'xs': xs,
            'ys': ys,
            'n_coords': n_coords,
        }

    return _cached('prepared_polygon', build)


def intersects_polygon_and_points():
    """Irregular polygon with deterministic interior, boundary, and exterior points."""

    def build():
        import gometry as gm
        import shapely

        ring = [
            (-4.0, -2.0),
            (-1.0, -4.0),
            (3.0, -3.0),
            (5.0, 0.0),
            (2.0, 4.0),
            (-2.0, 3.0),
            (-5.0, 1.0),
        ]
        categories = [
            ('interior', (0.0, 0.0)),
            ('interior', (1.0, 1.0)),
            ('boundary', ring[0]),
            ('boundary', (1.0, -3.5)),
            ('exterior', (10.0, 10.0)),
            ('exterior', (-10.0, -10.0)),
        ]
        coords = [point for _category, point in categories] * 2_000
        return {
            'gm_polygon': gm.Polygon(ring),
            'gm_points': gm.GeometryArray([gm.Point(x, y) for x, y in coords]),
            'sh_polygon': shapely.Polygon(ring),
            'sh_points': shapely.points(
                np.asarray([x for x, _ in coords]), np.asarray([y for _, y in coords])
            ),
            'labels': [category for category, _ in categories] * 2_000,
        }

    return _cached('intersects_polygon_points', build)


def geohash_encode_inputs():
    """Deterministic bulk inputs plus edge probes for precisions 1, 6, and 12."""

    def build():
        n = _N10K
        lon = np.asarray([-179.999 + 359.998 * _halton(i + 1, 2) for i in range(n)])
        lat = np.asarray([-89.999 + 179.998 * _halton(i + 1, 3) for i in range(n)])
        return {
            'lon': lon,
            'lat': lat,
            'edge_lon': np.asarray([-180.0, 180.0, 0.0, 179.999999, -179.999999]),
            'edge_lat': np.asarray([-90.0, 90.0, 0.0, 89.999999, -89.999999]),
        }

    return _cached('geohash_encode_10k', build)


# ---------------------------------------------------------------------------
# DWITHIN pairs — exact 50% matches at 100 m
# ---------------------------------------------------------------------------


def dwithin_pairs_10k():
    """Projected pairs: even rows 25-75 m offset, odd 125-200 m; threshold 100 m."""

    def build():
        import gometry as gm
        import shapely

        n = _N10K
        ax = np.empty(n, dtype=np.float64)
        ay = np.empty(n, dtype=np.float64)
        bx = np.empty(n, dtype=np.float64)
        by = np.empty(n, dtype=np.float64)
        for i in range(n):
            ax[i] = 500_000.0 + 10.0 * (i % 100)
            ay[i] = 5_200_000.0 + 10.0 * (i // 100)
            # deterministic angle
            ang = 0.37 * i
            if i % 2 == 0:
                # 25-75 m
                dist = 25.0 + 50.0 * ((i // 2) % 100) / 99.0
            else:
                # 125-200 m
                dist = 125.0 + 75.0 * ((i // 2) % 100) / 99.0
            bx[i] = ax[i] + dist * math.cos(ang)
            by[i] = ay[i] + dist * math.sin(ang)
        gm_a = gm.points(ax, ay, crs=_CRS_UTM)
        gm_b = gm.points(bx, by, crs=_CRS_UTM)
        sh_a = shapely.points(ax, ay)
        sh_b = shapely.points(bx, by)
        return {'gm_a': gm_a, 'gm_b': gm_b, 'sh_a': sh_a, 'sh_b': sh_b}

    return _cached('dwithin_10k', build)


# ---------------------------------------------------------------------------
# INTERSECTION pairs — 20/20/20/40 relation mix
# ---------------------------------------------------------------------------


def intersection_pairs_1k():
    """200 disjoint + 200 right-contained + 200 left-contained + 400 partial."""

    def build():
        import gometry as gm
        import shapely

        left_gm: list[gm.Geometry] = []
        right_gm: list[gm.Geometry] = []
        left_sh: list[Any] = []
        right_sh: list[Any] = []

        def add_pair(la, ra):
            left_gm.append(gm.Polygon(la))
            right_gm.append(gm.Polygon(ra))
            left_sh.append(shapely.Polygon(la))
            right_sh.append(shapely.Polygon(ra))

        # 200 disjoint
        for i in range(200):
            x = 1000.0 * i
            add_pair(
                _radial_polygon_xy(x, 0.0, 6, 20.0, 0.1 * i),
                _radial_polygon_xy(x, 200.0, 6, 20.0, 0.2 * i),
            )
        # 200 right contained in left
        for i in range(200):
            x = 1000.0 * i
            add_pair(
                _radial_polygon_xy(x, 1000.0, 8, 40.0, 0.1 * i),
                _radial_polygon_xy(x, 1000.0, 5, 12.0, 0.3 * i),
            )
        # 200 left contained in right
        for i in range(200):
            x = 1000.0 * i
            add_pair(
                _radial_polygon_xy(x, 2000.0, 5, 12.0, 0.15 * i),
                _radial_polygon_xy(x, 2000.0, 8, 40.0, 0.25 * i),
            )
        # 400 partially overlapping
        for i in range(400):
            x = 1000.0 * i
            add_pair(
                _radial_polygon_xy(x, 3000.0, 7, 25.0, 0.1 * i),
                _radial_polygon_xy(x + 20.0, 3000.0 + 15.0, 7, 25.0, 0.2 * i),
            )
        assert len(left_gm) == 1000
        return {
            'gm_a': gm.GeometryArray(left_gm, crs=_CRS_UTM),
            'gm_b': gm.GeometryArray(right_gm, crs=_CRS_UTM),
            'sh_a': np.asarray(left_sh, dtype=object),
            'sh_b': np.asarray(right_sh, dtype=object),
        }

    return _cached('intersection_1k', build)


# ---------------------------------------------------------------------------
# SERVICE_AREAS_1024 — direct 32-vertex radius-50 m polygons
# ---------------------------------------------------------------------------


def service_areas_1024():
    def build():
        import gometry as gm
        import shapely

        geoms_gm: list[gm.Geometry] = []
        geoms_sh: list[Any] = []
        spacing = 60.0
        radius = 50.0
        for row in range(32):
            for col in range(32):
                cx = 500_000.0 + spacing * col
                cy = 5_200_000.0 + spacing * row
                pts = _radial_polygon_xy(cx, cy, 32, radius, 0.0, wobble=0.0)
                geoms_gm.append(gm.Polygon(pts))
                geoms_sh.append(shapely.Polygon(pts))
        return {
            'gm': gm.GeometryArray(geoms_gm, crs=_CRS_UTM),
            'sh': np.asarray(geoms_sh, dtype=object),
        }

    return _cached('service_areas_1024', build)


# ---------------------------------------------------------------------------
# VALIDITY / REPAIR — WKT corpus
# ---------------------------------------------------------------------------


def _invalid_wkt_classes() -> dict[str, list[str]]:
    """Deterministic WKT for four invalid classes."""
    bowties: list[str] = []
    exterior_holes: list[str] = []
    overlapping_holes: list[str] = []
    nested_holes: list[str] = []
    for i in range(500):
        x = 10.0 * i
        # bowtie (self-crossing hourglass)
        bowties.append(f'POLYGON(({x} 0, {x + 2} 2, {x + 2} 0, {x} 2, {x} 0))')
        # exterior hole: hole outside shell (invalid)
        exterior_holes.append(
            f'POLYGON(({x} 0, {x + 4} 0, {x + 4} 4, {x} 4, {x} 0), '
            f'({x + 10} 0, {x + 12} 0, {x + 12} 2, {x + 10} 2, {x + 10} 0))'
        )
        # overlapping holes
        overlapping_holes.append(
            f'POLYGON(({x} 0, {x + 10} 0, {x + 10} 10, {x} 10, {x} 0), '
            f'({x + 2} 2, {x + 6} 2, {x + 6} 6, {x + 2} 6, {x + 2} 2), '
            f'({x + 4} 4, {x + 8} 4, {x + 8} 8, {x + 4} 8, {x + 4} 4))'
        )
        # nested holes (hole inside hole without intermediate shell)
        nested_holes.append(
            f'POLYGON(({x} 0, {x + 12} 0, {x + 12} 12, {x} 12, {x} 0), '
            f'({x + 2} 2, {x + 10} 2, {x + 10} 10, {x + 2} 10, {x + 2} 2), '
            f'({x + 4} 4, {x + 8} 4, {x + 8} 8, {x + 4} 8, {x + 4} 4))'
        )
    return {
        'bowtie': bowties,
        'exterior_hole': exterior_holes,
        'overlapping_holes': overlapping_holes,
        'nested_holes': nested_holes,
    }


def validity_10k_wkt() -> list[str]:
    """8k valid + 500 each of four invalid classes; identical WKT for both libs."""

    def build():
        wkts: list[str] = []
        for i in range(8000):
            x = 20.0 * (i % 100)
            y = 20.0 * (i // 100)
            # valid simple rectangle with optional hole on every 10th
            if i % 10 == 0:
                shell = f'{x} {y}, {x + 8} {y}, {x + 8} {y + 8}, {x} {y + 8}, {x} {y}'
                hole = (
                    f'{x + 2} {y + 2}, {x + 2} {y + 5}, '
                    f'{x + 5} {y + 5}, {x + 5} {y + 2}, {x + 2} {y + 2}'
                )
                wkts.append(f'POLYGON(({shell}), ({hole}))')
            else:
                n = 4 + i % 5
                pts = _radial_polygon_xy(x, y, n, 5.0 + i % 3, 0.05 * i)
                ring = ', '.join(f'{px} {py}' for px, py in pts)
                px0, py0 = pts[0]
                wkts.append(f'POLYGON(({ring}, {px0} {py0}))')
        classes = _invalid_wkt_classes()
        for key in ('bowtie', 'exterior_hole', 'overlapping_holes', 'nested_holes'):
            wkts.extend(classes[key])
        assert len(wkts) == 10_000
        return wkts

    return _cached('validity_10k_wkt', build)


def validity_10k_gometry():
    import gometry as gm

    def build():
        geoms = [gm.from_wkt(w) for w in validity_10k_wkt()]
        return gm.GeometryArray(geoms, crs=_CRS_UTM)

    return _cached('validity_10k_gm', build)


def validity_10k_shapely():
    import shapely

    def build():
        return shapely.from_wkt(validity_10k_wkt())

    return _cached('validity_10k_sh', build)


def repair_1k_wkt() -> list[str]:
    """250 examples from each invalid class."""

    def build():
        classes = _invalid_wkt_classes()
        out: list[str] = []
        for key in ('bowtie', 'exterior_hole', 'overlapping_holes', 'nested_holes'):
            out.extend(classes[key][:250])
        assert len(out) == 1000
        return out

    return _cached('repair_1k_wkt', build)


def repair_1k_gometry():
    import gometry as gm

    def build():
        return gm.GeometryArray([gm.from_wkt(w) for w in repair_1k_wkt()], crs=_CRS_UTM)

    return _cached('repair_1k_gm', build)


def repair_1k_shapely():
    import shapely

    def build():
        return shapely.from_wkt(repair_1k_wkt())

    return _cached('repair_1k_sh', build)


# ---------------------------------------------------------------------------
# PARCELS_10K — edge-matched warped grid
# ---------------------------------------------------------------------------


def parcels_10k():
    """100x100 warped node grid; adjacent parcels share exact nodes."""

    def build():
        import gometry as gm
        import shapely

        # 101x101 nodes
        nodes_x = np.empty((101, 101), dtype=np.float64)
        nodes_y = np.empty((101, 101), dtype=np.float64)
        for iy in range(101):
            for ix in range(101):
                nodes_x[iy, ix] = (
                    500_000.0 + 25.0 * ix + 1.5 * math.sin(0.17 * iy + 0.03 * ix)
                )
                nodes_y[iy, ix] = (
                    5_200_000.0 + 25.0 * iy + 1.5 * math.cos(0.13 * ix + 0.05 * iy)
                )
        gm_geoms: list[gm.Geometry] = []
        sh_geoms: list[Any] = []
        for iy in range(100):
            for ix in range(100):
                ring = [
                    (nodes_x[iy, ix], nodes_y[iy, ix]),
                    (nodes_x[iy, ix + 1], nodes_y[iy, ix + 1]),
                    (nodes_x[iy + 1, ix + 1], nodes_y[iy + 1, ix + 1]),
                    (nodes_x[iy + 1, ix], nodes_y[iy + 1, ix]),
                ]
                gm_geoms.append(gm.Polygon(ring))
                sh_geoms.append(shapely.Polygon(ring))
        return {
            'gm': gm.GeometryArray(gm_geoms, crs=_CRS_UTM),
            'sh': np.asarray(sh_geoms, dtype=object),
        }

    return _cached('parcels_10k', build)


# ---------------------------------------------------------------------------
# Masked CRS / BNG / geodesic / destination
# ---------------------------------------------------------------------------


def masked_crs_200k():
    """200k WGS84 points, every tenth missing; lon [-73.25,-72.75]."""

    def build():
        import geopandas as gpd
        import gometry as gm
        import shapely

        n = 200_000
        geoms_gm: list[Any] = []
        geoms_sh: list[Any] = []
        for i in range(n):
            if i % 10 == 0:
                geoms_gm.append(None)
                geoms_sh.append(None)
            else:
                lon = -73.25 + 0.5 * ((i % 1000) / 999.0)
                lat = 41.0 + 0.25 * math.sin(0.013 * i)
                geoms_gm.append(gm.Point(lon, lat))
                geoms_sh.append(shapely.Point(lon, lat))
        gm_arr = gm.GeometryArray(geoms_gm, crs=4326)
        gpd_s = gpd.GeoSeries(geoms_sh, crs=4326)
        return {'gm': gm_arr, 'gpd': gpd_s}

    return _cached('masked_crs_200k', build)


def bng_transform_10k() -> tuple[np.ndarray, np.ndarray]:
    def build():
        i = np.arange(_N10K, dtype=np.float64)
        x = 530_000.0 + 50_000.0 * np.sin(0.017 * i)
        y = 180_000.0 + 50_000.0 * np.cos(0.019 * i)
        return np.ascontiguousarray(x), np.ascontiguousarray(y)

    return _cached('bng_10k', build)


def geodesic_pairs_10k():
    """Low-discrepancy global pairs, lat ∈ ±70°, with antimeridian cases."""

    def build():
        import gometry as gm
        import shapely

        n = _N10K
        lon1 = np.empty(n, dtype=np.float64)
        lat1 = np.empty(n, dtype=np.float64)
        lon2 = np.empty(n, dtype=np.float64)
        lat2 = np.empty(n, dtype=np.float64)
        for i in range(n):
            u1 = _halton(i + 1, 2)
            v1 = _halton(i + 1, 3)
            u2 = _halton(i + 1, 5)
            v2 = _halton(i + 1, 7)
            lon1[i] = -180.0 + 360.0 * u1
            lat1[i] = -70.0 + 140.0 * v1
            lon2[i] = -180.0 + 360.0 * u2
            lat2[i] = -70.0 + 140.0 * v2
            # explicit antimeridian-crossing cases: every 50th pair
            if i % 50 == 0:
                lon1[i] = 170.0 + 8.0 * u1
                lon2[i] = -170.0 - 8.0 * u2
                lat1[i] = -40.0 + 80.0 * v1
                lat2[i] = -40.0 + 80.0 * v2
        return {
            'gm_a': gm.points(lon1, lat1, crs=4326),
            'gm_b': gm.points(lon2, lat2, crs=4326),
            'lon1': lon1,
            'lat1': lat1,
            'lon2': lon2,
            'lat2': lat2,
            'sh_a': shapely.points(lon1, lat1),
            'sh_b': shapely.points(lon2, lat2),
        }

    return _cached('geodesic_pairs_10k', build)


def destination_inputs_10k():
    """Varying starts, bearings in [-180,180), distances 1-2000 km."""

    def build():
        import gometry as gm

        n = _N10K
        lon = np.empty(n, dtype=np.float64)
        lat = np.empty(n, dtype=np.float64)
        az = np.empty(n, dtype=np.float64)
        dist = np.empty(n, dtype=np.float64)
        for i in range(n):
            lon[i] = -180.0 + 360.0 * _halton(i + 1, 2)
            lat[i] = -70.0 + 140.0 * _halton(i + 1, 3)
            az[i] = -180.0 + 360.0 * _halton(i + 1, 5)
            # 1 km .. 2000 km
            dist[i] = 1_000.0 + 1_999_000.0 * _halton(i + 1, 7)
        starts = gm.points(lon, lat, crs=4326)
        return {
            'starts': starts,
            'lon': lon,
            'lat': lat,
            'az': az,
            'dist': dist,
        }

    return _cached('destination_10k', build)


# ---------------------------------------------------------------------------
# Index query boxes / nearest queries (projected buildings)
# ---------------------------------------------------------------------------


def index_query_boxes_1k():
    """1k boxes over buildings grid; widths 100/300/600/1200 m."""

    def build():
        import gometry as gm
        import shapely

        widths = (100.0, 300.0, 600.0, 1_200.0)
        gm_boxes: list[gm.Geometry] = []
        sh_boxes: list[Any] = []
        for i in range(1000):
            w = widths[i % 4]
            # centers scatter over the 100x100 building grid (50 m spacing)
            col = (i * 7) % 100
            row = (i * 13) % 100
            cx = 500_000.0 + 50.0 * col
            cy = 5_200_000.0 + 50.0 * row
            half = w / 2.0
            gm_boxes.append(gm.box(cx - half, cy - half, cx + half, cy + half))
            sh_boxes.append(shapely.box(cx - half, cy - half, cx + half, cy + half))
        return {
            'gm': gm.GeometryArray(gm_boxes, crs=_CRS_UTM),
            'sh': np.asarray(sh_boxes, dtype=object),
        }

    return _cached('index_boxes_1k', build)


def index_nearest_queries_1k():
    """1k points guaranteed unique nonintersecting nearest building."""

    def build():
        import gometry as gm
        import shapely

        # Place queries near distinct building centers with small offset
        gm_q: list[gm.Geometry] = []
        sh_q: list[Any] = []
        for i in range(1000):
            # pick 1000 distinct buildings from the 10k grid
            bi = i * 10  # stride through grid
            col = bi % 100
            row = bi // 100
            cx = 500_000.0 + 50.0 * col + 0.5  # slight offset inside footprint
            cy = 5_200_000.0 + 50.0 * row + 0.5
            gm_q.append(gm.Point(cx, cy))
            sh_q.append(shapely.Point(cx, cy))
        return {
            'gm': gm.GeometryArray(gm_q, crs=_CRS_UTM),
            'sh': np.asarray(sh_q, dtype=object),
        }

    return _cached('index_nearest_1k', build)


def index_build_probe_boxes() -> list[Any]:
    """Frozen 64-box post-build probe for index.build oracle."""

    def build():
        import gometry as gm

        boxes = []
        for i in range(64):
            col = (i * 3) % 100
            row = (i * 5) % 100
            cx = 500_000.0 + 50.0 * col
            cy = 5_200_000.0 + 50.0 * row
            boxes.append(gm.box(cx - 80, cy - 80, cx + 80, cy + 80, crs=_CRS_UTM))
        return boxes

    return _cached('index_probe_64', build)
