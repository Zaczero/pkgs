"""The ordered, fully paired public benchmark manifest.

The registry is the editorial source for operation labels, fixture identities,
timed row names, oracle builders, filtering, and summary ordering.  Every
release operation is a two-sided competitor row; resource probes are kept out
of this publication matrix.
"""

from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

_ROOT = Path(__file__).resolve().parents[2]
_TOOLS = _ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from public_inventory import load_public_symbols, unknown_public_symbols

PUBLIC_API_SYMBOLS = load_public_symbols(
    _ROOT / 'python' / 'gometry' / '_lib.pyi',
    _ROOT / 'python' / 'gometry' / '__init__.py',
)

Domain = Literal[
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
]
Suite = Literal['competitors']
Footnote = Literal['geodesic', 'in_core', 'batched', 'noisy']

DOMAIN_ORDER: tuple[Domain, ...] = (
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
)


@dataclass(frozen=True, slots=True)
class ReleaseOperation:
    """One fully paired public release operation."""

    domain: Domain
    label: str
    workload: str
    suite: Suite
    canonical_symbols: tuple[str, ...]
    gometry: str
    competitor: str
    competitor_label: str
    footnotes: tuple[Footnote, ...] = ()

    @property
    def rows(self) -> tuple[str, str]:
        return self.gometry, self.competitor

    @property
    def paired(self) -> bool:
        return True


def _op(
    domain: Domain,
    label: str,
    workload: str,
    canonical_symbols: tuple[str, ...],
    gometry: str,
    competitor: str,
    competitor_label: str,
    footnotes: tuple[Footnote, ...] = (),
) -> ReleaseOperation:
    return ReleaseOperation(
        domain=domain,
        label=label,
        workload=workload,
        suite='competitors',
        canonical_symbols=canonical_symbols,
        gometry=gometry,
        competitor=competitor,
        competitor_label=competitor_label,
        footnotes=footnotes,
    )


# 37 logical operations / 74 rows.  Keep domains contiguous: this is both the
# docs order and the manifest completeness contract exercised before timing.
RELEASE_OPERATIONS: tuple[ReleaseOperation, ...] = (
    # --- Array construction & I/O — 7 ---
    _op(
        'Array construction & I/O', 'from_wkb mixed EWKB', '10k mixed EWKB',
        ('from_wkb',),
        'gometry.from_wkb.batch/10k_mixed_ewkb',
        'shapely.from_wkb.batch/10k_mixed_ewkb', 'Shapely', ('batched',),
    ),
    _op(
        'Array construction & I/O', 'from_arrow mixed GeoArrow WKB',
        '100k mixed GeoArrow WKB / 10% missing',
        ('from_arrow',),
        'gometry.from_arrow/100k_mixed_10pct_missing',
        'geopandas.GeoSeries.from_arrow/100k_mixed_10pct_missing',
        'GeoPandas', ('batched',),
    ),
    _op(
        'Array construction & I/O', 'to_wkb mixed EWKB', '10k mixed EWKB',
        ('GeometryArray.to_wkb',),
        'gometry.to_wkb.batch/10k_mixed_ewkb',
        'shapely.to_wkb.batch/10k_mixed_ewkb', 'Shapely', ('batched',),
    ),
    _op(
        'Array construction & I/O', 'get_coordinates with index',
        '100k vertices with index',
        ('get_coordinates',),
        'gometry.get_coordinates/100k_vertices_with_index',
        'shapely.get_coordinates/100k_vertices_with_index', 'Shapely', ('batched',),
    ),
    _op(
        'Array construction & I/O', 'points from NumPy XY', '10k NumPy XY',
        ('points',),
        'gometry.points/10k_numpy_xy', 'shapely.points/10k_numpy_xy',
        'Shapely', ('batched',),
    ),
    _op(
        'Array construction & I/O', 'from WKT mixed geometries',
        '10k realistic mixed WKT',
        ('from_wkt',),
        'gometry.from_wkt.batch/10k_realistic_mixed_wkt',
        'shapely.from_wkt.batch/10k_realistic_mixed_wkt', 'Shapely', ('batched',),
    ),
    _op(
        'Array construction & I/O', 'from GeoJSON countries', '217 countries / 2.26 MB',
        ('from_geojson',),
        'gometry.from_geojson/217_countries_2_26mb',
        'shapely.from_geojson/217_countries_2_26mb', 'Shapely', ('batched',),
    ),
    # --- Geometry — 15 ---
    _op(
        'Geometry', 'intersects irregular polygon points',
        '12k interior, boundary, and exterior probes',
        ('intersects',),
        'gometry.intersects/12k_irregular_polygon_point_array',
        'shapely.intersects/12k_irregular_polygon_point_array', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'prepared polygon contains XY',
        '100k probes / 1,316-coordinate holed polygon',
        ('contains_xy',),
        'gometry.contains_xy/prepared_100k_probes_1316_vertex_polygon',
        'shapely.contains_xy.prepared/100k_probes_1316_vertex_polygon',
        'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'dwithin pairwise', '10k pairs / 50% matches',
        ('dwithin',),
        'gometry.dwithin/pairwise_10k_50pct_matches',
        'shapely.dwithin/pairwise_10k_50pct_matches', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'area projected buildings', '10k projected buildings',
        ('GeometryArray.area',),
        'gometry.area/10k_projected_buildings',
        'shapely.area/10k_projected_buildings', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'length roads', '10k roads / 100k vertices',
        ('GeometryArray.length',),
        'gometry.length/10k_roads_100k_vertices',
        'shapely.length/10k_roads_100k_vertices', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'simplify Douglas-Peucker', '10k roads / 100k vertices',
        ('GeometryArray.simplify',),
        'gometry.simplify.dp/10k_roads_100k_vertices',
        'shapely.simplify.dp/10k_roads_100k_vertices', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'buffer projected points', '10k projected points',
        ('GeometryArray.buffer',),
        'gometry.buffer/10k_projected_points',
        'shapely.buffer/10k_projected_points', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'intersection pairwise', '1k irregular polygon pairs',
        ('intersection',),
        'gometry.intersection/pairwise_1k_irregular_polygons',
        'shapely.intersection/pairwise_1k_irregular_polygons', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'union_all service areas', '1024 service areas',
        ('GeometryArray.union_all',),
        'gometry.union_all/1024_service_areas',
        'shapely.union_all/1024_service_areas', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'coverage_union parcels', '10k edge-matched parcels',
        ('GeometryArray.coverage_union',),
        'gometry.coverage_union/10k_edge_matched_parcels',
        'shapely.coverage_union/10k_edge_matched_parcels', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'is_valid mixed polygons', '10k mixed polygons / 20% invalid',
        ('GeometryArray.is_valid',),
        'gometry.is_valid/10k_mixed_polygons_20pct_invalid',
        'shapely.is_valid/10k_mixed_polygons_20pct_invalid', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'repair linework', '1k invalid polygons',
        ('GeometryArray.repair',),
        'gometry.repair.linework/1k_invalid_polygons',
        'shapely.make_valid.linework/1k_invalid_polygons', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'bounds projected buildings', '10k projected buildings',
        ('GeometryArray.bounds',),
        'gometry.bounds/10k_projected_buildings',
        'shapely.bounds/10k_projected_buildings', 'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'clip_by_rect realistic linework',
        '10k linework / crossing, inside, outside categories',
        ('GeometryArray.clip_by_rect',),
        'gometry.clip_by_rect/10k_realistic_linework_categories',
        'shapely.clip_by_rect/10k_realistic_linework_categories',
        'Shapely', ('batched',),
    ),
    _op(
        'Geometry', 'point_on_surface buildings', '10k concave and holed buildings',
        ('GeometryArray.point_on_surface',),
        'gometry.point_on_surface/10k_concave_holed_buildings',
        'shapely.point_on_surface/10k_concave_holed_buildings', 'Shapely', ('batched',),
    ),
    # --- CRS & geodesy — 6 ---
    _op(
        'CRS & geodesy', 'to_crs masked points', '200k points / 10% missing',
        ('GeometryArray.to_crs',),
        'gometry.to_crs.masked/200k_points_10pct_missing',
        'geopandas.GeoSeries.to_crs/200k_points_10pct_missing', 'GeoPandas',
        ('in_core', 'batched'),
    ),
    _op(
        'CRS & geodesy', 'crs_transform WGS84 to Web Mercator',
        '10k EPSG:4326 to 3857',
        ('crs_transform',),
        'gometry.crs_transform/10k_epsg4326_to3857',
        'pyproj.Transformer.transform/10k_epsg4326_to3857', 'pyproj', ('batched',),
    ),
    _op(
        'CRS & geodesy', 'geodesic distance', '10k WGS84 pairs',
        ('distance',),
        'gometry.distance.geodesic/10k_wgs84_pairs',
        'pyproj.Geod.inv/10k_wgs84_pairs', 'pyproj', ('geodesic', 'batched'),
    ),
    _op(
        'CRS & geodesy', 'geodesic destination', '10k WGS84 destinations',
        ('GeometryArray.destination',),
        'gometry.destination.geodesic/10k_wgs84',
        'pyproj.Geod.fwd+Shapely.points+GeoPandas.GeoSeries/10k_wgs84',
        'pyproj + Shapely + GeoPandas', ('geodesic', 'batched'),
    ),
    _op(
        'CRS & geodesy', 'geodesic country area', '217-country collection',
        ('GeometryArray.area',),
        'gometry.geodesic_area/217_country_collection',
        'pyproj.Geod.geometry_area_perimeter/217_country_collection', 'pyproj',
        ('geodesic',),
    ),
    _op(
        'CRS & geodesy', 'geodesic exterior length', '1,034 exteriors / 16,135 vertices',
        ('GeometryArray.length',),
        'gometry.geodesic_length/1034_exteriors_16135_vertices',
        'pyproj.Geod.geometry_length/1034_exteriors_16135_vertices', 'pyproj',
        ('geodesic',),
    ),
    # --- Discrete global grids — 4 ---
    _op(
        'Discrete global grids', 'geohash encode tokens', '10k WGS84 points / precision 6',
        ('geohash_cells', 'CellArray.token'),
        'gometry.geohash_encode/10k_precision6', 'pygeohash.encode/10k_precision6',
        'pygeohash',
    ),
    _op(
        'Discrete global grids', 'h3 cover center Brazil', 'Brazil res5 / 32,260 cells',
        ('h3_cover', 'CellArray.to_numpy'),
        'gometry.h3_cover.center/BR_res5_32260_cells',
        'h3.numpy_int.h3shape_to_cells/BR_res5_32260_cells', 'h3-py', ('batched',),
    ),
    _op(
        'Discrete global grids', 'h3_compact', '32,260 to 1,732 cells',
        ('CellArray.compact', 'CellArray.to_numpy'),
        'gometry.h3_compact/32260_to_1732_cells',
        'h3.numpy_int.compact_cells/32260_to_1732_cells', 'h3-py', ('batched',),
    ),
    _op(
        'Discrete global grids', 'tile_cover bbox', 'Brazil z10 / 15,340 tiles',
        ('tile_cover',),
        'gometry.tile_cover.bbox/BR_z10_15340_tiles',
        'mercantile.tiles/BR_z10_15340_tiles', 'Mercantile', ('batched',),
    ),
    # --- Spatial index — 5 ---
    _op(
        'Spatial index', 'one-shot join within', '10k POIs x 217 countries',
        ('join',),
        'gometry.join.within/10k_pois_217_countries',
        'shapely.STRtree.query.one_shot.within/10k_pois_217_countries',
        'Shapely', ('batched',),
    ),
    _op(
        'Spatial index', 'index join within', '10k POIs x 217 countries',
        ('SpatialIndex.join',),
        'gometry.index.join.within/10k_pois_217_countries',
        'shapely.STRtree.query.within/10k_pois_217_countries', 'Shapely', ('batched',),
    ),
    _op(
        'Spatial index', 'index candidates', '1k queries x 10k polygons',
        ('SpatialIndex.candidates', 'Groups.to_pairs'),
        'gometry.index.candidates/1k_queries_10k_polygons',
        'shapely.STRtree.query/1k_queries_10k_polygons', 'Shapely', ('batched',),
    ),
    _op(
        'Spatial index', 'index nearest k=1', '1k non-intersecting queries x 10k polygons',
        ('SpatialIndex.nearest', 'Groups.to_pairs'),
        'gometry.index.nearest/1k_queries_10k_polygons_k1',
        'shapely.STRtree.query_nearest/1k_queries_10k_polygons_k1',
        'Shapely', ('batched',),
    ),
    _op(
        'Spatial index', 'index build', '10k polygons',
        ('SpatialIndex',),
        'gometry.index.build/10k_polygons', 'shapely.STRtree/10k_polygons',
        'Shapely', ('batched',),
    ),
)


def validate_canonical_symbols(
    operations: tuple[ReleaseOperation, ...] = RELEASE_OPERATIONS,
) -> None:
    """Reject release rows whose declared API ownership is not public."""
    for operation in operations:
        if not operation.canonical_symbols:
            raise ValueError(
                f'benchmark row {operation.gometry!r} has no canonical API symbol'
            )
        unknown = unknown_public_symbols(operation.canonical_symbols, PUBLIC_API_SYMBOLS)
        if unknown:
            raise ValueError(
                f'benchmark row {operation.gometry!r} maps to non-public API symbol(s): '
                + ', '.join(unknown)
            )


def _suite_rows(
    operations: tuple[ReleaseOperation, ...], suite: str
) -> tuple[str, ...]:
    names: list[str] = []
    for op in operations:
        if op.suite == suite:
            names.extend(op.rows)
    return tuple(names)


def operation_for_row(name: str) -> ReleaseOperation | None:
    for op in RELEASE_OPERATIONS:
        if name in op.rows:
            return op
    return None


def expand_filter_to_pairs(names: set[str]) -> set[str]:
    expanded = set(names)
    for op in RELEASE_OPERATIONS:
        if expanded.intersection(op.rows):
            expanded.update(op.rows)
    return expanded


@dataclass(frozen=True, slots=True)
class Profile:
    """One bounded benchmark run."""

    sampling_args: tuple[str, ...]
    paired_sampling_args: tuple[str, ...]
    row_timeout: int
    command_timeout: int
    total_timeout: int
    operations: tuple[ReleaseOperation, ...] = RELEASE_OPERATIONS

    def rows(self, suite: str) -> tuple[str, ...]:
        return _suite_rows(self.operations, suite)

    @property
    def competitors(self) -> tuple[str, ...]:
        return self.rows('competitors')


RELEASE = Profile(
    sampling_args=(
        '--processes', '6', '--values', '5', '--warmups', '2', '--min-time', '0.1',
    ),
    # AB and BA each use three processes.  The two rows are separate pyperf
    # benchmarks; normalized evidence matches their process ordinals within
    # each artifact and emits one independent ratio per process/artifact.
    paired_sampling_args=(
        '--processes', '3', '--values', '5', '--warmups', '2', '--min-time', '0.1',
    ),
    row_timeout=60,
    command_timeout=900,
    total_timeout=2400,
    operations=RELEASE_OPERATIONS,
)

SMOKE = dataclasses.replace(
    RELEASE,
    sampling_args=('--debug-single-value',),
    paired_sampling_args=('--debug-single-value',),
    row_timeout=60,
    command_timeout=600,
    total_timeout=900,
)

PROFILES = {'smoke': SMOKE, 'release': RELEASE}
SUITES = ('competitors',)
SCRIPTS = {'competitors': 'bench_competitors.py'}
CHUNK_SIZE = 40
