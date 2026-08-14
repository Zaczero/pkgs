"""The complete bounded benchmark manifest.

There are deliberately two profiles: a single-value smoke run that proves the
selected rows execute, and a statistically sampled release run.  Ad-hoc probes
belong in ``benches/cases`` and use ``bench_ab.py``; they do not need another
orchestrator profile.

``RELEASE_OPERATIONS`` is the single ordered source for suite rows, pairing,
oracle selection, driver filtering, labels, and summary ordering.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Literal

Domain = Literal[
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
    'Real-world workflows',
]
Suite = Literal['gometry', 'competitors', 'real_world']
Footnote = Literal['geodesic', 'in_core', 'batched', 'noisy']

DOMAIN_ORDER: tuple[Domain, ...] = (
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
    'Real-world workflows',
)


@dataclass(frozen=True, slots=True)
class ReleaseOperation:
    """One logical public RELEASE operation (paired or gometry-only)."""

    domain: Domain
    label: str
    workload: str
    suite: Suite
    gometry: str
    competitor: str | None
    competitor_label: str | None
    footnotes: tuple[Footnote, ...] = ()

    @property
    def rows(self) -> tuple[str, ...]:
        if self.competitor is None:
            return (self.gometry,)
        return (self.gometry, self.competitor)

    @property
    def paired(self) -> bool:
        return self.competitor is not None


# ---------------------------------------------------------------------------
# Ordered public RELEASE set — 35 logical ops
# ---------------------------------------------------------------------------

RELEASE_OPERATIONS: tuple[ReleaseOperation, ...] = (
    # --- Array construction & I/O — 6 ---
    ReleaseOperation(
        domain='Array construction & I/O',
        label='from_wkb mixed EWKB',
        workload='10k mixed EWKB',
        suite='competitors',
        gometry='gometry.from_wkb.batch/10k_mixed_ewkb',
        competitor='shapely.from_wkb.batch/10k_mixed_ewkb',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Array construction & I/O',
        label='from_arrow mixed GeoArrow',
        workload='100k mixed 10% missing',
        suite='competitors',
        gometry='gometry.from_arrow/100k_mixed_10pct_missing',
        competitor='geopandas.GeoSeries.from_arrow/100k_mixed_10pct_missing',
        competitor_label='GeoPandas',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Array construction & I/O',
        label='from_arrow BinaryView WKB',
        workload='10k mixed EWKB binary_view',
        suite='competitors',
        gometry='gometry.from_arrow.binary_view/10k_mixed_ewkb',
        competitor='geopandas.GeoSeries.from_arrow.binary_view/10k_mixed_ewkb',
        competitor_label='GeoPandas',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Array construction & I/O',
        label='to_wkb mixed EWKB',
        workload='10k mixed EWKB',
        suite='competitors',
        gometry='gometry.to_wkb.batch/10k_mixed_ewkb',
        competitor='shapely.to_wkb.batch/10k_mixed_ewkb',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Array construction & I/O',
        label='get_coordinates with index',
        workload='100k vertices with index',
        suite='competitors',
        gometry='gometry.get_coordinates/100k_vertices_with_index',
        competitor='shapely.get_coordinates/100k_vertices_with_index',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Array construction & I/O',
        label='points from NumPy XY',
        workload='10k NumPy XY',
        suite='competitors',
        gometry='gometry.points/10k_numpy_xy',
        competitor='shapely.points/10k_numpy_xy',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    # --- Geometry — 11 ---
    ReleaseOperation(
        domain='Geometry',
        label='intersects irregular polygon points',
        workload='10k interior, boundary, and exterior probes',
        suite='competitors',
        gometry='gometry.intersects/irregular_polygon_point_array',
        competitor='shapely.intersects/irregular_polygon_point_array',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='Prepared polygon contains XY',
        workload='100k probes / 1,316-coordinate holed polygon',
        suite='competitors',
        gometry='gometry.prepare.contains_xy/100k_probes_1316_vertex_polygon',
        competitor='shapely.prepare.contains_xy/100k_probes_1316_vertex_polygon',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='dwithin pairwise',
        workload='10k pairs 50% matches',
        suite='competitors',
        gometry='gometry.dwithin/pairwise_10k_50pct_matches',
        competitor='shapely.dwithin/pairwise_10k_50pct_matches',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='area projected buildings',
        workload='10k projected buildings',
        suite='competitors',
        gometry='gometry.area/10k_projected_buildings',
        competitor='shapely.area/10k_projected_buildings',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='length roads',
        workload='10k roads / 100k vertices',
        suite='competitors',
        gometry='gometry.length/10k_roads_100k_vertices',
        competitor='shapely.length/10k_roads_100k_vertices',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='simplify Douglas-Peucker',
        workload='10k roads / 100k vertices',
        suite='competitors',
        gometry='gometry.simplify.dp/10k_roads_100k_vertices',
        competitor='shapely.simplify.dp/10k_roads_100k_vertices',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='buffer projected points',
        workload='10k projected points',
        suite='competitors',
        gometry='gometry.buffer/10k_projected_points',
        competitor='shapely.buffer/10k_projected_points',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='intersection pairwise',
        workload='1k irregular polygon pairs',
        suite='competitors',
        gometry='gometry.intersection/pairwise_1k_irregular_polygons',
        competitor='shapely.intersection/pairwise_1k_irregular_polygons',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='union_all service areas',
        workload='1024 service areas',
        suite='competitors',
        gometry='gometry.union_all/1024_service_areas',
        competitor='shapely.union_all/1024_service_areas',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='coverage_union parcels',
        workload='10k edge-matched parcels',
        suite='competitors',
        gometry='gometry.coverage_union/10k_edge_matched_parcels',
        competitor='shapely.coverage_union/10k_edge_matched_parcels',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='is_valid mixed polygons',
        workload='10k mixed polygons 20% invalid',
        suite='competitors',
        gometry='gometry.is_valid/10k_mixed_polygons_20pct_invalid',
        competitor='shapely.is_valid/10k_mixed_polygons_20pct_invalid',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Geometry',
        label='repair linework',
        workload='1k invalid polygons',
        suite='competitors',
        gometry='gometry.repair.linework/1k_invalid_polygons',
        competitor='shapely.make_valid.linework/1k_invalid_polygons',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    # --- CRS & geodesy — 4 ---
    ReleaseOperation(
        domain='CRS & geodesy',
        label='to_crs masked points',
        workload='200k points 10% missing',
        suite='competitors',
        gometry='gometry.to_crs.masked/200k_points_10pct_missing',
        competitor='geopandas.GeoSeries.to_crs/200k_points_10pct_missing',
        competitor_label='GeoPandas',
        footnotes=('in_core', 'batched'),
    ),
    ReleaseOperation(
        domain='CRS & geodesy',
        label='crs_transform BNG to WGS84',
        workload='10k EPSG:27700→4326',
        suite='competitors',
        gometry='gometry.crs_transform/10k_epsg27700_to4326',
        competitor='pyproj.Transformer.transform/10k_epsg27700_to4326',
        competitor_label='pyproj',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='CRS & geodesy',
        label='geodesic distance',
        workload='10k WGS84 pairs',
        suite='competitors',
        gometry='gometry.distance.geodesic/10k_wgs84_pairs',
        competitor='pyproj.Geod.inv/10k_wgs84_pairs',
        competitor_label='pyproj',
        footnotes=('geodesic', 'batched'),
    ),
    ReleaseOperation(
        domain='CRS & geodesy',
        label='geodesic destination',
        workload='10k WGS84',
        suite='competitors',
        gometry='gometry.destination.geodesic/10k_wgs84',
        competitor='pyproj.Geod.fwd/10k_wgs84',
        competitor_label='pyproj + Shapely + GeoPandas',
        footnotes=('geodesic', 'batched'),
    ),
    # --- Discrete global grids — 4 ---
    ReleaseOperation(
        domain='Discrete global grids',
        label='geohash encode tokens',
        workload='10k WGS84 points precision 6',
        suite='competitors',
        gometry='gometry.geohash_encode/10k_precision6',
        competitor='pygeohash.encode/10k_precision6',
        competitor_label='pygeohash',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Discrete global grids',
        label='h3_cover center Brazil',
        workload='BR res5 32260 cells',
        suite='competitors',
        gometry='gometry.h3_cover.center/BR_res5_32260_cells',
        competitor='h3.numpy_int.h3shape_to_cells/BR_res5_32260_cells',
        competitor_label='h3-py',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Discrete global grids',
        label='h3_compact',
        workload='32260→1732 cells',
        suite='competitors',
        gometry='gometry.h3_compact/32260_to_1732_cells',
        competitor='h3.numpy_int.compact_cells/32260_to_1732_cells',
        competitor_label='h3-py',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Discrete global grids',
        label='s2_cover adaptive',
        workload='BR target256 overlap',
        suite='gometry',
        gometry='gometry.s2_cover.adaptive/BR_target256_overlap',
        competitor=None,
        competitor_label=None,
    ),
    ReleaseOperation(
        domain='Discrete global grids',
        label='tile_cover bbox',
        workload='BR z10 15340 tiles',
        suite='competitors',
        gometry='gometry.tile_cover.bbox/BR_z10_15340_tiles',
        competitor='mercantile.tiles/BR_z10_15340_tiles',
        competitor_label='Mercantile',
        footnotes=('batched',),
    ),
    # --- Spatial index — 4 ---
    ReleaseOperation(
        domain='Spatial index',
        label='one-shot join within',
        workload='10k POIs x 217 countries',
        suite='competitors',
        gometry='gometry.join.within/10k_pois_217_countries',
        competitor='shapely.STRtree.query.one_shot.within/10k_pois_217_countries',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Spatial index',
        label='index join within',
        workload='10k POIs x 217 countries',
        suite='competitors',
        gometry='gometry.index.join.within/10k_pois_217_countries',
        competitor='shapely.STRtree.query.within/10k_pois_217_countries',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Spatial index',
        label='index candidates',
        workload='1k queries x 10k polygons',
        suite='competitors',
        gometry='gometry.index.candidates/1k_queries_10k_polygons',
        competitor='shapely.STRtree.query/1k_queries_10k_polygons',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Spatial index',
        label='index nearest k=1',
        workload='1k queries x 10k polygons k1',
        suite='competitors',
        gometry='gometry.index.nearest/1k_queries_10k_polygons_k1',
        competitor='shapely.STRtree.query_nearest/1k_queries_10k_polygons_k1',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Spatial index',
        label='index build',
        workload='10k polygons',
        suite='competitors',
        gometry='gometry.index.build/10k_polygons',
        competitor='shapely.STRtree/10k_polygons',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    # --- Real-world workflows — 3 ---
    ReleaseOperation(
        domain='Real-world workflows',
        label='from_geojson countries',
        workload='217 countries 2.26 MB',
        suite='real_world',
        gometry='gometry.real_world.from_geojson/217_countries_2_26mb',
        competitor='shapely.from_geojson_get_parts/217_countries_2_26mb',
        competitor_label='Shapely',
        footnotes=('batched',),
    ),
    ReleaseOperation(
        domain='Real-world workflows',
        label='geodesic country area',
        workload='217 country collection',
        suite='real_world',
        gometry='gometry.real_world.geodesic_area/217_country_collection',
        competitor='pyproj.Geod.geometry_area_perimeter/217_country_collection',
        competitor_label='pyproj',
        footnotes=('geodesic',),
    ),
    ReleaseOperation(
        domain='Real-world workflows',
        label='geodesic exterior length',
        workload='1034 exteriors / 16135 vertices',
        suite='real_world',
        gometry='gometry.real_world.geodesic_length/1034_exteriors_16135_vertices',
        competitor='pyproj.Geod.geometry_length/1034_exteriors_16135_vertices',
        competitor_label='pyproj',
        footnotes=('geodesic',),
    ),
)


def _suite_rows(
    operations: tuple[ReleaseOperation, ...], suite: str
) -> tuple[str, ...]:
    names: list[str] = []
    for op in operations:
        if op.suite != suite:
            continue
        names.append(op.gometry)
        if op.competitor is not None:
            names.append(op.competitor)
    return tuple(names)


def operation_for_row(name: str) -> ReleaseOperation | None:
    """Return the ReleaseOperation that owns a raw row name, if any."""
    for op in RELEASE_OPERATIONS:
        if name in op.rows:
            return op
    return None


def expand_filter_to_pairs(names: set[str]) -> set[str]:
    """Expand a filter set so selecting either pair member includes both."""
    expanded = set(names)
    for op in RELEASE_OPERATIONS:
        members = set(op.rows)
        if expanded & members:
            expanded |= members
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
    def gometry(self) -> tuple[str, ...]:
        return self.rows('gometry')

    @property
    def competitors(self) -> tuple[str, ...]:
        return self.rows('competitors')

    @property
    def real_world(self) -> tuple[str, ...]:
        return self.rows('real_world')


RELEASE = Profile(
    sampling_args=(
        '--processes',
        '6',
        '--values',
        '5',
        '--warmups',
        '2',
        '--min-time',
        '0.1',
    ),
    # Competitive pairs run once in each lead order. Three processes per pass
    # preserves the six-process release sample budget while cancelling drift.
    paired_sampling_args=(
        '--processes',
        '3',
        '--values',
        '5',
        '--warmups',
        '2',
        '--min-time',
        '0.1',
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
SUITES = ('gometry', 'competitors', 'real_world')
SCRIPTS = {
    'gometry': 'bench_gometry.py',
    'competitors': 'bench_competitors.py',
    'real_world': 'bench_real_world.py',
}
CHUNK_SIZE = 40
