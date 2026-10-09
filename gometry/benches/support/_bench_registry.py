"""The ordered, fully paired public benchmark manifest.

The registry is the editorial source for operation labels, fixture identities,
timed row names, oracle builders, filtering, and summary ordering.  Every
release operation is a two-sided competitor row; resource probes are kept out
of this publication matrix.
"""

from __future__ import annotations

import dataclasses
import functools
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

_ROOT = Path(__file__).resolve().parents[2]
_TOOLS = _ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from public_inventory import load_public_symbols, unknown_public_symbols


@functools.cache
def public_api_symbols() -> frozenset[str]:
    """Public API inventory, parsed on first use: every paired-block worker
    imports this registry, and only manifest validation needs the inventory.
    """
    return load_public_symbols(
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
Phase = Literal['warm', 'steady', 'first-use']

DOMAIN_ORDER: tuple[Domain, ...] = (
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
)


@dataclass(frozen=True, slots=True)
class ScalingMetadata:
    """Typed curve membership attached to any canonical public operation."""

    curve_id: str
    dimension: str
    unit: str
    point: int | float
    point_label: str
    useful_work_unit: str


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
    phase: Phase = 'warm'
    scaling: ScalingMetadata | None = None

    @property
    def rows(self) -> tuple[str, str]:
        return self.gometry, self.competitor

    @property
    def paired(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class ScalingPoint:
    """One named point on a sparse, single-dimension scaling curve."""

    point: int | float
    label: str


@dataclass(frozen=True, slots=True)
class ScalingReuse:
    """A curve point owned by an existing representative operation."""

    point: ScalingPoint
    rows: tuple[str, str]


@dataclass(frozen=True, slots=True)
class ScalingCurve:
    """The only metadata needed to derive a three-point curve."""

    curve_id: str
    domain: Domain
    label: str
    workload: str
    canonical_symbols: tuple[str, ...]
    dimension: str
    unit: str
    points: tuple[ScalingPoint, ScalingPoint, ScalingPoint]
    gometry_prefix: str
    competitor_prefix: str
    competitor_label: str
    useful_work_unit: str
    phase: Phase = 'warm'
    reuse: ScalingReuse | None = None


@dataclass(frozen=True, slots=True)
class ScalingOperation:
    """One paired row derived from one point in ``SCALING_CURVES``."""

    curve_id: str
    domain: Domain
    label: str
    workload: str
    canonical_symbols: tuple[str, ...]
    dimension: str
    unit: str
    point: int | float
    point_label: str
    useful_work_unit: str
    phase: Phase
    gometry: str
    competitor: str
    competitor_label: str
    suite: Suite = 'competitors'

    @property
    def rows(self) -> tuple[str, str]:
        return self.gometry, self.competitor

    @property
    def paired(self) -> bool:
        return True

    @property
    def scaling(self) -> ScalingMetadata:
        return ScalingMetadata(
            curve_id=self.curve_id,
            dimension=self.dimension,
            unit=self.unit,
            point=self.point,
            point_label=self.point_label,
            useful_work_unit=self.useful_work_unit,
        )


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
_BASE_RELEASE_OPERATIONS: tuple[ReleaseOperation, ...] = (
    # --- Array construction & I/O — 7 ---
    _op(
        'Array construction & I/O',
        'from_wkb mixed EWKB',
        '10k mixed EWKB',
        ('from_wkb',),
        'gometry.from_wkb.batch/10k_mixed_ewkb',
        'shapely.from_wkb.batch/10k_mixed_ewkb',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Array construction & I/O',
        'from_arrow mixed GeoArrow WKB',
        '100k mixed GeoArrow WKB / 10% missing',
        ('from_arrow',),
        'gometry.from_arrow/100k_mixed_10pct_missing',
        'geopandas.GeoSeries.from_arrow/100k_mixed_10pct_missing',
        'GeoPandas',
        ('batched',),
    ),
    _op(
        'Array construction & I/O',
        'to_wkb mixed EWKB',
        '10k mixed EWKB',
        ('GeometryArray.to_wkb',),
        'gometry.to_wkb.batch/10k_mixed_ewkb',
        'shapely.to_wkb.batch/10k_mixed_ewkb',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Array construction & I/O',
        'coords matrix and row_index',
        '100k vertices / float64 matrix + int64 row index',
        (
            'GeometryArray.coords',
            'Coordinates.__array__',
            'Coordinates.row_index',
        ),
        'gometry.coords.matrix_and_index/100k_vertices',
        'shapely.get_coordinates/100k_vertices_with_index',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Array construction & I/O',
        'points from NumPy XY',
        '10k NumPy XY',
        ('points',),
        'gometry.points/10k_numpy_xy',
        'shapely.points/10k_numpy_xy',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Array construction & I/O',
        'from WKT mixed geometries',
        '10k realistic mixed WKT',
        ('from_wkt',),
        'gometry.from_wkt.batch/10k_realistic_mixed_wkt',
        'shapely.from_wkt.batch/10k_realistic_mixed_wkt',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Array construction & I/O',
        'from GeoJSON countries',
        '217 countries / 2.26 MB',
        ('from_geojson',),
        'gometry.from_geojson/217_countries_2_26mb',
        'shapely.from_geojson/217_countries_2_26mb',
        'Shapely',
        ('batched',),
    ),
    # --- Geometry — 15 ---
    _op(
        'Geometry',
        'intersects irregular polygon points',
        '12k interior, boundary, and exterior probes',
        ('intersects',),
        'gometry.intersects/12k_irregular_polygon_point_array',
        'shapely.intersects/12k_irregular_polygon_point_array',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'prepared polygon contains XY',
        '100k probes / 1,316-coordinate holed polygon',
        ('contains_xy',),
        'gometry.contains_xy/prepared_100k_probes_1316_vertex_polygon',
        'shapely.contains_xy.prepared/100k_probes_1316_vertex_polygon',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'dwithin pairwise',
        '10k pairs / 50% matches',
        ('dwithin',),
        'gometry.dwithin/pairwise_10k_50pct_matches',
        'shapely.dwithin/pairwise_10k_50pct_matches',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'area projected buildings',
        '10k projected buildings',
        ('GeometryArray.area',),
        'gometry.area/10k_projected_buildings',
        'shapely.area/10k_projected_buildings',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'length roads',
        '10k roads / 100k vertices',
        ('GeometryArray.length',),
        'gometry.length/10k_roads_100k_vertices',
        'shapely.length/10k_roads_100k_vertices',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'simplify Douglas-Peucker',
        '10k roads / 100k vertices',
        ('GeometryArray.simplify',),
        'gometry.simplify.dp/10k_roads_100k_vertices',
        'shapely.simplify.dp/10k_roads_100k_vertices',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'buffer projected points',
        '10k projected points',
        ('GeometryArray.buffer',),
        'gometry.buffer/10k_projected_points',
        'shapely.buffer/10k_projected_points',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'intersection pairwise',
        '1k irregular polygon pairs',
        ('intersection',),
        'gometry.intersection/pairwise_1k_irregular_polygons',
        'shapely.intersection/pairwise_1k_irregular_polygons',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'intersects pairwise polygons',
        '1k irregular polygon pairs',
        ('intersects',),
        'gometry.intersects/pairwise_1k_irregular_polygons',
        'shapely.intersects/pairwise_1k_irregular_polygons',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'union_all service areas',
        '1024 service areas',
        ('union_all',),
        'gometry.union_all/1024_service_areas',
        'shapely.union_all/1024_service_areas',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'coverage_union_all parcels',
        '10k edge-matched parcels',
        ('coverage_union_all',),
        'gometry.coverage_union_all/10k_edge_matched_parcels',
        'shapely.coverage_union_all/10k_edge_matched_parcels',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'is_valid mixed polygons',
        '10k mixed polygons / 20% invalid',
        ('GeometryArray.is_valid',),
        'gometry.is_valid/10k_mixed_polygons_20pct_invalid',
        'shapely.is_valid/10k_mixed_polygons_20pct_invalid',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'repair linework',
        '1k invalid polygons',
        ('GeometryArray.repair',),
        'gometry.repair.linework/1k_invalid_polygons',
        'shapely.make_valid.linework/1k_invalid_polygons',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'bounds projected buildings',
        '10k projected buildings',
        ('GeometryArray.bounds',),
        'gometry.bounds/10k_projected_buildings',
        'shapely.bounds/10k_projected_buildings',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'clip_by_rect realistic linework',
        '10k linework / crossing, inside, outside categories',
        ('GeometryArray.clip_by_rect',),
        'gometry.clip_by_rect/10k_realistic_linework_categories',
        'shapely.clip_by_rect/10k_realistic_linework_categories',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Geometry',
        'point_on_surface buildings',
        '10k concave and holed buildings',
        ('GeometryArray.point_on_surface',),
        'gometry.point_on_surface/10k_concave_holed_buildings',
        'shapely.point_on_surface/10k_concave_holed_buildings',
        'Shapely',
        ('batched',),
    ),
    # --- CRS & geodesy — 6 ---
    _op(
        'CRS & geodesy',
        'to_crs masked points',
        '200k points / 10% missing',
        ('GeometryArray.to_crs',),
        'gometry.to_crs.masked/200k_points_10pct_missing',
        'geopandas.GeoSeries.to_crs/200k_points_10pct_missing',
        'GeoPandas',
        ('in_core', 'batched'),
    ),
    _op(
        'CRS & geodesy',
        'crs_transform WGS84 to Web Mercator',
        '10k EPSG:4326 to 3857',
        ('crs_transform',),
        'gometry.crs_transform/10k_epsg4326_to3857',
        'pyproj.Transformer.transform/10k_epsg4326_to3857',
        'pyproj',
        ('batched',),
    ),
    _op(
        'CRS & geodesy',
        'geodesic distance',
        '10k WGS84 pairs',
        ('distance',),
        'gometry.distance.geodesic/10k_wgs84_pairs',
        'pyproj.Geod.inv/10k_wgs84_pairs',
        'pyproj',
        ('geodesic', 'batched'),
    ),
    _op(
        'CRS & geodesy',
        'geodesic destination',
        '10k WGS84 destinations',
        ('GeometryArray.destination',),
        'gometry.destination.geodesic/10k_wgs84',
        'pyproj.Geod.fwd+Shapely.points+GeoPandas.GeoSeries/10k_wgs84',
        'pyproj + Shapely + GeoPandas',
        ('geodesic', 'batched'),
    ),
    _op(
        'CRS & geodesy',
        'ellipsoidal chart-edge country area',
        '217 countries',
        ('GeometryArray.area',),
        'gometry.chart_edge_area/217_countries',
        'pyproj.chart_edge_area_reference/217_countries',
        'PyProj + NumPy quadrature reference',
        ('ellipsoidal', 'chart-edge', 'reference'),
    ),
    _op(
        'CRS & geodesy',
        'ellipsoidal chart-edge exterior length',
        '1,034 exteriors / 16,135 vertices',
        ('GeometryArray.length',),
        'gometry.chart_edge_length/1034_exteriors_16135_vertices',
        'pyproj.chart_edge_length_reference/1034_exteriors_16135_vertices',
        'PyProj + NumPy quadrature reference',
        ('ellipsoidal', 'chart-edge', 'reference'),
    ),
    # --- Discrete global grids — 4 ---
    _op(
        'Discrete global grids',
        'geohash encode tokens',
        '10k WGS84 points / precision 6',
        ('geohash_cells', 'CellArray.token'),
        'gometry.geohash_encode/10k_precision6',
        'pygeohash.encode/10k_precision6',
        'pygeohash',
    ),
    _op(
        'Discrete global grids',
        'h3 cover center Brazil',
        'Brazil res5 / 32,260 cells',
        ('h3_cover', 'CellArray.to_numpy'),
        'gometry.h3_cover.center/BR_res5_32260_cells',
        'h3.numpy_int.h3shape_to_cells/BR_res5_32260_cells',
        'h3-py',
        ('batched',),
    ),
    _op(
        'Discrete global grids',
        'h3_compact',
        '32,260 to 1,732 cells',
        ('CellArray.compact', 'CellArray.to_numpy'),
        'gometry.h3_compact/32260_to_1732_cells',
        'h3.numpy_int.compact_cells/32260_to_1732_cells',
        'h3-py',
        ('batched',),
    ),
    _op(
        'Discrete global grids',
        'tile_cover bbox',
        'Brazil z10 / 15,340 tiles',
        ('tile_cover',),
        'gometry.tile_cover.bbox/BR_z10_15340_tiles',
        'mercantile.tiles/BR_z10_15340_tiles',
        'Mercantile',
        ('batched',),
    ),
    # --- Spatial index — 5 ---
    _op(
        'Spatial index',
        'one-shot join within',
        '10k POIs x 217 countries',
        ('join',),
        'gometry.join.within/10k_pois_217_countries',
        'shapely.STRtree.query.one_shot.within/10k_pois_217_countries',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Spatial index',
        'index join within',
        '10k POIs x 217 countries',
        ('SpatialIndex.join',),
        'gometry.index.join.within/10k_pois_217_countries',
        'shapely.STRtree.query.within/10k_pois_217_countries',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Spatial index',
        'index candidates',
        '1k queries x 10k polygons',
        ('SpatialIndex.candidates', 'Groups.to_pairs'),
        'gometry.index.candidates/1k_queries_10k_polygons',
        'shapely.STRtree.query/1k_queries_10k_polygons',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Spatial index',
        'index nearest k=1',
        '1k non-intersecting queries x 10k polygons',
        ('SpatialIndex.nearest', 'Groups.to_pairs'),
        'gometry.index.nearest/1k_queries_10k_polygons_k1',
        'shapely.STRtree.query_nearest/1k_queries_10k_polygons_k1',
        'Shapely',
        ('batched',),
    ),
    _op(
        'Spatial index',
        'index build',
        '10k polygons',
        ('SpatialIndex',),
        'gometry.index.build/10k_polygons',
        'shapely.STRtree/10k_polygons',
        'Shapely',
        ('batched',),
    ),
)


SCALING_CURVES: tuple[ScalingCurve, ...] = (
    ScalingCurve(
        curve_id='spatial-index-cardinality',
        domain='Spatial index',
        label='index candidates: indexed cardinality',
        workload='1k queries / indexed polygon count',
        canonical_symbols=('SpatialIndex.candidates', 'Groups.to_pairs'),
        dimension='indexed-polygons',
        unit='polygons',
        points=(
            ScalingPoint(1_000, '1k'),
            ScalingPoint(5_000, '5k'),
            ScalingPoint(10_000, '10k'),
        ),
        gometry_prefix='gometry.index.candidates',
        competitor_prefix='shapely.STRtree.query',
        competitor_label='Shapely',
        useful_work_unit='candidate-pairs',
    ),
    ScalingCurve(
        curve_id='spatial-index-selectivity',
        domain='Spatial index',
        label='index candidates: query selectivity',
        workload='10k indexed polygons / 1k queries',
        canonical_symbols=('SpatialIndex.candidates', 'Groups.to_pairs'),
        dimension='query-selectivity',
        unit='fraction',
        points=(
            ScalingPoint(0.01, '1pct'),
            ScalingPoint(0.10, '10pct'),
            ScalingPoint(0.50, '50pct'),
        ),
        gometry_prefix='gometry.index.candidates',
        competitor_prefix='shapely.STRtree.query',
        competitor_label='Shapely',
        useful_work_unit='candidate-pairs',
    ),
    ScalingCurve(
        curve_id='spatial-index-result-amplification',
        domain='Spatial index',
        label='index candidates: result amplification',
        workload='1 query / candidate-pair output',
        canonical_symbols=('SpatialIndex.candidates', 'Groups.to_pairs'),
        dimension='candidate-pairs-per-query',
        unit='pairs/query',
        points=(
            ScalingPoint(1, '1'),
            ScalingPoint(10, '10'),
            ScalingPoint(100, '100'),
        ),
        gometry_prefix='gometry.index.candidates',
        competitor_prefix='shapely.STRtree.query',
        competitor_label='Shapely',
        useful_work_unit='candidate-pairs',
    ),
    ScalingCurve(
        curve_id='overlay-input-count',
        domain='Geometry',
        label='union_all: n-ary overlay input count',
        workload='edge-matched service areas',
        canonical_symbols=('union_all',),
        dimension='input-geometries',
        unit='geometries',
        points=(
            ScalingPoint(64, '64'),
            ScalingPoint(256, '256'),
            ScalingPoint(1_024, '1024'),
        ),
        gometry_prefix='gometry.union_all',
        competitor_prefix='shapely.union_all',
        competitor_label='Shapely',
        useful_work_unit='input-geometries',
        reuse=ScalingReuse(
            point=ScalingPoint(1_024, '1024'),
            rows=(
                'gometry.union_all/1024_service_areas',
                'shapely.union_all/1024_service_areas',
            ),
        ),
    ),
    ScalingCurve(
        curve_id='overlay-disjoint-input-count',
        domain='Geometry',
        label='union_all: disjoint input count',
        workload='disjoint unit squares on a grid',
        canonical_symbols=('union_all',),
        dimension='input-geometries',
        unit='geometries',
        points=(
            ScalingPoint(1_000, '1k'),
            ScalingPoint(4_000, '4k'),
            ScalingPoint(16_000, '16k'),
        ),
        gometry_prefix='gometry.union_all.disjoint',
        competitor_prefix='shapely.union_all.disjoint',
        competitor_label='Shapely',
        useful_work_unit='input-geometries',
    ),
    ScalingCurve(
        curve_id='overlay-hole-count',
        domain='Geometry',
        label='intersection: polygon hole count',
        workload='square shell with square holes / cutting box',
        canonical_symbols=('intersection',),
        dimension='holes',
        unit='holes',
        points=(
            ScalingPoint(250, '250'),
            ScalingPoint(1_000, '1k'),
            ScalingPoint(4_000, '4k'),
        ),
        gometry_prefix='gometry.intersection.holes',
        competitor_prefix='shapely.intersection.holes',
        competitor_label='Shapely',
        useful_work_unit='holes',
    ),
    ScalingCurve(
        curve_id='grid-cover-output-count',
        domain='Discrete global grids',
        label='tile_cover: output count',
        workload='Brazil bounding box / bounded output',
        canonical_symbols=('tile_cover',),
        dimension='grid-output-count',
        unit='tiles',
        points=(
            ScalingPoint(990, '990'),
            ScalingPoint(3_960, '3960'),
            ScalingPoint(15_340, '15340'),
        ),
        gometry_prefix='gometry.tile_cover.bbox',
        competitor_prefix='mercantile.tiles',
        competitor_label='Mercantile',
        useful_work_unit='output-tiles',
        reuse=ScalingReuse(
            point=ScalingPoint(15_340, '15340'),
            rows=(
                'gometry.tile_cover.bbox/BR_z10_15340_tiles',
                'mercantile.tiles/BR_z10_15340_tiles',
            ),
        ),
    ),
    ScalingCurve(
        curve_id='prepared-batch-boundary',
        domain='Geometry',
        label='contains_xy: certified batch boundary',
        workload='prepared polygon / probe count',
        canonical_symbols=('contains_xy',),
        dimension='probe-count',
        unit='probes',
        points=(
            ScalingPoint(9_999, 'below-gate'),
            ScalingPoint(10_000, 'gate'),
            ScalingPoint(10_001, 'above-gate'),
        ),
        gometry_prefix='gometry.contains_xy',
        competitor_prefix='shapely.contains_xy.prepared',
        competitor_label='Shapely',
        useful_work_unit='predicate-results',
    ),
)


def _scaling_row_name(prefix: str, curve: ScalingCurve, point: ScalingPoint) -> str:
    return f'{prefix}/scaling={curve.curve_id}/{curve.dimension}={point.label}'


def _scaling_metadata(curve: ScalingCurve, point: ScalingPoint) -> ScalingMetadata:
    return ScalingMetadata(
        curve_id=curve.curve_id,
        dimension=curve.dimension,
        unit=curve.unit,
        point=point.point,
        point_label=point.label,
        useful_work_unit=curve.useful_work_unit,
    )


def _attach_reused_scaling_membership(
    operations: tuple[ReleaseOperation, ...],
) -> tuple[ReleaseOperation, ...]:
    by_rows = {operation.rows: operation for operation in operations}
    memberships: dict[tuple[str, str], ScalingMetadata] = {}
    for curve in SCALING_CURVES:
        reuse = curve.reuse
        if reuse is None:
            continue
        operation = by_rows.get(reuse.rows)
        if operation is None:
            raise ValueError(
                f'scaling curve {curve.curve_id} reuses unknown operation rows: '
                f'{reuse.rows!r}'
            )
        if reuse.point not in curve.points:
            raise ValueError(
                f'scaling curve {curve.curve_id} reuses a point outside its curve'
            )
        memberships[reuse.rows] = _scaling_metadata(curve, reuse.point)
    return tuple(
        dataclasses.replace(operation, scaling=memberships.get(operation.rows))
        for operation in operations
    )


def _derive_scaling_operations() -> tuple[ScalingOperation, ...]:
    operations: list[ScalingOperation] = []
    for curve in SCALING_CURVES:
        if len(curve.points) != 3:
            raise ValueError(
                f'scaling curve {curve.curve_id} must have exactly three points'
            )
        values = [point.point for point in curve.points]
        if values != sorted(values) or len(set(values)) != 3:
            raise ValueError(
                f'scaling curve {curve.curve_id} points must be ordered and unique'
            )
        for point in curve.points:
            if curve.reuse is not None and point == curve.reuse.point:
                continue
            operations.append(
                ScalingOperation(
                    curve_id=curve.curve_id,
                    domain=curve.domain,
                    label=curve.label,
                    workload=curve.workload,
                    canonical_symbols=curve.canonical_symbols,
                    dimension=curve.dimension,
                    unit=curve.unit,
                    point=point.point,
                    point_label=point.label,
                    useful_work_unit=curve.useful_work_unit,
                    phase=curve.phase,
                    gometry=_scaling_row_name(curve.gometry_prefix, curve, point),
                    competitor=_scaling_row_name(curve.competitor_prefix, curve, point),
                    competitor_label=curve.competitor_label,
                )
            )
    return tuple(operations)


RELEASE_OPERATIONS = _attach_reused_scaling_membership(_BASE_RELEASE_OPERATIONS)
SCALING_OPERATIONS = _derive_scaling_operations()
ALL_PUBLIC_OPERATIONS = RELEASE_OPERATIONS + SCALING_OPERATIONS


def scaling_record(
    operation: ReleaseOperation | ScalingOperation,
) -> dict[str, object] | None:
    metadata = getattr(operation, 'scaling', None)
    return None if metadata is None else dataclasses.asdict(metadata)


def validate_canonical_symbols(
    operations: tuple[ReleaseOperation, ...] = RELEASE_OPERATIONS,
) -> None:
    """Reject release rows whose declared API ownership is not public."""
    for operation in operations:
        if not operation.canonical_symbols:
            raise ValueError(
                f'benchmark row {operation.gometry!r} has no canonical API symbol'
            )
        unknown = unknown_public_symbols(
            operation.canonical_symbols, public_api_symbols()
        )
        if unknown:
            raise ValueError(
                f'benchmark row {operation.gometry!r} maps to non-public API symbol(s): '
                + ', '.join(unknown)
            )


def validate_scaling_operations(
    operations: tuple[ReleaseOperation | ScalingOperation, ...] = ALL_PUBLIC_OPERATIONS,
) -> None:
    """Reject missing points, aliases, cross-products, or mixed curve units."""
    all_rows = [row for operation in operations for row in operation.rows]
    if len(all_rows) != len(set(all_rows)):
        raise ValueError(
            'public benchmark rows must be unique across canonical operations'
        )

    expected = {curve.curve_id: curve for curve in SCALING_CURVES}
    by_curve: dict[
        str, list[tuple[ScalingMetadata, ReleaseOperation | ScalingOperation]]
    ] = {}
    for operation in operations:
        scaling = operation.scaling
        if scaling is None:
            continue
        unknown = unknown_public_symbols(
            operation.canonical_symbols, public_api_symbols()
        )
        if unknown:
            raise ValueError(
                f'scaling row {operation.gometry!r} maps to non-public API symbol(s): '
                + ', '.join(unknown)
            )
        by_curve.setdefault(scaling.curve_id, []).append((scaling, operation))
    if set(by_curve) != set(expected):
        raise ValueError(
            f'scaling curve set mismatch: expected {sorted(expected)}, got {sorted(by_curve)}'
        )
    for curve_id, rows in by_curve.items():
        curve = expected[curve_id]
        if len(rows) != 3:
            raise ValueError(f'scaling curve {curve_id} must have three rows')
        memberships = [membership for membership, _operation in rows]
        points = [membership.point for membership in memberships]
        expected_points = [point.point for point in curve.points]
        if sorted(points) != expected_points or len(set(points)) != 3:
            raise ValueError(
                f'scaling curve {curve_id} points are incomplete or unordered'
            )
        if (
            len({(membership.dimension, membership.unit) for membership in memberships})
            != 1
        ):
            raise ValueError(f'scaling curve {curve_id} mixes dimensions or units')
        for point in curve.points:
            membership = _scaling_metadata(curve, point)
            matching = [
                operation for current, operation in rows if current == membership
            ]
            if len(matching) != 1:
                raise ValueError(
                    f'scaling curve {curve_id} has an invalid membership for {point.label}'
                )
            expected_rows = (
                curve.reuse.rows
                if curve.reuse is not None and point == curve.reuse.point
                else (
                    _scaling_row_name(curve.gometry_prefix, curve, point),
                    _scaling_row_name(curve.competitor_prefix, curve, point),
                )
            )
            if matching[0].rows != expected_rows:
                raise ValueError(
                    f'scaling curve {curve_id} point {point.label} has a duplicate alias'
                )


def _suite_rows(
    operations: tuple[ReleaseOperation | ScalingOperation, ...], suite: str
) -> tuple[str, ...]:
    names: list[str] = []
    for op in operations:
        if op.suite == suite:
            names.extend(op.rows)
    return tuple(names)


def operation_for_row(
    name: str,
) -> ReleaseOperation | ScalingOperation | None:
    for op in ALL_PUBLIC_OPERATIONS:
        if name in op.rows:
            return op
    return None


def expand_filter_to_pairs(names: set[str]) -> set[str]:
    expanded = set(names)
    for op in ALL_PUBLIC_OPERATIONS:
        if expanded.intersection(op.rows):
            expanded.update(op.rows)
    return expanded


@dataclass(frozen=True, slots=True)
class Profile:
    """One bounded benchmark run."""

    sampling_args: tuple[str, ...]
    paired_sampling_args: tuple[str, ...]
    paired_blocks: int
    row_timeout: int
    command_timeout: int
    total_timeout: int
    operations: tuple[ReleaseOperation, ...] = RELEASE_OPERATIONS
    scaling_operations: tuple[ScalingOperation, ...] = ()

    def rows(self, suite: str) -> tuple[str, ...]:
        return _suite_rows(self.operations + self.scaling_operations, suite)

    @property
    def all_operations(self) -> tuple[ReleaseOperation | ScalingOperation, ...]:
        return self.operations + self.scaling_operations

    @property
    def competitors(self) -> tuple[str, ...]:
        return self.rows('competitors')


RELEASE = Profile(
    sampling_args=('--blocks', '1', '--warmups', '0', '--repetitions', '1'),
    # Six independent process blocks: three AB and three BA.  Each block
    # executes both arms and writes one self-contained artifact.
    paired_sampling_args=('--warmups', '2', '--repetitions', '5'),
    paired_blocks=6,
    row_timeout=60,
    command_timeout=900,
    total_timeout=2400,
    operations=RELEASE_OPERATIONS,
    scaling_operations=SCALING_OPERATIONS,
)

SMOKE = dataclasses.replace(
    RELEASE,
    sampling_args=('--blocks', '1', '--warmups', '0', '--repetitions', '1'),
    paired_sampling_args=('--warmups', '1', '--repetitions', '1'),
    paired_blocks=1,
    row_timeout=60,
    command_timeout=600,
    total_timeout=900,
    scaling_operations=(),
)

PROFILES = {'smoke': SMOKE, 'release': RELEASE}
SUITES = ('competitors',)
PAIR_WORKER = 'bench_paired.py'
CHUNK_SIZE = 40
