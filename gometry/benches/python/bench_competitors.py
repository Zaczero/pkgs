from __future__ import annotations

import math
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import numpy as np
import gometry as gm
from _bench_config import queue_selected_benchmarks
from _bench_config import runner as bench_runner

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyperf
warnings.filterwarnings(
    'ignore',
    message='Best transformation is not available due to missing Grid',
    category=UserWarning,
    module='pyproj.transformer',
)
_T = TypeVar('_T')
POINT_COUNT = 10000
WKB_COUNT = 1000
TEXT_COUNT = 1000
LINE_MERGE_COUNT = 1000
CLIP_COUNT = 1000
LINE_REF_COUNT = 1000
OVERLAY_COUNT = 1000
SPLIT_COUNT = 1000
OFFSET_COUNT = 1000
SHARED_PATHS_COUNT = 1000
HULL_COUNT = 1000
POLYLABEL_COUNT = 1000
SUMMARY_COUNT = 1000
SIMILARITY_COUNT = 1000
CLEANUP_COUNT = 1000
PACKED_LINES_COUNT = 20000
PACKED_LINES_1K_COUNT = 1000
PACKED_POLYGON_COUNT = 1000
DENSIFY_MAX_SEGMENT = 0.5
SNAP_COUNT = 1000
NEAREST_COUNT = 1000
INDEX_POLYGON_COUNT = 10000
BUFFER_COUNT = 1000
BUFFER_RADIUS = 10.0
BUFFER_QUADRANT_SEGMENTS = 8
DWITHIN_DISTANCE = 2000.0
NEAREST_K = 10
QUERY_BOX_COUNT = 1000
ORDER_COUNT = 1000
STRUCTURAL_COUNT = 1000
TRIANGULATION_COUNT = 1000
POLYGONIZE_COUNT = 1000
INVALID_COUNT = 1000
RELATE_PATTERN = 'T*F**F***'
GRID_H3_RESOLUTION = 9
GRID_S2_LEVEL = 15
GRID_GEOHASH_PRECISION = 6
GRID_TILE_ZOOM = 5
CRS_INFO_COUNT = 1000
CRS_BOUNDS_COUNT = 1000
CRS_CHURN_COUNT = 120
CRS_DATABASE_COUNT = 120
REAL_WORLD_GEOJSON = (
    Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'
).read_text(encoding='utf-8')
REAL_WORLD_GEOMETRY = gm.from_geojson(REAL_WORLD_GEOJSON).set_crs(4326)
# One row per country: from_geojson parses the FeatureCollection straight to
# a GeometryArray (parts() would flatten multipolygon members).
REAL_WORLD_PARTS = list(REAL_WORLD_GEOMETRY)
REAL_WORLD_COUNTRY_COUNT = len(REAL_WORLD_PARTS)
REAL_WORLD_LABEL = f'{REAL_WORLD_COUNTRY_COUNT}_countries'
XS = tuple(math.sin(i * 0.017) * 8.0 for i in range(POINT_COUNT))
YS = tuple(math.cos(i * 0.019) * 8.0 for i in range(POINT_COUNT))
ZS = tuple(100.0 + math.sin(i * 0.011) * 50.0 for i in range(POINT_COUNT))
TS = tuple(2020.0 + i % 365 / 365.0 for i in range(POINT_COUNT))
POINTS = gm.points(XS, YS, crs=4326)
WGS84_CRS = gm.CRS(4326)
GEO_POINTS_B = gm.points(
    tuple(x + 0.1 for x in XS), tuple(y + 0.1 for y in YS), crs=4326
)
GEO_DWITHIN_DISTANCE = 50000.0
NP_XS = np.array(XS, dtype='float64')
NP_YS = np.array(YS, dtype='float64')
NP_ZEROS = np.zeros(POINT_COUNT, dtype='float64')
GEO_FWD_AZIMUTH = np.full(POINT_COUNT, 45.0)
GEO_FWD_DISTANCE = np.full(POINT_COUNT, 1000.0)
CRS_XS = tuple(530000.0 + math.sin(i * 0.017) * 50000.0 for i in range(POINT_COUNT))
CRS_YS = tuple(180000.0 + math.cos(i * 0.019) * 50000.0 for i in range(POINT_COUNT))
NP_CRS_XS = np.array(CRS_XS, dtype='float64')
NP_CRS_YS = np.array(CRS_YS, dtype='float64')
CRS_POINTS = gm.points(CRS_XS, CRS_YS, crs=27700)
CRS_AOI_AREA = (-75.0, 40.0, -73.0, 42.0)
CRS_AOI_XS = tuple(980000.0 + math.sin(i * 0.017) * 50000.0 for i in range(POINT_COUNT))
CRS_AOI_YS = tuple(190000.0 + math.cos(i * 0.019) * 50000.0 for i in range(POINT_COUNT))
CRS_AOI_POINTS = gm.points(CRS_AOI_XS, CRS_AOI_YS, crs=2263)
CRS_INFO_VALUES = tuple(
    (4326, 3857, 27700, 32634)[i % 4] for i in range(CRS_INFO_COUNT)
)
CRS_DECOMPOSE_VALUES = tuple(
    (27700, 7405, 9518, 32630)[i % 4] for i in range(CRS_CHURN_COUNT)
)
CRS_OPERATION_VALUES = tuple(
    ((4326, 3857), (3857, 4326), (27700, 4326), (4326, 32634))[i % 4]
    for i in range(CRS_INFO_COUNT)
)
CRS_OPERATION_AT_VALUES = tuple(
    ((4267, 4326, -75.0, 40.0), (4267, 4326, -120.0, 35.0))[i % 2]
    for i in range(CRS_INFO_COUNT)
)
CRS_ROUNDTRIP_XS = tuple(
    -73.0 + math.sin(i * 0.017) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_ROUNDTRIP_YS = tuple(
    41.0 + math.cos(i * 0.019) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_FACTOR_VALUES = tuple(
    ((3857, -73.0, 41.0), (32618, -73.0, 41.0))[i % 2] for i in range(CRS_INFO_COUNT)
)
CRS_FACTOR_LONGITUDES = tuple(
    -73.0 + math.sin(i * 0.017) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_FACTOR_LATITUDES = tuple(
    41.0 + math.cos(i * 0.019) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_VALUES = tuple(
    (
        (4326, -73.0, 41.0, -74.0, 42.0, None, None),
        (4267, -73.0, 41.0, -74.0, 42.0, 10.0, 110.0),
    )[i % 2]
    for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_LON1 = tuple(
    -73.0 + math.sin(i * 0.017) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_LAT1 = tuple(
    41.0 + math.cos(i * 0.019) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_LON2 = tuple(
    -74.0 + math.sin(i * 0.023) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_LAT2 = tuple(
    42.0 + math.cos(i * 0.029) * 0.1 for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_AZIMUTH = tuple(
    45.0 + math.sin(i * 0.013) * 10.0 for i in range(CRS_INFO_COUNT)
)
CRS_GEODESIC_DISTANCE = tuple(1000.0 + i for i in range(CRS_INFO_COUNT))
CRS_GEODESIC_POLYGON_COORDS = [
    (-73.0, 41.0),
    (-72.0, 41.0),
    (-72.0, 42.0),
    (-73.0, 42.0),
    (-73.0, 41.0),
]
CRS_GEODESIC_POLYGON_HOLE = [
    (-72.7, 41.3),
    (-72.3, 41.3),
    (-72.3, 41.7),
    (-72.7, 41.7),
    (-72.7, 41.3),
]
CRS_GEODESIC_POLYGON = gm.Polygon(
    CRS_GEODESIC_POLYGON_COORDS, holes=[CRS_GEODESIC_POLYGON_HOLE], crs=4267
)
CRS_GEODESIC_POLYGONS = gm.GeometryArray([CRS_GEODESIC_POLYGON] * CRS_INFO_COUNT)
CRS_AUTHORITY_VALUES = tuple(
    (4326, 3857, 4979, 27700)[i % 4] for i in range(CRS_DATABASE_COUNT)
)
CRS_CHURN_VALUES = tuple(list(range(32601, 32661)) + list(range(32701, 32761)))
CRS_OPERATION_CHURN_VALUES = tuple((4326, value) for value in CRS_CHURN_VALUES)
CRS_OPERATION_COLD_VALUES = tuple(
    (
        CRS_CHURN_VALUES[i // (len(CRS_CHURN_VALUES) - 1)],
        CRS_CHURN_VALUES[
            (i % (len(CRS_CHURN_VALUES) - 1))
            + (
                1
                if i % (len(CRS_CHURN_VALUES) - 1) >= i // (len(CRS_CHURN_VALUES) - 1)
                else 0
            )
        ],
    )
    for i in range(CRS_INFO_COUNT)
)
CRS_AFFINE_OPERATION = '+proj=pipeline +step +proj=affine +xoff=1 +yoff=2 +zoff=3'
CRS_BOUNDS_VALUES = tuple(
    (-1.0 + i * 0.001, 50.0, 1.0 + i * 0.001, 51.0) for i in range(CRS_BOUNDS_COUNT)
)
CRS_BOUNDS_3D_VALUES = tuple(
    (-73.0 + i * 0.0001, 41.0, 10.0, -72.5 + i * 0.0001, 41.5, 20.0)
    for i in range(CRS_BOUNDS_COUNT)
)
GOMETRY_ONLY_BENCHMARKS = frozenset({
    'gometry.segmentize.fraction/1k',
    'gometry.smooth/polygon_200',
})
POLYGON = gm.box(-9.0, -9.0, 9.0, 9.0, crs=4326)
LINE_MERGE_INPUTS = tuple(
    gm.MultiLineString([
        [(0.0, float(i)), (1.0, float(i))],
        [(2.0, float(i)), (1.0, float(i))],
    ])
    for i in range(LINE_MERGE_COUNT)
)
CLIP_INPUTS = tuple(
    gm.box(-1.0 + i * 0.001, -1.0, 2.0 + i * 0.001, 2.0) for i in range(CLIP_COUNT)
)
LINE_REF_INPUTS = tuple(
    gm.LineString([(0.0, float(i)), (3.0, float(i + 4)), (6.0, float(i + 4))])
    for i in range(LINE_REF_COUNT)
)
LINE_REF_INPUT_ARRAY = gm.GeometryArray(LINE_REF_INPUTS)
LINE_REF_POINT = gm.Point(4.0, 5.0)
LINE_REF_POINTS = gm.points(
    tuple(4.0 for _ in range(LINE_REF_COUNT)),
    tuple(float(i) + 5.0 for i in range(LINE_REF_COUNT)),
)
OVERLAY_LEFT = gm.GeometryArray([
    gm.box(float(i), 0.0, float(i) + 2.0, 2.0) for i in range(OVERLAY_COUNT)
])
OVERLAY_RIGHT = gm.GeometryArray([
    gm.box(float(i) + 1.0, 1.0, float(i) + 3.0, 3.0) for i in range(OVERLAY_COUNT)
])
SPLIT_INPUTS = tuple(
    gm.LineString([(0.0, float(i)), (4.0, float(i))]) for i in range(SPLIT_COUNT)
)
SPLIT_POINTS = tuple(
    gm.MultiPoint([(1.0, float(i)), (3.0, float(i))]) for i in range(SPLIT_COUNT)
)
SPLIT_INPUT_ARRAY = gm.GeometryArray(SPLIT_INPUTS)
SPLIT_POINT_ARRAY = gm.GeometryArray(SPLIT_POINTS)
OFFSET_INPUTS = tuple(
    gm.LineString([(0.0, offset), (2.0, offset), (2.0, offset + 2.0)])
    for offset in (float(i) for i in range(OFFSET_COUNT))
)
SHARED_PATHS_INPUTS = tuple(
    gm.LineString([(0.0, offset), (2.0, offset)])
    for offset in (float(i) for i in range(SHARED_PATHS_COUNT))
)
SHARED_PATHS_REFERENCES = tuple(
    gm.LineString([(1.0, offset), (3.0, offset)])
    for offset in (float(i) for i in range(SHARED_PATHS_COUNT))
)
HULL_INPUTS = tuple(
    gm.MultiPoint([
        (0.0, offset),
        (4.0, offset),
        (4.0, offset + 4.0),
        (2.0, offset + 1.0),
        (0.0, offset + 4.0),
        (1.0, offset + 2.0),
        (3.0, offset + 2.0),
    ])
    for offset in (float(i) for i in range(HULL_COUNT))
)
POLYLABEL_INPUTS = tuple(
    gm.box(0.0, offset, 4.0, offset + 2.0)
    for offset in (float(i) for i in range(POLYLABEL_COUNT))
)
SUMMARY_INPUTS = tuple(
    gm.Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 1.0 + i * 0.001), (0.0, 1.0), (0.0, 0.0)])
    for i in range(SUMMARY_COUNT)
)
SIMILARITY_INPUTS = tuple(
    gm.LineString([(0.0, float(i)), (3.0, float(i + 4)), (6.0, float(i + 4))])
    for i in range(SIMILARITY_COUNT)
)
SIMILARITY_TARGET = gm.LineString([(0.0, 1.0), (3.0, 5.0), (6.0, 5.0)])
CLEANUP_INPUTS = tuple(
    gm.LineString([(0.0, float(i)), (0.0, float(i)), (2.0, float(i))])
    for i in range(CLEANUP_COUNT)
)


def _wiggly_line_coords(i: int) -> list[tuple[float, float]]:
    base_y = float(i) * 0.001
    return [
        (0.0, base_y),
        (1.0, base_y + 0.1 * math.sin(i * 0.017)),
        (2.0, base_y - 0.1 * math.cos(i * 0.019)),
        (3.0, base_y),
    ]


def _wiggly_line_coords_planar(i: int) -> list[tuple[float, float]]:
    base_x = 530000.0 + float(i) * 1.0
    base_y = 180000.0 + float(i) * 0.001
    return [
        (base_x, base_y),
        (base_x + 1000.0, base_y + 100.0 * math.sin(i * 0.017)),
        (base_x + 2000.0, base_y - 100.0 * math.cos(i * 0.019)),
        (base_x + 3000.0, base_y),
    ]


PACKED_LINES_20K = gm.GeometryArray([
    gm.LineString(_wiggly_line_coords(i), crs=4326) for i in range(PACKED_LINES_COUNT)
])
PACKED_LINES_1K = PACKED_LINES_20K[:PACKED_LINES_1K_COUNT]


def _packed_polygon_storage_row(i: int) -> gm.Geometry:
    if i % 2 == 0:
        return gm.box(0.1, 0.1, 0.9, 0.9, crs=3857)
    return gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))', crs=3857)


PACKED_POLYGONS_1K = gm.GeometryArray([
    _packed_polygon_storage_row(i) for i in range(PACKED_POLYGON_COUNT)
])
PACKED_POLYGONS_FILTER_MASK = [i % 2 == 0 for i in range(PACKED_POLYGON_COUNT)]
PACKED_LINES_PLANAR_20K = gm.GeometryArray([
    gm.LineString(_wiggly_line_coords_planar(i), crs=27700)
    for i in range(PACKED_LINES_COUNT)
])
PACKED_LINES_PLANAR_1K = PACKED_LINES_PLANAR_20K[:PACKED_LINES_1K_COUNT]
PACKED_LINES_PLANAR_1K_SHIFTED = PACKED_LINES_PLANAR_1K[
    [
        *list(range(1, len(PACKED_LINES_PLANAR_1K))),
        0,
    ]
]


def _planar_index_polygon(i: int) -> gm.Geometry:
    row = i // 100
    col = i % 100
    base_x = 530000.0 + col * 1000.0
    base_y = 180000.0 + row * 1000.0
    return gm.box(base_x, base_y, base_x + 500.0, base_y + 500.0, crs=27700)


PLANAR_POLYGON = gm.box(520000.0, 170000.0, 550000.0, 200000.0, crs=27700)
INDEX_POLYGONS = gm.GeometryArray([
    _planar_index_polygon(i) for i in range(INDEX_POLYGON_COUNT)
])
INDEX_TREE = gm.SpatialIndex(INDEX_POLYGONS)
QUERY_BOXES = gm.GeometryArray([
    gm.box(CRS_XS[i], CRS_YS[i], CRS_XS[i] + 1000.0, CRS_YS[i] + 1000.0, crs=27700)
    for i in range(QUERY_BOX_COUNT)
])
NEAREST_QUERY_POINT = gm.Point(535000.0, 185000.0, crs=27700)
CRS_POINTS_B = gm.points(
    tuple(x + 250.0 for x in CRS_XS), tuple(y + 250.0 for y in CRS_YS), crs=27700
)
BUFFER_POINTS = CRS_POINTS[:BUFFER_COUNT]
BUFFER_POLYGON_INPUTS = gm.GeometryArray([
    gm.box(
        530000.0 + float(i) * 10.0,
        180000.0,
        530500.0 + float(i) * 10.0,
        180500.0,
        crs=27700,
    )
    for i in range(BUFFER_COUNT)
])
PLANAR_OVERLAY_LEFT = gm.GeometryArray([
    gm.box(
        530000.0 + float(i), 180000.0, 530000.0 + float(i) + 2000.0, 182000.0, crs=27700
    )
    for i in range(OVERLAY_COUNT)
])
PLANAR_OVERLAY_RIGHT = gm.GeometryArray([
    gm.box(
        530000.0 + float(i) + 1000.0,
        181000.0,
        530000.0 + float(i) + 3000.0,
        183000.0,
        crs=27700,
    )
    for i in range(OVERLAY_COUNT)
])
INTERSECTION_ALL_POLYGONS = PLANAR_OVERLAY_LEFT
_UNION_ALL_DISK_N = 1024
_UNION_ALL_DISK_COLS = 32
_UNION_ALL_DISK_SPACING = 10.0
_UNION_ALL_DISK_RADIUS = 7.0


def _union_all_disk_jitter(index: int, salt: int) -> float:
    return ((index * 2654435761 + salt * 40503) % 1000 / 1000.0 - 0.5) * 3.0


UNION_ALL_DISK_XS = np.array(
    [
        index % _UNION_ALL_DISK_COLS * _UNION_ALL_DISK_SPACING
        + _union_all_disk_jitter(index, 1)
        for index in range(_UNION_ALL_DISK_N)
    ],
    dtype='float64',
)
UNION_ALL_DISK_YS = np.array(
    [
        index // _UNION_ALL_DISK_COLS * _UNION_ALL_DISK_SPACING
        + _union_all_disk_jitter(index, 2)
        for index in range(_UNION_ALL_DISK_N)
    ],
    dtype='float64',
)
UNION_ALL_OVERLAP_DISKS = gm.points(UNION_ALL_DISK_XS, UNION_ALL_DISK_YS).buffer(
    _UNION_ALL_DISK_RADIUS, quadrant_segments=8
)
PLANAR_PREPARED = PLANAR_POLYGON.prepare()
SNAP_INPUTS = tuple(
    gm.LineString([(0.0, offset), (0.9, offset + 0.1), (2.0, offset)])
    for offset in (float(i) for i in range(SNAP_COUNT))
)
SNAP_POINTS = tuple(gm.Point(1.0, float(i)) for i in range(SNAP_COUNT))
SNAP_INPUT_ARRAY = gm.GeometryArray(SNAP_INPUTS)
SNAP_POINT_ARRAY = gm.GeometryArray(SNAP_POINTS)
SHARED_PATHS_INPUT_ARRAY = gm.GeometryArray(SHARED_PATHS_INPUTS)
SHARED_PATHS_REFERENCE_ARRAY = gm.GeometryArray(SHARED_PATHS_REFERENCES)
ORDER_INPUTS = tuple(
    gm.Polygon([(0.0, 0.0), (0.0, 1.0 + i * 0.001), (2.0, 1.0), (0.0, 0.0)])
    for i in range(ORDER_COUNT)
)
STRUCTURAL_INPUTS = tuple(
    gm.LineString([
        (0.0, float(i)),
        (1.0, float(i + 1)),
        (1.0, float(i)),
        (0.0, float(i + 1)),
    ])
    for i in range(STRUCTURAL_COUNT)
)
TRIANGULATION_INPUTS = tuple(
    gm.MultiPoint([
        (0.0, float(i)),
        (1.0, float(i)),
        (0.0, float(i + 1)),
        (1.0, float(i + 1)),
    ])
    for i in range(TRIANGULATION_COUNT)
)
CONSTRAINED_TRIANGULATION_INPUTS = tuple(
    gm.Polygon(
        [
            (0.0, offset),
            (2.0, offset),
            (2.0, offset + 2.0),
            (0.0, offset + 2.0),
            (0.0, offset),
        ],
        holes=[
            [
                (0.75, offset + 0.75),
                (1.25, offset + 0.75),
                (1.25, offset + 1.25),
                (0.75, offset + 1.25),
                (0.75, offset + 0.75),
            ]
        ],
    )
    for offset in (float(i) for i in range(TRIANGULATION_COUNT))
)
POLYGON_TRIANGULATION_INPUTS = tuple(
    gm.Polygon([
        (0.0, offset),
        (3.0, offset),
        (3.0, offset + 1.0),
        (1.0, offset + 1.0),
        (1.0, offset + 3.0),
        (0.0, offset + 3.0),
        (0.0, offset),
    ])
    for offset in (float(i) for i in range(TRIANGULATION_COUNT))
)
POLYGONIZE_INPUTS = tuple(
    gm.MultiLineString([
        [(0.0, offset), (1.0, offset)],
        [(1.0, offset), (1.0, offset + 1.0)],
        [(1.0, offset + 1.0), (0.0, offset + 1.0)],
        [(0.0, offset + 1.0), (0.0, offset)],
        [(2.0, offset), (3.0, offset + 1.0)],
    ])
    for offset in (float(i) for i in range(POLYGONIZE_COUNT))
)
WKB_POINTS = tuple(
    (
        gm.Point(x, y, crs=4326).to_wkb(include_srid=True, precision=7)
        for x, y in zip(XS[:WKB_COUNT], YS[:WKB_COUNT], strict=False)
    )
)
TEXT_POINTS = gm.points(XS[:TEXT_COUNT], YS[:TEXT_COUNT], crs=4326)
WKT_POINTS = TEXT_POINTS.to_wkt()
GEOJSON_POINTS = TEXT_POINTS.to_geojson()
H3_CELLS = gm.h3_cells(XS, YS, resolution=GRID_H3_RESOLUTION)
S2_CELLS = gm.s2_cells(XS, YS, level=GRID_S2_LEVEL)
GEOHASH_CELLS = gm.geohash_cells(XS, YS, precision=GRID_GEOHASH_PRECISION)
TILE_CELLS = gm.tile_cells(XS, YS, zoom=GRID_TILE_ZOOM)
INVALID_INPUTS = gm.GeometryArray([
    gm.from_wkt(
        f'POLYGON ((0 {i}.0, 1 {i + 1}.0, 1 {i}.0, 0 {i + 1}.0, 0 {i}.0))', crs=4326
    )
    for i in range(INVALID_COUNT)
])
RELATE_TARGETS = gm.GeometryArray(STRUCTURAL_INPUTS, crs=4326)


def _validate_checked_result(name: str, result: Any) -> None:
    if name.endswith('real_world_from_geojson'):
        if isinstance(result, gm.GeometryArray):
            # gometry: a FeatureCollection parses to one row per feature
            assert len(result) > 0
        else:
            # shapely: collapses to a single GeometryCollection
            assert result.geom_type == 'GeometryCollection'
    elif name.endswith(('real_world_bounds_cold', 'real_world_bounds_warm')):
        assert len(result) == 4
    elif name.endswith(('real_world_area_cold', 'real_world_area_warm')):
        assert abs(result) > 0
    elif name.endswith('real_world_point_on_surface'):
        assert len(result) == REAL_WORLD_COUNTRY_COUNT
    elif name.endswith('nearest_points'):
        assert len(result) == NEAREST_COUNT
    elif name.endswith(('reverse', 'orient_polygons', 'normalize')):
        assert len(result) == ORDER_COUNT
    elif name.endswith(('is_simple', 'minimum_clearance')):
        assert len(result) == STRUCTURAL_COUNT
    elif name.endswith('polygon_triangles'):
        assert len(result) == TRIANGULATION_COUNT
        assert (
            sum(
                len(value) if isinstance(value, gm.GeometryArray) else len(value.geoms)
                for value in result
            )
            == TRIANGULATION_COUNT * 4
        )
    elif name.endswith('constrained_delaunay_triangles'):
        assert len(result) == TRIANGULATION_COUNT
        assert (
            sum(
                len(value) if isinstance(value, gm.GeometryArray) else len(value.geoms)
                for value in result
            )
            > TRIANGULATION_COUNT * 2
        )
    elif name.endswith('delaunay_triangles'):
        assert (
            sum(
                len(value) if isinstance(value, gm.GeometryArray) else len(value.geoms)
                for value in result
            )
            == TRIANGULATION_COUNT * 2
        )
    elif name.endswith('voronoi_polygons'):
        assert len(result) == TRIANGULATION_COUNT
        assert sum(len(value) for value in result) == TRIANGULATION_COUNT * 4
    elif name.endswith('voronoi_edges'):
        assert len(result) == TRIANGULATION_COUNT
        assert sum(len(value) for value in result) >= TRIANGULATION_COUNT * 4
    elif name.endswith('polygonize'):
        assert len(result) == POLYGONIZE_COUNT
        assert (
            sum(
                len(value)
                if isinstance(value, (gm.GeometryArray, list))
                else len(value.geoms)
                for value in result
            )
            == POLYGONIZE_COUNT
        )
    elif name.endswith('polygonize_full'):
        assert len(result) == POLYGONIZE_COUNT
        assert (
            sum(
                len(value[0])
                if isinstance(value[0], gm.GeometryArray)
                else len(value[0].geoms)
                for value in result
            )
            == POLYGONIZE_COUNT
        )
    elif name.endswith(('centroid_packed_lines_20k', 'rotate_packed_lines_20k')):
        assert len(result) == PACKED_LINES_COUNT
    elif name.endswith((
        'segmentize_packed_lines_1k',
        'densify_packed_lines_1k',
        'simplify_packed_lines_1k',
        'simplify_vw_packed_lines_1k',
        'smooth_packed_lines_1k',
        'hausdorff_distance_packed_lines_1k',
        'hausdorff_distance_packed_lines_cross_1k',
        'frechet_distance_packed_lines_1k',
        'hausdorff_distance_geographic_1k',
    )):
        assert len(result) == PACKED_LINES_1K_COUNT
    elif name.endswith('concat_packed_polygons_2x1k'):
        assert len(result) == PACKED_POLYGON_COUNT * 2
    elif name.endswith('filter_packed_polygons_1k'):
        assert len(result) == sum(PACKED_POLYGONS_FILTER_MASK)
    elif name.endswith(('remove_repeated_points', 'segmentize', 'densify')):
        assert len(result) == CLEANUP_COUNT
    elif name.endswith(('snap', 'snap_pairwise')):
        assert len(result) == SNAP_COUNT
    elif name.endswith((
        '.points',
        'h3_cell',
        's2_cell',
        'geodesic_distance',
        'geodesic_bearing',
        'geodesic_destination',
        'geodesic_interpolate',
        'geodesic_nearest',
        'to_crs_fast',
        'to_crs_proj',
        'to_crs_aoi_options',
        'crs_transform',
        'crs_transform_numpy',
        'crs_transform_aoi',
        'crs_transform_3d',
        'crs_transform_4d',
        'crs_apply',
        'crs_apply_inverse',
    )):
        if isinstance(result, tuple) and len(result) in {2, 3, 4}:
            assert len(result[0]) == POINT_COUNT
            assert all(len(values) == POINT_COUNT for values in result)
        else:
            assert len(result) == POINT_COUNT
    elif name.endswith((
        'crs_info',
        'crs_operation',
        'crs_operation_at',
        'crs_roundtrip',
        'crs_factors',
        'crs_geodesic',
        'crs_operations',
    )):
        assert len(result) == CRS_INFO_COUNT
    elif name.endswith((
        'crs_geodesic_batch',
        'crs_geodesic_direct_batch',
        'crs_geodesic_interpolate_batch',
        'crs_geodesic_geometry_batch',
    )):
        assert len(result[0]) == CRS_INFO_COUNT
    elif name.endswith((
        'crs_info_churn',
        'crs_info_decompose',
        'crs_operation_churn',
        'crs_operation_reused',
    )):
        assert len(result) == CRS_CHURN_COUNT
    elif name.endswith('crs_operation_cold_distinct'):
        assert len(result) == CRS_INFO_COUNT
    elif name.endswith('crs_transform_bounds'):
        assert len(result) == CRS_BOUNDS_COUNT
        assert result[0][0] < result[0][2]
    elif name.endswith(('crs_transform_bounds_3d', 'crs_transform_bounds_3d_corners')):
        assert len(result) == CRS_BOUNDS_COUNT
        assert len(result[0]) == 6
        assert result[0][0] < result[0][3]
        assert result[0][2] < result[0][5]
    elif name.endswith((
        'crs_catalog',
        'crs_utm_zones',
        'crs_units',
        'crs_celestial_bodies',
        'crs_non_deprecated',
        'crs_search',
        'crs_exports',
        'crs_authority_conversion',
        'crs_cf',
    )):
        assert len(result) == CRS_DATABASE_COUNT
        assert len(result[0]) > 0
    elif name.endswith('crs_same'):
        assert len(result) == CRS_DATABASE_COUNT
        assert all(result)
    elif name.endswith('nearest_m'):
        assert len(result) == 10
    elif name.endswith((
        'contains_xy',
        'contains',
        'intersects_polygon_points',
        'within_polygon_points',
        'touches_polygon_points',
        'crosses_polygon_points',
        'overlaps_polygon_points',
        'disjoint_polygon_points',
        'covers_polygon_points',
        'covered_by_polygon_points',
        'prepared_contains_polygon_points',
    )):
        assert len(result) == POINT_COUNT
    elif name.endswith('index_build'):
        assert len(result) == INDEX_POLYGON_COUNT
    elif name.endswith('index_query'):
        if isinstance(result, list):
            assert len(result) == QUERY_BOX_COUNT
        else:
            assert result.offsets is not None
            assert len(result.offsets) == QUERY_BOX_COUNT + 1
    elif name.endswith('rtree_nearest_k10_planar'):
        # rtree.nearest returns MORE than num_results on distance ties
        assert len(result) >= NEAREST_K
    elif name.endswith('nearest_k10_planar'):
        assert len(result) == NEAREST_K
    elif name.endswith(('dwithin_pairwise', 'distance_pairwise')):
        assert len(result) == POINT_COUNT
    elif name.endswith('length_lines'):
        assert len(result) == PACKED_LINES_1K_COUNT
    elif name.endswith((
        'area_polygons',
        'buffer_points',
        'buffer_polygons_dilate',
        'buffer_polygons_erosion',
        'buffer_lines',
    )):
        assert len(result) == BUFFER_COUNT
    elif name.endswith(('union_all_overlap', 'intersection_all_overlap')):
        area = result.area
        assert area > 0
    elif name.endswith(('union_pairwise', 'symmetric_difference_pairwise')):
        assert len(result) == OVERLAY_COUNT
    elif name.endswith('rtree_index_build'):
        assert result is not None
    elif name.endswith('rtree_index_query'):
        assert len(result) == QUERY_BOX_COUNT
    elif name.endswith((
        'from_wkb',
        'to_wkt',
        'from_wkt',
        'to_geojson',
        'from_geojson',
    )):
        assert len(result) == WKB_COUNT
    elif name.endswith('line_merge'):
        assert len(result) == LINE_MERGE_COUNT
    elif name.endswith('clip_by_rect'):
        assert len(result) == CLIP_COUNT
    elif name.endswith((
        'line_interpolate_point',
        'line_substring',
        'line_locate_point',
        'line_locate_point_pairwise',
    )):
        assert len(result) == LINE_REF_COUNT
    elif name.endswith(('intersection_pairwise', 'difference_pairwise')):
        assert len(result) == OVERLAY_COUNT
    elif name.endswith('split_pairwise'):
        assert len(result) == SPLIT_COUNT * 3
    elif name.endswith('split'):
        assert len(result) == SPLIT_COUNT
        assert (
            sum(
                len(value) if isinstance(value, gm.GeometryArray) else len(value.geoms)
                for value in result
            )
            == SPLIT_COUNT * 3
        )
    elif name.endswith('offset_curve'):
        assert len(result) == OFFSET_COUNT
        assert all(
            (value.geometry_type if isinstance(value, gm.Geometry) else value.geom_type)
            == 'LineString'
            for value in result
        )
    elif name.endswith(('shared_paths', 'shared_paths_pairwise')):
        assert len(result) == SHARED_PATHS_COUNT
        assert all(
            (value.geometry_type if isinstance(value, gm.Geometry) else value.geom_type)
            == 'GeometryCollection'
            for value in result
        )
    elif name.endswith('concave_hull'):
        assert len(result) == HULL_COUNT
        assert all(value.area > 0 for value in result)
    elif name.endswith('polylabel'):
        assert len(result) == POLYLABEL_COUNT
        assert all(
            (value.geometry_type if isinstance(value, gm.Geometry) else value.geom_type)
            == 'Point'
            for value in result
        )
    elif name.endswith('maximum_inscribed_circle_filled'):
        assert len(result) == POLYLABEL_COUNT
        assert all(
            value.geom_type == 'Polygon' and value.area > 0.0 for value in result
        )
    elif name.endswith('h3_cell_to_boundary'):
        assert len(result) == POINT_COUNT
    elif name.endswith((
        '.centroid',
        '.point_on_surface',
        '.envelope',
        '.convex_hull',
        '.minimum_rotated_rectangle',
        '.oriented_envelope',
        '.boundary',
    )):
        # dotted suffixes: bare ones collide (h3_cell_to_boundary once fell in)
        assert len(result) == SUMMARY_COUNT
    elif name.endswith(('hausdorff_distance', 'frechet_distance')):
        assert len(result) == SIMILARITY_COUNT
    elif name.endswith((
        'to_wkb',
        'to_arrow_roundtrip',
        'from_arrow_roundtrip',
        'from_polyline',
        'to_polyline',
    )):
        assert len(result) == WKB_COUNT or len(result) == PACKED_LINES_1K_COUNT
    elif name.endswith(('from_geopandas_geometry_array_10k', 'to_wkb_mixed_10k')):
        assert len(result) == POINT_COUNT
    elif name.endswith((
        'scale_packed_lines_20k',
        'skew_packed_lines_20k',
        'translate_packed_lines_20k',
        'affine_transform_packed_lines_20k',
    )):
        assert len(result) == PACKED_LINES_COUNT
    elif name.endswith(('relate_1k', 'relate_pattern_1k')):
        assert len(result) == STRUCTURAL_COUNT
    elif name.endswith('is_valid_10k'):
        assert len(result) == POINT_COUNT
    elif name.endswith('repair_1k'):
        assert len(result) == INVALID_COUNT
        assert all(value.is_valid for value in result)
    elif name.endswith((
        'h3_boundary_10k',
        's2_boundary_10k',
        'geohash_boundary_10k',
        'tiles_boundary_10k',
    )):
        assert len(result) == POINT_COUNT
    elif name.endswith((
        'h3_to_polygon_10k',
        's2_to_polygon_10k',
        'geohash_to_polygon_10k',
        'tiles_to_polygon_10k',
    )):
        assert result.area > 0
    elif name.endswith('h3_compact_10k'):
        # compact merges complete sibling sets and dedupes coincident cells
        assert 0 < len(result) <= POINT_COUNT
    elif name.endswith(('geohash_cell_10k', 'tiles_cell_10k')):
        assert len(result) == POINT_COUNT
    elif name.endswith(('minimum_bounding_circle_1k', 'minimum_clearance_line_1k')):
        assert len(result) == SUMMARY_COUNT
    elif name.endswith('maximum_inscribed_circle_1k'):
        assert len(result) == POLYLABEL_COUNT


def _checked(name: str, func: Callable[[], _T]) -> Callable[[], _T]:
    checked = False

    def wrapper() -> _T:
        nonlocal checked
        result = func()
        if not checked:
            _validate_checked_result(name, result)
            checked = True
        return result

    return wrapper


def gometry_points() -> gm.GeometryArray:
    return gm.points(XS, YS, crs=4326)


def gometry_contains() -> np.ndarray:
    result = gm.contains(POLYGON, POINTS)
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.bool_
    return result


def gometry_contains_xy() -> np.ndarray:
    result = gm.contains_xy(POLYGON, XS, YS)
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.bool_
    return result


def gometry_from_wkb() -> list[gm.Geometry]:
    return [gm.from_wkb(value) for value in WKB_POINTS]


def gometry_from_wkb_batch() -> gm.GeometryArray:
    result = gm.from_wkb(WKB_POINTS)
    assert isinstance(result, gm.GeometryArray)
    return result


def gometry_to_wkt_batch() -> list[str]:
    return TEXT_POINTS.to_wkt()


def gometry_from_wkt_batch() -> gm.GeometryArray:
    result = gm.from_wkt(WKT_POINTS)
    assert isinstance(result, gm.GeometryArray)
    return result


def gometry_to_geojson_batch() -> list[str]:
    return TEXT_POINTS.to_geojson()


def gometry_from_geojson_batch() -> gm.GeometryArray:
    result = gm.from_geojson(GEOJSON_POINTS)
    assert isinstance(result, gm.GeometryArray)
    return result


def gometry_line_merge() -> list[gm.Geometry]:
    return [geometry.line_merge() for geometry in LINE_MERGE_INPUTS]


def gometry_clip_by_rect() -> list[gm.Geometry]:
    return [geometry.clip_by_rect(0.0, 0.0, 1.0, 1.0) for geometry in CLIP_INPUTS]


def gometry_line_interpolate() -> gm.GeometryArray:
    return LINE_REF_INPUT_ARRAY.line_interpolate(6.0)


def gometry_line_substring() -> gm.GeometryArray:
    return LINE_REF_INPUT_ARRAY.line_substring(2.0, 6.0)


def gometry_line_locate() -> np.ndarray:
    return LINE_REF_INPUT_ARRAY.line_locate(LINE_REF_POINT)


def gometry_line_locate_pairwise() -> np.ndarray:
    return LINE_REF_INPUT_ARRAY.line_locate(LINE_REF_POINTS)


def gometry_intersection_pairwise() -> gm.GeometryArray:
    return gm.intersection(OVERLAY_LEFT, OVERLAY_RIGHT)


def gometry_difference_pairwise() -> gm.GeometryArray:
    return gm.difference(OVERLAY_LEFT, OVERLAY_RIGHT)


def gometry_split() -> list[gm.GeometryArray]:
    return [
        gm.split(geometry, splitter)
        for geometry, splitter in zip(SPLIT_INPUTS, SPLIT_POINTS, strict=False)
    ]


def gometry_split_pairwise() -> gm.GeometryArray:
    return gm.split(SPLIT_INPUT_ARRAY, SPLIT_POINT_ARRAY)


def gometry_centroid() -> list[gm.Geometry]:
    return [geometry.centroid() for geometry in SUMMARY_INPUTS]


def gometry_point_on_surface() -> list[gm.Geometry]:
    return [geometry.point_on_surface() for geometry in SUMMARY_INPUTS]


def gometry_envelope() -> list[gm.Geometry]:
    return [geometry.envelope() for geometry in SUMMARY_INPUTS]


def gometry_convex_hull() -> list[gm.Geometry]:
    return [geometry.convex_hull() for geometry in SUMMARY_INPUTS]


def gometry_concave_hull() -> list[gm.Geometry]:
    return [geometry.concave_hull(concavity=1.0) for geometry in HULL_INPUTS]


def gometry_polylabel() -> list[gm.Geometry]:
    return [geometry.polylabel(tolerance=0.01) for geometry in POLYLABEL_INPUTS]


def gometry_minimum_rotated_rectangle() -> list[gm.Geometry]:
    return [geometry.minimum_rotated_rectangle() for geometry in SUMMARY_INPUTS]


def gometry_boundary() -> list[gm.Geometry]:
    return [geometry.boundary() for geometry in SUMMARY_INPUTS]


def gometry_remove_repeated_points() -> list[gm.Geometry]:
    return [geometry.remove_repeated_points() for geometry in CLEANUP_INPUTS]


def gometry_segmentize() -> list[gm.Geometry]:
    return [geometry.segmentize(0.5) for geometry in CLEANUP_INPUTS]


def gometry_segmentize_fraction() -> list[gm.Geometry]:
    return [
        geometry.segmentize(fraction=DENSIFY_MAX_SEGMENT) for geometry in CLEANUP_INPUTS
    ]


def gometry_centroid_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.centroid()


def gometry_rotate_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.rotate(45.0)


def gometry_segmentize_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_1K.segmentize(0.5)


def gometry_segmentize_fraction_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_1K.segmentize(fraction=DENSIFY_MAX_SEGMENT)


def gometry_concat_packed_polygons_2x1k() -> gm.GeometryArray:
    return PACKED_POLYGONS_1K.concat(PACKED_POLYGONS_1K)


def gometry_filter_packed_polygons_1k() -> gm.GeometryArray:
    return PACKED_POLYGONS_1K[PACKED_POLYGONS_FILTER_MASK]


def gometry_simplify_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_1K.simplify(0.5, preserve_topology=False)


def gometry_simplify_vw_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_1K.simplify(0.5, method='vw', preserve_topology=False)


def _smooth_polygon_ring_200() -> gm.Polygon:
    coords = [
        (math.cos(angle), math.sin(angle))
        for angle in (2.0 * math.pi * i / 200.0 for i in range(200))
    ]
    coords.append(coords[0])
    return gm.Polygon(coords)


SMOOTH_POLYGON_200 = _smooth_polygon_ring_200()


def gometry_smooth_polygon_200() -> gm.Polygon:
    return SMOOTH_POLYGON_200.smooth(iterations=2, method='chaikin')


def gometry_smooth_packed_lines_1k() -> gm.GeometryArray:
    return PACKED_LINES_1K.smooth(iterations=2, method='chaikin')


def gometry_snap() -> list[gm.Geometry]:
    return [
        gm.snap(geometry, reference, 0.25)
        for geometry, reference in zip(SNAP_INPUTS, SNAP_POINTS, strict=False)
    ]


def gometry_snap_pairwise() -> gm.GeometryArray:
    return gm.snap(SNAP_INPUT_ARRAY, SNAP_POINT_ARRAY, 0.25)


def gometry_shared_paths() -> list[gm.Geometry]:
    return [
        gm.shared_paths(geometry, reference)
        for geometry, reference in zip(
            SHARED_PATHS_INPUTS, SHARED_PATHS_REFERENCES, strict=False
        )
    ]


def gometry_shared_paths_pairwise() -> gm.GeometryArray:
    return gm.shared_paths(SHARED_PATHS_INPUT_ARRAY, SHARED_PATHS_REFERENCE_ARRAY)


def gometry_offset_curve() -> list[gm.Geometry]:
    return [geometry.offset_curve(0.5) for geometry in OFFSET_INPUTS]


def gometry_hausdorff_distance() -> list[float]:
    return [
        gm.hausdorff_distance(geometry, SIMILARITY_TARGET)
        for geometry in SIMILARITY_INPUTS
    ]


def gometry_hausdorff_distance_packed_lines() -> np.ndarray:
    return gm.hausdorff_distance(PACKED_LINES_PLANAR_1K, PACKED_LINES_PLANAR_1K)


def gometry_hausdorff_distance_packed_lines_cross() -> np.ndarray:
    return gm.hausdorff_distance(PACKED_LINES_PLANAR_1K, PACKED_LINES_PLANAR_1K_SHIFTED)


def gometry_hausdorff_distance_geographic() -> np.ndarray:
    return gm.hausdorff_distance(PACKED_LINES_1K, PACKED_LINES_1K)


def gometry_frechet_distance() -> list[float]:
    return [
        gm.frechet_distance(geometry, SIMILARITY_TARGET)
        for geometry in SIMILARITY_INPUTS
    ]


def gometry_frechet_distance_packed_lines() -> np.ndarray:
    return gm.frechet_distance(PACKED_LINES_PLANAR_1K, PACKED_LINES_PLANAR_1K)


def gometry_nearest_points() -> list[tuple[gm.Geometry, gm.Geometry]]:
    return [gm.nearest_points(geometry, LINE_REF_POINT) for geometry in LINE_REF_INPUTS]


def gometry_reverse() -> list[gm.Geometry]:
    return [geometry.reverse() for geometry in ORDER_INPUTS]


def gometry_orient_polygons() -> list[gm.Geometry]:
    return [geometry.orient_polygons() for geometry in ORDER_INPUTS]


def gometry_normalize() -> list[gm.Geometry]:
    return [geometry.normalize() for geometry in ORDER_INPUTS]


def gometry_is_simple() -> list[bool]:
    return [geometry.is_simple for geometry in STRUCTURAL_INPUTS]


def gometry_minimum_clearance() -> list[float]:
    return [geometry.minimum_clearance for geometry in STRUCTURAL_INPUTS]


def gometry_triangulate_delaunay() -> list[gm.GeometryArray]:
    return [
        geometry.triangulate(method='delaunay') for geometry in TRIANGULATION_INPUTS
    ]


def gometry_triangulate_constrained() -> list[gm.GeometryArray]:
    return [
        geometry.triangulate(method='constrained')
        for geometry in CONSTRAINED_TRIANGULATION_INPUTS
    ]


def gometry_triangulate_earcut() -> list[gm.GeometryArray]:
    return [
        geometry.triangulate(method='earcut')
        for geometry in POLYGON_TRIANGULATION_INPUTS
    ]


def gometry_voronoi_polygons() -> list[gm.GeometryArray]:
    return [
        geometry.voronoi_polygons(clip='envelope') for geometry in TRIANGULATION_INPUTS
    ]


def gometry_voronoi_edges() -> list[gm.GeometryArray]:
    return [
        geometry.voronoi_edges(clip='envelope') for geometry in TRIANGULATION_INPUTS
    ]


def gometry_polygonize() -> list[gm.GeometryArray]:
    return [geometry.polygonize() for geometry in POLYGONIZE_INPUTS]


def gometry_polygonize_full() -> list[
    tuple[gm.GeometryArray, gm.GeometryArray, gm.GeometryArray, gm.GeometryArray]
]:
    return [gm.polygonize_full(geometry) for geometry in POLYGONIZE_INPUTS]


def gometry_h3_cell() -> gm.CellArray[gm.H3Cell]:
    result = gm.h3_cells(XS, YS, resolution=9)
    assert isinstance(result, gm.CellArray)
    return result


def gometry_s2_cell() -> gm.CellArray[gm.S2Cell]:
    result = gm.s2_cells(XS, YS, level=15)
    assert isinstance(result, gm.CellArray)
    return result


def gometry_geodesic_distance() -> np.ndarray:
    result = gm.distance(POINTS, gm.Point(0, 0, crs=4326))
    assert isinstance(result, np.ndarray)
    return result


def gometry_distance_geodesic_point_pairs() -> np.ndarray:
    result = gm.distance(POINTS, GEO_POINTS_B)
    assert isinstance(result, np.ndarray)
    return result


def gometry_dwithin_geodesic_point_pairs() -> np.ndarray:
    result = gm.dwithin(POINTS, GEO_POINTS_B, GEO_DWITHIN_DISTANCE)
    assert isinstance(result, np.ndarray)
    return result


def gometry_swap_xy_packed_points() -> gm.GeometryArray:
    return POINTS.swap_xy()


def gometry_swap_xy_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.swap_xy()


def gometry_swap_xy_packed_polygons() -> gm.GeometryArray:
    return PACKED_POLYGONS_1K.swap_xy()


def gometry_quantize_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_1K.quantize(6)


def gometry_quantize_packed_polygons() -> gm.GeometryArray:
    return PACKED_POLYGONS_1K.quantize(6)


def gometry_bearing() -> list[float]:
    origin = gm.Point(0, 0, crs=4326)
    result = [gm.bearing(point, origin) for point in POINTS]
    assert isinstance(result, list)
    return result


def gometry_destination() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    result = WGS84_CRS.geodesic_direct(XS, YS, 45.0, 1000.0)
    return (result['longitude'], result['latitude'], result['final_azimuth'])


def gometry_point_between() -> gm.GeometryArray:
    origin = gm.Point(0, 0, crs=4326)
    result = gm.GeometryArray(
        [gm.point_between(point, origin, 0.5, normalized=True) for point in POINTS],
        crs=4326,
    )
    assert isinstance(result, gm.GeometryArray)
    return result


def gometry_to_crs_fast() -> gm.GeometryArray:
    return POINTS.to_crs(3857)


def gometry_to_crs_proj() -> gm.GeometryArray:
    return CRS_POINTS.to_crs(4326)


def gometry_to_crs_aoi_options() -> gm.GeometryArray:
    return CRS_AOI_POINTS.to_crs(
        4326,
        area_of_interest=CRS_AOI_AREA,
        authority='EPSG',
        accuracy=10.0,
        allow_ballpark=False,
    )


def gometry_crs_transform() -> np.ndarray:
    return gm.crs_transform(27700, 4326, NP_CRS_XS, NP_CRS_YS)


def gometry_crs_transform_aoi() -> np.ndarray:
    return gm.crs_transform(
        2263, 4326, CRS_AOI_XS, CRS_AOI_YS, area_of_interest=CRS_AOI_AREA
    )


def gometry_crs_transform_3d() -> np.ndarray:
    return gm.crs_transform(4979, 4978, XS, YS, ZS)


def gometry_crs_transform_4d() -> np.ndarray:
    return gm.crs_transform(4979, 4978, XS, YS, ZS, t=TS)


def gometry_crs_apply() -> tuple[list[float], list[float], list[float], list[float]]:
    return gm.crs_apply(CRS_AFFINE_OPERATION, XS, YS, ZS, t=TS)


def gometry_crs_apply_inverse() -> tuple[
    list[float], list[float], list[float], list[float]
]:
    return gm.crs_apply(CRS_AFFINE_OPERATION, XS, YS, ZS, t=TS, direction='inverse')


def gometry_crs_info() -> list[dict[str, object]]:
    return [gm.crs_info(value) for value in CRS_INFO_VALUES]


def gometry_crs_operation() -> list[dict[str, object]]:
    return [gm.CRS(source).operation(target) for source, target in CRS_OPERATION_VALUES]


def gometry_crs_operation_at() -> list[dict[str, object]]:
    return [
        gm.CRS(source).operation(target, at=(x, y))
        for source, target, x, y in CRS_OPERATION_AT_VALUES
    ]


def gometry_crs_roundtrip() -> list[float]:
    return gm.crs_roundtrip(4326, 3857, CRS_ROUNDTRIP_XS, CRS_ROUNDTRIP_YS)


def gometry_crs_factors() -> list[dict[str, object]]:
    return [
        gm.CRS(target).factors(longitude, latitude)
        for target, longitude, latitude in CRS_FACTOR_VALUES
    ]


def gometry_crs_geodesic() -> list[dict[str, object]]:
    return [
        gm.CRS(crs).geodesic(lon1, lat1, lon2, lat2, z1=z1, z2=z2)
        for crs, lon1, lat1, lon2, lat2, z1, z2 in CRS_GEODESIC_VALUES
    ]


def gometry_crs_geodesic_batch() -> tuple[
    list[float], list[float | None], list[float], list[float]
]:
    result = WGS84_CRS.geodesic(
        CRS_GEODESIC_LON1, CRS_GEODESIC_LAT1, CRS_GEODESIC_LON2, CRS_GEODESIC_LAT2
    )
    return (
        result['distance'],
        result['distance_3d'],
        result['forward_azimuth'],
        result['reverse_azimuth'],
    )


def gometry_crs_geodesic_direct_batch() -> tuple[list[float], list[float], list[float]]:
    result = WGS84_CRS.geodesic_direct(
        CRS_GEODESIC_LON1,
        CRS_GEODESIC_LAT1,
        CRS_GEODESIC_AZIMUTH,
        CRS_GEODESIC_DISTANCE,
    )
    return (result['longitude'], result['latitude'], result['final_azimuth'])


def gometry_crs_geodesic_interpolate_batch() -> tuple[
    list[float], list[float], list[float]
]:
    result = WGS84_CRS.geodesic_interpolate(
        CRS_GEODESIC_LON1,
        CRS_GEODESIC_LAT1,
        CRS_GEODESIC_LON2,
        CRS_GEODESIC_LAT2,
        0.5,
        normalized=True,
    )
    return (result['longitude'], result['latitude'], result['final_azimuth'])


def gometry_crs_geodesic_geometry_batch() -> tuple[list[float], list[float]]:
    return (
        [geometry.set_crs(4267).area for geometry in CRS_GEODESIC_POLYGONS],
        [geometry.set_crs(4267).length for geometry in CRS_GEODESIC_POLYGONS],
    )


def gometry_crs_operations() -> list[list[dict[str, object]]]:
    return [
        gm.CRS(source).operations(target) for source, target in CRS_OPERATION_VALUES
    ]


def gometry_crs_static_catalogs() -> tuple[
    list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]
]:
    return (gm.crs_proj_operations(), gm.crs_ellipsoids(), gm.crs_prime_meridians())


def gometry_crs_authority_conversion() -> list[
    tuple[tuple[str, str] | None, int | None, gm.CRS, gm.CRS]
]:
    return [
        (
            gm.CRS(value).to_authority(),
            gm.CRS(value).to_epsg(),
            gm.CRS(value).to_3d(),
            gm.CRS(value).to_2d(),
        )
        for value in CRS_AUTHORITY_VALUES
    ]


def gometry_crs_cf() -> list[dict[str, object]]:
    return [gm.CRS(value).to_cf() for value in CRS_AUTHORITY_VALUES]


def gometry_crs_info_churn() -> list[dict[str, object]]:
    return [gm.crs_info(value) for value in CRS_CHURN_VALUES]


def gometry_crs_info_decompose() -> list[dict[str, object]]:
    return [gm.crs_info(value) for value in CRS_DECOMPOSE_VALUES]


def gometry_crs_operation_churn() -> list[dict[str, object]]:
    return [
        gm.CRS(source).operation(target)
        for source, target in CRS_OPERATION_CHURN_VALUES
    ]


def gometry_crs_transform_bounds() -> list[tuple[float, float, float, float]]:
    return [gm.crs_transform_bounds(4326, 3857, bounds) for bounds in CRS_BOUNDS_VALUES]


def gometry_crs_transform_bounds_3d() -> list[
    tuple[float, float, float, float, float, float]
]:
    return [
        gm.crs_transform_bounds(4979, 4978, bounds) for bounds in CRS_BOUNDS_3D_VALUES
    ]


def gometry_crs_transform_bounds_3d_corners() -> list[
    tuple[float, float, float, float, float, float]
]:
    return [
        gm.crs_transform_bounds(4979, 4978, bounds, densify=0)
        for bounds in CRS_BOUNDS_3D_VALUES
    ]


def gometry_crs_catalog() -> list[list[dict[str, object]]]:
    return [
        gm.crs_catalog(authority='EPSG', kind='projected', area=(-1.0, 50.0, 1.0, 52.0))
        for _ in range(CRS_DATABASE_COUNT)
    ]


def gometry_crs_utm_zones() -> list[list[dict[str, object]]]:
    return [
        gm.crs_utm_zones(datum_name='WGS 84', area=(20.0, 51.0, 22.0, 53.0))
        for _ in range(CRS_DATABASE_COUNT)
    ]


def gometry_crs_units() -> list[list[dict[str, object]]]:
    return [gm.crs_units('EPSG', category='linear') for _ in range(CRS_DATABASE_COUNT)]


def gometry_crs_celestial_bodies() -> list[list[dict[str, object]]]:
    return [gm.crs_celestial_bodies() for _ in range(CRS_DATABASE_COUNT)]


def gometry_crs_non_deprecated() -> list[list[dict[str, object]]]:
    return [gm.CRS(2037).non_deprecated() for _ in range(CRS_DATABASE_COUNT)]


def gometry_crs_search() -> list[list[dict[str, object]]]:
    return [
        gm.crs_search('British National Grid', authority='EPSG', kind='projected')
        for _ in range(CRS_DATABASE_COUNT)
    ]


def gometry_crs_exports() -> list[tuple[str, str, str, str, dict[str, object]]]:
    return [
        (
            gm.CRS(4326).to_wkt(version='WKT2_2019_SIMPLIFIED'),
            gm.CRS(4326).to_wkt(version='WKT1_GDAL', output_axis='no'),
            gm.CRS(3857).to_proj(),
            gm.CRS(4326).to_projjson(pretty=True),
            gm.CRS(4326).to_projjson_dict(),
        )
        for _ in range(CRS_DATABASE_COUNT)
    ]


def gometry_crs_same() -> list[bool]:
    wkt = gm.CRS(4326).to_wkt()
    return [gm.CRS(4326).same_as(wkt) for _ in range(CRS_DATABASE_COUNT)]


def gometry_index_build() -> gm.SpatialIndex:
    return gm.SpatialIndex(INDEX_POLYGONS)


def gometry_index_query() -> object:
    return INDEX_TREE.candidates(QUERY_BOXES)


def gometry_index_nearest_k10_planar() -> np.ndarray:
    result = INDEX_TREE.nearest(NEAREST_QUERY_POINT, k=NEAREST_K, unit='planar')
    assert isinstance(result, np.ndarray)
    return result


def gometry_dwithin_pairwise() -> np.ndarray:
    result = gm.dwithin(CRS_POINTS, CRS_POINTS_B, DWITHIN_DISTANCE)
    assert isinstance(result, np.ndarray)
    return result


def gometry_prepared_contains() -> np.ndarray:
    result = PLANAR_PREPARED.contains(CRS_POINTS)
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.bool_
    return result


def gometry_intersects_polygon_points() -> np.ndarray:
    result = gm.intersects(PLANAR_POLYGON, CRS_POINTS)
    assert isinstance(result, np.ndarray)
    return result


def gometry_within_polygon_points() -> np.ndarray:
    result = gm.within(CRS_POINTS, PLANAR_POLYGON)
    assert isinstance(result, np.ndarray)
    return result


def gometry_touches_polygon_points() -> np.ndarray:
    result = gm.touches(PLANAR_POLYGON, CRS_POINTS)
    assert isinstance(result, np.ndarray)
    return result


def gometry_crosses_polygon_points() -> np.ndarray:
    result = gm.crosses(PLANAR_POLYGON, CRS_POINTS)
    assert isinstance(result, np.ndarray)
    return result


def gometry_overlaps_polygon_points() -> np.ndarray:
    result = gm.overlaps(PLANAR_POLYGON, CRS_POINTS)
    assert isinstance(result, np.ndarray)
    return result


def gometry_disjoint_polygon_points() -> np.ndarray:
    result = gm.disjoint(PLANAR_POLYGON, CRS_POINTS)
    assert isinstance(result, np.ndarray)
    return result


def gometry_covers_polygon_points() -> np.ndarray:
    result = gm.covers(PLANAR_POLYGON, CRS_POINTS)
    assert isinstance(result, np.ndarray)
    return result


def gometry_covered_by_polygon_points() -> np.ndarray:
    result = gm.covered_by(CRS_POINTS, PLANAR_POLYGON)
    assert isinstance(result, np.ndarray)
    return result


def gometry_buffer_points() -> gm.GeometryArray:
    return BUFFER_POINTS.buffer(
        BUFFER_RADIUS, quadrant_segments=BUFFER_QUADRANT_SEGMENTS
    )


def gometry_buffer_polygons_dilate() -> gm.GeometryArray:
    return BUFFER_POLYGON_INPUTS.buffer(
        BUFFER_RADIUS, quadrant_segments=BUFFER_QUADRANT_SEGMENTS
    )


def gometry_buffer_polygons_erosion() -> gm.GeometryArray:
    return BUFFER_POLYGON_INPUTS.buffer(
        -BUFFER_RADIUS, quadrant_segments=BUFFER_QUADRANT_SEGMENTS
    )


def gometry_buffer_lines() -> gm.GeometryArray:
    return PACKED_LINES_PLANAR_1K.buffer(
        BUFFER_RADIUS, quadrant_segments=BUFFER_QUADRANT_SEGMENTS
    )


def gometry_distance_pairwise() -> np.ndarray:
    result = gm.distance(CRS_POINTS, CRS_POINTS_B)
    assert isinstance(result, np.ndarray)
    return result


def gometry_length_lines() -> np.ndarray:
    result = PACKED_LINES_PLANAR_1K.length
    assert isinstance(result, np.ndarray)
    return result


def gometry_area_polygons() -> np.ndarray:
    result = BUFFER_POLYGON_INPUTS.area
    assert isinstance(result, np.ndarray)
    return result


def gometry_union_all_overlap() -> gm.Geometry:
    return UNION_ALL_OVERLAP_DISKS.union_all()


def gometry_union_pairwise() -> gm.GeometryArray:
    return gm.union(PLANAR_OVERLAY_LEFT, PLANAR_OVERLAY_RIGHT)


def gometry_symmetric_difference_pairwise() -> gm.GeometryArray:
    return gm.symmetric_difference(PLANAR_OVERLAY_LEFT, PLANAR_OVERLAY_RIGHT)


def gometry_intersection_all_overlap() -> gm.Geometry:
    return INTERSECTION_ALL_POLYGONS.intersection_all()


def gometry_nearest_m() -> list[int]:
    return gm.nearest(POINTS, gm.Point(0, 0, crs=4326), k=10, unit='meters')


def gometry_to_wkb_batch() -> list[bytes]:
    return TEXT_POINTS.to_wkb()


def gometry_to_arrow_roundtrip() -> gm.GeometryArray:
    return gm.from_arrow(TEXT_POINTS.to_arrow())


def gometry_from_arrow_roundtrip() -> gm.GeometryArray:
    arrow = TEXT_POINTS.to_arrow()
    return gm.from_arrow(arrow)


def gometry_to_polyline() -> list[str]:
    return [geometry.to_polyline() for geometry in PACKED_LINES_1K]


def gometry_from_polyline() -> gm.GeometryArray:
    encoded = [geometry.to_polyline() for geometry in PACKED_LINES_1K]
    return gm.GeometryArray([gm.from_polyline(value) for value in encoded], crs=4326)


def gometry_scale_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.scale(2.0, 2.0)


def gometry_skew_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.skew(5.0, 0.0)


def gometry_translate_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.translate(1.0, 1.0)


def gometry_affine_transform_packed_lines() -> gm.GeometryArray:
    return PACKED_LINES_20K.affine_transform([1.0, 0.0, 0.0, 0.0, 1.0, 0.0])


def gometry_relate_1k() -> list[str]:
    return gm.relate(RELATE_TARGETS, POLYGON)


def gometry_relate_pattern_1k() -> np.ndarray:
    result = gm.relate_pattern(RELATE_TARGETS, POLYGON, RELATE_PATTERN)
    assert isinstance(result, np.ndarray)
    return result


def gometry_is_valid_10k() -> np.ndarray:
    result = POINTS.is_valid
    assert isinstance(result, np.ndarray)
    return result


def gometry_repair_1k() -> gm.GeometryArray:
    return INVALID_INPUTS.repair()


def gometry_h3_boundary_10k() -> gm.GeometryArray:
    return H3_CELLS.polygon


def gometry_h3_to_polygon_10k() -> gm.Geometry:
    return H3_CELLS[:64].to_polygon()


def gometry_h3_compact_10k() -> gm.CellArray:
    return H3_CELLS.compact()


def gometry_s2_boundary_10k() -> gm.GeometryArray:
    return S2_CELLS.polygon


def gometry_s2_to_polygon_10k() -> gm.Geometry:
    return S2_CELLS[:64].to_polygon()


def gometry_geohash_cell_10k() -> list[object]:
    return GEOHASH_CELLS


def gometry_geohash_to_polygon_10k() -> gm.Geometry:
    return GEOHASH_CELLS[:64].to_polygon()


def gometry_tiles_cell_10k() -> list[object]:
    return TILE_CELLS


def gometry_tiles_to_polygon_10k() -> gm.Geometry:
    return TILE_CELLS[:64].to_polygon()


def gometry_minimum_bounding_circle_1k() -> gm.GeometryArray:
    return gm.GeometryArray(SUMMARY_INPUTS).minimum_bounding_circle()


def gometry_minimum_clearance_line_1k() -> gm.GeometryArray:
    return gm.GeometryArray(SUMMARY_INPUTS).minimum_clearance_line()


def gometry_maximum_inscribed_circle_1k() -> gm.GeometryArray:
    return gm.GeometryArray(POLYLABEL_INPUTS).maximum_inscribed_circle()


def gometry_real_world_from_geojson() -> gm.GeometryArray:
    return gm.from_geojson(REAL_WORLD_GEOJSON)


def gometry_real_world_bounds_cold() -> tuple[float, float, float, float] | None:
    return gm.from_geojson(REAL_WORLD_GEOJSON).total_bounds


def gometry_real_world_bounds_warm() -> tuple[float, float, float, float] | None:
    return REAL_WORLD_GEOMETRY.total_bounds


def gometry_real_world_area_cold() -> float:
    return float(gm.area(gm.from_geojson(REAL_WORLD_GEOJSON), unit='planar').sum())


def gometry_real_world_area_warm() -> float:
    return float(gm.area(REAL_WORLD_GEOMETRY, unit='planar').sum())


def gometry_real_world_point_on_surface() -> gm.GeometryArray:
    return REAL_WORLD_GEOMETRY.point_on_surface()


def _register_shapely(runner: pyperf.Runner) -> None:
    import geopandas as gpd
    import shapely
    from shapely import (
        STRtree,
        area,
        boundary,
        buffer,
        concave_hull,
        constrained_delaunay_triangles,
        contains_xy,
        convex_hull,
        covered_by,
        covers,
        crosses,
        delaunay_triangles,
        difference,
        disjoint,
        distance,
        dwithin,
        envelope,
        frechet_distance,
        from_geojson,
        from_wkb,
        from_wkt,
        hausdorff_distance,
        intersection,
        intersection_all,
        intersects,
        is_simple,
        is_valid,
        length,
        line_interpolate_point,
        line_locate_point,
        make_valid,
        maximum_inscribed_circle,
        minimum_bounding_circle,
        minimum_clearance,
        minimum_clearance_line,
        minimum_rotated_rectangle,
        normalize,
        offset_curve,
        orient_polygons,
        overlaps,
        point_on_surface,
        prepare,
        relate,
        relate_pattern,
        remove_repeated_points,
        reverse,
        segmentize,
        shared_paths,
        simplify,
        snap,
        symmetric_difference,
        to_geojson,
        to_wkb,
        to_wkt,
        touches,
        union,
        union_all,
        voronoi_polygons,
        within,
    )
    from shapely.affinity import affine_transform as affinity_affine_transform
    from shapely.affinity import rotate as affinity_rotate
    from shapely.affinity import scale as affinity_scale
    from shapely.affinity import skew as affinity_skew
    from shapely.affinity import translate as affinity_translate
    from shapely.geometry import (
        LineString,
        MultiLineString,
        MultiPoint,
        Point,
        Polygon,
        box,
    )
    from shapely.ops import (
        clip_by_rect,
        linemerge,
        nearest_points,
        polygonize,
        polygonize_full,
        polylabel,
        split,
        substring,
    )
    from shapely.wkb import dumps

    points = shapely.points(XS, YS)
    polygon = box(-9.0, -9.0, 9.0, 9.0)
    shapely_geoseries = gpd.GeoSeries(points, crs='EPSG:4326')
    mixed_geometries = tuple(
        Point(float(i), float(i % 97))
        if i % 3 == 0
        else LineString([(float(i), 0.0), (float(i) + 0.5, 1.0)])
        if i % 3 == 1
        else box(float(i), 0.0, float(i) + 0.25, 0.25)
        for i in range(POINT_COUNT)
    )
    wkb_points = tuple(
        (
            dumps(Point(x, y), srid=4326)
            for x, y in zip(XS[:WKB_COUNT], YS[:WKB_COUNT], strict=False)
        )
    )
    text_points = tuple(
        (Point(x, y) for x, y in zip(XS[:TEXT_COUNT], YS[:TEXT_COUNT], strict=False))
    )
    wkt_points = to_wkt(text_points)
    geojson_points = to_geojson(text_points)
    line_merge_inputs = tuple(
        MultiLineString([
            [(0.0, float(i)), (1.0, float(i))],
            [(2.0, float(i)), (1.0, float(i))],
        ])
        for i in range(LINE_MERGE_COUNT)
    )
    clip_inputs = tuple(
        box(-1.0 + i * 0.001, -1.0, 2.0 + i * 0.001, 2.0) for i in range(CLIP_COUNT)
    )
    line_ref_inputs = tuple(
        LineString([(0.0, float(i)), (3.0, float(i + 4)), (6.0, float(i + 4))])
        for i in range(LINE_REF_COUNT)
    )
    line_ref_point = Point(4.0, 5.0)
    line_ref_points = tuple(Point(4.0, float(i) + 5.0) for i in range(LINE_REF_COUNT))
    overlay_left = tuple(
        box(float(i), 0.0, float(i) + 2.0, 2.0) for i in range(OVERLAY_COUNT)
    )
    overlay_right = tuple(
        box(float(i) + 1.0, 1.0, float(i) + 3.0, 3.0) for i in range(OVERLAY_COUNT)
    )
    split_inputs = tuple(
        LineString([(0.0, float(i)), (4.0, float(i))]) for i in range(SPLIT_COUNT)
    )
    split_points = tuple(
        MultiPoint([(1.0, float(i)), (3.0, float(i))]) for i in range(SPLIT_COUNT)
    )
    offset_inputs = tuple(
        LineString([(0.0, offset), (2.0, offset), (2.0, offset + 2.0)])
        for offset in (float(i) for i in range(OFFSET_COUNT))
    )
    shared_paths_inputs = tuple(
        LineString([(0.0, offset), (2.0, offset)])
        for offset in (float(i) for i in range(SHARED_PATHS_COUNT))
    )
    shared_paths_references = tuple(
        LineString([(1.0, offset), (3.0, offset)])
        for offset in (float(i) for i in range(SHARED_PATHS_COUNT))
    )
    hull_inputs = tuple(
        MultiPoint([
            (0.0, offset),
            (4.0, offset),
            (4.0, offset + 4.0),
            (2.0, offset + 1.0),
            (0.0, offset + 4.0),
            (1.0, offset + 2.0),
            (3.0, offset + 2.0),
        ])
        for offset in (float(i) for i in range(HULL_COUNT))
    )
    polylabel_inputs = tuple(
        box(0.0, offset, 4.0, offset + 2.0)
        for offset in (float(i) for i in range(POLYLABEL_COUNT))
    )
    summary_inputs = tuple(
        Polygon([
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 1.0 + i * 0.001),
            (0.0, 1.0),
            (0.0, 0.0),
        ])
        for i in range(SUMMARY_COUNT)
    )
    similarity_inputs = tuple(
        LineString([(0.0, float(i)), (3.0, float(i + 4)), (6.0, float(i + 4))])
        for i in range(SIMILARITY_COUNT)
    )
    similarity_target = LineString([(0.0, 1.0), (3.0, 5.0), (6.0, 5.0)])
    cleanup_inputs = tuple(
        LineString([(0.0, float(i)), (0.0, float(i)), (2.0, float(i))])
        for i in range(CLEANUP_COUNT)
    )

    def _shapely_planar_index_polygon(i: int) -> Polygon:
        row = i // 100
        col = i % 100
        base_x = 530000.0 + col * 1000.0
        base_y = 180000.0 + row * 1000.0
        return box(base_x, base_y, base_x + 500.0, base_y + 500.0)

    planar_polygons = tuple(
        _shapely_planar_index_polygon(i) for i in range(INDEX_POLYGON_COUNT)
    )
    planar_polygon = box(520000.0, 170000.0, 550000.0, 200000.0)
    planar_points = shapely.points(NP_CRS_XS, NP_CRS_YS)
    planar_points_b = shapely.points(NP_CRS_XS + 250.0, NP_CRS_YS + 250.0)
    planar_query_boxes = tuple(
        box(CRS_XS[i], CRS_YS[i], CRS_XS[i] + 1000.0, CRS_YS[i] + 1000.0)
        for i in range(QUERY_BOX_COUNT)
    )
    planar_query_point = Point(535000.0, 185000.0)
    planar_tree = STRtree(planar_polygons)
    planar_prepared = from_wkb(to_wkb(planar_polygon))
    prepare(planar_prepared)
    buffer_points = planar_points[:BUFFER_COUNT]
    buffer_polygon_inputs = tuple(
        box(530000.0 + float(i) * 10.0, 180000.0, 530500.0 + float(i) * 10.0, 180500.0)
        for i in range(BUFFER_COUNT)
    )
    planar_overlay_left = tuple(
        box(530000.0 + float(i), 180000.0, 530000.0 + float(i) + 2000.0, 182000.0)
        for i in range(OVERLAY_COUNT)
    )
    planar_overlay_right = tuple(
        box(
            530000.0 + float(i) + 1000.0,
            181000.0,
            530000.0 + float(i) + 3000.0,
            183000.0,
        )
        for i in range(OVERLAY_COUNT)
    )
    union_all_overlap_disks = buffer(
        shapely.points(UNION_ALL_DISK_XS, UNION_ALL_DISK_YS),
        _UNION_ALL_DISK_RADIUS,
        quad_segs=8,
    )

    def _strtree_k_nearest(
        tree: STRtree, query: object, geoms: tuple[object, ...]
    ) -> object:
        candidates = tree.query(query, predicate='dwithin', distance=50000.0)
        if len(candidates) == 0:
            return candidates
        distances = distance(query, [geoms[int(index)] for index in candidates])
        order = np.argsort(distances)[:NEAREST_K]
        return candidates[order]

    packed_lines_20k = tuple(
        LineString(_wiggly_line_coords(i)) for i in range(PACKED_LINES_COUNT)
    )
    packed_lines_1k = packed_lines_20k[:PACKED_LINES_1K_COUNT]
    packed_lines_planar_1k = tuple(
        LineString(_wiggly_line_coords_planar(i)) for i in range(PACKED_LINES_1K_COUNT)
    )
    packed_polygons_1k = tuple(
        box(0.1, 0.1, 0.9, 0.9)
        if i % 2 == 0
        else Polygon([(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)])
        for i in range(PACKED_POLYGON_COUNT)
    )
    snap_inputs = tuple(
        LineString([(0.0, offset), (0.9, offset + 0.1), (2.0, offset)])
        for offset in (float(i) for i in range(SNAP_COUNT))
    )
    snap_points = tuple(Point(1.0, float(i)) for i in range(SNAP_COUNT))
    order_inputs = tuple(
        Polygon([(0.0, 0.0), (0.0, 1.0 + i * 0.001), (2.0, 1.0), (0.0, 0.0)])
        for i in range(ORDER_COUNT)
    )
    structural_inputs = tuple(
        LineString([
            (0.0, float(i)),
            (1.0, float(i + 1)),
            (1.0, float(i)),
            (0.0, float(i + 1)),
        ])
        for i in range(STRUCTURAL_COUNT)
    )
    triangulation_inputs = tuple(
        MultiPoint([
            (0.0, float(i)),
            (1.0, float(i)),
            (0.0, float(i + 1)),
            (1.0, float(i + 1)),
        ])
        for i in range(TRIANGULATION_COUNT)
    )
    constrained_triangulation_inputs = tuple(
        Polygon(
            [
                (0.0, offset),
                (2.0, offset),
                (2.0, offset + 2.0),
                (0.0, offset + 2.0),
                (0.0, offset),
            ],
            holes=[
                [
                    (0.75, offset + 0.75),
                    (1.25, offset + 0.75),
                    (1.25, offset + 1.25),
                    (0.75, offset + 1.25),
                    (0.75, offset + 0.75),
                ]
            ],
        )
        for offset in (float(i) for i in range(TRIANGULATION_COUNT))
    )
    polygon_triangulation_inputs = tuple(
        Polygon([
            (0.0, offset),
            (3.0, offset),
            (3.0, offset + 1.0),
            (1.0, offset + 1.0),
            (1.0, offset + 3.0),
            (0.0, offset + 3.0),
            (0.0, offset),
        ])
        for offset in (float(i) for i in range(TRIANGULATION_COUNT))
    )
    polygonize_inputs = tuple(
        MultiLineString([
            [(0.0, offset), (1.0, offset)],
            [(1.0, offset), (1.0, offset + 1.0)],
            [(1.0, offset + 1.0), (0.0, offset + 1.0)],
            [(0.0, offset + 1.0), (0.0, offset)],
            [(2.0, offset), (3.0, offset + 1.0)],
        ])
        for offset in (float(i) for i in range(POLYGONIZE_COUNT))
    )
    real_world_geometry = shapely.from_geojson(REAL_WORLD_GEOJSON)
    real_world_parts = tuple(real_world_geometry.geoms)

    def shapely_real_world_from_geojson() -> object:
        return shapely.from_geojson(REAL_WORLD_GEOJSON)

    def shapely_real_world_bounds_cold() -> tuple[float, float, float, float]:
        return shapely.from_geojson(REAL_WORLD_GEOJSON).bounds

    def shapely_real_world_bounds_warm() -> tuple[float, float, float, float]:
        return real_world_geometry.bounds

    def shapely_real_world_area_cold() -> float:
        return float(shapely.area(shapely.from_geojson(REAL_WORLD_GEOJSON)))

    def shapely_real_world_area_warm() -> float:
        return float(shapely.area(real_world_geometry))

    def shapely_real_world_point_on_surface() -> object:
        return point_on_surface(real_world_parts)

    def shapely_points() -> object:
        return shapely.points(XS, YS)

    def shapely_contains() -> object:
        return shapely.contains(polygon, points)

    def shapely_contains_xy() -> object:
        return contains_xy(polygon, XS, YS)

    def shapely_index_build() -> object:
        return STRtree(planar_polygons)

    def shapely_index_query() -> list[object]:
        return [planar_tree.query(query_box) for query_box in planar_query_boxes]

    def shapely_index_nearest_k10_planar() -> object:
        return _strtree_k_nearest(planar_tree, planar_query_point, planar_polygons)

    def shapely_dwithin_pairwise() -> object:
        return dwithin(planar_points, planar_points_b, DWITHIN_DISTANCE)

    def shapely_prepared_contains_polygon_points() -> object:
        return shapely.contains(planar_prepared, planar_points)

    def shapely_intersects_polygon_points() -> object:
        return intersects(planar_polygon, planar_points)

    def shapely_within_polygon_points() -> object:
        return within(planar_points, planar_polygon)

    def shapely_touches_polygon_points() -> object:
        return touches(planar_polygon, planar_points)

    def shapely_crosses_polygon_points() -> object:
        return crosses(planar_polygon, planar_points)

    def shapely_overlaps_polygon_points() -> object:
        return overlaps(planar_polygon, planar_points)

    def shapely_disjoint_polygon_points() -> object:
        return disjoint(planar_polygon, planar_points)

    def shapely_covers_polygon_points() -> object:
        return covers(planar_polygon, planar_points)

    def shapely_covered_by_polygon_points() -> object:
        return covered_by(planar_points, planar_polygon)

    def shapely_buffer_points() -> object:
        return buffer(buffer_points, BUFFER_RADIUS, quad_segs=BUFFER_QUADRANT_SEGMENTS)

    def shapely_buffer_polygons_dilate() -> object:
        return buffer(
            buffer_polygon_inputs, BUFFER_RADIUS, quad_segs=BUFFER_QUADRANT_SEGMENTS
        )

    def shapely_buffer_polygons_erosion() -> object:
        return buffer(
            buffer_polygon_inputs, -BUFFER_RADIUS, quad_segs=BUFFER_QUADRANT_SEGMENTS
        )

    def shapely_buffer_lines() -> object:
        return buffer(
            packed_lines_planar_1k, BUFFER_RADIUS, quad_segs=BUFFER_QUADRANT_SEGMENTS
        )

    def shapely_distance_pairwise() -> object:
        return distance(planar_points, planar_points_b)

    def shapely_length_lines() -> object:
        return length(packed_lines_planar_1k)

    def shapely_area_polygons() -> object:
        return area(buffer_polygon_inputs)

    def shapely_union_all_overlap() -> object:
        return union_all(union_all_overlap_disks)

    def shapely_union_pairwise() -> object:
        return union(planar_overlay_left, planar_overlay_right)

    def shapely_symmetric_difference_pairwise() -> object:
        return symmetric_difference(planar_overlay_left, planar_overlay_right)

    def shapely_intersection_all_overlap() -> object:
        return intersection_all(planar_overlay_left)

    def shapely_from_wkb() -> list[object]:
        return [from_wkb(value) for value in wkb_points]

    def shapely_from_wkb_batch() -> object:
        return from_wkb(wkb_points)

    def shapely_to_wkt_batch() -> object:
        return to_wkt(text_points)

    def shapely_from_wkt_batch() -> object:
        return from_wkt(wkt_points)

    def shapely_to_geojson_batch() -> object:
        return to_geojson(text_points)

    def shapely_from_geojson_batch() -> object:
        return from_geojson(geojson_points)

    def shapely_line_merge() -> list[object]:
        return [linemerge(value) for value in line_merge_inputs]

    def shapely_clip_by_rect() -> list[object]:
        return [clip_by_rect(value, 0.0, 0.0, 1.0, 1.0) for value in clip_inputs]

    def shapely_line_interpolate_point() -> object:
        return line_interpolate_point(line_ref_inputs, 6.0)

    def shapely_line_substring() -> list[object]:
        return [substring(value, 2.0, 6.0) for value in line_ref_inputs]

    def shapely_line_locate_point() -> object:
        return line_locate_point(line_ref_inputs, line_ref_point)

    def shapely_line_locate_point_pairwise() -> object:
        return line_locate_point(line_ref_inputs, line_ref_points)

    def shapely_intersection_pairwise() -> object:
        return intersection(overlay_left, overlay_right)

    def shapely_difference_pairwise() -> object:
        return difference(overlay_left, overlay_right)

    def shapely_split() -> list[object]:
        return [
            split(geometry, splitter)
            for geometry, splitter in zip(split_inputs, split_points, strict=False)
        ]

    def shapely_shared_paths() -> list[object]:
        return [
            shared_paths(geometry, reference)
            for geometry, reference in zip(
                shared_paths_inputs, shared_paths_references, strict=False
            )
        ]

    def shapely_offset_curve() -> list[object]:
        return [offset_curve(value, 0.5) for value in offset_inputs]

    def shapely_centroid() -> list[object]:
        return [value.centroid for value in summary_inputs]

    def shapely_point_on_surface() -> list[object]:
        return [point_on_surface(value) for value in summary_inputs]

    def shapely_envelope() -> list[object]:
        return [envelope(value) for value in summary_inputs]

    def shapely_convex_hull() -> list[object]:
        return [convex_hull(value) for value in summary_inputs]

    def shapely_concave_hull() -> list[object]:
        return [concave_hull(value, ratio=0.5) for value in hull_inputs]

    def shapely_maximum_inscribed_circle_filled() -> list[object]:
        disks = []
        for value in polylabel_inputs:
            center, witness = maximum_inscribed_circle(value, tolerance=0.01).coords
            radius = math.hypot(center[0] - witness[0], center[1] - witness[1])
            disks.append(buffer(Point(center), radius, quad_segs=16))
        return disks

    def shapely_polylabel() -> list[object]:
        return [polylabel(value, tolerance=0.01) for value in polylabel_inputs]

    def shapely_minimum_rotated_rectangle() -> list[object]:
        return [minimum_rotated_rectangle(value) for value in summary_inputs]

    def shapely_boundary() -> list[object]:
        return [boundary(value) for value in summary_inputs]

    def shapely_remove_repeated_points() -> list[object]:
        return [remove_repeated_points(value) for value in cleanup_inputs]

    def shapely_segmentize() -> list[object]:
        return [segmentize(value, 0.5) for value in cleanup_inputs]

    def shapely_centroid_packed_lines() -> list[object]:
        return [value.centroid for value in packed_lines_20k]

    def shapely_rotate_packed_lines() -> list[object]:
        return [
            affinity_rotate(value, 45.0, origin='centroid')
            for value in packed_lines_20k
        ]

    def shapely_segmentize_packed_lines() -> list[object]:
        return [segmentize(value, 0.5) for value in packed_lines_1k]

    def shapely_densify_packed_lines() -> list[object]:
        return [segmentize(value, DENSIFY_MAX_SEGMENT) for value in packed_lines_1k]

    def shapely_concat_packed_polygons_2x1k() -> list[object]:
        return list(packed_polygons_1k) + list(packed_polygons_1k)

    def shapely_filter_packed_polygons_1k() -> list[object]:
        return [
            geometry
            for geometry, keep in zip(
                packed_polygons_1k, PACKED_POLYGONS_FILTER_MASK, strict=False
            )
            if keep
        ]

    def shapely_simplify_packed_lines() -> list[object]:
        return [
            simplify(value, 0.5, preserve_topology=False) for value in packed_lines_1k
        ]

    def shapely_snap() -> list[object]:
        return [
            snap(geometry, reference, 0.25)
            for geometry, reference in zip(snap_inputs, snap_points, strict=False)
        ]

    def shapely_hausdorff_distance() -> list[float]:
        return [
            hausdorff_distance(value, similarity_target) for value in similarity_inputs
        ]

    def shapely_hausdorff_distance_packed_lines() -> list[float]:
        return [
            hausdorff_distance(left, right)
            for left, right in zip(
                packed_lines_planar_1k, packed_lines_planar_1k, strict=False
            )
        ]

    def shapely_hausdorff_distance_packed_lines_cross() -> list[float]:
        return [
            hausdorff_distance(left, right)
            for left, right in zip(
                packed_lines_planar_1k,
                packed_lines_planar_1k[1:] + packed_lines_planar_1k[:1],
                strict=False,
            )
        ]

    def shapely_hausdorff_distance_geographic() -> list[float]:
        return [
            hausdorff_distance(left, right)
            for left, right in zip(packed_lines_1k, packed_lines_1k, strict=False)
        ]

    def shapely_frechet_distance() -> list[float]:
        return [
            frechet_distance(value, similarity_target) for value in similarity_inputs
        ]

    def shapely_frechet_distance_packed_lines() -> list[float]:
        return [
            frechet_distance(left, right)
            for left, right in zip(
                packed_lines_planar_1k, packed_lines_planar_1k, strict=False
            )
        ]

    def shapely_nearest_points() -> list[tuple[object, object]]:
        return [nearest_points(value, line_ref_point) for value in line_ref_inputs]

    def shapely_reverse() -> list[object]:
        return [reverse(value) for value in order_inputs]

    def shapely_orient_polygons() -> list[object]:
        return [orient_polygons(value) for value in order_inputs]

    def shapely_normalize() -> list[object]:
        return [normalize(value) for value in order_inputs]

    def shapely_is_simple() -> list[bool]:
        return [bool(is_simple(value)) for value in structural_inputs]

    def shapely_minimum_clearance() -> list[float]:
        return [minimum_clearance(value) for value in structural_inputs]

    invalid_inputs = tuple(
        Polygon([
            (0.0, float(i)),
            (1.0, float(i + 1)),
            (1.0, float(i)),
            (0.0, float(i + 1)),
            (0.0, float(i)),
        ])
        for i in range(INVALID_COUNT)
    )

    def shapely_to_wkb_batch() -> object:
        return to_wkb(text_points)

    def shapely_to_wkb_mixed() -> object:
        return to_wkb(mixed_geometries)

    def shapely_from_geopandas_geometry_array() -> object:
        return np.asarray(shapely_geoseries.array)

    def shapely_from_arrow_roundtrip() -> object:
        from shapely import from_ragged_array, to_ragged_array

        packed = to_ragged_array(text_points)
        return from_ragged_array(packed[0], packed[1])

    def shapely_affine_scale_packed_lines() -> list[object]:
        return [
            affinity_scale(value, xfact=2.0, yfact=2.0) for value in packed_lines_20k
        ]

    def shapely_affine_skew_packed_lines() -> list[object]:
        return [affinity_skew(value, xs=5.0, ys=0.0) for value in packed_lines_20k]

    def shapely_affine_translate_packed_lines() -> list[object]:
        return [
            affinity_translate(value, xoff=1.0, yoff=1.0) for value in packed_lines_20k
        ]

    def shapely_affine_affine_transform_packed_lines() -> list[object]:
        return [
            affinity_affine_transform(value, [1.0, 0.0, 0.0, 0.0, 1.0, 0.0])
            for value in packed_lines_20k
        ]

    def shapely_relate_1k() -> list[str]:
        return [relate(value, polygon) for value in structural_inputs]

    def shapely_relate_pattern_1k() -> list[bool]:
        return [
            relate_pattern(value, polygon, RELATE_PATTERN)
            for value in structural_inputs
        ]

    def shapely_is_valid_10k() -> list[bool]:
        return [bool(is_valid(value)) for value in points]

    def shapely_make_valid_1k() -> list[object]:
        return [make_valid(value) for value in invalid_inputs]

    def shapely_minimum_bounding_circle_1k() -> list[object]:
        return [minimum_bounding_circle(value) for value in summary_inputs]

    def shapely_minimum_clearance_line_1k() -> list[object]:
        return [minimum_clearance_line(value) for value in summary_inputs]

    def shapely_delaunay_triangles() -> list[object]:
        return [delaunay_triangles(value) for value in triangulation_inputs]

    def shapely_constrained_delaunay_triangles() -> list[object]:
        return [
            constrained_delaunay_triangles(value)
            for value in constrained_triangulation_inputs
        ]

    def shapely_polygon_triangles() -> list[object]:
        return [
            constrained_delaunay_triangles(value)
            for value in polygon_triangulation_inputs
        ]

    def shapely_voronoi_polygons() -> list[object]:
        return [
            [
                intersection(cell, value.envelope)
                for cell in voronoi_polygons(value, extend_to=value.envelope).geoms
            ]
            for value in triangulation_inputs
        ]

    def shapely_voronoi_edges() -> list[object]:
        return [
            [
                clipped
                for edge in voronoi_polygons(
                    value, extend_to=value.envelope, only_edges=True
                ).geoms
                if not (clipped := intersection(edge, value.envelope)).is_empty
            ]
            for value in triangulation_inputs
        ]

    def shapely_polygonize() -> list[object]:
        return [list(polygonize(value)) for value in polygonize_inputs]

    def shapely_polygonize_full() -> list[object]:
        return [polygonize_full(value) for value in polygonize_inputs]

    runner.bench_func('shapely.points/10k', _checked('shapely.points', shapely_points))
    runner.bench_func(
        'shapely.contains/polygon_points_10k',
        _checked('shapely.contains', shapely_contains),
    )
    runner.bench_func(
        'shapely.contains_xy/polygon_points_10k',
        _checked('shapely.contains_xy', shapely_contains_xy),
    )
    runner.bench_func(
        'shapely.from_wkb/1k', _checked('shapely.from_wkb', shapely_from_wkb)
    )
    runner.bench_func(
        'shapely.from_wkb.batch/1k',
        _checked('shapely.from_wkb', shapely_from_wkb_batch),
    )
    runner.bench_func(
        'shapely.to_wkb.batch/1k', _checked('shapely.to_wkb', shapely_to_wkb_batch)
    )
    runner.bench_func(
        'shapely.to_wkb.mixed/10k',
        _checked('shapely.to_wkb_mixed_10k', shapely_to_wkb_mixed),
    )
    runner.bench_func(
        'shapely.from_geopandas.geometry_array/10k',
        _checked(
            'shapely.from_geopandas_geometry_array_10k',
            shapely_from_geopandas_geometry_array,
        ),
    )
    runner.bench_func(
        'shapely.from_arrow.roundtrip/1k',
        _checked('shapely.from_arrow_roundtrip', shapely_from_arrow_roundtrip),
    )
    runner.bench_func(
        'shapely.to_wkt.batch/1k', _checked('shapely.to_wkt', shapely_to_wkt_batch)
    )
    runner.bench_func(
        'shapely.from_wkt.batch/1k',
        _checked('shapely.from_wkt', shapely_from_wkt_batch),
    )
    runner.bench_func(
        'shapely.to_geojson.batch/1k',
        _checked('shapely.to_geojson', shapely_to_geojson_batch),
    )
    runner.bench_func(
        'shapely.from_geojson.batch/1k',
        _checked('shapely.from_geojson', shapely_from_geojson_batch),
    )
    runner.bench_func(
        'shapely.line_merge/1k', _checked('shapely.line_merge', shapely_line_merge)
    )
    runner.bench_func(
        'shapely.clip_by_rect/1k',
        _checked('shapely.clip_by_rect', shapely_clip_by_rect),
    )
    runner.bench_func(
        'shapely.line_interpolate_point/1k',
        _checked('shapely.line_interpolate_point', shapely_line_interpolate_point),
    )
    runner.bench_func(
        'shapely.line_substring/1k',
        _checked('shapely.line_substring', shapely_line_substring),
    )
    runner.bench_func(
        'shapely.line_locate_point/1k',
        _checked('shapely.line_locate_point', shapely_line_locate_point),
    )
    runner.bench_func(
        'shapely.line_locate_point_pairwise/1k',
        _checked(
            'shapely.line_locate_point_pairwise', shapely_line_locate_point_pairwise
        ),
    )
    runner.bench_func(
        'shapely.intersection_pairwise/1k',
        _checked('shapely.intersection_pairwise', shapely_intersection_pairwise),
    )
    runner.bench_func(
        'shapely.difference_pairwise/1k',
        _checked('shapely.difference_pairwise', shapely_difference_pairwise),
    )
    runner.bench_func('shapely.split/1k', _checked('shapely.split', shapely_split))
    runner.bench_func(
        'shapely.offset_curve/1k',
        _checked('shapely.offset_curve', shapely_offset_curve),
    )
    runner.bench_func(
        'shapely.shared_paths/1k',
        _checked('shapely.shared_paths', shapely_shared_paths),
    )
    runner.bench_func(
        'shapely.centroid/1k', _checked('shapely.centroid', shapely_centroid)
    )
    runner.bench_func(
        'shapely.point_on_surface/1k',
        _checked('shapely.point_on_surface', shapely_point_on_surface),
    )
    runner.bench_func(
        'shapely.envelope/1k', _checked('shapely.envelope', shapely_envelope)
    )
    runner.bench_func(
        'shapely.convex_hull/1k', _checked('shapely.convex_hull', shapely_convex_hull)
    )
    runner.bench_func(
        'shapely.concave_hull/1k',
        _checked('shapely.concave_hull', shapely_concave_hull),
    )
    runner.bench_func(
        'shapely.maximum_inscribed_circle/1k',
        _checked(
            'shapely.maximum_inscribed_circle_filled',
            shapely_maximum_inscribed_circle_filled,
        ),
    )
    runner.bench_func(
        'shapely.polylabel/1k', _checked('shapely.polylabel', shapely_polylabel)
    )
    runner.bench_func(
        'shapely.minimum_rotated_rectangle/1k',
        _checked(
            'shapely.minimum_rotated_rectangle', shapely_minimum_rotated_rectangle
        ),
    )
    runner.bench_func(
        'shapely.boundary/1k', _checked('shapely.boundary', shapely_boundary)
    )
    runner.bench_func(
        'shapely.remove_repeated_points/1k',
        _checked('shapely.remove_repeated_points', shapely_remove_repeated_points),
    )
    runner.bench_func(
        'shapely.segmentize/1k', _checked('shapely.segmentize', shapely_segmentize)
    )
    runner.bench_func(
        'shapely.centroid.packed_lines/20k',
        _checked('shapely.centroid_packed_lines_20k', shapely_centroid_packed_lines),
    )
    runner.bench_func(
        'shapely.rotate.packed_lines/20k',
        _checked('shapely.rotate_packed_lines_20k', shapely_rotate_packed_lines),
    )
    runner.bench_func(
        'shapely.affine.scale.packed_lines/20k',
        _checked(
            'shapely.affine_scale_packed_lines_20k', shapely_affine_scale_packed_lines
        ),
    )
    runner.bench_func(
        'shapely.affine.skew.packed_lines/20k',
        _checked(
            'shapely.affine_skew_packed_lines_20k', shapely_affine_skew_packed_lines
        ),
    )
    runner.bench_func(
        'shapely.affine.translate.packed_lines/20k',
        _checked(
            'shapely.affine_translate_packed_lines_20k',
            shapely_affine_translate_packed_lines,
        ),
    )
    runner.bench_func(
        'shapely.affine.affine_transform.packed_lines/20k',
        _checked(
            'shapely.affine_affine_transform_packed_lines_20k',
            shapely_affine_affine_transform_packed_lines,
        ),
    )
    runner.bench_func(
        'shapely.segmentize.packed_lines/1k',
        _checked('shapely.segmentize_packed_lines_1k', shapely_segmentize_packed_lines),
    )
    runner.bench_func(
        'shapely.densify.packed_lines/1k',
        _checked('shapely.densify_packed_lines_1k', shapely_densify_packed_lines),
    )
    runner.bench_func(
        'shapely.concat/packed_polygons_2x1k',
        _checked(
            'shapely.concat_packed_polygons_2x1k', shapely_concat_packed_polygons_2x1k
        ),
    )
    runner.bench_func(
        'shapely.filter/packed_polygons_1k',
        _checked(
            'shapely.filter_packed_polygons_1k', shapely_filter_packed_polygons_1k
        ),
    )
    runner.bench_func(
        'shapely.simplify.packed_lines/1k',
        _checked('shapely.simplify_packed_lines_1k', shapely_simplify_packed_lines),
    )
    runner.bench_func('shapely.snap/1k', _checked('shapely.snap', shapely_snap))
    runner.bench_func(
        'shapely.hausdorff_distance/1k',
        _checked('shapely.hausdorff_distance', shapely_hausdorff_distance),
    )
    runner.bench_func(
        'shapely.hausdorff_distance.packed_lines/1k',
        _checked(
            'shapely.hausdorff_distance_packed_lines_1k',
            shapely_hausdorff_distance_packed_lines,
        ),
    )
    runner.bench_func(
        'shapely.hausdorff_distance.packed_lines_cross/1k',
        _checked(
            'shapely.hausdorff_distance_packed_lines_cross_1k',
            shapely_hausdorff_distance_packed_lines_cross,
        ),
    )
    runner.bench_func(
        'shapely.hausdorff_distance.geographic/1k',
        _checked(
            'shapely.hausdorff_distance_geographic_1k',
            shapely_hausdorff_distance_geographic,
        ),
    )
    runner.bench_func(
        'shapely.frechet_distance/1k',
        _checked('shapely.frechet_distance', shapely_frechet_distance),
    )
    runner.bench_func(
        'shapely.frechet_distance.packed_lines/1k',
        _checked(
            'shapely.frechet_distance_packed_lines_1k',
            shapely_frechet_distance_packed_lines,
        ),
    )
    runner.bench_func(
        'shapely.nearest_points/1k',
        _checked('shapely.nearest_points', shapely_nearest_points),
    )
    runner.bench_func(
        'shapely.reverse/1k', _checked('shapely.reverse', shapely_reverse)
    )
    runner.bench_func(
        'shapely.orient_polygons/1k',
        _checked('shapely.orient_polygons', shapely_orient_polygons),
    )
    runner.bench_func(
        'shapely.normalize/1k', _checked('shapely.normalize', shapely_normalize)
    )
    runner.bench_func(
        'shapely.is_simple/1k', _checked('shapely.is_simple', shapely_is_simple)
    )
    runner.bench_func(
        'shapely.minimum_clearance/1k',
        _checked('shapely.minimum_clearance', shapely_minimum_clearance),
    )
    runner.bench_func(
        'shapely.relate/1k', _checked('shapely.relate_1k', shapely_relate_1k)
    )
    runner.bench_func(
        'shapely.relate_pattern/1k',
        _checked('shapely.relate_pattern_1k', shapely_relate_pattern_1k),
    )
    runner.bench_func(
        'shapely.is_valid/10k', _checked('shapely.is_valid_10k', shapely_is_valid_10k)
    )
    runner.bench_func(
        'shapely.make_valid/1k',
        _checked('shapely.make_valid_1k', shapely_make_valid_1k),
    )
    runner.bench_func(
        'shapely.minimum_bounding_circle/1k',
        _checked(
            'shapely.minimum_bounding_circle_1k', shapely_minimum_bounding_circle_1k
        ),
    )
    runner.bench_func(
        'shapely.minimum_clearance_line/1k',
        _checked(
            'shapely.minimum_clearance_line_1k', shapely_minimum_clearance_line_1k
        ),
    )
    runner.bench_func(
        'shapely.delaunay_triangles/1k',
        _checked('shapely.delaunay_triangles', shapely_delaunay_triangles),
    )
    runner.bench_func(
        'shapely.constrained_delaunay_triangles/1k',
        _checked(
            'shapely.constrained_delaunay_triangles',
            shapely_constrained_delaunay_triangles,
        ),
    )
    runner.bench_func(
        'shapely.polygon_triangles/1k',
        _checked('shapely.polygon_triangles', shapely_polygon_triangles),
    )
    runner.bench_func(
        'shapely.voronoi_polygons/1k',
        _checked('shapely.voronoi_polygons', shapely_voronoi_polygons),
    )
    runner.bench_func(
        'shapely.voronoi_edges/1k',
        _checked('shapely.voronoi_edges', shapely_voronoi_edges),
    )
    runner.bench_func(
        'shapely.polygonize/1k', _checked('shapely.polygonize', shapely_polygonize)
    )
    runner.bench_func(
        'shapely.polygonize_full/1k',
        _checked('shapely.polygonize_full', shapely_polygonize_full),
    )
    runner.bench_func(
        'shapely.index.build/10k', _checked('shapely.index_build', shapely_index_build)
    )
    runner.bench_func(
        'shapely.index.query/boxes_1k',
        _checked('shapely.index_query', shapely_index_query),
    )
    runner.bench_func(
        'shapely.index.nearest/k10_planar_10k',
        _checked('shapely.index_nearest_k10_planar', shapely_index_nearest_k10_planar),
    )
    runner.bench_func(
        'shapely.dwithin/pairwise_10k',
        _checked('shapely.dwithin_pairwise', shapely_dwithin_pairwise),
    )
    runner.bench_func(
        'shapely.prepared.contains/polygon_points_10k',
        _checked(
            'shapely.prepared_contains_polygon_points',
            shapely_prepared_contains_polygon_points,
        ),
    )
    runner.bench_func(
        'shapely.intersects/polygon_points_10k',
        _checked(
            'shapely.intersects_polygon_points', shapely_intersects_polygon_points
        ),
    )
    runner.bench_func(
        'shapely.within/polygon_points_10k',
        _checked('shapely.within_polygon_points', shapely_within_polygon_points),
    )
    runner.bench_func(
        'shapely.touches/polygon_points_10k',
        _checked('shapely.touches_polygon_points', shapely_touches_polygon_points),
    )
    runner.bench_func(
        'shapely.crosses/polygon_points_10k',
        _checked('shapely.crosses_polygon_points', shapely_crosses_polygon_points),
    )
    runner.bench_func(
        'shapely.overlaps/polygon_points_10k',
        _checked('shapely.overlaps_polygon_points', shapely_overlaps_polygon_points),
    )
    runner.bench_func(
        'shapely.disjoint/polygon_points_10k',
        _checked('shapely.disjoint_polygon_points', shapely_disjoint_polygon_points),
    )
    runner.bench_func(
        'shapely.covers/polygon_points_10k',
        _checked('shapely.covers_polygon_points', shapely_covers_polygon_points),
    )
    runner.bench_func(
        'shapely.covered_by/polygon_points_10k',
        _checked(
            'shapely.covered_by_polygon_points', shapely_covered_by_polygon_points
        ),
    )
    runner.bench_func(
        'shapely.buffer/points_1k',
        _checked('shapely.buffer_points', shapely_buffer_points),
    )
    runner.bench_func(
        'shapely.buffer/polygons_dilate_1k',
        _checked('shapely.buffer_polygons_dilate', shapely_buffer_polygons_dilate),
    )
    runner.bench_func(
        'shapely.buffer/polygons_erosion_1k',
        _checked('shapely.buffer_polygons_erosion', shapely_buffer_polygons_erosion),
    )
    runner.bench_func(
        'shapely.buffer/lines_1k',
        _checked('shapely.buffer_lines', shapely_buffer_lines),
    )
    runner.bench_func(
        'shapely.distance/pairwise_10k',
        _checked('shapely.distance_pairwise', shapely_distance_pairwise),
    )
    runner.bench_func(
        'shapely.length/lines_1k',
        _checked('shapely.length_lines', shapely_length_lines),
    )
    runner.bench_func(
        'shapely.area/polygons_1k',
        _checked('shapely.area_polygons', shapely_area_polygons),
    )
    runner.bench_func(
        'shapely.union_all/overlap_1k',
        _checked('shapely.union_all_overlap', shapely_union_all_overlap),
    )
    runner.bench_func(
        'shapely.union/pairwise_1k',
        _checked('shapely.union_pairwise', shapely_union_pairwise),
    )
    runner.bench_func(
        'shapely.symmetric_difference/pairwise_1k',
        _checked(
            'shapely.symmetric_difference_pairwise',
            shapely_symmetric_difference_pairwise,
        ),
    )
    runner.bench_func(
        'shapely.intersection_all/overlap_1k',
        _checked('shapely.intersection_all_overlap', shapely_intersection_all_overlap),
    )
    runner.bench_func(
        f'shapely.real_world.from_geojson/{REAL_WORLD_LABEL}',
        _checked('shapely.real_world_from_geojson', shapely_real_world_from_geojson),
    )
    runner.bench_func(
        f'shapely.real_world.bounds_cold/{REAL_WORLD_LABEL}',
        _checked('shapely.real_world_bounds_cold', shapely_real_world_bounds_cold),
    )
    runner.bench_func(
        f'shapely.real_world.bounds_warm/{REAL_WORLD_LABEL}',
        _checked('shapely.real_world_bounds_warm', shapely_real_world_bounds_warm),
    )
    runner.bench_func(
        f'shapely.real_world.area_cold/{REAL_WORLD_LABEL}',
        _checked('shapely.real_world_area_cold', shapely_real_world_area_cold),
    )
    runner.bench_func(
        f'shapely.real_world.area_warm/{REAL_WORLD_LABEL}',
        _checked('shapely.real_world_area_warm', shapely_real_world_area_warm),
    )
    runner.bench_func(
        f'shapely.real_world.point_on_surface/{REAL_WORLD_LABEL}',
        _checked(
            'shapely.real_world_point_on_surface', shapely_real_world_point_on_surface
        ),
    )


def _register_rtree(runner: pyperf.Runner) -> None:
    try:
        from rtree import index
    except ImportError:
        return
    from shapely.geometry import Point, box

    rtree_polygons = tuple(
        box(
            530000.0 + i % 100 * 1000.0,
            180000.0 + i // 100 * 1000.0,
            530000.0 + i % 100 * 1000.0 + 500.0,
            180000.0 + i // 100 * 1000.0 + 500.0,
        )
        for i in range(INDEX_POLYGON_COUNT)
    )
    rtree_query_boxes = tuple(
        box(CRS_XS[i], CRS_YS[i], CRS_XS[i] + 1000.0, CRS_YS[i] + 1000.0)
        for i in range(QUERY_BOX_COUNT)
    )
    rtree_query_bounds = Point(535000.0, 185000.0).bounds
    rtree_index = index.Index()
    for item_id, geometry in enumerate(rtree_polygons):
        rtree_index.insert(item_id, geometry.bounds)

    def rtree_index_build() -> object:
        built = index.Index()
        for item_id, geometry in enumerate(rtree_polygons):
            built.insert(item_id, geometry.bounds)
        return built

    def rtree_index_query() -> list[list[int]]:
        return [
            list(rtree_index.intersection(query_box.bounds))
            for query_box in rtree_query_boxes
        ]

    def rtree_nearest_k10_planar() -> list[int]:
        return list(rtree_index.nearest(rtree_query_bounds, NEAREST_K))

    runner.bench_func(
        'rtree.index.build/10k', _checked('rtree.rtree_index_build', rtree_index_build)
    )
    runner.bench_func(
        'rtree.index.query/boxes_1k',
        _checked('rtree.rtree_index_query', rtree_index_query),
    )
    runner.bench_func(
        'rtree.nearest/k10_planar_10k',
        _checked('rtree.rtree_nearest_k10_planar', rtree_nearest_k10_planar),
    )


def _register_h3(runner: pyperf.Runner) -> None:
    import h3

    h3_cells = [
        h3.latlng_to_cell(y, x, GRID_H3_RESOLUTION)
        for x, y in zip(XS, YS, strict=False)
    ]

    def h3_cell() -> list[str]:
        return [
            h3.latlng_to_cell(y, x, GRID_H3_RESOLUTION)
            for x, y in zip(XS, YS, strict=False)
        ]

    def h3_cell_to_boundary() -> list[list[tuple[float, float]]]:
        return [h3.cell_to_boundary(cell) for cell in h3_cells]

    def h3_compact_cells() -> list[str]:
        return h3.compact_cells(h3_cells)

    def h3_cells_to_geo() -> dict[str, object]:
        return h3.cells_to_geo(h3_cells[:64])

    runner.bench_func('h3.latlng_to_cell/10k', _checked('h3.h3_cell', h3_cell))
    runner.bench_func(
        'h3.cell_to_boundary/10k',
        _checked('h3.h3_cell_to_boundary', h3_cell_to_boundary),
    )
    runner.bench_func(
        'h3.compact_cells/10k', _checked('h3.h3_compact_cells', h3_compact_cells)
    )
    runner.bench_func(
        'h3.cells_to_geo/10k', _checked('h3.h3_cells_to_geo', h3_cells_to_geo)
    )


def _register_s2sphere(runner: pyperf.Runner) -> None:
    from s2sphere import CellId, LatLng

    def s2_cell() -> list[int]:
        return [
            CellId.from_lat_lng(LatLng.from_degrees(y, x)).parent(15).id()
            for x, y in zip(XS, YS, strict=False)
        ]

    runner.bench_func('s2sphere.cell/10k', _checked('s2.s2_cell', s2_cell))


def _register_pyproj(runner: pyperf.Runner) -> None:
    from pyproj import CRS, Geod, Proj, Transformer
    from pyproj.aoi import AreaOfInterest
    from pyproj.database import (
        PJType,
        get_units_map,
        query_crs_info,
        query_utm_crs_info,
    )
    from pyproj.enums import TransformDirection
    from pyproj.transformer import TransformerGroup
    from shapely.geometry import Polygon

    geod = Geod(ellps='WGS84')
    fast_transformer = Transformer.from_crs(4326, 3857, always_xy=True)
    authority_transformer = Transformer.from_crs(27700, 4326, always_xy=True)
    # Both steady-state rows own a cached transform. pyproj constructs its
    # Transformer here; warm gometry's lazy pipeline here as the equivalent
    # fixture setup (cold creation has dedicated churn benchmarks).
    gm.crs_transform(27700, 4326, NP_CRS_XS[:1], NP_CRS_YS[:1])
    aoi_transformer = Transformer.from_crs(
        2263, 4326, always_xy=True, area_of_interest=AreaOfInterest(*CRS_AOI_AREA)
    )
    aoi_options_transformer = Transformer.from_crs(
        2263,
        4326,
        always_xy=True,
        area_of_interest=AreaOfInterest(*CRS_AOI_AREA),
        authority='EPSG',
        accuracy=10.0,
        allow_ballpark=False,
    )
    geocentric_transformer = Transformer.from_crs(4979, 4978, always_xy=True)
    affine_transformer = Transformer.from_pipeline(CRS_AFFINE_OPERATION)
    churn_transformers = tuple(
        (
            Transformer.from_crs(source, target, always_xy=True)
            for source, target in CRS_OPERATION_CHURN_VALUES
        )
    )

    def geodesic_distance() -> np.ndarray:
        return geod.inv(NP_XS, NP_YS, NP_ZEROS, NP_ZEROS)[2]

    def geodesic_bearing() -> list[float]:
        return [
            geod.inv(x, y, 0.0, 0.0)[0] % 360.0 for x, y in zip(XS, YS, strict=False)
        ]

    def geodesic_destination() -> tuple[object, object, object]:
        longitude, latitude, back_azimuth = geod.fwd(
            NP_XS, NP_YS, GEO_FWD_AZIMUTH, GEO_FWD_DISTANCE
        )
        return (longitude, latitude, (back_azimuth + 180.0) % 360.0)

    def geodesic_interpolate() -> list[tuple[float, float]]:
        result = []
        for x, y in zip(XS, YS, strict=False):
            bearing, _, distance = geod.inv(x, y, 0.0, 0.0)
            result.append(geod.fwd(x, y, bearing, distance * 0.5)[:2])
        return result

    def geodesic_nearest() -> list[int]:
        distances = [
            (geod.inv(x, y, 0.0, 0.0)[2], idx)
            for idx, (x, y) in enumerate(zip(XS, YS, strict=False))
        ]
        distances.sort()
        return [idx for _, idx in distances[:10]]

    def to_crs_fast() -> tuple[tuple[float, ...], tuple[float, ...]]:
        return fast_transformer.transform(XS, YS)

    def to_crs_authority() -> tuple[tuple[float, ...], tuple[float, ...]]:
        return authority_transformer.transform(CRS_XS, CRS_YS)

    def crs_transform_numpy() -> np.ndarray:
        return np.column_stack(authority_transformer.transform(NP_CRS_XS, NP_CRS_YS))

    def crs_transform_aoi() -> tuple[tuple[float, ...], tuple[float, ...]]:
        return aoi_transformer.transform(CRS_AOI_XS, CRS_AOI_YS)

    def to_crs_aoi_options() -> tuple[tuple[float, ...], tuple[float, ...]]:
        return aoi_options_transformer.transform(CRS_AOI_XS, CRS_AOI_YS)

    def crs_transform_3d() -> tuple[
        tuple[float, ...], tuple[float, ...], tuple[float, ...]
    ]:
        return geocentric_transformer.transform(XS, YS, ZS)

    def crs_transform_4d() -> tuple[
        tuple[float, ...], tuple[float, ...], tuple[float, ...], tuple[float, ...]
    ]:
        return geocentric_transformer.transform(XS, YS, ZS, tt=TS)

    def crs_apply() -> tuple[
        tuple[float, ...], tuple[float, ...], tuple[float, ...], tuple[float, ...]
    ]:
        return affine_transformer.transform(XS, YS, ZS, tt=TS)

    def crs_apply_inverse() -> tuple[
        tuple[float, ...], tuple[float, ...], tuple[float, ...], tuple[float, ...]
    ]:
        return affine_transformer.transform(
            XS, YS, ZS, tt=TS, direction=TransformDirection.INVERSE
        )

    def crs_metadata(crs: object) -> tuple[object, ...]:
        area = crs.area_of_use
        datum = crs.datum
        ellipsoid = crs.ellipsoid
        prime_meridian = crs.prime_meridian
        coordinate_system = crs.coordinate_system
        operation = crs.coordinate_operation
        return (
            crs.name,
            crs.to_authority(),
            crs.type_name,
            crs.is_derived,
            crs.is_deprecated,
            crs.remarks,
            crs.scope,
            None if area is None else area.bounds,
            tuple(
                (axis.name, axis.abbrev, axis.direction, axis.unit_name)
                for axis in crs.axis_info
            ),
            None if datum is None else (datum.name, datum.type_name),
            None
            if ellipsoid is None
            else (
                ellipsoid.name,
                ellipsoid.semi_major_metre,
                ellipsoid.semi_minor_metre,
                ellipsoid.inverse_flattening,
            ),
            None
            if prime_meridian is None
            else (prime_meridian.name, prime_meridian.longitude),
            None if coordinate_system is None else coordinate_system.name,
            crs.source_crs.to_authority() if crs.source_crs else None,
            crs.target_crs.to_authority() if crs.target_crs else None,
            None
            if operation is None
            else (
                operation.name,
                operation.method_name,
                operation.accuracy,
                tuple(
                    (
                        parameter.name,
                        parameter.auth_name,
                        parameter.code,
                        parameter.value,
                        parameter.unit_name,
                    )
                    for parameter in operation.params
                ),
            ),
        )

    def crs_info() -> list[tuple[object, ...]]:
        result = []
        for value in CRS_INFO_VALUES:
            crs = CRS.from_user_input(value)
            result.append(crs_metadata(crs))
        return result

    def crs_operation() -> list[
        tuple[str, float, tuple[float, float, float, float] | None]
    ]:
        result = []
        for source, target in CRS_OPERATION_VALUES:
            transformer = Transformer.from_crs(source, target, always_xy=True)
            area = transformer.area_of_use
            result.append((
                transformer.description,
                transformer.accuracy,
                None if area is None else area.bounds,
            ))
        return result

    def crs_roundtrip() -> list[float]:
        forward = Transformer.from_crs(4326, 3857, always_xy=True)
        inverse = Transformer.from_crs(3857, 4326, always_xy=True)
        tx, ty = forward.transform(CRS_ROUNDTRIP_XS, CRS_ROUNDTRIP_YS)
        rx, ry = inverse.transform(tx, ty)
        return [
            math.hypot(result_x - x, result_y - y)
            for x, y, result_x, result_y in zip(
                CRS_ROUNDTRIP_XS, CRS_ROUNDTRIP_YS, rx, ry, strict=True
            )
        ]

    def crs_factors() -> list[
        tuple[
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
        ]
    ]:
        result = []
        for target, longitude, latitude in CRS_FACTOR_VALUES:
            factors = Proj(target).get_factors(longitude, latitude)
            result.append((
                factors.meridional_scale,
                factors.parallel_scale,
                factors.areal_scale,
                factors.angular_distortion,
                factors.meridian_parallel_angle,
                factors.meridian_convergence,
                factors.tissot_semimajor,
                factors.tissot_semiminor,
                factors.dx_dlam,
                factors.dx_dphi,
                factors.dy_dlam,
                factors.dy_dphi,
            ))
        return result

    def crs_factors_batch() -> tuple[
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
        tuple[float, ...],
    ]:
        factors = Proj(32618).get_factors(CRS_FACTOR_LONGITUDES, CRS_FACTOR_LATITUDES)
        return (
            factors.meridional_scale,
            factors.parallel_scale,
            factors.areal_scale,
            factors.angular_distortion,
            factors.meridian_parallel_angle,
            factors.meridian_convergence,
            factors.tissot_semimajor,
            factors.tissot_semiminor,
            factors.dx_dlam,
            factors.dx_dphi,
            factors.dy_dlam,
            factors.dy_dphi,
        )

    def crs_geodesic() -> list[tuple[float, float, float]]:
        geods = {}
        result = []
        for crs, lon1, lat1, lon2, lat2, z1, z2 in CRS_GEODESIC_VALUES:
            if crs not in geods:
                ellipsoid = CRS.from_user_input(crs).ellipsoid
                geods[crs] = Geod(
                    a=ellipsoid.semi_major_metre, rf=ellipsoid.inverse_flattening
                )
            forward, reverse, distance = geods[crs].inv(lon1, lat1, lon2, lat2)
            distance_3d = None if z1 is None else math.hypot(distance, z2 - z1)
            result.append((distance, forward, reverse - 180.0, distance_3d))
        return result

    def crs_geodesic_batch() -> tuple[list[float], list[float], list[float]]:
        ellipsoid = gm.crs_info(4326)['ellipsoid']
        assert ellipsoid is not None
        geod = Geod(a=ellipsoid['semi_major_metre'], rf=ellipsoid['inverse_flattening'])
        forward, reverse, distance = geod.inv(
            CRS_GEODESIC_LON1, CRS_GEODESIC_LAT1, CRS_GEODESIC_LON2, CRS_GEODESIC_LAT2
        )
        return (distance, forward, [value - 180.0 for value in reverse])

    def crs_geodesic_direct_batch() -> tuple[list[float], list[float], list[float]]:
        longitude, latitude, back_azimuth = geod.fwd(
            CRS_GEODESIC_LON1,
            CRS_GEODESIC_LAT1,
            CRS_GEODESIC_AZIMUTH,
            CRS_GEODESIC_DISTANCE,
        )
        return (longitude, latitude, [value + 180.0 for value in back_azimuth])

    def crs_geodesic_interpolate_batch() -> tuple[
        list[float], list[float], list[float]
    ]:
        ellipsoid = gm.crs_info(4326)['ellipsoid']
        assert ellipsoid is not None
        geod = Geod(a=ellipsoid['semi_major_metre'], rf=ellipsoid['inverse_flattening'])
        forward, _, distance = geod.inv(
            CRS_GEODESIC_LON1, CRS_GEODESIC_LAT1, CRS_GEODESIC_LON2, CRS_GEODESIC_LAT2
        )
        longitude, latitude, back_azimuth = geod.fwd(
            CRS_GEODESIC_LON1,
            CRS_GEODESIC_LAT1,
            forward,
            [value * 0.5 for value in distance],
        )
        return (longitude, latitude, [value + 180.0 for value in back_azimuth])

    crs_geodesic_polygons = tuple(
        Polygon(CRS_GEODESIC_POLYGON_COORDS, [CRS_GEODESIC_POLYGON_HOLE])
        for _ in range(CRS_INFO_COUNT)
    )

    def crs_geodesic_geometry_batch() -> tuple[list[float], list[float]]:
        ellipsoid = gm.crs_info(4267)['ellipsoid']
        assert ellipsoid is not None
        geod = Geod(a=ellipsoid['semi_major_metre'], rf=ellipsoid['inverse_flattening'])
        areas = []
        perimeters = []
        for polygon in crs_geodesic_polygons:
            area, perimeter = geod.geometry_area_perimeter(polygon)
            areas.append(abs(area))
            perimeters.append(perimeter)
        return (areas, perimeters)

    def crs_authority_conversion() -> list[
        tuple[tuple[str, str] | None, int | None, str, str, int]
    ]:
        result = []
        for value in CRS_AUTHORITY_VALUES:
            crs = CRS.from_user_input(value)
            result.append((
                crs.to_authority(),
                crs.to_epsg(),
                crs.to_3d().to_wkt(),
                crs.to_2d().to_wkt(),
                len(crs.list_authority()),
            ))
        return result

    def crs_cf() -> list[tuple[dict[str, object], list[dict[str, object]]]]:
        result = []
        for value in CRS_AUTHORITY_VALUES:
            crs = CRS.from_user_input(value)
            result.append((crs.to_cf(), crs.cs_to_cf()))
        return result

    def crs_operations() -> list[list[tuple[str, float, bool]]]:
        result = []
        for source, target in CRS_OPERATION_VALUES:
            group = TransformerGroup(source, target, always_xy=True)
            result.append(
                [
                    (transformer.description, transformer.accuracy, True)
                    for transformer in group.transformers
                ]
                + [
                    (operation.name, operation.accuracy, False)
                    for operation in group.unavailable_operations
                ]
            )
        return result

    def crs_info_churn() -> list[tuple[object, ...]]:
        result = []
        for value in CRS_CHURN_VALUES:
            crs = CRS.from_user_input(value)
            result.append(crs_metadata(crs))
        return result

    def crs_info_decompose() -> list[
        tuple[object, object, object, object, object, object]
    ]:
        result = []
        for value in CRS_DECOMPOSE_VALUES:
            crs = CRS.from_user_input(value)
            operation = crs.coordinate_operation
            result.append((
                crs.source_crs.to_authority() if crs.source_crs else None,
                crs.geodetic_crs.to_authority() if crs.geodetic_crs else None,
                None if crs.datum is None else crs.datum.name,
                [sub.to_authority() for sub in crs.sub_crs_list],
                None if operation is None else operation.name,
                [
                    (
                        parameter.name,
                        parameter.auth_name,
                        parameter.code,
                        parameter.value,
                        parameter.unit_name,
                        parameter.unit_auth_name,
                        parameter.unit_code,
                        parameter.unit_category,
                    )
                    for parameter in ([] if operation is None else operation.params)
                ],
            ))
        return result

    def crs_operation_churn() -> list[
        tuple[str, float, tuple[float, float, float, float] | None]
    ]:
        result = []
        for source, target in CRS_OPERATION_CHURN_VALUES:
            transformer = Transformer.from_crs(source, target, always_xy=True)
            area = transformer.area_of_use
            result.append((
                transformer.description,
                transformer.accuracy,
                None if area is None else area.bounds,
            ))
        return result

    def crs_operation_cold_distinct() -> list[
        tuple[str, float, tuple[float, float, float, float] | None]
    ]:
        result = []
        for source, target in CRS_OPERATION_COLD_VALUES:
            transformer = Transformer.from_crs(source, target, always_xy=True)
            area = transformer.area_of_use
            result.append((
                transformer.description,
                transformer.accuracy,
                None if area is None else area.bounds,
            ))
        return result

    def crs_operation_reused() -> list[
        tuple[str, float, tuple[float, float, float, float] | None]
    ]:
        result = []
        for transformer in churn_transformers:
            area = transformer.area_of_use
            result.append((
                transformer.description,
                transformer.accuracy,
                None if area is None else area.bounds,
            ))
        return result

    def crs_transform_bounds() -> list[tuple[float, float, float, float]]:
        return [
            fast_transformer.transform_bounds(*bounds) for bounds in CRS_BOUNDS_VALUES
        ]

    def crs_transform_bounds_3d() -> list[
        tuple[float, float, float, float, float, float]
    ]:
        result = []
        for minx, miny, minz, maxx, maxy, maxz in CRS_BOUNDS_3D_VALUES:
            xs, ys, zs = geocentric_transformer.transform(
                (minx, minx, minx, minx, maxx, maxx, maxx, maxx),
                (miny, miny, maxy, maxy, miny, miny, maxy, maxy),
                (minz, maxz, minz, maxz, minz, maxz, minz, maxz),
            )
            result.append((min(xs), min(ys), min(zs), max(xs), max(ys), max(zs)))
        return result

    def crs_catalog() -> list[list[object]]:
        area = AreaOfInterest(-1.0, 50.0, 1.0, 52.0)
        return [
            query_crs_info('EPSG', [PJType.PROJECTED_CRS], area)
            for _ in range(CRS_DATABASE_COUNT)
        ]

    def crs_utm_zones() -> list[list[object]]:
        area = AreaOfInterest(20.0, 51.0, 22.0, 53.0)
        return [query_utm_crs_info('WGS 84', area) for _ in range(CRS_DATABASE_COUNT)]

    def crs_units() -> list[list[object]]:
        return [
            list(get_units_map('EPSG', category='linear').values())
            for _ in range(CRS_DATABASE_COUNT)
        ]

    def crs_non_deprecated() -> list[list[CRS]]:
        return [
            CRS.from_epsg(2037).get_non_deprecated() for _ in range(CRS_DATABASE_COUNT)
        ]

    def crs_search() -> list[list[object]]:
        result = []
        for _ in range(CRS_DATABASE_COUNT):
            rows = query_crs_info('EPSG', [PJType.PROJECTED_CRS])
            result.append(
                [row for row in rows if 'british national grid' in row.name.lower()][
                    :20
                ]
            )
        return result

    def crs_exports() -> list[tuple[str, str, str, str, dict[str, object]]]:
        wgs84 = CRS.from_epsg(4326)
        webmerc = CRS.from_epsg(3857)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return [
                (
                    wgs84.to_wkt(version='WKT2_2019_SIMPLIFIED'),
                    wgs84.to_wkt(version='WKT1_GDAL', output_axis_rule=False),
                    webmerc.to_proj4(version=5),
                    wgs84.to_json(pretty=True),
                    wgs84.to_json_dict(),
                )
                for _ in range(CRS_DATABASE_COUNT)
            ]

    def crs_same() -> list[bool]:
        wgs84 = CRS.from_epsg(4326)
        other = CRS.from_wkt(wgs84.to_wkt())
        return [wgs84.equals(other) for _ in range(CRS_DATABASE_COUNT)]

    runner.bench_func(
        'pyproj.Geod.inv/10k', _checked('pyproj.geodesic_distance', geodesic_distance)
    )
    runner.bench_func(
        'pyproj.Geod.bearing/10k', _checked('pyproj.geodesic_bearing', geodesic_bearing)
    )
    runner.bench_func(
        'pyproj.Geod.fwd/10k',
        _checked('pyproj.geodesic_destination', geodesic_destination),
    )
    runner.bench_func(
        'pyproj.Geod.interpolate/10k',
        _checked('pyproj.geodesic_interpolate', geodesic_interpolate),
    )
    runner.bench_func(
        'pyproj.Geod.nearest_m/10k', _checked('pyproj.nearest_m', geodesic_nearest)
    )
    runner.bench_func(
        'pyproj.Transformer.to_crs_fast/10k',
        _checked('pyproj.to_crs_fast', to_crs_fast),
    )
    runner.bench_func(
        'pyproj.Transformer.to_crs_proj/10k',
        _checked('pyproj.to_crs_proj', to_crs_authority),
    )
    runner.bench_func(
        'pyproj.Transformer.transform_numpy/10k',
        _checked('pyproj.crs_transform_numpy', crs_transform_numpy),
    )
    runner.bench_func(
        'pyproj.Transformer.transform_aoi/10k',
        _checked('pyproj.crs_transform_aoi', crs_transform_aoi),
    )
    runner.bench_func(
        'pyproj.Transformer.to_crs_aoi_options/10k',
        _checked('pyproj.to_crs_aoi_options', to_crs_aoi_options),
    )
    runner.bench_func(
        'pyproj.Transformer.transform_3d/10k',
        _checked('pyproj.crs_transform_3d', crs_transform_3d),
    )
    runner.bench_func(
        'pyproj.Transformer.transform_4d/10k',
        _checked('pyproj.crs_transform_4d', crs_transform_4d),
    )
    runner.bench_func(
        'pyproj.Transformer.from_pipeline/10k', _checked('pyproj.crs_apply', crs_apply)
    )
    runner.bench_func(
        'pyproj.Transformer.from_pipeline_inverse/10k',
        _checked('pyproj.crs_apply_inverse', crs_apply_inverse),
    )
    runner.bench_func('pyproj.CRS.info/1k', _checked('pyproj.crs_info', crs_info))
    runner.bench_func(
        'pyproj.Transformer.operation_cold/1k',
        _checked('pyproj.crs_operation', crs_operation),
    )
    runner.bench_func(
        'pyproj.Transformer.roundtrip_reused/1k',
        _checked('pyproj.crs_roundtrip', crs_roundtrip),
    )
    runner.bench_func(
        'pyproj.Proj.factors/1k', _checked('pyproj.crs_factors', crs_factors)
    )
    runner.bench_func(
        'pyproj.Proj.factors_batch/1k',
        _checked('pyproj.crs_factors_batch', crs_factors_batch),
    )
    runner.bench_func(
        'pyproj.Geod.crs_geodesic/1k', _checked('pyproj.crs_geodesic', crs_geodesic)
    )
    runner.bench_func(
        'pyproj.Geod.crs_geodesic_batch/1k',
        _checked('pyproj.crs_geodesic_batch', crs_geodesic_batch),
    )
    runner.bench_func(
        'pyproj.Geod.crs_geodesic_direct_batch/1k',
        _checked('pyproj.crs_geodesic_direct_batch', crs_geodesic_direct_batch),
    )
    runner.bench_func(
        'pyproj.Geod.crs_geodesic_interpolate_batch/1k',
        _checked(
            'pyproj.crs_geodesic_interpolate_batch', crs_geodesic_interpolate_batch
        ),
    )
    runner.bench_func(
        'pyproj.Geod.crs_geodesic_geometry_batch/1k',
        _checked('pyproj.crs_geodesic_geometry_batch', crs_geodesic_geometry_batch),
    )
    runner.bench_func(
        'pyproj.CRS.authority_conversion/120',
        _checked('pyproj.crs_authority_conversion', crs_authority_conversion),
    )
    runner.bench_func('pyproj.CRS.cf/120', _checked('pyproj.crs_cf', crs_cf))
    runner.bench_func(
        'pyproj.TransformerGroup.operations_cold/1k',
        _checked('pyproj.crs_operations', crs_operations),
    )
    runner.bench_func(
        'pyproj.CRS.info_churn/120', _checked('pyproj.crs_info_churn', crs_info_churn)
    )
    runner.bench_func(
        'pyproj.CRS.info_decompose/120',
        _checked('pyproj.crs_info_decompose', crs_info_decompose),
    )
    runner.bench_func(
        'pyproj.Transformer.operation_churn/120',
        _checked('pyproj.crs_operation_churn', crs_operation_churn),
    )
    runner.bench_func(
        'pyproj.Transformer.operation_cold_distinct/1k',
        _checked('pyproj.crs_operation_cold_distinct', crs_operation_cold_distinct),
    )
    runner.bench_func(
        'pyproj.Transformer.operation_reused/120',
        _checked('pyproj.crs_operation_reused', crs_operation_reused),
    )
    runner.bench_func(
        'pyproj.Transformer.transform_bounds/1k',
        _checked('pyproj.crs_transform_bounds', crs_transform_bounds),
    )
    runner.bench_func(
        'pyproj.Transformer.transform_bounds_3d_corners/1k',
        _checked('pyproj.crs_transform_bounds_3d', crs_transform_bounds_3d),
    )
    runner.bench_func(
        'pyproj.database.query_crs_info/120',
        _checked('pyproj.crs_catalog', crs_catalog),
    )
    runner.bench_func(
        'pyproj.database.query_utm_crs_info/120',
        _checked('pyproj.crs_utm_zones', crs_utm_zones),
    )
    runner.bench_func(
        'pyproj.database.get_units_map/120', _checked('pyproj.crs_units', crs_units)
    )
    runner.bench_func(
        'pyproj.CRS.get_non_deprecated/120',
        _checked('pyproj.crs_non_deprecated', crs_non_deprecated),
    )
    runner.bench_func(
        'pyproj.database.query_crs_info_search/120',
        _checked('pyproj.crs_search', crs_search),
    )
    runner.bench_func(
        'pyproj.CRS.exports/120', _checked('pyproj.crs_exports', crs_exports)
    )
    runner.bench_func('pyproj.CRS.equals/120', _checked('pyproj.crs_same', crs_same))


def main() -> None:
    runner = bench_runner()
    runner.metadata['project'] = 'gometry'
    runner.metadata['fixture'] = 'deterministic-trig-points'
    runner.metadata['competitors'] = 'shapely,h3,pyproj,s2sphere,rtree'
    runner.metadata['gometry_only'] = ','.join(sorted(GOMETRY_ONLY_BENCHMARKS))
    flush_benchmarks = queue_selected_benchmarks(runner, 'competitors')
    runner.bench_func('gometry.points/10k', _checked('gometry.points', gometry_points))
    runner.bench_func(
        'gometry.contains/polygon_points_10k',
        _checked('gometry.contains', gometry_contains),
    )
    runner.bench_func(
        'gometry.contains_xy/polygon_points_10k',
        _checked('gometry.contains_xy', gometry_contains_xy),
    )
    runner.bench_func(
        'gometry.from_wkb/1k', _checked('gometry.from_wkb', gometry_from_wkb)
    )
    runner.bench_func(
        'gometry.from_wkb.batch/1k',
        _checked('gometry.from_wkb', gometry_from_wkb_batch),
    )
    runner.bench_func(
        'gometry.to_wkt.batch/1k', _checked('gometry.to_wkt', gometry_to_wkt_batch)
    )
    runner.bench_func(
        'gometry.from_wkt.batch/1k',
        _checked('gometry.from_wkt', gometry_from_wkt_batch),
    )
    runner.bench_func(
        'gometry.to_geojson.batch/1k',
        _checked('gometry.to_geojson', gometry_to_geojson_batch),
    )
    runner.bench_func(
        'gometry.from_geojson.batch/1k',
        _checked('gometry.from_geojson', gometry_from_geojson_batch),
    )
    runner.bench_func(
        'gometry.line_merge/1k', _checked('gometry.line_merge', gometry_line_merge)
    )
    runner.bench_func(
        'gometry.clip_by_rect/1k',
        _checked('gometry.clip_by_rect', gometry_clip_by_rect),
    )
    runner.bench_func(
        'gometry.line_interpolate/1k',
        _checked('gometry.line_interpolate', gometry_line_interpolate),
    )
    runner.bench_func(
        'gometry.line_substring/1k',
        _checked('gometry.line_substring', gometry_line_substring),
    )
    runner.bench_func(
        'gometry.line_locate/1k',
        _checked('gometry.line_locate', gometry_line_locate),
    )
    runner.bench_func(
        'gometry.line_locate.pairwise/1k',
        _checked('gometry.line_locate_pairwise', gometry_line_locate_pairwise),
    )
    runner.bench_func(
        'gometry.intersection_pairwise/1k',
        _checked('gometry.intersection_pairwise', gometry_intersection_pairwise),
    )
    runner.bench_func(
        'gometry.difference_pairwise/1k',
        _checked('gometry.difference_pairwise', gometry_difference_pairwise),
    )
    runner.bench_func('gometry.split/1k', _checked('gometry.split', gometry_split))
    runner.bench_func(
        'gometry.split_pairwise/1k',
        _checked('gometry.split_pairwise', gometry_split_pairwise),
    )
    runner.bench_func(
        'gometry.offset_curve/1k',
        _checked('gometry.offset_curve', gometry_offset_curve),
    )
    runner.bench_func(
        'gometry.shared_paths/1k',
        _checked('gometry.shared_paths', gometry_shared_paths),
    )
    runner.bench_func(
        'gometry.shared_paths_pairwise/1k',
        _checked('gometry.shared_paths_pairwise', gometry_shared_paths_pairwise),
    )
    runner.bench_func(
        'gometry.centroid/1k', _checked('gometry.centroid', gometry_centroid)
    )
    runner.bench_func(
        'gometry.point_on_surface/1k',
        _checked('gometry.point_on_surface', gometry_point_on_surface),
    )
    runner.bench_func(
        'gometry.envelope/1k', _checked('gometry.envelope', gometry_envelope)
    )
    runner.bench_func(
        'gometry.convex_hull/1k', _checked('gometry.convex_hull', gometry_convex_hull)
    )
    runner.bench_func(
        'gometry.concave_hull/1k',
        _checked('gometry.concave_hull', gometry_concave_hull),
    )
    runner.bench_func(
        'gometry.polylabel/1k', _checked('gometry.polylabel', gometry_polylabel)
    )
    runner.bench_func(
        'gometry.minimum_rotated_rectangle/1k',
        _checked(
            'gometry.minimum_rotated_rectangle', gometry_minimum_rotated_rectangle
        ),
    )
    runner.bench_func(
        'gometry.boundary/1k', _checked('gometry.boundary', gometry_boundary)
    )
    runner.bench_func(
        'gometry.remove_repeated_points/1k',
        _checked('gometry.remove_repeated_points', gometry_remove_repeated_points),
    )
    runner.bench_func(
        'gometry.segmentize/1k', _checked('gometry.segmentize', gometry_segmentize)
    )
    runner.bench_func(
        'gometry.centroid.packed_lines/20k',
        _checked('gometry.centroid_packed_lines_20k', gometry_centroid_packed_lines),
    )
    runner.bench_func(
        'gometry.rotate.packed_lines/20k',
        _checked('gometry.rotate_packed_lines_20k', gometry_rotate_packed_lines),
    )
    runner.bench_func(
        'gometry.segmentize.packed_lines/1k',
        _checked('gometry.segmentize_packed_lines_1k', gometry_segmentize_packed_lines),
    )
    runner.bench_func(
        'gometry.segmentize.fraction/1k',
        _checked('gometry.segmentize_fraction', gometry_segmentize_fraction),
    )
    runner.bench_func(
        'gometry.segmentize.fraction.packed_lines/1k',
        _checked(
            'gometry.segmentize_fraction_packed_lines_1k',
            gometry_segmentize_fraction_packed_lines,
        ),
    )
    runner.bench_func(
        'gometry.concat/packed_polygons_2x1k',
        _checked(
            'gometry.concat_packed_polygons_2x1k', gometry_concat_packed_polygons_2x1k
        ),
    )
    runner.bench_func(
        'gometry.filter/packed_polygons_1k',
        _checked(
            'gometry.filter_packed_polygons_1k', gometry_filter_packed_polygons_1k
        ),
    )
    runner.bench_func(
        'gometry.simplify.packed_lines/1k',
        _checked('gometry.simplify_packed_lines_1k', gometry_simplify_packed_lines),
    )
    runner.bench_func(
        'gometry.simplify_vw.packed_lines/1k',
        _checked(
            'gometry.simplify_vw_packed_lines_1k', gometry_simplify_vw_packed_lines
        ),
    )
    runner.bench_func(
        'gometry.smooth/polygon_200',
        _checked('gometry.smooth_polygon_200', gometry_smooth_polygon_200),
    )
    runner.bench_func(
        'gometry.smooth/packed_lines/1k',
        _checked('gometry.smooth_packed_lines_1k', gometry_smooth_packed_lines_1k),
    )
    runner.bench_func('gometry.snap/1k', _checked('gometry.snap', gometry_snap))
    runner.bench_func(
        'gometry.snap_pairwise/1k',
        _checked('gometry.snap_pairwise', gometry_snap_pairwise),
    )
    runner.bench_func(
        'gometry.hausdorff_distance/1k',
        _checked('gometry.hausdorff_distance', gometry_hausdorff_distance),
    )
    runner.bench_func(
        'gometry.hausdorff_distance.packed_lines/1k',
        _checked(
            'gometry.hausdorff_distance_packed_lines_1k',
            gometry_hausdorff_distance_packed_lines,
        ),
    )
    runner.bench_func(
        'gometry.hausdorff_distance.packed_lines_cross/1k',
        _checked(
            'gometry.hausdorff_distance_packed_lines_cross_1k',
            gometry_hausdorff_distance_packed_lines_cross,
        ),
    )
    runner.bench_func(
        'gometry.hausdorff_distance.geographic/1k',
        _checked(
            'gometry.hausdorff_distance_geographic_1k',
            gometry_hausdorff_distance_geographic,
        ),
    )
    runner.bench_func(
        'gometry.frechet_distance/1k',
        _checked('gometry.frechet_distance', gometry_frechet_distance),
    )
    runner.bench_func(
        'gometry.frechet_distance.packed_lines/1k',
        _checked(
            'gometry.frechet_distance_packed_lines_1k',
            gometry_frechet_distance_packed_lines,
        ),
    )
    runner.bench_func(
        'gometry.nearest_points/1k',
        _checked('gometry.nearest_points', gometry_nearest_points),
    )
    runner.bench_func(
        'gometry.reverse/1k', _checked('gometry.reverse', gometry_reverse)
    )
    runner.bench_func(
        'gometry.orient_polygons/1k',
        _checked('gometry.orient_polygons', gometry_orient_polygons),
    )
    runner.bench_func(
        'gometry.normalize/1k', _checked('gometry.normalize', gometry_normalize)
    )
    runner.bench_func(
        'gometry.is_simple/1k', _checked('gometry.is_simple', gometry_is_simple)
    )
    runner.bench_func(
        'gometry.minimum_clearance/1k',
        _checked('gometry.minimum_clearance', gometry_minimum_clearance),
    )
    runner.bench_func(
        'gometry.triangulate.delaunay/1k',
        _checked('gometry.triangulate_delaunay', gometry_triangulate_delaunay),
    )
    runner.bench_func(
        'gometry.triangulate.constrained/1k',
        _checked(
            'gometry.triangulate_constrained',
            gometry_triangulate_constrained,
        ),
    )
    runner.bench_func(
        'gometry.triangulate.earcut/1k',
        _checked('gometry.triangulate_earcut', gometry_triangulate_earcut),
    )
    runner.bench_func(
        'gometry.voronoi_polygons/1k',
        _checked('gometry.voronoi_polygons', gometry_voronoi_polygons),
    )
    runner.bench_func(
        'gometry.voronoi_edges/1k',
        _checked('gometry.voronoi_edges', gometry_voronoi_edges),
    )
    runner.bench_func(
        'gometry.polygonize/1k', _checked('gometry.polygonize', gometry_polygonize)
    )
    runner.bench_func(
        'gometry.polygonize_full/1k',
        _checked('gometry.polygonize_full', gometry_polygonize_full),
    )
    runner.bench_func(
        'gometry.h3_cells/10k', _checked('gometry.h3_cells', gometry_h3_cell)
    )
    runner.bench_func(
        'gometry.s2_cells/10k', _checked('gometry.s2_cells', gometry_s2_cell)
    )
    runner.bench_func(
        'gometry.distance/10k', _checked('gometry.distance', gometry_geodesic_distance)
    )
    runner.bench_func(
        'gometry.distance_geodesic_point_pairs/10k',
        _checked(
            'gometry.distance_geodesic_point_pairs',
            gometry_distance_geodesic_point_pairs,
        ),
    )
    runner.bench_func(
        'gometry.dwithin_geodesic_point_pairs/10k',
        _checked(
            'gometry.dwithin_geodesic_point_pairs', gometry_dwithin_geodesic_point_pairs
        ),
    )
    runner.bench_func(
        'gometry.swap_xy_packed_points/10k',
        _checked('gometry.swap_xy_packed_points', gometry_swap_xy_packed_points),
    )
    runner.bench_func(
        'gometry.swap_xy_packed_lines/20k',
        _checked('gometry.swap_xy_packed_lines', gometry_swap_xy_packed_lines),
    )
    runner.bench_func(
        'gometry.swap_xy_packed_polygons/1k',
        _checked('gometry.swap_xy_packed_polygons', gometry_swap_xy_packed_polygons),
    )
    runner.bench_func(
        'gometry.quantize_packed_lines/1k',
        _checked('gometry.quantize_packed_lines', gometry_quantize_packed_lines),
    )
    runner.bench_func(
        'gometry.quantize_packed_polygons/1k',
        _checked('gometry.quantize_packed_polygons', gometry_quantize_packed_polygons),
    )
    runner.bench_func(
        'gometry.bearing/10k', _checked('gometry.bearing', gometry_bearing)
    )
    runner.bench_func(
        'gometry.destination/10k', _checked('gometry.destination', gometry_destination)
    )
    runner.bench_func(
        'gometry.point_between/10k',
        _checked('gometry.point_between', gometry_point_between),
    )
    runner.bench_func(
        'gometry.to_crs_fast/10k', _checked('gometry.to_crs_fast', gometry_to_crs_fast)
    )
    runner.bench_func(
        'gometry.to_crs_proj/10k', _checked('gometry.to_crs_proj', gometry_to_crs_proj)
    )
    runner.bench_func(
        'gometry.to_crs_aoi_options/10k',
        _checked('gometry.to_crs_aoi_options', gometry_to_crs_aoi_options),
    )
    runner.bench_func(
        'gometry.crs_transform/10k',
        _checked('gometry.crs_transform', gometry_crs_transform),
    )
    runner.bench_func(
        'gometry.crs_transform_aoi/10k',
        _checked('gometry.crs_transform_aoi', gometry_crs_transform_aoi),
    )
    runner.bench_func(
        'gometry.crs_transform_3d/10k',
        _checked('gometry.crs_transform_3d', gometry_crs_transform_3d),
    )
    runner.bench_func(
        'gometry.crs_transform_4d/10k',
        _checked('gometry.crs_transform_4d', gometry_crs_transform_4d),
    )
    runner.bench_func(
        'gometry.crs_apply/10k', _checked('gometry.crs_apply', gometry_crs_apply)
    )
    runner.bench_func(
        'gometry.crs_apply_inverse/10k',
        _checked('gometry.crs_apply_inverse', gometry_crs_apply_inverse),
    )
    runner.bench_func(
        'gometry.crs_info/1k', _checked('gometry.crs_info', gometry_crs_info)
    )
    runner.bench_func(
        'gometry.crs_operation_warm/1k',
        _checked('gometry.crs_operation', gometry_crs_operation),
    )
    runner.bench_func(
        'gometry.crs_operation_at/1k',
        _checked('gometry.crs_operation_at', gometry_crs_operation_at),
    )
    runner.bench_func(
        'gometry.crs_roundtrip/1k',
        _checked('gometry.crs_roundtrip', gometry_crs_roundtrip),
    )
    runner.bench_func(
        'gometry.crs_factors/1k', _checked('gometry.crs_factors', gometry_crs_factors)
    )
    runner.bench_func(
        'gometry.crs_geodesic/1k',
        _checked('gometry.crs_geodesic', gometry_crs_geodesic),
    )
    runner.bench_func(
        'gometry.crs_geodesic_batch/1k',
        _checked('gometry.crs_geodesic_batch', gometry_crs_geodesic_batch),
    )
    runner.bench_func(
        'gometry.crs_geodesic_direct_batch/1k',
        _checked(
            'gometry.crs_geodesic_direct_batch', gometry_crs_geodesic_direct_batch
        ),
    )
    runner.bench_func(
        'gometry.crs_geodesic_interpolate_batch/1k',
        _checked(
            'gometry.crs_geodesic_interpolate_batch',
            gometry_crs_geodesic_interpolate_batch,
        ),
    )
    runner.bench_func(
        'gometry.crs_geodesic_geometry_batch/1k',
        _checked(
            'gometry.crs_geodesic_geometry_batch', gometry_crs_geodesic_geometry_batch
        ),
    )
    runner.bench_func(
        'gometry.crs_operations_warm/1k',
        _checked('gometry.crs_operations', gometry_crs_operations),
    )
    runner.bench_func(
        'gometry.crs_static_catalogs/120',
        _checked('gometry.crs_static_catalogs', gometry_crs_static_catalogs),
    )
    runner.bench_func(
        'gometry.crs_authority_conversion/120',
        _checked('gometry.crs_authority_conversion', gometry_crs_authority_conversion),
    )
    runner.bench_func('gometry.crs_cf/120', _checked('gometry.crs_cf', gometry_crs_cf))
    runner.bench_func(
        'gometry.crs_info_churn/120',
        _checked('gometry.crs_info_churn', gometry_crs_info_churn),
    )
    runner.bench_func(
        'gometry.crs_info_decompose/120',
        _checked('gometry.crs_info_decompose', gometry_crs_info_decompose),
    )
    runner.bench_func(
        'gometry.crs_operation_churn/120',
        _checked('gometry.crs_operation_churn', gometry_crs_operation_churn),
    )
    runner.bench_func(
        'gometry.crs_transform_bounds/1k',
        _checked('gometry.crs_transform_bounds', gometry_crs_transform_bounds),
    )
    runner.bench_func(
        'gometry.crs_transform_bounds_3d/1k',
        _checked('gometry.crs_transform_bounds_3d', gometry_crs_transform_bounds_3d),
    )
    runner.bench_func(
        'gometry.crs_transform_bounds_3d_corners/1k',
        _checked(
            'gometry.crs_transform_bounds_3d_corners',
            gometry_crs_transform_bounds_3d_corners,
        ),
    )
    runner.bench_func(
        'gometry.crs_list/120', _checked('gometry.crs_catalog', gometry_crs_catalog)
    )
    runner.bench_func(
        'gometry.crs_utm_zones/120',
        _checked('gometry.crs_utm_zones', gometry_crs_utm_zones),
    )
    runner.bench_func(
        'gometry.crs_units/120', _checked('gometry.crs_units', gometry_crs_units)
    )
    runner.bench_func(
        'gometry.crs_celestial_bodies/120',
        _checked('gometry.crs_celestial_bodies', gometry_crs_celestial_bodies),
    )
    runner.bench_func(
        'gometry.crs_non_deprecated/120',
        _checked('gometry.crs_non_deprecated', gometry_crs_non_deprecated),
    )
    runner.bench_func(
        'gometry.crs_search/120', _checked('gometry.crs_search', gometry_crs_search)
    )
    runner.bench_func(
        'gometry.crs_exports/120', _checked('gometry.crs_exports', gometry_crs_exports)
    )
    runner.bench_func(
        'gometry.crs_same/120', _checked('gometry.crs_same', gometry_crs_same)
    )
    runner.bench_func(
        'gometry.index.build/10k', _checked('gometry.index_build', gometry_index_build)
    )
    runner.bench_func(
        'gometry.index.query/boxes_1k',
        _checked('gometry.index_query', gometry_index_query),
    )
    runner.bench_func(
        'gometry.index.nearest/k10_planar_10k',
        _checked('gometry.index_nearest_k10_planar', gometry_index_nearest_k10_planar),
    )
    runner.bench_func(
        'gometry.dwithin/pairwise_10k',
        _checked('gometry.dwithin_pairwise', gometry_dwithin_pairwise),
    )
    runner.bench_func(
        'gometry.prepared.contains/polygon_points_10k',
        _checked('gometry.prepared_contains_polygon_points', gometry_prepared_contains),
    )
    runner.bench_func(
        'gometry.intersects/polygon_points_10k',
        _checked(
            'gometry.intersects_polygon_points', gometry_intersects_polygon_points
        ),
    )
    runner.bench_func(
        'gometry.within/polygon_points_10k',
        _checked('gometry.within_polygon_points', gometry_within_polygon_points),
    )
    runner.bench_func(
        'gometry.touches/polygon_points_10k',
        _checked('gometry.touches_polygon_points', gometry_touches_polygon_points),
    )
    runner.bench_func(
        'gometry.crosses/polygon_points_10k',
        _checked('gometry.crosses_polygon_points', gometry_crosses_polygon_points),
    )
    runner.bench_func(
        'gometry.overlaps/polygon_points_10k',
        _checked('gometry.overlaps_polygon_points', gometry_overlaps_polygon_points),
    )
    runner.bench_func(
        'gometry.disjoint/polygon_points_10k',
        _checked('gometry.disjoint_polygon_points', gometry_disjoint_polygon_points),
    )
    runner.bench_func(
        'gometry.covers/polygon_points_10k',
        _checked('gometry.covers_polygon_points', gometry_covers_polygon_points),
    )
    runner.bench_func(
        'gometry.covered_by/polygon_points_10k',
        _checked(
            'gometry.covered_by_polygon_points', gometry_covered_by_polygon_points
        ),
    )
    runner.bench_func(
        'gometry.buffer/points_1k',
        _checked('gometry.buffer_points', gometry_buffer_points),
    )
    runner.bench_func(
        'gometry.buffer/polygons_dilate_1k',
        _checked('gometry.buffer_polygons_dilate', gometry_buffer_polygons_dilate),
    )
    runner.bench_func(
        'gometry.buffer/polygons_erosion_1k',
        _checked('gometry.buffer_polygons_erosion', gometry_buffer_polygons_erosion),
    )
    runner.bench_func(
        'gometry.buffer/lines_1k',
        _checked('gometry.buffer_lines', gometry_buffer_lines),
    )
    runner.bench_func(
        'gometry.distance/pairwise_10k',
        _checked('gometry.distance_pairwise', gometry_distance_pairwise),
    )
    runner.bench_func(
        'gometry.length/lines_1k',
        _checked('gometry.length_lines', gometry_length_lines),
    )
    runner.bench_func(
        'gometry.area/polygons_1k',
        _checked('gometry.area_polygons', gometry_area_polygons),
    )
    runner.bench_func(
        'gometry.union_all/overlap_1k',
        _checked('gometry.union_all_overlap', gometry_union_all_overlap),
    )
    runner.bench_func(
        'gometry.union/pairwise_1k',
        _checked('gometry.union_pairwise', gometry_union_pairwise),
    )
    runner.bench_func(
        'gometry.symmetric_difference/pairwise_1k',
        _checked(
            'gometry.symmetric_difference_pairwise',
            gometry_symmetric_difference_pairwise,
        ),
    )
    runner.bench_func(
        'gometry.intersection_all/overlap_1k',
        _checked('gometry.intersection_all_overlap', gometry_intersection_all_overlap),
    )
    runner.bench_func(
        'gometry.nearest_m/10k', _checked('gometry.nearest_m', gometry_nearest_m)
    )
    runner.bench_func(
        'gometry.to_wkb.batch/1k', _checked('gometry.to_wkb', gometry_to_wkb_batch)
    )
    runner.bench_func(
        'gometry.to_arrow.roundtrip/1k',
        _checked('gometry.to_arrow_roundtrip', gometry_to_arrow_roundtrip),
    )
    runner.bench_func(
        'gometry.from_arrow.roundtrip/1k',
        _checked('gometry.from_arrow_roundtrip', gometry_from_arrow_roundtrip),
    )
    runner.bench_func(
        'gometry.to_polyline/1k', _checked('gometry.to_polyline', gometry_to_polyline)
    )
    runner.bench_func(
        'gometry.from_polyline/1k',
        _checked('gometry.from_polyline', gometry_from_polyline),
    )
    runner.bench_func(
        'gometry.scale.packed_lines/20k',
        _checked('gometry.scale_packed_lines_20k', gometry_scale_packed_lines),
    )
    runner.bench_func(
        'gometry.skew.packed_lines/20k',
        _checked('gometry.skew_packed_lines_20k', gometry_skew_packed_lines),
    )
    runner.bench_func(
        'gometry.translate.packed_lines/20k',
        _checked('gometry.translate_packed_lines_20k', gometry_translate_packed_lines),
    )
    runner.bench_func(
        'gometry.affine_transform.packed_lines/20k',
        _checked(
            'gometry.affine_transform_packed_lines_20k',
            gometry_affine_transform_packed_lines,
        ),
    )
    runner.bench_func(
        'gometry.relate/1k', _checked('gometry.relate_1k', gometry_relate_1k)
    )
    runner.bench_func(
        'gometry.relate_pattern/1k',
        _checked('gometry.relate_pattern_1k', gometry_relate_pattern_1k),
    )
    runner.bench_func(
        'gometry.is_valid/10k', _checked('gometry.is_valid_10k', gometry_is_valid_10k)
    )
    runner.bench_func(
        'gometry.repair/1k', _checked('gometry.repair_1k', gometry_repair_1k)
    )
    runner.bench_func(
        'gometry.h3_polygon/10k',
        _checked('gometry.h3_boundary_10k', gometry_h3_boundary_10k),
    )
    runner.bench_func(
        'gometry.h3_to_polygon/10k',
        _checked('gometry.h3_to_polygon_10k', gometry_h3_to_polygon_10k),
    )
    runner.bench_func(
        'gometry.h3_compact/10k',
        _checked('gometry.h3_compact_10k', gometry_h3_compact_10k),
    )
    runner.bench_func(
        'gometry.s2_polygon/10k',
        _checked('gometry.s2_boundary_10k', gometry_s2_boundary_10k),
    )
    runner.bench_func(
        'gometry.s2_to_polygon/10k',
        _checked('gometry.s2_to_polygon_10k', gometry_s2_to_polygon_10k),
    )
    runner.bench_func(
        'gometry.geohash_cell/10k',
        _checked('gometry.geohash_cell_10k', gometry_geohash_cell_10k),
    )
    runner.bench_func(
        'gometry.geohash_to_polygon/10k',
        _checked('gometry.geohash_to_polygon_10k', gometry_geohash_to_polygon_10k),
    )
    runner.bench_func(
        'gometry.tile_cell/10k',
        _checked('gometry.tiles_cell_10k', gometry_tiles_cell_10k),
    )
    runner.bench_func(
        'gometry.tile_to_polygon/10k',
        _checked('gometry.tiles_to_polygon_10k', gometry_tiles_to_polygon_10k),
    )
    runner.bench_func(
        'gometry.minimum_bounding_circle/1k',
        _checked(
            'gometry.minimum_bounding_circle_1k', gometry_minimum_bounding_circle_1k
        ),
    )
    runner.bench_func(
        'gometry.minimum_clearance_line/1k',
        _checked(
            'gometry.minimum_clearance_line_1k', gometry_minimum_clearance_line_1k
        ),
    )
    runner.bench_func(
        'gometry.maximum_inscribed_circle/1k',
        _checked(
            'gometry.maximum_inscribed_circle_1k', gometry_maximum_inscribed_circle_1k
        ),
    )
    runner.bench_func(
        f'gometry.real_world.from_geojson/{REAL_WORLD_LABEL}',
        _checked('gometry.real_world_from_geojson', gometry_real_world_from_geojson),
    )
    runner.bench_func(
        f'gometry.real_world.bounds_cold/{REAL_WORLD_LABEL}',
        _checked('gometry.real_world_bounds_cold', gometry_real_world_bounds_cold),
    )
    runner.bench_func(
        f'gometry.real_world.bounds_warm/{REAL_WORLD_LABEL}',
        _checked('gometry.real_world_bounds_warm', gometry_real_world_bounds_warm),
    )
    runner.bench_func(
        f'gometry.real_world.area_cold/{REAL_WORLD_LABEL}',
        _checked('gometry.real_world_area_cold', gometry_real_world_area_cold),
    )
    runner.bench_func(
        f'gometry.real_world.area_warm/{REAL_WORLD_LABEL}',
        _checked('gometry.real_world_area_warm', gometry_real_world_area_warm),
    )
    runner.bench_func(
        f'gometry.real_world.point_on_surface/{REAL_WORLD_LABEL}',
        _checked(
            'gometry.real_world_point_on_surface', gometry_real_world_point_on_surface
        ),
    )
    _register_shapely(runner)
    _register_rtree(runner)
    _register_h3(runner)
    _register_s2sphere(runner)
    _register_pyproj(runner)
    flush_benchmarks()


if __name__ == '__main__':
    main()
