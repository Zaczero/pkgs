from __future__ import annotations

import json
import math
from array import array
from functools import cache
from typing import Any

import numpy as np
import numpy.typing as npt
import gometry as gm
from _bench_config import queue_selected_benchmarks
from _bench_config import runner as bench_runner

POINT_COUNT = 10000
WKB_COUNT = 1000
FC_COUNT = 1000
CRS_COUNT = 1000
CRS_BOUNDS_COUNT = 1000
CRS_CHURN_COUNT = 120
CRS_DATABASE_COUNT = 120
GEODESIC_BUFFER_COUNT = 12
INDEX_DENSE_COUNT = 2000
GEOGRAPHIC_MEASURE_COUNT = 1000
MASKED_TO_CRS_COUNT = 200000
COVERAGE_PROBE_COUNT = 50000
COVERAGE_VERTEX_COUNT = 5000
P10B_PROBE_COUNT = 128
P10B_SEGMENT_COUNT = 1024
XS = tuple(math.sin(i * 0.017) * 8.0 for i in range(POINT_COUNT))
YS = tuple(math.cos(i * 0.019) * 8.0 for i in range(POINT_COUNT))
NP_XS = np.array(XS, dtype='float64')
NP_YS = np.array(YS, dtype='float64')
ZS = tuple(100.0 + math.sin(i * 0.011) * 50.0 for i in range(POINT_COUNT))
TS = tuple(2020.0 + i % 365 / 365.0 for i in range(POINT_COUNT))
BUFFER_XS = memoryview(array('d', XS))
BUFFER_YS = memoryview(array('d', YS))
BUFFER_ZS = memoryview(array('d', ZS))
BUFFER_TS = memoryview(array('d', TS))
CRS_AOI_AREA = (-75.0, 40.0, -73.0, 42.0)
CRS_AOI_XS = tuple(980000.0 + math.sin(i * 0.017) * 50000.0 for i in range(POINT_COUNT))
CRS_AOI_YS = tuple(190000.0 + math.cos(i * 0.019) * 50000.0 for i in range(POINT_COUNT))
CRS_AOI_POINTS = gm.points(CRS_AOI_XS, CRS_AOI_YS, crs=2263)
POINTS = gm.points(XS, YS, crs=4326)
POLYGON = gm.box(-9.0, -9.0, 9.0, 9.0, crs=4326)
CRS_VALUES = tuple((4326, 3857, 27700, 32634)[i % 4] for i in range(CRS_COUNT))
CRS_DECOMPOSE_VALUES = tuple(
    (27700, 7405, 9518, 32630)[i % 4] for i in range(CRS_CHURN_COUNT)
)
CRS_OPERATION_VALUES = tuple(
    ((4326, 3857), (3857, 4326), (27700, 4326), (4326, 32634))[i % 4]
    for i in range(CRS_COUNT)
)
CRS_OPERATION_AT_VALUES = tuple(
    ((4267, 4326, -75.0, 40.0), (4267, 4326, -120.0, 35.0))[i % 2]
    for i in range(CRS_COUNT)
)
CRS_ROUNDTRIP_XS = tuple(-73.0 + math.sin(i * 0.017) * 0.1 for i in range(CRS_COUNT))
CRS_ROUNDTRIP_YS = tuple(41.0 + math.cos(i * 0.019) * 0.1 for i in range(CRS_COUNT))
CRS_FACTOR_VALUES = tuple(
    ((3857, -73.0, 41.0), (32618, -73.0, 41.0))[i % 2] for i in range(CRS_COUNT)
)
CRS_FACTOR_CHURN_VALUES = tuple(
    (code, code % 100 * 6.0 - 183.0, 0.0)
    for code in list(range(32601, 32661)) + list(range(32701, 32761))
)
CRS_FACTOR_LONGITUDES = tuple(
    -73.0 + math.sin(i * 0.017) * 0.1 for i in range(CRS_COUNT)
)
CRS_FACTOR_LATITUDES = tuple(41.0 + math.cos(i * 0.019) * 0.1 for i in range(CRS_COUNT))
CRS_GEODESIC_VALUES = tuple(
    (
        (4326, -73.0, 41.0, -74.0, 42.0, None, None),
        (4267, -73.0, 41.0, -74.0, 42.0, 10.0, 110.0),
    )[i % 2]
    for i in range(CRS_COUNT)
)
CRS_GEODESIC_LON1 = tuple(-73.0 + math.sin(i * 0.017) * 0.1 for i in range(CRS_COUNT))
CRS_GEODESIC_LAT1 = tuple(41.0 + math.cos(i * 0.019) * 0.1 for i in range(CRS_COUNT))
CRS_GEODESIC_LON2 = tuple(-74.0 + math.sin(i * 0.023) * 0.1 for i in range(CRS_COUNT))
CRS_GEODESIC_LAT2 = tuple(42.0 + math.cos(i * 0.029) * 0.1 for i in range(CRS_COUNT))
CRS_GEODESIC_AZIMUTH = tuple(
    45.0 + math.sin(i * 0.013) * 10.0 for i in range(CRS_COUNT)
)
CRS_GEODESIC_DISTANCE = tuple(1000.0 + i for i in range(CRS_COUNT))
GEODESIC_DESTINATION_POINTS = gm.points(CRS_GEODESIC_LON1, CRS_GEODESIC_LAT1, crs=4326)
CRS_GEODESIC_POLYGON = gm.Polygon(
    [(-73.0, 41.0), (-72.0, 41.0), (-72.0, 42.0), (-73.0, 42.0), (-73.0, 41.0)],
    holes=[[(-72.7, 41.3), (-72.3, 41.3), (-72.3, 41.7), (-72.7, 41.7), (-72.7, 41.3)]],
    crs=4267,
)
CRS_GEODESIC_POLYGONS = gm.GeometryArray([CRS_GEODESIC_POLYGON] * CRS_COUNT)
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
    for i in range(CRS_COUNT)
)
CRS_LOCAL_GEOMETRIES = tuple(
    (
        gm.Point(21.0, 52.0, crs=4326),
        gm.Point(0.0, 89.0, crs=4326),
        gm.Point(0.0, -89.0, crs=4326),
    )[i % 3]
    for i in range(CRS_DATABASE_COUNT)
)
CRS_UPS_GEOMETRIES = tuple(
    gm.Point(0.0, 89.0 if i % 2 == 0 else -89.0, crs=4326)
    for i in range(CRS_DATABASE_COUNT)
)
GEODESIC_BUFFER_GEOMETRIES = tuple(
    (
        gm.Point(21.0, 52.0, crs=4326),
        gm.Point(0.0, 89.0, crs=4326),
        gm.Point(0.0, -89.0, crs=4326),
    )[i % 3]
    for i in range(GEODESIC_BUFFER_COUNT)
)
CRS_AFFINE_OPERATION = '+proj=pipeline +step +proj=affine +xoff=1 +yoff=2 +zoff=3'
CRS_BOUNDS_VALUES = tuple(
    (-1.0 + i * 0.001, 50.0, 1.0 + i * 0.001, 51.0) for i in range(CRS_BOUNDS_COUNT)
)
CRS_BOUNDS_3D_VALUES = tuple(
    (-73.0 + i * 0.0001, 41.0, 10.0, -72.5 + i * 0.0001, 41.5, 20.0)
    for i in range(CRS_BOUNDS_COUNT)
)
WKB_POINTS = tuple(
    (
        gm.Point(x, y, crs=4326).to_wkb(include_srid=True, precision=7)
        for x, y in zip(XS[:WKB_COUNT], YS[:WKB_COUNT], strict=False)
    )
)
BUFFER_LINES = gm.GeometryArray([
    gm.LineString([(i + t / 50.0, math.sin(t / 8.0)) for t in range(50)])
    for i in range(500)
])
POINT_LIST = [gm.Point(float(x), float(y)) for x, y in zip(NP_XS, NP_YS, strict=True)]
_GOMETRY_GEOSERIES: object | None = None
_SHAPELY_GEOSERIES: object | None = None


def _make_simple_fc(n: int) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for i in range(n):
        lon = i * 0.37 % 360.0 - 180.0
        lat = math.sin(i * 0.11) * 80.0
        features.append({
            'type': 'Feature',
            'id': i,
            'properties': {'idx': i, 'name': f'p{i}'},
            'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
        })
    return {'type': 'FeatureCollection', 'features': features}


FC_DICT = _make_simple_fc(FC_COUNT)
FC_STR = json.dumps(FC_DICT, separators=(',', ':'))


def _dense_index_box(i: int) -> gm.Geometry:
    row = i // 50
    col = i % 50
    x = col * 1.0 + ((i * 2654435761 % 1000) / 1000.0 - 0.5) * 0.6
    y = row * 1.0 + ((i * 40503 % 1000) / 1000.0 - 0.5) * 0.6
    return gm.box(x, y, x + 1.4, y + 1.4)


DENSE_INDEX_GEOMETRIES = gm.GeometryArray([
    _dense_index_box(i) for i in range(INDEX_DENSE_COUNT)
])
DENSE_INDEX = gm.SpatialIndex(DENSE_INDEX_GEOMETRIES)
MIXED_ARROW_GEOMETRIES = gm.GeometryArray([
    gm.Point(float(i), float(i % 97), crs=4326)
    if i % 3 == 0
    else gm.LineString([(float(i), 0.0), (float(i) + 0.5, 1.0)], crs=4326)
    if i % 3 == 1
    else gm.box(float(i), 0.0, float(i) + 0.25, 0.25, crs=4326)
    for i in range(POINT_COUNT)
])
MIXED_ARROW_ARRAY = MIXED_ARROW_GEOMETRIES.to_arrow()
GEOGRAPHIC_POLYGONS = gm.GeometryArray([
    gm.box(
        -73.0 + (i % 50) * 0.01,
        41.0 + (i // 50) * 0.01,
        -72.99 + (i % 50) * 0.01,
        41.01 + (i // 50) * 0.01,
        crs=4326,
    )
    for i in range(GEOGRAPHIC_MEASURE_COUNT)
])
GEOGRAPHIC_LINES = gm.GeometryArray([
    gm.LineString(
        [
            (-73.0 + (i % 50) * 0.01, 41.0 + (i // 50) * 0.01),
            (-72.99 + (i % 50) * 0.01, 41.02 + (i // 50) * 0.01),
        ],
        crs=4326,
    )
    for i in range(GEOGRAPHIC_MEASURE_COUNT)
])
MASKED_TO_CRS_XS = np.linspace(-73.25, -72.75, MASKED_TO_CRS_COUNT, dtype='float64')
MASKED_TO_CRS_YS = (
    41.0 + np.sin(np.arange(MASKED_TO_CRS_COUNT, dtype='float64') * 0.013) * 0.25
)
MASKED_TO_CRS_MASK = (np.arange(MASKED_TO_CRS_COUNT) % 10) == 0
MASKED_TO_CRS_POINTS = gm.points(
    MASKED_TO_CRS_XS, MASKED_TO_CRS_YS, crs=4326
)._with_missing(MASKED_TO_CRS_MASK)


def _jagged_coverage_source(vertices: int) -> gm.Polygon:
    coords = []
    for index in range(vertices):
        angle = math.tau * index / vertices
        radius = 1.0 + 0.12 * math.sin(11.0 * angle) + 0.05 * math.sin(29.0 * angle)
        coords.append((
            21.0 + 1.1 * radius * math.cos(angle),
            52.0 + 0.7 * radius * math.sin(angle),
        ))
    return gm.Polygon(coords, crs=4326)


@cache
def _contains_xy_fixture() -> tuple[
    gm.Geometry,
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
]:
    source = _jagged_coverage_source(COVERAGE_VERTEX_COUNT)
    xs = np.array(
        [
            19.4 + (i * 0.6180339887498949 % 1.0) * 3.2
            for i in range(COVERAGE_PROBE_COUNT)
        ],
        dtype='float64',
    )
    ys = np.array(
        [
            50.9 + (i * 0.4142135623730951 % 1.0) * 2.2
            for i in range(COVERAGE_PROBE_COUNT)
        ],
        dtype='float64',
    )
    return (source, xs, ys)


P10B_LINE = gm.LineString([
    (float(i), math.sin(i * 0.031) * 4.0, math.cos(i * 0.017) * 3.0)
    for i in range(P10B_SEGMENT_COUNT + 1)
])
P10B_PROBES = gm.points(
    np.array([float(i * 8) + 0.37 for i in range(P10B_PROBE_COUNT)], dtype='float64'),
    np.array(
        [5.0 + math.sin(i * 0.41) * 1.5 for i in range(P10B_PROBE_COUNT)],
        dtype='float64',
    ),
    z=np.array(
        [math.cos(i * 0.19) * 2.0 for i in range(P10B_PROBE_COUNT)], dtype='float64'
    ),
)


def buffer_lines_fast_path() -> gm.GeometryArray:
    result = BUFFER_LINES.buffer(0.005, quadrant_segments=8)
    return result


def buffer_lines_winding() -> gm.GeometryArray:
    result = BUFFER_LINES.buffer(0.2, quadrant_segments=8)
    return result


def build_points() -> gm.GeometryArray:
    return gm.points(XS, YS, crs=4326)


def contains_points() -> np.ndarray:
    result = gm.contains(POLYGON, POINTS)
    return result


def parse_wkb_points() -> list[gm.Geometry]:
    return [gm.from_wkb(value) for value in WKB_POINTS]


def parse_geojson_str_fc() -> gm.GeometryArray:
    result = gm.from_geojson(FC_STR)
    return result


def parse_geojson_dict_fc() -> gm.GeometryArray:
    result = gm.from_geojson(FC_DICT)
    return result


def parse_features_str_fc() -> gm.Features:
    return gm.from_features(FC_STR)


def geometryarray_from_points() -> gm.GeometryArray:
    return gm.GeometryArray(POINT_LIST, crs=4326)


def index_self_join_dense() -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    return DENSE_INDEX.self_join(predicate='intersects')


def _geopandas_fixtures() -> tuple[object, object]:
    global _GOMETRY_GEOSERIES, _SHAPELY_GEOSERIES
    if _GOMETRY_GEOSERIES is None or _SHAPELY_GEOSERIES is None:
        _GOMETRY_GEOSERIES = POINTS.to_pandas()
        _SHAPELY_GEOSERIES = POINTS.to_geopandas()
    return (_GOMETRY_GEOSERIES, _SHAPELY_GEOSERIES)


def from_pandas_extension() -> gm.GeometryArray:
    series, _ = _geopandas_fixtures()
    result = gm.from_pandas(series)
    return result


def from_geopandas_shapely_wkb() -> gm.GeometryArray:
    _, series = _geopandas_fixtures()
    result = gm.from_geopandas(series)
    return result


def to_arrow_mixed() -> object:
    return MIXED_ARROW_GEOMETRIES.to_arrow()


def from_arrow_mixed() -> gm.GeometryArray:
    return gm.from_arrow(MIXED_ARROW_ARRAY)


def geographic_polygon_area() -> np.ndarray:
    return GEOGRAPHIC_POLYGONS.area


def geographic_line_length() -> np.ndarray:
    return GEOGRAPHIC_LINES.length


def masked_to_crs() -> gm.GeometryArray:
    return MASKED_TO_CRS_POINTS.to_crs(3857)


def polygon_contains_xy() -> np.ndarray:
    source, xs, ys = _contains_xy_fixture()
    return gm.contains_xy(source, xs, ys)


def distance_3d_segment_bvh() -> np.ndarray:
    return gm.distance_3d(P10B_PROBES, P10B_LINE)


def crs_info() -> list[dict[str, object]]:
    return [gm.crs_info(value) for value in CRS_VALUES]


def crs_operation() -> list[dict[str, object]]:
    return [gm.CRS(source).operation(target) for source, target in CRS_OPERATION_VALUES]


def crs_operation_at() -> list[dict[str, object]]:
    return [
        gm.CRS(source).operation(target, at=(x, y))
        for source, target, x, y in CRS_OPERATION_AT_VALUES
    ]


def crs_roundtrip() -> list[float]:
    return gm.crs_roundtrip(4326, 3857, CRS_ROUNDTRIP_XS, CRS_ROUNDTRIP_YS)


def crs_factors() -> list[dict[str, object]]:
    return [
        gm.CRS(target).factors(longitude, latitude)
        for target, longitude, latitude in CRS_FACTOR_VALUES
    ]


def crs_factors_batch() -> dict[str, object]:
    return gm.CRS(32618).factors(CRS_FACTOR_LONGITUDES, CRS_FACTOR_LATITUDES)


def crs_geodesic() -> list[dict[str, object]]:
    return [
        gm.CRS(crs).geodesic_inverse(lon1, lat1, lon2, lat2, z1=z1, z2=z2)
        for crs, lon1, lat1, lon2, lat2, z1, z2 in CRS_GEODESIC_VALUES
    ]


def crs_geodesic_batch() -> dict[str, object]:
    return gm.CRS(4326).geodesic_inverse(
        CRS_GEODESIC_LON1, CRS_GEODESIC_LAT1, CRS_GEODESIC_LON2, CRS_GEODESIC_LAT2
    )


def crs_geodesic_direct_batch() -> dict[str, object]:
    return gm.CRS(4326).geodesic_direct(
        CRS_GEODESIC_LON1,
        CRS_GEODESIC_LAT1,
        CRS_GEODESIC_AZIMUTH,
        CRS_GEODESIC_DISTANCE,
    )


def geodesic_destination_batch() -> gm.GeometryArray:
    return GEODESIC_DESTINATION_POINTS.destination(
        CRS_GEODESIC_AZIMUTH, CRS_GEODESIC_DISTANCE
    )


def crs_geodesic_interpolate_batch() -> dict[str, object]:
    return gm.CRS(4326).geodesic_interpolate(
        CRS_GEODESIC_LON1,
        CRS_GEODESIC_LAT1,
        CRS_GEODESIC_LON2,
        CRS_GEODESIC_LAT2,
        0.5,
        normalized=True,
    )


def crs_geodesic_geometry_batch() -> tuple[list[float], list[float]]:
    return (
        [geometry.set_crs(4267).area for geometry in CRS_GEODESIC_POLYGONS],
        [geometry.set_crs(4267).length for geometry in CRS_GEODESIC_POLYGONS],
    )


def geodesic_buffer_local() -> list[gm.Geometry]:
    return [geometry.buffer(100.0) for geometry in GEODESIC_BUFFER_GEOMETRIES]


def crs_operations() -> list[list[dict[str, object]]]:
    return [
        gm.CRS(source).operations(target) for source, target in CRS_OPERATION_VALUES
    ]


def crs_static_catalogs() -> tuple[
    list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]
]:
    return (gm.crs_proj_operations(), gm.crs_ellipsoids(), gm.crs_prime_meridians())


def crs_authority_conversion() -> list[
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


def crs_cf() -> list[dict[str, object]]:
    return [gm.CRS(value).to_cf() for value in CRS_AUTHORITY_VALUES]


def crs_info_churn() -> list[dict[str, object]]:
    return [gm.crs_info(value) for value in CRS_CHURN_VALUES]


def crs_info_decompose() -> list[dict[str, object]]:
    return [gm.crs_info(value) for value in CRS_DECOMPOSE_VALUES]


def crs_operation_churn() -> list[dict[str, object]]:
    return [
        gm.CRS(source).operation(target)
        for source, target in CRS_OPERATION_CHURN_VALUES
    ]


def crs_operation_cold() -> list[dict[str, object]]:
    return [
        gm.CRS(source).operation(target) for source, target in CRS_OPERATION_COLD_VALUES
    ]


def crs_cache_info() -> list[dict[str, object]]:
    return [gm.crs_cache_info() for _ in range(CRS_DATABASE_COUNT)]


def crs_factors_churn() -> list[dict[str, object]]:
    return [
        gm.CRS(crs).factors(longitude, latitude)
        for crs, longitude, latitude in CRS_FACTOR_CHURN_VALUES
    ]


def crs_transform() -> np.ndarray:
    return gm.crs_transform(4326, 3857, XS, YS)


def crs_transform_numpy() -> np.ndarray:
    return gm.crs_transform(4326, 3857, NP_XS, NP_YS)


def crs_transform_buffer() -> np.ndarray:
    return gm.crs_transform(4326, 3857, BUFFER_XS, BUFFER_YS)


def crs_transform_aoi() -> np.ndarray:
    return gm.crs_transform(
        2263, 4326, CRS_AOI_XS, CRS_AOI_YS, area_of_interest=CRS_AOI_AREA
    )


def to_crs_aoi_options() -> gm.GeometryArray:
    return CRS_AOI_POINTS.to_crs(
        4326,
        area_of_interest=CRS_AOI_AREA,
        authority='EPSG',
        accuracy=10.0,
        allow_ballpark=False,
    )


def crs_transform_3d() -> np.ndarray:
    return gm.crs_transform(4979, 4978, XS, YS, ZS)


def crs_transform_4d() -> np.ndarray:
    return gm.crs_transform(4979, 4978, XS, YS, ZS, t=TS)


def crs_apply() -> tuple[list[float], list[float], list[float], list[float]]:
    return gm.crs_apply(CRS_AFFINE_OPERATION, XS, YS, ZS, t=TS)


def crs_apply_buffer() -> tuple[list[float], list[float], list[float], list[float]]:
    return gm.crs_apply(
        CRS_AFFINE_OPERATION, BUFFER_XS, BUFFER_YS, BUFFER_ZS, t=BUFFER_TS
    )


def crs_apply_inverse() -> tuple[list[float], list[float], list[float], list[float]]:
    return gm.crs_apply(CRS_AFFINE_OPERATION, XS, YS, ZS, t=TS, direction='inverse')


def crs_transform_bounds() -> list[tuple[float, float, float, float]]:
    return [gm.crs_transform_bounds(4326, 3857, bounds) for bounds in CRS_BOUNDS_VALUES]


def crs_transform_bounds_3d() -> list[tuple[float, float, float, float, float, float]]:
    return [
        gm.crs_transform_bounds(4979, 4978, bounds) for bounds in CRS_BOUNDS_3D_VALUES
    ]


def crs_transform_bounds_3d_corners() -> list[
    tuple[float, float, float, float, float, float]
]:
    return [
        gm.crs_transform_bounds(4979, 4978, bounds, densify=0)
        for bounds in CRS_BOUNDS_3D_VALUES
    ]


def crs_catalog() -> list[list[dict[str, object]]]:
    return [
        gm.crs_catalog(
            authority='EPSG', kind='projected', area_of_interest=(-1.0, 50.0, 1.0, 52.0)
        )
        for _ in range(CRS_DATABASE_COUNT)
    ]


def crs_utm_zones() -> list[list[dict[str, object]]]:
    return [
        gm.crs_utm_zones(datum_name='WGS 84', area_of_interest=(20.0, 51.0, 22.0, 53.0))
        for _ in range(CRS_DATABASE_COUNT)
    ]


def crs_geodetic_crs_from_datum() -> list[list[dict[str, object]]]:
    return [
        gm.crs_catalog(authority='EPSG', kind='geographic_2d')
        for _ in range(CRS_DATABASE_COUNT)
    ]


def crs_lookup() -> list[list[str]]:
    return [
        gm.crs_codes(
            'EPSG', kind=('ellipsoid', 'prime_meridian', 'datum_ensemble')[i % 3]
        )
        for i in range(CRS_DATABASE_COUNT)
    ]


def crs_estimate_local() -> list[gm.CRS]:
    return [geometry.estimate_local_crs() for geometry in CRS_LOCAL_GEOMETRIES]


def crs_estimate_polar() -> list[gm.CRS]:
    return [geometry.estimate_local_crs() for geometry in CRS_UPS_GEOMETRIES]


def crs_units() -> list[list[dict[str, object]]]:
    return [gm.crs_units('EPSG', category='linear') for _ in range(CRS_DATABASE_COUNT)]


def crs_celestial_bodies() -> list[list[dict[str, object]]]:
    return [gm.crs_celestial_bodies() for _ in range(CRS_DATABASE_COUNT)]


def crs_non_deprecated() -> list[list[dict[str, object]]]:
    return [gm.CRS(2037).non_deprecated for _ in range(CRS_DATABASE_COUNT)]


def crs_search() -> list[list[dict[str, object]]]:
    return [
        gm.crs_search('British National Grid', authority='EPSG', kind='projected')
        for _ in range(CRS_DATABASE_COUNT)
    ]


def crs_exports() -> list[tuple[str, str, str, str, dict[str, object]]]:
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


def crs_same() -> list[bool]:
    # mode='exact' matches pyproj CRS.equals default; same_as requires mode=.
    wkt = gm.CRS(4326).to_wkt()
    return [gm.CRS(4326).same_as(wkt, mode='exact') for _ in range(CRS_DATABASE_COUNT)]


def main() -> None:
    runner = bench_runner()
    runner.metadata['project'] = 'gometry'
    runner.metadata['fixture'] = 'deterministic-trig-points'
    flush_benchmarks = queue_selected_benchmarks(runner, 'gometry')
    runner.bench_func('gometry.points/10k', build_points)
    runner.bench_func('gometry.contains/polygon_points_10k', contains_points)
    # Old point-only precision-7 WKB row removed; public mixed-EWKB supersedes.
    runner.bench_func('gometry.from_geojson.str_fc/1k', parse_geojson_str_fc)
    runner.bench_func('gometry.from_geojson.dict_fc/1k', parse_geojson_dict_fc)
    runner.bench_func('gometry.from_features.str/1k', parse_features_str_fc)
    runner.bench_func(
        'gometry.geometryarray.from_points/10k',
        geometryarray_from_points,
    )
    runner.bench_func(
        'gometry.index.self_join/dense_2k',
        index_self_join_dense,
    )
    runner.bench_func(
        'gometry.from_pandas.extension/10k',
        from_pandas_extension,
    )
    runner.bench_func(
        'gometry.from_geopandas.shapely_wkb/10k',
        from_geopandas_shapely_wkb,
    )
    runner.bench_func('gometry.to_arrow.mixed/10k', to_arrow_mixed)
    runner.bench_func('gometry.from_arrow.mixed/10k', from_arrow_mixed)
    runner.bench_func(
        'gometry.area/geographic_polygons_1k',
        geographic_polygon_area,
    )
    runner.bench_func(
        'gometry.length/geographic_lines_1k',
        geographic_line_length,
    )
    runner.bench_func(
        'gometry.to_crs.masked/200k_10pct_missing',
        masked_to_crs,
    )
    runner.bench_func(
        'gometry.contains_xy/jagged_5k_50k',
        polygon_contains_xy,
    )
    runner.bench_func(
        'gometry.distance_3d/128x1024_segments',
        distance_3d_segment_bvh,
    )
    runner.bench_func(
        'gometry.buffer.lines_fast/500x50v',
        buffer_lines_fast_path,
    )
    runner.bench_func(
        'gometry.buffer.lines_winding/500x50v',
        buffer_lines_winding,
    )
    runner.bench_func('gometry.crs_info/1k', crs_info)
    runner.bench_func(
        'gometry.crs_info_decompose/120',
        crs_info_decompose,
    )
    runner.bench_func('gometry.crs_operation_warm/1k', crs_operation)
    runner.bench_func('gometry.crs_operation_at/1k', crs_operation_at)
    runner.bench_func('gometry.crs_roundtrip/1k', crs_roundtrip)
    runner.bench_func('gometry.crs_factors/1k', crs_factors)
    runner.bench_func('gometry.crs_factors_batch/1k', crs_factors_batch)
    runner.bench_func('gometry.crs_geodesic/1k', crs_geodesic)
    runner.bench_func(
        'gometry.crs_geodesic_batch/1k',
        crs_geodesic_batch,
    )
    runner.bench_func(
        'gometry.crs_geodesic_direct_batch/1k',
        crs_geodesic_direct_batch,
    )
    # Cross-suite alias gometry.geodesic.destination_batch/1k deleted.
    runner.bench_func(
        'gometry.crs_geodesic_interpolate_batch/1k',
        crs_geodesic_interpolate_batch,
    )
    runner.bench_func(
        'gometry.crs_geodesic_geometry_batch/1k',
        crs_geodesic_geometry_batch,
    )
    runner.bench_func(
        'gometry.geodesic.buffer_local/12',
        geodesic_buffer_local,
    )
    runner.bench_func('gometry.crs_operations_warm/1k', crs_operations)
    runner.bench_func(
        'gometry.crs_static_catalogs/120',
        crs_static_catalogs,
    )
    runner.bench_func(
        'gometry.crs_authority_conversion/120',
        crs_authority_conversion,
    )
    runner.bench_func('gometry.crs_cf/120', crs_cf)
    runner.bench_func('gometry.crs_info_churn/120', crs_info_churn)
    runner.bench_func(
        'gometry.crs_operation_churn/120',
        crs_operation_churn,
    )
    runner.bench_func(
        'gometry.crs_operation_cold/1k',
        crs_operation_cold,
    )
    runner.bench_func(
        'gometry.crs_factors_churn/120',
        crs_factors_churn,
    )
    runner.bench_func('gometry.crs_cache_info/120', crs_cache_info)
    runner.bench_func('gometry.crs_transform/10k', crs_transform)
    runner.bench_func(
        'gometry.crs_transform_numpy/10k',
        crs_transform_numpy,
    )
    runner.bench_func(
        'gometry.crs_transform_buffer/10k',
        crs_transform_buffer,
    )
    runner.bench_func(
        'gometry.crs_transform_aoi/10k',
        crs_transform_aoi,
    )
    runner.bench_func(
        'gometry.to_crs_aoi_options/10k',
        to_crs_aoi_options,
    )
    runner.bench_func('gometry.crs_transform_3d/10k', crs_transform_3d)
    runner.bench_func('gometry.crs_transform_4d/10k', crs_transform_4d)
    runner.bench_func('gometry.crs_apply/10k', crs_apply)
    runner.bench_func('gometry.crs_apply_buffer/10k', crs_apply_buffer)
    runner.bench_func(
        'gometry.crs_apply_inverse/10k',
        crs_apply_inverse,
    )
    runner.bench_func(
        'gometry.crs_transform_bounds/1k',
        crs_transform_bounds,
    )
    runner.bench_func(
        'gometry.crs_transform_bounds_3d/1k',
        crs_transform_bounds_3d,
    )
    runner.bench_func(
        'gometry.crs_transform_bounds_3d_corners/1k',
        crs_transform_bounds_3d_corners,
    )
    runner.bench_func('gometry.crs_list/120', crs_catalog)
    runner.bench_func('gometry.crs_utm_zones/120', crs_utm_zones)
    runner.bench_func(
        'gometry.crs_geodetic_crs_from_datum/120',
        crs_geodetic_crs_from_datum,
    )
    runner.bench_func('gometry.crs_lookup/120', crs_lookup)
    runner.bench_func(
        'gometry.geometry.estimate_local_crs/120',
        crs_estimate_local,
    )
    runner.bench_func(
        'gometry.geometry.estimate_local_crs.polar/120',
        crs_estimate_polar,
    )
    runner.bench_func('gometry.crs_units/120', crs_units)
    runner.bench_func(
        'gometry.crs_celestial_bodies/120',
        crs_celestial_bodies,
    )
    runner.bench_func(
        'gometry.crs_non_deprecated/120',
        crs_non_deprecated,
    )
    runner.bench_func('gometry.crs_search/120', crs_search)
    runner.bench_func('gometry.crs_exports/120', crs_exports)
    runner.bench_func('gometry.crs_same/120', crs_same)
    _register_public_release_ops(runner, 'gometry')
    flush_benchmarks()


def _register_public_release_ops(runner: Any, suite: str) -> None:
    """Register Lane-2 public timed callables (selected rows only; lazy fixtures)."""
    from _bench_config import register_selected_public_release_ops

    register_selected_public_release_ops(runner, suite)


if __name__ == '__main__':
    main()
