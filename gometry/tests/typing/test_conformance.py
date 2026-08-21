"""Static conformance for the stub's overload narrowing.

Every ``assert_type`` here is checked by a targeted pyright run in
``tools/gates/_check_typing_runtime.py`` (pyproject scopes the default pyright gate
to ``python/gometry`` only, so pytest alone does not type-check this file).
A wrong overload order, a TypeVar that stops flowing, or an orphaned overload
set therefore fails the typing-runtime gate — the ``h3_cell`` overloads were
once silently obscured by a stray bare def, which only a static probe like
this catches. At runtime ``assert_type`` is a no-op; the tiny fixtures keep
the module cheap to execute as an ordinary test, and the single test
function asserts the few values whose runtime class must match the narrowed
static type.
"""

# pandas and GeoPandas do not ship mypy-readable inline typing metadata; the
# pyright lane supplies their external stubs while both checkers still verify
# gometry's native method signatures and return narrowing.
# mypy: disable-error-code="import-untyped"

from __future__ import annotations

from typing import TYPE_CHECKING, Any, assert_type, cast

import gometry as gm
import gometry._types as sht
import numpy as np
import numpy.typing as npt

POINT = gm.Point(1, 1)
LINE = gm.LineString([(0, 0), (1, 1), (2, 0)])
MEASURED_LINE = gm.LineString([(0, 0, 0, 0), (1, 1, 0, 10), (2, 0, 0, 20)])
POLY = gm.box(0, 0, 2, 2)
POINTS = gm.GeometryArray([POINT, gm.Point(2, 2)])
LINES = gm.GeometryArray([LINE])
POLYS = gm.GeometryArray([POLY])


class _ArrowStream:
    def __arrow_c_stream__(self, requested_schema: object | None = None, /) -> object:
        return object()


class _ArrowArray:
    def __arrow_c_array__(
        self, requested_schema: object | None = None, /
    ) -> tuple[object, object]:
        return (object(), object())


class _GeoInterfaceObject:
    @property
    def __geo_interface__(self) -> Mapping[str, Any]:
        return {'type': 'Point', 'coordinates': [3.0, 4.0]}


assert_type(LINE.simplify(0.1), gm.LineString)
assert_type(POINT.reverse(), gm.Point)
assert_type(gm.snap(LINE, POINT, 0.5), gm.LineString)
assert_type(
    gm.GeometryArray([LINE]).polygonize(), gm.Groups[gm.GeometryArray[gm.Polygon]]
)
assert_type(gm.polygonize_full([LINE]), gm.PolygonizeResult)
assert_type(POINTS.simplify(0.1, method='vw'), gm.GeometryArray[gm.Point])
assert_type(cast('gm.Polygon', POINT.buffer(1.0)), gm.Polygon)
assert_type(cast('gm.Polygon', LINE.buffer(0.5)), gm.Polygon)
assert_type(cast('gm.MultiPoint', LINE.boundary()), gm.MultiPoint)
assert_type(POINT.centroid(), gm.Point)
assert_type(hash(POINT), int)
assert_type(POLY.exterior, gm.LineString)

# Scalar<->array return duality (the 32 `-> Self` lies, gated by pyo3stubs
# return-parity): kind-changing array methods return the scalar leaf union,
# kind-preserving ones return Self.
assert_type(POINTS.buffer(1.0), gm.GeometryArray[gm.Polygon | gm.MultiPolygon])
assert_type(POINTS.centroid(), gm.GeometryArray[gm.Point])
assert_type(
    POINTS.convex_hull(),
    gm.GeometryArray[gm.Point | gm.LineString | gm.Polygon | gm.GeometryCollection],
)
assert_type(POLYS.triangulate(method='earcut'), gm.Groups[gm.GeometryArray[gm.Polygon]])
assert_type(
    POINTS.triangulate(method='delaunay'), gm.Groups[gm.GeometryArray[gm.Polygon]]
)
assert_type(
    POLYS.triangulate(method='constrained', min_angle=25.0, max_area=1.0),
    gm.Groups[gm.GeometryArray[gm.Polygon]],
)
assert_type(
    POLYS.triangulate(method='constrained', min_angle=[25.0], max_area=[1.0]),
    gm.Groups[gm.GeometryArray[gm.Polygon]],
)
buffered = POINTS.buffer(1.0)[0]
assert buffered is not None
assert_type(buffered.area, float)
assert_type(gm.require(POINTS.set_crs(4326), crs=4326), gm.GeometryArray[gm.Point])
assert_type(gm.require(POINTS), gm.GeometryArray[gm.Point])
assert_type(POLY.polylabel(), gm.Point)
assert_type(POLY.minimum_bounding_radius(), float)
assert_type(POINTS.minimum_bounding_radius(), npt.NDArray[np.float64])
assert_type(
    gm.hausdorff_distance(LINES, LINE, densify=[0.5]),
    npt.NDArray[np.float64],
)
assert_type(
    gm.frechet_distance(LINES, LINE, densify=[0.5]),
    npt.NDArray[np.float64],
)
assert_type(POLY.minimum_clearance_line(), gm.LineString)
assert_type(cast('gm.Polygon', POLY.snap_to_grid(0.5)), gm.Polygon)
assert_type(POLY.snap_to_grid(0.5, repair=True), gm.Geometry)
assert_type(
    gm.nearest_points(POINTS, LINE),
    tuple[gm.GeometryArray[gm.Point], gm.GeometryArray[gm.Point]],
)
assert_type(gm.nearest_points(POINTS, LINE)[0], gm.GeometryArray[gm.Point])
assert_type(
    gm.nearest_points(LINE, POINTS),
    tuple[gm.GeometryArray[gm.Point], gm.GeometryArray[gm.Point]],
)
assert_type(gm.split(gm.GeometryArray([LINE]), POINT), gm.GeometryArray[gm.LineString])
assert_type(gm.from_wkt('POINT (0 0)'), gm.Geometry)
assert_type(gm.from_wkt(['POINT (0 0)']), gm.GeometryArray[gm.Geometry])
assert_type(POINT, gm.Point)
assert_type(POINTS, gm.GeometryArray[gm.Point])
assert_type(gm.MultiPoint([POINT, gm.Point(2, 2)]), gm.MultiPoint)
if TYPE_CHECKING:
    from collections.abc import Mapping

    import geopandas as gpd
    import pandas as pd
    import polars as pl

    pandas_series = POINTS.to_pandas()
    assert_type(pandas_series, pd.Series)
    assert_type(gm.from_pandas(pandas_series), gm.GeometryArray[gm.Geometry])

    polars_series = POINTS.to_polars()
    assert_type(polars_series, pl.Series)
    assert_type(gm.from_polars(polars_series), gm.GeometryArray[gm.Geometry])

    geopandas_series = POINTS.to_geopandas()
    assert_type(geopandas_series, gpd.GeoSeries)
    assert_type(gm.from_geopandas(geopandas_series), gm.GeometryArray[gm.Geometry])

    float_column: sht.FloatColumn = [1.0, 2.0]
    navigation_path: sht.NavigationPath = 'rhumb'
    features = gm.Features(POINTS)
    assert_type(POINTS.to_geoparquet('points.parquet', encoding='native'), None)
    assert_type(
        gm.to_feature_collection(features, properties=None, ids=None),
        sht.GeoJsonFeatureCollection,
    )
    geometry_like_rows: list[
        gm.Geometry | bytes | Mapping[str, Any] | _GeoInterfaceObject | None
    ] = [POINT, POINT.to_wkb(), _GeoInterfaceObject(), None]
    present_geometry_like_rows: list[
        gm.Geometry | bytes | Mapping[str, Any] | _GeoInterfaceObject
    ] = [POINT, POINT.to_wkb(), _GeoInterfaceObject()]
    binary_geometry_rows: list[gm.Geometry | bytes] = [POINT, POINT.to_wkb()]
    mixed_array = gm.GeometryArray(geometry_like_rows)
    assert_type(mixed_array, gm.GeometryArray[gm.Geometry])
    mixed_index = gm.SpatialIndex(geometry_like_rows)
    assert_type(mixed_index, gm.SpatialIndex)
    assert_type(mixed_index.insert(binary_geometry_rows), npt.NDArray[np.int64])
    assert_type(
        mixed_index.join(present_geometry_like_rows),
        tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]],
    )
    assert_type(gm.nearest(geometry_like_rows, POINT), npt.NDArray[np.int64])
    assert_type(
        gm.join(present_geometry_like_rows, geometry_like_rows),
        tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]],
    )
    assert_type(gm.bounds(geometry_like_rows), npt.NDArray[np.float64])
    assert_type(gm.union_all(geometry_like_rows), gm.Geometry)
    assert_type(gm.intersection_all(geometry_like_rows), gm.Geometry)
    assert_type(gm.symmetric_difference_all(geometry_like_rows), gm.Geometry)
    assert_type(gm.get_coordinates(geometry_like_rows), npt.NDArray[np.float64])
    assert_type(gm.polygonize(present_geometry_like_rows), gm.GeometryArray[gm.Polygon])
    assert_type(gm.coverage_is_valid([POLY.to_wkb()]), bool)
    assert_type(POINT.scale(2.0, origin=[0.0, 0.0]), gm.Point)
    assert_type(POINT.rotate(45.0, origin=iter([0.0, 0.0])), gm.Point)
    assert_type(
        POINT.triangulate(method='earcut', min_angle=None, max_area=None),
        gm.GeometryArray[gm.Polygon],
    )
    assert_type(
        POINTS.triangulate(method='delaunay', min_angle=None, max_area=None),
        gm.Groups[gm.GeometryArray[gm.Polygon]],
    )
    assert_type(LINE.segmentize(1.0, fraction=None), gm.LineString)
    assert_type(LINE.segmentize(None, fraction=0.5), gm.LineString)
    assert_type(
        MEASURED_LINE.line_substring(0.0, 10.0, basis='m', normalized=False, unit=None),
        gm.LineString | gm.Point,
    )
    assert_type(
        MEASURED_LINE.line_locate(POINT, basis='m', normalized=False, unit=None),
        float,
    )
    assert_type(
        LINE.line_interpolate(0.5, count=None),
        gm.Point,
    )
    assert_type(
        LINE.line_interpolate(None, count=2),
        gm.GeometryArray[gm.Point],
    )
    assert_type(
        MEASURED_LINE.line_interpolate(
            10.0, count=None, basis='m', normalized=False, unit=None
        ),
        gm.Point,
    )
    assert_type(
        gm.box(170.0, -10.0, -170.0, 10.0, crs=4326, wrap='split'),
        gm.Polygon | gm.MultiPolygon,
    )
    assert_type(
        gm.boxes([170.0], [-10.0], [-170.0], [10.0], crs=4326, wrap='split'),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon],
    )
    arrow_array: sht.SupportsArrowArray = _ArrowArray()
    arrow_stream: sht.SupportsArrowStream = _ArrowStream()
    assert_type(gm.from_arrow(arrow_array), gm.GeometryArray[gm.Geometry])
    assert_type(gm.from_arrow(arrow_stream), gm.GeometryArray[gm.Geometry])
    assert_type(gm.bearing(POINT, gm.Point(2, 2), path='rhumb'), float)
    assert_type(
        gm.bearing(POINTS, POINT, path=navigation_path), npt.NDArray[np.float64]
    )
    assert_type(POINT.destination(90.0, 1_000.0, path='rhumb', unit=None), gm.Point)
    assert_type(
        POINTS.destination(90.0, 1_000.0, path=navigation_path),
        gm.GeometryArray[gm.Point],
    )
    assert_type(POINT.destination([90.0], 1_000.0), gm.GeometryArray[gm.Point])
    assert_type(POINT.destination(90.0, [1_000.0]), gm.GeometryArray[gm.Point])
    assert_type(
        POINT.destination([90.0], [1_000.0], path='rhumb'),
        gm.GeometryArray[gm.Point],
    )
    scalar_array: np.ndarray[tuple[()], np.dtype[np.float64]] = np.array(90.0)
    assert_type(POINT.destination(scalar_array, 1_000.0), gm.Point)
    assert_type(gm.point_between(POINT, POINT, scalar_array), gm.Point)
    scalar_float32: np.ndarray[tuple[()], np.dtype[np.float32]] = np.array(
        90.0, dtype=np.float32
    )
    scalar_int: np.ndarray[tuple[()], np.dtype[np.int64]] = np.array(90, dtype=np.int64)
    assert_type(POINT.destination(scalar_float32, 1_000.0), gm.Point)
    assert_type(POINT.destination(scalar_int, 1_000.0), gm.Point)
    assert_type(gm.rhumb_distance(POINT, POINT), float)
    assert_type(gm.rhumb_distance(POINTS, POINT), npt.NDArray[np.float64])
    assert_type(
        gm.point_between(
            POINT,
            gm.Point(2, 2),
            0.5,
            normalized=True,
            path='rhumb',
            unit=None,
        ),
        gm.Point,
    )
    assert_type(
        gm.point_between(POINTS, POINT, 0.5, path=navigation_path, normalized=True),
        gm.GeometryArray[gm.Point],
    )
    assert_type(gm.points([1.0], [2.0]), gm.GeometryArray[gm.Point])
    assert_type(gm.boxes([0.0], [0.0], [1.0], [1.0]), gm.GeometryArray[gm.Polygon])
    assert_type(gm.h3_cells([1.0], [2.0], resolution=5), gm.CellArray[gm.H3Cell])
    assert_type(gm.H3VertexArray(['20a194e699ab7fff']), gm.H3VertexArray)
assert_type(gm.H3Cell(POINT, resolution=9), gm.H3Cell)
assert_type(gm.pluscode_encode(8.0, 47.0), str)
assert_type(gm.pluscode_polygon('8FVC2222+22'), gm.Polygon)
assert_type(gm.osm_shortlink_encode(8.0, 47.0), str)
assert_type(gm.osm_shortlink_location('0MbEUxVoG-'), tuple[float, float, int])
assert_type(gm.h3_cells([1.0, 2.0], [2.0, 3.0], resolution=9), gm.CellArray[gm.H3Cell])
assert_type(gm.h3_cells(POINTS, resolution=9), gm.CellArray[gm.H3Cell])
# A coarse resolution on purpose: `assert_type` is a runtime no-op, but Python
# still *evaluates* the argument, and this module is imported by every xdist
# worker.  `resolution=9` over a 2-degree box measured 3.55s per worker (~114
# CPU-seconds across 32) purely to build a value that is then discarded.  The
# narrowed static type depends only on the argument *types*, never on the
# resolution value, so pyright checks exactly the same thing at 2.
assert_type(
    gm.h3_cover(gm.GeometryArray([POLY]), resolution=2),
    gm.Groups[gm.CellArray[gm.H3Cell]],
)
assert_type(gm.h3_cover(POLY, resolution=2), gm.CellArray[gm.H3Cell])
h3_cell = gm.H3Cell(POINT, resolution=9)
assert_type(gm.CellArray([h3_cell]), gm.CellArray[gm.H3Cell])
assert_type(
    gm.CellArray([h3_cell.id], type=gm.H3Cell),
    gm.CellArray[gm.H3Cell],
)
h3_vertices = h3_cell.vertices
h3_edges = h3_cell.edges
assert_type(h3_vertices, gm.H3VertexArray)
assert_type(h3_vertices[0], gm.H3Vertex)
assert_type(h3_vertices[:2], gm.H3VertexArray)
assert_type(h3_vertices[[True, False, True, False, True, False]], gm.H3VertexArray)
assert_type(h3_vertices[np.asarray([0, 1], dtype=np.int64)], gm.H3VertexArray)
assert_type(gm.H3VertexArray(h3_vertices.token), gm.H3VertexArray)
assert_type(gm.H3VertexArray(h3_vertices.to_numpy()), gm.H3VertexArray)
assert_type(h3_vertices.point, gm.GeometryArray[gm.Point])
assert_type(h3_vertices[0].point, gm.Point)
assert_type(h3_edges, gm.H3EdgeArray)
assert_type(h3_edges[0], gm.H3Edge)
assert_type(h3_edges[:2], gm.H3EdgeArray)
assert_type(h3_edges[[True, False, True, False, True, False]], gm.H3EdgeArray)
assert_type(h3_edges[np.asarray([0, 1], dtype=np.int64)], gm.H3EdgeArray)
assert_type(gm.H3EdgeArray(h3_edges.token), gm.H3EdgeArray)
assert_type(gm.H3EdgeArray(h3_edges.to_numpy()), gm.H3EdgeArray)
assert_type(h3_edges.origin, gm.CellArray[gm.H3Cell])
assert_type(h3_edges.destination, gm.CellArray[gm.H3Cell])
assert_type(h3_edges.reverse(), gm.H3EdgeArray)
assert_type(h3_edges.line, gm.GeometryArray[gm.LineString])
assert_type(h3_edges.length, npt.NDArray[np.float64])
h3_cells = gm.h3_cells([1.0, 2.0], [2.0, 3.0], resolution=9)
assert_type(h3_cells.contains(h3_cell), npt.NDArray[np.bool_])
assert_type(h3_cells.intersects(h3_cells), npt.NDArray[np.bool_])
assert_type(
    gm.h3_cells([1.0, 2.0], [2.0, 3.0], resolution=9).value_counts(),
    tuple[gm.CellArray[gm.H3Cell], npt.NDArray[np.int64]],
)
assert_type(
    gm.h3_cells([1.0, 2.0], [2.0, 3.0], resolution=9).factorize(),
    tuple[npt.NDArray[np.int64], gm.CellArray[gm.H3Cell]],
)
assert_type(gm.S2Cell(POINT, level=12), gm.S2Cell)
assert_type(gm.GeohashCell(POINT, precision=7), gm.GeohashCell)
assert_type(gm.Tile(POINT, zoom=10), gm.Tile)
s2_cells = gm.s2_cells(POINTS, level=12)
geohash_cells = gm.geohash_cells(POINTS, precision=7)
tile_cells = gm.tile_cells(POINTS, zoom=10)
assert_type(s2_cells, gm.CellArray[gm.S2Cell])
assert_type(geohash_cells, gm.CellArray[gm.GeohashCell])
assert_type(tile_cells, gm.CellArray[gm.Tile])
assert_type(s2_cells.contains(gm.S2Cell(POINT, level=12)), npt.NDArray[np.bool_])
assert_type(
    geohash_cells.intersects(gm.GeohashCell(POINT, precision=7)),
    npt.NDArray[np.bool_],
)
assert_type(tile_cells.contains(gm.Tile(POINT, zoom=10)), npt.NDArray[np.bool_])
assert_type(
    gm.s2_cover(gm.GeometryArray([POLY]), level=12),
    gm.Groups[gm.CellArray[gm.S2Cell]],
)
assert_type(gm.s2_cover(POLY, level=12), gm.CellArray[gm.S2Cell])
assert_type(
    gm.geohash_cover(gm.GeometryArray([POLY]), precision=5),
    gm.Groups[gm.CellArray[gm.GeohashCell]],
)
assert_type(gm.geohash_cover(POLY, precision=5), gm.CellArray[gm.GeohashCell])
assert_type(
    gm.tile_cover(gm.GeometryArray([POLY]), zoom=10),
    gm.Groups[gm.CellArray[gm.Tile]],
)
assert_type(gm.tile_cover(POLY, zoom=10), gm.CellArray[gm.Tile])
H3_GROUPS = gm.h3_cover(gm.GeometryArray([POLY]), resolution=2)
assert_type(H3_GROUPS.index(H3_GROUPS[0]), int)
assert_type(H3_GROUPS.count(H3_GROUPS[0]), int)
assert_type(
    gm.h3_cover(POLY, resolution=2).to_numpy(),
    npt.NDArray[np.uint64] | npt.NDArray[np.object_],
)
INDEX = gm.SpatialIndex(POINTS)
assert_type(INDEX.crs, gm.CRS | None)
assert_type(INDEX.epoch, float | None)
assert_type(POLY.prepare().geometry, gm.Geometry)
assert_type(INDEX.query(POINT), npt.NDArray[np.int64])
assert_type(POINTS[INDEX.query(POINT)], gm.GeometryArray[gm.Point])
if TYPE_CHECKING:
    assert_type(INDEX.query(POINTS), gm.Groups[npt.NDArray[np.int64]])
else:
    assert isinstance(INDEX.query(POINTS), gm.Groups)
assert_type(INDEX.self_join(), tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]])
assert_type(
    INDEX.nearest(POINT, return_distance=True),
    tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]],
)
if TYPE_CHECKING:
    assert_type(
        INDEX.nearest(POINTS, return_distance=True),
        tuple[gm.Groups[npt.NDArray[np.int64]], npt.NDArray[np.float64]],
    )
assert_type(
    gm.join(POINTS, POINTS), tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]
)
assert_type(
    gm.nearest(POINTS, POINT, return_distance=True),
    tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]],
)
if TYPE_CHECKING:
    assert_type(
        gm.nearest(POINTS, POINTS, return_distance=True),
        tuple[gm.Groups[npt.NDArray[np.int64]], npt.NDArray[np.float64]],
    )
assert_type(POINTS.length, npt.NDArray[np.float64])
assert_type(POINTS.is_empty, npt.NDArray[np.bool_])
assert_type(cast('list[bool]', POINTS.is_empty.tolist()), list[bool])
assert_type(POINT.spatial_key(), int | None)
assert_type(POINT.spatial_key(curve='morton'), int | None)
assert_type(POINTS.spatial_key(), npt.NDArray[np.uint64])
assert_type(POINTS.spatial_key(curve='morton'), npt.NDArray[np.uint64])
assert_type(POINTS.sort_by_spatial_key(), gm.GeometryArray[gm.Point])
assert_type(POINTS.sort_by_spatial_key(curve='morton'), gm.GeometryArray[gm.Point])
assert_type(LINE.segmentize(0.5), gm.LineString)
assert_type(LINE.segmentize(fraction=0.5), gm.LineString)
assert_type(LINES.segmentize(0.5), gm.GeometryArray[gm.LineString])
assert_type(LINES.segmentize(fraction=0.5), gm.GeometryArray[gm.LineString])
assert_type(LINE.line_interpolate(0.5), gm.Point)
assert_type(LINE.line_interpolate([0.25, 0.75]), gm.GeometryArray[gm.Point])
assert_type(LINE.line_interpolate(count=3), gm.GeometryArray[gm.Point])
assert_type(MEASURED_LINE.line_interpolate(5.0), gm.Point)
assert_type(LINE.line_locate(POINT), float)
assert_type(MEASURED_LINE.line_locate(POINT), float)
assert_type(LINE.line_substring(0.25, 0.75), gm.LineString | gm.Point)
assert_type(MEASURED_LINE.line_substring(5.0, 10.0), gm.LineString | gm.Point)
assert_type(LINES.line_interpolate(0.5), gm.GeometryArray[gm.Point])
assert_type(LINES.line_interpolate(count=3), gm.Groups[gm.GeometryArray[gm.Point]])
assert_type(LINES.line_interpolate(count=[3]), gm.Groups[gm.GeometryArray[gm.Point]])
assert_type(POINT.sample_points(3, seed=7), gm.GeometryArray[gm.Point])
assert_type(
    POINTS.sample_points([2, 3], seed=[11, 12]),
    gm.Groups[gm.GeometryArray[gm.Point]],
)
assert_type(LINES.line_locate(POINT), npt.NDArray[np.float64])
assert_type(
    LINES.line_substring(0.25, 0.75),
    gm.GeometryArray[gm.LineString | gm.Point],
)
assert_type(np.asarray(POINTS.coords), npt.NDArray[np.float64])
first_point = POINTS[0]
assert first_point is not None
assert_type(first_point, gm.Point)
assert_type(POINTS[[True, False]], gm.GeometryArray[gm.Point])
assert_type(POINTS[[1, 0]], gm.GeometryArray[gm.Point])
assert_type(POINTS[POINTS.is_empty], gm.GeometryArray[gm.Point])
iter_point = next(iter(POINTS))
assert iter_point is not None
assert_type(iter_point, gm.Point)
GRID = gm.GeometryArray([POLY, gm.box(2, 0, 4, 2)])
assert_type(GRID.coverage_simplify(0.1), gm.GeometryArray[gm.Polygon | gm.MultiPolygon])
assert_type(gm.coverage_is_valid([POLY]), bool)
assert_type(gm.coverage_is_valid(POLY), bool)
assert_type(gm.coverage_union(iter([POLY])), gm.Polygon | gm.MultiPolygon)
assert_type(gm.coverage_union(POLY), gm.Polygon | gm.MultiPolygon)
assert_type(gm.polygonize(POLY), gm.GeometryArray[gm.Polygon])
assert_type(gm.coverage_union(GRID), gm.Polygon | gm.MultiPolygon)
assert_type(gm.polygonize(GRID), gm.GeometryArray[gm.Polygon])
assert_type(gm.union_all(GRID), gm.Geometry)
assert_type(GRID.union_all(), gm.Geometry)
assert_type(gm.to_feature_collection(POLY), sht.GeoJsonFeatureCollection)

# Overload-narrowing coverage: one probe per @overload group that once shipped
# dead (undecorated) variants — pyright's last-def-wins silently widened these
# to unions until the 2026-07 repair. Every group keeps a live probe.
assert_type(POLY & POLY, gm.Geometry)
assert_type(POLY | POLY, gm.Geometry)
assert_type(POLY - POLY, gm.Geometry)
assert_type(POLY ^ POLY, gm.Geometry)
assert_type(POLY & POINTS, gm.GeometryArray[gm.Geometry])
assert_type(POINTS[:1], gm.GeometryArray[gm.Point])
assert_type(POINTS.concat(POINTS), gm.GeometryArray[gm.Point])
MULTI = gm.MultiPoint([(0.0, 0.0), (1.0, 1.0)])
assert_type(MULTI.parts[0], gm.Point)
assert_type(MULTI.parts[:1], list[gm.Point])
BARE_OK: gm.GeometryArray = POINTS  # PEP 696 default: bare == [Geometry]
BARE_CELLS_OK: gm.CellArray = h3_cells  # PEP 696 default: bare == [Cell]

# --- High-value free-function overload groups (manifest: _OVERLOAD_TARGETS) ---
assert_type(gm.contains(POLY, POINT), bool)
assert_type(gm.contains(POLY.prepare(), POINT), bool)
assert_type(gm.contains(POLY, POINT.prepare()), bool)
assert_type(gm.equals(POLY.prepare(), POLY.prepare()), bool)
assert_type(gm.contains(POINTS, POINT), npt.NDArray[np.bool_])
assert_type(gm.intersects(POLY, POINT), bool)
assert_type(gm.intersects(POINTS, POINT), npt.NDArray[np.bool_])

# Prepared scalars are valid on either side of every topological predicate;
# arrays remain GeometryArray-only operands.
_PREP_POLY = POLY.prepare()
assert_type(gm.contains(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.contains(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.contains_properly(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.contains_properly(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.within(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.within(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.covers(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.covers(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.covered_by(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.covered_by(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.intersects(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.intersects(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.relate(_PREP_POLY, POLY), str)
assert_type(gm.relate(POLY, _PREP_POLY), str)
assert_type(gm.relate(_PREP_POLY, POINTS), list[str])
assert_type(gm.relate(POINTS, _PREP_POLY), list[str])
assert_type(gm.relate_pattern(_PREP_POLY, POLY, 'T********'), bool)
assert_type(gm.relate_pattern(POLY, _PREP_POLY, 'T********'), bool)
assert_type(gm.relate_pattern(_PREP_POLY, POINTS, 'T********'), npt.NDArray[np.bool_])
assert_type(gm.relate_pattern(POINTS, _PREP_POLY, 'T********'), npt.NDArray[np.bool_])
assert_type(gm.disjoint(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.disjoint(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.touches(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.touches(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.crosses(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.crosses(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.overlaps(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.overlaps(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.equals(_PREP_POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.equals(POINTS, _PREP_POLY), npt.NDArray[np.bool_])
assert_type(gm.intersects_xy(_PREP_POLY, 0.0, 0.0), bool)
assert_type(gm.intersects_xy(_PREP_POLY, [0.0], [0.0]), npt.NDArray[np.bool_])
assert_type(gm.equals(_PREP_POLY, _PREP_POLY), bool)
assert_type(gm.dwithin(_PREP_POLY, _PREP_POLY, 0.0), bool)
assert_type(gm.equals_exact(_PREP_POLY, _PREP_POLY), bool)
assert_type(gm.distance(POINT, POLY), float)
assert_type(gm.distance(POINTS, POLY), npt.NDArray[np.float64])
assert_type(gm.area(POLY), float)
assert_type(gm.area(POINTS), npt.NDArray[np.float64])
assert_type(gm.area([POLY]), npt.NDArray[np.float64])
assert_type(gm.area(POLY, unit='planar'), float)
assert_type(gm.area(POINTS, unit='planar'), npt.NDArray[np.float64])
assert_type(gm.length(LINE), float)
assert_type(gm.length(POINTS), npt.NDArray[np.float64])
assert_type(gm.length([LINE]), npt.NDArray[np.float64])
assert_type(gm.length(LINE, unit='planar'), float)
assert_type(gm.length(POINTS, unit='planar'), npt.NDArray[np.float64])
assert_type(gm.from_wkt('POINT (0 0)'), gm.Geometry)
assert_type(gm.from_wkt(['POINT (0 0)']), gm.GeometryArray[gm.Geometry])
_WKB = POINT.to_wkb()
assert_type(gm.from_wkb(_WKB), gm.Geometry)
assert_type(gm.from_wkb([_WKB]), gm.GeometryArray[gm.Geometry])
assert_type(gm.Point(0.0, 0.0), gm.Point)

# Dtype-family overloads: probe __array__/to_numpy directly (np.asarray's own
# overloads erase dtype precision for array-protocol objects). Pass real
# ``np.dtype`` instances so the runtime call (assert_type evaluates args) works.
assert_type(POINT.coords.__array__(), npt.NDArray[np.float64])
assert_type(POINT.coords.__array__(dtype=np.dtype(np.float32)), npt.NDArray[np.float32])
assert_type(POINTS.__array__(), npt.NDArray[np.object_])
assert_type(POINTS.to_numpy(), npt.NDArray[np.object_])
assert_type(h3_cells.__array__(), npt.NDArray[np.uint64] | npt.NDArray[np.object_])
assert_type(h3_cells.__array__(dtype=np.dtype(np.object_)), npt.NDArray[np.object_])
assert_type(h3_vertices.__array__(), npt.NDArray[np.uint64])
assert_type(h3_vertices.__array__(dtype=np.dtype(np.object_)), npt.NDArray[np.object_])
assert_type(h3_edges.__array__(), npt.NDArray[np.uint64])
assert_type(h3_edges.__array__(dtype=np.dtype(np.object_)), npt.NDArray[np.object_])

# Coordinates.to_dict / to_nested honesty.
assert_type(POINT.coords.to_dict(), dict[str, npt.NDArray[np.float64]])
assert_type(
    POINT.coords.to_dict(index=True),
    dict[str, npt.NDArray[np.float64] | npt.NDArray[np.int64]],
)
assert_type(POINT.coords.to_nested(), sht.NestedCoordinates)
assert_type(gm.LineString(LINE.coords), gm.LineString)
assert_type(gm.MultiPoint(POINTS.coords), gm.MultiPoint)

# CRS kind vocabulary.
assert_type(gm.CRS(4326).kind, sht.CrsKind)
if TYPE_CHECKING:
    _kind_ok: sht.CrsCatalogKind = 'projected'
    _db_kind_ok: sht.CrsDatabaseKind = 'ellipsoid'
assert_type(gm.crs_search('WGS', kind='geographic_2d'), list[sht.CrsCatalogInfo])
assert_type(gm.crs_codes('EPSG', kind='projected'), list[str])

# Bare CellArray defaults to Cell (protocol), not Any.
if TYPE_CHECKING:
    _bare_cell = h3_cells[0]
    assert _bare_cell is not None
    _bare_cell_ok: gm.Cell = _bare_cell
    _h3_cell_ok: gm.Cell = h3_cell
    _s2_cell_ok: gm.Cell = gm.S2Cell(POINT, level=12)
    _geohash_cell_ok: gm.Cell = gm.GeohashCell(POINT, precision=7)
    _tile_cell_ok: gm.Cell = gm.Tile(POINT, zoom=10)
    assert_type(_h3_cell_ok.children_count(), int)


def test_static_narrowing_matches_runtime() -> None:
    assert type(POINT.buffer(1.0)) is gm.Polygon
    assert type(LINE.boundary()) is gm.MultiPoint
    assert type(gm.H3Cell(POINT, resolution=9)) is gm.H3Cell
    assert type(POINTS[0]) is gm.Point
    np.testing.assert_array_equal(INDEX.query(POINT), np.array([0], dtype=np.int64))
    assert gm.contains(POLY, POINT) is True
    assert isinstance(gm.area(POLY, unit='planar'), float)
    assert isinstance(gm.from_wkb(POINT.to_wkb()), gm.Geometry)
    d = POINT.coords.to_dict(index=True)
    assert isinstance(d['index'][0], np.integer)
    assert np.asarray(POINT.coords, dtype=np.float32).dtype == np.float32
    assert np.asarray(h3_cells, dtype=object).dtype == object
    assert gm.CRS(4326).kind == 'geographic_2d'
