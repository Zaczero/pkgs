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

import types
from typing import TYPE_CHECKING, Any, TypeVar, assert_type, cast, reveal_type

import gometry as gm
import gometry._types as sht
import numpy as np
import numpy.typing as npt

POINT = gm.Point(1, 1)
LINE = gm.LineString([(0, 0), (1, 1), (2, 0)])
GEOGRAPHIC_POINT = gm.Point(1, 1, crs=4326)
GEOGRAPHIC_LINE = gm.LineString([(0, 0), (1, 1), (2, 0)], crs=4326)
MEASURED_LINE = gm.LineString([(0, 0, 0, 0), (1, 1, 0, 10), (2, 0, 0, 20)])
M_STATION_COLUMN = cast(
    'sht.FloatColumn',
    np.array([5.0, 15.0], dtype=np.float64),
)
MEASURED_LINES = gm.GeometryArray([MEASURED_LINE])
NULLABLE_MEASURED_LINES = gm.GeometryArray([MEASURED_LINE, None])
PROJECTED_LINE = gm.LineString([(0, 0), (1, 1)], crs=3857)
PROJECTED_POINT = gm.Point(1, 1, crs=3857)
MULTILINE = gm.MultiLineString([LINE])
POLY = gm.box(0, 0, 2, 2)
POINTS = gm.GeometryArray([POINT, gm.Point(2, 2)])
NULLABLE_POINTS = gm.GeometryArray([POINT, None])
LINES = gm.GeometryArray([LINE])
GEOGRAPHIC_LINES = gm.GeometryArray([GEOGRAPHIC_LINE])
LINES2 = gm.GeometryArray([LINE, LINE])
NULLABLE_LINES = gm.GeometryArray([LINE, None])
MULTILINES = gm.GeometryArray([MULTILINE])
NULLABLE_MULTILINES = gm.GeometryArray([MULTILINE, None])
POLYS = gm.GeometryArray([POLY])
NULLABLE_POLYGONS = gm.GeometryArray([POLY, None])
NORMALIZED: bool = True

_GeometryT = TypeVar('_GeometryT', bound=gm.Geometry)

if TYPE_CHECKING:
    _dense_points: gm.GeometryArray[gm.Point] = gm.GeometryArray([POINT])
    _nullable_points: gm.GeometryArray[gm.Point | None] = gm.GeometryArray([
        POINT,
        None,
    ])
    _point_rows: list[gm.Point] = [POINT, gm.Point(2, 2)]
    _nullable_point_rows: list[gm.Point | None] = [POINT, None]
    _coordinate_rows = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    _geometry_rows: list[gm.Geometry | None] = [POLY, None]

    reveal_type(gm.pluscode_encode(POINT))
    reveal_type(gm.pluscode_encode(8.0, 47.0))
    reveal_type(gm.pluscode_encode(_dense_points))
    reveal_type(gm.pluscode_encode(_nullable_points))
    reveal_type(gm.pluscode_encode([8.0, 9.0], [47.0, 48.0]))
    reveal_type(gm.pluscode_encode(_coordinate_rows[:, 0], 47.0))
    reveal_type(gm.osm_shortlink_encode(POINT))
    reveal_type(gm.osm_shortlink_encode(8.0, 47.0))
    reveal_type(gm.osm_shortlink_encode(_dense_points))
    reveal_type(gm.osm_shortlink_encode(_nullable_points))
    reveal_type(gm.osm_shortlink_encode([8.0, 9.0], [47.0, 48.0]))
    reveal_type(gm.osm_shortlink_encode(_coordinate_rows[:, 0], 47.0))

    reveal_type(gm.pluscode_shorten('9C3W9QCJ+2VX', POINT))
    reveal_type(gm.pluscode_shorten(['9C3W9QCJ+2VX'], POINT))
    reveal_type(gm.pluscode_shorten('9C3W9QCJ+2VX', [-1.2, -1.3], 51.0))
    reveal_type(gm.pluscode_shorten('9C3W9QCJ+2VX', _coordinate_rows[:, 0], 51.0))
    reveal_type(gm.pluscode_recover('CJ+2VX', POINT))
    reveal_type(gm.pluscode_recover(['CJ+2VX'], POINT))
    reveal_type(gm.pluscode_recover('CJ+2VX', [-1.2, -1.3], 51.0))
    reveal_type(gm.pluscode_recover('CJ+2VX', _coordinate_rows[:, 0], 51.0))

    reveal_type(gm.h3_cells(_dense_points, resolution=7))
    reveal_type(gm.h3_cells(_nullable_points, resolution=7))
    reveal_type(gm.h3_cells(_point_rows, resolution=7))
    reveal_type(gm.h3_cells(_nullable_point_rows, resolution=7))
    reveal_type(gm.h3_cells(_coordinate_rows, resolution=7))
    reveal_type(gm.h3_cells([1.0, 2.0], [3.0, 4.0], resolution=7))
    reveal_type(gm.s2_cells(_dense_points, level=12))
    reveal_type(gm.s2_cells(_nullable_points, level=12))
    reveal_type(gm.s2_cells(_point_rows, level=12))
    reveal_type(gm.s2_cells(_nullable_point_rows, level=12))
    reveal_type(gm.s2_cells(_coordinate_rows, level=12))
    reveal_type(gm.s2_cells([1.0, 2.0], [3.0, 4.0], level=12))
    reveal_type(gm.geohash_cells(_dense_points, precision=6))
    reveal_type(gm.geohash_cells(_nullable_points, precision=6))
    reveal_type(gm.geohash_cells(_point_rows, precision=6))
    reveal_type(gm.geohash_cells(_nullable_point_rows, precision=6))
    reveal_type(gm.geohash_cells(_coordinate_rows, precision=6))
    reveal_type(gm.geohash_cells([1.0, 2.0], [3.0, 4.0], precision=6))
    reveal_type(gm.tile_cells(_dense_points, zoom=12))
    reveal_type(gm.tile_cells(_nullable_points, zoom=12))
    reveal_type(gm.tile_cells(_point_rows, zoom=12))
    reveal_type(gm.tile_cells(_nullable_point_rows, zoom=12))
    reveal_type(gm.tile_cells(_coordinate_rows, zoom=12))
    reveal_type(gm.tile_cells([1.0, 2.0], [3.0, 4.0], zoom=12))

    reveal_type(gm.h3_cover(POLY, resolution=7))
    reveal_type(gm.h3_cover(_geometry_rows, resolution=7))
    reveal_type(gm.s2_cover(POLY, level=10))
    reveal_type(gm.s2_cover(POLY, level_mod=2))
    reveal_type(gm.s2_cover(_geometry_rows))
    reveal_type(gm.geohash_cover(POLY, precision=6))
    reveal_type(gm.geohash_cover(_geometry_rows, precision=6))
    reveal_type(gm.tile_cover(POLY, zoom=12))
    reveal_type(gm.tile_cover(_geometry_rows, zoom=12))

    _dense_h3 = gm.CellArray([gm.H3Cell(1.0, 2.0, resolution=7)])
    _nullable_h3 = gm.CellArray([gm.H3Cell(1.0, 2.0, resolution=7), None])
    _dense_s2 = gm.CellArray([gm.S2Cell(1.0, 2.0, level=12)])
    _nullable_s2 = gm.CellArray([gm.S2Cell(1.0, 2.0, level=12), None])
    _dense_geohash = gm.CellArray([gm.GeohashCell('u33d')])
    _nullable_geohash = gm.CellArray([gm.GeohashCell('u33d'), None])
    _dense_tile = gm.CellArray([gm.Tile(x=4, y=5, zoom=5)])
    _nullable_tile = gm.CellArray([gm.Tile(x=4, y=5, zoom=5), None])
    reveal_type(_dense_h3.contains(_dense_h3[0]))
    reveal_type(_dense_h3.contains(_dense_h3[0].id))
    reveal_type(_nullable_h3.contains(_dense_h3[0].token))
    reveal_type(_nullable_h3.intersects(_dense_h3[0].id))
    reveal_type(_dense_s2.contains(_dense_s2[0].id))
    reveal_type(_nullable_s2.intersects(_dense_s2[0].token))
    reveal_type(_dense_geohash.contains(_dense_geohash[0].token))
    reveal_type(_nullable_geohash.intersects(_dense_geohash[0].token))
    reveal_type(_dense_tile.contains(_dense_tile[0].id))
    reveal_type(_nullable_tile.intersects(_dense_tile[0].token))

    reveal_type(_dense_h3.token)
    reveal_type(_nullable_h3.token)
    reveal_type(_dense_h3.to_numpy())
    reveal_type(_nullable_h3.to_numpy())
    reveal_type(_nullable_geohash.to_numpy())
    reveal_type(_dense_h3.__array__())
    reveal_type(_nullable_h3.__array__())

    reveal_type(gm.h3_union([_dense_h3[0], None], [None]))


def _drop_missing_generic(
    values: gm.GeometryArray[_GeometryT | None],
) -> gm.GeometryArray[_GeometryT]:
    return values.drop_missing()


def _check_require_row_types(
    nullable_array: gm.GeometryArray[gm.Point | None],
    nullable_groups: gm.Groups[gm.GeometryArray[gm.Point | None]],
    nullable_rows: Iterable[gm.Point | None],
    foreign_rows: Iterable[gm.Point | str | None],
) -> None:
    assert_type(gm.require(nullable_array), gm.GeometryArray[gm.Point | None])
    assert_type(
        gm.require(nullable_groups),
        gm.GeometryArray[gm.Point | None],
    )
    assert_type(gm.require(nullable_rows), gm.GeometryArray[gm.Point | None])
    assert_type(
        gm.require(foreign_rows),
        gm.GeometryArray[gm.Geometry | None],
    )


def _check_cell_projection_row_types(
    h3: gm.CellArray[gm.H3Cell],
    nullable_h3: gm.CellArray[gm.H3Cell | None],
    s2: gm.CellArray[gm.S2Cell],
    nullable_s2: gm.CellArray[gm.S2Cell | None],
    geohash: gm.CellArray[gm.GeohashCell],
    nullable_geohash: gm.CellArray[gm.GeohashCell | None],
    tile: gm.CellArray[gm.Tile],
    nullable_tile: gm.CellArray[gm.Tile | None],
) -> None:
    assert_type(h3.center, gm.GeometryArray[gm.Point])
    assert_type(nullable_h3.center, gm.GeometryArray[gm.Point | None])
    assert_type(h3.polygon, gm.GeometryArray[gm.Polygon])
    assert_type(nullable_h3.polygon, gm.GeometryArray[gm.Polygon | None])
    assert_type(s2.center, gm.GeometryArray[gm.Point])
    assert_type(nullable_s2.center, gm.GeometryArray[gm.Point | None])
    assert_type(s2.polygon, gm.GeometryArray[gm.Polygon])
    assert_type(nullable_s2.polygon, gm.GeometryArray[gm.Polygon | None])
    assert_type(geohash.center, gm.GeometryArray[gm.Point])
    assert_type(nullable_geohash.center, gm.GeometryArray[gm.Point | None])
    assert_type(geohash.polygon, gm.GeometryArray[gm.Polygon])
    assert_type(nullable_geohash.polygon, gm.GeometryArray[gm.Polygon | None])
    assert_type(tile.center, gm.GeometryArray[gm.Point])
    assert_type(nullable_tile.center, gm.GeometryArray[gm.Point | None])
    assert_type(tile.polygon, gm.GeometryArray[gm.Polygon])
    assert_type(nullable_tile.polygon, gm.GeometryArray[gm.Polygon | None])


def _check_buffer_input_precision(
    dense: gm.GeometryArray[gm.Point],
    nullable: gm.GeometryArray[gm.Point | None],
    numpy_scalar: np.float64,
    scalar_array: np.ndarray[tuple[()], np.dtype[np.float64]],
    column: np.ndarray[tuple[int], np.dtype[np.float64]],
    values: list[float],
) -> None:
    assert_type(dense.buffer(1.0), gm.GeometryArray[gm.Polygon])
    assert_type(
        nullable.buffer(1.0),
        gm.GeometryArray[gm.Polygon | None],
    )
    assert_type(dense.buffer(numpy_scalar), gm.GeometryArray[gm.Polygon])
    assert_type(
        nullable.buffer(numpy_scalar),
        gm.GeometryArray[gm.Polygon | None],
    )
    assert_type(dense.buffer(scalar_array), gm.GeometryArray[gm.Polygon])
    assert_type(
        nullable.buffer(scalar_array),
        gm.GeometryArray[gm.Polygon | None],
    )
    assert_type(dense.buffer(column), gm.GeometryArray[gm.Polygon])
    assert_type(
        nullable.buffer(column),
        gm.GeometryArray[gm.Polygon | None],
    )
    assert_type(dense.buffer(values), gm.GeometryArray[gm.Polygon])
    assert_type(
        nullable.buffer(values),
        gm.GeometryArray[gm.Polygon | None],
    )


def _check_fixed_output_nullability(
    values: gm.GeometryArray[gm.Geometry | None],
    distances: list[float],
) -> None:
    assert_type(values.polylabel(), gm.GeometryArray[gm.Point | None])
    assert_type(values.minimum_clearance_line(), gm.GeometryArray[gm.LineString | None])
    assert_type(
        values.boundary(),
        gm.GeometryArray[
            gm.MultiPoint
            | gm.LineString
            | gm.MultiLineString
            | gm.GeometryCollection
            | None
        ],
    )
    assert_type(
        values.line_merge(),
        gm.GeometryArray[gm.LineString | gm.MultiLineString | None],
    )
    assert_type(
        values.minimum_rotated_rectangle(),
        gm.GeometryArray[gm.Point | gm.LineString | gm.Polygon | None],
    )
    assert_type(
        values.clip_by_rect(0.0, 0.0, 1.0, 1.0),
        gm.GeometryArray[gm.Geometry | None],
    )
    assert_type(
        values.line_substring(distances, distances),
        gm.GeometryArray[gm.LineString | gm.Point | None],
    )
    assert_type(
        values.unique_points(),
        gm.GeometryArray[gm.MultiPoint | gm.GeometryCollection | None],
    )
    assert_type(
        gm.coverage_invalid_edges(values),
        gm.GeometryArray[gm.LineString | gm.MultiLineString | None],
    )
    assert_type(
        gm.coverage_simplify(values, 0.1),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon | None],
    )
    assert_type(
        gm.coverage_clean(values),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon | None],
    )
    assert_type(values.repair(), gm.GeometryArray[gm.Geometry | None])
    assert_type(values.line_interpolate(distances), gm.GeometryArray[gm.Point | None])
    assert_type(
        values.maximum_inscribed_circle(tolerance=distances),
        gm.GeometryArray[gm.Point | gm.Polygon | None],
    )
    assert_type(
        values.minimum_bounding_circle(),
        gm.GeometryArray[gm.Point | gm.Polygon | None],
    )
    assert_type(
        values.concave_hull(concavity=distances, length_threshold=distances),
        gm.GeometryArray[gm.Point | gm.LineString | gm.Polygon | None],
    )
    assert_type(values.point_on_surface(), gm.GeometryArray[gm.Point | None])
    assert_type(values.extremes(), gm.Extremes[gm.GeometryArray[gm.Point | None]])
    assert_type(values.split_antimeridian(), gm.GeometryArray[gm.Geometry | None])
    assert_type(
        values.offset_curve(distances),
        gm.GeometryArray[
            gm.LineString | gm.MultiLineString | gm.GeometryCollection | None
        ],
    )
    assert_type(
        values.envelope(),
        gm.GeometryArray[gm.Point | gm.LineString | gm.Polygon | None],
    )
    assert_type(values.centroid(), gm.GeometryArray[gm.Point | None])
    assert_type(values.snap_to_grid(1.0), gm.GeometryArray[gm.Geometry | None])
    assert_type(
        values.build_area(),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon | gm.GeometryCollection | None],
    )
    assert_type(
        values.convex_hull(),
        gm.GeometryArray[gm.Point | gm.LineString | gm.Polygon | None],
    )
    assert_type(values.node(), gm.GeometryArray[gm.MultiLineString | None])


def _check_kind_narrowing(
    point: gm.Point,
    multipoint: gm.MultiPoint,
    line: gm.LineString,
    multiline: gm.MultiLineString,
    polygon: gm.Polygon,
    multipolygon: gm.MultiPolygon,
    collection: gm.GeometryCollection,
    geometry: gm.Geometry,
    points: gm.GeometryArray[gm.Point],
    lines: gm.GeometryArray[gm.LineString],
    multilines: gm.GeometryArray[gm.MultiLineString],
    polygons: gm.GeometryArray[gm.Polygon],
    nullable_points: gm.GeometryArray[gm.Point | None],
    nullable_lines: gm.GeometryArray[gm.LineString | None],
    nullable_multilines: gm.GeometryArray[gm.MultiLineString | None],
    nullable_polygons: gm.GeometryArray[gm.Polygon | None],
    mixed: gm.GeometryArray[gm.Point | gm.Polygon],
) -> None:
    assert_type(point.boundary(), gm.GeometryCollection)
    assert_type(collection.boundary(), gm.GeometryCollection)
    assert_type(multiline.boundary(), gm.MultiPoint)
    assert_type(
        polygon.boundary(),
        gm.MultiPoint | gm.LineString | gm.MultiLineString | gm.GeometryCollection,
    )
    assert_type(
        multipolygon.boundary(),
        gm.MultiPoint | gm.LineString | gm.MultiLineString | gm.GeometryCollection,
    )
    assert_type(
        geometry.boundary(),
        gm.MultiPoint | gm.LineString | gm.MultiLineString | gm.GeometryCollection,
    )
    assert_type(point.repair(), gm.Point)
    assert_type(
        multiline.repair(),
        gm.Point
        | gm.MultiPoint
        | gm.LineString
        | gm.MultiLineString
        | gm.GeometryCollection,
    )
    assert_type(collection.repair(), gm.GeometryCollection)
    assert_type(line.repair(), gm.Point | gm.LineString | gm.MultiLineString)
    assert_type(multipolygon.repair(), gm.Polygon | gm.MultiPolygon)
    assert_type(geometry.repair(), gm.Geometry)
    assert_type(polygon.snap_to_grid(1.0), gm.Polygon)
    assert_type(point.snap_to_grid(1.0, repair=True), gm.Point)
    assert_type(line.snap_to_grid(1.0, repair=True), gm.LineString | gm.MultiLineString)
    assert_type(polygon.snap_to_grid(1.0, repair=True), gm.Polygon | gm.MultiPolygon)
    assert_type(geometry.snap_to_grid(1.0, repair=True), gm.Geometry)
    assert_type(multipolygon.split_antimeridian(), gm.MultiPolygon)
    assert_type(line.split_antimeridian(), gm.LineString | gm.MultiLineString)
    assert_type(polygon.split_antimeridian(), gm.Polygon | gm.MultiPolygon)
    assert_type(line.offset_curve(1.0), gm.LineString)
    assert_type(collection.offset_curve(1.0), gm.GeometryCollection)
    assert_type(
        geometry.offset_curve(1.0),
        gm.LineString | gm.MultiLineString | gm.GeometryCollection,
    )
    assert_type(line.line_merge(), gm.LineString)
    assert_type(multiline.line_merge(), gm.LineString | gm.MultiLineString)
    assert_type(point.clip_by_rect(0, 0, 1, 1), gm.Point)
    assert_type(multipoint.clip_by_rect(0, 0, 1, 1), gm.MultiPoint | gm.Point)
    assert_type(polygon.unique_points(), gm.MultiPoint)
    assert_type(collection.unique_points(), gm.MultiPoint | gm.GeometryCollection)
    assert_type(line.build_area(), gm.Polygon | gm.MultiPolygon)
    assert_type(point.envelope(), gm.Point | gm.Polygon)

    assert_type(points.boundary(), gm.GeometryArray[gm.GeometryCollection])
    assert_type(lines.boundary(), gm.GeometryArray[gm.MultiPoint])
    assert_type(
        polygons.boundary(),
        gm.GeometryArray[
            gm.MultiPoint | gm.LineString | gm.MultiLineString | gm.GeometryCollection
        ],
    )
    assert_type(
        nullable_polygons.boundary(),
        gm.GeometryArray[
            gm.MultiPoint
            | gm.LineString
            | gm.MultiLineString
            | gm.GeometryCollection
            | None
        ],
    )
    assert_type(points.buffer(1.0), gm.GeometryArray[gm.Polygon])
    assert_type(nullable_points.buffer(1.0), gm.GeometryArray[gm.Polygon | None])
    assert_type(points.repair(), gm.GeometryArray[gm.Point])
    assert_type(nullable_points.repair(), gm.GeometryArray[gm.Point | None])
    assert_type(
        lines.repair(),
        gm.GeometryArray[gm.Point | gm.LineString | gm.MultiLineString],
    )
    assert_type(
        nullable_lines.repair(),
        gm.GeometryArray[gm.Point | gm.LineString | gm.MultiLineString | None],
    )
    assert_type(
        multilines.repair(),
        gm.GeometryArray[
            gm.Point
            | gm.MultiPoint
            | gm.LineString
            | gm.MultiLineString
            | gm.GeometryCollection
        ],
    )
    assert_type(
        nullable_multilines.repair(),
        gm.GeometryArray[
            gm.Point
            | gm.MultiPoint
            | gm.LineString
            | gm.MultiLineString
            | gm.GeometryCollection
            | None
        ],
    )
    assert_type(
        nullable_polygons.repair(),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon | None],
    )
    assert_type(mixed.repair(), gm.GeometryArray[gm.Geometry])
    assert_type(polygons.snap_to_grid(1.0), gm.GeometryArray[gm.Polygon])
    assert_type(
        nullable_lines.snap_to_grid(1.0, repair=True),
        gm.GeometryArray[gm.LineString | gm.MultiLineString | None],
    )
    assert_type(points.split_antimeridian(), gm.GeometryArray[gm.Point])
    assert_type(
        nullable_polygons.split_antimeridian(),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon | None],
    )
    assert_type(lines.offset_curve(1.0), gm.GeometryArray[gm.LineString])
    assert_type(nullable_lines.line_merge(), gm.GeometryArray[gm.LineString | None])
    assert_type(points.clip_by_rect(0, 0, 1, 1), gm.GeometryArray[gm.Point])
    assert_type(polygons.unique_points(), gm.GeometryArray[gm.MultiPoint])
    assert_type(
        nullable_lines.build_area(),
        gm.GeometryArray[gm.Polygon | gm.MultiPolygon | None],
    )
    assert_type(points.envelope(), gm.GeometryArray[gm.Point | gm.Polygon])


def _check_overlay_narrowing(
    point: gm.Point,
    multipoint: gm.MultiPoint,
    polygon: gm.Polygon,
    points: gm.GeometryArray[gm.Point],
    multipoints: gm.GeometryArray[gm.MultiPoint],
    polygons: gm.GeometryArray[gm.Polygon],
    nullable_points: gm.GeometryArray[gm.Point | None],
    nullable_polygons: gm.GeometryArray[gm.Polygon | None],
    nullable_multipoints: gm.GeometryArray[gm.MultiPoint | None],
    geometry: gm.Geometry,
    line: gm.LineString,
    multilinestring: gm.MultiLineString,
    multipolygon: gm.MultiPolygon,
    collection: gm.GeometryCollection,
    other: object,
) -> None:
    assert_type(gm.intersection(point, polygon), gm.Point)
    assert_type(gm.intersection(polygon, point), gm.Point)
    assert_type(gm.intersection(multipoint, polygon), gm.MultiPoint | gm.Point)
    assert_type(gm.intersection(polygon, multipoint), gm.MultiPoint | gm.Point)
    assert_type(gm.intersection(polygon, polygon), gm.Geometry)
    assert_type(gm.intersection(points, polygon), gm.GeometryArray[gm.Point])
    assert_type(gm.intersection(polygons, points), gm.GeometryArray[gm.Point])
    assert_type(gm.intersection(point, polygons), gm.GeometryArray[gm.Point])
    assert_type(gm.intersection(polygons, point), gm.GeometryArray[gm.Point])
    assert_type(
        gm.intersection(nullable_points, polygons),
        gm.GeometryArray[gm.Point | None],
    )
    assert_type(
        gm.intersection(points, nullable_polygons),
        gm.GeometryArray[gm.Point | None],
    )
    assert_type(
        gm.intersection(point, nullable_polygons),
        gm.GeometryArray[gm.Point | None],
    )
    assert_type(
        gm.intersection(multipoints, polygons),
        gm.GeometryArray[gm.MultiPoint | gm.Point],
    )
    assert_type(gm.intersection(polygons, polygons), gm.GeometryArray[gm.Geometry])
    assert_type(gm.difference(point, polygon), gm.Point)
    assert_type(gm.difference(multipoint, polygon), gm.MultiPoint | gm.Point)
    assert_type(gm.difference(polygon, point), gm.Geometry)
    assert_type(gm.difference(points, polygons), gm.GeometryArray[gm.Point])
    assert_type(
        gm.difference(nullable_points, polygon), gm.GeometryArray[gm.Point | None]
    )
    assert_type(gm.union(point, multipoint), gm.Point | gm.MultiPoint)
    assert_type(gm.union(point, polygon), gm.Geometry)
    assert_type(
        gm.union(points, multipoints), gm.GeometryArray[gm.Point | gm.MultiPoint]
    )
    assert_type(
        gm.symmetric_difference(nullable_points, point),
        gm.GeometryArray[gm.Point | gm.MultiPoint | None],
    )
    assert_type(point & polygon, gm.Point)
    assert_type(polygon & point, gm.Point)
    assert_type(polygon & multipoint, gm.MultiPoint | gm.Point)
    assert_type(point & polygons, gm.GeometryArray[gm.Point])
    assert_type(polygon & points, gm.GeometryArray[gm.Point])
    assert_type(point & nullable_polygons, gm.GeometryArray[gm.Point | None])
    assert_type(point - polygon, gm.Point)
    assert_type(polygon - point, gm.Geometry)
    assert_type(point | multipoint, gm.Point | gm.MultiPoint)
    assert_type(point ^ point, gm.Point | gm.MultiPoint)
    assert_type(points & polygon, gm.GeometryArray[gm.Point])
    assert_type(polygons & point, gm.GeometryArray[gm.Point])
    assert_type(polygons & points, gm.GeometryArray[gm.Point])
    assert_type(nullable_points & polygons, gm.GeometryArray[gm.Point | None])
    assert_type(points - polygons, gm.GeometryArray[gm.Point])
    assert_type(points | multipoints, gm.GeometryArray[gm.Point | gm.MultiPoint])
    assert_type(polygons & polygons, gm.GeometryArray[gm.Geometry])

    # Direct reflected calls are narrower by the left (other) operand.
    assert_type(point.__rand__(polygon), gm.Point)
    assert_type(polygon.__rand__(point), gm.Point)
    assert_type(multipoint.__rand__(polygon), gm.MultiPoint | gm.Point)
    assert_type(multipoint.__rand__(point), gm.Point)
    assert_type(point.__ror__(multipoint), gm.Point | gm.MultiPoint)
    assert_type(point.__rxor__(polygon), gm.Geometry)
    assert_type(multipoint.__ror__(point), gm.Point | gm.MultiPoint)
    assert_type(point.__rsub__(polygon), gm.Geometry)
    assert_type(polygon.__rsub__(point), gm.Point)
    assert_type(multipoint.__rsub__(point), gm.Point)
    assert_type(polygon.__rsub__(multipoint), gm.MultiPoint | gm.Point)
    assert_type(geometry.__rsub__(point), gm.Point)
    assert_type(line.__rsub__(point), gm.Point)
    assert_type(multilinestring.__rsub__(point), gm.Point)
    assert_type(multipolygon.__rsub__(point), gm.Point)
    assert_type(collection.__rsub__(point), gm.Point)

    # Array reflection accepts another array and preserves either side's
    # nullability; subtraction's kind narrowing follows the other array.
    assert_type(points.__rand__(polygons), gm.GeometryArray[gm.Point])
    assert_type(polygons.__rand__(points), gm.GeometryArray[gm.Point])
    assert_type(
        multipoints.__rand__(polygons),
        gm.GeometryArray[gm.MultiPoint | gm.Point],
    )
    assert_type(
        polygons.__rand__(multipoints),
        gm.GeometryArray[gm.MultiPoint | gm.Point],
    )
    assert_type(
        nullable_points.__rand__(polygons),
        gm.GeometryArray[gm.Point | None],
    )
    assert_type(
        points.__rand__(nullable_polygons),
        gm.GeometryArray[gm.Point | None],
    )
    assert_type(
        nullable_multipoints.__rand__(polygons),
        gm.GeometryArray[gm.MultiPoint | gm.Point | None],
    )
    assert_type(
        multipoints.__rand__(nullable_polygons),
        gm.GeometryArray[gm.MultiPoint | gm.Point | None],
    )
    assert_type(
        points.__ror__(multipoints),
        gm.GeometryArray[gm.Point | gm.MultiPoint],
    )
    assert_type(
        multipoints.__rxor__(points),
        gm.GeometryArray[gm.Point | gm.MultiPoint],
    )
    assert_type(
        nullable_points.__ror__(multipoints),
        gm.GeometryArray[gm.Point | gm.MultiPoint | None],
    )
    assert_type(
        points.__rxor__(nullable_multipoints),
        gm.GeometryArray[gm.Point | gm.MultiPoint | None],
    )
    assert_type(
        points.__rsub__(polygons),
        gm.GeometryArray[gm.Geometry],
    )
    assert_type(
        polygons.__rsub__(points),
        gm.GeometryArray[gm.Point],
    )
    assert_type(
        points.__rsub__(multipoints),
        gm.GeometryArray[gm.MultiPoint | gm.Point],
    )
    assert_type(
        multipoints.__rsub__(points),
        gm.GeometryArray[gm.Point],
    )
    assert_type(
        nullable_points.__rsub__(polygons),
        gm.GeometryArray[gm.Geometry | None],
    )
    assert_type(
        points.__rsub__(nullable_polygons),
        gm.GeometryArray[gm.Geometry | None],
    )
    assert_type(
        nullable_points.__rsub__(multipoints),
        gm.GeometryArray[gm.MultiPoint | gm.Point | None],
    )
    assert_type(
        points.__rsub__(nullable_multipoints),
        gm.GeometryArray[gm.MultiPoint | gm.Point | None],
    )

    # These descriptors decline cross-family calls directly, despite mixed
    # scalar/array infix operations being supported by the forward slots.
    assert_type(polygon.__rand__(points), types.NotImplementedType)
    assert_type(polygon.__ror__(points), types.NotImplementedType)
    assert_type(polygon.__rsub__(points), types.NotImplementedType)
    assert_type(polygon.__rxor__(points), types.NotImplementedType)
    assert_type(points.__rand__(polygon), types.NotImplementedType)
    assert_type(points.__ror__(polygon), types.NotImplementedType)
    assert_type(points.__rsub__(polygon), types.NotImplementedType)
    assert_type(points.__rxor__(polygon), types.NotImplementedType)
    assert_type(point.__rand__(polygons), types.NotImplementedType)

    # An object-typed operand may be a supported Geometry or GeometryArray.
    assert_type(
        point.__rand__(other),
        gm.Point | types.NotImplementedType,
    )
    assert_type(
        points.__rsub__(other),
        gm.GeometryArray[gm.Geometry | None] | types.NotImplementedType,
    )


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
assert_type(gm.polygonize_all([LINE]), gm.GeometryArray[gm.Polygon])
assert_type(
    gm.polygonize_all([LINE], diagnostics=True),
    gm.PolygonizeResult,
)


def _check_polygonize_diagnostics_bool_overload(diagnostics: bool) -> None:
    result = gm.polygonize_all([LINE], diagnostics=diagnostics)
    assert_type(result, gm.GeometryArray[gm.Polygon] | gm.PolygonizeResult)
    if TYPE_CHECKING:
        reveal_type(result)


assert_type(
    gm.polygonize_all([LINE], diagnostics=True).dangles,
    gm.Groups[gm.GeometryArray[gm.LineString]],
)
assert_type(POINTS.simplify(0.1, method='vw'), gm.GeometryArray[gm.Point])
assert_type(POINT.buffer(1.0), gm.Polygon)
assert_type(LINE.buffer(0.5), gm.Polygon | gm.MultiPolygon)
assert_type(LINE.boundary(), gm.MultiPoint)
assert_type(POINT.centroid(), gm.Point)
assert_type(hash(POINT), int)
assert_type(hash(POINTS), int)
assert_type(POLY.exterior, gm.LineString)

# Scalar<->array return duality (the 32 `-> Self` lies, gated by pyo3stubs
# return-parity): kind-changing array methods return the scalar leaf union,
# kind-preserving ones return Self.
assert_type(POINTS.buffer(1.0), gm.GeometryArray[gm.Polygon])
assert_type(POINTS.snap_to_grid(grid_size=1.0), gm.GeometryArray[gm.Point])
assert_type(POINTS.centroid(), gm.GeometryArray[gm.Point])
assert_type(
    POINTS.convex_hull(),
    gm.GeometryArray[gm.Point | gm.LineString | gm.Polygon],
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
assert_type(gm.require(POINT), gm.Point)
assert_type(gm.require([POINT, None]), gm.GeometryArray[gm.Point | None])
assert_type(gm.require('POINT (1 1)'), gm.Geometry)
assert_type(POLY.polylabel(), gm.Point)
assert_type(POLY.minimum_bounding_radius(), float)
assert_type(POINTS.minimum_bounding_radius(), npt.NDArray[np.float64])
assert_type(
    gm.hausdorff_distance(LINES, LINE, max_error=1e-12),
    npt.NDArray[np.float64],
)
assert_type(gm.hausdorff_distance(LINES, LINE), npt.NDArray[np.float64])
assert_type(
    gm.frechet_distance(LINES, LINE, densify=[0.5]),
    npt.NDArray[np.float64],
)
assert_type(POLY.minimum_clearance_line(), gm.LineString)
assert_type(POLY.snap_to_grid(0.5), gm.Polygon)
assert_type(POLY.snap_to_grid(grid_size=0.5), gm.Polygon)
assert_type(POLY.snap_to_grid(0.5, repair=True), gm.Polygon | gm.MultiPolygon)
assert_type(
    gm.nearest_points(POINTS, LINE),
    tuple[gm.GeometryArray[gm.Point], gm.GeometryArray[gm.Point]],
)
assert_type(gm.nearest_points(POINTS, LINE)[0], gm.GeometryArray[gm.Point])
assert_type(
    gm.nearest_points(LINE, POINTS),
    tuple[gm.GeometryArray[gm.Point], gm.GeometryArray[gm.Point]],
)
assert_type(gm.split(LINE, POINT), gm.GeometryArray[gm.LineString])
assert_type(
    gm.split(LINE, NULLABLE_POINTS),
    gm.GeometryArray[gm.LineString],
)
assert_type(
    gm.split(LINES, POINT),
    gm.Groups[gm.GeometryArray[gm.LineString]],
)
assert_type(
    gm.split(NULLABLE_LINES, POINT),
    gm.Groups[gm.GeometryArray[gm.LineString]],
)
assert_type(
    gm.split(NULLABLE_LINES, NULLABLE_POINTS),
    gm.Groups[gm.GeometryArray[gm.LineString]],
)
assert_type(gm.from_wkt('POINT (0 0)'), gm.Geometry)
assert_type(gm.from_wkt(['POINT (0 0)']), gm.GeometryArray[gm.Geometry])
assert_type(gm.from_wkt(['POINT (0 0)', None]), gm.GeometryArray[gm.Geometry | None])
assert_type(POINT, gm.Point)
assert_type(POINTS, gm.GeometryArray[gm.Point])
assert_type(gm.MultiPoint([POINT, gm.Point(2, 2)]), gm.MultiPoint)
if TYPE_CHECKING:
    from collections.abc import Hashable, Iterable, Mapping
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import geopandas as gpd
    import pandas as pd
    import polars as pl

    class _CrsAuthority:
        def to_authority(self) -> tuple[str, int]: ...

    class _CrsEpsg:
        def to_epsg(self) -> int: ...

    class _CrsJson:
        def to_json(self) -> str: ...

    class _CrsHolder:
        @property
        def crs(self) -> int: ...

    class _SrsHolder:
        @property
        def srs(self) -> int: ...

    reveal_type(gm.CRS(_CrsAuthority()))
    reveal_type(gm.CRS(_CrsEpsg()))
    reveal_type(gm.CRS(_CrsJson()))
    reveal_type(gm.CRS(_CrsHolder()))
    reveal_type(gm.CRS(_SrsHolder()))
    hashable_geometry_array: Hashable = POINTS
    pandas_series = POINTS.to_pandas()
    assert_type(pandas_series, pd.Series)
    assert_type(gm.from_pandas(pandas_series), gm.GeometryArray[gm.Geometry | None])

    polars_series = POINTS.to_polars()
    assert_type(polars_series, pl.Series)
    assert_type(gm.from_polars(polars_series), gm.GeometryArray[gm.Geometry | None])

    geopandas_series = POINTS.to_geopandas()
    assert_type(geopandas_series, gpd.GeoSeries)
    assert_type(
        gm.from_geopandas(geopandas_series), gm.GeometryArray[gm.Geometry | None]
    )

    float_column: sht.FloatColumn = [1.0, 2.0]
    assert_type(gm.Point(np.float32(1.0), np.int64(2)), gm.Point)
    assert_type(
        gm.Point(1.0, 2.0).set_coordinates(np.zeros((1, 2), dtype=np.float32)),
        gm.Point,
    )
    assert_type(POINTS[np.int64(0)], gm.Point)
    integer_index_0d: np.ndarray[tuple[()], np.dtype[np.int64]] = np.array(
        0, dtype=np.int64
    )
    assert_type(POINTS[integer_index_0d], gm.Point)
    assert_type(gm.contains_xy(POLY, np.float32(1.0), np.int64(1)), bool)
    assert_type(
        gm.crs_transform(4326, 3857, np.float32(1.0), np.float64(2.0)),
        tuple[float, float],
    )
    coordinate_column_1d: np.ndarray[tuple[int], np.dtype[np.float64]] = np.array(
        [1.0, 2.0], dtype=np.float64
    )
    reveal_type(gm.crs_transform(4326, 3857, 1.0, 2.0, None, t=2020.0))
    reveal_type(gm.crs_transform(4326, 3857, coordinate_column_1d, 2.0))
    reveal_type(gm.crs_transform(4326, 3857, 1.0, 2.0, coordinate_column_1d))
    reveal_type(gm.crs_transform(4326, 3857, 1.0, 2.0, t=coordinate_column_1d))
    reveal_type(gm.crs_apply('+proj=noop', 1.0, 2.0))
    reveal_type(gm.crs_apply('+proj=noop', 1.0, 2.0, 3.0))
    reveal_type(gm.crs_apply('+proj=noop', 1.0, 2.0, t=2020.0))
    reveal_type(gm.crs_apply('+proj=noop', coordinate_column_1d, 2.0))
    reveal_type(gm.crs_apply('+proj=noop', 1.0, coordinate_column_1d))
    reveal_type(gm.crs_apply('+proj=noop', 1.0, 2.0, z=coordinate_column_1d))
    reveal_type(gm.crs_apply('+proj=noop', 1.0, 2.0, t=coordinate_column_1d))
    reveal_type(
        gm.crs_apply(
            '+proj=noop', coordinate_column_1d, 2.0, z=3.0, t=coordinate_column_1d
        )
    )
    reveal_type(gm.crs_roundtrip(4326, 3857, 1.0, 2.0))
    reveal_type(gm.crs_roundtrip(4326, 3857, 1.0, 2.0, z=coordinate_column_1d))
    reveal_type(gm.crs_roundtrip(4326, 3857, 1.0, 2.0, t=coordinate_column_1d))
    bounds_array: np.ndarray[tuple[int], np.dtype[np.float64]] = np.array(
        [-1.0, 50.0, 1.0, 51.0], dtype=np.float64
    )
    assert_type(
        gm.crs_transform_bounds(4326, 3857, bounds_array),
        tuple[float, float, float, float]
        | tuple[float, float, float, float, float, float]
        | npt.NDArray[np.float64],
    )
    reveal_type(gm.crs_transform_bounds(4326, 3857, bounds_array))
    reveal_type(gm.CRS(np.int64(4326)))
    assert_type(
        gm.pluscode_encode(np.float32(8.0), np.float64(47.0), length=np.int64(10)),
        str,
    )
    assert_type(
        gm.osm_shortlink_encode(np.float32(8.0), np.float64(47.0), zoom=np.int64(16)),
        str,
    )
    navigation_path: sht.NavigationPath = 'rhumb'
    features = gm.Features(POINTS)
    with TemporaryDirectory() as _temporary:
        parquet_path = Path(_temporary) / 'points.parquet'
        assert_type(POINTS.to_geoparquet(parquet_path, encoding='native'), None)
        assert parquet_path.is_file()
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
    assert_type(mixed_array, gm.GeometryArray[gm.Geometry | None])
    present_array = gm.GeometryArray(present_geometry_like_rows)
    assert_type(present_array, gm.GeometryArray[gm.Geometry])
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
    assert_type(gm.union_all(geometry_like_rows), gm.Geometry)
    assert_type(gm.intersection_all(geometry_like_rows), gm.Geometry)
    assert_type(gm.symmetric_difference_all(geometry_like_rows), gm.Geometry)
    foreign_coordinate_view = gm.GeometryArray(geometry_like_rows).coords
    assert_type(
        np.asarray(foreign_coordinate_view, dtype=np.float64),
        npt.NDArray[np.float64],
    )
    point_coordinate_view = POINTS.coords
    assert_type(
        np.asarray(point_coordinate_view, dtype=np.float64),
        npt.NDArray[np.float64],
    )
    assert_type(
        point_coordinate_view.row_index,
        npt.NDArray[np.int64],
    )
    assert_type(POINTS.parts, gm.Groups[gm.GeometryArray[gm.Geometry]])
    assert_type(POINTS.bounds, npt.NDArray[np.float64])
    assert_type(POLY.parts, gm.GeometryParts[gm.Polygon])
    assert_type(POLY.exterior, gm.LineString)
    assert_type(POLY.interiors, list[gm.LineString])
    assert_type(
        gm.polygonize_all(present_geometry_like_rows), gm.GeometryArray[gm.Polygon]
    )
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
    assert_type(LINE.segmentize(1.0, path='geodesic'), gm.LineString)
    assert_type(LINE.segmentize(1.0, path='rhumb', unit=None), gm.LineString)
    assert_type(LINE.segmentize(fraction=0.5, path='geodesic'), gm.LineString)
    assert_type(LINES.segmentize(1.0, path='rhumb'), gm.GeometryArray[gm.LineString])
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
    assert_type(gm.from_arrow(arrow_array), gm.GeometryArray[gm.Geometry | None])
    assert_type(gm.from_arrow(arrow_stream), gm.GeometryArray[gm.Geometry | None])
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
    assert_type(gm.distance(POINT, POINT, path='rhumb'), float)
    reveal_type(gm.distance(POINT, POINT, path='geodesic'))
    assert_type(gm.distance(POINTS, POINT, path='rhumb'), npt.NDArray[np.float64])
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
assert_type(
    gm.H3VertexArray([h3_vertices[0], h3_vertices[0].id, h3_vertices[0].token]),
    gm.H3VertexArray,
)
assert_type(
    gm.H3EdgeArray([h3_edges[0], h3_edges[0].id, h3_edges[0].token]),
    gm.H3EdgeArray,
)
assert_type(h3_vertices, gm.H3VertexArray)
assert_type(h3_vertices[0], gm.H3Vertex)
assert_type(h3_vertices[:2], gm.H3VertexArray)
assert_type(h3_vertices[[True, False, True, False, True, False]], gm.H3VertexArray)
assert_type(h3_vertices[np.asarray([0, 1], dtype=np.int64)], gm.H3VertexArray)
assert_type(gm.H3VertexArray(h3_vertices.token), gm.H3VertexArray)
assert_type(gm.H3VertexArray(h3_vertices.to_numpy()), gm.H3VertexArray)
signed_vertex_ids: npt.NDArray[np.int64] = np.asarray(
    [h3_vertices[0].id], dtype=np.int64
)
assert_type(gm.H3VertexArray(signed_vertex_ids), gm.H3VertexArray)
assert_type(h3_vertices.point, gm.GeometryArray[gm.Point])
assert_type(h3_vertices[0].point, gm.Point)
assert_type(h3_edges, gm.H3EdgeArray)
assert_type(h3_edges[0], gm.H3Edge)
assert_type(h3_edges[:2], gm.H3EdgeArray)
assert_type(h3_edges[[True, False, True, False, True, False]], gm.H3EdgeArray)
assert_type(h3_edges[np.asarray([0, 1], dtype=np.int64)], gm.H3EdgeArray)
assert_type(gm.H3EdgeArray(h3_edges.token), gm.H3EdgeArray)
assert_type(gm.H3EdgeArray(h3_edges.to_numpy()), gm.H3EdgeArray)
signed_edge_ids: npt.NDArray[np.int64] = np.asarray([h3_edges[0].id], dtype=np.int64)
assert_type(gm.H3EdgeArray(signed_edge_ids), gm.H3EdgeArray)
assert_type(h3_edges.origin, gm.CellArray[gm.H3Cell])
assert_type(h3_edges.destination, gm.CellArray[gm.H3Cell])
assert_type(h3_edges.reverse(), gm.H3EdgeArray)
assert_type(h3_edges.line, gm.GeometryArray[gm.LineString])
assert_type(h3_edges.length, npt.NDArray[np.float64])
assert_type(
    gm.SpatialIndex([POINT]).query(POINTS).to_pairs(),
    tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]],
)
h3_cells = gm.h3_cells([1.0, 2.0], [2.0, 3.0], resolution=9)
assert_type(h3_cells.contains(h3_cell), npt.NDArray[np.bool_])
assert_type(h3_cells.neighbors, gm.Groups[gm.CellArray[gm.H3Cell]])
assert_type(h3_cells.children(), gm.Groups[gm.CellArray[gm.H3Cell]])
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
    gm.s2_cover(gm.GeometryArray([POLY])),
    gm.Groups[gm.CellArray[gm.S2Cell]],
)
assert_type(gm.s2_cover(POLY), gm.CellArray[gm.S2Cell])
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
assert_type(H3_GROUPS[0], gm.CellArray[gm.H3Cell])
assert_type(H3_GROUPS[0][0], gm.H3Cell)
assert_type(H3_GROUPS[:1], gm.Groups[gm.CellArray[gm.H3Cell]])
assert_type(H3_GROUPS.to_list(), list[list[gm.H3Cell]])
assert_type(
    gm.h3_cover(POLY, resolution=2).to_numpy(),
    npt.NDArray[np.uint64],
)
INDEX = gm.SpatialIndex(POINTS)
assert_type(INDEX.crs, gm.CRS | None)
assert_type(INDEX.epoch, float | None)
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
assert_type(POINTS.spatial_key(), npt.NDArray[np.uint64])
assert_type(POINTS.spatial_key(curve='morton'), npt.NDArray[np.uint64])
assert_type(POINTS.sort_by_spatial_key(), gm.GeometryArray[gm.Point])
assert_type(POINTS.sort_by_spatial_key(curve='morton'), gm.GeometryArray[gm.Point])
assert_type(LINE.segmentize(0.5), gm.LineString)
assert_type(LINE.segmentize(fraction=0.5), gm.LineString)
assert_type(LINE.segmentize(0.5, path='geodesic'), gm.LineString)
assert_type(
    GEOGRAPHIC_LINE.segmentize(fraction=0.5, path='rhumb'),
    gm.LineString,
)
assert_type(LINES.segmentize(0.5), gm.GeometryArray[gm.LineString])
assert_type(LINES.segmentize(fraction=0.5), gm.GeometryArray[gm.LineString])
assert_type(LINES.segmentize(0.5, path='geodesic'), gm.GeometryArray[gm.LineString])
assert_type(LINE.line_interpolate(0.5), gm.Point)
assert_type(LINE.line_interpolate(at=0.5), gm.Point)
assert_type(
    LINE.line_interpolate(0.5, normalized=True, unit='planar'),
    gm.Point,
)
assert_type(LINES.line_interpolate(at=0.5), gm.GeometryArray[gm.Point])
assert_type(LINE.line_interpolate([0.25, 0.75]), gm.GeometryArray[gm.Point])
assert_type(LINE.line_interpolate(count=3), gm.GeometryArray[gm.Point])
assert_type(
    GEOGRAPHIC_LINE.line_interpolate(count=3, unit='meters'),
    gm.GeometryArray[gm.Point],
)
assert_type(
    PROJECTED_LINE.line_interpolate(count=3, unit='meters'),
    gm.GeometryArray[gm.Point],
)
assert_type(MEASURED_LINE.line_interpolate(5.0), gm.Point)
assert_type(
    MEASURED_LINE.line_interpolate([5.0, 15.0], basis='m'),
    gm.GeometryArray[gm.Point],
)
assert_type(
    MEASURED_LINE.line_interpolate(M_STATION_COLUMN, basis='m'),
    gm.GeometryArray[gm.Point],
)
assert_type(
    MEASURED_LINES.line_interpolate([5.0], basis='m'),
    gm.GeometryArray[gm.Point],
)
assert_type(
    NULLABLE_MEASURED_LINES.line_interpolate([5.0, 5.0], basis='m'),
    gm.GeometryArray[gm.Point | None],
)
assert_type(
    LINES.line_interpolate([5.0], basis='m'),
    gm.GeometryArray[gm.Point],
)
assert_type(
    NULLABLE_LINES.line_interpolate([5.0, 15.0], basis='m'),
    gm.GeometryArray[gm.Point | None],
)
assert_type(LINE.line_locate(POINT), float)
assert_type(
    GEOGRAPHIC_LINE.line_locate(GEOGRAPHIC_POINT, normalized=True, unit='meters'),
    float,
)
assert_type(
    PROJECTED_LINE.line_locate(PROJECTED_POINT, normalized=True, unit='meters'),
    float,
)
assert_type(LINE.line_locate(point=POINT), float)
assert_type(LINE.line_locate(point=NULLABLE_POINTS), npt.NDArray[np.float64])
assert_type(
    LINE.line_locate(point=POINT, normalized=NORMALIZED),
    float,
)
assert_type(
    MEASURED_LINE.line_locate(
        point=NULLABLE_POINTS, basis='m', normalized=False, unit=None
    ),
    npt.NDArray[np.float64],
)
assert_type(MEASURED_LINE.line_locate(POINT), float)
assert_type(LINE.line_substring(0.25, 0.75), gm.LineString | gm.Point)
assert_type(
    LINE.line_substring(0.25, 0.75, normalized=True, unit='planar'),
    gm.LineString | gm.Point,
)
assert_type(MEASURED_LINE.line_substring(5.0, 10.0), gm.LineString | gm.Point)
assert_type(LINES.line_interpolate(0.5), gm.GeometryArray[gm.Point])
assert_type(LINES.line_interpolate(count=3), gm.Groups[gm.GeometryArray[gm.Point]])
assert_type(LINES.line_interpolate(count=[3]), gm.Groups[gm.GeometryArray[gm.Point]])
assert_type(
    LINES.line_interpolate(count=[3], unit='planar'),
    gm.Groups[gm.GeometryArray[gm.Point]],
)
assert_type(POINT.sample_points(3, seed=7), gm.GeometryArray[gm.Point])
assert_type(
    POINTS.sample_points([2, 3], seed=[11, 12]),
    gm.Groups[gm.GeometryArray[gm.Point]],
)
assert_type(LINES.line_locate(POINT), npt.NDArray[np.float64])
assert_type(LINES2.line_locate(point=NULLABLE_POINTS), npt.NDArray[np.float64])
assert_type(
    NULLABLE_MEASURED_LINES.line_locate(
        point=NULLABLE_POINTS, basis='m', normalized=False, unit=None
    ),
    npt.NDArray[np.float64],
)
assert_type(gm.GeometryArray([LINE]).validate(), list[gm.ValidationReport])
assert_type(NULLABLE_LINES.validate(), list[gm.ValidationReport | None])
assert_type(
    LINES.line_locate(POINT, normalized=NORMALIZED, unit='planar'),
    npt.NDArray[np.float64],
)
assert_type(
    LINES.line_substring(0.25, 0.75),
    gm.GeometryArray[gm.LineString | gm.Point],
)
assert_type(
    GEOGRAPHIC_LINES.line_substring(0.25, 0.75, normalized=True, unit='meters'),
    gm.GeometryArray[gm.LineString | gm.Point],
)
assert_type(
    gm.GeometryArray([PROJECTED_LINE]).line_substring(
        0.25, 0.75, normalized=True, unit='meters'
    ),
    gm.GeometryArray[gm.LineString | gm.Point],
)
assert_type(np.asarray(POINTS.coords), npt.NDArray[np.float64])
first_point = POINTS[0]
assert_type(first_point, gm.Point)
assert_type(first_point.x, float)
assert_type(POINTS[[True, False]], gm.GeometryArray[gm.Point])
assert_type(POINTS[[1, 0]], gm.GeometryArray[gm.Point])
assert_type(POINTS[POINTS.is_empty], gm.GeometryArray[gm.Point])
iter_point = next(iter(POINTS))
assert_type(iter_point, gm.Point)
assert_type(iter_point.x, float)
assert_type(NULLABLE_POINTS, gm.GeometryArray[gm.Point | None])
assert_type(NULLABLE_POINTS[0], gm.Point | None)
assert_type(NULLABLE_POINTS[:1], gm.GeometryArray[gm.Point | None])
assert_type(NULLABLE_POINTS[[True, False]], gm.GeometryArray[gm.Point | None])
if TYPE_CHECKING:
    reveal_type(gm.bearing(POINT, POINT))
    reveal_type(gm.bearing(POINTS, POINT))
    reveal_type(gm.bearing(NULLABLE_POINTS, POINT))
    reveal_type(gm.cross_track_distance(POINT, POINT, POINT))
    reveal_type(gm.cross_track_distance(NULLABLE_POINTS, POINT, POINT))
    reveal_type(gm.cross_track_distance(POINT, NULLABLE_POINTS, POINT))
    reveal_type(gm.point_between(POINT, POINT, 1.0))
    reveal_type(gm.point_between(NULLABLE_POINTS, POINT, 1.0))
    reveal_type(gm.point_between(POINT, NULLABLE_POINTS, 1.0, path='rhumb'))
    reveal_type(gm.distance(POINT, POINT, path='rhumb'))
    reveal_type(gm.distance(NULLABLE_POINTS, POINT, path='rhumb'))
    reveal_type(gm.crs_catalog('WGS 84'))
    reveal_type(gm.CRS(4326).to_projjson())
    reveal_type(gm.CRS(4326).to_projjson(as_dict=False))
    reveal_type(gm.CRS(4326).to_projjson(as_dict=True))
    reveal_type(gm.CRS(4326).to_projjson(as_dict=cast('bool', True)))
    reveal_type(gm.CRS(np.int64(4326)))
nullable_iter = next(iter(NULLABLE_POINTS))
assert_type(nullable_iter, gm.Point | None)
assert_type(NULLABLE_POINTS.centroid(), gm.GeometryArray[gm.Point | None])
assert_type(NULLABLE_POINTS.buffer(1.0), gm.GeometryArray[gm.Polygon | None])
assert_type(NULLABLE_POINTS.simplify(0.1), gm.GeometryArray[gm.Point | None])
assert_type(NULLABLE_POINTS.drop_missing(), gm.GeometryArray[gm.Point])
assert_type(NULLABLE_POINTS.fill_missing(POINT), gm.GeometryArray[gm.Point])
assert_type(
    NULLABLE_POINTS.fill_missing(gm.GeometryArray([None, POINT])),
    gm.GeometryArray[gm.Point],
)
assert_type(
    _drop_missing_generic(NULLABLE_POINTS),
    gm.GeometryArray[gm.Point],
)
assert_type(
    gm.GeometryArray([None, None]),
    gm.GeometryArray[gm.Geometry | None],
)
assert_type(gm.intersection(POINTS, POLY), gm.GeometryArray[gm.Point])
assert_type(gm.intersection(NULLABLE_POINTS, POLY), gm.GeometryArray[gm.Point | None])
assert_type(gm.union(POINTS, POINTS), gm.GeometryArray[gm.Point | gm.MultiPoint])
assert_type(
    gm.union(NULLABLE_POINTS, POINTS),
    gm.GeometryArray[gm.Point | gm.MultiPoint | None],
)
assert_type(gm.difference(POINTS, POLY), gm.GeometryArray[gm.Point])
assert_type(gm.difference(NULLABLE_POINTS, POLY), gm.GeometryArray[gm.Point | None])
assert_type(gm.symmetric_difference(POINTS, POLY), gm.GeometryArray[gm.Geometry])
assert_type(
    gm.symmetric_difference(NULLABLE_POINTS, POLY),
    gm.GeometryArray[gm.Geometry | None],
)
assert_type(POLY & NULLABLE_POINTS, gm.GeometryArray[gm.Point | None])
assert_type(
    gm.nearest_points(POINTS, POLY),
    tuple[gm.GeometryArray[gm.Point], gm.GeometryArray[gm.Point]],
)
assert_type(
    gm.nearest_points(NULLABLE_POINTS, POINT),
    tuple[gm.GeometryArray[gm.Point | None], gm.GeometryArray[gm.Point | None]],
)
assert_type(
    gm.nearest_points(POINT, NULLABLE_POINTS),
    tuple[gm.GeometryArray[gm.Point | None], gm.GeometryArray[gm.Point | None]],
)
assert_type(gm.shortest_line(POINTS, POLY), gm.GeometryArray[gm.LineString])
assert_type(
    gm.shortest_line(NULLABLE_POINTS, POINT), gm.GeometryArray[gm.LineString | None]
)
assert_type(
    gm.shortest_line(POINT, NULLABLE_POINTS), gm.GeometryArray[gm.LineString | None]
)
assert_type(gm.shared_paths(LINE, LINE), gm.GeometryCollection)
assert_type(
    gm.shared_paths(gm.GeometryArray([LINE]), LINE),
    gm.GeometryArray[gm.GeometryCollection],
)
assert_type(
    gm.shared_paths(gm.GeometryArray([LINE, None]), LINE),
    gm.GeometryArray[gm.GeometryCollection | None],
)
assert_type(
    gm.shared_paths(LINE, gm.GeometryArray([LINE, None])),
    gm.GeometryArray[gm.GeometryCollection | None],
)
assert_type(
    gm.from_geojson(['{"type": "Point", "coordinates": [0, 0]}']),
    gm.GeometryArray[gm.Geometry | None],
)
assert_type(
    gm.coverage_invalid_edges(POLY),
    gm.GeometryArray[gm.LineString | gm.MultiLineString],
)
assert_type(
    gm.coverage_invalid_edges([POLY, POLY]),
    gm.GeometryArray[gm.LineString | gm.MultiLineString],
)
assert_type(
    gm.coverage_simplify([POLY, gm.box(2, 0, 4, 2)], 0.1),
    gm.GeometryArray[gm.Polygon | gm.MultiPolygon],
)
assert_type(
    gm.coverage_clean([POLY, POLY]),
    gm.GeometryArray[gm.Polygon | gm.MultiPolygon],
)
assert_type(
    gm.from_polyline(['_p~iF~ps|U']),
    gm.GeometryArray[gm.Point | gm.LineString],
)
assert_type(
    gm.coverage_invalid_edges(gm.GeometryArray([POLY, None])),
    gm.GeometryArray[gm.LineString | gm.MultiLineString | None],
)
NULLABLE_H3 = gm.CellArray([h3_cell, None])
assert_type(NULLABLE_H3, gm.CellArray[gm.H3Cell | None])
assert_type(
    gm.CellArray([None, h3_cell], type=gm.H3Cell),
    gm.CellArray[gm.H3Cell | None],
)
assert_type(NULLABLE_H3[0], gm.H3Cell | None)
assert_type(NULLABLE_H3[:1], gm.CellArray[gm.H3Cell | None])
assert_type(next(iter(NULLABLE_H3)), gm.H3Cell | None)
assert_type(NULLABLE_H3.parent(), gm.CellArray[gm.H3Cell | None])
assert_type(NULLABLE_H3.compact(), gm.CellArray[gm.H3Cell])
assert_type(NULLABLE_H3.contains(h3_cell), npt.NDArray[np.bool_])
assert_type(NULLABLE_H3.to_numpy(), npt.NDArray[np.uint64] | npt.NDArray[np.object_])
assert_type(NULLABLE_H3.__array__(), npt.NDArray[np.uint64] | npt.NDArray[np.object_])
assert_type(NULLABLE_H3.__array__(dtype=np.dtype(np.object_)), npt.NDArray[np.object_])
nullable_h3_var: gm.CellArray[gm.H3Cell | None] = NULLABLE_H3
assert_type(
    nullable_h3_var.to_numpy(), npt.NDArray[np.uint64] | npt.NDArray[np.object_]
)
assert_type(
    nullable_h3_var.__array__(), npt.NDArray[np.uint64] | npt.NDArray[np.object_]
)
assert_type(NULLABLE_H3.neighbors, gm.Groups[gm.CellArray[gm.H3Cell]])
assert_type(NULLABLE_H3.children(), gm.Groups[gm.CellArray[gm.H3Cell]])
assert_type(
    NULLABLE_H3.value_counts(),
    tuple[gm.CellArray[gm.H3Cell], npt.NDArray[np.int64]],
)
assert_type(
    NULLABLE_H3.factorize(),
    tuple[npt.NDArray[np.int64], gm.CellArray[gm.H3Cell]],
)
GRID = gm.GeometryArray([POLY, gm.box(2, 0, 4, 2)])
assert_type(
    gm.coverage_simplify(GRID, 0.1), gm.GeometryArray[gm.Polygon | gm.MultiPolygon]
)
assert_type(gm.coverage_is_valid([POLY]), bool)
assert_type(gm.coverage_is_valid(POLY), bool)
assert_type(gm.coverage_union_all(iter([POLY])), gm.Polygon | gm.MultiPolygon)
assert_type(gm.coverage_union_all(POLY), gm.Polygon | gm.MultiPolygon)
assert_type(gm.polygonize_all(POLY), gm.GeometryArray[gm.Polygon])
assert_type(gm.coverage_union_all(GRID), gm.Polygon | gm.MultiPolygon)
assert_type(gm.coverage_union_all(POLYS), gm.Polygon | gm.MultiPolygon)
assert_type(
    gm.coverage_union_all(NULLABLE_POLYGONS),
    gm.Polygon | gm.MultiPolygon,
)
assert_type(gm.polygonize_all(GRID), gm.GeometryArray[gm.Polygon])
assert_type(gm.union_all(GRID), gm.Geometry)
assert_type(gm.union_all(POINT), gm.Point | gm.MultiPoint)
assert_type(gm.union_all(POINTS), gm.Point | gm.MultiPoint)
assert_type(gm.union_all(NULLABLE_POINTS), gm.Point | gm.MultiPoint)
assert_type(gm.intersection_all(POINT), gm.Point | gm.MultiPoint)
assert_type(gm.intersection_all(POINTS), gm.Point | gm.MultiPoint)
assert_type(gm.intersection_all(NULLABLE_POINTS), gm.Point | gm.MultiPoint)
assert_type(gm.symmetric_difference_all(POINT), gm.Point | gm.MultiPoint)
assert_type(gm.symmetric_difference_all(POINTS), gm.Point | gm.MultiPoint)
assert_type(gm.symmetric_difference_all(NULLABLE_POINTS), gm.Point | gm.MultiPoint)
assert_type(gm.to_feature_collection(POLY), sht.GeoJsonFeatureCollection)
assert_type(POINT.voronoi(), gm.GeometryArray[gm.Polygon])
assert_type(
    POINT.voronoi(output='edges'),
    gm.GeometryArray[gm.LineString],
)
assert_type(POINTS.voronoi(), gm.Groups[gm.GeometryArray[gm.Polygon]])
assert_type(
    POINTS.voronoi(output='edges'),
    gm.Groups[gm.GeometryArray[gm.LineString]],
)

# Overload-narrowing coverage: one probe per @overload group that once shipped
# dead (undecorated) variants — pyright's last-def-wins silently widened these
# to unions until the 2026-07 repair. Every group keeps a live probe.
assert_type(POLY & POLY, gm.Geometry)
assert_type(POLY | POLY, gm.Geometry)
assert_type(POLY - POLY, gm.Geometry)
assert_type(POLY ^ POLY, gm.Geometry)
assert_type(POLY & POINTS, gm.GeometryArray[gm.Point])
assert_type(POINTS.buffer(1.0), gm.GeometryArray[gm.Polygon])
assert_type(POINTS[:1], gm.GeometryArray[gm.Point])
assert_type(POINTS.concat(POINTS), gm.GeometryArray[gm.Point])
MULTI = gm.MultiPoint([(0.0, 0.0), (1.0, 1.0)])
assert_type(MULTI.parts[0], gm.Point)
assert_type(MULTI.parts[:1], list[gm.Point])
BARE_OK: gm.GeometryArray = POINTS  # PEP 696 default: bare == [Geometry]
BARE_CELLS_OK: gm.CellArray = h3_cells  # PEP 696 default: bare == [Cell]

# --- High-value free-function overload groups (manifest: _OVERLOAD_TARGETS) ---
assert_type(gm.contains(POLY, POINT), bool)
assert_type(gm.equals(POLY, POLY), bool)
assert_type(gm.contains(POINTS, POINT), npt.NDArray[np.bool_])
assert_type(gm.intersects(POLY, POINT), bool)
assert_type(gm.intersects(POINTS, POINT), npt.NDArray[np.bool_])

# A scalar is valid on either side of every topological predicate against an
# array.
assert_type(gm.contains(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.contains(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.contains_properly(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.contains_properly(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.within(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.within(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.covers(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.covers(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.covered_by(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.covered_by(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.intersects(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.intersects(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.relate(POLY, POLY), str)
assert_type(gm.relate(POLY, POLY), str)
assert_type(gm.relate(POLY, POINTS), list[str | None])
assert_type(gm.relate(POINTS, POLY), list[str | None])
assert_type(gm.relate(NULLABLE_POINTS, POLY), list[str | None])
assert_type(gm.relate(POLY, NULLABLE_POINTS), list[str | None])
assert_type(gm.relate_pattern(POLY, POLY, 'T********'), bool)
assert_type(gm.relate_pattern(POLY, POLY, 'T********'), bool)
assert_type(gm.relate_pattern(POLY, POINTS, 'T********'), npt.NDArray[np.bool_])
assert_type(gm.relate_pattern(POINTS, POLY, 'T********'), npt.NDArray[np.bool_])
assert_type(gm.disjoint(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.disjoint(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.touches(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.touches(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.crosses(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.crosses(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.overlaps(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.overlaps(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.equals(POLY, POINTS), npt.NDArray[np.bool_])
assert_type(gm.equals(POINTS, POLY), npt.NDArray[np.bool_])
assert_type(gm.intersects_xy(POLY, 0.0, 0.0), bool)
assert_type(gm.intersects_xy(POLY, [0.0], [0.0]), npt.NDArray[np.bool_])
assert_type(gm.equals(POLY, POLY), bool)
assert_type(gm.dwithin(POLY, POLY, 0.0), bool)
assert_type(gm.equals_exact(POLY, POLY), bool)
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
assert_type(gm.from_wkb([_WKB, None]), gm.GeometryArray[gm.Geometry | None])
assert_type(gm.Point(0.0, 0.0), gm.Point)

# Dtype-family overloads: probe __array__/to_numpy directly (np.asarray's own
# overloads erase dtype precision for array-protocol objects). Pass real
# ``np.dtype`` instances so the runtime call (assert_type evaluates args) works.
assert_type(POINT.coords.__array__(), npt.NDArray[np.float64])
assert_type(POINT.coords.__array__(dtype=np.dtype(np.float32)), npt.NDArray[np.float32])
assert_type(POINTS.__array__(), npt.NDArray[np.object_])
assert_type(POINTS.to_numpy(), npt.NDArray[np.object_])
assert_type(h3_cells.__array__(), npt.NDArray[np.uint64])
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
assert_type(gm.crs_catalog('WGS', kind='geographic_2d'), list[sht.CrsCatalogInfo])
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

if TYPE_CHECKING:

    def _check_voronoi_output_union(output: sht.VoronoiOutput) -> None:
        assert_type(
            POINT.voronoi(output=output),
            gm.GeometryArray[gm.Polygon] | gm.GeometryArray[gm.LineString],
        )
        assert_type(
            POINTS.voronoi(output=output),
            gm.Groups[gm.GeometryArray[gm.Polygon]]
            | gm.Groups[gm.GeometryArray[gm.LineString]],
        )
        reveal_type(POINT.voronoi(output=output))
        reveal_type(POINTS.voronoi(output=output))

    # CE01: repair narrows only to kinds its runtime can actually produce.
    reveal_type(LINE.repair())
    reveal_type(MULTILINE.repair())
    reveal_type(LINES.repair())
    reveal_type(NULLABLE_LINES.repair())
    reveal_type(MULTILINES.repair())
    reveal_type(NULLABLE_MULTILINES.repair())

    # CE02: split preserves a group for every line row and keeps scalar
    # splitter-array use pooled.
    reveal_type(gm.split(LINE, POINT))
    reveal_type(gm.split(LINE, NULLABLE_POINTS))
    reveal_type(gm.split(LINES, POINT))
    reveal_type(gm.split(NULLABLE_LINES, NULLABLE_POINTS))

    # CE03: one line accepts an M-station column as a point array.
    reveal_type(MEASURED_LINE.line_interpolate(5.0, basis='m'))
    reveal_type(MEASURED_LINE.line_interpolate([5.0, 15.0], basis='m'))
    reveal_type(MEASURED_LINE.line_interpolate(np.array([5.0, 15.0]), basis='m'))
    reveal_type(MEASURED_LINES.line_interpolate([5.0], basis='m'))
    reveal_type(NULLABLE_MEASURED_LINES.line_interpolate([5.0, 5.0], basis='m'))

    # CE04: validation preserves known row nullability and locate accepts only
    # point queries, including nullable point columns.
    reveal_type(gm.GeometryArray([LINE]).validate())
    reveal_type(NULLABLE_LINES.validate())
    reveal_type(LINE.line_locate(point=POINT))
    reveal_type(MEASURED_LINE.line_locate(point=NULLABLE_POINTS, basis='m'))
    reveal_type(LINES.line_locate(point=NULLABLE_POINTS, normalized=NORMALIZED))

    # CE05: the pooled coverage aggregate has the same result for scalar,
    # nullable-array and iterable polygon inputs.
    reveal_type(gm.coverage_union_all(POLY))
    reveal_type(gm.coverage_union_all(NULLABLE_POLYGONS))
    reveal_type(gm.coverage_union_all(iter([POLY])))

    # CE06: a Literal output chooses a return kind; a union-typed value keeps
    # both choices visible, and array inputs retain Groups.
    reveal_type(POINT.voronoi())
    reveal_type(POINT.voronoi(output='edges'))
    reveal_type(POINTS.voronoi())
    reveal_type(POINTS.voronoi(output='edges'))

    # CE08: point-only aggregates retain their closed result family for
    # scalar, dense-array and nullable-array inputs.
    reveal_type(gm.union_all(POINT))
    reveal_type(gm.union_all(POINTS))
    reveal_type(gm.union_all(NULLABLE_POINTS))
    reveal_type(gm.intersection_all(NULLABLE_POINTS))
    reveal_type(gm.symmetric_difference_all(NULLABLE_POINTS))
