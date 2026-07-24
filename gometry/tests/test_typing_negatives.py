"""Negative typing conformance: misuse that must FAIL type-checking.

Every line here is a deliberate error carrying a ``# type: ignore[code]``.
``tools/gates/_check_typing_runtime.py`` runs mypy with ``--warn-unused-ignores``
and pyright with ``reportUnnecessaryTypeIgnoreComment=error`` over this file:
if the stub ever *loosens* (an overload widens, a Literal vocabulary drifts,
``@final`` is dropped), the corresponding ignore becomes unused and both
checkers fail. The positive twin is ``test_typing_conformance.py``.

Everything lives under ``if TYPE_CHECKING`` — the misuse must never execute.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import gometry as gm


def test_negatives_are_static_only() -> None:
    """Runtime placeholder: the checks in this module are static."""


if TYPE_CHECKING:
    import gometry._types as sht
    import numpy as np

    POINT = gm.Point(0.0, 0.0)
    POLY = gm.box(0.0, 0.0, 2.0, 2.0)
    POINTS = gm.GeometryArray([POINT])
    GEOMS: gm.GeometryArray[gm.Geometry] = gm.GeometryArray([POLY])
    H3_CELLS = gm.h3_cells([1.0], [2.0], resolution=9)
    S2_CELLS = gm.s2_cells([1.0], [2.0], level=9)

    # Domain families are flat, prefix-discoverable top-level functions;
    # namespace spellings are deliberately not retained for this first release.

    # Literal vocabulary: unknown token must be rejected.
    POINT.buffer(1.0, cap_style='rounds')  # type: ignore[arg-type]

    # Argument types: strings are not distances.
    POINT.buffer('1.0')  # type: ignore[arg-type]

    # Unknown keyword arguments are rejected.
    POINT.buffer(1.0, wrong_kw=2)  # type: ignore[call-arg]

    # Algorithm options are available only for the matching literal method.
    POINT.triangulate(method='delaunay', min_angle=10.0)  # type: ignore[call-overload]
    POINT.triangulate(method='earcut', max_area=1.0)  # type: ignore[call-overload]
    POINT.triangulate(method='not_a_method')  # type: ignore[call-overload]
    gm.to_feature_collection(gm.Features(POINTS), properties={})  # type: ignore[arg-type]
    POINT.spatial_key(curve='zorder')  # type: ignore[arg-type]
    POINTS.sort_by_spatial_key(curve='zorder')  # type: ignore[arg-type]

    # area/length default ``unit`` exactly like their properties, so the bare
    # call is valid; what stays constrained is the token vocabulary.
    gm.area(POINT, unit='metres')  # type: ignore[call-overload]
    gm.length(POINT, unit='metres')  # type: ignore[call-overload]

    # Segmentization has exactly two mutually exclusive shapes.
    LINE = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    LINE.segmentize(max_length=0.5)  # type: ignore[call-overload]
    LINE.segmentize(0.5, fraction=0.5)  # type: ignore[call-overload]
    LINE.segmentize()  # type: ignore[call-overload]
    LINE.segmentize(None)  # type: ignore[call-overload]

    # ``basis`` is a closed vocabulary. The native boundary validates
    # distance-only kwargs whenever runtime ``basis='m'`` is chosen; the
    # canonical fallback keeps that exact runtime signature available.
    LINE.line_interpolate(1.0, basis='not_a_basis')  # type: ignore[call-overload]

    # Rhumb navigation is point-only and always uses metres: a literal rhumb
    # route must not accept the geodesic ``unit`` override.
    gm.bearing(POINT, POINT, path='constant')  # type: ignore[call-overload]
    gm.destination(POINT, 90.0, 1_000.0, path='rhumb', unit='meters')  # type: ignore[call-overload]
    gm.destination(POLY, 90.0, 1_000.0, path='rhumb')  # type: ignore[call-overload]
    gm.point_between(POINT, POINT, 0.5, path='rhumb', unit='meters')  # type: ignore[call-overload]
    gm.point_between(POINT, POINT, 0.5, path='rhumb', unit='planar')  # type: ignore[call-overload]
    LINE.line_interpolate(1.0, basis='m', unit='meters')  # type: ignore[call-overload]
    LINE.line_interpolate(count=2, basis='m')  # type: ignore[call-overload]
    LINE.line_substring(0.0, 1.0, basis='m', unit='planar')  # type: ignore[call-overload]
    LINE.line_substring(0.0, 1.0, basis='m', normalized=True)  # type: ignore[call-overload]

    # Runtime-final classes cannot be subclassed.
    class _MyPoint(gm.Point):  # type: ignore[misc]
        pass

    # Return-only native types have no statically constructible path.
    gm.Geometry()  # type: ignore[call-arg]
    gm.Coordinates()  # type: ignore[call-arg]
    gm.GeometryParts()  # type: ignore[call-arg]
    gm.Groups()  # type: ignore[call-arg]
    gm.PreparedGeometry()  # type: ignore[call-arg]
    gm.ValidationReport()  # type: ignore[call-arg]
    gm.H3Coverage()  # type: ignore[call-arg]
    gm.S2Coverage()  # type: ignore[call-arg]
    gm.GeohashCoverage()  # type: ignore[call-arg]
    gm.TileCoverage()  # type: ignore[call-arg]

    # Covariance permits this boundary widening, but its erased CellArray
    # receiver cannot consume a different grid scalar.
    def _reject_cross_grid_after_widening(
        cells: gm.CellArray[gm.Cell], other: gm.S2Cell
    ) -> None:
        cells.contains(other)  # type: ignore
        cells.intersects(other)  # type: ignore

    _reject_cross_grid_after_widening(H3_CELLS, S2_CELLS[0])

    # nearest_points is exactly a pair — a 3-name unpack must fail.
    _a, _b, _c = gm.nearest_points(POINT, POLY)  # type: ignore[misc]

    # Geometry operators take geometries, not numbers.
    POINT & 1.0  # type: ignore[operator]

    # Covariance points one way: a Geometry array is not a Point array.
    _pts: gm.GeometryArray[gm.Point] = GEOMS  # type: ignore[assignment]

    # Scalar overload result is a bare float — it has no ndarray attributes.
    _ = gm.distance(POINT, POLY).dtype  # type: ignore[attr-defined]

    # OSM shortlink zoom is an int.
    gm.osm_shortlink_encode(0.0, 0.0, zoom='17')  # type: ignore[call-overload]

    # query_pairs returns plain (left, right) columns, not an object.
    _ = gm.SpatialIndex(POINTS).query_pairs().pairs  # type: ignore[attr-defined]

    # Selector-dependent index calls deliberately have no broad fallback:
    # dynamically correlated ``predicate``/``distance`` values must be narrowed
    # before the call, so literal misuse gets an immediate diagnostic.
    INDEX = gm.SpatialIndex(POINTS)
    INDEX.query(POINT, predicate='dwithin')  # type: ignore[call-overload]
    INDEX.query(POINT, predicate='intersects', distance=1.0)  # type: ignore[call-overload]
    INDEX.join(POINTS, predicate='dwithin')  # type: ignore[call-overload]
    INDEX.join(POINTS, predicate='within', distance=1.0)  # type: ignore[call-overload]
    INDEX.query_pairs(predicate='dwithin')  # type: ignore[call-overload]
    INDEX.query_pairs(predicate='equals', distance=1.0)  # type: ignore[call-overload]
    INDEX.explain(predicate='dwithin')  # type: ignore[call-overload]
    INDEX.explain(predicate='intersects', distance=1.0)  # type: ignore[call-overload]
    gm.join(POINTS, POINTS, predicate='dwithin')  # type: ignore[call-overload]
    gm.join(POINTS, POINTS, predicate='overlaps', distance=1.0)  # type: ignore[call-overload]

    # --- High-value free-function overload groups (manifest: _OVERLOAD_TARGETS) ---
    # Binary predicates reject non-geometry operands.
    gm.contains(POINT, 1.0)  # type: ignore[call-overload]
    gm.intersects(POINT, 'not-a-geom')  # type: ignore[call-overload]
    gm.distance(POINT, 0)  # type: ignore[call-overload]
    # from_wkt/from_wkb reject wrong payload types.
    gm.from_wkt(123)  # type: ignore[call-overload]
    gm.from_wkb(123)  # type: ignore[call-overload]
    # Point constructor rejects non-numeric coordinates.
    gm.Point('x', 'y')  # type: ignore[call-overload]
    gm.Point(None, 1.0)  # type: ignore[call-overload]
    gm.LineString([(0.0, 0.0), (1.0, 1.0)], x=[0.0], y=[1.0])  # type: ignore[call-overload]
    POINT.set_coordinates()  # type: ignore[call-overload]
    POINT.set_coordinates(x=[0.0])  # type: ignore[call-overload]
    POINTS.to_geoparquet('points.parquet', encoding='bogus')  # type: ignore[arg-type]

    # Optional dataframe boundaries are explicit and statically typed; native
    # arrays are not framework Series/GeoSeries values.
    gm.from_pandas(POINTS)  # pyright: ignore[reportArgumentType]
    POINTS.to_pandas(name=[])  # type: ignore[arg-type]
    gm.from_polars(POINTS)  # type: ignore[arg-type]
    POINTS.to_polars(name=1)  # type: ignore[arg-type]
    gm.from_geopandas(POINTS)  # pyright: ignore[reportArgumentType]
    POINTS.to_geopandas(drop_epoch='yes')  # type: ignore[arg-type]

    # Geometry-like collection lanes accept native geometries, binary buffers,
    # mappings, and __geo_interface__ objects — not ambiguous text or numbers.
    gm.GeometryArray([POINT, 'POINT (1 2)'])  # type: ignore[list-item]
    gm.SpatialIndex([1])  # type: ignore[list-item]

    # Affine origins are two-value float iterables or the two named anchors.
    POINT.scale(2.0, origin=['x', 'y'])  # type: ignore[list-item]

    # CellArray inference requires actual homogeneous typed cells; raw
    # identities need an explicit type= discriminator.
    gm.CellArray([H3_CELLS[0].id])  # type: ignore[list-item]

    # Cell factories have two disjoint grammars: an existing identity, or a
    # point/longitude plus its required depth. A bare point is neither.
    gm.H3Cell(POINT)  # type: ignore[call-overload]
    gm.S2Cell(POINT)  # type: ignore[call-overload]
    gm.GeohashCell(POINT)  # type: ignore[call-overload]
    gm.Tile(POINT)  # type: ignore[call-overload]

    # A point is encoded alone; a numeric longitude always needs latitude.
    gm.pluscode_encode(8.5)  # type: ignore[call-overload]
    gm.pluscode_encode(POINT, 47.0)  # type: ignore[call-overload]

    # Antimeridian splitting is explicit metadata-bearing geography.
    gm.box(170.0, -10.0, -170.0, 10.0, wrap='split')  # type: ignore[call-overload]

    # Dtype families: GeometryArray/CellArray reject float dtypes statically
    # (probe __array__/to_numpy — np.asarray erases the protocol dtype check).
    POINTS.__array__(dtype=np.float64)  # type: ignore[arg-type]
    POINTS.to_numpy(np.float64)  # type: ignore[call-arg]
    H3_CELLS.__array__(dtype=np.float64)  # type: ignore[arg-type]
    POINT.coords.__array__(dtype=np.int64)  # type: ignore[arg-type]

    # CRS kind vocabulary rejects unknown tokens.
    gm.crs_search('WGS', kind='not_a_kind')  # type: ignore[arg-type]
    gm.crs_catalog(kind='ellipsoid')  # type: ignore[arg-type]
    gm.crs_codes('EPSG', kind='nope')  # type: ignore[arg-type]
    _bad_kind: sht.CrsKind = 'not_a_kind'  # type: ignore[assignment]
