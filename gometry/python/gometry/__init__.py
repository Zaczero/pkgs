"""gometry: fast, numpy-native geospatial geometry for Python.

A vectorized geometry library backed by a Rust core. Geometries are immutable
and CRS-aware, and operations broadcast element-wise over arrays without
per-element Python overhead. The CRS decides the measure: geographic CRS use
geodesic meters, projected CRS use native linear units by default, and
``unit='meters'`` forces SI when you need it.

The top-level namespace holds geometry classes (`Point`, `Polygon`, `box`,
...), binary predicates and overlays (`gm.intersects`, `gm.union`, ...),
`gm.distance(a, b)`, measure overrides (`gm.area` / `gm.length`, with optional
`unit=`), and unary work on `Geometry` / `GeometryArray` (`geom.area`,
`geom.buffer`, `geom.to_crs`, ...).
Specialized functions use searchable family prefixes: ``crs_*`` for coordinate
reference systems; ``h3_*``, ``s2_*``, ``geohash_*``, and ``tile_*`` for grids;
and codec-specific ``pluscode_*`` / ``osm_shortlink_*`` names for point codes.
API placement follows one doctrine: facts are properties, unary geometry work is
on `Geometry`/`GeometryArray`, binary relationships are free functions, and
grid receiver-ops live on cells, arrays, and coverages.

Examples
--------
>>> import gometry as gm
>>> stop = gm.Point(2.3479, 48.8589, crs=4326)
>>> gm.intersects(gm.box(2.34, 48.85, 2.36, 48.87, crs=4326), stop)
True
>>> round(stop.buffer(500).area)  # geodesic square meters
780942
"""

from __future__ import annotations

import collections.abc as _cabc
import importlib as _importlib
from typing import get_type_hints as _get_type_hints

TYPE_CHECKING = False

if TYPE_CHECKING:
    # Lazy exports (PEP 562 `__getattr__` below) declared for checkers and
    # IDEs; nothing here imports at runtime, so pandas/pyarrow/lonboard stay
    # deferred until first attribute access.
    from gometry._geopandas import from_geopandas as from_geopandas
    from gometry._geoparquet import from_geoparquet as from_geoparquet
    from gometry._pandas import from_pandas as from_pandas
    from gometry._polars import from_polars as from_polars
    from gometry._viz import explore as explore

# The exact sentinel spelling above is recognized by type checkers, but it is
# implementation machinery rather than a user-facing ``gm.`` suggestion.
del TYPE_CHECKING

from gometry._lib import (
    CRS,
    AccuracyWarning,
    CellArray,
    Coordinates,
    CRSError,
    CRSMismatchError,
    GeohashCell,
    GeohashCoverage,
    Geometry,
    GeometryArray,
    GeometryCollection,
    GeometryError,
    GeometryParts,
    GeometryTypeError,
    Groups,
    H3Cell,
    H3Coverage,
    H3Edge,
    H3EdgeArray,
    H3Vertex,
    H3VertexArray,
    InvalidGeometryError,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    ParseError,
    Point,
    Polygon,
    PreparedGeometry,
    S2Cell,
    S2Coverage,
    SpatialIndex,
    Tile,
    TileCoverage,
    TransformError,
    ValidationReport,
    area,
    bearing,
    bounds,
    box,
    boxes,
    contains,
    contains_properly,
    contains_xy,
    coverage_clean,
    coverage_invalid_edges,
    coverage_is_valid,
    coverage_simplify,
    coverage_union,
    covered_by,
    covers,
    cross_track_distance,
    crosses,
    crs_apply,
    crs_authorities,
    crs_cache_info,
    crs_catalog,
    crs_celestial_bodies,
    crs_clear_cache,
    crs_codes,
    crs_config,
    crs_configure,
    crs_ellipsoids,
    crs_engine,
    crs_grid,
    crs_info,
    crs_prime_meridians,
    crs_proj_operations,
    crs_reset,
    crs_roundtrip,
    crs_search,
    crs_transform,
    crs_transform_bounds,
    crs_unit,
    crs_units,
    crs_utm_zones,
    destination,
    difference,
    disjoint,
    distance,
    distance_3d,
    dwithin,
    equals,
    equals_exact,
    equals_identical,
    frechet_distance,
    from_arrow,
    from_features,
    from_geojson,
    from_polyline,
    from_wkb,
    from_wkt,
    geohash_bounding_cell,
    geohash_cells,
    geohash_cover,
    geohash_difference,
    geohash_intersection,
    geohash_union,
    get_coordinates,
    h3_base_cells,
    h3_bounding_cell,
    h3_cells,
    h3_cover,
    h3_difference,
    h3_intersection,
    h3_pentagons,
    h3_union,
    hausdorff_distance,
    intersection,
    intersection_all,
    intersects,
    intersects_xy,
    join,
    length,
    length_3d,
    line_strings,
    multi_line_strings,
    multi_points,
    multi_polygons,
    nearest,
    nearest_points,
    osm_shortlink_encode,
    osm_shortlink_location,
    overlaps,
    parts,
    pluscode_encode,
    pluscode_polygon,
    pluscode_recover,
    pluscode_shorten,
    point_between,
    points,
    polygonize,
    polygonize_full,
    polygons,
    relate,
    relate_pattern,
    require,
    rhumb_distance,
    rings,
    s2_bounding_cell,
    s2_cells,
    s2_cover,
    s2_difference,
    s2_intersection,
    s2_union,
    shared_paths,
    shortest_line,
    snap,
    split,
    symmetric_difference,
    symmetric_difference_all,
    tile_bounding_cell,
    tile_cells,
    tile_cover,
    tile_difference,
    tile_intersection,
    tile_union,
    to_feature,
    to_feature_collection,
    touches,
    union,
    union_all,
    within,
)
from gometry._lib import (
    __version__ as __version__,
)
from gometry._types import (
    Cell,
    Coverage,
    Extremes,
    Features,
    PolygonizeResult,
)

# Public Python-defined types have the same canonical identity users import
# and document; the private module is only their implementation home.
for _public_type in (Cell, Coverage, Extremes, Features, PolygonizeResult):
    # Resolve class annotations while their real implementation module still
    # owns the private aliases.  After publishing the canonical module name,
    # `typing.get_type_hints()` must not need fake private globals on gometry.
    _public_type.__annotations__ = _get_type_hints(_public_type)
    _public_type.__module__ = __name__
del _get_type_hints, _public_type

del annotations

# These native containers honor the full sequence protocol (len/getitem/iter/
# reversed/in/index/count); registering makes `isinstance(value, Sequence)`
# true. Each class also declares `sequence` natively so immutable type objects
# do not need this registration to acquire Py_TPFLAGS_SEQUENCE.
_NATIVE_SEQUENCE_TYPES = (
    GeometryArray,
    CellArray,
    Groups,
    GeometryParts,
    Coordinates,
    H3VertexArray,
    H3EdgeArray,
    H3Coverage,
    S2Coverage,
    GeohashCoverage,
    TileCoverage,
)


def _register_native_sequences() -> None:
    for sequence_type in _NATIVE_SEQUENCE_TYPES:
        _cabc.Sequence.register(sequence_type)  # pyright: ignore[reportAttributeAccessIssue]


_register_native_sequences()
del _register_native_sequences


__all__ = [  # noqa: RUF022 - public API order follows Python's sorted() contract
    'AccuracyWarning',
    'CRS',
    'CRSError',
    'CRSMismatchError',
    'Cell',
    'CellArray',
    'Coordinates',
    'Coverage',
    'Extremes',
    'Features',
    'GeohashCell',
    'GeohashCoverage',
    'Geometry',
    'GeometryArray',
    'GeometryCollection',
    'GeometryError',
    'GeometryParts',
    'GeometryTypeError',
    'Groups',
    'H3Cell',
    'H3Coverage',
    'H3Edge',
    'H3EdgeArray',
    'H3Vertex',
    'H3VertexArray',
    'InvalidGeometryError',
    'LineString',
    'MultiLineString',
    'MultiPoint',
    'MultiPolygon',
    'ParseError',
    'Point',
    'Polygon',
    'PolygonizeResult',
    'PreparedGeometry',
    'S2Cell',
    'S2Coverage',
    'SpatialIndex',
    'Tile',
    'TileCoverage',
    'TransformError',
    'ValidationReport',
    'area',
    'bearing',
    'bounds',
    'box',
    'boxes',
    'contains',
    'contains_properly',
    'contains_xy',
    'coverage_clean',
    'coverage_invalid_edges',
    'coverage_is_valid',
    'coverage_simplify',
    'coverage_union',
    'covered_by',
    'covers',
    'cross_track_distance',
    'crosses',
    'crs_apply',
    'crs_authorities',
    'crs_cache_info',
    'crs_catalog',
    'crs_celestial_bodies',
    'crs_clear_cache',
    'crs_codes',
    'crs_config',
    'crs_configure',
    'crs_ellipsoids',
    'crs_engine',
    'crs_grid',
    'crs_info',
    'crs_prime_meridians',
    'crs_proj_operations',
    'crs_reset',
    'crs_roundtrip',
    'crs_search',
    'crs_transform',
    'crs_transform_bounds',
    'crs_unit',
    'crs_units',
    'crs_utm_zones',
    'destination',
    'difference',
    'disjoint',
    'distance',
    'distance_3d',
    'dwithin',
    'equals',
    'equals_exact',
    'equals_identical',
    'frechet_distance',
    'from_arrow',
    'from_features',
    'from_geojson',
    'from_polyline',
    'from_wkb',
    'from_wkt',
    'geohash_bounding_cell',
    'geohash_cells',
    'geohash_cover',
    'geohash_difference',
    'geohash_intersection',
    'geohash_union',
    'get_coordinates',
    'h3_base_cells',
    'h3_bounding_cell',
    'h3_cells',
    'h3_cover',
    'h3_difference',
    'h3_intersection',
    'h3_pentagons',
    'h3_union',
    'hausdorff_distance',
    'intersection',
    'intersection_all',
    'intersects',
    'intersects_xy',
    'join',
    'length',
    'length_3d',
    'line_strings',
    'multi_line_strings',
    'multi_points',
    'multi_polygons',
    'nearest',
    'nearest_points',
    'osm_shortlink_encode',
    'osm_shortlink_location',
    'overlaps',
    'parts',
    'pluscode_encode',
    'pluscode_polygon',
    'pluscode_recover',
    'pluscode_shorten',
    'point_between',
    'points',
    'polygonize',
    'polygonize_full',
    'polygons',
    'relate',
    'relate_pattern',
    'require',
    'rhumb_distance',
    'rings',
    's2_bounding_cell',
    's2_cells',
    's2_cover',
    's2_difference',
    's2_intersection',
    's2_union',
    'shared_paths',
    'shortest_line',
    'snap',
    'split',
    'symmetric_difference',
    'symmetric_difference_all',
    'tile_bounding_cell',
    'tile_cells',
    'tile_cover',
    'tile_difference',
    'tile_intersection',
    'tile_union',
    'to_feature',
    'to_feature_collection',
    'touches',
    'union',
    'union_all',
    'within',
]


# Optional conversion and visualization functions stay lazy; their static
# declarations above preserve IDE discovery without importing dependencies.
# Resolved adapters are not written into module globals — the imported adapter
# module is already cached by the import system, and ``dir(gometry)`` /
# ``__all__`` stay the import-safe core surface (never force optional imports
# via ``pydoc`` / ``inspect.getmembers``).
_LAZY_EXPORTS: dict[str, str] = {
    'explore': 'gometry._viz',
    'from_geopandas': 'gometry._geopandas',
    'from_geoparquet': 'gometry._geoparquet',
    'from_pandas': 'gometry._pandas',
    'from_polars': 'gometry._polars',
}


def __getattr__(name: str) -> object:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is not None:
        return getattr(_importlib.import_module(module_name), name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
