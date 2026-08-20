"""Single presentation manifest for the generated API reference.

The public inventory still belongs to :mod:`gometry`: this file only assigns
each exported symbol to one user-facing page.  ``gen_api_pages`` checks that the
resolved manifest is an exact, non-overlapping partition of ``gometry.__all__``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Section:
    """One optional subsection of a reference page."""

    title: str | None
    symbols: tuple[str, ...] = ()
    prefixes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Page:
    """One generated reference page."""

    path: str
    title: str
    description: str
    sections: tuple[Section, ...]
    related: tuple[tuple[str, str], ...] = ()


def section(
    *symbols: str,
    title: str | None = None,
    prefixes: tuple[str, ...] = (),
) -> Section:
    return Section(title, symbols, prefixes)


GEOMETRY_LEAVES = (
    'Point',
    'MultiPoint',
    'LineString',
    'MultiLineString',
    'Polygon',
    'MultiPolygon',
    'GeometryCollection',
)

OPTIONAL_EXPORTS = frozenset({
    'explore',
    'from_geopandas',
    'from_geoparquet',
    'from_pandas',
    'from_polars',
})

CLASS_EXPORTS = frozenset({
    'AccuracyWarning',
    'CRS',
    'CRSError',
    'CRSMismatchError',
    'Cell',
    'CellArray',
    'Coordinates',
    'Extremes',
    'Features',
    'GeohashCell',
    'Geometry',
    'GeometryArray',
    'GeometryCollection',
    'GeometryError',
    'GeometryParts',
    'GeometryTypeError',
    'Groups',
    'H3Cell',
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
    'SpatialIndex',
    'Tile',
    'TransformError',
    'ValidationReport',
})

ERRORS = (
    'GeometryError',
    'InvalidGeometryError',
    'GeometryTypeError',
    'CRSError',
    'CRSMismatchError',
    'TransformError',
    'ParseError',
)


PAGES: tuple[Page, ...] = (
    Page(
        'toplevel/constructors-bulk-factories',
        'Constructors & bulk factories',
        'Build packed geometry columns from coordinates. Scalar constructors are the typed geometry classes linked from this page.',
        (
            section(
                'box',
                'boxes',
                'points',
                'line_strings',
                'polygons',
                'multi_points',
                'multi_line_strings',
                'multi_polygons',
            ),
        ),
        (('Geometry types', '../../api/geometry/geometry-types.md'),),
    ),
    Page(
        'toplevel/predicates',
        'Predicates',
        'Binary spatial relationships, coordinate probes, and DE-9IM predicates.',
        (
            section(
                'contains',
                'contains_properly',
                'contains_xy',
                'covered_by',
                'covers',
                'crosses',
                'disjoint',
                'dwithin',
                'equals',
                'equals_exact',
                'equals_identical',
                'intersects',
                'intersects_xy',
                'overlaps',
                'relate',
                'relate_pattern',
                'touches',
                'within',
            ),
        ),
        (('Predicates & relationships', '../../guide/predicates.md'),),
    ),
    Page(
        'toplevel/measurement',
        'Measurement',
        'Area, length, distance, bounds, nearest points, and similarity measures.',
        (
            section(
                'area',
                'bounds',
                'distance',
                'distance_3d',
                'frechet_distance',
                'hausdorff_distance',
                'length',
                'length_3d',
                'nearest_points',
                'shortest_line',
            ),
        ),
        (('CRS, units & measurement', '../../guide/crs.md'),),
    ),
    Page(
        'toplevel/overlay-dissolve',
        'Overlay & dissolve',
        'Binary set operations and reductions over geometry collections.',
        (
            section(
                'difference',
                'intersection',
                'intersection_all',
                'symmetric_difference',
                'symmetric_difference_all',
                'union',
                'union_all',
            ),
        ),
        (('Constructive operations', '../../guide/constructive.md'),),
    ),
    Page(
        'toplevel/constructive-parts-validation',
        'Constructive, parts & validation',
        'Noding, splitting, snapping, decomposition, polygonization, and validation boundaries.',
        (
            section(
                'parts',
                'polygonize',
                'polygonize_full',
                'require',
                'rings',
                'shared_paths',
                'snap',
                'split',
            ),
        ),
        (('Validation & repair', '../../guide/validation.md'),),
    ),
    Page(
        'toplevel/coverage-polygonal',
        'Coverage (polygonal)',
        'Validate, clean, simplify, and union edge-sharing polygon coverages.',
        (
            section(
                'coverage_clean',
                'coverage_invalid_edges',
                'coverage_is_valid',
                'coverage_simplify',
                'coverage_union',
            ),
        ),
        (('Validation & repair', '../../guide/validation.md'),),
    ),
    Page(
        'toplevel/geodesy-navigation',
        'Geodesy & navigation',
        'Bearings, destinations, route interpolation, cross-track distance, and rhumb distance.',
        (
            section(
                'bearing',
                'cross_track_distance',
                'point_between',
                'rhumb_distance',
            ),
        ),
        (('CRS, units & measurement', '../../guide/crs.md'),),
    ),
    Page(
        'toplevel/index-join',
        'Index & join',
        'Spatial joins and nearest-neighbor queries over geometry columns.',
        (section('join', 'nearest'),),
        (('Spatial indexing & joins', '../../guide/indexing.md'),),
    ),
    Page(
        'toplevel/io-interop',
        'I/O & interop',
        'Parse external representations, export coordinates, and build GeoJSON feature records.',
        (
            section(
                'from_arrow',
                'from_features',
                'from_geojson',
                'from_polyline',
                'from_wkb',
                'from_wkt',
                'get_coordinates',
                'to_feature',
                'to_feature_collection',
            ),
        ),
        (('Text & binary formats', '../../ecosystem/text-formats.md'),),
    ),
    Page(
        'geometry/geometry',
        'Geometry',
        'The shared scalar geometry API. Every typed geometry class inherits these real methods and properties.',
        (section('Geometry'),),
        (('Geometry & dimensions', '../../guide/geometry.md'),),
    ),
    Page(
        'geometry/geometry-types',
        'Geometry types',
        'The seven typed geometry classes and the members unique to each type. Shared operations live on Geometry.',
        (section(*GEOMETRY_LEAVES),),
        (('Geometry', 'geometry.md'),),
    ),
    Page(
        'geometry/geometryarray',
        'GeometryArray',
        'The immutable columnar geometry container, with NumPy-native results and the same operation names as Geometry.',
        (section('GeometryArray'),),
        (('Arrays & performance', '../../guide/arrays.md'),),
    ),
    Page(
        'geometry/coordinates',
        'Coordinates, parts & groups',
        'Coordinate views, lazy multipart views, and CSR-style ragged result groups.',
        (section('Coordinates', 'GeometryParts', 'Groups'),),
        (('Arrays & performance', '../../guide/arrays.md'),),
    ),
    Page(
        'crs',
        'CRS',
        'The first-class CRS value and globally discoverable crs_ functions for transforms, catalogs, resources, and runtime configuration.',
        (
            section('CRS', title='CRS object'),
            section(title='CRS functions', prefixes=('crs_',)),
        ),
        (('CRS, units & measurement', '../guide/crs.md'),),
    ),
    Page(
        'grids',
        'Grids & cells',
        'Grid-generic cells and the shared columnar CellArray container.',
        (section('Cell', 'CellArray'),),
        (('Grids & geocodes', '../guide/grids.md'),),
    ),
    Page(
        'h3',
        'H3',
        'H3 factories return CellArray/Groups; source geometry remains caller-owned, and exact membership uses free predicates.',
        (
            section(
                'H3Cell',
                'H3Vertex',
                'H3VertexArray',
                'H3Edge',
                'H3EdgeArray',
                title='Types',
            ),
            section(title='Functions', prefixes=('h3_',)),
        ),
        (('Grid-generic API', 'grids.md'),),
    ),
    Page(
        's2',
        'S2',
        'S2 factories return CellArray/Groups; source geometry remains caller-owned, and exact membership uses free predicates.',
        (
            section('S2Cell', title='Types'),
            section(title='Functions', prefixes=('s2_',)),
        ),
        (('Grid-generic API', 'grids.md'),),
    ),
    Page(
        'geohash',
        'Geohash',
        'Geohash factories return CellArray/Groups; source geometry remains caller-owned, and exact membership uses free predicates.',
        (
            section('GeohashCell', title='Types'),
            section(title='Functions', prefixes=('geohash_',)),
        ),
        (('Grid-generic API', 'grids.md'),),
    ),
    Page(
        'tiles',
        'XYZ tiles',
        'Web-mercator tile factories return CellArray/Groups; source geometry remains caller-owned, and exact membership uses free predicates.',
        (
            section('Tile', title='Types'),
            section(title='Functions', prefixes=('tile_',)),
        ),
        (('Grid-generic API', 'grids.md'),),
    ),
    Page(
        'geocode',
        'Point codes',
        'Plus-code and OSM-shortlink codecs using explicit longitude/latitude order.',
        (section(title=None, prefixes=('pluscode_', 'osm_shortlink_')),),
        (('Grids & geocodes', '../guide/grids.md'),),
    ),
    Page(
        'spatial-index',
        'Spatial index',
        'Bulk spatial indexing and prepared geometry for repeated relationship queries.',
        (section('SpatialIndex', 'PreparedGeometry'),),
        (('Spatial indexing & joins', '../guide/indexing.md'),),
    ),
    Page(
        'results',
        'Result types',
        'Named result values and aligned feature records returned by multi-output operations.',
        (section('ValidationReport', 'Extremes', 'Features', 'PolygonizeResult'),),
    ),
    Page(
        'errors',
        'Errors',
        'Warnings and exceptions for geometry, CRS, transformation, and parsing failures.',
        (
            section('AccuracyWarning', title='Warnings'),
            section(*ERRORS, title='Exceptions'),
        ),
        (('Errors & exceptions', '../guide/errors.md'),),
    ),
    Page(
        'interop',
        'Optional interoperability',
        'Typed conversion boundaries for optional dataframe, GeoParquet, and visualization ecosystems.',
        (section(*sorted(OPTIONAL_EXPORTS)),),
        (('Interoperability guide', '../ecosystem/index.md'),),
    ),
)


def resolved_symbols(page: Page, exports: set[str]) -> tuple[str, ...]:
    """Resolve explicit members and semantic-prefix families in page order."""
    out: list[str] = []
    for item in page.sections:
        out.extend(item.symbols)
        out.extend(
            name
            for name in sorted(exports)
            if item.prefixes and name.startswith(item.prefixes)
        )
    return tuple(out)


def generated_api_nav_paths() -> frozenset[str]:
    return frozenset({'api/index.md', *(f'api/{page.path}.md' for page in PAGES)})
