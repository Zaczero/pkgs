"""Private typed structures and token vocabularies of the ``gometry._lib`` API.

This is the private single source of truth for stub token aliases, protocols,
and structured return types. The top-level facade and native stub
(``gometry/_lib.pyi``) import it. NumPy is an unconditional runtime dependency
and is imported here eagerly for precise ``ndarray`` spellings
(``BoolArray`` / ``Float64Array``).

Do not redeclare these types elsewhere. Public users receive them through
precise signatures rather than a separate annotation catalog. Public
re-exports from this module are the cross-grid protocols ``Cell`` and
``Coverage`` plus the structured result records ``Extremes``, ``Features``,
and ``PolygonizeResult``.
"""

from __future__ import annotations

import itertools
import math
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import (
    Any,
    Generic,
    Literal,
    NamedTuple,
    NotRequired,
    Protocol,
    Self,
    TypeAlias,
    TypedDict,
    TypeVar,
)

import numpy as np
import numpy.typing as npt

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:  # Python 3.11: Buffer predates collections.abc
    from typing_extensions import Buffer

# NumPy is core: precise array aliases are real at runtime and for checkers.
BoolArray: TypeAlias = npt.NDArray[np.bool_]
Float64Array: TypeAlias = npt.NDArray[np.float64]

TYPE_CHECKING = False

if TYPE_CHECKING:
    # Optional-dependency typing assist: plain conditional imports, no
    # try/except — checkers never execute code, so an `except ImportError`
    # arm assigning `None`/`Any` statically poisons every alias into a
    # "variable in type expression". With a bare import, a user WITH the
    # dependency gets the real types; a user without it gets an unresolved
    # import inside library code, which both pyright and mypy suppress for
    # installed packages (the aliases degrade to ``Any``).
    import geopandas as gpd
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from pyproj import CRS as PyprojCrs  # noqa: N811 — CRS is a class, not a constant
    from pyproj.aoi import AreaOfInterest as PyprojAreaOfInterest

    PyArrowArray: TypeAlias = pa.Array | pa.ExtensionArray
    PyArrowChunkedArray: TypeAlias = pa.ChunkedArray
    PyArrowTable: TypeAlias = pa.Table
    PyArrowRecordBatch: TypeAlias = pa.RecordBatch
    PyArrowCapsule: TypeAlias = object
    PyArrowSchemaCapsule: TypeAlias = object
    GeoPandasSeries: TypeAlias = gpd.GeoSeries
    PandasDataFrame: TypeAlias = pd.DataFrame
    PandasSeries: TypeAlias = pd.Series
    PandasIndex: TypeAlias = pd.Index
    PolarsSeries: TypeAlias = pl.Series
else:
    PyArrowArray = Any
    PyArrowChunkedArray = Any
    PyArrowTable = Any
    PyArrowRecordBatch = Any
    PyArrowCapsule = Any
    PyArrowSchemaCapsule = Any
    GeoPandasSeries = Any
    PandasDataFrame = Any
    PandasSeries = Any
    PandasIndex = Any
    PolarsSeries = Any
    PyprojCrs = Any
    PyprojAreaOfInterest = Any

from gometry._lib import (
    CellArray,
    Geometry,
    GeometryArray,
    GeometryError,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
)


def mapping_as_dict(value: Any) -> dict[Any, Any]:
    """Copy a mapping via ``keys()`` + seen-set (keystone).

    Matches ``dict(value)`` key enumeration: iterate ``keys()`` through the
    iterator protocol (accepts ``__iter__`` *and* legacy ``__getitem__``
    sequences), then ``value[key]``. Rejects repeated keys; ``__len__`` is
    advisory and never limits a provider's key stream. Honest ``dict``
    instances take a shallow-copy fast path.
    """
    if isinstance(value, dict):
        return dict(value)
    keys_fn = getattr(value, 'keys', None)
    if keys_fn is None or not callable(keys_fn):
        raise TypeError('expected a mapping')
    keys: Any = keys_fn()
    # str/bytes are iterable of characters — not a key stream.
    if isinstance(keys, (str, bytes, bytearray)):
        raise TypeError('mapping keys() must return an iterable of keys')
    # iter() accepts legacy __getitem__ sequences that ABC Iterable rejects
    # (same as builtin dict(mapping)).
    try:
        key_iter = iter(keys)
    except TypeError:
        raise TypeError('mapping keys() must return an iterable of keys') from None
    out: dict[Any, Any] = {}
    for key in key_iter:
        if key in out:
            raise GeometryError('mapping has duplicate key')
        out[key] = value[key]
    return out


if TYPE_CHECKING:
    if sys.version_info >= (3, 15):
        from typing import TypeVar as _DefaultTypeVar
    else:
        from typing_extensions import TypeVar as _DefaultTypeVar

    _ExtremeT = _DefaultTypeVar('_ExtremeT', default=Point)
    CellT_co = TypeVar('CellT_co', bound='Cell', covariant=True)
else:
    _ExtremeT = TypeVar('_ExtremeT')
    CellT_co = TypeVar('CellT_co', covariant=True)

__all__ = [
    'ArrowEncoding',
    'BoxWrap',
    'BufferSide',
    'CapStyle',
    'Cell',
    'CellRule',
    'CoordinateAxes',
    'Coverage',
    'CoverageOverlapRule',
    'CrsAreaBounds',
    'CrsAreaInput',
    'CrsAreaOfInterestLike',
    'CrsAreaOfUse',
    'CrsAuthorityObject',
    'CrsAxisInfo',
    'CrsCacheBucketInfo',
    'CrsCacheInfo',
    'CrsCatalogInfo',
    'CrsCatalogKind',
    'CrsCelestialBodyInfo',
    'CrsCfAxisInfo',
    'CrsCfInfo',
    'CrsComparison',
    'CrsCoordinateOperationInfo',
    'CrsDatabaseKind',
    'CrsDatumInfo',
    'CrsDomainInfo',
    'CrsEllipsoidCatalogInfo',
    'CrsEllipsoidInfo',
    'CrsEngineInfo',
    'CrsGeodesicBatchInfo',
    'CrsGeodesicDirectBatchInfo',
    'CrsGeodesicDirectInfo',
    'CrsGeodesicInfo',
    'CrsGeodesicInterpolateBatchInfo',
    'CrsGeodesicInterpolateInfo',
    'CrsGridDatabaseInfo',
    'CrsGridInfo',
    'CrsIdentifyCandidate',
    'CrsInfo',
    'CrsInput',
    'CrsKind',
    'CrsMethodInfo',
    'CrsOperationInfo',
    'CrsOperationParameterInfo',
    'CrsPrimeMeridianCatalogInfo',
    'CrsPrimeMeridianInfo',
    'CrsProjOperationCatalogInfo',
    'CrsProjectionFactors',
    'CrsProjectionFactorsBatch',
    'CrsRuntimeConfig',
    'CrsUnitInfo',
    'DistanceUnit',
    'Extremes',
    'FeatureId',
    'Features',
    'FloatColumn',
    'FloatInput',
    'GeoJsonFeature',
    'GeoJsonFeatureCollection',
    'GeoJsonFeatureNonNull',
    'GeoJsonGeometry',
    'GeoJsonPosition',
    'GeoPandasSeries',
    'GeometryType',
    'GridOrigin',
    'JoinStyle',
    'LineReferenceBasis',
    'NavigationPath',
    'NestedCoordinates',
    'Origin',
    'PandasDataFrame',
    'PandasIndex',
    'PandasSeries',
    'PolarsSeries',
    'PolygonizeResult',
    'Predicate',
    'PyArrowArray',
    'PyArrowCapsule',
    'PyArrowChunkedArray',
    'PyArrowRecordBatch',
    'PyArrowTable',
    'PyprojAreaOfInterest',
    'PyprojCrs',
    'RepairMethod',
    'SimplifyMethod',
    'SmoothMethod',
    'SpatialCurve',
    'SupportsArrowArray',
    'SupportsArrowStream',
    'SupportsGeoInterface',
    'SupportsToWkt',
    'SymmetricPredicate',
    'SymmetricTopologicalPredicate',
    'TopologicalPredicate',
    'TransformDirection',
    'TriangulationMethod',
    'VoronoiClip',
    'WktAxisRule',
    'WktVersion',
]

#: Arrow export encoding: native GeoArrow where possible or forced WKB.
ArrowEncoding: TypeAlias = Literal['auto', 'wkb']
#: How ``coverage_clean`` assigns a region covered by more than one row.
CoverageOverlapRule: TypeAlias = Literal[
    'longest_border', 'max_area', 'min_area', 'min_index'
]
#: End-cap shape for ``buffer``.
CapStyle: TypeAlias = Literal['round', 'flat', 'square']
#: Which side(s) of lineal input ``buffer`` grows.
BufferSide: TypeAlias = Literal['both', 'left', 'right']

#: Simplification algorithm for ``simplify`` / ``coverage_simplify``:
#: ``'vw'`` (Visvalingam-Whyatt, area-based) or ``'dp'`` (Douglas-Peucker,
#: distance-based).
SimplifyMethod: TypeAlias = Literal['vw', 'dp']
#: Line and polygon boundary smoothing algorithm: ``'chaikin'`` (corner-cutting
#: quadratic B-spline) or ``'catmull_rom'`` (centripetal interpolating cubic).
SmoothMethod: TypeAlias = Literal['chaikin', 'catmull_rom']
#: Triangulation kernel selected by ``Geometry.triangulate``.
TriangulationMethod: TypeAlias = Literal['earcut', 'delaunay', 'constrained']
#: Space-filling curve selected by ``spatial_key`` and ``sort_by_spatial_key``.
SpatialCurve: TypeAlias = Literal['hilbert', 'morton']
#: Distance or stored-M stationing selected by the ``line_*`` reference methods.
LineReferenceBasis: TypeAlias = Literal['distance', 'm']
#: Route model for point-navigation functions. ``'geodesic'`` follows the
#: shortest ellipsoidal path; ``'rhumb'`` follows a constant-bearing loxodrome.
NavigationPath: TypeAlias = Literal['geodesic', 'rhumb']
#: Corner join shape for ``buffer``.
JoinStyle: TypeAlias = Literal['round', 'miter', 'bevel']
#: Voronoi clipping mode (a ``Polygon`` clip is passed as the geometry itself).
VoronoiClip: TypeAlias = Literal['padded', 'envelope']
#: Which grid cells a coverage materializes (the tiling rule), strictest to
#: loosest — uniform across H3, S2, geohash, and tiles.
CellRule: TypeAlias = Literal['center', 'within', 'overlap', 'bbox']


class Cell(Protocol):
    """One cell of a discrete global grid — the uniform surface every cell
    class (``H3Cell``, ``S2Cell``, ...) satisfies structurally, without
    inheriting it. Annotate grid-system-agnostic code with this protocol::

        import gometry as gm

        def ring_area(cell: gm.Cell) -> float:
            return sum(neighbor.area for neighbor in cell.neighbors)

    Each system keeps its own named depth property (``resolution``,
    ``level``) next to this shared surface; ``parent``/``children`` accept
    the depth positionally, defaulting to one step coarser/finer.

    ``contains``/``intersects`` are typed to accept a cell or its ``token``.
    Numeric-id systems also accept ``int`` ids on the concrete classes; the
    protocol leaves that out so token-based systems satisfy it too.
    """

    @property
    def token(self) -> str: ...
    @property
    def center(self) -> Point: ...
    @property
    def polygon(self) -> Polygon: ...
    @property
    def neighbors(self) -> CellArray[Self]: ...
    @property
    def area(self) -> float: ...
    def parent(self, depth: int | None = None, /) -> Self: ...
    def children(self, depth: int | None = None, /) -> CellArray[Self]: ...
    def contains(self, other: Self | str) -> bool: ...
    def intersects(self, other: Self | str) -> bool: ...


class Coverage(Protocol[CellT_co]):
    """One covering of a geometry by discrete-global-grid cells.

    The uniform surface every coverage class (``H3Coverage``, ``S2Coverage``,
    ``GeohashCoverage``, ``TileCoverage``) satisfies structurally. Annotate
    grid-system-agnostic code with this protocol::

        import gometry as gm

        def outline(cov: gm.Coverage[gm.H3Cell]) -> gm.Polygon | gm.MultiPolygon:
            return cov.to_polygon()

    ``cells``/``interior_cells``/``boundary_cells`` are the visible partition;
    ``covers``/``contains``/``intersects`` answer exactly against the source
    geometry, independent of ``cell_rule``. System-specific depth keywords
    (``min_resolution``/``min_level``/…) are accepted as extra kwargs on the
    concrete classes — the protocol only requires the zero-arg / positional
    forms that every system shares.
    """

    @property
    def cell_rule(self) -> CellRule: ...
    @property
    def cells(self) -> CellArray[CellT_co]: ...
    @property
    def interior_cells(self) -> CellArray[CellT_co]: ...
    @property
    def boundary_cells(self) -> CellArray[CellT_co]: ...
    def covers(self, geom: Geometry | GeometryArray) -> bool | BoolArray: ...
    def contains(self, geom: Geometry | GeometryArray) -> bool | BoolArray: ...
    def intersects(self, geom: Geometry | GeometryArray) -> bool | BoolArray: ...
    def contains_xy(self, x: FloatInput, y: FloatInput) -> bool | BoolArray: ...
    def intersects_xy(self, x: FloatInput, y: FloatInput) -> bool | BoolArray: ...
    def to_polygon(self) -> Polygon | MultiPolygon: ...
    def compact(self) -> Self: ...
    def uncompact(self, depth: int, /) -> Self: ...
    def with_parents(self) -> Self: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[CellT_co]: ...
    def __contains__(self, cell: object, /) -> bool: ...


#: Topology-preserving nested coordinate payload from ``Coordinates.to_nested``.
#: A point is a flat ``list[float]``; lines/multipoints are lists of ordinate
#: tuples; polygons nest rings; multiparts and arrays nest further. ``None``
#: appears only when a forced layout inserts a missing Z/M.
NestedCoordinates: TypeAlias = (
    list[float | None] | list[tuple[float | None, ...]] | list['NestedCoordinates']
)

#: Canonical CRS kind token returned by ``CRS.kind`` / ``CrsInfo['kind']``
#: (``crs_type_name`` output for CRS objects).
CrsKind: TypeAlias = Literal[
    'geographic_2d',
    'geographic_3d',
    'geographic',
    'geocentric',
    'projected',
    'vertical',
    'compound',
    'temporal',
    'engineering',
    'bound',
    'other',
    'unknown',
]

#: ``kind=`` filter accepted by ``gm.crs_search`` / ``gm.crs_catalog``
#: (``CrsObjectKind::parse_crs`` — CRS types only).
CrsCatalogKind: TypeAlias = Literal[
    'crs',
    'geodetic',
    'geographic',
    'geographic_2d',
    'geographic_3d',
    'geocentric',
    'projected',
    'vertical',
    'compound',
    'temporal',
    'engineering',
    'bound',
    'other',
]

#: ``kind=`` filter accepted by ``gm.crs_codes`` (``CrsObjectKind::parse`` — full
#: PROJ database object vocabulary, including non-CRS kinds and aliases).
CrsDatabaseKind: TypeAlias = (
    CrsCatalogKind
    | Literal[
        'derived_projected_crs',
        'ellipsoid',
        'prime_meridian',
        'geodetic_reference_frame',
        'dynamic_geodetic_reference_frame',
        'vertical_reference_frame',
        'dynamic_vertical_reference_frame',
        'datum_ensemble',
        'temporal_datum',
        'engineering_datum',
        'parametric_datum',
        'conversion',
        'transformation',
        'concatenated_operation',
        'other_coordinate_operation',
    ]
)

#: Direction of a CRS coordinate operation.
TransformDirection: TypeAlias = Literal['forward', 'inverse']
#: Predicates that never take a distance parameter.
TopologicalPredicate: TypeAlias = Literal[
    'intersects',
    'contains',
    'contains_properly',
    'covers',
    'within',
    'covered_by',
    'equals',
    'touches',
    'crosses',
    'overlaps',
]
#: Spatial predicate token for ``join``/``SpatialIndex`` queries.
Predicate: TypeAlias = TopologicalPredicate | Literal['dwithin']
#: The symmetric predicates ``SpatialIndex.query_pairs`` accepts (a self-join
#: needs a predicate that is the same in both directions).
SymmetricTopologicalPredicate: TypeAlias = Literal[
    'intersects', 'equals', 'touches', 'crosses', 'overlaps'
]
SymmetricPredicate: TypeAlias = SymmetricTopologicalPredicate | Literal['dwithin']
#: Anchor for the affine helpers (``rotate``/``scale``/``skew``): the
#: ``'centroid'``, the bounds ``'center'``, or any two-value ``(x, y)``
#: iterable (including lists, NumPy arrays, and generators).
Origin: TypeAlias = Literal['centroid', 'center'] | Iterable[float]
#: Grid origin for ``snap_to_grid``: only a two-value numeric ``(x, y)``
#: iterable (not ``'centroid'``/``'center'`` — those are affine-only).
GridOrigin: TypeAlias = Iterable[float]
#: Antimeridian handling for ``box``: ``'split'`` emits a multipart geometry.
BoxWrap: TypeAlias = Literal['split']
#: Distance/area unit for a CRS-aware metric's ``unit`` keyword. Omitted
#: follows the CRS: geodesic meters on a geographic CRS, the CRS's native
#: units on a projected one, coordinate units without a CRS. ``'planar'``
#: forces raw coordinate units; ``'meters'`` forces the CRS metric and
#: raises without a CRS.
DistanceUnit: TypeAlias = Literal['planar', 'meters']
#: Equality strictness for ``CRS`` comparisons.
CrsComparison: TypeAlias = Literal['ignore_axis_order', 'exact']

WktVersion: TypeAlias = Literal[
    'WKT2_2019',
    'WKT2_2019_SIMPLIFIED',
    'WKT2_2015',
    'WKT2_2015_SIMPLIFIED',
    'WKT1_GDAL',
    'WKT1_ESRI',
]

WktAxisRule: TypeAlias = Literal['auto', 'yes', 'no']
#: Repair backend for ``repair``.
RepairMethod: TypeAlias = Literal['linework', 'structure']
#: Ordinate layout of a geometry: which of X/Y/Z/M are present.
CoordinateAxes: TypeAlias = Literal['XY', 'XYZ', 'XYM', 'XYZM']
#: WKT name of a geometry's kind, as returned by ``geometry_type``.
GeometryType: TypeAlias = Literal[
    'Point',
    'MultiPoint',
    'LineString',
    'MultiLineString',
    'Polygon',
    'MultiPolygon',
    'GeometryCollection',
]


# GeoJSON typing vocabulary shared by the TypedDicts below and user code.
GeoJsonPosition: TypeAlias = list[float] | tuple[float, ...]
FeatureId: TypeAlias = str | int | float | None


class Features:
    """Parsed GeoJSON features: geometries plus parallel properties and ids.

    Returned by `from_features`. Destructures as
    ``geometries, properties, ids = features`` and supports ``match``
    patterns via ``__match_args__``. Deliberately not a tuple and has no
    ``len()`` because a field count of three reads like a feature count; use
    ``len(features.geometries)`` for the row count. The
    outer metadata tuples cannot drift out of alignment; property dictionaries
    remain editable.

    Hand-written frozen slots (not ``@dataclass``) so cold ``import gometry``
    never pays the ``dataclasses``/``inspect`` import chain.
    """

    __slots__ = ('geometries', 'ids', 'properties')
    __match_args__ = ('geometries', 'properties', 'ids')

    geometries: GeometryArray[Geometry]
    properties: tuple[dict[str, Any] | None, ...]
    ids: tuple[FeatureId, ...]

    def __init__(
        self,
        geometries: GeometryArray[Geometry],
        properties: Mapping[str, Any]
        | Iterable[Mapping[str, Any] | None]
        | None = None,
        ids: Iterable[FeatureId] | None = None,
    ) -> None:
        """Validate and freeze the three aligned outer columns.

        ``None`` supplies one missing value per geometry. A single properties
        mapping broadcasts through independent shallow ``dict`` copies; row
        iterables are consumed exactly once.
        """
        if not isinstance(geometries, GeometryArray):
            raise TypeError('geometries must be a GeometryArray')
        rows = len(geometries)
        if properties is None:
            property_rows: tuple[dict[str, Any] | None, ...] = (None,) * rows
        elif isinstance(properties, Mapping) or callable(
            getattr(properties, 'keys', None)
        ):
            # keys()+seen copier (N4): broadcast one mapping through independent
            # shallow copies; reject repeated keys / accept keys()-only ducks.
            # Validate string keys once while building the template (no re-
            # stringify or second validation pass).
            copied = mapping_as_dict(properties)
            template: dict[str, Any] = {}
            for key, value in copied.items():
                if not isinstance(key, str):
                    raise TypeError('properties keys must be strings')
                template[key] = value
            property_rows = tuple(dict(template) for _ in range(rows))
        else:
            if isinstance(properties, (str, bytes, bytearray)) or not isinstance(
                properties, Iterable
            ):
                raise TypeError('properties must be a mapping, iterable, or None')
            # Bound by known geometry row count so infinite iterators terminate
            # (rows+1 then the length-mismatch check below fires). Validate each
            # mapping's keys while collecting.
            collected: list[dict[str, Any] | None] = []
            for property_row in itertools.islice(properties, rows + 1):
                if property_row is None:
                    collected.append(None)
                elif isinstance(property_row, Mapping) or callable(
                    getattr(property_row, 'keys', None)
                ):
                    row = mapping_as_dict(property_row)
                    validated: dict[str, Any] = {}
                    for key, value in row.items():
                        if not isinstance(key, str):
                            raise TypeError('properties keys must be strings')
                        validated[key] = value
                    collected.append(validated)
                else:
                    raise TypeError('properties rows must be mappings or None')
            property_rows = tuple(collected)
        if ids is None:
            id_rows: tuple[FeatureId, ...] = (None,) * rows
        else:
            if isinstance(ids, (str, bytes, bytearray)) or not isinstance(
                ids, Iterable
            ):
                raise TypeError('ids must be an iterable or None')
            # Bound by known geometry row count; validate each id while collecting.
            collected_ids: list[FeatureId] = []
            for identifier in itertools.islice(ids, rows + 1):
                if identifier is None:
                    collected_ids.append(None)
                elif isinstance(identifier, bool) or not isinstance(
                    identifier, (str, int, float)
                ):
                    raise TypeError('feature ids must be strings, numbers, or None')
                elif isinstance(identifier, float) and not math.isfinite(identifier):
                    raise ValueError('numeric feature ids must be finite')
                else:
                    collected_ids.append(identifier)
            id_rows = tuple(collected_ids)
        if len(property_rows) != rows:
            raise ValueError(
                f'properties length {len(property_rows)} does not match geometries length {rows}'
            )
        if len(id_rows) != rows:
            raise ValueError(
                f'ids length {len(id_rows)} does not match geometries length {rows}'
            )
        # ``object.__setattr__`` bypasses the frozen instance ``__setattr__``.
        object.__setattr__(self, 'geometries', geometries)
        object.__setattr__(self, 'properties', property_rows)
        object.__setattr__(self, 'ids', id_rows)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f'cannot set attribute {name!r} on frozen Features')

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f'cannot delete attribute {name!r} on frozen Features')

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Features):
            return NotImplemented
        return (
            self.geometries == other.geometries
            and self.properties == other.properties
            and self.ids == other.ids
        )

    # Unhashable: property dicts are mutable. Frozen dataclass would also
    # refuse hashing when a field is unhashable at use time; be explicit.
    __hash__ = None  # type: ignore[assignment]

    def __replace__(self, /, **changes: Any) -> Features:
        """Return a new ``Features`` with the given fields replaced.

        Matches frozen-dataclass / ``copy.replace`` semantics: only the three
        column fields are accepted; unknown keywords raise ``TypeError``.
        Unspecified fields keep the receiver's values (re-validated by
        ``__init__`` so row alignment stays enforced).
        """
        known = ('geometries', 'properties', 'ids')
        for key in changes:
            if key not in known:
                raise TypeError(
                    f'Features.__replace__() got an unexpected keyword argument {key!r}'
                )
        return type(self)(
            changes.get('geometries', self.geometries),
            changes.get('properties', self.properties),
            changes.get('ids', self.ids),
        )

    def __reduce__(self):
        return (type(self), (self.geometries, self.properties, self.ids))

    def __iter__(
        self,
    ) -> Iterator[
        GeometryArray | tuple[dict[str, Any] | None, ...] | tuple[FeatureId, ...]
    ]:
        """Unpack as ``geometries, properties, ids`` (three fields)."""
        return iter((self.geometries, self.properties, self.ids))

    def __repr__(self) -> str:
        """Return a bounded row-oriented summary."""
        rows = len(self.geometries)
        shown = min(rows, 3)
        suffix = ', ...' if rows > shown else ''
        properties = repr(self.properties[:shown])
        ids = repr(self.ids[:shown])
        if len(properties) > 120:
            properties = f'{properties[:117]}...'
        if len(ids) > 120:
            ids = f'{ids[:117]}...'
        return (
            f'Features(rows={rows}, properties={properties}{suffix}, ids={ids}{suffix})'
        )


class Extremes(NamedTuple, Generic[_ExtremeT]):
    """The westmost/southmost/eastmost/northmost vertices of a geometry.

    Returned by `Geometry.extremes` (four `Point` fields) and
    `GeometryArray.extremes` (four row-aligned ``GeometryArray[Point]``
    columns); unpacks as ``(west, south, east, north)``, or use the named
    fields. Ties keep the first vertex in storage order; Z/M ride along.
    """

    west: _ExtremeT
    """The vertex with the smallest X."""

    south: _ExtremeT
    """The vertex with the smallest Y."""

    east: _ExtremeT
    """The vertex with the largest X."""

    north: _ExtremeT
    """The vertex with the largest Y."""


class PolygonizeResult(NamedTuple):
    """Full polygonization output: the polygons plus the leftover linework.

    Returned by `polygonize_full`; unpacks as ``(polygons, cut_edges,
    dangles, invalid_rings)``, or use the named fields. Each field is a
    `GeometryArray`; the three diagnostic arrays are empty when the input
    nodes cleanly into polygons.
    """

    polygons: GeometryArray[Polygon]
    """The polygons formed from the noded linework."""

    cut_edges: GeometryArray[LineString]
    """Edges that border two polygons (each used twice, both sides covered)."""

    dangles: GeometryArray[LineString]
    """Edges with at least one free end — not part of any polygon."""

    invalid_rings: GeometryArray[LineString]
    """Rings that close but bound no valid polygon area."""


class SupportsGeoInterface(Protocol):
    """Any object exposing a GeoJSON-like ``__geo_interface__`` mapping."""

    @property
    def __geo_interface__(self) -> Mapping[str, Any]: ...


class SupportsArrowArray(Protocol):
    """An object exporting Arrow C Data ``(schema, array)`` capsules."""

    def __arrow_c_array__(
        self, requested_schema: PyArrowSchemaCapsule | None = None, /
    ) -> tuple[PyArrowCapsule, PyArrowCapsule]: ...


class SupportsArrowStream(Protocol):
    """An object exporting an Arrow C stream capsule."""

    def __arrow_c_stream__(
        self, requested_schema: PyArrowSchemaCapsule | None = None, /
    ) -> PyArrowCapsule: ...


class SupportsToWkt(Protocol):
    """An object that serializes itself to WKT — e.g. a ``gometry.CRS`` or
    ``pyproj.CRS``.
    """

    def to_wkt(self) -> str: ...


#: A float lane: any iterable of floats, or a zero-copy buffer exporter
#: (numpy arrays — float32 included — ``array.array``, memoryviews).
FloatColumn: TypeAlias = Iterable[float] | Buffer

#: One float per geometry: a scalar broadcasts, or pass an iterable /
#: zero-copy buffer with one value per element.
FloatInput: TypeAlias = float | FloatColumn

#: Anything accepted where a CRS is expected: an authority string
#: (``'EPSG:4326'``, WKT, PROJ), an EPSG code, an ``(authority, code)`` pair,
#: a PROJJSON/CF mapping, or any object exposing ``to_wkt()`` (``gometry.CRS``,
#: ``pyproj.CRS``).
CrsInput: TypeAlias = (
    str
    | int
    | tuple[str, str | int]
    | list[str | int]
    | Mapping[str, Any]
    | SupportsToWkt
)

# ---------------------------------------------------------------------------
# Structured return shapes (GeoJSON + CRS metadata TypedDict catalog).
# ---------------------------------------------------------------------------


class GeoJsonPointGeometry(TypedDict):
    """GeoJSON Point geometry (RFC 7946 §3.1.2)."""

    type: Literal['Point']
    coordinates: GeoJsonPosition


class GeoJsonMultiPointGeometry(TypedDict):
    """GeoJSON MultiPoint geometry (RFC 7946 §3.1.3)."""

    type: Literal['MultiPoint']
    coordinates: Sequence[GeoJsonPosition]


class GeoJsonLineStringGeometry(TypedDict):
    """GeoJSON LineString geometry (RFC 7946 §3.1.4)."""

    type: Literal['LineString']
    coordinates: Sequence[GeoJsonPosition]


class GeoJsonMultiLineStringGeometry(TypedDict):
    """GeoJSON MultiLineString geometry (RFC 7946 §3.1.5)."""

    type: Literal['MultiLineString']
    coordinates: Sequence[Sequence[GeoJsonPosition]]


class GeoJsonPolygonGeometry(TypedDict):
    """GeoJSON Polygon geometry (RFC 7946 §3.1.6)."""

    type: Literal['Polygon']
    coordinates: Sequence[Sequence[GeoJsonPosition]]


class GeoJsonMultiPolygonGeometry(TypedDict):
    """GeoJSON MultiPolygon geometry (RFC 7946 §3.1.7)."""

    type: Literal['MultiPolygon']
    coordinates: Sequence[Sequence[Sequence[GeoJsonPosition]]]


class GeoJsonGeometryCollectionGeometry(TypedDict):
    """GeoJSON GeometryCollection (RFC 7946 §3.1.8)."""

    type: Literal['GeometryCollection']
    geometries: list[GeoJsonGeometry]


GeoJsonGeometry: TypeAlias = (
    GeoJsonPointGeometry
    | GeoJsonMultiPointGeometry
    | GeoJsonLineStringGeometry
    | GeoJsonMultiLineStringGeometry
    | GeoJsonPolygonGeometry
    | GeoJsonMultiPolygonGeometry
    | GeoJsonGeometryCollectionGeometry
)
"""A gometry-parseable GeoJSON geometry object (RFC 7946 §3.1).

Every geometry kind requires its payload key: ``coordinates`` for all kinds
except ``GeometryCollection``, which requires ``geometries``. A Feature may
carry ``geometry: None`` to represent a missing geometry row; missing payload
keys are rejected at parse time.
"""


class GeoJsonFeature(TypedDict):
    """A gometry-parseable GeoJSON Feature object (RFC 7946 §3.2).

    ``geometry`` may be ``None`` for missing rows in bulk FeatureCollection /
    ``from_features`` ingestion. Scalar ``from_geojson`` of a Feature rejects
    null geometry — use :class:`GeoJsonFeatureNonNull` for that overload.
    """

    type: Literal['Feature']
    geometry: GeoJsonGeometry | None
    properties: NotRequired[Mapping[str, Any] | None]
    id: NotRequired[str | int | float]


class GeoJsonFeatureNonNull(TypedDict):
    """A GeoJSON Feature with a non-null geometry (scalar ``from_geojson``)."""

    type: Literal['Feature']
    geometry: GeoJsonGeometry
    properties: NotRequired[Mapping[str, Any] | None]
    id: NotRequired[str | int | float]


class GeoJsonFeatureCollection(TypedDict):
    """A GeoJSON FeatureCollection object (RFC 7946 §3.3)."""

    type: Literal['FeatureCollection']
    features: list[GeoJsonFeature]


class CrsAreaBounds(TypedDict):
    west: float
    south: float
    east: float
    north: float


class CrsAreaOfUse(CrsAreaBounds):
    name: str | None


class CrsAreaOfInterestLike(Protocol):
    west_lon_degree: float
    south_lat_degree: float
    east_lon_degree: float
    north_lat_degree: float


CrsAreaInput: TypeAlias = (
    tuple[float, float, float, float]
    | Iterable[float]
    | CrsAreaBounds
    | Mapping[str, float]
    | CrsAreaOfInterestLike
)


class CrsAuthorityObject(TypedDict):
    crs: str
    authority: str | None
    code: str | None
    name: str | None
    kind: CrsKind
    deprecated: bool
    area_of_use: CrsAreaOfUse | None


class CrsAxisInfo(TypedDict):
    name: str | None
    abbreviation: str | None
    direction: str | None
    unit_name: str | None
    unit_conversion_factor: float


class CrsDatumInfo(TypedDict):
    name: str | None
    authority: str | None
    code: str | None
    kind: str | None
    frame_reference_epoch: float | None
    ensemble_accuracy: float | None
    ensemble_members: list[CrsDatumInfo]


class CrsEllipsoidInfo(TypedDict):
    name: str | None
    semi_major_metre: float
    semi_minor_metre: float
    inverse_flattening: float
    is_semi_minor_computed: bool


class CrsPrimeMeridianInfo(TypedDict):
    name: str | None
    longitude: float
    unit_name: str | None
    unit_conversion_factor: float


class CrsCfAxisInfo(TypedDict):
    standard_name: str
    long_name: str
    units: str
    axis: str
    positive: str


class CrsCfInfo(TypedDict, total=False):
    crs_wkt: str
    spatial_ref: str
    semi_major_axis: float
    semi_minor_axis: float
    inverse_flattening: float
    reference_ellipsoid_name: str
    longitude_of_prime_meridian: float
    prime_meridian_name: str
    geographic_crs_name: str
    horizontal_datum_name: str
    projected_crs_name: str
    grid_mapping_name: str
    latitude_of_projection_origin: float
    longitude_of_central_meridian: float
    longitude_of_projection_origin: float
    false_easting: float
    false_northing: float
    scale_factor_at_central_meridian: float
    scale_factor_at_projection_origin: float
    standard_parallel: float | list[float]
    straight_vertical_longitude_from_pole: float
    # Native projected linear unit token for CF→PROJ parse (m / ft / us-ft).
    units: str
    proj_units: str


class CrsDomainInfo(TypedDict):
    scope: str | None
    area_of_use: CrsAreaOfUse | None


class CrsMethodInfo(TypedDict):
    name: str | None
    authority: str | None
    code: str | None


class CrsOperationParameterInfo(TypedDict):
    name: str | None
    authority: str | None
    code: str | None
    value: float
    value_string: str | None
    unit_conversion_factor: float
    unit_name: str | None
    unit_authority: str | None
    unit_code: str | None
    unit_category: str | None


class CrsGridInfo(TypedDict):
    short_name: str | None
    full_name: str | None
    package_name: str | None
    available: bool


class CrsGridDatabaseInfo(TypedDict):
    name: str
    full_name: str | None
    package_name: str | None
    url: str | None
    direct_download: bool
    available: bool


class CrsCoordinateOperationInfo(TypedDict):
    name: str | None
    description: str | None
    definition: str | None
    accuracy: float | None
    has_inverse: bool
    has_ballpark_transformation: bool
    requires_coordinate_epoch: bool
    method: CrsMethodInfo | None
    parameters: list[CrsOperationParameterInfo]
    grids: list[CrsGridInfo]
    steps: list[CrsCoordinateOperationInfo]
    area_of_use: CrsAreaOfUse | None
    instantiable: bool


class CrsInfo(TypedDict):
    crs: str
    name: str | None
    authority: str | None
    code: str | None
    kind: CrsKind
    is_derived: bool
    deprecated: bool
    remarks: str | None
    scope: str | None
    sub_crs: list[CrsAuthorityObject]
    source_crs: CrsAuthorityObject | None
    target_crs: CrsAuthorityObject | None
    coordinate_operation: CrsCoordinateOperationInfo | None
    geodetic_crs: CrsAuthorityObject | None
    horizontal_datum: CrsAuthorityObject | None
    domains: list[CrsDomainInfo]
    datum: CrsDatumInfo | None
    ellipsoid: CrsEllipsoidInfo | None
    prime_meridian: CrsPrimeMeridianInfo | None
    celestial_body: str | None
    coordinate_system: str | None
    axis_order: list[str]
    axes: list[CrsAxisInfo]
    has_point_motion_operation: bool
    is_geographic: bool
    is_projected: bool
    is_vertical: bool
    is_geocentric: bool
    is_compound: bool
    is_engineering: bool
    is_bound: bool
    area_of_use: CrsAreaOfUse | None


class CrsEngineInfo(TypedDict):
    backend: str
    bundled_proj: bool
    version: str | None
    release: str | None
    major: int
    minor: int
    patch: int
    search_path: str | None
    paths: list[str]
    database_path: str | None
    database_metadata: dict[str, str]
    user_writable_directory: str | None


class CrsCacheBucketInfo(TypedDict):
    name: str
    entries: int
    capacity: int


class CrsCacheInfo(TypedDict):
    """CRS cache state and current-thread transform-engine observations.

    ``last_transform_engine`` answers whether the most recent transform used
    the accelerated ``'in_core'`` engine or bundled ``'proj'``;
    ``transform_invocations`` counts actual in-core batches and PROJ calls
    since the last cache clear/reset.
    """

    generation: int
    total_entries: int
    total_capacity: int
    buckets: list[CrsCacheBucketInfo]
    last_transform_engine: Literal['in_core', 'proj'] | None
    transform_invocations: int


class CrsRuntimeConfig(TypedDict):
    search_paths: list[str] | None
    user_writable_directory: str | None


class CrsCatalogInfo(CrsAuthorityObject):
    projection_method_name: str | None
    celestial_body: str | None


class CrsUnitInfo(TypedDict):
    authority: str | None
    code: str | None
    name: str | None
    category: str | None
    conversion_factor: float
    proj_short_name: str | None


class CrsIdentifyCandidate(TypedDict):
    crs: str
    name: str | None
    authority: str | None
    code: str | None
    confidence: int


class CrsCelestialBodyInfo(TypedDict):
    authority: str | None
    name: str | None


class CrsOperationInfo(CrsCoordinateOperationInfo):
    source: str
    target: str
    source_epoch: float | None
    target_epoch: float | None


class CrsProjectionFactors(TypedDict):
    meridional_scale: float
    parallel_scale: float
    areal_scale: float
    angular_distortion: float
    meridian_parallel_angle: float
    meridian_convergence: float
    tissot_semimajor: float
    tissot_semiminor: float
    dx_dlam: float
    dx_dphi: float
    dy_dlam: float
    dy_dphi: float


class CrsProjectionFactorsBatch(TypedDict):
    meridional_scale: Float64Array
    parallel_scale: Float64Array
    areal_scale: Float64Array
    angular_distortion: Float64Array
    meridian_parallel_angle: Float64Array
    meridian_convergence: Float64Array
    tissot_semimajor: Float64Array
    tissot_semiminor: Float64Array
    dx_dlam: Float64Array
    dx_dphi: Float64Array
    dy_dlam: Float64Array
    dy_dphi: Float64Array


class CrsGeodesicInfo(TypedDict):
    distance: float
    distance_3d: float | None
    forward_azimuth: float
    reverse_azimuth: float


class CrsGeodesicBatchInfo(TypedDict):
    distance: Float64Array
    distance_3d: Float64Array
    forward_azimuth: Float64Array
    reverse_azimuth: Float64Array


class CrsGeodesicDirectInfo(TypedDict):
    longitude: float
    latitude: float
    final_azimuth: float


class CrsGeodesicDirectBatchInfo(TypedDict):
    longitude: Float64Array
    latitude: Float64Array
    final_azimuth: Float64Array


class CrsGeodesicInterpolateInfo(TypedDict):
    longitude: float
    latitude: float
    final_azimuth: float
    distance: float


class CrsGeodesicInterpolateBatchInfo(TypedDict):
    longitude: Float64Array
    latitude: Float64Array
    final_azimuth: Float64Array
    distance: Float64Array


class CrsProjOperationCatalogInfo(TypedDict):
    id: str
    description: str | None


class CrsEllipsoidCatalogInfo(TypedDict):
    id: str
    semi_major: str | None
    definition: str | None
    name: str | None


class CrsPrimeMeridianCatalogInfo(TypedDict):
    id: str
    definition: str | None
