"""Private typed structures and token vocabularies of the ``gometry._lib`` API.

This is the private single source of truth for stub token aliases, protocols,
and structured return types. The top-level facade and native stub
(``gometry/_lib.pyi``) import it. NumPy is an unconditional runtime dependency
and is imported here eagerly for precise ``ndarray`` spellings
(``BoolArray`` / ``Float64Array``).

Do not redeclare these types elsewhere. Public users receive them through
precise signatures rather than a separate annotation catalog. Public
re-exports from this module are the cross-grid protocols ``Cell`` and
plus the structured result records ``Extremes``, ``Features``,
and ``PolygonizeResult``.
"""

# Features must remain explicitly unhashable; mypy needs a narrow assignment
# ignore for this runtime contract, while pyright does not.
# pyright: reportUnnecessaryTypeIgnoreComment=false

from __future__ import annotations

import itertools
import math
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    ForwardRef,
    Generic,
    Literal,
    NamedTuple,
    NotRequired,
    Protocol,
    Self,
    SupportsIndex,
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
# A 0-D NumPy array follows the scalar native numeric path. Ordinary NDArray
# aliases erase shape, so only an explicitly shaped value earns scalar returns.
ScalarFloatArray: TypeAlias = np.ndarray[
    tuple[()],
    np.dtype[np.floating[Any] | np.signedinteger[Any] | np.unsignedinteger[Any]],
]
# Python and NumPy numeric scalar values accepted by the native f64 boundary.
FloatScalar: TypeAlias = (
    float | np.floating[Any] | np.signedinteger[Any] | np.unsignedinteger[Any]
)

if TYPE_CHECKING:
    # Static consumers see the nominal optional dependency types. Runtime
    # aliases for those external types remain forward references until a caller
    # supplies the corresponding modules and symbol names. Arrow C capsules
    # intentionally remain opaque objects.
    import geopandas as gpd
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from pyproj import CRS as PyprojCrs  # noqa: N811 — CRS is a class, not a constant
    from pyproj.aoi import AreaOfInterest as PyprojAreaOfInterest

    PyArrowArray: TypeAlias = pa.Array | pa.ExtensionArray
    PyArrowChunkedArray: TypeAlias = pa.ChunkedArray
    PyArrowNativeFile: TypeAlias = pa.NativeFile
    PyArrowTable: TypeAlias = pa.Table
    PyArrowRecordBatch: TypeAlias = pa.RecordBatch
    PyArrowRecordBatchReader: TypeAlias = pa.RecordBatchReader
    PyArrowCapsule: TypeAlias = object
    PyArrowSchemaCapsule: TypeAlias = object
    GeoPandasSeries: TypeAlias = gpd.GeoSeries
    PandasDataFrame: TypeAlias = pd.DataFrame
    PandasSeries: TypeAlias = pd.Series
    PandasIndex: TypeAlias = pd.Index
    PolarsSeries: TypeAlias = pl.Series
else:
    PyArrowArray: TypeAlias = ForwardRef('pa.Array | pa.ExtensionArray')
    PyArrowChunkedArray: TypeAlias = ForwardRef('pa.ChunkedArray')
    PyArrowNativeFile: TypeAlias = ForwardRef('pa.NativeFile')
    PyArrowTable: TypeAlias = ForwardRef('pa.Table')
    PyArrowRecordBatch: TypeAlias = ForwardRef('pa.RecordBatch')
    PyArrowRecordBatchReader: TypeAlias = ForwardRef('pa.RecordBatchReader')
    PyArrowCapsule: TypeAlias = object
    PyArrowSchemaCapsule: TypeAlias = object
    GeoPandasSeries: TypeAlias = ForwardRef('gpd.GeoSeries')
    PandasDataFrame: TypeAlias = ForwardRef('pd.DataFrame')
    PandasSeries: TypeAlias = ForwardRef('pd.Series')
    PandasIndex: TypeAlias = ForwardRef('pd.Index')
    PolarsSeries: TypeAlias = ForwardRef('pl.Series')
    PyprojCrs: TypeAlias = ForwardRef('pyproj.CRS')
    PyprojAreaOfInterest: TypeAlias = ForwardRef('pyproj.aoi.AreaOfInterest')

from gometry._lib import (
    CellArray,
    Geometry,
    GeometryArray,
    GeometryError,
    Groups,
    LineString,
    Point,
    Polygon,
)


def mapping_as_dict(value: Any) -> dict[Any, Any]:
    """Copy a mapping via ``keys()`` + seen-set (keystone).

    Matches ``dict(value)`` key enumeration: iterate ``keys()`` through the
    iterator protocol (accepts ``__iter__`` *and* legacy ``__getitem__``
    sequences), then ``value[key]``. Rejects repeated keys. Exact ``dict``
    instances take a shallow-copy fast path.
    """
    if type(value) is dict:
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
    _GeoJsonInterfaceT_co = _DefaultTypeVar(
        '_GeoJsonInterfaceT_co',
        bound=Mapping[str, Any],
        covariant=True,
        default=Mapping[str, Any],
    )
else:
    _ExtremeT = TypeVar('_ExtremeT')
    _GeoJsonInterfaceT_co = TypeVar(
        '_GeoJsonInterfaceT_co',
        bound=Mapping[str, Any],
        covariant=True,
    )

__all__ = [
    'ArrowEncoding',
    'BoxWrap',
    'BufferSide',
    'CapStyle',
    'Cell',
    'CellRule',
    'CoordinateAxes',
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
    'ParseFormat',
    'PolarsSeries',
    'PolygonizeResult',
    'Predicate',
    'PyArrowArray',
    'PyArrowCapsule',
    'PyArrowChunkedArray',
    'PyArrowNativeFile',
    'PyArrowRecordBatch',
    'PyArrowRecordBatchReader',
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
    'VoronoiOutput',
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
#: Space-filling curve selected by ``GeometryArray.spatial_key`` and ``sort_by_spatial_key``.
SpatialCurve: TypeAlias = Literal['hilbert', 'morton']
#: Distance or stored-M stationing selected by the ``line_*`` reference methods.
LineReferenceBasis: TypeAlias = Literal['distance', 'm']
#: Route model for point navigation and ``segmentize``. ``'geodesic'`` follows
#: the shortest ellipsoidal path; ``'rhumb'`` follows a constant-bearing route.
NavigationPath: TypeAlias = Literal['geodesic', 'rhumb']
#: Corner join shape for ``buffer``.
JoinStyle: TypeAlias = Literal['round', 'miter', 'bevel']
#: Voronoi clipping mode (a ``Polygon`` clip is passed as the geometry itself).
VoronoiClip: TypeAlias = Literal['padded', 'envelope']
#: Geometry representation returned by ``Geometry.voronoi`` and
#: ``GeometryArray.voronoi``.
VoronoiOutput: TypeAlias = Literal['polygons', 'edges']
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
    def token(self) -> str:
        """The cell id in its system's canonical lowercase text form.

        Returns
        -------
        str
            Canonical text for this cell.
        """
        ...

    @property
    def center(self) -> Point:
        """Cell center as a WGS 84 ``Point`` (lon/lat).

        Returns
        -------
        Point
            The center in longitude/latitude coordinates.
        """
        ...

    @property
    def polygon(self) -> Polygon:
        """The cell as a filled WGS 84 ``Polygon`` (lon/lat).

        H3 and S2 emit planar chord proxies for their true spherical edges.

        Returns
        -------
        Polygon
            The filled cell boundary in longitude/latitude coordinates.
        """
        ...

    @property
    def neighbors(self) -> CellArray[Self]:
        """The edge-adjacent cells at this cell's depth.

        How many there are is system-specific, and can vary with position:
        six around an H3 hexagon but five around a pentagon, four for S2,
        eight for geohash away from the poles.

        Returns
        -------
        CellArray[Self]
            The edge-adjacent cells at the same depth.
        """
        ...

    @property
    def area(self) -> float:
        """Area of the cell in square meters, using its grid's area model.

        H3 and S2 use an authalic sphere; geohash and tile use the WGS 84
        ellipsoidal area of their lon/lat rectangle.

        Returns
        -------
        float
            Area in square meters.
        """
        ...

    def parent(self, depth: int | None = None, /) -> Self:
        """Parent cell at a coarser depth, one step coarser by default.

        Parameters
        ----------
        depth : int or None, optional, default None
            Requested parent depth; ``None`` selects one step coarser.

        Returns
        -------
        Self
            The ancestor at the requested depth.
        """
        ...

    def children(self, depth: int | None = None, /) -> CellArray[Self]:
        """Child cells at a finer depth, one step finer by default.

        Parameters
        ----------
        depth : int or None, optional, default None
            Requested child depth; ``None`` selects one step finer.

        Returns
        -------
        CellArray[Self]
            The cells at the requested depth.
        """
        ...

    def children_count(self, depth: int | None = None, /) -> int:
        """Count descendants at a finer depth without materializing them.

        Parameters
        ----------
        depth : int or None, optional, default None
            Requested child depth; ``None`` selects one step finer.

        Returns
        -------
        int
            Number of cells at the requested depth.
        """
        ...

    def contains(self, other: Self | str) -> bool:
        """Whether this cell is, or is an ancestor of, ``other``.

        Parameters
        ----------
        other : Self or str
            A cell or its canonical token.

        Returns
        -------
        bool
            Whether ``other`` is this cell or one of its descendants.
        """
        ...

    def intersects(self, other: Self | str) -> bool:
        """Whether this cell and ``other`` share any area.

        Hierarchy cells cannot partially overlap, so this holds exactly when
        one of the two contains the other.

        Parameters
        ----------
        other : Self or str
            A cell or its canonical token.

        Returns
        -------
        bool
            Whether the two cells overlap hierarchically.
        """
        ...


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

#: ``kind=`` filter accepted by ``gm.crs_catalog``
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
#: The symmetric predicates ``SpatialIndex.self_join`` accepts (a self-join
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
#: Distance/area unit for a CRS-aware metric's ``unit`` keyword. ``None``
#: follows the CRS: ellipsoidal units for geographic data, native linear units
#: for projected data, and raw coordinate units when CRS-free. Geographic
#: shape area and length follow straight longitude/latitude edges; point
#: distances use ellipsoidal geodesics. ``'planar'`` uses Cartesian coordinate
#: space (native angular units for geographic data); ``'meters'`` uses the CRS
#: metric and requires a CRS.
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
    """Aligned feature columns returned by ``from_geojson(..., features=True)``.

    Unpack as ``geometries, properties, ids = features``. Each metadata entry
    corresponds to the geometry at the same row; use ``len(features.geometries)``
    for the feature count. The outer columns are fixed, while property
    dictionaries remain editable.
    """

    __slots__ = ('geometries', 'ids', 'properties')
    __match_args__ = ('geometries', 'properties', 'ids')

    geometries: GeometryArray[Geometry | None]
    """One parsed geometry or ``None`` per input feature."""

    properties: tuple[dict[str, Any] | None, ...]
    """Row-aligned property mappings; ``None`` means no properties were supplied.

    A single input mapping is shallow-copied for each row, so editing one
    returned dictionary does not change another row.
    """

    ids: tuple[FeatureId, ...]
    """Row-aligned string, non-boolean integer, finite float, or ``None`` ids.

    An entry is ``None`` where the feature carried no id.
    """

    def __init__(
        self,
        geometries: GeometryArray[Geometry | None],
        properties: Mapping[str, Any]
        | Iterable[Mapping[str, Any] | None]
        | None = None,
        ids: Iterable[FeatureId] | None = None,
    ) -> None:
        """Build row-aligned feature columns.

        Parameters
        ----------
        geometries : GeometryArray[Geometry | None]
            One geometry or ``None`` for each input feature.
        properties : Mapping[str, Any] or Iterable[Mapping[str, Any] | None] or None
            A mapping or object with a callable ``keys()`` is broadcast as an
            independent shallow copy per row. An iterable supplies one mapping
            or ``None`` per geometry and is consumed once. ``None`` supplies
            one missing-properties value per geometry.
        ids : Iterable[FeatureId] or None
            One string, non-boolean integer, finite float, or ``None`` per
            geometry. ``None`` supplies one missing id per geometry.

        Returns
        -------
        Features
            The geometry, property, and id columns aligned by input row.

        Raises
        ------
        TypeError
            If geometries is not a GeometryArray, a property input is not a
            mapping or row iterable, a property key is not a string, or an id
            is not a string, non-boolean integer, finite float, or ``None``.
        GeometryError
            If property or id rows do not match the geometry count, property
            keys repeat, or a numeric id is not finite.
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
                    from gometry._lib import GeometryError

                    raise GeometryError('numeric feature ids must be finite')
                else:
                    collected_ids.append(identifier)
            id_rows = tuple(collected_ids)
        if len(property_rows) != rows:
            from gometry._lib import GeometryError

            raise GeometryError(
                f'properties length {len(property_rows)} does not match geometries length {rows}'
            )
        if len(id_rows) != rows:
            from gometry._lib import GeometryError

            raise GeometryError(
                f'ids length {len(id_rows)} does not match geometries length {rows}'
            )
        # ``object.__setattr__`` bypasses the frozen instance ``__setattr__``.
        object.__setattr__(self, 'geometries', geometries)
        object.__setattr__(self, 'properties', property_rows)
        object.__setattr__(self, 'ids', id_rows)

    @classmethod
    def _from_normalized(
        cls,
        geometries: GeometryArray[Geometry | None],
        properties: tuple[dict[str, Any] | None, ...],
        ids: tuple[FeatureId, ...],
    ) -> Self:
        """Finish native decoding after side-data admission and alignment."""
        instance = object.__new__(cls)
        object.__setattr__(instance, 'geometries', geometries)
        object.__setattr__(instance, 'properties', properties)
        object.__setattr__(instance, 'ids', ids)
        return instance

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f'cannot set attribute {name!r} on frozen Features')

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f'cannot delete attribute {name!r} on frozen Features')

    def __sizeof__(self) -> int:
        """Report the shallow retained footprint of the feature columns."""
        return (
            object.__sizeof__(self)
            + self.geometries.__sizeof__()
            + self.properties.__sizeof__()
            + self.ids.__sizeof__()
            + sum(
                property_row.__sizeof__()
                for property_row in self.properties
                if property_row is not None
            )
        )

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
    # The runtime object is intentionally unhashable; mypy models
    # object.__hash__ as callable and cannot express this contract.
    __hash__: ClassVar[None] = None  # type: ignore[assignment]

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

    def __reduce__(
        self,
    ) -> tuple[
        Callable[..., Features],
        tuple[
            GeometryArray[Geometry | None],
            tuple[dict[str, Any] | None, ...],
            tuple[FeatureId, ...],
        ],
    ]:
        return (_restore_features_v1, (self.geometries, self.properties, self.ids))

    def __iter__(
        self,
    ) -> Iterator[
        GeometryArray[Geometry | None]
        | tuple[dict[str, Any] | None, ...]
        | tuple[FeatureId, ...]
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


def _restore_features_v1(
    geometries: GeometryArray[Geometry | None],
    properties: tuple[dict[str, Any] | None, ...],
    ids: tuple[FeatureId, ...],
) -> Features:
    return Features(geometries, properties, ids)


class Extremes(NamedTuple, Generic[_ExtremeT]):
    """The westmost/southmost/eastmost/northmost vertices of a geometry.

    Returned by `Geometry.extremes()` (four scalar `Point` fields, or ``None``
    for an empty geometry) and `GeometryArray.extremes()` (four row-aligned
    ``GeometryArray[Point | None]`` columns; missing rows remain missing and
    present empty rows contain empty points). Unpacks as ``(west, south, east,
    north)``; ties keep the first vertex in storage order, and Z/M ordinates are
    carried through.
    """

    west: _ExtremeT
    """The vertex with the smallest X."""

    south: _ExtremeT
    """The vertex with the smallest Y."""

    east: _ExtremeT
    """The vertex with the largest X."""

    north: _ExtremeT
    """The vertex with the largest Y."""

    def __reduce__(self) -> tuple[Callable[..., Extremes[_ExtremeT]], tuple[Any, ...]]:
        return _restore_extremes_v1, tuple(self)


def _restore_extremes_v1(
    west: _ExtremeT, south: _ExtremeT, east: _ExtremeT, north: _ExtremeT
) -> Extremes[_ExtremeT]:
    return Extremes(west, south, east, north)


class PolygonizeResult(NamedTuple):
    """Full polygonization output: the polygons plus the leftover linework.

    Returned by `polygonize_all(diagnostics=True)`; unpacks as ``(polygons, cut_edges,
    dangles, invalid_rings)``, or use the named fields. ``polygons`` is the
    pooled result; each diagnostic is a ``Groups[GeometryArray[LineString]]``
    with one row per input source.
    """

    polygons: GeometryArray[Polygon]
    """The polygons formed from the noded linework."""

    cut_edges: Groups[GeometryArray[LineString]]
    """Noded edges that are not part of a polygon, grouped by source row."""

    dangles: Groups[GeometryArray[LineString]]
    """Noded edges with at least one free end, grouped by source row."""

    invalid_rings: Groups[GeometryArray[LineString]]
    """Invalid noded rings or duplicate edges, grouped by source row."""

    def __reduce__(self) -> tuple[Callable[..., PolygonizeResult], tuple[Any, ...]]:
        return _restore_polygonize_result_v1, tuple(self)


def _restore_polygonize_result_v1(
    polygons: GeometryArray[Polygon],
    cut_edges: Groups[GeometryArray[LineString]],
    dangles: Groups[GeometryArray[LineString]],
    invalid_rings: Groups[GeometryArray[LineString]],
) -> PolygonizeResult:
    return PolygonizeResult(polygons, cut_edges, dangles, invalid_rings)


class SupportsGeoInterface(Protocol[_GeoJsonInterfaceT_co]):
    """Any object exposing a typed GeoJSON-like ``__geo_interface__`` mapping."""

    @property
    def __geo_interface__(self) -> _GeoJsonInterfaceT_co: ...


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


class _SupportsToAuthority(Protocol):
    def to_authority(
        self,
    ) -> tuple[str, str | SupportsIndex] | list[str | SupportsIndex] | None: ...


class _SupportsToEpsg(Protocol):
    def to_epsg(self) -> SupportsIndex | None: ...


class _SupportsToJson(Protocol):
    def to_json(self) -> str: ...


class _SupportsCrsHolder(Protocol):
    @property
    def crs(self) -> CrsInput | None: ...


class _SupportsSrsHolder(Protocol):
    @property
    def srs(self) -> CrsInput | None: ...


#: A float lane: any iterable of floats, or a PEP-3118 buffer exporter
#: (numpy arrays — float32 included — ``array.array``, memoryviews). Buffer
#: values are copied into native storage; float32 values are converted to f64.
FloatColumn: TypeAlias = Iterable[float] | Buffer

#: One float per geometry: a scalar broadcasts, or pass an iterable /
#: zero-copy buffer with one value per element.
FloatInput: TypeAlias = FloatScalar | ScalarFloatArray | FloatColumn

ParseFormat: TypeAlias = Literal[
    'wkt',
    'wkb',
    'geojson',
    'geoarrow',
    'geoparquet',
    'h3',
    's2',
    'geohash',
    'tile',
    'quadkey',
    'polyline',
    'pluscode',
    'osm_shortlink',
    'pickle',
]

#: Anything accepted where a CRS is expected: an authority string
#: (``'EPSG:4326'``, WKT, PROJ), an integer-like EPSG code, an
#: ``(authority, code)`` pair, a PROJJSON/CF mapping, or a CRS-like object
#: exposing a supported conversion or holder protocol (including ``gometry.CRS``
#: and ``pyproj.CRS``).
CrsInput: TypeAlias = (
    str
    | SupportsIndex
    | tuple[str, str | SupportsIndex]
    | list[str | SupportsIndex]
    | Mapping[str, Any]
    | SupportsToWkt
    | _SupportsToAuthority
    | _SupportsToEpsg
    | _SupportsToJson
    | _SupportsCrsHolder
    | _SupportsSrsHolder
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

    ``geometry`` may be ``None`` in a FeatureCollection, in a GeoJSON row
    column, or with ``from_geojson(..., features=True)``. Geometry-only
    decoding rejects a scalar null-geometry Feature; use
    :class:`GeoJsonFeatureNonNull` for that overload.
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
    definition: NotRequired[str]
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
