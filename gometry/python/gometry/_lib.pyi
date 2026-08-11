# The CRS geodesic/factors calculators broadcast: passing any sequence ordinate
# returns batched results. The scalar and batched overloads necessarily overlap
# on the all-scalar call (the scalar overload, listed first, wins), which is
# exactly what `reportOverlappingOverload` flags — disable it for this stub.
# pyright: reportOverlappingOverload=false
import sys
import types
from collections.abc import (
    Callable,
    Hashable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    Sequence,
    ValuesView,
)
from os import PathLike
from typing import (
    Any,
    ClassVar,
    Final,
    Generic,
    Literal,
    Never,
    Self,
    SupportsIndex,
    TypeAlias,
    final,
    overload,
)

import numpy as np
import numpy.typing as npt

if sys.version_info >= (3, 15):
    from collections.abc import Buffer
    from typing import TypeVar, disjoint_base
else:
    from typing_extensions import Buffer, TypeVar, disjoint_base

from gometry._types import (
    ArrowEncoding,
    BoxWrap,
    BufferSide,
    CapStyle,
    Cell,
    CellRule,
    CoordinateAxes,
    CoverageOverlapRule,
    CrsAreaInput,
    CrsAreaOfUse,
    CrsAuthorityObject,
    CrsAxisInfo,
    CrsCacheInfo,
    CrsCatalogInfo,
    CrsCatalogKind,
    CrsCelestialBodyInfo,
    CrsCfInfo,
    CrsComparison,
    CrsDatabaseKind,
    CrsDatumInfo,
    CrsEllipsoidCatalogInfo,
    CrsEllipsoidInfo,
    CrsEngineInfo,
    CrsGeodesicBatchInfo,
    CrsGeodesicDirectBatchInfo,
    CrsGeodesicDirectInfo,
    CrsGeodesicInfo,
    CrsGeodesicInterpolateBatchInfo,
    CrsGeodesicInterpolateInfo,
    CrsGridDatabaseInfo,
    CrsIdentifyCandidate,
    CrsInfo,
    CrsInput,
    CrsKind,
    CrsOperationInfo,
    CrsPrimeMeridianCatalogInfo,
    CrsPrimeMeridianInfo,
    CrsProjectionFactors,
    CrsProjectionFactorsBatch,
    CrsProjOperationCatalogInfo,
    CrsRuntimeConfig,
    CrsUnitInfo,
    DistanceUnit,
    Extremes,
    FeatureId,
    Features,
    FloatColumn,
    FloatInput,
    GeoJsonFeature,
    GeoJsonFeatureCollection,
    GeoJsonFeatureNonNull,
    GeoJsonGeometry,
    GeometryType,
    GeoPandasSeries,
    GridOrigin,
    JoinStyle,
    NavigationPath,
    NestedCoordinates,
    Origin,
    PandasSeries,
    PolarsSeries,
    PolygonizeResult,
    PyArrowArray,
    PyArrowCapsule,
    PyArrowChunkedArray,
    PyArrowRecordBatch,
    PyArrowSchemaCapsule,
    PyArrowTable,
    RepairMethod,
    SimplifyMethod,
    SmoothMethod,
    SpatialCurve,
    SupportsArrowArray,
    SupportsArrowStream,
    SupportsGeoInterface,
    SymmetricTopologicalPredicate,
    TopologicalPredicate,
    TransformDirection,
    VoronoiClip,
    WktAxisRule,
    WktVersion,
)

# Kind-preserving transforms (reproject, affine, clean) return the same geometry
# subtype as their input, so a typed `Point`/`Polygon`/... flows through them.
_GeometryT = TypeVar('_GeometryT', bound=Geometry)
_GeometryOtherT = TypeVar('_GeometryOtherT', bound=Geometry)
# Invariant cell element type for `Groups[CellArray[...]]` self-type overloads
# (the covariant `_CellT_co` can't appear in an input position).
# Unbound: a `bound=Cell` here (and on `_CellT_co`) collides with the recursive
# `Cell` protocol (`neighbors`/`children` → `CellArray[Self]`) — structural
# membership can't prove `Self <: Cell` while `CellArray` demands that proof.
_CellT = TypeVar('_CellT')
# Dissolve group key element type (the `by` iterable's element type).
_GroupKeyT = TypeVar('_GroupKeyT', bound=Hashable)
_DefaultT = TypeVar('_DefaultT')
# Element type of a `GeometryArray`; covariant so `GeometryArray[Point]` is a
# `GeometryArray[Geometry]`, and defaulted (PEP 696) so a bare `GeometryArray`
# means `GeometryArray[Geometry]`. `gm.points(...)` yields `GeometryArray[Point]`.
_GeometryT_co = TypeVar(
    '_GeometryT_co', bound=Geometry, covariant=True, default=Geometry
)
# Row element type of a `Groups` container (int64 ndarray, `GeometryArray`, …).
# Bounded (not value-constrained) so aliases like `Groups[GeometryArray[Point]]`
# keep their precise element type, and covariant so a Groups of a narrower
# payload flows where a wider one is expected (Groups is read-only).
_GroupValuesT_co = TypeVar(
    '_GroupValuesT_co',
    covariant=True,
)

__version__: Final[str]

__all__ = [
    'CRS',
    'AccuracyWarning',
    'CRSError',
    'CRSMismatchError',
    'CellArray',
    'CellArrayIterator',
    'Coordinates',
    'CoordinatesIterator',
    'GeohashCell',
    'GeohashCoverage',
    'GeohashCoverageIterator',
    'Geometry',
    'GeometryArray',
    'GeometryArrayIterator',
    'GeometryCollection',
    'GeometryError',
    'GeometryParts',
    'GeometryPartsIterator',
    'GeometryTypeError',
    'Groups',
    'GroupsIterator',
    'H3Cell',
    'H3Coverage',
    'H3CoverageIterator',
    'H3Edge',
    'H3EdgeArray',
    'H3EdgeArrayIterator',
    'H3Vertex',
    'H3VertexArray',
    'H3VertexArrayIterator',
    'InvalidGeometryError',
    'LineString',
    'MultiLineString',
    'MultiPoint',
    'MultiPolygon',
    'ParseError',
    'Point',
    'Polygon',
    'PreparedGeometry',
    'S2Cell',
    'S2Coverage',
    'S2CoverageIterator',
    'SpatialIndex',
    'SpatialIndexIterator',
    'Tile',
    'TileCoverage',
    'TileCoverageIterator',
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

#: An integer input lane: either one scalar or a column of integers.
_IntInput: TypeAlias = int | Iterable[int] | Buffer
_IndexLane: TypeAlias = Iterable[int] | Buffer
_BoolLane: TypeAlias = Iterable[bool]
_Coordinate: TypeAlias = Iterable[float]
_AffineMatrix: TypeAlias = (
    tuple[float, float, float, float, float, float] | Iterable[float]
)
_Bounds2D: TypeAlias = tuple[float, float, float, float]
_Bounds3D: TypeAlias = tuple[float, float, float, float, float, float]
_WktOutputDimension: TypeAlias = Literal[2, 3, 4]

#: A scalar GeoJSON input: JSON text, any mapping, or a
#: ``__geo_interface__``-bearing object.
_GeoJsonScalar: TypeAlias = (
    str | bytes | bytearray | memoryview | Mapping[str, Any] | SupportsGeoInterface
)

# One geometry-like row accepted by the canonical collection-ingest boundary.
# Strings deliberately stay out: text has an ambiguous WKT/GeoJSON grammar and
# therefore goes through ``from_wkt`` / ``from_geojson`` explicitly.
_GeometryLike: TypeAlias = Geometry | Buffer | Mapping[str, Any] | SupportsGeoInterface
_ArealLike: TypeAlias = (
    Polygon | MultiPolygon | Buffer | Mapping[str, Any] | SupportsGeoInterface
)

_ArrowInput: TypeAlias = (
    SupportsArrowArray
    | SupportsArrowStream
    | PyArrowArray
    | PyArrowChunkedArray
    | PyArrowTable
    | PyArrowRecordBatch
)

class AccuracyWarning(UserWarning):
    """A CRS transform used a lower-accuracy fallback because a required grid is unavailable."""

class GeometryError(ValueError):
    """Base class for every error gometry raises about your data or parameters."""

    param: str | None
    """The offending parameter's name, for value-lane errors (else ``None``)."""
    value: float | None
    """The offending numeric value, for value-lane errors (else ``None``)."""
    operation: str | None
    """The operation associated with a bounded-output error (else ``None``)."""
    parameter: str | None
    """The parameter that controls bounded output (else ``None``)."""
    produced: int | None
    """The number of produced items for a bounded-output error (else ``None``)."""
    limit: int | None
    """The configured output limit, for bounded-output errors (else ``None``)."""

class InvalidGeometryError(GeometryError):
    """A geometry violates a structural or numeric rule."""

    operation: str | None
    """The operation that failed, for overlay errors (else ``None``)."""

class GeometryTypeError(GeometryError, TypeError):
    """An operation received a geometry of the wrong kind."""

    expected: str | None
    """The expected geometry kind, for wrong-kind errors (else ``None``)."""
    got: str | None
    """The received geometry kind when known (else ``None``)."""

class CRSError(GeometryError):
    """A CRS could not be created, identified, exported, or used."""

    crs: str | None
    """The offending CRS, for axis-unit-mismatch errors (else ``None``)."""

class CRSMismatchError(CRSError):
    """Operands carry incompatible CRS or coordinate-epoch metadata."""

    field: Literal['crs', 'epoch'] | None
    """The incompatible frame field (else ``None`` for hand-built errors)."""
    left: str | float | None
    """The left raw CRS string or coordinate epoch (else ``None``)."""
    right: str | float | None
    """The right raw CRS string or coordinate epoch (else ``None``)."""
    index: int | None
    """The offending collection item's index, for shared-frame errors (else ``None``)."""

class TransformError(CRSError):
    """A coordinate transform could not be built or failed to run."""

    source: str | None
    """The source CRS of the failed transform (else ``None``)."""
    target: str | None
    """The target CRS of the failed transform (else ``None``)."""

class ParseError(GeometryError):
    """Serialized input (WKT, WKB, GeoJSON, GeoArrow, or pickle) is malformed."""

    format: (
        Literal[
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
        | None
    )
    """Which codec rejected the input."""
    position: int | None
    """WKT UTF-8 input length; WKB true detection byte offset; otherwise None when unavailable."""

@final
class CoordinatesIterator:
    """Lazy iterator over a ``Coordinates`` view, yielding one coordinate tuple per
    step. Holds a cursor into the view (O(1) construct, O(1) next on storage-
    shaped owners) rather than materializing all points up front.
    """
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """
    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> tuple[float | None, ...]:
        """Implement next(self)."""

@final
class Coordinates(Sequence[tuple[float | None, ...]]):
    """Flat, indexable coordinate sequence behind `geom.coords`:
    coordinates flattened depth-first across parts/rings, with per-axis columns.

    Random access (`coords[i]`) is storage-shaped O(1)/O(log runs) on packed
    columns and single-run shapes; iteration is a view-owning cursor (O(1)
    construct, O(1) next) rather than an eager materialization of every vertex.

    For `GeometryArray.coords`, missing rows contribute no vertices. The view is
    a flattened vertex stream, not a row-aligned container; use
    `get_coordinates(..., return_index=True)` when you need source-row indexes,
    or call `drop_missing()` first for an explicit dense-only path.
    """

    __array_ufunc__: ClassVar[None]
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Coordinates are returned by ``geom.coords`` and cannot be constructed."""
    @overload
    def __array__(
        self, dtype: None = None, copy: bool | None = None
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.float64] | np.dtype[np.float64] | Literal['float64', 'f8', 'd'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.float32] | np.dtype[np.float32] | Literal['float32', 'f4'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.float32]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.float16] | np.dtype[np.float16] | Literal['float16', 'f2', 'e'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.float16]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.floating[Any] | float] | np.dtype[np.floating[Any]],
        copy: bool | None = None,
    ) -> npt.NDArray[np.floating[Any]]:
        """NumPy array protocol: export as a ``(N, dims)`` floating ndarray.

        Parameters
        ----------
        dtype : float dtype, optional
            ``None`` or any floating dtype (native export is ``float64``;
            other floating dtypes are cast with ``astype``).
        copy : bool, optional
            When ``False``, raises — coordinate export always copies.

        Returns
        -------
        numpy.ndarray
            The ``(N, dims)`` coordinate matrix (``float64`` by default).

        Raises
        ------
        ValueError
            If ``copy`` is ``False``.
        GeometryError
            If ``dtype`` is not a floating dtype.
        """
    def __len__(self) -> int:
        """Number of vertices in this coordinate view.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the wrapper plus the logical Rust-side
        coordinate heap retained by this view (ordinate payload and structural
        row metadata for array-backed views). Shared backing buffers are
        reported like NumPy views, not as the full parent allocation. Parent
        geometry/array sidecars (prepared caches, coverage membership, …) are
        not owned by the view and are not counted here.
        """
    def __reduce__(self) -> Never: ...
    def __copy__(self) -> Self: ...
    def __deepcopy__(self, memo: object) -> Self: ...
    @property
    def nbytes(self) -> int:
        """Logical coordinate payload in bytes (numpy's ``nbytes`` convention):
        the stored ``f64`` ordinate values behind this view only. Slices and
        array-backed views report only their logical rows. Temporary NumPy
        matrices from ``numpy.asarray(coords)`` and any lazy prepared-geometry
        or membership sidecars on the parent geometry/array are not included —
        those live on the owner, not on this view.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> tuple[float | None, ...]: ...
    @overload
    def __getitem__(self, index: slice, /) -> list[tuple[float | None, ...]]: ...
    @overload
    def __getitem__(
        self, index: SupportsIndex | slice, /
    ) -> tuple[float | None, ...] | list[tuple[float | None, ...]]:
        """Select vertices by integer or slice.

        An ``int`` returns one coordinate tuple ``(x, y[, z[, m]])``.
        A ``slice`` returns a ``list`` of those tuples.

        Returns
        -------
        tuple or list of tuple
        """
    def __iter__(self) -> CoordinatesIterator:
        """Iterate coordinate tuples in vertex order.

        Returns
        -------
        iterator of tuple
        """
    def __reversed__(self) -> CoordinatesIterator:
        """Iterate coordinate tuples in reverse vertex order.

        Returns
        -------
        iterator of tuple
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    __hash__: ClassVar[None]  # type: ignore[assignment]
    def __contains__(self, item: object, /) -> bool:
        """Whether a coordinate tuple appears among the vertices.

        Returns
        -------
        bool
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of an equal coordinate in ``[start, stop)``.

        Parameters
        ----------
        value : object
            The coordinate value to locate.
        start : int, default 0
            First position searched.
        stop : int, optional
            One past the last position searched.

        Returns
        -------
        int
            The first matching position.

        Raises
        ------
        ValueError
            If no coordinate in the window equals ``value``.
        """
    def count(self, value: object) -> int:
        """Number of coordinates equal to ``value`` under the visible layout
        (same representation as iteration / ``select``).

        Parameters
        ----------
        value : object
            The coordinate value to count.

        Returns
        -------
        int
        """
    @property
    def coordinate_axes(self) -> CoordinateAxes:
        """Coordinate layout: ``'XY'``, ``'XYZ'``, ``'XYM'``, or ``'XYZM'`` — the
        `select`-forced layout when set, else the union of the present axes.
        """
    def select(self, axes: CoordinateAxes) -> Coordinates:
        """Return a view of the same coordinates in a fixed ``coordinate_axes`` layout
        (``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'``): every tuple/column has that
        shape. Nested/tuple output (``to_nested`` / ``select`` iteration) uses
        ``None`` where a coordinate lacks the requested Z/M; ndarray columns
        (``.x``/``.y``/``.z``/``.m``) and ``numpy.asarray`` use NaN for absent
        axes. Makes a mixed-dimension array rectangular for iteration and
        ``numpy.asarray``.

        Parameters
        ----------
        axes : {'XY', 'XYZ', 'XYM', 'XYZM'}
            The layout every tuple and column should take.

        Returns
        -------
        Coordinates
            The same coordinates with the forced layout.

        Examples
        --------
        >>> import gometry as gm
        >>> coords = gm.LineString([(0, 0), (1, 1)]).coords
        >>> coords.select('XY').to_nested()
        [(0.0, 0.0), (1.0, 1.0)]
        """
    @property
    def row_index(self) -> npt.NDArray[np.int64]:
        """Per-coordinate source geometry row (all ``0`` for a scalar geometry);
        the source row for each vertex of a ``GeometryArray``.
        """
    @property
    def x(self) -> npt.NDArray[np.float64]:
        """The X ordinates as a read-only ``float64`` ``numpy.ndarray``."""
    @property
    def y(self) -> npt.NDArray[np.float64]:
        """The Y ordinates as a read-only ``float64`` ``numpy.ndarray``."""
    @property
    def z(self) -> npt.NDArray[np.float64]:
        """The Z ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
        length (``NaN`` when no coordinate carries Z).
        """
    @property
    def m(self) -> npt.NDArray[np.float64]:
        """The M ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
        length (``NaN`` when no coordinate carries M).
        """
    def to_nested(self) -> NestedCoordinates:
        """Return the coordinates as a nested structure mirroring the geometry's topology
        (point → ``[x, y]``, line → list of tuples, polygon → list of rings;
        arrays → one entry per present geometry) — the ``__geo_interface__``-style
        nesting, as Python lists. Missing array rows are skipped. The flat
        columns do not preserve this shape.

        Returns
        -------
        list
            Nested Python lists and coordinate tuples matching the topology.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).coords.to_nested()
        [(0.0, 0.0), (1.0, 1.0)]
        """
    @overload
    def to_dict(
        self, *, index: Literal[False] = False
    ) -> dict[str, npt.NDArray[np.float64]]: ...
    @overload
    def to_dict(
        self, *, index: Literal[True]
    ) -> dict[str, npt.NDArray[np.float64] | npt.NDArray[np.int64]]:
        """Return a dependency-free column dict — ``{'x': ndarray, 'y': ndarray, …}`` — ready
        for ``pandas``/``polars`` (``pd.DataFrame(coords.to_dict())``).
        ``z``/``m`` columns appear when present or `select`-forced (``NaN`` for
        rows that lack them); ``index=True`` adds the source-geometry row
        column.

        Parameters
        ----------
        index : bool, default False
            Add an ``'index'`` column with each coordinate's source row.

        Returns
        -------
        dict
            One read-only ndarray per axis (plus ``'index'`` when requested).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).coords.to_dict()['x'].tolist()
        [0.0, 1.0]
        """

_CoordinatesInput: TypeAlias = Iterable[_Coordinate] | Coordinates
_RingsInput: TypeAlias = Iterable[_CoordinatesInput]

@disjoint_base
class Geometry:
    """An immutable geometry with optional CRS.

    The frozen scalar value at the heart of gometry: a point, linestring,
    polygon, multi-part, or collection, carrying its coordinate
    dimensionality (XY/XYZ/XYM/XYZM) and an optional CRS + epoch frame.
    Construct with the leaf classes (``Point(...)``, ``LineString(...)``) or
    parsers (``from_wkt(...)``),
    inspect it (``geom.bounds``, ``geom.coords``), relate it
    (``contains(geom, other)``), measure it (``geom.area`` — meters when
    a CRS is set), and derive from it (``geom.buffer(10.0)``); every
    operation returns a new geometry. Instances are one of the typed subclasses
    (``Point``, ``LineString``, ``Polygon``, ...), so ``isinstance`` narrows.
    """
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Create and return a new object.  See help(type) for accurate signature."""
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle support: round-trip through plain WKB (Z/M preserved) plus the
        frame (canonical CRS string, epoch). Enables ``pickle``, ``copy``, and
        ``deepcopy``, so it round-trips through multiprocessing and caches.
        """
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — geometries are immutable
        values, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    @overload
    def __and__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __and__(self, other: GeometryArray, /) -> GeometryArray: ...
    @overload
    def __and__(self, other: Geometry | GeometryArray, /) -> Geometry | GeometryArray:
        """Return self&value."""
    @overload
    def __or__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __or__(self, other: GeometryArray, /) -> GeometryArray: ...
    @overload
    def __or__(self, other: Geometry | GeometryArray, /) -> Geometry | GeometryArray:
        """Return self|value."""
    @overload
    def __sub__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __sub__(self, other: GeometryArray, /) -> GeometryArray: ...
    @overload
    def __sub__(self, other: Geometry | GeometryArray, /) -> Geometry | GeometryArray:
        """Return self-value."""
    @overload
    def __xor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __xor__(self, other: GeometryArray, /) -> GeometryArray: ...
    @overload
    def __xor__(self, other: Geometry | GeometryArray, /) -> Geometry | GeometryArray:
        """Return self^value."""
    def __bool__(self) -> bool:
        """``False`` when the geometry is empty; ``True`` otherwise.

        Empty means a typed empty shape (for example ``POINT EMPTY``), not
        a missing array row.

        Returns
        -------
        bool
        """
    def __format__(self, spec: str) -> str:
        """Format-spec display: ``''`` → WKT;
        ``[0][.N][fFgG]`` → WKT with ``N``-decimal coordinates, fixed (``f``)
        or trailing-zero-trimmed (``g``); ``x``/``X`` → lower/uppercase hex
        ISO WKB. Display only — the geometry is unchanged.
        """
    def __sizeof__(self) -> int:
        """Retained native cost of this geometry for ``sys.getsizeof``.

        Counts the Python-facing struct, the Arc-owned ``ShapeData`` block
        (including the ``Shape`` payload — coordinate columns **and**
        container allocations such as multipart ``Vec``s, polygon hole
        ``Arc``s, and nested collection members — plus any
        *already-initialized* prepared caches), and the Arc-owned
        frame-cache sidecar with any products already built on it.
        Uninitialized lazy caches are not counted and this method never
        builds them — so two cold ``__sizeof__`` reads report the same
        size, and warming (``bounds``, ``prepare``, distance, …) can only
        increase it. Container geometries therefore scale with part/member
        count even when members carry no ordinate payload (e.g. empty
        points in a ``GeometryCollection``).

        ``nbytes`` remains the coordinate-only payload (numpy convention);
        use ``__sizeof__`` when measuring object retention.

        Returns
        -------
        int
        """
    @property
    def nbytes(self) -> int:
        """Raw coordinate payload in bytes (numpy's ``nbytes`` convention): the
        stored ``f64`` ordinate columns only — object headers, prepared-state
        caches, and CRS metadata are excluded.

        Returns
        -------
        int
        """
    def _repr_svg_(self) -> str:
        """Render the geometry to a standalone SVG string for inline display in
        Jupyter and the documentation. Valid geometries are drawn green, invalid
        ones red. For geographic (lon/lat) coordinates this
        is a schematic, not a projection.

        Returns
        -------
        str
            The standalone SVG markup.
        """
    def _repr_html_(self) -> str:
        """HTML preview: a one-line header (`__repr__`) followed by the SVG.

        Returns
        -------
        str
            The HTML preview markup.
        """
    @property
    def geometry_type(self) -> GeometryType:
        """OGC geometry type name, e.g. ``'Point'`` or ``'MultiPolygon'``.

        Returns
        -------
        str
            One of ``'Point'``, ``'LineString'``, ``'Polygon'``,
            ``'MultiPoint'``, ``'MultiLineString'``, ``'MultiPolygon'``,
            ``'GeometryCollection'``.
        """
    @property
    def crs(self) -> CRS | None:
        """CRS attached to this geometry, or ``None``.

        Returns
        -------
        CRS or None
        """
    @property
    def epoch(self) -> float | None:
        """Coordinate epoch of this geometry, if set.

        Returns
        -------
        float or None
        """
    @property
    def coordinate_axes(self) -> CoordinateAxes:
        """Ordinate layout: ``'XY'``, ``'XYZ'``, ``'XYM'``, or ``'XYZM'``.
        This is the coordinate *layout* (which ordinates are present), not the
        topological dimension (see `topological_dimension`).

        Returns
        -------
        str
            The geometry's exact ``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'``
            coordinate layout.
        """
    @property
    def topological_dimension(self) -> Literal[0, 1, 2]:
        """Topological dimension: ``0`` (point), ``1`` (curve), or ``2`` (surface).
        The maximum over members for collections. Distinct from the coordinate
        layout (see `coordinate_axes`).

        Returns
        -------
        int
        """
    @property
    def has_z(self) -> bool:
        """Whether the geometry carries a Z ordinate.

        Returns
        -------
        bool
        """
    @property
    def has_m(self) -> bool:
        """Whether the geometry carries an M ordinate.

        Returns
        -------
        bool
        """
    @property
    def bounds(self) -> tuple[float, float, float, float] | None:
        """Axis-aligned bounds ``(minx, miny, maxx, maxy)``, or ``None`` if empty.
        Empty rows in a `GeometryArray` are all-``nan`` instead — see
        `GeometryArray.bounds`.

        Returns
        -------
        tuple or None
        """
    @property
    def is_empty(self) -> bool:
        """Whether the geometry is empty (no points, rings, or parts).

        Returns
        -------
        bool
        """
    @property
    def num_geometries(self) -> int:
        """Number of top-level parts: ``1`` for a single point/line/polygon,
        the member count for a multi/collection, ``0`` for empty — the
        ``O(1)`` counterpart to ``len(geoms)`` without materializing parts.

        Returns
        -------
        int
        """
    @property
    def parts(self) -> GeometryParts[Geometry]:
        """Lazy view over this geometry's top-level parts.

        Simple geometries expose themselves as a one-row view; multipart and
        collection geometries expose their members. Use free function ``parts`` for
        the materialized `GeometryArray` form.

        Returns
        -------
        GeometryParts
        """
    def estimate_local_crs(self) -> CRS:
        """Estimate a conformal metric CRS for this geometry.

        The complete geometry extent is evaluated against a fixed 0.1% linear
        scale-error ceiling. A datum-aware UTM or UPS CRS is preferred when it
        fits; otherwise a receiver-centered conformal CRS is considered.
        Empty, CRS-free, or geographically unsafe geometries raise ``CRSError``.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError
            If the geometry has no CRS, is empty, or one safe local frame
            cannot represent its extent.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(-122.4, 37.8, crs=4326).estimate_local_crs()
        CRS("EPSG:32610")
        """
    @property
    def is_closed(self) -> bool:
        """Whether every (multi)linestring component starts and ends at the same
        point.

        Returns
        -------
        bool
        """
    @property
    def is_ring(self) -> bool:
        """Whether the geometry is a closed, simple ``LineString`` (a ring).
        Geographic antimeridian crossings are normalized before the simplicity
        test; projected and CRS-free geometry remains planar.

        Returns
        -------
        bool
        """
    @property
    def is_ccw(self) -> bool:
        """Whether a closed ``LineString`` winds counter-clockwise.
        ``False`` for open lines and non-lineal geometry.

        Returns
        -------
        bool
        """
    @property
    def is_simple(self) -> bool:
        """Whether the geometry has no self-intersections or self-tangencies.
        Repeated CONSECUTIVE vertices are removable redundancy, not a
        self-intersection, so they do not affect simplicity. Areal simplicity
        is validity: a polygon/multipolygon
        is simple exactly when it is ``is_valid`` — so holes outside the
        shell, nested holes, or a disconnected interior make it not simple
        even with no ring self-crossing. Collections are never simple.
        Geographic antimeridian crossings are normalized before topology is
        evaluated; projected and CRS-free geometry remains planar.

        Returns
        -------
        bool
        """
    @property
    def is_valid(self) -> bool:
        """Whether the geometry is topologically valid in its coordinate frame.
        Geographic antimeridian crossings are normalized first; projected and
        CRS-free geometry uses ordinary planar OGC validity.
        ``True`` exactly when `validate` finds no issue; call
        `validate` for the reason, location, and path of a failure, and
        ``repair`` to fix it.

        Returns
        -------
        bool
        """
    @property
    def __geo_interface__(self) -> GeoJsonGeometry:
        """GeoJSON-like mapping for the `__geo_interface__` protocol.

        Z ordinates are included when present; M ordinates are deliberately
        omitted (`GeoJSON` has no M slot); measure values are
        not folded into ``coordinates`` — use ``coords``, ``to_wkt()``, or
        WKB when M must round-trip.

        Returns
        -------
        dict
            A mapping with ``type`` and ``coordinates`` (or ``geometries`` for
            collections).
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    __array_ufunc__: ClassVar[None]
    @property
    def area(self) -> float:
        """Area in CRS-natural units for the geometry's CRS.

        A geographic CRS gives ellipsoidal square meters (geodesic, on the CRS's
        own ellipsoid); a projected CRS gives squared native coordinate units;
        a CRS-free geometry gives squared coordinate units. Use ``to_crs`` to
        change frame.

        Returns
        -------
        float
            The area; ``0`` for non-areal geometries.

        Raises
        ------
        CRSError
            If the CRS lacks linear axis units for a metric result.

        See Also
        --------
        length : Length/perimeter under the same CRS-aware metric.
        """
    @property
    def length(self) -> float:
        """Length (curves) or perimeter (areal), measured for the geometry's CRS.

        A geographic CRS gives ellipsoidal meters (geodesic, on the CRS's own
        ellipsoid); a projected CRS gives native linear units; a CRS-free
        geometry gives coordinate units. Use ``to_crs`` to change frame.

        Returns
        -------
        float
            The length or perimeter; ``0`` for points.

        Raises
        ------
        CRSError
            If the CRS lacks linear axis units for a metric result.

        See Also
        --------
        area : Area under the same CRS-aware metric.
        """
    @property
    def length_3d(self) -> float:
        """3D length of curves with Z, measured for the geometry's CRS.

        Returns
        -------
        float

        Raises
        ------
        CRSError
            If the CRS lacks linear axis units for a metric result.
        InvalidGeometryError
            If the geometry lacks Z on every vertex.
        """
    @property
    def min_z(self) -> float | None:
        """Smallest Z ordinate, or ``None`` if no vertex carries Z.

        Returns
        -------
        float or None
        """
    @property
    def max_z(self) -> float | None:
        """Largest Z ordinate, or ``None`` if no vertex carries Z.

        Returns
        -------
        float or None
        """
    @property
    def z_range(self) -> float | None:
        """Span of Z ordinates (``max_z - min_z``), or ``None`` without Z.

        Returns
        -------
        float or None
        """
    @property
    def min_m(self) -> float | None:
        """Smallest M ordinate, or ``None`` if no vertex carries M.

        Returns
        -------
        float or None
        """
    @property
    def max_m(self) -> float | None:
        """Largest M ordinate, or ``None`` if no vertex carries M.

        Returns
        -------
        float or None
        """
    @property
    def m_range(self) -> float | None:
        """Span of M ordinates (``max_m - min_m``), or ``None`` without M.

        Returns
        -------
        float or None
        """
    @property
    def bounds_3d(self) -> tuple[float, float, float, float, float, float] | None:
        """3D bounding box ``(minx, miny, minz, maxx, maxy, maxz)``.

        Returns
        -------
        tuple of float or None
            ``None`` when the geometry is empty or carries no Z ordinate.
        """
    @property
    def coords(self) -> Coordinates:
        """Coordinate view over this geometry's vertices (storage-shaped index /
        cursor iteration — not an eagerly materialized vertex list).

        Returns
        -------
        Coordinates
            Flat, indexable view of vertex coordinates (X/Y and active Z/M).
        """
    @property
    def num_coordinates(self) -> int:
        """Total number of coordinates in this geometry.

        Returns
        -------
        int
        """
    @overload
    def set_coordinates(self, coordinates: npt.ArrayLike | Coordinates, /) -> Self: ...
    @overload
    def set_coordinates(
        self,
        coordinates: None = None,
        /,
        *,
        x: FloatColumn,
        y: FloatColumn,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
    ) -> Self:
        """Return a geometry with the same topology and replacement coordinates.

        Pass one ``(N, dims)`` matrix (including a `Coordinates` view) or
        explicit ``x=`` and ``y=`` columns. The vertex count and ordinate
        layout are preserved; use dimension setters for adding or removing
        Z/M axes.

        Parameters
        ----------
        coordinates : sequence of float, optional
            Replacement ``(N, dims)`` coordinate matrix, including a
            `Coordinates` view.
        x, y : sequence of float, optional
            Replacement X and Y columns.
        z, m : sequence of float, optional
            Replacement Z and M columns when this geometry already has those
            axes. Omitted axes are carried unchanged; ``None`` is not a
            clearing sentinel.

        Returns
        -------
        Geometry

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).set_coordinates([(5, 5), (6, 6)]).to_wkt()
        'LINESTRING (5 5, 6 6)'
        """
    def map_coordinates(
        self, func: Callable[[npt.NDArray[np.float64]], npt.ArrayLike]
    ) -> Self:
        """Apply a vectorized callback to this geometry's coordinate matrix.

        The callback receives a read-only ``(N, dims)`` float64 matrix and must
        return a matrix with the same shape. Topology, CRS, epoch, and ordinate
        layout are preserved.

        Parameters
        ----------
        func : callable
            Function called with the read-only coordinate matrix.

        Returns
        -------
        Geometry

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).map_coordinates(lambda m: m + 1).to_wkt()
        'LINESTRING (1 1, 2 2)'
        """
    @property
    def crosses_antimeridian(self) -> bool:
        """Whether a geographic geometry crosses the antimeridian.

        Returns
        -------
        bool

        Raises
        ------
        CRSError
            If the CRS is projected (a geographic CRS or CRS-free lon/lat is
            required).
        """
    def prepare(self) -> PreparedGeometry:
        """Build a `PreparedGeometry` with a cached spatial index.
        Amortizes repeated predicate queries (``contains``/``intersects``/…)
        against this geometry; build once, query many times.

        Returns
        -------
        PreparedGeometry

        Examples
        --------
        >>> import gometry as gm
        >>> prep = gm.box(0, 0, 2, 2).prepare()
        >>> prep.contains(gm.Point(1, 1))
        True
        """
    def __replace__(
        self, *, crs: CrsInput | None = ..., epoch: float | None = ...
    ) -> Self:
        """Return a copy with the given CRS/epoch metadata replaced.

        Supports ``copy.replace`` on Python 3.13+; omitted keyword arguments
        keep the current value. ``crs=None`` / ``epoch=None`` clear metadata.

        Parameters
        ----------
        crs : str or int or None, optional
            Replace or clear the CRS label.

        epoch : float or None, optional
            Replace or clear the coordinate epoch.

        Returns
        -------
        Geometry
        """
    def set_epoch(self, epoch: float | None, *, overwrite: bool = False) -> Self:
        """Declare (or clear) the coordinate epoch without moving coordinates.
        The epoch is the decimal year a dynamic-frame coordinate set was
        observed (e.g. ``2020.0``), metadata for transforms between dynamic and
        static datums. ``set_epoch(None)`` clears it. Changing a present epoch
        to a different value requires ``overwrite=True`` (the
        silent-frame-change guard, like ``set_crs``).

        Parameters
        ----------
        epoch : float or None
            Decimal year, or ``None`` to clear.

        overwrite : bool, default False
            Allow replacing an existing, different epoch.

        Returns
        -------
        Geometry
            A copy carrying the new epoch (same coordinates and CRS).

        Raises
        ------
        CRSError
            If a present epoch would change without ``overwrite=True``.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(-122.4, 37.8, crs=4326).set_epoch(2020.0).epoch
        2020.0
        """

    def set_crs(self, crs: CrsInput | None, *, overwrite: bool = False) -> Self:
        """Attach or relabel the CRS *without* moving coordinates.
        Declares what the coordinates already mean; to actually reproject them
        use `to_crs`. Attaching to a CRS-free geometry, clearing with
        ``None``, and identical re-tags are free; replacing one declared CRS
        with a *different* one requires ``overwrite=True`` (it is almost always
        a reprojection mistake).

        Parameters
        ----------
        crs : str or int
            Target CRS (EPSG or authority/WKT), or ``None`` to clear.

        overwrite : bool, default False
            Allow replacing an existing, different CRS label.

        Returns
        -------
        Geometry
            A copy carrying the new CRS label.

        Raises
        ------
        CRSError
            If ``crs`` is not a recognized CRS, or it would silently replace a
            different declared CRS without ``overwrite``.

        See Also
        --------
        to_crs : Reproject coordinates to another CRS.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 2).set_crs(4326).crs
        CRS("EPSG:4326")
        """
    def to_crs(
        self,
        crs: CrsInput,
        *,
        area_of_interest: CrsAreaInput | None = None,
        epoch: float | None = None,
        authority: str | None = None,
        accuracy: float | None = None,
        allow_ballpark: bool | None = None,
        only_best: bool | None = None,
        force_over: bool = False,
    ) -> Self:
        """Reproject coordinates to a target CRS.
        The source coordinate epoch is this geometry's own ``epoch`` metadata;
        transform between dynamic frames by stamping the source with
        ``set_epoch`` first. ``epoch`` here labels the *output* coordinate
        epoch.

        Parameters
        ----------
        crs : str or int
            Target CRS as an EPSG code or authority/WKT string.

        area_of_interest : sequence of float, optional
            Bounding ``(west, south, east, north)`` to pick the best transform.

        epoch : float, optional
            Output coordinate epoch (decimal year) to tag on the result, for
            dynamic frames. Omitted keeps the source epoch while it still
            means something: the CRS is unchanged, or the target CRS is
            dynamic (time-dependent). A static target clears it.

        authority : str, optional
            Restrict candidate coordinate operations to this authority
            (e.g. ``'EPSG'``).

        accuracy : float, optional
            Maximum acceptable operation accuracy, in meters.

        allow_ballpark : bool, optional
            Allow low-accuracy ballpark operations when no precise one exists.

        only_best : bool, optional
            Use only the single best operation; no fallback.

        force_over : bool, optional
            Keep coordinates on the source side of the antimeridian instead of
            wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
            ``only_best``, this also collapses operation selection to a single
            candidate, so enumerating surfaces return exactly one operation.

        Returns
        -------
        Geometry
            The geometry reprojected to ``crs`` (same geometry type).

        Raises
        ------
        CRSError
            If a CRS is invalid or the source is missing.
        TransformError
            If no transform exists between the frames or it fails to apply.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        See Also
        --------
        set_crs : Declare/relabel the CRS *without* moving coordinates.

        Examples
        --------
        >>> import gometry as gm
        >>> round(gm.Point(1, 2, crs=4326).to_crs(3857).x, 2)
        111319.49
        """
    def to_arrow(self, *, encoding: ArrowEncoding = 'auto') -> PyArrowArray:
        """Export the geometry as a `GeoArrow` array.

        Parameters
        ----------
        encoding : {'auto', 'wkb'}, default auto
            ``auto`` exports the geometry as its native GeoArrow layout;
            ``wkb`` exports a GeoArrow WKB array.

        Returns
        -------
        object

        Examples
        --------
        >>> import gometry as gm
        >>> type(gm.Point(1, 2).to_arrow()).__name__
        'ExtensionArray'
        """
    def __arrow_c_schema__(self) -> PyArrowCapsule:
        """Export the geometry's Arrow C Data schema as an ``arrow_schema``
        capsule.
        """
    def __arrow_c_array__(
        self, requested_schema: PyArrowSchemaCapsule | None = None
    ) -> tuple[PyArrowCapsule, PyArrowCapsule]:
        """Export the geometry as Arrow C Data ``(schema, array)`` capsules."""
    def __arrow_c_stream__(
        self, requested_schema: PyArrowSchemaCapsule | None = None
    ) -> PyArrowCapsule:
        """Export the geometry as a one-batch Arrow C stream capsule."""
    def to_polyline(self, *, precision: int = 5, drop_epoch: bool = False) -> str:
        """Encode the ``LineString`` or ``Point`` as Google polyline text (see
        `from_polyline`).

        Parameters
        ----------
        precision : int, default 5
            Decimal digits encoded per ordinate (``0`` to ``11``).
        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which polyline cannot
            encode.

        Returns
        -------
        str
            The encoded polyline.

        Raises
        ------
        GeometryTypeError
            If the geometry is not a ``LineString`` or ``Point``.
        CRSError
            If the CRS is set and is not EPSG:4326 longitude/latitude.
        InvalidGeometryError
            If the geometry carries Z/M, or a coordinate is outside the
            longitude/latitude domain. Flatten explicitly with ``force_2d()``.
        GeometryError
            If ``precision`` is out of range.

        See Also
        --------
        from_polyline : Decode Google polyline text into geometries.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(-120.2, 38.5), (-120.95, 40.7)]).to_polyline()
        '_p~iF~ps|U_ulLnnqC'
        """
    @property
    def is_convex(self) -> bool:
        """Whether the polygon is convex.

        Every shell turn has one orientation — collinear edges allowed —
        and there are no holes; the empty polygon is convex. Non-polygon
        geometries return ``False``.

        Returns
        -------
        bool
        """
    def interpolate_m(
        self,
        start_m: float,
        end_m: float,
        *,
        overwrite: bool = False,
        unit: DistanceUnit | None = None,
    ) -> Self:
        """Interpolate an M ordinate along the line's arc length (CRS-aware). M runs
        from ``start_m`` at the start to ``end_m`` at the end, continuously across
        multipart linework (the PostGIS ``ST_AddMeasure`` shape). The stationing
        follows the CRS like length — geodesic on a geographic CRS, planar
        otherwise (coordinates are never moved). Z is preserved; existing M requires
        ``overwrite=True``.

        Parameters
        ----------
        start_m, end_m : float
            The measure range (finite, ``end_m >= start_m``).
        overwrite : bool, default False
            Replace existing M ordinates instead of raising.

        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        Geometry
            The geometry with interpolated M values (same kind as the input).


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework is empty, or carries M without ``overwrite``.
        GeometryError
            If the measure range is invalid.

        See Also
        --------
        line_interpolate : Point at a distance or M location along the line.
        line_substring : Extract a contiguous portion of the line.
        line_locate : Project a geometry onto the line.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.interpolate_m(0.0, 100.0).to_wkt()
        'LINESTRING M (0 0 0, 10 0 100)'
        """

    def polylabel(
        self,
        *,
        tolerance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> Point:
        """Pole of inaccessibility: the most distant interior point. Center of the
        largest inscribed circle — the best label anchor — measured for the CRS
        exactly like maximum_inscribed_circle (whose center this is).

        See Also
        --------
        maximum_inscribed_circle : Filled disk whose center this is.
        centroid : Area/length-weighted center (may fall outside).
        point_on_surface : A guaranteed-interior representative point.

        Parameters
        ----------
        tolerance : float, optional
            Precision of the search, interpreted for the CRS (see ``unit``).
            Omitted selects a scale-aware tolerance from the geometry's extent.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        Point
            The pole of inaccessibility.


        Raises
        ------
        InvalidGeometryError
            If the pole of inaccessibility cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).polylabel().to_wkt()
        'POINT (1 1)'
        """

    def minimum_clearance_line(self, *, unit: DistanceUnit | None = None) -> LineString:
        """Two-point line realizing `minimum_clearance`. The metric matches
        minimum_clearance: on a geographic CRS, the witness is selected in the
        geometry's best local projection and returned in source coordinates, so it is a
        local-projection approximation rather than an exact ellipsoidal clearance
        search.
        ``LINESTRING EMPTY`` when the clearance is infinite (fewer than two distinct
        vertices).

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        LineString
            The two-point line realizing the minimum clearance.

        See Also
        --------
        minimum_clearance : The clearance distance itself.

        Examples
        --------
        >>> import gometry as gm
        >>> (gm.box(0, 0, 3, 2).minimum_clearance_line()).to_wkt()
        'LINESTRING (0 0, 0 2)'
        """

    def to_geojson(self, *, include_z: bool = True, drop_epoch: bool = False) -> str:
        """Serialize to `GeoJSON` text. `GeoJSON` is WGS84 by specification (RFC 7946):
        CRS-tagged input must be ``EPSG:4326`` (or ``OGC:CRS84``) — reproject with
        ``to_crs(4326)`` first. CRS-free input is serialized as-is.

        Parameters
        ----------
        include_z : bool, default True
            Write Z ordinates when present.

            `GeoJSON` cannot represent M; remove it explicitly with ``set_m(None)``.

        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which GeoJSON cannot encode.

        Returns
        -------
        str
            The `GeoJSON` geometry string.

        Raises
        ------
        CRSError
            If the input carries a CRS other than WGS84.
        InvalidGeometryError
            If input carries M ordinates.
        GeometryError
            If input carries a coordinate epoch and ``drop_epoch`` is false.

        See Also
        --------
        from_geojson : Parse `GeoJSON` back into a geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 2).to_geojson()
        '{"type":"Point","coordinates":[1.0,2.0]}'
        """

    def boundary(
        self,
    ) -> MultiPoint | LineString | MultiLineString | GeometryCollection:
        """Return the topological boundary of the geometry.

        Returns
        -------
        MultiPoint, LineString, MultiLineString, or GeometryCollection
            The topological boundary, one dimension below the input.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).boundary().to_wkt()
        'LINESTRING (0 0, 2 0, 2 2, 0 2, 0 0)'
        """

    @overload
    def triangulate(
        self,
        *,
        method: Literal['earcut'],
        min_angle: None = None,
        max_area: None = None,
    ) -> GeometryArray[Polygon]: ...
    @overload
    def triangulate(
        self,
        *,
        method: Literal['delaunay'],
        min_angle: None = None,
        max_area: None = None,
    ) -> GeometryArray[Polygon]: ...
    @overload
    def triangulate(
        self,
        *,
        method: Literal['constrained'],
        min_angle: float | None = None,
        max_area: float | None = None,
    ) -> GeometryArray[Polygon]:
        """Triangulate geometry with an explicit algorithm.

        Parameters
        ----------
        method : {'earcut', 'delaunay', 'constrained'}
            Required algorithm choice: ``earcut`` triangulates polygon interiors,
            ``delaunay`` triangulates input vertices, and ``constrained`` preserves
            polygon boundaries.
        min_angle : float, optional
            Minimum triangle angle in degrees; valid only with
            ``method='constrained'``.

        max_area : float, optional
            Maximum triangle area in square coordinate units; valid only with
            ``method='constrained'``. Setting either option enables refinement;
            without either option, triangle corners preserve input ordinates.
            Refinement inserts Steiner vertices and therefore returns XY.

        Returns
        -------
        GeometryArray[Polygon]
            The generated triangles.

        Raises
        ------
        GeometryError
            If method-specific options are used with the wrong algorithm.
        InvalidGeometryError
            If the triangulation cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2), (1, 0.5)])
        >>> tris = sites.triangulate(method='delaunay')
        >>> (len(tris), tris.geometry_type[0])
        (3, 'Polygon')
        """

    def smooth(
        self,
        *,
        iterations: int = 2,
        method: SmoothMethod = 'chaikin',
        keep_endpoints: bool = True,
    ) -> Self:
        """Smooth line and polygon boundary linework (planar).
        Two algorithms, selected by ``method``:

        - ``'chaikin'`` (the default) applies corner-cutting quadratic B-spline
          refinement. Each iteration replaces every edge with points at
          one-quarter and three-quarters along it (~doubling vertices). Open
          lines honor ``keep_endpoints``; polygon rings are always treated as
          cyclic.
        - ``'catmull_rom'`` subdivides each segment with a centripetal Catmull-Rom
          cubic that passes through every original vertex. ``iterations`` sets the
          per-segment sample count as ``2**iterations`` (``0`` is identity).
          It always interpolates endpoints, so ``keep_endpoints=False`` is rejected.

        Parameters
        ----------
        iterations : int
            Smoothing strength; ``0`` returns the input unchanged.
        method : {'chaikin', 'catmull_rom'}, default 'chaikin'
            ``'chaikin'`` corner-cuts every edge; ``'catmull_rom'`` subdivides
            with a centripetal interpolating cubic.

        keep_endpoints : bool, default True
            For open lines under ``'chaikin'``, hold the first and last vertices
            fixed. Rings are cyclic; Catmull-Rom requires ``True``.

        Returns
        -------
        Geometry
            The smoothed geometry (same kind as the input).


        Raises
        ------
        GeometryError
            If ``iterations`` is negative, or if it would smooth the geometry to
            more coordinates than the output budget allows (a tiny input with a
            very large ``iterations``).

        Examples
        --------
        >>> import gometry as gm
        >>> square = gm.box(0, 0, 1, 1)
        >>> square.smooth(iterations=1).area < square.area
        True
        """

    def line_merge(self) -> LineString | MultiLineString:
        """Merge connected LineString parts into longer LineStrings.

        Returns
        -------
        LineString or MultiLineString
            The merged linework.


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.

        Examples
        --------
        >>> import gometry as gm
        >>> a, b = [(0, 0), (1, 1)], [(1, 1), (2, 2)]
        >>> (gm.MultiLineString([a, b]).line_merge()).to_wkt()
        'LINESTRING (0 0, 1 1, 2 2)'
        """

    def spatial_key(
        self,
        *,
        curve: SpatialCurve = 'hilbert',
        level: int = 16,
        bounds: Iterable[float] | None = None,
    ) -> int | None:
        """Space-filling-curve key of this geometry's bbox center.
        Discretizes the center into a ``2^level x 2^level`` grid over ``bounds``
        and returns its distance along the selected curve.

        Parameters
        ----------
        curve : {'hilbert', 'morton'}, default hilbert
            ``hilbert`` prioritizes locality; ``morton`` uses Z-order.

        level : int, default 16
            Grid order (``1`` to ``32``); 16 matches GeoPandas/DuckDB.

        bounds : tuple of float, optional
            The frame ``(minx, miny, maxx, maxy)``; this geometry's own bounds
            when omitted. Keys compare across geometries only against a *shared*
            frame — pass the same ``bounds`` when keying separate geometries.

        Returns
        -------
        int or None
            Spatial curve key, or ``None`` for an empty geometry — the same
            contract as ``bounds`` and the other extent accessors.

        Raises
        ------
        GeometryError
            If ``level`` or ``bounds`` is invalid (a bad parameter is an error
            whatever the geometry).

        Examples
        --------
        >>> import gometry as gm
        >>> bounds = (0, 0, 10, 10)
        >>> gm.Point(0, 0).spatial_key(bounds=bounds) != gm.Point(10, 10).spatial_key(bounds=bounds)
        True
        """

    def minimum_rotated_rectangle(self) -> Point | LineString | Polygon:
        """Minimum-area rotated bounding rectangle, returned in XY.

        Returns
        -------
        Point, LineString, or Polygon
            The minimum rotated rectangle (degenerate inputs reduce dimension).


        Raises
        ------
        InvalidGeometryError
            If the rotated rectangle cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> rect = gm.box(0, 0, 2, 2).minimum_rotated_rectangle()
        >>> gm.equals(rect, gm.box(0, 0, 2, 2))
        True
        """

    def clip_by_rect(
        self,
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> Geometry:
        """Clip a geometry to a rectangle ``(minx, miny, maxx, maxy)``.

        Source ordinates are carried where meaningful; synthesized clip vertices use
        the operation's natural XY result.

        Parameters
        ----------
        minx, miny, maxx, maxy : float
            The clip rectangle bounds.

        Returns
        -------
        Geometry
            The clipped geometry (kind may change; empty when fully outside).


        Raises
        ------
        GeometryError
            If the rectangle bounds are non-finite or unordered.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (10, 0)]).clip_by_rect(2, -1, 5, 1).to_wkt()
        'LINESTRING (2 0, 5 0)'
        """

    def swap_xy(self) -> Self:
        """Swap the X and Y ordinate of every coordinate (Z/M untouched). The axis-
        order repair for data delivered latitude-first: gometry is always ``(x, y)``
        = ``(lon, lat)``, so latitude-ordered input swaps once on ingest.

        Returns
        -------
        Geometry
            The geometry with X and Y swapped (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> latitude_first = gm.Point(52.5, 13.4)
        >>> latitude_first.swap_xy().to_wkt()
        'POINT (13.4 52.5)'
        """

    @overload
    def line_substring(
        self,
        start: float,
        end: float,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> LineString | Point: ...
    @overload
    def line_substring(
        self,
        start: float,
        end: float,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> LineString | Point:
        """Return the portion of linework from ``start`` through ``end``.

        Parameters
        ----------
        start, end : float or sequence of float
            Ordered locations on the selected basis. Distance values follow the CRS;
            M values are stored route measures. A scalar applies to every array row.

        basis : {'distance', 'm'}, default 'distance'
            Use CRS-aware distance, or the line's monotonic M ordinate.

        normalized : bool, default False
            Interpret distance-basis positions as fractions in [0, 1]. Invalid with
            ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            Distance-basis unit override. Omitted follows the CRS; invalid with
            ``basis='m'``.

        Returns
        -------
        LineString or Point
            The substring (a ``Point`` when ``start == end``).


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework is empty, or M values are missing or non-monotonic.
        GeometryError
            If locations are non-finite or out of order, or a distance-only option is
            used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a location along the line.
        line_locate : Project a geometry onto the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_substring(2, 6).to_wkt()
        'LINESTRING (2 0, 6 0)'
        """

    def buffer(
        self,
        distance: float,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> Polygon | MultiPolygon:
        """Buffer a geometry by ``distance``, returning the offset region (measured for
        the CRS).

        Parameters
        ----------
        distance : float
            Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
            meters on a geographic CRS, native units on a projected one, coordinate
            units otherwise.
        cap_style : {'round', 'flat', 'square'}, default 'round'
            End-cap shape for open ends.

        join_style : {'round', 'miter', 'bevel'}, default 'round'
            Corner join shape.

        quadrant_segments : int, default 8
            Segments used to approximate a quarter circle.

        miter_limit : float, default 5.0
            With ``join_style='miter'``: how far a mitered corner may reach, in
            multiples of ``distance``; sharper corners are clipped flat at that
            reach. Must be positive and finite.

        side : {'both', 'left', 'right'}, default 'both'
            Which side(s) of lineal input to buffer. ``'left'``/``'right'`` build
            the one-sided strip between the line and its offset curve (flat ends,
            miter joins — ``offset_curve(join_style='miter')`` closed into a
            polygon); the style parameters apply to ``'both'`` only, and sided
            buffers take a non-negative distance.

            Buffer boundaries are synthesized and therefore returned in XY; Z/M are
            not fabricated.

        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        Polygon or MultiPolygon
            The offset region.


        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite.
        GeometryError
            If ``distance``/``quadrant_segments``/style parameters are invalid, or
            ``unit=meters`` is requested for a CRS-free geometry.
        GeometryTypeError
            If ``side`` is ``'left'``/``'right'`` and the geometry is not lineal.

        See Also
        --------
        offset_curve : One-sided raw parallel curve of a line.

        Examples
        --------
        >>> import gometry as gm
        >>> disc = gm.Point(0, 0).buffer(10)
        >>> round(disc.area)  # ~ pi * 10^2
        312
        """

    def unique_points(self) -> MultiPoint:
        """Distinct vertices in first-occurrence order. Vertices compare by exact
        structural identity (every active ordinate by bit pattern, the
        ``equals_identical`` notion), so XYZ points that differ only in Z stay
        distinct.

        Returns
        -------
        MultiPoint
            The distinct vertices (``MULTIPOINT EMPTY`` for an empty geometry).

        See Also
        --------
        remove_repeated_points : Collapse consecutive duplicate vertices in place, keeping the geometry kind.

        Examples
        --------
        >>> import gometry as gm
        >>> loop = gm.LineString([(0, 0), (1, 1), (0, 0), (2, 2)])
        >>> loop.unique_points().to_wkt()
        'MULTIPOINT ((0 0), (1 1), (2 2))'
        """

    def simplify(
        self,
        tolerance: float,
        *,
        method: SimplifyMethod = 'vw',
        preserve_topology: bool = True,
    ) -> Self:
        """Simplify a geometry, dropping vertices below a tolerance (planar). Two
        algorithms, selected by ``method``, both reading ``tolerance`` on the
        same distance scale:

        - ``'vw'`` (Visvalingam-Whyatt, the default) removes the least visually
          significant vertices first — the smallest effective triangle spanned
          with its two neighbors — for a smoother, more natural cartographic
          result. The effective-area threshold is ``tolerance**2 / 2``.
        - ``'dp'`` (Douglas-Peucker) removes vertices whose perpendicular distance
          from the retained chord is within ``tolerance``.

        Parameters
        ----------
        tolerance : float
            Distance scale of removable detail, in coordinate units.
        method : {'vw', 'dp'}, default 'vw'
            ``'vw'`` is area-based (Visvalingam-Whyatt); ``'dp'`` is distance-
            based (Douglas-Peucker).

        preserve_topology : bool, default True
            Guarantee the output keeps the input's topology: a polygon stays valid
            and non-collapsed, a simple line stays simple. The raw algorithm runs
            first (the fast path); a guarded pass only kicks in when it broke
            something. ``False`` is the raw algorithm.

        Returns
        -------
        Geometry
            The simplified geometry (same kind as the input).


        Raises
        ------
        GeometryError
            If ``tolerance`` is negative or non-finite.

        See Also
        --------
        coverage_simplify : Topology-preserving simplification across a polygonal coverage.

        Examples
        --------
        >>> import gometry as gm
        >>> wiggly = gm.LineString([(0, 0), (1, 0.1), (2, -0.1), (3, 0)])
        >>> (wiggly.simplify(1.0)).to_wkt()
        'LINESTRING (0 0, 3 0)'
        """

    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Geometry:
        """Repair invalid geometry, returning corrected result(s) (OGC). Already-valid
        input is returned unchanged at validation cost. Geographic antimeridian
        crossings are normalized before validity is decided, so a valid seam-crossing
        geometry is never destructively repaired; an invalid crossing repairs from its
        seam-split form. Projected and CRS-free geometry remains planar. Z/M ordinates
        are carried through the rebuild.

        Parameters
        ----------
        method : {'linework', 'structure'}, default linework
            Repair strategy: ``linework`` nodes all boundary linework and
            reassembles regions by even-odd parity, keeping every input edge;
            ``structure`` rebuilds each ring's enclosed area and recombines them
            as shells minus holes, discarding collapsed components.

            Z/M are carried at vertices traceable to the input; a rebuild that needs
            unsourceable vertices returns the mathematically natural XY result.

        Returns
        -------
        Geometry
            A valid geometry.


        Raises
        ------
        InvalidGeometryError
            If the geometry cannot be repaired.

        See Also
        --------
        validate : Structured validity report.
        is_valid : Boolean-only test.

        Examples
        --------
        >>> import gometry as gm
        >>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
        >>> fixed = bowtie.repair()
        >>> (fixed.is_valid, fixed.geometry_type)
        (True, 'MultiPolygon')
        """

    @overload
    def line_locate(
        self,
        geom: Geometry,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> float: ...
    @overload
    def line_locate(
        self,
        geom: GeometryArray,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def line_locate(
        self,
        geom: Geometry,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> float: ...
    @overload
    def line_locate(
        self,
        geom: GeometryArray,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def line_locate(
        self,
        geom: Geometry | GeometryArray,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> float | npt.NDArray[np.float64]:
        """Locate the position on linework nearest ``geom``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            A geometry to project, or one geometry per line row.

        basis : {'distance', 'm'}, default 'distance'
            Return a CRS-aware distance, or the line's monotonic M ordinate.

        normalized : bool, default False
            Return a distance-basis fraction in [0, 1]. Invalid with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            Distance-basis unit override. Omitted follows the CRS; invalid with
            ``basis='m'``.

        Returns
        -------
        float or numpy.ndarray
            One location, or a column when ``geom`` is an array.


        Raises
        ------
        CRSError
            If the CRS cannot provide an unambiguous distance metric.
        CRSMismatchError
            If operands' CRS or coordinate-epoch metadata differ.
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If linework is empty, or M values are missing or non-monotonic.
        GeometryError
            If a distance-only option is used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a location along the line (inverse of locate).
        line_substring : Extract a contiguous portion of the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_locate(gm.Point(4, 3))
        4.0
        """

    def force_2d(self) -> Self:
        """Make each geometry planar, dropping any Z and M ordinates. Returns pure XY
        of the same geometry type and CRS. Already-2D input is returned unchanged.
        The one obvious way to flatten.

        Returns
        -------
        Geometry
            The XY-only geometry (same kind as the input).

        See Also
        --------
        force_3d : Add a Z ordinate, filling missing vertices.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.from_wkt('POINT Z (1 2 3)').force_2d().to_wkt()
        'POINT (1 2)'
        """

    def self_intersections(self) -> GeometryArray[Point]:
        """Return points where the geometry coincides with itself. Reports proper linework
        self-crossings, non-adjacent touches, the endpoints of collinear overlaps
        (spikes and backtracks), contact between distinct parts, and duplicate point
        coordinates; legal adjacent shared vertices, ring closures, and removable
        repeated consecutive vertices are not nodes. For point/lineal input the
        result is non-empty exactly when is_simple is ``False``; areal input
        diagnoses its rings' linework, and collections are diagnosed recursively.
        Geographic antimeridian crossings use normalized topology; projected and
        CRS-free geometry remains planar. Points are XY only.

        Returns
        -------
        GeometryArray
            The distinct self-intersection points.

        Examples
        --------
        >>> import gometry as gm
        >>> cross = gm.from_wkt('LINESTRING (0 0, 1 1, 1 0, 0 1)')
        >>> cross.self_intersections().to_wkt()
        ['POINT (0.5 0.5)']
        """

    def translate(self, x_offset: float, y_offset: float) -> Self:
        """Translate a geometry by ``(x_offset, y_offset)``.

        Parameters
        ----------
        x_offset, y_offset : float
            Offsets along the X and Y axes.

        Returns
        -------
        Geometry
            The transformed geometry (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 2).translate(10, 20).to_wkt()
        'POINT (11 22)'
        """

    def minimum_clearance(self, *, unit: DistanceUnit | None = None) -> float:
        """Smallest distance by which a vertex could move to invalidate the geometry.
        On a geographic CRS, the witness is selected in the geometry's best local
        projection and then measured geodesically in source coordinates; this is a
        local-projection approximation, not an exact ellipsoidal clearance search.
        Projected CRS and CRS-free geometries measure in the active planar units.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        float
            The minimum clearance distance.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 3, 2).minimum_clearance()
        2.0
        """

    @overload
    def line_interpolate(
        self,
        at: float,
        /,
        *,
        basis: Literal['distance'] = 'distance',
        count: None = None,
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> Point: ...
    @overload
    def line_interpolate(
        self,
        at: FloatColumn,
        /,
        *,
        basis: Literal['distance'] = 'distance',
        count: None = None,
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self,
        at: None = None,
        /,
        *,
        count: int,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self,
        at: float,
        /,
        *,
        count: None = None,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> Point:
        """Interpolate point locations along linework.

        Parameters
        ----------
        at : float or sequence of float, optional
            One location or many explicit distance-basis locations. Under
            ``basis='m'``, pass one stored M value (or one value per array row).


        count : int, optional
            Number of evenly spaced distance-basis samples (``>= 1``). Mutually
            exclusive with ``at`` and unavailable with ``basis='m'``.

        basis : {'distance', 'm'}, default 'distance'
            Use CRS-aware distance, or the line's monotonic M ordinate.

        normalized : bool, default False
            Interpret distance-basis ``at`` values as fractions in [0, 1]. Invalid
            with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            Distance-basis unit override. Omitted follows the CRS; invalid with
            ``basis='m'``.

        Returns
        -------
        Point or GeometryArray[Point]
            One point for scalar ``at``; a point column for many ``at`` values or
            ``count`` samples.


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework is empty, or M values are missing or non-monotonic.
        GeometryError
            If input forms conflict, a value is non-finite, or a distance-only option
            is used with ``basis='m'``.

        See Also
        --------
        line_locate : Project a geometry onto the line (inverse of interpolate).
        line_substring : Extract a contiguous portion of the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_interpolate(4).to_wkt()
        'POINT (4 0)'
        """

    def normalize(self) -> Self:
        """Return a geometry in canonical (normalized) form. The canonical form is the
        lexicographically smallest equivalent presentation: parts sort ascending,
        lines take their smaller direction (closed lines the smallest rotation), and
        polygon rings lead with their minimum vertex under RFC 7946 winding
        (exterior counter-clockwise, holes clockwise).

        Returns
        -------
        Geometry
            The canonical form (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> messy = gm.from_wkt('MULTIPOINT ((1 1), (0 0))')
        >>> messy.normalize().to_wkt()
        'MULTIPOINT ((0 0), (1 1))'
        """

    def maximum_inscribed_circle(
        self,
        *,
        tolerance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> Point | Polygon:
        """Largest circle inscribed in a polygonal geometry, as a filled disk.
        CRS-aware via local projection (approximate). Centered at the pole of
        inaccessibility (see polylabel), with radius reaching the nearest boundary
        point. Mirrors minimum_bounding_circle; the radius alone is
        maximum_inscribed_radius.

        Parameters
        ----------
        tolerance : float, optional
            Precision of the center search (pole-of-inaccessibility refinement).
            Omitted selects a scale-aware tolerance from the geometry's extent.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        Point or Polygon
            The filled inscribed circle. A degenerate (zero-area) polygon returns
            its center `Point`.

        See Also
        --------
        polylabel : Pole of inaccessibility (the circle center alone).
        maximum_inscribed_radius : The radius alone.
        point_on_surface : A guaranteed-interior representative point.
        centroid : Area/length-weighted center (may fall outside).

        Raises
        ------
        InvalidGeometryError
            If the inscribed circle cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> disk = gm.box(0, 0, 2, 2).maximum_inscribed_circle()
        >>> (disk.geometry_type, round(disk.area, 2))
        ('Polygon', 3.14)
        """
    def maximum_inscribed_radius(
        self, *, tolerance: float | None = None, unit: DistanceUnit | None = None
    ) -> float:
        """Radius of the largest inscribed circle — the distance from the pole of
        inaccessibility (see polylabel) to the nearest boundary point. The numeric
        twin of minimum_bounding_radius; the circle itself is
        maximum_inscribed_circle.

        Parameters
        ----------
        tolerance : float, optional
            Precision of the center search (pole-of-inaccessibility refinement).
            Omitted selects a scale-aware tolerance from the geometry's extent.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        float
            The inscribed radius in the requested/CRS metric units (see ``unit``); ``NaN`` for empty input.


        See Also
        --------
        maximum_inscribed_circle : The filled inscribed circle (center and radius).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).maximum_inscribed_radius()
        1.0
        """

    def minimum_bounding_circle(
        self,
        *,
        unit: DistanceUnit | None = None,
    ) -> Point | Polygon:
        """Smallest circle enclosing the geometry, as a polygon. The standard shape:
        the enclosing circle as a round 64-gon about the exact Welzl center and
        radius. A single distinct vertex returns itself; empty input returns
        ``POLYGON EMPTY``. CRS-aware via local projection (approximate) on a
        geographic CRS; projected CRS distances default to the CRS's native unit
        and scale through the CRS linear unit only with ``unit='meters'``.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        Polygon
            The smallest enclosing circle.


        Raises
        ------
        InvalidGeometryError
            If the bounding circle cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> pts = gm.MultiPoint([(0, 0), (4, 0)])
        >>> (pts.minimum_bounding_circle().geometry_type, pts.minimum_bounding_radius())
        ('Polygon', 2.0)
        """
    def minimum_bounding_radius(self, *, unit: DistanceUnit | None = None) -> float:
        """Radius of the smallest circle enclosing the geometry. This is the numeric
        twin of minimum_bounding_circle: computed by the same Welzl center/support
        kernel without materializing the polygon. Empty input yields ``NaN``; a single
        distinct vertex yields ``0``. CRS-aware via local projection/ellipsoid support
        measurement (approximate for geographic point sets with three or more
        distinct vertices); two-point geographic inputs are the exact geodesic
        half-distance.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        float
            The enclosing circle radius.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.MultiPoint([(0, 0), (4, 0)]).minimum_bounding_radius()
        2.0
        """

    def concave_hull(
        self,
        *,
        concavity: float = 2.0,
        length_threshold: float = 0.0,
        unit: DistanceUnit | None = None,
    ) -> Point | LineString | Polygon:
        """Compute the concave hull of the geometry. CRS-aware via local projection
        (approximate) and does NOT auto-split antimeridian-crossing geographic input;
        call ``split_antimeridian`` first. Uses gometry's chi-shape kernel: Delaunay
        boundary triangles are peeled from longest edge to shortest, with output
        independent of input point order. Hull vertices are input vertices, so X/Y/Z/M
        ordinates are preserved exactly.

        Parameters
        ----------
        concavity : float, default 2.0
            Higher values are looser: fewer edges are peeled and area grows toward
            the convex hull. ``0`` disables the distance guard.
        length_threshold : float, default 0.0
            Boundary edges at or below this length are kept, so higher values also
            make the hull looser; interpreted for the CRS (see ``unit``). On a
            geographic CRS the threshold is evaluated in a local projection, while the
            output vertices are emitted from the original input coordinates.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        Point, LineString, or Polygon
            The concave hull; degenerate inputs reduce dimension.


        Raises
        ------
        GeometryError
            If parameters are non-finite.

        See Also
        --------
        convex_hull : Smallest convex set containing the geometry (planar).

        Examples
        --------
        >>> import gometry as gm
        >>> mp = gm.MultiPoint([(0, 0), (2, 0), (2, 2), (0, 2), (1, 0.2)])
        >>> hull = mp.concave_hull(concavity=1.0)
        >>> (hull.geometry_type, round(hull.area, 2))
        ('Polygon', 3.0)
        """

    def point_on_surface(self) -> Point:
        """Representative point guaranteed to lie on the geometry. Geographic (lon/lat)
        input crossing the antimeridian is auto-split-normalized; no manual
        ``split_antimeridian`` is required. Always inside (or on) the geometry,
        unlike centroid. The representative point is computed in XY and does not
        imply a source Z/M.

        See Also
        --------
        centroid : Area/length-weighted center (may fall outside).
        polylabel : Pole of inaccessibility (best label anchor).


        Returns
        -------
        Point
            A point guaranteed to lie on the geometry.


        Raises
        ------
        InvalidGeometryError
            If a finite representative point cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> square = gm.box(0, 0, 2, 2)
        >>> gm.within(square.point_on_surface(), square)
        True
        """

    def subdivide(self, *, max_vertices: int = 256) -> GeometryArray[Geometry]:
        """Split geometry into parts of bounded complexity. Recursively halves each
        geometry's bounds across the longer axis and clips until every part has at
        most ``max_vertices`` coordinates (the PostGIS ``ST_Subdivide`` shape).
        Parts cover the input exactly. Source ordinates are carried where meaningful;
        synthesized clip vertices use the operation's natural XY result.

        Parameters
        ----------
        max_vertices : int, default 256
            Maximum coordinates per part (at least ``8``).

        Returns
        -------
        GeometryArray
            The parts of the input geometry, in input order.


        Raises
        ------
        GeometryError
            If ``max_vertices`` is below ``8``.

        Examples
        --------
        >>> import gometry as gm
        >>> parts = gm.LineString([(i, 0) for i in range(20)]).subdivide(max_vertices=8)
        >>> len(parts)
        4
        """

    def scale(
        self,
        x_factor: float,
        y_factor: float | None = None,
        *,
        origin: Origin = 'centroid',
    ) -> Self:
        """Scale a geometry about an origin.

        Parameters
        ----------
        x_factor, y_factor : float
            Scale factors along the X and Y axes; ``y_factor`` defaults to ``x_factor``.

        origin : str or sequence of float, optional
            Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
            y)`` point.

        Returns
        -------
        Geometry
            The transformed geometry (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 1, 1).scale(2, 2, origin=(0, 0)).bounds
        (0.0, 0.0, 2.0, 2.0)
        """

    def extremes(self) -> Extremes | None:
        """Return the west, south, east, and north extreme vertices of the
        geometry (numeric X/Y; ties keep the first vertex in storage order).

        Returns
        -------
        Extremes or None
            The ``(west, south, east, north)`` named tuple, or ``None`` for an empty
            geometry — the same contract as ``bounds`` and the other extent
            accessors.


        Raises
        ------
        InvalidGeometryError
            If the geometry is empty.

        Examples
        --------
        >>> import gometry as gm
        >>> extremes = gm.box(0, 0, 2, 4).extremes()
        >>> assert extremes is not None  # None only for an empty geometry
        >>> (extremes.west.to_wkt(), extremes.north.to_wkt())
        ('POINT (0 0)', 'POINT (2 4)')
        """

    @overload
    def segmentize(
        self,
        max_length: float,
        /,
        *,
        fraction: None = None,
        unit: DistanceUnit | None = None,
    ) -> Self: ...
    @overload
    def segmentize(
        self, max_length: None = None, /, *, fraction: float, unit: None = None
    ) -> Self:
        """Densify linework by inserting vertices so no segment exceeds
        ``max_length`` (or a fraction of its length).

        ``max_length`` is a real-world distance measured for the CRS, exactly like
        ``length``: a geographic CRS subdivides along the ellipsoid in meters, a
        projected CRS uses its native linear units, and a CRS-free geometry uses
        coordinate units. Every original vertex survives unchanged — this operation
        only inserts.

        Parameters
        ----------
        max_length : float, optional
            Maximum segment length in coordinate units (positive). Pass this
            positional argument or use ``fraction``, but not both.

        fraction : float, optional
            Fraction in ``(0, 1]`` of each source segment. Keyword-only; use when
            the subdivision is naturally relative rather than expressed in units.

        unit : {'planar', 'meters'} or None, default None
            ``None`` follows the CRS. ``'planar'`` forces raw coordinate units
            (degrees-as-Cartesian on a geographic CRS — only for deliberate
            coordinate-space math); ``'meters'`` forces the CRS metric and raises
            without a CRS. Cannot be combined with ``fraction``, which is already
            relative to each segment.

        Returns
        -------
        Geometry
            The segmentized geometry (same kind as the input).


        Raises
        ------
        CRSError
            If ``unit='meters'`` is requested and the CRS lacks linear axis units.
        GeometryError
            If neither or both constraints are supplied, ``max_length`` is not a
            positive finite number, ``fraction`` is outside ``(0, 1]``, ``unit`` is
            combined with ``fraction``, or ``unit='meters'`` is requested for a
            CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (4, 0)]).segmentize(2).to_wkt()
        'LINESTRING (0 0, 2 0, 4 0)'
        >>> # On a geographic CRS the bound is meters along the ellipsoid.
        >>> line = gm.LineString([(0, 0), (1, 0)], crs=4326)
        >>> len(list(line.segmentize(20_000).coords))
        7
        """

    def voronoi_polygons(
        self,
        *,
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> GeometryArray[Polygon]:
        """Voronoi diagram polygons of the geometry's vertices. Operates in planar
        lon/lat space and does NOT auto-split antimeridian-crossing geographic
        input; call ``split_antimeridian`` first.

        Parameters
        ----------
        tolerance : float, default 0.0
            Tolerance in coordinate units (non-negative).
        clip : {'padded', 'envelope'} or Polygon, default 'padded'
            How to bound the unbounded outer cells: a padded box, the input
            envelope, or a `Polygon` to clip the diagram to.
            Diagram vertices are synthesized and returned in XY.

        Returns
        -------
        GeometryArray
            The Voronoi cells of the input geometry.


        Raises
        ------
        InvalidGeometryError
            If the Voronoi diagram cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
        >>> len(sites.voronoi_polygons())
        3
        """

    def split_antimeridian(self) -> Geometry:
        """Split at the antimeridian. Parts that cross come back as multiple parts
        whose edges follow the seam — each side keeping its own seam sign — so the
        result renders and computes correctly in lon/lat tools (the JOSS
        ``antimeridian`` algorithm). Crossings split at the great-circle latitude; a
        ring running off the seam closes over its pole automatically. Geometries
        that do not cross are returned unchanged. A split ``LineString`` becomes a
        ``MultiLineString`` and a split ``Polygon`` a ``MultiPolygon``, like
        repair. Seam vertices interpolate Z/M.

        Returns
        -------
        Geometry
            The seam-split geometry.


        Raises
        ------
        CRSError
            If the CRS is projected (CRS-free lon/lat and geographic CRS are
            accepted), or a coordinate is outside the longitude/latitude domain.
        InvalidGeometryError
            If stitching fails, or pole closure would have to invent Z/M
            ordinates.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(170, 0), (-170, 0)], crs=4326)
        >>> split = line.split_antimeridian()
        >>> (split.geometry_type, len(split.parts))
        ('MultiLineString', 2)
        """

    def offset_curve(
        self,
        distance: float,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> LineString | MultiLineString:
        """Compute a line offset to one side of a linestring by ``distance`` (measured
        for the CRS).

        Parameters
        ----------
        distance : float
            Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
            CRS, native units on a projected one, coordinate units otherwise. The result is the RAW parallel
            curve: where the input folds back within ``distance`` the curve can
            self-intersect (GEOS trims such curls away); ``buffer(side=...)``
            gives the trimmed area instead.
        join_style : {'round', 'miter', 'bevel'}, default 'round'
            Corner treatment at outside turns: ``'round'`` inscribes fillet arcs,
            ``'miter'`` extends the carriers to their crossing (clipped at
            ``miter_limit``), ``'bevel'`` connects the offsets directly.

        quadrant_segments : int, default 8
            Segments per quarter circle of every round join.

        miter_limit : float, default 5.0
            With ``join_style='miter'``: how far a mitered corner may reach, in
            multiples of ``distance``, before it is clipped flat.

        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        LineString or MultiLineString
            The raw offset curve.


        Raises
        ------
        GeometryError
            If ``distance``/``quadrant_segments``/style parameters are invalid.

        See Also
        --------
        buffer : Offset region (optionally one-sided via ``side``).

        Examples
        --------
        >>> import gometry as gm
        >>> path = gm.LineString([(0, 0), (4, 0)])
        >>> (path.offset_curve(1)).to_wkt()
        'LINESTRING (0 1, 4 1)'
        """

    def reverse(self) -> Self:
        """Reverse the vertex order of a geometry.

        Returns
        -------
        Geometry
            The geometry with vertex order reversed (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (1, 1), (2, 2)])
        >>> line.reverse().to_wkt()
        'LINESTRING (2 2, 1 1, 0 0)'
        """

    def envelope(self) -> Point | LineString | Polygon:
        """Axis-aligned bounding-box polygon of the geometry, returned in XY.

        Returns
        -------
        Point, LineString, or Polygon
            The axis-aligned bounding shape (degenerate inputs reduce dimension).


        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (3, 1)]).envelope().to_wkt()
        'POLYGON ((0 0, 3 0, 3 1, 0 1, 0 0))'
        """

    def sample_points(self, count: int, *, seed: int) -> GeometryArray[Point]:
        """Random points on the geometry. The sample space is the geometry's highest
        dimension: uniform over area for areal input, along length for lineal input,
        and across the member points of a point set — falling back a dimension when
        the higher one is degenerate (a zero-area polygon samples its boundary),
        like centroid. Deterministic: the same input and ``seed`` always produce
        the same points (an explicit seed is required — no hidden global RNG). Array
        rows draw distinct deterministic streams derived from ``seed`` and the row
        index, and a scalar geometry IS row 0 — so ``arr.sample_points(n, seed=s)[0]``
        and ``arr[0].sample_points(n, seed=s)`` agree. An empty row yields an empty
        group rather than failing the batch; an empty SCALAR raises. Sampled points are invented interior points, so they cannot carry the
        source geometry's Z/M and are returned in XY.

        Parameters
        ----------
        count : int
            Number of points to draw (``>= 0``).
        seed : int
            Seed for the deterministic sample stream.


        Returns
        -------
        GeometryArray
            The ``count`` sampled points.


        Raises
        ------
        InvalidGeometryError
            If ``count > 0`` and a geometry is empty.
        GeometryError
            If ``count`` or ``seed`` is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> square = gm.box(0, 0, 10, 10)
        >>> pts = square.sample_points(5, seed=42)
        >>> (len(pts), all(p is not None and gm.within(p, square) for p in pts))
        (5, True)
        """

    def quantize(self, precision: int) -> Self:
        """Round coordinates to a fixed number of decimal places.

        Parameters
        ----------
        precision : int
            Decimal places to keep (``0``-``15``).

        Returns
        -------
        Geometry
            The rounded geometry (same kind as the input; vertices preserved).


        Raises
        ------
        GeometryError
            If ``precision`` is outside ``0``-``15``.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(2.3479218, 48.8589321).quantize(3).to_wkt()
        'POINT (2.348 48.859)'
        """

    def rotate(
        self, angle: float, *, origin: Origin = 'centroid', radians: bool = False
    ) -> Self:
        """Rotate a geometry about an origin.

        Parameters
        ----------
        angle : float
            Rotation angle (degrees by default; radians if ``radians=True``).

        origin : str or sequence of float, optional
            Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
            y)`` point.

        radians : bool, optional
            Interpret ``angle`` in radians instead of degrees.

        Returns
        -------
        Geometry
            The transformed geometry (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 0).rotate(90, origin=(0, 0)).to_wkt(precision=6)
        'POINT (0 1)'
        """

    def affine_transform(self, matrix: _AffineMatrix) -> Self:
        """Apply a 2D affine transform ``(a, b, d, e, xoff, yoff)`` to a geometry.

        Parameters
        ----------
        matrix : sequence of float
            The six affine coefficients (a, b, d, e, xoff, yoff).

        Returns
        -------
        Geometry
            The transformed geometry (same kind as the input).


        Raises
        ------
        GeometryError
            If ``matrix`` is not 6 finite numbers.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).affine_transform([2, 0, 0, 2, 1, 1]).to_wkt()
        'LINESTRING (1 1, 3 3)'
        """

    def centroid(self) -> Point:
        """Area/length-weighted center of the geometry. Geographic (lon/lat) input
        crossing the antimeridian is auto-split-normalized; no manual
        ``split_antimeridian`` is required. The computed center is an XY point.

        Returns
        -------
        Point
            Area/length-weighted center; may lie outside the geometry.

        See Also
        --------
        point_on_surface : A guaranteed-interior representative point.
        polylabel : Pole of inaccessibility (best label anchor).

        Raises
        ------
        InvalidGeometryError
            If a finite centroid cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> (gm.box(0, 0, 2, 4).centroid()).to_wkt()
        'POINT (1 2)'
        """

    def set_z(self, z: float | None) -> Self:
        """Set the Z ordinate at every vertex, or remove it. A numeric ``z`` writes
        that Z at every vertex (replacing any existing Z); ``None`` removes the Z
        ordinate. M passes through unchanged. To fill only the vertices that lack Z,
        use force_3d; to drop to XY, use force_2d.

        Parameters
        ----------
        z : float or None
            Z to assign at every vertex, or ``None`` to remove the Z ordinate.

        Returns
        -------
        Geometry
            The geometry with the new Z ordinate applied (same kind as the input).

        See Also
        --------
        set_m : Set or clear the M ordinate.
        force_3d : Fill only the vertices that lack Z.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.set_z(30.0).to_wkt()[0]
        'POINT Z (1 2 30)'
        """

    def force_3d(self, z: float = 0.0) -> Self:
        """Make each geometry 3D, filling vertices that lack Z with ``z``. Vertices
        that already carry Z keep it; M passes through. The one obvious way to lift
        to 3D.

        Parameters
        ----------
        z : float, default 0.0
            Z to assign where it is missing.

        Returns
        -------
        Geometry
            The geometry with a Z ordinate on every vertex (same kind as the
            input).

        See Also
        --------
        force_2d : Drop Z and M to plain XY.
        set_z : Set the Z ordinate at every vertex (overwriting existing Z).

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.force_3d().to_wkt()[0]
        'POINT Z (1 2 0)'
        """

    def snap_to_grid(
        self,
        size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> Geometry:
        """Snap every coordinate onto a regular grid and clean the result. X/Y move to
        the nearest ``origin + k * size`` grid node; consecutive duplicate vertices
        collapse, and parts that degenerate below their minimum become empty (the
        PostGIS ``ST_SnapToGrid`` shape — output may be non-simple). Z/M ride on
        surviving vertices. quantize is the decimal-rounding, vertex-preserving
        sibling.

        Parameters
        ----------
        size : float or tuple of float
            Grid spacing — one value for a square grid or ``(sx, sy)`` (positive
            finite).

        origin : tuple of float, default (0.0, 0.0)
            A grid node anchoring the lattice.

        repair : bool, default False
            If ``True``, guarantee a valid result: snap, linework-repair, and re-
            snap to a fixpoint. The geometry kind may change (a ``Polygon`` whose
            snapped shell pinches splits into a ``MultiPolygon``). Geographic
            antimeridian crossings use normalized validity; projected and CRS-free
            geometry remains planar.

        Returns
        -------
        Geometry
            The snapped geometry; parts below their minimum degenerate to empty.


        Raises
        ------
        GeometryError
            If ``size`` or ``origin`` is invalid, or the grid is too fine for the
            coordinate magnitude.
        InvalidGeometryError
            If ``repair=True`` and repair would have to invent Z/M ordinates, or
            the snap-repair loop cannot converge.

        Examples
        --------
        >>> import gometry as gm
        >>> jittery = gm.LineString([(0.12, 0.88), (2.49, 1.51)])
        >>> (jittery.snap_to_grid(0.5)).to_wkt()
        'LINESTRING (0 1, 2.5 1.5)'
        """

    def build_area(self) -> Polygon | MultiPolygon:
        """Assemble linework into one areal geometry. Input ordinates are carried
        where vertices can be sourced; otherwise the mathematically planar result is XY.

        Returns
        -------
        Polygon or MultiPolygon
            The maximal areal geometry covered by the input.


        Raises
        ------
        InvalidGeometryError
            If the area cannot be assembled from the input linework.

        Examples
        --------
        >>> import gometry as gm
        >>> edges = [[(0,0),(2,0)],[(2,0),(0,2)],[(0,2),(0,0)]]
        >>> gm.MultiLineString(edges).build_area().to_wkt()
        'POLYGON ((0 0, 2 0, 0 2, 0 0))'
        """

    def orient_polygons(self, *, ccw: bool = True) -> Self:
        """Orient polygon rings to a consistent winding.

        Parameters
        ----------
        ccw : bool, default True
            ``True`` (default): exterior CCW, holes CW; ``False`` flips.

        Returns
        -------
        Geometry
            The geometry with ring winding normalized (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> cw = gm.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
        >>> cw.orient_polygons().exterior.is_ccw
        True
        """

    def polygonize(self) -> GeometryArray[Polygon]:
        """Build polygons from a geometry's own noded linework. Each geometry is
        polygonized in isolation; to reconstruct polygons from a pile of edges pooled
        across many geometries, use free function ``polygonize`` on an iterable of
        values. Input ordinates are carried where possible; unsourceable noding seams
        yield XY.

        Returns
        -------
        GeometryArray
            The polygons built from the input linework.


        Raises
        ------
        InvalidGeometryError
            If polygons cannot be assembled from the noded linework.

        Examples
        --------
        >>> import gometry as gm
        >>> a, b = [(0, 0), (1, 0)], [(1, 0), (1, 1)]
        >>> edges = gm.MultiLineString([a, b, [(1, 1), (0, 0)]])
        >>> edges.polygonize().to_wkt()[0]
        'POLYGON ((0 0, 1 0, 1 1, 0 0))'
        """

    def convex_hull(self) -> Point | LineString | Polygon | GeometryCollection:
        """Compute the convex hull of the geometry. Operates in planar lon/lat space and does NOT
        auto-split antimeridian-crossing geographic input; call
        ``split_antimeridian`` first. Hull vertices are input vertices, so Z/M
        ordinates are preserved.

        Returns
        -------
        Point, LineString, or Polygon
            The convex hull; degenerate inputs reduce dimension.

        See Also
        --------
        concave_hull : Concave hull that can follow non-convex outlines.

        Examples
        --------
        >>> import gometry as gm
        >>> pts = gm.MultiPoint([(0, 0), (2, 0), (1, 1), (0, 2), (2, 2)])
        >>> pts.convex_hull().to_wkt()
        'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'
        """

    def validate(self) -> ValidationReport:
        """Structured validity report in the geometry's coordinate frame.
        Geographic antimeridian crossings are normalized before validation;
        projected and CRS-free geometry uses ordinary planar OGC validity.

        Returns
        -------
        ValidationReport
            Truthy when valid.

        See Also
        --------
        is_valid : Boolean-only test.
        repair : Fix what the report diagnoses.

        Examples
        --------
        >>> import gometry as gm
        >>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
        >>> report = bowtie.validate()
        >>> (report.valid, report.reason)
        (False, 'exterior ring has a self-intersection')
        """

    def node(self) -> MultiLineString:
        """Node linework by splitting every edge at all intersections. Input
        ordinates are carried where possible; unsourceable seam vertices yield XY.

        Returns
        -------
        MultiLineString
            The noded linework.


        Raises
        ------
        InvalidGeometryError
            If noding fails on the input linework.

        Examples
        --------
        >>> import gometry as gm
        >>> lines = gm.MultiLineString([[(0,0),(2,0)],[(1,-1),(1,1)]])
        >>> lines.node().to_wkt()
        'MULTILINESTRING ((0 0, 1 0), (1 0, 2 0), (1 -1, 1 0), (1 0, 1 1))'
        """

    def set_m(self, m: float | None) -> Self:
        """Set the M ordinate at every vertex, or remove it. A numeric ``m`` writes
        that M at every vertex (replacing any existing M); ``None`` removes the M
        ordinate. Z passes through unchanged.

        Parameters
        ----------
        m : float or None
            M to assign at every vertex, or ``None`` to remove the M ordinate.

        Returns
        -------
        Geometry
            The geometry with the new M ordinate applied (same kind as the input).

        See Also
        --------
        set_z : Set or clear the Z ordinate.
        interpolate_m : Assign M by interpolating along the linework.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.set_m(5.0).to_wkt()[0]
        'POINT M (1 2 5)'
        """

    def to_wkt(
        self,
        *,
        output_dimension: _WktOutputDimension | None = None,
        include_srid: bool = False,
        precision: int | None = None,
        drop_epoch: bool = False,
    ) -> str:
        """Serialize to Well-Known Text.

        Parameters
        ----------
        output_dimension : int, optional
            Cap the written ordinate count (2, 3, or 4) to at most the
            geometry's own dimensionality; defaults to writing all present
            ordinates. Cannot invent Z/M that the geometry does not carry.

        include_srid : bool, default False
            Embed the EPSG code as an EWKT ``SRID=<code>;`` prefix. The PostGIS wire
            aliases ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979;
            decoding either alias yields that EPSG identity.

        precision : int, optional
            Decimal places to round coordinates to (omit for full precision).

        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which WKT cannot encode.

        Returns
        -------
        str
            The WKT string.


        Raises
        ------
        GeometryError
            If ``output_dimension`` is not 2, 3, or 4, or ``precision`` is not
            between 0 and 15, or the geometry carries a coordinate epoch and
            ``drop_epoch`` is false.
        CRSError
            If ``include_srid`` is set and the CRS has no EPSG code.

        See Also
        --------
        from_wkt : Parse WKT back into a geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1.5, 2.5).to_wkt()
        'POINT (1.5 2.5)'
        >>> gm.GeometryArray([gm.Point(1.5, 2.5)]).to_wkt()
        ['POINT (1.5 2.5)']
        """

    def to_wkb(
        self,
        *,
        include_srid: bool = False,
        precision: int | None = None,
        drop_epoch: bool = False,
    ) -> bytes:
        """Serialize to Well-Known Binary.

        Parameters
        ----------
        include_srid : bool, default False
            Embed the EPSG code as an EWKB SRID. The PostGIS wire aliases
            ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979; decoding
            either alias yields that EPSG identity.

        precision : int, optional
            Decimal places to round coordinates to (omit for full precision).

        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which (E)WKB cannot encode.

        Returns
        -------
        bytes
            The WKB payload.

        Notes
        -----
        The coordinate epoch is not representable in (E)WKB and does not survive a
        round-trip; use Arrow interchange when the epoch matters.

        Raises
        ------
        GeometryError
            If ``precision`` is not between 0 and 15, or the geometry carries a
            coordinate epoch and ``drop_epoch`` is false.
        CRSError
            If ``include_srid`` is set and the CRS has no EPSG code.

        See Also
        --------
        from_wkb : Parse WKB/EWKB back into a geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> pt = gm.Point(1, 2)
        >>> pt.to_wkt() == gm.from_wkb(pt.to_wkb()).to_wkt()
        True
        """

    def remove_repeated_points(self, *, tolerance: float = 0.0) -> Self:
        """Drop consecutive duplicate coordinates within ``tolerance``.

        Parameters
        ----------
        tolerance : float, default 0.0
            Tolerance in coordinate units (non-negative).


        Returns
        -------
        Geometry
            The deduplicated geometry (same kind as the input).


        Raises
        ------
        GeometryError
            If ``tolerance`` is negative or non-finite.

        Examples
        --------
        >>> import gometry as gm
        >>> stuttery = gm.LineString([(0, 0), (0, 0), (1, 1)])
        >>> stuttery.remove_repeated_points().to_wkt()
        'LINESTRING (0 0, 1 1)'
        """

    def skew(
        self,
        x_angle: float = 0.0,
        y_angle: float = 0.0,
        *,
        origin: Origin = 'centroid',
        radians: bool = False,
    ) -> Self:
        """Skew (shear) a geometry about an origin.

        Parameters
        ----------
        x_angle, y_angle : float, default 0.0
            Shear angles along the X and Y axes (degrees by default; radians if
            ``radians=True``).

        origin : str or sequence of float, optional
            Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
            y)`` point.

        radians : bool, optional
            Interpret ``x_angle``/``y_angle`` in radians instead of degrees.

        Returns
        -------
        Geometry
            The transformed geometry (same kind as the input).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(0, 2).skew(x_angle=45, origin=(0, 0)).to_wkt(precision=6)
        'POINT (2 2)'
        """

    def voronoi_edges(
        self,
        *,
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> GeometryArray[LineString]:
        """Voronoi diagram edges of the geometry's vertices. Operates in planar lon/lat
        space and does NOT auto-split antimeridian-crossing geographic input; call
        ``split_antimeridian`` first.

        Parameters
        ----------
        tolerance : float, default 0.0
            Tolerance in coordinate units (non-negative).
        clip : {'padded', 'envelope'} or Polygon, default 'padded'
            How to bound the unbounded outer cells: a padded box, the input
            envelope, or a `Polygon` to clip the diagram to.
            Diagram vertices are synthesized and returned in XY.

        Returns
        -------
        GeometryArray
            The Voronoi edges of the input geometry.


        Raises
        ------
        InvalidGeometryError
            If the Voronoi diagram cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
        >>> len(sites.voronoi_edges())
        3
        """

@final
class Point(Geometry):
    """A single point geometry.

    Parameters
    ----------
    x, y : float, optional
        Point coordinates; both omitted builds ``POINT EMPTY``.
    z : float, optional
        Z ordinate (adds a 3D dimension).
    m : float, optional
        M (measure) ordinate.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """

    __match_args__: Final = ('x', 'y')

    @overload
    def __new__(
        cls,
        x: None = None,
        y: None = None,
        *,
        z: None = None,
        m: None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        x: float,
        y: float,
        *,
        z: float | None = None,
        m: float | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a ``Point`` geometry — ``XY``, ``XYZ``, ``XYM``, or ``XYZM``.

        Pass ``z`` and/or ``m`` for higher-dimensional points; to build many
        points at once use ``points``.

        Parameters
        ----------
        x, y : float, optional
            Finite coordinates (lon, lat for a geographic ``crs``). Omit both for
            an empty point.

        z : float, optional
            Z (elevation) ordinate, producing an ``XYZ`` or ``XYZM`` point.

        m : float, optional
            M (measure) ordinate, producing an ``XYM`` or ``XYZM`` point.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        Point
            A point geometry — empty when ``x``/``y`` are omitted.

        Raises
        ------
        InvalidGeometryError
            If any coordinate is not finite.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 2).to_wkt()
        'POINT (1 2)'
        >>> gm.Point(1, 2, z=3).to_wkt()
        'POINT Z (1 2 3)'
        >>> gm.Point(1, 2, m=9).to_wkt()
        'POINT M (1 2 9)'
        >>> gm.Point(13.4, 52.5, crs=4326).crs
        CRS("EPSG:4326")
        >>> gm.Point().to_wkt()
        'POINT EMPTY'
        """

    @property
    def x(self) -> float:
        """X coordinate of the point.

        Raises
        ------
        AttributeError
            If the point is empty (``POINT EMPTY``).
        """
    @property
    def y(self) -> float:
        """Y coordinate of the point.

        Raises
        ------
        AttributeError
            If the point is empty (``POINT EMPTY``).
        """
    @property
    def z(self) -> float:
        """Z (elevation) ordinate.

        Raises
        ------
        GeometryTypeError
            If the point has no Z ordinate.
        AttributeError
            If the point is empty (``POINT EMPTY``).
        """
    @property
    def m(self) -> float:
        """M (measure) ordinate.

        Raises
        ------
        GeometryTypeError
            If the point has no M ordinate.
        AttributeError
            If the point is empty (``POINT EMPTY``).
        """
    def __replace__(
        self,
        *,
        x: float = ...,
        y: float = ...,
        z: float | None = ...,
        m: float | None = ...,
        crs: CrsInput | None = ...,
        epoch: float | None = ...,
    ) -> Self:
        """Return a copy with the given ordinates and metadata replaced.

        Supports ``copy.replace`` on Python 3.13+; omitted keyword arguments
        keep the current value. ``crs=None`` / ``epoch=None`` clear metadata;
        ``z=None`` / ``m=None`` drop those ordinates.

        Parameters
        ----------
        x, y : float, optional
            Replace the X/Y coordinates.

        z, m : float or None, optional
            Replace or clear the Z/M ordinates.

        crs : str or int or None, optional
            Replace or clear the CRS label.

        epoch : float or None, optional
            Replace or clear the coordinate epoch.

        Returns
        -------
        Point
        """

@final
class MultiPoint(Geometry):
    """A collection of points.

    Parameters
    ----------
    coordinates : sequence of points or coordinate tuples, optional
        The member points; omitted builds an empty multipoint.
    x, y, z, m : sequence of float, optional
        Column form: parallel ordinate arrays, one entry per point.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """
    @overload
    def __new__(
        cls,
        coordinates: None = None,
        *,
        x: None = None,
        y: None = None,
        z: None = None,
        m: None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        coordinates: Iterable[Point | _Coordinate],
        *,
        z: None = None,
        m: None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        coordinates: _CoordinatesInput,
        *,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        coordinates: None = None,
        *,
        x: FloatColumn,
        y: FloatColumn,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a ``MultiPoint`` geometry from a coordinate sequence.

        Parameters
        ----------
        coordinates : sequence, optional
            Member coordinate tuples ``(x, y[, z[, m]])``. Mutually exclusive with
            the ``x``/``y`` column form. Omit all inputs for an empty multipoint.

        x, y : sequence of float, optional
            Per-point X and Y ordinates as parallel columns, as an alternative to
            ``coordinates``. Both are required together.

        z, m : sequence of float, optional
            Per-point Z and M ordinates, as an alternative to inline tuples.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        MultiPoint
            A multipoint geometry — empty when no coordinates are given.

        Raises
        ------
        InvalidGeometryError
            If any coordinate is non-finite or has mixed dimensionality.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.MultiPoint([(0, 0), (1, 1)]).to_wkt()
        'MULTIPOINT ((0 0), (1 1))'
        >>> gm.MultiPoint().to_wkt()
        'MULTIPOINT EMPTY'
        """
    __match_args__: Final = ('parts',)

    @property
    def parts(self) -> GeometryParts[Point]:
        """The component geometries."""
    def __len__(self) -> int:
        """Number of component parts.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> Point: ...
    @overload
    def __getitem__(self, index: slice, /) -> list[Point]: ...
    @overload
    def __getitem__(self, index: SupportsIndex | slice, /) -> Point | list[Point]:
        """Select parts by integer or slice.

        An ``int`` returns one component geometry. A ``slice`` returns a
        ``list`` of component geometries.

        Returns
        -------
        Geometry or list of Geometry
        """
    def __iter__(self) -> Iterator[Point]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> Iterator[Point]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """

@final
class LineString(Geometry):
    """A single linestring (polyline).

    Parameters
    ----------
    coordinates : sequence of coordinate tuples, optional
        The vertices as ``(x, y[, z, m])`` tuples; omitted builds an empty line.
    x, y, z, m : sequence of float, optional
        Column form: parallel ordinate arrays, one entry per vertex.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """
    @overload
    def __new__(
        cls,
        coordinates: None = None,
        *,
        x: None = None,
        y: None = None,
        z: None = None,
        m: None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        coordinates: _CoordinatesInput,
        *,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        coordinates: None = None,
        *,
        x: FloatColumn,
        y: FloatColumn,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a LineString from an ordered coordinate sequence.

        Parameters
        ----------
        coordinates : sequence, optional
            Ordered ``(x, y[, z[, m]])`` tuples, or an ``(N, 2..4)`` array.
            Mutually exclusive with the ``x``/``y`` column form. Omit all inputs
            for an empty linestring.

        x, y : sequence of float, optional
            Per-vertex X and Y ordinates as parallel columns, as an alternative to
            ``coordinates``. Both are required together.

        z, m : sequence of float, optional
            Per-vertex Z and M ordinates, as an alternative to inline tuples.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        LineString
            A linestring geometry — empty when no coordinates are given.

        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite, ragged, or fewer than two vertices.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).to_wkt()
        'LINESTRING (0 0, 1 1)'
        >>> gm.LineString().to_wkt()
        'LINESTRING EMPTY'
        """

    __match_args__: Final = ('coords',)

@final
class MultiLineString(Geometry):
    """A collection of linestrings.

    Parameters
    ----------
    lines : sequence of LineString or coordinate sequences, optional
        The member lines; omitted builds an empty multilinestring.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """
    def __new__(
        cls,
        lines: Iterable[LineString | Iterable[Iterable[float]]] | None = None,
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a ``MultiLineString`` from a sequence of line coordinate sequences.

        Parameters
        ----------
        lines : sequence, optional
            Each member is an ordered coordinate sequence (a line) or an
            already-built ``LineString``. Omit for an empty multilinestring.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        MultiLineString
            A multilinestring geometry — empty when ``lines`` is omitted.

        Raises
        ------
        InvalidGeometryError
            If a member line has fewer than two vertices or non-finite coordinates.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]).to_wkt()
        'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))'
        >>> gm.MultiLineString().to_wkt()
        'MULTILINESTRING EMPTY'
        """
    __match_args__: Final = ('parts',)

    @property
    def parts(self) -> GeometryParts[LineString]:
        """The component geometries."""
    def __len__(self) -> int:
        """Number of component parts.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> LineString: ...
    @overload
    def __getitem__(self, index: slice, /) -> list[LineString]: ...
    @overload
    def __getitem__(self, index: SupportsIndex | slice, /) -> LineString | list[LineString]:
        """Select parts by integer or slice.

        An ``int`` returns one component geometry. A ``slice`` returns a
        ``list`` of component geometries.

        Returns
        -------
        Geometry or list of Geometry
        """
    def __iter__(self) -> Iterator[LineString]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> Iterator[LineString]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """

@final
class Polygon(Geometry):
    """A single polygon (an exterior ring with optional holes).

    Parameters
    ----------
    shell : sequence of coordinate tuples, optional
        Exterior ring vertices; omitted builds ``POLYGON EMPTY``.
    holes : sequence of rings, optional
        Interior rings, each a coordinate sequence.
    x, y, z, m : sequence of float, optional
        Column form for the exterior ring: parallel ordinate arrays.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """
    @overload
    def __new__(
        cls,
        shell: None = None,
        holes: None = None,
        *,
        x: None = None,
        y: None = None,
        z: None = None,
        m: None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        shell: _CoordinatesInput,
        holes: _RingsInput | None = None,
        *,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        shell: None = None,
        holes: _RingsInput | None = None,
        *,
        x: FloatColumn,
        y: FloatColumn,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a ``Polygon`` from an exterior ring and optional holes.

        Parameters
        ----------
        shell : sequence, optional
            Exterior ring coordinates; closed automatically (needs ≥3 corners).
            Mutually exclusive with the ``x``/``y`` column form.

        holes : sequence of sequence, optional
            Interior ring (hole) coordinate sequences, each closed automatically.

        x, y : sequence of float, optional
            Per-vertex X and Y ordinates for the exterior ring, as an alternative
            to ``shell``. Both are required together.

        z, m : sequence of float, optional
            Per-vertex Z and M ordinates for the exterior ring.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        Polygon
            A polygon geometry.

        Raises
        ------
        InvalidGeometryError
            If a ring has fewer than three corners or any coordinate is non-finite.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]).to_wkt()
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
        >>> gm.Polygon().to_wkt()
        'POLYGON EMPTY'
        """
    __match_args__: Final = ('exterior', 'interiors')

    @property
    def exterior(self) -> LineString:
        """Exterior ring as a closed ``LineString``."""
    @property
    def interiors(self) -> list[LineString]:
        """Interior rings (holes), each a closed ``LineString``."""

@final
class MultiPolygon(Geometry):
    """A collection of polygons.

    Parameters
    ----------
    polygons : sequence of Polygon or (shell, holes) pairs, optional
        The member polygons; omitted builds an empty multipolygon.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """
    def __new__(
        cls,
        polygons: Iterable[Polygon | _CoordinatesInput | _RingsInput] | None = None,
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a ``MultiPolygon`` from a sequence of polygons.

        Parameters
        ----------
        polygons : sequence, optional
            Each member is ``[shell]`` or ``[shell, *holes]`` of coordinate rings,
            or an already-built ``Polygon``. Omit for an empty multipolygon.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        MultiPolygon
            A multipolygon geometry — empty when ``polygons`` is omitted.

        Raises
        ------
        InvalidGeometryError
            If any ring has fewer than three corners or non-finite coordinates.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> left = [[(0, 0), (1, 0), (1, 1)]]
        >>> right = [[(2, 2), (3, 2), (3, 3)]]
        >>> len(gm.MultiPolygon([left, right]).parts)
        2
        >>> gm.MultiPolygon().to_wkt()
        'MULTIPOLYGON EMPTY'
        """
    __match_args__: Final = ('parts',)

    @property
    def parts(self) -> GeometryParts[Polygon]:
        """The component geometries."""
    def __len__(self) -> int:
        """Number of component parts.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> Polygon: ...
    @overload
    def __getitem__(self, index: slice, /) -> list[Polygon]: ...
    @overload
    def __getitem__(self, index: SupportsIndex | slice, /) -> Polygon | list[Polygon]:
        """Select parts by integer or slice.

        An ``int`` returns one component geometry. A ``slice`` returns a
        ``list`` of component geometries.

        Returns
        -------
        Geometry or list of Geometry
        """
    def __iter__(self) -> Iterator[Polygon]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> Iterator[Polygon]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """

@final
class GeometryCollection(Geometry):
    """A heterogeneous collection of geometries.

    Parameters
    ----------
    geometries : sequence of Geometry, optional
        The member geometries, of any types; omitted builds an empty collection.
    crs : CRS, int, str, or None, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch for a dynamic CRS; allowed only with ``crs``.
    """
    def __new__(
        cls,
        geometries: Iterable[Geometry] | None = None,
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self:
        """Create a ``GeometryCollection`` from a sequence of geometries.

        Parameters
        ----------
        geometries : sequence of Geometry, optional
            Member geometries; may be of mixed types. Omit for an empty collection.

        crs : str or int, optional
            CRS as an EPSG code or authority/WKT. Declares; no transform.

        epoch : float, optional
            Coordinate epoch (decimal year) for time-dependent frames.

        Returns
        -------
        GeometryCollection
            A geometry collection — empty when ``geometries`` is omitted.

        Raises
        ------
        TypeError
            If any member is not a Geometry.
        CRSError
            If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
        CRSMismatchError
            If members carry conflicting CRS/epoch metadata.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gc = gm.GeometryCollection([gm.Point(0, 0), gm.Point(1, 1)])
        >>> gc.geometry_type
        'GeometryCollection'
        >>> gm.GeometryCollection().to_wkt()
        'GEOMETRYCOLLECTION EMPTY'
        """
    __match_args__: Final = ('parts',)

    @property
    def parts(self) -> GeometryParts[Geometry]:
        """The component geometries."""
    def __len__(self) -> int:
        """Number of component parts.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> Geometry: ...
    @overload
    def __getitem__(self, index: slice, /) -> list[Geometry]: ...
    @overload
    def __getitem__(self, index: SupportsIndex | slice, /) -> Geometry | list[Geometry]:
        """Select parts by integer or slice.

        An ``int`` returns one component geometry. A ``slice`` returns a
        ``list`` of component geometries.

        Returns
        -------
        Geometry or list of Geometry
        """
    def __iter__(self) -> Iterator[Geometry]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> Iterator[Geometry]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """

@final
class GeometryPartsIterator(Generic[_GeometryT_co]):
    """Lazy iterator over a ``GeometryParts`` view: one typed leaf per
    ``__next__`` via ``part_at``, without building the full part list up front.
    """
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """

    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> _GeometryT_co:
        """Implement next(self)."""

@final
class GeometryParts(Sequence[_GeometryT_co], Generic[_GeometryT_co]):
    """Lazy view over one geometry's immediate parts.

    Returned by ``.parts`` on every geometry. Simple geometries expose a
    singleton view of themselves; multipart and collection geometries expose
    their immediate members. Scalar indexing and iteration materialize one
    typed geometry at a time via ``part_at``; slice indexing may still build a
    list up front.
    """
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Geometry parts are returned by ``geom.parts`` and cannot be constructed."""
    @property
    def nbytes(self) -> int:
        """Logical coordinate payload retained by the source geometry.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: this lazy view plus the source geometry's
        logical coordinate heap. The view shares the geometry; it does not
        materialize individual parts.
        """
    def __len__(self) -> int:
        """Number of component parts.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _GeometryT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> list[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: SupportsIndex | slice, /) -> _GeometryT_co | list[_GeometryT_co]:
        """Select parts by integer or slice.

        An ``int`` returns one component geometry. A ``slice`` returns a
        ``list`` of component geometries.

        Returns
        -------
        Geometry or list of Geometry
        """
    def __iter__(self) -> GeometryPartsIterator[_GeometryT_co]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> GeometryPartsIterator[_GeometryT_co]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """
    def __contains__(self, item: object, /) -> bool:
        """Whether a geometry equals one of the component parts.

        Returns
        -------
        bool
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of an equal part in ``[start, stop)``.

        Parameters
        ----------
        value : object
            The geometry value to locate.
        start : int, default 0
            First position searched.
        stop : int, optional
            One past the last position searched.

        Returns
        -------
        int
            The first matching position.

        Raises
        ------
        ValueError
            If no part in the window equals ``value``.
        """
    def count(self, value: object) -> int:
        """Number of parts equal to ``value``.

        Parameters
        ----------
        value : object
            The geometry value to count.

        Returns
        -------
        int
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    __hash__: ClassVar[None]  # type: ignore[assignment]
    def __copy__(self) -> Self:
        """``copy.copy`` returns this immutable view itself."""
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns this immutable view itself."""
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle support through the parent geometry's ``parts`` property."""

@final
class GeometryArrayIterator(Generic[_GeometryT_co]):
    """Lazy element iterator for `GeometryArray` (both directions): one typed
    scalar per `next`, so iterating a packed point array never materializes
    the whole wrapper list up front.
    """
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """

    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> _GeometryT_co | None:
        """Implement next(self)."""

@final
class GeometryArray(Sequence[_GeometryT_co | None]):
    """An immutable, vectorized geometry column with one shared CRS/epoch frame.

    Homogeneous arrays use packed coordinate storage and batched Rust kernels;
    indexing and slicing preserve zero-copy views where possible.
    """
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle support: packed point/line/polygon arrays round-trip their raw
        little-endian f64 columns (plus CSR offsets for lineal storage);
        mixed arrays round-trip per-row WKB. The frame (canonical CRS string,
        epoch) travels alongside.
        """
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — a GeometryArray is an
        immutable value, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    def __and__(self, other: Geometry | GeometryArray, /) -> GeometryArray[Geometry]:
        """Return self&value."""
    def __or__(self, other: Geometry | GeometryArray, /) -> GeometryArray[Geometry]:
        """Return self|value."""
    def __sub__(self, other: Geometry | GeometryArray, /) -> GeometryArray[Geometry]:
        """Return self-value."""
    def __xor__(self, other: Geometry | GeometryArray, /) -> GeometryArray[Geometry]:
        """Return self^value."""
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __bool__(self) -> bool:
        """``False`` only when the array has zero rows.

        Returns
        -------
        bool
        """
    __array_ufunc__: ClassVar[None]
    @overload
    def __array__(
        self, dtype: None = None, copy: bool | None = None
    ) -> npt.NDArray[np.object_]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.object_] | np.dtype[np.object_] | Literal['O', 'object'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]:
        """NumPy array protocol: export as an object ndarray of typed geometries.

        Parameters
        ----------
        dtype : object, optional
            Must be ``numpy.object_`` or ``None`` (defaults to object).
        copy : bool, optional
            When ``False``, raises — the object array is always freshly
            allocated.

        Returns
        -------
        numpy.ndarray
            One typed leaf geometry per row.

        Raises
        ------
        GeometryError
            If ``dtype`` is not ``object`` or ``None``.
        ValueError
            If ``copy`` is ``False``.
        """
    def __contains__(self, value: object, /) -> bool:
        """Whether a geometry (or missing row via ``None``) is present.

        Equality is structural (``Geometry.__eq__``): CRS, epoch, and exact
        geometry. ``None in arr`` is true when any row is missing.

        Returns
        -------
        bool
        """
    def __reversed__(self) -> GeometryArrayIterator[_GeometryT_co]:
        """Iterate geometries in reverse row order.

        Returns
        -------
        iterator of Geometry or None
        """
    def __sizeof__(self) -> int:
        """`sys.getsizeof` support: the wrapper plus this array's logical
        Rust-side heap — coordinate payload, CSR offsets, row maps, missing
        mask, and any already-materialized prepared-geometry / frame sidecars.
        Lazy cache slots that have not been populated do not inflate the total.
        Shared backing buffers are accounted like NumPy views: the logical view
        is reported, not the whole shared parent allocation.
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of a structurally equal element in `[start, stop)`
        (list.index semantics, using Geometry.__eq__:
        same
        CRS, epoch,
        and exact geometry; negative bounds count from the end).

        Parameters
        ----------
        value : Geometry
            The element to locate.

        start : int, default 0
            First position searched.

        stop : int, optional
            One past the last position searched (the array length when omitted).

        Returns
        -------
        int
            The first matching position.

        Raises
        ------
        ValueError
            If no element in the window equals ``value``.
        """
    def count(self, value: object) -> int:
        """Number of structurally equal elements (`list.count` semantics, using
        `Geometry.__eq__`).

        Parameters
        ----------
        value : Geometry
            The element to count.

        Returns
        -------
        int
            How many elements equal ``value``.
        """
    @property
    def nbytes(self) -> int:
        """Logical coordinate payload in bytes (numpy's ``nbytes`` convention):
        the stored ``f64`` ordinate columns for this array's selected rows.
        Slices and fancy-indexed arrays report only their logical rows. Object
        headers, CSR offset columns, row maps, prepared-geometry / frame
        sidecars, gather memos, and CRS metadata are excluded (``nbytes`` is
        payload-only, matching NumPy).

        Returns
        -------
        int
        """
    def _repr_html_(self) -> str:
        """HTML preview for notebooks: an interactive lonboard map when lonboard is
        installed and the array has a CRS with finite bounds, otherwise a grid of
        inline SVGs (see ``_repr_html_svg``).

        Returns
        -------
        str
            The HTML preview markup.
        """
    def _repr_html_svg(self) -> str:
        """SVG grid preview rendering up to the first `SVG_ARRAY_PREVIEW` geometries
        as a wrapping grid of SVGs, with an `"N geometries"` caption. Bounded so a
        large array never renders thousands of elements.

        Returns
        -------
        str
            The SVG grid preview markup.
        """
    @overload
    def __new__(
        cls,
        values: Iterable[None],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[_GeometryT | None],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> GeometryArray[_GeometryT]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[Buffer | None],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[Mapping[str, Any] | SupportsGeoInterface | None],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[_GeometryLike | None],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> GeometryArray[Geometry]:
        """Create and return a new object.  See help(type) for accurate signature."""
    @property
    def crs(self) -> CRS | None:
        """CRS shared by every geometry in the array, or ``None``.

        Returns
        -------
        CRS or None
        """
    def estimate_local_crs(self) -> CRS:
        """Estimate one conformal metric CRS for all present rows.

        Missing rows are ignored. The complete present extent is evaluated
        against a fixed 0.1% linear scale-error ceiling; empty/all-missing,
        CRS-free, or geographically unsafe arrays raise ``CRSError``.

        Returns
        -------
        CRS

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(-122.4, 37.8, crs=4326)])
        >>> arr.estimate_local_crs()
        CRS("EPSG:32610")
        """
    @property
    def epoch(self) -> float | None:
        """Coordinate epoch shared by the array, if set.

        Returns
        -------
        float or None
        """
    @property
    def __geo_interface__(self) -> GeoJsonFeatureCollection:
        """``__geo_interface__`` for the whole array: a GeoJSON-style
        ``FeatureCollection`` mapping (one ``Feature`` per row, positional
        ``id``, empty ``properties``) — the shape geopandas and mapping
        libraries expect from a geometry column.
        """
    @property
    def coordinate_axes(self) -> list[CoordinateAxes | None]:
        """Per-geometry ordinate layout (see `Geometry.coordinate_axes`), with
        ``None`` at missing rows.

        Returns
        -------
        list of str or None
            One ``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'`` token per row.
        """
    @property
    def common_coordinate_axes(self) -> CoordinateAxes | None:
        """Ordinate layout shared by every present row, or ``None`` when rows
        differ.

        Returns
        -------
        str or None
        """
    @property
    def has_z(self) -> npt.NDArray[np.bool_]:
        """Whether each geometry carries a Z ordinate. Missing rows are false.

        Returns
        -------
        numpy.ndarray
        """
    @property
    def any_has_z(self) -> bool:
        """Whether any present geometry carries a Z ordinate.

        Returns
        -------
        bool
        """
    @property
    def has_m(self) -> npt.NDArray[np.bool_]:
        """Whether each geometry carries an M ordinate. Missing rows are false.

        Returns
        -------
        numpy.ndarray
        """
    @property
    def any_has_m(self) -> bool:
        """Whether any present geometry carries an M ordinate.

        Returns
        -------
        bool
        """
    @property
    def bounds(self) -> npt.NDArray[np.float64]:
        """Per-geometry bounds ``(minx, miny, maxx, maxy)`` (see
        `Geometry.bounds`), as a read-only ``(rows, 4)`` float64 ndarray.
        Empty rows are all-``nan`` (intentional: a fixed-width ndarray cannot
        hold ``None`` like a scalar geometry); missing rows are also all-``nan``
        and are identified by `.is_missing`.

        Returns
        -------
        numpy.ndarray
            One ``minx, miny, maxx, maxy`` row per input geometry.
        """
    @property
    def total_bounds(self) -> tuple[float, float, float, float] | None:
        """Combined bounds ``(minx, miny, maxx, maxy)`` over all geometries, or
        ``None`` if every geometry is empty.

        Returns
        -------
        tuple or None
        """
    @property
    def geometry_type(self) -> list[GeometryType | None]:
        """Per-geometry OGC type name (see `Geometry.geometry_type`), with
        ``None`` at missing rows.

        Returns
        -------
        list of str or None
            One name per input geometry, e.g. ``'Point'`` or ``'MultiPolygon'``.
        """
    def _geoparquet_geometry_types(self) -> list[str]:
        """Sorted unique GeoParquet ``geometry_types`` inventory for present rows.

        Labels match GeoParquet 1.x (``'Point'``, ``'Point Z'``, …). Missing
        rows are skipped; an all-missing or empty array yields ``[]``.

        Returns
        -------
        list of str
        """
    @property
    def parts(self) -> Groups[GeometryArray[Geometry]]:
        """Top-level parts grouped by source row.

        Simple geometries form one-element groups, multipart and collection
        geometries expose their immediate members, and empty or missing rows
        form empty groups. Use free function ``parts`` for a flattened materialized
        `GeometryArray` instead.

        Returns
        -------
        Groups
            One `GeometryArray` group per input row.
        """
    @property
    def num_geometries(self) -> npt.NDArray[np.int64]:
        """Per-geometry top-level part count (see `Geometry.num_geometries`):
        ``1`` for a single point/line/polygon, the member count for a
        multi/collection, ``0`` for empty. Missing rows use Shapely's
        ``None``-geometry sentinel ``0``; use `.is_missing` to distinguish
        them from present empty geometries.

        Returns
        -------
        numpy.ndarray
            One count per input geometry.
        """
    @property
    def num_coordinates(self) -> npt.NDArray[np.int64]:
        """Per-geometry coordinate count. Missing rows are ``0``.

        Returns
        -------
        numpy.ndarray
            One ``int64`` coordinate count per input geometry.
        """
    @overload
    def set_coordinates(self, coordinates: npt.ArrayLike | Coordinates, /) -> Self: ...
    @overload
    def set_coordinates(
        self,
        coordinates: None = None,
        /,
        *,
        x: FloatColumn,
        y: FloatColumn,
        z: FloatColumn | None = None,
        m: FloatColumn | None = None,
    ) -> Self:
        """Return an array with the same topology and replacement coordinates.

        Pass one ``(N, dims)`` matrix (including a `Coordinates` view) or
        explicit ``x=`` and ``y=`` columns. Missing rows contribute no input
        coordinates and remain missing in the result.

        Parameters
        ----------
        coordinates : sequence of float, optional
            Replacement ``(N, dims)`` coordinate matrix, including a
            `Coordinates` view.
        x, y : sequence of float, optional
            Replacement X and Y columns.
        z, m : sequence of float, optional
            Replacement Z and M columns when the array already has those axes.
            Omitted axes are carried unchanged; ``None`` is not a clearing
            sentinel.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])])
        >>> arr.set_coordinates([(5, 5), (6, 6)]).to_wkt()
        ['LINESTRING (5 5, 6 6)']
        """
    def map_coordinates(
        self, func: Callable[[npt.NDArray[np.float64]], npt.ArrayLike]
    ) -> Self:
        """Apply a vectorized callback to this array's coordinate matrix.

        The callback receives a read-only ``(N, dims)`` float64 matrix for
        present rows and must return a matrix with the same shape.
        Non-uniform arrays (mixed-axes rows or a heterogeneous
        GeometryCollection) use the view's union layout with NaN padding;
        each member keeps its native axes on apply.

        Parameters
        ----------
        func : callable
            Function called with the read-only coordinate matrix.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])])
        >>> arr.map_coordinates(lambda m: m + 1).to_wkt()
        ['LINESTRING (1 1, 2 2)']
        """
    @property
    def topological_dimension(self) -> npt.NDArray[np.int64]:
        """Per-geometry topological dimension — ``0`` point, ``1`` curve, ``2``
        areal (see `Geometry.topological_dimension`). Missing rows use
        Shapely's ``None``-geometry sentinel ``-1``; use `.is_missing` to
        distinguish them from present point-like geometries.

        Returns
        -------
        numpy.ndarray
            One dimension per input geometry.
        """
    @property
    def is_empty(self) -> npt.NDArray[np.bool_]:
        """Whether each geometry is empty (no points, rings, or parts).

        Returns
        -------
        numpy.ndarray
            One result per input geometry.
        """
    @property
    def is_closed(self) -> npt.NDArray[np.bool_]:
        """Per-geometry closed test (see `Geometry.is_closed`).

        Returns
        -------
        numpy.ndarray
            One result per input geometry.
        """
    @property
    def is_ring(self) -> npt.NDArray[np.bool_]:
        """Per-geometry ring test (see `Geometry.is_ring`).

        Returns
        -------
        numpy.ndarray
            One result per input geometry.
        """
    @property
    def is_ccw(self) -> npt.NDArray[np.bool_]:
        """Per-geometry counter-clockwise test (see `Geometry.is_ccw`).

        Returns
        -------
        numpy.ndarray
            One result per input geometry.
        """
    @property
    def is_simple(self) -> npt.NDArray[np.bool_]:
        """Per-geometry simplicity test (see `Geometry.is_simple`).

        Returns
        -------
        numpy.ndarray
            One result per input geometry.
        """
    @property
    def crosses_antimeridian(self) -> npt.NDArray[np.bool_]:
        """Element-wise antimeridian-crossing test.

        Returns
        -------
        numpy.ndarray
            One result per input geometry.

        Raises
        ------
        CRSError
            If the CRS is projected (a geographic CRS or CRS-free lon/lat is
            required).
        """
    @property
    def area(self) -> npt.NDArray[np.float64]:
        """Per-row area, measured for the array's CRS.

        A geographic CRS gives ellipsoidal square meters (geodesic, on the CRS's
        own ellipsoid); a projected CRS gives squared native coordinate units;
        a CRS-free array gives squared coordinate units. Use ``to_crs`` to
        change frame.

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``0`` for
            points and curves; ``nan`` for missing rows.

        Raises
        ------
        CRSError
            If the CRS lacks linear axis units for a metric result.

        See Also
        --------
        length : Length/perimeter under the same CRS-aware metric.
        """
    @property
    def length(self) -> npt.NDArray[np.float64]:
        """Per-row length (curves) or perimeter (areal), measured for the array's
        CRS.

        A geographic CRS gives ellipsoidal meters (geodesic, on the CRS's own
        ellipsoid); a projected CRS gives native linear units; a CRS-free array
        gives coordinate units. Use ``to_crs`` to change frame.

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``0`` for
            points; ``nan`` for missing rows.

        Raises
        ------
        CRSError
            If the CRS lacks linear axis units for a metric result.

        See Also
        --------
        area : Area under the same CRS-aware metric.
        """
    @property
    def length_3d(self) -> npt.NDArray[np.float64]:
        """Per-row 3D length of curves with Z, measured for the array's CRS.

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where Z is missing on a vertex or the row is missing.

        Raises
        ------
        CRSError
            If the CRS lacks linear axis units for a metric result.
        """
    def __class_getitem__(cls, key: Any) -> types.GenericAlias:
        """See PEP 585"""
    def __len__(self) -> int:
        """Number of rows (including missing rows).

        Returns
        -------
        int
        """
    @property
    def coords(self) -> Coordinates:
        """Returns
        -------
        Coordinates
            Flattened coordinates from present rows; missing rows contribute
            no coordinates. This is a vertex stream, not a row-aligned view;
            use `get_coordinates(..., return_index=True)` to recover source-row
            alignment, or call `drop_missing()` first for an explicit dense path.
        """
    def __iter__(self) -> Iterator[_GeometryT_co | None]:
        """Iterate geometries in row order (including missing rows as ``None``).

        Returns
        -------
        iterator of Geometry or None
        """
    @overload
    def __getitem__(self, index: int, /) -> _GeometryT_co | None: ...
    @overload
    def __getitem__(self, index: slice, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(
        self, index: npt.NDArray[np.bool_], /
    ) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(
        self, index: npt.NDArray[np.int64], /
    ) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(
        self,
        index: int
        | slice
        | npt.NDArray[np.bool_]
        | _BoolLane
        | _IndexLane
        | npt.NDArray[np.int64],
        /,
    ) -> _GeometryT_co | GeometryArray[_GeometryT_co] | None:
        """Select rows by integer, slice, or fancy index.

        An ``int`` returns one typed geometry (or raises ``IndexError``).
        A ``slice`` or integer sequence / boolean mask returns a gathered
        ``GeometryArray`` (missing rows stay missing).

        Returns
        -------
        Geometry or GeometryArray
            Scalar geometry for an int index; array for slice/fancy selection.
        """
    def to_arrow(self, *, encoding: ArrowEncoding = 'auto') -> PyArrowArray:
        """Export the array as a `GeoArrow` array.

        Parameters
        ----------
        encoding : {'auto', 'wkb'}, default auto
            ``auto`` exports homogeneous arrays as their native GeoArrow layout
            and falls back to WKB for mixed geometry types; ``wkb`` always
            exports a GeoArrow WKB array.

        Returns
        -------
        object
            A GeoArrow-compatible array.

        See Also
        --------
        from_arrow : Decode a GeoArrow array into a ``GeometryArray``.

        Examples
        --------
        >>> import gometry as gm
        >>> type(gm.GeometryArray([gm.Point(1, 2)]).to_arrow()).__name__
        'ExtensionArray'
        """
    def __arrow_c_schema__(self) -> PyArrowCapsule:
        """Export the array's Arrow C Data schema as an ``arrow_schema`` capsule."""
    def __arrow_c_array__(
        self, requested_schema: PyArrowSchemaCapsule | None = None
    ) -> tuple[PyArrowCapsule, PyArrowCapsule]:
        """Export the array as Arrow C Data ``(schema, array)`` capsules."""
    def __arrow_c_stream__(
        self, requested_schema: PyArrowSchemaCapsule | None = None
    ) -> PyArrowCapsule:
        """Export the array as a one-batch Arrow C stream capsule."""
    def to_pandas(
        self, *, index: Any | None = None, name: Hashable | None = None
    ) -> PandasSeries:
        """Build a pandas Series backed by gometry's concrete extension dtype.

        Parameters
        ----------
        index : sequence or pandas.Index, optional
            Forwarded to ``pandas.Series``.
        name : hashable, optional
            Series name.

        Returns
        -------
        object
            A pandas Series sharing this geometry array.

        Examples
        --------
        >>> import gometry as gm
        >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_pandas()).__name__
        'Series'
        """
    def to_polars(
        self,
        *,
        name: str = 'geometry',
        drop_epoch: bool = False,
        drop_crs: bool = False,
    ) -> PolarsSeries:
        """Encode this array as a Polars binary (E)WKB Series.

        Parameters
        ----------
        name : str, default "geometry"
            Output Series name.
        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which WKB cannot encode.
        drop_crs : bool, default False
            Permit losing a CRS that EWKB cannot embed (no EPSG authority
            code); restore it via ``from_polars(..., crs=...)``. EPSG SRIDs
            are always embedded when available.

        Returns
        -------
        object
            A Polars Series containing WKB or EWKB values.

        Examples
        --------
        >>> import gometry as gm
        >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_polars()).__name__
        'Series'
        """
    def to_geopandas(self, *, drop_epoch: bool = False) -> GeoPandasSeries:
        """Convert this array to a GeoPandas GeoSeries through vectorized WKB.

        Parameters
        ----------
        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which GeoPandas cannot
            represent.

        Returns
        -------
        object
            A GeoPandas GeoSeries carrying this array's CRS.

        Examples
        --------
        >>> import gometry as gm
        >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_geopandas()).__name__
        'GeoSeries'
        """
    def to_geoparquet(
        self,
        path: str | PathLike[str],
        *,
        attributes: PyArrowTable | Mapping[str, Any] | None = None,
        encoding: Literal['wkb', 'native'] = 'wkb',
        **kwargs: Any,
    ) -> None:
        """Write this array to a GeoParquet file, optionally as a feature table.

        Parameters
        ----------
        path : path-like
            Output Parquet path.
        attributes : pyarrow.Table or mapping, optional
            Per-row attribute columns written beside the geometry column
            (lengths must match).
        encoding : str, default "wkb"
            Geometry encoding: ``'wkb'`` (portable default) or ``'native'``
            (GeoArrow separated coordinates for homogeneous arrays).
        kwargs : mapping, optional
            Additional options forwarded to ``pyarrow.parquet.write_table``.

        Returns
        -------
        None

        Raises
        ------
        GeometryError
            If encoding is unknown, or attributes clash with the geometry
            column or mismatch the row count.

        Examples
        --------
        >>> import gometry as gm
        >>> import tempfile, os
        >>> path = tempfile.mktemp(suffix='.parquet')
        >>> gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_geoparquet(path)
        >>> os.path.getsize(path) > 0
        True
        """
    def to_polyline(
        self, *, precision: int = 5, drop_epoch: bool = False
    ) -> list[str | None]:
        """Encode every ``LineString`` or ``Point`` row as Google polyline text (see
        `Geometry.to_polyline`).

        Parameters
        ----------
        precision : int, default 5
            Decimal digits encoded per ordinate (``0`` to ``11``).
        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which polyline cannot
            encode.

        Returns
        -------
        list of str or None
            One encoded polyline per row, with ``None`` at missing rows.

        Raises
        ------
        GeometryTypeError
            If a row is not a ``LineString`` or ``Point``.
        CRSError
            If the CRS is set and is not EPSG:4326 longitude/latitude.
        InvalidGeometryError
            If a row carries Z/M, or a coordinate is outside the
            longitude/latitude domain. Flatten explicitly with ``force_2d()``.
        GeometryError
            If ``precision`` is out of range.

        See Also
        --------
        from_polyline : Decode Google polyline text into geometries.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])]).to_polyline()
        ['??_ibE_ibE']
        """
    def set_epoch(self, epoch: float | None, *, overwrite: bool = False) -> Self:
        """Declare (or clear) the array's coordinate epoch (see
        `Geometry.set_epoch`). ``None`` clears it; changing a present epoch
        needs ``overwrite=True``.

        Parameters
        ----------
        epoch : float or None
            Decimal year, or ``None`` to clear.

        overwrite : bool, default False
            Allow replacing an existing, different epoch.

        Returns
        -------
        GeometryArray
            A copy carrying the new epoch.

        Raises
        ------
        CRSError
            If a present epoch would change without ``overwrite=True``.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(-122.4, 37.8, crs=4326)])
        >>> arr.set_epoch(2015.0).epoch
        2015.0
        """

    def set_crs(self, crs: CrsInput | None, *, overwrite: bool = False) -> Self:
        """Relabel the CRS of all geometries without moving coordinates (see
        `Geometry.set_crs`; replacing a different declared CRS requires
        ``overwrite=True``).

        Parameters
        ----------
        crs : str or int
            CRS as an EPSG code or authority/WKT string.

        overwrite : bool, default False
            Allow replacing an existing, different CRS label.

        Returns
        -------
        GeometryArray

        Raises
        ------
        CRSError
            If ``crs`` is not a recognized CRS, or it would silently replace a
            different declared CRS without ``overwrite``.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([gm.Point(1, 2)]).set_crs(4326).crs
        CRS("EPSG:4326")
        """
    def to_crs(
        self,
        crs: CrsInput,
        *,
        area_of_interest: CrsAreaInput | None = None,
        epoch: float | None = None,
        authority: str | None = None,
        accuracy: float | None = None,
        allow_ballpark: bool | None = None,
        only_best: bool | None = None,
        force_over: bool = False,
    ) -> Self:
        """Reproject all geometries to a target CRS.
        The source coordinate epoch is the array's own ``epoch`` metadata
        (stamp it with ``set_epoch`` first to transform between dynamic
        frames);
        ``epoch`` here labels the *output* coordinate epoch.

        Parameters
        ----------
        crs : str or int
            CRS as an EPSG code or authority/WKT string.

        area_of_interest : sequence of float, optional
            Bounding ``(west, south, east, north)`` to pick the best transform.

        epoch : float, optional
            Output coordinate epoch (decimal year) to tag on the result, for
            dynamic frames. Omitted keeps the source epoch while it still
            means something: the CRS is unchanged, or the target CRS is
            dynamic (time-dependent). A static target clears it.

        authority : str, optional
            Restrict candidate coordinate operations to this authority
            (e.g. ``'EPSG'``).

        accuracy : float, optional
            Maximum acceptable operation accuracy, in meters.

        allow_ballpark : bool, optional
            Allow low-accuracy ballpark operations when no precise one exists.

        only_best : bool, optional
            Use only the single best operation; no fallback.

        force_over : bool, optional
            Keep coordinates on the source side of the antimeridian instead of
            wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
            ``only_best``, this also collapses operation selection to a single
            candidate, so enumerating surfaces return exactly one operation.

        Returns
        -------
        GeometryArray

        Raises
        ------
        TransformError
            If no transform exists between the frames or it fails to apply.
        CRSError
            If a CRS is invalid or the source is missing.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2, crs=4326)])
        >>> round(float(gm.get_coordinates(arr.to_crs(3857))[0][0]), 2)
        111319.49
        """
    def sort_by_spatial_key(
        self,
        *,
        curve: SpatialCurve = 'hilbert',
        level: int = 16,
        bounds: Iterable[float] | None = None,
    ) -> Self:
        """Return the array reordered along a space-filling curve.

        Parameters
        ----------
        curve : {'hilbert', 'morton'}, default hilbert
            ``hilbert`` prioritizes locality; ``morton`` uses Z-order.

        level : int, default 16
            Curve depth (``1``-``32``).

        bounds : iterable[float], optional
            ``(minx, miny, maxx, maxy)`` extent for keying; defaults to ``total_bounds``.

        Returns
        -------
        GeometryArray
            A new array with the same rows in curve-key order; empty and
            missing rows sort last.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 0), gm.Point(0, 0)])
        >>> arr.sort_by_spatial_key().to_wkt()
        ['POINT (0 0)', 'POINT (1 0)']
        """
    @property
    def is_convex(self) -> npt.NDArray[np.bool_]:
        """Per-geometry polygon convexity test (see `Geometry.is_convex`).

        Returns
        -------
        numpy.ndarray
        """
    def intersection_all(
        self,
    ) -> Geometry:
        """Intersect all present geometries into one geometry.

        The region common to EVERY present row (missing rows are skipped, the
        SQL/pandas aggregate convention). The array sibling of
        `intersection_all`, which takes raw iterables.

        Returns
        -------
        Geometry
            A single geometry covered by every present row.

        Raises
        ------
        InvalidGeometryError
            If the array has no present rows or the overlay cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> panes = gm.GeometryArray([
        ...     gm.box(0, 0, 3, 3), gm.box(1, 1, 4, 4), gm.box(2, 2, 5, 5),
        ... ])
        >>> panes.intersection_all().to_wkt()
        'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
        """

    def symmetric_difference_all(
        self,
    ) -> Geometry:
        """Symmetric difference of all present geometries.

        The region covered by an ODD number of present rows (missing rows are
        skipped). The array sibling of `symmetric_difference_all`, which takes
        raw iterables.

        Returns
        -------
        Geometry
            A single geometry covered by an odd number of present rows.

        Raises
        ------
        InvalidGeometryError
            If the array has no present rows or the overlay cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> panes = gm.GeometryArray([
        ...     gm.box(0, 0, 2, 2), gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3),
        ... ])
        >>> panes.symmetric_difference_all().to_wkt()  # the duplicate cancels
        'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'
        """

    def union_all(self) -> Geometry:
        """Union all present geometries into one geometry.

        Missing rows are skipped (the SQL/pandas aggregate convention).

        Returns
        -------
        Geometry
            A single geometry covering every present row.

        Raises
        ------
        InvalidGeometryError
            If the array has no present rows or the overlay cannot be constructed.

        See Also
        --------
        coverage_union : Faster dissolve for a valid polygonal coverage.

        Examples
        --------
        >>> import gometry as gm
        >>> panes = gm.GeometryArray([gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)])
        >>> panes.union_all().to_wkt()
        'POLYGON ((0 0, 2 0, 2 1, 3 1, 3 3, 1 3, 1 2, 0 2, 0 0))'
        """

    def dissolve(
        self, by: Iterable[_GroupKeyT]
    ) -> tuple[GeometryArray[Geometry], list[_GroupKeyT]]:
        """Dissolve geometries into per-group unions.

        Parameters
        ----------
        by : iterable
            One grouping key per row (same length as the array).

        Returns
        -------
        tuple of (GeometryArray, list)
            One union per distinct key plus the parallel keys in first-occurrence order.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
        >>> geoms, keys = arr.dissolve(by=[0, 0])
        >>> geoms.to_wkt()
        ['POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))']
        """
    @property
    def is_valid(self) -> npt.NDArray[np.bool_]:
        """Per-element validity mask (see `Geometry.is_valid`).

        Returns
        -------
        numpy.ndarray
            ``True`` where the geometry is valid, one entry per row.
        """
    def concat(
        self: GeometryArray[_GeometryT_co], *others: GeometryArray[_GeometryOtherT]
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT]:
        """Concatenate one or more arrays sharing this array's CRS and epoch.

        Parameters
        ----------
        *others : GeometryArray
            Arrays to append, in order; each must share this array's CRS and
            epoch. With no arguments the array itself is returned.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> import gometry as gm
        >>> a = gm.GeometryArray([gm.box(0, 0, 1, 1)])
        >>> a.concat(gm.GeometryArray([gm.Point(2, 2)])).to_wkt()
        ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POINT (2 2)']
        """
    @property
    def min_z(self) -> npt.NDArray[np.float64]:
        """Per-row smallest Z ordinate (``nan`` where a geometry carries no Z).

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where Z is absent or the row is missing.
        """
    @property
    def max_z(self) -> npt.NDArray[np.float64]:
        """Per-row largest Z ordinate (``nan`` where a geometry carries no Z).

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where Z is absent or the row is missing.
        """
    @property
    def z_range(self) -> npt.NDArray[np.float64]:
        """Per-row Z span (``max_z - min_z``; ``nan`` without Z).

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where Z is absent or the row is missing.
        """
    @property
    def min_m(self) -> npt.NDArray[np.float64]:
        """Per-row smallest M ordinate (``nan`` where a geometry carries no M).

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where M is absent or the row is missing.
        """
    @property
    def max_m(self) -> npt.NDArray[np.float64]:
        """Per-row largest M ordinate (``nan`` where a geometry carries no M).

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where M is absent or the row is missing.
        """
    @property
    def m_range(self) -> npt.NDArray[np.float64]:
        """Per-row M span (``max_m - min_m``; ``nan`` without M).

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
            where M is absent or the row is missing.
        """
    @property
    def bounds_3d(self) -> npt.NDArray[np.float64]:
        """Per-row 3D bounding box ``(minx, miny, minz, maxx, maxy, maxz)``.

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n, 6)``; ``nan``
            where Z is absent or the row is missing.
        """
    @property
    def is_missing(self) -> npt.NDArray[np.bool_]:
        """Which rows are missing, as a boolean `numpy.ndarray`.

        True marks a missing row (None on access); a dense array
        returns all-False. The pandas/pyarrow validity convention, with
        gometry's full-word spelling.

        Returns
        -------
        numpy.ndarray
            One ``bool`` per row.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([gm.Point(0, 0), None]).is_missing.tolist()
        [False, True]
        """
    def drop_missing(self) -> Self:
        """Return a new array without the missing rows.

        Returns
        -------
        GeometryArray
            The present rows, in order; the input unchanged (and returned
            as-is when dense).

        Examples
        --------
        >>> import gometry as gm
        >>> len(gm.GeometryArray([gm.Point(0, 0), None]).drop_missing())
        1
        """
    @overload
    def fill_missing(
        self, value: _GeometryOtherT
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT]: ...
    @overload
    def fill_missing(
        self, value: GeometryArray[_GeometryOtherT]
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT]:
        """Return a new array with every missing row replaced by ``value``.

        Parameters
        ----------
        value : Geometry or GeometryArray
            A scalar fill geometry, or a row-aligned array whose values fill
            the matching missing rows. Only masked rows are consumed from an
            array fill value; every consumed row must be present.

        Returns
        -------
        GeometryArray
            A dense array (no missing rows); the input unchanged (and
            returned as-is when already dense).

        Raises
        ------
        CRSMismatchError
            If ``value``'s CRS or coordinate-epoch metadata conflicts with the
            array's.
        ValueError
            If an array fill value has a different length.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(0, 0), None])
        >>> arr.fill_missing(gm.Point(9, 9)).to_wkt()[1]
        'POINT (9 9)'
        """
    def _replace_at(
        self,
        positions: Iterable[int],
        values: Iterable[_GeometryOtherT | None],
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT]:
        """Private batch scatter used by the pandas adapter: replace selected
        positions in one native call (never rebuilds the column through Python
        per row). ``positions`` and ``values`` are equal-length sequences;
        each value is a ``Geometry`` or missing (``None``).

        Parameters
        ----------
        positions : sequence of int
            Non-negative logical row indices (already bounds-normalized).
        values : sequence of Geometry or None
            Replacement values aligned with ``positions``.

        Returns
        -------
        GeometryArray
        """
    def _with_missing(self, mask: Sequence[bool] | npt.NDArray[np.bool_]) -> Self:
        """Attach a missing mask to this array's rows (internal; the pandas
        bridge builds masked arrays without a rebuild). Length-checked;
        new bits are OR-merged with any existing mask so clearing a bit
        cannot expose NaN placeholders in packed storage as trusted geometry.
        All-present results normalize back to dense. Use ``fill_missing`` to
        replace missing rows with real geometry.

        The mask is collected with a fixed expected length and fallible
        reservation — never generic ``Vec<bool>`` extraction from a lying
        ``__len__`` (which can allocator-abort before the length check).
        """
    def to_numpy(self) -> npt.NDArray[np.object_]:
        """Export the array as a ``numpy.ndarray`` of typed geometry objects.

        Returns
        -------
        numpy.ndarray
            One typed leaf geometry per row.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([gm.Point(1, 2)]).to_numpy()[0].to_wkt()
        'POINT (1 2)'
        """

    def interpolate_m(
        self,
        start_m: FloatInput,
        end_m: FloatInput,
        *,
        overwrite: bool = False,
        unit: DistanceUnit | None = None,
    ) -> Self:
        """Interpolate an M ordinate along the line's arc length (CRS-aware). M runs
        from ``start_m`` at the start to ``end_m`` at the end, continuously across
        multipart linework (the PostGIS ``ST_AddMeasure`` shape). The stationing
        follows the CRS like length — geodesic on a geographic CRS, planar
        otherwise (coordinates are never moved). Z is preserved; existing M requires
        ``overwrite=True``.

        Parameters
        ----------
        start_m, end_m : float or sequence of float
            The measure range (finite, ``end_m >= start_m``) — a scalar applies to
            every geometry, or pass one value per geometry.
        overwrite : bool, default False
            Replace existing M ordinates instead of raising.

        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            One result per row (kinds preserved).


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework is empty, or carries M without ``overwrite``.
        GeometryError
            If the measure range is invalid.

        See Also
        --------
        line_interpolate : Point at a distance or M location along the line.
        line_substring : Extract a contiguous portion of the line.
        line_locate : Project a geometry onto the line.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.interpolate_m(0.0, 100.0).to_wkt()
        'LINESTRING M (0 0 0, 10 0 100)'
        """

    def polylabel(
        self,
        *,
        tolerance: FloatInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]:
        """Pole of inaccessibility: the most distant interior point. Center of the
        largest inscribed circle — the best label anchor — measured for the CRS
        exactly like maximum_inscribed_circle (whose center this is).

        See Also
        --------
        maximum_inscribed_circle : Filled disk whose center this is.
        centroid : Area/length-weighted center (may fall outside).
        point_on_surface : A guaranteed-interior representative point.

        Parameters
        ----------
        tolerance : float or sequence of float, optional
            Precision of the search, interpreted for the CRS (see ``unit``) — a
            scalar applies to every geometry, or pass one value per geometry. Omitted
            selects a scale-aware tolerance independently for each geometry.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray[Point]
            One point per row.


        Raises
        ------
        InvalidGeometryError
            If the pole of inaccessibility cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).polylabel().to_wkt()
        'POINT (1 1)'
        """

    def minimum_clearance_line(
        self, *, unit: DistanceUnit | None = None
    ) -> GeometryArray[LineString]:
        """Two-point line realizing `minimum_clearance`. The metric matches
        minimum_clearance: on a geographic CRS, the witness is selected in the
        geometry's best local projection and returned in source coordinates, so it is a
        local-projection approximation rather than an exact ellipsoidal clearance
        search.
        ``LINESTRING EMPTY`` when the clearance is infinite (fewer than two distinct
        vertices).

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            One realizing line per row.

        See Also
        --------
        minimum_clearance : The clearance distance itself.

        Examples
        --------
        >>> import gometry as gm
        >>> (gm.box(0, 0, 3, 2).minimum_clearance_line()).to_wkt()
        'LINESTRING (0 0, 0 2)'
        """

    def to_geojson(
        self, *, include_z: bool = True, drop_epoch: bool = False
    ) -> list[str | None]:
        """Serialize to `GeoJSON` text. `GeoJSON` is WGS84 by specification (RFC 7946):
        CRS-tagged input must be ``EPSG:4326`` (or ``OGC:CRS84``) — reproject with
        ``to_crs(4326)`` first. CRS-free input is serialized as-is.

        Parameters
        ----------
        include_z : bool, default True
            Write Z ordinates when present.

            `GeoJSON` cannot represent M; remove it explicitly with ``set_m(None)``.

        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which GeoJSON cannot encode.

        Returns
        -------
        list of str
            One `GeoJSON` geometry string per row.

        Raises
        ------
        CRSError
            If the input carries a CRS other than WGS84.
        InvalidGeometryError
            If input carries M ordinates.
        GeometryError
            If input carries a coordinate epoch and ``drop_epoch`` is false.

        See Also
        --------
        from_geojson : Parse `GeoJSON` back into a geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 2).to_geojson()
        '{"type":"Point","coordinates":[1.0,2.0]}'
        """

    def boundary(
        self,
    ) -> GeometryArray[MultiPoint | LineString | MultiLineString | GeometryCollection]:
        """Return the topological boundary of the geometry.

        Returns
        -------
        GeometryArray
            One boundary per row.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).boundary().to_wkt()
        'LINESTRING (0 0, 2 0, 2 2, 0 2, 0 0)'
        """

    @overload
    def triangulate(
        self,
        *,
        method: Literal['earcut'],
        min_angle: None = None,
        max_area: None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def triangulate(
        self,
        *,
        method: Literal['delaunay'],
        min_angle: None = None,
        max_area: None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def triangulate(
        self,
        *,
        method: Literal['constrained'],
        min_angle: FloatInput | None = None,
        max_area: FloatInput | None = None,
    ) -> Groups[GeometryArray[Polygon]]:
        """Triangulate each geometry with an explicit algorithm.

        Parameters
        ----------
        method : {'earcut', 'delaunay', 'constrained'}
            Required triangulation algorithm.
        min_angle : float or sequence of float, optional
            Minimum triangle angle in degrees; valid only with
            ``method='constrained'``.

        max_area : float or sequence of float, optional
            Maximum triangle area in square coordinate units; valid only with
            ``method='constrained'``. Setting either option enables refinement;
            inserted Steiner vertices can return XY.

        Returns
        -------
        Groups of GeometryArray[Polygon]
            One ragged triangle group per input geometry.

        Raises
        ------
        GeometryError
            If method-specific options are used with the wrong algorithm.

        Examples
        --------
        >>> import gometry as gm
        >>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
        >>> groups[0].to_wkt()
        ['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 0, 2 2, 0 2, 2 0))']
        """

    def smooth(
        self,
        *,
        iterations: _IntInput = 2,
        method: SmoothMethod = 'chaikin',
        keep_endpoints: bool = True,
    ) -> Self:
        """Smooth line and polygon boundary linework (planar).
        Two algorithms, selected by ``method``:

        - ``'chaikin'`` (the default) applies corner-cutting quadratic B-spline
          refinement. Each iteration replaces every edge with points at
          one-quarter and three-quarters along it (~doubling vertices). Open
          lines honor ``keep_endpoints``; polygon rings are always treated as
          cyclic.
        - ``'catmull_rom'`` subdivides each segment with a centripetal Catmull-Rom
          cubic that passes through every original vertex. ``iterations`` sets the
          per-segment sample count as ``2**iterations`` (``0`` is identity).
          It always interpolates endpoints, so ``keep_endpoints=False`` is rejected.

        Parameters
        ----------
        iterations : int or sequence of int
            Smoothing strength — a scalar applies to every geometry, or pass one
            value per geometry. ``0`` returns the input unchanged.
        method : {'chaikin', 'catmull_rom'}, default 'chaikin'
            ``'chaikin'`` corner-cuts every edge; ``'catmull_rom'`` subdivides
            with a centripetal interpolating cubic.

        keep_endpoints : bool, default True
            For open lines under ``'chaikin'``, hold the first and last vertices
            fixed. Rings are cyclic; Catmull-Rom requires ``True``.

        Returns
        -------
        GeometryArray
            One smoothed geometry per row (kinds preserved).


        Raises
        ------
        GeometryError
            If ``iterations`` is negative, or if it would smooth the geometry to
            more coordinates than the output budget allows (a tiny input with a
            very large ``iterations``).

        Examples
        --------
        >>> import gometry as gm
        >>> square = gm.box(0, 0, 1, 1)
        >>> square.smooth(iterations=1).area < square.area
        True
        """

    def line_merge(self) -> GeometryArray[LineString | MultiLineString]:
        """Merge connected LineString parts into longer LineStrings.

        Returns
        -------
        GeometryArray
            One merged linework per row.


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.

        Examples
        --------
        >>> import gometry as gm
        >>> a, b = [(0, 0), (1, 1)], [(1, 1), (2, 2)]
        >>> (gm.MultiLineString([a, b]).line_merge()).to_wkt()
        'LINESTRING (0 0, 1 1, 2 2)'
        """

    def minimum_rotated_rectangle(self) -> GeometryArray[Point | LineString | Polygon]:
        """Minimum-area rotated bounding rectangle, returned in XY.

        Returns
        -------
        GeometryArray
            One rotated rectangle per row.


        Raises
        ------
        InvalidGeometryError
            If the rotated rectangle cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> rect = gm.box(0, 0, 2, 2).minimum_rotated_rectangle()
        >>> gm.equals(rect, gm.box(0, 0, 2, 2))
        True
        """

    def clip_by_rect(
        self,
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[Geometry]:
        """Clip a geometry to a rectangle ``(minx, miny, maxx, maxy)``.

        Source ordinates are carried where meaningful; synthesized clip vertices use
        the operation's natural XY result.

        Parameters
        ----------
        minx, miny, maxx, maxy : float
            The clip rectangle bounds.

        Returns
        -------
        GeometryArray
            One clipped geometry per row.


        Raises
        ------
        GeometryError
            If the rectangle bounds are non-finite or unordered.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (10, 0)]).clip_by_rect(2, -1, 5, 1).to_wkt()
        'LINESTRING (2 0, 5 0)'
        """

    def swap_xy(self) -> Self:
        """Swap the X and Y ordinate of every coordinate (Z/M untouched). The axis-
        order repair for data delivered latitude-first: gometry is always ``(x, y)``
        = ``(lon, lat)``, so latitude-ordered input swaps once on ingest.

        Returns
        -------
        GeometryArray
            One swapped geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> latitude_first = gm.Point(52.5, 13.4)
        >>> latitude_first.swap_xy().to_wkt()
        'POINT (13.4 52.5)'
        """

    @overload
    def line_substring(
        self,
        start: FloatInput,
        end: FloatInput,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[LineString | Point]: ...
    @overload
    def line_substring(
        self,
        start: FloatInput,
        end: FloatInput,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | Point]:
        """Return the portion of linework from ``start`` through ``end``.

        Parameters
        ----------
        start, end : float or sequence of float
            Ordered locations on the selected basis. Distance values follow the CRS;
            M values are stored route measures. A scalar applies to every array row.

        basis : {'distance', 'm'}, default 'distance'
            Use CRS-aware distance, or the line's monotonic M ordinate.

        normalized : bool, default False
            Interpret distance-basis positions as fractions in [0, 1]. Invalid with
            ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            Distance-basis unit override. Omitted follows the CRS; invalid with
            ``basis='m'``.

        Returns
        -------
        GeometryArray
            One substring per row.


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework is empty, or M values are missing or non-monotonic.
        GeometryError
            If locations are non-finite or out of order, or a distance-only option is
            used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a location along the line.
        line_locate : Project a geometry onto the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_substring(2, 6).to_wkt()
        'LINESTRING (2 0, 6 0)'
        """

    def buffer(
        self,
        distance: FloatInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon | MultiPolygon]:
        """Buffer a geometry by ``distance``, returning the offset region (measured for
        the CRS).

        Parameters
        ----------
        distance : float or sequence of float
            Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
            meters on a geographic CRS, native units on a projected one, coordinate
            units otherwise — a scalar applies to every geometry, or pass one value
            per geometry.
        cap_style : {'round', 'flat', 'square'}, default 'round'
            End-cap shape for open ends.

        join_style : {'round', 'miter', 'bevel'}, default 'round'
            Corner join shape.

        quadrant_segments : int, default 8
            Segments used to approximate a quarter circle.

        miter_limit : float, default 5.0
            With ``join_style='miter'``: how far a mitered corner may reach, in
            multiples of ``distance``; sharper corners are clipped flat at that
            reach. Must be positive and finite.

        side : {'both', 'left', 'right'}, default 'both'
            Which side(s) of lineal input to buffer. ``'left'``/``'right'`` build
            the one-sided strip between the line and its offset curve (flat ends,
            miter joins — ``offset_curve(join_style='miter')`` closed into a
            polygon); the style parameters apply to ``'both'`` only, and sided
            buffers take a non-negative distance.

            Buffer boundaries are synthesized and therefore returned in XY; Z/M are
            not fabricated.

        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            One offset region per row.


        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite.
        GeometryError
            If ``distance``/``quadrant_segments``/style parameters are invalid, or
            ``unit=meters`` is requested for a CRS-free geometry.
        GeometryTypeError
            If ``side`` is ``'left'``/``'right'`` and the geometry is not lineal.

        See Also
        --------
        offset_curve : One-sided raw parallel curve of a line.

        Examples
        --------
        >>> import gometry as gm
        >>> disc = gm.Point(0, 0).buffer(10)
        >>> round(disc.area)  # ~ pi * 10^2
        312
        """

    def unique_points(self) -> GeometryArray[MultiPoint]:
        """Distinct vertices in first-occurrence order. Vertices compare by exact
        structural identity (every active ordinate by bit pattern, the
        ``equals_identical`` notion), so XYZ points that differ only in Z stay
        distinct.

        Returns
        -------
        GeometryArray
            One ``MultiPoint`` of distinct vertices per row.

        See Also
        --------
        remove_repeated_points : Collapse consecutive duplicate vertices in place, keeping the geometry kind.

        Examples
        --------
        >>> import gometry as gm
        >>> loop = gm.LineString([(0, 0), (1, 1), (0, 0), (2, 2)])
        >>> loop.unique_points().to_wkt()
        'MULTIPOINT ((0 0), (1 1), (2 2))'
        """

    def simplify(
        self,
        tolerance: FloatInput,
        *,
        method: SimplifyMethod = 'vw',
        preserve_topology: bool = True,
    ) -> Self:
        """Simplify a geometry, dropping vertices below a tolerance (planar). Two
        algorithms, selected by ``method``, both reading ``tolerance`` on the
        same distance scale:

        - ``'vw'`` (Visvalingam-Whyatt, the default) removes the least visually
          significant vertices first — the smallest effective triangle spanned
          with its two neighbors — for a smoother, more natural cartographic
          result. The effective-area threshold is ``tolerance**2 / 2``.
        - ``'dp'`` (Douglas-Peucker) removes vertices whose perpendicular distance
          from the retained chord is within ``tolerance``.

        Parameters
        ----------
        tolerance : float or sequence of float
            Distance scale of removable detail, in coordinate units — a scalar
            applies to every geometry, or pass one value per geometry.
        method : {'vw', 'dp'}, default 'vw'
            ``'vw'`` is area-based (Visvalingam-Whyatt); ``'dp'`` is distance-
            based (Douglas-Peucker).

        preserve_topology : bool, default True
            Guarantee the output keeps the input's topology: a polygon stays valid
            and non-collapsed, a simple line stays simple. The raw algorithm runs
            first (the fast path); a guarded pass only kicks in when it broke
            something. ``False`` is the raw algorithm.

        Returns
        -------
        GeometryArray
            One simplified geometry per row (kinds preserved).


        Raises
        ------
        GeometryError
            If ``tolerance`` is negative or non-finite.

        See Also
        --------
        coverage_simplify : Topology-preserving simplification across a polygonal coverage.

        Examples
        --------
        >>> import gometry as gm
        >>> wiggly = gm.LineString([(0, 0), (1, 0.1), (2, -0.1), (3, 0)])
        >>> (wiggly.simplify(1.0)).to_wkt()
        'LINESTRING (0 0, 3 0)'
        """

    def coverage_is_valid(self, *, gap_width: float = 0.0) -> bool:
        """Test whether this polygonal coverage is valid.

        Parameters
        ----------
        gap_width : float, default 0.0
            Also flag boundaries that face a neighbor across a gap narrower
            than this (0 disables gap detection).

        Returns
        -------
        bool
            ``True`` when no row has invalid coverage edges.

        Raises
        ------
        GeometryTypeError
            If a row is not a `Polygon` or `MultiPolygon`.
        GeometryError
            If ``gap_width`` is negative or non-finite.

        See Also
        --------
        coverage_invalid_edges : The offending linework itself.
        coverage_clean : Rebuild an exact coverage from a near-coverage.

        Examples
        --------
        >>> import gometry as gm
        >>> grid = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
        >>> grid.coverage_is_valid()
        True
        >>> gm.GeometryArray([gm.box(0, 0, 1.1, 1), gm.box(1, 0, 2, 1)]).coverage_is_valid()
        False
        """

    def coverage_invalid_edges(
        self, *, gap_width: float = 0.0
    ) -> GeometryArray[LineString | MultiLineString]:
        """Per-row invalid coverage boundary linework (see `coverage_invalid_edges`).

        Parameters
        ----------
        gap_width : float, default 0.0
            Also flag boundaries that face a neighbor across a gap narrower
            than this (0 disables gap detection).

        Returns
        -------
        GeometryArray
            One `LineString`/`MultiLineString` per input row.

        Raises
        ------
        GeometryTypeError
            If a row is not a `Polygon` or `MultiPolygon`.
        GeometryError
            If ``gap_width`` is negative or non-finite.

        See Also
        --------
        coverage_is_valid : The boolean verdict.

        Examples
        --------
        >>> import gometry as gm
        >>> len(gm.GeometryArray(
        ...     [gm.box(0, 0, 1, 1), gm.box(0.5, 0, 1.5, 1)]
        ... ).coverage_invalid_edges())
        2
        """

    def coverage_simplify(
        self,
        tolerance: float,
        *,
        method: SimplifyMethod = 'vw',
        simplify_boundary: bool = True,
    ) -> GeometryArray[Polygon | MultiPolygon]:
        """Simplify this valid polygonal coverage's boundaries (see `coverage_simplify`).

        Parameters
        ----------
        tolerance : float
            Distance-scale simplification tolerance, in coordinate units;
            non-negative finite.
        method : {'vw', 'dp'}, default 'vw'
            Importance criterion: ``'vw'`` is area-based (Visvalingam-Whyatt),
            ``'dp'`` is distance-based (Douglas-Peucker).
        simplify_boundary : bool, default True
            Also simplify exterior (unshared) boundaries; ``False`` pins them
            and simplifies only the shared interfaces.

        Returns
        -------
        GeometryArray
            One simplified `Polygon`/`MultiPolygon` per input row.

        Raises
        ------
        GeometryTypeError
            If a row is not a `Polygon` or `MultiPolygon`.
        GeometryError
            If ``tolerance`` is negative or non-finite.
        InvalidGeometryError
            If the rows do not form a valid coverage.

        See Also
        --------
        Geometry.simplify : Per-geometry simplify (not coverage-topology-preserving).
        coverage_is_valid : Whether the rows form a valid polygonal coverage.

        Examples
        --------
        >>> import gometry as gm
        >>> left = gm.Polygon([(0, 0), (1, 0), (1.05, 0.5), (1, 1), (0, 1)])
        >>> right = gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1.05, 0.5)])
        >>> out = gm.GeometryArray([left, right]).coverage_simplify(0.5)
        >>> out.to_wkt()[0]
        'POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))'
        """

    def coverage_union(self) -> Polygon | MultiPolygon:
        """Union this polygonal coverage into one geometry (see `coverage_union`).

        Returns
        -------
        Geometry
            A single `Polygon`/`MultiPolygon` covering the merged area.

        Raises
        ------
        InvalidGeometryError
            If the array is empty or the rows do not form a valid coverage.

        See Also
        --------
        union_all : General multi-geometry union (handles overlaps).
        coverage_is_valid : Whether the rows form a valid polygonal coverage.

        Examples
        --------
        >>> import gometry as gm
        >>> tiles = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
        >>> tiles.coverage_union().normalize().to_wkt()
        'POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))'
        """

    def coverage_clean(
        self,
        *,
        grid_size: float = 0.0,
        gap_width: float = 0.0,
        overlap_rule: CoverageOverlapRule = 'longest_border',
    ) -> GeometryArray[Polygon | MultiPolygon]:
        """Clean this near-coverage into an exact polygonal coverage (see `coverage_clean`).

        Parameters
        ----------
        grid_size : float, default 0.0
            Vertex snapping grid in coordinate units; ``0`` preserves input
            coordinates and disables snapping.
        gap_width : float, default 0.0
            Merge enclosed gaps narrower than this into a neighbor (0 keeps
            gaps).
        overlap_rule : str, default 'longest_border'
            Which row keeps a region covered more than once:
            ``'longest_border'``, ``'max_area'``, ``'min_area'``, ``'min_index'``.
            Cleaning rebuilds faces and returns their natural 2D geometry.

        Returns
        -------
        GeometryArray
            One cleaned `Polygon`/`MultiPolygon` per input row.

        Raises
        ------
        GeometryTypeError
            If a row is not a `Polygon` or `MultiPolygon`.
        GeometryError
            If ``grid_size`` or ``gap_width`` is negative or non-finite.
        InvalidGeometryError
            If ``grid_size > 0`` and snap-repair cannot converge on a valid
            grid-aligned result.

        See Also
        --------
        coverage_is_valid : Test whether a polygonal coverage is already valid.

        Examples
        --------
        >>> import gometry as gm
        >>> rows = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
        >>> rows.coverage_is_valid()
        False
        >>> cleaned = rows.coverage_clean()
        >>> cleaned.coverage_is_valid()
        True
        """

    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Geometry]:
        """Repair invalid geometry, returning corrected result(s) (OGC). Already-valid
        input is returned unchanged at validation cost. Geographic antimeridian
        crossings are normalized before validity is decided, so a valid seam-crossing
        geometry is never destructively repaired; an invalid crossing repairs from its
        seam-split form. Projected and CRS-free geometry remains planar. Z/M ordinates
        are carried through the rebuild.

        Parameters
        ----------
        method : {'linework', 'structure'}, default linework
            Repair strategy: ``linework`` nodes all boundary linework and
            reassembles regions by even-odd parity, keeping every input edge;
            ``structure`` rebuilds each ring's enclosed area and recombines them
            as shells minus holes, discarding collapsed components.

            Z/M are carried at vertices traceable to the input; a rebuild that needs
            unsourceable vertices returns the mathematically natural XY result.

        Returns
        -------
        GeometryArray
            One valid geometry per row.


        Raises
        ------
        InvalidGeometryError
            If the geometry cannot be repaired.

        See Also
        --------
        validate : Structured validity report.
        is_valid : Boolean-only test.

        Examples
        --------
        >>> import gometry as gm
        >>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
        >>> fixed = bowtie.repair()
        >>> (fixed.is_valid, fixed.geometry_type)
        (True, 'MultiPolygon')
        """

    @overload
    def line_locate(
        self,
        geom: Geometry | GeometryArray,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def line_locate(
        self,
        geom: Geometry | GeometryArray,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.float64]:
        """Locate the position on linework nearest ``geom``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            A geometry to project, or one geometry per line row.

        basis : {'distance', 'm'}, default 'distance'
            Return a CRS-aware distance, or the line's monotonic M ordinate.

        normalized : bool, default False
            Return a distance-basis fraction in [0, 1]. Invalid with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            Distance-basis unit override. Omitted follows the CRS; invalid with
            ``basis='m'``.

        Returns
        -------
        numpy.ndarray
            One location per line row.


        Raises
        ------
        CRSError
            If the CRS cannot provide an unambiguous distance metric.
        CRSMismatchError
            If operands' CRS or coordinate-epoch metadata differ.
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If linework is empty, or M values are missing or non-monotonic.
        GeometryError
            If a distance-only option is used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a location along the line (inverse of locate).
        line_substring : Extract a contiguous portion of the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_locate(gm.Point(4, 3))
        4.0
        """

    def force_2d(self) -> Self:
        """Make each geometry planar, dropping any Z and M ordinates. Returns pure XY
        of the same geometry type and CRS. Already-2D input is returned unchanged.
        The one obvious way to flatten.

        Returns
        -------
        GeometryArray
            One XY-only geometry per row (kinds preserved).

        See Also
        --------
        force_3d : Add a Z ordinate, filling missing vertices.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.from_wkt('POINT Z (1 2 3)').force_2d().to_wkt()
        'POINT (1 2)'
        """

    def self_intersections(self) -> Groups[GeometryArray[Point]]:
        """Return points where the geometry coincides with itself. Reports proper linework
        self-crossings, non-adjacent touches, the endpoints of collinear overlaps
        (spikes and backtracks), contact between distinct parts, and duplicate point
        coordinates; legal adjacent shared vertices, ring closures, and removable
        repeated consecutive vertices are not nodes. For point/lineal input the
        result is non-empty exactly when is_simple is ``False``; areal input
        diagnoses its rings' linework, and collections are diagnosed recursively.
        Geographic antimeridian crossings use normalized topology; projected and
        CRS-free geometry remains planar. Points are XY only.

        Returns
        -------
        Groups
            One ``GeometryArray`` of distinct self-intersection points per
            element; missing rows yield an empty group.

        Examples
        --------
        >>> import gometry as gm
        >>> cross = gm.from_wkt('LINESTRING (0 0, 1 1, 1 0, 0 1)')
        >>> cross.self_intersections().to_wkt()
        ['POINT (0.5 0.5)']
        """

    def translate(self, x_offset: float, y_offset: float) -> Self:
        """Translate a geometry by ``(x_offset, y_offset)``.

        Parameters
        ----------
        x_offset, y_offset : float
            Offsets along the X and Y axes.

        Returns
        -------
        GeometryArray
            One transformed geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 2).translate(10, 20).to_wkt()
        'POINT (11 22)'
        """

    def minimum_clearance(
        self, *, unit: DistanceUnit | None = None
    ) -> npt.NDArray[np.float64]:
        """Smallest distance by which a vertex could move to invalidate the geometry.
        On a geographic CRS, the witness is selected in the geometry's best local
        projection and then measured geodesically in source coordinates; this is a
        local-projection approximation, not an exact ellipsoidal clearance search.
        Projected CRS and CRS-free geometries measure in the active planar units.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        numpy.ndarray
            One clearance distance per row.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 3, 2).minimum_clearance()
        2.0
        """

    @overload
    def line_interpolate(
        self,
        at: FloatInput,
        /,
        *,
        basis: Literal['distance'] = 'distance',
        count: None = None,
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self,
        at: None = None,
        /,
        *,
        count: _IntInput,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> Groups[GeometryArray[Point]]: ...
    @overload
    def line_interpolate(
        self,
        at: FloatInput,
        /,
        *,
        count: None = None,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[Point]:
        """Interpolate point locations along linework.

        Parameters
        ----------
        at : float or sequence of float, optional
            One location or many explicit distance-basis locations. Under
            ``basis='m'``, pass one stored M value (or one value per array row).


        count : int or iterable of int, optional
            Number of evenly spaced distance-basis samples per row (``>= 1``). A scalar
            broadcasts; otherwise pass one count per row. Mutually exclusive with
            ``at`` and unavailable with ``basis='m'``.

        basis : {'distance', 'm'}, default 'distance'
            Use CRS-aware distance, or the line's monotonic M ordinate.

        normalized : bool, default False
            Interpret distance-basis ``at`` values as fractions in [0, 1]. Invalid
            with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            Distance-basis unit override. Omitted follows the CRS; invalid with
            ``basis='m'``.

        Returns
        -------
        GeometryArray[Point] or Groups[GeometryArray[Point]]
            One point per row for scalar or row-aligned ``at``; one point group per
            row for ``count`` samples.


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework is empty, or M values are missing or non-monotonic.
        GeometryError
            If input forms conflict, a value is non-finite, or a distance-only option
            is used with ``basis='m'``.

        See Also
        --------
        line_locate : Project a geometry onto the line (inverse of interpolate).
        line_substring : Extract a contiguous portion of the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_interpolate(4).to_wkt()
        'POINT (4 0)'
        """

    def normalize(self) -> Self:
        """Return a geometry in canonical (normalized) form. The canonical form is the
        lexicographically smallest equivalent presentation: parts sort ascending,
        lines take their smaller direction (closed lines the smallest rotation), and
        polygon rings lead with their minimum vertex under RFC 7946 winding
        (exterior counter-clockwise, holes clockwise).

        Returns
        -------
        GeometryArray
            The canonical form of every element (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> messy = gm.from_wkt('MULTIPOINT ((1 1), (0 0))')
        >>> messy.normalize().to_wkt()
        'MULTIPOINT ((0 0), (1 1))'
        """

    def maximum_inscribed_circle(
        self,
        *,
        tolerance: FloatInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | Polygon]:
        """Largest circle inscribed in a polygonal geometry, as a filled disk.
        CRS-aware via local projection (approximate). Centered at the pole of
        inaccessibility (see polylabel), with radius reaching the nearest boundary
        point. Mirrors minimum_bounding_circle; the radius alone is
        maximum_inscribed_radius.

        Parameters
        ----------
        tolerance : float or sequence of float, optional
            Precision of the center search — a scalar applies to every geometry,
            or pass one value per geometry. Omitted selects a scale-aware tolerance
            independently for each geometry.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            The filled inscribed circle `Polygon` (or center `Point` when
            degenerate) per row.

        See Also
        --------
        polylabel : Pole of inaccessibility (the circle center alone).
        maximum_inscribed_radius : The radius alone.
        point_on_surface : A guaranteed-interior representative point.
        centroid : Area/length-weighted center (may fall outside).

        Raises
        ------
        InvalidGeometryError
            If the inscribed circle cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> disk = gm.box(0, 0, 2, 2).maximum_inscribed_circle()
        >>> (disk.geometry_type, round(disk.area, 2))
        ('Polygon', 3.14)
        """
    def maximum_inscribed_radius(
        self, *, tolerance: FloatInput | None = None, unit: DistanceUnit | None = None
    ) -> npt.NDArray[np.float64]:
        """Radius of the largest inscribed circle — the distance from the pole of
        inaccessibility (see polylabel) to the nearest boundary point. The numeric
        twin of minimum_bounding_radius; the circle itself is
        maximum_inscribed_circle.

        Parameters
        ----------
        tolerance : float or sequence of float, optional
            Precision of the center search — a scalar applies to every geometry,
            or pass one value per geometry. Omitted selects a scale-aware tolerance
            independently for each geometry.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        numpy.ndarray of float
            The inscribed radius per row; ``NaN`` where empty.


        See Also
        --------
        maximum_inscribed_circle : The filled inscribed circle (center and radius).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).maximum_inscribed_radius()
        1.0
        """

    def minimum_bounding_circle(
        self,
        *,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | Polygon]:
        """Smallest circle enclosing the geometry, as a polygon. The standard shape:
        the enclosing circle as a round 64-gon about the exact Welzl center and
        radius. A single distinct vertex returns itself; empty input returns
        ``POLYGON EMPTY``. CRS-aware via local projection (approximate) on a
        geographic CRS; projected CRS distances default to the CRS's native unit
        and scale through the CRS linear unit only with ``unit='meters'``.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            The smallest enclosing circle per row.


        Raises
        ------
        InvalidGeometryError
            If the bounding circle cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> pts = gm.MultiPoint([(0, 0), (4, 0)])
        >>> (pts.minimum_bounding_circle().geometry_type, pts.minimum_bounding_radius())
        ('Polygon', 2.0)
        """
    def minimum_bounding_radius(
        self, *, unit: DistanceUnit | None = None
    ) -> npt.NDArray[np.float64]:
        """Radius of the smallest circle enclosing the geometry. This is the numeric
        twin of minimum_bounding_circle: computed by the same Welzl center/support
        kernel without materializing the polygon. Empty input yields ``NaN``; a single
        distinct vertex yields ``0``. CRS-aware via local projection/ellipsoid support
        measurement (approximate for geographic point sets with three or more
        distinct vertices); two-point geographic inputs are the exact geodesic
        half-distance.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS. ``planar`` forces
            raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        numpy.ndarray
            One enclosing circle radius per row.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.MultiPoint([(0, 0), (4, 0)]).minimum_bounding_radius()
        2.0
        """

    def concave_hull(
        self,
        *,
        concavity: FloatInput = 2.0,
        length_threshold: FloatInput = 0.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | LineString | Polygon]:
        """Compute the concave hull of the geometry. CRS-aware via local projection
        (approximate) and does NOT auto-split antimeridian-crossing geographic input;
        call ``split_antimeridian`` first. Uses gometry's chi-shape kernel: Delaunay
        boundary triangles are peeled from longest edge to shortest, with output
        independent of input point order. Hull vertices are input vertices, so X/Y/Z/M
        ordinates are preserved exactly.

        Parameters
        ----------
        concavity : float or sequence of float, default 2.0
            Higher values are looser: fewer edges are peeled and area grows toward
            the convex hull. ``0`` disables the distance guard — a scalar applies
            to every geometry, or pass one value per geometry.
        length_threshold : float or sequence of float, default 0.0
            Boundary edges at or below this length are kept, so higher values also
            make the hull looser; interpreted for the CRS (see ``unit``). On a
            geographic CRS the threshold is evaluated in a local projection, while the
            output vertices are emitted from the original input coordinates — a scalar
            applies to every geometry, or pass one value per geometry.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            One concave hull per row; degenerate inputs reduce dimension.


        Raises
        ------
        GeometryError
            If parameters are non-finite.

        See Also
        --------
        convex_hull : Smallest convex set containing the geometry (planar).

        Examples
        --------
        >>> import gometry as gm
        >>> mp = gm.MultiPoint([(0, 0), (2, 0), (2, 2), (0, 2), (1, 0.2)])
        >>> hull = mp.concave_hull(concavity=1.0)
        >>> (hull.geometry_type, round(hull.area, 2))
        ('Polygon', 3.0)
        """

    def point_on_surface(self) -> GeometryArray[Point]:
        """Representative point guaranteed to lie on the geometry. Geographic (lon/lat)
        input crossing the antimeridian is auto-split-normalized; no manual
        ``split_antimeridian`` is required. Always inside (or on) the geometry,
        unlike centroid. The representative point is computed in XY and does not
        imply a source Z/M.

        See Also
        --------
        centroid : Area/length-weighted center (may fall outside).
        polylabel : Pole of inaccessibility (best label anchor).


        Returns
        -------
        GeometryArray[Point]
            One interior point per row.


        Raises
        ------
        InvalidGeometryError
            If a finite representative point cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> square = gm.box(0, 0, 2, 2)
        >>> gm.within(square.point_on_surface(), square)
        True
        """

    def subdivide(self, *, max_vertices: int = 256) -> Groups[GeometryArray[Geometry]]:
        """Split geometry into parts of bounded complexity. Recursively halves each
        geometry's bounds across the longer axis and clips until every part has at
        most ``max_vertices`` coordinates (the PostGIS ``ST_Subdivide`` shape).
        Parts cover the input exactly. Source ordinates are carried where meaningful;
        synthesized clip vertices use the operation's natural XY result.

        Parameters
        ----------
        max_vertices : int, default 256
            Maximum coordinates per part (at least ``8``).

        Returns
        -------
        Groups of GeometryArray
            One ragged group of parts per input geometry, in input order (row ``i``
            is ``self[i].subdivide(...)``).


        Raises
        ------
        GeometryError
            If ``max_vertices`` is below ``8``.

        Examples
        --------
        >>> import gometry as gm
        >>> parts = gm.LineString([(i, 0) for i in range(20)]).subdivide(max_vertices=8)
        >>> len(parts)
        4
        """

    def scale(
        self,
        x_factor: float,
        y_factor: float | None = None,
        *,
        origin: Origin = 'centroid',
    ) -> Self:
        """Scale a geometry about an origin.

        Parameters
        ----------
        x_factor, y_factor : float
            Scale factors along the X and Y axes; ``y_factor`` defaults to ``x_factor``.

        origin : str or sequence of float, optional
            Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
            y)`` point.

        Returns
        -------
        GeometryArray
            One transformed geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 1, 1).scale(2, 2, origin=(0, 0)).bounds
        (0.0, 0.0, 2.0, 2.0)
        """

    def extremes(self) -> Extremes[GeometryArray[Point]]:
        """Return the west, south, east, and north extreme vertices of the
        geometry (numeric X/Y; ties keep the first vertex in storage order).

        Returns
        -------
        Extremes
            Four row-aligned ``Point`` arrays as ``(west, south, east, north)``.
            Missing rows stay missing in every column; empty rows degrade to an
            empty point per column.


        Raises
        ------
        InvalidGeometryError
            If the geometry is empty.

        Examples
        --------
        >>> import gometry as gm
        >>> extremes = gm.box(0, 0, 2, 4).extremes()
        >>> assert extremes is not None  # None only for an empty geometry
        >>> (extremes.west.to_wkt(), extremes.north.to_wkt())
        ('POINT (0 0)', 'POINT (2 4)')
        """

    @overload
    def segmentize(
        self,
        max_length: FloatInput,
        /,
        *,
        fraction: None = None,
        unit: DistanceUnit | None = None,
    ) -> Self: ...
    @overload
    def segmentize(
        self, max_length: None = None, /, *, fraction: FloatInput, unit: None = None
    ) -> Self:
        """Densify linework by inserting vertices so no segment exceeds
        ``max_length`` (or a fraction of its length).

        ``max_length`` is a real-world distance measured for the CRS, exactly like
        ``length``: a geographic CRS subdivides along the ellipsoid in meters, a
        projected CRS uses its native linear units, and a CRS-free geometry uses
        coordinate units. Every original vertex survives unchanged — this operation
        only inserts.

        Parameters
        ----------
        max_length : float or sequence of float, optional
            Maximum segment length in coordinate units (positive) — a scalar
            applies to every geometry, or pass one value per geometry. Pass this
            positional argument or use ``fraction``, but not both.

        fraction : float or sequence of float, optional
            Fraction in ``(0, 1]`` of each source segment. Keyword-only; a scalar
            applies to every geometry, or pass one value per geometry.

        unit : {'planar', 'meters'} or None, default None
            ``None`` follows the CRS. ``'planar'`` forces raw coordinate units
            (degrees-as-Cartesian on a geographic CRS — only for deliberate
            coordinate-space math); ``'meters'`` forces the CRS metric and raises
            without a CRS. Cannot be combined with ``fraction``, which is already
            relative to each segment.

        Returns
        -------
        GeometryArray
            One segmentized geometry per row (kinds preserved).


        Raises
        ------
        CRSError
            If ``unit='meters'`` is requested and the CRS lacks linear axis units.
        GeometryError
            If neither or both constraints are supplied, ``max_length`` is not a
            positive finite number, ``fraction`` is outside ``(0, 1]``, ``unit`` is
            combined with ``fraction``, or ``unit='meters'`` is requested for a
            CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (4, 0)]).segmentize(2).to_wkt()
        'LINESTRING (0 0, 2 0, 4 0)'
        >>> # On a geographic CRS the bound is meters along the ellipsoid.
        >>> line = gm.LineString([(0, 0), (1, 0)], crs=4326)
        >>> len(list(line.segmentize(20_000).coords))
        7
        """

    def voronoi_polygons(
        self,
        *,
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> Groups[GeometryArray[Polygon]]:
        """Voronoi diagram polygons of the geometry's vertices. Operates in planar
        lon/lat space and does NOT auto-split antimeridian-crossing geographic
        input; call ``split_antimeridian`` first.

        Parameters
        ----------
        tolerance : float, default 0.0
            Tolerance in coordinate units (non-negative).
        clip : {'padded', 'envelope'} or Polygon, default 'padded'
            How to bound the unbounded outer cells: a padded box, the input
            envelope, or a `Polygon` to clip the diagram to.
            Diagram vertices are synthesized and returned in XY.

        Returns
        -------
        Groups of GeometryArray
            One ragged group of Voronoi cells per input geometry, in input order.


        Raises
        ------
        InvalidGeometryError
            If the Voronoi diagram cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
        >>> len(sites.voronoi_polygons())
        3
        """

    def split_antimeridian(self) -> GeometryArray[Geometry]:
        """Split at the antimeridian. Parts that cross come back as multiple parts
        whose edges follow the seam — each side keeping its own seam sign — so the
        result renders and computes correctly in lon/lat tools (the JOSS
        ``antimeridian`` algorithm). Crossings split at the great-circle latitude; a
        ring running off the seam closes over its pole automatically. Geometries
        that do not cross are returned unchanged. A split ``LineString`` becomes a
        ``MultiLineString`` and a split ``Polygon`` a ``MultiPolygon``, like
        repair. Seam vertices interpolate Z/M.

        Returns
        -------
        GeometryArray
            One seam-split geometry per row.


        Raises
        ------
        CRSError
            If the CRS is projected (CRS-free lon/lat and geographic CRS are
            accepted), or a coordinate is outside the longitude/latitude domain.
        InvalidGeometryError
            If stitching fails, or pole closure would have to invent Z/M
            ordinates.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(170, 0), (-170, 0)], crs=4326)
        >>> split = line.split_antimeridian()
        >>> (split.geometry_type, len(split.parts))
        ('MultiLineString', 2)
        """

    def offset_curve(
        self,
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | MultiLineString]:
        """Compute a line offset to one side of a linestring by ``distance`` (measured
        for the CRS).

        Parameters
        ----------
        distance : float or sequence of float
            Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
            CRS, native units on a projected one, coordinate units otherwise — a scalar applies to every
            geometry, or pass one value per geometry. The result is the RAW
            parallel curve: where the input folds back within ``distance`` the
            curve can self-intersect (GEOS trims such curls away);
            ``buffer(side=...)`` gives the trimmed area instead.
        join_style : {'round', 'miter', 'bevel'}, default 'round'
            Corner treatment at outside turns: ``'round'`` inscribes fillet arcs,
            ``'miter'`` extends the carriers to their crossing (clipped at
            ``miter_limit``), ``'bevel'`` connects the offsets directly.

        quadrant_segments : int, default 8
            Segments per quarter circle of every round join.

        miter_limit : float, default 5.0
            With ``join_style='miter'``: how far a mitered corner may reach, in
            multiples of ``distance``, before it is clipped flat.

        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
            forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
            — only for deliberate coordinate-space math); ``meters`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        GeometryArray
            One offset curve per row.


        Raises
        ------
        GeometryError
            If ``distance``/``quadrant_segments``/style parameters are invalid.

        See Also
        --------
        buffer : Offset region (optionally one-sided via ``side``).

        Examples
        --------
        >>> import gometry as gm
        >>> path = gm.LineString([(0, 0), (4, 0)])
        >>> (path.offset_curve(1)).to_wkt()
        'LINESTRING (0 1, 4 1)'
        """

    def reverse(self) -> Self:
        """Reverse the vertex order of a geometry.

        Returns
        -------
        GeometryArray
            One reversed geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (1, 1), (2, 2)])
        >>> line.reverse().to_wkt()
        'LINESTRING (2 2, 1 1, 0 0)'
        """

    def envelope(self) -> GeometryArray[Point | LineString | Polygon]:
        """Axis-aligned bounding-box polygon of the geometry, returned in XY.

        Returns
        -------
        GeometryArray
            One envelope per row.


        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (3, 1)]).envelope().to_wkt()
        'POLYGON ((0 0, 3 0, 3 1, 0 1, 0 0))'
        """

    def sample_points(
        self, count: _IntInput, *, seed: _IntInput
    ) -> Groups[GeometryArray[Point]]:
        """Random points on the geometry. The sample space is the geometry's highest
        dimension: uniform over area for areal input, along length for lineal input,
        and across the member points of a point set — falling back a dimension when
        the higher one is degenerate (a zero-area polygon samples its boundary),
        like centroid. Deterministic: the same input and ``seed`` always produce
        the same points (an explicit seed is required — no hidden global RNG). Array
        rows draw distinct deterministic streams derived from ``seed`` and the row
        index, and a scalar geometry IS row 0 — so ``arr.sample_points(n, seed=s)[0]``
        and ``arr[0].sample_points(n, seed=s)`` agree. An empty row yields an empty
        group rather than failing the batch; an empty SCALAR raises. Sampled points are invented interior points, so they cannot carry the
        source geometry's Z/M and are returned in XY.

        Parameters
        ----------
        count : int or iterable of int
            Number of points to draw per row (``>= 0``). A scalar broadcasts;
            otherwise pass one count per row.
        seed : int or iterable of int
            Seed for the deterministic sample stream. A scalar derives a distinct
            stream for every row; otherwise pass one explicit seed per row.


        Returns
        -------
        Groups
            One ``GeometryArray`` of ``count`` sampled points per row.


        Raises
        ------
        InvalidGeometryError
            If ``count > 0`` and a geometry is empty.
        GeometryError
            If ``count`` or ``seed`` is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> square = gm.box(0, 0, 10, 10)
        >>> pts = square.sample_points(5, seed=42)
        >>> (len(pts), all(p is not None and gm.within(p, square) for p in pts))
        (5, True)
        """

    def quantize(self, precision: int) -> Self:
        """Round coordinates to a fixed number of decimal places.

        Parameters
        ----------
        precision : int
            Decimal places to keep (``0``-``15``).

        Returns
        -------
        GeometryArray
            One rounded geometry per row (kinds preserved).


        Raises
        ------
        GeometryError
            If ``precision`` is outside ``0``-``15``.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(2.3479218, 48.8589321).quantize(3).to_wkt()
        'POINT (2.348 48.859)'
        """

    def spatial_key(
        self,
        *,
        curve: SpatialCurve = 'hilbert',
        level: int = 16,
        bounds: Iterable[float] | None = None,
    ) -> npt.NDArray[np.uint64]:
        """Space-filling-curve key of each geometry's bbox center.
        Discretizes centers into a ``2^level x 2^level`` grid over ``bounds``
        and returns distances along the selected curve.

        Parameters
        ----------
        curve : {'hilbert', 'morton'}, default hilbert
            ``hilbert`` prioritizes locality; ``morton`` uses Z-order.

        level : int, default 16
            Grid order (``1`` to ``32``); 16 matches GeoPandas/DuckDB.

        bounds : tuple of float, optional
            The frame ``(minx, miny, maxx, maxy)``; the array's total bounds
            when omitted. Keys compare across geometries only against a *shared*
            frame — pass the same ``bounds`` when keying separate geometries.

        Returns
        -------
        numpy.ndarray
            One result per row. Empty and missing rows use ``uint64.max``
            and therefore sort last.

        Raises
        ------
        GeometryError
            If ``level`` or ``bounds`` is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> keys = gm.GeometryArray([gm.Point(0, 0), gm.Point(10, 10)]).spatial_key(bounds=(0, 0, 10, 10))
        >>> bool(keys[0] != keys[1])
        True
        """

    def rotate(
        self, angle: float, *, origin: Origin = 'centroid', radians: bool = False
    ) -> Self:
        """Rotate a geometry about an origin.

        Parameters
        ----------
        angle : float
            Rotation angle (degrees by default; radians if ``radians=True``).

        origin : str or sequence of float, optional
            Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
            y)`` point.

        radians : bool, optional
            Interpret ``angle`` in radians instead of degrees.

        Returns
        -------
        GeometryArray
            One transformed geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1, 0).rotate(90, origin=(0, 0)).to_wkt(precision=6)
        'POINT (0 1)'
        """

    def affine_transform(self, matrix: _AffineMatrix) -> Self:
        """Apply a 2D affine transform ``(a, b, d, e, xoff, yoff)`` to a geometry.

        Parameters
        ----------
        matrix : sequence of float
            The six affine coefficients (a, b, d, e, xoff, yoff).

        Returns
        -------
        GeometryArray
            One transformed geometry per row (kinds preserved).


        Raises
        ------
        GeometryError
            If ``matrix`` is not 6 finite numbers.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).affine_transform([2, 0, 0, 2, 1, 1]).to_wkt()
        'LINESTRING (1 1, 3 3)'
        """

    def centroid(self) -> GeometryArray[Point]:
        """Area/length-weighted center of the geometry. Geographic (lon/lat) input
        crossing the antimeridian is auto-split-normalized; no manual
        ``split_antimeridian`` is required. The computed center is an XY point.

        Returns
        -------
        GeometryArray[Point]
            One centroid per row (area/length-weighted; may lie outside).

        See Also
        --------
        point_on_surface : A guaranteed-interior representative point.
        polylabel : Pole of inaccessibility (best label anchor).

        Raises
        ------
        InvalidGeometryError
            If a finite centroid cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> (gm.box(0, 0, 2, 4).centroid()).to_wkt()
        'POINT (1 2)'
        """

    def set_z(self, z: float | None) -> Self:
        """Set the Z ordinate at every vertex, or remove it. A numeric ``z`` writes
        that Z at every vertex (replacing any existing Z); ``None`` removes the Z
        ordinate. M passes through unchanged. To fill only the vertices that lack Z,
        use force_3d; to drop to XY, use force_2d.

        Parameters
        ----------
        z : float or None
            Z to assign at every vertex, or ``None`` to remove the Z ordinate.

        Returns
        -------
        GeometryArray
            One result per row (kinds preserved).

        See Also
        --------
        set_m : Set or clear the M ordinate.
        force_3d : Fill only the vertices that lack Z.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.set_z(30.0).to_wkt()[0]
        'POINT Z (1 2 30)'
        """

    def force_3d(self, z: float = 0.0) -> Self:
        """Make each geometry 3D, filling vertices that lack Z with ``z``. Vertices
        that already carry Z keep it; M passes through. The one obvious way to lift
        to 3D.

        Parameters
        ----------
        z : float, default 0.0
            Z to assign where it is missing.

        Returns
        -------
        GeometryArray
            One 3D geometry per row (kinds preserved).

        See Also
        --------
        force_2d : Drop Z and M to plain XY.
        set_z : Set the Z ordinate at every vertex (overwriting existing Z).

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.force_3d().to_wkt()[0]
        'POINT Z (1 2 0)'
        """

    def snap_to_grid(
        self,
        size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> GeometryArray[Geometry]:
        """Snap every coordinate onto a regular grid and clean the result. X/Y move to
        the nearest ``origin + k * size`` grid node; consecutive duplicate vertices
        collapse, and parts that degenerate below their minimum become empty (the
        PostGIS ``ST_SnapToGrid`` shape — output may be non-simple). Z/M ride on
        surviving vertices. quantize is the decimal-rounding, vertex-preserving
        sibling.

        Parameters
        ----------
        size : float or tuple of float
            Grid spacing — one value for a square grid or ``(sx, sy)`` (positive
            finite).

        origin : tuple of float, default (0.0, 0.0)
            A grid node anchoring the lattice.

        repair : bool, default False
            If ``True``, guarantee a valid result: snap, linework-repair, and re-
            snap to a fixpoint. The geometry kind may change (a ``Polygon`` whose
            snapped shell pinches splits into a ``MultiPolygon``). Geographic
            antimeridian crossings use normalized validity; projected and CRS-free
            geometry remains planar.

        Returns
        -------
        GeometryArray
            One snapped geometry per row.


        Raises
        ------
        GeometryError
            If ``size`` or ``origin`` is invalid, or the grid is too fine for the
            coordinate magnitude.
        InvalidGeometryError
            If ``repair=True`` and repair would have to invent Z/M ordinates, or
            the snap-repair loop cannot converge.

        Examples
        --------
        >>> import gometry as gm
        >>> jittery = gm.LineString([(0.12, 0.88), (2.49, 1.51)])
        >>> (jittery.snap_to_grid(0.5)).to_wkt()
        'LINESTRING (0 1, 2.5 1.5)'
        """

    def build_area(self) -> GeometryArray[Polygon | MultiPolygon]:
        """Assemble linework into one areal geometry. Input ordinates are carried
        where vertices can be sourced; otherwise the mathematically planar result is XY.

        Returns
        -------
        GeometryArray
            One areal geometry per row.


        Raises
        ------
        InvalidGeometryError
            If the area cannot be assembled from the input linework.

        Examples
        --------
        >>> import gometry as gm
        >>> edges = [[(0,0),(2,0)],[(2,0),(0,2)],[(0,2),(0,0)]]
        >>> gm.MultiLineString(edges).build_area().to_wkt()
        'POLYGON ((0 0, 2 0, 0 2, 0 0))'
        """

    def orient_polygons(self, *, ccw: bool = True) -> Self:
        """Orient polygon rings to a consistent winding.

        Parameters
        ----------
        ccw : bool, default True
            ``True`` (default): exterior CCW, holes CW; ``False`` flips.

        Returns
        -------
        GeometryArray
            One reoriented geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> cw = gm.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
        >>> cw.orient_polygons().exterior.is_ccw
        True
        """

    def polygonize(self) -> Groups[GeometryArray[Polygon]]:
        """Build polygons from a geometry's own noded linework. Each geometry is
        polygonized in isolation; to reconstruct polygons from a pile of edges pooled
        across many geometries, use free function ``polygonize`` on an iterable of
        values. Input ordinates are carried where possible; unsourceable noding seams
        yield XY.

        Returns
        -------
        Groups of GeometryArray
            One ragged group of polygons per input geometry, in input order — each
            input's OWN linework is polygonized independently (row ``i`` is
            ``self[i].polygonize()``). To pool ALL rows' edges into one graph so a
            ring can close across inputs, use the free function ``polygonize``.


        Raises
        ------
        InvalidGeometryError
            If polygons cannot be assembled from the noded linework.

        Examples
        --------
        >>> import gometry as gm
        >>> a, b = [(0, 0), (1, 0)], [(1, 0), (1, 1)]
        >>> edges = gm.MultiLineString([a, b, [(1, 1), (0, 0)]])
        >>> edges.polygonize().to_wkt()[0]
        'POLYGON ((0 0, 1 0, 1 1, 0 0))'
        """

    def convex_hull(
        self,
    ) -> GeometryArray[Point | LineString | Polygon | GeometryCollection]:
        """Compute the convex hull of the geometry. Operates in planar lon/lat space and does NOT
        auto-split antimeridian-crossing geographic input; call
        ``split_antimeridian`` first. Hull vertices are input vertices, so Z/M
        ordinates are preserved.

        Returns
        -------
        GeometryArray
            One convex hull per row.

        See Also
        --------
        concave_hull : Concave hull that can follow non-convex outlines.

        Examples
        --------
        >>> import gometry as gm
        >>> pts = gm.MultiPoint([(0, 0), (2, 0), (1, 1), (0, 2), (2, 2)])
        >>> pts.convex_hull().to_wkt()
        'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'
        """

    def validate(self) -> list[ValidationReport | None]:
        """Structured validity report in the geometry's coordinate frame.
        Geographic antimeridian crossings are normalized before validation;
        projected and CRS-free geometry uses ordinary planar OGC validity.

        Returns
        -------
        list of ValidationReport
            One report per row; missing rows are ``None``.

        See Also
        --------
        is_valid : Boolean-only test.
        repair : Fix what the report diagnoses.

        Examples
        --------
        >>> import gometry as gm
        >>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
        >>> report = bowtie.validate()
        >>> (report.valid, report.reason)
        (False, 'exterior ring has a self-intersection')
        """

    def node(self) -> GeometryArray[MultiLineString]:
        """Node linework by splitting every edge at all intersections. Input
        ordinates are carried where possible; unsourceable seam vertices yield XY.

        Returns
        -------
        GeometryArray
            Noded linework per row.


        Raises
        ------
        InvalidGeometryError
            If noding fails on the input linework.

        Examples
        --------
        >>> import gometry as gm
        >>> lines = gm.MultiLineString([[(0,0),(2,0)],[(1,-1),(1,1)]])
        >>> lines.node().to_wkt()
        'MULTILINESTRING ((0 0, 1 0), (1 0, 2 0), (1 -1, 1 0), (1 0, 1 1))'
        """

    def set_m(self, m: float | None) -> Self:
        """Set the M ordinate at every vertex, or remove it. A numeric ``m`` writes
        that M at every vertex (replacing any existing M); ``None`` removes the M
        ordinate. Z passes through unchanged.

        Parameters
        ----------
        m : float or None
            M to assign at every vertex, or ``None`` to remove the M ordinate.

        Returns
        -------
        GeometryArray
            One result per row (kinds preserved).

        See Also
        --------
        set_z : Set or clear the Z ordinate.
        interpolate_m : Assign M by interpolating along the linework.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.set_m(5.0).to_wkt()[0]
        'POINT M (1 2 5)'
        """

    def to_wkt(
        self,
        *,
        output_dimension: _WktOutputDimension | None = None,
        include_srid: bool = False,
        precision: int | None = None,
        drop_epoch: bool = False,
    ) -> list[str | None]:
        """Serialize to Well-Known Text.

        Parameters
        ----------
        output_dimension : int, optional
            Cap the written ordinate count (2, 3, or 4) to at most the
            geometry's own dimensionality; defaults to writing all present
            ordinates. Cannot invent Z/M that the geometry does not carry.

        include_srid : bool, default False
            Embed the EPSG code as an EWKT ``SRID=<code>;`` prefix. The PostGIS wire
            aliases ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979;
            decoding either alias yields that EPSG identity.

        precision : int, optional
            Decimal places to round coordinates to (omit for full precision).

        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which WKT cannot encode.

        Returns
        -------
        list of str
            One WKT string per row.


        Raises
        ------
        GeometryError
            If ``output_dimension`` is not 2, 3, or 4, or ``precision`` is not
            between 0 and 15, or the geometry carries a coordinate epoch and
            ``drop_epoch`` is false.
        CRSError
            If ``include_srid`` is set and the CRS has no EPSG code.

        See Also
        --------
        from_wkt : Parse WKT back into a geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(1.5, 2.5).to_wkt()
        'POINT (1.5 2.5)'
        >>> gm.GeometryArray([gm.Point(1.5, 2.5)]).to_wkt()
        ['POINT (1.5 2.5)']
        """

    def to_wkb(
        self,
        *,
        include_srid: bool = False,
        precision: int | None = None,
        drop_epoch: bool = False,
    ) -> list[bytes | None]:
        """Serialize to Well-Known Binary.

        Parameters
        ----------
        include_srid : bool, default False
            Embed the EPSG code as an EWKB SRID. The PostGIS wire aliases
            ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979; decoding
            either alias yields that EPSG identity.

        precision : int, optional
            Decimal places to round coordinates to (omit for full precision).

        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which (E)WKB cannot encode.

        Returns
        -------
        list of bytes
            One WKB payload per row.

        Notes
        -----
        The coordinate epoch is not representable in (E)WKB and does not survive a
        round-trip; use Arrow interchange when the epoch matters.

        Raises
        ------
        GeometryError
            If ``precision`` is not between 0 and 15, or the geometry carries a
            coordinate epoch and ``drop_epoch`` is false.
        CRSError
            If ``include_srid`` is set and the CRS has no EPSG code.

        See Also
        --------
        from_wkb : Parse WKB/EWKB back into a geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> pt = gm.Point(1, 2)
        >>> pt.to_wkt() == gm.from_wkb(pt.to_wkb()).to_wkt()
        True
        """

    def remove_repeated_points(self, *, tolerance: FloatInput = 0.0) -> Self:
        """Drop consecutive duplicate coordinates within ``tolerance``.

        Parameters
        ----------
        tolerance : float or sequence of float, default 0.0
            Tolerance in coordinate units (non-negative) — a scalar applies to
            every geometry, or pass one value per geometry.


        Returns
        -------
        GeometryArray
            One deduplicated geometry per row (kinds preserved).


        Raises
        ------
        GeometryError
            If ``tolerance`` is negative or non-finite.

        Examples
        --------
        >>> import gometry as gm
        >>> stuttery = gm.LineString([(0, 0), (0, 0), (1, 1)])
        >>> stuttery.remove_repeated_points().to_wkt()
        'LINESTRING (0 0, 1 1)'
        """

    def skew(
        self,
        x_angle: float = 0.0,
        y_angle: float = 0.0,
        *,
        origin: Origin = 'centroid',
        radians: bool = False,
    ) -> Self:
        """Skew (shear) a geometry about an origin.

        Parameters
        ----------
        x_angle, y_angle : float, default 0.0
            Shear angles along the X and Y axes (degrees by default; radians if
            ``radians=True``).

        origin : str or sequence of float, optional
            Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
            y)`` point.

        radians : bool, optional
            Interpret ``x_angle``/``y_angle`` in radians instead of degrees.

        Returns
        -------
        GeometryArray
            One transformed geometry per row (kinds preserved).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(0, 2).skew(x_angle=45, origin=(0, 0)).to_wkt(precision=6)
        'POINT (2 2)'
        """

    def voronoi_edges(
        self,
        *,
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> Groups[GeometryArray[LineString]]:
        """Voronoi diagram edges of the geometry's vertices. Operates in planar lon/lat
        space and does NOT auto-split antimeridian-crossing geographic input; call
        ``split_antimeridian`` first.

        Parameters
        ----------
        tolerance : float, default 0.0
            Tolerance in coordinate units (non-negative).
        clip : {'padded', 'envelope'} or Polygon, default 'padded'
            How to bound the unbounded outer cells: a padded box, the input
            envelope, or a `Polygon` to clip the diagram to.
            Diagram vertices are synthesized and returned in XY.

        Returns
        -------
        Groups of GeometryArray
            One ragged group of Voronoi edges per input geometry, in input order.


        Raises
        ------
        InvalidGeometryError
            If the Voronoi diagram cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
        >>> len(sites.voronoi_edges())
        3
        """

@final
class PreparedGeometry:
    """A geometry with a prebuilt edge index for repeated predicate tests.

    Returned by ``geom.prepare()``: the full predicate surface
    (``contains``/``intersects``/...) against one fixed geometry whose spatial
    structure is indexed once and reused; each call accepts a scalar or array
    of probes. Prefer it when the same geometry is tested across many separate
    calls — the array-broadcast surfaces already auto-prepare internally.
    """

    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Prepared geometries are returned by ``geom.prepare()``."""
    @property
    def geometry(self) -> Geometry:
        """Source geometry retained by this prepared handle.

        Returns
        -------
        Geometry
            The original typed geometry, sharing its immutable coordinate payload.
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the wrapper plus the source geometry's
        retained native cost (``ShapeData`` Arc, shape payload, and any
        prepared/frame caches already built on that shared handle). Calling
        this does not build new caches.
        """
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — the prepared handle is an
        immutable value (geometry + cached indexes), so a copy IS the
        original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    def __reduce__(self) -> tuple[object, tuple[Geometry]]:
        """Pickles as the source geometry plus a re-`prepare()` on load: the
        cached indexes are transient state, rebuilt cheaply on first use in
        the new process (`multiprocessing`/`dask` round-trips just work).
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    @overload
    def contains(self, geom: Geometry) -> bool: ...
    @overload
    def contains(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry contains ``geom``.

        Same definition as ``contains``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        contains : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().contains(gm.Point(1, 1))
        True
        """
    @overload
    def contains_properly(self, geom: Geometry) -> bool: ...
    @overload
    def contains_properly(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains_properly(
        self, geom: Geometry | GeometryArray
    ) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry contains ``geom`` properly.

        Same definition as ``contains_properly`` (no boundary contact).

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        contains_properly : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().contains_properly(gm.Point(1, 1))
        True
        """
    @overload
    def intersects(self, geom: Geometry) -> bool: ...
    @overload
    def intersects(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self, geom: Geometry | GeometryArray
    ) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry intersects ``geom``.

        Same definition as ``intersects``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        intersects : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().intersects(gm.Point(1, 1))
        True
        """
    @overload
    def within(self, geom: Geometry) -> bool: ...
    @overload
    def within(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def within(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry lies within ``geom``.

        Same definition as ``within``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        within : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().within(gm.box(-1, -1, 3, 3))
        True
        """
    @overload
    def covers(self, geom: Geometry) -> bool: ...
    @overload
    def covers(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def covers(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry covers ``geom``.

        Same definition as ``covers``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        covers : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().covers(gm.Point(0, 0))
        True
        """
    @overload
    def covered_by(self, geom: Geometry) -> bool: ...
    @overload
    def covered_by(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def covered_by(
        self, geom: Geometry | GeometryArray
    ) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry is covered by ``geom``.

        Same definition as ``covered_by``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        covered_by : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().covered_by(gm.box(-1, -1, 3, 3))
        True
        """
    @overload
    def disjoint(self, geom: Geometry) -> bool: ...
    @overload
    def disjoint(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def disjoint(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry is disjoint from ``geom``.

        Same definition as ``disjoint``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        disjoint : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().disjoint(gm.Point(5, 5))
        True
        """
    @overload
    def touches(self, geom: Geometry) -> bool: ...
    @overload
    def touches(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def touches(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry touches ``geom``.

        Same definition as ``touches``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        touches : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().touches(gm.Point(0, 1))
        True
        """
    @overload
    def crosses(self, geom: Geometry) -> bool: ...
    @overload
    def crosses(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def crosses(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry crosses ``geom``.

        Same definition as ``crosses``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        crosses : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().crosses(gm.LineString([(-1, 1), (3, 1)]))
        True
        """
    @overload
    def overlaps(self, geom: Geometry) -> bool: ...
    @overload
    def overlaps(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def overlaps(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry overlaps ``geom``.

        Same definition as ``overlaps``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        overlaps : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().overlaps(gm.box(1, 1, 3, 3))
        True
        """
    @overload
    def equals(self, geom: Geometry) -> bool: ...
    @overload
    def equals(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def equals(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry is topologically equal to ``geom``.

        Same definition as ``equals``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Probe geometry or array of probes; must share the prepared
            geometry's CRS. A scalar gives one ``bool``; an array gives one
            result per row.

        Returns
        -------
        bool or numpy.ndarray
            Whether the relation holds; one result per input.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.

        See Also
        --------
        equals : Free-function form of the same predicate.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().equals(gm.box(0, 0, 2, 2))
        True
        """
    @overload
    def dwithin(
        self, geom: Geometry, distance: float, *, unit: DistanceUnit | None = None
    ) -> bool: ...
    @overload
    def dwithin(
        self, geom: GeometryArray, distance: float, *, unit: DistanceUnit | None = None
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def dwithin(
        self,
        geom: Geometry | GeometryArray,
        distance: float,
        *,
        unit: DistanceUnit | None = None,
    ) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry is within ``distance`` of ``geom``.

        Same definition as ``dwithin``.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            Geometry (or array) to test; must share this geometry's CRS.
        distance : float
            Non-negative threshold.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
            geographic CRS — only for deliberate coordinate-space math);
            ``'meters'`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        bool or numpy.ndarray
            One result per input geometry.

        Raises
        ------
        CRSMismatchError
            If the operands' CRS or coordinate-epoch metadata differ.
        GeometryError
            If ``distance`` is negative or non-finite, or ``unit='meters'`` is
            requested for a CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().dwithin(gm.Point(3, 0), 1.0)
        True
        """
    @overload
    def contains_xy(self, x: float, y: float) -> bool: ...
    @overload
    def contains_xy(self, x: FloatColumn, y: FloatInput) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains_xy(self, x: FloatInput, y: FloatColumn) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains_xy(self, x: FloatInput, y: FloatInput) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry contains each ``(x, y)`` point.

        Parameters
        ----------
        x, y : float or sequence of float
            Finite coordinates in the prepared geometry's CRS. Geographic
            antimeridian seams and poles use full point-predicate topology.

        Returns
        -------
        bool or numpy.ndarray
            A single bool for scalar ``x, y``, or one result per coordinate.

        Raises
        ------
        InvalidGeometryError
            If ``x``/``y`` are non-finite or differ in length.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().contains_xy(1, 1)
        True
        """
    @overload
    def intersects_xy(self, x: float, y: float) -> bool: ...
    @overload
    def intersects_xy(self, x: FloatColumn, y: FloatInput) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects_xy(self, x: FloatInput, y: FloatColumn) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects_xy(
        self, x: FloatInput, y: FloatInput
    ) -> bool | npt.NDArray[np.bool_]:
        """Test whether this prepared geometry intersects each ``(x, y)`` point.

        Boundary-inclusive (unlike ``contains_xy``).

        Parameters
        ----------
        x, y : float or sequence of float
            Finite coordinates in the prepared geometry's CRS. Geographic
            antimeridian seams and poles use full point-predicate topology.

        Returns
        -------
        bool or numpy.ndarray
            A single bool for scalar ``x, y``, or one result per coordinate.

        Raises
        ------
        InvalidGeometryError
            If ``x``/``y`` are non-finite or differ in length.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().intersects_xy(3, 3)
        False
        """
    def explain(self) -> list[str]:
        """Describe the prepared-predicate plan.

        Returns
        -------
        list of str
            One line per plan step.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).prepare().explain()[0]
        'prepared geometry: Polygon'
        """

@final
class Groups(Sequence[_GroupValuesT_co], Generic[_GroupValuesT_co]):
    """Shared CSR ragged container: one flat `values` payload plus row `offsets`.
    ``groups[i]`` is a zero-copy row view; ``groups[s]`` shares the backing
    with a sub-offset window. ``.values``/``.offsets``/``.counts`` expose the
    Arrow ListArray columns for vectorized work without copying.
    """

    __array_ufunc__: ClassVar[None]
    __hash__: ClassVar[None]  # type: ignore[assignment]
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Not constructed directly — the error points at the producers."""
    def __class_getitem__(cls, key: Any) -> types.GenericAlias:
        """See PEP 585"""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the wrapper plus this group's logical CSR
        payload. Sliced groups report the visible values window and rebased
        logical offsets, not the whole shared backing allocation.
        """
    @property
    def nbytes(self) -> int:
        """Logical CSR payload in bytes: the selected flat values payload plus the
        ``len(self) + 1`` int64 offsets column. For geometry-valued groups this
        uses the values ``GeometryArray.nbytes`` and excludes geometry
        structural offsets, matching NumPy's payload-only convention.

        Returns
        -------
        int
        """
    @property
    def values(self) -> _GroupValuesT_co:
        """The flat backing column (int64 ndarray or `GeometryArray`)."""
    @property
    def offsets(self) -> npt.NDArray[np.int64]:
        """The `len(self) + 1` row boundaries into ``values`` (CSR offsets
        column): row ``i`` is ``values[offsets[i]:offsets[i + 1]]``.
        """
    @property
    def counts(self) -> npt.NDArray[np.int64]:
        """Per-group element counts (`offsets[i + 1] - offsets[i]`)."""
    def __len__(self) -> int:
        """Number of row groups.

        Returns
        -------
        int
        """
    def __bool__(self) -> bool:
        """``False`` only when there are zero groups.

        Returns
        -------
        bool
        """
    def __iter__(self) -> Iterator[_GroupValuesT_co]:
        """Iterate one row group at a time.

        Returns
        -------
        iterator
        """
    def __reversed__(self) -> Iterator[_GroupValuesT_co]:
        """Iterate row groups in reverse order.

        Returns
        -------
        iterator
        """
    def __contains__(self, item: object, /) -> bool:
        """Whether any group equals ``item`` (whole-row value equality).

        Returns
        -------
        bool
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of an equal row in ``[start, stop)``.

        Parameters
        ----------
        value : object
            The row value to locate.
        start : int, default 0
            First position searched.
        stop : int, optional
            One past the last position searched.

        Returns
        -------
        int
            The first matching position.

        Raises
        ------
        ValueError
            If no row in the window equals ``value``.
        """
    def count(self, value: object) -> int:
        """Number of rows equal to ``value``.

        Parameters
        ----------
        value : object
            The row value to count.

        Returns
        -------
        int
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _GroupValuesT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> Groups[_GroupValuesT_co]: ...
    @overload
    def __getitem__(
        self, index: SupportsIndex | slice, /
    ) -> _GroupValuesT_co | Groups[_GroupValuesT_co]:
        """Select groups by integer or slice.

        An ``int`` returns one group's values (for example an ``int64``
        ndarray of matched ids). A ``slice`` returns a rebased ``Groups``.

        Returns
        -------
        numpy.ndarray or Groups
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    @overload
    def to_list(self: Groups[npt.NDArray[np.int64]]) -> list[list[int]]: ...
    @overload
    def to_list(self: Groups[GeometryArray[_GeometryT]]) -> list[list[_GeometryT]]: ...
    @overload
    def to_list(self: Groups[CellArray[_CellT]]) -> list[list[_CellT]]: ...
    @overload
    def to_list(self) -> list[list[int]] | list[list[Geometry]] | list[list[Cell]]:
        """Copy into a plain nested Python list.

        Returns
        -------
        list of list
            Materialized rows of the grouped values.

        Examples
        --------
        >>> import gometry as gm
        >>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
        >>> [g.to_wkt() for g in groups.to_list()[0]]
        ['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 0, 2 2, 0 2, 2 0))']
        """
    def to_pairs(
        self: Groups[npt.NDArray[np.int64]],
    ) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
        """Expand integer CSR rows into parallel ``(row_ids, values)`` columns.

        The right column is a zero-copy read-only view of the flat CSR values;
        only the repeated row-id column is materialized. Row ids are positions
        in this logical ``Groups`` object, so sliced groups start again at zero.

        Returns
        -------
        tuple of numpy.ndarray
            Parallel read-only int64 ``(row_ids, values)`` columns.

        Raises
        ------
        TypeError
            If the groups contain geometry or cell rows rather than integers.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([
        ...     gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3), gm.box(10, 10, 11, 11)])
        >>> row_ids, values = idx.query(
        ...     gm.GeometryArray([gm.Point(1.5, 1.5), gm.Point(10.5, 10.5)])
        ... ).to_pairs()
        >>> (row_ids.tolist(), values.tolist())
        ([0, 0, 1], [0, 1, 2])
        """

@final
class GroupsIterator(Iterator[_GroupValuesT_co]):
    """Lazy row iterator for [`Groups`]."""
    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """
    def __next__(self) -> _GroupValuesT_co:
        """Implement next(self)."""

@final
class SpatialIndexIterator(Iterator[int]):
    """Lazy ascending-handle iterator over a live index. It scans the sparse handle
    table on demand instead of collecting and sorting the live entries before
    the first result. Mutation invalidates iteration, matching mapping iterator
    semantics and keeping ``__length_hint__`` exact.
    """
    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __length_hint__(self) -> int: ...
    def __next__(self) -> int:
        """Implement next(self)."""

@final
class SpatialIndex(Mapping[int, Geometry]):
    """A packed STR-tree over geometries sharing one CRS/epoch frame.

    Built by ``SpatialIndex(geoms)``: ask set questions against the indexed
    geometries — exact predicate matches (``idx.query(geom)``), bounding-box
    candidates (``idx.candidates(geom)``), proximity (``idx.nearest(geom)``),
    self-joins (``idx.query_pairs()``) — and mutate it incrementally with
    ``insert``/``remove``. Distances follow the indexed CRS: meters on a
    geographic frame, native linear units on a projected frame, coordinate
    units when CRS-free.
    """
    def __new__(
        cls,
        values: Geometry | GeometryArray | Iterable[_GeometryLike | None] | None = None,
    ) -> SpatialIndex:
        """Build a spatial index (STR-tree) over present geometries.

        Parameters
        ----------
        values : GeometryArray, iterable of Geometry or None, default None
            Geometries to index. ``None`` builds an empty mutable index for
            later ``insert`` calls. Every indexed geometry must share one CRS
            and coordinate epoch. Missing rows are skipped but retain their
            original positions as stable, non-live handles, so query and join
            results always refer to the input row ids.

        Raises
        ------
        CRSMismatchError
            If items carry conflicting CRS or coordinate-epoch metadata.

        See Also
        --------
        join : High-level spatial join built on the index.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6)])
        >>> [int(i) for i in idx.query(gm.Point(0.5, 0.5), predicate='intersects')]
        [0]
        """
    def __sizeof__(self) -> int:
        """`sys.getsizeof` support: the wrapper plus the retained Rust-side
        index payload — packed or boxed row geometry coordinates, the immutable
        STR tree, overflow R-tree entries, frame metadata, and any built
        geodesic cap cache. Shared buffers are reported as this index's
        logical retained footprint.
        """
    @property
    def crs(self) -> CRS | None:
        """CRS shared by the indexed geometries, or ``None`` for an unframed index.

        Returns
        -------
        CRS or None
        """
    @property
    def epoch(self) -> float | None:
        """Coordinate epoch shared by the indexed geometries, if set.

        Returns
        -------
        float or None
        """
    def __len__(self) -> int:
        """Number of live (non-removed) geometries in the index.

        Returns
        -------
        int
        """
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle as the full sparse row table plus live handles.

        Handles are the public identity returned by ``query``/``nearest`` and
        consumed by ``remove``/``__getitem__``; round-tripping must therefore
        preserve tombstones instead of compactly renumbering live rows.
        """
    def __copy__(self) -> Self: ...
    def __deepcopy__(self, memo: object) -> Self: ...
    def __getitem__(self, handle: int, /) -> Geometry:
        """Return the geometry at a live handle.

        Raises ``KeyError`` when the handle is unknown or has been removed.

        Returns
        -------
        Geometry
        """
    def __contains__(self, handle: object, /) -> bool:
        """Whether ``handle`` is a live geometry handle.

        Non-integer probes return ``False`` instead of raising, matching
        Python's container protocol.

        Returns
        -------
        bool
        """
    def __iter__(self) -> Iterator[int]:
        """Iterate live handles lazily in ascending handle order.

        Returns
        -------
        iterator of int
        """
    def keys(self) -> KeysView[int]:
        """Return a dynamic view of the live handles.

        Returns
        -------
        KeysView
            The live handles, in ascending order.

        Examples
        --------
        >>> import gometry as gm
        >>> list(gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]).keys())
        [0, 1]
        """
    def values(self) -> ValuesView[Geometry]:
        """Return a dynamic view of the geometries at live handles.

        Returns
        -------
        ValuesView
            One geometry per live handle, in handle order.

        Examples
        --------
        >>> import gometry as gm
        >>> list(gm.SpatialIndex([gm.box(0, 0, 1, 1)]).values())[0].to_wkt()
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
        """
    def items(self) -> ItemsView[int, Geometry]:
        """Return a dynamic view of `(handle, geometry)` pairs.

        Returns
        -------
        ItemsView
            ``(handle, geometry)`` pairs for the live handles.

        Examples
        --------
        >>> import gometry as gm
        >>> list(gm.SpatialIndex([gm.box(0, 0, 1, 1)]).items())[0][0]
        0
        """
    @overload
    def get(self, handle: int, /) -> Geometry | None: ...
    @overload
    def get(
        self, handle: int, /, default: Geometry | _DefaultT
    ) -> Geometry | _DefaultT:
        """Return the geometry at handle, or default when it is not live.

        Parameters
        ----------
        handle : int
            A row handle (positional-only; handles are integers, so a
            non-integer probe raises ``TypeError`` like ``Mapping.get``).

        default : object, optional
            Value returned when the handle is not live.

        Returns
        -------
        Geometry or object
            The live geometry, else ``default``.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
        >>> g = idx.get(0)
        >>> g is not None and g.to_wkt().startswith('POLYGON')
        True
        """
    @overload
    def query(
        self,
        geom: Geometry,
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.int64]: ...
    @overload
    def query(
        self,
        geom: GeometryArray,
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> Groups[npt.NDArray[np.int64]]: ...
    @overload
    def query(
        self,
        geom: Geometry,
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.int64]: ...
    @overload
    def query(
        self,
        geom: GeometryArray,
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> Groups[npt.NDArray[np.int64]]:
        """Return exact predicate-refined matches for a query geometry or array.

        Candidates are refined with predicate (`'intersects'` by
        default); distance is accepted only with `'dwithin'`. Use
        candidates for a bounding-box-only prefilter. A single geometry
        returns an int64 ndarray; a GeometryArray returns
        Groups — one id row per row, CSR-grouped.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            The query geometry, or one query per array element.
        predicate : str, default 'intersects'
            Spatial relation each match must satisfy (``'dwithin'`` requires
            ``distance``).
        distance : float, optional
            ``'dwithin'`` distance threshold, in ``unit``.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
            geographic CRS — only for deliberate coordinate-space math);
            ``'meters'`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        int64 numpy.ndarray or Groups
            Matching index handles (row ids). A scalar query returns a read-only
            ``int64`` ndarray of handles; an array query returns ``Groups`` of
            handles, one group per query row.

        Raises
        ------
        CRSMismatchError
            If the query does not share the index's CRS/epoch frame.
        GeometryError
            If a query parameter is invalid, or ``unit='meters'`` is requested
            for a CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> idx.query(gm.Point(0.5, 0.5)).tolist()
        [0]
        """
    @overload
    def join(
        self,
        queries: Geometry | GeometryArray | Iterable[_GeometryLike],
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]: ...
    @overload
    def join(
        self,
        queries: Geometry | GeometryArray | Iterable[_GeometryLike],
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
        """Join query rows against this prebuilt index.

        Reuses the index instead of rebuilding the right side on every call.
        Predicate orientation is ``predicate(query, indexed_geometry)``, exactly
        matching free-function ``join(queries, indexed_values, ...)``. Missing query rows
        produce no pairs; missing rows skipped while building the index retain
        their original handles.

        Parameters
        ----------
        queries : Geometry, GeometryArray, or iterable of Geometry
            Left-side geometries to join against the indexed rows.
        predicate : str, default 'intersects'
            Spatial predicate each returned pair must satisfy.
        distance : float, optional
            Required when ``predicate='dwithin'``: the maximum separation in
            CRS-natural units.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units; ``'meters'`` forces the
            CRS metric and raises without a CRS.

        Returns
        -------
        tuple of numpy.ndarray
            ``(query_ids, handles)`` parallel read-only int64 columns.

        Raises
        ------
        CRSMismatchError
            If the query and index CRS/coordinate-epoch frames differ.
        GeometryError
            If a predicate or distance option is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> left, right = idx.join(gm.GeometryArray([gm.Point(0.5, 0.5)]))
        >>> (left.tolist(), right.tolist())
        ([0], [0])
        """
    @overload
    def query_pairs(
        self,
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]: ...
    @overload
    def query_pairs(
        self,
        *,
        predicate: SymmetricTopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
        """All index pairs ``(i, j)`` with ``i < j`` whose geometries satisfy a
        symmetric ``predicate`` — a self-join over the index.

        Parameters
        ----------
        predicate : str, default 'intersects'
            A symmetric relation: ``'intersects'``, ``'equals'``,
            ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
            Directional predicates (``'contains'``, ``'within'``, ...) are
            rejected — unordered pairs would drop the reverse direction; use
            ``join(...)`` for directed relations.
        distance : float, optional
            ``'dwithin'`` distance threshold, in ``unit``.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
            geographic CRS — only for deliberate coordinate-space math);
            ``'meters'`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        tuple of numpy.ndarray
            ``(left, right)`` parallel int64 row-id columns.

        Raises
        ------
        CRSMismatchError
            If indexed items carry conflicting CRS/epoch frames.
        GeometryError
            If ``predicate`` is unknown or directional, ``distance`` is missing
            or invalid for ``predicate='dwithin'``, or ``unit='meters'`` is
            requested for a CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> left, right = idx.query_pairs()
        >>> (left.tolist(), right.tolist())
        ([], [])
        """
    @overload
    def candidates(
        self,
        geom: Geometry,
        *,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.int64]: ...
    @overload
    def candidates(
        self,
        geom: GeometryArray,
        *,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> Groups[npt.NDArray[np.int64]]: ...
    @overload
    def candidates(
        self,
        geom: Geometry | GeometryArray,
        *,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.int64] | Groups[npt.NDArray[np.int64]]:
        """Bounding-box candidate matches for a query geometry or array (not
        exact).

        A single geometry returns an `int64` ndarray; a `GeometryArray`
        returns `Groups` — one candidate row per row.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            The query geometry, or one query per array element.
        distance : float, optional
            Expand the query envelope by this much, in ``unit``.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
            geographic CRS — only for deliberate coordinate-space math);
            ``'meters'`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        int64 numpy.ndarray or Groups

        Raises
        ------
        CRSMismatchError
            If the query does not share the index's CRS/epoch frame.
        GeometryError
            If a query parameter is invalid, or ``unit='meters'`` is requested
            for a CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> idx.candidates(gm.box(0, 0, 1, 1)).tolist()
        [0]
        """
    @overload
    def explain(
        self,
        geom: Geometry | None = None,
        *,
        predicate: None = None,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> list[str]: ...
    @overload
    def explain(
        self,
        geom: Geometry | None = None,
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> list[str]: ...
    @overload
    def explain(
        self,
        geom: Geometry | None = None,
        *,
        predicate: TopologicalPredicate,
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> list[str]:
        """Describe the query plan steps.

        Parameters
        ----------
        geom : Geometry, optional
            The query to plan for; omitted, the index itself is described.
        predicate : str, optional
            Spatial relation the plan would refine with; omitted, the plan
            stops at the candidate filter.
        distance : float, optional
            ``'dwithin'`` distance threshold (or envelope expansion), in ``unit``.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
            geographic CRS — only for deliberate coordinate-space math);
            ``'meters'`` forces the CRS metric and raises without a CRS.

        Returns
        -------
        list of str
            One line per plan step.

        Raises
        ------
        CRSMismatchError
            If the query does not share the index's CRS/epoch frame.
        GeometryError
            If ``predicate`` is unknown, ``distance`` is invalid, or
            ``unit='meters'`` is requested for a CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]).explain()[0]
        'loaded 2 geometries'
        >>> gm.SpatialIndex([gm.box(0, 0, 1, 1)]).explain()[0]
        'loaded 1 geometry'
        """
    @overload
    def nearest(
        self,
        geom: Geometry,
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[False] = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> npt.NDArray[np.int64]: ...
    @overload
    def nearest(
        self,
        geom: Geometry,
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[True],
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]: ...
    @overload
    def nearest(
        self,
        geom: GeometryArray,
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[False] = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> Groups[npt.NDArray[np.int64]]: ...
    @overload
    def nearest(
        self,
        geom: GeometryArray,
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[True],
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> tuple[Groups[npt.NDArray[np.int64]], npt.NDArray[np.float64]]: ...
    @overload
    def nearest(
        self,
        geom: Geometry | GeometryArray,
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: bool = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> (
        npt.NDArray[np.int64]
        | Groups[npt.NDArray[np.int64]]
        | tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]
        | tuple[Groups[npt.NDArray[np.int64]], npt.NDArray[np.float64]]
    ):
        """Nearest indexed geometries to the query.

        Parameters
        ----------
        geom : Geometry or GeometryArray
            The query geometry, or one query per array element.
        k : int, default 1
            How many nearest neighbors to return.
        max_distance : float, optional
            Ignore matches farther than this, in ``unit``.
        return_distance : bool, default False
            Return distances alongside handles — ``(indices, distances)`` for
            a scalar query, ``(matches, distances)`` for an array query.
        unit : {'planar', 'meters'}, default None
            Omitted follows the CRS: geodesic meters on a geographic CRS, native
            units on a projected one, coordinate units without a CRS.
            ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
            geographic CRS — only for deliberate coordinate-space math);
            ``'meters'`` forces the CRS metric and raises without a CRS.
        exclusive : bool, default False
            Skip an indexed geometry equal to the query (self-matches in
            joins over the indexed set itself).
        ties : bool, default False
            Also return every geometry TYING the k-th nearest distance
            (exact comparison) — results can then exceed ``k``.

        Returns
        -------
        int64 numpy.ndarray, Groups, or tuple
            The nearest handles — an `int64` ndarray for a scalar query,
            CSR `Groups` for an array query. With ``return_distance=True``,
            plain tuple field order is ``(indices, distances)`` for a scalar
            query or ``(matches, distances)`` for an array query (distances
            parallel to ``matches.values``).

        Raises
        ------
        CRSMismatchError
            If the query does not share the index's CRS/epoch frame.
        GeometryError
            If ``k`` or ``max_distance`` is invalid, or ``unit='meters'`` is
            requested for a CRS-free geometry.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> idx.nearest(gm.Point(4, 4)).tolist()
        [1]
        """
    @overload
    def insert(self, geom: Geometry) -> int: ...
    @overload
    def insert(
        self, geom: GeometryArray | Iterable[_GeometryLike]
    ) -> npt.NDArray[np.int64]: ...
    @overload
    def insert(
        self, geom: Geometry | GeometryArray | Iterable[_GeometryLike]
    ) -> int | npt.NDArray[np.int64]:
        """Insert one geometry or many geometries and return their stable handles.

        A single `Geometry` returns one ``int`` handle. A `GeometryArray` or
        generic iterable of geometries returns a read-only `int64` ndarray of
        handles in input order. Batch inserts follow the same frame and envelope rules as scalar
        insert: the first inserted row fixes an empty index's CRS/epoch frame,
        later inserts must match it, and geographic antimeridian-crossing rows use
        the wrapped-band envelope required by ``query_pairs``.

        Parameters
        ----------
        geom : Geometry or GeometryArray or iterable of Geometry
            Values to append to the index; all must share the
            index's CRS/epoch frame. Empty geometries cannot be inserted.

        Returns
        -------
        int or numpy.ndarray
            Stable handle for a scalar insert, or stable handles assigned to a
            batch insert in input order as a read-only int64 ndarray.

        Raises
        ------
        CRSMismatchError
            If the geometry or geometries do not share the index's CRS/epoch
            frame.
        GeometryError
            If any inserted geometry is empty.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([])
        >>> idx.insert(gm.Point(1, 1))
        0
        """
    def remove(self, handle: int) -> bool:
        """Remove a geometry by its handle. Returns ``True`` if a live geometry
        was removed, ``False`` if the handle is unknown or was already removed.
        Removed handles are not reused, so surviving handles stay stable.

        Parameters
        ----------
        handle : int
            The handle returned by ``insert`` (or a position from building).

        Returns
        -------
        bool
            Whether a live geometry was removed.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([])
        >>> handle = idx.insert(gm.Point(1, 1))
        >>> idx.remove(handle)
        True
        """

@final
class CRS:
    """Coordinate reference system.

    A PROJ-backed CRS object: introspect it (``crs.is_geographic``,
    ``crs.ellipsoid``), serialize it (``crs.to_wkt()``, ``crs.to_epsg()``),
    compute on it (``crs.factors(...)``, ``crs.geodesic(...)``), and attach it
    to geometries via ``to_crs``. Accepts an authority string, EPSG code,
    authority tuple, PROJJSON/CF mapping, WKT/PROJ string, or another ``CRS``.

    Equality is structural — a ``CRS`` compares equal only to the same
    canonical stored label (``crs == 4326`` and ``crs == 'EPSG:4326'``).
    Operational equivalence, including axis-order-only differences, is the
    explicit ``same_as(..., mode='ignore_axis_order')`` query. It is therefore
    unhashable; key mappings by ``crs.canonical`` instead.
    """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the wrapper plus owned Rust-side CRS text
        retained by this object. This includes the canonical CRS string when it
        spills out of `SmolStr` inline storage and any heap strings/vectors in
        the lazily cached `CrsInfo`. Opaque PROJ process-global caches are not
        owned by the Python object and are not counted.
        """
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — a CRS is an immutable
        value, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    def __reduce__(self) -> tuple[type[CRS], tuple[str]]:
        """Pickle support: a CRS is its canonical string; the constructor
        rebuilds everything else lazily.
        """
    def _repr_html_(self) -> str:
        """HTML preview for notebooks: a compact table of `info` fields."""
    def __new__(cls, value: CrsInput) -> Self:
        """Build a CRS from any accepted input.

        Parameters
        ----------
        value : CRS-like or CRS
            Authority string, EPSG code, authority tuple, PROJJSON/CF mapping,
            WKT/PROJ string, CRS-holder object, or another ``CRS``.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError
            If the value is not a recognized CRS.
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    __hash__: ClassVar[None]  # type: ignore[assignment]
    @property
    def is_geographic(self) -> bool:
        """Whether this CRS is geographic (lon/lat on an ellipsoid).

        For a compound CRS this is ``True`` when **any component** is
        geographic (not root-kind only) — e.g. ``EPSG:9707`` (WGS 84 + height).

        Returns
        -------
        bool
        """
    @property
    def is_projected(self) -> bool:
        """Whether this CRS is projected (planar, metric).

        For a compound CRS this is ``True`` when **any component** is
        projected (not root-kind only).

        Returns
        -------
        bool
        """
    @property
    def is_vertical(self) -> bool:
        """Whether this CRS is vertical (height/depth only).

        For a compound CRS this is ``True`` when **any component** is
        vertical (not root-kind only).

        Returns
        -------
        bool
        """
    @property
    def is_geocentric(self) -> bool:
        """Whether this CRS is geocentric (earth-centered Cartesian).

        For a compound CRS this is ``True`` when **any component** is
        geocentric (not root-kind only).

        Returns
        -------
        bool
        """
    @property
    def is_compound(self) -> bool:
        """Whether this CRS is compound (horizontal + vertical).

        Returns
        -------
        bool
        """
    @property
    def is_engineering(self) -> bool:
        """Whether this CRS is engineering (local/non-geodetic).

        Returns
        -------
        bool
        """
    @property
    def is_bound(self) -> bool:
        """Whether this CRS is a bound CRS (carries a datum transform).

        Returns
        -------
        bool
        """
    @property
    def is_deprecated(self) -> bool:
        """Whether this CRS is flagged deprecated by its authority.

        Returns
        -------
        bool
        """
    @property
    def is_derived(self) -> bool:
        """Whether this CRS is derived from a base CRS.

        Returns
        -------
        bool
        """
    @property
    def kind(self) -> CrsKind:
        """CRS kind as a snake_case token (``"geographic_2d"``, ``"geographic_3d"``,
        ``"projected"``, ``"geocentric"``, ``"vertical"``, ``"compound"``,
        ``"bound"``, ``"other"``, ...).

        Returns
        -------
        str
        """
    @property
    def name(self) -> str | None:
        """Human-readable CRS name, if known.

        Returns
        -------
        str or None
        """
    @property
    def authority(self) -> str | None:
        """Registry authority (e.g. ``"EPSG"``), if identified.

        Returns
        -------
        str or None
        """
    @property
    def code(self) -> str | None:
        """Authority code (e.g. ``"4326"``), if identified.

        Returns
        -------
        str or None
        """
    @property
    def canonical(self) -> str:
        """Canonical identifier string for this CRS.

        Returns
        -------
        str
        """
    @property
    def axis_order(self) -> list[str]:
        """Axis roles in CRS order (lowercase, e.g. ``["lat", "lon"]`` for a
        lat/lon CRS; tokens ``"lat"``/``"lon"``/``"x"``/``"y"``/``"z"``/
        ``"height"``/``"other"``). For the raw PROJ abbreviations use ``axes``.

        Returns
        -------
        list of str
        """
    @property
    def celestial_body(self) -> str | None:
        """Name of the celestial body (usually ``"Earth"``), if known.

        Returns
        -------
        str or None
        """
    @property
    def axes(self) -> list[CrsAxisInfo]:
        """Coordinate-system axes as a list of dicts.

        Returns
        -------
        list of CrsAxisInfo
        """
    @property
    def area_of_use(self) -> CrsAreaOfUse | None:
        """Area of use as a ``{west, south, east, north, name}`` dict, or ``None``.

        Returns
        -------
        CrsAreaOfUse or None
        """
    @property
    def ellipsoid(self) -> CrsEllipsoidInfo | None:
        """Reference ellipsoid as a dict, or ``None``.

        Returns
        -------
        CrsEllipsoidInfo or None
        """
    @property
    def datum(self) -> CrsDatumInfo | None:
        """Geodetic datum as a dict, or ``None``.

        Returns
        -------
        CrsDatumInfo or None
        """
    @property
    def prime_meridian(self) -> CrsPrimeMeridianInfo | None:
        """Prime meridian as a dict, or ``None``.

        Returns
        -------
        CrsPrimeMeridianInfo or None
        """
    @property
    def geodetic_crs(self) -> CrsAuthorityObject | None:
        """Underlying geodetic CRS as an authority-object dict, or ``None``.

        Returns
        -------
        CrsAuthorityObject or None
        """
    @property
    def info(self) -> CrsInfo:
        """Full raw PROJ description as a dict (escape hatch).

        Returns
        -------
        CrsInfo
        """
    def to_epsg(self, *, min_confidence: int = 70) -> int | None:
        """EPSG integer code, or ``None`` if it cannot be determined.

        Parameters
        ----------
        min_confidence : int, optional
            Minimum identification confidence (0-100, default 70).

        Returns
        -------
        int or None

        Raises
        ------
        CRSError
            If ``min_confidence`` is outside ``0``-``100``.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_epsg()
        4326
        """
    def to_authority(
        self, *, authority: str | None = None, min_confidence: int = 70
    ) -> tuple[str, str] | None:
        """``(authority, code)`` pair, or ``None`` if it cannot be determined.

        Parameters
        ----------
        authority : str, optional
            Restrict identification to this authority.

        min_confidence : int, optional
            Minimum identification confidence (0-100, default 70).

        Returns
        -------
        tuple or None

        Raises
        ------
        CRSError
            If ``min_confidence`` is outside ``0``-``100``.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_authority()
        ('EPSG', '4326')
        """
    def to_wkt(
        self,
        *,
        version: WktVersion = 'WKT2_2019',
        pretty: bool = False,
        output_axis: WktAxisRule = 'auto',
        strict: bool = True,
        indentation_width: int = 4,
    ) -> str:
        """Serialize this CRS to WKT.

        Parameters
        ----------
        version : str, optional
            WKT dialect (default ``"WKT2_2019"``).

        pretty : bool, optional
            Indent the output (default ``False``).

        output_axis : str, default "auto"
            Axis-output policy (``"auto"``, ``"yes"``, ``"no"``).

        strict : bool, optional
            Fail on lossy output (default ``True``).

        indentation_width : int, default 4
            Spaces per indent level when ``pretty``.

        Returns
        -------
        str

        Raises
        ------
        CRSError
            If export fails or a formatting option is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_wkt().startswith('GEOGCRS')
        True
        """
    def to_proj(
        self,
        *,
        version: Literal[4, 5] = 5,
        pretty: bool = False,
        approximate_tmerc: bool = False,
        indentation_width: int = 2,
        max_line_length: int = 80,
    ) -> str:
        """Serialize this CRS to a PROJ string.

        Parameters
        ----------
        version : int, optional
            PROJ string version (4 or 5).

        pretty : bool, optional
            Indent the output (default ``False``).

        approximate_tmerc : bool, optional
            Use the approximate transverse-Mercator formulation.

        indentation_width : int, default 2
            Spaces per indent level when ``pretty``.

        max_line_length : int, optional
            Wrap lines at this width.

        Returns
        -------
        str

        Raises
        ------
        CRSError
            If export fails or a formatting option is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> '+proj=longlat' in gm.CRS(4326).to_proj()
        True
        """
    def to_projjson(self, *, pretty: bool = False, indentation_width: int = 2) -> str:
        """Serialize this CRS to a PROJJSON string.

        Parameters
        ----------
        pretty : bool, optional
            Indent the output (default ``False``).

        indentation_width : int, default 2
            Spaces per indent level when ``pretty``.

        Returns
        -------
        str

        Raises
        ------
        CRSError
            If export fails or a formatting option is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> 'GeographicCRS' in gm.CRS(4326).to_projjson()
        True
        """
    def to_projjson_dict(self) -> dict[str, object]:
        """Serialize this CRS to a PROJJSON ``dict``.

        Returns
        -------
        dict[str, object]

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_projjson_dict()['type']
        'GeographicCRS'
        """
    def to_cf(self, *, wkt_version: WktVersion = 'WKT2_2019') -> CrsCfInfo:
        """CF (Climate and Forecast) grid-mapping attributes as a dict.

        Parameters
        ----------
        wkt_version : str, optional
            WKT dialect embedded in ``crs_wkt`` (default ``"WKT2_2019"``).

        Returns
        -------
        CrsCfInfo

        Raises
        ------
        CRSError
            If export fails.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_cf().get('grid_mapping_name')
        'latitude_longitude'
        """
    def to_2d(self, *, name: str | None = None) -> CRS:
        """Return the 2D (horizontal) form of this CRS.

        Converts the CRS definition by removing the ellipsoidal-height axis;
        it does not add or remove Z ordinates on geometries — use
        `force_2d`/`force_3d` or `set_z` for that.

        Parameters
        ----------
        name : str, optional
            Name for the derived CRS.

        Returns
        -------
        CRS

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4979).to_2d().to_epsg()
        4326
        """
    def to_3d(self, *, name: str | None = None) -> CRS:
        """Return the 3D form of this CRS (adds an ellipsoidal height axis).

        Converts the CRS definition only; it does not add or remove Z
        ordinates on geometries — use `force_2d`/`force_3d` or `set_z` for
        that.

        Parameters
        ----------
        name : str, optional
            Name for the derived CRS.

        Returns
        -------
        CRS

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_3d().to_epsg()
        4979
        """
    def same_as(self, other: CrsInput, *, mode: CrsComparison) -> bool:
        """Test whether this CRS describes the same system as ``other``.

        Parameters
        ----------
        other : str or int or CRS
            The other CRS-like value to compare against.

        mode : str
            One of ``'ignore_axis_order'``
            (same but axis-swapped CRS count as equal), or ``'exact'``
            (strict, detail-identical match).

        Returns
        -------
        bool

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).same_as(gm.CRS(4326), mode='exact')
        True
        """
    def identify(self, *, authority: str | None = None) -> list[CrsIdentifyCandidate]:
        """Candidate authority matches for this CRS, best first.

        Parameters
        ----------
        authority : str, optional
            Restrict matches to this authority.

        Returns
        -------
        list of CrsIdentifyCandidate

        Raises
        ------
        CRSError
            If identification fails.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).identify()[0]['code']
        '4326'
        """
    def non_deprecated(self) -> list[CrsAuthorityObject]:
        """Non-deprecated authority objects equivalent to this CRS.

        Returns
        -------
        list of CrsAuthorityObject

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).non_deprecated()
        []
        """
    def geoid_models(self) -> list[str]:
        """Geoid model names available for this CRS.

        Returns
        -------
        list of str

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).geoid_models()
        []
        """
    @overload
    def factors(
        self, lon: float, lat: float, *, radians: bool = False
    ) -> CrsProjectionFactors: ...
    @overload
    def factors(
        self,
        lon: FloatInput,
        lat: FloatInput,
        *,
        radians: bool = False,
    ) -> CrsProjectionFactorsBatch:
        """Map-projection factors (scale, distortion, Tissot, …) at a lon/lat.

        For a **projected** CRS the evaluation point is geographic lon/lat on
        the base ellipsoid and the returned scales/distortions describe the
        projection at that location. For a pure geographic CRS the factors are
        near-identity (meridional/parallel scale ≈ 1).

        Parameters
        ----------
        lon : float or sequence of float
            Longitude of the evaluation point(s), degrees unless
            ``radians=True``.

        lat : float or sequence of float
            Latitude of the evaluation point(s), degrees unless
            ``radians=True``.

        radians : bool, optional
            Interpret angular inputs as radians (default ``False``).

        Returns
        -------
        dict
            Scalar call: mapping of factor names to floats
            (``meridional_scale``, ``parallel_scale``, ``areal_scale``,
            ``angular_distortion``, ``meridian_parallel_angle``,
            ``meridian_convergence``, ``tissot_semimajor``,
            ``tissot_semiminor``, ``dx_dlam``, ``dx_dphi``, ``dy_dlam``,
            ``dy_dphi``). Array call: same keys with float arrays.

        Raises
        ------
        InvalidGeometryError
            If ``lon``/``lat`` are non-finite or differ in length.

        Examples
        --------
        >>> import gometry as gm
        >>> f = gm.CRS(3857).factors(0.0, 0.0)
        >>> round(f['meridional_scale'], 5)
        1.0
        """
    @overload
    def geodesic(
        self,
        lon1: float,
        lat1: float,
        lon2: float,
        lat2: float,
        z1: float | None = None,
        z2: float | None = None,
        *,
        radians: bool = False,
    ) -> CrsGeodesicInfo: ...
    @overload
    def geodesic(
        self,
        lon1: FloatInput,
        lat1: FloatInput,
        lon2: FloatInput,
        lat2: FloatInput,
        z1: FloatInput | None = None,
        z2: FloatInput | None = None,
        *,
        radians: bool = False,
    ) -> CrsGeodesicBatchInfo:
        """Compute the geodesic inverse solution between two points on this CRS's ellipsoid.

        Parameters
        ----------
        lon1, lat1 : float
            First point.

        lon2, lat2 : float
            Second point.

        z1, z2 : float, optional
            Heights for a 3D (slant) distance.

        radians : bool, optional
            Interpret angular inputs as radians (default ``False``).

        Returns
        -------
        CrsGeodesicInfo or CrsGeodesicBatchInfo

        Raises
        ------
        InvalidGeometryError
            If coordinate columns are non-finite, differ in length, or only
            one of ``z1``/``z2`` is given.
        GeometryError
            If the CRS does not expose a usable ellipsoid for geodesic use.

        Examples
        --------
        >>> import gometry as gm
        >>> round(gm.CRS(4326).geodesic(-122.4, 37.8, -122.3, 37.9)['distance'])
        14165
        """
    @overload
    def geodesic_direct(
        self,
        lon: float,
        lat: float,
        azimuth: float,
        distance: float,
        *,
        radians: bool = False,
    ) -> CrsGeodesicDirectInfo: ...
    @overload
    def geodesic_direct(
        self,
        lon: FloatInput,
        lat: FloatInput,
        azimuth: FloatInput,
        distance: FloatInput,
        *,
        radians: bool = False,
    ) -> CrsGeodesicDirectBatchInfo:
        """Compute the geodesic direct solution: project a point along an azimuth.

        Parameters
        ----------
        lon, lat : float
            Start point.

        azimuth : float
            Forward azimuth in degrees (or radians if ``radians``).

        distance : float
            Geodesic distance in meters.

        radians : bool, optional
            Interpret/return angular values as radians (default ``False``).

        Returns
        -------
        CrsGeodesicDirectInfo or CrsGeodesicDirectBatchInfo

        Raises
        ------
        CRSError
            If the value is not a recognized CRS.
        InvalidGeometryError
            If coordinate columns are non-finite or differ in length.

        Examples
        --------
        >>> import gometry as gm
        >>> d = gm.CRS(4326).geodesic_direct(-122.4, 37.8, 45.0, 1000.0)
        >>> (round(d['longitude'], 5), round(d['latitude'], 5))
        (-122.39197, 37.80637)
        """
    @overload
    def geodesic_interpolate(
        self,
        lon1: float,
        lat1: float,
        lon2: float,
        lat2: float,
        distance: float,
        *,
        normalized: bool = False,
        radians: bool = False,
    ) -> CrsGeodesicInterpolateInfo: ...
    @overload
    def geodesic_interpolate(
        self,
        lon1: FloatInput,
        lat1: FloatInput,
        lon2: FloatInput,
        lat2: FloatInput,
        distance: FloatInput,
        *,
        normalized: bool = False,
        radians: bool = False,
    ) -> CrsGeodesicInterpolateBatchInfo:
        """Interpolate a point a given distance along the geodesic between two
        points.

        Parameters
        ----------
        lon1, lat1, lon2, lat2 : float
            Endpoints of the geodesic.

        distance : float
            Distance from the first point, in meters (or a fraction if
            ``normalized``).

        normalized : bool, default False
            Treat ``distance`` as a fraction of the total length (default
            ``False``).

        radians : bool, optional
            Interpret/return angular values as radians (default ``False``).

        Returns
        -------
        CrsGeodesicInterpolateInfo or CrsGeodesicInterpolateBatchInfo

        Raises
        ------
        CRSError
            If the value is not a recognized CRS.
        InvalidGeometryError
            If coordinate columns are non-finite or differ in length.

        Examples
        --------
        >>> import gometry as gm
        >>> mid = gm.CRS(4326).geodesic_interpolate(
        ...     -122.4, 37.8, -122.3, 37.9, 0.5, normalized=True)
        >>> (round(mid['longitude'], 5), round(mid['latitude'], 5))
        (-122.35003, 37.85001)
        """
    def operation(
        self,
        target: CrsInput,
        *,
        at: tuple[float, float]
        | tuple[float, float, float]
        | tuple[float, float, float, float]
        | None = None,
        area_of_interest: CrsAreaInput | None = None,
        source_epoch: float | None = None,
        target_epoch: float | None = None,
        authority: str | None = None,
        accuracy: float | None = None,
        allow_ballpark: bool | None = None,
        only_best: bool | None = None,
        force_over: bool = False,
    ) -> CrsOperationInfo:
        """Best coordinate operation from this CRS to ``target``.

        Parameters
        ----------
        target : str or int or CRS
            Destination CRS.

        at : tuple of float, optional
            Coordinate at which to select the best operation: ``(x, y)``,
            ``(x, y, z)``, or ``(x, y, z, t)`` in the source CRS. This is an
            alternative to a broader ``area_of_interest``.

        area_of_interest : sequence of float, optional
            ``(west, south, east, north)`` area of interest.

        source_epoch, target_epoch : float, optional
            Coordinate epochs for dynamic CRS.

        authority : str, optional
            Restrict candidate coordinate operations to this authority
            (e.g. ``'EPSG'``).

        accuracy : float, optional
            Maximum acceptable operation accuracy, in meters.

        allow_ballpark : bool, optional
            Allow low-accuracy ballpark operations when no precise one exists.

        only_best : bool, optional
            Require PROJ's best operation. If a required transformation grid is
            unavailable, raise ``TransformError`` instead of using a less
            accurate fallback operation.

        force_over : bool, optional
            Keep coordinates on the source side of the antimeridian instead of
            wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
            ``only_best``, this also collapses operation selection to a single
            candidate, so enumerating surfaces return exactly one operation.

        Returns
        -------
        CrsOperationInfo

        Raises
        ------
        CRSError
            If the CRS arguments are unrecognized.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).operation(3857)['name']
        'pipeline'
        """
    def operations(
        self,
        target: CrsInput,
        *,
        area_of_interest: CrsAreaInput | None = None,
        source_epoch: float | None = None,
        target_epoch: float | None = None,
        authority: str | None = None,
        accuracy: float | None = None,
        allow_ballpark: bool | None = None,
        only_best: bool | None = None,
        force_over: bool = False,
    ) -> list[CrsOperationInfo]:
        """All candidate operations from this CRS to ``target``, best first.

        Parameters
        ----------
        target : str or int or CRS
            Destination CRS.

        area_of_interest : sequence of float, optional
            ``(west, south, east, north)`` area of interest.

        source_epoch, target_epoch : float, optional
            Coordinate epochs for dynamic CRS.

        authority : str, optional
            Restrict candidate coordinate operations to this authority
            (e.g. ``'EPSG'``).

        accuracy : float, optional
            Maximum acceptable operation accuracy, in meters.

        allow_ballpark : bool, optional
            Allow low-accuracy ballpark operations when no precise one exists.

        only_best : bool, optional
            Require PROJ's best operation. If a required transformation grid is
            unavailable, raise ``TransformError`` instead of using a less
            accurate fallback operation.

        force_over : bool, optional
            Keep coordinates on the source side of the antimeridian instead of
            wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
            ``only_best``, this also collapses operation selection to a single
            candidate, so enumerating surfaces return exactly one operation.

        Returns
        -------
        list of CrsOperationInfo

        Examples
        --------
        >>> import gometry as gm
        >>> len(gm.CRS(4326).operations(3857)) >= 1
        True
        """

# Element type of a `CellArray`; covariant + PEP 696 default so a bare
# `CellArray` means `CellArray[Cell]` (the public cell protocol). No
# `bound=Cell`: see `_CellT` — recursive Protocol + TypeVar bound is circular.
_CellT_co = TypeVar('_CellT_co', covariant=True, default=Cell)

@final
class CellArrayIterator(Generic[_CellT_co]):
    """Lazy iterator over a `CellArray`'s typed cells (both directions)."""
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """

    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> _CellT_co:
        """Implement next(self)."""

@final
class CellArray(Sequence[_CellT_co]):
    """An immutable array of one grid cell type backed by a shared ``uint64`` id
    column.

    Index with an integer for the typed cell object; slice or mask for a new
    `CellArray`. Build from a non-empty homogeneous iterable of typed cell
    objects. For raw ids, tokens, arrays, buffers, or empty inputs, pass
    ``type=`` explicitly; every id is validated for that grid.

    Parameters
    ----------
    values : numpy.ndarray or iterable
        Typed cell objects, or raw ids/tokens when ``type`` is supplied.
    type : type, optional
        Native cell class. Inferred only from non-empty homogeneous typed cells.
    """

    __array_ufunc__: ClassVar[None]
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the wrapper plus this array's logical
        ``uint64`` id payload and any row-selection map. Shared backing buffers
        are reported like NumPy views, not as the full parent allocation.
        """
    def __reduce__(self) -> tuple[Any, tuple[list[int] | list[str], str]]:
        """Pickle support: round-trip through the id column and grid token."""
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — a CellArray is an immutable
        value, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __bool__(self) -> bool:
        """``False`` only when the array is empty.

        Returns
        -------
        bool
        """
    def __contains__(self, value: object, /) -> bool:
        """Whether a cell id / cell object appears in the array.

        Returns
        -------
        bool
        """
    def __reversed__(self) -> CellArrayIterator[_CellT_co]:
        """Iterate cells in reverse row order.

        Returns
        -------
        iterator of Cell
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of an equal cell id in `[start, stop)`.

        Parameters
        ----------
        value : cell object or int id
            The element to locate.
        start : int, default 0
            First position searched.
        stop : int, optional
            One past the last position searched (the array length when
            omitted).

        Returns
        -------
        int
            The first matching position.

        Raises
        ------
        ValueError
            If no element in the window equals ``value``.
        """
    def count(self, value: object) -> int:
        """Number of elements with the same cell id.

        Parameters
        ----------
        value : cell object or int id
            The element to count.

        Returns
        -------
        int
        """
    @overload
    def contains(
        self: CellArray[H3Cell], other: H3Cell | CellArray[H3Cell]
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(
        self: CellArray[S2Cell], other: S2Cell | CellArray[S2Cell]
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(
        self: CellArray[GeohashCell], other: GeohashCell | CellArray[GeohashCell]
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(
        self: CellArray[Tile], other: Tile | CellArray[Tile]
    ) -> npt.NDArray[np.bool_]:
        """Test whether every row hierarchically contains the paired cell.

        Parameters
        ----------
        other : cell or CellArray
            One same-grid cell broadcast to every row, or a same-length array.

        Returns
        -------
        numpy.ndarray
            One read-only boolean per row.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> gm.CellArray([cell, list(cell.neighbors)[0]]).contains(cell).tolist()
        [True, False]
        """
    @overload
    def intersects(
        self: CellArray[H3Cell], other: H3Cell | CellArray[H3Cell]
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self: CellArray[S2Cell], other: S2Cell | CellArray[S2Cell]
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self: CellArray[GeohashCell], other: GeohashCell | CellArray[GeohashCell]
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self: CellArray[Tile], other: Tile | CellArray[Tile]
    ) -> npt.NDArray[np.bool_]:
        """Test whether every row hierarchically intersects the paired cell.

        Two cells intersect when either is an ancestor of the other.

        Parameters
        ----------
        other : cell or CellArray
            One same-grid cell broadcast to every row, or a same-length array.

        Returns
        -------
        numpy.ndarray
            One read-only boolean per row.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> gm.CellArray([cell, list(cell.neighbors)[0]]).intersects(cell).tolist()
        [True, False]
        """
    def value_counts(self) -> tuple[CellArray[_CellT_co], npt.NDArray[np.int64]]:
        """Unique cells and counts, ordered by descending count (pandas
        value_counts parity), with first appearance breaking ties.

        Returns
        -------
        tuple
            ``(unique_cells, counts)`` where ``unique_cells`` is a CellArray
            and ``counts`` is a read-only `int64` ndarray.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> unique, counts = gm.CellArray([cell, cell]).value_counts()
        >>> counts.tolist()
        [2]
        """
    def factorize(self) -> tuple[npt.NDArray[np.int64], CellArray[_CellT_co]]:
        """Factorize cells into dense integer codes and first-seen uniques
        (pandas factorize parity).

        Returns
        -------
        tuple
            ``(codes, unique_cells)`` where ``codes`` is a read-only `int64`
            ndarray and ``unique_cells`` is a CellArray.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> codes, unique = gm.CellArray([cell, list(cell.neighbors)[0]]).factorize()
        >>> codes.tolist()
        [0, 1]
        """
    @property
    def center(self) -> GeometryArray[Point]:
        """Center points of every cell, as a packed WGS84 point array.

        The bulk twin of each scalar cell's center property — one
        vectorized call for the index-millions-of-points workflow, returning
        zero-copy packed point storage.

        Returns
        -------
        GeometryArray
            One ``Point`` (lon/lat, ``OGC:CRS84``) per cell.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=1)])
        >>> arr.center.to_wkt()[0]
        'POINT (-90 42.5255643899033)'
        """
    @property
    def area(self) -> npt.NDArray[np.float64]:
        """Geodesic cell areas in square meters.

        The bulk twin of each scalar cell's ``area`` property: H3 and S2 use
        their exact cell geometry; geohash and tile cells use the ellipsoidal
        area of their lon/lat rectangle.

        Returns
        -------
        numpy.ndarray
            One ``float64`` area (m²) per cell.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=0)])
        >>> float(arr.area[0]) > 1e14
        True
        """
    @property
    def is_missing(self) -> npt.NDArray[np.bool_]:
        """Per-row missing mask as a read-only boolean NumPy array.

        Bulk cell factories set a missing row when the corresponding geometry
        was missing; dense constructions return all-``False``.

        Returns
        -------
        numpy.ndarray of bool
        """
    @property
    def polygon(self) -> GeometryArray[Polygon]:
        """Filled WGS84 polygon of every cell, as a geometry array.

        Returns
        -------
        GeometryArray
        """
    def children_count(self, depth: int | None = None, /) -> npt.NDArray[np.uint64]:
        """Number of descendant cells each cell has at ``depth``.

        The columnar mirror of the scalar ``children_count`` — the count only,
        without materializing the children (which ``children`` does, as ragged
        rows). Counts are exact and can be very large at a coarse-to-fine
        depth gap, so they are returned as ``uint64``.

        Parameters
        ----------
        depth : int, optional
            Target depth (resolution / level / precision / zoom). Omitted means
            one step finer than each cell.

        Returns
        -------
        numpy.ndarray
            Read-only ``uint64`` ``numpy.ndarray`` of shape ``(n,)``.

        Raises
        ------
        GeometryError
            If ``depth`` is out of range for the grid.

        See Also
        --------
        children : The descendant cells themselves, as ragged rows.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> cells = gm.CellArray([cell, list(cell.neighbors)[0]])
        >>> cells.children_count(9).tolist()
        [49, 49]
        """
    def parent(self, depth: int | None = None, /) -> CellArray[_CellT_co]:
        """Parent cell of every input cell.

        Parameters
        ----------
        depth : int, optional
            Target depth; defaults to one coarser than each row.

        Returns
        -------
        CellArray

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> cells = gm.CellArray([cell, list(cell.neighbors)[0]])
        >>> cells.parent(6)[0].token
        '86283082fffffff'
        """
    @property
    def neighbors(self) -> Groups[CellArray[_CellT_co]]:
        """The edge-adjacent cells of every cell, as ragged rows.

        Returns
        -------
        Groups of CellArray
            One row of neighbors per input cell, in input order. Neighbor
            counts vary (e.g. H3 pentagons have five), so the result is a
            Groups, not a rectangular CellArray.
        """
    def children(self, depth: int | None = None, /) -> Groups[CellArray[_CellT_co]]:
        """Return the child cells of every cell at a finer depth, as ragged rows.

        Parameters
        ----------
        depth : int, optional
            Target depth; must not be coarser than any input cell. Defaults to
            one finer than each cell's own depth.

        Returns
        -------
        Groups of CellArray
            One row of children per input cell, in input order.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> len(gm.CellArray([cell]).children(8)[0])
        7
        """
    def compact(self, depth: int | None = None, /) -> CellArray[_CellT_co]:
        """Compact this cell set to the coarsest exact covering.

        Parameters
        ----------
        depth : int, optional
            Coarsest depth allowed.

        Returns
        -------
        CellArray

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> cells = gm.CellArray([cell, list(cell.neighbors)[0]])
        >>> len(cells.compact(5))
        2
        """
    def uncompact(self, depth: int, /) -> CellArray[_CellT_co]:
        """Expand this cell set to a uniform depth.

        Parameters
        ----------
        depth : int
            Target depth; no coarser than any row.

        Returns
        -------
        CellArray

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> len(gm.CellArray([cell]).uncompact(8))
        7
        """
    def to_polygon(self) -> Polygon | MultiPolygon:
        """Dissolve this cell set into one outline geometry.

        Returns
        -------
        Polygon or MultiPolygon

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> gm.CellArray([cell]).to_polygon().geometry_type
        'Polygon'
        """
    @property
    def token(self) -> list[str]:
        """Canonical string token of every cell, in order.

        For H3, S2, and tiles this is the text form of the numeric id exposed
        by `to_numpy()`. For Geohash it is the public string identity itself;
        Geohash `to_numpy()` instead returns typed `GeohashCell` objects.

        Returns
        -------
        list of str
            One canonical token per cell (H3 hex, S2 token, geohash, or tile
            quadkey).

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CellArray([gm.GeohashCell('u33d')]).token
        ['u33d']
        """
    def __class_getitem__(cls, key: Any) -> types.GenericAlias:
        """See PEP 585"""
    def __len__(self) -> int:
        """Number of cells.

        Returns
        -------
        int
        """
    def __iter__(self) -> CellArrayIterator[_CellT_co]:
        """Iterate cells in row order.

        Returns
        -------
        iterator of Cell
        """
    @overload
    def __getitem__(self, index: int, /) -> _CellT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: npt.NDArray[np.bool_], /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: npt.NDArray[np.int64], /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(
        self,
        index: int
        | slice
        | npt.NDArray[np.bool_]
        | _BoolLane
        | _IndexLane
        | npt.NDArray[np.int64],
        /,
    ) -> _CellT_co | CellArray[_CellT_co]:
        """Select cells by integer, slice, or fancy index.

        An ``int`` returns one cell object. A ``slice`` or fancy index returns
        a ``CellArray`` of the same cell kind.

        Returns
        -------
        Cell or CellArray
        """
    @overload
    def __new__(
        cls,
        values: Iterable[H3Cell],
        *,
        type: None = None,
    ) -> CellArray[H3Cell]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[S2Cell],
        *,
        type: None = None,
    ) -> CellArray[S2Cell]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[GeohashCell],
        *,
        type: None = None,
    ) -> CellArray[GeohashCell]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[Tile],
        *,
        type: None = None,
    ) -> CellArray[Tile]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[GeohashCell | str],
        *,
        type: type[GeohashCell],
    ) -> CellArray[GeohashCell]: ...
    @overload
    def __new__(
        cls,
        values: npt.NDArray[np.uint64] | Buffer | Iterable[H3Cell | int | str],
        *,
        type: type[H3Cell],
    ) -> CellArray[H3Cell]: ...
    @overload
    def __new__(
        cls,
        values: npt.NDArray[np.uint64] | Buffer | Iterable[S2Cell | int | str],
        *,
        type: type[S2Cell],
    ) -> CellArray[S2Cell]: ...
    @overload
    def __new__(
        cls,
        values: npt.NDArray[np.uint64] | Buffer | Iterable[Tile | int | str],
        *,
        type: type[Tile],
    ) -> CellArray[Tile]:
        """Create and return a new object.  See help(type) for accurate signature."""
    @property
    def grid(self) -> Literal['h3', 's2', 'tile', 'geohash']:
        """Grid-system token for the stored cell type.

        Returns
        -------
        str
        """
    @property
    def nbytes(self) -> int:
        """Logical id payload in bytes (`len * 8`).

        Returns
        -------
        int
        """
    @overload
    def to_numpy(self: CellArray[GeohashCell]) -> npt.NDArray[np.object_]: ...
    @overload
    def to_numpy(
        self: CellArray[H3Cell | S2Cell | Tile],
    ) -> npt.NDArray[np.uint64]:
        """Return a read-only NumPy identity column.

        H3, S2, and tile arrays expose their validated ids as uint64
        (zero-copy for contiguous selections). Geohash has no public numeric
        id, so it returns an object array of typed GeohashCell values.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7).cells[0]
        >>> type(gm.CellArray([cell]).to_numpy()).__name__
        'ndarray'
        """
    @overload
    def __array__(
        self: CellArray[GeohashCell], dtype: None = None, copy: bool | None = None
    ) -> npt.NDArray[np.object_]: ...
    @overload
    def __array__(
        self: CellArray[H3Cell | S2Cell | Tile],
        dtype: None = None,
        copy: bool | None = None,
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self: CellArray[H3Cell | S2Cell | Tile],
        dtype: type[np.uint64] | np.dtype[np.uint64] | Literal['uint64', 'u8'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.object_] | np.dtype[np.object_] | Literal['O', 'object'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]:
        """NumPy array protocol.

        With `dtype=None`, H3/S2/tile arrays export raw uint64 ids and
        Geohash arrays export typed GeohashCell objects. `dtype=uint64` is
        available only for the numeric grids; `dtype=object` exports typed
        cell objects for every grid, matching iteration.

        Parameters
        ----------
        dtype : uint64 or object, optional
        copy : bool, optional
            ``False`` requires a contiguous zero-copy numeric-id export;
            gathered ids and every object export raise because they materialize.

        Returns
        -------
        numpy.ndarray
        """

@final
class H3VertexArrayIterator:
    """Lazy iterator over an `H3VertexArray`'s vertices."""
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """
    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> H3Vertex:
        """Implement next(self)."""

@final
class H3VertexArray(Sequence[H3Vertex]):
    """An immutable array of H3 topological vertex ids.

    Index with an integer for an `H3Vertex`; slice or mask for another
    `H3VertexArray`. The id column is exposed as read-only ``uint64`` data.
    """

    __array_ufunc__: ClassVar[None]
    def __new__(
        cls,
        values: npt.NDArray[np.uint64]
        | Buffer
        | Iterable[H3Vertex]
        | Iterable[int]
        | Iterable[str],
    ) -> Self:
        """Create and return a new object.  See help(type) for accurate signature."""
    @property
    def values(self) -> npt.NDArray[np.uint64]:
        """Return a read-only ``uint64`` ndarray view of the id column.

        Returns
        -------
        numpy.ndarray
        """
    @property
    def token(self) -> list[str]:
        """Hexadecimal token of every row.

        Returns
        -------
        list of str
        """
    @property
    def point(self) -> GeometryArray[Point]:
        """The location of every vertex.

        Returns
        -------
        GeometryArray
            One ``Point`` (lon/lat, ``OGC:CRS84``) per vertex.
        """
    @property
    def nbytes(self) -> int:
        """Logical id payload in bytes (``len * 8``).

        Returns
        -------
        int
        """
    def to_numpy(self) -> npt.NDArray[np.uint64]:
        """Return a read-only ``uint64`` ndarray view of the id column.

        Contiguous selections are zero-copy; gathered selections
        materialize the logical id order.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> type(edges.to_numpy()).__name__
        'ndarray'
        """
    @overload
    def __array__(
        self, dtype: None = None, copy: bool | None = None
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.uint64] | np.dtype[np.uint64] | Literal['uint64', 'u8'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.object_] | np.dtype[np.object_] | Literal['O', 'object'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]:
        """NumPy array protocol.

        ``dtype=None`` / ``uint64`` exports the raw validated ids;
        ``dtype=object`` exports typed H3 values, matching iteration.

        Parameters
        ----------
        dtype : uint64 or object, optional
        copy : bool, optional
            ``False`` requires a zero-copy id export; gathered ids and
            ``dtype=object`` raise because they must materialize.

        Returns
        -------
        numpy.ndarray
        """
    def value_counts(self) -> tuple[H3VertexArray, npt.NDArray[np.int64]]:
        """Unique values and counts, ordered by descending count.

        Returns
        -------
        tuple
            ``(unique_values, counts)`` with read-only ``int64`` counts.

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> unique, counts = edges.value_counts()
        >>> counts.tolist()
        [1, 1, 1, 1, 1, 1]
        """
    def factorize(self) -> tuple[npt.NDArray[np.int64], H3VertexArray]:
        """Factorize values into dense integer codes and first-seen uniques.

        Returns
        -------
        tuple
            ``(codes, unique_values)``.

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> codes, unique = edges.factorize()
        >>> len(codes) == len(edges)
        True
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of an equal id in ``[start, stop)``.

        Parameters
        ----------
        value : H3 value or int id
            The element to locate.
        start : int, default 0
            First position searched.
        stop : int, optional
            One past the last position searched.

        Returns
        -------
        int
        """
    def count(self, value: object) -> int:
        """Number of elements with the same id.

        Parameters
        ----------
        value : H3 value or int id
            The element to count.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int: ...
    def __reduce__(self) -> tuple[Any, tuple[list[int]]]: ...
    def __copy__(self) -> Self: ...
    def __deepcopy__(self, memo: object) -> Self: ...
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __bool__(self) -> bool:
        """``False`` only when the array is empty.

        Returns
        -------
        bool
        """
    def __len__(self) -> int:
        """Number of edges or vertices.

        Returns
        -------
        int
        """
    def __contains__(self, value: object, /) -> bool:
        """Whether an edge/vertex id appears in the array.

        Returns
        -------
        bool
        """
    def __iter__(self) -> H3VertexArrayIterator:
        """Iterate elements in row order.

        Returns
        -------
        iterator
        """
    def __reversed__(self) -> H3VertexArrayIterator:
        """Iterate elements in reverse row order.

        Returns
        -------
        iterator
        """
    @overload
    def __getitem__(self, index: int, /) -> H3Vertex: ...
    @overload
    def __getitem__(self, index: slice, /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: npt.NDArray[np.bool_], /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: npt.NDArray[np.int64], /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> H3VertexArray: ...
    @overload
    def __getitem__(
        self,
        index: int
        | slice
        | npt.NDArray[np.bool_]
        | _BoolLane
        | _IndexLane
        | npt.NDArray[np.int64],
        /,
    ) -> H3Vertex | H3VertexArray:
        """Select by integer, slice, or fancy index.

        An ``int`` returns one edge/vertex. A ``slice`` or fancy index
        returns an array of the same type.

        Returns
        -------
        H3Edge or H3Vertex or H3EdgeArray or H3VertexArray
        """

@final
class H3EdgeArrayIterator:
    """Lazy iterator over an `H3EdgeArray`'s edges."""
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the logical
        payload it keeps alive while iterating.
        """
    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> H3Edge:
        """Implement next(self)."""

@final
class H3EdgeArray(Sequence[H3Edge]):
    """An immutable array of H3 directed-edge ids.

    Index with an integer for an `H3Edge`; slice or mask for another
    `H3EdgeArray`. The id column is exposed as read-only ``uint64`` data.
    """

    __array_ufunc__: ClassVar[None]
    def __new__(
        cls,
        values: npt.NDArray[np.uint64]
        | Buffer
        | Iterable[H3Edge]
        | Iterable[int]
        | Iterable[str],
    ) -> Self:
        """Create and return a new object.  See help(type) for accurate signature."""
    @property
    def values(self) -> npt.NDArray[np.uint64]:
        """Return a read-only ``uint64`` ndarray view of the id column.

        Returns
        -------
        numpy.ndarray
        """
    @property
    def token(self) -> list[str]:
        """Hexadecimal token of every row.

        Returns
        -------
        list of str
        """
    @property
    def origin(self) -> CellArray[H3Cell]:
        """The cells these directed edges leave.

        Returns
        -------
        CellArray
            One origin H3Cell per edge.
        """
    @property
    def destination(self) -> CellArray[H3Cell]:
        """The cells these directed edges enter.

        Returns
        -------
        CellArray
            One destination H3Cell per edge.
        """
    def reverse(self) -> H3EdgeArray:
        """Reverse every directed edge from its destination back to its origin.

        Returns
        -------
        H3EdgeArray

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> edges.reverse()[0].token
        '1672830829ffffff'
        """
    @property
    def line(self) -> GeometryArray[LineString]:
        """Edge linework along each shared cell boundary.

        Returns
        -------
        GeometryArray
            One ``LineString`` (lon/lat, ``OGC:CRS84``) per edge.
        """
    @property
    def length(self) -> npt.NDArray[np.float64]:
        """Length of every edge in meters (spherical, like `H3Cell.area`).

        Returns
        -------
        numpy.ndarray
            One ``float64`` length per edge.
        """
    @property
    def nbytes(self) -> int:
        """Logical id payload in bytes (``len * 8``).

        Returns
        -------
        int
        """
    def to_numpy(self) -> npt.NDArray[np.uint64]:
        """Return a read-only ``uint64`` ndarray view of the id column.

        Contiguous selections are zero-copy; gathered selections
        materialize the logical id order.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> type(edges.to_numpy()).__name__
        'ndarray'
        """
    @overload
    def __array__(
        self, dtype: None = None, copy: bool | None = None
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.uint64] | np.dtype[np.uint64] | Literal['uint64', 'u8'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self,
        dtype: type[np.object_] | np.dtype[np.object_] | Literal['O', 'object'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]:
        """NumPy array protocol.

        ``dtype=None`` / ``uint64`` exports the raw validated ids;
        ``dtype=object`` exports typed H3 values, matching iteration.

        Parameters
        ----------
        dtype : uint64 or object, optional
        copy : bool, optional
            ``False`` requires a zero-copy id export; gathered ids and
            ``dtype=object`` raise because they must materialize.

        Returns
        -------
        numpy.ndarray
        """
    def value_counts(self) -> tuple[H3EdgeArray, npt.NDArray[np.int64]]:
        """Unique values and counts, ordered by descending count.

        Returns
        -------
        tuple
            ``(unique_values, counts)`` with read-only ``int64`` counts.

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> unique, counts = edges.value_counts()
        >>> counts.tolist()
        [1, 1, 1, 1, 1, 1]
        """
    def factorize(self) -> tuple[npt.NDArray[np.int64], H3EdgeArray]:
        """Factorize values into dense integer codes and first-seen uniques.

        Returns
        -------
        tuple
            ``(codes, unique_values)``.

        Examples
        --------
        >>> import gometry as gm
        >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
        >>> codes, unique = edges.factorize()
        >>> len(codes) == len(edges)
        True
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """First index of an equal id in ``[start, stop)``.

        Parameters
        ----------
        value : H3 value or int id
            The element to locate.
        start : int, default 0
            First position searched.
        stop : int, optional
            One past the last position searched.

        Returns
        -------
        int
        """
    def count(self, value: object) -> int:
        """Number of elements with the same id.

        Parameters
        ----------
        value : H3 value or int id
            The element to count.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int: ...
    def __reduce__(self) -> tuple[Any, tuple[list[int]]]: ...
    def __copy__(self) -> Self: ...
    def __deepcopy__(self, memo: object) -> Self: ...
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __bool__(self) -> bool:
        """``False`` only when the array is empty.

        Returns
        -------
        bool
        """
    def __len__(self) -> int:
        """Number of edges or vertices.

        Returns
        -------
        int
        """
    def __contains__(self, value: object, /) -> bool:
        """Whether an edge/vertex id appears in the array.

        Returns
        -------
        bool
        """
    def __iter__(self) -> H3EdgeArrayIterator:
        """Iterate elements in row order.

        Returns
        -------
        iterator
        """
    def __reversed__(self) -> H3EdgeArrayIterator:
        """Iterate elements in reverse row order.

        Returns
        -------
        iterator
        """
    @overload
    def __getitem__(self, index: int, /) -> H3Edge: ...
    @overload
    def __getitem__(self, index: slice, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: npt.NDArray[np.bool_], /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: npt.NDArray[np.int64], /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(
        self,
        index: int
        | slice
        | npt.NDArray[np.bool_]
        | _BoolLane
        | _IndexLane
        | npt.NDArray[np.int64],
        /,
    ) -> H3Edge | H3EdgeArray:
        """Select by integer, slice, or fancy index.

        An ``int`` returns one edge/vertex. A ``slice`` or fancy index
        returns an array of the same type.

        Returns
        -------
        H3Edge or H3Vertex or H3EdgeArray or H3VertexArray
        """

class _CoverageIterator(Generic[_CellT_co]):
    """Stub-only generic base for the four native coverage iterators."""
    def __length_hint__(self) -> int:
        """Remaining cells — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the iterator plus the copied cell
        buffer it walks.
        """
    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __reversed__(self) -> Self:
        """Return a reverse iterator over the same coverage cells."""
    def __next__(self) -> _CellT_co:
        """Implement next(self)."""

class _Coverage(Sequence[_CellT_co]):
    """Stub-only generic base for native grid coverages."""

    __array_ufunc__: ClassVar[None]
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Coverages are returned by grid coverage factories and cannot be constructed."""
    @property
    def nbytes(self) -> int:
        """Logical cell-id payload in bytes for the visible cell set.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the wrapper plus visible cell ids,
        partition data, and the retained source geometry.
        """
    @property
    def cell_rule(self) -> CellRule:
        """The rule that materialized ``cells`` (``'center'``, ``'within'``,
        ``'overlap'``, or ``'bbox'``). It shapes only the visible cell set; the
        exact membership predicates never depend on it.

        Returns
        -------
        str
            The ``cell_rule`` token the covering was built with.
        """
    @property
    def cells(self) -> CellArray[_CellT_co]:
        """The cells that make up the covering.

        Returns
        -------
        `CellArray`
        """
    @property
    def interior_cells(self) -> CellArray[_CellT_co]:
        """Cells certified entirely inside the source geometry.

        Returns
        -------
        `CellArray`
        """
    @property
    def boundary_cells(self) -> CellArray[_CellT_co]:
        """Cells partially overlapping the source geometry (the fringe where cell membership cannot answer the geometry question).

        Returns
        -------
        `CellArray`
        """
    @overload
    def covers(self, geom: Geometry) -> bool: ...
    @overload
    def covers(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def covers(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Exact, boundary-inclusive membership of candidates in the covered area.

        Always answers against the source geometry — never the cells — so the
        result is exact regardless of ``cell_rule``.

        Parameters
        ----------
        geom : Geometry or `GeometryArray`
            Candidate geometry (or array). Follows the grid input policy:
            WGS84 and CRS-free lon/lat pass through, any other CRS is
            reprojected.

        Returns
        -------
        bool or ndarray
            One result per input geometry.
        """
    @overload
    def contains(self, geom: Geometry) -> bool: ...
    @overload
    def contains(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(self, geom: Geometry | GeometryArray) -> bool | npt.NDArray[np.bool_]:
        """Exact, strict-interior membership of candidates in the covered area.

        Like ``covers`` but boundary-exclusive: a point on the source
        geometry's boundary is ``False``.

        Parameters
        ----------
        geom : Geometry or `GeometryArray`
            Candidate geometry (or array). Follows the grid input policy:
            WGS84 and CRS-free lon/lat pass through, any other CRS is
            reprojected.

        Returns
        -------
        bool or ndarray
            One result per input geometry.
        """
    @overload
    def intersects(self, geom: Geometry) -> bool: ...
    @overload
    def intersects(self, geom: GeometryArray) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self, geom: Geometry | GeometryArray
    ) -> bool | npt.NDArray[np.bool_]:
        """Exact intersection test of candidates against the covered area.

        For points this matches ``covers``; for lines and polygons it is true
        when the candidate shares any point with the source geometry.

        Parameters
        ----------
        geom : Geometry or `GeometryArray`
            Candidate geometry (or array). Follows the grid input policy:
            WGS84 and CRS-free lon/lat pass through, any other CRS is
            reprojected.

        Returns
        -------
        bool or ndarray
            One result per input geometry.
        """
    @overload
    def contains_xy(self, x: float, y: float) -> bool: ...
    @overload
    def contains_xy(self, x: FloatColumn, y: FloatInput) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains_xy(self, x: FloatInput, y: FloatColumn) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains_xy(self, x: FloatInput, y: FloatInput) -> bool | npt.NDArray[np.bool_]:
        """Exact, strict ``contains`` test for raw lon/lat coordinates.

        Answers exactly against the source geometry, independent of
        ``cell_rule``.

        Parameters
        ----------
        x, y : float or sequence of float
            Longitude and latitude in degrees.

        Returns
        -------
        bool or ndarray
            A single bool for scalar ``x, y``, or one result per coordinate.
        """
    @overload
    def intersects_xy(self, x: float, y: float) -> bool: ...
    @overload
    def intersects_xy(self, x: FloatColumn, y: FloatInput) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects_xy(self, x: FloatInput, y: FloatColumn) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects_xy(
        self, x: FloatInput, y: FloatInput
    ) -> bool | npt.NDArray[np.bool_]:
        """Exact, boundary-inclusive membership test for raw lon/lat coordinates.

        Parameters
        ----------
        x, y : float or sequence of float
            Longitude and latitude in degrees.

        Returns
        -------
        bool or ndarray
            A single bool for scalar ``x, y``, or one result per coordinate.
        """
    def explain(self) -> list[str]:
        """Describe the membership plan.

        Returns
        -------
        list of str
            One line per plan step.
        """
    def to_polygon(self) -> Polygon | MultiPolygon:
        """Dissolve the coverage into one outline geometry.

        Returns
        -------
        `Polygon` or `MultiPolygon`

        Raises
        ------
        GeometryError
            If the coverage is empty.
        """
    def __len__(self) -> int:
        """Number of visible cells in the coverage.

        Returns
        -------
        int
        """
    def __bool__(self) -> bool:
        """``False`` only when the coverage has no visible cells.

        Returns
        -------
        bool
        """
    @overload
    def __getitem__(self, index: int, /) -> _CellT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: int | slice, /) -> _CellT_co | CellArray[_CellT_co]:
        """Select visible cells by integer or slice.

        An ``int`` returns one cell. A ``slice`` returns a ``CellArray``
        of those cells (not a sliced coverage — membership still answers
        against the full source geometry via the coverage).

        Returns
        -------
        Cell or CellArray
        """
    def __iter__(self) -> _CoverageIterator[_CellT_co]:
        """Iterate visible cells in coverage order.

        Returns
        -------
        iterator of Cell
        """
    def __reversed__(self) -> _CoverageIterator[_CellT_co]:
        """``reversed(coverage)`` — lazy end-to-start iteration of cells."""
    def __contains__(self, cell: object, /) -> bool:
        """Whether a cell is among the visible coverage cells.

        Returns
        -------
        bool
        """
    def index(self, value: object, start: int = 0, stop: int | None = None) -> int: ...
    def count(self, value: object) -> int: ...
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __copy__(self) -> Self:
        """``copy.copy`` returns the coverage itself — it is an immutable value."""
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the coverage itself: every field is immutable."""
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle support: round-trip through the source geometry, cell ids,
        partition data, rule, and depth fields.
        """

class _Cell:
    """Stub-only generic base for shared scalar cell members."""
    @property
    def nbytes(self) -> int:
        """Raw scalar cell id payload in bytes.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: scalar cells are heap-free value ids."""
    @property
    def token(self) -> str:
        """Cell token.

        Returns
        -------
        str
        """
    @property
    def center(self) -> Point:
        """Cell center as a WGS84 ``Point`` (lon/lat).

        Returns
        -------
        Point
        """
    @property
    def polygon(self) -> Polygon:
        """The cell as a filled WGS84 ``Polygon`` (lon/lat).

        Returns
        -------
        Polygon
        """
    @property
    def area(self) -> float:
        """Geodesic area of the cell in square meters.

        Returns
        -------
        float
        """
    @property
    def neighbors(self) -> CellArray[Self]:
        """Edge-adjacent neighbor cells.

        Returns
        -------
        CellArray
        """
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — cells are immutable
        values, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __lt__(self, other: Self, /) -> bool:
        """Return self<value."""
    def __le__(self, other: Self, /) -> bool:
        """Return self<=value."""
    def __gt__(self, other: Self, /) -> bool:
        """Return self>value."""
    def __ge__(self, other: Self, /) -> bool:
        """Return self>=value."""
    def __ne__(self, other: object, /) -> bool:
        """Return self!=value."""
    def __hash__(self) -> int:
        """Return hash(self)."""

class _NumericCell(_Cell):
    """Stub-only base for numeric-id scalar cells."""
    @property
    def id(self) -> int:
        """The numeric cell id.

        Returns
        -------
        int
        """
    def contains(self, other: Self | int | str) -> bool:
        """Whether this cell contains another cell."""
    def intersects(self, other: Self | int | str) -> bool:
        """Whether this cell intersects another cell."""
    def __reduce__(self) -> tuple[object, tuple[int]]:
        """Pickle support: a cell is its id."""
    def __int__(self) -> int:
        """int(self)"""
    def __index__(self) -> int:
        """Return self converted to an integer, if self is suitable for use as an index into a list."""

@final
class H3CoverageIterator(_CoverageIterator['H3Cell']):
    """Lazy iterator over a coverage's cells, yielding one cell per step."""
    def __reversed__(self) -> Self:
        """Return a reverse iterator over the same coverage cells."""

@final
class H3Coverage(_Coverage['H3Cell']):
    """An H3 covering of a geometry.

    Returned by ``h3_cover(...)``: ``coverage.cells`` materializes the
    cells selected by ``cell_rule`` (join keys, bins, visualization), while
    ``covers``/``contains``/``intersects`` answer exactly against the source
    geometry, independent of the rule. Iterate it, test ``cell in coverage``,
    or ``compact``/``with_parents`` across resolutions.
    """

    __match_args__: Final = ('cells',)
    @property
    def resolution(self) -> int | None:
        """Uniform H3 resolution of the covering's cells, or ``None`` for mixed
        resolutions.

        Returns
        -------
        int or None
        """
    def compact(self, *, min_resolution: int = 0) -> H3Coverage:
        """Compact the cell set to its coarsest covering.

        Parameters
        ----------
        min_resolution : int, default 0
            Coarsest resolution compaction may produce; merging stops at this
            floor (cells already coarser pass through unchanged).

        Returns
        -------
        H3Coverage
            The compacted covering (same area, fewest cells).

        Examples
        --------
        >>> import gometry as gm
        >>> poly = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
        >>> cov = gm.h3_cover(poly, resolution=7)
        >>> len(cov.compact().cells) < len(cov.cells)
        True
        """
    def uncompact(self, resolution: int) -> H3Coverage:
        """Expand the cell set to a uniform resolution (every cell subdivided down
        to ``resolution``).

        Parameters
        ----------
        resolution : int
            Target H3 resolution (``0``-``15``); no coarser than any
            current cell.

        Returns
        -------
        H3Coverage
            The expanded covering.

        Raises
        ------
        GeometryError
            If ``resolution`` is out of range.

        Examples
        --------
        >>> import gometry as gm
        >>> poly = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
        >>> cov = gm.h3_cover(poly, resolution=7).compact()
        >>> len(cov.uncompact(7).cells) >= len(cov.cells)
        True
        """
    def with_parents(self, *, min_resolution: int = 0) -> H3Coverage:
        """Include parent cells down to a minimum resolution.

        Parameters
        ----------
        min_resolution : int, default 0
            Coarsest resolution to add parents for (0 is the base-cell
            resolution).

        Returns
        -------
        H3Coverage

        Raises
        ------
        GeometryError
            If ``min_resolution`` is out of range.

        Examples
        --------
        >>> import gometry as gm
        >>> poly = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
        >>> cov = gm.h3_cover(poly, resolution=7)
        >>> len(cov.with_parents().cells) > len(cov.cells)
        True
        """

@final
class H3Vertex:
    """A canonical H3 topological vertex.

    Vertexes carry shared identity: adjacent cells yield *equal* vertex
    objects for their shared corners, so they deduplicate across cell sets
    (``{v for cell in cells for v in cell.vertices}``). Obtained from
    `H3Cell.vertices`; convert with ``int(vertex)`` or ``vertex.token``.

    Parameters
    ----------
    value : H3Vertex, int, or str
        An existing vertex, its 64-bit id, or its token.
    """

    @property
    def nbytes(self) -> int:
        """Raw vertex id payload in bytes.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: H3 vertices are heap-free value ids."""
    def __new__(cls, value: H3Vertex | int | str) -> Self:
        """One H3 vertex from an existing `H3Vertex`, a 64-bit id, or a token.

        Parameters
        ----------
        value : H3Vertex, int, or str
            The vertex, its 64-bit id, or its token.

        Returns
        -------
        H3Vertex

        Raises
        ------
        ParseError
            If ``value`` is not a valid H3 vertex id or token.
        TypeError
            If ``value`` is not an `H3Vertex`, int, or str.
        """

    @property
    def id(self) -> int:
        """The vertex's 64-bit H3 index.

        Returns
        -------
        int
        """
    @property
    def token(self) -> str:
        """The vertex's hexadecimal token.

        Returns
        -------
        str
        """
    @property
    def point(self) -> Point:
        """The vertex's location.

        Returns
        -------
        Point
            Longitude/latitude point tagged ``OGC:CRS84``.
        """
    def __int__(self) -> int:
        """int(self)"""
    def __index__(self) -> int:
        """Return self converted to an integer, if self is suitable for use as an index into a list."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __lt__(self, other: H3Vertex, /) -> bool:
        """Return self<value."""
    def __le__(self, other: H3Vertex, /) -> bool:
        """Return self<=value."""
    def __gt__(self, other: H3Vertex, /) -> bool:
        """Return self>value."""
    def __ge__(self, other: H3Vertex, /) -> bool:
        """Return self>=value."""
    def __ne__(self, other: object, /) -> bool:
        """Return self!=value."""
    def __reduce__(self) -> tuple[object, tuple[int]]:
        """Helper for pickle."""
    __match_args__: Final = ('id',)
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — vertices are immutable
        values, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """

@final
class H3Edge:
    """One directed H3 edge: the shared boundary between an origin cell and a
    neighboring destination cell, with its own 64-bit H3 index.

    Obtained from `H3Cell.edge` / `H3Cell.edges`; convert with
    ``int(edge)`` or ``edge.token``, and rebuild with ``H3Edge(token)``.

    Parameters
    ----------
    value : H3Edge, int, or str
        An existing edge, its 64-bit id, or its token.
    """

    @property
    def nbytes(self) -> int:
        """Raw directed-edge id payload in bytes.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: H3 edges are heap-free value ids."""
    def __new__(cls, value: H3Edge | int | str) -> Self:
        """One H3 directed edge from an existing `H3Edge`, a 64-bit id, or a token.

        Parameters
        ----------
        value : H3Edge, int, or str
            The edge, its 64-bit id, or its token.

        Returns
        -------
        H3Edge

        Raises
        ------
        ParseError
            If ``value`` is not a valid H3 directed-edge id or token.
        TypeError
            If ``value`` is not an `H3Edge`, int, or str.
        """

    @property
    def id(self) -> int:
        """The edge's 64-bit H3 index.

        Returns
        -------
        int
        """
    @property
    def token(self) -> str:
        """The edge's hexadecimal token.

        Returns
        -------
        str
        """
    @property
    def origin(self) -> H3Cell:
        """The cell this directed edge leaves.

        Returns
        -------
        H3Cell
        """
    @property
    def destination(self) -> H3Cell:
        """The cell this directed edge enters.

        Returns
        -------
        H3Cell
        """
    @property
    def cells(self) -> tuple[H3Cell, H3Cell]:
        """The ``(origin, destination)`` cell pair.

        Returns
        -------
        tuple of H3Cell
        """
    def reverse(self) -> H3Edge:
        """Reverse this directed edge from ``destination`` back to ``origin``.

        Returns
        -------
        H3Edge

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> edge = cell.edge(list(cell.neighbors)[0])
        >>> edge.reverse().token
        '137283082cffffff'
        """
    @property
    def line(self) -> LineString:
        """The edge's linework along the shared cell boundary.

        Returns
        -------
        LineString
            Longitude/latitude line tagged ``OGC:CRS84``.
        """
    @property
    def length(self) -> float:
        """Length of the edge in meters (spherical, like `H3Cell.area`).

        Returns
        -------
        float

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.H3Cell(13.4, 52.5, resolution=7)
        >>> edge = cell.edge(cell.neighbors[0])
        >>> 1000 < edge.length < 3000
        True
        """
    def __int__(self) -> int:
        """int(self)"""
    def __index__(self) -> int:
        """Return self converted to an integer, if self is suitable for use as an index into a list."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __lt__(self, other: H3Edge, /) -> bool:
        """Return self<value."""
    def __le__(self, other: H3Edge, /) -> bool:
        """Return self<=value."""
    def __gt__(self, other: H3Edge, /) -> bool:
        """Return self>value."""
    def __ge__(self, other: H3Edge, /) -> bool:
        """Return self>=value."""
    def __ne__(self, other: object, /) -> bool:
        """Return self!=value."""
    def __reduce__(self) -> tuple[object, tuple[int]]:
        """Helper for pickle."""
    __match_args__: Final = ('id',)
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — edges are immutable values,
        so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """

@final
class GeohashCell(_Cell):
    """One geohash cell: a base-32 character prefix addressing a lon/lat
    rectangle.

    Wraps the packed cell with typed accessors (``cell.precision``,
    ``cell.token``, ``cell.polygon``, ``cell.center``) and hierarchy moves
    (``parent``/``children``/``neighbors``). Geohash tokens are the public
    identity — text, not integers. Convert via ``GeohashCell(...)``.
    """

    __match_args__: Final = ('token',)

    @property
    def precision(self) -> int:
        """Geohash precision of this cell (``1``-``12`` characters).

        Returns
        -------
        int
        """
    @overload
    def __new__(cls, value: GeohashCell | str, /) -> GeohashCell: ...
    @overload
    def __new__(cls, value: Point, /, *, precision: int) -> GeohashCell: ...
    @overload
    def __new__(cls, value: float, /, lat: float, *, precision: int) -> GeohashCell:
        """One geohash cell from a token, lon/lat pair, or point geometry.

        Parameters
        ----------
        lon : GeohashCell, str, float, or Point
            A cell token, the longitude of a ``lon, lat`` pair, or a point
            geometry.

        lat : float, optional
            Latitude when ``lon`` is a scalar longitude.

        precision : int, optional
            Geohash precision (``1``-``12``); required for coordinate
            construction.

        Returns
        -------
        GeohashCell

        Raises
        ------
        ParseError
            If ``value`` is not a valid geohash token.
        GeometryError
            If ``precision`` is out of range.
        InvalidGeometryError
            If a scalar coordinate is non-finite or out of range.
        """
    def parent(self, precision: int | None = None) -> GeohashCell:
        """Parent cell at a coarser precision.

        Parameters
        ----------
        precision : int, optional
            Target precision; must not be finer than this cell's.
            Defaults to one coarser than this cell's precision.

        Returns
        -------
        GeohashCell
            The ancestor cell.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
        >>> cell.parent().token
        '9q8yy'
        """
    def children(self, precision: int | None = None) -> CellArray[GeohashCell]:
        """Child cells at a finer precision.

        Parameters
        ----------
        precision : int, optional
            Target precision; must not be coarser than this cell's.
            Defaults to one finer than this cell's precision;
            a maximum-depth cell has no children and yields an empty
            CellArray.

        Returns
        -------
        CellArray of GeohashCell
            The descendant cells.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
        >>> len(cell.children())
        32
        """
    def children_count(self, precision: int | None = None) -> int:
        """Number of descendant cells at a finer precision, counted closed-form without materializing them.

        Parameters
        ----------
        precision : int, optional
            Target precision; must not be coarser than this cell's.
            Defaults to one finer than this cell's precision;
            a maximum-depth cell has no children and returns ``0``.

        Returns
        -------
        int
            The exact descendant count (H3 pentagons have slightly fewer
            than hexagons).

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
        >>> cell.children_count()
        32
        """
    def contains(self, other: GeohashCell | str) -> bool:
        """Test whether this cell contains another cell (itself, or any
        descendant of it in the cell hierarchy).

        Parameters
        ----------
        other : GeohashCell or str
            The candidate cell.

        Returns
        -------
        bool

        Raises
        ------
        ParseError
            If an id or token is not a valid cell.
        TypeError
            If ``other`` is not a valid cell object, id, or token.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
        >>> cell.contains(cell)
        True
        """
    def intersects(self, other: GeohashCell | str) -> bool:
        """Test whether this cell intersects another cell (one contains the
        other — hierarchy cells cannot partially overlap).

        Parameters
        ----------
        other : GeohashCell or str
            The candidate cell.

        Returns
        -------
        bool

        Raises
        ------
        ParseError
            If an id or token is not a valid cell.
        TypeError
            If ``other`` is not a valid cell object, id, or token.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
        >>> cell.intersects(cell.parent())
        True
        """
    def __reduce__(self) -> tuple[object, tuple[str]]:
        """Pickle support: a cell is its id."""

@final
class GeohashCoverageIterator(_CoverageIterator[GeohashCell]):
    """Lazy iterator over a coverage's cells, yielding one cell per step."""
    def __reversed__(self) -> Self:
        """Return a reverse iterator over the same coverage cells."""

@final
class GeohashCoverage(_Coverage[GeohashCell]):
    """A geohash covering of a geometry (the ``geohash_cover`` backend).

    Returned by ``geohash_cover(...)``: ``coverage.cells`` materializes
    the cells selected by ``cell_rule`` at the chosen precision (join keys,
    bins, visualization), while ``covers``/``contains``/``intersects``
    answer exactly against the source geometry, independent of the rule.
    """

    __match_args__: Final = ('cells',)
    @property
    def precision(self) -> int | None:
        """Uniform geohash precision of the covering's cells, or ``None`` for
        mixed precisions.

        Returns
        -------
        int or None
        """
    def compact(self, *, min_precision: int = 1) -> GeohashCoverage:
        """Compact the cell set to its coarsest covering.

        Parameters
        ----------
        min_precision : int, default 1
            Coarsest precision compaction may produce.

        Returns
        -------
        GeohashCoverage

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.geohash_cover(p, precision=6)
        >>> len(cov.compact().cells) <= len(cov.cells)
        True
        """
    def uncompact(self, precision: int) -> GeohashCoverage:
        """Expand the cell set to a uniform precision.

        Parameters
        ----------
        precision : int
            Target precision (``1``-``12``); no coarser than any current cell.

        Returns
        -------
        GeohashCoverage

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.geohash_cover(p, precision=6)
        >>> len(cov.uncompact(7).cells) >= len(cov.cells)
        True
        """
    def with_parents(self, *, min_precision: int = 1) -> GeohashCoverage:
        """Include parent cells down to a minimum precision.

        Parameters
        ----------
        min_precision : int, default 1
            Coarsest precision to add parents for.

        Returns
        -------
        GeohashCoverage

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.geohash_cover(p, precision=6)
        >>> len(cov.with_parents().cells) >= len(cov.cells)
        True
        """

@final
class H3Cell(_NumericCell):
    """One H3 cell: a resolution-addressed hexagonal (or pentagonal) tile.

    Wraps the 64-bit cell index with typed accessors (``cell.resolution``,
    ``cell.polygon``, ``cell.center``, ``cell.area``), hierarchy moves
    (``parent``/``children``), and grid traversal (``grid_disk``,
    ``grid_ring``, ``grid_path``, ``grid_distance``). Convert via
    ``H3Cell(...)``, and back with ``int(cell)``.
    """

    __match_args__: Final = ('id',)
    @property
    def resolution(self) -> int:
        """H3 resolution of this cell (``0``-``15``).

        Returns
        -------
        int
        """
    @overload
    def __new__(cls, value: H3Cell | int | str, /) -> H3Cell: ...
    @overload
    def __new__(cls, value: Point, /, *, resolution: int) -> H3Cell: ...
    @overload
    def __new__(cls, value: float, /, lat: float, *, resolution: int) -> H3Cell:
        """One H3 cell from an id, token, lon/lat pair, or point geometry.

        Parameters
        ----------
        lon : H3Cell, int, str, float, or Point
            A cell id/token, the longitude of a ``lon, lat`` pair, or a point
            geometry.

        lat : float, optional
            Latitude when ``lon`` is a scalar longitude.

        resolution : int, optional
            H3 resolution (``0``-``15``); required for coordinate construction.

        Returns
        -------
        H3Cell

        Raises
        ------
        ParseError
            If ``value`` is not a valid H3 cell id or token.
        GeometryError
            If ``resolution`` is out of range.
        InvalidGeometryError
            If a scalar coordinate is non-finite.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.H3Cell(13.4, 52.5, resolution=7).resolution
        7
        """
    def parent(self, resolution: int | None = None) -> H3Cell:
        """Parent cell at a coarser resolution.

        Parameters
        ----------
        resolution : int, optional
            Target resolution; must not be finer than this cell's.
            Defaults to one coarser than this cell's resolution.

        Returns
        -------
        H3Cell
            The ancestor cell.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.parent(resolution=6).token
        '86283082fffffff'
        """
    def children(self, resolution: int | None = None) -> CellArray[H3Cell]:
        """Child cells at a finer resolution.

        Parameters
        ----------
        resolution : int, optional
            Target resolution; must not be coarser than this cell's.
            Defaults to one finer than this cell's resolution;
            a maximum-depth cell has no children and yields an empty
            CellArray.

        Returns
        -------
        CellArray of H3Cell
            The descendant cells.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> len(cell.children(resolution=8))
        7
        """
    @property
    def base_cell(self) -> int:
        """The base (resolution-0) cell number this cell descends from
        (0-121).

        Returns
        -------
        int
        """
    def is_neighbor(self, other: H3Cell | int | str) -> bool:
        """Test whether ``other`` is an edge-adjacent neighbor of this cell.

        Parameters
        ----------
        other : H3Cell, int, or str
            The candidate neighbor; must share this cell's resolution.

        Returns
        -------
        bool

        Raises
        ------
        GeometryError
            If the cells cannot be compared (different resolutions).
        ParseError
            If ``other`` is not a valid H3 cell.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.is_neighbor(list(cell.neighbors)[0])
        True
        """
    def local_ij(self, origin: H3Cell | int | str) -> tuple[int, int]:
        """Local ``(i, j)`` coordinates of this cell relative to ``origin``
        (the H3 local-IJ indexing space, for grid algebra around an anchor).

        Parameters
        ----------
        origin : H3Cell, int, or str
            Anchor cell; must share this cell's resolution and be near it.

        Returns
        -------
        tuple of int
            The ``(i, j)`` coordinates.

        Raises
        ------
        GeometryError
            If the cells are too far apart for local IJ.
        ParseError
            If ``origin`` is not a valid H3 cell.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.local_ij(cell)
        (160, 88)
        """
    def cell_from_local_ij(self, i: int, j: int) -> H3Cell:
        """Return the cell at local ``(i, j)`` coordinates relative to this origin —
        the inverse of `local_ij`.

        Parameters
        ----------
        i, j : int
            Local IJ coordinates.

        Returns
        -------
        H3Cell

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.cell_from_local_ij(160, 88).token
        '872830828ffffff'
        """
    def child_position(self, resolution: int) -> int | None:
        """This cell's position among `parent(resolution)`'s descendants at
        this cell's resolution, or None when resolution is finer than
        the cell's own.

        Parameters
        ----------
        resolution : int
            The ancestor resolution to count from.

        Returns
        -------
        int or None

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.children(resolution=8)[0].child_position(7)
        0
        """
    @property
    def is_pentagon(self) -> bool:
        """Whether this cell is one of the 12 pentagons at its resolution.

        Returns
        -------
        bool
        """
    def children_count(self, resolution: int | None = None) -> int:
        """Number of descendant cells at a finer resolution, counted closed-form without materializing them.

        Parameters
        ----------
        resolution : int, optional
            Target resolution; must not be coarser than this cell's.
            Defaults to one finer than this cell's resolution;
            a maximum-depth cell has no children and returns ``0``.

        Returns
        -------
        int
            The exact descendant count (H3 pentagons have slightly fewer
            than hexagons).

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.children_count(resolution=8)
        7
        """
    def center_child(self, resolution: int) -> H3Cell:
        """Return the child whose center coincides with this cell's center.

        Parameters
        ----------
        resolution : int
            Target resolution (``0``-``15``); must not be coarser than this
            cell's.

        Returns
        -------
        H3Cell
            The center child at ``resolution``.

        Raises
        ------
        GeometryError
            If ``resolution`` is out of range or coarser than the cell's.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.center_child(resolution=8).token
        '8828308281fffff'
        """
    def child_at(self, position: int, resolution: int) -> H3Cell:
        """Return the child at ``position`` in this cell's ordered descendants.

        The inverse of ``child_position``:
        ``cell.child_at(position, resolution)`` recovers the cell that
        reported ``position`` at this cell's resolution.

        Parameters
        ----------
        position : int
            Zero-based position among this cell's descendants at
            ``resolution`` (``0`` to ``children_count(resolution) - 1``).
        resolution : int
            Target resolution (``0``-``15``); must not be coarser than this
            cell's.

        Returns
        -------
        H3Cell
            The descendant cell at ``position``.

        Raises
        ------
        GeometryError
            If ``resolution`` is out of range or coarser than the cell's.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.child_at(0, 8).token
        '8828308281fffff'
        """
    @property
    def vertices(self) -> H3VertexArray:
        """The cell's topological vertices, with canonical shared identity.

        Adjacent cells return the *same* vertex objects for their shared
        corners (equal ids), so vertices deduplicate across a coverage —
        unlike `polygon`, which yields per-cell coordinate copies.

        Returns
        -------
        H3VertexArray
            Five vertices for a pentagon, six for a hexagon.
        """
    def edge(self, destination: H3Cell | int | str) -> H3Edge:
        """Return the directed edge from this cell to a neighboring cell.

        Parameters
        ----------
        destination : H3Cell, int, or str
            The neighboring cell the edge points into.

        Returns
        -------
        H3Edge

        Raises
        ------
        GeometryError
            If ``destination`` is not a neighbor of this cell.
        ParseError
            If ``destination`` is not a valid H3 cell.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.H3Cell(13.4, 52.5, resolution=7)
        >>> edge = cell.edge(cell.neighbors[0])
        >>> (edge.origin == cell, edge.destination == cell.neighbors[0])
        (True, True)
        """
    @property
    def edges(self) -> H3EdgeArray:
        """The directed edges leaving this cell (6, or 5 on a pentagon).

        Returns
        -------
        H3EdgeArray
        """
    def grid_disk(self, k: int) -> CellArray[H3Cell]:
        """Return cells within k grid steps (filled disk).

        Parameters
        ----------
        k : int
            Grid radius in steps (``>= 0``); ``k=0`` is this cell alone.

        Returns
        -------
        CellArray of H3Cell

        Raises
        ------
        GeometryError
            If ``k`` is negative or too large.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> len(cell.grid_disk(1))
        7
        """
    def grid_ring(self, k: int) -> CellArray[H3Cell]:
        """Return cells exactly ``k`` grid steps away (hollow ring).

        Parameters
        ----------
        k : int
            Grid radius in steps (``>= 0``); ``k=0`` is this cell alone.

        Returns
        -------
        CellArray of H3Cell

        Raises
        ------
        GeometryError
            If ``k`` is negative or too large.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> len(cell.grid_ring(1))
        6
        """
    def grid_distance(self, other: H3Cell | int | str) -> int:
        """Grid-step distance to another cell.

        Parameters
        ----------
        other : H3Cell, int, or str
            The target cell; must share this cell's resolution.

        Returns
        -------
        int

        Raises
        ------
        GeometryError
            If the cells cannot be connected (different resolutions or too far apart).
        ParseError
            If ``other`` is not a valid H3 cell.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> cell.grid_distance(list(cell.neighbors)[0])
        1
        """
    def grid_path(self, other: H3Cell | int | str) -> CellArray[H3Cell]:
        """Grid path of cells to another cell.

        Parameters
        ----------
        other : H3Cell, int, or str
            The target cell; must share this cell's resolution.

        Returns
        -------
        CellArray of H3Cell

        Raises
        ------
        GeometryError
            If the cells cannot be connected (different resolutions or too far apart).
        ParseError
            If ``other`` is not a valid H3 cell.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
        >>> nbr = list(cell.neighbors)[0]
        >>> len(cell.grid_path(nbr))
        2
        """

@final
class S2CoverageIterator(_CoverageIterator['S2Cell']):
    """Lazy iterator over a coverage's cells, yielding one cell per step."""
    def __reversed__(self) -> Self:
        """Return a reverse iterator over the same coverage cells."""

@final
class S2Coverage(_Coverage['S2Cell']):
    """An exact-classified S2 covering of a geometry.

    Returned by ``s2_cover(...)``: ``coverage.cells`` materializes the
    cells selected by ``cell_rule`` within the level budget (join keys,
    bins, visualization), while ``covers``/``contains``/``intersects``
    answer exactly against the source geometry, independent of the rule.
    Iterate it, test ``cell in coverage``, or ``compact``/``uncompact``
    across levels.
    """

    __match_args__: Final = ('cells',)
    @property
    def level_mod(self) -> int:
        """Level stride of the covering (emitted levels step by this much).

        Returns
        -------
        int
        """
    @property
    def min_level(self) -> int:
        """Minimum (coarsest) cell level allowed in the covering.

        Returns
        -------
        int
        """
    @property
    def max_level(self) -> int:
        """Maximum (finest) cell level allowed in the covering.

        Returns
        -------
        int
        """
    @property
    def max_cells(self) -> int | None:
        """Configured fixed-level emission cap from the factory. Adaptive covers
        retain this value for introspection but use ``target_cells`` instead.
        ``None`` means unlimited for fixed-level construction.

        Returns
        -------
        int or None
        """
    @property
    def target_cells(self) -> int:
        """Adaptive refinement target from the factory. It guides optional
        subdivision and does not affect fixed-level construction.

        Returns
        -------
        int
        """
    @property
    def level(self) -> int | None:
        """Fixed S2 cell level of the **visible** cell set, or ``None`` when the
        visible cells span multiple levels (adaptive / compacted).

        Factory cover bounds stay on ``min_level`` / ``max_level`` (pickle
        recompute uses those); after ``uncompact`` the visible set is uniform
        even when the source covering was adaptive.

        Returns
        -------
        int or None
        """
    def with_parents(self, *, min_level: int = 0) -> S2Coverage:
        """Include parent cells down to a minimum level.

        Parameters
        ----------
        min_level : int, default 0
            Coarsest level to add parents for (0 is the root face level).

        Returns
        -------
        S2Coverage

        Raises
        ------
        GeometryError
            If ``min_level`` is out of range.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.s2_cover(p, level=12)
        >>> len(cov.with_parents().cells) >= len(cov.cells)
        True
        """
    def compact(self, *, min_level: int = 0) -> S2Coverage:
        """Compact the cell set to its coarsest covering (merge complete sibling
        groups into their parent).

        Parameters
        ----------
        min_level : int, default 0
            Coarsest level compaction may produce; merging stops at this floor
            (cells already coarser pass through unchanged).

        Returns
        -------
        S2Coverage
            The compacted covering (same area, fewest cells).

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.s2_cover(p, level=12)
        >>> len(cov.compact().cells) <= len(cov.cells)
        True
        """
    def uncompact(self, level: int) -> S2Coverage:
        """Expand the cell set to a uniform level (every cell subdivided down to
        ``level``).

        Parameters
        ----------
        level : int
            Target S2 level (``0``-``30``); no coarser than any current cell.

        Returns
        -------
        S2Coverage
            The expanded covering.

        Raises
        ------
        GeometryError
            If ``level`` is out of range.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.s2_cover(p, level=12)
        >>> len(cov.uncompact(12).cells) >= len(cov.cells)
        True
        """

@final
class S2Cell(_NumericCell):
    """One S2 cell: a level-addressed quadrilateral tile on the sphere.

    Wraps the 64-bit cell id with typed accessors (``cell.level``,
    ``cell.token``, ``cell.polygon``, ``cell.center``) and hierarchy
    moves (``parent``/``children``/``neighbors``). Convert via
    ``S2Cell(...)``, and back with ``int(cell)``.
    """

    __match_args__: Final = ('id',)
    @property
    def level(self) -> int:
        """S2 level of this cell (``0``-``30``).

        Returns
        -------
        int
        """
    @overload
    def __new__(cls, value: S2Cell | int | str, /) -> S2Cell: ...
    @overload
    def __new__(cls, value: Point, /, *, level: int) -> S2Cell: ...
    @overload
    def __new__(cls, value: float, /, lat: float, *, level: int) -> S2Cell:
        """One S2 cell from an id, token, lon/lat pair, or point geometry.

        Parameters
        ----------
        lon : S2Cell, int, str, float, or Point
            A cell id/token, the longitude of a ``lon, lat`` pair, or a point
            geometry.

        lat : float, optional
            Latitude when ``lon`` is a scalar longitude.

        level : int, optional
            S2 level (``0``-``30``); required for coordinate construction.

        Returns
        -------
        S2Cell

        Raises
        ------
        ParseError
            If ``value`` is not a valid S2 cell id or token.
        GeometryError
            If ``level`` is out of range.
        InvalidGeometryError
            If a scalar coordinate is non-finite or out of range.
        """
    def parent(self, level: int | None = None) -> S2Cell:
        """Parent cell at a coarser level.

        Parameters
        ----------
        level : int, optional
            Target level; must not be finer than this cell's.
            Defaults to one coarser than this cell's level.

        Returns
        -------
        S2Cell
            The ancestor cell.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
        >>> cell.parent(10).token
        '808581'
        """
    def children(self, level: int | None = None) -> CellArray[S2Cell]:
        """Child cells at a finer level.

        Parameters
        ----------
        level : int, optional
            Target level; must not be coarser than this cell's.
            Defaults to one finer than this cell's level;
            a maximum-depth cell has no children and yields an empty
            CellArray.

        Returns
        -------
        CellArray of S2Cell
            The descendant cells.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
        >>> len(cell.children(13))
        4
        """
    def children_count(self, level: int | None = None) -> int:
        """Number of descendant cells at a finer level, counted closed-form without materializing them.

        Parameters
        ----------
        level : int, optional
            Target level; must not be coarser than this cell's.
            Defaults to one finer than this cell's level;
            a maximum-depth cell has no children and returns ``0``.

        Returns
        -------
        int
            The exact descendant count (H3 pentagons have slightly fewer
            than hexagons).

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
        >>> cell.children_count(13)
        4
        """

@final
class Tile(_NumericCell):
    """One XYZ web-mercator tile: the slippy-map ``z/x/y`` address.

    Wraps the tile with typed accessors (``tile.zoom``/``x``/``y``,
    ``tile.token``, ``tile.polygon``, ``tile.center``) and hierarchy
    moves (``parent``/``children``/``neighbors``). The token is the Bing
    quadkey (empty at ``z0``); the 64-bit id packs ``(zoom, Morton x/y)``
    so sorted ids group spatial neighbors. Convert via ``Tile(value)``,
    ``Tile(Point(...), zoom=...)``, ``Tile(lon=..., lat=..., zoom=...)``, or
    ``Tile(x=..., y=..., zoom=...)``. Coordinate frames are always named.
    """

    __match_args__: Final = ('id',)

    @property
    def zoom(self) -> int:
        """Zoom level of this tile (``0``-``29``).

        Returns
        -------
        int
        """
    @overload
    def __new__(cls, value: Tile | int | str, /) -> Tile: ...
    @overload
    def __new__(cls, value: Point, /, *, zoom: int) -> Tile: ...
    @overload
    def __new__(cls, /, *, lon: float, lat: float, zoom: int) -> Tile: ...
    @overload
    def __new__(cls, /, *, x: int, y: int, zoom: int) -> Tile:
        """One XYZ tile from a packed id, quadkey, lon/lat keywords, point geometry,
        or explicit ``x=``/``y=`` tile coordinates.

        Parameters
        ----------
        value : Tile, int, str, or Point, optional
            A tile id/quadkey, or a point geometry when paired with ``zoom``.

        lon, lat : float, optional
            Geographic coordinates, supplied together with ``zoom``. They are
            keyword-only because two bare numbers do not select a coordinate
            frame.

        zoom : int, optional
            Zoom level (``0``-``29``); keyword-only, required for every
            coordinate form.

        x, y : int, optional
            Explicit tile column/row (keyword-only, with ``zoom``) — never
            inferred from a positional pair, so lon/lat can't be misread as
            tile coordinates.

        Returns
        -------
        Tile

        Raises
        ------
        ParseError
            If ``value`` is not a valid tile id or quadkey.
        GeometryError
            If ``zoom`` is out of range, or ``x``/``y`` is outside
            ``[0, 2**zoom)``.
        InvalidGeometryError
            If a scalar coordinate is non-finite or out of range.
        """
    def parent(self, zoom: int | None = None) -> Tile:
        """Parent cell at a coarser zoom.

        Parameters
        ----------
        zoom : int, optional
            Target zoom; must not be finer than this cell's.
            Defaults to one coarser than this cell's zoom.

        Returns
        -------
        Tile
            The ancestor cell.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
        >>> str(cell.parent())
        '023010203'
        """
    def children(self, zoom: int | None = None) -> CellArray[Tile]:
        """Child cells at a finer zoom.

        Parameters
        ----------
        zoom : int, optional
            Target zoom; must not be coarser than this cell's.
            Defaults to one finer than this cell's zoom;
            a maximum-depth cell has no children and yields an empty
            CellArray.

        Returns
        -------
        CellArray of Tile
            The descendant cells.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
        >>> len(cell.children())
        4
        """
    def children_count(self, zoom: int | None = None) -> int:
        """Number of descendant cells at a finer zoom, counted closed-form without materializing them.

        Parameters
        ----------
        zoom : int, optional
            Target zoom; must not be coarser than this cell's.
            Defaults to one finer than this cell's zoom;
            a maximum-depth cell has no children and returns ``0``.

        Returns
        -------
        int
            The exact descendant count (H3 pentagons have slightly fewer
            than hexagons).

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
        >>> cell.children_count()
        4
        """
    @property
    def x(self) -> int:
        """Tile column (west to east).

        Returns
        -------
        int
        """
    @property
    def y(self) -> int:
        """Tile row (north to south).

        Returns
        -------
        int
        """
    @property
    def morton(self) -> int:
        """The Morton (Z-order) index of ``x``/``y`` within this zoom.

        Returns
        -------
        int
        """

@final
class TileCoverageIterator(_CoverageIterator[Tile]):
    """Lazy iterator over a coverage's cells, yielding one cell per step."""
    def __reversed__(self) -> Self:
        """Return a reverse iterator over the same coverage cells."""

@final
class TileCoverage(_Coverage[Tile]):
    """An XYZ-tile covering of a geometry (the ``tile_cover`` backend).

    Returned by ``tile_cover(...)``: ``coverage.cells`` materializes
    the tiles selected by ``cell_rule`` at the chosen zoom (join keys,
    bins, visualization), while ``covers``/``contains``/``intersects``
    answer exactly against the source geometry, independent of the rule.
    """

    __match_args__: Final = ('cells',)
    @property
    def zoom(self) -> int | None:
        """Uniform zoom level of the covering's tiles, or ``None`` for mixed
        zooms.

        Returns
        -------
        int or None
        """
    def compact(self, *, min_zoom: int = 0) -> TileCoverage:
        """Compact the tile set to its coarsest covering.

        Parameters
        ----------
        min_zoom : int, default 0
            Coarsest zoom compaction may produce.

        Returns
        -------
        TileCoverage

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.tile_cover(p, zoom=10)
        >>> len(cov.compact().cells) <= len(cov.cells)
        True
        """
    def uncompact(self, zoom: int) -> TileCoverage:
        """Expand the tile set to a uniform zoom.

        Parameters
        ----------
        zoom : int
            Target zoom (``0``-``29``); no coarser than any current tile.

        Returns
        -------
        TileCoverage

        Raises
        ------
        GeometryError
            If ``zoom`` is coarser than a current tile.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.tile_cover(p, zoom=10)
        >>> len(cov.uncompact(10).cells) >= len(cov.cells)
        True
        """
    def with_parents(self, *, min_zoom: int = 0) -> TileCoverage:
        """Include parent tiles down to a minimum zoom.

        Parameters
        ----------
        min_zoom : int, default 0
            Coarsest zoom to add parents for.

        Returns
        -------
        TileCoverage

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cov = gm.tile_cover(p, zoom=10)
        >>> len(cov.with_parents().cells) >= len(cov.cells)
        True
        """

@final
class ValidationReport:
    """A structured geometry-validity verdict.

    Returned by ``geom.validate()``: truthy when the geometry is valid;
    otherwise ``report.reason`` names the OGC violation, ``report.location``
    pinpoints it, and ``report.path`` addresses the offending part.
    ``report.repair(...)`` returns a repaired copy of the reported geometry.
    """
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Validation reports are returned by ``geom.validate()``."""
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: the report plus the retained geometry
        coordinate payload and any validation issue strings.
        """
    def __copy__(self) -> Self:
        """``copy.copy`` returns the report itself — it is an immutable value."""
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the report itself: every field is immutable
        and holds no Python references.
        """
    def __reduce__(
        self,
    ) -> tuple[
        Any, tuple[Geometry, tuple[str, tuple[float, float] | None, str | None] | None]
    ]:
        """Pickle support: serialize the geometry only; the verdict is recomputed
        on unpickle (never trusts derived state in the payload).
        """
    __match_args__: Final = ('valid', 'reason', 'location', 'path')
    def __bool__(self) -> bool:
        """True if self else False"""
    @property
    def valid(self) -> bool:
        """Whether the geometry is valid (also the report's truth value).

        Returns
        -------
        bool
        """
    @property
    def reason(self) -> str | None:
        """Human-readable reason for the first validity problem, or ``None``.

        Returns
        -------
        str or None
        """
    @property
    def location(self) -> tuple[float, float] | None:
        """``(x, y)`` location of the first problem, when known.

        Returns
        -------
        tuple or None
        """
    @property
    def path(self) -> str | None:
        """Structural path to the first problem (e.g. ``'$.shell'``), when known.

        Returns
        -------
        str or None
        """
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Geometry:
        """Return a repaired copy of the validated geometry (see
        `Geometry.repair`).

        Parameters
        ----------
        method : {'linework', 'structure'}, default 'linework'
            Repair strategy: rebuild from noded linework, or fix ring structure.

        Returns
        -------
        Geometry
            A valid geometry.


        Examples
        --------
        >>> import gometry as gm
        >>> bad = gm.from_wkt('POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))')
        >>> bad.validate().repair().is_valid
        True
        """

@overload
def box(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    *,
    crs: CrsInput | None = None,
    wrap: None = None,
    ccw: bool = True,
    epoch: float | None = None,
) -> Polygon: ...
@overload
def box(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    *,
    crs: CrsInput,
    wrap: BoxWrap,
    ccw: bool = True,
    epoch: float | None = None,
) -> Polygon | MultiPolygon:
    """Create a rectangular ``Polygon`` from bounds ``(minx, miny, maxx, maxy)``.

    Parameters
    ----------
    minx, miny, maxx, maxy : float
        Finite rectangle bounds; each minimum must not exceed its maximum,
        except that ``minx > maxx`` is allowed with ``wrap='split'`` to wrap a
        geographic box across the antimeridian.

    crs : str or int, optional
        CRS label (EPSG code, authority string, or WKT); attached as metadata,
        coordinates are not transformed.
        With a geographic degree CRS, horizontal sides are latitude parallels,
        not corner-to-corner geodesics. Material departures are tessellated
        with equal longitude chords.

    wrap : {'split'}, optional
        Antimeridian handling for geographic (EPSG:4326) boxes. ``'split'``
        lets ``minx`` exceed ``maxx`` to span the 180° meridian, returning a
        `MultiPolygon` split at the antimeridian. The default (``None``) leaves
        coordinates unwrapped and requires ``minx <= maxx``.

    ccw : bool, default True
        If ``True`` the ring is counter-clockwise; ``False`` makes it clockwise.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    Polygon | MultiPolygon
        A rectangular polygon, or a MultiPolygon when ``wrap='split'`` spans
        the antimeridian.

    Raises
    ------
    InvalidGeometryError
        If a bound is non-finite or a minimum exceeds its maximum without
        ``wrap='split'``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``wrap='split'`` is used
        without ``crs=4326``.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.box(0, 0, 2, 1).to_wkt()
    'POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'
    >>> gm.box(0, 0, 2, 1, ccw=False).to_wkt()
    'POLYGON ((0 0, 0 1, 2 1, 2 0, 0 0))'
    """

def points(
    x: FloatInput,
    y: FloatInput,
    *,
    z: FloatInput | None = None,
    m: FloatInput | None = None,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Point]:
    """Create a ``GeometryArray`` of points from parallel coordinate columns.

    Taking separate ``x`` and ``y`` (not interleaved tuples) keeps axis order
    explicit and avoids the lon/lat-vs-lat/lon footgun. Each column accepts a
    scalar (broadcast to every row) or a sequence of floats (one per point).

    Parameters
    ----------
    x, y : float or sequence of float
        X and Y ordinates. At least one must be sequence of float to set the row count;
        scalars broadcast numpy-style.

    z, m : float or sequence of float, optional
        Z and M ordinates, broadcast like ``x``/``y``.

    crs : str or int, optional
        CRS label (EPSG code, authority string, or WKT); attached as metadata,
        coordinates are not transformed.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One Point per coordinate.

    Raises
    ------
    InvalidGeometryError
        If columns differ in length or are non-finite.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If every argument is scalar (use `Point`) or ``epoch`` is invalid.

    See Also
    --------
    Point : Build a single point.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.points([0, 1], [0, 1]).to_wkt()
    ['POINT (0 0)', 'POINT (1 1)']
    """

@overload
def boxes(
    minx: FloatInput,
    miny: FloatInput,
    maxx: FloatInput,
    maxy: FloatInput,
    *,
    crs: CrsInput | None = None,
    wrap: None = None,
    ccw: bool = True,
    epoch: float | None = None,
) -> GeometryArray[Polygon]: ...
@overload
def boxes(
    minx: FloatInput,
    miny: FloatInput,
    maxx: FloatInput,
    maxy: FloatInput,
    *,
    crs: CrsInput,
    wrap: BoxWrap,
    ccw: bool = True,
    epoch: float | None = None,
) -> GeometryArray[Polygon | MultiPolygon]: ...
@overload
def boxes(
    minx: FloatInput,
    miny: FloatInput,
    maxx: FloatInput,
    maxy: FloatInput,
    *,
    crs: CrsInput | None = None,
    wrap: BoxWrap | None = None,
    ccw: bool = True,
    epoch: float | None = None,
) -> GeometryArray[Polygon | MultiPolygon]:
    """Create a ``GeometryArray`` of rectangular polygons from bound columns.

    Parameters
    ----------
    minx, miny, maxx, maxy : float or sequence of float
        Rectangle bounds per row. At least one must be sequence of float; scalars
        broadcast numpy-style.

    crs : str or int, optional
        CRS applied to every box. With a geographic degree CRS, horizontal
        sides follow latitude parallels; material departures are tessellated
        with equal longitude chords.

    wrap : {'split'}, optional
        Antimeridian handling for geographic (EPSG:4326) boxes — same as `box`.

    ccw : bool, default True
        If ``True`` each ring is counter-clockwise; ``False`` makes it clockwise.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One `Polygon` or `MultiPolygon` per bound tuple.

    Raises
    ------
    InvalidGeometryError
        If bounds are non-finite or invalid per row.
    CRSError
        If ``epoch`` is set without ``crs``, or ``wrap='split'`` is used
        without ``crs=4326``.
    GeometryError
        If every argument is scalar (use `box`) or ``epoch`` is invalid.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.boxes(0, 0, [1, 2], [1, 2]).to_wkt()
    ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))']
    """

def line_strings(
    values: Iterable[Iterable[Iterable[float]]],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[LineString]:
    """Create a ``GeometryArray`` of linestrings from per-line coordinate inputs.

    Parameters
    ----------
    values : sequence
        Each member is a raw coordinate sequence accepted by `LineString`.

    crs : str or int, optional
        CRS applied to every linestring.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One `LineString` per input sequence.

    Raises
    ------
    InvalidGeometryError
        If a member line has fewer than two vertices or non-finite coordinates.
    TypeError
        If a member is an already-built geometry; use `GeometryArray(values)`.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> lines = gm.line_strings([[(0, 0), (1, 1)], [(2, 2), (3, 3)]])
    >>> len(lines)
    2
    """

def polygons(
    values: Iterable[Iterable[Iterable[float]] | Iterable[Iterable[Iterable[float]]]],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Polygon]:
    """Create a ``GeometryArray`` of polygons from per-polygon ring inputs.

    Parameters
    ----------
    values : sequence
        Each member is a raw shell coordinate sequence or ``[shell, *holes]``.

    crs : str or int, optional
        CRS applied to every polygon.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One `Polygon` per input.

    Raises
    ------
    InvalidGeometryError
        If a ring has fewer than three corners or non-finite coordinates.
    TypeError
        If a member is an already-built geometry; use `GeometryArray(values)`.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> polys = gm.polygons([[(0, 0), (1, 0), (1, 1), (0, 1)]])
    >>> len(polys)
    1
    """

def multi_points(
    values: Iterable[Iterable[Iterable[float]]],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[MultiPoint]:
    """Create a ``GeometryArray`` of multipoints from per-multipoint inputs.

    Parameters
    ----------
    values : sequence
        Each member is a raw coordinate sequence accepted by `MultiPoint`.

    crs : str or int, optional
        CRS applied to every multipoint.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One `MultiPoint` per input.

    Raises
    ------
    InvalidGeometryError
        If any coordinate is non-finite or has mixed dimensionality.
    TypeError
        If a member is an already-built geometry; use `GeometryArray(values)`.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.multi_points([[(0, 0), (1, 1)]]).to_wkt()
    ['MULTIPOINT ((0 0), (1 1))']
    """

def multi_line_strings(
    values: Iterable[Iterable[Iterable[Iterable[float]]]],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[MultiLineString]:
    """Create a ``GeometryArray`` of multilinestrings from per-multiline inputs.

    Parameters
    ----------
    values : sequence
        Each member is raw line coordinate sequences accepted by `MultiLineString`.

    crs : str or int, optional
        CRS applied to every multilinestring.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One `MultiLineString` per input.

    Raises
    ------
    InvalidGeometryError
        If a member line has fewer than two vertices or non-finite coordinates.
    TypeError
        If a member is an already-built geometry; use `GeometryArray(values)`.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.multi_line_strings([[[(0, 0), (1, 1)]]]).to_wkt()
    ['MULTILINESTRING ((0 0, 1 1))']
    """

def multi_polygons(
    values: Iterable[Iterable[Iterable[Iterable[Iterable[float]]]]],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[MultiPolygon]:
    """Create a ``GeometryArray`` of multipolygons from per-multipolygon inputs.

    Parameters
    ----------
    values : sequence
        Each member is raw polygon coordinate sequences accepted by `MultiPolygon`.

    crs : str or int, optional
        CRS applied to every multipolygon.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        One `MultiPolygon` per input.

    Raises
    ------
    InvalidGeometryError
        If any ring has fewer than three corners or non-finite coordinates.
    TypeError
        If a member is an already-built geometry; use `GeometryArray(values)`.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.multi_polygons([[[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]]]]).to_wkt()
    ['MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))']
    """

@overload
def area(geom: Geometry, *, unit: DistanceUnit | None = None) -> float: ...
@overload
def area(
    geom: GeometryArray, *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def area(
    geom: Iterable[_GeometryLike], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def area(
    geom: Geometry | GeometryArray | Iterable[_GeometryLike],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute area in CRS-natural units or with a ``unit`` override.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values
        Input geometry, array, or iterable materialized as an array.
    unit : {'planar', 'meters'} or None, default None
        ``None`` follows the geometry's CRS, exactly like ``geom.area``.
        ``'planar'`` forces raw coordinate units; ``'meters'`` forces the CRS
        metric and raises without a CRS.

    Returns
    -------
    float or numpy.ndarray
        Scalar area or one value per row.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result.
    GeometryError
        If ``unit='meters'`` is requested for a CRS-free geometry.

    See Also
    --------
    area : CRS-natural property form.
    length : Length/perimeter with the same ``unit`` override.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.area(gm.box(0, 0, 2, 2), unit='planar')
    4.0
    """

@overload
def length(geom: Geometry, *, unit: DistanceUnit | None = None) -> float: ...
@overload
def length(
    geom: GeometryArray, *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length(
    geom: Iterable[_GeometryLike], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length(
    geom: Geometry | GeometryArray | Iterable[_GeometryLike],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute length in CRS-natural units or with a ``unit`` override.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values
        Input geometry, array, or iterable materialized as an array.
    unit : {'planar', 'meters'} or None, default None
        ``None`` follows the geometry's CRS, exactly like ``geom.length``.
        ``'planar'`` forces raw coordinate units; ``'meters'`` forces the CRS
        metric and raises without a CRS.

    Returns
    -------
    float or numpy.ndarray
        Scalar length or one value per row.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result.
    GeometryError
        If ``unit='meters'`` is requested for a CRS-free geometry.

    See Also
    --------
    length : CRS-natural property form.
    area : Area with the same ``unit`` override.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.length(gm.LineString([(0, 0), (3, 4)]), unit='planar')
    5.0
    """

@overload
def length_3d(geom: Geometry, *, unit: DistanceUnit | None = None) -> float: ...
@overload
def length_3d(
    geom: GeometryArray, *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length_3d(
    geom: Iterable[_GeometryLike], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length_3d(
    geom: Geometry | GeometryArray | Iterable[_GeometryLike],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute 3D length in CRS-natural units or with a ``unit`` override.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values
        Input geometry, array, or iterable materialized as an array.
    unit : {'planar', 'meters'} or None, default None
        ``None`` follows the geometry's CRS, exactly like ``geom.length_3d``.
        ``'planar'`` forces raw coordinate units; ``'meters'`` forces the CRS
        metric and raises without a CRS.

    Returns
    -------
    float or numpy.ndarray
        Scalar 3D length or one value per row.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result.
    GeometryError
        If ``unit='meters'`` is requested for a CRS-free geometry.
    InvalidGeometryError
        If a scalar geometry lacks Z on every vertex.

    See Also
    --------
    length_3d : CRS-natural property form.
    length : The 2D sibling, with the same ``unit`` override.
    distance_3d : Pairwise 3D distance under the same metric.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.length_3d(gm.from_wkt('LINESTRING Z (0 0 0, 3 4 0)'), unit='planar')
    5.0
    """

@overload
def snap(geom: _GeometryT, reference: Geometry, tolerance: float) -> _GeometryT: ...
@overload
def snap(
    geom: GeometryArray[_GeometryT],
    reference: Geometry | GeometryArray,
    tolerance: FloatInput,
) -> GeometryArray[_GeometryT]: ...
@overload
def snap(
    geom: _GeometryT,
    reference: GeometryArray,
    tolerance: FloatInput,
) -> GeometryArray[_GeometryT]: ...
@overload
def snap(
    geom: Geometry | GeometryArray,
    reference: Geometry | GeometryArray,
    tolerance: FloatInput,
) -> Geometry | GeometryArray:
    """Snap vertices of ``geom`` onto ``reference`` within ``tolerance``.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Geometry whose vertices are moved.
    reference : Geometry or GeometryArray
        Target geometry to snap onto.
    tolerance : float or sequence of float
        Maximum snap distance in coordinate units.

    Returns
    -------
    Geometry or GeometryArray
        Snapped result(s).

    Raises
    ------
    CRSMismatchError
        If operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``tolerance`` is invalid.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.snap(gm.Point(0.01, 0), gm.Point(0, 0), 0.1).to_wkt()
    'POINT (0 0)'
    """

@overload
def bearing(
    left: Point,
    right: Point,
    *,
    path: NavigationPath = 'geodesic',
) -> float: ...
@overload
def bearing(
    left: GeometryArray[Point],
    right: Point | GeometryArray[Point],
    *,
    path: NavigationPath = 'geodesic',
) -> npt.NDArray[np.float64]: ...
@overload
def bearing(
    left: Point,
    right: GeometryArray[Point],
    *,
    path: NavigationPath = 'geodesic',
) -> npt.NDArray[np.float64]: ...
@overload
def bearing(
    left: Point | GeometryArray[Point],
    right: Point | GeometryArray[Point],
    *,
    path: NavigationPath = 'geodesic',
) -> float | npt.NDArray[np.float64]:
    """Initial bearing from one point to another, in degrees clockwise from north
    (``0..360``).

    Geodesic by default on a geographic CRS (on the CRS ellipsoid); grid
    azimuth on a projected or CRS-free point. ``path='rhumb'`` selects the
    constant compass course on a geographic CRS.

    Parameters
    ----------
    left, right : Point or GeometryArray
        Origin and destination; must share a CRS.
    path : {'geodesic', 'rhumb'}, default 'geodesic'
        Route model. Rhumb bearings require a geographic CRS.

    Returns
    -------
    float or numpy.ndarray
        One bearing per input pair.

    Raises
    ------
    GeometryTypeError
        If either operand is not a `Point`.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    CRSError
        If a coordinate is outside the longitude/latitude domain.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.bearing(gm.Point(0, 0), gm.Point(1, 0))
    90.0
    """

@overload
def cross_track_distance(point: Point, start: Point, end: Point) -> float: ...
@overload
def cross_track_distance(
    point: Point | GeometryArray[Point],
    start: Point | GeometryArray[Point],
    end: Point | GeometryArray[Point],
) -> npt.NDArray[np.float64]:
    """Signed distance from a point to the great circle through ``start`` and
    ``end``, in meters.

    Positive when the point lies left of the directed path ``start -> end``,
    negative right, zero on it. Spherical on the CRS ellipsoid's mean radius.
    Geographic CRS only.

    Parameters
    ----------
    point, start, end : Point or GeometryArray
        The probe point and two distinct endpoints defining the directed path;
        all must share a CRS.

    Returns
    -------
    float or numpy.ndarray
        Signed meters off-path.

    Raises
    ------
    GeometryTypeError
        If any operand is not a `Point`.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    CRSError
        If the CRS is missing or not geographic.
    InvalidGeometryError
        If ``start`` equals ``end``.

    Examples
    --------
    >>> import gometry as gm
    >>> a, b = gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326)
    >>> probe = gm.Point(0.5, 0.1, crs=4326)
    >>> round(gm.cross_track_distance(probe, a, b) / 1000, 1)
    11.1
    """

@overload
def destination(
    point: Point,
    bearing: float,
    distance: float,
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> Point: ...
@overload
def destination(
    point: GeometryArray[Point],
    bearing: FloatInput,
    distance: FloatInput,
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def destination(
    point: Point,
    bearing: FloatColumn,
    distance: FloatInput,
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def destination(
    point: Point,
    bearing: FloatInput,
    distance: FloatColumn,
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def destination(
    point: Point,
    bearing: float,
    distance: float,
    *,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> Point: ...
@overload
def destination(
    point: GeometryArray[Point],
    bearing: FloatInput,
    distance: FloatInput,
    *,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]: ...
@overload
def destination(
    point: Point,
    bearing: FloatColumn,
    distance: FloatInput,
    *,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]: ...
@overload
def destination(
    point: Point,
    bearing: FloatInput,
    distance: FloatColumn,
    *,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]:
    """Return the point reached from ``point`` along ``bearing`` for ``distance``.

    CRS-aware like every metric: geodesic on a geographic CRS, a planar offset
    in native units on a projected CRS, and coordinate units when CRS-free.
    ``path='rhumb'`` instead follows a constant compass course in meters on a
    geographic CRS.

    Parameters
    ----------
    point : Point or GeometryArray
        Starting point(s).
    bearing : float or sequence of float
        Initial azimuth(s) in degrees clockwise from north.
    distance : float or sequence of float
        Distance(s) to travel (geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units otherwise).
    path : {'geodesic', 'rhumb'}, default 'geodesic'
        Route model. Rhumb paths require a geographic CRS and use meters.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    Point or GeometryArray
        One destination per input point.

    Raises
    ------
    GeometryError
        If ``bearing`` or ``distance`` is invalid.
    GeometryTypeError
        If ``point`` is not a `Point`.
    CRSError
        If a coordinate is outside the longitude/latitude domain.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.destination(gm.Point(0, 0, crs=4326), distance=1000, bearing=90).to_wkt(precision=5)
    'POINT (0.00898 0)'
    """

@overload
def point_between(
    left: Point,
    right: Point,
    distance: float,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> Point: ...
@overload
def point_between(
    left: GeometryArray[Point],
    right: Point | GeometryArray[Point],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: Point,
    right: GeometryArray[Point],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: Point,
    right: Point,
    distance: FloatColumn,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: Point,
    right: Point,
    distance: float,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> Point: ...
@overload
def point_between(
    left: GeometryArray[Point],
    right: Point | GeometryArray[Point],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: Point,
    right: GeometryArray[Point],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: Point,
    right: Point,
    distance: FloatColumn,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]:
    """Return a point interpolated from ``left`` towards ``right``.

    CRS-aware like every metric: geodesic on a geographic CRS, planar on a
    projected CRS, and coordinate units when CRS-free. ``path='rhumb'`` follows
    the constant-bearing track on a geographic CRS. Z/M interpolate.

    Parameters
    ----------
    left, right : Point or GeometryArray
        Endpoints; must share a CRS.
    distance : float or sequence of float
        Distance(s) from ``left`` (or ``[0, 1]`` fractions if ``normalized``).
    normalized : bool, default False
        Treat ``distance`` as a fraction of the total distance.
    path : {'geodesic', 'rhumb'}, default 'geodesic'
        Route model. Rhumb paths require a geographic CRS and use meters.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    Point or GeometryArray
        One interpolated point per input pair.

    Raises
    ------
    GeometryTypeError
        If either operand is not a `Point`.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``distance`` is not finite.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.point_between(gm.Point(0, 0), gm.Point(2, 0), 0.5).to_wkt()
    'POINT (0.5 0)'
    """

@overload
def rhumb_distance(left: Point, right: Point) -> float: ...
@overload
def rhumb_distance(
    left: GeometryArray[Point], right: Point | GeometryArray[Point]
) -> npt.NDArray[np.float64]: ...
@overload
def rhumb_distance(
    left: Point, right: GeometryArray[Point]
) -> npt.NDArray[np.float64]: ...
@overload
def rhumb_distance(
    left: Point | GeometryArray[Point], right: Point | GeometryArray[Point]
) -> float | npt.NDArray[np.float64]:
    """Rhumb-line (loxodrome) distance between two points, in meters.

    The length of the constant-bearing track on the CRS ellipsoid — the
    navigation sibling of the (shorter) geodesic `distance`. Geographic CRS
    only.

    Parameters
    ----------
    left, right : Point or GeometryArray
        Endpoints; must share a geographic CRS.

    Returns
    -------
    float or numpy.ndarray
        Meters along the rhumb line.

    Raises
    ------
    GeometryTypeError
        If either operand is not a `Point`.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    CRSError
        If the CRS is missing or not geographic.

    Examples
    --------
    >>> import gometry as gm
    >>> jfk, lhr = gm.Point(-73.8, 40.6, crs=4326), gm.Point(-0.5, 51.6, crs=4326)
    >>> round(gm.rhumb_distance(jfk, lhr) / 1000, 1)
    5771.1
    """

@overload
def shared_paths(left: Geometry, right: Geometry) -> GeometryCollection: ...
@overload
def shared_paths(
    left: GeometryArray,
    right: Geometry | GeometryArray,
) -> GeometryArray[GeometryCollection]: ...
@overload
def shared_paths(
    left: Geometry,
    right: GeometryArray,
) -> GeometryArray[GeometryCollection]: ...
@overload
def shared_paths(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> Geometry | GeometryArray:
    """Shared paths between two lineal geometries.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Two lineal geometries.

    Returns
    -------
    Geometry or GeometryArray
        The shared linework.

    Raises
    ------
    CRSMismatchError
        If operands' CRS or coordinate-epoch metadata differ.
    GeometryTypeError
        If either operand is not lineal.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.shared_paths(gm.LineString([(0, 0), (2, 0)]), gm.LineString([(1, 0), (3, 0)])).to_wkt()
    'GEOMETRYCOLLECTION (MULTILINESTRING ((1 0, 2 0)), MULTILINESTRING EMPTY)'
    """

@overload
def bounds(values: GeometryArray) -> npt.NDArray[np.float64]: ...
@overload
def bounds(
    values: GeometryArray | Iterable[_GeometryLike | None],
) -> npt.NDArray[np.float64]:
    """Axis-aligned bounds as a matrix (see `GeometryArray.bounds`).

    Parameters
    ----------
    values : iterable of Geometry or GeometryArray
        Input geometry collection.

    Returns
    -------
    numpy.ndarray
        ``(minx, miny, maxx, maxy)`` per row.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.bounds(gm.GeometryArray([gm.box(0, 0, 2, 3)])).tolist()
    [[0.0, 0.0, 2.0, 3.0]]
    """

@overload
def from_polyline(
    data: str,
    *,
    precision: int = 5,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Point | LineString: ...
@overload
def from_polyline(
    data: Iterable[str | None],
    *,
    precision: int = 5,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray[Point | LineString]:
    """Decode polyline text into ``LineString``/``Point`` geometries.

    The Google Encoded Polyline Algorithm Format — the compact lat/lon
    route encoding used by Google Maps, OSRM, and Valhalla. Accepts one
    string (returns a ``Point`` for one coordinate, otherwise a ``LineString``)
    or an iterable of strings/``None`` rows (returns a `GeometryArray` with
    missing rows). Polylines are WGS84 by definition, so results carry OGC:CRS84
    unless ``crs=None`` explicitly requests a CRS-free result;
    ``epoch`` restores coordinate-epoch metadata on round-trip.

    Parameters
    ----------
    data : str or iterable of str
        Encoded polyline text.
    precision : int, default 5
        Decimal digits encoded per ordinate (``0`` to ``11``); 5 is the
        classic default, 6 the high-resolution variant.
    crs : str or int or None, default 'OGC:CRS84'
        Frame for the decoded longitude/latitude coordinates. Only WGS84
        longitude/latitude CRS are valid; pass ``None`` for CRS-free output.
    epoch : float, optional
        Coordinate epoch (decimal year) to attach as frame metadata.

    Returns
    -------
    Point, LineString, or GeometryArray
        A ``Point`` when the encoding has one coordinate, a ``LineString``
        for two or more, or a `GeometryArray` for iterable input.

    Raises
    ------
    ParseError
        If the text is not valid polyline data.
    GeometryError
        If ``precision`` is out of range.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.

    See Also
    --------
    Geometry.to_polyline : Encode a LineString or Point as a polyline.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.from_polyline('_p~iF~ps|U_ulLnnqC_mqNvxq`@').to_wkt()
    'LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)'
    """

@overload
def contains(left: Geometry, right: Geometry) -> bool: ...
@overload
def contains(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def contains(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` contains ``right``.

    Returns ``True`` if no points of ``right`` lie outside ``left`` and at least
    one interior point of ``right`` lies in the interior of ``left``.
    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Container (``left``) and candidate (``right``). Scalar and
        ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
        epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    within : Inverse relation.
    covers : Boundary-inclusive containment.

    Examples
    --------
    >>> import gometry as gm
    >>> square = gm.box(0, 0, 2, 2)
    >>> gm.contains(square, gm.Point(1, 1))
    True
    >>> gm.contains(square, gm.Point(2, 1))  # boundary: not contained
    False
    """

@overload
def contains_properly(left: Geometry, right: Geometry) -> bool: ...
@overload
def contains_properly(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_properly(
    left: Geometry, right: GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_properly(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` contains ``right`` with no boundary contact.

    Like ``contains``, but ``right`` must lie entirely in the interior of
    ``left`` — touching the boundary of ``left`` anywhere fails (DE-9IM
    ``T**FF*FF*``). Evaluated in the coordinate plane; geographic inputs crossing the antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Container (``left``) and candidate (``right``). Scalar and
        ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
        epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    contains : Boundary contact allowed.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.contains_properly(gm.box(0, 0, 2, 2), gm.Point(1, 1))
    True
    """

@overload
def within(left: Geometry, right: Geometry) -> bool: ...
@overload
def within(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def within(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def within(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` lies within ``right``; inverse of ``contains``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Candidate (``left``) and container (``right``). Scalar and
        ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
        epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    contains : Inverse relation (container first).

    Examples
    --------
    >>> import gometry as gm
    >>> gm.within(gm.Point(1, 1), gm.box(0, 0, 2, 2))
    True
    """

@overload
def covers(left: Geometry, right: Geometry) -> bool: ...
@overload
def covers(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def covers(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def covers(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` covers ``right``: every point of ``right`` lies in
    ``left``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Container (``left``) and candidate (``right``). Scalar and
        ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
        epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    covered_by : Inverse relation.

    Examples
    --------
    >>> import gometry as gm
    >>> square = gm.box(0, 0, 2, 2)
    >>> gm.covers(square, gm.Point(2, 1))  # boundary counts
    True
    """

@overload
def covered_by(left: Geometry, right: Geometry) -> bool: ...
@overload
def covered_by(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def covered_by(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def covered_by(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` is covered by ``right``; inverse of ``covers``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Candidate (``left``) and container (``right``). Scalar and
        ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
        epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    covers : Inverse relation.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.covered_by(gm.Point(2, 1), gm.box(0, 0, 2, 2))  # boundary counts
    True
    """

@overload
def contains_xy(geom: Geometry, x: float, y: float) -> bool: ...
@overload
def contains_xy(
    geom: Geometry,
    x: FloatColumn,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_xy(
    geom: Geometry,
    x: FloatInput,
    y: FloatColumn,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_xy(
    geom: GeometryArray,
    x: FloatInput,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_xy(
    geom: Geometry | GeometryArray, x: FloatInput, y: FloatInput
) -> bool | npt.NDArray[np.bool_]:
    """Test whether a geometry contains each ``(x, y)`` point (vectorized).

    Parameters
    ----------
    geom : Geometry or GeometryArray
        The geometry row(s) to test against.
    x, y : float or sequence of float
        Finite coordinates in ``geom``'s CRS. Geographic antimeridian seams
        and poles use the same topology as point-geometry predicates.

    Returns
    -------
    bool or numpy.ndarray
        A single bool for scalar geometry and coordinates, otherwise one result
        per broadcast row.

    Raises
    ------
    InvalidGeometryError
        If ``x``/``y`` are non-finite or differ in length.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.contains_xy(gm.box(0, 0, 2, 2), 1, 1)
    True
    """

@overload
def intersects_xy(geom: Geometry, x: float, y: float) -> bool: ...
@overload
def intersects_xy(
    geom: Geometry,
    x: FloatColumn,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects_xy(
    geom: Geometry,
    x: FloatInput,
    y: FloatColumn,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects_xy(
    geom: GeometryArray,
    x: FloatInput,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects_xy(
    geom: Geometry | GeometryArray, x: FloatInput, y: FloatInput
) -> bool | npt.NDArray[np.bool_]:
    """Test whether a geometry intersects each ``(x, y)`` point (vectorized).

    Boundary-inclusive (unlike ``contains_xy``), and skips building point
    geometries.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        The geometry row(s) to test against.
    x, y : float or sequence of float
        Finite coordinates in ``geom``'s CRS. Geographic antimeridian seams
        and poles use the same topology as point-geometry predicates.

    Returns
    -------
    bool or numpy.ndarray
        A single bool for scalar geometry and coordinates, otherwise one result
        per broadcast row.

    Raises
    ------
    InvalidGeometryError
        If ``x``/``y`` are non-finite or differ in length.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.intersects_xy(gm.box(0, 0, 2, 2), 3, 3)
    False
    """

@overload
def intersects(left: Geometry, right: Geometry) -> bool: ...
@overload
def intersects(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` share any point.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the two geometries share any point; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    disjoint : Logical negation.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.intersects(gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3))
    True
    """

@overload
def disjoint(left: Geometry, right: Geometry) -> bool: ...
@overload
def disjoint(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def disjoint(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def disjoint(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` share no point; negation of
    ``intersects``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    intersects : Logical negation.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.disjoint(gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3))
    True
    """

@overload
def touches(left: Geometry, right: Geometry) -> bool: ...
@overload
def touches(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def touches(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def touches(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` touch only at boundaries.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.touches(gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1))
    True
    """

@overload
def crosses(left: Geometry, right: Geometry) -> bool: ...
@overload
def crosses(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def crosses(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def crosses(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` cross (interiors meet with lower
    dimension).

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    Examples
    --------
    >>> import gometry as gm
    >>> rising = gm.LineString([(0, 0), (2, 2)])
    >>> falling = gm.LineString([(0, 2), (2, 0)])
    >>> gm.crosses(rising, falling)
    True
    """

@overload
def overlaps(left: Geometry, right: Geometry) -> bool: ...
@overload
def overlaps(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def overlaps(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def overlaps(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` overlap (same dimension, partial
    interior overlap).

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.overlaps(gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3))
    True
    """

@overload
def relate(left: Geometry, right: Geometry) -> str: ...
@overload
def relate(left: GeometryArray, right: Geometry | GeometryArray) -> list[str]: ...
@overload
def relate(left: Geometry, right: GeometryArray) -> list[str]: ...
@overload
def relate(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> str | list[str]:
    """Compute the DE-9IM intersection matrix string for ``left`` and
    ``right``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    str or list of str
        The nine-character DE-9IM pattern; one per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.relate(gm.box(0, 0, 2, 2), gm.Point(1, 1))
    '0F2FF1FF2'
    """

@overload
def relate_pattern(left: Geometry, right: Geometry, pattern: str) -> bool: ...
@overload
def relate_pattern(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    pattern: str,
) -> npt.NDArray[np.bool_]: ...
@overload
def relate_pattern(
    left: Geometry,
    right: GeometryArray,
    pattern: str,
) -> npt.NDArray[np.bool_]: ...
@overload
def relate_pattern(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray, pattern: str
) -> bool | npt.NDArray[np.bool_]:
    """Test whether two geometries' DE-9IM matrix matches a pattern.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.
    pattern : str
        A DE-9IM pattern string (``T``/``F``/``*``/``0``/``1``/``2`` per cell).

    Returns
    -------
    bool or numpy.ndarray
        Whether the matrix matches; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``pattern`` is not a 9-character DE-9IM pattern.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.relate_pattern(gm.box(0, 0, 1, 1), gm.box(0.5, 0.5, 1.5, 1.5), 'T*T***T**')
    True
    """

@overload
def equals(left: Geometry, right: Geometry) -> bool: ...
@overload
def equals(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def equals(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def equals(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` are spatially equal.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    bool or numpy.ndarray
        Whether the two geometries are spatially equal; one result per
        input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    Examples
    --------
    >>> import gometry as gm
    >>> a = gm.from_wkt('LINESTRING (0 0, 2 2)')
    >>> b = gm.from_wkt('LINESTRING (2 2, 1 1, 0 0)')
    >>> gm.equals(a, b)  # same point set, different vertices
    True
    """

@overload
def equals_identical(left: Geometry, right: Geometry) -> bool: ...
@overload
def equals_identical(
    left: GeometryArray, right: Geometry | GeometryArray
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_identical(left: Geometry, right: GeometryArray) -> npt.NDArray[np.bool_]: ...
@overload
def equals_identical(
    left: Geometry | GeometryArray, right: Geometry | GeometryArray
) -> bool | npt.NDArray[np.bool_]:
    """Test full value identity elementwise: the vectorized ``==``.

    Two geometries are identical when they share the same CRS, coordinate
    epoch, geometry kind, and every active ordinate bit-for-bit in the same
    vertex order — exactly the scalar ``left == right``. A frame (CRS/epoch)
    difference is an *unequal value*, never an error, so mixed-frame data can
    be compared safely. Use `equals` for the order-independent topological
    test, or `equals_exact` for tolerance-based coordinate comparison.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        The two operands (scalar/array broadcasting).

    Returns
    -------
    bool or numpy.ndarray
        Whether the values are identical; one result per input pair.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.equals_identical(gm.Point(1, 2), gm.Point(1, 2))
    True
    >>> gm.equals_identical(gm.Point(1, 2), gm.Point(1, 2, crs=4326))
    False
    """

@overload
def equals_exact(
    left: Geometry,
    right: Geometry,
    tolerance: float = 0.0,
    *,
    include_z: bool = True,
    include_m: bool = True,
) -> bool: ...
@overload
def equals_exact(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    tolerance: FloatInput = 0.0,
    *,
    include_z: bool = True,
    include_m: bool = True,
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_exact(
    left: Geometry,
    right: GeometryArray,
    tolerance: FloatInput = 0.0,
    *,
    include_z: bool = True,
    include_m: bool = True,
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_exact(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    tolerance: FloatInput = 0.0,
    *,
    include_z: bool = True,
    include_m: bool = True,
) -> bool | npt.NDArray[np.bool_]:
    """Test coordinate equality within ``tolerance``, optionally comparing Z/M.

    Two geometries are equal when they share the same structure and every paired
    ordinate agrees to within ``tolerance`` (``|left - right| <= tolerance``).
    ``tolerance=0.0`` is exact. Like every binary operation, both operands must
    share one CRS/epoch frame; use `equals_identical` (the vectorized ``==``)
    for full value identity including the frame, or `equals` for an
    order-independent topological test.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        The two operands (scalar/array broadcasting).
    tolerance : float or sequence of float, default 0.0
        Maximum permitted per-ordinate difference — a scalar applies to every
        pair, or pass one value per geometry.
    include_z, include_m : bool, default True
        Whether the Z and M ordinates participate in the comparison.

    Returns
    -------
    bool or numpy.ndarray
        Whether the coordinates match; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``tolerance`` is negative or non-finite.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.equals_exact(gm.Point(1, 1), gm.Point(1, 1.0000001), 1e-6)
    True
    """

@overload
def distance(
    left: Geometry, right: Geometry, *, unit: DistanceUnit | None = None
) -> float: ...
@overload
def distance(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Geometry, right: GeometryArray, *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute the minimum distance between ``left`` and ``right``.

    CRS-aware: geodesic meters on a geographic CRS, native linear units on a
    projected CRS, coordinate units when CRS-free.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    float or numpy.ndarray
        One result per input pair.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``unit='meters'`` is requested for a CRS-free geometry.

    See Also
    --------
    dwithin : Within-distance test.
    nearest_points : Closest-pair witness points.
    shortest_line : Connecting line between closest points.
    hausdorff_distance : Hausdorff (set-to-set) similarity distance.
    frechet_distance : Curve-to-curve similarity (linestrings).

    Examples
    --------
    >>> import gometry as gm
    >>> gm.distance(gm.Point(0, 0), gm.Point(3, 4))
    5.0
    >>> a, b = gm.Point(13.0, 52.0, crs=4326), gm.Point(13.1, 52.0, crs=4326)
    >>> round(gm.distance(a, b))  # meters
    6868
    """

@overload
def distance_3d(
    left: Geometry, right: Geometry, *, unit: DistanceUnit | None = None
) -> float: ...
@overload
def distance_3d(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance_3d(
    left: Geometry, right: GeometryArray, *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def distance_3d(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute the minimum 3D (Euclidean) distance between two geometries,
    measured for their CRS.

    Distance is over linework: points/multipoints as degenerate segments,
    polygons via their boundary rings. Part of the 3D Euclidean family with
    ``length_3d`` only — area and perimeter are inherently 2D (XY footprint)
    and have no 3D form; ``bounds_3d`` gives the 3D extent (there is no
    ``envelope_3d``).

    A projected CRS gives native linear units and a CRS-free geometry gives
    coordinate units — the same defaults as the 2D ``distance``. A geographic
    CRS raises under every ``unit``: a Euclidean norm cannot combine degrees
    with meter heights. Every vertex on both operands must carry a Z ordinate.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.
    unit : {'planar', 'meters'} or None, default None
        ``None`` follows the CRS, exactly like ``distance``. ``'planar'``
        forces raw coordinate units; ``'meters'`` forces SI meters and raises
        without a CRS.

    Returns
    -------
    float or numpy.ndarray
        One result per input pair.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``unit='meters'`` is requested for a CRS-free geometry.
    InvalidGeometryError
        If either operand lacks a Z ordinate on every vertex.

    See Also
    --------
    distance : The 2D sibling, with the same ``unit`` override.
    length_3d : Total 3D length under the same metric.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.distance_3d(gm.from_wkt('POINT Z (0 0 0)'), gm.from_wkt('POINT Z (0 0 3)'))
    3.0
    """

@overload
def hausdorff_distance(
    left: Geometry,
    right: Geometry,
    *,
    densify: float | None = None,
    unit: DistanceUnit | None = None,
) -> float: ...
@overload
def hausdorff_distance(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def hausdorff_distance(
    left: Geometry,
    right: GeometryArray,
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def hausdorff_distance(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute the continuous Hausdorff (set-to-set) distance between ``left``
    and ``right``.

    CRS-aware: geodesic meters on a geographic CRS, native linear units on a
    projected CRS, coordinate units when CRS-free.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.
    densify : float, optional
        Subdivide every segment into pieces no longer than this fraction of
        its length (in ``(0, 1]``) before measuring. The metric remains the
        continuous Hausdorff distance; gometry does not offer a discrete,
        vertex-only Hausdorff variant.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    float or numpy.ndarray
        One result per input pair.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``densify`` is outside ``(0, 1]``, or ``unit='meters'`` is
        requested for a CRS-free geometry.

    See Also
    --------
    distance : Minimum pairwise distance.
    frechet_distance : Curve-to-curve similarity (linestrings).
    dwithin : Within-distance test.
    nearest_points : Closest-pair witness points.
    shortest_line : Connecting line between closest points.

    Examples
    --------
    >>> import gometry as gm
    >>> a = gm.LineString([(0, 0), (10, 0)])
    >>> b = gm.LineString([(0, 1), (10, 3)])
    >>> gm.hausdorff_distance(a, b)
    3.0
    """

@overload
def frechet_distance(
    left: Geometry,
    right: Geometry,
    *,
    densify: float | None = None,
    unit: DistanceUnit | None = None,
) -> float: ...
@overload
def frechet_distance(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def frechet_distance(
    left: Geometry,
    right: GeometryArray,
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def frechet_distance(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Discrete Fréchet distance between two linestrings.

    Measured for the CRS like ``distance``: geodesic meters on a geographic
    CRS, native units on a projected one, coordinate units when CRS-free.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        The two linestrings.
    densify : float, optional
        Subdivide every segment into pieces no longer than this
        fraction of its length (in ``(0, 1]``) before measuring,
        tightening the discrete vertex metric toward the continuous
        one. Omitted measures vertices only.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    float or numpy.ndarray
        One result per input pair.

    Raises
    ------
    GeometryError
        If ``densify`` is outside ``(0, 1]``.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If the linework is empty.
    GeometryTypeError
        If either geometry is not a single line.

    See Also
    --------
    distance : Minimum pairwise distance.
    hausdorff_distance : Hausdorff (set-to-set) similarity distance.
    dwithin : Within-distance test.
    nearest_points : Closest-pair witness points.
    shortest_line : Connecting line between closest points.

    Examples
    --------
    >>> import gometry as gm
    >>> a = gm.LineString([(0, 0), (10, 0)])
    >>> b = gm.LineString([(0, 1), (10, 1)])
    >>> gm.frechet_distance(a, b)
    1.0
    """

@overload
def nearest_points(
    left: Geometry, right: Geometry, *, unit: DistanceUnit | None = None
) -> tuple[Point, Point]: ...
@overload
def nearest_points(
    left: Geometry,
    right: GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point], GeometryArray[Point]]: ...
@overload
def nearest_points(
    left: GeometryArray,
    right: Geometry,
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point], GeometryArray[Point]]: ...
@overload
def nearest_points(
    left: GeometryArray,
    right: GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point], GeometryArray[Point]]: ...
@overload
def nearest_points(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> tuple[Point, Point] | tuple[GeometryArray[Point], GeometryArray[Point]]:
    """Return the closest pair of points between two geometries.

    Realizes the same minimizer as ``distance``: the geodesically closest pair
    on a geographic CRS, the planar-closest pair on a projected one, and the
    coordinate-space closest pair when CRS-free. Accepts any geometry pair, not
    only points (the witness on a polygon/line may be an edge-interior point);
    the connecting line is ``shortest_line``. An empty operand yields
    ``(POINT EMPTY, POINT EMPTY)``, and the pair is dropped to the operands'
    common axes so it matches ``shortest_line``'s endpoints exactly.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch. Unpack order is point on ``left``, then on ``right``.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    tuple of Point or tuple of GeometryArray
        A ``(left_point, right_point)`` pair for scalar inputs, or a
        ``(left, right)`` pair of parallel witness point columns when either
        operand is a `GeometryArray`.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    distance : Minimum pairwise distance.
    shortest_line : Connecting line between closest points.
    dwithin : Within-distance test.
    hausdorff_distance : Hausdorff (set-to-set) similarity distance.
    frechet_distance : Curve-to-curve similarity (linestrings).

    Examples
    --------
    >>> import gometry as gm
    >>> square = gm.box(0, 0, 1, 1)
    >>> a, b = gm.nearest_points(square, gm.Point(3, 0.5))
    >>> (a.to_wkt(), b.to_wkt())
    ('POINT (1 0.5)', 'POINT (3 0.5)')
    """

@overload
def shortest_line(
    left: Geometry, right: Geometry, *, unit: DistanceUnit | None = None
) -> LineString: ...
@overload
def shortest_line(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString]: ...
@overload
def shortest_line(
    left: Geometry,
    right: GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString]: ...
@overload
def shortest_line(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    *,
    unit: DistanceUnit | None = None,
) -> LineString | GeometryArray[LineString]:
    """Return the shortest connecting line between two geometries (CRS-aware).

    ``nearest_points`` as a ``LineString`` — degenerate when the geometries
    touch. Accepts any geometry pair (point/line/polygon/multi/collection), not
    only points, and the witness is the true closest approach, so an endpoint
    may land on an edge interior rather than a vertex. An empty operand yields
    ``LINESTRING EMPTY`` (the output-type sentinel, like ``distance``'s ``inf``).
    Output ordinates are the operands' common axes (an XYZM line versus an XY
    point gives an XY line). See `nearest_points` (the endpoints) and `distance`
    (the length).

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    Geometry or GeometryArray
        One connecting ``LineString`` per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    distance : Minimum pairwise distance.
    nearest_points : Closest-pair witness points.
    dwithin : Within-distance test.
    hausdorff_distance : Hausdorff (set-to-set) similarity distance.
    frechet_distance : Curve-to-curve similarity (linestrings).

    Examples
    --------
    >>> import gometry as gm
    >>> square = gm.box(0, 0, 1, 1)
    >>> gm.shortest_line(square, gm.Point(3, 0.5)).to_wkt()
    'LINESTRING (1 0.5, 3 0.5)'
    >>> # The near point can fall mid-edge, not on a vertex:
    >>> a = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
    >>> b = gm.from_wkt('POLYGON ((4 1, 5 0, 6 1, 5 2, 4 1))')
    >>> gm.shortest_line(a, b).to_wkt()
    'LINESTRING (2 1, 4 1)'
    """

@overload
def split(
    geom: Geometry,
    splitter: Geometry | GeometryArray,
    *,
    tolerance: float = 0.0,
) -> GeometryArray[LineString]: ...
@overload
def split(
    geom: GeometryArray,
    splitter: Geometry | GeometryArray,
    *,
    tolerance: float = 0.0,
) -> GeometryArray[LineString]: ...
@overload
def split(
    geom: Geometry | GeometryArray,
    splitter: Geometry | GeometryArray,
    *,
    tolerance: float = 0.0,
) -> GeometryArray[LineString]:
    """Split lineal geometry by point splitter(s).

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Lineal geometry to split.
    splitter : Geometry or GeometryArray
        Point or multipoint cutter.
    tolerance : float, keyword-only, optional
        Coordinate-space distance within which a splitter point counts as
        on the line and near-equal cut offsets collapse. The default ``0.0``
        is exact topological membership (a point splits only when it lies
        exactly on the linework) with identity deduplication.

    Returns
    -------
    GeometryArray
        Split parts.

    Raises
    ------
    InvalidGeometryError
        If ``tolerance`` is negative or non-finite.
    CRSMismatchError
        If operands' CRS or coordinate-epoch metadata differ.
    GeometryTypeError
        If ``geom`` is not lineal.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.split(gm.LineString([(0, 0), (2, 0)]), gm.Point(1, 0)).to_wkt()
    ['LINESTRING (0 0, 1 0)', 'LINESTRING (1 0, 2 0)']
    """

@overload
def dwithin(
    left: Geometry,
    right: Geometry,
    distance: float,
    *,
    unit: DistanceUnit | None = None,
) -> bool: ...
@overload
def dwithin(
    left: GeometryArray,
    right: Geometry | GeometryArray,
    distance: FloatInput,
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def dwithin(
    left: Geometry,
    right: GeometryArray,
    distance: FloatInput,
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def dwithin(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
    distance: FloatInput,
    *,
    unit: DistanceUnit | None = None,
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` are within ``distance`` of each other.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.
    distance : float or sequence of float
        CRS-aware distance threshold: geodesic meters on a geographic CRS,
        native units on a projected one, coordinate units otherwise. A scalar
        applies to every pair; with an array operand, an iterable supplies one
        threshold per result row.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    bool or numpy.ndarray
        One result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``distance`` is negative or non-finite, or ``unit='meters'`` is
        requested for a CRS-free geometry.

    See Also
    --------
    distance : Minimum pairwise distance.
    nearest_points : Closest-pair witness points.
    shortest_line : Connecting line between closest points.
    hausdorff_distance : Hausdorff (set-to-set) similarity distance.
    frechet_distance : Curve-to-curve similarity (linestrings).

    Examples
    --------
    >>> import gometry as gm
    >>> a, b = gm.Point(13.0, 52.0, crs=4326), gm.Point(13.1, 52.0, crs=4326)
    >>> gm.dwithin(a, b, 7000)
    True
    """

@overload
def nearest(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    geom: Geometry,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[False] = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> npt.NDArray[np.int64]: ...
@overload
def nearest(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    geom: Geometry,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[True],
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    geom: GeometryArray,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[False] = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> Groups[npt.NDArray[np.int64]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    geom: GeometryArray,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[True],
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> tuple[Groups[npt.NDArray[np.int64]], npt.NDArray[np.float64]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    geom: Geometry | GeometryArray,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: bool = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> (
    npt.NDArray[np.int64]
    | Groups[npt.NDArray[np.int64]]
    | tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]
    | tuple[Groups[npt.NDArray[np.int64]], npt.NDArray[np.float64]]
):
    """Find the nearest of `values` to each query geometry (builds an index).

    Parameters
    ----------
    values : sequence of Geometry or GeometryArray
        Candidate geometries to index and search.
    geom : Geometry or GeometryArray
        Query geometry (or array of queries).
    k : int, default 1
        Number of nearest neighbors to return per query.
    max_distance : float, optional
        Ignore candidates farther than this from the query.
    return_distance : bool, default False
        If ``True``, return distances alongside handles — ``(indices,
        distances)`` for a scalar query, ``(matches, distances)`` for an
        array query.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.
    exclusive : bool, default False
        If ``True``, skip candidates structurally equal to the query geometry
        (same exact coordinates) — "the nearest *other* feature".
    ties : bool, default False
        Also return every candidate TYING the k-th nearest distance (exact
        comparison) — results can then exceed ``k``.

    Returns
    -------
    int64 numpy.ndarray, Groups, or tuple
        Indices into `values` of the nearest geometries — an ``int64`` ndarray
        for a scalar query, CSR ``Groups`` for an array query. With
        ``return_distance=True``, plain tuple field order is ``(indices,
        distances)`` for a scalar query or ``(matches, distances)`` for an
        array query (distances parallel to ``matches.values``).

    Examples
    --------
    >>> import gometry as gm
    >>> sites = [gm.Point(0, 0), gm.Point(5, 5)]
    >>> gm.nearest(sites, gm.Point(4, 4))
    array([1])
    """

@overload
def join(
    left: Geometry | GeometryArray | Iterable[_GeometryLike],
    right: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    *,
    predicate: Literal['dwithin'],
    distance: float,
    unit: DistanceUnit | None = None,
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]: ...
@overload
def join(
    left: Geometry | GeometryArray | Iterable[_GeometryLike],
    right: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    *,
    predicate: TopologicalPredicate = 'intersects',
    distance: None = None,
    unit: DistanceUnit | None = None,
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    """Perform a spatial join between two geometry collections via an internal index.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or sequence of Geometry
        The two geometry collections to join. Missing rows produce no pairs;
        their original row positions are preserved in every returned id.
    predicate : str, default 'intersects'
        Spatial predicate each returned pair must satisfy — one of
        ``'intersects'``, ``'contains'``, ``'contains_properly'``,
        ``'covers'``, ``'within'``, ``'covered_by'``, ``'equals'``,
        ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
    distance : float, optional
        Required when ``predicate='dwithin'``: the maximum separation, in
        CRS-natural units — geodesic meters on a geographic CRS, native
        units on a projected CRS, coordinate units when CRS-free.
    unit : {'planar', 'meters'}, default None
        Omitted follows the CRS: geodesic meters on a geographic CRS, native
        units on a projected one, coordinate units without a CRS.
        ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
        geographic CRS — only for deliberate coordinate-space math);
        ``'meters'`` forces the CRS metric and raises without a CRS.

    Returns
    -------
    tuple of numpy.ndarray
        ``(left, right)`` parallel int64 row-id columns satisfying the
        predicate.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``predicate`` is unknown, ``distance`` is missing or invalid for
        ``predicate='dwithin'``, or ``unit='meters'`` is requested for a
        CRS-free geometry.

    Examples
    --------
    >>> import gometry as gm
    >>> stops = gm.GeometryArray([gm.Point(0.5, 0.5), gm.Point(9, 9)])
    >>> zones = gm.GeometryArray([gm.box(0, 0, 1, 1)])
    >>> left, right = gm.join(stops, zones, predicate='within')
    >>> (left, right)
    (array([0]), array([0]))
    """

@overload
def intersection(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def intersection(
    left: GeometryArray,
    right: Geometry,
) -> GeometryArray: ...
@overload
def intersection(
    left: GeometryArray,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def intersection(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
) -> Geometry | GeometryArray:
    """Compute the set-theoretic intersection of ``left`` and ``right``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    Geometry or GeometryArray
        One result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If the overlay cannot be constructed.

    Examples
    --------
    >>> import gometry as gm
    >>> left, right = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
    >>> gm.intersection(left, right).to_wkt()
    'POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'
    """

@overload
def union(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def union(
    left: Geometry,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def union(
    left: GeometryArray,
    right: Geometry,
) -> GeometryArray: ...
@overload
def union(
    left: GeometryArray,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def union(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
) -> Geometry | GeometryArray:
    """Compute the set-theoretic union of ``left`` and ``right``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    Geometry or GeometryArray
        One result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If the overlay cannot be constructed.

    See Also
    --------
    union_all : Union of many geometries at once.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.union(gm.box(0, 0, 1, 2), gm.box(1, 0, 2, 2)).to_wkt()
    'POLYGON ((0 0, 1 0, 2 0, 2 2, 1 2, 0 2, 0 0))'
    """

@overload
def difference(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def difference(
    left: Geometry,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def difference(
    left: GeometryArray,
    right: Geometry,
) -> GeometryArray: ...
@overload
def difference(
    left: GeometryArray,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def difference(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
) -> Geometry | GeometryArray:
    """Compute the set-theoretic difference ``left - right``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Geometry to subtract from (``left``) and geometry subtracted
        (``right``). Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    Geometry or GeometryArray
        One result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If the overlay cannot be constructed.

    Examples
    --------
    >>> import gometry as gm
    >>> left, right = gm.box(0, 0, 2, 1), gm.box(1, 0, 2, 1)
    >>> gm.difference(left, right).to_wkt()
    'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    """

@overload
def symmetric_difference(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def symmetric_difference(
    left: Geometry,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def symmetric_difference(
    left: GeometryArray,
    right: Geometry,
) -> GeometryArray: ...
@overload
def symmetric_difference(
    left: GeometryArray,
    right: GeometryArray,
) -> GeometryArray: ...
@overload
def symmetric_difference(
    left: Geometry | GeometryArray,
    right: Geometry | GeometryArray,
) -> Geometry | GeometryArray:
    """Compute the set-theoretic symmetric difference of ``left`` and
    ``right``.

    Evaluated in the coordinate plane; geographic inputs crossing the
    antimeridian are split-normalized first.

    Parameters
    ----------
    left, right : Geometry or GeometryArray
        Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
        coordinate epoch.

    Returns
    -------
    Geometry or GeometryArray
        One result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If the overlay cannot be constructed.

    Examples
    --------
    >>> import gometry as gm
    >>> left, right = gm.box(0, 0, 2, 1), gm.box(1, 0, 3, 1)
    >>> gm.symmetric_difference(left, right).area
    2.0
    """

def coverage_is_valid(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike | None],
    *,
    gap_width: float = 0.0,
) -> bool:
    """Test whether geometries form a valid polygonal coverage.

    A polygonal coverage (parcels, admin boundaries) is a set of polygons
    with no interior overlaps whose shared
    boundaries use vector-identical linework. Distinct from the DGGS cell
    `Coverage` classes.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).

    gap_width : float, default 0.0
        Gap width in coordinate units; 0 disables gap detection.

    Returns
    -------
    bool
        ``True`` when no row has invalid coverage edges.

    Raises
    ------
    GeometryTypeError
        If a row is not a `Polygon` or `MultiPolygon`.
    GeometryError
        If ``gap_width`` is negative or non-finite.
    CRSMismatchError
        If the rows' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    coverage_invalid_edges : The offending linework itself.
    coverage_clean : Rebuild an exact coverage from a near-coverage.

    Examples
    --------
    >>> import gometry as gm
    >>> grid = [gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)]
    >>> gm.coverage_is_valid(grid)
    True
    >>> gm.coverage_is_valid([gm.box(0, 0, 1.1, 1), gm.box(1, 0, 2, 1)])
    False
    """

def coverage_invalid_edges(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike | None],
    *,
    gap_width: float = 0.0,
) -> GeometryArray[LineString | MultiLineString]:
    """Per-row invalid coverage boundary linework.

    Each output row merges the row's boundary segments that violate the
    coverage contract (overlaps, T-joins, slivers, and — with ``gap_width``
    — narrow gaps); rows that are clean come back as ``LINESTRING EMPTY``.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).

    gap_width : float, default 0.0
        Gap width in coordinate units; 0 disables gap detection.

    Returns
    -------
    GeometryArray
        One `LineString`/`MultiLineString` per input row.

    Raises
    ------
    GeometryTypeError
        If a row is not a `Polygon` or `MultiPolygon`.
    GeometryError
        If ``gap_width`` is negative or non-finite.
    CRSMismatchError
        If the rows' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    coverage_is_valid : The boolean verdict.

    Examples
    --------
    >>> import gometry as gm
    >>> len(gm.coverage_invalid_edges(
    ...     [gm.box(0, 0, 1, 1), gm.box(0.5, 0, 1.5, 1)]))
    2
    """

def coverage_simplify(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike | None],
    tolerance: float,
    *,
    method: SimplifyMethod = 'vw',
    simplify_boundary: bool = True,
) -> GeometryArray[Polygon | MultiPolygon]:
    """Simplify a VALID polygonal coverage's boundaries, preserving its
    topology.

    Shared boundaries are simplified once and spliced into both neighbors,
    so the result stays an exact coverage (vector-identical interfaces, no
    new crossings — a conservative topology guard rejects any vertex removal
    that would intersect other linework or swallow a neighbor). Input must
    be a valid coverage; invalid rows raise with repair guidance.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).
    tolerance : float
        Distance-scale simplification tolerance, in coordinate units;
        non-negative finite. Read on the same scale by both methods (the
        Visvalingam-Whyatt effective area is ``tolerance**2 / 2``).
    method : {'vw', 'dp'}, default 'vw'
        Importance criterion: ``'vw'`` is area-based (Visvalingam-Whyatt),
        ``'dp'`` is distance-based (Douglas-Peucker). Both run under the same
        topology guard.
    simplify_boundary : bool, default True
        Also simplify exterior (unshared) boundaries; ``False`` pins them
        and simplifies only the shared interfaces.

    Returns
    -------
    GeometryArray
        One simplified `Polygon`/`MultiPolygon` per input row.

    Raises
    ------
    GeometryTypeError
        If a row is not a `Polygon` or `MultiPolygon`.
    GeometryError
        If ``tolerance`` is negative or non-finite.
    CRSMismatchError
        If the rows' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If the rows are not a valid polygonal coverage.

    See Also
    --------
    Geometry.simplify : Per-geometry simplify (not coverage-topology-preserving).
    coverage_is_valid : Whether the rows form a valid polygonal coverage.

    Examples
    --------
    >>> import gometry as gm
    >>> left = gm.Polygon([(0, 0), (1, 0), (1.05, 0.5), (1, 1), (0, 1)])
    >>> right = gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1.05, 0.5)])
    >>> out = gm.coverage_simplify([left, right], 0.5)
    >>> out.to_wkt()[0]
    'POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))'
    """

def polygonize(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
) -> GeometryArray[Polygon]:
    """Build polygons from the pooled linework of many geometries.

    ALL inputs' edges are noded into ONE planar graph, so a ring can close from
    edges spread across different inputs — the canonical "reconstruct polygons
    from this pile of noded lines" call. Returns every enclosed face as a
    `GeometryArray` of polygons.

    This is the whole-collection aggregate. To polygonize each geometry's own
    linework independently (one polygon set per input), use the per-element
    method `GeometryArray.polygonize` (which returns `Groups`) or
    `Geometry.polygonize` on a single geometry.

    Parameters
    ----------
    values : Geometry or iterable of Geometry
        One geometry or linework values to node and polygonize together.

    Returns
    -------
    GeometryArray
        The polygons enclosed by the pooled linework.

    See Also
    --------
    polygonize_full : The same pooled aggregate plus dangles, cut edges, and invalid-ring diagnostics.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.polygonize([
    ...     gm.LineString([(0, 0), (1, 0)]),
    ...     gm.LineString([(1, 0), (1, 1)]),
    ...     gm.LineString([(1, 1), (0, 0)]),
    ... ]).to_wkt()
    ['POLYGON ((0 0, 1 0, 1 1, 0 0))']
    """

def polygonize_full(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
) -> PolygonizeResult:
    """Polygonize pooled linework and return full diagnostics.

    Like ``polygonize``, all inputs form ONE planar graph,
    including rings whose edges span multiple input geometries. In addition to
    polygons, returns cut edges, dangles, and invalid rings. Passing a
    `GeometryArray` pools its present rows; `array.polygonize()` remains the
    independent row-wise spelling.

    Parameters
    ----------
    values : Geometry or iterable of Geometry
        One geometry or linework values to node and polygonize together.

    Returns
    -------
    PolygonizeResult
        Pooled polygons, cut edges, dangles, and invalid rings.

    Examples
    --------
    >>> import gometry as gm
    >>> result = gm.polygonize_full([
    ...     gm.LineString([(0, 0), (1, 0)]),
    ...     gm.LineString([(1, 0), (1, 1)]),
    ...     gm.LineString([(1, 1), (0, 0)]),
    ... ])
    >>> result.polygons.to_wkt()
    ['POLYGON ((0 0, 1 0, 1 1, 0 0))']
    """

def coverage_union(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike | None],
) -> Polygon | MultiPolygon:
    """Union a polygonal *coverage* into one geometry by dissolving shared edges.

    A fast specialization of ``union_all`` for a **valid
    coverage** — polygons that tile a region edge-to-edge with no overlaps and
    no T-junctions (see ``coverage_is_valid``). It
    cancels the interior edges shared between adjacent polygons rather than
    noding and classifying every intersection, so it is dramatically faster on
    tilings (grids, parcels, administrative boundaries). On overlapping or
    badly-noded input raises before the specialized dissolve runs; use
    `union_all` there.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One polygonal row or multiple rows of a valid coverage. Missing array
        rows are skipped.

    Returns
    -------
    Geometry
        A single `Polygon`/`MultiPolygon` covering the merged area.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If ``values`` is empty or the rows are not a valid coverage.

    See Also
    --------
    union_all : General multi-geometry union (handles overlaps).
    coverage_is_valid : Whether the rows form a valid polygonal coverage.

    Examples
    --------
    >>> import gometry as gm
    >>> tiles = [gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)]
    >>> gm.coverage_union(tiles).normalize().to_wkt()
    'POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))'
    """

def coverage_clean(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike | None],
    *,
    grid_size: float = 0.0,
    gap_width: float = 0.0,
    overlap_rule: CoverageOverlapRule = 'longest_border',
) -> GeometryArray[Polygon | MultiPolygon]:
    """Clean a near-coverage into an exact polygonal coverage.

    Optionally snaps vertices, nodes every boundary, then assigns each face
    of the arrangement to exactly one row: overlaps go to the row chosen by
    ``overlap_rule``, and enclosed gaps narrower than ``gap_width`` merge into
    the neighbor with the longest shared border. Both sides of every
    interface come from the same noded linework, so the result is an exact
    coverage by construction.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).
    grid_size : float, default 0.0
        Vertex snapping grid in coordinate units; ``0`` preserves input
        coordinates and disables snapping.

    gap_width : float, default 0.0
        Merge enclosed gaps narrower than this coordinate-space width (0 keeps
        gaps).
    overlap_rule : str, default 'longest_border'
        Which row keeps a region covered more than once:
        ``'longest_border'``, ``'max_area'``, ``'min_area'``, or
        ``'min_index'``.
        Cleaning rebuilds faces and returns their natural 2D geometry.

    Returns
    -------
    GeometryArray
        One cleaned `Polygon`/`MultiPolygon` per input row.

    Raises
    ------
    GeometryTypeError
        If a row is not a `Polygon` or `MultiPolygon`.
    GeometryError
        If ``grid_size`` or ``gap_width`` is negative or non-finite.
    InvalidGeometryError
        If ``grid_size > 0`` and snap-repair cannot converge on a valid
        grid-aligned result.
    CRSMismatchError
        If the rows' CRS or coordinate-epoch metadata differ.

    See Also
    --------
    coverage_is_valid : Test whether a polygonal coverage is already valid.

    Examples
    --------
    >>> import gometry as gm
    >>> rows = [gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)]
    >>> gm.coverage_is_valid(rows)
    False
    >>> cleaned = gm.coverage_clean(rows)
    >>> cleaned.coverage_is_valid()
    True
    """

def union_all(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
) -> Geometry:
    """Union of many geometries into one (planar overlay).

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One geometry or geometries to union. Missing array rows are skipped.

    Returns
    -------
    Geometry
        A single geometry that is the union of all inputs.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If ``values`` is empty or the overlay cannot be constructed.

    See Also
    --------
    coverage_union : Faster dissolve for a valid polygonal coverage.

    Examples
    --------
    >>> import gometry as gm
    >>> panes = [gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]
    >>> gm.union_all(panes).to_wkt()
    'POLYGON ((0 0, 2 0, 2 1, 3 1, 3 3, 1 3, 1 2, 0 2, 0 0))'
    """

def intersection_all(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
) -> Geometry:
    """Common intersection of many geometries (planar overlay).

    The region inside EVERY input — `g0 ∩ g1 ∩ … ∩ gn`. Reduces the sequence
    with the pairwise ``intersection``, so mixed
    dimensions narrow to the lowest shared dimension and an empty result keeps
    that dimension's typed empty.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One geometry or geometries to intersect. Missing array rows are skipped.

    Returns
    -------
    Geometry
        A single geometry that is the intersection of all inputs.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If ``values`` is empty or the overlay cannot be constructed.

    Examples
    --------
    >>> import gometry as gm
    >>> panes = [gm.box(0, 0, 3, 3), gm.box(1, 1, 4, 4), gm.box(2, 2, 5, 5)]
    >>> gm.intersection_all(panes).to_wkt()
    'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
    """

def symmetric_difference_all(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
) -> Geometry:
    """Symmetric difference of many geometries (planar overlay).

    The region covered by an ODD number of inputs — `g0 ▵ g1 ▵ … ▵ gn`. Reduces
    the sequence with the pairwise
    ``symmetric_difference``; the fold is
    order-independent (`▵` is associative and commutative).

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One geometry or geometries to combine. Missing array rows are skipped.

    Returns
    -------
    Geometry
        A single geometry covered by an odd number of inputs.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If ``values`` is empty or the overlay cannot be constructed.

    Examples
    --------
    >>> import gometry as gm
    >>> panes = [gm.box(0, 0, 2, 2), gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]
    >>> gm.symmetric_difference_all(panes).to_wkt()  # the duplicate cancels
    'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'
    """

@overload
def get_coordinates(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    *,
    axes: CoordinateAxes | None = None,
    return_index: Literal[False] = False,
) -> npt.NDArray[np.float64]: ...
@overload
def get_coordinates(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    *,
    axes: CoordinateAxes | None = None,
    return_index: Literal[True],
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.int64]]: ...
@overload
def get_coordinates(
    values: Geometry | GeometryArray | Iterable[_GeometryLike | None],
    *,
    axes: CoordinateAxes | None = None,
    return_index: bool = False,
) -> npt.NDArray[np.float64] | tuple[npt.NDArray[np.float64], npt.NDArray[np.int64]]:
    """Extract coordinates as a dense ``(N, k)`` ``float64`` matrix (Shapely
    ``get_coordinates`` parity).

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry/None
        The geometry values whose coordinates are flattened depth-first.
    axes : {'XY', 'XYZ', 'XYM', 'XYZM'}, optional
        Output coordinate columns. ``None`` means ``'XY'``.
    return_index : bool, default False
        Also return the per-coordinate source-geometry row index.

    Returns
    -------
    numpy.ndarray or tuple of numpy.ndarray
        The ``(N, k)`` coordinate matrix, or ``(matrix, index)`` when
        ``return_index=True``. Missing
        `GeometryArray` rows contribute no coordinates; the matrix is a
        flattened vertex stream, not row-aligned. Use ``return_index=True`` to
        recover source-row alignment, or call ``drop_missing()`` before
        extraction for an explicit dense path.

    Raises
    ------
    TypeError
        If ``values`` is not geometry-like.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.get_coordinates(gm.Point(1, 2)).tolist()
    [[1.0, 2.0]]
    """

@overload
def parts(
    geom: Point | MultiPoint | GeometryArray[Point | MultiPoint],
) -> GeometryArray[Point]: ...
@overload
def parts(
    geom: LineString | MultiLineString | GeometryArray[LineString | MultiLineString],
) -> GeometryArray[LineString]: ...
@overload
def parts(
    geom: Polygon | MultiPolygon | GeometryArray[Polygon | MultiPolygon],
) -> GeometryArray[Polygon]: ...
@overload
def parts(
    geom: Geometry | GeometryArray | Iterable[_GeometryLike],
) -> GeometryArray[Geometry]:
    """Component geometries of a multipart geometry.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values
        The geometry, array, or iterable materialized as an array.

    Returns
    -------
    GeometryArray
        The flattened component geometries, carrying the input CRS/epoch.

    Examples
    --------
    >>> import gometry as gm
    >>> multi = gm.from_wkt('MULTIPOINT ((0 0), (1 1))')
    >>> for part in gm.parts(multi):
    ...     assert part is not None
    ...     print(part.to_wkt())
    POINT (0 0)
    POINT (1 1)
    """

def rings(
    geom: Geometry | GeometryArray | Iterable[_GeometryLike],
) -> GeometryArray[LineString]:
    """Return the rings (exterior + interiors) of a polygonal geometry.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values
        The geometry, array, or iterable materialized as an array.

    Returns
    -------
    GeometryArray
        The flattened rings as `LineString` geometries, carrying the input
        CRS/epoch.

    Examples
    --------
    >>> import gometry as gm
    >>> hole = [(1, 1), (2, 1), (2, 2), (1, 2)]
    >>> shell = [(0, 0), (4, 0), (4, 4), (0, 4)]
    >>> donut = gm.Polygon(shell, holes=[hole])
    >>> len(gm.rings(donut))
    2
    """

@overload
def from_wkt(
    data: str, *, crs: CrsInput | None = None, epoch: float | None = None
) -> Geometry: ...
@overload
def from_wkt(
    data: Iterable[str | None],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray:
    """Parse a geometry (or array) from Well-Known Text.

    Also accepts EWKT; an embedded ``SRID=...;`` prefix becomes the
    geometry's CRS. ``SRID=0`` is PostGIS unknown/unspecified and yields a
    CRS-free geometry; nonzero codes resolve through the canonical PROJ CRS
    parser (invalid codes raise ``CRSError``).

    Parameters
    ----------
    data : str or iterable of str
        A single WKT/EWKT string, or any iterable of them for a
        ``GeometryArray``.

    crs : str or int, optional
        CRS for SRID-less input. An embedded SRID that *contradicts* an
        explicit ``crs`` raises rather than silently winning.

    epoch : float, optional
        Coordinate epoch (decimal year) to attach as frame metadata.

    Returns
    -------
    Geometry or GeometryArray
        A ``GeometryArray`` when ``data`` is an iterable, else a ``Geometry``.

    Raises
    ------
    ParseError
        If the WKT is malformed.
    CRSMismatchError
        If an embedded SRID conflicts with ``crs``.
    CRSError
        If ``crs`` or an embedded nonzero SRID is not recognized, or
        ``epoch`` is set without ``crs``.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    See Also
    --------
    Geometry.to_wkt : Serialize a geometry to WKT.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.from_wkt('POINT (1 2)').geometry_type
    'Point'
    >>> gm.from_wkt('SRID=4326;POINT (1 2)').crs
    CRS("EPSG:4326")
    """

@overload
def from_wkb(
    data: Buffer, *, crs: CrsInput | None = None, epoch: float | None = None
) -> Geometry: ...
@overload
def from_wkb(
    data: Iterable[Buffer | None],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray: ...
@overload
def from_wkb(
    data: Buffer | Iterable[Buffer | None],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> Geometry | GeometryArray:
    """Parse a geometry (or array) from Well-Known Binary.

    Also accepts EWKB; an embedded SRID becomes the geometry's CRS.
    ``SRID=0`` / EWKB SRID 0 is PostGIS unknown → CRS-free; nonzero codes
    resolve through the canonical PROJ CRS parser (invalid codes raise).

    Parameters
    ----------
    data : bytes or sequence of bytes
        A WKB/EWKB buffer, or an iterable of them for a ``GeometryArray``.

    crs : str or int, optional
        CRS for SRID-less input. An embedded SRID that *contradicts* an
        explicit ``crs`` raises rather than silently winning.

    epoch : float, optional
        Coordinate epoch (decimal year) to attach as frame metadata.

    Returns
    -------
    Geometry or GeometryArray
        A ``GeometryArray`` when ``data`` is an iterable, else a ``Geometry``.

    Raises
    ------
    ParseError
        If the WKB is malformed.
    CRSMismatchError
        If an embedded SRID conflicts with ``crs``.
    CRSError
        If ``crs`` or an embedded nonzero SRID is not recognized, or
        ``epoch`` is set without ``crs``.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Notes
    -----
    The (E)WKB format does not carry a coordinate epoch and one does not
    survive a WKB round-trip; ``epoch=`` attaches it as frame metadata (as
    with ``GeometryArray``), and Arrow interchange preserves it on export.

    See Also
    --------
    Geometry.to_wkb : Serialize a geometry to WKB.

    Examples
    --------
    >>> import gometry as gm
    >>> hex = '0101000000000000000000f03f0000000000000040'
    >>> gm.from_wkb(bytes.fromhex(hex)).to_wkt()
    'POINT (1 2)'
    """

def _parse_geoarrow_extension_metadata(
    metadata: bytes,
) -> tuple[str | None, float | None]:
    """Private Python entry: parse GeoArrow extension metadata bytes → (crs, epoch)."""

def _parse_geoparquet_column_frame(
    metadata: Mapping[str, Any],
    column_name: str,
) -> tuple[str | None, float | None]:
    """Private Python entry: parse one already-decoded GeoParquet column mapping for
    the shared CRS/epoch/edges frame (defaults missing CRS to OGC:CRS84; CRS must
    be a PROJJSON object or null when present).
    """

def _admit_geoparquet_geometry_storage(
    arrow_type: Any,
    encoding: str,
    column_name: str,
    field: Any | None = None,
) -> tuple[bool, str | None, float | None]:
    """Admit GeoParquet geometry storage against a declared encoding.

    Dictionary-wrapped WKB is accepted. ExtensionType and Field metadata are
    reconciled together (name + frame) so raw-field frame metadata is never
    discarded. Returns ``(has_extension, crs, epoch)`` — when
    ``has_extension`` is true the frame came from reconciled extension
    metadata (possibly both-None for empty metadata).
    """

def from_arrow(
    data: _ArrowInput, *, crs: CrsInput | None = None, epoch: float | None = None
) -> GeometryArray[Geometry]:
    """Build geometries from an Arrow array (``GeoArrow`` interchange).

    Parameters
    ----------
    data : Arrow array
        A GeoArrow-encoded array (anything exposing the Arrow C stream).

    crs : str or int, optional
        CRS for arrays without embedded `GeoArrow` metadata. When metadata
        carries a CRS, ``crs=`` must agree or `CRSMismatchError` is raised.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    GeometryArray
        Arrow containers return a `GeometryArray`, even when they hold one row.
        CRS and coordinate epoch come from the `GeoArrow` metadata.

    Raises
    ------
    TypeError
        If ``data`` is not an Arrow container or Arrow C provider.

    ParseError
        If ``data`` is an Arrow object but not a supported `GeoArrow` layout.
    CRSMismatchError
        If embedded `GeoArrow` CRS or epoch metadata conflicts with ``crs`` /
        ``epoch``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Notes
    -----
    Import copies the selected geometry schema and every span it validates or
    decodes (validity, offsets, views, referenced BinaryView size entries,
    coordinates, WKB payload) into owned storage, then validates and decodes
    only that snapshot. Native
    Arrow-C providers must not modify exported structs, pointer tables, schema
    memory, or buffers before gometry invokes their release callback; direct
    PyArrow objects must not be mutated while ``from_arrow`` is running. Arrow C
    capsule producers (``__arrow_c_array__`` / ``__arrow_c_stream__``) are trusted
    to be ABI-conforming — a deliberately hostile duck-typed producer that forges
    its own buffers is out of the threat model (same line as pyarrow; the C Data
    Interface carries no buffer capacity except BinaryView's mandatory
    variadic-sizes buffer, which is enforced). See ``docs/ecosystem/arrow.md``.

    See Also
    --------
    GeometryArray.to_arrow : Encode geometries as a GeoArrow array.

    Examples
    --------
    >>> import gometry as gm
    >>> arr = gm.GeometryArray([gm.Point(1, 2)])
    >>> gm.from_arrow(arr.to_arrow()).to_wkt()
    ['POINT (1 2)']
    """

def from_features(
    features: str
    | bytes
    | bytearray
    | memoryview[Any]
    | Mapping[str, Any]
    | Iterable[Mapping[str, Any]],
    *,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Features:
    """Parse GeoJSON features into geometries plus parallel properties and ids.

    Accepts a ``FeatureCollection``/``Feature`` mapping, JSON text of one
    (``str`` or UTF-8 bytes/buffer), or an iterable of ``Feature`` mappings.
    Unlike `from_geojson`, per-feature ``properties`` and ``id`` values are
    preserved. Missing ``properties`` normalize to ``{}``; an explicit JSON
    null remains ``None``.

    Parameters
    ----------
    features : str, bytes, mapping, or iterable of mapping
        A ``FeatureCollection``/``Feature`` mapping, JSON text of one, or an
        iterable of ``Feature`` mappings.
    crs : str or int, default 'OGC:CRS84'
        CRS to attach. GeoJSON coordinates are WGS84 by specification, so the
        default declares OGC:CRS84 (lon/lat); pass ``None`` for CRS-free
        geometries or ``crs=4326`` for EPSG:4326.
    epoch : float, optional
        Coordinate epoch (decimal year) to attach as frame metadata.

    Returns
    -------
    Features
        A ``Features`` record with one row per
        feature. Null geometries are represented by missing array rows.

    Raises
    ------
    ParseError
        If the input is not a Feature/FeatureCollection/iterable, a feature is
        malformed, a geometry cannot be parsed, or a legacy ``crs`` member is
        unsupported or conflicts with ``crs`` (``format`` is ``"GeoJSON"``).
    InvalidGeometryError
        If a position is outside the WGS84 lon/lat domain or a ring fails
        structural ring admission.
    CRSError
        If ``crs`` is not recognized or is outside the WGS84 family, or
        ``epoch`` is set with ``crs=None``.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Notes
    -----
    Text input is decoded in Rust. Coordinate numbers follow the same admission
    as ``from_geojson``: correctly-rounded binary64 floats, and integers only
    when exactly representable as ``float`` (non-exact integers raise). Object
    keys in text input are returned sorted; mapping input keeps key order and
    opaque property values.

    See Also
    --------
    from_geojson : Decode geometry while dropping feature side data.
    to_feature_collection : Encode geometry and aligned side data.

    Examples
    --------
    >>> import gometry as gm
    >>> feats = gm.from_features([{
    ...     'type': 'Feature',
    ...     'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
    ...     'properties': {'a': 1},
    ... }])
    >>> feats.geometries.to_wkt()
    ['POINT (1 2)']
    """

def to_feature(
    geom: Geometry | None,
    *,
    properties: Mapping[str, Any] | None = None,
    id: FeatureId = None,
) -> GeoJsonFeature:
    """Build a GeoJSON Feature mapping from a geometry and side data.

    Parameters
    ----------
    geom : Geometry, optional
        Geometry to encode. ``None`` emits a null GeoJSON geometry. A geometry
        with a CRS must use EPSG:4326 longitude/latitude coordinates.
    properties : Mapping[str, Any], optional
        Feature properties. The mapping is copied and all keys must be strings.
    id : str or finite number, optional
        Feature identifier.

    Returns
    -------
    GeoJsonFeature
        A new ``{"type": "Feature", ...}`` mapping.

    Raises
    ------
    TypeError
        If ``geom`` is not a Geometry or ``None``.
    GeometryError
        If properties are not a string-keyed mapping, or the id is invalid.
    InvalidGeometryError
        If a WGS84-tagged geometry has coordinates outside the lon/lat domain.
    CRSError
        If a CRS-tagged geometry is not in the WGS84 lon/lat family.

    Notes
    -----
    Coordinate epoch metadata is not representable in GeoJSON Feature mappings
    and is **silently dropped** (same contract as ``to_geojson(..., drop_epoch=True)``).

    Examples
    --------
    >>> import gometry as gm
    >>> feature = gm.to_feature(gm.Point(1, 2), properties={"name": "A"})
    >>> feature.get("properties")
    {'name': 'A'}
    """

@overload
def to_feature_collection(
    values: Features, *, properties: None = None, ids: None = None
) -> GeoJsonFeatureCollection: ...
@overload
def to_feature_collection(
    values: Geometry | GeometryArray | Iterable[Geometry | None] | None,
    *,
    properties: Mapping[str, Any] | Iterable[Mapping[str, Any] | None] | None = None,
    ids: Iterable[FeatureId] | None = None,
) -> GeoJsonFeatureCollection:
    """Build a GeoJSON FeatureCollection from geometries and aligned side data.

    Parameters
    ----------
    values : Features, Geometry, None, GeometryArray, or iterable of Geometry or None
        A `Features` record reuses its aligned geometries, properties, and ids.
        Otherwise, one geometry or geometry rows to encode. CRS-tagged rows must
        use EPSG:4326 longitude/latitude coordinates.
    properties : Mapping or iterable of Mapping or None, optional
        One mapping broadcasts to every geometry. An iterable supplies one
        mapping or explicit ``None`` per row. Omit for independent empty mappings.
    ids : iterable of str, finite number, or None, optional
        One optional feature identifier per geometry.

    Returns
    -------
    GeoJsonFeatureCollection
        A new ``{"type": "FeatureCollection", "features": [...]}`` mapping.

    Raises
    ------
    TypeError
        If a geometry row is not a Geometry or ``None``.
    GeometryError
        If properties or ids are invalid or are not aligned with geometries.
    CRSError
        If a CRS-tagged geometry is not EPSG:4326 longitude/latitude.

    Examples
    --------
    >>> import gometry as gm
    >>> fc = gm.to_feature_collection(gm.GeometryArray([gm.Point(1, 2)]))
    >>> fc["type"], len(fc["features"])
    ('FeatureCollection', 1)
    """

@overload
def from_geojson(
    data: GeoJsonFeatureCollection,
    *,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray: ...
@overload
def from_geojson(
    data: GeoJsonGeometry | GeoJsonFeatureNonNull,
    *,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry: ...
@overload
def from_geojson(
    data: _GeoJsonScalar,
    *,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry | GeometryArray: ...
@overload
def from_geojson(
    data: Iterable[_GeoJsonScalar | None],
    *,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray: ...
@overload
def from_geojson(
    data: _GeoJsonScalar | Iterable[_GeoJsonScalar | None],
    *,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry | GeometryArray:
    """Parse `geojson` from a string or mapping.

    A geometry or ``Feature`` decodes to a ``Geometry`` (Feature properties
    are dropped — see `from_features` to keep them); a ``FeatureCollection``
    is a feature set, so it decodes to a ``GeometryArray`` with one geometry
    per feature.

    Coordinate sequences are **axis-uniform**: every position in one sequence
    (LineString, MultiPoint member list, ring, …) must share the same axes
    (all XY or all XYZ). RFC 7946 makes the third ordinate optional per
    position, but gometry's coordinate model requires finite values on every
    active ordinate and rejects non-finite coordinates, so mixed XY/XYZ within
    one sequence is a ``ParseError`` rather than a silent 0-elevation fill.
    Distinct members of a ``GeometryCollection`` may still differ in axes.

    Parameters
    ----------
    data : str or mapping
        A `geojson` string or mapping (Feature/FeatureCollection ok).

    crs : str or int, default 'OGC:CRS84'
        CRS to attach. `geojson` coordinates are WGS84 by specification, so
        the default declares OGC:CRS84 (lon/lat, matching GeoParquet);
        pass ``None`` for a CRS-free geometry or ``crs=4326`` for EPSG:4326.
        Only the WGS84 family (``EPSG:4326``, ``EPSG:4979``, ``OGC:CRS84``,
        ``OGC:CRS84h``) is accepted — reproject first for other CRS. A legacy
        top-level ``crs`` member (pre-RFC 7946) is ignored when it agrees with
        ``crs=`` and raises on conflict or unsupported declarations.

    epoch : float, optional
        Coordinate epoch (decimal year) to attach as frame metadata.

    Returns
    -------
    Geometry or GeometryArray
        The decoded geometry, or one geometry per feature for a
        ``FeatureCollection``.

    Raises
    ------
    ParseError
        If the `geojson` is malformed or an unsupported type, a coordinate
        sequence mixes axes (XY with XYZ), a coordinate integer is not exactly
        representable as binary64, or a legacy ``crs`` member is unsupported or
        conflicts with ``crs`` (``format`` is ``"geojson"``).
    InvalidGeometryError
        If a position is outside the WGS84 lon/lat domain or a ring fails
        structural ring admission.
    CRSError
        If ``crs`` is not a recognized CRS or is outside the WGS84 family, or
        ``epoch`` is set with ``crs=None``.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Notes
    -----
    Finite decimals parse as correctly-rounded binary64 (bit-exact round-trip
    with ``to_geojson``). Integer tokens and Python ``int`` values are admitted
    only when exactly representable as ``float``; non-exact integers raise
    rather than silently rounding. Text and mapping inputs share this rule.

    See Also
    --------
    Geometry.to_geojson : Serialize a geometry to GeoJSON.
    from_features : Keep per-feature properties and ids.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.from_geojson('{"type": "Point", "coordinates": [1, 2]}').to_wkt()
    'POINT (1 2)'
    """

@overload
def require(
    value: _GeometryT,
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> _GeometryT: ...
@overload
def require(
    value: GeometryArray[_GeometryT],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[_GeometryT]: ...
@overload
def require(
    value: str | Buffer | Mapping[str, Any] | SupportsGeoInterface,
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> Geometry: ...
@overload
def require(
    value: Iterable[_GeometryT | None],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[_GeometryT]: ...
@overload
def require(
    value: Iterable[
        Geometry | str | Buffer | Mapping[str, Any] | SupportsGeoInterface | None
    ],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[Geometry]:
    """Parse and require a geometry contract at an input boundary.

    Parameters
    ----------
    value : geometry-like or iterable of geometry-like
        One geometry, a `GeometryArray`, or an iterable. Foreign scalar inputs
        may be WKT, WKB, GeoJSON mappings/text, or ``__geo_interface__`` objects.
    crs : str or int, optional
        CRS as an EPSG code or authority/WKT string to attach.
    axes : {'XY', 'XYZ', 'XYM', 'XYZM'}, optional
        If given, require the geometry's coordinate axes to match exactly,
        otherwise raise.

    Returns
    -------
    Geometry or GeometryArray
        The validated input. Iterables return a `GeometryArray`.

    Raises
    ------
    CRSError
        If ``crs`` is not a recognized CRS.
    ParseError
        If foreign GeoJSON is malformed, or its legacy ``crs`` member is
        unsupported or conflicts with ``crs`` (``format`` is ``"GeoJSON"``).
    CRSMismatchError
        If an already-decoded / native / non-GeoJSON geometry's CRS differs
        from ``crs``.
    InvalidGeometryError
        If the geometry is invalid, or its axes differ from ``axes``.
        Geographic antimeridian crossings are validated after topology
        normalization; projected and CRS-free geometry remains planar.

    See Also
    --------
    Geometry.validate : Structured validity report.
    Geometry.repair : Fix the geometry.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.require(gm.Point(1, 2, crs=4326), crs=4326).to_wkt()
    'POINT (1 2)'
    """

def crs_engine() -> CrsEngineInfo:
    """Name/version of the underlying CRS engine (PROJ).

    Returns
    -------
    dict
        Engine metadata. ``paths`` is the effective per-context grid search
        path configured through :func:`crs_configure`.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_engine()['version']
    '9.8.1'
    """

def crs_config() -> CrsRuntimeConfig:
    """Return the current CRS engine configuration.

    Returns
    -------
    dict
        The current CRS engine configuration.

    Examples
    --------
    >>> import gometry as gm
    >>> isinstance(gm.crs_config(), dict)
    True
    """

def crs_configure(
    *,
    search_paths: str | PathLike[str] | Iterable[str | PathLike[str]] | None = None,
    user_writable_directory: str | PathLike[str] | None = None,
) -> CrsRuntimeConfig:
    """Configure local CRS engine paths.

    Parameters
    ----------
    search_paths : str, path, or sequence of these, optional
        Directories PROJ searches for its database and grids.
    user_writable_directory : str or path, optional
        Directory PROJ includes in local grid lookup.

    Returns
    -------
    dict
        The effective configuration after applying the changes.

    Examples
    --------
    >>> import gometry as gm
    >>> isinstance(gm.crs_configure(), dict)
    True
    """

def crs_clear_cache() -> None:
    """Clear the CRS object cache.

    Returns
    -------
    None

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_clear_cache()
    >>> True
    True
    """

def crs_info(value: CrsInput) -> CrsInfo:
    """Descriptive information about a CRS.

    Parameters
    ----------
    value : CRS-like
        CRS as an EPSG code or authority/WKT string.

    Returns
    -------
    dict
        Descriptive CRS metadata (name, authority/code, datum, ellipsoid, axes).

    Raises
    ------
    CRSError
        If the value is not a recognized CRS.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_info(4326)['name']
    'WGS 84'
    """

def crs_cache_info() -> CrsCacheInfo:
    """Statistics about the CRS caches.

    Returns
    -------
    dict
        CRS cache statistics (generation, entry counts, per-bucket info).

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_cache_info()['total_capacity'] > 0
    True
    """

def crs_reset() -> CrsRuntimeConfig:
    """Reset CRS engine configuration to defaults.

    Returns
    -------
    dict
        The configuration after the reset.

    Examples
    --------
    >>> import gometry as gm
    >>> isinstance(gm.crs_reset(), dict)
    True
    """

def crs_grid(name: str) -> CrsGridDatabaseInfo:
    """Information about a transformation grid.

    Parameters
    ----------
    name : str
        Grid short name (e.g. ``'us_noaa_g2018u0.tif'``).

    Returns
    -------
    dict
        Local grid metadata and availability.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_grid('null')['available']
    True
    """

def crs_search(
    name: str,
    *,
    authority: str | None = None,
    kind: CrsCatalogKind | None = None,
    approximate: bool = False,
    limit: int = 20,
) -> list[CrsCatalogInfo]:
    """Search the PROJ catalog for CRS by name.

    Parameters
    ----------
    name : str
        Substring to match against CRS names.
    authority : str, optional
        Restrict to this authority.
    kind : str, optional
        Restrict to a CRS kind (e.g. ``"projected"``).
    approximate : bool, optional
        Allow approximate name matching (default ``False``).
    limit : int, optional
        Maximum number of results.

    Returns
    -------
    list of CrsCatalogInfo

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_search('WGS 84')[0]['code']
    '4326'
    """

def crs_catalog(
    *,
    authority: str | None = None,
    kind: CrsCatalogKind | None = None,
    area_of_interest: CrsAreaInput | None = None,
    contains_area_of_interest: bool = False,
    allow_deprecated: bool = False,
    celestial_body: str | None = None,
) -> list[CrsCatalogInfo]:
    """Catalog of CRS in the database matching the given filters.

    Parameters
    ----------
    authority : str, optional
        Registry authority to search (default ``"EPSG"``).
    kind : str, optional
        Restrict to a CRS kind.
    area_of_interest : sequence of float, dict, or object, optional
        Filter area as ``(west, south, east, north)`` in DEGREES, an area
        mapping, or an AreaOfInterest-like object.
    contains_area_of_interest : bool, optional
        Require the CRS area to contain ``area_of_interest``
        (default ``False``).
    allow_deprecated : bool, optional
        Include deprecated CRS (default ``False``).
    celestial_body : str, optional
        Restrict to this celestial body.

    Returns
    -------
    list of CrsCatalogInfo

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_catalog(authority='EPSG')[0]['code']
    '2000'
    """

def crs_authorities() -> list[str]:
    """Names of all registry authorities known to PROJ.

    Returns
    -------
    list of str

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_authorities()[:3]
    ['EPSG', 'ESRI', 'IAU_2015']
    """

def crs_codes(
    authority: str,
    *,
    kind: CrsDatabaseKind | None = None,
    allow_deprecated: bool = False,
) -> list[str]:
    """Object codes within a registry authority.

    Parameters
    ----------
    authority : str
        Registry authority, e.g. ``"EPSG"``.
    kind : str, optional
        Restrict to a CRS kind.
    allow_deprecated : bool, optional
        Include deprecated codes (default ``False``).

    Returns
    -------
    list of str

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_codes('EPSG')[:3]
    ['10150', '10151', '10156']
    """

def crs_utm_zones(
    *,
    datum_name: str | None = None,
    area_of_interest: CrsAreaInput | None = None,
    contains_area_of_interest: bool = False,
    allow_deprecated: bool = False,
) -> list[CrsCatalogInfo]:
    """UTM-zone CRS from the catalog.

    Parameters
    ----------
    datum_name : str, optional
        Restrict to this datum.
    area_of_interest : sequence of float, dict, or object, optional
        Filter area as ``(west, south, east, north)`` in DEGREES, an area
        mapping, or an AreaOfInterest-like object.
    contains_area_of_interest : bool, optional
        Require the zone area to contain ``area_of_interest``
        (default ``False``).
    allow_deprecated : bool, optional
        Include deprecated zones (default ``False``).

    Returns
    -------
    list of CrsCatalogInfo

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_utm_zones()[0]['code']
    '10286'
    """

def crs_celestial_bodies(*, authority: str | None = None) -> list[CrsCelestialBodyInfo]:
    """Celestial bodies known to the CRS database.

    Parameters
    ----------
    authority : str, optional
        Restrict to this authority.

    Returns
    -------
    list of CrsCelestialBodyInfo

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_celestial_bodies()[0]['name']
    '1_Ceres'
    """

def crs_proj_operations() -> list[CrsProjOperationCatalogInfo]:
    """List PROJ operations available.

    Returns
    -------
    list
        One entry per available PROJ operation.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_proj_operations()[0]['id']
    'adams_hemi'
    """

def crs_ellipsoids() -> list[CrsEllipsoidCatalogInfo]:
    """List ellipsoids in the CRS database.

    Returns
    -------
    list
        One entry per ellipsoid in the CRS database.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_ellipsoids()[0]['id']
    'MERIT'
    """

def crs_prime_meridians() -> list[CrsPrimeMeridianCatalogInfo]:
    """List prime meridians in the CRS database.

    Returns
    -------
    list
        One entry per prime meridian in the CRS database.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_prime_meridians()[0]['id']
    'greenwich'
    """

def crs_unit(authority: str, code: str) -> CrsUnitInfo:
    """Detail record for a single unit of measure.

    Parameters
    ----------
    authority, code : str
        Unit authority and code (e.g. ``'EPSG'``, ``'9001'``).

    Returns
    -------
    dict
        The unit-of-measure record (name, category, conversion factor).

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_unit('EPSG', '9001')['name']
    'metre'
    """

def crs_units(
    authority: str,
    *,
    category: str | None = None,
    allow_deprecated: bool = False,
) -> list[CrsUnitInfo]:
    """List the units of measure an authority defines.

    Parameters
    ----------
    authority : str
        Unit authority (e.g. ``'EPSG'``).
    category : str, optional
        Restrict to one unit category (e.g. ``'linear'``, ``'angular'``).
    allow_deprecated : bool, default False
        Whether deprecated units are included.

    Returns
    -------
    list
        One record per matching unit of measure.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_units('EPSG')[0]['name']
    '(bin)'
    """

@overload
def crs_roundtrip(
    source: CrsInput,
    target: CrsInput,
    x: float,
    y: float,
    z: float | None = None,
    *,
    t: float | None = None,
    iterations: int = 1,
    direction: TransformDirection = 'forward',
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> float: ...
@overload
def crs_roundtrip(
    source: CrsInput,
    target: CrsInput,
    x: FloatColumn,
    y: FloatColumn,
    z: FloatInput | None = None,
    *,
    t: FloatInput | None = None,
    iterations: int = 1,
    direction: TransformDirection = 'forward',
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> npt.NDArray[np.float64]: ...
@overload
def crs_roundtrip(
    source: CrsInput,
    target: CrsInput,
    x: FloatColumn,
    y: FloatInput,
    z: FloatInput | None = None,
    *,
    t: FloatInput | None = None,
    iterations: int = 1,
    direction: TransformDirection = 'forward',
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> npt.NDArray[np.float64]: ...
@overload
def crs_roundtrip(
    source: CrsInput,
    target: CrsInput,
    x: FloatInput,
    y: FloatColumn,
    z: FloatInput | None = None,
    *,
    t: FloatInput | None = None,
    iterations: int = 1,
    direction: TransformDirection = 'forward',
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> npt.NDArray[np.float64]: ...
@overload
def crs_roundtrip(
    source: CrsInput,
    target: CrsInput,
    x: FloatInput,
    y: FloatInput,
    z: FloatInput | None = None,
    *,
    t: FloatInput | None = None,
    iterations: int = 1,
    direction: TransformDirection = 'forward',
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> float | npt.NDArray[np.float64]:
    """Round-trip coordinates through a CRS pair to measure error.

    Parameters
    ----------
    source, target : CRS-like
        Source and destination CRS (EPSG code or authority/WKT string).
    x, y : float or sequence of float
        Coordinates, scalar or batch.
    z, t : float or sequence of float, optional
        Height and time ordinates when provided.
    iterations : int, default 1
        How many forward+inverse passes to apply.
    direction : {'forward', 'inverse'}, default 'forward'
        Which leg runs first.
    area_of_interest : sequence of float, dict, or object, optional
        Area of interest guiding operation selection, as
        ``(west, south, east, north)`` in DEGREES, an area mapping, or an
        AreaOfInterest-like object.
    source_epoch, target_epoch : float, optional
        Coordinate epochs for dynamic CRS.
    authority : str, optional
        Restrict candidate coordinate operations to this authority
        (e.g. ``'EPSG'``).

    accuracy : float, optional
        Maximum acceptable operation accuracy, in meters.

    allow_ballpark : bool, optional
        Allow low-accuracy ballpark operations when no precise one exists.

    only_best : bool, optional
        Require PROJ's best operation. If a required transformation grid is
        unavailable, raise ``TransformError`` instead of using a less accurate
        fallback operation.

    force_over : bool, optional
        Keep coordinates on the source side of the antimeridian instead of
        wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
        ``only_best``, this also collapses operation selection to a single
        candidate, so enumerating surfaces return exactly one operation.

    Returns
    -------
    float or numpy.ndarray
        The round-trip error per coordinate; scalar in, scalar out, and
        lane inputs return a read-only ``float64`` ``numpy.ndarray``.

    Raises
    ------
    TransformError
        If no transform exists between the frames or it fails to apply.
    CRSError
        If ``source``/``target`` are unrecognized.
    InvalidGeometryError
        If coordinate columns are non-finite or differ in length.
    GeometryError
        If an epoch option is invalid.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.crs_roundtrip(4326, 3857, -122.4, 37.8) < 1e-9
    True
    """

@overload
def crs_apply(
    operation: str,
    x: float,
    y: float,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: float,
    y: float,
    z: float,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[float, float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: float,
    y: float,
    z: float,
    *,
    t: float,
    direction: TransformDirection = 'forward',
) -> tuple[float, float, float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatColumn,
    y: FloatColumn,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatColumn,
    y: FloatColumn,
    z: FloatColumn,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatColumn,
    y: FloatColumn,
    z: FloatColumn,
    *,
    t: FloatColumn,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatColumn,
    y: FloatInput,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatColumn,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatInput,
    *,
    direction: TransformDirection = 'forward',
) -> tuple[float, float] | tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatInput,
    z: FloatInput,
    *,
    direction: TransformDirection = 'forward',
) -> (
    tuple[float, float, float]
    | tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]]
): ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatInput,
    z: FloatInput,
    *,
    t: FloatInput,
    direction: TransformDirection = 'forward',
) -> (
    tuple[float, float, float, float]
    | tuple[
        npt.NDArray[np.float64],
        npt.NDArray[np.float64],
        npt.NDArray[np.float64],
        npt.NDArray[np.float64],
    ]
): ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatInput,
    z: FloatInput | None = None,
    *,
    t: FloatInput | None = None,
    direction: TransformDirection = 'forward',
) -> (
    tuple[float, float]
    | tuple[float, float, float]
    | tuple[float, float, float, float]
    | tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]
    | tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]]
    | tuple[
        npt.NDArray[np.float64],
        npt.NDArray[np.float64],
        npt.NDArray[np.float64],
        npt.NDArray[np.float64],
    ]
):
    """Apply a PROJ pipeline/operation definition to coordinates.

    Runs an explicit operation (e.g. a
    ``+proj=pipeline`` string) rather than resolving one from a CRS pair.

    Parameters
    ----------
    operation : str
        PROJ operation or pipeline definition.
    x, y : float or sequence of float
        Coordinate columns (scalars transform a single point).
    z : float or sequence of float, optional
        Vertical column for 3D operations.
    t : float or sequence of float, optional
        Coordinate epoch column.
    direction : {'forward', 'inverse'}, default 'forward'
        Operation direction.

    Returns
    -------
    tuple
        The transformed columns — ``(x, y)``, ``(x, y, z)``, or
        ``(x, y, z, t)``. Scalars in, scalars out; lane inputs return
        read-only ``float64`` ``numpy.ndarray`` columns (``np.asarray(column)``
        reads the values directly, ``list(column)`` materializes them).

    Raises
    ------
    TransformError
        If no transform exists between the frames or it fails to apply.
    CRSError
        If ``source``/``target`` are unrecognized.
    InvalidGeometryError
        If coordinate columns are non-finite or differ in length.
    GeometryError
        If ``direction`` is invalid.

    Examples
    --------
    >>> import gometry as gm
    >>> import numpy as np
    >>> op = gm.CRS(4326).operation(3857).get('definition') or ''
    >>> np.round(np.asarray(gm.crs_apply(op, -122.4, 37.8)), 1).tolist()
    [-13625505.7, 4551210.9]
    """

@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: float,
    y: float,
    *,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> tuple[float, float]: ...
@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: float,
    y: float,
    z: float,
    *,
    t: float | None = None,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> tuple[float, float, float]: ...
@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: FloatColumn,
    y: FloatColumn,
    z: FloatColumn | None = None,
    *,
    t: FloatColumn | None = None,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> npt.NDArray[np.float64]: ...
@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: FloatInput,
    y: FloatInput,
    z: FloatInput | None = None,
    *,
    t: FloatInput | None = None,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> tuple[float, float] | tuple[float, float, float] | npt.NDArray[np.float64]:
    """Reproject raw coordinates from one CRS to another.

    For geometries use ``to_crs``; this is
    the lower-level coordinate-column form.

    Parameters
    ----------
    source, target : CRS-like
        Source and target CRS (EPSG code or authority/WKT string).
    x, y : float or sequence of float
        Coordinate columns (scalars transform a single point).
    z : float or sequence of float, optional
        Vertical column for 3D transforms.
    t : float or sequence of float, optional
        Coordinate epoch column.
    area_of_interest : sequence of float, dict, or object, optional
        Area of interest guiding operation selection, as
        ``(west, south, east, north)`` in DEGREES, an area mapping, or an
        AreaOfInterest-like object.
    source_epoch, target_epoch : float, optional
        Coordinate epochs for dynamic CRS.
    authority : str, optional
        Restrict candidate coordinate operations to this authority
        (e.g. ``'EPSG'``).

    accuracy : float, optional
        Maximum acceptable operation accuracy, in meters.

    allow_ballpark : bool, optional
        Allow low-accuracy ballpark operations when no precise one exists.

    only_best : bool, optional
        Require PROJ's best operation. If a required transformation grid is
        unavailable, raise ``TransformError`` instead of using a less accurate
        fallback operation.

    force_over : bool, optional
        Keep coordinates on the source side of the antimeridian instead of
        wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
        ``only_best``, this also collapses operation selection to a single
        candidate, so enumerating surfaces return exactly one operation.

    Returns
    -------
    tuple or numpy.ndarray
        Scalars in, scalars out: ``(x, y)`` or ``(x, y, z)``. Lane input returns
        a read-only ``(N, 2)``/``(N, 3)`` ``float64`` matrix (interleaved, the
        same shape as `get_coordinates`), so ``result[:, 0]`` / ``result[:, 1]``
        read the transformed columns directly. The input epoch ``t`` is not a
        transformed spatial ordinate and is not returned (use `apply` if you
        need the raw columns echoed back).

    Raises
    ------
    CRSError
        If ``source``/``target`` are unrecognized.
    TransformError
        If the transform is undefined or fails to apply.
    InvalidGeometryError
        If coordinate columns are non-finite or differ in length.
    GeometryError
        If an epoch option is invalid.

    See Also
    --------
    Geometry.to_crs : Reproject a geometry.

    Examples
    --------
    >>> import gometry as gm
    >>> import numpy as np
    >>> np.round(np.asarray(gm.crs_transform(4326, 3857, -122.4, 37.8)), 1).tolist()
    [-13625505.7, 4551210.9]
    """

@overload
def crs_transform_bounds(
    source: CrsInput,
    target: CrsInput,
    bounds: _Bounds2D,
    *,
    densify: int = 21,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> tuple[float, float, float, float]: ...
@overload
def crs_transform_bounds(
    source: CrsInput,
    target: CrsInput,
    bounds: _Bounds3D,
    *,
    densify: int = 21,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> tuple[float, float, float, float, float, float]: ...
@overload
def crs_transform_bounds(
    source: CrsInput,
    target: CrsInput,
    bounds: Iterable[_Bounds2D] | Iterable[_Bounds3D],
    *,
    densify: int = 21,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> npt.NDArray[np.float64]: ...
@overload
def crs_transform_bounds(
    source: CrsInput,
    target: CrsInput,
    bounds: Iterable[float],
    *,
    densify: int = 21,
    area_of_interest: CrsAreaInput | None = None,
    source_epoch: float | None = None,
    target_epoch: float | None = None,
    authority: str | None = None,
    accuracy: float | None = None,
    allow_ballpark: bool | None = None,
    only_best: bool | None = None,
    force_over: bool = False,
) -> (
    tuple[float, float, float, float] | tuple[float, float, float, float, float, float]
):
    """Reproject a bounding box, densifying edges for accuracy.

    Parameters
    ----------
    source, target : CRS-like
        Source and destination CRS (EPSG code or authority/WKT string).
    bounds : tuple
        ``(minx, miny, maxx, maxy)`` (or a 3D ``(minx, miny, minz, maxx,
        maxy, maxz)``) box in the source CRS.
    densify : int, default 21
        Points added per edge before transforming, to track curved edges.
    area_of_interest : sequence of float, dict, or object, optional
        Area of interest guiding operation selection, as
        ``(west, south, east, north)`` in DEGREES, an area mapping, or an
        AreaOfInterest-like object.
    source_epoch, target_epoch : float, optional
        Coordinate epochs for dynamic CRS.
    authority : str, optional
        Restrict candidate coordinate operations to this authority
        (e.g. ``'EPSG'``).

    accuracy : float, optional
        Maximum acceptable operation accuracy, in meters.

    allow_ballpark : bool, optional
        Allow low-accuracy ballpark operations when no precise one exists.

    only_best : bool, optional
        Require PROJ's best operation. If a required transformation grid is
        unavailable, raise ``TransformError`` instead of using a less accurate
        fallback operation.

    force_over : bool, optional
        Keep coordinates on the source side of the antimeridian instead of
        wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
        ``only_best``, this also collapses operation selection to a single
        candidate, so enumerating surfaces return exactly one operation.

    Returns
    -------
    tuple
        The reprojected box in the target CRS.

    Raises
    ------
    TransformError
        If no transform exists between the frames or it fails to apply.
    CRSError
        If ``source``/``target`` are unrecognized.
    GeometryError
        If ``bounds`` is not a 4- or 6-value sequence of finite floats.

    Examples
    --------
    >>> import gometry as gm
    >>> import numpy as np
    >>> np.round(np.asarray(gm.crs_transform_bounds(
    ...     4326, 3857, (-123, 37, -122, 38))), 0).tolist()
    [-13692297.0, 4439107.0, -13580978.0, 4579426.0]
    """

@overload
def geohash_cover(
    geom: GeometryArray,
    precision: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[GeohashCell]]: ...
@overload
def geohash_cover(
    geom: Geometry,
    precision: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> GeohashCoverage:
    """Cover a geometry with geohash cells at ``precision``.

    The result carries both ``cells`` — exactly the
    cells satisfying ``cell_rule`` — and the exact membership predicates
    ``covers``/``contains``/``intersects``, which always answer against
    the source geometry.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Geometry to cover (WGS84 lon/lat or projected). An array returns one
        grouped cell row per input geometry.

    precision : int
        Geohash precision (``1``-``12``; finer at higher values).

    cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize, strictest to loosest. ``'center'``:
        cells whose center is inside — unique assignment, balanced point
        binning. ``'within'``: only cells entirely inside — cells the area
        fully owns. ``'overlap'``: every cell touching the geometry — a
        complete-coverage superset, the safe default for candidate keys.
        ``'bbox'``: cells whose bounding box overlaps — loosest and fastest;
        for geohash a cell IS its bbox, so identical to ``'overlap'``. The
        rule never affects the exact predicates.

    max_cells : int or None, default 1000000
        Upper bound on emitted cells. Raise to allow a larger covering, or
        pass ``None`` for unlimited (bounded only by memory).

    Returns
    -------
    GeohashCoverage or Groups of CellArray
        A scalar returns its coverage; an array returns one cell group per row.

    Raises
    ------
    GeometryError
        If the geometry, depth, or a coverage parameter is invalid, or if
        the covering would exceed ``max_cells``.

    Examples
    --------
    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cov = gm.geohash_cover(p, precision=6)
    >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
    (1, True, '9q8yyk')
    """

def geohash_cells(
    values: GeometryArray[Point] | FloatInput,
    lat: FloatInput | None = None,
    *,
    precision: _IntInput,
) -> CellArray[GeohashCell]:
    """Build geohash cells from parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, float, or sequence of float
        Point geometries or WGS84 longitudes. Projected point arrays are
        reprojected in one native batch.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Scalars
        broadcast numpy-style; at least one coordinate column must be sequence of float.

    precision : int or sequence of int
        Geohash precision (1-12 characters; finer at higher values). A scalar
        broadcasts to every row; an array supplies one precision per row.

    Returns
    -------
    CellArray of GeohashCell
        One cell per input coordinate.

    See Also
    --------
    GeohashCell : Build a single cell.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.GeohashCell(112.5584, 37.8324, precision=9).token
    'ww8p1r4t8'
    """

def geohash_bounding_cell(
    value: Geometry | GeometryArray | Iterable[float],
) -> GeohashCell:
    """Return the deepest single geohash cell containing a geometry or lon/lat bounds.

    Walks the corner cells up to their common prefix. There is no global
    geohash root, so bounds that straddle the precision-1 grid have no
    containing cell and raise.

    Parameters
    ----------
    value : Geometry, GeometryArray, or sequence of float
        A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
        ``(minx, miny, maxx, maxy)`` bounds.

    Returns
    -------
    GeohashCell
        The deepest cell whose rectangle contains the whole bounds.

    Raises
    ------
    InvalidGeometryError
        If the geometry is empty or coordinates leave the lon/lat domain.
    GeometryError
        If no single geohash cell contains the bounds, or bare bounds are
        not ordered min <= max.

    Examples
    --------
    >>> import gometry as gm
    >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    >>> gm.geohash_bounding_cell(berlin).token
    'u33'
    """

def geohash_union(
    left: GeohashCell | str | Iterable[GeohashCell | str],
    right: GeohashCell | str | Iterable[GeohashCell | str],
) -> CellArray[GeohashCell]:
    """Hierarchy-aware union of two geohash cell sets.

    Returns the normalized cell union: sorted, with contained cells absorbed
    by their ancestors and complete sibling groups merged into parents.


    Parameters
    ----------
    left, right : GeohashCell, str, or iterable of those
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

    Returns
    -------
    CellArray of GeohashCell

    Raises
    ------
    ParseError
        If a cell input is not valid for the geohash grid.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.geohash_cover(p, precision=6).cells)
    >>> len(gm.geohash_union(cells, cells))
    1
    """

def geohash_intersection(
    left: GeohashCell | str | Iterable[GeohashCell | str],
    right: GeohashCell | str | Iterable[GeohashCell | str],
) -> CellArray[GeohashCell]:
    """Hierarchy-aware intersection of two geohash cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.


    Parameters
    ----------
    left, right : GeohashCell, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of GeohashCell

    Raises
    ------
    ParseError
        If an id or token is not a valid geohash cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.geohash_cover(p, precision=6).cells)
    >>> len(gm.geohash_intersection(cells, cells))
    1
    """

def geohash_difference(
    left: GeohashCell | str | Iterable[GeohashCell | str],
    right: GeohashCell | str | Iterable[GeohashCell | str],
) -> CellArray[GeohashCell]:
    """Hierarchy-aware difference of two geohash cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized.


    Parameters
    ----------
    left, right : GeohashCell, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of GeohashCell

    Raises
    ------
    ParseError
        If an id or token is not a valid geohash cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.geohash_cover(p, precision=6).cells)
    >>> len(gm.geohash_difference(cells, []))
    1
    """

@overload
def tile_cover(
    geom: GeometryArray,
    zoom: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[Tile]]: ...
@overload
def tile_cover(
    geom: Geometry,
    zoom: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> TileCoverage:
    """Cover a geometry with XYZ web-mercator tiles at ``zoom``.

    The result carries both ``cells`` — exactly the
    tiles satisfying ``cell_rule`` — and the exact membership predicates
    ``covers``/``contains``/``intersects``, which always answer against
    the source geometry.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Geometry to cover (WGS84 lon/lat or projected). An array returns one
        grouped cell row per input geometry.

    zoom : int
        Tile zoom (``0``-``29``; finer at higher values).

    cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
        Which tiles to materialize, strictest to loosest. ``'center'``:
        tiles whose center is inside — unique assignment, balanced point
        binning. ``'within'``: only tiles entirely inside — tiles the area
        fully owns. ``'overlap'``: every tile touching the geometry — a
        complete-coverage superset, the safe default for candidate keys.
        ``'bbox'``: tiles whose bounding box overlaps — loosest and fastest;
        a tile IS its bbox, so identical to ``'overlap'``. The rule never
        affects the exact predicates.

    max_cells : int or None, default 1000000
        Upper bound on emitted cells. Raise to allow a larger covering, or
        pass ``None`` for unlimited (bounded only by memory).

    Returns
    -------
    TileCoverage or Groups of CellArray
        A scalar returns its coverage; an array returns one cell group per row.

    Raises
    ------
    GeometryError
        If the geometry, depth, or a coverage parameter is invalid, or if
        the covering would exceed ``max_cells``.
    InvalidGeometryError
        If any vertex latitude is outside the Web Mercator domain
        (±85.05112878°). Tile coverings cannot extend past that edge; out-of-
        domain geometries are rejected (same typed error as ``Tile`` /
        ``tile_cells``), never silently clipped.

    Examples
    --------
    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cov = gm.tile_cover(p, zoom=10)
    >>> (len(cov.cells), cov.contains(p), str(cov.cells[0]))
    (1, True, '0230102033')
    """

def tile_cells(
    values: GeometryArray[Point] | FloatInput,
    lat: FloatInput | None = None,
    *,
    zoom: _IntInput,
) -> CellArray[Tile]:
    """Build tiles from parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, float, or sequence of float
        Point geometries or WGS84 longitudes. Projected point arrays are
        reprojected in one native batch.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Scalars
        broadcast numpy-style; at least one coordinate column must be sequence of float.
        Latitudes outside the Web Mercator domain (±85.051129°) raise
        ``InvalidGeometryError`` (no silent clamp).

    zoom : int or sequence of int
        Zoom level (0-29; finer at higher values). A scalar broadcasts to
        every row; an array supplies one zoom per row.

    Returns
    -------
    CellArray of Tile
        One tile per input coordinate.

    See Also
    --------
    Tile : Build a single tile.

    Examples
    --------
    >>> import gometry as gm
    >>> tile = gm.Tile(lon=-105.939, lat=35.687, zoom=9)
    >>> (tile.zoom, tile.x, tile.y)
    (9, 105, 201)
    """

def tile_bounding_cell(value: Geometry | GeometryArray | Iterable[float]) -> Tile:
    """Return the deepest single tile containing a geometry or lon/lat bounds.

    The mercantile ``bounding_tile``: walks corner tiles up to their common
    ancestor. Bounds spanning hemispheres bottom out at the ``z0`` root.
    Latitudes outside the Web Mercator domain raise
    ``InvalidGeometryError`` (no silent clamp).

    Parameters
    ----------
    value : Geometry, GeometryArray, or sequence of float
        A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
        ``(minx, miny, maxx, maxy)`` bounds.

    Returns
    -------
    Tile
        The deepest tile whose rectangle contains the whole bounds.

    Raises
    ------
    InvalidGeometryError
        If the geometry is empty or coordinates leave the lon/lat domain.
    GeometryError
        If bare bounds are not ordered min <= max.

    Examples
    --------
    >>> import gometry as gm
    >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    >>> tile = gm.tile_bounding_cell(berlin)
    >>> (tile.zoom, tile.x, tile.y)
    (5, 17, 10)
    """

def tile_union(
    left: Tile | int | str | Iterable[Tile | int | str],
    right: Tile | int | str | Iterable[Tile | int | str],
) -> CellArray[Tile]:
    """Hierarchy-aware union of two tile cell sets.

    Returns the normalized cell union: sorted, with contained cells absorbed
    by their ancestors and complete sibling groups merged into parents.


    Parameters
    ----------
    left, right : Tile, int, str, or iterable of those
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

    Returns
    -------
    CellArray of Tile

    Raises
    ------
    ParseError
        If a cell input is not valid for the tile grid.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.tile_cover(p, zoom=10).cells)
    >>> len(gm.tile_union(cells, cells))
    1
    """

def tile_intersection(
    left: Tile | int | str | Iterable[Tile | int | str],
    right: Tile | int | str | Iterable[Tile | int | str],
) -> CellArray[Tile]:
    """Hierarchy-aware intersection of two tile cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.


    Parameters
    ----------
    left, right : Tile, int, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of Tile

    Raises
    ------
    ParseError
        If an id or token is not a valid tile cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.tile_cover(p, zoom=10).cells)
    >>> len(gm.tile_intersection(cells, cells))
    1
    """

def tile_difference(
    left: Tile | int | str | Iterable[Tile | int | str],
    right: Tile | int | str | Iterable[Tile | int | str],
) -> CellArray[Tile]:
    """Hierarchy-aware difference of two tile cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized.


    Parameters
    ----------
    left, right : Tile, int, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of Tile

    Raises
    ------
    ParseError
        If an id or token is not a valid tile cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.tile_cover(p, zoom=10).cells)
    >>> len(gm.tile_difference(cells, []))
    1
    """

@overload
def pluscode_encode(value: Point, *, length: int = 10) -> str: ...
@overload
def pluscode_encode(value: float, lat: float, *, length: int = 10) -> str: ...
@overload
def pluscode_encode(
    value: GeometryArray[Point], lat: None = None, *, length: int = 10
) -> list[str | None]: ...
@overload
def pluscode_encode(
    value: FloatColumn,
    lat: FloatInput,
    *,
    length: int = 10,
) -> list[str]: ...
@overload
def pluscode_encode(
    value: FloatInput,
    lat: FloatColumn,
    *,
    length: int = 10,
) -> list[str]:
    """Plus code (Open Location Code) of a point.

    Encodes WGS84 coordinates as Google's Open Location Code — the
    offline-friendly "plus codes" used where street addresses are missing.
    Accepts a ``Point``/`GeometryArray` (CRS-aware, reprojected to lon/lat)
    or a bare ``lon, lat`` pair.

    Parameters
    ----------
    value : Point, GeometryArray, or float
        The point(s) to encode, or a bare longitude.
    lat : float, optional
        Latitude when ``value`` is a longitude.
    length : int, default 10
        Significant digits (even from 2 to 10, then 11-15); 10 is roughly a
        14 m cell, each pair beyond divides it further.

    Returns
    -------
    str or list of str
        The plus code(s), e.g. ``'8FVC2222+22'``.

    Raises
    ------
    GeometryError
        If ``length`` is invalid.
    InvalidGeometryError
        If a coordinate is non-finite.

    Notes
    -----
    Bare longitude/latitude and geometry inputs are validated against the
    WGS84 lon/lat domain before encoding. Out-of-domain finite coordinates
    raise ``InvalidGeometryError`` rather than silent clip/wrap (the OLC
    reference clips, but gometry rejects so huge finite inputs cannot mint
    a code for a different location).

    Examples
    --------
    >>> import gometry as gm
    >>> gm.pluscode_encode(8.628, 47.366)
    '8FVC9J8H+C6'
    """

@overload
def pluscode_polygon(code: str) -> Polygon: ...
@overload
def pluscode_polygon(
    code: Iterable[str],
) -> GeometryArray[Polygon]:
    """Return the rectangular cell a plus code covers, as a WGS84 ``Polygon``.

    Parameters
    ----------
    code : str or iterable of str
        A full plus code (e.g. ``'8FVC9G8F+6X'``), or one code per row.

    Returns
    -------
    Polygon or GeometryArray
        The code cell(s), CRS ``OGC:CRS84``.

    Raises
    ------
    ParseError
        If ``code`` is not a full plus code.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.pluscode_polygon('8FVC9G8F+6X').bounds
    (8.524875, 47.3655, 8.525, 47.36562499999999)
    """

@overload
def pluscode_shorten(code: str, reference: float, lat: float) -> str: ...
@overload
def pluscode_shorten(
    code: Iterable[str],
    reference: FloatInput,
    lat: FloatInput,
) -> list[str]:
    """Shorten a full plus code relative to a nearby reference point.

    Removes leading digits that the reference location implies (at least
    four when close enough); `pluscode_recover` restores them.

    Parameters
    ----------
    code : str or iterable of str
        A full, unpadded plus code with at least 6 digits.
    reference, lat : float or sequence of float
        The reference location(s).

    Returns
    -------
    str or list of str
        The shortened code(s) — or the original when the reference is too far.

    Raises
    ------
    ParseError
        If ``code`` is not a full plus code.
    GeometryError
        If the code is padded or has fewer than 6 digits.
    InvalidGeometryError
        If ``reference``/``lat`` are non-finite.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.pluscode_shorten('8FVC9G8F+6X', 8.5, 47.4)
    '9G8F+6X'
    """

@overload
def pluscode_recover(code: str, reference: float, lat: float) -> str: ...
@overload
def pluscode_recover(
    code: Iterable[str],
    reference: FloatInput,
    lat: FloatInput,
) -> list[str]:
    """Recover the nearest full plus code from a shortened one.

    Parameters
    ----------
    code : str or iterable of str
        A short plus code (e.g. ``'9G8F+6X'``); a full code passes through
        normalized.
    reference, lat : float or sequence of float
        The reference location(s) the code is near.

    Returns
    -------
    str or list of str
        The full plus code(s) closest to the reference.

    Raises
    ------
    ParseError
        If ``code`` is neither a short nor a full plus code.
    InvalidGeometryError
        If ``reference``/``lat`` are non-finite.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.pluscode_recover('9G8F+6X', 8.5, 47.4)
    '8FVC9G8F+6X'
    """

@overload
def osm_shortlink_encode(
    value: Point | float, lat: float | None = None, *, zoom: int = 16
) -> str: ...
@overload
def osm_shortlink_encode(
    value: GeometryArray[Point], lat: None = None, *, zoom: int = 16
) -> list[str | None]: ...
@overload
def osm_shortlink_encode(
    value: FloatColumn,
    lat: FloatInput,
    *,
    zoom: int = 16,
) -> list[str]: ...
@overload
def osm_shortlink_encode(
    value: FloatInput,
    lat: FloatColumn,
    *,
    zoom: int = 16,
) -> list[str]: ...
@overload
def osm_shortlink_encode(
    value: Point | GeometryArray[Point] | FloatInput,
    lat: FloatInput | None = None,
    *,
    zoom: int = 16,
) -> str | list[str | None]:
    """`OpenStreetMap` shortlink code of a point.

    The compact location code in ``https://osm.org/go/...`` URLs (a Morton
    quadtile path, six bits per character). Accepts a
    ``Point``/`GeometryArray` (CRS-aware) or a bare ``lon, lat`` pair.

    Parameters
    ----------
    value : Point, GeometryArray, or float
        The point(s) to encode, or a bare longitude.
    lat : float, optional
        Latitude when ``value`` is a longitude.
    zoom : int, default 16
        Map zoom the link opens at (``0`` to ``22``).

    Returns
    -------
    str or list of str
        The shortlink code(s), e.g. ``'0EEQjE--'``.

    Raises
    ------
    GeometryError
        If ``zoom`` is out of range.
    InvalidGeometryError
        If a coordinate is non-finite or out of the lon/lat domain.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.osm_shortlink_encode(13.365, 52.5077, zoom=17)
    '0MbEUxVoG-'
    """

@overload
def osm_shortlink_location(code: str) -> tuple[float, float, int]: ...
@overload
def osm_shortlink_location(
    code: Iterable[str],
) -> tuple[
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
    npt.NDArray[np.int64],
]:
    """Parse an OSM shortlink code back into its location and zoom.

    Accepts the modern ``~`` spelling and the legacy ``@`` one.

    Parameters
    ----------
    code : str or iterable of str
        The shortlink code(s) (the part after ``osm.org/go/``).

    Returns
    -------
    tuple
        Scalar input returns ``(lon, lat, zoom)``. Bulk input returns
        ``(lon_array, lat_array, zoom_array)``.

    Raises
    ------
    ParseError
        If ``code`` contains characters outside the shortlink alphabet, or is
        too short/long to name a real zoom level.

    Examples
    --------
    >>> import gometry as gm
    >>> lon, lat, zoom = gm.osm_shortlink_location('0MbEUxVoG-')
    >>> round(lon, 3), round(lat, 3), zoom
    (13.365, 52.508, 17)
    """

@overload
def h3_cover(
    geom: GeometryArray,
    resolution: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[H3Cell]]: ...
@overload
def h3_cover(
    geom: Geometry,
    resolution: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> H3Coverage:
    """Cover a geometry with H3 cells at ``resolution``.

    The result carries both ``cells`` — exactly the cells
    satisfying ``cell_rule`` (join keys, bins, visualization) — and the
    exact membership predicates ``covers``/``contains``/``intersects``,
    which always answer against the source geometry.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Geometry to cover (WGS84 lon/lat or projected). An array returns one
        grouped cell row per input geometry.

    resolution : int
        H3 resolution (``0``-``15``; finer at higher values).

    cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize, strictest to loosest. ``'center'``:
        cells whose center is inside — unique assignment, balanced point
        binning. ``'within'``: only cells entirely inside — cells the area
        fully owns. ``'overlap'``: every cell touching the geometry — a
        complete-coverage superset, the safe default for candidate keys.
        ``'bbox'``: cells whose bounding box overlaps — loosest and fastest.
        The rule never affects the exact predicates.

    max_cells : int or None, default 1000000
        Upper bound on emitted cells. Raise to allow a larger covering, or
        pass ``None`` for unlimited (bounded only by memory).

    Returns
    -------
    H3Coverage or Groups of CellArray
        A scalar returns its coverage; an array returns one cell group per row.

    Raises
    ------
    GeometryError
        If the geometry, depth, or a coverage parameter is invalid, or if
        the covering would exceed ``max_cells``.

    See Also
    --------
    h3_cells : Build H3 cells from lon/lat columns.

    Examples
    --------
    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cov = gm.h3_cover(p, resolution=7)
    >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
    (1, True, '872830828ffffff')
    """

def h3_cells(
    values: GeometryArray[Point] | FloatInput,
    lat: FloatInput | None = None,
    *,
    resolution: _IntInput,
) -> CellArray[H3Cell]:
    """Build H3 cells from parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, float, or sequence of float
        Point geometries or WGS84 longitudes. Projected point arrays are
        reprojected in one native batch.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Scalars
        broadcast numpy-style; at least one coordinate column must be sequence of float.

    resolution : int or sequence of int
        H3 resolution (0-15; finer at higher values). A scalar broadcasts to
        every row; an array supplies one resolution per row.

    Returns
    -------
    CellArray of H3Cell
        One cell per input coordinate.

    Raises
    ------
    GeometryError
        If ``resolution`` is out of range or every argument is scalar.
    InvalidGeometryError
        If a coordinate is non-finite or columns differ in length.

    See Also
    --------
    H3Cell : Build a single cell.
    h3_cover : Cover a geometry with H3 cells.

    Examples
    --------
    >>> import gometry as gm
    >>> cells = gm.h3_cells([-122.4, -122.3], [37.8, 37.7], resolution=7)
    >>> (len(cells), cells[0].token)
    (2, '87283080cffffff')
    """

def h3_bounding_cell(value: Geometry | GeometryArray | Iterable[float]) -> H3Cell:
    """Return the deepest single H3 cell containing a geometry or lon/lat bounds.

    Walks all four corner cells up to a common ancestor, then verifies the
    candidate's actual H3 boundary covers the whole bounds.

    Parameters
    ----------
    value : Geometry, GeometryArray, or sequence of float
        A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
        ``(minx, miny, maxx, maxy)`` bounds.

    Returns
    -------
    H3Cell
        The deepest cell whose region contains the whole bounds.

    Raises
    ------
    InvalidGeometryError
        If the geometry is empty or coordinates leave the lon/lat domain.
    GeometryError
        If no single H3 cell contains the bounds, or bare bounds are not
        ordered min <= max.

    Examples
    --------
    >>> import gometry as gm
    >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    >>> gm.h3_bounding_cell(berlin).resolution
    2
    """

def h3_union(
    left: H3Cell | int | str | Iterable[H3Cell | int | str],
    right: H3Cell | int | str | Iterable[H3Cell | int | str],
) -> CellArray[H3Cell]:
    """Hierarchy-aware union of two H3 cell sets.

    Returns the normalized cell union: sorted, with contained cells absorbed
    by their ancestors and complete sibling groups merged into parents.
    This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.

    Parameters
    ----------
    left, right : H3Cell, int, str, or iterable of those
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

    Returns
    -------
    CellArray of H3Cell

    Raises
    ------
    ParseError
        If a cell input is not valid for the H3 grid.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cell = gm.h3_cover(p, resolution=7).cells[0]
    >>> nbr = list(cell.neighbors)[0]
    >>> len(gm.h3_union([cell], [nbr]))
    2
    """

def h3_intersection(
    left: H3Cell | int | str | Iterable[H3Cell | int | str],
    right: H3Cell | int | str | Iterable[H3Cell | int | str],
) -> CellArray[H3Cell]:
    """Hierarchy-aware intersection of two H3 cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.
    This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.

    Parameters
    ----------
    left, right : H3Cell, int, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of H3Cell

    Raises
    ------
    ParseError
        If an id or token is not a valid H3 cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cell = gm.h3_cover(p, resolution=7).cells[0]
    >>> nbr = list(cell.neighbors)[0]
    >>> len(gm.h3_intersection([cell, nbr], [nbr]))
    1
    """

def h3_difference(
    left: H3Cell | int | str | Iterable[H3Cell | int | str],
    right: H3Cell | int | str | Iterable[H3Cell | int | str],
) -> CellArray[H3Cell]:
    """Hierarchy-aware difference of two H3 cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized.
    This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.

    Parameters
    ----------
    left, right : H3Cell, int, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of H3Cell

    Raises
    ------
    ParseError
        If an id or token is not a valid H3 cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cell = gm.h3_cover(p, resolution=7).cells[0]
    >>> nbr = list(cell.neighbors)[0]
    >>> len(gm.h3_difference([cell, nbr], [nbr]))
    1
    """

def h3_pentagons(resolution: int) -> CellArray[H3Cell]:
    """All pentagon cells at an H3 resolution (twelve per resolution).

    Parameters
    ----------
    resolution : int
        H3 resolution (``0``-``15``).

    Returns
    -------
    CellArray of H3Cell

    Raises
    ------
    GeometryError
        If ``resolution`` is out of range.

    Examples
    --------
    >>> import gometry as gm
    >>> len(gm.h3_pentagons(7))
    12
    """

def h3_base_cells() -> CellArray[H3Cell]:
    """Return the 122 resolution-0 H3 base cells.

    Returns
    -------
    CellArray of H3Cell

    Examples
    --------
    >>> import gometry as gm
    >>> len(gm.h3_base_cells())
    122
    """

@overload
def s2_cover(
    geom: GeometryArray,
    level: int | None = None,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
    target_cells: int = 8,
    min_level: int | None = None,
    max_level: int | None = None,
    level_mod: Literal[1, 2, 3] = 1,
) -> Groups[CellArray[S2Cell]]: ...
@overload
def s2_cover(
    geom: Geometry,
    level: int | None = None,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
    target_cells: int = 8,
    min_level: int | None = None,
    max_level: int | None = None,
    level_mod: Literal[1, 2, 3] = 1,
) -> S2Coverage:
    """Cover a geometry with S2 cells within a level budget.

    The result carries both ``cells`` — the S2 cells selected by
    ``cell_rule`` within the level budget — and exact membership
    predicates ``covers``/``contains``/``intersects``, which always answer
    against the source geometry, never the cells.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Geometry to cover (WGS84 lon/lat or projected). An array returns one
        grouped cell row per input geometry.

    level : int, optional
        S2 cell level (``0``-``30``; finer at higher values). Fixes both
        ``min_level`` and ``max_level``. Omit for an adaptive multi-level
        covering guided by ``target_cells``.

    cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize, strictest to loosest. ``'center'``:
        cells whose center is inside — unique assignment, balanced point
        binning. ``'within'``: only cells entirely inside — cells the area
        fully owns. ``'overlap'``: every cell touching the geometry — a
        complete-coverage superset, the safe default for candidate keys.
        ``'bbox'``: cells whose bounding box overlaps — loosest and fastest.
        The rule never affects the exact predicates.

    max_cells : int or None, default 1000000
        Hard cap on emitted cells when ``level`` fixes the cover depth. It is
        retained as metadata for adaptive covers, whose size is instead guided
        by ``target_cells``. Pass ``None`` for an unlimited fixed-level cover.

    target_cells : int, default 8
        S2-idiomatic approximation target for optional adaptive refinement
        when ``level`` is omitted. It does not affect fixed-level coverings.

    min_level, max_level : int, optional
        Coarsest/finest S2 levels allowed (default to ``level``).

    level_mod : int, default 1
        Restrict cells to levels a multiple of ``level_mod`` from
        ``min_level``.

    Returns
    -------
    S2Coverage or Groups of CellArray
        A scalar returns its coverage; an array returns one cell group per row.

    Raises
    ------
    GeometryError
        If the geometry, depth, or a coverage parameter is invalid, or if
        a fixed-level covering would exceed ``max_cells``.

    See Also
    --------
    s2_cells : Build S2 cells from lon/lat columns.

    Examples
    --------
    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cov = gm.s2_cover(p, level=12)
    >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
    (1, True, '8085809')
    """

def s2_cells(
    values: GeometryArray[Point] | FloatInput,
    lat: FloatInput | None = None,
    *,
    level: _IntInput,
) -> CellArray[S2Cell]:
    """Build S2 cells from parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, float, or sequence of float
        Point geometries or WGS84 longitudes. Projected point arrays are
        reprojected in one native batch.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Scalars
        broadcast numpy-style; at least one coordinate column must be sequence of float.

    level : int or sequence of int
        S2 cell level (0-30; finer at higher values). A scalar broadcasts to
        every row; an array supplies one level per row.

    Returns
    -------
    CellArray of S2Cell
        One cell per input coordinate.

    Raises
    ------
    GeometryError
        If ``level`` is out of range or every argument is scalar.
    InvalidGeometryError
        If a coordinate is non-finite or columns differ in length.

    See Also
    --------
    S2Cell : Build a single cell.
    s2_cover : Cover a geometry with S2 cells.

    Examples
    --------
    >>> import gometry as gm
    >>> len(gm.s2_cells([21.0, 22.0], [52.0, 53.0], level=10))
    2
    """

def s2_bounding_cell(value: Geometry | GeometryArray | Iterable[float]) -> S2Cell:
    """Return the deepest S2 cell that **provably** contains a geometry's lon/lat bounding box.

    Sibling-consistent with ``geohash_bounding_cell`` / ``tile_bounding_cell`` /
    ``h3_bounding_cell``: non-point inputs collapse to their lon/lat envelope,
    then the deepest cell that can be proven to cover that rectangle is returned.
    Near cell boundaries the result may be one level coarser than the theoretical
    deepest (always containing). A single point yields its exact level-30 leaf;
    a multipoint uses its bounding box (same path as any multi-vertex region —
    not a leaf-LCA of the vertices alone). Regions that span multiple cube faces
    have no single containing cell and raise.

    Parameters
    ----------
    value : Geometry, GeometryArray, or sequence of float
        A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
        ``(minx, miny, maxx, maxy)`` bounds.

    Returns
    -------
    S2Cell
        The deepest cell that provably contains the geometry's bounding box;
        near boundaries may be coarser than theoretical deepest (always
        containing).

    Raises
    ------
    InvalidGeometryError
        If the geometry is empty or coordinates leave the lon/lat domain.
    GeometryError
        If no single S2 cell contains the bounds, or bare bounds are not
        ordered min <= max.

    Examples
    --------
    >>> import gometry as gm
    >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    >>> gm.s2_bounding_cell(berlin).level
    8
    """

def s2_union(
    left: S2Cell | int | str | Iterable[S2Cell | int | str],
    right: S2Cell | int | str | Iterable[S2Cell | int | str],
) -> CellArray[S2Cell]:
    """Hierarchy-aware union of two S2 cell sets.

    Returns the normalized cell union: sorted, with contained cells absorbed
    by their ancestors and complete sibling groups merged into parents.


    Parameters
    ----------
    left, right : S2Cell, int, str, or iterable of those
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

    Returns
    -------
    CellArray of S2Cell

    Raises
    ------
    ParseError
        If a cell input is not valid for the S2 grid.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.s2_cover(p, level=12).cells)
    >>> len(gm.s2_union(cells, cells))
    1
    """

def s2_intersection(
    left: S2Cell | int | str | Iterable[S2Cell | int | str],
    right: S2Cell | int | str | Iterable[S2Cell | int | str],
) -> CellArray[S2Cell]:
    """Hierarchy-aware intersection of two S2 cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.


    Parameters
    ----------
    left, right : S2Cell, int, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of S2Cell

    Raises
    ------
    ParseError
        If an id or token is not a valid S2 cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.s2_cover(p, level=12).cells)
    >>> len(gm.s2_intersection(cells, cells))
    1
    """

def s2_difference(
    left: S2Cell | int | str | Iterable[S2Cell | int | str],
    right: S2Cell | int | str | Iterable[S2Cell | int | str],
) -> CellArray[S2Cell]:
    """Hierarchy-aware difference of two S2 cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized.


    Parameters
    ----------
    left, right : S2Cell, int, str, or iterable of those
        The two cell sets (any mix of cell objects, ids, or tokens).

    Returns
    -------
    CellArray of S2Cell

    Raises
    ------
    ParseError
        If an id or token is not a valid S2 cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = list(gm.s2_cover(p, level=12).cells)
    >>> len(gm.s2_difference(cells, []))
    1
    """
