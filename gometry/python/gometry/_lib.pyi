# The CRS geodesic/factors calculators broadcast: passing any sequence ordinate
# returns batched results. The scalar and batched overloads necessarily overlap
# on the all-scalar call (the scalar overload, listed first, wins), which is
# exactly what `reportOverlappingOverload` flags — disable it for this stub.
# pyright: reportOverlappingOverload=false
# The uninitialized ClassVar[None] declarations below need a mypy assignment
# ignore, but Pyright correctly sees no assignment error in a stub.
# pyright: reportUnnecessaryTypeIgnoreComment=false
# Runtime dunder declarations below are intentional protocol surface, even where
# Ruff's generic stub heuristic considers them redundant.
# Keep the builtin object class explicit: PYI055's combined form is rejected by mypy.
# ruff: noqa: PYI029, PYI055
# Native sequence methods keep their runtime-accurate signatures rather than
# widening them to match collections.abc.Sequence's protocol declarations.
# pyright: reportIncompatibleMethodOverride=false
# Mypy compares reflected signatures as infix fallback variants. PyO3 exposes
# each slot's reflected wrapper directly as well; a direct cross-family call can
# return NotImplemented while mixed infix dispatch succeeds through the other
# class's forward slot. The local `misc` ignores are scoped to that model mismatch.
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
    BinaryIO,
    ClassVar,
    Final,
    Generic,
    Literal,
    Never,
    Protocol,
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
    FloatScalar,
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
    ParseFormat,
    PolarsSeries,
    PolygonizeResult,
    PyArrowArray,
    PyArrowCapsule,
    PyArrowChunkedArray,
    PyArrowNativeFile,
    PyArrowRecordBatch,
    PyArrowRecordBatchReader,
    PyArrowSchemaCapsule,
    PyArrowTable,
    RepairMethod,
    ScalarFloatArray,
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
_GeometryRowT = TypeVar('_GeometryRowT', bound=Geometry | None)
_GeometryBatch: TypeAlias = GeometryArray[_GeometryRowT] | Iterable[_GeometryRowT]
# Dissolve group key element type (the `by` iterable's element type).
_GroupKeyT = TypeVar('_GroupKeyT', bound=Hashable)
_DefaultT = TypeVar('_DefaultT')
# Element type of a `GeometryArray`, including `None` when the array is
# nullable. Covariant so `GeometryArray[Point]` is a `GeometryArray[Geometry]`
# and a `GeometryArray[Point | None]`. Defaulted (PEP 696) so a bare
# `GeometryArray` means `GeometryArray[Geometry]` (non-nullable).
# `gm.points(...)` yields `GeometryArray[Point]`; `GeometryArray([pt, None])`
# yields `GeometryArray[Point | None]`.
_GeometryT_co = TypeVar(
    '_GeometryT_co', bound=Geometry | None, covariant=True, default=Geometry
)
# Flat payload type; source-row validity is represented by `_GroupRowT_co`.
_GroupValuesT_co = TypeVar(
    '_GroupValuesT_co',
    bound=np.ndarray[Any, Any] | GeometryArray[Any] | CellArray[Any],
    covariant=True,
)
# One row of a `Groups` sequence may be a flat payload or a missing source row.
_GroupRowT_co = TypeVar(
    '_GroupRowT_co',
    bound=np.ndarray[Any, Any] | GeometryArray[Any] | CellArray[Any] | None,
    covariant=True,
)
_GroupPayloadT = TypeVar(
    '_GroupPayloadT',
    bound=np.ndarray[Any, Any] | GeometryArray[Any] | CellArray[Any],
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
    'S2Cell',
    'SpatialIndex',
    'SpatialIndexIterator',
    'Tile',
    'TransformError',
    'ValidationReport',
    'area',
    'bearing',
    'box',
    'boxes',
    'contains',
    'contains_properly',
    'coverage_clean',
    'coverage_invalid_edges',
    'coverage_is_valid',
    'coverage_simplify',
    'coverage_union_all',
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
    'crs_configure',
    'crs_ellipsoids',
    'crs_engine',
    'crs_grid',
    'crs_prime_meridians',
    'crs_proj_operations',
    'crs_reset',
    'crs_roundtrip',
    'crs_transform',
    'crs_transform_bounds',
    'crs_unit',
    'crs_units',
    'crs_utm_zones',
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
    'pluscode_encode',
    'pluscode_polygon',
    'pluscode_recover',
    'pluscode_shorten',
    'point_between',
    'points',
    'polygonize_all',
    'polygons',
    'relate',
    'relate_pattern',
    'require',
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

#: Numeric scalar and fixed-rank NumPy lanes used by the public overloads.
_FloatScalarInput: TypeAlias = FloatScalar | ScalarFloatArray
_FloatArray1D: TypeAlias = np.ndarray[
    tuple[int],
    np.dtype[np.floating[Any] | np.signedinteger[Any] | np.unsignedinteger[Any]],
]
_IntScalar: TypeAlias = int | np.integer[Any]
_IntArray0D: TypeAlias = np.ndarray[
    tuple[()], np.dtype[np.signedinteger[Any] | np.unsignedinteger[Any]]
]
_IntScalarInput: TypeAlias = _IntScalar | _IntArray0D
_IntArray1D: TypeAlias = np.ndarray[
    tuple[int], np.dtype[np.signedinteger[Any] | np.unsignedinteger[Any]]
]
_IntArrayUnknownRank: TypeAlias = np.ndarray[
    tuple[Any, ...],
    np.dtype[np.signedinteger[Any] | np.unsignedinteger[Any]],
]
_BoolArray1D: TypeAlias = np.ndarray[tuple[int], np.dtype[np.bool_]]
_FloatColumn1D: TypeAlias = FloatColumn | _FloatArray1D
_IntInput: TypeAlias = _IntScalar | _IntArray0D | _IntArray1D | Iterable[int] | Buffer
_IndexLane: TypeAlias = Iterable[int] | _IntArray1D | Buffer
_BoolLane: TypeAlias = Iterable[bool]
_IdArray: TypeAlias = _IntArray0D | _IntArray1D | Buffer
_Coordinate: TypeAlias = Iterable[float]
_CoordinateMatrix: TypeAlias = (
    Sequence[Sequence[float]]
    | np.ndarray[
        tuple[int, int],
        np.dtype[
            np.floating[Any]
            | np.signedinteger[Any]
            | np.unsignedinteger[Any]
            | np.bool_
        ],
    ]
)
_H3SetInput: TypeAlias = (
    H3Cell | _IntScalarInput | str | Iterable[H3Cell | _IntScalarInput | str | None]
)
_S2SetInput: TypeAlias = (
    S2Cell | _IntScalarInput | str | Iterable[S2Cell | _IntScalarInput | str | None]
)
_GeohashSetInput: TypeAlias = GeohashCell | str | Iterable[GeohashCell | str | None]
_TileSetInput: TypeAlias = (
    Tile | _IntScalarInput | str | Iterable[Tile | _IntScalarInput | str | None]
)
_AffineMatrix: TypeAlias = (
    tuple[float, float, float, float, float, float] | Iterable[float]
)
_Bounds2D: TypeAlias = tuple[float, float, float, float]
_Bounds3D: TypeAlias = tuple[float, float, float, float, float, float]

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
    | PyArrowRecordBatchReader
)

class AccuracyWarning(UserWarning):
    """A CRS transform used a lower-accuracy fallback because a required grid is unavailable."""

class GeometryError(ValueError):
    """Base class for every error gometry raises about your data or parameters."""

    category: Literal['layout', 'metadata'] | None
    """GeoParquet admission category, or ``None`` for other geometry errors."""
    code: str | None
    """Stable machine-readable reason code when the error has one (else ``None``)."""
    param: str | None
    """The offending parameter's name, for value-lane errors (else ``None``)."""
    value: float | None
    """The offending numeric value, for value-lane errors (else ``None``)."""
    operation: str | None
    """The operation associated with a bounded-output or certified-embedding error (else ``None``)."""
    parameter: str | None
    """The parameter that controls bounded output (else ``None``)."""
    produced: int | None
    """The number of produced items for a bounded-output error (else ``None``)."""
    limit: int | None
    """The configured output limit, for bounded-output errors (else ``None``)."""

class InvalidGeometryError(GeometryError):
    """A geometry violates a structural or numeric rule."""

    operation: str | None
    """The operation for an empty-sequence or invalid-coverage error (else ``None``)."""
    row: int | None
    """The failing array row, for row-wise validation (else ``None``)."""
    path: str | None
    """Structural path to the first validity issue (else ``None``)."""
    code: str | None
    """Stable machine-readable validity code (else ``None``)."""
    detail: str | None
    """Human-readable detail for the validity issue (else ``None``)."""
    location: tuple[float, float] | None
    """Coordinate witness for the validity issue (else ``None``).

    For quotient-topology issues, the location is a canonical coordinate of the
    issue's contact set, chosen deterministically by minimum latitude then longitude.
    """

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
    """Serialized, grid-cell, or point-code input is malformed."""

    format: ParseFormat | None
    """Which codec rejected the input."""
    position: int | None
    """Zero-based UTF-8 byte offset where WKT/EWKT parsing detected the error; WKB reports a binary byte offset; otherwise None when unavailable."""
    path: str | None
    """JSONPath-like location of the malformed value, when available."""

@final
class CoordinatesIterator:
    """Lazy iterator over a ``Coordinates`` view. A traversal stack retains the
    current shape or packed row and yields tuples without materializing all
    points up front.
    """
    def __length_hint__(self) -> int:
        """Advisory remaining count, capped at Python's largest sequence length."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support for the iterator and its retained view.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
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
    a flattened vertex stream, not a row-aligned container; use `.row_index`
    when you need source-row indexes,
    or call `drop_missing()` first for an explicit dense-only path.
    """

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
            other floating dtypes, including opposite-endian ``float64``, are
            cast with ``astype``).
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
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        Rust-side coordinate heap retained by this view (ordinate payload and
        structural row metadata for array-backed views). Shared backing buffers
        are estimated from the visible view, not the full parent allocation.
        Parent geometry/array sidecars (prepared caches, coverage membership,
        …) are not owned by the view and are not counted here.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """
    def __repr__(self) -> str:
        """Return the bounded representation of this coordinate view."""
    def __reduce__(self) -> tuple[Callable[..., Coordinates], tuple[Any, ...]]:
        """Restore a Coordinates view from its owning geometry or array and
        selected layout.
        """
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

        Returns
        -------
        CoordinateAxes
            The exact ordinate layout visible through this coordinate view.
        """
    def select(self, axes: CoordinateAxes) -> Coordinates:
        """Return a view of the same coordinates in a fixed ``coordinate_axes`` layout
        (``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'``). This layout defines the schema
        of coordinate tuples, nested output, matrices, dictionaries, and equality.
        Tuple/nested output uses ``None`` where a coordinate lacks the requested
        Z/M; matrices and dictionary columns use ``NaN`` there. Independent axis
        accessors such as ``.z`` continue to read the source view, regardless of
        the selected layout. Selection does not change the geometry.

        Parameters
        ----------
        axes : {'XY', 'XYZ', 'XYM', 'XYZM'}
            The layout every tuple and column should take.

        Returns
        -------
        Coordinates
            The same coordinates with the selected view layout.

        Examples
        --------
        >>> import gometry as gm
        >>> coords = gm.LineString([(0, 0), (1, 1)]).coords
        >>> coords.select('XYZ').to_nested()
        [(0.0, 0.0, None), (1.0, 1.0, None)]
        """
    @property
    def row_index(self) -> np.ndarray[tuple[int], np.dtype[np.int64]]:
        """Per-coordinate source geometry row (all ``0`` for a scalar geometry);
        the source row for each vertex of a ``GeometryArray``.

        Returns
        -------
        numpy.ndarray
            A materialized, read-only int64 array in flat coordinate order, one
            source-row label per vertex.
        """
    @property
    def x(self) -> npt.NDArray[np.float64]:
        """The X ordinates as a read-only ``float64`` ``numpy.ndarray``.

        Returns
        -------
        numpy.ndarray
            One read-only value per vertex in view order, in the geometry's
            native coordinate units (raw coordinate units when CRS-free). The
            array shares the immutable coordinate column when one is available.
        """
    @property
    def y(self) -> npt.NDArray[np.float64]:
        """The Y ordinates as a read-only ``float64`` ``numpy.ndarray``.

        Returns
        -------
        numpy.ndarray
            One read-only value per vertex in view order, in the geometry's
            native coordinate units (raw coordinate units when CRS-free). The
            array shares the immutable coordinate column when one is available.
        """
    @property
    def z(self) -> npt.NDArray[np.float64]:
        """The Z ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
        length (``NaN`` when no coordinate carries Z).

        Returns
        -------
        numpy.ndarray
            One read-only value per vertex in view order; missing Z ordinates
            are ``NaN``. Values remain in the geometry's native coordinate
            units; absent columns are materialized.
        """
    @property
    def m(self) -> npt.NDArray[np.float64]:
        """The M ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
        length (``NaN`` when no coordinate carries M).

        Returns
        -------
        numpy.ndarray
            One read-only value per vertex in view order; missing M ordinates
            are ``NaN``. M is a measure ordinate, not a CRS unit; absent columns
            are materialized.
        """
    def to_nested(self) -> NestedCoordinates:
        """Return the coordinates as a nested structure mirroring the geometry's topology
        (point → ``[x, y]``, line → list of tuples, polygon → list of rings;
        arrays → one entry per present geometry) — the ``__geo_interface__``-style
        nesting, as Python lists. A selected view layout determines each
        coordinate's ordinates; absent selected Z/M values are ``None``. Missing
        array rows are skipped. The flat columns do not preserve this shape.

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
    ) -> dict[
        str, npt.NDArray[np.float64] | np.ndarray[tuple[int], np.dtype[np.int64]]
    ]:
        """Return a dependency-free column dict — ``{'x': ndarray, 'y': ndarray, …}`` — ready
        for ``pandas``/``polars`` (``pd.DataFrame(coords.to_dict())``).
        ``z``/``m`` columns appear when carried by the view or selected layout,
        with ``NaN`` for rows that lack an ordinate; ``index=True`` adds the
        source-geometry row column.

        Parameters
        ----------
        index : bool
            Add an ``'index'`` column with each coordinate's source row when
            ``True``; the ``False`` form omits that column.

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
    (``contains(geom, other)``), measure it (``geom.area`` — ellipsoidal square
    meters of the longitude/latitude chart region on a geographic CRS), and
    derive from it (``geom.buffer(10.0)``); every
    operation returns a new geometry. Instances are one of the typed subclasses
    (``Point``, ``LineString``, ``Polygon``, ...), so ``isinstance`` narrows.
    """
    def __new__(cls, _nonconstructible: Never, /) -> Self: ...
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle support: round-trip through plain WKB (Z/M preserved) plus the
        frame (stored CRS definition and epoch stored separately). Enables ``pickle``, ``copy``, and
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
    def __and__(self, other: Point, /) -> Point: ...
    @overload
    def __and__(self, other: MultiPoint, /) -> MultiPoint | Point: ...
    @overload
    def __and__(self, other: GeometryArray[Point], /) -> GeometryArray[Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[MultiPoint], /
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[Point | None], /
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(
        self, other: GeometryArray[MultiPoint | None], /
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __and__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __and__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __and__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __and__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self&value."""
    @overload
    def __rand__(self, other: Point, /) -> Point: ...  # type: ignore[misc]
    @overload
    def __rand__(self, other: MultiPoint, /) -> MultiPoint | Point: ...  # type: ignore[misc]
    @overload
    def __rand__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rand__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value&self."""
    @overload
    def __or__(self, other: Point | MultiPoint, /) -> Geometry: ...
    @overload
    def __or__(
        self, other: GeometryArray[Point | MultiPoint], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __or__(
        self, other: GeometryArray[Point | MultiPoint | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __or__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __or__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __or__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __or__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self|value."""
    @overload
    def __ror__(self, other: Point | MultiPoint, /) -> Geometry: ...  # type: ignore[misc]
    @overload
    def __ror__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __ror__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __ror__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value|self."""
    @overload
    def __sub__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __sub__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __sub__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __sub__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self-value."""
    @overload
    def __rsub__(self, other: Point, /) -> Point: ...  # type: ignore[misc]
    @overload
    def __rsub__(self, other: MultiPoint, /) -> MultiPoint | Point: ...  # type: ignore[misc]
    @overload
    def __rsub__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rsub__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value-self."""
    @overload
    def __xor__(self, other: Point | MultiPoint, /) -> Geometry: ...
    @overload
    def __xor__(
        self, other: GeometryArray[Point | MultiPoint], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __xor__(
        self, other: GeometryArray[Point | MultiPoint | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __xor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __xor__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __xor__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __xor__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self^value."""
    @overload
    def __rxor__(self, other: Point | MultiPoint, /) -> Geometry: ...  # type: ignore[misc]
    @overload
    def __rxor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __rxor__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rxor__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value^self."""
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
    def __repr__(self) -> str:
        """Return the bounded angle-bracket geometry representation."""
    def __str__(self) -> str:
        """Return the geometry's WKT representation."""
    def __sizeof__(self) -> int:
        """Python's allocation base plus the retained native cost of this geometry
        for ``sys.getsizeof``.

        Counts the Python object allocation base and the Arc-owned ``ShapeData`` block
        (including the ``Shape`` payload — coordinate columns **and**
        container allocations such as multipart ``Vec``s, polygon hole
        ``Arc``s, and nested collection members), any attached
        *already-initialized* ``PreparedPlan`` products, and the Arc-owned
        frame-cache sidecar with any products already built on it. Spilled CRS
        frame text is included; inline CRS text is already part of the struct.
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

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
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
        Geographic bounds use periodic longitude; ``minx > maxx`` denotes an
        interval crossing that frame's seam. Empty rows in a `GeometryArray`
        are all-``nan`` instead — see `GeometryArray.bounds`.

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
        including a typed empty; the member count for a multi/collection, and
        ``0`` for an empty multipart or collection. Explicit empty members
        still count in their multipart's member count.

        Returns
        -------
        int
        """
    @property
    def parts(self) -> GeometryParts[Geometry]:
        """Lazy view over this geometry's top-level parts.

        Simple geometries expose themselves as a one-row view; multipart and
        collection geometries expose their members. `GeometryArray(geom.parts)`
        materializes them as an array.

        Returns
        -------
        GeometryParts
        """
    def estimate_local_crs(self) -> CRS:
        """Estimate a conformal metric CRS for this geometry.

        Every local frame retains the source geodetic datum. A datum-matched
        UTM or UPS CRS is preferred when it meets the fixed 0.1% linear scale-
        error ceiling; otherwise a transverse-Mercator frame centered on the
        geometry is built on the source datum.
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
        Geographic closure uses quotient vertex identity and simplicity uses
        the frame-aware topology; projected and CRS-free geometry remains planar.

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
        Geographic simplicity uses the CRS longitude quotient, so periodic
        aliases and seam cuts do not depend on their stored spelling. Geographic
        coordinate-admission failures still raise; projected and CRS-free
        geometry remains planar.

        Returns
        -------
        bool
        """
    @property
    def is_valid(self) -> bool:
        """Whether the geometry is topologically valid in its coordinate frame.
        Geographic validity uses exact quotient topology, so period aliases and
        seam cuts do not depend on their stored longitude spelling. An
        out-of-domain latitude returns ``False`` and is reported by ``validate``;
        geographic operations that require coordinate admission still raise.
        Projected and CRS-free geometry uses ordinary planar OGC validity.
        A nonempty LineString needs at least two distinct XY positions;
        consecutive repeats remain valid when another position is present.
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

        Coordinates are interpreted as WGS84 longitude/latitude, including
        when the geometry is CRS-free; out-of-domain coordinates raise and
        antimeridian crossings are split according to RFC 7946.
        Z ordinates are included when present. M ordinates and coordinate epochs
        are not representable by GeoJSON and therefore raise. Use an explicit
        export operation with its metadata-dropping option when loss is intended.

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

        A geographic CRS gives ellipsoidal square meters for the region bounded
        by edges straight in longitude/latitude;
        a projected CRS gives squared native linear units; a CRS-free geometry
        gives squared raw coordinate units. Use ``to_crs`` to
        change frame. Geographic polygons measure their longitude/latitude chart
        interior, including regions larger than half the ellipsoid.

        A ``GeometryCollection`` counts overlapping members once, as the area
        of their union. Parts of a valid ``MultiPolygon`` cannot overlap.

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

        A geographic CRS gives ellipsoidal meters along edges straight in
        longitude/latitude; a projected CRS gives native linear units; a CRS-free
        geometry gives raw coordinate units. Use ``to_crs`` to change frame.

        Geographic polygon perimeter measures straight chart edges and resolves
        seam aliases in the CRS-native periodic longitude frame.

        A ``GeometryCollection`` sums every member's length or perimeter,
        including overlapping members.

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

        Geographic CRSs do not support 3D Euclidean length.

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
    def with_coordinates(
        self, coordinates: _CoordinateMatrix | Coordinates, /
    ) -> Self: ...
    @overload
    def with_coordinates(
        self,
        coordinates: None = None,
        /,
        *,
        x: _FloatScalarInput | FloatColumn = ...,
        y: _FloatScalarInput | FloatColumn = ...,
        z: _FloatScalarInput | FloatColumn | None = ...,
        m: _FloatScalarInput | FloatColumn | None = ...,
    ) -> Self:
        """Return a geometry with selected coordinate ordinates replaced.

        Pass one ``(N, dims)`` matrix (including a `Coordinates` view), or
        replace any of ``x=``, ``y=``, ``z=``, and ``m=`` by a scalar or a
        column over the flattened coordinate sequence. Omitted ordinates are
        preserved. ``z=None`` and ``m=None`` remove those dimensions; X and Y
        cannot be removed. Matrix and column modes cannot be mixed. Calling
        without arguments returns an unchanged value.

        Parameters
        ----------
        coordinates : sequence of float, optional
            Replacement ``(N, dims)`` coordinate matrix, including a
            `Coordinates` view.
        x, y, z, m : float, sequence of float, or None, optional
            Replace one ordinate by a scalar broadcast over vertices or a
            column with one value per coordinate. Omitted ordinates are
            preserved. ``None`` removes Z or M and is not valid for X or Y.

        Returns
        -------
        Geometry

        Examples
        --------
        >>> import gometry as gm
        >>> gm.LineString([(0, 0), (1, 1)]).with_coordinates([(5, 5), (6, 6)]).to_wkt()
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
    def __replace__(
        self, *, crs: CrsInput | None = ..., epoch: float | None = ...
    ) -> Self:
        """Return a copy with the given CRS/epoch metadata replaced.

        Supports ``copy.replace`` on Python 3.13+; omitted keyword arguments
        keep the current value. ``crs=None`` / ``epoch=None`` clear metadata.

        Parameters
        ----------
        crs : CRS-like or None, optional
            Replace or clear the CRS label.

        epoch : float or None, optional
            Replace or clear the coordinate epoch.

        Returns
        -------
        Geometry
        """
    def with_epoch(self, epoch: float | None, *, overwrite: bool = False) -> Self:
        """Declare (or clear) the coordinate epoch without moving coordinates.
        The epoch is the decimal year the coordinate set was observed
        (e.g. ``2020.0``). It supplies metadata to operations that use
        coordinate epochs; the CRS frame must admit an epoch.
        ``with_epoch(None)`` clears it. Changing a present epoch to a different
        value requires ``overwrite=True`` (the silent-frame-change guard, like
        ``with_crs``).

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
            If the CRS frame cannot carry an epoch or a present epoch would
            change without ``overwrite=True``.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.Point(-122.4, 37.8, crs=4326).with_epoch(2020.0).epoch
        2020.0
        """

    def with_crs(self, crs: CrsInput | None, *, overwrite: bool = False) -> Self:
        """Attach or relabel the CRS *without* moving coordinates.
        Declares what the coordinates already mean; to actually reproject them
        use `to_crs`. Attaching to a CRS-free geometry, clearing with
        ``None``, and identical re-tags are free; replacing one declared CRS
        with a *different* one requires ``overwrite=True`` (it is almost always
        a reprojection mistake).

        Parameters
        ----------
        crs : CRS-like
            Target CRS-like input, or ``None`` to clear.

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
        >>> gm.Point(1, 2).with_crs(4326).crs
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
        assign it with ``with_epoch`` when the coordinate operation needs one.
        ``epoch`` here labels the *output* coordinate epoch.

        Parameters
        ----------
        crs : CRS-like
            Target CRS-like input.

        area_of_interest : sequence of float, optional
            Bounding ``(west, south, east, north)`` to pick the best transform.

        epoch : float, optional
            Output coordinate epoch (decimal year) to tag on the result. Omitted
            keeps the source epoch when the CRS is unchanged or the target admits
            geometry epochs; otherwise the output has no epoch.

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
        with_crs : Declare/relabel the CRS *without* moving coordinates.

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
        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

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
            If ``precision`` is out of range, or the geometry carries a
            coordinate epoch and ``drop_epoch`` is false.

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
        start_m: _FloatScalarInput,
        end_m: _FloatScalarInput,
        *,
        overwrite: bool = False,
        unit: DistanceUnit | None = None,
    ) -> Self:
        """Interpolate an M ordinate along the line's arc length (CRS-aware). M runs
        from ``start_m`` at the start to ``end_m`` at the end, continuously across
        multipart linework (the PostGIS ``ST_AddMeasure`` shape). The stationing
        uses ellipsoidal arclength along straight source-chart edges on geographic CRSs
        and planar distance otherwise; ``unit='planar'`` always uses Cartesian
        coordinate space. Coordinates are never moved. Geographic length, perimeter,
        and LRS use the same chart-edge model with CRS-aware units. Z is preserved;
        existing M requires ``overwrite=True``. Every stored vertex is retained; a
        zero-length X/Y segment has equal M values at both ends. Empty linework returns
        the same empty with an M axis.

        Parameters
        ----------
        start_m, end_m : float
            The measure range (finite, ``end_m >= start_m``).
        overwrite : bool, default False
            Replace existing M ordinates instead of raising.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        Geometry
            The geometry with interpolated M values (same kind as the input).


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework carries M without ``overwrite``.
        GeometryError
            If the measure range is invalid.

        See Also
        --------
        line_interpolate : Point at a distance or M location along the line.
        line_substring : Extract a contiguous portion of the line.
        line_locate : Locate the nearest route position on the line.

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
        tolerance: _FloatScalarInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> Point:
        """Pole of inaccessibility: an approximate most-distant interior point
        for label placement. Uses a tolerance-based cell search; its clearance is within the requested tolerance
        of the global inradius for planar/projected input.
        Geographic line and ring boundaries may be refined in the local projection
        before the search; ``tolerance`` controls the search, not the separate
        input-refinement budget.
        For a nonempty polygon with no filled interior, returns the midpoint of its
        stored bounds with radius zero, including when its rings cancel the entire
        support.
        Empty geometries return ``POINT EMPTY``.

        See Also
        --------
        maximum_inscribed_circle : Certified filled disk from its own center search.
        centroid : Area/length-weighted center (may fall outside).
        point_on_surface : A representative point guaranteed on the geometry.

        Parameters
        ----------
        tolerance : float, optional
            Positive absolute bound on the planar global-inradius gap, interpreted
            for the CRS (see ``unit``).
            Omitted selects a scale-aware tolerance from the geometry's extent.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        Point
            The pole of inaccessibility.


        Raises
        ------
        GeometryError
            If an explicit ``tolerance`` is not positive and finite.
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
        search. Original vertices remain clearance sites; inserted chart-edge support
        samples do not become new sites, and an on-edge witness is interpolated on the
        original source segment.
        ``LINESTRING EMPTY`` when the clearance is infinite (fewer than two distinct
        vertices).

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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

    def to_geojson(self, *, drop_epoch: bool = False) -> str:
        """Serialize to `GeoJSON` text. `GeoJSON` is WGS84 by specification (RFC 7946):
        CRS-tagged input must use the 2D longitude/latitude family
        (``EPSG:4326`` or ``OGC:CRS84``) or the 3D longitude/latitude/height family
        (``EPSG:4979`` or ``OGC:CRS84h``); reproject other CRS values first. CRS-free
        input is explicitly assumed to already be WGS84 longitude/latitude:
        coordinates outside longitude ``[-180, 180]`` or latitude ``[-90, 90]`` are
        rejected, and antimeridian-crossing geometry is split according to RFC 7946.
        If CRS-free coordinates have projected or other planar semantics, transform
        them or choose another format.

        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

        GeoJSON cannot represent M; drop it with ``with_coordinates(m=None)`` (or ``force_2d()``).

        Parameters
        ----------
        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which GeoJSON cannot encode.

        Returns
        -------
        str
            The `GeoJSON` geometry string.

        Raises
        ------
        CRSError
            If the input carries a CRS outside the 2D/3D WGS84 longitude/latitude family.
        InvalidGeometryError
            If input carries M ordinates; drop them with ``with_coordinates(m=None)`` (or ``force_2d()``).
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
        """Return the topological boundary of the geometry's occupied support.

        Returns
        -------
        MultiPoint, LineString, MultiLineString, or GeometryCollection
            Boundary-location cells, one dimension below the effective dimension of
            the occupied support. A Polygon or MultiPolygon is evaluated as one union
            of polygonal support, so internal edges, collapsed interior spans, and
            ring edges outside that support are omitted. A GeometryCollection is
            processed member by member; non-empty member occurrences are retained.

        Notes
        -----
        In a geographic CRS, seam aliases and all longitudes at a pole identify the
        same vertex. Edges introduced only by a chart cut or pole closure are not
        boundaries, although a seam vertex may appear where the geographic line
        emitter splits a run. The CRS and coordinate epoch are preserved; coordinates
        are not transformed. Source Z/M values are kept where emitted vertices have
        unambiguous ordinates; an axis is dropped when an emitted vertex has no value.
        Typed empty inputs keep their dimension-appropriate empty boundary.

        Raises
        ------
        CRSError
            If the declared CRS cannot be resolved to a topology frame.
        InvalidGeometryError
            If a geographic coordinate read by boundary is outside its CRS domain.
        GeometryError
            If exact work exceeds the generated-item budget or emitted cells cannot
            be certified.

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
        min_angle: _FloatScalarInput | None = None,
        max_area: _FloatScalarInput | None = None,
    ) -> GeometryArray[Polygon]:
        """Triangulate geometry with an explicit algorithm.

        Parameters
        ----------
        method : {'earcut', 'delaunay', 'constrained'}
            Required algorithm choice: ``earcut`` triangulates polygon interiors,
            ``delaunay`` triangulates input vertices, and ``constrained`` preserves
            polygon boundaries. Delaunay methods resolve exact cocircular ties with
            a canonical rule keyed by lexicographic XY coordinates, independent of
            insertion order.
        min_angle : float, optional
            Minimum triangle angle in degrees; positive finite values up to 30 are
            valid with ``method='constrained'``.

        max_area : float, optional
            Positive finite maximum triangle area in coordinate space. Geographic
            CRSs use squared native angular units, projected CRSs use squared native
            linear units, and CRS-free inputs use squared raw coordinate units. Valid
            with ``method='constrained'``. Setting either option enables
            refinement;
            without either option, duplicate XY sites use their first source
            occurrence and triangle corners preserve the selected source ordinates.
            Refinement inserts Steiner vertices and therefore returns uniform XY.
            Refinement processes the worst triangle first: angle violations by
            circumradius-to-shortest-edge ratio, then by area when ``max_area`` is
            also set; triangles meeting the angle target follow in descending area.
            Equal priorities use sorted numeric XY corners. Refinement is independent
            of insertion order and input ring rotation or reversal.
            Each boundary Steiner vertex is the nearest binary64 coordinate pair to
            its exact position on the source edge, so the triangles cover the source
            polygon up to that rounding.

        Returns
        -------
        GeometryArray[Polygon]
            The generated triangles. Delaunay methods return counter-clockwise
            triangles starting at their smallest XY corner, sorted by corner coordinates.

        Raises
        ------
        GeometryError
            If a quality constraint is invalid or method-specific options are used
            with the wrong algorithm. A boundary Steiner point that cannot be
            represented at float resolution raises this error with
            ``operation='triangulate'``.
        InvalidGeometryError
            If the input polygon is invalid or the triangulation cannot be constructed.

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
        iterations: _IntScalar = 2,
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
        minx: _FloatScalarInput,
        miny: _FloatScalarInput,
        maxx: _FloatScalarInput,
        maxy: _FloatScalarInput,
    ) -> Geometry:
        """Return the intersection with the closed rectangle
        ``(minx, miny, maxx, maxy)``. Its WKB is identical to
        ``intersection(self, box(minx, miny, maxx, maxy))`` when the box carries the
        same CRS and coordinate epoch. Point-only and edge-only
        contacts are retained; equal ``minx``/``maxx`` or ``miny``/``maxy`` select a
        closed line or point.

        When the closed rectangle contains the subject's raw bounds, the subject is
        returned verbatim for Cartesian inputs and canonical seam-free geographic bounds.

        Source ordinates are carried where meaningful; synthesized clip vertices use
        the operation's natural XY result.

        In a geographic CRS, ``minx``/``maxx`` are the west/east longitudes and must be
        within the CRS frame's native half-period. ``west <= east`` selects the ordinary
        closed interval; ``west > east`` selects the closed interval crossing the
        longitude seam. The endpoints ``-half_period`` and ``half_period`` select a full
        longitude band, and equal endpoints select a zero-width meridian. ``miny`` and
        ``maxy`` are the south/north latitudes and must lie within the frame's poles,
        with ``south <= north``. To construct a box for that interval, use :func:`box`
        with ``wrap='split'`` and the same CRS.

        Geographic inputs use frame-aware overlay so longitude aliases, seam crossings,
        and pole vertices follow the CRS topology. Geographic rectangle horizontal
        edges follow parallels and use the same construction as :func:`box` when that
        box uses the same CRS and coordinate epoch.

        Parameters
        ----------
        minx, miny, maxx, maxy : float
            The Cartesian bounds, or the geographic west, south, east, and north
            endpoints respectively.

        Returns
        -------
        Geometry
            The clipped geometry (kind may change; empty when fully outside).


        Raises
        ------
        GeometryError
            If the rectangle coordinates are non-finite, south exceeds north, or a
            geographic endpoint is outside its CRS frame, or no adjacent binary64
            assignment preserves the exact clipped result or the bounded embedding
            search is exhausted.
        InvalidGeometryError
            If the input contains coordinates outside its geographic CRS domain.

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
        start: _FloatScalarInput,
        end: _FloatScalarInput,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> LineString | Point: ...
    @overload
    def line_substring(
        self,
        start: _FloatScalarInput,
        end: _FloatScalarInput,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> LineString | Point:
        """Return the portion of linework from ``start`` through ``end``. Empty
        linework yields ``LINESTRING EMPTY``.

        Both bounds are inclusive. Stored vertices at zero-length X/Y segments consume
        no distance but remain in the result when their active ordinates differ,
        including consecutive vertices with identical X/Y. This applies to distance
        and M basis.

        With CRS-aware units, geographic cut points are inverted by ellipsoidal
        arclength along straight source-chart edges. New endpoints embedded in a
        retained source edge keep that edge's continuous longitude spelling.
        ``unit='planar'`` instead interpolates in the stored coordinate space.

        Parameters
        ----------
        start, end : float or sequence of float
            Ordered locations on the selected basis. Distance values follow the CRS;
            M values are stored route measures. A scalar applies to every array row.

        basis : {'distance', 'm'}
            Use CRS-aware distance, or the line's monotonic M ordinate. Distance forms
            omit ``basis``; M stationing requires ``basis='m'``. Its M fraction selects
            the same arclength fraction on each edge: ellipsoidal chart-edge arclength
            on a geographic CRS, planar length otherwise.

        normalized : bool, default False
            Interpret distance-basis positions as fractions of the route length under
            the selected metric. Changing ``unit`` can change the cut points. Invalid
            with ``basis='m'``.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            Available only with ``basis='distance'``; invalid with
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
            If M values are missing or non-monotonic; an array row degrades to an
            empty result instead.
        GeometryError
            If locations are non-finite or out of order, or a distance-only option is
            used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a location along the line.
        line_locate : Locate the nearest route position on the line.
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
        distance: _FloatScalarInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: _IntScalar = 8,
        miter_limit: _FloatScalarInput = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> Polygon | MultiPolygon:
        """Buffer a geometry by ``distance``, returning the offset region (measured for
        the CRS).

        On a geographic CRS, stored source-chart edges are refined when their projected
        curves cannot be certified within the local-frame input budget before the planar
        kernel runs. This input-fidelity budget is separate from the 0.1% frame-fit
        limit and from round-arc resolution, which ``quadrant_segments`` controls; it
        does not promise an end-to-end ellipsoidal distance error.
        For single-sided buffers, chord directions determine endpoint normals, so the
        input-fidelity budget does not bound output endpoint displacement.

        Parameters
        ----------
        distance : float
            Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
            meters on a geographic CRS, native linear units on a projected CRS, raw
            coordinate units when CRS-free. Must be finite.
        cap_style : {'round', 'flat', 'square'}, default 'round'
            End-cap shape at the original first and last vertices of open lines. Closed lines have no end caps.
            Point inputs always buffer to round discs, and MultiPoint buffers are
            unions of round discs; `cap_style` does not affect either. Sided buffers
            always use flat ends, so this parameter applies to ``side='both'`` only.

        join_style : {'round', 'miter', 'bevel'}, default 'round'
            Interior corner style for both two-sided and sided line buffers, including
            180-degree reversals.

        quadrant_segments : int, default 8
            Segments used to approximate a quarter-circle join in either buffer mode.

        miter_limit : float, default 5.0
            With ``join_style='miter'``: how far a mitered corner may reach, in
            multiples of ``distance``; sharper corners are clipped flat at that reach.
            Applies to both two-sided and sided buffers; must be positive and finite.

        side : {'both', 'left', 'right'}, default 'both'
            Which side(s) of lineal input to buffer. ``'left'``/``'right'`` build
            the one-sided strip between the line and its offset curve, with flat ends.
            These sides honor the join parameters: ``join_style``,
            ``quadrant_segments``, and ``miter_limit``. The distance must be
            non-negative.

            Buffer boundaries are synthesized and therefore returned in XY; Z/M are
            not fabricated.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        Polygon or MultiPolygon
            The offset region.


        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite or buffer arithmetic produces a non-finite
            derived coordinate.
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
        >>> round(disc.area)  # ~ pi * 10²
        312
        """

    def unique_points(self) -> MultiPoint | GeometryCollection:
        """Distinct vertices in first-occurrence order. Vertices compare by exact
        structural identity (every active ordinate by bit pattern, the
        ``equals_identical`` notion), so XYZ points that differ only in Z stay
        distinct.

        Returns
        -------
        MultiPoint or GeometryCollection
            The distinct vertices. Homogeneous surviving vertices use a ``MultiPoint``;
            when surviving vertices carry different axis layouts, a ``GeometryCollection``
            of ordered ``Point`` members preserves each layout without promotion or loss.
            An empty input returns a typed ``MULTIPOINT EMPTY``.

        Notes
        -----
        A heterogeneous result follows ordinary ``GeometryCollection`` semantics:
        ``is_simple`` is false by convention and structural equality remains type-sensitive.
        Mixed-axis and collection exports use WKB for Arrow interchange; see
        [Arrow & storage](../../ecosystem/arrow.md).

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
        tolerance: _FloatScalarInput,
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
        - ``'dp'`` (Douglas-Peucker) compares exact distances to the closed retained
          chord against ``tolerance``. Equal maximum distances select the earliest
          source vertex.

        Parameters
        ----------
        tolerance : float
            Distance scale of removable detail in coordinate space: native angular
            units for geographic CRSs, native linear units for projected CRSs, and
            raw coordinate units when CRS-free.
        method : {'vw', 'dp'}, default 'vw'
            ``'vw'`` is area-based (Visvalingam-Whyatt); ``'dp'`` is distance-
            based (Douglas-Peucker).

        preserve_topology : bool, default True
            Preserve topology within each geometry checked: valid polygonal components
            stay valid and non-collapsed, and simple lineal components stay simple. A
            ``GeometryCollection`` is checked child by child; relationships between
            separate children are not preserved. The raw algorithm runs first (the
            fast path); a guarded pass only runs when a component fails those checks.
            ``False`` skips the guarded check; collapsed areal rings are still removed.

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
        input is returned unchanged at validation cost. Geographic repair checks exact
        quotient validity before rebuilding; when output construction needs seam
        vertices it splits the source while preserving traceable Z/M ordinates.
        Projected and CRS-free geometry remains planar. A collapsed LineString becomes
        a Point at its first coordinate. Collapsed MultiLineString members become a
        Point or MultiPoint, combined with surviving linework in a GeometryCollection
        when both dimensions remain.

        Parameters
        ----------
        method : {'linework', 'structure'}, default linework
            Repair strategy: ``linework`` nodes all boundary linework and selects
            regions from each original ring occurrence's winding modulo two before
            recombining each polygon's shell minus its holes; distinct polygon members
            are unioned. ``structure`` rebuilds each ring's nonzero-winding enclosed
            area and recombines it as shells minus holes.

            Z/M are carried at vertices traceable to the input; a rebuild that needs
            unsourceable vertices returns the mathematically natural XY result.

        Returns
        -------
        Geometry
            A valid geometry.


        Raises
        ------
        GeometryError
            If no adjacent binary64 assignment preserves the exact repaired geometry
            or the bounded embedding search is exhausted.
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
        point: Point,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> float: ...
    @overload
    def line_locate(
        self,
        point: GeometryArray[Point | None],
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def line_locate(
        self,
        point: Point,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> float: ...
    @overload
    def line_locate(
        self,
        point: GeometryArray[Point | None],
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> npt.NDArray[np.float64]:
        """Locate the position on linework nearest ``point``. Empty linework, or
        an empty query point, locates to ``nan``.

        Projection is in X/Y. Zero-length X/Y segments add no distance-basis projection.
        Planar distance-basis locate selects the exactly nearest segment; exact ties
        prefer an interior projection over an endpoint, then the first segment in route
        order, matching planar proximity. Geographic locate applies the same role and
        source order when evaluated backend distances compare equal. It ranks available
        measurements, including the chart projection trial, rather than asserting an
        exact geodesic minimum. With ``basis='m'``, the returned M is taken from the
        selected projection.

        With CRS-aware units on a geographic CRS, locate minimizes geodesic distance to
        the straight chart edges and returns cumulative ellipsoidal arclength to the
        nearest point. ``unit='planar'`` instead uses Cartesian distance in the stored
        coordinate space.

        Parameters
        ----------
        point : Point or GeometryArray[Point | None]
            A point to project, or one point per line row. Missing query rows produce
            ``nan`` in the result column.

        basis : {'distance', 'm'}
            Return a CRS-aware distance, or the line's monotonic M ordinate. Distance
            forms omit ``basis``; M stationing requires ``basis='m'``. M-based
            projection follows the same chart-edge metric: ellipsoidal arclength on a
            geographic CRS, planar length otherwise.

        normalized : bool, default False
            Return a distance-basis fraction in [0, 1] under the selected metric.
            Changing ``unit`` can change the nearest projection and returned fraction.
            Invalid with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            Available only with ``basis='distance'``; invalid with
            ``basis='m'``.

        Returns
        -------
        float or numpy.ndarray
            One location, or a column when ``point`` is an array.


        Raises
        ------
        CRSError
            If the CRS cannot provide an unambiguous distance metric.
        CRSMismatchError
            If operands' CRS or coordinate-epoch metadata differ.
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If M values are missing or non-monotonic; an array row degrades to
            ``nan`` instead.
        GeometryError
            If a distance-only option is used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a route station; locate/interpolate round-trip only at a unique nearest position.
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
        Geographic diagnostics follow the CRS longitude quotient; projected and
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

    def translate(
        self, x_offset: _FloatScalarInput, y_offset: _FloatScalarInput
    ) -> Self:
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
        Original vertices remain clearance sites. Refined chart-edge samples improve
        boundary support without becoming new sites; an on-edge witness is interpolated
        on its original source segment.
        Projected CRS and CRS-free geometries measure in the active planar units.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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
        at: _FloatScalarInput,
        *,
        basis: Literal['distance'] = 'distance',
        count: None = None,
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> Point: ...
    @overload
    def line_interpolate(
        self,
        at: _FloatColumn1D,
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
        *,
        count: _IntScalar,
        basis: Literal['distance'] = 'distance',
        normalized: Literal[False] = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self,
        at: _FloatColumn1D,
        *,
        basis: Literal['m'],
        count: None = None,
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self,
        at: _FloatScalarInput,
        *,
        count: None = None,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> Point:
        """Interpolate point locations along linework. Empty linework yields
        ``POINT EMPTY`` at every location.

        Distance-basis routes retain vertices at zero-length X/Y segments. At an
        interior prefix shared by such a run, an exact boundary selects its first
        stored vertex; normalized ``0`` and ``1`` select the first and last stored
        vertices. M-basis interpolation follows the stored monotonic measure and keeps
        vertices whose active Z/M ordinates differ even when X/Y is equal.

        With CRS-aware units (the default on a geographic CRS), each source edge is
        straight in the CRS's longitude and latitude chart and distance is ellipsoidal
        arclength along that edge. An independently interpolated point uses canonical
        longitude; original endpoints retain their stored spelling. ``unit='planar'``
        instead uses Cartesian distance in the stored coordinate space.

        Parameters
        ----------
        at : float or sequence of float, optional
            One location or many explicit distance-basis locations. Under
            ``basis='m'``, pass one stored M value for a scalar result or a sequence of
            stored M values for one line; an array accepts one M value per row.


        count : int, optional
            Number of evenly spaced distance-basis samples (``>= 1``) along the metric
            selected by ``unit``. Mutually exclusive with ``at`` and unavailable with
            ``basis='m'``; ``normalized=True`` is invalid because count already supplies
            fractional positions.

        basis : {'distance', 'm'}
            Use CRS-aware distance, or the line's monotonic M ordinate. Distance forms
            omit ``basis``; M stationing requires ``basis='m'``. Its M fraction selects
            the same arclength fraction on each edge: ellipsoidal chart-edge arclength
            on a geographic CRS, planar length otherwise.

        normalized : bool, default False
            Interpret distance-basis ``at`` values as fractions of the route length
            under the selected metric. Changing ``unit`` can change their positions.
            Invalid with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            Available only with ``basis='distance'``; invalid with
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
            If M values are missing or non-monotonic; an array row degrades to an
            empty result instead.
        GeometryError
            If input forms conflict, a value is non-finite, or a distance-only option
            is used with ``basis='m'``.

        See Also
        --------
        line_locate : Locate the nearest route position; repeated support makes the inverse non-unique.
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
        tolerance: _FloatScalarInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> Point | Polygon:
        """Largest circle inscribed in a polygonal geometry. Unlike Shapely's
        same-named operation, which returns a two-point ``LineString`` from center to
        boundary, gometry returns a contained filled polygonal approximation (or a
        ``Point`` for a degenerate radius). The returned polygon's centroid gives the
        circle center; ``maximum_inscribed_radius`` gives its numerical radius.
        Planar/projected input uses one deterministic representable center and
        certifies the global inradius against the requested absolute tolerance.
        Geographic input retains the documented local-projection approximation.
        Empty geometries return ``POLYGON EMPTY``.
        A nonempty polygon with no filled interior returns the midpoint of its stored
        bounds with radius zero, including when its rings cancel the entire support.
        Geographic line and ring inputs may be refined in the source chart to bound
        projected input-edge deviation; this does not change round-arc resolution.
        Mirrors minimum_bounding_circle.

        Parameters
        ----------
        tolerance : float, optional
            Positive absolute bound on the planar global-inradius gap. Omitted
            selects the existing scale-aware tolerance from the geometry's extent.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        Point or Polygon
            The fixed-resolution filled polygonal approximation of the inscribed
            circle, or the stored-bounds midpoint for a nonempty polygon with no
            filled interior.
            A certified subnormal-radius result returns its center ``Point``.

        See Also
        --------
        polylabel : Approximate pole-of-inaccessibility label point.
        maximum_inscribed_radius : The radius alone.
        point_on_surface : A representative point guaranteed on the geometry.
        centroid : Area/length-weighted center (may fall outside).

        Raises
        ------
        GeometryError
            If ``tolerance`` is not positive and finite, or a finite planar/projected
            result cannot be certified and represented as a contained polygon.
        InvalidGeometryError
            If invalid polygonal structure prevents the operation from completing.

        Examples
        --------
        >>> import gometry as gm
        >>> disk = gm.box(0, 0, 2, 2).maximum_inscribed_circle()
        >>> (disk.geometry_type, round(disk.area, 2))
        ('Polygon', 3.14)
        """
    def maximum_inscribed_radius(
        self,
        *,
        tolerance: _FloatScalarInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> float:
        """Radius of the largest inscribed circle. For planar/projected input,
        one deterministic representable center is retained and the returned
        downward-rounded boundary clearance of that underlying circle certifies
        the global inradius against the requested absolute tolerance. Geographic
        input uses the documented local-projection approximation. The numeric twin
        of minimum_bounding_radius; the polygonal approximation is
        maximum_inscribed_circle.
        Geographic boundary edges may be refined before the planar search; ``tolerance``
        controls the inradius search, not that input-refinement budget.

        Parameters
        ----------
        tolerance : float, optional
            Positive absolute bound on the planar global-inradius gap. Omitted
            selects the existing scale-aware tolerance from the geometry's extent.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        float
            The inscribed radius in the requested/CRS metric units (see ``unit``);
            ``NaN`` for empty input and zero for a polygon with no filled interior.


        Raises
        ------
        GeometryError
            If ``tolerance`` is not positive and finite, or ``unit='meters'`` is
            requested for a CRS-free geometry.
        CRSError
            If the CRS lacks linear axis units for a metric result.

        See Also
        --------
        maximum_inscribed_circle : The contained polygonal approximation.

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
        for planar/projected input, ``minimum_bounding_radius`` is the correctly rounded
        mathematical minimum radius and this method realizes that disk as a certified
        circumscribed 64-gon. Every edge encloses the disk and every vertex lies outside
        it. A single distinct vertex returns itself; empty input returns
        ``POLYGON EMPTY``. CRS-aware via local projection (approximate) on a
        geographic CRS; projected CRS distances default to the CRS's native linear units
        and scale through the CRS linear unit only with ``unit='meters'``. Geographic
        line and ring inputs use certified source-chart edge refinement in that local
        projection before their enclosing support circle is computed.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        Point or Polygon
            The smallest enclosing circle, or the source point for a zero-radius result.


        Raises
        ------
        GeometryError
            If the correctly rounded circle cannot be emitted as a finite, valid,
            source-enclosing polygon without changing its mathematical center or scalar
            radius.
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
        """Radius of the smallest circle enclosing the geometry. For planar/projected
        input it is rounded from the exact source minimum; this is the numeric twin of
        minimum_bounding_circle, and both use the same exact support kernel. Empty
        input yields ``NaN``; a single distinct vertex yields ``0``. Geographic point
        sets with three or more distinct vertices use the planar center and
        ellipsoidal support measurement (an approximation); two-point geographic
        inputs are the exact geodesic half-distance. For a projected
        ``unit='meters'`` result, the exact squared radius is scaled before the one
        binary64 rounding. Geographic line and ring inputs instead use the same
        local-projection support calculation as ``minimum_bounding_circle`` after
        chart-edge refinement. A two-coordinate line is edge-bearing and does not use
        the point-set two-point geodesic case.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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
        concavity: _FloatScalarInput = 2.0,
        length_threshold: _FloatScalarInput = 0.0,
        unit: DistanceUnit | None = None,
    ) -> Point | LineString | Polygon:
        """Compute the concave hull of the geometry. CRS-aware via local projection
        (approximate) and does NOT auto-split antimeridian-crossing geographic input;
        call ``split_antimeridian`` first. Uses gometry's chi-shape kernel: Delaunay
        boundary triangles are peeled from longest edge to shortest. The geometric XY
        hull is independent of input point order. Hull vertices are copies of input
        vertices, so X/Y/Z/M ordinates are preserved exactly. When several input vertices
        share an XY position, the first in storage order is the survivor.

        Parameters
        ----------
        concavity : float, default 2.0
            Sets the peel distance guard: a candidate triangle's opposite vertex must
            lie within ``edge_length / concavity`` of its boundary edge. ``0`` disables
            this guard. Must be finite and non-negative. Higher values typically keep
            more area, but the greedy peel does not guarantee area monotonicity as
            ``concavity`` increases.
        length_threshold : float, default 0.0
            Boundary edges at or below this length are kept; must be finite and
            non-negative. For fixed input and
            ``concavity``, a larger threshold always gives a hull that contains the
            smaller-threshold hull, so area never decreases. Interpreted for the CRS
            (see ``unit``). On a geographic CRS the threshold is evaluated in a local
            projection, while the output vertices are emitted from the original input
            coordinates.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        Point, LineString, or Polygon
            The concave hull; degenerate inputs reduce dimension.


        Raises
        ------
        GeometryError
            If parameters are non-finite or negative, or selected hull vertices have different
            coordinate axis layouts.

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
        ``split_antimeridian`` is required. The point lies inside or on the geometry,
        unlike centroid. Polygonal scanline candidates are checked after rounding; an
        uncovered candidate falls back to a source boundary point. The representative
        is computed in XY and does not imply a source Z/M. On planar polygonal input,
        the first polygon component wins when representative chords have equal width.

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

    def subdivide(self, *, max_vertices: _IntScalar = 256) -> GeometryArray[Geometry]:
        """Split geometry into parts of bounded complexity. Recursively clips
        across the longer axis until every part has at most ``max_vertices``
        coordinates (the PostGIS ``ST_Subdivide`` shape). Linework with no
        representable rectangular cut is divided between source vertices. Parts cover
        the input up to binary64 rounding at constructed cut vertices. Source
        ordinates are carried where meaningful; synthesized clip vertices use the
        operation's natural XY result.

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
            If ``max_vertices`` is below ``8``, exact partitioning cannot produce
            parts within the requested bound, no adjacent binary64 assignment
            preserves the exact subdivision, or the bounded embedding search is
            exhausted.

        Examples
        --------
        >>> import gometry as gm
        >>> parts = gm.LineString([(i, 0) for i in range(20)]).subdivide(max_vertices=8)
        >>> len(parts)
        4
        """

    def scale(
        self,
        x_factor: _FloatScalarInput,
        y_factor: _FloatScalarInput | None = None,
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

    def extremes(self) -> Extremes[Point] | None:
        """Return the west, south, east, and north extreme vertices of the
        geometry (numeric X/Y; ties keep the first vertex in storage order).

        Returns
        -------
        Extremes or None
            The ``(west, south, east, north)`` named tuple, or ``None`` for an empty
            geometry — the same contract as ``bounds`` and the other extent
            accessors.


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
        max_length: _FloatScalarInput,
        /,
        *,
        fraction: None = None,
        path: None = None,
        unit: DistanceUnit | None = None,
    ) -> Self: ...
    @overload
    def segmentize(
        self,
        max_length: _FloatScalarInput,
        /,
        *,
        fraction: None = None,
        path: NavigationPath,
        unit: None = None,
    ) -> Self: ...
    @overload
    def segmentize(
        self,
        max_length: None = None,
        /,
        *,
        fraction: _FloatScalarInput,
        path: NavigationPath | None = None,
        unit: None = None,
    ) -> Self:
        """Densify linework by inserting vertices so no segment exceeds
        ``max_length`` (or a fraction of its length).

        By default, each geographic edge keeps its stored shape: the straight line
        between its longitude/latitude vertices. ``max_length`` is measured along that
        edge on the ellipsoid. ``path='geodesic'`` or ``path='rhumb'`` instead places
        new vertices along the shortest ellipsoidal route or the constant-bearing route
        between each pair of endpoints. The result remains a polyline with straight
        longitude/latitude edges, so its length approaches the selected route length as
        the pieces get shorter. Projected geometries use native linear units, and
        CRS-free geometries use raw coordinate units. Every original vertex survives
        unchanged — this operation only inserts. Consecutive equal-X/Y vertices with
        distinct active Z/M ordinates remain in their stored order; their zero-length
        segments are not subdivided.

        Parameters
        ----------
        max_length : float, optional
            Positive maximum segment length, interpreted for the CRS (see ``unit``).
            Pass this
            positional argument or use ``fraction``, but not both.

        fraction : float, optional
            Fraction in ``(0, 1]`` of each source segment. Keyword-only; use when
            the subdivision is naturally relative rather than expressed in units.
        path : {'geodesic', 'rhumb'}, optional
            Route for inserted geographic vertices. ``None`` keeps each stored edge;
            ``'geodesic'`` follows the shortest ellipsoidal route and is planar for
            projected or CRS-free inputs; ``'rhumb'`` follows a constant-bearing route
            and requires a geographic CRS. ``fraction`` is measured along the selected
            route.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            The override applies only to ``max_length``; ``fraction`` has no unit
            and cannot be combined with ``unit``. ``path`` cannot be combined with
            ``unit``; geographic route distances are meters.

        Returns
        -------
        Geometry
            The segmentized geometry (same kind as the input).


        Raises
        ------
        CRSError
            If ``unit='meters'`` is requested and the CRS lacks linear axis units, or
            ``path='rhumb'`` is requested without a geographic CRS, or the selected
            geodesic route has multiple shortest paths.
        GeometryError
            If neither or both constraints are supplied, ``max_length`` is not a
            positive finite number, ``fraction`` is outside ``(0, 1]``, ``unit`` is
            combined with ``fraction``, ``path`` is combined with ``unit``,
            ``unit='meters'`` is requested for a CRS-free geometry, or the
            endpoint-preserving binary64 output cannot satisfy ``max_length``.

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

    @overload
    def voronoi(
        self,
        *,
        output: Literal['polygons'] = 'polygons',
        tolerance: _FloatScalarInput = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> GeometryArray[Polygon]: ...
    @overload
    def voronoi(
        self,
        *,
        output: Literal['edges'],
        tolerance: _FloatScalarInput = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> GeometryArray[LineString]:
        """Construct a Voronoi diagram from the geometry's vertices. Operates in
        planar longitude/latitude space and does NOT auto-split antimeridian-crossing
        geographic input; call ``split_antimeridian`` first. Collinear vertices divide
        the clip region into strips at their perpendicular bisectors, and a lone
        vertex owns the whole clip polygon; the padded and envelope frames of a lone
        vertex, or the envelope of a horizontal or vertical row, bound no cells.

        Parameters
        ----------
        output : {'polygons', 'edges'}
            Return clipped Voronoi cells or the internal Voronoi edges. The polygon
            form omits ``output``; edge output requires ``output='edges'`` and excludes
            the clip polygon's exterior boundary.
        tolerance : float, default 0.0
            Finite non-negative coordinate-space tolerance: native angular units for
            geographic CRSs, native linear units for projected CRSs, and raw coordinate
            units when CRS-free.
        clip : {'padded', 'envelope'} or Polygon, default 'padded'
            How to bound the unbounded outer cells: a padded box, the input
            envelope, or a `Polygon` to clip the diagram to.
            Diagram vertices are synthesized and returned in XY. Rounded clip
            incidences need not lie on the source polygon under overlay membership.

        Returns
        -------
        GeometryArray[Polygon] or GeometryArray[LineString]
            The selected output for the scalar geometry's vertices.

        Raises
        ------
        GeometryError
            If the output cannot be represented in binary64 or its embedding search
            exhausts the budget. ``operation`` is ``'voronoi_cells'`` for polygon
            output and ``'voronoi_edge_lines'`` for edge output.
        InvalidGeometryError
            If the clip polygon is invalid or exact Voronoi construction fails.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
        >>> len(sites.voronoi())
        3
        >>> len(sites.voronoi(output='edges'))
        3
        """
    def split_antimeridian(self) -> Geometry:
        """Split at the antimeridian. CRS-free input is treated as CRS84; a tagged
        geographic CRS supplies its own longitude frame and ellipsoid. Parts that cross
        come back as multiple parts
        whose edges follow the seam — each side keeping its own seam sign — so the
        result remains a coordinate-planar longitude/latitude chart for topology.
        Inserted seam vertices keep the affine chart crossing in X/Y; eligible Z/M
        follow ellipsoidal arclength along the retained source edge. An authored
        full-period edge is retained with quarter witnesses before its longitudes are
        canonicalized. The two seam aliases and a displacement of multiple periods do
        not imply a full-period edge. A ring running off the seam closes over its pole
        automatically. Geometries that do not cross are returned unchanged. A split
        ``LineString`` becomes a ``MultiLineString`` and a split ``Polygon`` a
        ``MultiPolygon``, like repair. On a geographic CRS, the split subdivides
        existing chart edges, so ellipsoidal area and perimeter are conserved to
        rounding.

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
        distance: _FloatScalarInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: _IntScalar = 8,
        miter_limit: _FloatScalarInput = 5.0,
        unit: DistanceUnit | None = None,
    ) -> LineString | MultiLineString | GeometryCollection:
        """Compute a line offset to one side of a linestring by ``distance`` (measured
        for the CRS).

        Geographic input uses the same local-frame admission and source-chart edge
        refinement as ``buffer``. Its input-fidelity budget is separate from the 0.1%
        frame-fit limit and round-join resolution, which ``quadrant_segments`` controls.
        Chord directions determine endpoint normals, so the input-fidelity budget does
        not bound output endpoint displacement.

        Parameters
        ----------
        distance : float
            Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
            CRS, native linear units on a projected CRS, raw coordinate units when
            CRS-free. The result is the RAW parallel
            curve: where the input folds back within ``distance`` the curve can
            self-intersect; ``buffer(side=...)`` gives the trimmed area instead. Must
            be finite.
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
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        LineString, MultiLineString, or GeometryCollection
            The raw offset curve, of the input's kind.


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
        """Reverse the vertex order of a geometry. Linework retains every stored
        vertex and its active Z/M ordinates, including equal-X/Y vertices, in reverse
        order.

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

    def sample_points(
        self, count: _IntScalar, *, seed: _IntScalar
    ) -> GeometryArray[Point]:
        """Random points on the geometry. The sample space is its highest
        dimension: coordinate-area weighted for areal input, coordinate-length
        weighted for lineal input, and equal weight across point atoms. Geographic
        lines follow each retained longitude branch, while geographic areas are split
        into seam- and pole-aware chart parts before sampling. Length and area weights
        use the native coordinate chart, not ellipsoidal measures; for example, a
        degree-based 170° to -170° line follows its retained +20° path, contributing
        20° rather than the raw 340° chord. Seam-crossing polygons use their seam-split
        chart area, not the complementary raw-coordinate span. Each logical
        member contributes separately, so overlapping members retain their sampling
        multiplicity. A zero-area polygon falls back to its linework, and zero-length
        linework falls back to vertices, like centroid. Deterministic: the same input
        and ``seed`` always produce the same points (an explicit seed is required —
        no hidden global RNG). Array rows draw distinct deterministic streams derived
        from ``seed`` and the row index, and a scalar geometry IS row 0 — so
                ``arr.sample_points(n, seed=s)[0]`` and
                ``arr[0].sample_points(n, seed=s)`` agree. An empty geometry has nothing to
                sample and yields no points. Results retain the input CRS and coordinate
                epoch. Sampled points are new XY locations, so they have no source Z/M.

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

    def quantize(self, precision: _IntScalar) -> Self:
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
        self,
        angle: _FloatScalarInput,
        *,
        origin: Origin = 'centroid',
        radians: bool = False,
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
        point_on_surface : A representative point guaranteed on the geometry.
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

    def force_3d(self, z: _FloatScalarInput = 0.0) -> Self:
        """Make each geometry 3D, filling vertices that lack Z with ``z``. Vertices
        that already carry Z keep it; M passes through. The one obvious way to lift
        to 3D.

        Parameters
        ----------
        z : float, default 0.0
            Finite Z to assign where it is missing.

        Returns
        -------
        Geometry
            The geometry with a Z ordinate on every vertex (same kind as the
            input).

        Raises
        ------
        InvalidGeometryError
            If ``z`` is not finite.

        See Also
        --------
        force_2d : Drop Z and M to plain XY.
        with_coordinates : Replace individual coordinate ordinates.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.force_3d().to_wkt()[0]
        'POINT Z (1 2 0)'
        """

    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: Literal[False] = False,
    ) -> Self: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> Geometry:
        """Snap every coordinate onto a regular grid and clean the result. X/Y move to
        the nearest ``origin + k * grid_size`` grid node; consecutive duplicate vertices
        collapse, and parts that degenerate below their minimum become empty (the
        PostGIS ``ST_SnapToGrid`` shape — output may be non-simple). Z/M ride on
        surviving vertices. quantize is the decimal-rounding, vertex-preserving
        sibling.

        Parameters
        ----------
        grid_size : float or tuple of float
            Positive finite coordinate-space grid spacing — native angular units for
            geographic CRSs, native linear units for projected CRSs, and raw
            coordinate units when CRS-free. Pass one value for a square grid or
            ``(sx, sy)``.
            Zero is invalid here; ``coverage_clean(grid_size=0)`` instead disables
            snapping.

        origin : tuple of float, default (0.0, 0.0)
            A grid node anchoring the lattice.

        repair : bool
            If ``True``, guarantee a valid result: snap, linework-repair, and re-
            snap to a fixpoint. The geometry kind may change (a ``Polygon`` whose
            snapped shell pinches splits into a ``MultiPolygon``). Geographic
            ``repair=True`` validates the exact quotient result; projected and
            CRS-free geometry remains planar.

        Returns
        -------
        Geometry
            The snapped geometry; parts below their minimum degenerate to empty.


        Raises
        ------
        GeometryError
            If ``grid_size`` or ``origin`` is invalid, or the grid is too fine for the
            coordinate magnitude.
        InvalidGeometryError
            If ``repair=True`` and repair would have to invent Z/M ordinates, or
            the snap-repair loop cannot converge.

        Examples
        --------
        >>> import gometry as gm
        >>> jittery = gm.LineString([(0.12, 0.88), (2.49, 1.51)])
        >>> jittery.snap_to_grid(grid_size=0.5).to_wkt()
        'LINESTRING (0 1, 2.5 1.5)'
        """

    def build_area(self) -> Polygon | MultiPolygon | GeometryCollection:
        """Assemble linework into one areal geometry. Input ordinates are carried
        where vertices can be sourced; otherwise the mathematically planar result is XY.
        When returned polygons retain different coordinate axes, they remain separate
        members of a GeometryCollection rather than being promoted or flattened.

        Returns
        -------
        Polygon, MultiPolygon, or GeometryCollection
            The maximal areal geometry covered by the input. A GeometryCollection is
            returned only when multiple output polygons retain different coordinate axes;
            each polygon member preserves its own ordinates.


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
        """Build polygons from a geometry's own linework, automatically noding
        crossings and overlaps. Each geometry is polygonized in isolation; to reconstruct
        polygons from a pile of edges pooled across many geometries, use free function
        ``polygonize_all`` on an iterable of
        values. Input ordinates are carried where possible; unsourceable noding seams
        yield XY.

        Returns
        -------
        GeometryArray
            The polygons built from the input linework.


        See Also
        --------
        polygonize_all : Pool linework across all supplied geometries.

        Raises
        ------
        GeometryError
            If no adjacent binary64 assignment preserves the exact polygonized output
            or the bounded embedding search is exhausted.
        InvalidGeometryError
            If polygons cannot be assembled from the linework.

        Examples
        --------
        >>> import gometry as gm
        >>> a, b = [(0, 0), (1, 0)], [(1, 0), (1, 1)]
        >>> edges = gm.MultiLineString([a, b, [(1, 1), (0, 0)]])
        >>> edges.polygonize().to_wkt()[0]
        'POLYGON ((0 0, 1 0, 1 1, 0 0))'
        """

    def convex_hull(self) -> Point | LineString | Polygon:
        """Compute the convex hull of the geometry. Operates in planar lon/lat space and does NOT
        auto-split antimeridian-crossing geographic input; call
        ``split_antimeridian`` first. Hull vertices are copies of input vertices, so Z/M
        ordinates are preserved. When several input vertices share an XY position, the
        first in storage order is the survivor.

        Returns
        -------
        Point, LineString, or Polygon
            The convex hull; degenerate inputs reduce dimension.

        Raises
        ------
        GeometryError
            If selected hull vertices have different coordinate axis layouts.

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
        Geographic validity reads exact quotient topology, so period aliases and seam
        cuts do not depend on their stored longitude spelling. An out-of-domain
        latitude is reported as ``latitude_out_of_range``; geographic operations that
        require coordinate admission still raise. For quotient-topology issues, the
        location is a canonical coordinate of the issue's contact set, chosen
        deterministically by minimum latitude then longitude. Projected and CRS-free geometry uses ordinary
        planar OGC validity. A nonempty LineString with fewer than two distinct XY
        positions is invalid and reports ``line_collapsed``.

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
        GeometryError
            If no adjacent binary64 assignment preserves the exact noded linework or
            the bounded embedding search is exhausted.
        InvalidGeometryError
            If noding fails on the input linework.

        Examples
        --------
        >>> import gometry as gm
        >>> lines = gm.MultiLineString([[(0,0),(2,0)],[(1,-1),(1,1)]])
        >>> noded = lines.node()
        >>> (noded.num_geometries, noded.length)
        (4, 4.0)
        """

    def to_wkt(
        self,
        *,
        include_srid: bool = False,
        precision: _IntScalar | None = None,
        drop_epoch: bool = False,
    ) -> str:
        """Serialize to Well-Known Text.

        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

        Parameters
        ----------
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
            If ``precision`` is not between 0 and 15, or the geometry carries a coordinate epoch and
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
        precision: _IntScalar | None = None,
        drop_epoch: bool = False,
    ) -> bytes:
        """Serialize to Well-Known Binary.

        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

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

    def remove_repeated_points(self, *, tolerance: _FloatScalarInput = 0.0) -> Self:
        """Drop consecutive duplicate coordinates within ``tolerance``.

        Parameters
        ----------
        tolerance : float, default 0.0
            Finite non-negative coordinate-space tolerance: native angular units for
            geographic CRSs, native linear units for projected CRSs, and raw coordinate
            units when CRS-free.


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
        x_angle: _FloatScalarInput = 0.0,
        y_angle: _FloatScalarInput = 0.0,
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

@final
class Point(Geometry):
    """A single point geometry.

    Parameters
    ----------
    x : Point, coordinate sequence, or float, optional
        A Point to copy, one finite coordinate value, or the X coordinate.
        Omit both coordinates for an empty point.
    y : float, optional
        Finite Y coordinate when ``x`` is a scalar. Omit it when ``x`` is a
        coordinate value or existing Point.
    z : float, optional
        Z ordinate (adds a 3D dimension).
    m : float, optional
        M (measure) ordinate.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
    """

    __match_args__: Final = ('x', 'y')

    @property
    def parts(self) -> GeometryParts[Point]:
        """Lazy view over this geometry's top-level parts.

        Simple geometries expose themselves as a one-row view; multipart and
        collection geometries expose their members. `GeometryArray(geom.parts)`
        materializes them as an array.

        Returns
        -------
        GeometryParts
        """
    @overload
    def destination(
        self,
        bearing: ScalarFloatArray,
        distance: _FloatScalarInput,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> Point: ...
    @overload
    def destination(
        self,
        bearing: _FloatScalarInput,
        distance: ScalarFloatArray,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> Point: ...
    @overload
    def destination(
        self,
        bearing: _FloatScalarInput,
        distance: _FloatScalarInput,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> Point: ...
    @overload
    def destination(
        self,
        bearing: _FloatColumn1D,
        distance: _FloatScalarInput,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: _FloatScalarInput,
        distance: _FloatColumn1D,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: _FloatColumn1D,
        distance: _FloatColumn1D,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: FloatInput,
        distance: FloatInput,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> Point | GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: ScalarFloatArray,
        distance: _FloatScalarInput,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> Point: ...
    @overload
    def destination(
        self,
        bearing: _FloatScalarInput,
        distance: ScalarFloatArray,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> Point: ...
    @overload
    def destination(
        self,
        bearing: _FloatScalarInput,
        distance: _FloatScalarInput,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> Point: ...
    @overload
    def destination(
        self,
        bearing: _FloatColumn1D,
        distance: _FloatScalarInput,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: _FloatScalarInput,
        distance: _FloatColumn1D,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: _FloatColumn1D,
        distance: _FloatColumn1D,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self,
        bearing: FloatInput,
        distance: FloatInput,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> Point | GeometryArray[Point]:
        """Walk from this point along a geodesic or rhumb path. An empty point
        stays ``POINT EMPTY``.

        Parameters
        ----------
        bearing : float or sequence of float
            Initial azimuth in degrees clockwise from north.
        distance : float or sequence of float
            Finite, non-negative distance to travel in CRS-natural units, unless
            `unit` is set.
        path : {'geodesic', 'rhumb'}
            Route model. Geodesic forms omit ``path``; rhumb forms require
            ``path='rhumb'`` and a geographic CRS.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.


            ``path='rhumb'`` uses meters and does not accept a unit override.

        Returns
        -------
        Point or GeometryArray[Point]
            The destination point, or one destination per bearing or distance.

        Raises
        ------
        GeometryError
            If `bearing` or `distance` is invalid.
        CRSError
            If the selected route requires an unavailable CRS metric.
        InvalidGeometryError
            If a coordinate is outside the longitude/latitude domain.

        Examples
        --------
        >>> import gometry as gm
        >>> point = gm.Point(0, 0, crs=4326)
        >>> point.destination(90, 1000).to_wkt(precision=5)
        'POINT (0.00898 0)'
        """

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
        x: Point | _Coordinate,
        y: None = None,
        *,
        z: _FloatScalarInput | None = None,
        m: _FloatScalarInput | None = None,
        crs: CrsInput | None = None,
        epoch: _FloatScalarInput | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        x: _FloatScalarInput,
        y: _FloatScalarInput,
        *,
        z: _FloatScalarInput | None = None,
        m: _FloatScalarInput | None = None,
        crs: CrsInput | None = None,
        epoch: _FloatScalarInput | None = None,
    ) -> Self:
        """Create a ``Point`` geometry — ``XY``, ``XYZ``, ``XYM``, or ``XYZM``.

        Pass one coordinate value or separate ``x``/``y`` scalars. A coordinate
        of length two, three, or four means ``XY``, ``XYZ``, or ``XYZM``.
        Pass ``m=`` to build ``XYM``; to build many points use ``points``.

        Parameters
        ----------
        x : Point, coordinate sequence, or float, optional
            A Point to copy, one finite coordinate value, or the X coordinate.
            Omit both coordinates for an empty point.
        y : float, optional
            Finite Y coordinate when ``x`` is a scalar. Omit it when ``x`` is a
            coordinate value or existing Point.

        z : float, optional
            Z (elevation) ordinate, producing an ``XYZ`` or ``XYZM`` point.

        m : float, optional
            M (measure) ordinate, producing an ``XYM`` or ``XYZM`` point.

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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
        >>> gm.Point((1, 2, 3, 9)).to_wkt()
        'POINT ZM (1 2 3 9)'
        >>> gm.Point(13.4, 52.5, crs=4326).crs
        CRS("EPSG:4326")
        >>> gm.Point().to_wkt()
        'POINT EMPTY'
        """

    @property
    def x(self) -> float:
        """X coordinate of the point.

        Returns
        -------
        float
            The X ordinate in the geometry's coordinate units.

        Raises
        ------
        AttributeError
            If the point is empty (``POINT EMPTY``).
        """
    @property
    def y(self) -> float:
        """Y coordinate of the point.

        Returns
        -------
        float
            The Y ordinate in the geometry's coordinate units.

        Raises
        ------
        AttributeError
            If the point is empty (``POINT EMPTY``).
        """
    @property
    def z(self) -> float:
        """Z (elevation) ordinate.

        Returns
        -------
        float
            The Z ordinate in the geometry's coordinate units.

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

        Returns
        -------
        float
            The M measure value; it is not converted using CRS units.

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

        crs : CRS-like or None, optional
            Replace or clear the CRS label.

        epoch : float or None, optional
            Replace or clear the coordinate epoch.

        Returns
        -------
        Point
        """
    def boundary(
        self,
    ) -> GeometryCollection: ...
    def buffer(
        self,
        distance: _FloatScalarInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: _IntScalar = 8,
        miter_limit: _FloatScalarInput = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> Polygon: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Point: ...
    def split_antimeridian(
        self,
    ) -> Point: ...
    def clip_by_rect(
        self,
        minx: _FloatScalarInput,
        miny: _FloatScalarInput,
        maxx: _FloatScalarInput,
        maxy: _FloatScalarInput,
    ) -> Point: ...
    def unique_points(
        self,
    ) -> MultiPoint: ...
    def build_area(
        self,
    ) -> Polygon | MultiPolygon: ...
    def envelope(
        self,
    ) -> Point | Polygon: ...
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> Point: ...
    @overload
    def __and__(self, other: Point, /) -> Point: ...
    @overload
    def __and__(self, other: MultiPoint, /) -> Point: ...
    @overload
    def __and__(self, other: GeometryArray[Point], /) -> GeometryArray[Point]: ...
    @overload
    def __and__(self, other: GeometryArray[MultiPoint], /) -> GeometryArray[Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[Point | None], /
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(
        self, other: GeometryArray[MultiPoint | None], /
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(self, other: Geometry, /) -> Point: ...
    @overload
    def __and__(self, other: GeometryArray[_GeometryT], /) -> GeometryArray[Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Point | GeometryArray[Point | None]:
        """Return self&value."""
    @overload
    def __rand__(self, other: Point, /) -> Point: ...
    @overload
    def __rand__(self, other: MultiPoint, /) -> Point: ...  # type: ignore[misc]
    @overload
    def __rand__(self, other: Geometry, /) -> Point: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rand__(self, other: object, /) -> Point | types.NotImplementedType:
        """Return value&self."""
    @overload
    def __sub__(self, other: Geometry, /) -> Point: ...
    @overload
    def __sub__(self, other: GeometryArray[_GeometryT], /) -> GeometryArray[Point]: ...
    @overload
    def __sub__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __sub__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Point | GeometryArray[Point | None]:
        """Return self-value."""
    @overload
    def __or__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __or__(
        self, other: GeometryArray[Point | MultiPoint], /
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __or__(
        self, other: GeometryArray[Point | MultiPoint | None], /
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __or__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __or__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __or__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __or__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self|value."""
    @overload
    def __ror__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __ror__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __ror__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __ror__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value|self."""
    @overload
    def __xor__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __xor__(
        self, other: GeometryArray[Point | MultiPoint], /
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __xor__(
        self, other: GeometryArray[Point | MultiPoint | None], /
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __xor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __xor__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __xor__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __xor__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self^value."""
    @overload
    def __rxor__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __rxor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __rxor__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rxor__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value^self."""

@final
class MultiPoint(Geometry):
    """A collection of points.

    Parameters
    ----------
    coordinates : iterable of points or coordinate tuples, optional
        The member points; omitted builds an empty multipoint.
    x, y, z, m : sequence of float, optional
        Column form: parallel ordinate arrays, one entry per point.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
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
        coordinates : iterable of points or coordinate tuples, optional
            Member coordinate tuples ``(x, y[, z[, m]])``. Mutually exclusive with
            the ``x``/``y`` column form. Omit all inputs for an empty multipoint.

        x, y : sequence of float, optional
            Per-point X and Y ordinates as parallel columns, as an alternative to
            ``coordinates``. Both are required together.

        z, m : sequence of float, optional
            Per-point Z and M ordinates, as an alternative to inline tuples.

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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
        """The component geometries.

        Returns
        -------
        GeometryParts
            A lazy view of immediate components in component order. It
            retains the source geometry; each component has the type
            declared by its owning geometry class. Scalar indexing and
            iteration create one typed geometry at a time, while slices
            return a list.
        """
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
    def __iter__(self) -> GeometryPartsIterator[Point]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> GeometryPartsIterator[Point]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """
    def __contains__(self, item: object, /) -> bool: ...
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> part = multi[0]
        >>> multi.index(part)
        0
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> multi.count(multi[0])
        1
        """
    def boundary(
        self,
    ) -> GeometryCollection: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> MultiPoint: ...
    def split_antimeridian(
        self,
    ) -> MultiPoint: ...
    def clip_by_rect(
        self,
        minx: _FloatScalarInput,
        miny: _FloatScalarInput,
        maxx: _FloatScalarInput,
        maxy: _FloatScalarInput,
    ) -> MultiPoint | Point: ...
    def unique_points(
        self,
    ) -> MultiPoint: ...
    def build_area(
        self,
    ) -> Polygon | MultiPolygon: ...
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> MultiPoint: ...
    @overload
    def __and__(self, other: Point, /) -> Point: ...
    @overload
    def __and__(self, other: MultiPoint, /) -> MultiPoint | Point: ...
    @overload
    def __and__(self, other: GeometryArray[Point], /) -> GeometryArray[Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[MultiPoint], /
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[Point | None], /
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(
        self, other: GeometryArray[MultiPoint | None], /
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __and__(self, other: Geometry, /) -> MultiPoint | Point: ...
    @overload
    def __and__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __and__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __and__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> MultiPoint | Point | GeometryArray[MultiPoint | Point | None]:
        """Return self&value."""
    @overload
    def __rand__(self, other: Point, /) -> Point: ...  # type: ignore[misc]
    @overload
    def __rand__(self, other: MultiPoint, /) -> MultiPoint | Point: ...
    @overload
    def __rand__(self, other: Geometry, /) -> MultiPoint | Point: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rand__(
        self, other: object, /
    ) -> MultiPoint | Point | types.NotImplementedType:
        """Return value&self."""
    @overload
    def __sub__(self, other: Geometry, /) -> MultiPoint | Point: ...
    @overload
    def __sub__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __sub__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __sub__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> MultiPoint | Point | GeometryArray[MultiPoint | Point | None]:
        """Return self-value."""
    @overload
    def __or__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __or__(
        self, other: GeometryArray[Point | MultiPoint], /
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __or__(
        self, other: GeometryArray[Point | MultiPoint | None], /
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __or__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __or__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __or__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __or__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self|value."""
    @overload
    def __ror__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __ror__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __ror__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __ror__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value|self."""
    @overload
    def __xor__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __xor__(
        self, other: GeometryArray[Point | MultiPoint], /
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __xor__(
        self, other: GeometryArray[Point | MultiPoint | None], /
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __xor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __xor__(
        self, other: GeometryArray[_GeometryT], /
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __xor__(
        self, other: GeometryArray[_GeometryT | None], /
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __xor__(
        self, other: Geometry | GeometryArray[Geometry | None], /
    ) -> Geometry | GeometryArray[Geometry | None]:
        """Return self^value."""
    @overload
    def __rxor__(self, other: Point | MultiPoint, /) -> Point | MultiPoint: ...
    @overload
    def __rxor__(self, other: Geometry, /) -> Geometry: ...
    @overload
    def __rxor__(  # type: ignore[misc]
        self, other: GeometryArray[Geometry | None], /
    ) -> types.NotImplementedType: ...
    @overload
    def __rxor__(self, other: object, /) -> Geometry | types.NotImplementedType:
        """Return value^self."""

@final
class LineString(Geometry):
    """A single linestring (polyline).

    Parameters
    ----------
    coordinates : iterable of coordinate tuples, optional
        The vertices as ``(x, y[, z, m])`` tuples; omitted builds an empty line.
    x, y, z, m : sequence of float, optional
        Column form: parallel ordinate arrays, one entry per vertex.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
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
        coordinates : iterable of coordinate tuples, optional
            Ordered ``(x, y[, z[, m]])`` tuples, or an ``(N, 2..4)`` array.
            Mutually exclusive with the ``x``/``y`` column form. Omit all inputs
            for an empty linestring.

        x, y : sequence of float, optional
            Per-vertex X and Y ordinates as parallel columns, as an alternative to
            ``coordinates``. Both are required together.

        z, m : sequence of float, optional
            Per-vertex Z and M ordinates, as an alternative to inline tuples.

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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

    @property
    def parts(self) -> GeometryParts[LineString]:
        """Lazy view over this geometry's top-level parts.

        Simple geometries expose themselves as a one-row view; multipart and
        collection geometries expose their members. `GeometryArray(geom.parts)`
        materializes them as an array.

        Returns
        -------
        GeometryParts
        """
    def boundary(
        self,
    ) -> MultiPoint: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Point | LineString | MultiLineString: ...
    def split_antimeridian(
        self,
    ) -> LineString | MultiLineString: ...
    def offset_curve(
        self,
        distance: _FloatScalarInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: _IntScalar = 8,
        miter_limit: _FloatScalarInput = 5.0,
        unit: DistanceUnit | None = None,
    ) -> LineString: ...
    def line_merge(
        self,
    ) -> LineString: ...
    def unique_points(
        self,
    ) -> MultiPoint: ...
    def build_area(
        self,
    ) -> Polygon | MultiPolygon: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: Literal[False] = False,
    ) -> LineString: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> LineString | MultiLineString: ...

@final
class MultiLineString(Geometry):
    """A collection of linestrings.

    Parameters
    ----------
    lines : iterable of LineString or coordinate sequences, optional
        The member lines; omitted builds an empty multilinestring.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
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
        lines : iterable of LineString or coordinate sequences, optional
            Each member is an ordered coordinate sequence (a line) or an
            already-built ``LineString``. Omit for an empty multilinestring.

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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
        """The component geometries.

        Returns
        -------
        GeometryParts
            A lazy view of immediate components in component order. It
            retains the source geometry; each component has the type
            declared by its owning geometry class. Scalar indexing and
            iteration create one typed geometry at a time, while slices
            return a list.
        """
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
    def __getitem__(
        self, index: SupportsIndex | slice, /
    ) -> LineString | list[LineString]:
        """Select parts by integer or slice.

        An ``int`` returns one component geometry. A ``slice`` returns a
        ``list`` of component geometries.

        Returns
        -------
        Geometry or list of Geometry
        """
    def __iter__(self) -> GeometryPartsIterator[LineString]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> GeometryPartsIterator[LineString]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """
    def __contains__(self, item: object, /) -> bool: ...
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> part = multi[0]
        >>> multi.index(part)
        0
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> multi.count(multi[0])
        1
        """
    def boundary(
        self,
    ) -> MultiPoint: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Point | MultiPoint | LineString | MultiLineString | GeometryCollection: ...
    def split_antimeridian(
        self,
    ) -> MultiLineString: ...
    def offset_curve(
        self,
        distance: _FloatScalarInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: _IntScalar = 8,
        miter_limit: _FloatScalarInput = 5.0,
        unit: DistanceUnit | None = None,
    ) -> MultiLineString: ...
    def unique_points(
        self,
    ) -> MultiPoint: ...
    def build_area(
        self,
    ) -> Polygon | MultiPolygon: ...
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> MultiLineString: ...

@final
class Polygon(Geometry):
    """A single polygon (an exterior ring with optional holes).

    Parameters
    ----------
    shell : iterable of coordinate tuples, optional
        Exterior ring vertices; omitted builds ``POLYGON EMPTY``.
    holes : iterable of rings, optional
        Interior rings, each a coordinate sequence.
    x, y, z, m : sequence of float, optional
        Column form for the exterior ring: parallel ordinate arrays.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
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
        shell : iterable of coordinate tuples, optional
            Exterior ring coordinates; closed automatically (needs ≥3 corners).
            Mutually exclusive with the ``x``/``y`` column form.

        holes : iterable of rings, optional
            Interior ring (hole) coordinate sequences, each closed automatically.

        x, y : sequence of float, optional
            Per-vertex X and Y ordinates for the exterior ring, as an alternative
            to ``shell``. Both are required together.

        z, m : sequence of float, optional
            Per-vertex Z and M ordinates for the exterior ring.

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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
    def parts(self) -> GeometryParts[Polygon]:
        """Lazy view over this geometry's top-level parts.

        Simple geometries expose themselves as a one-row view; multipart and
        collection geometries expose their members. `GeometryArray(geom.parts)`
        materializes them as an array.

        Returns
        -------
        GeometryParts
        """
    @property
    def exterior(self) -> LineString:
        """Exterior ring as a closed ``LineString``. ``POLYGON EMPTY`` returns an
        empty line preserving the polygon's declared axes.

        Returns
        -------
        LineString
            The closed exterior ring, or an axis-preserving empty line for
            ``POLYGON EMPTY``. The result retains the polygon's CRS and epoch.
        """
    @property
    def interiors(self) -> list[LineString]:
        """Interior rings (holes), each a closed ``LineString``. The list follows
        the polygon's ring order; ``POLYGON EMPTY`` has no interior rings.

        Returns
        -------
        list of LineString
            A new list with one closed ring per hole, in polygon ring order.
            Empty polygons return an empty list. Each ring retains the polygon's
            declared axes, CRS, and epoch.
        """
    def boundary(
        self,
    ) -> MultiPoint | LineString | MultiLineString | GeometryCollection: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Polygon | MultiPolygon: ...
    def split_antimeridian(
        self,
    ) -> Polygon | MultiPolygon: ...
    def unique_points(
        self,
    ) -> MultiPoint: ...
    def build_area(
        self,
    ) -> Polygon | MultiPolygon: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: Literal[False] = False,
    ) -> Polygon: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> Polygon | MultiPolygon: ...

@final
class MultiPolygon(Geometry):
    """A collection of polygons.

    Parameters
    ----------
    polygons : iterable of Polygon or (shell, holes) pairs, optional
        The member polygons; omitted builds an empty multipolygon.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
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
        polygons : iterable of Polygon or (shell, holes) pairs, optional
            Each member is ``[shell]`` or ``[shell, *holes]`` of coordinate rings,
            or an already-built ``Polygon``. Omit for an empty multipolygon.

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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
        """The component geometries.

        Returns
        -------
        GeometryParts
            A lazy view of immediate components in component order. It
            retains the source geometry; each component has the type
            declared by its owning geometry class. Scalar indexing and
            iteration create one typed geometry at a time, while slices
            return a list.
        """
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
    def __iter__(self) -> GeometryPartsIterator[Polygon]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> GeometryPartsIterator[Polygon]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """
    def __contains__(self, item: object, /) -> bool: ...
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> part = multi[0]
        >>> multi.index(part)
        0
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> multi.count(multi[0])
        1
        """
    def boundary(
        self,
    ) -> MultiPoint | LineString | MultiLineString | GeometryCollection: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> Polygon | MultiPolygon: ...
    def split_antimeridian(
        self,
    ) -> MultiPolygon: ...
    def unique_points(
        self,
    ) -> MultiPoint: ...
    def build_area(
        self,
    ) -> Polygon | MultiPolygon: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: Literal[False] = False,
    ) -> MultiPolygon: ...
    @overload
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> Polygon | MultiPolygon: ...

@final
class GeometryCollection(Geometry):
    """A heterogeneous collection of geometries.

    Parameters
    ----------
    geometries : sequence of Geometry, optional
        The member geometries, of any types; omitted builds an empty collection.
    crs : CRS-like, optional
        Coordinate reference system, attached as metadata (never transforms coordinates).
    epoch : float or None, optional
        Coordinate epoch metadata; the CRS frame must admit geometry epochs.
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

        crs : CRS-like, optional
            Any supported CRS input. Declares the frame; does not transform coordinates.

        epoch : float or None, optional
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
        """The component geometries.

        Returns
        -------
        GeometryParts
            A lazy view of immediate components in component order. It
            retains the source geometry; each component has the type
            declared by its owning geometry class. Scalar indexing and
            iteration create one typed geometry at a time, while slices
            return a list.
        """
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
    def __iter__(self) -> GeometryPartsIterator[Geometry]:
        """Iterate component geometries.

        Returns
        -------
        iterator of Geometry
        """
    def __reversed__(self) -> GeometryPartsIterator[Geometry]:
        """Iterate component geometries in reverse order.

        Returns
        -------
        iterator of Geometry
        """
    def __contains__(self, item: object, /) -> bool: ...
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> part = multi[0]
        >>> multi.index(part)
        0
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

        Examples
        --------
        >>> import gometry as gm
        >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
        >>> multi.count(multi[0])
        1
        """
    def boundary(
        self,
    ) -> GeometryCollection: ...
    def repair(
        self,
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryCollection: ...
    def split_antimeridian(
        self,
    ) -> GeometryCollection: ...
    def offset_curve(
        self,
        distance: _FloatScalarInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: _IntScalar = 8,
        miter_limit: _FloatScalarInput = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryCollection: ...
    def snap_to_grid(
        self,
        grid_size: _FloatScalarInput | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> GeometryCollection: ...

@final
class GeometryPartsIterator(Generic[_GeometryT_co]):
    """Lazy iterator over a ``GeometryParts`` view: one typed leaf per
    ``__next__`` via ``part_at``, without building the full part list up front.
    """
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        payload it keeps alive while iterating.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
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

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """
    def __repr__(self) -> str:
        """Return the bounded representation of this parts view."""
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
    def __getitem__(
        self, index: SupportsIndex | slice, /
    ) -> _GeometryT_co | list[_GeometryT_co]:
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
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        payload it keeps alive while iterating.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """

    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> _GeometryT_co:
        """Implement next(self)."""

class _GeometryArrayPartsProjection(Protocol):
    @overload
    def __get__(
        self,
        instance: GeometryArray[Point],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Point]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[MultiPoint],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Point]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[LineString],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[LineString]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[MultiLineString],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[LineString]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[Polygon],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[MultiPolygon],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[GeometryCollection],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Geometry]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[Point | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Point] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[MultiPoint | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Point] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[LineString | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[LineString] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[MultiLineString | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[LineString] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[Polygon | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Polygon] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[MultiPolygon | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Polygon] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[GeometryCollection | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Geometry] | None]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[Geometry],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Geometry]]: ...
    @overload
    def __get__(
        self,
        instance: GeometryArray[Geometry | None],
        owner: type[GeometryArray[Geometry | None]] | None = None,
    ) -> Groups[GeometryArray[Geometry] | None]: ...
    @overload
    def __get__(
        self,
        instance: None,
        owner: type[GeometryArray[Geometry | None]],
    ) -> Self: ...
    def __set__(
        self,
        instance: GeometryArray[Geometry | None],
        value: Never,
    ) -> None: ...

@final
class GeometryArray(Sequence[_GeometryT_co]):
    """An immutable array of geometries sharing one CRS/epoch frame.

    The vectorized counterpart of ``Geometry``: methods mirror the scalar
    names element-wise (``arr.area``, ``(arr).buffer(10.0)``,
    ``contains(arr, geom)``) and run as batched Rust kernels with no
    per-element Python. Homogeneous point, line, and polygon arrays pack into
    shared coordinate columns (with CSR offsets where needed) for zero-copy
    Arrow and columnar kernels. Build with ``GeometryArray(...)`` /
    ``points(...)`` or the WKT/WKB/GeoJSON/Arrow importers; index and
    slice it like a sequence.
    An immutable, vectorized geometry column with one shared CRS/epoch frame.

    Homogeneous arrays use packed coordinate storage and batched Rust kernels;
    indexing and slicing preserve zero-copy views where possible.
    """
    @overload
    def destination(
        self: GeometryArray[Point],
        bearing: float | FloatInput,
        distance: float | FloatInput,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self: GeometryArray[Point],
        bearing: float | FloatInput,
        distance: float | FloatInput,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def destination(
        self: GeometryArray[Point | None],
        bearing: float | FloatInput,
        distance: float | FloatInput,
        *,
        path: Literal['geodesic'] = 'geodesic',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def destination(
        self: GeometryArray[Point | None],
        bearing: float | FloatInput,
        distance: float | FloatInput,
        *,
        path: Literal['rhumb'],
        unit: None = None,
    ) -> GeometryArray[Point | None]:
        """Walk each point along a geodesic or rhumb path. An empty point stays
        ``POINT EMPTY``.

        Parameters
        ----------
        bearing : float or sequence of float
            Initial azimuth in degrees clockwise from north.
        distance : float or sequence of float
            Finite, non-negative distance to travel in CRS-natural units, unless
            `unit` is set.
        path : {'geodesic', 'rhumb'}
            Route model. Geodesic forms omit ``path``; rhumb forms require
            ``path='rhumb'`` and a geographic CRS.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.


            ``path='rhumb'`` uses meters and does not accept a unit override.

        Returns
        -------
        GeometryArray[Point]
            One destination point per array row.

        Raises
        ------
        GeometryError
            If `bearing` or `distance` is invalid.
        CRSError
            If the selected route requires an unavailable CRS metric.
        GeometryTypeError
            If an input row is not a point.
        InvalidGeometryError
            If a coordinate is outside the longitude/latitude domain.

        Examples
        --------
        >>> import gometry as gm
        >>> points = gm.GeometryArray([
        ...     gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326)
        ... ])
        >>> points.destination(90, 1000).to_wkt(precision=5)[0]
        'POINT (0.00898 0)'
        """
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Pickle durable array state; packed storage uses portable little-endian
        columns and offsets, while mixed storage uses WKB rows.
        """
    def __reduce_ex__(
        self, protocol: SupportsIndex, /
    ) -> tuple[Callable[..., GeometryArray[Geometry | None]], tuple[Any, ...]]:
        """Use protocol-5 out-of-band buffers for the encoded array payload when
        the pickler provides a buffer callback. The buffers are read-only.
        """
    def __repr__(self) -> str:
        """Return the bounded representation of this geometry array."""
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — a GeometryArray is an
        immutable value, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    @overload
    def __and__(
        self: GeometryArray[Point],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Point]: ...
    @overload
    def __and__(
        self: GeometryArray[_GeometryT],
        other: Point | GeometryArray[Point],
        /,
    ) -> GeometryArray[Point]: ...
    @overload
    def __and__(
        self: GeometryArray[MultiPoint],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __and__(
        self: GeometryArray[_GeometryT],
        other: MultiPoint | GeometryArray[MultiPoint],
        /,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __and__(
        self: GeometryArray[Point | None],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(
        self: GeometryArray[_GeometryT | None],
        other: Point | GeometryArray[Point | None],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __and__(
        self: GeometryArray[MultiPoint | None],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __and__(
        self: GeometryArray[_GeometryT | None],
        other: MultiPoint | GeometryArray[MultiPoint | None],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __and__(
        self: GeometryArray[_GeometryT],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __and__(
        self: GeometryArray[_GeometryT_co],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Geometry | None]:
        """Return self&value."""
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[Point],
        other: GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Point]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT],
        other: GeometryArray[Point],
        /,
    ) -> GeometryArray[Point]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[MultiPoint],
        other: GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT],
        other: GeometryArray[MultiPoint],
        /,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[Point | None],
        other: GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT | None],
        other: GeometryArray[Point | None],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[MultiPoint | None],
        other: GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT | None],
        other: GeometryArray[MultiPoint | None],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT],
        other: GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT_co],
        other: GeometryArray[Geometry | None],
        /,
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __rand__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT_co],
        other: Geometry,
        /,
    ) -> types.NotImplementedType: ...
    @overload
    def __rand__(
        self: GeometryArray[_GeometryT_co],
        other: object,
        /,
    ) -> GeometryArray[Geometry | None] | types.NotImplementedType:
        """Return value&self."""
    @overload
    def __or__(
        self: GeometryArray[Point | MultiPoint],
        other: Point | MultiPoint | GeometryArray[Point | MultiPoint],
        /,
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __or__(
        self: GeometryArray[Point | MultiPoint | None],
        other: Point | MultiPoint | GeometryArray[Point | MultiPoint | None],
        /,
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __or__(
        self: GeometryArray[_GeometryT],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __or__(
        self: GeometryArray[_GeometryT_co],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Geometry | None]:
        """Return self|value."""
    @overload
    def __ror__(
        self: GeometryArray[Point | MultiPoint],
        other: GeometryArray[Point | MultiPoint],
        /,
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __ror__(
        self: GeometryArray[Point | MultiPoint | None],
        other: GeometryArray[Point | MultiPoint | None],
        /,
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __ror__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT],
        other: GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __ror__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT_co],
        other: GeometryArray[Geometry | None],
        /,
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __ror__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT_co],
        other: Geometry,
        /,
    ) -> types.NotImplementedType: ...
    @overload
    def __ror__(
        self: GeometryArray[_GeometryT_co],
        other: object,
        /,
    ) -> GeometryArray[Geometry | None] | types.NotImplementedType:
        """Return value|self."""
    @overload
    def __sub__(
        self: GeometryArray[Point],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Point]: ...
    @overload
    def __sub__(
        self: GeometryArray[MultiPoint],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __sub__(
        self: GeometryArray[Point | None],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __sub__(
        self: GeometryArray[MultiPoint | None],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __sub__(
        self: GeometryArray[_GeometryT],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __sub__(
        self: GeometryArray[_GeometryT_co],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Geometry | None]:
        """Return self-value."""
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry],
        other: GeometryArray[Point],
        /,
    ) -> GeometryArray[Point]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry],
        other: GeometryArray[MultiPoint],
        /,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry | None],
        other: GeometryArray[Point],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry | None],
        other: GeometryArray[MultiPoint],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry],
        other: GeometryArray[Point | None],
        /,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry],
        other: GeometryArray[MultiPoint | None],
        /,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry],
        other: GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __rsub__(
        self: GeometryArray[Geometry | None],
        other: GeometryArray[Geometry | None],
        /,
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __rsub__(  # type: ignore[misc]
        self: GeometryArray[Geometry | None],
        other: Geometry,
        /,
    ) -> types.NotImplementedType: ...
    @overload
    def __rsub__(
        self: GeometryArray[Geometry | None],
        other: object,
        /,
    ) -> GeometryArray[Geometry | None] | types.NotImplementedType:
        """Return value-self."""
    @overload
    def __xor__(
        self: GeometryArray[Point | MultiPoint],
        other: Point | MultiPoint | GeometryArray[Point | MultiPoint],
        /,
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __xor__(
        self: GeometryArray[Point | MultiPoint | None],
        other: Point | MultiPoint | GeometryArray[Point | MultiPoint | None],
        /,
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __xor__(
        self: GeometryArray[_GeometryT],
        other: Geometry | GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __xor__(
        self: GeometryArray[_GeometryT_co],
        other: Geometry | GeometryArray[_GeometryOtherT | None],
        /,
    ) -> GeometryArray[Geometry | None]:
        """Return self^value."""
    @overload
    def __rxor__(
        self: GeometryArray[Point | MultiPoint],
        other: GeometryArray[Point | MultiPoint],
        /,
    ) -> GeometryArray[Point | MultiPoint]: ...
    @overload
    def __rxor__(
        self: GeometryArray[Point | MultiPoint | None],
        other: GeometryArray[Point | MultiPoint | None],
        /,
    ) -> GeometryArray[Point | MultiPoint | None]: ...
    @overload
    def __rxor__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT],
        other: GeometryArray[_GeometryOtherT],
        /,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def __rxor__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT_co],
        other: GeometryArray[Geometry | None],
        /,
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __rxor__(  # type: ignore[misc]
        self: GeometryArray[_GeometryT_co],
        other: Geometry,
        /,
    ) -> types.NotImplementedType: ...
    @overload
    def __rxor__(
        self: GeometryArray[_GeometryT_co],
        other: object,
        /,
    ) -> GeometryArray[Geometry | None] | types.NotImplementedType:
        """Return value^self."""
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
    # mypy does not treat `type[object | np.object_]` as accepting dtype=object.
    def __array__(
        self,
        dtype: type[object]
        | type[np.object_]
        | np.dtype[np.object_]
        | Literal['O', 'object'],
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
        """`sys.getsizeof` support: Python's allocation base plus this array's logical
        Rust-side heap — coordinate payload, CSR offsets, row maps, missing
        mask, cache owners and their already-materialized contents, spilled CRS
        frame text, and any already-materialized prepared-geometry / frame
        sidecars.
        Lazy cache slots that have not been populated do not inflate the total.
        Shared backing buffers use the logical view estimate, not the whole
        shared parent allocation.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
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
        sidecars and CRS metadata are excluded (``nbytes`` is
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
    ) -> GeometryArray[Geometry | None]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[_GeometryT_co],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> Self: ...
    @overload
    def __new__(
        cls,
        values: Iterable[
            _GeometryT_co | Buffer | Mapping[str, Any] | SupportsGeoInterface
        ],
        *,
        crs: CrsInput | None = None,
        epoch: float | None = None,
    ) -> GeometryArray[Geometry | _GeometryT_co]:
        """Construct an immutable array from geometry-like rows.

        Iterable values are consumed once in input order. ``None`` creates a
        missing row. A single ``Geometry`` is not a row iterable; wrap it in a
        one-item sequence. Rows may be native geometries, WKB byte buffers,
        GeoJSON mappings, or objects exposing ``__geo_interface__``. ``crs=``
        attaches shared metadata and never transforms coordinates.

        Parameters
        ----------
        values : iterable of Geometry, Buffer, Mapping, or None
            One native geometry, WKB buffer, GeoJSON mapping, or object exposing
            ``__geo_interface__`` per input position; ``None`` marks a missing
            row.
        crs : CrsInput or None, optional, default None
            Shared CRS label. CRS-free rows adopt it; existing CRS values must
            be equivalent to it.
        epoch : float or None, optional, default None
            Shared coordinate epoch for a CRS that admits geometry epochs.
            Existing epochs must agree with it.

        Returns
        -------
        GeometryArray
            An immutable, row-aligned array with missing positions preserved.

        Raises
        ------
        TypeError
            If ``values`` is a single geometry or contains an unsupported value.
        CRSError
            If ``crs`` cannot be resolved or an epoch is invalid for its CRS.
        CRSMismatchError
            If input CRS or epoch metadata conflicts with the shared frame.
        GeometryError
            If ``epoch`` is non-finite.
        ParseError
            If a WKB or GeoJSON row is malformed.
        """
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
        WGS84 longitude/latitude ``FeatureCollection`` mapping, with one
        ``Feature`` per row, positional ``id``, and empty ``properties`` — the
        shape geopandas and mapping libraries expect from a geometry column.
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
    def has_m(self) -> npt.NDArray[np.bool_]:
        """Whether each geometry carries an M ordinate. Missing rows are false.

        Returns
        -------
        numpy.ndarray
        """
    @property
    def bounds(self) -> npt.NDArray[np.float64]:
        """Per-geometry bounds ``(minx, miny, maxx, maxy)`` (see
        `Geometry.bounds`), as a read-only ``(rows, 4)`` float64 ndarray.
        Empty rows are all-``nan`` (intentional: a fixed-width ndarray cannot
        hold ``None`` like a scalar geometry); missing rows are also all-``nan``
        and are identified by `.is_missing`.
        Geographic longitude intervals may cross the seam, with ``minx > maxx``.

        Returns
        -------
        numpy.ndarray
            One ``minx, miny, maxx, maxy`` row per input geometry.
        """
    @property
    def total_bounds(self) -> tuple[float, float, float, float] | None:
        """Combined bounds ``(minx, miny, maxx, maxy)`` over all geometries, or
        ``None`` if every geometry is empty. Geographic longitude bounds use
        the CRS's periodic interval, so ``minx > maxx`` denotes a seam crossing.

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
    parts: _GeometryArrayPartsProjection
    """Top-level parts grouped by source row.

    Simple geometries form one-element groups, multipart and collection
    geometries expose their immediate members, empty geometries form empty
    groups, and missing source rows remain missing groups.

    Returns
    -------
    Groups
        One group per input row; missing rows yield `None`.
    """
    @property
    def num_geometries(self) -> np.ndarray[tuple[int], np.dtype[np.int64]]:
        """Per-geometry top-level part count (see `Geometry.num_geometries`):
        ``1`` for a single point/line/polygon, including a typed empty; the
        member count for a multi/collection, and ``0`` for an empty multipart
        or collection. Explicit empty members still count in their multipart's
        member count. Missing rows have count ``0``; use `.is_missing` to
        distinguish them from present empty geometries.

        Returns
        -------
        numpy.ndarray
            One count per input geometry.
        """
    @property
    def num_coordinates(
        self,
    ) -> np.ndarray[tuple[int], np.dtype[np.int64]] | npt.NDArray[np.object_]:
        """Per-geometry coordinate count. Missing rows are ``0``.

        Returns
        -------
        numpy.ndarray
            One coordinate count per input geometry. The array uses ``int64``
            when every count fits; otherwise it uses ``object`` with exact
            Python integers.
        """
    @overload
    def with_coordinates(
        self, coordinates: _CoordinateMatrix | Coordinates, /
    ) -> Self: ...
    @overload
    def with_coordinates(
        self,
        coordinates: None = None,
        /,
        *,
        x: _FloatScalarInput | FloatColumn = ...,
        y: _FloatScalarInput | FloatColumn = ...,
        z: _FloatScalarInput | FloatColumn | None = ...,
        m: _FloatScalarInput | FloatColumn | None = ...,
    ) -> Self:
        """Return an array with selected coordinate ordinates replaced.

        Pass one ``(N, dims)`` matrix (including a `Coordinates` view), or
        replace any of ``x=``, ``y=``, ``z=``, and ``m=`` by a scalar or a
        column over the flattened coordinate sequence. Omitted ordinates are
        preserved. ``z=None`` and ``m=None`` remove those dimensions; X and Y
        cannot be removed. Missing rows contribute no coordinates and remain
        missing. Matrix and column modes cannot be mixed. Calling without
        arguments returns an unchanged value.

        Parameters
        ----------
        coordinates : sequence of float, optional
            Replacement ``(N, dims)`` coordinate matrix, including a
            `Coordinates` view.
        x, y, z, m : float, sequence of float, or None, optional
            Replace one ordinate by a scalar broadcast over vertices or a
            column with one value per coordinate. Omitted ordinates are
            preserved. ``None`` removes Z or M and is not valid for X or Y.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])])
        >>> arr.with_coordinates([(5, 5), (6, 6)]).to_wkt()
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
    def topological_dimension(self) -> np.ndarray[tuple[int], np.dtype[np.int64]]:
        """Per-geometry topological dimension — ``0`` point, ``1`` curve, ``2``
        areal (see `Geometry.topological_dimension`). Missing rows use ``-1``;
        use `.is_missing` to distinguish them from present point-like
        geometries.

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

        A geographic CRS gives ellipsoidal square meters for the region bounded
        by edges straight in longitude/latitude; a projected CRS gives squared
        native linear units; a CRS-free array gives squared raw coordinate units.
        Use ``to_crs`` to change frame.

        Each ``GeometryCollection`` row counts overlapping members once, as
        the area of their union. Parts of a valid ``MultiPolygon`` cannot
        overlap.

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

        A geographic CRS gives ellipsoidal meters along edges straight in
        longitude/latitude; a projected CRS gives native linear units; a CRS-free
        array gives raw coordinate units. Use ``to_crs`` to change frame.

        Geographic polygon perimeter measures straight chart edges and resolves
        seam aliases in the CRS-native periodic longitude frame.

        Each ``GeometryCollection`` row sums every member's length or
        perimeter, including overlapping members.

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

        Geographic CRSs do not support 3D Euclidean length.

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
            use `.row_index` to recover source-row alignment, or call
            `drop_missing()` first for an explicit dense path.
        """
    def __iter__(self) -> Iterator[_GeometryT_co]:
        """Iterate geometries in row order (including missing rows as ``None``).

        Returns
        -------
        iterator of Geometry or None
        """
    @overload
    def __getitem__(self, index: _IntArray0D, /) -> _GeometryT_co: ...
    @overload
    def __getitem__(self, index: _IntArray1D, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: _BoolArray1D, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(
        self, index: _IntArrayUnknownRank, /
    ) -> _GeometryT_co | GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _GeometryT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __getitem__(
        self,
        index: SupportsIndex
        | slice
        | _BoolArray1D
        | _BoolLane
        | _IndexLane
        | _IntArray0D
        | _IntArray1D
        | _IntArrayUnknownRank,
        /,
    ) -> _GeometryT_co | GeometryArray[_GeometryT_co]:
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

        Raises
        ------
        GeometryError
            If coordinate epoch metadata or a CRS that cannot be embedded as an
            EWKB SRID would be lost without ``drop_epoch`` or ``drop_crs``.
        ModuleNotFoundError
            If Polars is not installed.

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

        Raises
        ------
        GeometryError
            If the array carries a coordinate epoch and ``drop_epoch`` is false.
        ModuleNotFoundError
            If GeoPandas is not installed.

        Examples
        --------
        >>> import gometry as gm
        >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_geopandas()).__name__
        'GeoSeries'
        """
    def to_geoparquet(
        self,
        path: str | PathLike[str] | BinaryIO | PyArrowNativeFile,
        *,
        attributes: PyArrowTable | Mapping[str, Any] | None = None,
        encoding: Literal['wkb', 'native'] = 'wkb',
        **kwargs: Any,
    ) -> None:
        """Write this array to a GeoParquet file, optionally as a feature table.

        Parameters
        ----------
        path : str, path-like, or binary stream
            Output Parquet path or caller-owned binary writable stream. Text
            streams are not supported; the writer leaves streams open.
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
    @overload
    def to_polyline(
        self: GeometryArray[Geometry], *, precision: int = 5, drop_epoch: bool = False
    ) -> list[str]: ...
    @overload
    def to_polyline(
        self, *, precision: int = 5, drop_epoch: bool = False
    ) -> list[str | None]:
        """Encode every ``LineString`` or ``Point`` row as Google polyline text (see
        `Geometry.to_polyline`).
        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

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
            If ``precision`` is out of range, or the array carries a coordinate
            epoch and ``drop_epoch`` is false.

        See Also
        --------
        from_polyline : Decode Google polyline text into geometries.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])]).to_polyline()
        ['??_ibE_ibE']
        """
    def with_epoch(self, epoch: float | None, *, overwrite: bool = False) -> Self:
        """Declare (or clear) the array's coordinate epoch (see
        `Geometry.with_epoch`). The CRS frame must admit an epoch. ``None``
        clears it; changing a present epoch needs ``overwrite=True``.

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
            If the CRS frame cannot carry an epoch or a present epoch would
            change without ``overwrite=True``.
        GeometryError
            If ``epoch`` is not a finite decimal year.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(-122.4, 37.8, crs=4326)])
        >>> arr.with_epoch(2015.0).epoch
        2015.0
        """

    def with_crs(self, crs: CrsInput | None, *, overwrite: bool = False) -> Self:
        """Relabel the CRS of all geometries without moving coordinates (see
        `Geometry.with_crs`; replacing a different declared CRS requires
        ``overwrite=True``).

        Parameters
        ----------
        crs : CRS-like
            Any supported CRS-like input.

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
        >>> gm.GeometryArray([gm.Point(1, 2)]).with_crs(4326).crs
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
        (stamp it with ``with_epoch`` first when the operation needs one);
        ``epoch`` here labels the *output* coordinate epoch.

        Parameters
        ----------
        crs : CRS-like
            Any supported CRS-like input.

        area_of_interest : sequence of float, optional
            Bounding ``(west, south, east, north)`` to pick the best transform.

        epoch : float, optional
            Output coordinate epoch (decimal year) to tag on the result. Omitted
            keeps the source epoch when the CRS is unchanged or the target admits
            geometry epochs; otherwise the output has no epoch.

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
        >>> round(float(arr.to_crs(3857).coords.x[0]), 2)
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
            Ordered ``(minx, miny, maxx, maxy)`` rectangle for keying. When
            omitted, uses the present rows' minimum and maximum stored
            coordinates without periodic wrapping. Geographic ``total_bounds``
            may wrap, so it is not necessarily a curve frame.

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
    @overload
    def dissolve(
        self: GeometryArray[_GeometryT], by: Iterable[_GroupKeyT]
    ) -> tuple[GeometryArray[Geometry], list[_GroupKeyT]]: ...
    @overload
    def dissolve(
        self: GeometryArray[_GeometryT | None], by: Iterable[_GroupKeyT]
    ) -> tuple[GeometryArray[Geometry | None], list[_GroupKeyT]]:
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
        ['POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))']
        """
    @property
    def is_valid(self) -> npt.NDArray[np.bool_]:
        """Per-element validity mask (see `Geometry.is_valid`).

        Returns
        -------
        numpy.ndarray
            ``True`` where the geometry is valid, one entry per row.
        """
    @overload
    def concat(
        self: GeometryArray[_GeometryT_co],
        other: GeometryArray[_GeometryOtherT],
        *others: GeometryArray[_GeometryOtherT],
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT]: ...
    @overload
    def concat(
        self: GeometryArray[_GeometryT_co],
        other: GeometryArray[_GeometryOtherT | None],
        *others: GeometryArray[_GeometryOtherT | None],
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT | None]: ...
    @overload
    def concat(
        self: GeometryArray[_GeometryT_co],
    ) -> GeometryArray[_GeometryT_co]:
        """Concatenate one or more arrays sharing this array's CRS and epoch.

        Parameters
        ----------
        other, *others : GeometryArray
            Arrays to append in order; each must share this array's CRS and
            epoch. The zero-argument form omits both names and returns this array.

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
    def drop_missing(
        self: GeometryArray[_GeometryT | None],
    ) -> GeometryArray[_GeometryT]:
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
        self: GeometryArray[_GeometryT | None], value: _GeometryOtherT
    ) -> GeometryArray[_GeometryT | _GeometryOtherT]: ...
    @overload
    def fill_missing(
        self: GeometryArray[_GeometryT | None],
        value: GeometryArray[_GeometryOtherT | None],
    ) -> GeometryArray[_GeometryT | _GeometryOtherT]:
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
    ) -> GeometryArray[_GeometryT_co | _GeometryOtherT | None]:
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
    def _with_missing(
        self, mask: Sequence[bool] | npt.NDArray[np.bool_]
    ) -> GeometryArray[_GeometryT_co | None]:
        """Attach a missing mask to this array's rows (internal; the pandas
        bridge builds masked arrays without a rebuild). Length-checked;
        new bits are OR-merged with any existing mask so clearing a bit
        cannot expose NaN placeholders in packed storage as trusted geometry.
        All-present results normalize back to dense. Use ``fill_missing`` to
        replace missing rows with real geometry.

        The array's row count determines the expected mask length. The mask's
        length hint is ignored; its iterator supplies the values.
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
        uses ellipsoidal arclength along straight source-chart edges on geographic CRSs
        and planar distance otherwise; ``unit='planar'`` always uses Cartesian
        coordinate space. Coordinates are never moved. Geographic length, perimeter,
        and LRS use the same chart-edge model with CRS-aware units. Z is preserved;
        existing M requires ``overwrite=True``. Every stored vertex is retained; a
        zero-length X/Y segment has equal M values at both ends. Empty linework returns
        the same empty with an M axis.

        Parameters
        ----------
        start_m, end_m : float or sequence of float
            The measure range (finite, ``end_m >= start_m``) — a scalar applies to
            every geometry, or pass one value per geometry.
        overwrite : bool, default False
            Replace existing M ordinates instead of raising.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        GeometryArray
            One result per row (kinds preserved).


        Raises
        ------
        GeometryTypeError
            If the geometry is not lineal.
        InvalidGeometryError
            If the linework carries M without ``overwrite``.
        GeometryError
            If the measure range is invalid.

        See Also
        --------
        line_interpolate : Point at a distance or M location along the line.
        line_substring : Extract a contiguous portion of the line.
        line_locate : Locate the nearest route position on the line.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.interpolate_m(0.0, 100.0).to_wkt()
        'LINESTRING M (0 0 0, 10 0 100)'
        """

    @overload
    def polylabel(
        self: GeometryArray[Geometry],
        *,
        tolerance: FloatInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def polylabel(
        self: GeometryArray[Geometry | None],
        *,
        tolerance: FloatInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | None]:
        """Pole of inaccessibility: an approximate most-distant interior point
        for label placement. Uses a tolerance-based cell search; its clearance is within the requested tolerance
        of the global inradius for planar/projected input.
        Geographic line and ring boundaries may be refined in the local projection
        before the search; ``tolerance`` controls the search, not the separate
        input-refinement budget.
        For a nonempty polygon with no filled interior, returns the midpoint of its
        stored bounds with radius zero, including when its rings cancel the entire
        support.
        Empty geometries return ``POINT EMPTY``.

        See Also
        --------
        maximum_inscribed_circle : Certified filled disk from its own center search.
        centroid : Area/length-weighted center (may fall outside).
        point_on_surface : A representative point guaranteed on the geometry.

        Parameters
        ----------
        tolerance : float or sequence of float, optional
            Positive absolute bound on the planar global-inradius gap, interpreted
            for the CRS (see ``unit``) — a scalar applies to every geometry, or pass
            one value per geometry.
            Omitted selects a scale-aware tolerance independently for each geometry.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        GeometryArray[Point]
            One point per row.


        Raises
        ------
        GeometryError
            If an explicit ``tolerance`` is not positive and finite.
        InvalidGeometryError
            If the pole of inaccessibility cannot be computed.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).polylabel().to_wkt()
        'POINT (1 1)'
        """

    @overload
    def minimum_clearance_line(
        self: GeometryArray[Geometry], *, unit: DistanceUnit | None = None
    ) -> GeometryArray[LineString]: ...
    @overload
    def minimum_clearance_line(
        self: GeometryArray[Geometry | None], *, unit: DistanceUnit | None = None
    ) -> GeometryArray[LineString | None]:
        """Two-point line realizing `minimum_clearance`. The metric matches
        minimum_clearance: on a geographic CRS, the witness is selected in the
        geometry's best local projection and returned in source coordinates, so it is a
        local-projection approximation rather than an exact ellipsoidal clearance
        search. Original vertices remain clearance sites; inserted chart-edge support
        samples do not become new sites, and an on-edge witness is interpolated on the
        original source segment.
        ``LINESTRING EMPTY`` when the clearance is infinite (fewer than two distinct
        vertices).

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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

    @overload
    def to_geojson(
        self: GeometryArray[Geometry], *, drop_epoch: bool = False
    ) -> list[str]: ...
    @overload
    def to_geojson(self, *, drop_epoch: bool = False) -> list[str | None]:
        """Serialize to `GeoJSON` text. `GeoJSON` is WGS84 by specification (RFC 7946):
        CRS-tagged input must use the 2D longitude/latitude family
        (``EPSG:4326`` or ``OGC:CRS84``) or the 3D longitude/latitude/height family
        (``EPSG:4979`` or ``OGC:CRS84h``); reproject other CRS values first. CRS-free
        input is explicitly assumed to already be WGS84 longitude/latitude:
        coordinates outside longitude ``[-180, 180]`` or latitude ``[-90, 90]`` are
        rejected, and antimeridian-crossing geometry is split according to RFC 7946.
        If CRS-free coordinates have projected or other planar semantics, transform
        them or choose another format.

        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

        GeoJSON cannot represent M; drop it with ``with_coordinates(m=None)`` (or ``force_2d()``).

        Parameters
        ----------
        drop_epoch : bool, default False
            Permit losing coordinate-epoch metadata, which GeoJSON cannot encode.

        Returns
        -------
        list of str
            One `GeoJSON` geometry string per row; missing rows yield ``None``.

        Raises
        ------
        CRSError
            If the input carries a CRS outside the 2D/3D WGS84 longitude/latitude family.
        InvalidGeometryError
            If input carries M ordinates; drop them with ``with_coordinates(m=None)`` (or ``force_2d()``).
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

    @overload
    def boundary(
        self: GeometryArray[Point | MultiPoint | GeometryCollection],
    ) -> GeometryArray[GeometryCollection]: ...
    @overload
    def boundary(
        self: GeometryArray[LineString | MultiLineString],
    ) -> GeometryArray[MultiPoint]: ...
    @overload
    def boundary(
        self: GeometryArray[Polygon | MultiPolygon],
    ) -> GeometryArray[
        MultiPoint | LineString | MultiLineString | GeometryCollection
    ]: ...
    @overload
    def boundary(
        self: GeometryArray[Point | MultiPoint | GeometryCollection | None],
    ) -> GeometryArray[GeometryCollection | None]: ...
    @overload
    def boundary(
        self: GeometryArray[LineString | MultiLineString | None],
    ) -> GeometryArray[MultiPoint | None]: ...
    @overload
    def boundary(
        self: GeometryArray[Polygon | MultiPolygon | None],
    ) -> GeometryArray[
        MultiPoint | LineString | MultiLineString | GeometryCollection | None
    ]: ...
    @overload
    def boundary(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[
        MultiPoint | LineString | MultiLineString | GeometryCollection
    ]: ...
    @overload
    def boundary(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[
        MultiPoint | LineString | MultiLineString | GeometryCollection | None
    ]:
        """Return the topological boundary of the geometry's occupied support.

        Returns
        -------
        GeometryArray
            One boundary per row, retaining missing rows.

        Notes
        -----
        Each row is processed independently. Non-missing outputs retain their row's
        CRS and coordinate epoch. Output coordinate axes reflect ordinates available
        at every emitted vertex.

        Raises
        ------
        CRSError
            If the declared CRS cannot be resolved to a topology frame.
        InvalidGeometryError
            If a geographic coordinate read by boundary is outside its CRS domain.
        GeometryError
            If exact work exceeds the generated-item budget or emitted cells cannot
            be certified.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).boundary().to_wkt()
        'LINESTRING (0 0, 2 0, 2 2, 0 2, 0 0)'
        """

    @overload
    def triangulate(
        self: GeometryArray[Geometry],
        *,
        method: Literal['earcut'],
        min_angle: None = None,
        max_area: None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def triangulate(
        self: GeometryArray[Geometry | None],
        *,
        method: Literal['earcut'],
        min_angle: None = None,
        max_area: None = None,
    ) -> Groups[GeometryArray[Polygon] | None]: ...
    @overload
    def triangulate(
        self: GeometryArray[Geometry],
        *,
        method: Literal['delaunay'],
        min_angle: None = None,
        max_area: None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def triangulate(
        self: GeometryArray[Geometry | None],
        *,
        method: Literal['delaunay'],
        min_angle: None = None,
        max_area: None = None,
    ) -> Groups[GeometryArray[Polygon] | None]: ...
    @overload
    def triangulate(
        self: GeometryArray[Geometry],
        *,
        method: Literal['constrained'],
        min_angle: FloatInput | None = None,
        max_area: FloatInput | None = None,
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def triangulate(
        self: GeometryArray[Geometry | None],
        *,
        method: Literal['constrained'],
        min_angle: FloatInput | None = None,
        max_area: FloatInput | None = None,
    ) -> Groups[GeometryArray[Polygon] | None]:
        """Triangulate each geometry with an explicit algorithm.

        Parameters
        ----------
        method : {'earcut', 'delaunay', 'constrained'}
            Required triangulation algorithm. Delaunay methods resolve exact
            cocircular ties with a canonical rule keyed by lexicographic XY
            coordinates, independent of insertion order.
        min_angle : float or sequence of float, optional
            Minimum triangle angle in degrees; each value must be positive, finite,
            and at most 30. Valid only with ``method='constrained'``.

        max_area : float or sequence of float, optional
            Maximum triangle area in coordinate space; each value must be positive
            and finite. Geographic CRSs use squared native angular units, projected
            CRSs use squared native linear units, and CRS-free inputs use squared raw
            coordinate units. Valid only with ``method='constrained'``. Setting
            either option enables refinement;
            inserted Steiner vertices return uniform XY. Scalar and array rows use
            quality-first refinement: angle violations by circumradius-to-shortest-edge
            ratio, then area when both options are set; triangles meeting the angle
            target follow in descending area. Equal priorities use sorted numeric XY
            corners. Refinement is independent of insertion order and input ring
            rotation or reversal. Scalar and array rows use
            the same first-occurrence source-site and ordinate policy. Each boundary
            Steiner vertex is the nearest binary64 coordinate pair to its exact
            position on the source edge, so the triangles cover the source polygon
            up to that rounding.

        Returns
        -------
        Groups of GeometryArray[Polygon]
            One ragged triangle group per input geometry. Delaunay methods return
            counter-clockwise triangles starting at their smallest XY corner,
            sorted by corner coordinates within each group.

        Raises
        ------
        GeometryError
            If a quality constraint is invalid or method-specific options are used
            with the wrong algorithm. A boundary Steiner point that cannot be
            represented at float resolution raises this error with
            ``operation='triangulate'``.
        InvalidGeometryError
            If an input polygon is invalid or its triangulation cannot be constructed.

        Examples
        --------
        >>> import gometry as gm
        >>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
        >>> groups[0].to_wkt()
        ['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 2, 0 2, 2 0, 2 2))']
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

    @overload
    def line_merge(
        self: GeometryArray[LineString],
    ) -> GeometryArray[LineString]: ...
    @overload
    def line_merge(
        self: GeometryArray[LineString | None],
    ) -> GeometryArray[LineString | None]: ...
    @overload
    def line_merge(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[LineString | MultiLineString]: ...
    @overload
    def line_merge(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[LineString | MultiLineString | None]:
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

    @overload
    def minimum_rotated_rectangle(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[Point | LineString | Polygon]: ...
    @overload
    def minimum_rotated_rectangle(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[Point | LineString | Polygon | None]:
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

    @overload
    def clip_by_rect(
        self: GeometryArray[Point],
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[Point]: ...
    @overload
    def clip_by_rect(
        self: GeometryArray[MultiPoint],
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[MultiPoint | Point]: ...
    @overload
    def clip_by_rect(
        self: GeometryArray[Point | None],
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def clip_by_rect(
        self: GeometryArray[MultiPoint | None],
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[MultiPoint | Point | None]: ...
    @overload
    def clip_by_rect(
        self: GeometryArray[Geometry],
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def clip_by_rect(
        self: GeometryArray[Geometry | None],
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
    ) -> GeometryArray[Geometry | None]:
        """Return the intersection with the closed rectangle
        ``(minx, miny, maxx, maxy)``. Its WKB is identical to
        ``intersection(self, box(minx, miny, maxx, maxy))`` when the box carries the
        same CRS and coordinate epoch. Point-only and edge-only
        contacts are retained; equal ``minx``/``maxx`` or ``miny``/``maxy`` select a
        closed line or point.

        When the closed rectangle contains the subject's raw bounds, the subject is
        returned verbatim for Cartesian inputs and canonical seam-free geographic bounds.

        Source ordinates are carried where meaningful; synthesized clip vertices use
        the operation's natural XY result.

        In a geographic CRS, ``minx``/``maxx`` are the west/east longitudes and must be
        within the CRS frame's native half-period. ``west <= east`` selects the ordinary
        closed interval; ``west > east`` selects the closed interval crossing the
        longitude seam. The endpoints ``-half_period`` and ``half_period`` select a full
        longitude band, and equal endpoints select a zero-width meridian. ``miny`` and
        ``maxy`` are the south/north latitudes and must lie within the frame's poles,
        with ``south <= north``. To construct a box for that interval, use :func:`box`
        with ``wrap='split'`` and the same CRS.

        Geographic inputs use frame-aware overlay so longitude aliases, seam crossings,
        and pole vertices follow the CRS topology. Geographic rectangle horizontal
        edges follow parallels and use the same construction as :func:`box` when that
        box uses the same CRS and coordinate epoch.

        Parameters
        ----------
        minx, miny, maxx, maxy : float
            The Cartesian bounds, or the geographic west, south, east, and north
            endpoints respectively.

        Returns
        -------
        GeometryArray
            One clipped geometry per row.


        Raises
        ------
        GeometryError
            If the rectangle coordinates are non-finite, south exceeds north, or a
            geographic endpoint is outside its CRS frame, or no adjacent binary64
            assignment preserves the exact clipped result or the bounded embedding
            search is exhausted.
        InvalidGeometryError
            If the input contains coordinates outside its geographic CRS domain.

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
        self: GeometryArray[Geometry],
        start: FloatInput,
        end: FloatInput,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[LineString | Point]: ...
    @overload
    def line_substring(
        self: GeometryArray[Geometry | None],
        start: FloatInput,
        end: FloatInput,
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[LineString | Point | None]: ...
    @overload
    def line_substring(
        self: GeometryArray[Geometry],
        start: FloatInput,
        end: FloatInput,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | Point]: ...
    @overload
    def line_substring(
        self: GeometryArray[Geometry | None],
        start: FloatInput,
        end: FloatInput,
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | Point | None]:
        """Return the portion of linework from ``start`` through ``end``. Empty
        linework yields ``LINESTRING EMPTY``.

        Both bounds are inclusive. Stored vertices at zero-length X/Y segments consume
        no distance but remain in the result when their active ordinates differ,
        including consecutive vertices with identical X/Y. This applies to distance
        and M basis.

        With CRS-aware units, geographic cut points are inverted by ellipsoidal
        arclength along straight source-chart edges. New endpoints embedded in a
        retained source edge keep that edge's continuous longitude spelling.
        ``unit='planar'`` instead interpolates in the stored coordinate space.

        Parameters
        ----------
        start, end : float or sequence of float
            Ordered locations on the selected basis. Distance values follow the CRS;
            M values are stored route measures. A scalar applies to every array row.

        basis : {'distance', 'm'}
            Use CRS-aware distance, or the line's monotonic M ordinate. Distance forms
            omit ``basis``; M stationing requires ``basis='m'``. Its M fraction selects
            the same arclength fraction on each edge: ellipsoidal chart-edge arclength
            on a geographic CRS, planar length otherwise.

        normalized : bool, default False
            Interpret distance-basis positions as fractions of the route length under
            the selected metric. Changing ``unit`` can change the cut points. Invalid
            with ``basis='m'``.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            Available only with ``basis='distance'``; invalid with
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
            If M values are missing or non-monotonic; an array row degrades to an
            empty result instead.
        GeometryError
            If locations are non-finite or out of order, or a distance-only option is
            used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a location along the line.
        line_locate : Locate the nearest route position on the line.
        interpolate_m : Assign M ordinates along arc length.

        Examples
        --------
        >>> import gometry as gm
        >>> line = gm.LineString([(0, 0), (10, 0)])
        >>> line.line_substring(2, 6).to_wkt()
        'LINESTRING (2 0, 6 0)'
        """

    @overload
    def buffer(
        self: GeometryArray[Point],
        distance: FloatInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon]: ...
    @overload
    def buffer(
        self: GeometryArray[Point | None],
        distance: FloatInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon | None]: ...
    @overload
    def buffer(
        self: GeometryArray[Geometry],
        distance: float,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon | MultiPolygon]: ...
    @overload
    def buffer(
        self: GeometryArray[Geometry | None],
        distance: float,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon | MultiPolygon | None]: ...
    @overload
    def buffer(
        self: GeometryArray[Geometry],
        distance: FloatInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon | MultiPolygon]: ...
    @overload
    def buffer(
        self: GeometryArray[Geometry | None],
        distance: FloatInput,
        *,
        cap_style: CapStyle = 'round',
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        side: BufferSide = 'both',
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Polygon | MultiPolygon | None]:
        """Buffer a geometry by ``distance``, returning the offset region (measured for
        the CRS).

        On a geographic CRS, stored source-chart edges are refined when their projected
        curves cannot be certified within the local-frame input budget before the planar
        kernel runs. This input-fidelity budget is separate from the 0.1% frame-fit
        limit and from round-arc resolution, which ``quadrant_segments`` controls; it
        does not promise an end-to-end ellipsoidal distance error.
        For single-sided buffers, chord directions determine endpoint normals, so the
        input-fidelity budget does not bound output endpoint displacement.

        Parameters
        ----------
        distance : float or sequence of float
            Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
            meters on a geographic CRS, native linear units on a projected CRS, raw
            coordinate units when CRS-free — a scalar applies to every geometry, or pass one value
            per geometry. Every distance must be finite.
        cap_style : {'round', 'flat', 'square'}, default 'round'
            End-cap shape at the original first and last vertices of open lines. Closed lines have no end caps.
            Point inputs always buffer to round discs, and MultiPoint buffers are
            unions of round discs; `cap_style` does not affect either. Sided buffers
            always use flat ends, so this parameter applies to ``side='both'`` only.

        join_style : {'round', 'miter', 'bevel'}, default 'round'
            Interior corner style for both two-sided and sided line buffers, including
            180-degree reversals.

        quadrant_segments : int, default 8
            Segments used to approximate a quarter-circle join in either buffer mode.

        miter_limit : float, default 5.0
            With ``join_style='miter'``: how far a mitered corner may reach, in
            multiples of ``distance``; sharper corners are clipped flat at that reach.
            Applies to both two-sided and sided buffers; must be positive and finite.

        side : {'both', 'left', 'right'}, default 'both'
            Which side(s) of lineal input to buffer. ``'left'``/``'right'`` build
            the one-sided strip between the line and its offset curve, with flat ends.
            These sides honor the join parameters: ``join_style``,
            ``quadrant_segments``, and ``miter_limit``. The distance must be
            non-negative.

            Buffer boundaries are synthesized and therefore returned in XY; Z/M are
            not fabricated.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        GeometryArray
            One offset region per row.


        Raises
        ------
        InvalidGeometryError
            If coordinates are non-finite or buffer arithmetic produces a non-finite
            derived coordinate.
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
        >>> round(disc.area)  # ~ pi * 10²
        312
        """

    @overload
    def unique_points(
        self: GeometryArray[
            Point | MultiPoint | LineString | MultiLineString | Polygon | MultiPolygon
        ],
    ) -> GeometryArray[MultiPoint]: ...
    @overload
    def unique_points(
        self: GeometryArray[
            Point
            | MultiPoint
            | LineString
            | MultiLineString
            | Polygon
            | MultiPolygon
            | None
        ],
    ) -> GeometryArray[MultiPoint | None]: ...
    @overload
    def unique_points(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[MultiPoint | GeometryCollection]: ...
    @overload
    def unique_points(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[MultiPoint | GeometryCollection | None]:
        """Distinct vertices in first-occurrence order. Vertices compare by exact
        structural identity (every active ordinate by bit pattern, the
        ``equals_identical`` notion), so XYZ points that differ only in Z stay
        distinct.

        Returns
        -------
        GeometryArray
            One distinct-vertex result per row: a ``MultiPoint`` for homogeneous rows or
            a ``GeometryCollection`` of ordered ``Point`` members for heterogeneous rows.

        Notes
        -----
        A heterogeneous result follows ordinary ``GeometryCollection`` semantics:
        ``is_simple`` is false by convention and structural equality remains type-sensitive.
        Mixed-axis and collection exports use WKB for Arrow interchange; see
        [Arrow & storage](../../ecosystem/arrow.md).

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
        - ``'dp'`` (Douglas-Peucker) compares exact distances to the closed retained
          chord against ``tolerance``. Equal maximum distances select the earliest
          source vertex.

        Parameters
        ----------
        tolerance : float or sequence of float
            Distance scale of removable detail in coordinate space — native angular
            units for geographic CRSs, native linear units for projected CRSs, and raw
            coordinate units when CRS-free. A scalar applies to every geometry, or
            pass one value per geometry.
        method : {'vw', 'dp'}, default 'vw'
            ``'vw'`` is area-based (Visvalingam-Whyatt); ``'dp'`` is distance-
            based (Douglas-Peucker).

        preserve_topology : bool, default True
            Preserve topology within each geometry checked: valid polygonal components
            stay valid and non-collapsed, and simple lineal components stay simple. A
            ``GeometryCollection`` is checked child by child; relationships between
            separate children are not preserved. The raw algorithm runs first (the
            fast path); a guarded pass only runs when a component fails those checks.
            ``False`` skips the guarded check; collapsed areal rings are still removed.

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

    @overload
    def repair(
        self: GeometryArray[Point],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Point]: ...
    @overload
    def repair(
        self: GeometryArray[MultiPoint],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[MultiPoint]: ...
    @overload
    def repair(
        self: GeometryArray[MultiLineString],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[
        Point | MultiPoint | LineString | MultiLineString | GeometryCollection
    ]: ...
    @overload
    def repair(
        self: GeometryArray[GeometryCollection],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[GeometryCollection]: ...
    @overload
    def repair(
        self: GeometryArray[LineString],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Point | LineString | MultiLineString]: ...
    @overload
    def repair(
        self: GeometryArray[Polygon | MultiPolygon],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Polygon | MultiPolygon]: ...
    @overload
    def repair(
        self: GeometryArray[Point | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Point | None]: ...
    @overload
    def repair(
        self: GeometryArray[MultiPoint | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[MultiPoint | None]: ...
    @overload
    def repair(
        self: GeometryArray[MultiLineString | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[
        Point | MultiPoint | LineString | MultiLineString | GeometryCollection | None
    ]: ...
    @overload
    def repair(
        self: GeometryArray[GeometryCollection | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[GeometryCollection | None]: ...
    @overload
    def repair(
        self: GeometryArray[LineString | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Point | LineString | MultiLineString | None]: ...
    @overload
    def repair(
        self: GeometryArray[Polygon | MultiPolygon | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Polygon | MultiPolygon | None]: ...
    @overload
    def repair(
        self: GeometryArray[Geometry],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Geometry]: ...
    @overload
    def repair(
        self: GeometryArray[Geometry | None],
        *,
        method: RepairMethod = 'linework',
    ) -> GeometryArray[Geometry | None]:
        """Repair invalid geometry, returning corrected result(s) (OGC). Already-valid
        input is returned unchanged at validation cost. Geographic repair checks exact
        quotient validity before rebuilding; when output construction needs seam
        vertices it splits the source while preserving traceable Z/M ordinates.
        Projected and CRS-free geometry remains planar. A collapsed LineString becomes
        a Point at its first coordinate. Collapsed MultiLineString members become a
        Point or MultiPoint, combined with surviving linework in a GeometryCollection
        when both dimensions remain.

        Parameters
        ----------
        method : {'linework', 'structure'}, default linework
            Repair strategy: ``linework`` nodes all boundary linework and selects
            regions from each original ring occurrence's winding modulo two before
            recombining each polygon's shell minus its holes; distinct polygon members
            are unioned. ``structure`` rebuilds each ring's nonzero-winding enclosed
            area and recombines it as shells minus holes.

            Z/M are carried at vertices traceable to the input; a rebuild that needs
            unsourceable vertices returns the mathematically natural XY result.

        Returns
        -------
        GeometryArray
            One valid geometry per row.


        Raises
        ------
        GeometryError
            If no adjacent binary64 assignment preserves the exact repaired geometry
            or the bounded embedding search is exhausted.
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
        point: Point | GeometryArray[Point | None],
        *,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> npt.NDArray[np.float64]: ...
    @overload
    def line_locate(
        self,
        point: Point | GeometryArray[Point | None],
        *,
        basis: Literal['distance'] = 'distance',
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> npt.NDArray[np.float64]:
        """Locate the position on linework nearest ``point``. Empty linework, or
        an empty query point, locates to ``nan``.

        Projection is in X/Y. Zero-length X/Y segments add no distance-basis projection.
        Planar distance-basis locate selects the exactly nearest segment; exact ties
        prefer an interior projection over an endpoint, then the first segment in route
        order, matching planar proximity. Geographic locate applies the same role and
        source order when evaluated backend distances compare equal. It ranks available
        measurements, including the chart projection trial, rather than asserting an
        exact geodesic minimum. With ``basis='m'``, the returned M is taken from the
        selected projection.

        With CRS-aware units on a geographic CRS, locate minimizes geodesic distance to
        the straight chart edges and returns cumulative ellipsoidal arclength to the
        nearest point. ``unit='planar'`` instead uses Cartesian distance in the stored
        coordinate space.

        Parameters
        ----------
        point : Point or GeometryArray[Point | None]
            A point to project, or one point per line row. Missing query rows produce
            ``nan`` in the result column.

        basis : {'distance', 'm'}
            Return a CRS-aware distance, or the line's monotonic M ordinate. Distance
            forms omit ``basis``; M stationing requires ``basis='m'``. M-based
            projection follows the same chart-edge metric: ellipsoidal arclength on a
            geographic CRS, planar length otherwise.

        normalized : bool, default False
            Return a distance-basis fraction in [0, 1] under the selected metric.
            Changing ``unit`` can change the nearest projection and returned fraction.
            Invalid with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            Available only with ``basis='distance'``; invalid with
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
            If M values are missing or non-monotonic; an array row degrades to
            ``nan`` instead.
        GeometryError
            If a distance-only option is used with ``basis='m'``.

        See Also
        --------
        line_interpolate : Point at a route station; locate/interpolate round-trip only at a unique nearest position.
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

    @overload
    def self_intersections(
        self: GeometryArray[Geometry],
    ) -> Groups[GeometryArray[Point]]: ...
    @overload
    def self_intersections(
        self: GeometryArray[Geometry | None],
    ) -> Groups[GeometryArray[Point] | None]:
        """Return points where the geometry coincides with itself. Reports proper linework
        self-crossings, non-adjacent touches, the endpoints of collinear overlaps
        (spikes and backtracks), contact between distinct parts, and duplicate point
        coordinates; legal adjacent shared vertices, ring closures, and removable
        repeated consecutive vertices are not nodes. For point/lineal input the
        result is non-empty exactly when is_simple is ``False``; areal input
        diagnoses its rings' linework, and collections are diagnosed recursively.
        Geographic diagnostics follow the CRS longitude quotient; projected and
        CRS-free geometry remains planar. Points are XY only.

        Returns
        -------
        Groups
            One ``GeometryArray`` of distinct self-intersection points per
            element; missing rows yield ``None`` groups.

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
        Original vertices remain clearance sites. Refined chart-edge samples improve
        boundary support without becoming new sites; an on-edge witness is interpolated
        on its original source segment.
        Projected CRS and CRS-free geometries measure in the active planar units.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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
        self: GeometryArray[Geometry],
        at: FloatInput,
        *,
        basis: Literal['distance'] = 'distance',
        count: None = None,
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self: GeometryArray[Geometry | None],
        at: FloatInput,
        *,
        basis: Literal['distance'] = 'distance',
        count: None = None,
        normalized: bool = False,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def line_interpolate(
        self: GeometryArray[Geometry],
        at: None = None,
        *,
        count: _IntInput,
        basis: Literal['distance'] = 'distance',
        normalized: Literal[False] = False,
        unit: DistanceUnit | None = None,
    ) -> Groups[GeometryArray[Point]]: ...
    @overload
    def line_interpolate(
        self: GeometryArray[Geometry | None],
        at: None = None,
        *,
        count: _IntInput,
        basis: Literal['distance'] = 'distance',
        normalized: Literal[False] = False,
        unit: DistanceUnit | None = None,
    ) -> Groups[GeometryArray[Point] | None]: ...
    @overload
    def line_interpolate(
        self: GeometryArray[Geometry],
        at: FloatInput,
        *,
        count: None = None,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[Point]: ...
    @overload
    def line_interpolate(
        self: GeometryArray[Geometry | None],
        at: FloatInput,
        *,
        count: None = None,
        basis: Literal['m'],
        normalized: Literal[False] = False,
        unit: None = None,
    ) -> GeometryArray[Point | None]:
        """Interpolate point locations along linework. Empty linework yields
        ``POINT EMPTY`` at every location.

        Distance-basis routes retain vertices at zero-length X/Y segments. At an
        interior prefix shared by such a run, an exact boundary selects its first
        stored vertex; normalized ``0`` and ``1`` select the first and last stored
        vertices. M-basis interpolation follows the stored monotonic measure and keeps
        vertices whose active Z/M ordinates differ even when X/Y is equal.

        With CRS-aware units (the default on a geographic CRS), each source edge is
        straight in the CRS's longitude and latitude chart and distance is ellipsoidal
        arclength along that edge. An independently interpolated point uses canonical
        longitude; original endpoints retain their stored spelling. ``unit='planar'``
        instead uses Cartesian distance in the stored coordinate space.

        Parameters
        ----------
        at : float or sequence of float, optional
            One location or many explicit distance-basis locations. Under
            ``basis='m'``, pass one stored M value for a scalar result or a sequence of
            stored M values for one line; an array accepts one M value per row.


        count : int or iterable of int, optional
            Number of evenly spaced distance-basis samples per row (``>= 1``) along the
            metric selected by ``unit``. A scalar broadcasts; otherwise pass one count
            per row. Mutually exclusive with ``at`` and unavailable with ``basis='m'``;
            ``normalized=True`` is invalid because count already supplies fractional
            positions.

        basis : {'distance', 'm'}
            Use CRS-aware distance, or the line's monotonic M ordinate. Distance forms
            omit ``basis``; M stationing requires ``basis='m'``. Its M fraction selects
            the same arclength fraction on each edge: ellipsoidal chart-edge arclength
            on a geographic CRS, planar length otherwise.

        normalized : bool, default False
            Interpret distance-basis ``at`` values as fractions of the route length
            under the selected metric. Changing ``unit`` can change their positions.
            Invalid with ``basis='m'``.

        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            Available only with ``basis='distance'``; invalid with
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
            If M values are missing or non-monotonic; an array row degrades to an
            empty result instead.
        GeometryError
            If input forms conflict, a value is non-finite, or a distance-only option
            is used with ``basis='m'``.

        See Also
        --------
        line_locate : Locate the nearest route position; repeated support makes the inverse non-unique.
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

    @overload
    def maximum_inscribed_circle(
        self: GeometryArray[Geometry],
        *,
        tolerance: FloatInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | Polygon]: ...
    @overload
    def maximum_inscribed_circle(
        self: GeometryArray[Geometry | None],
        *,
        tolerance: FloatInput | None = None,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | Polygon | None]:
        """Largest circle inscribed in a polygonal geometry. Unlike Shapely's
        same-named operation, which returns a two-point ``LineString`` from center to
        boundary, gometry returns a contained filled polygonal approximation (or a
        ``Point`` for a degenerate radius). The returned polygon's centroid gives the
        circle center; ``maximum_inscribed_radius`` gives its numerical radius.
        Planar/projected input uses one deterministic representable center and
        certifies the global inradius against the requested absolute tolerance.
        Geographic input retains the documented local-projection approximation.
        Empty geometries return ``POLYGON EMPTY``.
        A nonempty polygon with no filled interior returns the midpoint of its stored
        bounds with radius zero, including when its rings cancel the entire support.
        Geographic line and ring inputs may be refined in the source chart to bound
        projected input-edge deviation; this does not change round-arc resolution.
        Mirrors minimum_bounding_circle.

        Parameters
        ----------
        tolerance : float or sequence of float, optional
            Positive absolute bound on the planar global-inradius gap — a scalar
            applies to every geometry, or pass one value per geometry. Omitted
            selects a scale-aware tolerance independently for each geometry.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        GeometryArray
            The fixed-resolution filled polygonal approximation (or center `Point`
            when degenerate) per row.

        See Also
        --------
        polylabel : Approximate pole-of-inaccessibility label point.
        maximum_inscribed_radius : The radius alone.
        point_on_surface : A representative point guaranteed on the geometry.
        centroid : Area/length-weighted center (may fall outside).

        Raises
        ------
        GeometryError
            If ``tolerance`` is not positive and finite, or a finite planar/projected
            result cannot be certified and represented as a contained polygon.
        InvalidGeometryError
            If invalid polygonal structure prevents the operation from completing.

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
        """Radius of the largest inscribed circle. For planar/projected input,
        one deterministic representable center is retained and the returned
        downward-rounded boundary clearance of that underlying circle certifies
        the global inradius against the requested absolute tolerance. Geographic
        input uses the documented local-projection approximation. The numeric twin
        of minimum_bounding_radius; the polygonal approximation is
        maximum_inscribed_circle.
        Geographic boundary edges may be refined before the planar search; ``tolerance``
        controls the inradius search, not that input-refinement budget.

        Parameters
        ----------
        tolerance : float or sequence of float, optional
            Positive absolute bound on the planar global-inradius gap — a scalar
            applies to every geometry, or pass one value per geometry. Omitted
            selects a scale-aware tolerance independently for each geometry.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        numpy.ndarray of float
            The inscribed radius per row; ``NaN`` where empty.


        Raises
        ------
        GeometryError
            If ``tolerance`` is not positive and finite, or ``unit='meters'`` is
            requested for a CRS-free geometry.
        CRSError
            If the CRS lacks linear axis units for a metric result.

        See Also
        --------
        maximum_inscribed_circle : The contained polygonal approximation.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.box(0, 0, 2, 2).maximum_inscribed_radius()
        1.0
        """

    @overload
    def minimum_bounding_circle(
        self: GeometryArray[Geometry],
        *,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | Polygon]: ...
    @overload
    def minimum_bounding_circle(
        self: GeometryArray[Geometry | None],
        *,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | Polygon | None]:
        """Smallest circle enclosing the geometry, as a polygon. The standard shape:
        for planar/projected input, ``minimum_bounding_radius`` is the correctly rounded
        mathematical minimum radius and this method realizes that disk as a certified
        circumscribed 64-gon. Every edge encloses the disk and every vertex lies outside
        it. A single distinct vertex returns itself; empty input returns
        ``POLYGON EMPTY``. CRS-aware via local projection (approximate) on a
        geographic CRS; projected CRS distances default to the CRS's native linear units
        and scale through the CRS linear unit only with ``unit='meters'``. Geographic
        line and ring inputs use certified source-chart edge refinement in that local
        projection before their enclosing support circle is computed.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        GeometryArray
            The smallest enclosing circle per row.


        Raises
        ------
        GeometryError
            If the correctly rounded circle cannot be emitted as a finite, valid,
            source-enclosing polygon without changing its mathematical center or scalar
            radius.
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
        """Radius of the smallest circle enclosing the geometry. For planar/projected
        input it is rounded from the exact source minimum; this is the numeric twin of
        minimum_bounding_circle, and both use the same exact support kernel. Empty
        input yields ``NaN``; a single distinct vertex yields ``0``. Geographic point
        sets with three or more distinct vertices use the planar center and
        ellipsoidal support measurement (an approximation); two-point geographic
        inputs are the exact geodesic half-distance. For a projected
        ``unit='meters'`` result, the exact squared radius is scaled before the one
        binary64 rounding. Geographic line and ring inputs instead use the same
        local-projection support calculation as ``minimum_bounding_circle`` after
        chart-edge refinement. A two-coordinate line is edge-bearing and does not use
        the point-set two-point geodesic case.

        Parameters
        ----------
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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

    @overload
    def concave_hull(
        self: GeometryArray[Geometry],
        *,
        concavity: FloatInput = 2.0,
        length_threshold: FloatInput = 0.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | LineString | Polygon]: ...
    @overload
    def concave_hull(
        self: GeometryArray[Geometry | None],
        *,
        concavity: FloatInput = 2.0,
        length_threshold: FloatInput = 0.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[Point | LineString | Polygon | None]:
        """Compute the concave hull of the geometry. CRS-aware via local projection
        (approximate) and does NOT auto-split antimeridian-crossing geographic input;
        call ``split_antimeridian`` first. Uses gometry's chi-shape kernel: Delaunay
        boundary triangles are peeled from longest edge to shortest. The geometric XY
        hull is independent of input point order. Hull vertices are copies of input
        vertices, so X/Y/Z/M ordinates are preserved exactly. When several input vertices
        share an XY position, the first in storage order is the survivor.

        Parameters
        ----------
        concavity : float or sequence of float, default 2.0
            Sets the peel distance guard: a candidate triangle's opposite vertex must
            lie within ``edge_length / concavity`` of its boundary edge. ``0`` disables
            this guard. Higher values typically keep more area, but the greedy peel
            does not guarantee area monotonicity as ``concavity`` increases — a scalar
            applies to every geometry, or pass one value per geometry. Each value must
            be finite and non-negative.
        length_threshold : float or sequence of float, default 0.0
            Boundary edges at or below this length are kept; each value must be finite
            and non-negative. For fixed input and
            ``concavity``, a larger threshold always gives a hull that contains the
            smaller-threshold hull, so area never decreases. Interpreted for the CRS
            (see ``unit``). On a geographic CRS the threshold is evaluated in a local
            projection, while the output vertices are emitted from the original input
            coordinates — a scalar applies to every geometry, or pass one value per
            geometry.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        GeometryArray
            One concave hull per row; degenerate inputs reduce dimension.


        Raises
        ------
        GeometryError
            If parameters are non-finite or negative, or selected hull vertices have different
            coordinate axis layouts.

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

    @overload
    def point_on_surface(self: GeometryArray[Geometry]) -> GeometryArray[Point]: ...
    @overload
    def point_on_surface(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[Point | None]:
        """Representative point guaranteed to lie on the geometry. Geographic (lon/lat)
        input crossing the antimeridian is auto-split-normalized; no manual
        ``split_antimeridian`` is required. The point lies inside or on the geometry,
        unlike centroid. Polygonal scanline candidates are checked after rounding; an
        uncovered candidate falls back to a source boundary point. The representative
        is computed in XY and does not imply a source Z/M. On planar polygonal input,
        the first polygon component wins when representative chords have equal width.

        See Also
        --------
        centroid : Area/length-weighted center (may fall outside).
        polylabel : Pole of inaccessibility (best label anchor).


        Returns
        -------
        GeometryArray[Point]
            One point inside or on each row's geometry.


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

    @overload
    def subdivide(
        self: GeometryArray[Geometry], *, max_vertices: int = 256
    ) -> Groups[GeometryArray[Geometry]]: ...
    @overload
    def subdivide(
        self: GeometryArray[Geometry | None], *, max_vertices: int = 256
    ) -> Groups[GeometryArray[Geometry] | None]:
        """Split geometry into parts of bounded complexity. Recursively clips
        across the longer axis until every part has at most ``max_vertices``
        coordinates (the PostGIS ``ST_Subdivide`` shape). Linework with no
        representable rectangular cut is divided between source vertices. Parts cover
        the input up to binary64 rounding at constructed cut vertices. Source
        ordinates are carried where meaningful; synthesized clip vertices use the
        operation's natural XY result.

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
            If ``max_vertices`` is below ``8``, exact partitioning cannot produce
            parts within the requested bound, no adjacent binary64 assignment
            preserves the exact subdivision, or the bounded embedding search is
            exhausted.

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

    @overload
    def extremes(self: GeometryArray[Geometry]) -> Extremes[GeometryArray[Point]]: ...
    @overload
    def extremes(
        self: GeometryArray[Geometry | None],
    ) -> Extremes[GeometryArray[Point | None]]:
        """Return the west, south, east, and north extreme vertices of the
        geometry (numeric X/Y; ties keep the first vertex in storage order).

        Returns
        -------
        Extremes
            Four row-aligned ``Point`` arrays as ``(west, south, east, north)``.
            Missing rows stay missing in every column; empty rows degrade to an
            empty point per column.


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
        path: None = None,
        unit: DistanceUnit | None = None,
    ) -> Self: ...
    @overload
    def segmentize(
        self,
        max_length: FloatInput,
        /,
        *,
        fraction: None = None,
        path: NavigationPath,
        unit: None = None,
    ) -> Self: ...
    @overload
    def segmentize(
        self,
        max_length: None = None,
        /,
        *,
        fraction: FloatInput,
        path: NavigationPath | None = None,
        unit: None = None,
    ) -> Self:
        """Densify linework by inserting vertices so no segment exceeds
        ``max_length`` (or a fraction of its length).

        By default, each geographic edge keeps its stored shape: the straight line
        between its longitude/latitude vertices. ``max_length`` is measured along that
        edge on the ellipsoid. ``path='geodesic'`` or ``path='rhumb'`` instead places
        new vertices along the shortest ellipsoidal route or the constant-bearing route
        between each pair of endpoints. The result remains a polyline with straight
        longitude/latitude edges, so its length approaches the selected route length as
        the pieces get shorter. Projected geometries use native linear units, and
        CRS-free geometries use raw coordinate units. Every original vertex survives
        unchanged — this operation only inserts. Consecutive equal-X/Y vertices with
        distinct active Z/M ordinates remain in their stored order; their zero-length
        segments are not subdivided.

        Parameters
        ----------
        max_length : float or sequence of float, optional
            Positive maximum segment length, interpreted for the CRS (see ``unit``) —
            a scalar applies to every geometry, or pass one value per geometry. Pass this
            positional argument or use ``fraction``, but not both.

        fraction : float or sequence of float, optional
            Fraction in ``(0, 1]`` of each source segment. Keyword-only; a scalar
            applies to every geometry, or pass one value per geometry.
        path : {'geodesic', 'rhumb'}, optional
            Route for inserted geographic vertices. ``None`` keeps each stored edge;
            ``'geodesic'`` follows the shortest ellipsoidal route and is planar for
            projected or CRS-free inputs; ``'rhumb'`` follows a constant-bearing route
            and requires a geographic CRS. ``fraction`` is measured along the selected
            route.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.

            The override applies only to ``max_length``; ``fraction`` has no unit
            and cannot be combined with ``unit``. ``path`` cannot be combined with
            ``unit``; geographic route distances are meters.

        Returns
        -------
        GeometryArray
            One segmentized geometry per row (kinds preserved).


        Raises
        ------
        CRSError
            If ``unit='meters'`` is requested and the CRS lacks linear axis units, or
            ``path='rhumb'`` is requested without a geographic CRS, or the selected
            geodesic route has multiple shortest paths.
        GeometryError
            If neither or both constraints are supplied, ``max_length`` is not a
            positive finite number, ``fraction`` is outside ``(0, 1]``, ``unit`` is
            combined with ``fraction``, ``path`` is combined with ``unit``,
            ``unit='meters'`` is requested for a CRS-free geometry, or the
            endpoint-preserving binary64 output cannot satisfy ``max_length``.

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

    @overload
    def voronoi(
        self: GeometryArray[Geometry],
        *,
        output: Literal['polygons'] = 'polygons',
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def voronoi(
        self: GeometryArray[Geometry | None],
        *,
        output: Literal['polygons'] = 'polygons',
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> Groups[GeometryArray[Polygon] | None]: ...
    @overload
    def voronoi(
        self: GeometryArray[Geometry],
        *,
        output: Literal['edges'],
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> Groups[GeometryArray[LineString]]: ...
    @overload
    def voronoi(
        self: GeometryArray[Geometry | None],
        *,
        output: Literal['edges'],
        tolerance: float = 0.0,
        clip: VoronoiClip | Polygon = 'padded',
    ) -> Groups[GeometryArray[LineString] | None]:
        """Construct a Voronoi diagram from the geometry's vertices. Operates in
        planar longitude/latitude space and does NOT auto-split antimeridian-crossing
        geographic input; call ``split_antimeridian`` first. Collinear vertices divide
        the clip region into strips at their perpendicular bisectors, and a lone
        vertex owns the whole clip polygon; the padded and envelope frames of a lone
        vertex, or the envelope of a horizontal or vertical row, bound no cells.

        Parameters
        ----------
        output : {'polygons', 'edges'}
            Return clipped Voronoi cells or the internal Voronoi edges. The polygon
            form omits ``output``; edge output requires ``output='edges'`` and excludes
            the clip polygon's exterior boundary.
        tolerance : float, default 0.0
            Finite non-negative coordinate-space tolerance: native angular units for
            geographic CRSs, native linear units for projected CRSs, and raw coordinate
            units when CRS-free.
        clip : {'padded', 'envelope'} or Polygon, default 'padded'
            How to bound the unbounded outer cells: a padded box, the input
            envelope, or a `Polygon` to clip the diagram to.
            Diagram vertices are synthesized and returned in XY. Rounded clip
            incidences need not lie on the source polygon under overlay membership.

        Returns
        -------
        Groups[GeometryArray[Polygon]] or Groups[GeometryArray[LineString]]
            One ragged group of the selected output per input geometry, in input order.

        Raises
        ------
        GeometryError
            If the output cannot be represented in binary64 or its embedding search
            exhausts the budget. ``operation`` is ``'voronoi_cells'`` for polygon
            output and ``'voronoi_edge_lines'`` for edge output.
        InvalidGeometryError
            If the clip polygon is invalid or exact Voronoi construction fails.

        Examples
        --------
        >>> import gometry as gm
        >>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
        >>> len(sites.voronoi())
        3
        >>> len(sites.voronoi(output='edges'))
        3
        """
    @overload
    def split_antimeridian(
        self: GeometryArray[Point],
    ) -> GeometryArray[Point]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[MultiPoint],
    ) -> GeometryArray[MultiPoint]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[MultiLineString],
    ) -> GeometryArray[MultiLineString]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[MultiPolygon],
    ) -> GeometryArray[MultiPolygon]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[GeometryCollection],
    ) -> GeometryArray[GeometryCollection]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[LineString],
    ) -> GeometryArray[LineString | MultiLineString]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[Polygon],
    ) -> GeometryArray[Polygon | MultiPolygon]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[Point | None],
    ) -> GeometryArray[Point | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[MultiPoint | None],
    ) -> GeometryArray[MultiPoint | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[MultiLineString | None],
    ) -> GeometryArray[MultiLineString | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[MultiPolygon | None],
    ) -> GeometryArray[MultiPolygon | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[GeometryCollection | None],
    ) -> GeometryArray[GeometryCollection | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[LineString | None],
    ) -> GeometryArray[LineString | MultiLineString | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[Polygon | None],
    ) -> GeometryArray[Polygon | MultiPolygon | None]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[Geometry]: ...
    @overload
    def split_antimeridian(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[Geometry | None]:
        """Split at the antimeridian. CRS-free input is treated as CRS84; a tagged
        geographic CRS supplies its own longitude frame and ellipsoid. Parts that cross
        come back as multiple parts
        whose edges follow the seam — each side keeping its own seam sign — so the
        result remains a coordinate-planar longitude/latitude chart for topology.
        Inserted seam vertices keep the affine chart crossing in X/Y; eligible Z/M
        follow ellipsoidal arclength along the retained source edge. An authored
        full-period edge is retained with quarter witnesses before its longitudes are
        canonicalized. The two seam aliases and a displacement of multiple periods do
        not imply a full-period edge. A ring running off the seam closes over its pole
        automatically. Geometries that do not cross are returned unchanged. A split
        ``LineString`` becomes a ``MultiLineString`` and a split ``Polygon`` a
        ``MultiPolygon``, like repair. On a geographic CRS, the split subdivides
        existing chart edges, so ellipsoidal area and perimeter are conserved to
        rounding.

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

    @overload
    def offset_curve(
        self: GeometryArray[LineString],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString]: ...
    @overload
    def offset_curve(
        self: GeometryArray[MultiLineString],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[MultiLineString]: ...
    @overload
    def offset_curve(
        self: GeometryArray[GeometryCollection],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[GeometryCollection]: ...
    @overload
    def offset_curve(
        self: GeometryArray[LineString | None],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | None]: ...
    @overload
    def offset_curve(
        self: GeometryArray[MultiLineString | None],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[MultiLineString | None]: ...
    @overload
    def offset_curve(
        self: GeometryArray[GeometryCollection | None],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[GeometryCollection | None]: ...
    @overload
    def offset_curve(
        self: GeometryArray[Geometry],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | MultiLineString | GeometryCollection]: ...
    @overload
    def offset_curve(
        self: GeometryArray[Geometry | None],
        distance: FloatInput,
        *,
        join_style: JoinStyle = 'round',
        quadrant_segments: int = 8,
        miter_limit: float = 5.0,
        unit: DistanceUnit | None = None,
    ) -> GeometryArray[LineString | MultiLineString | GeometryCollection | None]:
        """Compute a line offset to one side of a linestring by ``distance`` (measured
        for the CRS).

        Geographic input uses the same local-frame admission and source-chart edge
        refinement as ``buffer``. Its input-fidelity budget is separate from the 0.1%
        frame-fit limit and round-join resolution, which ``quadrant_segments`` controls.
        Chord directions determine endpoint normals, so the input-fidelity budget does
        not bound output endpoint displacement.

        Parameters
        ----------
        distance : float or sequence of float
            Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
            CRS, native linear units on a projected CRS, raw coordinate units when
            CRS-free — a scalar applies to every
            geometry, or pass one value per geometry. The result is the RAW
            parallel curve: where the input folds back within ``distance`` the
            curve can self-intersect; ``buffer(side=...)`` gives the trimmed area
            instead. Every distance must be finite.
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
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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
        """Reverse the vertex order of a geometry. Linework retains every stored
        vertex and its active Z/M ordinates, including equal-X/Y vertices, in reverse
        order.

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

    @overload
    def envelope(
        self: GeometryArray[Point],
    ) -> GeometryArray[Point | Polygon]: ...
    @overload
    def envelope(
        self: GeometryArray[Point | None],
    ) -> GeometryArray[Point | Polygon | None]: ...
    @overload
    def envelope(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[Point | LineString | Polygon]: ...
    @overload
    def envelope(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[Point | LineString | Polygon | None]:
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

    @overload
    def sample_points(
        self: GeometryArray[Geometry],
        count: _IntInput,
        *,
        seed: _IntInput,
    ) -> Groups[GeometryArray[Point]]: ...
    @overload
    def sample_points(
        self: GeometryArray[Geometry | None],
        count: _IntInput,
        *,
        seed: _IntInput,
    ) -> Groups[GeometryArray[Point] | None]:
        """Random points on the geometry. The sample space is its highest
        dimension: coordinate-area weighted for areal input, coordinate-length
        weighted for lineal input, and equal weight across point atoms. Geographic
        lines follow each retained longitude branch, while geographic areas are split
        into seam- and pole-aware chart parts before sampling. Length and area weights
        use the native coordinate chart, not ellipsoidal measures; for example, a
        degree-based 170° to -170° line follows its retained +20° path, contributing
        20° rather than the raw 340° chord. Seam-crossing polygons use their seam-split
        chart area, not the complementary raw-coordinate span. Each logical
        member contributes separately, so overlapping members retain their sampling
        multiplicity. A zero-area polygon falls back to its linework, and zero-length
        linework falls back to vertices, like centroid. Deterministic: the same input
        and ``seed`` always produce the same points (an explicit seed is required —
        no hidden global RNG). Array rows draw distinct deterministic streams derived
        from ``seed`` and the row index, and a scalar geometry IS row 0 — so
                ``arr.sample_points(n, seed=s)[0]`` and
                ``arr[0].sample_points(n, seed=s)`` agree. An empty geometry has nothing to
                sample and yields no points. Results retain the input CRS and coordinate
                epoch. Sampled points are new XY locations, so they have no source Z/M.

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
            Ordered ``(minx, miny, maxx, maxy)`` rectangle for keying. When
            omitted, uses the present rows' minimum and maximum stored
            coordinates without periodic wrapping. Geographic ``total_bounds``
            may wrap, so it is not necessarily a curve frame. Keys compare
            across geometries only against a *shared* frame — pass the same
            ``bounds`` when keying separate geometries.

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

    @overload
    def centroid(self: GeometryArray[Geometry]) -> GeometryArray[Point]: ...
    @overload
    def centroid(self: GeometryArray[Geometry | None]) -> GeometryArray[Point | None]:
        """Area/length-weighted center of the geometry. Geographic (lon/lat) input
        crossing the antimeridian is auto-split-normalized; no manual
        ``split_antimeridian`` is required. The computed center is an XY point.

        Returns
        -------
        GeometryArray[Point]
            One centroid per row (area/length-weighted; may lie outside).

        See Also
        --------
        point_on_surface : A representative point guaranteed on the geometry.
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

    def force_3d(self, z: float = 0.0) -> Self:
        """Make each geometry 3D, filling vertices that lack Z with ``z``. Vertices
        that already carry Z keep it; M passes through. The one obvious way to lift
        to 3D.

        Parameters
        ----------
        z : float, default 0.0
            Finite Z to assign where it is missing.

        Returns
        -------
        GeometryArray
            One 3D geometry per row (kinds preserved).

        Raises
        ------
        InvalidGeometryError
            If ``z`` is not finite.

        See Also
        --------
        force_2d : Drop Z and M to plain XY.
        with_coordinates : Replace individual coordinate ordinates.

        Examples
        --------
        >>> import gometry as gm
        >>> arr = gm.GeometryArray([gm.Point(1, 2)])
        >>> arr.force_3d().to_wkt()[0]
        'POINT Z (1 2 0)'
        """

    @overload
    def snap_to_grid(
        self: GeometryArray[_GeometryT],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: Literal[False] = False,
    ) -> GeometryArray[_GeometryT]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[Point],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[Point]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[MultiPoint],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[MultiPoint]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[MultiLineString],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[MultiLineString]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[GeometryCollection],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[GeometryCollection]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[LineString],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[LineString | MultiLineString]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[Polygon | MultiPolygon],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[Polygon | MultiPolygon]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[_GeometryT | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: Literal[False] = False,
    ) -> GeometryArray[_GeometryT | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[Point | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[Point | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[MultiPoint | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[MultiPoint | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[MultiLineString | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[MultiLineString | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[GeometryCollection | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[GeometryCollection | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[LineString | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[LineString | MultiLineString | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[Polygon | MultiPolygon | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool,
    ) -> GeometryArray[Polygon | MultiPolygon | None]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[Geometry],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> GeometryArray[Geometry]: ...
    @overload
    def snap_to_grid(
        self: GeometryArray[Geometry | None],
        grid_size: float | tuple[float, float] | Iterable[float],
        *,
        origin: GridOrigin = (0.0, 0.0),
        repair: bool = False,
    ) -> GeometryArray[Geometry | None]:
        """Snap every coordinate onto a regular grid and clean the result. X/Y move to
        the nearest ``origin + k * grid_size`` grid node; consecutive duplicate vertices
        collapse, and parts that degenerate below their minimum become empty (the
        PostGIS ``ST_SnapToGrid`` shape — output may be non-simple). Z/M ride on
        surviving vertices. quantize is the decimal-rounding, vertex-preserving
        sibling.

        Parameters
        ----------
        grid_size : float or tuple of float
            Positive finite coordinate-space grid spacing — native angular units for
            geographic CRSs, native linear units for projected CRSs, and raw
            coordinate units when CRS-free. Pass one value for a square grid or
            ``(sx, sy)``.
            Zero is invalid here; ``coverage_clean(grid_size=0)`` instead disables
            snapping.

        origin : tuple of float, default (0.0, 0.0)
            A grid node anchoring the lattice.

        repair : bool
            If ``True``, guarantee a valid result: snap, linework-repair, and re-
            snap to a fixpoint. The geometry kind may change (a ``Polygon`` whose
            snapped shell pinches splits into a ``MultiPolygon``). Geographic
            ``repair=True`` validates the exact quotient result; projected and
            CRS-free geometry remains planar.

        Returns
        -------
        GeometryArray
            One snapped geometry per row.


        Raises
        ------
        GeometryError
            If ``grid_size`` or ``origin`` is invalid, or the grid is too fine for the
            coordinate magnitude.
        InvalidGeometryError
            If ``repair=True`` and repair would have to invent Z/M ordinates, or
            the snap-repair loop cannot converge.

        Examples
        --------
        >>> import gometry as gm
        >>> jittery = gm.LineString([(0.12, 0.88), (2.49, 1.51)])
        >>> jittery.snap_to_grid(grid_size=0.5).to_wkt()
        'LINESTRING (0 1, 2.5 1.5)'
        """

    @overload
    def build_area(
        self: GeometryArray[
            Point | MultiPoint | LineString | MultiLineString | Polygon | MultiPolygon
        ],
    ) -> GeometryArray[Polygon | MultiPolygon]: ...
    @overload
    def build_area(
        self: GeometryArray[
            Point
            | MultiPoint
            | LineString
            | MultiLineString
            | Polygon
            | MultiPolygon
            | None
        ],
    ) -> GeometryArray[Polygon | MultiPolygon | None]: ...
    @overload
    def build_area(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[Polygon | MultiPolygon | GeometryCollection]: ...
    @overload
    def build_area(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[Polygon | MultiPolygon | GeometryCollection | None]:
        """Assemble linework into one areal geometry. Input ordinates are carried
        where vertices can be sourced; otherwise the mathematically planar result is XY.
        When returned polygons retain different coordinate axes, they remain separate
        members of a GeometryCollection rather than being promoted or flattened.

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

    @overload
    def polygonize(self: GeometryArray[Geometry]) -> Groups[GeometryArray[Polygon]]: ...
    @overload
    def polygonize(
        self: GeometryArray[Geometry | None],
    ) -> Groups[GeometryArray[Polygon] | None]:
        """Build polygons from a geometry's own linework, automatically noding
        crossings and overlaps. Each geometry is polygonized in isolation; to reconstruct
        polygons from a pile of edges pooled across many geometries, use free function
        ``polygonize_all`` on an iterable of
        values. Input ordinates are carried where possible; unsourceable noding seams
        yield XY.

        Returns
        -------
        Groups of GeometryArray
            One ragged group of polygons per input geometry, in input order — each
            input's OWN linework is polygonized independently (row ``i`` is
            ``self[i].polygonize()``). To pool ALL rows' edges into one graph so a
            ring can close across inputs, use the free function ``polygonize_all``.


        See Also
        --------
        polygonize_all : Pool linework across all supplied geometries.

        Raises
        ------
        GeometryError
            If no adjacent binary64 assignment preserves the exact polygonized output
            or the bounded embedding search is exhausted.
        InvalidGeometryError
            If polygons cannot be assembled from the linework.

        Examples
        --------
        >>> import gometry as gm
        >>> a, b = [(0, 0), (1, 0)], [(1, 0), (1, 1)]
        >>> edges = gm.MultiLineString([a, b, [(1, 1), (0, 0)]])
        >>> edges.polygonize().to_wkt()[0]
        'POLYGON ((0 0, 1 0, 1 1, 0 0))'
        """

    @overload
    def convex_hull(
        self: GeometryArray[Geometry],
    ) -> GeometryArray[Point | LineString | Polygon]: ...
    @overload
    def convex_hull(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[Point | LineString | Polygon | None]:
        """Compute the convex hull of the geometry. Operates in planar lon/lat space and does NOT
        auto-split antimeridian-crossing geographic input; call
        ``split_antimeridian`` first. Hull vertices are copies of input vertices, so Z/M
        ordinates are preserved. When several input vertices share an XY position, the
        first in storage order is the survivor.

        Returns
        -------
        GeometryArray
            One convex hull per row.

        Raises
        ------
        GeometryError
            If selected hull vertices have different coordinate axis layouts.

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

    @overload
    def validate(self: GeometryArray[Geometry]) -> list[ValidationReport]: ...
    @overload
    def validate(self: GeometryArray[Geometry | None]) -> list[ValidationReport | None]:
        """Structured validity report in the geometry's coordinate frame.
        Geographic validity reads exact quotient topology, so period aliases and seam
        cuts do not depend on their stored longitude spelling. An out-of-domain
        latitude is reported as ``latitude_out_of_range``; geographic operations that
        require coordinate admission still raise. For quotient-topology issues, the
        location is a canonical coordinate of the issue's contact set, chosen
        deterministically by minimum latitude then longitude. Projected and CRS-free geometry uses ordinary
        planar OGC validity. A nonempty LineString with fewer than two distinct XY
        positions is invalid and reports ``line_collapsed``.

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

    @overload
    def node(self: GeometryArray[Geometry]) -> GeometryArray[MultiLineString]: ...
    @overload
    def node(
        self: GeometryArray[Geometry | None],
    ) -> GeometryArray[MultiLineString | None]:
        """Node linework by splitting every edge at all intersections. Input
        ordinates are carried where possible; unsourceable seam vertices yield XY.

        Returns
        -------
        GeometryArray
            Noded linework per row.


        Raises
        ------
        GeometryError
            If no adjacent binary64 assignment preserves the exact noded linework or
            the bounded embedding search is exhausted.
        InvalidGeometryError
            If noding fails on the input linework.

        Examples
        --------
        >>> import gometry as gm
        >>> lines = gm.MultiLineString([[(0,0),(2,0)],[(1,-1),(1,1)]])
        >>> noded = lines.node()
        >>> (noded.num_geometries, noded.length)
        (4, 4.0)
        """

    @overload
    def to_wkt(
        self: GeometryArray[Geometry],
        *,
        include_srid: bool = False,
        precision: _IntScalar | None = None,
        drop_epoch: bool = False,
    ) -> list[str]: ...
    @overload
    def to_wkt(
        self,
        *,
        include_srid: bool = False,
        precision: _IntScalar | None = None,
        drop_epoch: bool = False,
    ) -> list[str | None]:
        """Serialize to Well-Known Text.

        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

        Parameters
        ----------
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
            One WKT string per row; missing rows yield ``None``.


        Raises
        ------
        GeometryError
            If ``precision`` is not between 0 and 15, or the geometry carries a coordinate epoch and
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

    @overload
    def to_wkb(
        self: GeometryArray[Geometry],
        *,
        include_srid: bool = False,
        precision: _IntScalar | None = None,
        drop_epoch: bool = False,
    ) -> list[bytes]: ...
    @overload
    def to_wkb(
        self,
        *,
        include_srid: bool = False,
        precision: _IntScalar | None = None,
        drop_epoch: bool = False,
    ) -> list[bytes | None]:
        """Serialize to Well-Known Binary.

        Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

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
            One WKB payload per row; missing rows yield ``None``.

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
            Finite non-negative coordinate-space tolerance — native angular units for
            geographic CRSs, native linear units for projected CRSs, and raw coordinate
            units when CRS-free. A scalar applies to every geometry, or pass one value
            per geometry.


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

@final
class Groups(Sequence[_GroupRowT_co], Generic[_GroupRowT_co]):
    """Shared CSR ragged container: one flat `values` payload plus row `offsets`.
    The values payload keeps its actual type (int64, geometry array, or cell
    array). A contiguous values window may share backing; offsets for a window
    starting after row zero are rebased, and counts are materialized.
    """
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Not constructed directly — the error points at the producers."""
    values: _GroupsValuesProjection
    """The logical flat payload window (int64 ndarray, `GeometryArray`, or
    `CellArray`), with values in group order. A contiguous source window
    can share backing; gathered values may be materialized.

    Returns
    -------
    Group values
        The selected flat payload window. Numeric values are read-only int64;
        geometry and cell payloads retain their immutable array types.
    """
    @property
    def offsets(self) -> np.ndarray[tuple[int], np.dtype[np.int64]]:
        """The `len(self) + 1` row boundaries into ``values`` (CSR offsets
        column): row ``i`` is ``values[offsets[i]:offsets[i + 1]]``.

        Returns
        -------
        numpy.ndarray
            A read-only int64 array of row boundaries. Slices are rebased to
            start at zero; a slice starting after row zero owns a rebased copy.
        """
    @property
    def counts(self) -> np.ndarray[tuple[int], np.dtype[np.int64]]:
        """Per-group element counts (`offsets[i + 1] - offsets[i]`).

        Returns
        -------
        numpy.ndarray
            A materialized, read-only int64 count for each group in row order.
        """
    @property
    def is_missing(self) -> np.ndarray[tuple[int], np.dtype[np.bool_]]:
        """Missing source groups, distinct from present groups with no outputs.

        Returns
        -------
        numpy.ndarray
            A read-only boolean mask in group order.
        """
    @property
    def nbytes(self) -> int:
        """Logical CSR payload in bytes: the selected flat values payload, the
        ``len(self) + 1`` int64 offsets column, and a row-validity bitmap when
        any group is missing. For geometry-valued groups this uses the values
        ``GeometryArray.nbytes`` and excludes geometry structural offsets,
        matching NumPy's payload-only convention.

        Returns
        -------
        int
        """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus this group's
        logical CSR payload. Sliced groups report the visible values window and
        rebased logical offsets, not the whole shared backing allocation.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """
    def __class_getitem__(cls, key: Any) -> types.GenericAlias:
        """See PEP 585"""
    __array_ufunc__: ClassVar[None]
    __hash__: ClassVar[None]  # type: ignore[assignment]
    def __copy__(self) -> Self: ...
    def __deepcopy__(self, memo: object) -> Self: ...
    def __contains__(self, value: object, /) -> bool: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __iter__(self) -> GroupsIterator[_GroupRowT_co]: ...
    def __reversed__(self) -> GroupsIterator[_GroupRowT_co]:
        """Iterate row groups in reverse order.

        Returns
        -------
        iterator
        """
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _GroupRowT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> Self: ...
    @overload
    @overload
    def to_pairs(
        self: Groups[np.ndarray[tuple[int], np.dtype[np.int64]]],
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]: ...
    @overload
    def to_pairs(
        self: Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]: ...
    @overload
    def to_pairs(
        self: Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]:
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
    @overload
    def to_list(
        self: Groups[np.ndarray[tuple[int], np.dtype[np.int64]]],
    ) -> list[list[int]]: ...
    @overload
    def to_list(
        self: Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
    ) -> list[list[int] | None]: ...
    @overload
    def to_list(
        self: Groups[GeometryArray[_GeometryT_co]],
    ) -> list[list[_GeometryT_co]]: ...
    @overload
    def to_list(
        self: Groups[GeometryArray[_GeometryT_co] | None],
    ) -> list[list[_GeometryT_co] | None]: ...
    @overload
    def to_list(self: Groups[CellArray[_CellT_co]]) -> list[list[_CellT_co]]: ...
    @overload
    def to_list(
        self: Groups[CellArray[_CellT_co] | None],
    ) -> list[list[_CellT_co] | None]:
        """Copy into a plain nested Python list.

        Returns
        -------
        list
            Materialized rows of the grouped values; missing rows are ``None``.

        Examples
        --------
        >>> import gometry as gm
        >>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
        >>> [g.to_wkt() for g in groups.to_list()[0]]
        ['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 2, 0 2, 2 0, 2 2))']
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

@final
class GroupsIterator(Iterator[_GroupRowT_co]):
    """Lazy row iterator for [`Groups`]."""
    def __iter__(self) -> Self: ...
    def __next__(self) -> _GroupRowT_co: ...
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        payload it keeps alive while iterating.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """

class _GroupsValuesProjection(Protocol):
    @overload
    def __get__(
        self,
        instance: Groups[_GroupPayloadT],
        owner: type[Groups[Any]] | None = None,
    ) -> _GroupPayloadT: ...
    @overload
    def __get__(
        self,
        instance: Groups[_GroupPayloadT | None],
        owner: type[Groups[Any]] | None = None,
    ) -> _GroupPayloadT: ...
    @overload
    def __get__(self, instance: None, owner: type[Groups[Any]]) -> Self: ...
    def __set__(self, instance: Groups[Any], value: Never) -> None: ...

@final
class SpatialIndexIterator(Iterator[int]):
    """Lazy ascending-handle iterator over the live range directory. Mutation
    invalidates iteration, matching mapping iterator semantics and keeping
    ``__length_hint__`` exact.
    """
    def __iter__(self) -> Self: ...
    def __next__(self) -> int: ...
    def __length_hint__(self) -> int: ...
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support for this iterator and its retained index.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """

@final
class SpatialIndex(Mapping[int, Geometry]):
    """A packed STR-tree over geometries sharing one CRS/epoch frame.

    Built by ``SpatialIndex(geoms)``: ask set questions against the indexed
    geometries — exact predicate matches (``idx.query(geom)``), bounding-box
    candidates (``idx.candidates(geom)``), proximity (``idx.nearest(geom)``),
    self-joins (``idx.self_join()``) — and mutate it incrementally with
    ``insert``/``remove``. Distances follow the indexed CRS: meters on a
    geographic frame, native linear units on a projected frame, raw coordinate
    units when CRS-free.
    """

    __hash__: ClassVar[None]  # type: ignore[assignment]
    """A packed STR-tree over geometries sharing one CRS/epoch frame.

    Built by ``SpatialIndex(geoms)``: ask set questions against the indexed
    geometries — exact predicate matches (``idx.query(geom)``), bounding-box
    candidates (``idx.candidates(geom)``), proximity (``idx.nearest(geom)``),
    self-joins (``idx.self_join()``) — and mutate it incrementally with
    ``insert``/``remove``. Distances follow the indexed CRS: meters on a
    geographic frame, native linear units on a projected frame, raw coordinate
    units when CRS-free.
    """
    def __new__(
        cls,
        values: Geometry
        | GeometryArray[Geometry | None]
        | Iterable[_GeometryLike | None]
        | None = None,
    ) -> Self:
        """Build a spatial index (STR-tree) over present geometries.

        Parameters
        ----------
        values : Geometry, GeometryArray, iterable, or None, default None
            Geometries to index. Iterable rows may be native geometries, WKB
            buffers, GeoJSON mappings, or objects exposing
            ``__geo_interface__``. ``None`` builds an empty mutable index for
            later ``insert`` calls. Every indexed geometry must share one CRS
            and coordinate epoch. Missing rows are skipped but retain their
            original positions as stable, non-live handles, so query and join
            results always refer to the input row ids. Packed arrays retain
            their immutable column storage; empty rows likewise have no live
            tree entry.

        Returns
        -------
        SpatialIndex
            A mutable STR-tree whose handles refer to the original input rows.

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
        """`sys.getsizeof` support: Python's allocation base plus the retained Rust-side
        index payload — packed or boxed row geometry coordinates, the immutable
        STR tree, overflow STR runs, and frame metadata. Shared buffers are
        reported as this index's logical retained footprint.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """
    def __repr__(self) -> str:
        """Return the bounded representation of this spatial index."""
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
        """Pickle live geometries with their stable handles and next identity.

        Handles are the public identity returned by ``query``/``nearest`` and
        consumed by ``remove``/``__getitem__``. Values are stored compactly in
        handle order; removed and missing rows do not occupy pickle rows.
        """
    def __copy__(self) -> Self: ...
    def __deepcopy__(self, memo: object) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __getitem__(self, handle: SupportsIndex, /) -> Geometry:
        """Return the geometry at a live handle.

        The handle must be a non-negative integer accepted by ``__index__``;
        ``bool`` is not accepted. Raises ``KeyError`` when a valid handle is
        unknown or has been removed.

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
    def __iter__(self) -> SpatialIndexIterator:
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
    def get(self, handle: SupportsIndex, /) -> Geometry | None: ...
    @overload
    def get(
        self, handle: SupportsIndex, /, default: Geometry | _DefaultT
    ) -> Geometry | _DefaultT:
        """Return the geometry at handle, or default when it is not live.

        Parameters
        ----------
        handle : SupportsIndex
            A non-negative integer/index handle (positional-only; ``bool`` is
            rejected). Invalid or negative handles raise instead of returning
            ``default``.

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
    ) -> np.ndarray[tuple[int], np.dtype[np.int64]]: ...
    @overload
    def query(
        self,
        geom: _GeometryBatch[Geometry],
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]: ...
    @overload
    def query(
        self,
        geom: _GeometryBatch[Geometry | None],
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]: ...
    @overload
    def query(
        self,
        geom: Geometry,
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> np.ndarray[tuple[int], np.dtype[np.int64]]: ...
    @overload
    def query(
        self,
        geom: _GeometryBatch[Geometry],
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]: ...
    @overload
    def query(
        self,
        geom: _GeometryBatch[Geometry | None],
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]:
        """Return exact predicate-refined matches for a query geometry, array, or
        iterable of geometry rows.

        Topological predicates use `'intersects'` when `predicate` is omitted;
        `'dwithin'` requires an explicit predicate and distance. Use
        candidates for a bounding-box-only prefilter. A single geometry
        returns an int64 ndarray; arrays and iterables return Groups, with one
        id row per input row.

        Parameters
        ----------
        geom : Geometry, GeometryArray, or iterable of Geometry or None
            One query geometry or one query per sequence row. Missing rows
            produce missing result groups (`None`); present rows with no matches
            produce empty groups.
        predicate : str, default varies by call form
            Spatial relation each match must satisfy, evaluated as
            ``predicate(query_geometry, indexed_geometry)``. Topological forms
            omit ``predicate`` to use ``'intersects'``; ``'dwithin'`` forms
            require an explicit predicate and distance.
        distance : float, optional
            ``'dwithin'`` distance threshold, in ``unit``. A positive infinite
            threshold is accepted; other values must be finite and
            non-negative. Missing query rows remain missing result groups;
            present empty query rows produce empty result groups.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        int64 numpy.ndarray or Groups
            Matching index handles (row ids). A scalar query returns a read-only
            ``int64`` ndarray of handles; an array query returns ``Groups`` of
            handles, one row per query row. Missing queries yield ``None`` rows,
            while present queries with no matches yield empty groups. Each result
            is in ascending source-row order.

        Raises
        ------
        CRSMismatchError
            If the query does not share the index's CRS/epoch frame.
        GeometryError
            If a query parameter is invalid, or ``unit='meters'`` is requested
            for a CRS-free geometry.
        CRSError
            If geographic ``dwithin`` refinement cannot prove a result within
            the point-edge evaluation budget.

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
        queries: Geometry
        | _GeometryBatch[Geometry | None]
        | Iterable[_GeometryLike | None],
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]: ...
    @overload
    def join(
        self,
        queries: Geometry
        | _GeometryBatch[Geometry | None]
        | Iterable[_GeometryLike | None],
        *,
        predicate: TopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]:
        """Join query rows against this prebuilt index.

        Reuses the index instead of rebuilding the right side on every call.
        Predicate orientation is ``predicate(query, indexed_geometry)``, exactly
        matching free-function ``join(queries, indexed_values, ...)``. Missing query rows
        produce no pairs; missing rows skipped while building the index retain
        their original handles.

        Parameters
        ----------
        queries : Geometry, GeometryArray, or iterable of Geometry or None
            Left-side geometries to join against the indexed rows.
        predicate : str, default varies by call form
            Spatial predicate each returned pair must satisfy. Topological forms
            omit ``predicate`` to use ``'intersects'``; ``'dwithin'`` forms
            require an explicit predicate and distance.
        distance : float, optional
            Required when ``predicate='dwithin'``: the maximum separation in
            CRS-natural units. Finite non-negative values and positive infinity
            are accepted; missing query rows produce no pairs.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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
        CRSError
            If geographic ``dwithin`` refinement cannot prove a result within
            the point-edge evaluation budget.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> left, right = idx.join(gm.GeometryArray([gm.Point(0.5, 0.5)]))
        >>> (left.tolist(), right.tolist())
        ([0], [0])
        """
    @overload
    def self_join(
        self,
        *,
        predicate: Literal['dwithin'],
        distance: float,
        unit: DistanceUnit | None = None,
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]: ...
    @overload
    def self_join(
        self,
        *,
        predicate: SymmetricTopologicalPredicate = 'intersects',
        distance: None = None,
        unit: DistanceUnit | None = None,
    ) -> tuple[
        np.ndarray[tuple[int], np.dtype[np.int64]],
        np.ndarray[tuple[int], np.dtype[np.int64]],
    ]:
        """All index pairs ``(i, j)`` with ``i < j`` whose geometries satisfy a
        symmetric ``predicate`` — a self-join over the index.

        Parameters
        ----------
        predicate : str
            A symmetric relation: ``'intersects'``, ``'equals'``,
            ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
            Directional predicates (``'contains'``, ``'within'``, ...) are
            rejected — unordered pairs would drop the reverse direction; use
            ``join(...)`` for directed relations.
        distance : float, optional
            ``'dwithin'`` distance threshold, in ``unit``. Positive infinity is
            accepted; other values must be finite and non-negative. Empty
            geometries never produce a pair.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



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
        CRSError
            If geographic ``dwithin`` refinement cannot prove a result within
            the point-edge evaluation budget.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
        >>> left, right = idx.self_join()
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
    ) -> np.ndarray[tuple[int], np.dtype[np.int64]]: ...
    @overload
    def candidates(
        self,
        geom: _GeometryBatch[Geometry],
        *,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]: ...
    @overload
    def candidates(
        self,
        geom: _GeometryBatch[Geometry | None],
        *,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]: ...
    @overload
    def candidates(
        self,
        geom: Geometry | _GeometryBatch[Geometry | None],
        *,
        distance: float | None = None,
        unit: DistanceUnit | None = None,
    ) -> (
        np.ndarray[tuple[int], np.dtype[np.int64]]
        | Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]
    ):
        """Bounding-box candidate matches for a query geometry, array, or iterable (not
        exact). If you refine these candidates with a directional predicate,
        evaluate it as ``predicate(query_geometry, indexed_geometry)``.

        A single geometry returns an `int64` ndarray; arrays and iterables
        return `Groups` — one candidate row per input row.

        Parameters
        ----------
        geom : Geometry, GeometryArray, or iterable of Geometry or None
            One query geometry or one query per sequence row. Missing rows
            produce ``None`` groups; present rows with no candidates produce
            empty groups.
        distance : float, optional
            Expand the query envelope by this much, in ``unit``. Finite
            non-negative expansions and positive infinity are accepted;
            infinity admits every live non-empty row for a query with bounds.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.



        Returns
        -------
        int64 numpy.ndarray or Groups
            Source-row handles in ascending order.

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
    ) -> np.ndarray[tuple[int], np.dtype[np.int64]]: ...
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
    ) -> tuple[np.ndarray[tuple[int], np.dtype[np.int64]], npt.NDArray[np.float64]]: ...
    @overload
    def nearest(
        self,
        geom: _GeometryBatch[Geometry],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[False] = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]: ...
    @overload
    def nearest(
        self,
        geom: _GeometryBatch[Geometry | None],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[False] = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]: ...
    @overload
    def nearest(
        self,
        geom: _GeometryBatch[Geometry],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[True],
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> tuple[
        Groups[np.ndarray[tuple[int], np.dtype[np.int64]]],
        npt.NDArray[np.float64],
    ]: ...
    @overload
    def nearest(
        self,
        geom: _GeometryBatch[Geometry | None],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: Literal[True],
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> tuple[
        Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
        npt.NDArray[np.float64],
    ]: ...
    @overload
    def nearest(
        self,
        geom: Geometry,
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: bool = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> (
        npt.NDArray[np.int64] | tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]
    ): ...
    @overload
    def nearest(
        self,
        geom: _GeometryBatch[Geometry],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: bool = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> (
        Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]
        | tuple[
            Groups[np.ndarray[tuple[int], np.dtype[np.int64]]],
            npt.NDArray[np.float64],
        ]
    ): ...
    @overload
    def nearest(
        self,
        geom: _GeometryBatch[Geometry | None],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: bool = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> (
        Groups[npt.NDArray[np.int64] | None]
        | tuple[Groups[npt.NDArray[np.int64] | None], npt.NDArray[np.float64]]
    ): ...
    @overload
    def nearest(
        self,
        geom: Geometry | _GeometryBatch[Geometry | None],
        *,
        k: int = 1,
        max_distance: float | None = None,
        return_distance: bool = False,
        unit: DistanceUnit | None = None,
        exclusive: bool = False,
        ties: bool = False,
    ) -> (
        np.ndarray[tuple[int], np.dtype[np.int64]]
        | Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]
        | tuple[np.ndarray[tuple[int], np.dtype[np.int64]], npt.NDArray[np.float64]]
        | tuple[
            Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
            npt.NDArray[np.float64],
        ]
    ):
        """Nearest indexed geometries to the query.

        Parameters
        ----------
        geom : Geometry, GeometryArray, or iterable of Geometry or None
            One query geometry or one query per sequence row. Missing rows
            produce ``None`` groups and retain their logical positions.
        k : int, default 1
            How many nearest neighbors to return; ``0`` returns no neighbors.
        max_distance : float, optional
            Ignore matches farther than this, in ``unit``. Must be finite and
            non-negative.
        return_distance : bool
            Return distances alongside handles — ``(indices, distances)`` for
            a scalar query, ``(matches, distances)`` for an array query. The
            distance-returning form requires ``return_distance=True``.
        unit : {'planar', 'meters'}, default None
            ``None`` (the default) follows the CRS: geographic shape area and length
            use ellipsoidal chart-edge measures while point distances use geodesics;
            projected measurements use native linear units, and CRS-free measurements
            use raw coordinate units (area results square the corresponding unit).
            ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
            in their native angular units. ``'meters'`` uses the CRS-aware metric and
            requires a CRS.


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
    def insert(self, values: Geometry) -> int: ...
    @overload
    def insert(
        self, values: GeometryArray[Geometry | None] | Iterable[_GeometryLike]
    ) -> np.ndarray[tuple[int], np.dtype[np.int64]]: ...
    @overload
    def insert(
        self,
        values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike],
    ) -> int | np.ndarray[tuple[int], np.dtype[np.int64]]:
        """Insert one geometry or many geometries and return their stable handles.

        A single `Geometry` returns one ``int`` handle. A `GeometryArray` or
        generic iterable of geometries returns a read-only `int64` ndarray of
        handles in input order. Batch inserts follow the same frame rules as
        scalar insert: the first inserted row fixes an empty index's CRS/epoch
        frame, and later inserts must match it. A GeometryArray containing a
        missing row is rejected. All input-validation failures leave the index
        unchanged; this is not a transaction guarantee for allocation or
        publication failures.

        Parameters
        ----------
        values : Geometry or GeometryArray or iterable of Geometry
            Values to append to the index; all must share the index's
            CRS/epoch frame. Empty geometries and missing GeometryArray rows
            cannot be inserted.

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
            If any inserted geometry is empty, or a GeometryArray contains a
            missing row.

        Examples
        --------
        >>> import gometry as gm
        >>> idx = gm.SpatialIndex([])
        >>> idx.insert(gm.Point(1, 1))
        0
        """
    def remove(self, handle: SupportsIndex) -> bool:
        """Remove a geometry by its handle. Returns ``True`` if a live geometry
        was removed, ``False`` if the handle is unknown or was already removed.
        Removed handles are not reused, so surviving handles stay stable.

        Parameters
        ----------
        handle : SupportsIndex
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
    evaluate it (``crs.factors(...)``), and attach it to geometries via
    ``to_crs``. Accepts an authority string, EPSG code,
    authority tuple, PROJJSON/CF mapping, WKT/PROJ string, or another ``CRS``.

    Equality is structural between ``CRS`` instances: the stored normalized
    definitions must match. Other operand types do not compare equal; use
    ``CRS(value)`` when comparing stored identity with raw input. Operational
    equivalence, including axis-order-only differences, is the explicit
    ``same_as(..., mode='ignore_axis_order')`` query. Equal ``CRS`` values are
    hashable and can be used as mapping keys.
    """
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus owned Rust-side CRS text
        retained by this object. This includes the stored CRS definition when it
        spills out of `SmolStr` inline storage and any heap strings/vectors in
        the lazily cached `CrsInfo`. Opaque PROJ process-global caches are not
        owned by the Python object and are not counted.
        """
    def __str__(self) -> str:
        """Return the stored CRS definition."""
    def __repr__(self) -> str:
        """Return the constructor-style CRS representation."""
    def __copy__(self) -> Self:
        """``copy.copy`` returns the object itself — a CRS is an immutable
        value, so a copy IS the original (like ``tuple``).
        """
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the object itself: every field is immutable
        and holds no Python references, so there is nothing to copy.
        """
    def __reduce__(self) -> tuple[Callable[[str], CRS], tuple[str]]:
        """Pickle support: retain the definition and rebuild derived metadata lazily."""
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
    def __hash__(self) -> int:
        """Hash the stored normalized CRS definition."""
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
    def definition(self) -> str:
        """Stored definition string for this CRS.

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
            Indent the WKT output (default ``False``).

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
    @overload
    def to_projjson(
        self,
        *,
        as_dict: Literal[False] = False,
        pretty: bool = False,
        indentation_width: int = 2,
    ) -> str: ...
    @overload
    def to_projjson(
        self,
        *,
        as_dict: Literal[True],
    ) -> dict[str, object]: ...
    @overload
    def to_projjson(
        self,
        *,
        as_dict: bool,
        pretty: bool = False,
        indentation_width: int = 2,
    ) -> str | dict[str, object]:
        """Export this CRS as PROJJSON text or a fresh Python mapping.

        Parameters
        ----------
        as_dict : bool
            Return a Python mapping; the JSON-text form omits ``as_dict`` and
            the mapping form requires ``as_dict=True``.

        pretty : bool, optional
            Indent JSON text (default ``False``). Only applies when
            ``as_dict=False``.

        indentation_width : int
            Spaces per indent level for pretty JSON text; available only when
            ``as_dict=False``.

        Returns
        -------
        str or dict[str, object]

        Raises
        ------
        CRSError
            If export fails or a formatting option is invalid.
        GeometryError
            If formatting is requested with ``as_dict=True``.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).to_projjson(as_dict=True)['type']
        'GeographicCRS'
        """
    def to_cf(self, *, wkt_version: WktVersion = 'WKT2_2019') -> CrsCfInfo:
        """CF (Climate and Forecast) grid-mapping attributes as a dict.

        Projected ``units`` is ``m``, ``ft``, or ``us-ft`` when every horizontal
        axis admits the same token. Otherwise the reconstructible grid mapping
        and ``units`` are omitted; ``crs_wkt`` still carries the full CRS.

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
        `force_2d`/`force_3d` or `with_coordinates(z=...)` for that.

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
        ordinates on geometries — use `force_2d`/`force_3d` or `with_coordinates(z=...)` for
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
        other : CRS-like
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
    @property
    def non_deprecated(self) -> list[CrsAuthorityObject]:
        """Non-deprecated authority objects equivalent to this CRS.

        Returns
        -------
        list of CrsAuthorityObject

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).non_deprecated
        []
        """
    @property
    def geoid_models(self) -> list[str]:
        """Geoid model names available for this CRS.

        Returns
        -------
        list of str

        Examples
        --------
        >>> import gometry as gm
        >>> gm.CRS(4326).geoid_models
        []
        """
    @overload
    def factors(
        self,
        lon: _FloatScalarInput,
        lat: _FloatScalarInput,
        *,
        radians: bool = False,
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
        target : CRS-like
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
            The operation or a nested step's ``definition`` may be ``None`` when
            the selected default angular semantics cannot be represented
            faithfully by an executable PROJ string. Method, parameters, steps,
            grids, source, target, and epoch metadata remain available.

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
        target : CRS-like
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
            A candidate's or nested step's ``definition`` may be ``None`` when
            the selected default angular semantics cannot be represented
            faithfully by an executable PROJ string. Method, parameters, steps,
            grids, source, target, and epoch metadata remain available.

        Examples
        --------
        >>> import gometry as gm
        >>> len(gm.CRS(4326).operations(3857)) >= 1
        True
        """

# Element type of a `CellArray`, including `None` when the array is nullable.
# Covariant + PEP 696 default so a bare `CellArray` means `CellArray[Cell]`
# (the public cell protocol, non-nullable). `_CellT` is its invariant twin for
# self annotations (dense vs nullable, `Groups[CellArray[...]]`), since a
# covariant variable cannot appear in an input position. Neither is
# `bound=Cell`: the recursive `Cell` protocol (`neighbors`/`children` ->
# `CellArray[Self]`) cannot prove `Self <: Cell` while `CellArray` demands it.
_CellT = TypeVar('_CellT', bound=Cell)
_CellT_co = TypeVar('_CellT_co', covariant=True, default=Cell)

class _CellGeometryProjection(Protocol[_GeometryT_co]):
    @overload
    def __get__(
        self,
        instance: CellArray[Cell],
        owner: type[CellArray[Cell | None]] | None = None,
    ) -> GeometryArray[_GeometryT_co]: ...
    @overload
    def __get__(
        self,
        instance: CellArray[Cell | None],
        owner: type[CellArray[Cell | None]] | None = None,
    ) -> GeometryArray[_GeometryT_co | None]: ...
    @overload
    def __get__(
        self,
        instance: None,
        owner: type[CellArray[Cell | None]],
    ) -> Self: ...
    def __set__(
        self,
        instance: CellArray[Cell | None],
        value: Never,
    ) -> None: ...

class _CellGroupsProjection(Protocol):
    @overload
    def __get__(
        self,
        instance: CellArray[_CellT],
        owner: type[CellArray[Cell | None]] | None = None,
    ) -> Groups[CellArray[_CellT]]: ...
    @overload
    def __get__(
        self,
        instance: CellArray[_CellT | None],
        owner: type[CellArray[Cell | None]] | None = None,
    ) -> Groups[CellArray[_CellT] | None]: ...
    @overload
    def __get__(
        self,
        instance: None,
        owner: type[CellArray[Cell | None]],
    ) -> Self: ...
    def __set__(
        self,
        instance: CellArray[Cell | None],
        value: Never,
    ) -> None: ...

class _CellTokenProjection(Protocol):
    @overload
    def __get__(
        self,
        instance: CellArray[Cell],
        owner: type[CellArray[Cell | None]] | None = None,
    ) -> list[str]: ...
    @overload
    def __get__(
        self,
        instance: CellArray[Cell | None],
        owner: type[CellArray[Cell | None]] | None = None,
    ) -> list[str | None]: ...
    @overload
    def __get__(
        self,
        instance: None,
        owner: type[CellArray[Cell | None]],
    ) -> Self: ...
    def __set__(
        self,
        instance: CellArray[Cell | None],
        value: Never,
    ) -> None: ...

@final
class CellArrayIterator(Generic[_CellT_co]):
    """Lazy iterator over a `CellArray`'s typed cells (both directions)."""
    def __length_hint__(self) -> int:
        """Remaining rows — lets ``list(iter)`` preallocate."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        payload it keeps alive while iterating.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """

    def __iter__(self) -> Self:
        """Implement iter(self)."""
    def __next__(self) -> _CellT_co:
        """Return the next typed cell, or ``None`` for a missing row."""

@final
class CellArray(Sequence[_CellT_co]):
    """An immutable array of one grid cell type backed by a shared ``uint64`` id
    column.

    Index with an integer for the typed cell object or `None` for a missing row;
    slice or mask for a new `CellArray`. Build from an iterable containing
    typed cell objects and `None`; the grid is inferred from the first concrete
    cell. Raw ids for H3, S2, and Tile and tokens for any grid may accompany a
    typed cell of the same grid. Geohash cells use tokens, not integer ids. For
    raw-only values, numeric arrays or buffers, empty input, or all-missing
    input, pass ``type=`` explicitly; every id and token is validated.

    Parameters
    ----------
    values : iterable, numpy.ndarray, or buffer
        Typed cell objects and ``None``; raw ids for H3/S2/Tile and tokens for
        any grid are accepted when a concrete cell supplies the family.
    type : type
        Native cell class. Inferred from a concrete typed cell when present.
    """

    __array_ufunc__: ClassVar[None]
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus this array's logical
        ``uint64`` id payload, missing flags, and any row-selection map. Shared
        backing buffers are estimated from the selected view, not the full
        parent allocation.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """
    def __repr__(self) -> str:
        """Return the bounded representation of this cell array."""
    def __reduce__(self) -> tuple[Any, tuple[list[int] | list[str], str, bytes | None]]:
        """Pickle support: public identities plus a mask (0=present, 1=missing)."""
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
        self: CellArray[H3Cell | None],
        other: H3Cell | _IntScalarInput | str | CellArray[H3Cell | None],
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(
        self: CellArray[S2Cell | None],
        other: S2Cell | _IntScalarInput | str | CellArray[S2Cell | None],
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(
        self: CellArray[GeohashCell | None],
        other: GeohashCell | str | CellArray[GeohashCell | None],
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def contains(
        self: CellArray[Tile | None],
        other: Tile | _IntScalarInput | str | CellArray[Tile | None],
    ) -> npt.NDArray[np.bool_]:
        """Test whether every row hierarchically contains the paired cell.

        Parameters
        ----------
        other : same-grid cell, integer id, string token, or CellArray
            One identity broadcast to every row, or a same-length array.

        Returns
        -------
        numpy.ndarray
            One read-only boolean per row.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> gm.CellArray([cell, neighbor]).contains(cell).tolist()
        [True, False]
        """
    @overload
    def intersects(
        self: CellArray[H3Cell | None],
        other: H3Cell | _IntScalarInput | str | CellArray[H3Cell | None],
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self: CellArray[S2Cell | None],
        other: S2Cell | _IntScalarInput | str | CellArray[S2Cell | None],
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self: CellArray[GeohashCell | None],
        other: GeohashCell | str | CellArray[GeohashCell | None],
    ) -> npt.NDArray[np.bool_]: ...
    @overload
    def intersects(
        self: CellArray[Tile | None],
        other: Tile | _IntScalarInput | str | CellArray[Tile | None],
    ) -> npt.NDArray[np.bool_]:
        """Test whether every row hierarchically intersects the paired cell.

        Two cells intersect when either is an ancestor of the other.

        Parameters
        ----------
        other : same-grid cell, integer id, string token, or CellArray
            One identity broadcast to every row, or a same-length array.

        Returns
        -------
        numpy.ndarray
            One read-only boolean per row.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> gm.CellArray([cell, neighbor]).intersects(cell).tolist()
        [True, False]
        """
    def value_counts(
        self: CellArray[_CellT | None],
    ) -> tuple[CellArray[_CellT], np.ndarray[tuple[int], np.dtype[np.int64]]]:
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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> unique, counts = gm.CellArray([cell, cell]).value_counts()
        >>> counts.tolist()
        [2]
        """
    def factorize(
        self: CellArray[_CellT | None],
    ) -> tuple[np.ndarray[tuple[int], np.dtype[np.int64]], CellArray[_CellT]]:
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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> codes, unique = gm.CellArray([cell, neighbor]).factorize()
        >>> codes.tolist()
        [0, 1]
        """
    center: _CellGeometryProjection[Point]
    """Center points of every cell, as a packed WGS84 point array.

    Computes each center from its cell id and allocates a packed point
    coordinate column.

    Returns
    -------
    GeometryArray
        Dense cells yield one ``Point`` (lon/lat, ``OGC:CRS84``) per row.
        Missing rows in a nullable ``CellArray`` remain ``None``.

    Examples
    --------
    >>> import gometry as gm
    >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=1)])
    >>> arr.center.to_wkt()[0]
    'POINT (-90 42.5255643899033)'
    """
    @property
    def area(self) -> npt.NDArray[np.float64]:
        """Cell areas in square meters, using each grid's area model.

        H3 and S2 use an authalic-sphere model. Geohash and tile use the WGS 84
        ellipsoidal area of their lon/lat rectangle.

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
    polygon: _CellGeometryProjection[Polygon]
    """Filled WGS84 polygon of every cell, as a geometry array.

    Returns
    -------
    GeometryArray
        Dense cells yield one polygon per row. Missing rows in a nullable
        ``CellArray`` remain ``None``.
    """
    def children_count(self, depth: int | None = None, /) -> npt.NDArray[np.float64]:
        """Number of descendant cells each cell has at ``depth``.

        The columnar mirror of the scalar ``children_count`` — the count only,
        without materializing the children (which ``children`` does, as ragged
        rows). Present-row counts are integer-valued but are represented as
        ``float64``; they are exact only when within float64's integer precision.
        Missing rows are represented by ``NaN`` to preserve row alignment.

        Parameters
        ----------
        depth : int, optional
            Target depth (resolution / level / precision / zoom). Omitted means
            one step finer than each cell.

        Returns
        -------
        numpy.ndarray
            Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``;
            missing rows are ``NaN``.

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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> cells = gm.CellArray([cell, neighbor])
        >>> cells.children_count(9).tolist()
        [49.0, 49.0]
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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> cells = gm.CellArray([cell, neighbor])
        >>> parent = cells.parent(6)[0]
        >>> assert parent is not None
        >>> parent.token
        '86283082fffffff'
        """
    neighbors: _CellGroupsProjection
    """The edge-adjacent cells of every cell, as ragged rows.

    Returns
    -------
    Groups of CellArray
        One row of neighbors per input cell, in input order. Neighbor
        counts vary, so the result is a Groups, not a rectangular
        CellArray. Missing input rows yield ``None`` groups.
    """
    @overload
    def children(
        self: CellArray[_CellT], depth: int | None = None, /
    ) -> Groups[CellArray[_CellT]]: ...
    @overload
    def children(
        self: CellArray[_CellT | None], depth: int | None = None, /
    ) -> Groups[CellArray[_CellT] | None]:
        """Return the child cells of every cell at a finer depth, as ragged rows.

        Parameters
        ----------
        depth : int, optional
            Target depth; must not be coarser than any input cell. Defaults to
            one finer than each cell's own depth.

        Returns
        -------
        Groups of CellArray
            One row of children per input cell, in input order. Missing input
            rows yield ``None`` groups.

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> children = gm.CellArray([cell]).children(8)[0]
        >>> assert children is not None
        >>> len(children)
        7
        """
    def compact(
        self: CellArray[_CellT | None], depth: int | None = None, /
    ) -> CellArray[_CellT]:
        """Compact this cell set to the coarsest exact covering, returned in the
        grid's public identity order. Duplicates and descendants are absorbed;
        complete sibling groups merge recursively, stopping at ``depth``.
        Missing rows are skipped; this is a set operation over present cells.

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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> cells = gm.CellArray([cell, neighbor])
        >>> len(cells.compact(5))
        2
        """
    def uncompact(self: CellArray[_CellT | None], depth: int, /) -> CellArray[_CellT]:
        """Expand this cell set to a uniform depth, returned in the grid's public
        identity order. Expansion is bounded only by checked size arithmetic
        and allocator capacity; it has no fixed cell-count limit.
        Missing rows are skipped; this is a set operation over present cells.

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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> len(gm.CellArray([cell]).uncompact(8))
        7
        """
    def to_polygon(self) -> Polygon | MultiPolygon:
        """Dissolve this cell set into one outline geometry.
        Missing rows are skipped; this is a set operation over present cells.

        Returns
        -------
        Polygon or MultiPolygon

        Examples
        --------
        >>> import gometry as gm
        >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> gm.CellArray([cell]).to_polygon().geometry_type
        'Polygon'
        """
    token: _CellTokenProjection
    """Canonical string token of every cell, in order.

    For H3, S2, and tiles this is the text form of the numeric id exposed
    by `to_numpy()`. For Geohash it is the public string identity itself;
    Geohash `to_numpy()` instead returns typed `GeohashCell` objects.

    Returns
    -------
    list of str or None
        One canonical token per logical row (H3 hex, S2 token, geohash, or
        tile quadkey); ``None`` marks a missing row.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.CellArray([gm.GeohashCell('u33d')]).token
    ['u33d']
    """
    def __class_getitem__(cls, key: Any) -> types.GenericAlias:
        """See PEP 585"""
    def __len__(self) -> int:
        """Number of logical rows, including missing rows.

        Returns
        -------
        int
        """
    def __iter__(self) -> CellArrayIterator[_CellT_co]:
        """Iterate logical rows in row order, yielding cells or ``None``.

        Returns
        -------
        iterator of Cell or None
        """
    @overload
    def __getitem__(self, index: _IntArray0D, /) -> _CellT_co: ...
    @overload
    def __getitem__(self, index: _IntArray1D, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: _BoolArray1D, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(
        self, index: _IntArrayUnknownRank, /
    ) -> _CellT_co | CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _CellT_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> CellArray[_CellT_co]: ...
    @overload
    def __getitem__(
        self,
        index: SupportsIndex
        | slice
        | _BoolArray1D
        | _BoolLane
        | _IndexLane
        | _IntArray0D
        | _IntArray1D
        | _IntArrayUnknownRank,
        /,
    ) -> _CellT_co | CellArray[_CellT_co]:
        """Select logical rows by integer, slice, or fancy index.

        An ``int`` returns one cell object or ``None`` for a missing row. A
        ``slice`` or fancy index returns a ``CellArray`` of the same cell kind.

        Returns
        -------
        Cell, None, or CellArray
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
        values: Iterable[H3Cell | None],
        *,
        type: None = None,
    ) -> CellArray[H3Cell | None]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[S2Cell | None],
        *,
        type: None = None,
    ) -> CellArray[S2Cell | None]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[GeohashCell | None],
        *,
        type: None = None,
    ) -> CellArray[GeohashCell | None]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[Tile | None],
        *,
        type: None = None,
    ) -> CellArray[Tile | None]: ...
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
        values: Iterable[GeohashCell | str | None],
        *,
        type: type[GeohashCell],
    ) -> CellArray[GeohashCell | None]: ...
    @overload
    def __new__(
        cls,
        values: _IdArray | Iterable[H3Cell | _IntScalarInput | str],
        *,
        type: type[H3Cell],
    ) -> CellArray[H3Cell]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[H3Cell | _IntScalarInput | str | None],
        *,
        type: type[H3Cell],
    ) -> CellArray[H3Cell | None]: ...
    @overload
    def __new__(
        cls,
        values: _IdArray | Iterable[S2Cell | _IntScalarInput | str],
        *,
        type: type[S2Cell],
    ) -> CellArray[S2Cell]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[S2Cell | _IntScalarInput | str | None],
        *,
        type: type[S2Cell],
    ) -> CellArray[S2Cell | None]: ...
    @overload
    def __new__(
        cls,
        values: _IdArray | Iterable[Tile | _IntScalarInput | str],
        *,
        type: type[Tile],
    ) -> CellArray[Tile]: ...
    @overload
    def __new__(
        cls,
        values: Iterable[Tile | _IntScalarInput | str | None],
        *,
        type: type[Tile],
    ) -> CellArray[Tile | None]:
        """Construct a cell array from typed cells, ids, or tokens.

        The grid is inferred from a typed cell when ``type=None``. H3, S2, and
        Tile ids and any grid's tokens may accompany a typed cell from the same
        grid; Geohash cells accept tokens, not integer ids. Supply ``type=`` for
        raw-only values, numeric arrays or buffers, empty input, or input
        containing only ``None``. Wrap a single cell or supported id in an
        iterable; a bare token string is ambiguous, so wrap one token in a
        one-item iterable.

        Parameters
        ----------
        values : iterable, numpy.ndarray, or buffer
            Typed cells and ``None``; raw ids for H3/S2/Tile and string tokens
            are accepted when the grid can be inferred. Iterables are consumed
            in input order.
        type : type
            One of the native ``H3Cell``, ``S2Cell``, ``Tile``, or
            ``GeohashCell`` classes, required when the grid cannot be inferred.

        Returns
        -------
        CellArray
            An immutable array in input order; ``None`` entries remain missing
            rows.

        Raises
        ------
        TypeError
            If the grid cannot be inferred, ``type`` is unsupported, or a value
            does not match the selected grid.
        ParseError
            If an id or token is invalid for the selected grid.
        """
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
    def to_numpy(self: CellArray[GeohashCell | None]) -> npt.NDArray[np.object_]: ...
    @overload
    def to_numpy(
        self: CellArray[H3Cell | S2Cell | Tile],
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def to_numpy(
        self: CellArray[Cell | None],
    ) -> npt.NDArray[np.uint64] | npt.NDArray[np.object_]:
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
        >>> cell = gm.h3_cover(p, resolution=7)[0]
        >>> assert cell is not None
        >>> type(gm.CellArray([cell]).to_numpy()).__name__
        'ndarray'
        """
    @overload
    def __array__(
        self: CellArray[GeohashCell | None],
        dtype: None = None,
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]: ...
    @overload
    def __array__(
        self: CellArray[H3Cell | S2Cell | Tile],
        dtype: None = None,
        copy: bool | None = None,
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self: CellArray[Cell | None], dtype: None = None, copy: bool | None = None
    ) -> npt.NDArray[np.uint64] | npt.NDArray[np.object_]: ...
    @overload
    def __array__(
        self: CellArray[H3Cell | S2Cell | Tile],
        dtype: type[np.uint64] | np.dtype[np.uint64] | Literal['uint64', 'u8'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def __array__(
        self: CellArray[_CellT | None],
        dtype: type[object]
        | type[np.object_]
        | np.dtype[np.object_]
        | Literal['O', 'object'],
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
            opposite-endian numeric ids, gathered ids, and every object export
            raise because they materialize.

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
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        payload it keeps alive while iterating.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
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
        values: _IdArray | Iterable[H3Vertex | _IntScalarInput | str],
    ) -> Self:
        """Build an array of H3 vertices from vertex objects, ids, or tokens.

        Iterables may mix index objects, integer ids, and tokens. Integer NumPy
        arrays with zero or one dimension and unsigned 64-bit buffers are also
        accepted. Wrap one token in a one-item iterable.
        ``None`` is not a missing row.

        Parameters
        ----------
        values : iterable, numpy.ndarray, or uint64 buffer
            An iterable of H3Vertex objects, non-negative H3 ids, and/or vertex
            tokens. Integer NumPy arrays and uint64 buffers must contain valid
            H3 vertex ids. Byte strings and byte-formatted buffers are not collections.

        Returns
        -------
        H3VertexArray
            An immutable array in input order with an owned, read-only
            ``uint64`` id column.

        Raises
        ------
        TypeError
            If a value is not an H3 vertex, integer id, or token, or if an array
            has unsupported dimensions.
        ParseError
            If an id or token does not identify a valid H3 vertex.
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
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
        dtype: type[object]
        | type[np.object_]
        | np.dtype[np.object_]
        | Literal['O', 'object'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]:
        """NumPy array protocol.

        ``dtype=None`` / ``uint64`` exports the raw validated ids;
        ``dtype=object`` exports typed H3 values, matching iteration.

        Parameters
        ----------
        dtype : uint64 or object, optional
        copy : bool, optional
            ``False`` requires a zero-copy id export; gathered ids,
            opposite-endian numeric ids, and ``dtype=object`` raise
            because they must materialize.

        Returns
        -------
        numpy.ndarray
        """
    def value_counts(
        self,
    ) -> tuple[H3VertexArray, np.ndarray[tuple[int], np.dtype[np.int64]]]:
        """Unique values and counts, ordered by descending count, with
        first appearance breaking ties.

        Returns
        -------
        tuple
            ``(unique_values, counts)`` with read-only ``int64`` counts.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
        >>> unique, counts = edges.value_counts()
        >>> counts.tolist()
        [1, 1, 1, 1, 1, 1]
        """
    def factorize(
        self,
    ) -> tuple[np.ndarray[tuple[int], np.dtype[np.int64]], H3VertexArray]:
        """Factorize values into dense integer codes and first-seen uniques.

        Returns
        -------
        tuple
            ``(codes, unique_values)``.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
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
    def __repr__(self) -> str: ...
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
    def __getitem__(self, index: _IntArray0D, /) -> H3Vertex: ...
    @overload
    def __getitem__(self, index: _IntArray1D, /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: _BoolArray1D, /) -> H3VertexArray: ...
    @overload
    def __getitem__(
        self, index: _IntArrayUnknownRank, /
    ) -> H3Vertex | H3VertexArray: ...
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> H3Vertex: ...
    @overload
    def __getitem__(self, index: slice, /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> H3VertexArray: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> H3VertexArray: ...
    @overload
    def __getitem__(
        self,
        index: SupportsIndex
        | slice
        | _BoolArray1D
        | _BoolLane
        | _IndexLane
        | _IntArray0D
        | _IntArray1D
        | _IntArrayUnknownRank,
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
        """``sys.getsizeof`` support: Python's allocation base plus the logical
        payload it keeps alive while iterating.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
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
        values: _IdArray | Iterable[H3Edge | _IntScalarInput | str],
    ) -> Self:
        """Build an array of H3 directed edges from edge objects, ids, or tokens.

        Iterables may mix index objects, integer ids, and tokens. Integer NumPy
        arrays with zero or one dimension and unsigned 64-bit buffers are also
        accepted. Wrap one token in a one-item iterable.
        ``None`` is not a missing row.

        Parameters
        ----------
        values : iterable, numpy.ndarray, or uint64 buffer
            An iterable of H3Edge objects, non-negative H3 ids, and/or edge
            tokens. Integer NumPy arrays and uint64 buffers must contain valid
            directed-edge ids. Byte strings and byte-formatted buffers are not collections.

        Returns
        -------
        H3EdgeArray
            An immutable array in input order with an owned, read-only
            ``uint64`` id column.

        Raises
        ------
        TypeError
            If a value is not an H3 edge, integer id, or token, or if an array
            has unsupported dimensions.
        ParseError
            If an id or token does not identify a valid H3 directed edge.
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
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
        dtype: type[object]
        | type[np.object_]
        | np.dtype[np.object_]
        | Literal['O', 'object'],
        copy: bool | None = None,
    ) -> npt.NDArray[np.object_]:
        """NumPy array protocol.

        ``dtype=None`` / ``uint64`` exports the raw validated ids;
        ``dtype=object`` exports typed H3 values, matching iteration.

        Parameters
        ----------
        dtype : uint64 or object, optional
        copy : bool, optional
            ``False`` requires a zero-copy id export; gathered ids,
            opposite-endian numeric ids, and ``dtype=object`` raise
            because they must materialize.

        Returns
        -------
        numpy.ndarray
        """
    def value_counts(
        self,
    ) -> tuple[H3EdgeArray, np.ndarray[tuple[int], np.dtype[np.int64]]]:
        """Unique values and counts, ordered by descending count, with
        first appearance breaking ties.

        Returns
        -------
        tuple
            ``(unique_values, counts)`` with read-only ``int64`` counts.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
        >>> unique, counts = edges.value_counts()
        >>> counts.tolist()
        [1, 1, 1, 1, 1, 1]
        """
    def factorize(
        self,
    ) -> tuple[np.ndarray[tuple[int], np.dtype[np.int64]], H3EdgeArray]:
        """Factorize values into dense integer codes and first-seen uniques.

        Returns
        -------
        tuple
            ``(codes, unique_values)``.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> edges = cell.edges
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
    def __repr__(self) -> str: ...
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
    def __getitem__(self, index: _IntArray0D, /) -> H3Edge: ...
    @overload
    def __getitem__(self, index: _IntArray1D, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: _BoolArray1D, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: _IntArrayUnknownRank, /) -> H3Edge | H3EdgeArray: ...
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> H3Edge: ...
    @overload
    def __getitem__(self, index: slice, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: _BoolLane, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(self, index: _IndexLane, /) -> H3EdgeArray: ...
    @overload
    def __getitem__(
        self,
        index: SupportsIndex
        | slice
        | _BoolArray1D
        | _BoolLane
        | _IndexLane
        | _IntArray0D
        | _IntArray1D
        | _IntArrayUnknownRank,
        /,
    ) -> H3Edge | H3EdgeArray:
        """Select by integer, slice, or fancy index.

        An ``int`` returns one edge/vertex. A ``slice`` or fancy index
        returns an array of the same type.

        Returns
        -------
        H3Edge or H3Vertex or H3EdgeArray or H3VertexArray
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
    def __str__(self) -> str:
        """Return the canonical cell token."""
    def __repr__(self) -> str:
        """Return the angle-bracket cell display summary."""
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
        """Area of the cell in square meters, using its grid's area model.

        H3 and S2 use an authalic sphere; geohash and tile use the WGS 84
        ellipsoidal area of their lon/lat rectangle.

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
        """Pickle support: a cell is its identity value."""
    def __int__(self) -> int:
        """int(self)"""
    def __index__(self) -> int:
        """Return self converted to an integer, if self is suitable for use as an index into a list."""

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
        """``sys.getsizeof`` support for this heap-free vertex value."""
    def __new__(cls, value: H3Vertex | _IntScalarInput | str) -> Self:
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
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
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
        """``sys.getsizeof`` support for this heap-free edge value."""
    def __new__(cls, value: H3Edge | _IntScalarInput | str) -> Self:
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
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
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
    def reverse(self) -> H3Edge:
        """Reverse this directed edge from ``destination`` back to ``origin``.

        Returns
        -------
        H3Edge

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> edge = cell.edge(neighbor)
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
        >>> neighbor = cell.neighbors[0]
        >>> assert neighbor is not None
        >>> edge = cell.edge(neighbor)
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
    def __new__(
        cls,
        value: _FloatScalarInput,
        /,
        lat: _FloatScalarInput,
        *,
        precision: _IntScalar,
    ) -> GeohashCell:
        """One geohash cell from a token, lon/lat pair, or point geometry.

        Parameters
        ----------
        value : GeohashCell, str, float, or Point
            An existing cell, token, longitude of a ``lon, lat`` pair, or a
            point geometry.

        lat : float, optional
            Latitude when ``value`` is a scalar longitude.

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
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6)[0]
        >>> assert cell is not None
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
            The descendant cells in the grid's public identity order.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6)[0]
        >>> assert cell is not None
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
            The exact descendant count.

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6)[0]
        >>> assert cell is not None
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
            If a cell or token is not a valid cell.
        TypeError
            If ``other`` is not a valid cell object or token.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6)[0]
        >>> assert cell is not None
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
            If a cell or token is not a valid cell.
        TypeError
            If ``other`` is not a valid cell object or token.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6)[0]
        >>> assert cell is not None
        >>> cell.intersects(cell.parent())
        True
        """
    def __reduce__(self) -> tuple[object, tuple[str]]:
        """Pickle support: a cell is its identity value."""

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
    def __new__(cls, value: H3Cell | _IntScalarInput | str, /) -> H3Cell: ...
    @overload
    def __new__(cls, value: Point, /, *, resolution: _IntScalar) -> H3Cell: ...
    @overload
    def __new__(
        cls,
        value: _FloatScalarInput,
        /,
        lat: _FloatScalarInput,
        *,
        resolution: _IntScalar,
    ) -> H3Cell:
        """One H3 cell from an id, token, lon/lat pair, or point geometry.

        Parameters
        ----------
        value : H3Cell, int, str, float, or Point
            An existing cell, id, token, longitude of a ``lon, lat`` pair, or
            a point geometry.

        lat : float, optional
            Latitude when ``value`` is a scalar longitude.

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
            If a scalar coordinate is non-finite or out of range.

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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
            The descendant cells in the grid's public identity order.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> cell.is_neighbor(neighbor)
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> child = cell.children(resolution=8)[0]
        >>> assert child is not None
        >>> child.child_position(7)
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
            The exact descendant count (H3 pentagons have slightly fewer than hexagons).

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> neighbor = cell.neighbors[0]
        >>> assert neighbor is not None
        >>> edge = cell.edge(neighbor)
        >>> (edge.origin == cell, edge.destination == neighbor)
        (True, True)
        """
    @property
    def edges(self) -> H3EdgeArray:
        """The directed edges leaving this cell (6, or 5 on a pentagon).

        Returns
        -------
        H3EdgeArray
        """
    def grid_disk(
        self, k: int, *, max_cells: int | None = 1000000
    ) -> CellArray[H3Cell]:
        """Return cells within k grid steps (filled disk).

        Parameters
        ----------
        k : int
            Grid radius in steps (``>= 0``); ``k=0`` is this cell alone.
        max_cells : int or None, default 1000000
            Hard cap on returned cells. Pass ``None`` for unlimited traversal.

        Returns
        -------
        CellArray of H3Cell

        Raises
        ------
        GeometryError
            If ``k`` is negative or too large, or the result exceeds ``max_cells``.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> len(cell.grid_disk(1))
        7
        """
    def grid_ring(
        self, k: int, *, max_cells: int | None = 1000000
    ) -> CellArray[H3Cell]:
        """Return cells exactly ``k`` grid steps away (hollow ring).

        Parameters
        ----------
        k : int
            Grid radius in steps (``>= 0``); ``k=0`` is this cell alone.
        max_cells : int or None, default 1000000
            Hard cap on returned cells. Pass ``None`` for unlimited traversal.

        Returns
        -------
        CellArray of H3Cell

        Raises
        ------
        GeometryError
            If ``k`` is negative or too large, or the result exceeds ``max_cells``.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
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
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> neighbor = list(cell.neighbors)[0]
        >>> assert neighbor is not None
        >>> cell.grid_distance(neighbor)
        1
        """
    def grid_path(
        self, other: H3Cell | int | str, *, max_cells: int | None = 1000000
    ) -> CellArray[H3Cell]:
        """Grid path of cells to another cell.

        Parameters
        ----------
        other : H3Cell, int, or str
            The target cell; must share this cell's resolution.
        max_cells : int or None, default 1000000
            Hard cap on returned cells. Pass ``None`` for unlimited traversal.

        Returns
        -------
        CellArray of H3Cell

        Raises
        ------
        GeometryError
            If the cells cannot be connected or the result exceeds ``max_cells``.
        ParseError
            If ``other`` is not a valid H3 cell.

        Examples
        --------
        >>> import gometry as gm
        >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
        >>> assert cell is not None
        >>> nbr = list(cell.neighbors)[0]
        >>> assert nbr is not None
        >>> len(cell.grid_path(nbr))
        2
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
    def __new__(cls, value: S2Cell | _IntScalarInput | str, /) -> S2Cell: ...
    @overload
    def __new__(cls, value: Point, /, *, level: _IntScalar) -> S2Cell: ...
    @overload
    def __new__(
        cls,
        value: _FloatScalarInput,
        /,
        lat: _FloatScalarInput,
        *,
        level: _IntScalar,
    ) -> S2Cell:
        """One S2 cell from an id, token, lon/lat pair, or point geometry.

        Parameters
        ----------
        value : S2Cell, int, str, float, or Point
            An existing cell, id, token, longitude of a ``lon, lat`` pair, or
            a point geometry.

        lat : float, optional
            Latitude when ``value`` is a scalar longitude.

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
        >>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12)[0]
        >>> assert cell is not None
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
            The descendant cells in the grid's public identity order.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12)[0]
        >>> assert cell is not None
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
            The exact descendant count.

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12)[0]
        >>> assert cell is not None
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
    Geographic point ownership follows the emitted binary64 tile boundaries,
    with longitude west-inclusive/east-exclusive and latitude
    north-inclusive/south-exclusive ties. This is a representational boundary
    contract, not a claim of exact ideal-transcendental Web Mercator rounding.
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
    def __new__(cls, value: Tile | _IntScalarInput | str, /) -> Tile: ...
    @overload
    def __new__(cls, value: Point, /, *, zoom: _IntScalar) -> Tile: ...
    @overload
    def __new__(
        cls,
        /,
        *,
        lon: _FloatScalarInput,
        lat: _FloatScalarInput,
        zoom: _IntScalar,
    ) -> Tile: ...
    @overload
    def __new__(
        cls,
        /,
        *,
        x: _IntScalar,
        y: _IntScalar,
        zoom: _IntScalar,
    ) -> Tile:
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
        >>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10)[0]
        >>> assert cell is not None
        >>> parent = cell.parent()
        >>> assert parent is not None
        >>> str(parent)
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
            The descendant cells in the grid's public identity order.

        Raises
        ------
        GeometryError
            If the target depth is invalid for this cell.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10)[0]
        >>> assert cell is not None
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
            The exact descendant count.

        Raises
        ------
        GeometryError
            If the target depth is coarser than this cell's, or invalid.

        Examples
        --------

        >>> import gometry as gm
        >>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10)[0]
        >>> assert cell is not None
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
class ValidationReport:
    """A structured geometry-validity verdict.

    Returned by ``geom.validate()``: truthy when the geometry is valid;
    otherwise ``report.reason`` names the OGC violation, ``report.location``
    pinpoints it, ``report.path`` addresses the offending part, and
    ``report.code``/``report.detail`` provide stable machine-readable and
    human-readable issue data.
    Validation reports describe invalid geometries and their reasons.
    """
    def __new__(cls, _nonconstructible: Never, /) -> Self:
        """Validation reports are returned by ``geom.validate()``."""
    def __eq__(self, other: object, /) -> bool:
        """Return self==value."""
    def __hash__(self) -> int:
        """Return hash(self)."""
    def __sizeof__(self) -> int:
        """``sys.getsizeof`` support: Python's allocation base plus the retained geometry
        coordinate payload and any validation issue strings.

        Raises
        ------
        OverflowError
            If the reported size exceeds Python's ``Py_ssize_t`` range.
        """
    def __repr__(self) -> str:
        """Return the bounded representation of this validation report."""
    def __copy__(self) -> Self:
        """``copy.copy`` returns the report itself — it is an immutable value."""
    def __deepcopy__(self, memo: object) -> Self:
        """``copy.deepcopy`` returns the report itself: every field is immutable
        and holds no Python references.
        """
    def __reduce__(
        self,
    ) -> tuple[Callable[[Geometry], ValidationReport], tuple[Geometry]]:
        """Pickle support: serialize the geometry only; the verdict is recomputed
        on unpickle (never trusts derived state in the payload).
        """
    __match_args__: Final = ('valid', 'reason', 'location', 'path', 'code', 'detail')
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
    def code(self) -> str | None:
        """Stable machine-readable validity code, or ``None`` for a valid report.

        Returns
        -------
        str or None
        """
    @property
    def detail(self) -> str | None:
        """Detail for the first validity issue, or ``None`` for a valid report.

        Returns
        -------
        str or None
        """
    @property
    def location(self) -> tuple[float, float] | None:
        """``(x, y)`` location of the first problem, when known.

        For quotient-topology issues, the location is a canonical coordinate of
        the issue's contact set, chosen deterministically by minimum latitude
        then longitude.

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

@overload
def box(
    minx: _FloatScalarInput,
    miny: _FloatScalarInput,
    maxx: _FloatScalarInput,
    maxy: _FloatScalarInput,
    *,
    crs: CrsInput | None = None,
    wrap: None = None,
    ccw: bool = True,
    epoch: float | None = None,
) -> Polygon: ...
@overload
def box(
    minx: _FloatScalarInput,
    miny: _FloatScalarInput,
    maxx: _FloatScalarInput,
    maxy: _FloatScalarInput,
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
        geographic box across the CRS frame's longitude seam.

    crs : CRS-like, optional
        CRS label (EPSG code, authority string, or WKT); attached as metadata,
        coordinates are not transformed.
        With a geographic CRS, positive-size ``wrap=None`` bounds wholly inside
        the frame's native longitude and latitude ranges use chart edges along
        parallels and meridians. Only enough intermediate longitudes are
        inserted to keep each step below half the period; geographic area and
        length measure those stored edges. Other ``wrap=None`` bounds use the
        corner-only, unwrapped construction; the native-range directed-span
        guarantee does not apply on that path.

    wrap : {'split'}, optional
        Seam handling for boxes with a geographic CRS. ``'split'`` lets
        ``minx`` exceed ``maxx`` to cross the frame's longitude seam, returning
        a `MultiPolygon`. The default (``None``) leaves coordinates unwrapped
        and requires ``minx <= maxx``. Latitude bounds must lie within the frame's
        poles. Longitude endpoints must lie within its native half-period unless
        their eastward span is at least one period, which selects the full band.

    ccw : bool, default True
        If ``True`` the ring is counter-clockwise; ``False`` makes it clockwise.

    epoch : float, optional
        Coordinate epoch (decimal year) for time-dependent frames.

    Returns
    -------
    Polygon | MultiPolygon
        A rectangular polygon, or a MultiPolygon when ``wrap='split'`` crosses
        the frame's longitude seam.

    Raises
    ------
    InvalidGeometryError
        If a bound is non-finite or a minimum exceeds its maximum without
        ``wrap='split'``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``wrap='split'`` is used
        without a geographic CRS.
    GeometryError
        If ``epoch`` is not a finite decimal year or wrapped geographic bounds
        lie outside their CRS frame.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.box(0, 0, 2, 1).to_wkt()
    'POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'
    >>> gm.box(0, 0, 2, 1, ccw=False).to_wkt()
    'POLYGON ((0 0, 0 1, 2 1, 2 0, 0 0))'
    """

@overload
def points(
    x: _CoordinatesInput | Iterable[Point],
    y: None = None,
    *,
    z: FloatInput | None = None,
    m: FloatInput | None = None,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Point]: ...
@overload
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

    Pass either coordinate rows or separate ``x``/``y`` columns. Separate
    columns keep axis order explicit and accept scalars that broadcast across
    rows; when both are scalars, the result contains one point.

    Parameters
    ----------
    x : coordinate rows or float or sequence of float
        Coordinate rows as ``(x, y[, z[, m]])`` values, or the X column.
    y : float or sequence of float, optional
        Y column when ``x`` is an X column. Omit it when ``x`` contains rows.

    z, m : float or sequence of float, optional
        Z and M ordinates, broadcast like ``x``/``y``.

    crs : CRS-like, optional
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
        If ``epoch`` is invalid.

    See Also
    --------
    Point : Build a single point.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.points([0, 1], [0, 1]).to_wkt()
    ['POINT (0 0)', 'POINT (1 1)']
    >>> gm.points(1, 2).to_wkt()
    ['POINT (1 2)']
    >>> gm.points([(1, 2), (3, 4)]).to_wkt()
    ['POINT (1 2)', 'POINT (3 4)']
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
        Rectangle bounds per row. Scalars broadcast numpy-style; four scalar
        bounds produce a one-row array.

    crs : CRS-like, optional
        CRS applied to every box. Geographic rows follow the same native-range
        chart-edge and corner-only ``wrap=None`` rules as `box`. Geographic area
        and length measure the stored chart edges when the chart-edge path applies.

    wrap : {'split'}, optional
        Seam handling for geographic boxes — same as `box`.

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
        without a geographic CRS.
    GeometryError
        If ``epoch`` is invalid, or wrapped geographic bounds lie outside their
        CRS frame.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.boxes(0, 0, [1, 2], [1, 2]).to_wkt()
    ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))']
    """

def line_strings(
    values: Iterable[LineString | _CoordinatesInput],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[LineString]:
    """Create a ``GeometryArray`` of linestrings from coordinate rows or Linestrings.

    Parameters
    ----------
    values : sequence
        Each member is a ``LineString`` or a raw coordinate sequence accepted
        by `LineString`.

    crs : CRS-like, optional
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
        If a geometry row is not a ``LineString``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    CRSMismatchError
        If geometry rows do not share a CRS or coordinate epoch.
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
    values: Iterable[Polygon | _CoordinatesInput | _RingsInput],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Polygon]:
    """Create a ``GeometryArray`` of polygons from raw coordinates or Polygons.

    Parameters
    ----------
    values : sequence
        Each member is a ``Polygon``, a raw shell coordinate sequence, or
        ``[shell, *holes]``.

    crs : CRS-like, optional
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
        If a geometry row is not a ``Polygon``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    CRSMismatchError
        If geometry rows do not share a CRS or coordinate epoch.
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
    values: Iterable[MultiPoint | Iterable[Point | _Coordinate] | _CoordinatesInput],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[MultiPoint]:
    """Create a ``GeometryArray`` of multipoints from per-row inputs.

    Parameters
    ----------
    values : sequence
        Each member is a ``MultiPoint`` or input accepted by `MultiPoint`,
        including ``Point`` members and raw coordinate values.

    crs : CRS-like, optional
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
        If a geometry row is not a ``MultiPoint``, or a member is not a
        ``Point``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    CRSMismatchError
        If geometry rows or multipart members do not share a CRS or epoch.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.multi_points([[(0, 0), (1, 1)]]).to_wkt()
    ['MULTIPOINT ((0 0), (1 1))']
    """

def multi_line_strings(
    values: Iterable[MultiLineString | Iterable[LineString | _CoordinatesInput]],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[MultiLineString]:
    """Create a ``GeometryArray`` of multilinestrings from per-row inputs.

    Parameters
    ----------
    values : sequence
        Each member is a ``MultiLineString`` or input accepted by
        `MultiLineString`, including ``LineString`` members.

    crs : CRS-like, optional
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
        If a geometry row is not a ``MultiLineString``, or a member is not a
        ``LineString``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    CRSMismatchError
        If geometry rows or multipart members do not share a CRS or epoch.
    GeometryError
        If ``epoch`` is not a finite decimal year.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.multi_line_strings([[[(0, 0), (1, 1)]]]).to_wkt()
    ['MULTILINESTRING ((0 0, 1 1))']
    """

def multi_polygons(
    values: Iterable[
        MultiPolygon | Iterable[Polygon | _CoordinatesInput | _RingsInput]
    ],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[MultiPolygon]:
    """Create a ``GeometryArray`` of multipolygons from per-row inputs.

    Parameters
    ----------
    values : sequence
        Each member is a ``MultiPolygon`` or input accepted by `MultiPolygon`,
        including ``Polygon`` members and shell/ring coordinate shorthand.

    crs : CRS-like, optional
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
        If a geometry row is not a ``MultiPolygon``, or a member is not a
        ``Polygon``.
    CRSError
        If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    CRSMismatchError
        If geometry rows or multipart members do not share a CRS or epoch.
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
    geom: _GeometryBatch[Geometry | None], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def area(
    geom: Iterable[_GeometryLike | None], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def area(
    geom: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute area in CRS-natural units or with a ``unit`` override.

    For a ``GeometryCollection``, overlapping members count once, as the area
    of their union. Parts of a valid ``MultiPolygon`` cannot overlap.

    For geographic geometries, area is the ellipsoidal area of the region whose
    edges are straight in longitude/latitude.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values or None
        Input geometry, array, or iterable materialized as a row-preserving
        array. Missing rows return ``nan``.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    geom: _GeometryBatch[Geometry | None], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length(
    geom: Iterable[_GeometryLike | None], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length(
    geom: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute length in CRS-natural units or with a ``unit`` override.

    For a ``GeometryCollection``, length sums every member's length or
    perimeter, including overlapping members.

    For geographic geometries, length and perimeter are measured along edges
    straight in longitude/latitude on the CRS ellipsoid.

    Geographic polygon perimeter removes chart-cut copies only: within one
    ring, antiparallel coincident arcs at different longitude lifts cancel;
    on the canonical seam meridian, opposite-interior copies cancel across all
    rings, including a ring with itself. Same-side seam duplicates, off-seam
    same-lift retraces, and every other stored overlap remain in the perimeter.
    This seam convention is tied to the canonical seam: rotating the longitude
    origin can change which arcs are seam cuts.

    Arc identity uses exact canonical longitudes and admitted latitudes; no
    tolerance is applied. Between the two seam spellings, the longitude step is
    degenerate; differing latitudes still trace a meridian arc. An explicit
    one-period edge between non-seam longitudes follows the full circle. Planar
    and projected perimeter rules are unchanged.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values or None
        Input geometry, array, or iterable materialized as a row-preserving
        array. Missing rows return ``nan``.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    geom: _GeometryBatch[Geometry | None], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length_3d(
    geom: Iterable[_GeometryLike | None], *, unit: DistanceUnit | None = None
) -> npt.NDArray[np.float64]: ...
@overload
def length_3d(
    geom: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute 3D length in CRS-natural units or with a ``unit`` override.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of geometry-like values or None
        Input geometry, array, or iterable materialized as a row-preserving
        array. Missing rows return ``nan``.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.


        Geographic CRSs are not supported by 3D Euclidean metrics.

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
    reference: Geometry | GeometryArray[_GeometryOtherT],
    tolerance: FloatInput,
) -> GeometryArray[_GeometryT]: ...
@overload
def snap(
    geom: GeometryArray[_GeometryT | None],
    reference: Geometry | GeometryArray[_GeometryOtherT | None],
    tolerance: FloatInput,
) -> GeometryArray[_GeometryT | None]: ...
@overload
def snap(
    geom: _GeometryT,
    reference: GeometryArray[_GeometryOtherT],
    tolerance: FloatInput,
) -> GeometryArray[_GeometryT]: ...
@overload
def snap(
    geom: _GeometryT,
    reference: GeometryArray[_GeometryOtherT | None],
    tolerance: FloatInput,
) -> GeometryArray[_GeometryT | None]: ...
@overload
def snap(
    geom: Geometry | GeometryArray[Geometry | None],
    reference: Geometry | GeometryArray[Geometry | None],
    tolerance: FloatInput,
) -> Geometry | GeometryArray[Geometry | None]:
    """Snap vertices of ``geom`` onto ``reference`` within ``tolerance``.

    Each source vertex snaps independently to its nearest reference vertex at
    or below ``tolerance``; if none is in range, it stays unchanged. An exact
    computed-distance tie goes to the reference vertex with the smaller X,
    then Y, using IEEE total order. Multiple source vertices may snap to the
    same reference vertex.

    On each line segment, every reference vertex whose projection lies in the
    open segment, whose segment distance is at or below ``tolerance``, and
    whose distance from both endpoints is greater than ``tolerance`` is
    inserted at its reference X/Y. Insertions follow segment order; equal
    projections are ordered by reference X/Y. Snapped source vertices retain
    their own Z/M, while inserted vertices interpolate Z/M from the source
    segment. Consecutive line coordinates with identical X/Y are emitted once,
    keeping the first. If that leaves only one coordinate, the snapped start
    and end are retained as two endpoints, which may share X/Y. Closed rings
    are reclosed.

    Snapping may collapse polygon rings: collapsed holes are dropped and a
    collapsed shell produces an empty polygon. Snapping does not validate or
    repair topology, so a snapped polygon may be invalid.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Geometry whose vertices are moved.
    reference : Geometry or GeometryArray
        Target geometry to snap onto.
    tolerance : float or sequence of float
        Maximum finite non-negative snap distance in coordinate space:
        geographic CRSs use native angular units, projected CRSs use native
        linear units, and CRS-free geometries use raw coordinate units.

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
    left: GeometryArray[Point | None],
    right: Point | GeometryArray[Point | None],
    *,
    path: NavigationPath = 'geodesic',
) -> npt.NDArray[np.float64]: ...
@overload
def bearing(
    left: Point,
    right: GeometryArray[Point | None],
    *,
    path: NavigationPath = 'geodesic',
) -> npt.NDArray[np.float64]: ...
@overload
def bearing(
    left: Point | GeometryArray[Point | None],
    right: Point | GeometryArray[Point | None],
    *,
    path: NavigationPath = 'geodesic',
) -> float | npt.NDArray[np.float64]:
    """Initial bearing from one point to another, in degrees clockwise from north
    (``0..360``).

    Geodesic by default on a geographic CRS (on the CRS ellipsoid); grid
    azimuth on a projected or CRS-free point. ``path='rhumb'`` selects the
    constant compass course on a geographic CRS. An empty point yields ``nan``.

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
    point: GeometryArray[Point | None],
    start: Point | GeometryArray[Point | None],
    end: Point | GeometryArray[Point | None],
) -> npt.NDArray[np.float64]: ...
@overload
def cross_track_distance(
    point: Point,
    start: GeometryArray[Point | None],
    end: Point | GeometryArray[Point | None],
) -> npt.NDArray[np.float64]: ...
@overload
def cross_track_distance(
    point: Point,
    start: Point,
    end: GeometryArray[Point | None],
) -> npt.NDArray[np.float64]: ...
@overload
def cross_track_distance(
    point: Point | GeometryArray[Point | None],
    start: Point | GeometryArray[Point | None],
    end: Point | GeometryArray[Point | None],
) -> float | npt.NDArray[np.float64]:
    """Signed distance from a point to the great circle through ``start`` and
    ``end``, in meters.

    Positive when the point lies left of the directed path ``start -> end``,
    negative right, zero on it. Spherical on the CRS ellipsoid's mean radius.
    Geographic CRS only. An empty point yields ``nan``.

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
        If ``start`` and ``end`` are coincident or antipodal and therefore do
        not define a unique great circle.

    Examples
    --------
    >>> import gometry as gm
    >>> a, b = gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326)
    >>> probe = gm.Point(0.5, 0.1, crs=4326)
    >>> round(gm.cross_track_distance(probe, a, b) / 1000, 1)
    11.1
    """

@overload
def point_between(
    left: Point,
    right: Point,
    distance: ScalarFloatArray,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> Point: ...
@overload
def point_between(
    left: Point,
    right: Point,
    distance: _FloatScalarInput,
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
    distance: ScalarFloatArray,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> Point: ...
@overload
def point_between(
    left: Point,
    right: Point,
    distance: _FloatColumn1D,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: Point,
    right: Point,
    distance: _FloatScalarInput,
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
    distance: _FloatColumn1D,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point]: ...
@overload
def point_between(
    left: GeometryArray[Point | None],
    right: Point | GeometryArray[Point | None],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point | None]: ...
@overload
def point_between(
    left: Point,
    right: GeometryArray[Point | None],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> GeometryArray[Point | None]: ...
@overload
def point_between(
    left: GeometryArray[Point | None],
    right: Point | GeometryArray[Point | None],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point | None]: ...
@overload
def point_between(
    left: Point,
    right: GeometryArray[Point | None],
    distance: FloatInput,
    *,
    normalized: bool = False,
    path: NavigationPath = 'geodesic',
    unit: None = None,
) -> GeometryArray[Point | None]:
    """Return a point interpolated from ``left`` towards ``right``.

    CRS-aware like every metric: geodesic on a geographic CRS, planar on a
    projected CRS, and raw coordinate units when CRS-free. ``path='rhumb'`` follows
    the constant-bearing track on a geographic CRS. Z/M interpolate. An empty
    endpoint yields ``POINT EMPTY``.

    Parameters
    ----------
    left, right : Point or GeometryArray
        Endpoints; must share a CRS.
    distance : float or sequence of float
        Finite distance(s) from ``left`` (or fractions if ``normalized``).
        Values outside the segment clamp to its endpoints.
    normalized : bool, default False
        Treat ``distance`` as a fraction of the total distance.
    path : {'geodesic', 'rhumb'}, default 'geodesic'
        Route model. Rhumb paths require a geographic CRS and use meters.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.


        ``path='rhumb'`` uses meters and does not accept a unit override.

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
        If ``distance`` is non-finite.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.point_between(gm.Point(0, 0), gm.Point(2, 0), 0.5).to_wkt()
    'POINT (0.5 0)'
    """

@overload
def shared_paths(left: Geometry, right: Geometry) -> GeometryCollection: ...
@overload
def shared_paths(
    left: Geometry,
    right: _GeometryBatch[_GeometryT],
) -> GeometryArray[GeometryCollection]: ...
@overload
def shared_paths(
    left: _GeometryBatch[_GeometryT],
    right: Geometry,
) -> GeometryArray[GeometryCollection]: ...
@overload
def shared_paths(
    left: _GeometryBatch[_GeometryT],
    right: _GeometryBatch[_GeometryOtherT],
) -> GeometryArray[GeometryCollection]: ...
@overload
def shared_paths(
    left: Geometry,
    right: _GeometryBatch[_GeometryT | None],
) -> GeometryArray[GeometryCollection | None]: ...
@overload
def shared_paths(
    left: _GeometryBatch[_GeometryT | None],
    right: Geometry,
) -> GeometryArray[GeometryCollection | None]: ...
@overload
def shared_paths(
    left: _GeometryBatch[_GeometryT | None],
    right: _GeometryBatch[_GeometryOtherT | None],
) -> GeometryArray[GeometryCollection | None]: ...
@overload
def shared_paths(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> GeometryCollection | GeometryArray[GeometryCollection | None]:
    """Shared paths between two lineal geometries.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Two lineal operands. Scalar and sequence inputs broadcast pairwise;
        sequences must have equal length and compatible CRS/epoch frames.

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
def from_polyline(
    data: str,
    *,
    precision: int = 5,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Point | LineString: ...
@overload
def from_polyline(
    data: PandasSeries | PolarsSeries | PyArrowArray | PyArrowChunkedArray,
    *,
    precision: int = 5,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray[Point | LineString | None]: ...
@overload
def from_polyline(
    data: Iterable[str],
    *,
    precision: int = 5,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray[Point | LineString]: ...
@overload
def from_polyline(
    data: Iterable[str | None],
    *,
    precision: int = 5,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray[Point | LineString | None]:
    """Decode polyline text into ``LineString``/``Point`` geometries.

    The Google Encoded Polyline Algorithm Format — the compact lat/lon
    route encoding used by Google Maps, OSRM, and Valhalla. Accepts one
    string (returns a ``Point`` for one coordinate, otherwise a ``LineString``)
    or an iterable/dataframe/Arrow column of strings/``None`` rows (returns a
    `GeometryArray` with missing rows). Polylines are WGS84 by definition, so results carry OGC:CRS84
    unless ``crs=None`` explicitly requests a CRS-free result;
    ``epoch`` restores coordinate-epoch metadata on round-trip.

    Parameters
    ----------
    data : str, iterable of str, or dataframe/Arrow column
        Encoded polyline text or a pandas/Polars Series or PyArrow
        Array/ChunkedArray. Missing column values become missing rows.
    precision : int, default 5
        Decimal digits encoded per ordinate (``0`` to ``11``); 5 is the
        classic default, 6 the high-resolution variant.
    crs : CRS-like or None, default 'OGC:CRS84'
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
def contains(
    left: Geometry,
    right: Geometry,
    *,
    x: None = None,
    y: None = None,
) -> bool: ...
@overload
def contains(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    x: None = None,
    y: None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    *,
    x: None = None,
    y: None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: Geometry,
    right: None = None,
    *,
    x: _FloatScalarInput,
    y: _FloatScalarInput,
) -> bool: ...
@overload
def contains(
    left: Geometry,
    right: None = None,
    *,
    x: _FloatColumn1D,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: Geometry,
    right: None = None,
    *,
    x: FloatInput,
    y: _FloatColumn1D,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: _GeometryBatch[Geometry | None],
    right: None = None,
    *,
    x: FloatInput,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: None = None,
    *,
    x: FloatInput,
    y: FloatInput,
) -> bool | npt.NDArray[np.bool_]: ...
@overload
def contains(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    x: None = None,
    y: None = None,
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` contains ``right`` or contains the point ``(x, y)``.

    Returns ``True`` if no point of ``right`` lies outside ``left`` and the
    interiors of the two geometries share a point.
    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left : Geometry, GeometryArray, or iterable of Geometry or None
        Container geometry. Array and iterable rows retain their logical
        positions.
    right : Geometry, GeometryArray, iterable of Geometry or None, optional
        Candidate geometry; scalar and sequence operands broadcast pairwise,
        and two sequences must have equal lengths and compatible frames.
    x, y : float or sequence of float, keyword-only
        Raw coordinates in ``left``'s CRS. Supply both instead of ``right``;
        provide exactly one input mode.

    Returns
    -------
    bool or numpy.ndarray
        Whether the relation holds; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If ``x`` or ``y`` contains a non-finite coordinate, or the coordinate
        columns have incompatible lengths.
    TypeError
        If ``right`` is combined with ``x`` or ``y``, or if only one coordinate
        keyword is supplied.

    See Also
    --------
    within : Inverse relation.
    covers : Boundary-inclusive containment.
    intersects : Boundary-inclusive point test.

    Examples
    --------
    >>> import gometry as gm
    >>> square = gm.box(0, 0, 2, 2)
    >>> gm.contains(square, gm.Point(1, 1))
    True
    >>> gm.contains(square, gm.Point(2, 1))  # boundary: not contained
    False
    >>> gm.contains(square, x=1, y=1)
    True
    """

@overload
def contains_properly(left: Geometry, right: Geometry) -> bool: ...
@overload
def contains_properly(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_properly(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def contains_properly(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` contains ``right`` with no boundary contact.

    Like ``contains``, but ``right`` must lie entirely in the interior of
    ``left`` — touching the boundary of ``left`` anywhere fails (DE-9IM
    ``T**FF*FF*``). Evaluated from the exact DE-9IM relation matrix; geographic
    inputs use the longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Container and candidate rows. Scalar and sequence operands broadcast
        pairwise; sequences must have equal length and compatible CRS/epoch.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def within(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def within(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` lies within ``right``; inverse of ``contains``.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Candidate and container rows. Scalar and sequence operands broadcast
        pairwise; sequences must have equal length and compatible CRS/epoch.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def covers(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def covers(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` covers ``right``: every point of ``right`` lies in
    ``left``.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Container and candidate rows. Scalar and sequence operands broadcast
        pairwise; sequences must have equal length and compatible CRS/epoch.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def covered_by(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def covered_by(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` is covered by ``right``; inverse of ``covers``.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Candidate and container rows. Scalar and sequence operands broadcast
        pairwise; sequences must have equal length and compatible CRS/epoch.

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
def intersects(
    left: Geometry,
    right: Geometry,
    *,
    x: None = None,
    y: None = None,
) -> bool: ...
@overload
def intersects(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    x: None = None,
    y: None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    *,
    x: None = None,
    y: None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: Geometry,
    right: None = None,
    *,
    x: _FloatScalarInput,
    y: _FloatScalarInput,
) -> bool: ...
@overload
def intersects(
    left: Geometry,
    right: None = None,
    *,
    x: _FloatColumn1D,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: Geometry,
    right: None = None,
    *,
    x: FloatInput,
    y: _FloatColumn1D,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: _GeometryBatch[Geometry | None],
    right: None = None,
    *,
    x: FloatInput,
    y: FloatInput,
) -> npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: None = None,
    *,
    x: FloatInput,
    y: FloatInput,
) -> bool | npt.NDArray[np.bool_]: ...
@overload
def intersects(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    x: None = None,
    y: None = None,
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` share any point, or whether ``left``
    intersects the point ``(x, y)``.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left : Geometry, GeometryArray, or iterable of Geometry or None
        Geometry being tested. Array and iterable rows retain their logical
        positions.
    right : Geometry, GeometryArray, iterable of Geometry or None, optional
        Geometry compared with ``left``. Scalar and sequence operands broadcast
        pairwise, and two sequences must have equal lengths and compatible frames.
    x, y : float or sequence of float, keyword-only
        Raw coordinates in ``left``'s CRS. Supply both instead of ``right``;
        provide exactly one input mode.

    Returns
    -------
    bool or numpy.ndarray
        Whether the two geometries share any point; one result per input pair.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    InvalidGeometryError
        If ``x`` or ``y`` contains a non-finite coordinate, or the coordinate
        columns have incompatible lengths.
    TypeError
        If ``right`` is combined with ``x`` or ``y``, or if only one coordinate
        keyword is supplied.

    See Also
    --------
    disjoint : Logical negation.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.intersects(gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3))
    True
    >>> gm.intersects(gm.box(0, 0, 2, 2), x=2, y=1)
    True
    """

@overload
def disjoint(left: Geometry, right: Geometry) -> bool: ...
@overload
def disjoint(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def disjoint(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def disjoint(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` share no point; negation of
    ``intersects``.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Operands broadcast pairwise; sequences must have equal lengths and
        compatible CRS/epoch frames.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def touches(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def touches(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` touch only at boundaries.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Operands broadcast pairwise; sequences must have equal lengths and
        compatible CRS/epoch frames.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def crosses(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def crosses(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` cross.

    The dimension pair determines the relation. A line crosses a polygon when
    part of the line interior is inside the polygon and part is outside. Two
    curves cross when their interiors meet at a point.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Operands broadcast pairwise; sequences must have equal lengths and
        compatible CRS/epoch frames.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def overlaps(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def overlaps(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` overlap (same dimension, partial
    interior overlap).

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Operands broadcast pairwise; sequences must have equal lengths and
        compatible CRS/epoch frames.

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
def relate(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> list[str | None]: ...
@overload
def relate(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> list[str | None]: ...
@overload
def relate(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> str | list[str | None]:
    """Compute the DE-9IM intersection matrix string for ``left`` and
    ``right``.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.

    Returns
    -------
    str or list of str or None
        The nine-character DE-9IM pattern; one per input pair. A missing input
        row yields ``None`` in its result slot.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    pattern: str,
) -> npt.NDArray[np.bool_]: ...
@overload
def relate_pattern(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    pattern: str,
) -> npt.NDArray[np.bool_]: ...
@overload
def relate_pattern(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    pattern: str,
) -> bool | npt.NDArray[np.bool_]:
    """Test whether two geometries' DE-9IM matrix matches a pattern.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.
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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def equals(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def equals(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` are spatially equal.

    Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
    longitude and pole quotient.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_identical(
    left: Geometry, right: _GeometryBatch[Geometry | None]
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_identical(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
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
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        The two operands. Scalar and sequence inputs broadcast pairwise;
        sequences must have equal length.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    tolerance: FloatInput = 0.0,
    *,
    include_z: bool = True,
    include_m: bool = True,
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_exact(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    tolerance: FloatInput = 0.0,
    *,
    include_z: bool = True,
    include_m: bool = True,
) -> npt.NDArray[np.bool_]: ...
@overload
def equals_exact(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
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
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        The two operands. Scalar and sequence inputs broadcast pairwise;
        sequences must have equal length and compatible CRS/epoch frames.
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
    left: Point,
    right: Point,
    *,
    path: Literal['rhumb'],
    unit: None = None,
) -> float: ...
@overload
def distance(
    left: _GeometryBatch[Point | None],
    right: Point | _GeometryBatch[Point | None],
    *,
    path: Literal['rhumb'],
    unit: None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Point,
    right: _GeometryBatch[Point | None],
    *,
    path: Literal['rhumb'],
    unit: None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Point | _GeometryBatch[Point | None],
    right: Point | _GeometryBatch[Point | None],
    *,
    path: Literal['rhumb'],
    unit: None = None,
) -> float | npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Geometry,
    right: Geometry,
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> float: ...
@overload
def distance(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    path: Literal['geodesic'] = 'geodesic',
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute the minimum distance between ``left`` and ``right``.

    CRS-aware: geodesic meters on a geographic CRS, native linear units on a
    projected CRS, raw coordinate units when CRS-free. On a geographic CRS,
    point-to-point distances use ellipsoidal geodesics, and shape edges are
    straight in the CRS's longitude/latitude chart with short-way longitude
    across the antimeridian. Filled polygon interiors participate. For two
    points, ``path='rhumb'`` measures the constant-bearing route in meters on a
    geographic CRS.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.
    path : {'geodesic', 'rhumb'}, default varies by call form
        Route for point operands. Omit for geodesic distance; rhumb distance
        requires ``path='rhumb'`` and a geographic CRS and uses meters.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    GeometryTypeError
        If ``path='rhumb'`` is used with non-point operands.
    CRSError
        If ``path='rhumb'`` is used without a geographic CRS.
    GeometryError
        If ``unit='meters'`` is requested for a CRS-free geometry or a unit
        override is supplied with ``path='rhumb'``.

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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance_3d(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def distance_3d(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute the minimum 3D (Euclidean) distance between two geometries,
    measured for their CRS.

    Distance is over linework: points/multipoints as degenerate segments,
    polygons via their boundary rings. Part of the 3D Euclidean family with
    ``length_3d`` only — area and perimeter are inherently 2D (XY footprint)
    and have no 3D form; the 3D extent is ``bounds`` with ``min_z``/``max_z``.

    A projected CRS gives native linear units and a CRS-free geometry gives
    raw coordinate units — the same defaults as the 2D ``distance``. A geographic
    CRS raises under every ``unit``: a Euclidean norm cannot combine degrees
    with meter heights. Every vertex on both operands must carry a Z ordinate.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    max_error: float | None = None,
    unit: DistanceUnit | None = None,
) -> float: ...
@overload
def hausdorff_distance(
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    max_error: float | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def hausdorff_distance(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    *,
    max_error: float | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def hausdorff_distance(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    max_error: float | None = None,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Compute the continuous Hausdorff (set-to-set) distance between ``left``
    and ``right``.

    CRS-aware: geodesic meters on a geographic CRS, native linear units on a
    projected CRS, raw coordinate units when CRS-free. Geographic Hausdorff
    ranges continuously over straight chart edges and supports point and
    linework operands only; filled polygon interiors require ``unit='planar'``
    or a projected CRS. Planar results use a certified relative bound by
    default or a caller-supplied absolute bound.

    Planar polygons are filled sets, including their interiors. For example,
    a square ``[-2, 2] by [-2, 2]`` and the same square with a diamond hole at
    ``(0, 1), (1, 0), (0, -1), (-1, 0)`` have filled-set Hausdorff distance
    ``1 / sqrt(2)``, whereas their boundary Hausdorff distance is ``1.5``.
    Shapely's discrete and densified Hausdorff forms measure boundaries instead.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.
    max_error : float, optional
        Positive finite scalar absolute error bound in final result units.
        Planar results default to a certified relative error of at most
        ``1e-9``. Omit it for geographic Hausdorff, which uses a fixed
        radius-scaled algorithmic gap rather than a caller-requested bound.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



    Returns
    -------
    float or numpy.ndarray
        One result per input pair. A planar result is zero exactly when the
        operands have equal closed support; unequal positive distances remain
        positive even when smaller than the effective error bound. An empty
        operand yields ``inf``, as in ``distance``.

    Raises
    ------
    CRSError
        If the CRS lacks linear axis units for a metric result, or a geographic
        operand includes a filled polygon interior, or the geographic source
        frontier cannot close its numerical gap before binary64 parameter
        refinement stops.
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``max_error`` is not a positive finite scalar or is supplied for a
        geodesic result; if ``unit='meters'`` is requested for a CRS-free
        geometry; or if a finite planar result cannot be certified within
        representable output and the available generated-work or memory
        resources.

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
    >>> abs(gm.hausdorff_distance(a, b, max_error=1e-12) - 3.0) <= 1e-12
    True
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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def frechet_distance(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.float64]: ...
@overload
def frechet_distance(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    densify: FloatInput | None = None,
    unit: DistanceUnit | None = None,
) -> float | npt.NDArray[np.float64]:
    """Discrete Fréchet distance between two linestrings.

    Measured for the CRS like ``distance``: geodesic meters on a geographic
    CRS, native linear units on a projected CRS, raw coordinate units when CRS-free.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        The line operands; scalar and sequence inputs broadcast pairwise.
    densify : float, optional
        Subdivide every segment into pieces no longer than this
        fraction of its length (in ``(0, 1]``) before measuring,
        tightening the discrete vertex metric toward the continuous
        one. Omitted measures vertices only.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    right: _GeometryBatch[_GeometryT],
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point], GeometryArray[Point]]: ...
@overload
def nearest_points(
    left: _GeometryBatch[_GeometryT],
    right: Geometry,
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point], GeometryArray[Point]]: ...
@overload
def nearest_points(
    left: _GeometryBatch[_GeometryT],
    right: _GeometryBatch[_GeometryOtherT],
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point], GeometryArray[Point]]: ...
@overload
def nearest_points(
    left: Geometry,
    right: _GeometryBatch[_GeometryT | None],
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point | None], GeometryArray[Point | None]]: ...
@overload
def nearest_points(
    left: _GeometryBatch[_GeometryT | None],
    right: Geometry,
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point | None], GeometryArray[Point | None]]: ...
@overload
def nearest_points(
    left: _GeometryBatch[_GeometryT | None],
    right: _GeometryBatch[_GeometryOtherT | None],
    *,
    unit: DistanceUnit | None = None,
) -> tuple[GeometryArray[Point | None], GeometryArray[Point | None]]: ...
@overload
def nearest_points(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    unit: DistanceUnit | None = None,
) -> (
    tuple[Point, Point]
    | tuple[GeometryArray[Point | None], GeometryArray[Point | None]]
):
    """Return the closest pair of points between two geometries.

    Realizes the same minimizer as ``distance``: point-to-point distance is
    geodesic on a geographic CRS, planar on a projected one, and in coordinate
    space when CRS-free. Geographic shape edges are straight in the CRS's
    longitude/latitude chart, using short-way longitude across the antimeridian;
    filled polygon interiors participate. Planar and projected metrics accept
    any geometry pair, not only points (the witness on a polygon/line may be an
    edge-interior point). The connecting line is ``shortest_line``. An empty operand yields
    ``(POINT EMPTY, POINT EMPTY)``, and the pair is dropped to the operands'
    common axes so it matches ``shortest_line``'s endpoints exactly.
    Equal-distance witnesses prefer an interior projection over a point or
    endpoint, then source order. Geographic metrics apply this order only to
    equal evaluated backend distances; analytic branches retain their available
    candidate. This does not assert an exact geodesic minimum.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames. Unpack order is point on
        ``left``, then on ``right``.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    left: Geometry,
    right: _GeometryBatch[_GeometryT],
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString]: ...
@overload
def shortest_line(
    left: _GeometryBatch[_GeometryT],
    right: Geometry,
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString]: ...
@overload
def shortest_line(
    left: _GeometryBatch[_GeometryT],
    right: _GeometryBatch[_GeometryOtherT],
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString]: ...
@overload
def shortest_line(
    left: Geometry,
    right: _GeometryBatch[_GeometryT | None],
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString | None]: ...
@overload
def shortest_line(
    left: _GeometryBatch[_GeometryT | None],
    right: Geometry,
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString | None]: ...
@overload
def shortest_line(
    left: _GeometryBatch[_GeometryT | None],
    right: _GeometryBatch[_GeometryOtherT | None],
    *,
    unit: DistanceUnit | None = None,
) -> GeometryArray[LineString | None]: ...
@overload
def shortest_line(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    *,
    unit: DistanceUnit | None = None,
) -> LineString | GeometryArray[LineString | None]:
    """Return the shortest connecting line between two geometries (CRS-aware).

    ``nearest_points`` as a connecting ``LineString`` — degenerate when the
    geometries touch. Planar and projected metrics accept any geometry pair, not
    only points, and the witness is the closest approach, so an endpoint may
    land on an edge interior rather than a vertex. On a geographic CRS,
    point-to-point separation uses ellipsoidal geodesics. Shape edges are
    straight in the CRS-native periodic longitude/latitude chart: admitted
    ordinary edges use their shortest periodic step, while an explicitly stored
    full-period circle edge retains its full turn. See the [CRS guide's
    polar-ring and full-turn-edge rules](../../guide/crs.md#polar-rings-and-full-turn-edges).
    Filled polygon interiors participate.
    The returned line follows the chart-edge model. With the CRS-aware
    geographic metric, its ellipsoidal length is no shorter than the witness
    separation in exact geometry, subject to binary64 rounding. If
    ``unit='planar'`` overrides the metric, measure the line with the same
    ``unit`` when comparing lengths. Publication may add vertices to preserve
    the computed chart edge; do not assume the line always stores exactly two
    coordinates. An empty operand yields
    ``LINESTRING EMPTY`` (the output-type sentinel, like
    ``distance``'s ``inf``). Output ordinates are the operands' common axes (an
    XYZM line versus an XY point gives an XY line). See ``nearest_points`` for
    the witnesses and ``distance`` for their metric separation.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    geom: LineString | MultiLineString,
    splitter: Point | MultiPoint | GeometryArray[Point | MultiPoint | None],
    *,
    tolerance: float = 0.0,
) -> GeometryArray[LineString]: ...
@overload
def split(
    geom: GeometryArray[LineString | MultiLineString],
    splitter: Point | MultiPoint | GeometryArray[Point | MultiPoint],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString]]: ...
@overload
def split(
    geom: GeometryArray[LineString | MultiLineString | None],
    splitter: Point | MultiPoint | GeometryArray[Point | MultiPoint],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString] | None]: ...
@overload
def split(
    geom: GeometryArray[LineString | MultiLineString],
    splitter: GeometryArray[Point | MultiPoint | None],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString] | None]: ...
@overload
def split(
    geom: GeometryArray[LineString | MultiLineString | None],
    splitter: GeometryArray[Point | MultiPoint | None],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString] | None]: ...
@overload
def split(
    geom: Geometry,
    splitter: Geometry | GeometryArray[Geometry | None],
    *,
    tolerance: float = 0.0,
) -> GeometryArray[LineString]: ...
@overload
def split(
    geom: GeometryArray[Geometry],
    splitter: Geometry | GeometryArray[Geometry],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString]]: ...
@overload
def split(
    geom: GeometryArray[Geometry | None],
    splitter: Geometry | GeometryArray[Geometry | None],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString] | None]: ...
@overload
def split(
    geom: GeometryArray[Geometry],
    splitter: GeometryArray[Geometry | None],
    *,
    tolerance: float = 0.0,
) -> Groups[GeometryArray[LineString] | None]:
    """Split lineal geometry by point splitter(s).

    Each piece retains source vertices, including consecutive equal-X/Y
    vertices whose active Z/M ordinates differ.

    Parameters
    ----------
    geom : Geometry or GeometryArray
        Lineal geometry to split.
    splitter : Geometry or GeometryArray
        Point or multipoint cutter.
    tolerance : float, keyword-only, optional
        Coordinate-space distance within which a splitter point counts as
        on the line and near-equal cut offsets collapse: native angular units
        on geographic CRSs, native linear units on projected CRSs, and raw
        coordinate units when CRS-free. The default ``0.0``
        is exact topological membership (a point splits only when it lies
        exactly on the linework) with identity deduplication.

    Returns
    -------
    GeometryArray or Groups[GeometryArray]
        A scalar line returns all split parts in one `GeometryArray`, even when
        its cutter is an array of points. A line array returns one `Groups` row
        per source line; an array cutter is paired row by row. Missing line or
        cutter rows produce ``None`` groups; present rows with no split parts
        produce empty groups.

    Raises
    ------
    GeometryError
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
    >>> pieces = gm.split(gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]), gm.Point(1, 0))
    >>> pieces.offsets.tolist()
    [0, 2]
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
    left: _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    distance: FloatInput,
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def dwithin(
    left: Geometry,
    right: _GeometryBatch[Geometry | None],
    distance: FloatInput,
    *,
    unit: DistanceUnit | None = None,
) -> npt.NDArray[np.bool_]: ...
@overload
def dwithin(
    left: Geometry | _GeometryBatch[Geometry | None],
    right: Geometry | _GeometryBatch[Geometry | None],
    distance: FloatInput,
    *,
    unit: DistanceUnit | None = None,
) -> bool | npt.NDArray[np.bool_]:
    """Test whether ``left`` and ``right`` are within ``distance`` of each other.

    On a geographic CRS, point-to-point distances use ellipsoidal geodesics, and
    shape edges are straight in the CRS's longitude/latitude chart with
    short-way longitude across the antimeridian. Filled polygon interiors
    participate.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of Geometry or None
        Scalar and sequence operands broadcast pairwise; sequences must have
        equal length and compatible CRS/epoch frames.
    distance : float or sequence of float
        CRS-aware distance threshold: geodesic meters on a geographic CRS,
        native linear units on a projected CRS, raw coordinate units when
        CRS-free. A scalar
        applies to every pair; with an array operand, an iterable supplies one
        threshold per result row. Positive infinity is accepted.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



    Returns
    -------
    bool or numpy.ndarray
        One result per input pair. Array results retain one boolean per input
        row; missing rows and present empty geometries are ``False`` rather
        than being dropped. Positive infinity matches only admitted non-empty
        operands.

    Raises
    ------
    CRSMismatchError
        If the operands' CRS or coordinate-epoch metadata differ.
    GeometryError
        If ``distance`` is negative, NaN, or negative infinity, or
        ``unit='meters'`` is requested for a CRS-free geometry. Positive
        infinity is accepted and matches every pair of admitted non-empty
        operands.
    CRSError
        If geographic point-edge search exhausts its work allowance (a
        binary64 midpoint-depth factor times the current live frontier, subject
        to a 16,384-evaluation ceiling) or parameter precision ends without
        certifying the threshold.

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
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: Geometry,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[False] = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> np.ndarray[tuple[int], np.dtype[np.int64]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: Geometry,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[True],
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> tuple[np.ndarray[tuple[int], np.dtype[np.int64]], npt.NDArray[np.float64]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: _GeometryBatch[Geometry],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[False] = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: _GeometryBatch[Geometry | None],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[False] = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: _GeometryBatch[Geometry],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[True],
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> tuple[
    Groups[np.ndarray[tuple[int], np.dtype[np.int64]]],
    npt.NDArray[np.float64],
]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: _GeometryBatch[Geometry | None],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: Literal[True],
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> tuple[
    Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
    npt.NDArray[np.float64],
]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: Geometry,
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: bool = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> npt.NDArray[np.int64] | tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]: ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: _GeometryBatch[Geometry],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: bool = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> (
    Groups[np.ndarray[tuple[int], np.dtype[np.int64]]]
    | tuple[
        Groups[np.ndarray[tuple[int], np.dtype[np.int64]]],
        npt.NDArray[np.float64],
    ]
): ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: _GeometryBatch[Geometry | None],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: bool = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> (
    Groups[npt.NDArray[np.int64] | None]
    | tuple[Groups[npt.NDArray[np.int64] | None], npt.NDArray[np.float64]]
): ...
@overload
def nearest(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    geom: Geometry | _GeometryBatch[Geometry | None],
    *,
    k: int = 1,
    max_distance: float | None = None,
    return_distance: bool = False,
    unit: DistanceUnit | None = None,
    exclusive: bool = False,
    ties: bool = False,
) -> (
    np.ndarray[tuple[int], np.dtype[np.int64]]
    | Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None]
    | tuple[np.ndarray[tuple[int], np.dtype[np.int64]], npt.NDArray[np.float64]]
    | tuple[
        Groups[np.ndarray[tuple[int], np.dtype[np.int64]] | None],
        npt.NDArray[np.float64],
    ]
):
    """Find the nearest of `values` to each query geometry (builds an index).

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of geometry-like values or None
        Candidate geometries to index and search.
    geom : Geometry, GeometryArray, or iterable of Geometry or None
        Query geometry or one query per sequence row. Missing rows produce
        empty result groups and retain their logical positions.
    k : int, default 1
        Number of nearest neighbors to return per query; ``0`` returns no
        neighbors.
    max_distance : float, optional
        Ignore candidates farther than this from the query. Must be finite and
        non-negative.
    return_distance : bool
        If ``True``, return distances alongside handles — ``(indices,
        distances)`` for a scalar query, ``(matches, distances)`` for an
        array query. The distance-returning form requires
        ``return_distance=True``.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.


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
        for a scalar query, CSR ``Groups`` for an array or iterable query. With
        ``return_distance=True``, plain tuple field order is ``(indices,
        distances)`` for a scalar query or ``(matches, distances)`` for an
        array or iterable query (distances parallel to ``matches.values``).

    Raises
    ------
    GeometryError
        If ``k`` is negative, ``max_distance`` is negative or non-finite, or
        ``unit='meters'`` is requested for a CRS-free geometry.
    CRSMismatchError
        If query and indexed geometries have different CRS or coordinate-epoch
        metadata.

    Examples
    --------
    >>> import gometry as gm
    >>> sites = [gm.Point(0, 0), gm.Point(5, 5)]
    >>> gm.nearest(sites, gm.Point(4, 4))
    array([1])
    """

@overload
def join(
    left: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    right: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    predicate: Literal['dwithin'],
    distance: float,
    unit: DistanceUnit | None = None,
) -> tuple[
    np.ndarray[tuple[int], np.dtype[np.int64]],
    np.ndarray[tuple[int], np.dtype[np.int64]],
]: ...
@overload
def join(
    left: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    right: Geometry | _GeometryBatch[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    predicate: TopologicalPredicate = 'intersects',
    distance: None = None,
    unit: DistanceUnit | None = None,
) -> tuple[
    np.ndarray[tuple[int], np.dtype[np.int64]],
    np.ndarray[tuple[int], np.dtype[np.int64]],
]:
    """Perform a spatial join between two geometry collections via an internal index.

    Parameters
    ----------
    left, right : Geometry, GeometryArray, or iterable of geometry-like values or None
        The two geometry collections to join. Missing rows produce no pairs;
        their original row positions are preserved in every returned id.
    predicate : str
        Spatial predicate each returned pair must satisfy — one of
        ``'intersects'``, ``'contains'``, ``'contains_properly'``,
        ``'covers'``, ``'within'``, ``'covered_by'``, ``'equals'``,
        ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
        Topological forms omit ``predicate`` to use ``'intersects'``;
        ``'dwithin'`` requires it explicitly.
    distance : float, optional
        Required when ``predicate='dwithin'``: the maximum separation, in
        CRS-natural units — geodesic meters on a geographic CRS, native
        linear units on a projected CRS, raw coordinate units when CRS-free. Finite
        non-negative values and positive infinity are accepted; missing or
        empty rows produce no pairs.
    unit : {'planar', 'meters'}, default None
        ``None`` (the default) follows the CRS: geographic shape area and length
        use ellipsoidal chart-edge measures while point distances use geodesics;
        projected measurements use native linear units, and CRS-free measurements
        use raw coordinate units (area results square the corresponding unit).
        ``'planar'`` uses Cartesian coordinate space, with geographic coordinates
        in their native angular units. ``'meters'`` uses the CRS-aware metric and
        requires a CRS.



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
    CRSError
        If geographic ``dwithin`` refinement cannot prove a result within the
        point-edge evaluation budget.

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
    left: Point,
    right: Geometry,
) -> Point: ...
@overload
def intersection(
    left: Geometry,
    right: Point,
) -> Point: ...
@overload
def intersection(
    left: MultiPoint,
    right: Geometry,
) -> MultiPoint | Point: ...
@overload
def intersection(
    left: Geometry,
    right: MultiPoint,
) -> MultiPoint | Point: ...
@overload
def intersection(
    left: GeometryArray[Point],
    right: Geometry | GeometryArray[_GeometryOtherT],
) -> GeometryArray[Point]: ...
@overload
def intersection(
    left: Point,
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[Point]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT],
    right: Point | GeometryArray[Point],
) -> GeometryArray[Point]: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray[Point],
) -> GeometryArray[Point]: ...
@overload
def intersection(
    left: GeometryArray[MultiPoint],
    right: Geometry | GeometryArray[_GeometryOtherT],
) -> GeometryArray[MultiPoint | Point]: ...
@overload
def intersection(
    left: MultiPoint,
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[MultiPoint | Point]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT],
    right: MultiPoint | GeometryArray[MultiPoint],
) -> GeometryArray[MultiPoint | Point]: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray[MultiPoint],
) -> GeometryArray[MultiPoint | Point]: ...
@overload
def intersection(
    left: GeometryArray[Point | None],
    right: Geometry | GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Point | None]: ...
@overload
def intersection(
    left: Point,
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Point | None]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT | None],
    right: Point | GeometryArray[Point | None],
) -> GeometryArray[Point | None]: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray[Point | None],
) -> GeometryArray[Point | None]: ...
@overload
def intersection(
    left: GeometryArray[MultiPoint | None],
    right: Geometry | GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[MultiPoint | Point | None]: ...
@overload
def intersection(
    left: MultiPoint,
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[MultiPoint | Point | None]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT | None],
    right: MultiPoint | GeometryArray[MultiPoint | None],
) -> GeometryArray[MultiPoint | Point | None]: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray[MultiPoint | None],
) -> GeometryArray[MultiPoint | Point | None]: ...
@overload
def intersection(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray[_GeometryT],
) -> GeometryArray[Geometry]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT],
    right: Geometry,
) -> GeometryArray[Geometry]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT],
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[Geometry]: ...
@overload
def intersection(
    left: Geometry,
    right: GeometryArray[_GeometryT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT | None],
    right: Geometry,
) -> GeometryArray[Geometry | None]: ...
@overload
def intersection(
    left: GeometryArray[_GeometryT | None],
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def intersection(
    left: Geometry | GeometryArray[Geometry | None],
    right: Geometry | GeometryArray[Geometry | None],
) -> Geometry | GeometryArray[Geometry | None]:
    """Compute the set-theoretic intersection of ``left`` and ``right``.

    Evaluated in the coordinate plane. In a geographic CRS, topology resolves
    longitude aliases, seam spellings, and pole longitudes in the CRS-native
    frame; seam-crossing inputs are split into chart pieces before construction.
    A closed axis-aligned rectangle containing the other operand's raw bounds
    retains that operand verbatim when its bounds are sound in the operation's frame.

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
        If either operand has coordinates outside its geographic CRS domain.
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.

    Examples
    --------
    >>> import gometry as gm
    >>> left, right = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
    >>> gm.intersection(left, right).to_wkt()
    'POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'
    """

@overload
def union(
    left: Point | MultiPoint,
    right: Point | MultiPoint,
) -> Point | MultiPoint: ...
@overload
def union(
    left: GeometryArray[Point | MultiPoint],
    right: Point | MultiPoint | GeometryArray[Point | MultiPoint],
) -> GeometryArray[Point | MultiPoint]: ...
@overload
def union(
    left: Point | MultiPoint,
    right: GeometryArray[Point | MultiPoint],
) -> GeometryArray[Point | MultiPoint]: ...
@overload
def union(
    left: GeometryArray[Point | MultiPoint | None],
    right: Point | MultiPoint | GeometryArray[Point | MultiPoint | None],
) -> GeometryArray[Point | MultiPoint | None]: ...
@overload
def union(
    left: Point | MultiPoint,
    right: GeometryArray[Point | MultiPoint | None],
) -> GeometryArray[Point | MultiPoint | None]: ...
@overload
def union(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def union(
    left: Geometry,
    right: GeometryArray[_GeometryT],
) -> GeometryArray[Geometry]: ...
@overload
def union(
    left: GeometryArray[_GeometryT],
    right: Geometry,
) -> GeometryArray[Geometry]: ...
@overload
def union(
    left: GeometryArray[_GeometryT],
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[Geometry]: ...
@overload
def union(
    left: Geometry,
    right: GeometryArray[_GeometryT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def union(
    left: GeometryArray[_GeometryT | None],
    right: Geometry,
) -> GeometryArray[Geometry | None]: ...
@overload
def union(
    left: GeometryArray[_GeometryT | None],
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def union(
    left: Geometry | GeometryArray[Geometry | None],
    right: Geometry | GeometryArray[Geometry | None],
) -> Geometry | GeometryArray[Geometry | None]:
    """Compute the set-theoretic union of ``left`` and ``right``.

    Evaluated in the coordinate plane. In a geographic CRS, topology resolves
    longitude aliases, seam spellings, and pole longitudes in the CRS-native
    frame; seam-crossing inputs are split into chart pieces before construction.

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
        If either operand has coordinates outside its geographic CRS domain.
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.

    See Also
    --------
    union_all : Union of many geometries at once.

    Examples
    --------
    >>> import gometry as gm
    >>> gm.union(gm.box(0, 0, 1, 2), gm.box(1, 0, 2, 2)).to_wkt()
    'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'
    """

@overload
def difference(
    left: Point,
    right: Geometry,
) -> Point: ...
@overload
def difference(
    left: MultiPoint,
    right: Geometry,
) -> MultiPoint | Point: ...
@overload
def difference(
    left: GeometryArray[Point],
    right: Geometry | GeometryArray[_GeometryOtherT],
) -> GeometryArray[Point]: ...
@overload
def difference(
    left: Point,
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[Point]: ...
@overload
def difference(
    left: GeometryArray[MultiPoint],
    right: Geometry | GeometryArray[_GeometryOtherT],
) -> GeometryArray[MultiPoint | Point]: ...
@overload
def difference(
    left: MultiPoint,
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[MultiPoint | Point]: ...
@overload
def difference(
    left: GeometryArray[Point | None],
    right: Geometry | GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Point | None]: ...
@overload
def difference(
    left: Point,
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Point | None]: ...
@overload
def difference(
    left: GeometryArray[MultiPoint | None],
    right: Geometry | GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[MultiPoint | Point | None]: ...
@overload
def difference(
    left: MultiPoint,
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[MultiPoint | Point | None]: ...
@overload
def difference(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def difference(
    left: Geometry,
    right: GeometryArray[_GeometryT],
) -> GeometryArray[Geometry]: ...
@overload
def difference(
    left: GeometryArray[_GeometryT],
    right: Geometry,
) -> GeometryArray[Geometry]: ...
@overload
def difference(
    left: GeometryArray[_GeometryT],
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[Geometry]: ...
@overload
def difference(
    left: Geometry,
    right: GeometryArray[_GeometryT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def difference(
    left: GeometryArray[_GeometryT | None],
    right: Geometry,
) -> GeometryArray[Geometry | None]: ...
@overload
def difference(
    left: GeometryArray[_GeometryT | None],
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def difference(
    left: Geometry | GeometryArray[Geometry | None],
    right: Geometry | GeometryArray[Geometry | None],
) -> Geometry | GeometryArray[Geometry | None]:
    """Compute the set-theoretic difference ``left - right``.

    Evaluated in the coordinate plane. In a geographic CRS, topology resolves
    longitude aliases, seam spellings, and pole longitudes in the CRS-native
    frame; seam-crossing inputs are split into chart pieces before construction.

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
        If either operand has coordinates outside its geographic CRS domain.
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.

    Examples
    --------
    >>> import gometry as gm
    >>> left, right = gm.box(0, 0, 2, 1), gm.box(1, 0, 2, 1)
    >>> gm.difference(left, right).to_wkt()
    'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    """

@overload
def symmetric_difference(
    left: Point | MultiPoint,
    right: Point | MultiPoint,
) -> Point | MultiPoint: ...
@overload
def symmetric_difference(
    left: GeometryArray[Point | MultiPoint],
    right: Point | MultiPoint | GeometryArray[Point | MultiPoint],
) -> GeometryArray[Point | MultiPoint]: ...
@overload
def symmetric_difference(
    left: Point | MultiPoint,
    right: GeometryArray[Point | MultiPoint],
) -> GeometryArray[Point | MultiPoint]: ...
@overload
def symmetric_difference(
    left: GeometryArray[Point | MultiPoint | None],
    right: Point | MultiPoint | GeometryArray[Point | MultiPoint | None],
) -> GeometryArray[Point | MultiPoint | None]: ...
@overload
def symmetric_difference(
    left: Point | MultiPoint,
    right: GeometryArray[Point | MultiPoint | None],
) -> GeometryArray[Point | MultiPoint | None]: ...
@overload
def symmetric_difference(
    left: Geometry,
    right: Geometry,
) -> Geometry: ...
@overload
def symmetric_difference(
    left: Geometry,
    right: GeometryArray[_GeometryT],
) -> GeometryArray[Geometry]: ...
@overload
def symmetric_difference(
    left: GeometryArray[_GeometryT],
    right: Geometry,
) -> GeometryArray[Geometry]: ...
@overload
def symmetric_difference(
    left: GeometryArray[_GeometryT],
    right: GeometryArray[_GeometryOtherT],
) -> GeometryArray[Geometry]: ...
@overload
def symmetric_difference(
    left: Geometry,
    right: GeometryArray[_GeometryT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def symmetric_difference(
    left: GeometryArray[_GeometryT | None],
    right: Geometry,
) -> GeometryArray[Geometry | None]: ...
@overload
def symmetric_difference(
    left: GeometryArray[_GeometryT | None],
    right: GeometryArray[_GeometryOtherT | None],
) -> GeometryArray[Geometry | None]: ...
@overload
def symmetric_difference(
    left: Geometry | GeometryArray[Geometry | None],
    right: Geometry | GeometryArray[Geometry | None],
) -> Geometry | GeometryArray[Geometry | None]:
    """Compute the set-theoretic symmetric difference of ``left`` and
    ``right``.

    Evaluated in the coordinate plane. In a geographic CRS, topology resolves
    longitude aliases, seam spellings, and pole longitudes in the CRS-native
    frame; seam-crossing inputs are split into chart pieces before construction.

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
        If either operand has coordinates outside its geographic CRS domain.
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.

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
        Finite non-negative coordinate-space gap width; 0 disables gap
        detection. Geographic CRSs use native angular units, projected CRSs
        use native linear units, and CRS-free inputs use raw coordinate units.

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
    >>> grid = gm.GeometryArray([
    ...     gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
    ...     gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
    ... ])
    >>> gm.coverage_is_valid(grid)
    True
    >>> gm.coverage_is_valid(gm.GeometryArray([
    ...     gm.Polygon([(0, 0), (1.1, 0), (1.1, 1), (0, 1)]),
    ...     gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
    ... ]))
    False
    """

@overload
def coverage_invalid_edges(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike],
    *,
    gap_width: float = 0.0,
) -> GeometryArray[LineString | MultiLineString]: ...
@overload
def coverage_invalid_edges(
    values: GeometryArray[Polygon | MultiPolygon | None] | Iterable[_ArealLike | None],
    *,
    gap_width: float = 0.0,
) -> GeometryArray[LineString | MultiLineString | None]:
    """Per-row invalid coverage boundary linework.

    Each output row merges the row's boundary segments that violate the
    coverage contract (overlaps, T-joins, slivers, and — with ``gap_width``
    — narrow gaps); rows that are clean come back as ``LINESTRING EMPTY``.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry
        One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).

    gap_width : float, default 0.0
        Finite non-negative coordinate-space gap width; 0 disables gap
        detection. Geographic CRSs use native angular units, projected CRSs
        use native linear units, and CRS-free inputs use raw coordinate units.

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

@overload
def coverage_simplify(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike],
    tolerance: float,
    *,
    method: SimplifyMethod = 'vw',
    simplify_boundary: bool = True,
) -> GeometryArray[Polygon | MultiPolygon]: ...
@overload
def coverage_simplify(
    values: GeometryArray[Polygon | MultiPolygon | None] | Iterable[_ArealLike | None],
    tolerance: float,
    *,
    method: SimplifyMethod = 'vw',
    simplify_boundary: bool = True,
) -> GeometryArray[Polygon | MultiPolygon | None]:
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
        Finite non-negative coordinate-space distance-scale tolerance:
        geographic CRSs use native angular units, projected CRSs use native
        linear units, and CRS-free inputs use raw coordinate units. Read on the
        same scale by both methods (the
        Visvalingam-Whyatt effective area is ``tolerance**2 / 2``).
    method : {'vw', 'dp'}, default 'vw'
        Importance criterion: ``'vw'`` is area-based (Visvalingam-Whyatt),
        ``'dp'`` is distance-based (Douglas-Peucker). Both run under the same
        topology guard. For coverage simplification, ``'dp'`` is a local
        importance criterion; it does not provide scalar Douglas-Peucker's
        original-span error contract.
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

@overload
def polygonize_all(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    diagnostics: Literal[False] = False,
) -> GeometryArray[Polygon]: ...
@overload
def polygonize_all(
    values: Geometry | GeometryArray[Geometry] | Iterable[_GeometryLike],
    *,
    diagnostics: Literal[True],
) -> PolygonizeResult[Never]: ...
@overload
def polygonize_all(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    diagnostics: Literal[True],
) -> PolygonizeResult[None]: ...
@overload
def polygonize_all(
    values: Geometry | GeometryArray[Geometry] | Iterable[_GeometryLike],
    *,
    diagnostics: bool,
) -> GeometryArray[Polygon] | PolygonizeResult[Never]: ...
@overload
def polygonize_all(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
    *,
    diagnostics: bool,
) -> GeometryArray[Polygon] | PolygonizeResult[None]:
    """Build polygons from pooled linework, optionally returning full diagnostics.

    All input geometries' edges are noded into one planar graph, so a ring can
    close across inputs. `diagnostics=False` returns the enclosed polygons;
    `diagnostics=True` returns those same polygons together with cut edges,
    dangles, and invalid rings grouped by source row. Missing source rows are
    `None` groups; present rows with no diagnostic linework are empty groups.
    `Geometry.polygonize()` and `GeometryArray.polygonize()` instead polygonize
    each geometry's own linework independently.

    Parameters
    ----------
    values : Geometry, GeometryArray, or iterable of Geometry-like values
        One geometry or linework values to node and polygonize together. Array
        and iterable inputs may contain ``None`` rows; they are omitted from the
        pooled linework.
    diagnostics : bool
        Return a `PolygonizeResult` containing the pooled polygons and
        source-row `Groups` for cut edges, dangles, and invalid rings. The
        diagnostic result form requires ``diagnostics=True``; the polygon-only
        form omits the option.

    Returns
    -------
    GeometryArray or PolygonizeResult
        With the default ``False``, the polygons enclosed by the pooled
        linework. With ``True``, the same polygons plus the leftover-linework
        diagnostics.

    Raises
    ------
    GeometryError
        If no adjacent binary64 assignment preserves the exact polygonized
        output, or the bounded embedding search is exhausted.

    See Also
    --------
    Geometry.polygonize : Polygonize one geometry's own linework.
    GeometryArray.polygonize : Polygonize each array row independently.
    union_all : Union of many geometries into one.
    intersection_all : Common region shared by many geometries.

    Examples
    --------
    >>> import gometry as gm
    >>> edges = [
    ...     gm.LineString([(0, 0), (1, 0)]),
    ...     gm.LineString([(1, 0), (1, 1)]),
    ...     gm.LineString([(1, 1), (0, 0)]),
    ... ]
    >>> gm.polygonize_all(edges).to_wkt()
    ['POLYGON ((0 0, 1 0, 1 1, 0 0))']
    >>> result = gm.polygonize_all(edges, diagnostics=True)
    >>> result.polygons.to_wkt()
    ['POLYGON ((0 0, 1 0, 1 1, 0 0))']
    """

def coverage_union_all(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon | None]
    | Iterable[_ArealLike | None],
) -> Polygon | MultiPolygon:
    """Union a polygonal *coverage* into one geometry by dissolving shared edges.

    A fast specialization of ``union_all`` for a **valid
    coverage** — polygons that tile a region edge-to-edge with no overlaps and
    no T-junctions (see ``coverage_is_valid``). It
    cancels the interior edges shared between adjacent polygons rather than
    general noding and classification. On overlapping or badly-noded input it
    raises before the specialized dissolve runs; use
    `union_all` there.

    In a geographic CRS, rows whose raw bounds are unsound at the periodic seam,
    a pole, or a non-canonical longitude are split into chart-safe pieces before
    dissolve. Canonical seam-free and pole-free rows retain the direct path. The
    result preserves the shared CRS and coordinate epoch.

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
        If ``values`` is empty, the rows are not a valid coverage, or coordinates
        are outside their geographic CRS domain.

    See Also
    --------
    union_all : General multi-geometry union (handles overlaps).
    coverage_is_valid : Whether the rows form a valid polygonal coverage.

    Examples
    --------
    >>> import gometry as gm
    >>> tiles = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
    >>> merged = gm.coverage_union_all(tiles)
    >>> (merged.geometry_type, round(merged.area, 6))
    ('Polygon', 2.0)
    """

@overload
def coverage_clean(
    values: Polygon
    | MultiPolygon
    | GeometryArray[Polygon | MultiPolygon]
    | Iterable[_ArealLike],
    *,
    grid_size: float = 0.0,
    gap_width: float = 0.0,
    overlap_rule: CoverageOverlapRule = 'longest_border',
) -> GeometryArray[Polygon | MultiPolygon]: ...
@overload
def coverage_clean(
    values: GeometryArray[Polygon | MultiPolygon | None] | Iterable[_ArealLike | None],
    *,
    grid_size: float = 0.0,
    gap_width: float = 0.0,
    overlap_rule: CoverageOverlapRule = 'longest_border',
) -> GeometryArray[Polygon | MultiPolygon | None]:
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
        Finite non-negative vertex snapping grid in coordinate space:
        geographic CRSs use native angular units, projected CRSs use native
        linear units, and CRS-free inputs use raw coordinate units. ``0``
        preserves input coordinates and disables snapping. Unlike
        ``snap_to_grid(grid_size=...)``, this operation accepts zero.

    gap_width : float, default 0.0
        Finite non-negative coordinate-space width of gaps to merge (0 keeps
        gaps).
    overlap_rule : str, default 'longest_border'
        Which row keeps a region covered more than once:
        ``'longest_border'``, ``'max_area'``, ``'min_area'``, or
        ``'min_index'``.
        Cleaning rebuilds faces from one exact partition. Source Z/M ordinates
        are retained for rows whose emitted nodes have one consistent source
        value; an unavailable or conflicting axis is dropped for that row.

    Returns
    -------
    GeometryArray
        One cleaned `Polygon`/`MultiPolygon` per input row.

    Raises
    ------
    GeometryTypeError
        If a row is not a `Polygon` or `MultiPolygon`.
    GeometryError
        If ``grid_size`` or ``gap_width`` is negative or non-finite, or no
        adjacent binary64 assignment preserves the exact cleaned coverage or
        the bounded embedding search is exhausted.
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
    >>> gm.coverage_is_valid(cleaned)
    True
    """

@overload
def union_all(
    values: Point
    | MultiPoint
    | GeometryArray[Point | MultiPoint | None]
    | Iterable[Point | MultiPoint | None],
) -> Point | MultiPoint: ...
@overload
def union_all(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
) -> Geometry:
    """Union of many geometries into one.

    Strictly disjoint inputs are assembled independently; inputs that can meet
    use one joint arrangement. In a geographic CRS, topology resolves longitude
    aliases and seam and pole equivalence in the native frame.

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
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.
    InvalidGeometryError
        If ``values`` is empty or contains coordinates outside its geographic CRS
        domain.

    See Also
    --------
    coverage_union_all : Faster dissolve for a valid polygonal coverage.
    intersection_all : Common region shared by many geometries.
    polygonize_all : Build polygons from pooled linework.

    Examples
    --------
    >>> import gometry as gm
    >>> panes = [gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]
    >>> gm.union_all(panes).to_wkt()
    'POLYGON ((0 0, 2 0, 2 1, 3 1, 3 3, 1 3, 1 2, 0 2, 0 0))'
    """

@overload
def intersection_all(
    values: Point
    | MultiPoint
    | GeometryArray[Point | MultiPoint | None]
    | Iterable[Point | MultiPoint | None],
) -> Point | MultiPoint: ...
@overload
def intersection_all(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
) -> Geometry:
    """Common intersection of many geometries.

    The region inside every input. When sound coordinate bounds prove operands
    disjoint, returns the typed empty directly; in a geographic CRS this requires
    every operand to be seam-free, pole-free, and canonically spelled. Otherwise,
    the result is assembled from one joint arrangement. Mixed dimensions narrow
    to the lowest shared dimension and an empty result keeps that dimension's
    typed empty.

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
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.
    InvalidGeometryError
        If ``values`` is empty or contains coordinates outside its geographic CRS
        domain.

    See Also
    --------
    union_all : Union of many geometries into one.
    polygonize_all : Build polygons from pooled linework.

    Examples
    --------
    >>> import gometry as gm
    >>> panes = [gm.box(0, 0, 3, 3), gm.box(1, 1, 4, 4), gm.box(2, 2, 5, 5)]
    >>> gm.intersection_all(panes).to_wkt()
    'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
    """

@overload
def symmetric_difference_all(
    values: Point
    | MultiPoint
    | GeometryArray[Point | MultiPoint | None]
    | Iterable[Point | MultiPoint | None],
) -> Point | MultiPoint: ...
@overload
def symmetric_difference_all(
    values: Geometry | GeometryArray[Geometry | None] | Iterable[_GeometryLike | None],
) -> Geometry:
    """Symmetric difference of many geometries.

    The region covered by an odd number of inputs. Strictly disjoint inputs are
    assembled independently; inputs that can meet use one joint arrangement. In
    a geographic CRS, topology resolves longitude aliases and seam and pole
    equivalence in the native frame. The result is order-independent (`▵` is
    associative and commutative).

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
    GeometryError
        If no adjacent binary64 assignment preserves the exact result topology,
        or the bounded embedding search is exhausted.
    InvalidGeometryError
        If ``values`` is empty or contains coordinates outside its geographic CRS
        domain.

    Examples
    --------
    >>> import gometry as gm
    >>> panes = [gm.box(0, 0, 2, 2), gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]
    >>> gm.symmetric_difference_all(panes).to_wkt()  # the duplicate cancels
    'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'
    """

@overload
def from_wkt(
    data: str, *, crs: CrsInput | None = None, epoch: float | None = None
) -> Geometry: ...
@overload
def from_wkt(
    data: PandasSeries | PolarsSeries | PyArrowArray | PyArrowChunkedArray,
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]: ...
@overload
def from_wkt(
    data: Iterable[str],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry]: ...
@overload
def from_wkt(
    data: Iterable[str | None],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]:
    """Parse a geometry (or array) from Well-Known Text.

    Also accepts EWKT; an embedded ``SRID=...;`` prefix becomes the
    geometry's CRS. ``SRID=0`` is PostGIS unknown/unspecified and yields a
    CRS-free geometry; nonzero codes resolve through the canonical PROJ CRS
    parser (invalid codes raise ``CRSError``).

    Parameters
    ----------
    data : str, iterable of str, or dataframe/Arrow column
        A single WKT/EWKT string, an iterable of rows, or a pandas/Polars
        Series or PyArrow Array/ChunkedArray. Missing column values become
        missing rows.

    crs : CRS-like, optional
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
    data: PandasSeries | PolarsSeries | PyArrowArray | PyArrowChunkedArray,
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]: ...
@overload
def from_wkb(
    data: npt.NDArray[np.uint8] | npt.NDArray[np.int8],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> Geometry: ...
@overload
def from_wkb(
    data: npt.NDArray[np.object_],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]: ...
@overload
def from_wkb(
    data: npt.NDArray[np.bytes_],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry]: ...
@overload
def from_wkb(
    data: Buffer, *, crs: CrsInput | None = None, epoch: float | None = None
) -> Geometry: ...
@overload
def from_wkb(
    data: Iterable[Buffer],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry]: ...
@overload
def from_wkb(
    data: Iterable[Buffer | None],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]: ...
@overload
def from_wkb(
    data: Buffer | Iterable[Buffer | None],
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> Geometry | GeometryArray[Geometry | None]:
    """Parse a geometry (or array) from Well-Known Binary.

    Also accepts EWKB; an embedded SRID becomes the geometry's CRS.
    ``SRID=0`` / EWKB SRID 0 is PostGIS unknown → CRS-free; nonzero codes
    resolve through the canonical PROJ CRS parser (invalid codes raise).

    Parameters
    ----------
    data : bytes, byte buffer, iterable of buffers, or dataframe/Arrow column
        A WKB/EWKB byte buffer, an iterable of byte buffers, or a pandas/Polars
        Series or PyArrow Array/ChunkedArray of WKB values. A one-byte integer
        ndarray is one scalar byte buffer; other ndarrays are row columns.
        Missing column values become missing rows.

    crs : CRS-like, optional
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

@final
class _GeoParquetColumnFrame:
    """GeoParquet column-level CRS/epoch/edges boundary (frame only — encoding,
    geometry_types, orientation, and bbox stay in the Python reader).
    """

    @property
    def crs(self) -> str | None:
        """Normalized CRS declared by GeoParquet column metadata.

        Returns
        -------
        str or None
            ``'OGC:CRS84'`` when the ``crs`` key is absent, ``None`` for an
            explicit JSON null, or the normalized CRS from a PROJJSON object.
        """

    @property
    def epoch(self) -> float | None:
        """Coordinate epoch declared by GeoParquet column metadata.

        Returns
        -------
        float or None
            A finite decimal year when ``epoch`` is numeric; ``None`` when the
            key is absent or explicitly null. A non-null epoch requires a CRS.
        """

    @property
    def crs_present(self) -> bool:
        """Whether GeoParquet column metadata contained a ``crs`` key.

        Returns
        -------
        bool
            ``False`` means the key was absent and the CRS was defaulted to
            ``'OGC:CRS84'``; ``True`` includes an explicit JSON null.
        """

    @property
    def epoch_present(self) -> bool:
        """Whether GeoParquet column metadata contained an ``epoch`` key.

        Returns
        -------
        bool
            ``False`` means the epoch was omitted; ``True`` includes an
            explicit JSON null, which is distinct from omission.
        """

    def __iter__(self) -> Iterator[str | float | None]: ...

@final
class _GeoParquetPhysicalLayout:
    """One native admission result for a GeoParquet geometry column.

    The record is produced from the file schema before any materialized column
    is decoded.  Its encoding, physical nesting, ordinate fields, and embedded
    frame are immutable facts carried through the Python adapter; decoding must
    not infer them again from a post-read PyArrow value.
    """

    @property
    def encoding(self) -> str:
        """Canonical GeoParquet encoding label admitted for the field.

        Returns
        -------
        str
            ``'WKB'`` for binary storage, or the lower-case native kind label:
            ``'point'``, ``'multipoint'``, ``'linestring'``,
            ``'multilinestring'``, ``'polygon'``, or ``'multipolygon'``.
        """

    @property
    def has_extension(self) -> bool:
        """Whether the field carried reconciled GeoArrow extension metadata.

        Returns
        -------
        bool
            ``True`` when extension metadata was found on the Arrow type or
            field and reconciled; ``False`` for plain or untyped storage.
        """

    @property
    def embedded_crs(self) -> str | None:
        """Normalized CRS from the field's own GeoArrow extension metadata.

        Returns
        -------
        str or None
            The embedded CRS label when the extension declares one; ``None``
            when the extension metadata omits a CRS. This is not the caller's
            GeoParquet column declaration.
        """

    @property
    def embedded_epoch(self) -> float | None:
        """Coordinate epoch from the field's own GeoArrow extension metadata.

        Returns
        -------
        float or None
            A finite decimal year when embedded metadata declares an epoch;
            ``None`` when it omits one. This is not the caller's declaration.
        """

    @property
    def list_depth(self) -> int | None:
        """Native GeoArrow list levels above coordinate storage.

        Returns
        -------
        int or None
            The number of nested list levels above the coordinate struct or
            interleaved coordinate list; ``None`` for WKB storage.
        """

    @property
    def has_z(self) -> bool:
        """Whether admitted native coordinate storage contains a Z ordinate field.

        Returns
        -------
        bool
            ``True`` when the terminal native coordinate storage has a Z field;
            ``False`` for XY storage and WKB, whose dimensions live in payloads.
        """

    @property
    def has_m(self) -> bool:
        """Whether admitted native coordinate storage contains an M ordinate field.

        Returns
        -------
        bool
            ``True`` when the terminal native coordinate storage has an M field;
            ``False`` for XY/Z storage and WKB, whose dimensions live in payloads.
        """

    @property
    def wkb_offset_width(self) -> str:
        """WKB binary offset representation selected for admitted storage.

        Returns
        -------
        str
            ``'int32'`` for ``Binary``, ``'int64'`` for ``LargeBinary``, or
            ``'view'`` for ``BinaryView``. Native encodings report ``'int32'``.
        """

def _parse_geoparquet_column_frame(
    metadata: Mapping[str, Any],
    column_name: str,
) -> _GeoParquetColumnFrame:
    """Private Python entry: parse one already-decoded GeoParquet column mapping for
    the shared CRS/epoch/edges frame (defaults missing CRS to OGC:CRS84; CRS must
    be a PROJJSON object or null when present).
    """

def _admit_geoparquet_geometry_storage(
    arrow_type: Any,
    encoding: str,
    column_name: str,
    field: Any | None = None,
) -> _GeoParquetPhysicalLayout:
    """Admit GeoParquet geometry storage against a declared encoding.

    Dictionary-wrapped WKB is accepted. ExtensionType and Field metadata are
    reconciled together (name + frame) so raw-field frame metadata is never
    discarded. Returns one immutable ``_GeoParquetPhysicalLayout`` record whose
    fields are consumed by decoding without re-inspecting the materialized
    value.
    """

def _from_geoparquet_arrow(
    data: Any,
    layout: _GeoParquetPhysicalLayout,
    *,
    crs: str | None = None,
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]:
    """Decode a PyArrow column with one already-admitted physical layout."""

def from_arrow(
    data: _ArrowInput, *, crs: CrsInput | None = None, epoch: float | None = None
) -> GeometryArray[Geometry | None]:
    """Build geometries from an Arrow array, table, record batch, reader, or
    Arrow-C provider. Protocol callbacks and PyArrow object inspection remain
    attached; selected buffers are owned before the decoder is detached.
    Arrow null slots become missing rows; typed empty geometries remain typed
    empty values. Empty arrays and streams return an empty array while retaining
    compatible CRS and epoch metadata when the input provides it.

    Parameters
    ----------
    data : object
        A foreign PyArrow ``Array``, ``ChunkedArray``, ``Table``,
        ``RecordBatch``, or ``RecordBatchReader``, or an object implementing
        ``__arrow_c_array__`` or ``__arrow_c_stream__``. The input must use a
        supported GeoArrow encoding, including GeoArrow WKB.

    crs : str, int, or CRS-like object, optional
        CRS to attach when the input has no embedded CRS. If embedded metadata
        is present, it must be operationally compatible with this value and the
        explicit value is retained as the result's CRS. Passing ``None`` does
        not strip compatible embedded metadata.

    epoch : float, optional
        Coordinate epoch (decimal year) to attach as frame metadata. A supplied
        epoch must be finite and have a compatible embedded or explicit CRS; an
        embedded epoch is preserved when this argument is omitted.

    Returns
    -------
    GeometryArray[Geometry | None]
        Decoded geometry rows. Arrow null slots become missing ``None`` rows;
        typed empty geometries remain typed empty values. Empty input produces
        an empty array, retaining compatible CRS and epoch metadata when
        available.

    Raises
    ------
    CRSError
        If ``crs`` or an embedded EWKB SRID cannot be resolved, or ``epoch`` is
        supplied without a CRS or with a static CRS.
    CRSMismatchError
        If explicit CRS or epoch metadata conflicts with embedded metadata, or
        EWKB row SRIDs disagree.
    GeometryError
        If ``epoch`` is not finite or admitted geometry data violates a geometry
        invariant.
    ParseError
        If GeoArrow metadata, offsets, validity buffers, coordinate buffers, or
        WKB data are malformed, or a selected table geometry child is absent
        or out of range.
    TypeError
        If ``data`` is not a supported Arrow object or provider, its schema or
        encoding cannot be classified, it has no geometry column, a C array
        length or offset is negative, its ``null_count`` is below -1 (``-1``
        means unknown) or a known count exceeds the logical length, or its
        format-specific buffer/child layout conflicts with the schema.
    OSError
        If an Arrow C stream schema or batch callback returns a nonzero status
        other than ``ENOMEM``; the callback status is exposed as ``errno``.
    MemoryError
        If an Arrow C stream callback returns ``ENOMEM``. Native Rust allocator
        exhaustion is process-fatal and is not translated into a Python error.
    OverflowError
        If a nonnegative Arrow buffer byte span cannot fit ``usize`` or the
        maximum size of a Rust slice.

    See Also
    --------
    GeometryArray.to_arrow : Export an array as a GeoArrow array.

    Examples
    --------
    >>> import gometry as gm
    >>> source = gm.points([1.0], [2.0])
    >>> restored = gm.from_arrow(source.to_arrow())
    >>> restored.to_wkt()
    ['POINT (1 2)']
    """

def to_feature(
    geom: Geometry | None,
    *,
    properties: Mapping[str, Any] | None = None,
    id: FeatureId = None,
    drop_epoch: bool = False,
) -> GeoJsonFeature:
    """Build a GeoJSON Feature mapping from a geometry and side data.
    Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

    Parameters
    ----------
    geom : Geometry, optional
        Geometry to encode. ``None`` emits a null GeoJSON geometry. A geometry
        with a CRS must use WGS84 longitude/latitude coordinates. A CRS-free
        geometry is explicitly assumed to already use those coordinates too.
    properties : Mapping[str, Any], optional
        Feature properties. The mapping is copied and all keys must be strings.
    id : str or finite number, optional
        Feature identifier.
    drop_epoch : bool, default False
        GeoJSON Feature mappings cannot encode coordinate epoch metadata. If
        ``False``, an epoch-bearing geometry raises ``GeometryError``; pass
        ``True`` to acknowledge and explicitly lose the epoch metadata.

    Returns
    -------
    GeoJsonFeature
        A new ``{"type": "Feature", ...}`` mapping.

    Raises
    ------
    TypeError
        If ``geom`` is not a Geometry or ``None``.
    GeometryError
        If properties are not a string-keyed mapping, the id is invalid, or
        coordinate epoch metadata is present without ``drop_epoch=True``.
    InvalidGeometryError
        If the geometry has an M ordinate or coordinates outside the WGS84
        longitude/latitude domain. GeoJSON Feature mappings have no
        representation for M; drop it with ``with_coordinates(m=None)`` (or ``force_2d()``).
    CRSError
        If a CRS-tagged geometry is not in the WGS84 lon/lat family.

    Notes
    -----
    Coordinate epoch metadata is not representable in GeoJSON Feature mappings.
    It is rejected unless ``drop_epoch=True``; that option explicitly
    acknowledges and loses the epoch metadata.

    Examples
    --------
    >>> import gometry as gm
    >>> feature = gm.to_feature(gm.Point(1, 2), properties={"name": "A"})
    >>> feature.get("properties")
    {'name': 'A'}
    """

@overload
def to_feature_collection(
    values: Features,
    *,
    properties: None = None,
    ids: None = None,
    drop_epoch: bool = False,
) -> GeoJsonFeatureCollection: ...
@overload
def to_feature_collection(
    values: Geometry
    | GeometryArray[Geometry | None]
    | Iterable[Geometry | None]
    | None,
    *,
    properties: Mapping[str, Any] | Iterable[Mapping[str, Any] | None] | None = None,
    ids: Iterable[FeatureId] | None = None,
    drop_epoch: bool = False,
) -> GeoJsonFeatureCollection:
    """Build a GeoJSON FeatureCollection from geometries and aligned side data.
    Each writer preserves all ordinates the format supports and rejects unsupported axes; remove geometry axes with `force_2d()`, `with_coordinates(z=None)`, or `with_coordinates(m=None)` before writing, or use `coords.select(...)` to choose a coordinate-view export layout.

    Parameters
    ----------
    values : Features, Geometry, None, GeometryArray, or iterable of Geometry or None
        A `Features` record reuses its aligned geometries, properties, and ids.
        Features already owns aligned properties and ids, so properties and ids
        must be omitted; supplying them raises ``TypeError``.
        Otherwise, one geometry or geometry rows to encode. CRS-tagged rows must
        use WGS84 longitude/latitude coordinates; CRS-free rows are explicitly
        assumed to already use those coordinates.
    properties : Mapping or iterable of Mapping or None, optional
        One mapping broadcasts to every geometry. An iterable supplies one
        mapping or explicit ``None`` per row. Omit for independent empty mappings.
    ids : iterable of str, finite number, or None, optional
        One optional feature identifier per geometry.
    drop_epoch : bool, default False
        GeoJSON Feature mappings cannot encode coordinate epoch metadata. If
        ``False``, an epoch-bearing geometry raises ``GeometryError``; pass
        ``True`` to acknowledge and explicitly lose the epoch metadata.

    Returns
    -------
    GeoJsonFeatureCollection
        A new ``{"type": "FeatureCollection", "features": [...]}`` mapping.

    Raises
    ------
    TypeError
        If a geometry row is not a Geometry or ``None``, or if properties or ids
        are supplied when values is a Features record.
    GeometryError
        If properties or ids are invalid or are not aligned with geometries, or
        coordinate epoch metadata is present without ``drop_epoch=True``.
    InvalidGeometryError
        If a geometry has an M ordinate or coordinates outside the WGS84
        longitude/latitude domain. GeoJSON Feature mappings cannot represent M;
        drop it with ``with_coordinates(m=None)`` (or ``force_2d()``).
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
    features: Literal[False] = False,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]: ...
@overload
def from_geojson(
    data: GeoJsonGeometry | GeoJsonFeatureNonNull,
    *,
    features: Literal[False] = False,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry: ...
@overload
def from_geojson(
    data: _GeoJsonScalar,
    *,
    features: Literal[False] = False,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry | GeometryArray[Geometry | None]: ...
@overload
def from_geojson(
    data: PandasSeries
    | PolarsSeries
    | PyArrowArray
    | PyArrowChunkedArray
    | Iterable[_GeoJsonScalar | None],
    *,
    features: Literal[False] = False,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> GeometryArray[Geometry | None]: ...
@overload
def from_geojson(
    data: _GeoJsonScalar
    | PandasSeries
    | PolarsSeries
    | PyArrowArray
    | PyArrowChunkedArray
    | Iterable[_GeoJsonScalar | None],
    *,
    features: Literal[False] = False,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry | GeometryArray[Geometry | None]: ...
@overload
def from_geojson(
    data: _GeoJsonScalar
    | PandasSeries
    | PolarsSeries
    | PyArrowArray
    | PyArrowChunkedArray
    | Iterable[_GeoJsonScalar | None],
    *,
    features: Literal[True],
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Features: ...
@overload
def from_geojson(
    data: _GeoJsonScalar
    | PandasSeries
    | PolarsSeries
    | PyArrowArray
    | PyArrowChunkedArray
    | Iterable[_GeoJsonScalar | None],
    *,
    features: bool,
    crs: CrsInput | None = 'OGC:CRS84',
    epoch: float | None = None,
) -> Geometry | GeometryArray[Geometry | None] | Features:
    """Parse GeoJSON geometry, rows, or feature metadata.

    A geometry or ``Feature`` decodes to a ``Geometry`` by default; a
    ``FeatureCollection`` decodes to a ``GeometryArray`` with one geometry per
    feature. Set ``features=True`` to retain feature properties and IDs in a
    ``Features`` result.

    Coordinate sequences are **axis-uniform**: every position in one sequence
    (LineString, MultiPoint member list, ring, …) must share the same axes
    (all XY or all XYZ). RFC 7946 makes the third ordinate optional per
    position, but gometry's coordinate model requires finite values on every
    active ordinate and rejects non-finite coordinates, so mixed XY/XYZ within
    one sequence is a ``ParseError`` rather than a silent 0-elevation fill.
    Distinct members of a ``GeometryCollection`` may still differ in axes.

    Parameters
    ----------
    data : str, mapping, row iterable, or dataframe/Arrow column
        A GeoJSON string, mapping, row iterable, or pandas/Polars Series or
        PyArrow Array/ChunkedArray. Feature and FeatureCollection inputs are
        accepted. Missing column values become missing rows.

    features : bool, default varies by call form
        Preserve Feature properties and IDs in a ``Features`` result. Geometry-
        only inputs are not valid when this is ``True``.

    crs : CRS-like, default 'OGC:CRS84'
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
    Features
        When ``features=True``, one geometry and its aligned properties and ID
        per input feature.

    Raises
    ------
    ParseError
        If the `geojson` is malformed or an unsupported type, a coordinate
        sequence mixes axes (XY with XYZ), a coordinate integer is not exactly
        representable as binary64, a ``bbox``, Feature ``properties`` or ``id``
        is malformed, a Python mapping contains non-JSON data, or a legacy
        ``crs`` member is unsupported or conflicts with ``crs``, a position
        is outside the WGS84 domain, or a ring fails structural admission
        (``format`` is ``"geojson"``).
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
    Dataframe and Arrow columns are converted to their native Python row values
    before the same decoder runs.

    See Also
    --------
    Geometry.to_geojson : Serialize a geometry to GeoJSON.
    Features : The aligned feature properties and IDs returned by
        ``features=True``.

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
    value: GeometryArray[_GeometryRowT],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[_GeometryRowT]: ...
@overload
def require(
    value: Groups[GeometryArray[_GeometryRowT]],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[_GeometryRowT]: ...
@overload
def require(
    value: str | Buffer | Mapping[str, Any] | SupportsGeoInterface,
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> Geometry: ...
@overload
def require(
    value: Iterable[_GeometryRowT],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[_GeometryRowT]: ...
@overload
def require(
    value: Iterable[
        Geometry | str | Buffer | Mapping[str, Any] | SupportsGeoInterface | None
    ],
    *,
    crs: CrsInput | None = None,
    axes: CoordinateAxes | None = None,
) -> GeometryArray[Geometry | None]:
    """Parse and require a geometry contract at an input boundary.

    Parameters
    ----------
    value : geometry-like or iterable of geometry-like
        One geometry, a `GeometryArray`, a geometry-valued `Groups`, or an
        iterable. Foreign scalar inputs may be WKT, WKB, GeoJSON mappings/text,
        or ``__geo_interface__`` objects. Geometry-valued groups are flattened
        in group order and return a `GeometryArray`.
    crs : CrsInput, optional
        Expected CRS for the result. CRS-free coordinates are stamped with
        this CRS; coordinates are never transformed. An existing CRS that is
        operationally equivalent is relabelled to this requested spelling,
        while a conflicting CRS raises ``CRSMismatchError``. When omitted or
        ``None``, no target is required and an existing CRS is preserved.
        Accepted forms include authority/WKT/PROJ strings, non-boolean
        integer-like EPSG codes, ``(authority, code)`` pairs, PROJJSON/CF
        mappings, objects exposing ``to_authority()``, ``to_epsg()``,
        ``to_wkt()``, or ``to_json()``, and objects whose ``crs`` or ``srs``
        property contains another accepted form. Coordinate epochs are
        preserved and revalidated against the resulting CRS.
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
        unsupported or conflicts with ``crs`` (``format`` is ``"geojson"``).
    CRSMismatchError
        If an already-decoded / native / non-GeoJSON geometry's CRS conflicts
        with ``crs``. EWKT/EWKB embedded SRID conflicts use this error too.
    InvalidGeometryError
        If the geometry is invalid, or its axes differ from ``axes``.
        Geographic validity uses exact quotient topology, so longitude aliases
        and seam cuts do not depend on stored spelling. Out-of-domain latitude
        is invalid. For quotient-topology issues, the location is a canonical
        coordinate of the issue's contact set, chosen deterministically by
        minimum latitude then longitude.
        Projected and CRS-free geometry remains planar.

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
    '9.6.2'
    """

def crs_configure(
    *,
    search_paths: str | PathLike[str] | Iterable[str | PathLike[str]] | None = ...,
    user_writable_directory: str | PathLike[str] | None = ...,
) -> CrsRuntimeConfig:
    """Configure local CRS engine paths.

    Omitted arguments keep their current value, while explicit ``None`` restores
    that field's default. A call without arguments returns the current
    configuration unchanged; use `crs_reset` to restore both defaults.

    Parameters
    ----------
    search_paths : str, path, or sequence of these, optional
        Directories PROJ searches for its database and grids. ``None`` restores
        the default search paths. When a named grid is absent from these
        directories, PROJ may also try the bare grid filename in the current
        working directory.
    user_writable_directory : str or path, optional
        Directory PROJ includes in local grid lookup. ``None`` restores the
        default writable directory.

    Notes
    -----
    The configured directories are not an exclusive grid-file whitelist. A
    grid in the current working directory can therefore change grid
    availability, the selected transformation, and whether ``AccuracyWarning``
    is emitted after the configured directories miss.

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

def crs_catalog(
    name: str | None = None,
    *,
    authority: str | None = None,
    kind: CrsCatalogKind | None = None,
    area_of_interest: CrsAreaInput | None = None,
    contains_area_of_interest: bool = False,
    allow_deprecated: bool = False,
    celestial_body: str | None = None,
    approximate: bool = False,
    limit: int | None = None,
) -> list[CrsCatalogInfo]:
    """Catalog of CRS in the database matching the given filters.

    Parameters
    ----------
    name : str, optional
        Match CRS names using PROJ's database name matcher.
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
    approximate : bool, default False
        Allow approximate matching when ``name`` is supplied.
    limit : int, optional
        Maximum number of returned records; omitted means no result limit.

    Returns
    -------
    list of CrsCatalogInfo

    Examples
    --------
    >>> import gometry as gm
    >>> any(item['crs'] == 'EPSG:4326' for item in gm.crs_catalog(name='WGS 84'))
    True
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
        Restrict to projected CRSs whose displayed name starts with this datum
        prefix followed by ``" / UTM zone "``. Matching is ASCII
        case-insensitive, but spaces and punctuation are significant. For
        example, use ``"WGS 84"``, ``"NAD83(CSRS)"``, or
        ``"ETRS89/DREF91/2016"``. Only CRSs named ``"UTM zone"`` are
        included; arbitrary substrings and long datum descriptions do not
        match.
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
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: _FloatScalarInput | None = None,
    *,
    t: _FloatScalarInput | None = None,
    iterations: _IntScalar = 1,
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
    x: _FloatColumn1D,
    y: _FloatColumn1D,
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
    x: _FloatColumn1D,
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
    y: _FloatColumn1D,
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
    z: _FloatColumn1D,
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
    t: _FloatColumn1D,
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
        Source and destination CRS-like inputs.
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
        Euclidean residual of the raw input columns in their original units
        after the out-and-back legs. When Z or T is present the hypot mixes
        those column units with X and Y. There is no geodesic distance and no
        circular-longitude normalization. Scalar in, scalar out, and lane
        inputs return a read-only ``float64`` ``numpy.ndarray``.

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
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: None = None,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: _FloatScalarInput,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[float, float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: None = None,
    *,
    t: _FloatScalarInput,
    direction: TransformDirection = 'forward',
) -> tuple[float, float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: _FloatScalarInput,
    *,
    t: _FloatScalarInput,
    direction: TransformDirection = 'forward',
) -> tuple[float, float, float, float]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatColumn1D,
    y: FloatInput,
    z: None = None,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: _FloatColumn1D,
    z: None = None,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatColumn1D,
    y: FloatInput,
    z: FloatInput,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: _FloatColumn1D,
    z: FloatInput,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatInput,
    z: _FloatColumn1D,
    *,
    t: None = None,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatColumn1D,
    y: FloatInput,
    z: None = None,
    *,
    t: FloatInput,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: _FloatColumn1D,
    z: None = None,
    *,
    t: FloatInput,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: FloatInput,
    y: FloatInput,
    z: None = None,
    *,
    t: _FloatColumn1D,
    direction: TransformDirection = 'forward',
) -> tuple[
    npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]
]: ...
@overload
def crs_apply(
    operation: str,
    x: _FloatColumn1D,
    y: FloatInput,
    z: FloatInput,
    *,
    t: FloatInput,
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
    x: FloatInput,
    y: _FloatColumn1D,
    z: FloatInput,
    *,
    t: FloatInput,
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
    x: FloatInput,
    y: FloatInput,
    z: _FloatColumn1D,
    *,
    t: FloatInput,
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
    x: FloatInput,
    y: FloatInput,
    z: FloatInput,
    *,
    t: _FloatColumn1D,
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
        The transformed columns — ``(x, y)``, ``(x, y, z)``, ``(x, y, t)``
        when T is supplied without Z, or ``(x, y, z, t)``. Scalars in, scalars
        out; lane inputs return
        read-only ``float64`` ``numpy.ndarray`` columns (``np.asarray(column)``
        reads the values directly, ``list(column)`` materializes them).

    Raises
    ------
    TransformError
        If no transform exists between the frames or it fails to apply.
    CRSError
        If the operation definition is invalid.
    InvalidGeometryError
        If coordinate columns are non-finite or differ in length.
    GeometryError
        If ``direction`` is invalid.

    Notes
    -----
    Supply a literal operation definition. A definition returned by
    ``CRS.operation`` may be ``None`` when selected angular semantics cannot be
    represented faithfully as an executable PROJ string.

    Examples
    --------
    >>> import gometry as gm
    >>> op = '+proj=affine +xoff=1 +yoff=2'
    >>> gm.crs_apply(op, -122.4, 37.8)
    (-121.4, 39.8)
    """

@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: None = None,
    *,
    t: _FloatScalarInput | None = None,
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
    x: _FloatScalarInput,
    y: _FloatScalarInput,
    z: _FloatScalarInput,
    *,
    t: _FloatScalarInput | None = None,
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
    x: _FloatColumn1D,
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
) -> npt.NDArray[np.float64]: ...
@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: FloatInput,
    y: _FloatColumn1D,
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
) -> npt.NDArray[np.float64]: ...
@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: FloatInput,
    y: FloatInput,
    z: _FloatColumn1D,
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
) -> npt.NDArray[np.float64]: ...
@overload
def crs_transform(
    source: CrsInput,
    target: CrsInput,
    x: FloatInput,
    y: FloatInput,
    z: FloatInput | None = None,
    *,
    t: _FloatColumn1D,
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
        Source and target CRS-like inputs.
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
        same shape as a NumPy matrix exported from ``geometry.coords``), so
        ``result[:, 0]`` / ``result[:, 1]``
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
    bounds: npt.NDArray[np.floating[Any]],
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
    tuple[float, float, float, float]
    | tuple[float, float, float, float, float, float]
    | npt.NDArray[np.float64]
): ...
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
        Source and destination CRS-like inputs.
    bounds : sequence of float or iterable of sequences of float
        A scalar box is a 4-value ``(minx, miny, maxx, maxy)`` sequence or a
        6-value ``(minx, miny, minz, maxx, maxy, maxz)`` sequence in the source
        CRS. A batch is a nonempty iterable of 4- or 6-value rows; every row
        must have the same dimensionality. One-dimensional NumPy arrays supply
        scalar bounds and two-dimensional NumPy arrays supply batch rows.
        Batch row order is preserved.
    densify : int, default 21
        Points added per edge before transforming, to track curved edges.
        Must be a non-boolean, nonnegative integer that fits in ``u32``. For a
        non-identity transform it must be at most 10,000; if the target CRS is
        geographic, it must also be at least 2. Identity transforms do not
        sample bounds and skip these transform-specific limits.
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
        Bounds use literal longitude values in this mode: geographic source
        bounds sample the closed numeric segment between west and east, and
        geographic target bounds return numeric longitude extrema rather than
        a periodic cover. Literal identity bounds retain out-of-period values;
        descending source longitudes use the segment between their sorted
        endpoints.

    Returns
    -------
    tuple or numpy.ndarray
        The reprojected box in the target CRS. Scalar input returns a 4- or
        6-value tuple. Batch input returns a ``float64`` array of shape
        ``(n, 4)`` or ``(n, 6)`` in input row order.

    Raises
    ------
    TransformError
        If no transform exists between the frames or it fails to apply.
    CRSError
        If ``source``/``target`` are unrecognized, or a non-identity transform
        has ``densify`` above 10,000 or below 2 for a geographic target.
    GeometryError
        If a bounds box does not contain four or six finite values, a batch is
        empty, batch rows have inconsistent dimensions, or ``densify`` is
        outside the nonnegative ``u32`` range.
    TypeError
        If ``bounds`` is not iterable or ``densify`` is not an integer index
        value (``bool`` is not accepted).
    OverflowError
        If ``densify`` is outside the signed 64-bit input range.

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
    geom: GeometryArray[Geometry] | Iterable[Geometry],
    precision: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[GeohashCell]]: ...
@overload
def geohash_cover(
    geom: GeometryArray[Geometry | None] | Iterable[Geometry | None],
    precision: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[GeohashCell] | None]: ...
@overload
def geohash_cover(
    geom: Geometry,
    precision: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> CellArray[GeohashCell]:
    """Cover a geometry with geohash cells at a fixed precision.

    The result is a ``CellArray`` for scalar input or ``Groups[CellArray]`` for
    array input, with geohash cells selected by ``cell_rule``.
    Keep the source geometry separately and use the free ``gm.*`` predicates
    for exact geometry questions.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of Geometry or None
        Geometry to cover (WGS84 lon/lat or projected). A scalar returns a
        flat cell array; an array or iterable returns one group per input row.
        ``None`` produces a missing group; a present empty geometry produces
        an empty group.

    precision : int
        Geohash precision (``1``-``12``; finer at higher values).

    cell_rule : {'within', 'center', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize. ``'within'`` selects cells entirely inside;
        ``'center'`` selects cells whose center is inside; ``'overlap'``
        selects every cell touching the geometry; and ``'bbox'`` selects every
        cell meeting the geometry's envelope (one window per part, split at the
        antimeridian). The rule never affects exact predicates.

    max_cells : int or None, default 1000000
        Hard cap on returned cells and retained result allocation. Discarded
        candidates do not consume it; array calls apply it cumulatively across
        groups. Pass ``None`` for unlimited output (bounded only by memory).

    Returns
    -------
    CellArray or Groups[CellArray]
        A scalar returns a ``CellArray``; an array returns one ``CellArray``
        group per input geometry.

    Raises
    ------
    GeometryError
        If the geometry, precision, ``cell_rule``, or ``max_cells`` is invalid,
        or if the returned coverage would exceed ``max_cells``.
    InvalidGeometryError
        If the geometry is empty.

    Examples
    --------
    >>> import gometry as gm
    >>> point = gm.Point(1, 1)
    >>> cells = gm.geohash_cover(point, precision=1)
    >>> len(cells)
    1
    """

@overload
def geohash_cells(
    values: GeometryArray[Point] | Iterable[Point] | _CoordinateMatrix | FloatInput,
    lat: FloatInput | None = None,
    *,
    precision: _IntInput,
) -> CellArray[GeohashCell]: ...
@overload
def geohash_cells(
    values: GeometryArray[Point | None] | Iterable[Point | None],
    lat: None = None,
    *,
    precision: _IntInput,
) -> CellArray[GeohashCell | None]:
    """Build geohash cells from points, coordinate rows, or parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, iterable of Point or None, coordinate matrix, or float
        Point geometries (``None`` makes a missing cell), a coordinate matrix
        with at least two columns, or WGS84 longitudes. Iterable points must
        share one CRS and epoch; projected point inputs are reprojected to
        WGS84. Coordinate matrices use only X and Y, ignoring extra ordinates.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Coordinate
        columns broadcast numpy-style; at least one must be a sequence.

    precision : int or sequence of int
        Geohash precision (1-12 characters; finer at higher values). A scalar
        broadcasts to every row; an array supplies one precision per row.

    Returns
    -------
    CellArray of GeohashCell
        One cell per input coordinate.

    Raises
    ------
    GeometryError
        If the input is scalar, precision is invalid, or coordinate columns
        have different lengths.
    InvalidGeometryError
        If coordinates are non-finite or leave the lon/lat domain.

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
    value: Geometry | GeometryArray[Geometry | None] | Iterable[float],
) -> GeohashCell:
    """Return the deepest single geohash cell containing a geometry or lon/lat bounds.

    Descends the cell containing the lower corner and accepts it only after its
    complete rectangle contains both axes of the query. There is no global
    wrapped geohash cell, so an antimeridian-spanning region has no single-cell
    result and raises.

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
    left: _GeohashSetInput,
    right: _GeohashSetInput,
) -> CellArray[GeohashCell]:
    """Hierarchy-aware union of two geohash cell sets.

    Returns the normalized cell union in the grid's public identity
    order: contained cells are absorbed by their ancestors and complete
    sibling groups are merged into parents.


    Parameters
    ----------
    left, right : GeohashCell, str, or iterable of those with missing values skipped
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

    Returns
    -------
    CellArray of GeohashCell

    Raises
    ------
    ParseError
        If a cell or token is not a valid geohash cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = [cell for cell in gm.geohash_cover(p, precision=6) if cell is not None]
    >>> len(gm.geohash_union(cells, cells))
    1
    """

def geohash_intersection(
    left: _GeohashSetInput,
    right: _GeohashSetInput,
) -> CellArray[GeohashCell]:
    """Hierarchy-aware intersection of two geohash cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.


    Parameters
    ----------
    left, right : GeohashCell, str, or iterable of those with missing values skipped
        The two cell sets (any mix of cell objects or tokens).

    Returns
    -------
    CellArray of GeohashCell

    Raises
    ------
    ParseError
        If a cell or token is not a valid geohash cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = [cell for cell in gm.geohash_cover(p, precision=6) if cell is not None]
    >>> len(gm.geohash_intersection(cells, cells))
    1
    """

def geohash_difference(
    left: _GeohashSetInput,
    right: _GeohashSetInput,
) -> CellArray[GeohashCell]:
    """Hierarchy-aware difference of two geohash cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized in the grid's
    public identity order.


    Parameters
    ----------
    left, right : GeohashCell, str, or iterable of those with missing values skipped
        The two cell sets (any mix of cell objects or tokens).

    Returns
    -------
    CellArray of GeohashCell

    Raises
    ------
    ParseError
        If a cell or token is not a valid geohash cell.

    Examples
    --------

    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cells = [cell for cell in gm.geohash_cover(p, precision=6) if cell is not None]
    >>> len(gm.geohash_difference(cells, []))
    1
    """

@overload
def tile_cover(
    geom: GeometryArray[Geometry] | Iterable[Geometry],
    zoom: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[Tile]]: ...
@overload
def tile_cover(
    geom: GeometryArray[Geometry | None] | Iterable[Geometry | None],
    zoom: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[Tile] | None]: ...
@overload
def tile_cover(
    geom: Geometry,
    zoom: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> CellArray[Tile]:
    """Cover a geometry with Web Mercator tiles at a fixed zoom.

    The result is a ``CellArray`` for scalar input or ``Groups[CellArray]`` for
    array input, with tiles selected by ``cell_rule``. Keep the source geometry
    separately and use the free ``gm.*`` predicates for exact geometry
    questions.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of Geometry or None
        Geometry to cover (WGS84 lon/lat or projected). A scalar returns a
        flat cell array; an array or iterable returns one group per input row.
        ``None`` produces a missing group; a present empty geometry produces
        an empty group.

    zoom : int
        Tile zoom (``0``-``29``; finer at higher values).
        Tile rectangles use their emitted binary64 boundaries; this is a
        representational contract rather than exact ideal-transcendental
        Web Mercator arithmetic.

    cell_rule : {'within', 'center', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize. ``'within'`` selects cells entirely inside;
        ``'center'`` selects cells whose center is inside; ``'overlap'``
        selects every cell touching the geometry; and ``'bbox'`` selects every
        cell meeting the geometry's envelope (one window per part, split at the
        antimeridian). The rule never affects exact predicates.

    max_cells : int or None, default 1000000
        Hard cap on returned cells and retained result allocation. Discarded
        candidates do not consume it; array calls apply it cumulatively across
        groups. Pass ``None`` for unlimited output (bounded only by memory).

    Returns
    -------
    CellArray or Groups[CellArray]
        A scalar returns a ``CellArray``; an array returns one ``CellArray``
        group per input geometry.

    Raises
    ------
    GeometryError
        If the geometry, zoom, ``cell_rule``, or ``max_cells`` is invalid, or
        if the returned coverage would exceed ``max_cells``.
    InvalidGeometryError
        If the geometry is empty or any non-polar latitude is outside the square
        Web Mercator tile grid (±85.05112878 degrees).

    Examples
    --------
    >>> import gometry as gm
    >>> point = gm.Point(1, 1)
    >>> cells = gm.tile_cover(point, zoom=0)
    >>> len(cells)
    1
    """

@overload
def tile_cells(
    values: GeometryArray[Point] | Iterable[Point] | _CoordinateMatrix | FloatInput,
    lat: FloatInput | None = None,
    *,
    zoom: _IntInput,
) -> CellArray[Tile]: ...
@overload
def tile_cells(
    values: GeometryArray[Point | None] | Iterable[Point | None],
    lat: None = None,
    *,
    zoom: _IntInput,
) -> CellArray[Tile | None]:
    """Build tiles from points, coordinate rows, or parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, iterable of Point or None, coordinate matrix, or float
        Point geometries (``None`` makes a missing cell), a coordinate matrix
        with at least two columns, or WGS84 longitudes. Iterable points must
        share one CRS and epoch; projected point inputs are reprojected to
        WGS84. Coordinate matrices use only X and Y, ignoring extra ordinates.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Coordinate
        columns broadcast numpy-style; at least one must be a sequence.
        Latitudes outside the square Web Mercator tile grid (±85.051129°) raise
        ``InvalidGeometryError`` (no silent clamp).

    zoom : int or sequence of int
        Zoom level (0-29; finer at higher values). A scalar broadcasts to
        every row; an array supplies one zoom per row.
        Geographic point ownership follows the emitted binary64 tile edges,
        with west-inclusive/east-exclusive longitude and
        north-inclusive/south-exclusive latitude ties.

    Returns
    -------
    CellArray of Tile
        One tile per input coordinate.

    Raises
    ------
    GeometryError
        If the input is scalar, zoom is invalid, or coordinate columns have
        different lengths.
    InvalidGeometryError
        If coordinates are non-finite or leave the lon/lat domain, or a
        latitude is outside the square Web Mercator tile grid (±85.051129°).

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

def tile_bounding_cell(
    value: Geometry | GeometryArray[Geometry | None] | Iterable[float],
) -> Tile:
    """Return the deepest single tile containing a geometry or lon/lat bounds.

    The mercantile ``bounding_tile``: descends the lower-corner tile and accepts
    a candidate only after its complete lon/lat rectangle contains the query.
    An antimeridian-spanning region is contained by the wrapped z0 world tile;
    narrower tiles cannot represent both sides of the seam.

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
    left: _TileSetInput,
    right: _TileSetInput,
) -> CellArray[Tile]:
    """Hierarchy-aware union of two tile cell sets.

    Returns the normalized cell union in the grid's public identity
    order: contained cells are absorbed by their ancestors and complete
    sibling groups are merged into parents.


    Parameters
    ----------
    left, right : Tile, int, str, or iterable of those with missing values skipped
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

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
    >>> cells = [cell for cell in gm.tile_cover(p, zoom=10) if cell is not None]
    >>> len(gm.tile_union(cells, cells))
    1
    """

def tile_intersection(
    left: _TileSetInput,
    right: _TileSetInput,
) -> CellArray[Tile]:
    """Hierarchy-aware intersection of two tile cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.


    Parameters
    ----------
    left, right : Tile, int, str, or iterable of those with missing values skipped
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
    >>> cells = [cell for cell in gm.tile_cover(p, zoom=10) if cell is not None]
    >>> len(gm.tile_intersection(cells, cells))
    1
    """

def tile_difference(
    left: _TileSetInput,
    right: _TileSetInput,
) -> CellArray[Tile]:
    """Hierarchy-aware difference of two tile cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized in the grid's
    public identity order.


    Parameters
    ----------
    left, right : Tile, int, str, or iterable of those with missing values skipped
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
    >>> cells = [cell for cell in gm.tile_cover(p, zoom=10) if cell is not None]
    >>> len(gm.tile_difference(cells, []))
    1
    """

@overload
def pluscode_encode(
    value: Point, lat: None = None, *, length: _IntScalar = 10
) -> str: ...
@overload
def pluscode_encode(
    value: FloatScalar,
    lat: FloatScalar,
    *,
    length: _IntScalar = 10,
) -> str: ...
@overload
def pluscode_encode(
    value: GeometryArray[Point], lat: None = None, *, length: _IntScalar = 10
) -> list[str]: ...
@overload
def pluscode_encode(
    value: GeometryArray[Point | None],
    lat: None = None,
    *,
    length: _IntScalar = 10,
) -> list[str | None]: ...
@overload
def pluscode_encode(
    value: Sequence[float],
    lat: FloatInput,
    *,
    length: _IntScalar = 10,
) -> list[str]: ...
@overload
def pluscode_encode(
    value: FloatInput,
    lat: Sequence[float],
    *,
    length: _IntScalar = 10,
) -> list[str]: ...
@overload
def pluscode_encode(
    value: Buffer, lat: FloatInput, *, length: _IntScalar = 10
) -> str | list[str]: ...
@overload
def pluscode_encode(
    value: FloatInput, lat: Buffer, *, length: _IntScalar = 10
) -> str | list[str]: ...
@overload
def pluscode_encode(
    value: Iterable[float], lat: FloatInput, *, length: _IntScalar = 10
) -> str | list[str]: ...
@overload
def pluscode_encode(
    value: FloatInput, lat: Iterable[float], *, length: _IntScalar = 10
) -> str | list[str]:
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
def pluscode_shorten(code: str, reference: Point, lat: None = None) -> str: ...
@overload
def pluscode_shorten(
    code: Iterable[str], reference: Point, lat: None = None
) -> list[str]: ...
@overload
def pluscode_shorten(code: str, reference: FloatScalar, lat: FloatScalar) -> str: ...
@overload
def pluscode_shorten(
    code: str, reference: Sequence[float], lat: FloatInput
) -> list[str]: ...
@overload
def pluscode_shorten(
    code: str, reference: FloatInput, lat: Sequence[float]
) -> list[str]: ...
@overload
def pluscode_shorten(
    code: str, reference: Buffer, lat: FloatInput
) -> str | list[str]: ...
@overload
def pluscode_shorten(
    code: str, reference: FloatInput, lat: Buffer
) -> str | list[str]: ...
@overload
def pluscode_shorten(
    code: str, reference: Iterable[float], lat: FloatInput
) -> str | list[str]: ...
@overload
def pluscode_shorten(
    code: str, reference: FloatInput, lat: Iterable[float]
) -> str | list[str]: ...
@overload
def pluscode_shorten(
    code: Iterable[str], reference: FloatInput, lat: FloatInput
) -> list[str]:
    """Shorten a full plus code relative to a nearby reference point.

    Removes two to six leading digits that the reference location implies;
    `pluscode_recover` restores them.

    Parameters
    ----------
    code : str or iterable of str
        A full, unpadded plus code with at least 6 digits.
    reference : Point or float
        A CRS-aware reference point or longitude column.
    lat : float or sequence of float, optional
        Latitude column when ``reference`` is a longitude. Coordinate columns
        broadcast with one another and with a scalar code.

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
    >>> gm.pluscode_shorten('8FVC9G8F+6X', gm.Point(8.5, 47.4, crs=4326))
    '9G8F+6X'
    """

@overload
def pluscode_recover(code: str, reference: Point, lat: None = None) -> str: ...
@overload
def pluscode_recover(
    code: Iterable[str], reference: Point, lat: None = None
) -> list[str]: ...
@overload
def pluscode_recover(code: str, reference: FloatScalar, lat: FloatScalar) -> str: ...
@overload
def pluscode_recover(
    code: str, reference: Sequence[float], lat: FloatInput
) -> list[str]: ...
@overload
def pluscode_recover(
    code: str, reference: FloatInput, lat: Sequence[float]
) -> list[str]: ...
@overload
def pluscode_recover(
    code: str, reference: Buffer, lat: FloatInput
) -> str | list[str]: ...
@overload
def pluscode_recover(
    code: str, reference: FloatInput, lat: Buffer
) -> str | list[str]: ...
@overload
def pluscode_recover(
    code: str, reference: Iterable[float], lat: FloatInput
) -> str | list[str]: ...
@overload
def pluscode_recover(
    code: str, reference: FloatInput, lat: Iterable[float]
) -> str | list[str]: ...
@overload
def pluscode_recover(
    code: Iterable[str], reference: FloatInput, lat: FloatInput
) -> list[str]:
    """Recover the nearest full plus code from a shortened one.

    Parameters
    ----------
    code : str or iterable of str
        A short plus code (e.g. ``'9G8F+6X'``); a full code passes through
        normalized.
    reference : Point or float
        A CRS-aware reference point or longitude column.
    lat : float or sequence of float, optional
        Latitude column when ``reference`` is a longitude. Coordinate columns
        broadcast with one another and with a scalar code.

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
    >>> gm.pluscode_recover('9G8F+6X', gm.Point(8.5, 47.4, crs=4326))
    '8FVC9G8F+6X'
    """

@overload
def osm_shortlink_encode(
    value: Point,
    lat: None = None,
    *,
    zoom: _IntScalar = 16,
) -> str: ...
@overload
def osm_shortlink_encode(
    value: FloatScalar,
    lat: FloatScalar,
    *,
    zoom: _IntScalar = 16,
) -> str: ...
@overload
def osm_shortlink_encode(
    value: GeometryArray[Point],
    lat: None = None,
    *,
    zoom: _IntScalar = 16,
) -> list[str]: ...
@overload
def osm_shortlink_encode(
    value: GeometryArray[Point | None],
    lat: None = None,
    *,
    zoom: _IntScalar = 16,
) -> list[str | None]: ...
@overload
def osm_shortlink_encode(
    value: Sequence[float],
    lat: FloatInput,
    *,
    zoom: _IntScalar = 16,
) -> list[str]: ...
@overload
def osm_shortlink_encode(
    value: FloatInput,
    lat: Sequence[float],
    *,
    zoom: _IntScalar = 16,
) -> list[str]: ...
@overload
def osm_shortlink_encode(
    value: Buffer, lat: FloatInput, *, zoom: _IntScalar = 16
) -> str | list[str]: ...
@overload
def osm_shortlink_encode(
    value: FloatInput, lat: Buffer, *, zoom: _IntScalar = 16
) -> str | list[str]: ...
@overload
def osm_shortlink_encode(
    value: Iterable[float], lat: FloatInput, *, zoom: _IntScalar = 16
) -> str | list[str]: ...
@overload
def osm_shortlink_encode(
    value: FloatInput, lat: Iterable[float], *, zoom: _IntScalar = 16
) -> str | list[str]:
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
    np.ndarray[tuple[int], np.dtype[np.int64]],
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
    geom: GeometryArray[Geometry] | Iterable[Geometry],
    resolution: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[H3Cell]]: ...
@overload
def h3_cover(
    geom: GeometryArray[Geometry | None] | Iterable[Geometry | None],
    resolution: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[H3Cell] | None]: ...
@overload
def h3_cover(
    geom: Geometry,
    resolution: int,
    *,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> CellArray[H3Cell]:
    """Cover a geometry with H3 cells at ``resolution``.

    The result is a ``CellArray`` of cells satisfying ``cell_rule``. Keep the
    source geometry separately and use the free ``gm.*`` predicates for exact
    geometry questions.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of Geometry or None
        Geometry to cover (WGS84 lon/lat or projected). An array or iterable
        returns one group per input row; ``None`` produces a missing group and
        a present empty geometry produces an empty group.

    resolution : int
        H3 resolution (``0``-``15``; finer at higher values).

    cell_rule : {'within', 'center', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize, strictest to loosest. ``'within'``: only
        cells entirely inside — cells the area fully owns. ``'center'``:
        cells whose center is inside — unique assignment, balanced point
        binning. ``'overlap'``: every cell touching the geometry — a
        complete-coverage superset, the safe default for candidate keys.
        ``'bbox'``: every cell meeting the geometry's envelope, one window per
        part and split at the antimeridian — loosest and fastest.
        The rule never affects the exact predicates.

    max_cells : int or None, default 1000000
        Hard cap on returned cells and retained result allocation. Discarded
        candidates do not consume it; array calls apply it cumulatively across
        groups. Pass ``None`` for unlimited output (bounded only by memory).

    Returns
    -------
    CellArray or Groups of CellArray
        A scalar returns its coverage; an array returns one cell group per row.

    Raises
    ------
    GeometryError
        If the geometry, depth, or a coverage parameter is invalid, or if
        the covering would exceed ``max_cells``.
    InvalidGeometryError
        If the geometry is empty or coordinates leave the lon/lat domain.

    See Also
    --------
    h3_cells : Build H3 cells from lon/lat columns.

    Examples
    --------
    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cov = gm.h3_cover(p, resolution=7)
    >>> cell = cov[0]
    >>> assert cell is not None
    >>> (len(cov), cell.token)
    (1, '872830828ffffff')
    """

@overload
def h3_cells(
    values: GeometryArray[Point] | Iterable[Point] | _CoordinateMatrix | FloatInput,
    lat: FloatInput | None = None,
    *,
    resolution: _IntInput,
) -> CellArray[H3Cell]: ...
@overload
def h3_cells(
    values: GeometryArray[Point | None] | Iterable[Point | None],
    lat: None = None,
    *,
    resolution: _IntInput,
) -> CellArray[H3Cell | None]:
    """Build H3 cells from points, coordinate rows, or parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, iterable of Point or None, coordinate matrix, or float
        Point geometries (``None`` makes a missing cell), a coordinate matrix
        with at least two columns, or WGS84 longitudes. Iterable points must
        share one CRS and epoch; projected point inputs are reprojected to
        WGS84. Coordinate matrices use only X and Y, ignoring extra ordinates.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Coordinate
        columns broadcast numpy-style; at least one must be a sequence.

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
        If a coordinate is non-finite or coordinate columns differ in length.

    See Also
    --------
    H3Cell : Build a single cell.
    h3_cover : Cover a geometry with H3 cells.

    Examples
    --------
    >>> import gometry as gm
    >>> cells = gm.h3_cells([-122.4, -122.3], [37.8, 37.7], resolution=7)
    >>> cell = cells[0]
    >>> assert cell is not None
    >>> (len(cells), cell.token)
    (2, '87283080cffffff')
    """

def h3_bounding_cell(
    value: Geometry | GeometryArray[Geometry | None] | Iterable[float],
) -> H3Cell:
    """Return the deepest single H3 cell containing a geometry or lon/lat bounds.

    Walks a necessary ancestor path, then proves the complete region against
    every ordered native H3 edge. Vertex ownership is not a containment proof;
    when the finest candidate cannot be proved, a coarser proven ancestor is
    returned.

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
    left: _H3SetInput,
    right: _H3SetInput,
) -> CellArray[H3Cell]:
    """Hierarchy-aware union of two H3 cell sets.

    Returns the normalized cell union in the grid's public identity
    order: contained cells are absorbed by their ancestors and complete
    sibling groups are merged into parents.
    This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.

    Parameters
    ----------
    left, right : H3Cell, int, str, or iterable of those with missing values skipped
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

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
    >>> cover = gm.h3_cover(p, resolution=7)
    >>> cell = cover[0]
    >>> assert cell is not None
    >>> neighbors = cell.neighbors
    >>> nbr = neighbors[0]
    >>> assert nbr is not None
    >>> cells = [item for item in cover if item is not None]
    >>> len(gm.h3_union(cells, [item for item in neighbors if item is not None][:1]))
    2
    """

def h3_intersection(
    left: _H3SetInput,
    right: _H3SetInput,
) -> CellArray[H3Cell]:
    """Hierarchy-aware intersection of two H3 cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.
    This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.

    Parameters
    ----------
    left, right : H3Cell, int, str, or iterable of those with missing values skipped
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
    >>> cover = gm.h3_cover(p, resolution=7)
    >>> cell = cover[0]
    >>> assert cell is not None
    >>> neighbors = cell.neighbors
    >>> nbr = neighbors[0]
    >>> assert nbr is not None
    >>> cells = [item for item in cover if item is not None]
    >>> len(gm.h3_intersection(cells + [nbr], [item for item in neighbors if item is not None][:1]))
    1
    """

def h3_difference(
    left: _H3SetInput,
    right: _H3SetInput,
) -> CellArray[H3Cell]:
    """Hierarchy-aware difference of two H3 cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized in the grid's
    public identity order.
    This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.

    Parameters
    ----------
    left, right : H3Cell, int, str, or iterable of those with missing values skipped
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
    >>> cover = gm.h3_cover(p, resolution=7)
    >>> cell = cover[0]
    >>> assert cell is not None
    >>> neighbors = cell.neighbors
    >>> nbr = neighbors[0]
    >>> assert nbr is not None
    >>> cells = [item for item in cover if item is not None]
    >>> len(gm.h3_difference(cells + [nbr], [item for item in neighbors if item is not None][:1]))
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
    geom: GeometryArray[Geometry] | Iterable[Geometry],
    level: int | None = None,
    *,
    target_cells: int = 8,
    min_level: int | None = None,
    max_level: int | None = None,
    level_mod: Literal[1, 2, 3] = 1,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[S2Cell]]: ...
@overload
def s2_cover(
    geom: GeometryArray[Geometry | None] | Iterable[Geometry | None],
    level: int | None = None,
    *,
    target_cells: int = 8,
    min_level: int | None = None,
    max_level: int | None = None,
    level_mod: Literal[1, 2, 3] = 1,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> Groups[CellArray[S2Cell] | None]: ...
@overload
def s2_cover(
    geom: Geometry,
    level: int | None = None,
    *,
    target_cells: int = 8,
    min_level: int | None = None,
    max_level: int | None = None,
    level_mod: Literal[1, 2, 3] = 1,
    cell_rule: CellRule = 'overlap',
    max_cells: int | None = 1000000,
) -> CellArray[S2Cell]:
    """Cover a geometry with fixed-level or adaptively refined S2 cells.

    Omit ``level`` to adapt refinement to ``target_cells`` and the optional
    level range. Supplying ``level`` fixes the output depth; non-default
    adaptive controls cannot be combined with it. Keep the source geometry
    separately and use the free ``gm.*`` predicates for exact geometry
    questions.

    Parameters
    ----------
    geom : Geometry, GeometryArray, or iterable of Geometry or None
        Geometry to cover (WGS84 lon/lat or projected). An array or iterable
        returns one group per input row; ``None`` produces a missing group and
        a present empty geometry produces an empty group.

    level : int, optional
        Fixed S2 cell level (``0``-``30``; finer at higher values). The level is
        never coarsened to fit the output cap. Omit it for adaptive refinement.

    target_cells : int, default 8
        Approximate adaptive refinement target; the minimum admissible cover
        can exceed this value.

    min_level : int or None, default None
        Coarsest adaptive S2 level allowed; omission uses level ``0``.

    max_level : int or None, default None
        Finest adaptive S2 level allowed; omission uses level ``30``.

    level_mod : {1, 2, 3}, default 1
        Restrict adaptive output levels to increments from ``min_level``.

    cell_rule : {'within', 'center', 'overlap', 'bbox'}, default 'overlap'
        Which cells to materialize, strictest to loosest. ``'within'``: only
        cells entirely inside — cells the area fully owns. ``'center'``:
        cells whose center is inside — unique assignment, balanced point
        binning. ``'overlap'``: every cell touching the geometry — a
        complete-coverage superset, the safe default for candidate keys.
        ``'bbox'``: every cell meeting the geometry's envelope, one window per
        part and split at the antimeridian — loosest and fastest.
        The rule never affects the exact predicates.

    max_cells : int or None, default 1000000
        Hard cap on returned cells, applied cumulatively across array groups.
        In fixed-level mode an insufficient cap raises; adaptive mode may
        coarsen only within its requested level range. Pass ``None`` for an
        unlimited cover.

    Returns
    -------
    CellArray or Groups of CellArray
        A scalar returns its coverage; an array returns one cell group per row.

    Raises
    ------
    GeometryError
        If the geometry, level, or a coverage parameter is invalid, if fixed
        and non-default adaptive controls are combined, or if the cover exceeds
        ``max_cells``.
    InvalidGeometryError
        If a scalar geometry is empty or coordinates leave the lon/lat domain.

    See Also
    --------
    s2_cells : Build S2 cells from lon/lat columns.
    s2_cover : Cover with fixed or adaptive levels.

    Examples
    --------
    >>> import gometry as gm
    >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    >>> cov = gm.s2_cover(p, level=12)
    >>> cell = cov[0]
    >>> assert cell is not None
    >>> (len(cov), cell.token)
    (1, '8085809')
    """

@overload
def s2_cells(
    values: GeometryArray[Point] | Iterable[Point] | _CoordinateMatrix | FloatInput,
    lat: FloatInput | None = None,
    *,
    level: _IntInput,
) -> CellArray[S2Cell]: ...
@overload
def s2_cells(
    values: GeometryArray[Point | None] | Iterable[Point | None],
    lat: None = None,
    *,
    level: _IntInput,
) -> CellArray[S2Cell | None]:
    """Build S2 cells from points, coordinate rows, or parallel lon/lat columns.

    Parameters
    ----------
    values : GeometryArray of Point, iterable of Point or None, coordinate matrix, or float
        Point geometries (``None`` makes a missing cell), a coordinate matrix
        with at least two columns, or WGS84 longitudes. Iterable points must
        share one CRS and epoch; projected point inputs are reprojected to
        WGS84. Coordinate matrices use only X and Y, ignoring extra ordinates.

    lat : float or sequence of float, optional
        WGS84 latitude per row when ``values`` supplies longitudes. Coordinate
        columns broadcast numpy-style; at least one must be a sequence.

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

def s2_bounding_cell(
    value: Geometry | GeometryArray[Geometry | None] | Iterable[float],
) -> S2Cell:
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
    left: _S2SetInput,
    right: _S2SetInput,
) -> CellArray[S2Cell]:
    """Hierarchy-aware union of two S2 cell sets.

    Returns the normalized cell union in the grid's public identity
    order: contained cells are absorbed by their ancestors and complete
    sibling groups are merged into parents.


    Parameters
    ----------
    left, right : S2Cell, int, str, or iterable of those with missing values skipped
        A single cell identity or a collection on either side (any
        accepted mix of cell objects and identity values for this grid).

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
    >>> cells = [cell for cell in gm.s2_cover(p, level=12) if cell is not None]
    >>> len(gm.s2_union(cells, cells))
    1
    """

def s2_intersection(
    left: _S2SetInput,
    right: _S2SetInput,
) -> CellArray[S2Cell]:
    """Hierarchy-aware intersection of two S2 cell sets.

    A cell survives where the two sets overlap; ancestor/descendant overlap
    keeps the finer cell.


    Parameters
    ----------
    left, right : S2Cell, int, str, or iterable of those with missing values skipped
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
    >>> cells = [cell for cell in gm.s2_cover(p, level=12) if cell is not None]
    >>> len(gm.s2_intersection(cells, cells))
    1
    """

def s2_difference(
    left: _S2SetInput,
    right: _S2SetInput,
) -> CellArray[S2Cell]:
    """Hierarchy-aware difference of two S2 cell sets.

    Cells of ``left`` partially covered by ``right`` split into children
    until the remainder is exact; the result is normalized in the grid's
    public identity order.


    Parameters
    ----------
    left, right : S2Cell, int, str, or iterable of those with missing values skipped
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
    >>> cells = [cell for cell in gm.s2_cover(p, level=12) if cell is not None]
    >>> len(gm.s2_difference(cells, []))
    1
    """
