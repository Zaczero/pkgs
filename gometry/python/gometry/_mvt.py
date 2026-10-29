"""Mapbox Vector Tile feature interchange."""

from __future__ import annotations

import math
import sys
from collections.abc import Generator, Iterable, Mapping
from dataclasses import dataclass
from fractions import Fraction
from itertools import chain, islice
from operator import index
from types import EllipsisType  # ruff: ignore[typing-only-standard-library-import] — runtime annotations
from typing import (
    Any,
    Literal,
    SupportsIndex,
    TypeAlias,
    TypedDict,
    Unpack,
    cast,
    overload,
)

import numpy as np

from gometry._lib import (
    CRS,
    CRSError,
    Geometry,
    GeometryArray,
    InvalidGeometryError,
    Point,
    Tile,
    _FeaturePropertyRows,
    _mvt_decode_local,
    _mvt_encode_layer,
    _mvt_locate_grid,
    _mvt_prepare_grid,
    _mvt_validate_inputs,
    _mvt_validate_layer,
    _MVTSpatialIndex,
    box,
    coverage_is_valid,
    equals_identical,
)
from gometry._types import (
    Buffer,
    CrsInput,
    Features,
    FloatScalar,
    ScalarFloatArray,
    _EpochInput,
)

_MVTScalar: TypeAlias = str | bool | int | float | None
_MVTFeatures: TypeAlias = Features[Geometry, _MVTScalar, int]
_Ordinary: TypeAlias = Features | Iterable[Features]


class _TileOptions(TypedDict, total=False):
    extent: SupportsIndex
    buffer: SupportsIndex
    tolerance: FloatScalar | ScalarFloatArray
    method: Literal['vw', 'dp']
    clip: bool
    on_collapse: Literal['omit', 'raise']


class _PrepareOptions(_TileOptions, total=False):
    drop_zm: bool


def _extent(value: SupportsIndex) -> int:
    if isinstance(value, (bool, np.bool_)):
        raise TypeError('extent must be a positive uint32')
    result = index(value)
    if not 0 < result <= (1 << 32) - 1:
        raise ValueError('extent must be a positive uint32')
    return result


def _buffer(value: SupportsIndex) -> int:
    if isinstance(value, (bool, np.bool_)):
        raise TypeError('buffer must be a nonnegative integer')
    result = index(value)
    if result < 0:
        raise ValueError('buffer must be a nonnegative integer')
    return result


def _outward(value: Fraction, *, upper: bool) -> float:
    limit = Fraction(sys.float_info.max)
    if value <= -limit:
        return -sys.float_info.max
    if value >= limit:
        return sys.float_info.max
    rounded = float(value)
    represented = Fraction(rounded)
    if (upper and represented < value) or (not upper and represented > value):
        return math.nextafter(rounded, math.inf if upper else -math.inf)
    return rounded


class TileFrame:
    """A tile rectangle and the coordinate frame in which it is expressed."""

    __slots__ = ('_bounds', '_crs', '_epoch')

    _bounds: tuple[float, float, float, float]
    _crs: CRS | None
    _epoch: float | None

    def __init__(
        self,
        bounds: Iterable[float],
        *,
        crs: CrsInput | None,
        epoch: _EpochInput | None = None,
    ) -> None:
        """Define a rectangular tile coordinate frame.

        Parameters
        ----------
        bounds : iterable of float
            The ``(minx, miny, maxx, maxy)`` rectangle, with positive width
            and height.
        crs : CrsInput or None
            Coordinate reference system of the bounds, or ``None`` for an
            unreferenced frame.
        epoch : float or None, default None
            Coordinate epoch associated with the frame.

        Returns
        -------
        TileFrame
            The immutable tile rectangle and its coordinate frame.

        Raises
        ------
        CRSError
            If the coordinate reference system or epoch is invalid.
        GeometryError
            If the bounds cannot define a valid rectangle.
        """
        bounds = tuple(islice(bounds, 5))
        if len(bounds) != 4:
            raise ValueError('tile bounds must contain four coordinates')
        rectangle = box(*bounds)
        rectangle_bounds = rectangle.bounds
        assert rectangle_bounds is not None
        minx, miny, maxx, maxy = rectangle_bounds
        if minx >= maxx or miny >= maxy:
            raise ValueError('tile bounds must have positive width and height')
        frame = Point(0, 0, crs=crs, epoch=epoch)
        object.__setattr__(self, '_bounds', (minx, miny, maxx, maxy))
        object.__setattr__(self, '_crs', frame.crs)
        object.__setattr__(self, '_epoch', frame.epoch)

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """The frame rectangle.

        Returns
        -------
        tuple of float
            Bounds as ``(minx, miny, maxx, maxy)``.
        """
        return self._bounds

    @property
    def crs(self) -> CRS | None:
        """The coordinate reference system.

        Returns
        -------
        CRS or None
            The coordinate reference system, if present.
        """
        return self._crs

    @property
    def epoch(self) -> float | None:
        """The coordinate epoch.

        Returns
        -------
        float or None
            The coordinate epoch, if present.
        """
        return self._epoch

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError('TileFrame is immutable')

    def __delattr__(self, name: str) -> None:
        raise AttributeError('TileFrame is immutable')

    def __copy__(self) -> TileFrame:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> TileFrame:
        return self

    def __replace__(
        self,
        *,
        bounds: Iterable[float] | EllipsisType = ...,
        crs: CrsInput | EllipsisType | None = ...,
        epoch: _EpochInput | EllipsisType | None = ...,
    ) -> TileFrame:
        return TileFrame(
            self.bounds if bounds is ... else bounds,
            crs=self.crs if crs is ... else crs,
            epoch=self.epoch if epoch is ... else epoch,
        )

    def __repr__(self) -> str:
        return f'TileFrame({self.bounds!r}, crs={self.crs!r}, epoch={self.epoch!r})'


class MVTLayer:
    """An immutable feature snapshot in an MVT-local integer grid."""

    __slots__ = ('_extent', '_features')

    _extent: int
    _features: _MVTFeatures

    def __init__(self, features: Features, *, extent: SupportsIndex = 4096) -> None:
        """Create a layer from geometry already in the local integer grid.

        Parameters
        ----------
        features : Features
            Local geometries, supported scalar properties, and optional
            unsigned integer IDs. Missing and empty geometries are omitted.
        extent : int, default 4096
            Positive integer width and height of the tile grid.

        Returns
        -------
        MVTLayer
            The immutable local layer.
        """
        if not isinstance(features, Features):
            raise TypeError('MVTLayer requires Features')
        extent = _extent(extent)
        _mvt_validate_layer(
            features.geometries, features._properties, features.ids, extent
        )
        keep = ~(features.geometries.is_missing | features.geometries.is_empty)
        if not keep.all():
            features = features[keep]
        object.__setattr__(self, '_features', cast('_MVTFeatures', features))
        object.__setattr__(self, '_extent', extent)

    @classmethod
    def _from_decoded(cls, features: Features, extent: int) -> MVTLayer:
        result = object.__new__(cls)
        object.__setattr__(result, '_features', cast('_MVTFeatures', features))  # ruff: ignore[unnecessary-dunder-call] — initialize immutable instance
        object.__setattr__(result, '_extent', extent)  # ruff: ignore[unnecessary-dunder-call] — initialize immutable instance
        return result

    @property
    def features(self) -> _MVTFeatures:
        """The layer's feature snapshot.

        Returns
        -------
        Features
            Local geometry, sparse properties and optional IDs.
        """
        return self._features

    @property
    def extent(self) -> int:
        """The tile-grid extent.

        Returns
        -------
        int
            Integer width and height of the grid.
        """
        return self._extent

    @property
    def version(self) -> int:
        """The MVT wire version.

        Returns
        -------
        int
            The version number, always 2.
        """
        return 2

    @overload
    def to_features(
        self,
        *,
        tile: Tile,
        crs: CrsInput | None = None,
        epoch: _EpochInput | None = None,
    ) -> _MVTFeatures: ...

    @overload
    def to_features(
        self,
        *,
        frame: TileFrame,
        crs: CrsInput | None = None,
        epoch: _EpochInput | None = None,
    ) -> _MVTFeatures: ...

    def to_features(
        self,
        *,
        tile: Tile | None = None,
        frame: TileFrame | None = None,
        crs: CrsInput | None = None,
        epoch: _EpochInput | None = None,
    ) -> _MVTFeatures:
        """Locate local coordinates in an XYZ tile or custom frame.

        Parameters
        ----------
        tile : Tile
            XYZ address in Web Mercator. Supply exactly one of ``tile`` and
            ``frame``.
        frame : TileFrame
            Custom rectangle and coordinate reference system.
        crs : CrsInput or None, default None
            Optional output coordinate reference system.
        epoch : float or None, default None
            Optional output coordinate epoch.

        Returns
        -------
        Features
            Located geometries with the layer's properties and IDs.

        Raises
        ------
        CRSError
            If the requested coordinate transformation cannot be performed.
        GeometryError
            If the located geometry cannot be represented.
        """
        if (tile is None) == (frame is None):
            raise TypeError('choose exactly one tile or frame')
        if tile is not None and not isinstance(tile, Tile):
            raise TypeError('tile must be a Tile')
        if frame is not None and not isinstance(frame, TileFrame):
            raise TypeError('frame must be a TileFrame')
        address = None if tile is None else (tile.zoom, tile.x, tile.y)
        bounds = None if frame is None else frame.bounds
        geometries = _mvt_locate_grid(
            self.features.geometries, bounds, address, self.extent
        )
        source_crs: CRS | None
        if tile is not None:
            source_crs = CRS(3857)
            source_epoch = None
        else:
            assert frame is not None
            source_crs = frame.crs
            source_epoch = frame.epoch
        if source_crs is not None:
            geometries = geometries.with_crs(source_crs)
        if source_epoch is not None:
            geometries = geometries.with_epoch(source_epoch)
        if crs is not None or epoch is not None:
            if source_crs is None:
                raise CRSError(
                    'a CRS-free tile frame cannot be reprojected or assigned an epoch'
                )
            geometries = geometries.to_crs(
                source_crs if crs is None else crs, epoch=epoch
            )
        return self.features.with_geometries(geometries)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError('MVTLayer is immutable')

    def __delattr__(self, name: str) -> None:
        raise AttributeError('MVTLayer is immutable')

    def __copy__(self) -> MVTLayer:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> MVTLayer:
        return self

    def __replace__(
        self,
        *,
        features: Features | EllipsisType = ...,
        extent: SupportsIndex | EllipsisType = ...,
    ) -> MVTLayer:
        return MVTLayer(
            self.features if features is ... else features,
            extent=self.extent if extent is ... else extent,
        )

    def __repr__(self) -> str:
        return f'MVTLayer(features={self.features!r}, extent={self.extent})'


@overload
def mvt_query_regions(
    *, tile: Tile, extent: SupportsIndex = 4096, buffer: SupportsIndex = 64
) -> tuple[TileFrame, ...]: ...


@overload
def mvt_query_regions(
    *, frame: TileFrame, extent: SupportsIndex = 4096, buffer: SupportsIndex = 64
) -> tuple[TileFrame, ...]: ...


def mvt_query_regions(
    *,
    tile: Tile | None = None,
    frame: TileFrame | None = None,
    extent: SupportsIndex = 4096,
    buffer: SupportsIndex = 64,
) -> tuple[TileFrame, ...]:
    """Return conservative source-frame rectangles for clipped tile preparation.

    Parameters
    ----------
    tile : Tile
        XYZ address. Supply exactly one of ``tile`` and ``frame``.
    frame : TileFrame
        Custom frame to expand.
    extent : int, default 4096
        Positive integer tile-grid extent.
    buffer : int, default 64
        Nonnegative tile-grid buffer.

    Returns
    -------
    tuple of TileFrame
        Conservative query rectangles. XYZ rectangles are in EPSG:3857 and
        may be split at the world seam.
    """
    if (tile is None) == (frame is None):
        raise TypeError('choose exactly one tile or frame')
    if tile is not None and not isinstance(tile, Tile):
        raise TypeError('tile must be a Tile')
    if frame is not None and not isinstance(frame, TileFrame):
        raise TypeError('frame must be a TileFrame')
    extent = _extent(extent)
    buffer = _buffer(buffer)
    if frame is not None:
        return (
            TileFrame(
                _frame_query_bounds(frame, extent, buffer),
                crs=frame.crs,
                epoch=frame.epoch,
            ),
        )

    assert tile is not None
    regions = _tile_block_regions(
        tile.zoom, tile.x, tile.x, tile.y, tile.y, extent, buffer
    )
    return tuple(TileFrame(bounds, crs=3857) for bounds in sorted(regions))


def _frame_query_bounds(
    frame: TileFrame, extent: int, buffer: int
) -> tuple[float, float, float, float]:
    minx, miny, maxx, maxy = map(Fraction, frame.bounds)
    margin = buffer + 1
    x_margin = (maxx - minx) * margin / extent
    y_margin = (maxy - miny) * margin / extent
    return (
        _outward(minx - x_margin, upper=False),
        _outward(miny - y_margin, upper=False),
        _outward(maxx + x_margin, upper=True),
        _outward(maxy + y_margin, upper=True),
    )


def _names(value: Iterable[str] | None, argument: str) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f'{argument} must be an iterable of names, not a string')
    result = []
    seen = set()
    for name in value:
        if not isinstance(name, str):
            raise TypeError(f'{argument} must contain strings')
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


@dataclass(frozen=True, slots=True)
class _Policy:
    extent: int
    buffer: int
    tolerance: float
    method: str
    clip: bool
    on_collapse: str
    drop_zm: bool


def _policy(
    *,
    extent: SupportsIndex = 4096,
    buffer: SupportsIndex = 64,
    tolerance: FloatScalar | ScalarFloatArray = 0,
    method: str = 'vw',
    clip: bool = True,
    on_collapse: str = 'omit',
    drop_zm: bool = False,
) -> _Policy:
    extent = _extent(extent)
    buffer = _buffer(buffer)
    if not isinstance(clip, (bool, np.bool_)) or not isinstance(
        drop_zm, (bool, np.bool_)
    ):
        raise TypeError('clip and drop_zm must be boolean')
    if method not in ('vw', 'dp'):
        raise ValueError("method must be 'vw' or 'dp'")
    if on_collapse not in ('omit', 'raise'):
        raise ValueError("on_collapse must be 'omit' or 'raise'")
    if isinstance(tolerance, np.ndarray) and tolerance.ndim != 0:
        raise TypeError('tolerance must be a scalar')
    tolerance = float(cast('Any', tolerance))
    if not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError('tolerance must be finite and nonnegative')
    return _Policy(
        extent, buffer, tolerance, method, bool(clip), on_collapse, bool(drop_zm)
    )


def _validate_features(
    features: Features,
    *,
    drop_zm: bool,
    coverage: bool,
) -> None:
    _mvt_validate_inputs(
        features.geometries, features._properties, features.ids, len(features)
    )
    geometries = features.geometries
    active = ~(geometries.is_missing | geometries.is_empty)
    invalid = np.flatnonzero(~geometries.is_valid & active)
    if invalid.size:
        raise InvalidGeometryError(f'feature {int(invalid[0])} is invalid')
    if not drop_zm and ((geometries.has_z | geometries.has_m) & active).any():
        raise InvalidGeometryError('tile preparation requires explicit Z/M removal')
    if coverage and not coverage_is_valid(geometries):
        raise InvalidGeometryError('source is not a valid polygonal coverage')


def _project_features(
    features: Features,
    geometries: GeometryArray[Geometry | None],
    destination: CRS | None,
    destination_epoch: float | None,
    *,
    drop_zm: bool,
) -> Features:
    if geometries.crs != destination or geometries.epoch != destination_epoch:
        if geometries.crs is None or destination is None:
            raise CRSError('tile frame and geometry must have compatible CRS metadata')
        geometries = geometries.to_crs(destination, epoch=destination_epoch)
        if geometries.epoch != destination_epoch:
            raise CRSError('tile frame and geometry must have compatible epochs')
        active = ~(geometries.is_missing | geometries.is_empty)
        invalid = np.flatnonzero(~geometries.is_valid & active)
        if invalid.size:
            raise InvalidGeometryError(
                f'feature {int(invalid[0])} is invalid after reprojection'
            )
    if drop_zm:
        geometries = geometries.force_2d()
    return features.with_geometries(geometries)


def _admit_features(
    features: Features,
    destination: CRS | None,
    destination_epoch: float | None,
    *,
    drop_zm: bool,
    coverage: bool,
    xyz: bool,
) -> Features:
    _validate_features(features, drop_zm=drop_zm, coverage=coverage)
    geometries = features.geometries
    if xyz and geometries.crs is not None and geometries.crs.is_geographic:
        geometries = geometries.split_antimeridian()
    return _project_features(
        features, geometries, destination, destination_epoch, drop_zm=drop_zm
    )


def _admit_source_batch(
    features: Features,
    destination: CRS | None,
    destination_epoch: float | None,
    *,
    drop_zm: bool,
    coverage: bool,
) -> tuple[Features, Features]:
    _validate_features(features, drop_zm=drop_zm, coverage=coverage)
    geometries = features.geometries
    literal = _project_features(
        features, geometries, destination, destination_epoch, drop_zm=drop_zm
    )
    if (
        destination != CRS(3857)
        or geometries.crs is None
        or not geometries.crs.is_geographic
    ):
        return literal, literal
    split = geometries.split_antimeridian()
    if np.all(equals_identical(geometries, split) | geometries.is_missing):
        return literal, literal
    xyz = _project_features(
        features, split, destination, destination_epoch, drop_zm=drop_zm
    )
    return literal, xyz


def _concatenate_features(
    parts: list[Features], *, crs: CRS | None, epoch: float | None
) -> Features:
    if not parts:
        return Features(GeometryArray([], crs=crs, epoch=epoch))
    if len(parts) == 1:
        return parts[0]
    geometries = parts[0].geometries.concat(*(part.geometries for part in parts[1:]))
    properties = _FeaturePropertyRows.concatenate([part._properties for part in parts])
    ids = tuple(chain.from_iterable(part.ids for part in parts))
    id_column = 'id' if any(part._id_column is not None for part in parts) else None
    return Features._from_property_rows(geometries, properties, ids, id_column)


def _compact_features(features: Features) -> Features:
    return Features._from_property_rows(
        features.geometries,
        features._properties._compact(),
        tuple(features.ids),
        features._id_column,
    )


@dataclass(frozen=True, slots=True)
class _SourceData:
    features: Features
    index: _MVTSpatialIndex
    xyz_features: Features
    xyz_index: _MVTSpatialIndex
    coverage: bool


class MVTSource:
    """A projected immutable feature source with reusable spatial selection."""

    __slots__ = ('_data', '_policy')

    _data: _SourceData
    _policy: _Policy

    def __init__(
        self,
        features: Features | Iterable[Features],
        *,
        crs: CrsInput | None = 3857,
        epoch: _EpochInput | None = None,
        coverage: bool = False,
        **options: Unpack[_PrepareOptions],
    ) -> None:
        """Snapshot and index a feature collection for repeated tile preparation.

        Parameters
        ----------
        features : Features or iterable of Features
            Input features or batches. Batches are consumed once.
        crs : CrsInput or None, default 3857
            Coordinate reference system of the indexed source. XYZ tiles
            require EPSG:3857 without an epoch.
        epoch : float or None, default None
            Coordinate epoch of the indexed source.
        coverage : bool, default False
            Treat the source as a polygonal coverage and validate it jointly.
        **options
            Preparation settings: ``extent`` (4096), ``buffer`` (64),
            ``tolerance`` (0, in grid units), ``method`` (``'vw'`` by
            default, or ``'dp'``), ``clip`` (True), ``on_collapse``
            (``'omit'`` by default, or ``'raise'``), and ``drop_zm`` (False).

        Returns
        -------
        MVTSource
            The projected snapshot and reusable spatial index.

        Raises
        ------
        CRSError
            If input geometries cannot be transformed into the source frame.
        GeometryError
            If an input geometry or declared coverage is invalid.
        """
        if not isinstance(coverage, (bool, np.bool_)):
            raise TypeError('coverage must be boolean')
        policy = _policy(**options)
        frame = Point(0, 0, crs=crs, epoch=epoch)
        if isinstance(features, Features):
            admitted, xyz_admitted = _admit_source_batch(
                features,
                frame.crs,
                frame.epoch,
                drop_zm=policy.drop_zm,
                coverage=bool(coverage),
            )
        else:
            parts = []
            xyz_parts = []
            has_distinct_xyz = False
            for batch in features:
                if not isinstance(batch, Features):
                    raise TypeError('source batches must contain Features')
                part, xyz_part = _admit_source_batch(
                    batch,
                    frame.crs,
                    frame.epoch,
                    drop_zm=policy.drop_zm,
                    coverage=False,
                )
                parts.append(part)
                xyz_parts.append(xyz_part)
                has_distinct_xyz |= xyz_part is not part
            admitted = _concatenate_features(parts, crs=frame.crs, epoch=frame.epoch)
            xyz_admitted = (
                _concatenate_features(xyz_parts, crs=frame.crs, epoch=frame.epoch)
                if has_distinct_xyz
                else admitted
            )
        if coverage and (
            not coverage_is_valid(admitted.geometries)
            or (
                xyz_admitted is not admitted
                and not coverage_is_valid(xyz_admitted.geometries)
            )
        ):
            raise InvalidGeometryError('source is not a valid polygonal coverage')
        spatial = _MVTSpatialIndex(admitted.geometries, frame.crs == CRS(3857))
        xyz_spatial = (
            spatial
            if xyz_admitted is admitted
            else _MVTSpatialIndex(xyz_admitted.geometries, True)
        )
        object.__setattr__(
            self,
            '_data',
            _SourceData(admitted, spatial, xyz_admitted, xyz_spatial, bool(coverage)),
        )
        object.__setattr__(self, '_policy', policy)

    def with_options(self, **options: Unpack[_TileOptions]) -> MVTSource:
        """Return another tile policy sharing the source and spatial index.

        Parameters
        ----------
        **options
            Replacement ``extent``, ``buffer``, ``tolerance``, ``method``,
            ``clip``, or ``on_collapse`` settings.

        Returns
        -------
        MVTSource
            A source sharing the indexed snapshot with the new tile policy.
        """
        allowed = {'extent', 'buffer', 'tolerance', 'method', 'clip', 'on_collapse'}
        if unsupported := options.keys() - allowed:
            raise TypeError(f'unsupported source option: {min(unsupported)}')
        policy = _policy(
            extent=options.get('extent', self._policy.extent),
            buffer=options.get('buffer', self._policy.buffer),
            tolerance=options.get('tolerance', self._policy.tolerance),
            method=options.get('method', self._policy.method),
            clip=options.get('clip', self._policy.clip),
            on_collapse=options.get('on_collapse', self._policy.on_collapse),
            drop_zm=self._policy.drop_zm,
        )
        result = object.__new__(MVTSource)
        object.__setattr__(result, '_data', self._data)  # ruff: ignore[unnecessary-dunder-call] — initialize immutable instance
        object.__setattr__(result, '_policy', policy)  # ruff: ignore[unnecessary-dunder-call] — initialize immutable instance
        return result

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError('MVTSource is immutable')

    def __delattr__(self, name: str) -> None:
        raise AttributeError('MVTSource is immutable')

    def __copy__(self) -> MVTSource:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> MVTSource:
        return self


@overload
def mvt_prepare(
    features: _Ordinary, *, tile: Tile, **options: Unpack[_PrepareOptions]
) -> MVTLayer: ...


@overload
def mvt_prepare(
    features: _Ordinary, *, frame: TileFrame, **options: Unpack[_PrepareOptions]
) -> MVTLayer: ...


@overload
def mvt_prepare(features: MVTSource, *, tile: Tile) -> MVTLayer: ...


@overload
def mvt_prepare(features: MVTSource, *, frame: TileFrame) -> MVTLayer: ...


def mvt_prepare(
    features: Features | MVTSource | Iterable[Features],
    *,
    tile: Tile | None = None,
    frame: TileFrame | None = None,
    **options: Any,
) -> MVTLayer:
    """Prepare feature geometry on a tile's integer grid.

    Parameters
    ----------
    features : Features, MVTSource or iterable of Features
        Features, a reusable indexed source, or batches of features.
    tile : Tile
        XYZ address. Supply exactly one of ``tile`` and ``frame``.
    frame : TileFrame
        Custom tile frame.
    **options
        Preparation settings for ordinary features: ``extent`` (4096),
        ``buffer`` (64), ``tolerance`` (0, in grid units), ``method`` (``'vw'``
        by default, or ``'dp'``), ``clip`` (True), ``on_collapse`` (``'omit'``
        by default, or ``'raise'``), and ``drop_zm`` (False). An ``MVTSource``
        owns its settings and does not accept per-call options.

    Returns
    -------
    MVTLayer
        Prepared local integer geometry and aligned properties and IDs.
    """
    if (tile is None) == (frame is None):
        raise TypeError('choose exactly one tile or frame')
    if tile is not None and not isinstance(tile, Tile):
        raise TypeError('tile must be a Tile')
    if frame is not None and not isinstance(frame, TileFrame):
        raise TypeError('frame must be a TileFrame')
    if not isinstance(features, (Features, MVTSource)):
        policy = _policy(**options)
        parts = []
        for batch in features:
            if not isinstance(batch, Features):
                raise TypeError('feature batches must contain Features')
            prepared_batch = (
                mvt_prepare(batch, tile=tile, **options)
                if tile is not None
                else mvt_prepare(batch, frame=cast('TileFrame', frame), **options)
            ).features
            if len(prepared_batch):
                parts.append(_compact_features(prepared_batch))
        return MVTLayer._from_decoded(
            _concatenate_features(parts, crs=None, epoch=None), policy.extent
        )
    if isinstance(features, MVTSource):
        if options:
            raise TypeError('an MVTSource owns its preparation settings')
        source = features
        policy = source._policy
        rows = source._data.xyz_features if tile is not None else source._data.features
        destination = CRS(3857) if tile is not None else cast('TileFrame', frame).crs
        destination_epoch = None if tile is not None else cast('TileFrame', frame).epoch
        if (
            rows.geometries.crs != destination
            or rows.geometries.epoch != destination_epoch
        ):
            raise CRSError('source and tile frame must have matching CRS and epoch')
        if policy.clip:
            regions = (
                _tile_block_regions(
                    tile.zoom,
                    tile.x,
                    tile.x,
                    tile.y,
                    tile.y,
                    policy.extent,
                    policy.buffer,
                )
                if tile is not None
                else [
                    _frame_query_bounds(
                        cast('TileFrame', frame), policy.extent, policy.buffer
                    )
                ]
            )
            spatial = source._data.xyz_index if tile is not None else source._data.index
            selected = spatial.candidates(regions, tile is not None)
            rows = rows[selected]
    else:
        source = None
        policy = _policy(**options)
        destination = CRS(3857) if tile is not None else cast('TileFrame', frame).crs
        destination_epoch = None if tile is not None else cast('TileFrame', frame).epoch
        rows = _admit_features(
            features,
            destination,
            destination_epoch,
            drop_zm=policy.drop_zm,
            coverage=False,
            xyz=tile is not None,
        )
    geometries = rows.geometries
    tile_address = None if tile is None else (tile.zoom, tile.x, tile.y)
    bounds = None if frame is None else frame.bounds
    encoded_buffer = policy.buffer.to_bytes(
        (policy.buffer.bit_length() + 7) // 8, 'little'
    )
    prepared, selected = _mvt_prepare_grid(
        geometries,
        bounds,
        tile_address,
        policy.extent,
        encoded_buffer,
        policy.clip,
        policy.on_collapse == 'raise',
        policy.tolerance,
        policy.method == 'dp',
        source is not None and source._data.coverage,
    )
    selected_features = rows[selected].with_geometries(prepared)
    return MVTLayer._from_decoded(selected_features, policy.extent)


@overload
def from_mvt(
    data: Buffer,
    *,
    layer: str,
    properties: Iterable[str] | None = None,
) -> MVTLayer: ...


@overload
def from_mvt(
    data: Buffer,
    *,
    layers: Iterable[str] | None = None,
    properties: Iterable[str] | None = None,
) -> dict[str, MVTLayer]: ...


@overload
def from_mvt(
    data: Buffer,
    *,
    layer: str,
    tile: Tile,
    properties: Iterable[str] | None = None,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> _MVTFeatures: ...


@overload
def from_mvt(
    data: Buffer,
    *,
    layer: str,
    frame: TileFrame,
    properties: Iterable[str] | None = None,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> _MVTFeatures: ...


@overload
def from_mvt(
    data: Buffer,
    *,
    tile: Tile,
    layers: Iterable[str] | None = None,
    properties: Iterable[str] | None = None,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> dict[str, _MVTFeatures]: ...


@overload
def from_mvt(
    data: Buffer,
    *,
    frame: TileFrame,
    layers: Iterable[str] | None = None,
    properties: Iterable[str] | None = None,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> dict[str, _MVTFeatures]: ...


def from_mvt(
    data: Buffer,
    *,
    layer: str | None = None,
    layers: Iterable[str] | None = None,
    properties: Iterable[str] | None = None,
    tile: Tile | None = None,
    frame: TileFrame | None = None,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> MVTLayer | dict[str, MVTLayer] | _MVTFeatures | dict[str, _MVTFeatures]:
    """Decode raw MVT bytes into local or located feature layers.

    Parameters
    ----------
    data : buffer
        Uncompressed MVT bytes.
    layer : str or None
        Select one named layer and return it directly.
    layers : iterable of str or None
        Select named layers, returning a mapping. Mutually exclusive with
        ``layer``; if both are omitted, all layers are returned.
    properties : iterable of str or None, default None
        Property names to retain, or ``None`` for all properties.
    tile : Tile or None
        Locate geometries in an XYZ tile. Mutually exclusive with ``frame``.
    frame : TileFrame or None
        Locate geometries in a custom frame.
    crs : CrsInput or None
        Optional output coordinate reference system for located geometries.
    epoch : float or None
        Optional output coordinate epoch for located geometries.

    Returns
    -------
    MVTLayer, Features or dict
        Local layers by default, or located features when a tile or frame is
        supplied. A single ``layer`` returns that value rather than a mapping.

    Raises
    ------
    CRSError
        If located geometries cannot be transformed to the requested frame.
    GeometryError
        If decoded geometry is invalid.
    ParseError
        If the vector tile is malformed.
    """
    if layer is not None and layers is not None:
        raise TypeError('layer and layers are mutually exclusive')
    if tile is not None and frame is not None:
        raise TypeError('tile and frame are mutually exclusive')
    if tile is None and frame is None and (crs is not None or epoch is not None):
        raise TypeError('crs and epoch require a tile or frame')
    if layer is not None and not isinstance(layer, str):
        raise TypeError('layer must be a string')
    selected = [layer] if layer is not None else _names(layers, 'layers')
    fields = _names(properties, 'properties')
    decoded = _mvt_decode_local(data, selected, fields)
    result = {
        name: MVTLayer._from_decoded(features, extent)
        for name, (features, extent) in decoded.items()
    }
    if tile is not None or frame is not None:
        located = (
            {
                name: value.to_features(tile=tile, crs=crs, epoch=epoch)
                for name, value in result.items()
            }
            if tile is not None
            else {
                name: value.to_features(
                    frame=cast('TileFrame', frame), crs=crs, epoch=epoch
                )
                for name, value in result.items()
            }
        )
        return located[layer] if layer is not None else located
    return result[layer] if layer is not None else result


@overload
def to_mvt(
    values: _Ordinary, *, layer: str, tile: Tile, **options: Unpack[_PrepareOptions]
) -> bytes: ...


@overload
def to_mvt(
    values: _Ordinary,
    *,
    layer: str,
    frame: TileFrame,
    **options: Unpack[_PrepareOptions],
) -> bytes: ...


@overload
def to_mvt(
    values: Mapping[str, _Ordinary], *, tile: Tile, **options: Unpack[_PrepareOptions]
) -> bytes: ...


@overload
def to_mvt(
    values: Mapping[str, _Ordinary],
    *,
    frame: TileFrame,
    **options: Unpack[_PrepareOptions],
) -> bytes: ...


@overload
def to_mvt(values: MVTSource, *, layer: str, tile: Tile) -> bytes: ...


@overload
def to_mvt(values: MVTSource, *, layer: str, frame: TileFrame) -> bytes: ...


@overload
def to_mvt(values: Mapping[str, _Ordinary | MVTSource], *, tile: Tile) -> bytes: ...


@overload
def to_mvt(
    values: Mapping[str, _Ordinary | MVTSource], *, frame: TileFrame
) -> bytes: ...


@overload
def to_mvt(values: MVTLayer, *, layer: str) -> bytes: ...


@overload
def to_mvt(values: Mapping[str, MVTLayer]) -> bytes: ...


def to_mvt(
    values: MVTLayer
    | Features
    | MVTSource
    | Iterable[Features]
    | Mapping[str, MVTLayer | Features | MVTSource | Iterable[Features]],
    *,
    layer: str | None = None,
    tile: Tile | None = None,
    frame: TileFrame | None = None,
    **options: Any,
) -> bytes:
    """Prepare and encode feature data as an uncompressed vector tile.

    Parameters
    ----------
    values : MVTLayer, Features, MVTSource, iterable or mapping
        A single layer or a mapping of names to layer inputs. Ordinary
        features are prepared in the supplied tile or frame; local layers
        are encoded directly.
    layer : str
        Name of a single layer. Omit for a mapping of layers.
    tile : Tile
        XYZ address used to prepare ordinary or source features.
    frame : TileFrame
        Custom tile frame, mutually exclusive with ``tile``.
    **options
        Preparation settings for ordinary features; see ``mvt_prepare``.
        Local layers and indexed sources do not accept per-call settings.

    Returns
    -------
    bytes
        Uncompressed MVT data.
    """
    layers: Mapping[str, MVTLayer | Features | MVTSource | Iterable[Features]]
    if not isinstance(values, Mapping):
        if not isinstance(layer, str):
            raise TypeError('a single layer requires layer= as its name')
        layers = {layer: values}
    else:
        if layer is not None:
            raise TypeError('layer= is only valid for a single MVT layer')
        layers = cast(
            'Mapping[str, MVTLayer | Features | MVTSource | Iterable[Features]]',
            values,
        )
    if options and any(isinstance(value, MVTSource) for value in layers.values()):
        raise TypeError('source layers do not accept common preparation settings')
    parts = []
    seen = set()
    for name, value in layers.items():
        if not isinstance(name, str):
            raise TypeError('layers must map string names to feature inputs')
        if name in seen:
            raise ValueError(f'duplicate layer name {name!r}')
        seen.add(name)
        if isinstance(value, MVTLayer):
            if tile is not None or frame is not None or options:
                raise TypeError('local MVT layers do not accept preparation settings')
        elif isinstance(value, MVTSource):
            if tile is not None and frame is None:
                value = mvt_prepare(value, tile=tile)
            elif frame is not None and tile is None:
                value = mvt_prepare(value, frame=frame)
            else:
                raise TypeError('choose exactly one tile or frame')
        elif tile is not None and frame is None:
            value = mvt_prepare(value, tile=tile, **options)
        elif frame is not None and tile is None:
            value = mvt_prepare(value, frame=frame, **options)
        else:
            raise TypeError('choose exactly one tile or frame')
        features = value.features
        parts.append(
            _mvt_encode_layer(
                name,
                features.geometries,
                features._properties,
                features.ids,
                value.extent,
            )
        )
    return b''.join(parts)


def _tile_block_regions(
    zoom: int,
    xmin: int,
    xmax: int,
    ymin: int,
    ymax: int,
    extent: int,
    buffer: int,
) -> list[tuple[float, float, float, float]]:
    world = Fraction(math.pi * 6_378_137)
    period = 2 * world
    span = (1 << zoom) * extent
    margin = buffer + 1
    left = -world + period * (xmin * extent - margin) / span
    right = -world + period * ((xmax + 1) * extent + margin) / span
    top = world - period * (ymin * extent - margin) / span
    bottom = world - period * ((ymax + 1) * extent + margin) / span
    if right - left >= period:
        intervals = [(-world, world)]
    else:
        start = left - ((left + world) // period) * period
        end = start + right - left
        intervals = (
            [(start, end)] if end <= world else [(start, world), (-world, end - period)]
        )
    return [
        (
            _outward(start, upper=False),
            _outward(bottom, upper=False),
            _outward(end, upper=True),
            _outward(top, upper=True),
        )
        for start, end in intervals
    ]


@overload
def mvt_tiles(
    sources: Mapping[str, MVTSource], *, tiles: Iterable[Tile]
) -> Generator[tuple[Tile, bytes], None, None]: ...


@overload
def mvt_tiles(
    sources: Mapping[str, MVTSource],
    *,
    min_zoom: SupportsIndex,
    max_zoom: SupportsIndex,
) -> Generator[tuple[Tile, bytes], None, None]: ...


def mvt_tiles(
    sources: Mapping[str, MVTSource],
    *,
    tiles: Iterable[Tile] | None = None,
    min_zoom: SupportsIndex | None = None,
    max_zoom: SupportsIndex | None = None,
) -> Generator[tuple[Tile, bytes], None, None]:
    """Generate addressed tiles lazily, or traverse nonempty tiles in zoom order.

    Parameters
    ----------
    sources : mapping of str to MVTSource
        Named layers indexed in EPSG:3857 without an epoch.
    tiles : iterable of Tile
        Explicit addresses to generate in input order, including empty tiles.
        Do not combine with a zoom range.
    min_zoom : int
        First zoom level for automatic traversal.
    max_zoom : int
        Last zoom level for automatic traversal. Both zoom bounds are
        inclusive and must lie between 0 and 29. Automatic traversal requires
        clipped sources and omits empty tiles.

    Returns
    -------
    generator of (Tile, bytes)
        Tile addresses and their uncompressed MVT data.
    """
    if not isinstance(sources, Mapping):
        raise TypeError('sources must map layer names to MVTSource objects')
    layers = tuple(sources.items())
    if any(
        not isinstance(name, str) or not isinstance(source, MVTSource)
        for name, source in layers
    ):
        raise TypeError('sources must map layer names to MVTSource objects')
    for _, source in layers:
        geometries = source._data.features.geometries
        if geometries.crs != CRS(3857) or geometries.epoch is not None:
            raise CRSError(
                'XYZ tile generation requires a source in EPSG:3857 without an epoch'
            )
    if tiles is not None:
        if min_zoom is not None or max_zoom is not None:
            raise TypeError('tiles cannot be combined with a zoom range')
        iterator = iter(tiles)
        named_layers = dict(layers)

        def explicit() -> Generator[tuple[Tile, bytes], None, None]:
            for tile in iterator:
                if not isinstance(tile, Tile):
                    raise TypeError('tiles must contain Tile objects')
                yield tile, to_mvt(named_layers, tile=tile)

        return explicit()
    if min_zoom is None or max_zoom is None:
        raise TypeError('supply tiles or both min_zoom and max_zoom')
    if isinstance(min_zoom, (bool, np.bool_)) or isinstance(max_zoom, (bool, np.bool_)):
        raise TypeError('zoom must be an integer from 0 to 29')
    low, high = index(min_zoom), index(max_zoom)
    if not 0 <= low <= high <= 29:
        raise ValueError('zoom range must be within 0 through 29')
    if any(not source._policy.clip for _, source in layers):
        raise ValueError('automatic tile traversal requires clip=True')

    def automatic() -> Generator[tuple[Tile, bytes], None, None]:
        def visit(
            zoom: int, xmin: int, xmax: int, ymin: int, ymax: int
        ) -> Generator[tuple[Tile, bytes], None, None]:
            possible = False
            for _, source in layers:
                policy = source._policy
                regions = _tile_block_regions(
                    zoom, xmin, xmax, ymin, ymax, policy.extent, policy.buffer
                )
                if source._data.xyz_index.candidates(regions, True):
                    possible = True
                    break
            if not possible:
                return
            if xmin != xmax:
                middle = (xmin + xmax) // 2
                yield from visit(zoom, xmin, middle, ymin, ymax)
                yield from visit(zoom, middle + 1, xmax, ymin, ymax)
            elif ymin != ymax:
                middle = (ymin + ymax) // 2
                yield from visit(zoom, xmin, xmax, ymin, middle)
                yield from visit(zoom, xmin, xmax, middle + 1, ymax)
            else:
                tile = Tile(x=xmin, y=ymin, zoom=zoom)
                prepared = {
                    name: mvt_prepare(source, tile=tile) for name, source in layers
                }
                if any(len(layer.features) for layer in prepared.values()):
                    yield tile, to_mvt(prepared)

        for zoom in range(low, high + 1):
            yield from visit(zoom, 0, (1 << zoom) - 1, 0, (1 << zoom) - 1)

    return automatic()
