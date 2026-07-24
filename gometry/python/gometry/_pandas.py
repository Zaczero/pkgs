"""pandas extension storage and conversion — thin glue over GeometryArray.

Missing-data model (glue-level, geopandas-compatible): ``None``/``pd.NA`` is
*missing*, an empty geometry is a *value*. Missing rows are gometry-native
``GeometryArray`` state, so pandas stores and round-trips the same mask the
Rust core uses while preserving pandas' nullable scalar conventions.
"""

from __future__ import annotations

import types  # noqa: TC003 - required by runtime get_type_hints
from collections.abc import (  # noqa: TC003 - required by runtime get_type_hints
    Callable,
    Hashable,
    Sequence,
)
from typing import Any, TypeAlias, cast

import numpy as np
import numpy.typing as npt

from gometry._lib import Geometry, GeometryArray
from gometry._optional import missing_optional_dependency

TYPE_CHECKING = False

_PANDAS_INSTALL = "install the 'gometry[pandas]' extra"
_ARROW_INSTALL = "install the 'gometry[arrow]' extra"

BoolArray: TypeAlias = npt.NDArray[np.bool_]
IndexArray: TypeAlias = npt.NDArray[np.integer[Any]]
ObjectArray: TypeAlias = npt.NDArray[np.object_]


def _pandas() -> Any:
    try:
        import pandas as pd
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error, 'pandas', f'pandas interop requires pandas; {_PANDAS_INSTALL}'
        ) from error
    return pd


if TYPE_CHECKING:
    import pandas as pd

    # pandas' public stubs model several extension-protocol members more
    # narrowly than pandas itself accepts. Keep the implementation bases
    # dynamic while retaining exact public Series/Index annotations.
    _pd: Any = pd
else:
    _pd = _pandas()
    pd = _pd

_ExtensionDtype = _pd.api.extensions.ExtensionDtype
_ExtensionArray = _pd.api.extensions.ExtensionArray


class GeometryDtype(_ExtensionDtype):
    """pandas extension dtype for gometry geometries."""

    name = 'gometry.geometry'
    type = Geometry
    na_value = None

    @classmethod
    def construct_from_string(cls, string: str) -> GeometryDtype:
        if not isinstance(string, str):
            raise TypeError(
                f"'construct_from_string' expects a string, got {type(string)}"
            )
        if string == cls.name:
            return cls()
        raise TypeError(f"Cannot construct a '{cls.__name__}' from '{string}'")

    @classmethod
    def construct_array_type(cls) -> type[GeometryExtensionArray]:
        return GeometryExtensionArray

    def __from_arrow__(self, array: object) -> GeometryExtensionArray:
        from gometry import from_arrow

        return GeometryExtensionArray(from_arrow(array))

    def __repr__(self) -> str:
        return 'GeometryDtype()'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return other == self.name
        return isinstance(other, GeometryDtype)

    def __hash__(self) -> int:
        return hash(self.name)


def _is_missing_scalar(value: object) -> bool:
    """None / pd.NA / float NaN — the scalars pandas treats as missing."""
    if value is None or value is _pd.NA:
        return True
    return isinstance(value, float) and np.isnan(value)


def _unpickle_geometry_extension_array(
    state: list[GeometryArray],
) -> GeometryExtensionArray:
    array = GeometryExtensionArray.__new__(GeometryExtensionArray)
    array._state = state
    return array


class GeometryExtensionArray(_ExtensionArray):
    """pandas ExtensionArray backed by a gometry ``GeometryArray``.

    Missing rows are first-class in the core array itself (``is_missing`` /
    ``None`` on access); this wrapper only adds pandas' mutable-container
    contract on top.
    """

    __hash__ = None  # mutable container
    _dtype = GeometryDtype()

    def __init__(
        self,
        values: GeometryArray,
        mask: BoolArray | Sequence[bool] | None = None,
    ) -> None:
        if mask is not None:
            mask = np.asarray(mask, dtype=np.bool_)
            if len(mask) != len(values):
                raise ValueError('mask length must match values length')
            mask = np.logical_or(mask, values.is_missing)
            values = values._with_missing(mask)
        # one shared, mutable cell so ``view()`` twins alias the same data
        self._state = [values]

    @property
    def _geoms(self) -> GeometryArray:
        return self._state[0]

    @_geoms.setter
    def _geoms(self, values: GeometryArray) -> None:
        self._state[0] = values

    @property
    def _mask(self) -> BoolArray | None:
        mask = self._geoms.is_missing
        return mask if mask.any() else None

    def view(self, dtype: object | None = None) -> GeometryExtensionArray:
        if dtype is not None:
            raise NotImplementedError(
                'GeometryExtensionArray.view supports only dtype=None'
            )
        twin = type(self).__new__(type(self))
        twin._state = self._state
        twin._readonly = self._readonly
        return twin

    def __reduce__(self):
        return (_unpickle_geometry_extension_array, (self._state,))

    @property
    def geometry_array(self) -> GeometryArray:
        return self._geoms

    @property
    def missing_mask(self) -> BoolArray | None:
        """The validity mask (``True`` = missing), or ``None`` when complete."""
        return self._mask

    @property
    def dtype(self) -> GeometryDtype:
        return self._dtype

    def __len__(self) -> int:
        return len(self._geoms)

    def __getitem__(
        self,
        item: int | slice | BoolArray | IndexArray | Sequence[int],
    ) -> Geometry | None | GeometryExtensionArray:
        from pandas.api.indexers import check_array_indexer

        if isinstance(item, tuple):
            # numpy-style (..., :) forms from block slicing
            parts = [part for part in item if part is not Ellipsis]
            if len(parts) > 1:
                raise IndexError('too many indices for GeometryExtensionArray')
            item = parts[0] if parts else slice(None)
        if isinstance(item, str) or (isinstance(item, float) and not np.isnan(item)):
            raise IndexError(
                'only integers, slices (`:`), ellipsis (`...`), numpy.newaxis '
                '(`None`) and integer or boolean arrays are valid indices'
            )
        if isinstance(item, (int, np.integer)) and not isinstance(item, bool):
            index = int(item)
            if index < 0:
                index += len(self)
            if not 0 <= index < len(self):
                raise IndexError('index out of bounds')
            return self._geoms[index]
        if item is Ellipsis:
            item = slice(None)
        if isinstance(item, slice):
            if item == slice(None) or item == slice(None, None, 1):
                # `self[:]` is pandas' canonical view: share the state cell
                return self.view()
            result = type(self)(self._geoms[item])
            result._readonly = self._readonly
            return result
        key = check_array_indexer(self, item)
        if isinstance(key, slice):
            result = type(self)(self._geoms[key])
            result._readonly = self._readonly
            return result
        key_arr = np.asarray(key)
        if key_arr.ndim != 1:
            raise IndexError(
                'only integers, slices (`:`), ellipsis (`...`), numpy.newaxis '
                '(`None`) and integer or boolean arrays are valid indices'
            )
        if key_arr.dtype == np.bool_:
            key_arr = np.flatnonzero(key_arr)
        # Fancy selection always materializes a copy; do not propagate
        # readonly (only real views/slices share the parent flag).
        return type(self)(self._geoms[key_arr])

    def __setitem__(self, key: object, value: object) -> None:
        from pandas.api.indexers import check_array_indexer

        if self._readonly:
            raise ValueError('Cannot modify read-only array')
        size = len(self)
        if isinstance(key, (int, np.integer)) and not isinstance(key, bool):
            idx = int(key)
            if not -size <= idx < size:
                raise IndexError(
                    f'index {idx} is out of bounds for axis 0 with size {size}'
                )
            positions = np.array([idx + size if idx < 0 else idx], dtype=np.intp)
        else:
            key = check_array_indexer(self, key)
            if isinstance(key, slice):
                positions = np.arange(size, dtype=np.intp)[key]
            else:
                key_arr = np.asarray(key)
                if key_arr.dtype == np.bool_:
                    positions = np.flatnonzero(key_arr)
                else:
                    # Bounds-check + normalize negatives (never modulo-wrap).
                    positions = key_arr.astype(np.intp)
                    out_of_bounds = (positions < -size) | (positions >= size)
                    if out_of_bounds.any():
                        bad = int(positions[out_of_bounds][0])
                        raise IndexError(
                            f'index {bad} is out of bounds for axis 0 with size {size}'
                        )
                    positions = np.where(positions < 0, positions + size, positions)
        if len(positions) == 0:
            return
        if isinstance(value, Geometry) or _is_missing_scalar(value):
            fill_values: list[object] = [value] * len(positions)
        else:
            # m08: never ``list(value)`` — CPython pre-sizes from ``__len__``
            # and a lying ``sys.maxsize`` raises MemoryError before iteration.
            # Bound growth by the known selection size (plus one for mismatch).
            expected = len(positions)
            fill_values = []
            for item in value:  # type: ignore[attr-defined]
                fill_values.append(item)
                if len(fill_values) > expected:
                    break
            if len(fill_values) != expected:
                raise ValueError(
                    f'cannot set {expected} positions from '
                    f'{len(fill_values)} values'
                )
        geoms: list[Geometry | None] = list(self._geoms)
        for position, item in zip(positions, fill_values, strict=True):
            if _is_missing_scalar(item):
                geoms[position] = None
            elif isinstance(item, Geometry):
                geoms[position] = item
            else:
                raise TypeError(
                    f'expected Geometry or missing, got {type(item).__name__}'
                )
        # Rebuild in the source frame even when every row becomes missing.
        self._geoms = GeometryArray(
            geoms,
            crs=self._geoms.crs,
            epoch=self._geoms.epoch,
        )

    def _compare(self, other: object, *, invert: bool) -> BoolArray | None:
        """Elementwise value identity (``gometry.equals_identical`` — the
        vectorized scalar ``==``); missing rows compare unequal.
        """
        from gometry import equals_identical

        if isinstance(other, GeometryExtensionArray):
            other_values: object = other._geoms
        elif isinstance(other, (GeometryArray, Geometry)):
            other_values = other
        elif _is_missing_scalar(other):
            result = np.zeros(len(self), dtype=np.bool_)
            return ~result if invert else result
        else:
            return None
        # Missing rows compare False in the core (predicate null semantics).
        result = np.array(
            equals_identical(self._geoms, other_values), dtype=np.bool_, copy=True
        )
        return ~result if invert else result

    def __eq__(self, other: object) -> BoolArray | types.NotImplementedType:
        result = self._compare(other, invert=False)
        # NotImplemented defers to the other operand (standard dunder protocol)
        return NotImplemented if result is None else result

    def __ne__(self, other: object) -> BoolArray | types.NotImplementedType:
        result = self._compare(other, invert=True)
        return NotImplemented if result is None else result

    def __contains__(self, item: object) -> bool:
        if _is_missing_scalar(item):
            # only the dtype's own na_value counts (pandas GH-37867)
            return item is None and self._mask is not None
        if isinstance(item, Geometry):
            matches = self._compare(item, invert=False)
            return matches is not None and bool(matches.any())
        return False

    def _values_for_argsort(self) -> ObjectArray:
        """WKB bytes: a deterministic (not spatial) order so pandas internals
        that sort — groupby keys, ``value_counts``, ``unstack`` — work on an
        otherwise unordered dtype. Scalar geometries still refuse ``<``.
        """
        rows = self._geoms.to_wkb(drop_epoch=True)
        return np.asarray([b'' if row is None else row for row in rows], dtype=object)

    def isna(self) -> BoolArray:
        # writable copy: pandas mutates this in place (ffill limit_area)
        return np.array(self._geoms.is_missing, dtype=np.bool_)

    def take(
        self,
        indices: Sequence[int],
        *,
        allow_fill: bool = False,
        fill_value: object = None,
    ) -> GeometryExtensionArray:
        indices_arr = np.asarray(indices, dtype=np.intp)
        if not allow_fill:
            length = len(self)
            if length == 0 and len(indices_arr):
                raise IndexError('cannot do a non-empty take from an empty axes')
            indices_arr = np.where(indices_arr < 0, indices_arr + length, indices_arr)
            if len(indices_arr) and (
                (indices_arr < 0).any() or (indices_arr >= length).any()
            ):
                raise IndexError('take index out of bounds')
            return type(self)(self._geoms[indices_arr])

        if (indices_arr < -1).any():
            raise ValueError('take with allow_fill accepts only -1 as a fill index')
        fill_positions = indices_arr == -1
        real = indices_arr[~fill_positions]
        if len(real) and len(self) == 0:
            raise IndexError('cannot do a non-empty take from an empty axes')
        if len(real) and (real >= len(self)).any():
            raise IndexError('take index out of bounds')

        fill_missing = _is_missing_scalar(fill_value)
        if not fill_missing and not isinstance(fill_value, Geometry):
            raise TypeError(
                f'fill_value must be a Geometry or missing, got '
                f'{type(fill_value).__name__}'
            )

        safe_indices = np.where(fill_positions, 0, indices_arr)
        if len(self) == 0:
            rows: list[Geometry | None] = [None] * len(indices_arr)
        else:
            # A pandas fill slot is logically absent, not a selected physical
            # row with an additional mask.  Rebuild through the canonical
            # nullable constructor so packed layout and pickle/Arrow format
            # depend only on the logical values, never on take history.
            rows = list(self._geoms[safe_indices])
        fill: Geometry | None = None if fill_missing else cast('Geometry', fill_value)
        for position in np.flatnonzero(fill_positions):
            rows[position] = fill
        crs = self._geoms.crs
        epoch = self._geoms.epoch
        rebuilt = GeometryArray(rows, crs=crs, epoch=epoch)
        return type(self)(rebuilt)

    def _values_for_factorize(self) -> tuple[ObjectArray, object]:
        return np.asarray(self._geoms.to_wkb(drop_epoch=True), dtype=object), None

    def copy(self) -> GeometryExtensionArray:
        return type(self)(self._geoms)

    @classmethod
    def _from_sequence(
        cls,
        scalars: Sequence[object],
        *,
        dtype: object | None = None,
        copy: bool = False,
    ) -> GeometryExtensionArray:
        del dtype, copy
        geometries: list[Geometry | None] = []
        for index, item in enumerate(scalars):
            if _is_missing_scalar(item):
                geometries.append(None)
            elif isinstance(item, Geometry):
                geometries.append(item)
            else:
                raise TypeError(
                    f'expected Geometry or missing at position {index}, '
                    f'got {type(item).__name__}'
                )
        # The core constructor accepts None rows (first-class missing).
        return cls(GeometryArray(geometries))

    @classmethod
    def _from_factorized(
        cls,
        values: ObjectArray,
        original: object,
    ) -> GeometryExtensionArray:
        from gometry import from_wkb

        crs = None
        epoch = None
        if isinstance(original, GeometryExtensionArray):
            crs = original._geoms.crs
            epoch = original._geoms.epoch
        # ``ndarray`` implements the buffer protocol as well as iteration, so
        # passing it directly is ambiguous to static overload resolution. The
        # factorized payload is row-wise WKB; make that batch shape explicit.
        encoded: list[bytes | None] = values.tolist()
        return cls(from_wkb(encoded, crs=crs, epoch=epoch))

    @classmethod
    def _concat_same_type(
        cls,
        to_concat: Sequence[GeometryExtensionArray],
    ) -> GeometryExtensionArray:
        blocks = list(to_concat)
        if not blocks:
            return cls(GeometryArray([]))
        # Core concat carries the masks (dense sides contribute all-present).
        merged = blocks[0]._geoms.concat(*(block._geoms for block in blocks[1:]))
        return cls(merged)

    @property
    def nbytes(self) -> int:
        return self._geoms.nbytes

    def _formatter(self, boxed: bool = False) -> Callable[[object], str]:
        del boxed
        return lambda value: (
            str(value) if isinstance(value, Geometry) else repr(value)
        )

    def __arrow_array__(self, type: object | None = None) -> object:
        try:
            import pyarrow as pa
        except ModuleNotFoundError as error:
            raise missing_optional_dependency(
                error, 'pyarrow', f'Arrow export requires pyarrow; {_ARROW_INSTALL}'
            ) from error

        array = self._geoms.to_arrow()
        if type is None:
            return array
        try:
            return array.cast(type)
        except (TypeError, ValueError, pa.ArrowException) as error:
            raise TypeError(
                f'cannot cast gometry geometry Arrow array to {type!r}'
            ) from error


def to_pandas(
    values: GeometryArray,
    *,
    index: pd.Index | Sequence[Hashable] | None = None,
    name: Hashable | None = None,
    mask: BoolArray | Sequence[bool] | None = None,
) -> pd.Series:
    """Build a pandas Series with ``GeometryDtype`` from a ``GeometryArray``.

    Parameters
    ----------
    values : GeometryArray
        Geometries to wrap (zero-copy; the Series shares the array).
    index, name : optional
        Forwarded to the Series constructor.
    mask : ndarray of bool, optional
        Missing-row mask (``True`` = missing); rows so marked read back as
        ``None``.

    Returns
    -------
    pandas.Series
        A ``gometry.geometry``-dtyped Series.
    """
    array = GeometryExtensionArray(values)
    if mask is not None:
        array = GeometryExtensionArray(array.geometry_array, np.asarray(mask))
    return _pd.Series(
        array,
        index=index,
        name=name,
        dtype=GeometryDtype(),
    )


def from_pandas(series: pd.Series) -> GeometryArray:
    """Extract the backing ``GeometryArray`` from a geometry Series.

    Zero-copy: the Series and the returned array share storage. Missing rows
    are first-class — they come back as missing rows of the ``GeometryArray``
    (``arr.is_missing`` / ``None`` on access).

    Parameters
    ----------
    series : pandas.Series
        A ``gometry.geometry``-dtyped Series (see `to_pandas`).

    Returns
    -------
    GeometryArray

    Raises
    ------
    TypeError
        If the Series is not backed by gometry's extension array.
    """
    data = series.array
    if not isinstance(data, GeometryExtensionArray):
        raise TypeError(
            'expected a Series backed by gometry GeometryExtensionArray, '
            f'got {type(data).__name__}'
        )
    return data.geometry_array
