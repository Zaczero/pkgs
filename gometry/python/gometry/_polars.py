"""Polars binary-Series conversion backed by batched (E)WKB."""

from __future__ import annotations

from typing import Any

from gometry._lib import (
    CRSError,
    GeometryArray,
    GeometryError,
    from_wkb,
)
from gometry._optional import missing_optional_dependency
from gometry._types import CrsInput  # noqa: TC001 - required by annotations

TYPE_CHECKING = False

_POLARS_INSTALL = "install the 'gometry[polars]' extra"


def _polars() -> Any:
    try:
        import polars as pl
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error, 'polars', f'polars interop requires polars; {_POLARS_INSTALL}'
        ) from error
    return pl


if TYPE_CHECKING:
    import polars as pl

    _pl = pl
else:
    _pl = _polars()
    pl = _pl


def to_polars(
    values: GeometryArray,
    *,
    name: str = 'geometry',
    drop_crs: bool = False,
    drop_epoch: bool = False,
) -> pl.Series:
    """Encode a ``GeometryArray`` as a polars binary (EWKB) Series.

    Parameters
    ----------
    values : GeometryArray
        Geometries to encode.
    name : str, default 'geometry'
        Series name.
    drop_crs : bool, default False
        Allow encoding without CRS metadata when the CRS cannot be embedded as
        an EWKB SRID. EPSG codes and the PostGIS wire aliases ``OGC:CRS84`` /
        ``OGC:CRS84h`` are always embedded when present (identity loss for the
        OGC spellings is documented under EWKB). Other CRSs require this
        acknowledgement because a Polars Series has no metadata channel for
        their full definition. Use ``to_wkb()`` for deliberately plain WKB.
    drop_epoch : bool, default False
        Allow encoding an epoch-bearing array even though WKB/EWKB cannot
        represent coordinate epoch metadata. When false, epoch-bearing arrays
        raise instead of silently losing frame semantics.

    Returns
    -------
    polars.Series
        Binary WKB/EWKB storage.

    Raises
    ------
    GeometryError
        If CRS or coordinate epoch metadata would be lost without the
        corresponding acknowledgement.
    """
    if values.epoch is not None and not drop_epoch:
        raise GeometryError(
            'to_polars cannot encode coordinate epoch metadata in WKB/EWKB; '
            'pass drop_epoch=True to acknowledge the loss'
        )
    # Let the native WKB writer decide encodability (EPSG codes + the exact
    # PostGIS wire aliases OGC:CRS84→4326 / OGC:CRS84h→4979). Do not duplicate
    # an authority=='EPSG' gate here — that blocked CRS84 despite a valid EWKB
    # alias. When the writer can embed an SRID it always does (same as prior
    # EPSG behaviour, even with drop_crs=True); drop_crs only acknowledges
    # loss for CRS values the writer cannot encode.
    if values.crs is None:
        storage = values.to_wkb(include_srid=False, drop_epoch=drop_epoch)
    else:
        try:
            storage = values.to_wkb(include_srid=True, drop_epoch=drop_epoch)
        except CRSError as exc:
            if not drop_crs:
                raise GeometryError(
                    'to_polars cannot encode this CRS in WKB/EWKB; '
                    'pass drop_crs=True to acknowledge the loss'
                ) from exc
            storage = values.to_wkb(include_srid=False, drop_epoch=drop_epoch)
    return _pl.Series(name, storage, dtype=_pl.Binary)


def from_polars(
    series: pl.Series,
    *,
    crs: CrsInput | None = None,
    epoch: float | None = None,
) -> GeometryArray:
    """Decode a WKB/EWKB polars Series into a ``GeometryArray``.

    Parameters
    ----------
    series : polars.Series
        Binary WKB/EWKB storage (see `to_polars`).
    crs : str, int, or CRS input, optional
        Restore CRS metadata when the binary values do not embed an EPSG SRID.
        If EWKB does carry an SRID, this value must agree with it.
    epoch : float, optional
        Restore coordinate epoch metadata. Requires a CRS.

    Returns
    -------
    GeometryArray
        Decoded geometries with the embedded or explicitly supplied frame.

    Raises
    ------
    CRSError
        If crs is not a valid CRS identifier or disagrees with an embedded
        EWKB SRID.
    GeometryError
        If epoch is supplied without a CRS.
    """
    result = from_wkb(series, crs=crs, epoch=epoch)
    if not isinstance(result, GeometryArray):
        raise TypeError('expected a Polars binary Series, got scalar WKB')
    return result
