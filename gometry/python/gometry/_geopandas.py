"""geopandas round-trip helpers — thin glue over WKB + CRS."""

from __future__ import annotations

from types import SimpleNamespace

from gometry._lib import (
    GeometryArray,
    GeometryError,
    from_wkb,
)
from gometry._optional import missing_optional_dependency

TYPE_CHECKING = False

if TYPE_CHECKING:
    import geopandas as gpd
else:
    try:
        import geopandas as gpd
    except ModuleNotFoundError as error:
        if error.name != 'geopandas' and not (error.name or '').startswith(
            'geopandas.'
        ):
            raise
        gpd = SimpleNamespace(GeoDataFrame=object, GeoSeries=object)

_GEOPANDAS_INSTALL = "install the 'gometry[geopandas]' extra"


def _crs_to_geopandas(crs: object | None) -> str | None:
    if crs is None:
        return None
    return str(crs)


def from_geopandas(
    geoseries_or_gdf: gpd.GeoSeries | gpd.GeoDataFrame,
) -> GeometryArray:
    """Convert a geopandas ``GeoSeries`` or geometry column to a ``GeometryArray``.

    Parameters
    ----------
    geoseries_or_gdf : GeoSeries or GeoDataFrame
        geopandas geometry container.

    Returns
    -------
    GeometryArray
        Decoded geometries with CRS taken from the geopandas frame metadata.

    Raises
    ------
    ModuleNotFoundError
        If geopandas is not installed.
    TypeError
        If the input is not a ``GeoSeries`` or ``GeoDataFrame``.
    """
    try:
        import geopandas as gpd
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error,
            'geopandas',
            f'geopandas interop requires geopandas; {_GEOPANDAS_INSTALL}',
        ) from error

    if isinstance(geoseries_or_gdf, gpd.GeoDataFrame):
        series = geoseries_or_gdf.geometry
    elif isinstance(geoseries_or_gdf, gpd.GeoSeries):
        series = geoseries_or_gdf
    else:
        raise TypeError(
            'expected geopandas.GeoSeries or geopandas.GeoDataFrame, '
            f'got {type(geoseries_or_gdf).__name__}; use from_pandas for a '
            'pandas Series'
        )

    # Vectorized WKB export in one shapely C call beats a per-element
    # ``geom.wkb`` Python loop by ~11x on a 10k shapely-backed GeoSeries.
    wkb_values = series.to_wkb()
    crs = _crs_to_geopandas(series.crs)
    return from_wkb(wkb_values, crs=crs)


def to_geopandas(
    geometry_array: GeometryArray,
    *,
    drop_epoch: bool = False,
) -> gpd.GeoSeries:
    """Convert a ``GeometryArray`` to a geopandas ``GeoSeries``.

    Parameters
    ----------
    geometry_array : GeometryArray
        Source geometries.
    drop_epoch : bool, default False
        Allow encoding an epoch-bearing array even though Shapely/geopandas
        WKB storage cannot represent coordinate epoch metadata. When false,
        epoch-bearing arrays raise instead of silently losing frame semantics.

    Returns
    -------
    GeoSeries
        Shapely geometries with CRS taken from the array metadata.

    Raises
    ------
    ModuleNotFoundError
        If geopandas is not installed.
    GeometryError
        If the array carries a coordinate epoch and ``drop_epoch`` is false.
    """
    try:
        import geopandas as gpd
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error,
            'geopandas',
            f'geopandas interop requires geopandas; {_GEOPANDAS_INSTALL}',
        ) from error
    from shapely import from_wkb

    if geometry_array.epoch is not None and not drop_epoch:
        raise GeometryError(
            'to_geopandas cannot encode coordinate epoch metadata in WKB; '
            'pass drop_epoch=True to acknowledge the loss'
        )
    # One vectorized shapely decode of the batched WKB beats a per-blob loop.
    geoms = from_wkb(geometry_array.to_wkb(drop_epoch=drop_epoch))
    return gpd.GeoSeries(geoms, crs=_crs_to_geopandas(geometry_array.crs))
