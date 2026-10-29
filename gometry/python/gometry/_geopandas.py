"""geopandas round-trip helpers — thin glue over WKB + CRS."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

from gometry._lib import (
    Geometry,
    GeometryArray,
    GeometryError,
    from_wkb,
)
from gometry._optional import missing_optional_dependency

if TYPE_CHECKING:
    from types import EllipsisType

    import geopandas as gpd

    from gometry._types import Features

_GEOPANDAS_INSTALL = "install the 'gometry[geopandas]' extra"


def _crs_to_geopandas(crs: object | None) -> str | None:
    if crs is None:
        return None
    return str(crs)


@overload
def from_geopandas(
    geoseries_or_gdf: gpd.GeoSeries,
    *,
    geometry: None = None,
    id_column: EllipsisType | None = ...,
) -> GeometryArray[Geometry | None]: ...


@overload
def from_geopandas(
    geoseries_or_gdf: gpd.GeoDataFrame,
    *,
    geometry: str | None = None,
    id_column: str | EllipsisType | None = ...,
) -> Features: ...


def from_geopandas(
    geoseries_or_gdf: gpd.GeoSeries | gpd.GeoDataFrame,
    *,
    geometry: str | None = None,
    id_column: str | EllipsisType | None = ...,
) -> GeometryArray[Geometry | None] | Features:
    """Convert a GeoSeries to geometries or a GeoDataFrame to aligned features.

    Parameters
    ----------
    geoseries_or_gdf : GeoSeries or GeoDataFrame
        The geometry series or feature table to snapshot.
    geometry : str, optional
        Active geometry column in a GeoDataFrame. Defaults to its active column.
    id_column : str or None, optional
        Feature ID column. When omitted, use the ``gometry:feature_id`` entry
        in the frame's ``attrs``; explicit None disables the ID role.
        DataFrame indexes and columns named ``id`` are never guessed.

    Returns
    -------
    GeometryArray or Features
        Geometry series or aligned feature snapshot, with CRS retained.

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

    if isinstance(geoseries_or_gdf, gpd.GeoSeries):
        if geometry is not None or (id_column is not None and id_column is not ...):
            raise TypeError('geometry and id_column require a GeoDataFrame')
        return _from_geoseries(geoseries_or_gdf)
    if not isinstance(geoseries_or_gdf, gpd.GeoDataFrame):
        raise TypeError(
            'expected geopandas.GeoSeries or geopandas.GeoDataFrame, '
            f'got {type(geoseries_or_gdf).__name__}; use from_pandas for a '
            'pandas Series'
        )

    frame = geoseries_or_gdf
    if id_column is ...:
        id_column = frame.attrs.get('gometry:feature_id')
    names = list(frame.columns)
    if any(not isinstance(name, str) for name in names):
        raise TypeError('GeoDataFrame column names must be strings')
    if len(set(names)) != len(names):
        raise ValueError('GeoDataFrame column names must be unique')
    geometry_name = frame.active_geometry_name if geometry is None else geometry
    if geometry_name is None:
        candidates = [name for name in names if isinstance(frame[name], gpd.GeoSeries)]
        if len(candidates) != 1:
            raise ValueError(
                'select a geometry column when none or several are present'
            )
        geometry_name = candidates[0]
    if not isinstance(geometry_name, str) or geometry_name not in names:
        raise ValueError('geometry must name a geometry column')
    series = frame[geometry_name]
    if not isinstance(series, gpd.GeoSeries):
        raise TypeError(f'column {geometry_name!r} is not a geometry column')
    if id_column is not None and (
        not isinstance(id_column, str)
        or id_column not in names
        or id_column == geometry_name
    ):
        raise ValueError('id_column must name a non-geometry column')

    from gometry._types import (
        Features,
        _snapshot_feature_columns,
        _snapshot_feature_ids,
    )

    geometries = _from_geoseries(series)
    columns: dict[str, Any] = {}
    for name in names:
        if name in (geometry_name, id_column):
            continue
        column = frame[name]
        columns[name] = (
            _from_geoseries(column)
            if isinstance(column, gpd.GeoSeries)
            else _dataframe_values(column)
        )
    ids = None if id_column is None else _dataframe_values(frame[id_column])
    return Features._from_property_rows(
        geometries,
        _snapshot_feature_columns(columns, len(frame)),
        _snapshot_feature_ids(ids, len(frame)),
        id_column,
        None,
        geometry_name,
    )


def _from_geoseries(series: gpd.GeoSeries) -> GeometryArray[Geometry | None]:
    # Vectorized WKB export in one shapely C call beats a per-element
    # ``geom.wkb`` Python loop by ~11x on a 10k shapely-backed GeoSeries.
    wkb_values = series.to_wkb()
    crs = _crs_to_geopandas(series.crs)
    return from_wkb(wkb_values, crs=crs)


def _dataframe_values(series: Any) -> Any:
    """Keep native dense columns bulk; normalize pandas null sentinels."""
    import pandas as pd

    values = (
        series.to_numpy(dtype=object, na_value=None)
        if pd.api.types.is_extension_array_dtype(series.dtype) and series.isna().any()
        else series.to_numpy()
    )
    if values.dtype.kind != 'O':
        return values
    return tuple(
        None if value is pd.NA or value is pd.NaT else value for value in values
    )


@overload
def to_geopandas(
    geometry_array: GeometryArray[Geometry | None], *, drop_epoch: bool = False
) -> gpd.GeoSeries: ...


@overload
def to_geopandas(
    geometry_array: Features, *, drop_epoch: bool = False
) -> gpd.GeoDataFrame: ...


def to_geopandas(
    geometry_array: GeometryArray[Geometry | None] | Features,
    *,
    drop_epoch: bool = False,
) -> gpd.GeoSeries | gpd.GeoDataFrame:
    """Convert geometries or aligned features to a geopandas object.

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
    from gometry._lib import _ArrowColumn
    from gometry._types import Features

    if isinstance(geometry_array, Features):
        from gometry._feature_arrow import _property_columns

        features = geometry_array
        geometry_name = features._geometry_column or 'geometry'
        columns = _property_columns(features)
        if geometry_name in columns:
            raise ValueError(
                f'geometry column {geometry_name!r} conflicts with a property'
            )
        for name, values in columns.items():
            if isinstance(values, GeometryArray):
                columns[name] = to_geopandas(values, drop_epoch=drop_epoch)
            elif isinstance(values, _ArrowColumn):
                import pandas as pd

                columns[name] = pd.Series(list(values), dtype=object)
        if features._id_column is not None:
            if features._id_column in columns or features._id_column == geometry_name:
                raise ValueError('feature ID column conflicts with another column')
            import pandas as pd

            columns[features._id_column] = pd.Series(list(features.ids), dtype=object)
        columns[geometry_name] = to_geopandas(
            features.geometries, drop_epoch=drop_epoch
        )
        frame = gpd.GeoDataFrame(columns, geometry=geometry_name)
        if features._id_column is not None:
            frame.attrs['gometry:feature_id'] = features._id_column
        return frame

    from shapely import from_wkb

    if geometry_array.epoch is not None and not drop_epoch:
        raise GeometryError(
            'to_geopandas cannot encode coordinate epoch metadata in WKB; '
            'pass drop_epoch=True to acknowledge the loss'
        )
    # One vectorized shapely decode of the batched WKB beats a per-blob loop.
    geoms = from_wkb(geometry_array.to_wkb(drop_epoch=drop_epoch))
    return gpd.GeoSeries(geoms, crs=_crs_to_geopandas(geometry_array.crs))
