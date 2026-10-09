"""Import-cheap pandas conversion entry point."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gometry._lib import (
    Geometry,
    GeometryArray,
)

if TYPE_CHECKING:
    import pandas as pd


def from_pandas(series: pd.Series) -> GeometryArray[Geometry | None]:
    """Import a pandas Series of geometries.

    A Series backed by ``GeometryExtensionArray`` shares its native storage
    without copying. An ordinary object-dtype Series imports Geometry objects,
    WKB buffers, GeoJSON mappings, and ``__geo_interface__`` rows; pandas
    missing values become missing geometry rows.

    Parameters
    ----------
    series : pandas.Series
        A geometry Series. Use ``from_geopandas`` for a GeoSeries so its CRS is
        retained.

    Returns
    -------
    GeometryArray

    Raises
    ------
    ModuleNotFoundError
        If pandas is not installed.
    TypeError
        If a row is not a supported geometry representation.
    """
    import pandas as pd

    from gometry._pandas import GeometryExtensionArray

    if not isinstance(series, pd.Series):
        raise TypeError(f'expected pandas.Series, got {type(series).__name__}')

    data = series.array
    if isinstance(data, GeometryExtensionArray):
        return data.geometry_array
    if any(
        base.__name__ == 'GeoSeries' and base.__module__ == 'geopandas.geoseries'
        for base in type(series).__mro__
    ):
        raise TypeError(
            'from_pandas does not preserve GeoSeries CRS metadata; use from_geopandas'
        )
    missing = series.isna().tolist()
    return GeometryArray(
        None if is_missing else value
        for value, is_missing in zip(data, missing, strict=True)
    )
