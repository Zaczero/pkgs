"""Import-cheap pandas conversion entry point."""

from __future__ import annotations

from types import SimpleNamespace

from gometry._lib import (  # noqa: TC001 - required by runtime get_type_hints
    Geometry,
    GeometryArray,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    import pandas as pd
else:
    pd = SimpleNamespace(Series=object)


def from_pandas(series: pd.Series) -> GeometryArray[Geometry | None]:
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
    ModuleNotFoundError
        If pandas is not installed.
    TypeError
        If the Series is not backed by gometry's extension array.
    """
    from gometry._pandas import GeometryExtensionArray

    data = series.array
    if not isinstance(data, GeometryExtensionArray):
        raise TypeError(
            'expected a Series backed by gometry GeometryExtensionArray, '
            f'got {type(data).__name__}'
        )
    return data.geometry_array
